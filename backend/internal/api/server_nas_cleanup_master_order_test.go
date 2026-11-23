package api

import (
	"context"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// cleanupMasterLocksWhileWaiting counts backends that already hold the master
// row lock and are now blocked on some other fence. Every one of them would
// make an emergency OFF queue behind that fence, which is exactly the latency
// the master must never have.
func cleanupMasterLocksWhileWaiting(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	err := pool.QueryRow(context.Background(), `SELECT count(*) FROM pg_locks l
 JOIN pg_stat_activity a ON a.pid=l.pid
 WHERE l.relation='nas_cleanup_master'::regclass AND l.mode='RowShareLock' AND l.granted
 AND a.wait_event_type='Lock' AND a.datname=current_database()`).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// flipMasterOffWithin returns the handler's status, or fails if OFF waited
// longer than the bound. An OFF that queues behind a fence the caller has not
// even reached yet means the master lock was taken too early.
func flipMasterOffWithin(t *testing.T, s *Server, bound time.Duration) int {
	t.Helper()
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- cleanupMasterCall(s, map[string]any{"enabled": false, "claimed_by": "test", "reason": "emergency off"})
	}()
	select {
	case w := <-done:
		if w.Code != 200 {
			t.Fatalf("emergency OFF: %d %s", w.Code, w.Body.String())
		}
		return w.Code
	case <-time.After(bound):
		t.Fatalf("emergency OFF stalled for more than %v", bound)
		return 0
	}
}

// A real dry-run observation and a lease that offers zero execution files are
// non-deleters: neither may take the master row lock at all, so an emergency
// OFF can never wait behind them, whatever fence they are stuck on.
func TestNASCleanupDryRunObservationAndZeroFileLeaseNeverLockTheMaster(t *testing.T) {
	f := newCleanupFixture(t, 24)
	_, _, _ = f.admit("off-no-stall-dry", syntheticFiles(100, 4, 10))
	_, _, _ = f.admit("off-no-stall-lease", syntheticFiles(300, 4, 10))
	setCleanupMaster(t, f.s, true)
	ctx, cancel := context.WithTimeout(f.ctx, 60*time.Second)
	defer cancel()
	observedTask := f.leaseV2(2, 0, 0, "")
	if len(observedTask) == 0 {
		t.Fatal("no dry-run task to observe")
	}

	t.Run("observation", func(t *testing.T) {
		blocker, err := f.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer blocker.Rollback(context.Background())
		if _, err = blocker.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasCleanupPublicFence); err != nil {
			t.Fatal(err)
		}
		observed := make(chan *httptest.ResponseRecorder, 1)
		task := observedTask[0]
		go func() {
			observed <- cleanupCall(t, f.s.handleAccountNASCleanupObserve, 47, 5, "itemId", strconv.FormatInt(task.ItemID, 10), map[string]any{"attempt": task.Attempt, "plan_sha256": task.PlanSHA, "reconciliation_protocol": nasCleanupReconciliationProtocol, "outcome": "match", "proof": fixtureProof(task.Item), "error": "", "client_version": f.version}, "fixture-boot "+f.version)
		}()
		cleanupWaitBlocked(t, f, blocker.Conn().PgConn().PID())
		if n := cleanupMasterLocksWhileWaiting(t, f.pool); n != 0 {
			t.Fatalf("dry-run observation held the master row lock while fenced: %d", n)
		}
		flipMasterOffWithin(t, f.s, 5*time.Second)
		if err = blocker.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		select {
		case w := <-observed:
			if w.Code != 200 || strings.Contains(w.Body.String(), `"delete":true`) {
				t.Fatalf("dry-run observation after OFF: %d %s", w.Code, w.Body.String())
			}
		case <-ctx.Done():
			t.Fatal("dry-run observation did not finish")
		}
	})

	t.Run("zero-execution-file lease", func(t *testing.T) {
		setCleanupMaster(t, f.s, true)
		// Block the lease inside its expiry sweep, after its scope fences and
		// before any master read: an early master read would wait here.
		leasePlan, _, leaseIDs := f.admit("off-no-stall-lease-2", syntheticFiles(400, 4, 10))
		if _, err := f.pool.Exec(ctx, `UPDATE nas_cleanup_items SET state='leased',lease_expires_at=now()-interval '1 minute' WHERE id=$1`, leaseIDs[0]); err != nil {
			t.Fatal(err)
		}
		blocker, err := f.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer blocker.Rollback(context.Background())
		if _, err = blocker.Exec(ctx, `SELECT id FROM nas_cleanup_items WHERE id=$1 FOR UPDATE`, leaseIDs[0]); err != nil {
			t.Fatal(err)
		}
		leased := make(chan *httptest.ResponseRecorder, 1)
		go func() {
			leased <- cleanupCall(t, f.s.handleAccountNASCleanupLease, 47, 5, "", "", map[string]any{
				"client_version": f.version, "protocol": 2, "reconciliation_protocol": nasCleanupReconciliationProtocol,
				"limit": 8, "execution_files": 0, "execution_bytes": 0}, "fixture-boot "+f.version)
		}()
		cleanupWaitBlocked(t, f, blocker.Conn().PgConn().PID())
		if n := cleanupMasterLocksWhileWaiting(t, f.pool); n != 0 {
			t.Fatalf("zero-execution-file lease held the master row lock while fenced: %d", n)
		}
		flipMasterOffWithin(t, f.s, 5*time.Second)
		if err = blocker.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		select {
		case w := <-leased:
			if w.Code != 200 {
				t.Fatalf("lease after OFF: %d %s", w.Code, w.Body.String())
			}
			if strings.Contains(w.Body.String(), "approved_proof") {
				t.Fatalf("zero-execution-file lease returned execution work: %s", w.Body.String())
			}
		case <-ctx.Done():
			t.Fatal("lease did not finish")
		}
		for _, task := range f.leaseV2(8, 0, 0, "") {
			if task.ApprovedProof != nil {
				t.Fatalf("permit leased after OFF: %+v", task)
			}
		}
		_ = leasePlan
	})
}

// Batch enable must read the master only after its own plan locks: flipping the
// master OFF while the handler is still blocked must succeed at once, and the
// final read must then refuse the batch instead of creating one.
func TestNASCleanupBatchEnableReadsMasterAfterEveryFence(t *testing.T) {
	f := newCleanupFixture(t, 24)
	p1, sha1, _ := f.admit("batch-order", syntheticFiles(100, 2, 10))
	f.dryRun()
	setCleanupMaster(t, f.s, true)
	ctx, cancel := context.WithTimeout(f.ctx, 60*time.Second)
	defer cancel()

	blocker, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(context.Background())
	if _, err = blocker.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasCleanupPlanLockBase+p1); err != nil {
		t.Fatal(err)
	}
	enabled := make(chan struct {
		code int
		body string
	}, 1)
	go func() {
		code, body := f.enableBatch([]int64{p1}, []string{sha1}, 2)
		enabled <- struct {
			code int
			body string
		}{code, body}
	}()
	cleanupWaitBlocked(t, f, blocker.Conn().PgConn().PID())
	if n := cleanupMasterLocksWhileWaiting(t, f.pool); n != 0 {
		t.Fatalf("batch enable held the master row lock while waiting on a plan fence: %d", n)
	}
	flipMasterOffWithin(t, f.s, 5*time.Second)
	if err = blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-enabled:
		if got.code != 409 || !strings.Contains(got.body, "master switch") {
			t.Fatalf("batch enable after OFF: %d %s", got.code, got.body)
		}
	case <-ctx.Done():
		t.Fatal("batch enable did not finish")
	}
	var batches int
	if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM nas_cleanup_batches`).Scan(&batches); err != nil || batches != 0 {
		t.Fatalf("batch created after OFF: %d %v", batches, err)
	}
}

// A plan enable whose master is already OFF must refuse from the advisory peek
// before it reaches any blocking fence; without the peek it would sit on the
// public fence until the other holder commits.
func TestNASCleanupControlEnableRefusesOffMasterBeforeAnyFence(t *testing.T) {
	f := newCleanupFixture(t, 24)
	plan, sha, _ := f.admit("peek-order", syntheticFiles(100, 2, 10))
	f.dryRun()
	setCleanupMaster(t, f.s, false)
	ctx, cancel := context.WithTimeout(f.ctx, 60*time.Second)
	defer cancel()

	blocker, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(context.Background())
	if _, err = blocker.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasCleanupPublicFence); err != nil {
		t.Fatal(err)
	}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- cleanupCall(t, f.s.handleAdminNASCleanupControl, 0, 0, "id", strconv.FormatInt(plan, 10), map[string]any{"plan_sha256": sha, "mode": "enabled", "reason": "off master"})
	}()
	select {
	case w := <-done:
		if w.Code != 409 || !strings.Contains(w.Body.String(), "master switch") {
			t.Fatalf("enable with master OFF: %d %s", w.Code, w.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("plan enable waited on a fence instead of refusing an already-OFF master")
	}
	if mode, _ := f.planMode(plan); mode != "dry_run" {
		t.Fatalf("plan armed with master OFF: %s", mode)
	}
}

// cleanupQueryOrder records the SQL of every statement a pool runs, in order,
// so a test can assert that a decision was made after the fences it guards on.
type cleanupQueryOrder struct {
	mu  sync.Mutex
	sql []string
}

func (o *cleanupQueryOrder) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	o.mu.Lock()
	o.sql = append(o.sql, d.SQL)
	o.mu.Unlock()
	return ctx
}

func (o *cleanupQueryOrder) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// since returns the SQL of every statement after position i.
func (o *cleanupQueryOrder) since(i int) []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	if i+1 >= len(o.sql) {
		return nil
	}
	out := make([]string, len(o.sql)-i-1)
	copy(out, o.sql[i+1:])
	return out
}

// index returns the first statement containing every fragment, or -1.
func (o *cleanupQueryOrder) index(fragments ...string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	for n, sql := range o.sql {
		matched := true
		for _, fragment := range fragments {
			if !strings.Contains(sql, fragment) {
				matched = false
				break
			}
		}
		if matched {
			return n
		}
	}
	return -1
}

// Batch advance must not read the master before the freeze activation it
// guards: activation takes table locks and performs replay work, so holding the
// master lock across it would let an emergency OFF wait on all of that.
func TestNASCleanupBatchAdvanceReadsMasterAfterActivation(t *testing.T) {
	f := newCleanupFixture(t, 24)
	p1, sha1, _ := f.admit("advance-order-1", syntheticFiles(100, 2, 10))
	p2, sha2, _ := f.admit("advance-order-2", syntheticFiles(200, 2, 10))
	f.dryRun()
	setCleanupMaster(t, f.s, true)
	if code, body := f.enableBatch([]int64{p1, p2}, []string{sha1, sha2}, 2); code != 200 {
		t.Fatalf("batch enable: %d %s", code, body)
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET state='keep' WHERE plan_id=$1`, p1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 30*time.Second)
	defer cancel()
	order := &cleanupQueryOrder{}
	cfg := f.pool.Config()
	cfg.ConnConfig.Tracer = order
	traced, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer traced.Close()
	s := &Server{pool: traced, r2: f.s.r2, cfg: f.s.cfg}
	if err = s.cleanupAdvanceBatch(ctx); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if mode, _ := f.planMode(p2); mode != "enabled" {
		t.Fatalf("advance did not enable the next plan: %s", mode)
	}
	activation := order.index("INSERT INTO nas_cleanup_frozen_streams")
	master := order.index(cleanupMasterReadSQL)
	if activation < 0 {
		t.Fatalf("advance never activated the ranking freeze: %v", order.sql)
	}
	if master < 0 {
		t.Fatalf("advance never read the master: %v", order.sql)
	}
	if master < activation {
		t.Fatalf("advance read the master at %d before activation at %d", master, activation)
	}
}

// A batch observation on an enabled plan must take the blocking budget
// advisory lock before the master row lock: holding the master first would make
// an emergency OFF wait for whoever holds the budget.
func TestNASCleanupBatchObservationTakesMasterAfterBudgetFence(t *testing.T) {
	f := newCleanupFixture(t, 24)
	plan, sha, _ := f.admit("batch-budget-order", syntheticFiles(100, 4, 10))
	f.dryRun()
	setCleanupMaster(t, f.s, true)
	if code, body := f.enableBatch([]int64{plan}, []string{sha}, 2); code != 200 {
		t.Fatalf("batch enable: %d %s", code, body)
	}
	tasks := f.leaseV2(64, 2, 1<<40, "")
	if len(tasks) == 0 || tasks[0].ApprovedProof == nil {
		t.Fatalf("no execution task: %+v", tasks)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 60*time.Second)
	defer cancel()

	blocker, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(context.Background())
	if _, err = blocker.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasCleanupBudgetLock); err != nil {
		t.Fatal(err)
	}
	observed := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		observed <- cleanupCall(t, f.s.handleAccountNASCleanupObserveBatch, 47, 5, "", "", map[string]any{"client_version": f.version, "observations": []any{map[string]any{
			"item_id": tasks[0].ItemID, "attempt": tasks[0].Attempt, "plan_sha256": tasks[0].PlanSHA,
			"reconciliation_protocol": nasCleanupReconciliationProtocol, "outcome": "match",
			"proof": fixtureProof(tasks[0].Item), "error": "", "client_version": f.version}}}, "fixture-boot "+f.version)
	}()
	cleanupWaitBlocked(t, f, blocker.Conn().PgConn().PID())
	if n := cleanupMasterLocksWhileWaiting(t, f.pool); n != 0 {
		t.Fatalf("batch observation held the master row lock while waiting on the budget fence: %d", n)
	}
	flipMasterOffWithin(t, f.s, 5*time.Second)
	if err = blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case w := <-observed:
		if w.Code != 200 || strings.Contains(w.Body.String(), `"delete":true`) {
			t.Fatalf("batch observation after OFF issued authority: %d %s", w.Code, w.Body.String())
		}
	case <-ctx.Done():
		t.Fatal("batch observation did not finish")
	}
}

// The history guard is the only thing standing between the audit trail and a
// forger: every bypass shape must be refused, and each refusal must come from
// the check that names it.
func TestNASCleanupHistoryGuardRefusesEveryBypass(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	s := &Server{pool: pool}
	setCleanupMaster(t, s, true)
	before := len(cleanupMasterHistory(t, pool))
	columns := `(changed_at timestamptz,actor text,claimed_by text,old_enabled boolean,new_enabled boolean,reason text)`
	valid := `INSERT INTO nas_cleanup_master_history(changed_at,actor,claimed_by,old_enabled,new_enabled,reason) VALUES(now(),'forged','',false,true,'forged')`

	// Trigger depth: a direct insert is depth 1 and is refused before the role
	// is even considered.
	_, err := pool.Exec(ctx, valid)
	if err == nil || !strings.Contains(err.Error(), "append-only") {
		t.Fatalf("direct history insert: %v", err)
	}
	// Trigger origin: a trigger on a foreign table (with the recorder's exact
	// columns, so only the origin check can stop it) is refused.
	if _, err = pool.Exec(ctx, `CREATE TABLE cleanup_recorder_probe `+columns+`;
CREATE TRIGGER cleanup_recorder_probe_insert BEFORE INSERT ON cleanup_recorder_probe FOR EACH ROW EXECUTE FUNCTION record_nas_cleanup_master()`); err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `INSERT INTO cleanup_recorder_probe VALUES(now(),'forged','',NULL,true,'forged')`)
	if err == nil || !strings.Contains(err.Error(), "bound to the master table") {
		t.Fatalf("recorder accepted a foreign-trigger origin: %v", err)
	}
	// Trigger event: the recorder never appends for a delete or truncate.
	if _, err = pool.Exec(ctx, `CREATE TABLE cleanup_recorder_delete_probe `+columns+`;
INSERT INTO cleanup_recorder_delete_probe VALUES(now(),'forged','',NULL,true,'forged');
CREATE TRIGGER cleanup_recorder_delete_probe_delete AFTER DELETE ON cleanup_recorder_delete_probe FOR EACH ROW EXECUTE FUNCTION record_nas_cleanup_master()`); err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `DELETE FROM cleanup_recorder_delete_probe`)
	if err == nil || !strings.Contains(err.Error(), "insert/update") {
		t.Fatalf("recorder accepted a non-insert/update event: %v", err)
	}
	// Writer identity: a depth-2 insert from an unrelated table's trigger is
	// refused because the caller is not the recorder role.
	if _, err = pool.Exec(ctx, `CREATE TABLE cleanup_history_probe(id int);
CREATE FUNCTION cleanup_history_probe_insert() RETURNS trigger LANGUAGE plpgsql AS $f$
BEGIN INSERT INTO nas_cleanup_master_history(changed_at,actor,claimed_by,old_enabled,new_enabled,reason)
 VALUES(now(),'forged','',false,true,'forged'); RETURN NEW; END $f$;
CREATE TRIGGER cleanup_history_probe_insert BEFORE INSERT ON cleanup_history_probe FOR EACH ROW EXECUTE FUNCTION cleanup_history_probe_insert()`); err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `INSERT INTO cleanup_history_probe VALUES(1)`)
	if err == nil || !strings.Contains(err.Error(), "only by its recorder") {
		t.Fatalf("unrelated trigger appended history as a non-recorder role: %v", err)
	}
	// Trigger depth again, from the recorder role itself: the role check passes
	// but a depth-1 insert outside the master trigger's chain is still refused.
	var recorderRole bool
	if err = pool.QueryRow(ctx, `SELECT pg_has_role(current_user,'nas_cleanup_master_writer','MEMBER')`).Scan(&recorderRole); err != nil {
		t.Fatal(err)
	}
	if !recorderRole {
		t.Fatal("test role cannot assume the recorder role")
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SET LOCAL ROLE nas_cleanup_master_writer`); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, valid)
	if err == nil || !strings.Contains(err.Error(), "append-only") {
		t.Fatalf("recorder role appended history outside a trigger chain: %v", err)
	}
	if err = tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if got := cleanupMasterHistory(t, pool); len(got) != before {
		t.Fatalf("bypass appended history: %d rows, want %d", len(got), before)
	}
}

// Plan enable resets every item to pending. That reset must not be the first
// statement to touch those rows: taking the master row lock before an
// unprelocked item row would make an emergency OFF queue behind that item
// waiter. Both enable paths (single plan control, batch advance) must fence the
// item rows first, so an OFF arriving while they wait on the item succeeds at
// once and the enable then rolls back instead of arming the plan.
func TestNASCleanupEnablePathsFenceItemRowsBeforeTheMaster(t *testing.T) {
	for _, tc := range []struct {
		name  string
		start func(t *testing.T, f *cleanupFixture, ctx context.Context) func() int
	}{
		{
			name: "plan enable",
			start: func(t *testing.T, f *cleanupFixture, ctx context.Context) func() int {
				plan, sha, _ := f.admit("p1-item-order", syntheticFiles(100, 2, 10))
				f.dryRun()
				return func() int {
					return cleanupCall(t, f.s.handleAdminNASCleanupControl, 0, 0, "id", strconv.FormatInt(plan, 10),
						map[string]any{"plan_sha256": sha, "mode": "enabled", "reason": "item fence order"}).Code
				}
			},
		},
		{
			name: "batch advance",
			start: func(t *testing.T, f *cleanupFixture, ctx context.Context) func() int {
				p1, sha1, _ := f.admit("p1-adv-order-1", syntheticFiles(100, 2, 10))
				p2, sha2, _ := f.admit("p2-adv-order-2", syntheticFiles(200, 2, 10))
				f.dryRun()
				if code, body := f.enableBatch([]int64{p1, p2}, []string{sha1, sha2}, 2); code != 200 {
					t.Fatalf("batch enable: %d %s", code, body)
				}
				if _, err := f.pool.Exec(ctx, `UPDATE nas_cleanup_items SET state='keep' WHERE plan_id=$1`, p1); err != nil {
					t.Fatal(err)
				}
				return func() int {
					if err := f.s.cleanupAdvanceBatch(ctx); err != nil {
						t.Fatalf("advance: %v", err)
					}
					return 200
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newCleanupFixture(t, 24)
			setCleanupMaster(t, f.s, true)
			ctx, cancel := context.WithTimeout(f.ctx, 90*time.Second)
			defer cancel()
			enable := tc.start(t, f, ctx)
			// The next plan in order is the one that gets enabled: its first item
			// is the row a later reset would update.
			var target int64
			if err := f.pool.QueryRow(ctx, `SELECT i.id FROM nas_cleanup_items i JOIN nas_cleanup_plans p ON p.id=i.plan_id
 WHERE p.mode='dry_run' ORDER BY p.id,i.ordinal LIMIT 1`).Scan(&target); err != nil {
				t.Fatal(err)
			}
			blocker, err := f.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer blocker.Rollback(context.Background())
			if _, err = blocker.Exec(ctx, `SELECT id FROM nas_cleanup_items WHERE id=$1 FOR UPDATE`, target); err != nil {
				t.Fatal(err)
			}
			done := make(chan int, 1)
			go func() { done <- enable() }()
			cleanupWaitBlocked(t, f, blocker.Conn().PgConn().PID())
			if n := cleanupMasterLocksWhileWaiting(t, f.pool); n != 0 {
				t.Fatalf("%s held the master row lock while waiting on an item row: %d", tc.name, n)
			}
			flipMasterOffWithin(t, f.s, 5*time.Second)
			if err = blocker.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("blocked enable never finished")
			}
			for _, mode := range planModes(t, f.pool, ctx) {
				if mode == "enabled" {
					t.Fatalf("%s armed a plan after the item-contested OFF", tc.name)
				}
			}
		})
	}
}

// planModes returns every plan's mode in this fixture's schema.
func planModes(t *testing.T, pool interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, ctx context.Context) []string {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT mode FROM nas_cleanup_plans ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var modes []string
	for rows.Next() {
		var mode string
		if err := rows.Scan(&mode); err != nil {
			t.Fatal(err)
		}
		modes = append(modes, mode)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return modes
}

// The authoritative master read must be the last statement that can block before
// the commit: no SQL and no lock acquisition may follow it, or an emergency OFF
// queues behind that work. This traces a real execution observation and requires
// the read to come after the fences, the rank replay, the aborted-stage lookup and
// the tentative item write, with only COMMIT/ROLLBACK after it.
func TestNASCleanupObservationDoesNoSQLOrLockingAfterTheMasterRead(t *testing.T) {
	f := newCleanupFixture(t, 24)
	plan, sha, _ := f.admit("order-after-master", syntheticFiles(100, 4, 10))
	f.dryRun()
	setCleanupMaster(t, f.s, true)
	if code, body := f.enableBatch([]int64{plan}, []string{sha}, 2); code != 200 {
		t.Fatalf("batch enable: %d %s", code, body)
	}
	tasks := f.leaseV2(8, 1, int64(1)<<40, "")
	if len(tasks) == 0 || tasks[0].ApprovedProof == nil {
		t.Fatalf("no execution task: %+v", tasks)
	}
	task := tasks[0]
	ctx, cancel := context.WithTimeout(f.ctx, 60*time.Second)
	defer cancel()
	order := &cleanupQueryOrder{}
	cfg := f.pool.Config()
	cfg.ConnConfig.Tracer = order
	traced, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer traced.Close()
	// The traced pool backs the real handler, including its R2 prefetch.
	f.s = &Server{pool: traced, r2: f.s.r2, cfg: f.s.cfg, nasCleanupReportCache: f.s.nasCleanupReportCache}
	results := f.observe([]cleanupTask{task}, nil)
	if len(results) != 1 || results[0].Status != 200 || !results[0].Delete {
		t.Fatalf("observation did not issue: %+v", results)
	}
	master := order.index(cleanupMasterReadSQL)
	if master < 0 {
		t.Fatalf("no authoritative master read: %v", order.sql)
	}
	for _, earlier := range []struct {
		name  string
		match []string
	}{
		{"item fence", []string{"FOR UPDATE OF i"}},
		{"budget advisory lock", []string{"pg_advisory_xact_lock($"}},
		{"tentative item write", []string{"UPDATE nas_cleanup_items"}},
	} {
		at := order.index(earlier.match...)
		if at < 0 {
			t.Fatalf("this observation never ran its %s", earlier.name)
		}
		if at > master {
			t.Fatalf("%s at %d ran after the master read at %d", earlier.name, at, master)
		}
	}
	for _, sql := range order.since(master) {
		upper := strings.ToUpper(strings.TrimSpace(sql))
		if strings.HasPrefix(upper, "COMMIT") || strings.HasPrefix(upper, "ROLLBACK") {
			continue
		}
		t.Fatalf("SQL after the master read: %q", sql)
	}
}

// The post-OFF cleanup an advance performs must be scoped to the exact batch it
// was advancing. It runs after the advance's own locks are released, so without
// that scope it could pause a successor batch or act outside this batch's roster.
// Each unsafe shape gets its own fixture because batch identity is immutable.
func TestNASCleanupPostOffBatchCloseIsScopedToTheExactBatch(t *testing.T) {
	// newAdvancingBatch returns a fixture with one enabled batch covering one
	// plan whose items are all complete, plus the ids the helper needs.
	newAdvancingBatch := func(t *testing.T) (*cleanupFixture, int64, int64, []int64, context.Context, context.CancelFunc) {
		t.Helper()
		f := newCleanupFixture(t, 24)
		plan, sha, _ := f.admit("post-off-scope", syntheticFiles(100, 2, 10))
		f.dryRun()
		setCleanupMaster(t, f.s, true)
		if code, body := f.enableBatch([]int64{plan}, []string{sha}, 2); code != 200 {
			t.Fatalf("batch enable: %d %s", code, body)
		}
		ctx, cancel := context.WithTimeout(f.ctx, 60*time.Second)
		if _, err := f.pool.Exec(ctx, `UPDATE nas_cleanup_items SET state='keep' WHERE plan_id=$1`, plan); err != nil {
			t.Fatal(err)
		}
		var batchID int64
		if err := f.pool.QueryRow(ctx, `SELECT id FROM nas_cleanup_batches WHERE mode='enabled'`).Scan(&batchID); err != nil {
			t.Fatal(err)
		}
		return f, plan, batchID, []int64{plan}, ctx, cancel
	}

	t.Run("successor batch", func(t *testing.T) {
		f, plan, batchID, roster, ctx, cancel := newAdvancingBatch(t)
		defer cancel()
		setCleanupMaster(t, f.s, false)
		// Our batch is superseded: it is no longer the enabled one, and a different
		// batch is. The helper must act on neither.
		if _, err := f.pool.Exec(ctx, `UPDATE nas_cleanup_batches SET mode='paused' WHERE id=$1`, batchID); err != nil {
			t.Fatal(err)
		}
		var successor int64
		if err := f.pool.QueryRow(ctx, `INSERT INTO nas_cleanup_batches(account_id,connection_id,batch_sha256,report_sha256,plan_ids,plan_sha256s,mode,approval_reason,approved_until)
			VALUES(47,13,repeat('c',64),$1,$2,ARRAY[$3],'enabled','successor',now()+interval '1 hour') RETURNING id`, cleanupPolicySHA, roster, strings.Repeat("d", 64)).Scan(&successor); err != nil {
			t.Fatal(err)
		}
		if err := f.s.cleanupPauseBatchAfterOff(ctx, batchID, roster, "master switch is off"); err != nil {
			t.Fatalf("pause after OFF: %v", err)
		}
		var successorMode string
		if err := f.pool.QueryRow(ctx, `SELECT mode FROM nas_cleanup_batches WHERE id=$1`, successor).Scan(&successorMode); err != nil {
			t.Fatal(err)
		}
		if successorMode != "enabled" {
			t.Fatalf("the scoped close paused a successor batch: %s", successorMode)
		}
		if mode, _ := f.planMode(plan); mode != "enabled" {
			t.Fatalf("the scoped close touched a plan owned by a successor batch: %s", mode)
		}
	})

	t.Run("off then on", func(t *testing.T) {
		f, plan, batchID, roster, ctx, cancel := newAdvancingBatch(t)
		defer cancel()
		setCleanupMaster(t, f.s, true)
		if err := f.s.cleanupPauseBatchAfterOff(ctx, batchID, roster, "master switch is off"); err != nil {
			t.Fatalf("pause after OFF: %v", err)
		}
		if mode, _ := f.batchMode(); mode != "enabled" {
			t.Fatalf("the scoped close paused a batch after OFF->ON: %s", mode)
		}
		if mode, _ := f.planMode(plan); mode != "enabled" {
			t.Fatalf("the scoped close closed a plan after OFF->ON: %s", mode)
		}
	})

	t.Run("plan outside the roster", func(t *testing.T) {
		f, plan, batchID, _, ctx, cancel := newAdvancingBatch(t)
		defer cancel()
		setCleanupMaster(t, f.s, false)
		// The batch is still the enabled one, but this advance's roster does not
		// include the completed plan: closing it would reach outside the batch.
		if err := f.s.cleanupPauseBatchAfterOff(ctx, batchID, []int64{plan + 1}, "master switch is off"); err != nil {
			t.Fatalf("pause after OFF: %v", err)
		}
		if mode, _ := f.planMode(plan); mode != "enabled" {
			t.Fatalf("the scoped close paused a completed plan outside its roster: %s", mode)
		}
	})

	t.Run("still off", func(t *testing.T) {
		f, plan, batchID, roster, ctx, cancel := newAdvancingBatch(t)
		defer cancel()
		setCleanupMaster(t, f.s, false)
		if err := f.s.cleanupPauseBatchAfterOff(ctx, batchID, roster, "master switch is off"); err != nil {
			t.Fatalf("pause after OFF: %v", err)
		}
		if mode, reason := f.batchMode(); mode != "paused" || !strings.Contains(reason, "master switch is off") {
			t.Fatalf("scoped close did not pause its own batch: %s %q", mode, reason)
		}
		if mode, _ := f.planMode(plan); mode != "paused" {
			t.Fatalf("scoped close did not close its own completed plan: %s", mode)
		}
	})
}

// The post-OFF bookkeeping does its blocking writes and only then revalidates OFF
// under a locked read. An ON committed in that window must win: the writes are
// discarded so a deliberate restart is never undone. This drives the real
// interleaving without holding the master row across the blocking work.
func TestNASCleanupPostOffStandDownWhenOnCommitsDuringWrites(t *testing.T) {
	f := newCleanupFixture(t, 24)
	plan, sha, _ := f.admit("post-off-race", syntheticFiles(100, 2, 10))
	f.dryRun()
	setCleanupMaster(t, f.s, true)
	if code, body := f.enableBatch([]int64{plan}, []string{sha}, 2); code != 200 {
		t.Fatalf("batch enable: %d %s", code, body)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 90*time.Second)
	defer cancel()
	if _, err := f.pool.Exec(ctx, `UPDATE nas_cleanup_items SET state='keep' WHERE plan_id=$1`, plan); err != nil {
		t.Fatal(err)
	}
	var batchID int64
	if err := f.pool.QueryRow(ctx, `SELECT id FROM nas_cleanup_batches WHERE mode='enabled'`).Scan(&batchID); err != nil {
		t.Fatal(err)
	}
	setCleanupMaster(t, f.s, false)

	// Hold a shared public fence so the helper's plan lock queues, and commit the
	// ON while it waits. The helper then does its writes and must see ON at its
	// final locked read.
	holder, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback(context.Background())
	if _, err = holder.Exec(ctx, `SELECT pg_advisory_xact_lock_shared($1)`, nasCleanupPublicFence); err != nil {
		t.Fatal(err)
	}
	// An exclusive plan lock blocks the helper's ascending plan row locks.
	if _, err = holder.Exec(ctx, `SELECT id FROM nas_cleanup_plans WHERE id=$1 FOR UPDATE`, plan); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- f.s.cleanupPauseBatchAfterOff(ctx, batchID, []int64{plan}, "master switch is off") }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var waiting bool
		if err := f.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND $1::integer=ANY(pg_blocking_pids(pid)))`, holder.Conn().PgConn().PID()).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the post-OFF bookkeeping never queued on its plan fence")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// ON commits while the helper is still queued, before its blocking writes.
	setCleanupMaster(t, f.s, true)
	if err := holder.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("bookkeeping: %v", err)
	}
	if mode, _ := f.batchMode(); mode != "enabled" {
		t.Fatalf("an ON committed before the writes was undone: batch %s", mode)
	}
	if mode, _ := f.planMode(plan); mode != "enabled" {
		t.Fatalf("an ON committed before the writes had its plan closed: %s", mode)
	}
}

// The positive control paths must end with the real locked master read and then
// only COMMIT: no RELEASE SAVEPOINT, no plain read, no further locking statement.
// A savepoint-wrapped read would leave RELEASE as the final statement, which is
// the shape this boundary forbids.
func TestNASCleanupEnablePathsEndWithTheRealLockedRead(t *testing.T) {
	f := newCleanupFixture(t, 24)
	setCleanupMaster(t, f.s, true)
	p1, sha1, _ := f.admit("final-read-enable", syntheticFiles(100, 2, 10))
	f.dryRun()
	ctx, cancel := context.WithTimeout(f.ctx, 60*time.Second)
	defer cancel()
	order := &cleanupQueryOrder{}
	cfg := f.pool.Config()
	cfg.ConnConfig.Tracer = order
	traced, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer traced.Close()
	s := &Server{pool: traced, r2: f.s.r2, cfg: f.s.cfg, nasCleanupReportCache: f.s.nasCleanupReportCache}
	w := cleanupCall(t, s.handleAdminNASCleanupControl, 0, 0, "id", strconv.FormatInt(p1, 10),
		map[string]any{"plan_sha256": sha1, "mode": "enabled", "reason": "final read"})
	if w.Code != 200 {
		t.Fatalf("plan enable: %d %s", w.Code, w.Body.String())
	}
	assertLockedReadThenCommitOnly(t, order)
}

func assertLockedReadThenCommitOnly(t *testing.T, order *cleanupQueryOrder) {
	t.Helper()
	master := -1
	// The LAST authoritative read, not an earlier advisory peek.
	for i, sql := range order.sql {
		if strings.Contains(sql, cleanupMasterReadSQL) {
			master = i
		}
	}
	if master < 0 {
		t.Fatalf("no authoritative master read: %v", order.sql)
	}
	// Only statements inside the deciding transaction matter. The first
	// COMMIT/ROLLBACK after the read ends it; anything later (the best-effort
	// freeze release callback, which opens its own transaction after commit) is
	// post-commit housekeeping, not part of this decision.
	var after []string
	for _, sql := range order.since(master) {
		trimmed := strings.TrimSpace(sql)
		upper := strings.ToUpper(trimmed)
		if strings.HasPrefix(upper, "COMMIT") || strings.HasPrefix(upper, "ROLLBACK") {
			break
		}
		after = append(after, trimmed)
	}
	if len(after) != 0 {
		t.Fatalf("statements after the locked read before COMMIT/ROLLBACK: %q", after)
	}
}

// A positive advance must end with the real locked master read and then only
// COMMIT. A savepoint-wrapped probe would leave RELEASE as the final statement,
// which is the shape the transaction boundary forbids.
func TestNASCleanupAdvanceEndsWithTheRealLockedRead(t *testing.T) {
	f := newCleanupFixture(t, 24)
	setCleanupMaster(t, f.s, true)
	p1, sha1, _ := f.admit("adv-final-1", syntheticFiles(100, 2, 10))
	p2, sha2, _ := f.admit("adv-final-2", syntheticFiles(200, 2, 10))
	f.dryRun()
	if code, body := f.enableBatch([]int64{p1, p2}, []string{sha1, sha2}, 2); code != 200 {
		t.Fatalf("batch enable: %d %s", code, body)
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET state='keep' WHERE plan_id=$1`, p1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 60*time.Second)
	defer cancel()
	order := &cleanupQueryOrder{}
	cfg := f.pool.Config()
	cfg.ConnConfig.Tracer = order
	traced, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer traced.Close()
	s := &Server{pool: traced, r2: f.s.r2, cfg: f.s.cfg, nasCleanupReportCache: f.s.nasCleanupReportCache}
	if err = s.cleanupAdvanceBatch(ctx); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if mode, _ := f.planMode(p2); mode != "enabled" {
		t.Fatalf("advance did not enable the next plan: %s", mode)
	}
	assertLockedReadThenCommitOnly(t, order)
}

// The post-OFF pause must do the same: its readable-OFF branch ends with the real
// locked read and then only COMMIT/ROLLBACK. This traces the helper's own
// transaction.
func TestNASCleanupPostOffPauseEndsWithTheRealLockedRead(t *testing.T) {
	f := newCleanupFixture(t, 24)
	plan, sha, _ := f.admit("pause-final", syntheticFiles(100, 2, 10))
	f.dryRun()
	setCleanupMaster(t, f.s, true)
	if code, body := f.enableBatch([]int64{plan}, []string{sha}, 2); code != 200 {
		t.Fatalf("batch enable: %d %s", code, body)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 60*time.Second)
	defer cancel()
	if _, err := f.pool.Exec(ctx, `UPDATE nas_cleanup_items SET state='keep' WHERE plan_id=$1`, plan); err != nil {
		t.Fatal(err)
	}
	var batchID int64
	if err := f.pool.QueryRow(ctx, `SELECT id FROM nas_cleanup_batches WHERE mode='enabled'`).Scan(&batchID); err != nil {
		t.Fatal(err)
	}
	setCleanupMaster(t, f.s, false)
	order := &cleanupQueryOrder{}
	cfg := f.pool.Config()
	cfg.ConnConfig.Tracer = order
	traced, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer traced.Close()
	s := &Server{pool: traced, r2: f.s.r2, cfg: f.s.cfg, nasCleanupReportCache: f.s.nasCleanupReportCache}
	if err = s.cleanupPauseBatchAfterOff(ctx, batchID, []int64{plan}, "master switch is off"); err != nil {
		t.Fatalf("pause after OFF: %v", err)
	}
	if mode, _ := f.batchMode(); mode != "paused" {
		t.Fatalf("pause did not land: %s", mode)
	}
	assertLockedReadThenCommitOnly(t, order)
}

// Both observation endpoints must take exactly one initial posture read and carry
// it through the provider prefetch. A second read inside the prefetch could
// disagree with the first across a race. This traces a certified observation on
// each endpoint and counts master reads before any transaction's locked read.
func TestNASCleanupObservationReadsTheMasterOnceBeforePrefetch(t *testing.T) {
	f := newCleanupFixture(t, 24)
	f.useClientVersion(nasCleanupCertifiedStitchVersion)
	plan, sha, _ := f.admit("one-posture", syntheticFiles(100, 2, 10))
	f.dryRun()
	setCleanupMaster(t, f.s, true)
	if code, body := f.enableBatch([]int64{plan}, []string{sha}, 2); code != 200 {
		t.Fatalf("batch enable: %d %s", code, body)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 60*time.Second)
	defer cancel()
	tasks := f.leaseV2(8, 1, int64(1)<<40, "")
	if len(tasks) == 0 {
		t.Fatalf("no task")
	}
	task := tasks[0]
	order := &cleanupQueryOrder{}
	cfg := f.pool.Config()
	cfg.ConnConfig.Tracer = order
	traced, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer traced.Close()
	s := &Server{pool: traced, r2: f.s.r2, cfg: f.s.cfg, nasCleanupReportCache: f.s.nasCleanupReportCache}
	// Observation trace starts here; the count is only of unlocked posture reads,
	// i.e. reads that are not the locked `FOR SHARE` decision.
	before := countUnlockedMasterReads(order)
	_ = cleanupCall(t, s.handleAccountNASCleanupObserve, 47, 5, "itemId", strconv.FormatInt(task.ItemID, 10),
		cleanupObserve{ReconciliationProtocol: nasCleanupReconciliationProtocol, Attempt: task.Attempt,
			PlanSHA: task.PlanSHA, Outcome: "match", Proof: fixtureProof(task.Item), ClientVersion: f.version},
		"fixture-boot "+f.version)
	after := countUnlockedMasterReads(order)
	if got := after - before; got > 1 {
		t.Fatalf("the single-item endpoint read the master %d times before its locked decision, want at most 1", got)
	}
}

// countUnlockedMasterReads counts posture reads of the control (any read that is
// not the locked `FOR SHARE` decision).
func countUnlockedMasterReads(order *cleanupQueryOrder) int {
	order.mu.Lock()
	defer order.mu.Unlock()
	n := 0
	for _, sql := range order.sql {
		trimmed := strings.TrimSpace(sql)
		if !strings.Contains(trimmed, "nas_cleanup_master") {
			continue
		}
		if strings.Contains(trimmed, "FOR SHARE") || strings.Contains(trimmed, "FOR UPDATE") {
			continue
		}
		if strings.HasPrefix(strings.ToUpper(trimmed), "SELECT") {
			n++
		}
	}
	return n
}

// A lease request that cannot execute must not trigger the advance, which takes
// the exclusive public fence and the master row lock. The gate requires a
// reviewed execution session plus nonzero files AND bytes: a file cap with zero
// bytes leases nothing, so it must not start the advance either.
func TestNASCleanupLeaseAdvanceGateRequiresRealExecutionWork(t *testing.T) {
	cases := []struct {
		name    string
		proto   int
		files   *int
		bytes   *int64
		session string
		want    bool
	}{
		{name: "protocol 1 execution session", proto: 1, session: nasCleanupExecutionClientVersion, want: true},
		{name: "protocol 1 non-execution session", proto: 1, session: "dryrun-only-dev", want: false},
		{name: "protocol 2 files and bytes", proto: 2, files: cleanupCapFiles(1), bytes: cleanupCapBytes(1), session: nasCleanupExecutionClientVersion, want: true},
		{name: "protocol 2 zero files", proto: 2, files: cleanupCapFiles(0), bytes: cleanupCapBytes(1), session: nasCleanupExecutionClientVersion, want: false},
		{name: "protocol 2 zero bytes", proto: 2, files: cleanupCapFiles(4), bytes: cleanupCapBytes(0), session: nasCleanupExecutionClientVersion, want: false},
		{name: "protocol 2 nil caps", proto: 2, session: nasCleanupExecutionClientVersion, want: false},
		{name: "protocol 2 non-execution session", proto: 2, files: cleanupCapFiles(1), bytes: cleanupCapBytes(1), session: "dryrun-only-dev", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := cleanupLeaseRequestCanExecute(tc.proto, tc.files, tc.bytes, cleanupClientSession{version: tc.session, boot: "b"})
			if got != tc.want {
				t.Fatalf("canExecute=%t, want %t", got, tc.want)
			}
		})
	}
}

func cleanupCapFiles(v int) *int     { return &v }
func cleanupCapBytes(v int64) *int64 { return &v }
