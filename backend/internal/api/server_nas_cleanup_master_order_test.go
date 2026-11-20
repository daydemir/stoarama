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

// cleanupMasterReadSQL is the authoritative FOR SHARE read. It is the only
// statement allowed to take the master row lock on a deletion path.
const cleanupMasterReadSQL = `SELECT enabled FROM nas_cleanup_master FOR SHARE`

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
