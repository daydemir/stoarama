package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// setCleanupMaster flips the durable master switch through the audited
// operator handler, exactly as `stoaramactl nas-cleanup master` does.
func setCleanupMaster(t testing.TB, s *Server, on bool) {
	t.Helper()
	b, _ := json.Marshal(map[string]any{"enabled": on, "claimed_by": "test", "reason": "fixture"})
	w := httptest.NewRecorder()
	s.handleAdminNASCleanupMasterSet(w, httptest.NewRequest("POST", "/api/v1/service/nas-cleanup/master", bytes.NewReader(b)))
	if w.Code != 200 {
		t.Fatalf("set cleanup master %v: %d %s", on, w.Code, w.Body.String())
	}
}

func cleanupMasterCall(s *Server, body map[string]any) *httptest.ResponseRecorder {
	b, _ := json.Marshal(body)
	w := httptest.NewRecorder()
	s.handleAdminNASCleanupMasterSet(w, httptest.NewRequest("POST", "/api/v1/service/nas-cleanup/master", bytes.NewReader(b)))
	return w
}

type cleanupMasterHistoryRow struct {
	Actor, ClaimedBy, Reason string
	Old                      *bool
	New                      bool
}

func cleanupMasterHistory(t *testing.T, pool *pgxpool.Pool) []cleanupMasterHistoryRow {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT actor,claimed_by,reason,old_enabled,new_enabled FROM nas_cleanup_master_history ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (cleanupMasterHistoryRow, error) {
		var h cleanupMasterHistoryRow
		return h, r.Scan(&h.Actor, &h.ClaimedBy, &h.Reason, &h.Old, &h.New)
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestNASCleanupMasterDefaultsOffAndAuditsEveryChange(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	s := &Server{pool: pool}
	if cleanupMasterOn(ctx, pool) {
		t.Fatal("master must ship OFF")
	}
	status := httptest.NewRecorder()
	s.handleAdminNASCleanupMasterStatus(status, httptest.NewRequest("GET", "/api/v1/service/nas-cleanup/master", nil))
	var m cleanupMasterState
	if status.Code != 200 || json.Unmarshal(status.Body.Bytes(), &m) != nil || m.Enabled || m.Actor != "migration 0229" {
		t.Fatalf("status %d %s", status.Code, status.Body.String())
	}
	for _, body := range []map[string]any{
		{"enabled": true, "claimed_by": "test"},
		{"enabled": true, "claimed_by": "test", "reason": ""},
		{"enabled": true, "claimed_by": "test", "reason": "   "},
		{"claimed_by": "test", "reason": "no state"},
		{"enabled": true, "reason": "no name"},
		// The client cannot set the authoritative actor.
		{"enabled": true, "actor": "forged", "claimed_by": "test", "reason": "actor is server-side"},
	} {
		if w := cleanupMasterCall(s, body); w.Code != 400 {
			t.Fatalf("incomplete change %v accepted: %d %s", body, w.Code, w.Body.String())
		}
	}
	type change struct {
		Before, After cleanupMasterState
		Changed       bool
	}
	flip := func(on bool, reason string) change {
		t.Helper()
		w := cleanupMasterCall(s, map[string]any{"enabled": on, "claimed_by": "ops@jelly", "reason": reason})
		var c change
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &c) != nil {
			t.Fatalf("flip: %d %s", w.Code, w.Body.String())
		}
		return c
	}
	if c := flip(true, "start plan 7"); !c.Changed || c.Before.Enabled || !c.After.Enabled || c.After.Actor != nasCleanupControllerActor || c.After.ClaimedBy != "ops@jelly" || c.After.Reason != "start plan 7" || !c.After.ChangedAt.After(c.Before.ChangedAt) {
		t.Fatalf("on: %+v", c)
	}
	if !cleanupMasterOn(ctx, pool) {
		t.Fatal("master ON not read live")
	}
	if c := flip(true, "again"); c.Changed || !c.Before.Enabled || c.After != c.Before {
		t.Fatalf("idempotent on changed state: %+v", c)
	}
	if c := flip(false, "deploy window"); !c.Changed || !c.Before.Enabled || c.After.Enabled {
		t.Fatalf("off: %+v", c)
	}
	on, off := true, false
	want := []cleanupMasterHistoryRow{
		{Actor: "migration 0229", Reason: "initial state: OFF", New: false},
		{Actor: nasCleanupControllerActor, ClaimedBy: "ops@jelly", Reason: "start plan 7", Old: &off, New: true},
		{Actor: nasCleanupControllerActor, ClaimedBy: "ops@jelly", Reason: "deploy window", Old: &on, New: false},
	}
	got := cleanupMasterHistory(t, pool)
	if len(got) != len(want) {
		t.Fatalf("history %+v", got)
	}
	for n := range want {
		if got[n].Actor != want[n].Actor || got[n].ClaimedBy != want[n].ClaimedBy || got[n].Reason != want[n].Reason || got[n].New != want[n].New || (got[n].Old == nil) != (want[n].Old == nil) || (got[n].Old != nil && *got[n].Old != *want[n].Old) {
			t.Fatalf("history[%d] = %+v, want %+v", n, got[n], want[n])
		}
	}
	for _, sql := range []string{
		`UPDATE nas_cleanup_master_history SET reason='rewritten'`,
		`DELETE FROM nas_cleanup_master_history`,
		`TRUNCATE nas_cleanup_master_history`,
		`INSERT INTO nas_cleanup_master_history(changed_at,actor,old_enabled,new_enabled,reason) VALUES(now(),'forged',false,true,'forged')`,
		`DELETE FROM nas_cleanup_master`,
		`TRUNCATE nas_cleanup_master`,
		`UPDATE nas_cleanup_master SET reason='silent edit'`,
		`UPDATE nas_cleanup_master SET reason='   ' WHERE enabled=false`,
		`INSERT INTO nas_cleanup_master(enabled,actor,reason) VALUES(true,'second','row')`,
	} {
		if _, err := pool.Exec(ctx, sql); err == nil {
			t.Fatalf("audit bypass accepted: %s", sql)
		}
	}
	if got := cleanupMasterHistory(t, pool); len(got) != len(want) {
		t.Fatalf("history changed: %+v", got)
	}
	var always int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_trigger WHERE tgrelid IN ('nas_cleanup_master'::regclass,'nas_cleanup_master_history'::regclass) AND NOT tgisinternal AND tgenabled='A'`).Scan(&always); err != nil || always != 5 {
		t.Fatalf("ENABLE ALWAYS triggers %d %v", always, err)
	}
	// The recorder is bound to this schema's history table and to a dedicated
	// insert-only owner, with an explicit search_path.
	var owner, config, body, currentSchema string
	if err := pool.QueryRow(ctx, `SELECT current_schema()`).Scan(&currentSchema); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT pg_get_userbyid(proowner),coalesce(array_to_string(proconfig,','),''),pg_get_functiondef(oid) FROM pg_proc WHERE proname='record_nas_cleanup_master' AND pronamespace=current_schema()::regnamespace`).Scan(&owner, &config, &body); err != nil {
		t.Fatal(err)
	}
	if owner != "nas_cleanup_master_writer" || !strings.Contains(config, "search_path=") || !strings.Contains(body, "INSERT INTO "+currentSchema+".") {
		t.Fatalf("recorder provenance owner=%q config=%q body=%q", owner, config, body)
	}
}

// The master route is controller-only: the runner's service token can never
// flip deletion authority, and an unconfigured controller credential fails
// closed instead of falling back to the service credential.
func TestNASCleanupMasterRejectsServiceTokenAndFailsClosed(t *testing.T) {
	pool := cleanupTestPool(t)
	s := &Server{pool: pool}
	const service = "runner-service-token-32-bytes-long-0000"
	const controller = "nas-cleanup-controller-32-bytes-long-000"
	s.cfg.ServiceToken = service
	s.cfg.APIToken = service
	s.cfg.NASCleanupControllerToken = controller
	// Drive the mounted router, not the handler: the guard is the wiring under
	// test, so a route mounted back on the service credential must fail here.
	router := s.router()
	call := func(path, token string) int {
		req := httptest.NewRequest("POST", path, strings.NewReader(`{"enabled":true,"claimed_by":"ops@jelly","reason":"router probe"}`))
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		return w.Code
	}
	if code := call("/api/v1/service/nas-cleanup/master", service); code != 401 {
		t.Fatalf("runner service token flipped the master through the router: %d", code)
	}
	if code := call("/api/v1/service/nas-cleanup/master", ""); code != 401 {
		t.Fatalf("anonymous master call: %d", code)
	}
	if code := call("/api/v1/service/nas-cleanup/master", controller); code != 200 {
		t.Fatalf("controller token rejected by the router: %d", code)
	}
	for name, mutate := range map[string]func(*Server){
		"unconfigured":  func(b *Server) { b.cfg.NASCleanupControllerToken = "" },
		"service alias": func(b *Server) { b.cfg.NASCleanupControllerToken = service },
	} {
		bad := &Server{pool: pool, cfg: s.cfg}
		mutate(bad)
		guarded := bad.requireCleanupControllerAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
		req := httptest.NewRequest("POST", "/api/v1/service/nas-cleanup/master", nil)
		req.Header.Set("Authorization", "Bearer "+controller)
		w := httptest.NewRecorder()
		guarded.ServeHTTP(w, req)
		if w.Code != 503 {
			t.Fatalf("%s controller credential did not fail closed: %d", name, w.Code)
		}
	}
	// The authenticated principal, not the caller's claim, is the actor.
	var actor, claimed string
	if err := pool.QueryRow(context.Background(), `SELECT actor,claimed_by FROM nas_cleanup_master`).Scan(&actor, &claimed); err != nil || actor != nasCleanupControllerActor || claimed != "ops@jelly" {
		t.Fatalf("actor %q claimed %q %v", actor, claimed, err)
	}
}

// Status must never turn a read error into a false OFF, and it must report both
// real states.
func TestNASCleanupMasterStatusReadsBothStatesAndRefusesUnknown(t *testing.T) {
	pool := cleanupTestPool(t)
	s := &Server{pool: pool}
	planStatus := func() (int, map[string]any) {
		w := cleanupCall(t, s.handleAdminNASCleanupStatus, 47, 5, "id", strconv.FormatInt(testCleanupStatusPlanID(), 10), nil)
		var body map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &body)
		return w.Code, body
	}
	seed := func() {
		t.Helper()
		if _, err := pool.Exec(context.Background(), `INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256) VALUES($1,47,13,'status',$2,'[]',repeat('b',64))`, testCleanupStatusPlanID(), cleanupPolicySHA); err != nil {
			t.Fatal(err)
		}
	}
	seed()
	if _, err := pool.Exec(context.Background(), `INSERT INTO nas_cleanup_batches(account_id,connection_id,batch_sha256,report_sha256,plan_ids,plan_sha256s,mode,approval_reason,approved_until) VALUES(47,13,repeat('c',64),repeat('d',64),ARRAY[$1::bigint],ARRAY[repeat('b',64)],'paused','status fixture',now()+interval '1 hour')`, testCleanupStatusPlanID()); err != nil {
		t.Fatal(err)
	}
	for _, state := range []bool{false, true} {
		setCleanupMaster(t, s, state)
		if code, body := planStatus(); code != 200 || body["master_deletion_enabled"] != state {
			t.Fatalf("plan status with master=%v: %d %v", state, code, body)
		}
		w := cleanupCall(t, s.handleAdminNASCleanupBatchStatus, 47, 5, "id", "1", nil)
		var batch map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &batch)
		if w.Code != 200 || batch["master_deletion_enabled"] != state {
			t.Fatalf("batch status with master=%v: %d %v", state, w.Code, batch)
		}
	}
	for name, breakControl := range map[string]string{
		"missing row":      `ALTER TABLE nas_cleanup_master DISABLE TRIGGER USER;DELETE FROM nas_cleanup_master`,
		"unreadable table": `ALTER TABLE nas_cleanup_master RENAME TO nas_cleanup_master_gone`,
	} {
		t.Run(name, func(t *testing.T) {
			pool := cleanupTestPool(t)
			s := &Server{pool: pool}
			seedID := testCleanupStatusPlanID()
			if _, err := pool.Exec(context.Background(), `INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256) VALUES($1,47,13,'status',$2,'[]',repeat('b',64))`, seedID, cleanupPolicySHA); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(context.Background(), `INSERT INTO nas_cleanup_batches(account_id,connection_id,batch_sha256,report_sha256,plan_ids,plan_sha256s,mode,approval_reason,approved_until) VALUES(47,13,repeat('c',64),repeat('d',64),ARRAY[$1::bigint],ARRAY[repeat('b',64)],'paused','status fixture',now()+interval '1 hour')`, seedID); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(context.Background(), breakControl); err != nil {
				t.Fatal(err)
			}
			if w := cleanupCall(t, s.handleAdminNASCleanupStatus, 47, 5, "id", strconv.FormatInt(seedID, 10), nil); w.Code < 500 {
				t.Fatalf("plan status reported a false state on a read error: %d %s", w.Code, w.Body.String())
			}
			if w := cleanupCall(t, s.handleAdminNASCleanupBatchStatus, 47, 5, "id", "1", nil); w.Code < 500 {
				t.Fatalf("batch status reported a false state on a read error: %d %s", w.Code, w.Body.String())
			}
		})
	}
}

func testCleanupStatusPlanID() int64 { return 909090 }

func TestNASCleanupMasterFailsClosed(t *testing.T) {
	ctx := context.Background()
	for name, breakControl := range map[string]string{
		"missing row":       `ALTER TABLE nas_cleanup_master DISABLE TRIGGER USER;DELETE FROM nas_cleanup_master`,
		"unparseable value": `ALTER TABLE nas_cleanup_master DISABLE TRIGGER USER,ALTER COLUMN enabled DROP NOT NULL;UPDATE nas_cleanup_master SET enabled=NULL`,
		"unreadable table":  `ALTER TABLE nas_cleanup_master RENAME TO nas_cleanup_master_gone`,
	} {
		t.Run(name, func(t *testing.T) {
			pool := cleanupTestPool(t)
			s := &Server{pool: pool}
			setCleanupMaster(t, s, true)
			if _, err := pool.Exec(ctx, breakControl); err != nil {
				t.Fatal(err)
			}
			if cleanupMasterOn(ctx, pool) {
				t.Fatal("broken master control read as ON")
			}
			if _, err := pool.Exec(ctx, `INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256) VALUES(1,47,13,'fixture',$1,'[]',repeat('b',64))`, cleanupPolicySHA); err != nil {
				t.Fatal(err)
			}
			// The master is read last, so an unreadable control surfaces as a
			// refusal, never as an armed plan.
			if w := cleanupCall(t, s.handleAdminNASCleanupControl, 47, 5, "id", "1", map[string]any{"plan_sha256": strings.Repeat("b", 64), "mode": "enabled", "reason": "fixture"}); w.Code != 409 {
				t.Fatalf("broken master allowed arming: %d %s", w.Code, w.Body.String())
			}
			var armed string
			if err := pool.QueryRow(ctx, `SELECT mode FROM nas_cleanup_plans WHERE id=1`).Scan(&armed); err != nil || armed != "dry_run" {
				t.Fatalf("broken master armed the plan: %q %v", armed, err)
			}
			// A failed master read must not poison the caller's transaction:
			// a paused control still commits.
			if w := cleanupCall(t, s.handleAdminNASCleanupControl, 47, 5, "id", "1", map[string]any{"plan_sha256": strings.Repeat("b", 64), "mode": "paused", "reason": "read error must not abort"}); w.Code != 200 {
				t.Fatalf("read error aborted the caller's transaction: %d %s", w.Code, w.Body.String())
			}
			var mode string
			if err := pool.QueryRow(ctx, `SELECT mode FROM nas_cleanup_plans WHERE id=1`).Scan(&mode); err != nil || mode != "paused" {
				t.Fatalf("commit lost: %q %v", mode, err)
			}
		})
	}
}

// A master decision must ride the caller's transaction: it may not need a
// second pool connection (a pool read would either stall or, worse, take the
// lock outside the transaction it guards).
func TestNASCleanupMasterDecisionUsesCallerTransaction(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cfg := pool.Config()
	cfg.MaxConns = 1
	one, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer one.Close()
	s := &Server{pool: one}
	setCleanupMaster(t, s, true)
	tx, err := one.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !cleanupMasterOn(ctx, tx) {
		tx.Rollback(ctx)
		t.Fatal("in-transaction master read did not use the caller's connection")
	}
	// Proof it stayed on the caller's connection: the row lock belongs to the
	// transaction's own backend, so no second pool connection was used.
	var wrongLocks int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM pg_locks WHERE relation='nas_cleanup_master'::regclass AND mode='RowShareLock' AND granted AND pid<>pg_backend_pid()`).Scan(&wrongLocks); err != nil || wrongLocks != 0 {
		t.Fatalf("master decision took its lock on another connection: %d %v", wrongLocks, err)
	}
	// The lock is real: a flip waits while this transaction is open.
	flipped := make(chan int, 1)
	go func() {
		flipped <- cleanupMasterCall(s, map[string]any{"enabled": false, "claimed_by": "test", "reason": "wait"}).Code
	}()
	select {
	case code := <-flipped:
		t.Fatalf("master flip did not wait for the decision: %d", code)
	case <-time.After(150 * time.Millisecond):
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case code := <-flipped:
		if code != 200 {
			t.Fatalf("flip after decision: %d", code)
		}
	case <-ctx.Done():
		t.Fatal("flip did not finish")
	}
}

// A pool-read regression inside a handler is caught: with a single pool
// connection, any decision that went back to the pool while its transaction
// holds that connection would deadlock instead of answering.
func TestNASCleanupMasterHandlerDecisionNeverRereadsPool(t *testing.T) {
	f := newCleanupFixture(t, 24)
	f.admit("pool-read", syntheticFiles(100, 2, 10))
	tasks := f.leaseV2(64, 0, 0, "")
	if len(tasks) != 2 {
		t.Fatalf("lease %d", len(tasks))
	}
	ctx, cancel := context.WithTimeout(f.ctx, 30*time.Second)
	defer cancel()
	cfg := f.pool.Config()
	cfg.MaxConns = 1
	one, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer one.Close()
	single := &Server{pool: one, r2: f.s.r2, cfg: f.s.cfg}
	results := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		observations := []map[string]any{}
		for _, task := range tasks {
			observations = append(observations, map[string]any{"item_id": task.ItemID, "attempt": task.Attempt, "plan_sha256": task.PlanSHA, "reconciliation_protocol": nasCleanupReconciliationProtocol, "outcome": "match", "proof": fixtureProof(task.Item), "error": "", "client_version": f.version})
		}
		results <- cleanupCall(t, single.handleAccountNASCleanupObserveBatch, 47, 5, "", "", map[string]any{"client_version": f.version, "observations": observations}, "fixture-boot "+f.version)
	}()
	select {
	case w := <-results:
		if w.Code != 200 {
			t.Fatalf("single-connection observation: %d %s", w.Code, w.Body.String())
		}
	case <-time.After(20 * time.Second):
		t.Fatal("observation re-read the pool for the master switch and deadlocked")
	}
}

// An emergency OFF must not stall behind a handler that is still waiting on a
// fence, and once it returns no permit may be issued.
func TestNASCleanupMasterOffDoesNotStallBehindFencedHandler(t *testing.T) {
	f := newCleanupFixture(t, 24)
	plan, sha, _ := f.admit("off-no-stall", syntheticFiles(100, 4, 10))
	f.dryRun()
	setCleanupMaster(t, f.s, true)
	ctx, cancel := context.WithTimeout(f.ctx, 60*time.Second)
	defer cancel()

	// A realistic competitor: another operator holds the public fence.
	blocker, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(context.Background())
	if _, err = blocker.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasCleanupPublicFence); err != nil {
		t.Fatal(err)
	}
	enabled := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		enabled <- cleanupCall(t, f.s.handleAdminNASCleanupControl, 0, 0, "id", strconv.FormatInt(plan, 10), map[string]any{"plan_sha256": sha, "mode": "enabled", "reason": "blocked enable"})
	}()
	cleanupWaitBlocked(t, f, blocker.Conn().PgConn().PID())
	// While blocked on the fence the enable must hold no master row lock; if it
	// did, OFF would wait for it.
	var masterLocks int
	if err = f.pool.QueryRow(ctx, `SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid
 WHERE l.relation='nas_cleanup_master'::regclass AND l.mode='RowShareLock' AND l.granted AND a.query LIKE '%mode=$2%'`).Scan(&masterLocks); err != nil {
		t.Fatal(err)
	}
	if masterLocks != 0 {
		t.Fatalf("enable held the master lock while waiting on the fence: %d", masterLocks)
	}

	start := time.Now()
	off := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		off <- cleanupMasterCall(f.s, map[string]any{"enabled": false, "claimed_by": "test", "reason": "emergency off"})
	}()
	select {
	case w := <-off:
		if w.Code != 200 {
			t.Fatalf("emergency OFF: %d %s", w.Code, w.Body.String())
		}
		if waited := time.Since(start); waited > 5*time.Second {
			t.Fatalf("emergency OFF stalled for %v", waited)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("emergency OFF stalled behind a fenced handler")
	}
	if err = blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case w := <-enabled:
		if w.Code != 409 || !strings.Contains(w.Body.String(), "master switch") {
			t.Fatalf("blocked enable proceeded after OFF: %d %s", w.Code, w.Body.String())
		}
	case <-ctx.Done():
		t.Fatal("blocked enable did not finish")
	}
	var mode string
	if err := f.pool.QueryRow(ctx, `SELECT mode FROM nas_cleanup_plans WHERE id=$1`, plan).Scan(&mode); err != nil || mode != "dry_run" {
		t.Fatalf("plan enabled after OFF: %q %v", mode, err)
	}
	if tasks := f.leaseV2(64, 1, 1<<40, ""); len(tasks) != 0 && tasks[0].ApprovedProof != nil {
		t.Fatalf("permit leased after OFF: %+v", tasks)
	}
}

// Batch auto-pause must still commit when the master read fails: the savepoint
// keeps the caller's transaction usable.
func TestNASCleanupMasterReadErrorStillPausesBatch(t *testing.T) {
	f := newCleanupFixture(t, 24)
	p1, sha1, _ := f.admit("autopause-1", syntheticFiles(100, 2, 10))
	p2, sha2, _ := f.admit("autopause-2", syntheticFiles(200, 2, 10))
	f.dryRun()
	setCleanupMaster(t, f.s, true)
	if code, body := f.enableBatch([]int64{p1, p2}, []string{sha1, sha2}, 2); code != 200 {
		t.Fatalf("batch enable: %d %s", code, body)
	}
	// Finish the current plan so the next lease advances the batch.
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET state='keep' WHERE plan_id=$1`, p1); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(f.ctx, `ALTER TABLE nas_cleanup_master RENAME TO nas_cleanup_master_gone`); err != nil {
		t.Fatal(err)
	}
	if err := f.s.cleanupAdvanceBatch(f.ctx); err != nil {
		t.Fatalf("advance failed instead of pausing: %v", err)
	}
	if mode, reason := f.batchMode(); mode != "paused" || !strings.Contains(reason, "master switch is off") {
		t.Fatalf("batch not paused on an unreadable master: %s %q", mode, reason)
	}
	if mode, _ := f.planMode(p2); mode != "dry_run" {
		t.Fatalf("plan enabled with an unreadable master: %s", mode)
	}
}

// Flipping the durable master ON lets an approved, enabled plan execute;
// flipping it OFF mid-plan stops every further lease and permit at once, and
// waits for an in-flight permit decision that already read it ON.
func TestNASCleanupMasterGovernsAnEnabledPlanLive(t *testing.T) {
	f := newCleanupFixture(t, 24)
	p1, sha1, _ := f.admit("master-live", syntheticFiles(100, 4, 10))
	f.dryRun()
	if code, body := f.enableBatch([]int64{p1}, []string{sha1}, 2); code != 409 || !strings.Contains(body, "master switch") {
		t.Fatalf("master OFF allowed batch enable: %d %s", code, body)
	}
	setCleanupMaster(t, f.s, true)
	if code, body := f.enableBatch([]int64{p1}, []string{sha1}, 2); code != 200 {
		t.Fatalf("batch enable: %d %s", code, body)
	}
	w := cleanupCall(t, f.s.handleAdminNASCleanupStatus, 47, 5, "id", strconv.FormatInt(p1, 10), nil)
	var status map[string]any
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &status) != nil || status["master_deletion_enabled"] != true {
		t.Fatalf("plan status with master ON: %d %s", w.Code, w.Body.String())
	}
	execute := func() []cleanupTask {
		t.Helper()
		tasks := f.leaseV2(64, 1, 1<<40, "")
		if len(tasks) != 1 || tasks[0].ApprovedProof == nil {
			t.Fatalf("master ON did not lease execution: %+v", tasks)
		}
		return tasks
	}
	first := execute()
	r := f.observe(first, nil)
	if !r[0].Delete {
		t.Fatalf("master ON withheld the permit: %+v", r[0])
	}
	f.receipts(first, r, "deleted")
	second := execute()

	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !cleanupMasterOn(f.ctx, tx) {
		t.Fatal("in-flight decision read OFF")
	}
	flipped := make(chan int)
	go func() {
		flipped <- cleanupMasterCall(f.s, map[string]any{"enabled": false, "claimed_by": "test", "reason": "mid-plan stop"}).Code
	}()
	select {
	case code := <-flipped:
		t.Fatalf("master OFF did not wait for an in-flight decision: %d", code)
	case <-time.After(300 * time.Millisecond):
	}
	if err = tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	if code := <-flipped; code != 200 {
		t.Fatalf("master OFF: %d", code)
	}

	if r := f.observe(second, nil); r[0].Delete {
		t.Fatalf("master OFF mid-plan issued a permit: %+v", r[0])
	}
	for _, task := range f.leaseV2(64, 64, 1<<40, "") {
		if task.ApprovedProof != nil {
			t.Fatalf("master OFF mid-plan leased execution: %+v", task)
		}
	}
	w = cleanupCall(t, f.s.handleAdminNASCleanupStatus, 47, 5, "id", strconv.FormatInt(p1, 10), nil)
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &status) != nil || status["master_deletion_enabled"] != false {
		t.Fatalf("plan status master: %d %s", w.Code, w.Body.String())
	}
}

// A dry-run observation whose master read fails still commits; the failure is
// treated as OFF for the decision only.
func TestNASCleanupMasterReadErrorKeepsDryRunCommittable(t *testing.T) {
	f := newCleanupFixture(t, 24)
	plan, _, _ := f.admit("dryrun-read-error", syntheticFiles(100, 2, 10))
	tasks := f.leaseV2(64, 0, 0, "")
	if len(tasks) != 2 {
		t.Fatalf("lease %d", len(tasks))
	}
	if _, err := f.pool.Exec(f.ctx, `ALTER TABLE nas_cleanup_master RENAME TO nas_cleanup_master_gone`); err != nil {
		t.Fatal(err)
	}
	results := f.observe(tasks, nil)
	for _, r := range results {
		if r.Delete {
			t.Fatalf("permit issued with an unreadable master: %+v", r)
		}
		if r.Action != "would_delete" {
			t.Fatalf("dry-run observation did not commit: %+v", r)
		}
	}
	var states int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_cleanup_items WHERE plan_id=$1 AND state='would_delete'`, plan).Scan(&states); err != nil || states != 2 {
		t.Fatalf("dry-run state lost: %d %v", states, err)
	}
}

// The Render env var is inert: no code path may read it again.
func TestNASCleanupMasterEnvVarIsGone(t *testing.T) {
	name := "NAS_CLEANUP_" + "DELETION_ENABLED"
	err := filepath.WalkDir("../../..", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == ".git" || d.Name() == "node_modules") {
			return filepath.SkipDir
		}
		switch filepath.Ext(path) {
		case ".go", ".py", ".sh", ".yaml", ".yml", ".js", ".ts", ".swift", ".toml":
		default:
			return nil
		}
		b, err := os.ReadFile(path)
		if err == nil && bytes.Contains(b, []byte(name)) {
			t.Errorf("%s still references %s", path, name)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}
