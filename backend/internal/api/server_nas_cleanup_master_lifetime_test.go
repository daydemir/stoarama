package api

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The permit transaction must not keep an ON across a savepoint rollback, and it
// must not treat a pre-read as authority: the only authoritative decision is the
// `SELECT ... FOR SHARE` that runs last, on the transaction that commits, after
// every blocking step. This drives the real batch handler: the replay of an
// already-issued observation is buffered inside a per-item savepoint (write
// false), an independent OFF commits afterwards, and the response must not
// contain deletion authority. Then the same batch is replayed and must stay a
// pure exact replay (no new permit, no second issuance).
func TestNASCleanupBatchReplayIsNotAuthorityAfterIndependentOff(t *testing.T) {
	f := newCleanupFixture(t, 24)
	f.useClientVersion(nasCleanupSerialGroupClientVersion)
	plan, sha, _ := f.admit("replay-off", syntheticFiles(100, 4, 10))
	f.dryRun()
	setCleanupMaster(t, f.s, true)
	if code, body := f.enableBatch([]int64{plan}, []string{sha}, 2); code != 200 {
		t.Fatalf("batch enable: %d %s", code, body)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 120*time.Second)
	defer cancel()
	requestID := strings.Repeat("d", 32)
	lease := func() []cleanupTask {
		t.Helper()
		w := cleanupCall(t, f.s.handleAccountNASCleanupLease, 47, 5, "", "", map[string]any{
			"client_version": f.version, "protocol": 2, "reconciliation_protocol": nasCleanupReconciliationProtocol,
			"limit": 8, "execution_files": 2, "execution_bytes": int64(1) << 40,
			"request_id": requestID, "execution_batch": true}, "fixture-boot "+f.version)
		if w.Code != 200 {
			t.Fatalf("lease: %d %s", w.Code, w.Body.String())
		}
		var r struct {
			Tasks []cleanupTask `json:"tasks"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		return r.Tasks
	}
	fresh := lease()
	if len(fresh) == 0 || fresh[0].ApprovedProof == nil {
		t.Fatalf("no execution lease: %+v", fresh)
	}
	first := fresh[0]
	roster := make([]int64, 0, len(fresh))
	for _, task := range fresh {
		roster = append(roster, task.ItemID)
	}
	// The exact observation the client submits, including its request id.
	observation := map[string]any{
		"item_id": first.ItemID, "attempt": first.Attempt, "plan_sha256": first.PlanSHA,
		"reconciliation_protocol": nasCleanupReconciliationProtocol, "outcome": "match",
		"proof": fixtureProof(first.Item), "error": "", "client_version": f.version,
		"request_id": requestID,
	}
	batch := func(observations []map[string]any) *httptest.ResponseRecorder {
		t.Helper()
		return cleanupCall(t, f.s.handleAccountNASCleanupObserveBatch, 47, 5, "", "", map[string]any{
			"client_version": f.version, "observations": observations,
			"execution_batch": true, "execution_group": roster}, "fixture-boot "+f.version)
	}
	// The first submission issues and commits a permit.
	if w := batch([]map[string]any{observation}); w.Code != 200 || !strings.Contains(w.Body.String(), `"delete":true`) {
		t.Fatalf("first permit: %d %s", w.Code, w.Body.String())
	}
	// An independent, committed OFF.
	setCleanupMaster(t, f.s, false)
	if cleanupMasterOn(ctx, f.pool) {
		t.Fatal("independent OFF did not commit")
	}
	// The buffered replay inside the batch must not hand back a permit. The
	// authoritative read finds OFF, so the whole pass is discarded and the answer
	// is the ordinary non-deleting one.
	w := batch([]map[string]any{observation})
	if w.Code != 200 {
		t.Fatalf("replay after OFF: %d %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), `"delete":true`) {
		t.Fatalf("replay after OFF returned deletion authority: %s", w.Body.String())
	}
	// Nothing was written: the permit's original expiry is unchanged and no
	// second issuance exists.
	var issued int
	if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM nas_cleanup_items WHERE plan_id=$1 AND state='delete_issued'`, plan).Scan(&issued); err != nil {
		t.Fatal(err)
	}
	if issued != 1 {
		t.Fatalf("replay changed issued permits: %d", issued)
	}
}

// A batch whose tentative pass is discarded must not leak one item's permit.
// The ordinary OFF contract still applies — a non-permit observation is recorded
// as would_delete and pauses the enabled plan — but no deletion authority may
// escape, and that pause must be the *committed* denied-pass result, never the
// rolled-back tentative pass reporting a pause it did not land.
func TestNASCleanupBatchOffLeaksNoTentativePermit(t *testing.T) {
	f := newCleanupFixture(t, 24)
	plan, sha, _ := f.admit("mixed-off", syntheticFiles(100, 4, 10))
	f.dryRun()
	setCleanupMaster(t, f.s, true)
	if code, body := f.enableBatch([]int64{plan}, []string{sha}, 2); code != 200 {
		t.Fatalf("batch enable: %d %s", code, body)
	}
	tasks := f.leaseV2(8, 2, int64(1)<<40, "")
	if len(tasks) == 0 {
		t.Fatal("no execution tasks")
	}
	setCleanupMaster(t, f.s, false)
	results := f.observe(tasks, nil)
	for _, r := range results {
		if r.Delete {
			t.Fatalf("permit escaped an OFF batch: %+v", r)
		}
		if r.Status != 200 || r.Action != "would_delete" {
			t.Fatalf("OFF batch did not record the ordinary non-deleting answer: %+v", r)
		}
	}
	// The pause is real because it came from the committed denied pass.
	if mode, reason := f.planMode(plan); mode != "paused" || !strings.Contains(reason, "auto-paused") {
		t.Fatalf("denied pass pause did not commit: %s %q", mode, reason)
	}
	var issued int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_cleanup_items WHERE plan_id=$1 AND delete_ever_issued`, plan).Scan(&issued); err != nil {
		t.Fatal(err)
	}
	if issued != 0 {
		t.Fatalf("a discarded pass left issued authority behind: %d", issued)
	}
}

// The budget advisory lock belongs to the outer request transaction, not to a
// per-item savepoint: a rolled-back item must not release it, and the issued
// total must only move for permits that really commit.
func TestNASCleanupBudgetAccountingSurvivesRolledBackItemScopes(t *testing.T) {
	f := newCleanupFixture(t, 24)
	plan, sha, _ := f.admit("budget-off", syntheticFiles(100, 4, 10))
	f.dryRun()
	setCleanupMaster(t, f.s, true)
	if code, body := f.enableBatch([]int64{plan}, []string{sha}, 2); code != 200 {
		t.Fatalf("batch enable: %d %s", code, body)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 120*time.Second)
	defer cancel()
	var baseline int64
	if err := f.pool.QueryRow(ctx, cleanupIssuedBytes24hSQL).Scan(&baseline); err != nil {
		t.Fatal(err)
	}
	tasks := f.leaseV2(8, 2, int64(1)<<40, "")
	if len(tasks) == 0 {
		t.Fatal("no execution tasks")
	}
	// A discarded (OFF) pass must not move issued accounting nor leave the budget
	// advisory lock held by a rolled-back item scope.
	setCleanupMaster(t, f.s, false)
	if results := f.observe(tasks, nil); len(results) == 0 {
		t.Fatal("no results")
	}
	var after int64
	if err := f.pool.QueryRow(ctx, cleanupIssuedBytes24hSQL).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != baseline {
		t.Fatalf("an OFF batch changed issued accounting: %d -> %d", baseline, after)
	}
	var free bool
	if err := f.pool.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, nasCleanupBudgetLock).Scan(&free); err != nil {
		t.Fatal(err)
	}
	if !free {
		t.Fatal("a rolled-back item scope left the budget advisory lock held")
	}
	if _, err := f.pool.Exec(ctx, `SELECT pg_advisory_unlock($1)`, nasCleanupBudgetLock); err != nil {
		t.Fatal(err)
	}
}

// A permit that really commits is the only thing that moves the issued total.
func TestNASCleanupCommittedPermitIsAccounted(t *testing.T) {
	f := newCleanupFixture(t, 24)
	plan, sha, _ := f.admit("budget-on", syntheticFiles(100, 4, 10))
	f.dryRun()
	setCleanupMaster(t, f.s, true)
	if code, body := f.enableBatch([]int64{plan}, []string{sha}, 2); code != 200 {
		t.Fatalf("batch enable: %d %s", code, body)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 120*time.Second)
	defer cancel()
	var baseline int64
	if err := f.pool.QueryRow(ctx, cleanupIssuedBytes24hSQL).Scan(&baseline); err != nil {
		t.Fatal(err)
	}
	tasks := f.leaseV2(8, 1, int64(1)<<40, "")
	if len(tasks) == 0 {
		t.Fatal("no execution task")
	}
	results := f.observe(tasks, nil)
	if len(results) == 0 || !results[0].Delete {
		t.Fatalf("no permit issued: %+v", results)
	}
	var after int64
	if err := f.pool.QueryRow(ctx, cleanupIssuedBytes24hSQL).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after <= baseline {
		t.Fatalf("committed permit was not accounted: %d -> %d", baseline, after)
	}
}

// The single-item permit path obeys the same boundary as the batch: with the
// switch OFF the authoritative read discards the tentative permit, the request is
// re-run under the denied posture, and the answer is the ordinary non-deleting
// observation that auto-pauses the enabled plan. No deletion authority escapes.
func TestNASCleanupSingleObservationOffIssuesNoPermit(t *testing.T) {
	f := newCleanupFixture(t, 24)
	plan, sha, _ := f.admit("single-off", syntheticFiles(100, 4, 10))
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
	setCleanupMaster(t, f.s, false)
	before := f.issuedPermits(t, plan)
	w := cleanupCall(t, f.s.handleAccountNASCleanupObserve, 47, 5, "itemId", strconv.FormatInt(task.ItemID, 10),
		cleanupObserve{ReconciliationProtocol: nasCleanupReconciliationProtocol, Attempt: task.Attempt,
			PlanSHA: task.PlanSHA, Outcome: "match", Proof: fixtureProof(task.Item), ClientVersion: f.version},
		"fixture-boot "+f.version)
	if w.Code != 200 {
		t.Fatalf("observation under OFF: %d %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), `"delete":true`) {
		t.Fatalf("single observation issued authority under OFF: %s", w.Body.String())
	}
	if after := f.issuedPermits(t, plan); after != before {
		t.Fatalf("permit count changed under OFF: %d -> %d", before, after)
	}
	// The denied pass still records the ordinary observation and pause.
	if mode, reason := f.planMode(plan); mode != "paused" || !strings.Contains(reason, "auto-paused") {
		t.Fatalf("denied single observation did not pause: %s %q", mode, reason)
	}
}

// issuedPermits counts items in the plan that ever received a permit.
func (f *cleanupFixture) issuedPermits(t *testing.T, plan int64) int {
	t.Helper()
	var n int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_cleanup_items WHERE plan_id=$1 AND delete_ever_issued`, plan).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// The final master lock can itself wait. Every deadline the grant depends on is
// retained when the permit is tentatively made and rechecked against the clock
// after that lock, before the commit that makes it real. This holds the master
// row lock past the item lease's expiry, so the handler must refuse the permit
// rather than commit authority whose lease has already run out.
func TestNASCleanupPermitIsRefusedWhenDeadlinesExpireBehindTheMasterLock(t *testing.T) {
	f := newCleanupFixture(t, 24)
	plan, sha, ids := f.admit("deadline-behind-lock", syntheticFiles(100, 4, 10))
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
	// Shorten this item's lease so the handler's own deadline accounting is what
	// decides, not the 30-minute default.
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET lease_expires_at=now()+interval '3 seconds' WHERE id=$1`, task.ItemID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 90*time.Second)
	defer cancel()
	// Hold the master row so the handler's authoritative FOR SHARE must queue,
	// and keep it past the permit deadline.
	holder, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback(context.Background())
	if _, err = holder.Exec(ctx, `SELECT enabled FROM nas_cleanup_master FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- cleanupCall(t, f.s.handleAccountNASCleanupObserve, 47, 5, "itemId", strconv.FormatInt(task.ItemID, 10),
			cleanupObserve{ReconciliationProtocol: nasCleanupReconciliationProtocol, Attempt: task.Attempt,
				PlanSHA: task.PlanSHA, Outcome: "match", Proof: fixtureProof(task.Item), ClientVersion: f.version},
			"fixture-boot "+f.version)
	}()
	// Let the handler do all its blocking work and reach the master lock, then
	// expire the deadlines it retained.
	deadline := time.Now().Add(10 * time.Second)
	for {
		var waiting bool
		if err := f.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%nas_cleanup_master%')`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("observation never reached the master row lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(4 * time.Second)
	if err = holder.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case w := <-done:
		// The security property is that no authority is handed out: the expired
		// grant is discarded after the master lock, and the retry reports the
		// ordinary expired-lease refusal rather than a permit.
		if strings.Contains(w.Body.String(), `"delete":true`) {
			t.Fatalf("permit committed after its lease expired behind the master lock: %d %s", w.Code, w.Body.String())
		}
		if w.Code != 200 && w.Code != 409 {
			t.Fatalf("observation after an expired lease: %d %s", w.Code, w.Body.String())
		}
	case <-ctx.Done():
		t.Fatal("observation did not finish")
	}
	var issued int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_cleanup_items WHERE plan_id=$1 AND delete_ever_issued`, plan).Scan(&issued); err != nil {
		t.Fatal(err)
	}
	if issued != 0 {
		t.Fatalf("expired permit was persisted: %d", issued)
	}
	_ = ids
}

// Within one pass, only a permit that actually commits its item savepoint may
// debit the in-memory issued total. The remaining budget fits exactly one item.
// Item A passes the budget test (it is genuinely permitted, with a charge), then
// its savepoint is rolled back; item B in the same pass must still be permitted.
// A debit applied inside the budget test would poison B.
func TestNASCleanupRolledBackItemDoesNotDebitTheBudget(t *testing.T) {
	f := newCleanupFixture(t, 24)
	f.useClientVersion(nasCleanupGroupClientVersion)
	plan, sha, _ := f.admit("budget-two-items", syntheticFiles(100, 4, 10))
	f.dryRun()
	setCleanupMaster(t, f.s, true)
	if code, body := f.enableBatch([]int64{plan}, []string{sha}, 2); code != 200 {
		t.Fatalf("batch enable: %d %s", code, body)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 90*time.Second)
	defer cancel()
	la := cleanupCall(t, f.s.handleAccountNASCleanupLease, 47, 5, "", "", map[string]any{
		"client_version": f.version, "protocol": 2, "reconciliation_protocol": nasCleanupReconciliationProtocol,
		"limit": 8, "execution_files": 2, "execution_bytes": int64(1) << 40,
		"request_id": strings.Repeat("e", 32), "execution_batch": true}, "fixture-boot "+f.version)
	if la.Code != 200 {
		t.Fatalf("group lease: %d %s", la.Code, la.Body.String())
	}
	var leased struct {
		Tasks []cleanupTask `json:"tasks"`
	}
	if err := json.Unmarshal(la.Body.Bytes(), &leased); err != nil || len(leased.Tasks) < 2 {
		t.Fatalf("want a two-item lease, got %v %s", err, la.Body.String())
	}
	first, second := leased.Tasks[0], leased.Tasks[1]
	roster := make([]int64, 0, len(leased.Tasks))
	for _, task := range leased.Tasks {
		roster = append(roster, task.ItemID)
	}

	// Exactly one item's worth of remaining budget.
	base := nasCleanupMaxIssuedBytes24h - second.Item.SizeBytes
	env := &cleanupObserveEnv{
		session:        cleanupClientSession{boot: "fixture-boot", version: f.version},
		executionGroup: roster,
		budgetHeld:     true,
		issued:         base,
		authority:      cleanupAuthorityTentative,
		freshRequests:  map[int64]bool{first.ItemID: true, second.ItemID: true},
		prefetched:     f.s.cleanupPrefetchRecovery(ctx, 13, map[int64]string{first.ItemID: "", second.ItemID: ""}),
	}
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if err = cleanupLockItems(ctx, tx, 13, roster); err != nil {
		t.Fatal(err)
	}
	env.scopeLocked = true

	match := func(task cleanupTask) cleanupObserve {
		return cleanupObserve{
			ReconciliationProtocol: nasCleanupReconciliationProtocol,
			Attempt:                task.Attempt,
			PlanSHA:                task.PlanSHA,
			Outcome:                "match",
			Proof:                  fixtureProof(task.Item),
			ClientVersion:          f.version,
		}
	}

	// Item A passes the budget test: it earns a permit carrying a charge.
	spA, err := tx.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	a := f.s.cleanupObserveOne(ctx, spA, 13, first.ItemID, match(first), env)
	if a.body["delete"] != true {
		t.Fatalf("item A did not pass the budget test: %+v", a)
	}
	if a.budgetCharge <= 0 {
		t.Fatalf("item A was permitted without a charge: %+v", a)
	}
	// The charge must not have been applied yet: it belongs to the caller, after
	// this item's savepoint commits.
	if env.issued != base {
		t.Fatalf("the budget was debited before the item committed: %d -> %d", base, env.issued)
	}
	// A failure after the budget decision rolls the item back.
	if err = spA.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	// Item B must still fit: the rolled-back item debited nothing.
	spB, err := tx.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	b := f.s.cleanupObserveOne(ctx, spB, 13, second.ItemID, match(second), env)
	if b.body["delete"] != true {
		t.Fatalf("a phantom debit from the rolled-back item starved the next permit: %+v", b)
	}
	if err = spB.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	// Only now, for the item that actually committed, does the caller debit.
	env.issued += b.budgetCharge
	if env.issued != nasCleanupMaxIssuedBytes24h {
		t.Fatalf("committed permit not accounted exactly: %d", env.issued)
	}
}

// A grouped lease can buffer one approved task and then set allowExecution=false
// on the next item's cap check. The final master gate must depend on the buffered
// task, not that flag, or the approved task commits with no master read at all.
//
// The interleaving is made deterministic: a holder takes the master row FOR
// UPDATE while the committed value is still ON, the OFF flip queues behind it,
// and only then does the lease run. The lease's unlocked peek reads the committed
// ON (it takes no lock), buffers its approved task, and its final FOR SHARE queues
// behind the OFF. Releasing the holder lets OFF land first, so the lease's own
// locked read must see it.
func TestNASCleanupGroupedLeaseOverflowStillGatedByFinalMaster(t *testing.T) {
	f := newCleanupFixture(t, 24)
	f.useClientVersion(nasCleanupGroupClientVersion)
	plan, sha, _ := f.admit("group-overflow", syntheticFiles(100, 4, 10))
	f.dryRun()
	setCleanupMaster(t, f.s, true)
	if code, body := f.enableBatch([]int64{plan}, []string{sha}, 2); code != 200 {
		t.Fatalf("batch enable: %d %s", code, body)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 90*time.Second)
	defer cancel()
	var oneItem int64
	if err := f.pool.QueryRow(ctx, `SELECT min(size_bytes) FROM nas_cleanup_items WHERE plan_id=$1`, plan).Scan(&oneItem); err != nil {
		t.Fatal(err)
	}
	requestID := strings.Repeat("f", 32)
	lease := func() *httptest.ResponseRecorder {
		return cleanupCall(t, f.s.handleAccountNASCleanupLease, 47, 5, "", "", map[string]any{
			"client_version": f.version, "protocol": 2, "reconciliation_protocol": nasCleanupReconciliationProtocol,
			"limit": 8, "execution_files": 8, "execution_bytes": oneItem,
			"request_id": requestID, "execution_batch": true}, "fixture-boot "+f.version)
	}
	// Control with the switch ON: the overflow shape must yield exactly one
	// approved task, otherwise this test is not exercising the divergence.
	control := lease()
	if control.Code != 200 {
		t.Fatalf("control lease: %d %s", control.Code, control.Body.String())
	}
	var controlTasks struct {
		Tasks []cleanupTask `json:"tasks"`
	}
	if err := json.Unmarshal(control.Body.Bytes(), &controlTasks); err != nil {
		t.Fatal(err)
	}
	approved := 0
	for _, task := range controlTasks.Tasks {
		if task.ApprovedProof != nil {
			approved++
		}
	}
	if approved != 1 {
		t.Fatalf("control lease approved %d tasks, want exactly 1: %s", approved, control.Body.String())
	}
	if _, err := f.pool.Exec(ctx, `UPDATE nas_cleanup_items SET state='pending',lease_expires_at=NULL,lease_request_id=NULL,lease_connection_id=NULL WHERE plan_id=$1`, plan); err != nil {
		t.Fatal(err)
	}

	holder, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback(context.Background())
	if _, err = holder.Exec(ctx, `SELECT enabled FROM nas_cleanup_master FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	// The OFF queues behind the holder while the committed value is still ON.
	offDone := make(chan int, 1)
	go func() {
		offDone <- cleanupMasterCall(f.s, map[string]any{"enabled": false, "claimed_by": "test", "reason": "overflow off"}).Code
	}()
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
			t.Fatal("the OFF flip never queued on the master row")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Now the lease: its unlocked peek still reads the committed ON, it buffers
	// one approved task, and its final FOR SHARE queues behind the OFF.
	leaseDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { leaseDone <- lease() }()
	// Give it time to reach the gate, then let the queued OFF commit first.
	time.Sleep(300 * time.Millisecond)
	if err = holder.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case code := <-offDone:
		if code != 200 {
			t.Fatalf("emergency OFF: %d", code)
		}
	case <-ctx.Done():
		t.Fatal("OFF did not complete")
	}
	select {
	case w := <-leaseDone:
		if w.Code != 200 {
			t.Fatalf("lease after OFF: %d %s", w.Code, w.Body.String())
		}
		if strings.Contains(w.Body.String(), "approved_proof") {
			t.Fatalf("an approved group task was handed out after OFF: %s", w.Body.String())
		}
	case <-ctx.Done():
		t.Fatal("lease did not finish")
	}
	// Nothing was written: the denied lease rolled its own indentations back.
	var leased int
	if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM nas_cleanup_items WHERE plan_id=$1 AND state='leased'`, plan).Scan(&leased); err != nil {
		t.Fatal(err)
	}
	if leased != 0 {
		t.Fatalf("a denied lease left %d leased items behind", leased)
	}
}

// Replays carry their own approval deadline too. This makes the plan approval
// expire while a replay waits on the master row, and requires no approved task to
// be returned and no write committed.
func TestNASCleanupLeaseReplayExpiredPlanIsNotHandedBack(t *testing.T) {
	f := newCleanupFixture(t, 24)
	f.useClientVersion(nasCleanupGroupClientVersion)
	plan, sha, _ := f.admit("replay-expiry", syntheticFiles(100, 4, 10))
	f.dryRun()
	setCleanupMaster(t, f.s, true)
	if code, body := f.enableBatch([]int64{plan}, []string{sha}, 2); code != 200 {
		t.Fatalf("batch enable: %d %s", code, body)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 90*time.Second)
	defer cancel()
	requestID := strings.Repeat("a", 32)
	lease := func() *httptest.ResponseRecorder {
		return cleanupCall(t, f.s.handleAccountNASCleanupLease, 47, 5, "", "", map[string]any{
			"client_version": f.version, "protocol": 2, "reconciliation_protocol": nasCleanupReconciliationProtocol,
			"limit": 8, "execution_files": 8, "execution_bytes": int64(1) << 40,
			"request_id": requestID, "execution_batch": true}, "fixture-boot "+f.version)
	}
	first := lease()
	if first.Code != 200 || !strings.Contains(first.Body.String(), "approved_proof") {
		t.Fatalf("initial lease: %d %s", first.Code, first.Body.String())
	}
	// A lease takes the plan rows before its advisory fence, so the approval must
	// be short before the replay starts: after it queues, the rows are locked and
	// any later UPDATE would block behind the very transaction under test.
	if _, err := f.pool.Exec(ctx, `UPDATE nas_cleanup_plans SET approved_until=now()+interval '2 seconds' WHERE id=$1`, plan); err != nil {
		t.Fatal(err)
	}
	holder, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback(context.Background())
	if _, err = holder.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasCleanupLeaseLock); err != nil {
		t.Fatal(err)
	}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- lease() }()
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
			t.Fatal("replay never queued on its blocking fence")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Let the short approval lapse for real while the replay is still queued.
	time.Sleep(2500 * time.Millisecond)
	if err = holder.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case w := <-done:
		if w.Code != 200 {
			t.Fatalf("replay after approval expiry: %d %s", w.Code, w.Body.String())
		}
		if strings.Contains(w.Body.String(), "approved_proof") {
			t.Fatalf("a replay handed back an approved task after its plan approval expired: %s", w.Body.String())
		}
	case <-ctx.Done():
		t.Fatal("replay did not finish")
	}
}
