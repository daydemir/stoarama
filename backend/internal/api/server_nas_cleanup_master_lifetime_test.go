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
