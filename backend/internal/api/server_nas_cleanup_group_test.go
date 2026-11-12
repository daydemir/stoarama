package api

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
)

func groupLease(t *testing.T, f *cleanupFixture, request string) []cleanupTask {
	t.Helper()
	w := cleanupCall(t, f.s.handleAccountNASCleanupLease, 47, 5, "", "", map[string]any{"client_version": nasCleanupGroupClientVersion, "protocol": 2, "reconciliation_protocol": 1, "limit": 64, "execution_files": 64, "execution_bytes": int64(1 << 40), "request_id": request, "execution_batch": true}, "fixture-boot "+nasCleanupGroupClientVersion)
	var r struct {
		Tasks []cleanupTask `json:"tasks"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &r) != nil {
		t.Fatalf("lease %d %s", w.Code, w.Body.String())
	}
	return r.Tasks
}
func groupObserve(t *testing.T, f *cleanupFixture, tasks []cleanupTask, generation int) *httptest.ResponseRecorder {
	t.Helper()
	obs := []map[string]any{}
	for _, task := range tasks {
		obs = append(obs, map[string]any{"item_id": task.ItemID, "attempt": task.Attempt, "plan_sha256": task.PlanSHA, "outcome": "match", "proof": fixtureProof(task.Item), "error": "", "client_version": nasCleanupGroupClientVersion, "reconciliation_protocol": 1, "request_id": fmt.Sprintf("%016x%016x", generation, task.ItemID)})
	}
	return cleanupCall(t, f.s.handleAccountNASCleanupObserveBatch, 47, 5, "", "", map[string]any{"client_version": nasCleanupGroupClientVersion, "execution_batch": true, "observations": obs}, "fixture-boot "+nasCleanupGroupClientVersion)
}
func TestNASCleanupExecutionGroupMembershipAndUncertainty(t *testing.T) {
	f := newCleanupFixture(t, 24)
	id, sha, _ := f.admit("group", syntheticFiles(100, 20, 20))
	f.dryRun()
	if _, err := f.pool.Exec(f.ctx, `UPDATE connections SET client_version=$1 WHERE id=13`, nasCleanupGroupClientVersion); err != nil {
		t.Fatal(err)
	}
	f.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := f.enableBatch([]int64{id}, []string{sha}, 2); code != 200 {
		t.Fatalf("enable %d %s", code, body)
	}
	tasks := groupLease(t, f, strings.Repeat("a", 32))
	if len(tasks) != 8 {
		t.Fatalf("size %d", len(tasks))
	}
	if replay := groupLease(t, f, strings.Repeat("a", 32)); len(replay) != 8 || replay[0].Attempt != tasks[0].Attempt {
		t.Fatal("lease replay changed group")
	}
	if more := groupLease(t, f, strings.Repeat("b", 32)); len(more) != 0 {
		t.Fatal("second group leased")
	}
	if w := groupObserve(t, f, tasks[:7], 1); w.Code != 409 {
		t.Fatalf("subset %d %s", w.Code, w.Body.String())
	}
	w := groupObserve(t, f, tasks, 2)
	var r struct {
		Results []batchResult `json:"results"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &r) != nil || len(r.Results) != 8 {
		t.Fatalf("permits %d %s", w.Code, w.Body.String())
	}
	for _, p := range r.Results {
		if !p.Delete || p.Status != 200 {
			t.Fatalf("permit %+v %s", p, w.Body.String())
		}
	}
	if retry := groupObserve(t, f, tasks, 2); retry.Code != 409 {
		t.Fatalf("replayed group %d %s", retry.Code, retry.Body.String())
	}
	receipts := []cleanupReceipt{}
	attempted := true
	for n, task := range tasks {
		receipts = append(receipts, cleanupReceipt{ItemID: task.ItemID, Attempt: task.Attempt, PlanSHA: sha, ProofSHA: r.Results[n].ProofSHA, Outcome: "deleted", UnlinkAttempted: &attempted})
	}
	send := func(rs []cleanupReceipt) {
		t.Helper()
		w := cleanupCall(t, f.s.handleAccountNASCleanupResultBatch, 47, 5, "", "", map[string]any{"client_version": nasCleanupGroupClientVersion, "receipts": rs})
		if w.Code != 200 {
			t.Fatalf("receipts %d %s", w.Code, w.Body.String())
		}
	}
	send(receipts[:4])
	send(receipts[:4])
	if more := groupLease(t, f, strings.Repeat("c", 32)); len(more) != 0 {
		t.Fatal("partial receipts released execution")
	}
	receipts[4].Outcome = "unverified"
	send(receipts[4:])
	var mode string
	var issued, uncertain int
	if err := f.pool.QueryRow(f.ctx, `SELECT mode,(SELECT count(*) FROM nas_cleanup_items WHERE plan_id=$1 AND delete_ever_issued),(SELECT count(*) FROM nas_cleanup_items WHERE plan_id=$1 AND state='unverified') FROM nas_cleanup_plans WHERE id=$1`, id).Scan(&mode, &issued, &uncertain); err != nil || mode != "paused" || issued != 8 || uncertain != 1 {
		t.Fatalf("uncertainty %s %d %d %v", mode, issued, uncertain, err)
	}
	if more := groupLease(t, f, strings.Repeat("d", 32)); len(more) != 0 {
		t.Fatal("uncertain group released execution")
	}
}
