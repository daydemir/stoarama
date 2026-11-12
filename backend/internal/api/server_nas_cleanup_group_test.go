package api

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func groupLease(t *testing.T, f *cleanupFixture, request string, versions ...string) []cleanupTask {
	t.Helper()
	version := nasCleanupGroupClientVersion
	if len(versions) != 0 {
		version = versions[0]
	}
	w := cleanupCall(t, f.s.handleAccountNASCleanupLease, 47, 5, "", "", map[string]any{"client_version": version, "protocol": 2, "reconciliation_protocol": 1, "limit": 64, "execution_files": 64, "execution_bytes": int64(1 << 40), "request_id": request, "execution_batch": true}, "fixture-boot "+version)
	var r struct {
		Tasks []cleanupTask `json:"tasks"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &r) != nil {
		t.Fatalf("lease %d %s", w.Code, w.Body.String())
	}
	return r.Tasks
}

func TestNASCleanupSerialGroupRosterAndReplay(t *testing.T) {
	for _, version := range []string{nasCleanupSerialGroupClientVersion, nasCleanupDurableGroupClientVersion, nasCleanupBatchedCapsClientVersion, nasCleanupDeliveryHoldClientVersion} {
		t.Run(version, func(t *testing.T) { testNASCleanupSerialGroupRosterAndReplay(t, version) })
	}
}

func testNASCleanupSerialGroupRosterAndReplay(t *testing.T, version string) {
	f := newCleanupFixture(t, 24)
	id, sha, _ := f.admit("serial-group", syntheticFiles(100, 20, 20))
	f.dryRun()
	if _, err := f.pool.Exec(f.ctx, `UPDATE connections SET client_version=$1 WHERE id=13`, version); err != nil {
		t.Fatal(err)
	}
	f.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := f.enableBatch([]int64{id}, []string{sha}, 2); code != 200 {
		t.Fatalf("enable %d %s", code, body)
	}
	tasks := groupLease(t, f, strings.Repeat("a", 32), version)
	if len(tasks) != 8 {
		t.Fatalf("lease size %d", len(tasks))
	}
	roster := []int64{}
	for _, task := range tasks {
		roster = append(roster, task.ItemID)
	}
	generation := 0
	observe := func(task cleanupTask, ids []int64) *httptest.ResponseRecorder {
		generation++
		return cleanupCall(t, f.s.handleAccountNASCleanupObserveBatch, 47, 5, "", "", map[string]any{
			"client_version": version, "execution_batch": true, "execution_group": ids,
			"observations": []map[string]any{{"item_id": task.ItemID, "attempt": task.Attempt, "plan_sha256": sha,
				"outcome": "match", "proof": fixtureProof(task.Item), "error": "", "client_version": version,
				"reconciliation_protocol": 1, "request_id": fmt.Sprintf("%032x", generation)}},
		}, "fixture-boot "+version)
	}
	for _, invalid := range [][]int64{nil, roster[:7], roster[1:], append(append([]int64{}, roster[:7]...), roster[0]), append(append([]int64{}, roster...), 999999)} {
		if w := observe(tasks[0], invalid); w.Code != 409 {
			t.Fatalf("invalid roster %v: %d %s", invalid, w.Code, w.Body.String())
		}
	}
	for _, column := range []string{"lease_request_id", "lease_client_boot_id", "lease_client_version", "state"} {
		var original string
		if err := f.pool.QueryRow(f.ctx, "SELECT "+column+" FROM nas_cleanup_items WHERE id=$1", tasks[7].ItemID).Scan(&original); err != nil {
			t.Fatal(err)
		}
		value := "alien"
		if column == "lease_request_id" {
			value = strings.Repeat("b", 32)
		}
		if column == "state" {
			value = "unverified"
		}
		if _, err := f.pool.Exec(f.ctx, "UPDATE nas_cleanup_items SET "+column+"=$1 WHERE id=$2", value, tasks[7].ItemID); err != nil {
			t.Fatal(err)
		}
		if w := observe(tasks[0], roster); w.Code != 409 {
			t.Fatalf("mismatched %s: %d %s", column, w.Code, w.Body.String())
		}
		if _, err := f.pool.Exec(f.ctx, "UPDATE nas_cleanup_items SET "+column+"=$1 WHERE id=$2", original, tasks[7].ItemID); err != nil {
			t.Fatal(err)
		}
	}
	// A member that ceased to be busy still belongs to this exact lease. Its
	// omission cannot turn an incomplete roster into a new execution group.
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET state='pending' WHERE id=$1`, tasks[7].ItemID); err != nil {
		t.Fatal(err)
	}
	if w := observe(tasks[0], roster[:7]); w.Code != 409 {
		t.Fatalf("omitted non-busy member: %d %s", w.Code, w.Body.String())
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET state='leased' WHERE id=$1`, tasks[7].ItemID); err != nil {
		t.Fatal(err)
	}
	receipts := []cleanupReceipt{}
	attempted := true
	for n, task := range tasks {
		w := observe(task, roster)
		var result struct {
			Results []batchResult `json:"results"`
		}
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &result) != nil || len(result.Results) != 1 || !result.Results[0].Delete {
			t.Fatalf("permit %d: %d %s", n, w.Code, w.Body.String())
		}
		var issued int
		if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_cleanup_items WHERE plan_id=$1 AND delete_ever_issued`, id).Scan(&issued); err != nil || issued != n+1 {
			t.Fatalf("not one new permit: %d %v", issued, err)
		}
		receipts = append(receipts, cleanupReceipt{ItemID: task.ItemID, Attempt: task.Attempt, PlanSHA: sha,
			ProofSHA: result.Results[0].ProofSHA, Outcome: "deleted", UnlinkAttempted: &attempted})
		if more := groupLease(t, f, fmt.Sprintf("%032x", n+100), version); len(more) != 0 {
			t.Fatal("unacknowledged group lost execution slot")
		}
	}
	// Lease/permit expiry cannot invalidate a durable result. Replaying the
	// exact owned refusal never converts an interrupted intent into one.
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET lease_expires_at=now()-interval '1 hour' WHERE id=ANY($1)`, roster); err != nil {
		t.Fatal(err)
	}
	time.Sleep(31 * time.Second) // Real expiry; never disable immutable-authority triggers.
	noAttempt := false
	receipts[7].Outcome, receipts[7].UnlinkAttempted = "unverified", &noAttempt
	receipts[7].RefusalReason, receipts[7].RefusalProtocol = "deadline_exceeded", 1
	for n := 0; n < 2; n++ {
		w := cleanupCall(t, f.s.handleAccountNASCleanupResultBatch, 47, 5, "", "", map[string]any{"client_version": version, "receipts": receipts})
		if w.Code != 200 || strings.Contains(w.Body.String(), `"status":409`) {
			t.Fatalf("receipt replay %d: %d %s", n, w.Code, w.Body.String())
		}
	}
	var deleted, kept int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FILTER(WHERE state='deleted'),count(*) FILTER(WHERE state='keep') FROM nas_cleanup_items WHERE id=ANY($1)`, roster).Scan(&deleted, &kept); err != nil || deleted != 7 || kept != 1 {
		t.Fatalf("durable results %d deleted %d kept: %v", deleted, kept, err)
	}
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
