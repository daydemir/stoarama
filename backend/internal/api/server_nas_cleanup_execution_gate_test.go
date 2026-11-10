package api

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestNASCleanupExecutionCapabilityAndConcurrentLeaseLimit(t *testing.T) {
	f := newCleanupFixture(t, 24)
	id, sha, _ := f.admit("execution-gate", syntheticFiles(100, 20, 10))
	f.dryRun()
	f.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := f.enableBatch([]int64{id}, []string{sha}, 2); code != 200 {
		t.Fatalf("enable: %d %s", code, body)
	}
	request := func(protocol, capability int, requestID string) []cleanupTask {
		t.Helper()
		body := map[string]any{"client_version": "old-protocol-2", "protocol": protocol, "limit": 8, "reconciliation_protocol": capability}
		if protocol == 2 {
			body["limit"], body["execution_files"], body["execution_bytes"] = 64, 64, int64(1<<40)
			body["request_id"] = requestID
		}
		w := cleanupCall(t, f.s.handleAccountNASCleanupLease, 47, 5, "", "", body)
		var result struct {
			Tasks []cleanupTask `json:"tasks"`
		}
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &result) != nil {
			t.Errorf("lease: %d %s", w.Code, w.Body.String())
		}
		return result.Tasks
	}
	for _, protocol := range []int{1, 2} {
		for _, capability := range []int{0, 2} {
			if tasks := request(protocol, capability, ""); len(tasks) != 0 {
				t.Fatalf("legacy/unknown capability leased execution: %+v", tasks)
			}
		}
	}
	// All sixteen clients ask for 64 execution items at once. The persisted
	// outstanding count must be one, including across different request IDs.
	start := make(chan struct{})
	results := make(chan []cleanupTask, 16)
	for n := 0; n < 16; n++ {
		go func(n int) {
			<-start
			results <- request(2, nasCleanupReconciliationProtocol, fmt.Sprintf("%032x", n+1))
		}(n)
	}
	close(start)
	var leased []cleanupTask
	for n := 0; n < 16; n++ {
		leased = append(leased, (<-results)...)
	}
	if len(leased) != 1 {
		t.Fatalf("concurrent execution leases: %d", len(leased))
	}
	var requestID string
	if err := f.pool.QueryRow(f.ctx, `SELECT lease_request_id FROM nas_cleanup_items WHERE id=$1`, leased[0].ItemID).Scan(&requestID); err != nil {
		t.Fatal(err)
	}
	if tasks := request(2, 0, requestID); len(tasks) != 0 {
		t.Fatal("old client replay received execution lease")
	}
	if tasks := request(2, nasCleanupReconciliationProtocol, requestID); len(tasks) != 1 || tasks[0].Attempt != leased[0].Attempt {
		t.Fatal("capable lease replay changed identity")
	}
	permit := f.observe(leased, nil)
	if !permit[0].Delete {
		t.Fatalf("permit: %+v", permit)
	}
	if tasks := request(2, nasCleanupReconciliationProtocol, strings.Repeat("f", 32)); len(tasks) != 0 {
		t.Fatal("lease overtook outstanding permit")
	}
	f.receipts(leased, permit, "deleted")
	if tasks := request(2, nasCleanupReconciliationProtocol, ""); len(tasks) != 1 {
		t.Fatal("receipt did not unblock next execution lease")
	}
	// Legacy clients remain useful for dry runs while execution is occupied.
	f.admit("legacy-dry-run", syntheticFiles(200, 3, 10))
	dry := request(2, 0, "")
	if len(dry) != 3 {
		t.Fatalf("legacy dry-run tasks: %d", len(dry))
	}
	for _, task := range dry {
		if len(task.ApprovedProof) != 0 {
			t.Fatal("old client received execution mixed with dry run")
		}
	}
}

func TestNASCleanupOldProtocolReplayCannotMintFirstPermit(t *testing.T) {
	f := newCleanupFixture(t, 24)
	id, sha, _ := f.admit("old-replay", syntheticFiles(100, 1, 1))
	f.dryRun()
	f.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := f.enableBatch([]int64{id}, []string{sha}, 2); code != 200 {
		t.Fatalf("enable: %d %s", code, body)
	}
	task := f.leaseV2(64, 64, 1<<40, "")[0]
	// Model a pre-upgrade lease whose original observation never committed.
	// Old clients retry /observe (also via observe-batch) instead of /reconcile.
	body := map[string]any{"attempt": task.Attempt, "plan_sha256": sha, "outcome": "match", "proof": fixtureProof(task.Item), "client_version": "old", "request_id": strings.Repeat("a", 32)}
	for n := 0; n < 2; n++ {
		w := cleanupCall(t, f.s.handleAccountNASCleanupObserve, 47, 5, "itemId", fmt.Sprint(task.ItemID), body)
		if w.Code != 409 || strings.Contains(w.Body.String(), `"delete":true`) {
			t.Fatalf("old replay minted permit: %d %s", w.Code, w.Body.String())
		}
		body["item_id"] = task.ItemID
		w = cleanupCall(t, f.s.handleAccountNASCleanupObserveBatch, 47, 5, "", "", map[string]any{"client_version": "old", "observations": []any{body}})
		delete(body, "item_id")
		var response struct {
			Results []batchResult `json:"results"`
		}
		if json.Unmarshal(w.Body.Bytes(), &response) != nil || len(response.Results) != 1 || response.Results[0].Status != 409 || response.Results[0].Delete {
			t.Fatalf("old batch replay: %d %s", w.Code, w.Body.String())
		}
	}
	var unchanged bool
	if err := f.pool.QueryRow(f.ctx, `SELECT state='leased' AND NOT delete_ever_issued AND permit_expires_at IS NULL FROM nas_cleanup_items WHERE id=$1`, task.ItemID).Scan(&unchanged); err != nil || !unchanged {
		t.Fatalf("old replay changed authority: %t %v", unchanged, err)
	}
}
