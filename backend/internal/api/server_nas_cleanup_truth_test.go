package api

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestNASCleanupItemTruthReplaysMixedResultsAfterPause(t *testing.T) {
	f := newCleanupFixture(t, 24)
	id, sha, _ := f.admit("per-item-truth", syntheticFiles(100, 8, 8))
	f.dryRun()
	version := nasCleanupItemTruthClientVersion
	if _, err := f.pool.Exec(f.ctx, `UPDATE connections SET client_version=$1 WHERE id=13`, version); err != nil {
		t.Fatal(err)
	}
	f.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := f.enableBatch([]int64{id}, []string{sha}, 2); code != 200 {
		t.Fatalf("enable: %d %s", code, body)
	}
	tasks := groupLease(t, f, strings.Repeat("a", 32), version)
	if len(tasks) != 8 {
		t.Fatalf("lease size %d", len(tasks))
	}
	observations := make([]map[string]any, 0, 8)
	for n, task := range tasks {
		observations = append(observations, map[string]any{"item_id": task.ItemID, "attempt": task.Attempt, "plan_sha256": sha, "outcome": "match", "proof": fixtureProof(task.Item), "error": "", "client_version": version, "reconciliation_protocol": 1, "request_id": fmt.Sprintf("%032x", n+1)})
	}
	response := cleanupCall(t, f.s.handleAccountNASCleanupObserveBatch, 47, 5, "", "", map[string]any{"client_version": version, "execution_batch": true, "observations": observations}, "fixture-boot "+version)
	var permits struct {
		Results []batchResult `json:"results"`
	}
	if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &permits) != nil || len(permits.Results) != 8 {
		t.Fatalf("permit %d %s", response.Code, response.Body.String())
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_plans SET mode='paused' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	receipts := make([]map[string]any, 0, 8)
	for n, task := range tasks {
		receipt := map[string]any{"item_id": task.ItemID, "attempt": task.Attempt, "plan_sha256": sha, "proof_sha256": permits.Results[n].ProofSHA, "outcome": "unverified", "error": "deadline exceeded before unlink", "unlink_attempted": false, "refusal_protocol": 1, "refusal_reason": "deadline_exceeded"}
		if n == 0 {
			receipt["outcome"] = "deleted"
			receipt["error"] = ""
			receipt["unlink_attempted"] = true
			delete(receipt, "refusal_protocol")
			delete(receipt, "refusal_reason")
		}
		receipts = append(receipts, receipt)
	}
	for attempt := 0; attempt < 2; attempt++ {
		response = cleanupCall(t, f.s.handleAccountNASCleanupResultBatch, 47, 5, "", "", map[string]any{"client_version": version, "receipts": receipts})
		if response.Code != 200 {
			t.Fatalf("replay %d: %d %s", attempt, response.Code, response.Body.String())
		}
	}
	var deleted, refused, issued, reserved int
	err := f.pool.QueryRow(f.ctx, `SELECT count(*) FILTER(WHERE state='deleted'),count(*) FILTER(WHERE state='keep' AND outcome->>'unlink_attempted'='false' AND outcome->>'refusal_protocol'='1'),count(*) FILTER(WHERE delete_ever_issued AND recovery='r2_etag_bound'),(SELECT count(*) FROM nas_cleanup_reserved_files WHERE label=(SELECT label FROM nas_cleanup_plans WHERE id=$1)) FROM nas_cleanup_items WHERE plan_id=$1`, id).Scan(&deleted, &refused, &issued, &reserved)
	if err != nil || deleted != 1 || refused != 7 || issued != 8 || reserved != 8 {
		t.Fatalf("truth/holds %d/%d/%d/%d: %v", deleted, refused, issued, reserved, err)
	}
	if more := groupLease(t, f, strings.Repeat("c", 32), version); len(more) != 0 {
		t.Fatal("paused group leased more execution")
	}
}
