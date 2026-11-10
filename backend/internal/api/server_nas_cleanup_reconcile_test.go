package api

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestNASCleanupReconciliationCannotMintFirstPermit(t *testing.T) {
	f := newCleanupFixture(t, 24)
	id, sha, _ := f.admit("reconcile", syntheticFiles(100, 1, 1))
	f.dryRun()
	f.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := f.enableBatch([]int64{id}, []string{sha}, 2); code != 200 {
		t.Fatalf("enable: %d %s", code, body)
	}
	task := f.leaseV2(1, 1, 1<<30, "")[0]
	body := map[string]any{"attempt": task.Attempt, "plan_sha256": sha, "outcome": "match", "proof": fixtureProof(task.Item), "error": "", "client_version": "test", "request_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	gets, heads := f.bucket.counts()
	// The original observation failed before issuance. Reconciliation must work
	// even with R2 unavailable, and repeats must not consume another attempt.
	f.s.r2 = nil
	for n := 0; n < 2; n++ {
		w := cleanupCall(t, f.s.handleAccountNASCleanupReconcile, 47, 5, "itemId", fmt.Sprint(task.ItemID), body)
		var ack map[string]any
		json.Unmarshal(w.Body.Bytes(), &ack)
		if w.Code != 200 || ack["action"] != "pending" || ack["delete"] != false || ack["expires_at"] != nil {
			t.Fatalf("replay minted authority: %d %s", w.Code, w.Body.String())
		}
	}
	var state string
	var issued bool
	var count int
	if err := f.pool.QueryRow(f.ctx, `SELECT state,delete_ever_issued FROM nas_cleanup_items WHERE id=$1`, task.ItemID).Scan(&state, &issued); err != nil || state != "pending" || issued {
		t.Fatalf("state=%s issued=%t err=%v", state, issued, err)
	}
	f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_cleanup_reconciliations WHERE item_id=$1`, task.ItemID).Scan(&count)
	if count != 1 {
		t.Fatalf("audit count %d", count)
	}
	afterGets, afterHeads := f.bucket.counts()
	if gets != afterGets || heads != afterHeads {
		t.Fatal("replay did recovery I/O")
	}
	if mode, _ := f.planMode(id); mode != "enabled" {
		t.Fatalf("replay paused plan: %s", mode)
	}
	next := f.leaseV2(1, 1, 1<<30, "")[0]
	if next.Attempt != task.Attempt+1 {
		t.Fatal("new lease did not advance attempt")
	}
	if w := cleanupCall(t, f.s.handleAccountNASCleanupReconcile, 47, 5, "itemId", fmt.Sprint(task.ItemID), body); w.Code != 409 {
		t.Fatalf("old replay affected new lease: %d", w.Code)
	}
}

func TestNASCleanupReconciliationRetainsIssuedAuthority(t *testing.T) {
	f := newCleanupFixture(t, 24)
	id, sha, _ := f.admit("issued-reconcile", syntheticFiles(100, 1, 1))
	f.dryRun()
	f.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := f.enableBatch([]int64{id}, []string{sha}, 2); code != 200 {
		t.Fatalf("enable: %d %s", code, body)
	}
	task := f.leaseV2(1, 1, 1<<30, "")[0]
	permit := f.observe([]cleanupTask{task}, nil)[0]
	body := map[string]any{"attempt": task.Attempt, "plan_sha256": sha, "outcome": "match", "proof": fixtureProof(task.Item), "error": "", "client_version": "test"}
	// Even a still-live original permit is never returned for execution.
	w := cleanupCall(t, f.s.handleAccountNASCleanupReconcile, 47, 5, "itemId", fmt.Sprint(task.ItemID), body)
	var ack map[string]any
	json.Unmarshal(w.Body.Bytes(), &ack)
	if w.Code != 200 || ack["action"] != "reconcile" || ack["delete"] != false || ack["expires_at"] != nil {
		t.Fatalf("issued replay: %d %s", w.Code, w.Body.String())
	}
	var state, proof string
	var issued bool
	f.pool.QueryRow(f.ctx, `SELECT state,proof_sha256,delete_ever_issued FROM nas_cleanup_items WHERE id=$1`, task.ItemID).Scan(&state, &proof, &issued)
	if state != "delete_issued" || !issued || proof != permit.ProofSHA {
		t.Fatal("replay discarded issued authority")
	}
	body["proof"] = map[string]any{"different": "proof"}
	if w := cleanupCall(t, f.s.handleAccountNASCleanupReconcile, 47, 5, "itemId", fmt.Sprint(task.ItemID), body); w.Code != 409 {
		t.Fatal("wrong proof reconciled issued item")
	}
	if w := cleanupCall(t, f.s.handleAccountNASCleanupReconcile, 48, 6, "itemId", fmt.Sprint(task.ItemID), body); w.Code != 409 {
		t.Fatal("foreign account reconciled item")
	}
	if !pullPathAllowed("POST", "/api/v1/account/connections/cleanup/1/reconcile") {
		t.Fatal("pull cannot reconcile")
	}
}
