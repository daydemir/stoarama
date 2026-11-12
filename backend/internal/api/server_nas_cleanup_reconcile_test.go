package api

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestNASCleanupReturnDryLeaseBeforeObservation(t *testing.T) {
	f := newCleanupFixture(t, 24)
	id, sha, _ := f.admit("return-dry-lease", syntheticFiles(100, 1, 1))
	task := f.leaseV2(1, 0, 0, "")[0]
	body := cleanupObserve{Attempt: task.Attempt, PlanSHA: sha, Outcome: "unverified", Proof: json.RawMessage("null"), ReturnLease: true}
	for n := 0; n < 2; n++ {
		w := cleanupCall(t, f.s.handleAccountNASCleanupReconcile, 47, 5, "itemId", fmt.Sprint(task.ItemID), body)
		if w.Code != 200 || !strings.Contains(w.Body.String(), `"delete":false`) || !strings.Contains(w.Body.String(), `"action":"pending"`) {
			t.Fatalf("dry return: %d %s", w.Code, w.Body.String())
		}
	}
	if mode, _ := f.planMode(id); mode != "dry_run" {
		t.Fatalf("dry return changed mode: %s", mode)
	}
	next := f.leaseV2(1, 0, 0, "")[0]
	if next.Attempt != task.Attempt+1 || len(next.ApprovedProof) != 0 {
		t.Fatal("dry return changed execution authority or attempt fencing")
	}
	if w := cleanupCall(t, f.s.handleAccountNASCleanupReconcile, 47, 5, "itemId", fmt.Sprint(task.ItemID), body); w.Code != 409 {
		t.Fatalf("old dry return affected new attempt: %d", w.Code)
	}
}

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

// A pre-permit writer timeout can safely give up its serial slot. Identity,
// attempts and monotonic issuance history must survive lost return responses.
func TestNASCleanupReturnUnusedLeaseGuards(t *testing.T) {
	f := newCleanupFixture(t, 24)
	id, sha, _ := f.admit("return-lease", syntheticFiles(100, 1, 1))
	f.dryRun()
	f.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := f.enableBatch([]int64{id}, []string{sha}, 2); code != 200 {
		t.Fatalf("enable: %d %s", code, body)
	}
	task := f.leaseV2(1, 1, 1<<30, "")[0]
	body := cleanupObserve{Attempt: task.Attempt, PlanSHA: sha, Outcome: "unverified", Proof: json.RawMessage("null"), ReturnLease: true}
	call := func(body cleanupObserve, session string) int {
		return cleanupCall(t, f.s.handleAccountNASCleanupReconcile, 47, 5, "itemId", fmt.Sprint(task.ItemID), body, session).Code
	}
	session := "fixture-boot " + nasCleanupExecutionClientVersion
	for _, wrong := range []string{"", "other-boot " + nasCleanupExecutionClientVersion, "fixture-boot unreviewed"} {
		if code := call(body, wrong); code != 409 {
			t.Fatalf("wrong session returned lease: %q %d", wrong, code)
		}
	}
	wrong := body
	wrong.Attempt++
	if code := call(wrong, session); code != 409 {
		t.Fatalf("wrong attempt returned lease: %d", code)
	}
	wrong = body
	wrong.PlanSHA = strings.Repeat("f", 64)
	if code := call(wrong, session); code != 409 {
		t.Fatalf("wrong plan returned lease: %d", code)
	}
	if w := cleanupCall(t, f.s.handleAccountNASCleanupReconcile, 48, 6, "itemId", fmt.Sprint(task.ItemID), body, session); w.Code != 409 {
		t.Fatalf("foreign connection returned lease: %d", w.Code)
	}
	// A return flag cannot be sent to a live observation path to mint authority.
	wrong = body
	wrong.Outcome, wrong.Proof = "match", fixtureProof(task.Item)
	if w := cleanupCall(t, f.s.handleAccountNASCleanupObserve, 47, 5, "itemId", fmt.Sprint(task.ItemID), wrong); w.Code != 400 {
		t.Fatalf("return flag accepted by observe: %d %s", w.Code, w.Body.String())
	}
	if code := call(wrong, session); code != 400 {
		t.Fatalf("return carried a matching proof: %d", code)
	}
	for n := 0; n < 2; n++ {
		w := cleanupCall(t, f.s.handleAccountNASCleanupReconcile, 47, 5, "itemId", fmt.Sprint(task.ItemID), body, session)
		var ack map[string]any
		json.Unmarshal(w.Body.Bytes(), &ack)
		if w.Code != 200 || ack["action"] != "pending" || ack["delete"] != false {
			t.Fatalf("return: %d %s", w.Code, w.Body.String())
		}
	}
	if mode, _ := f.planMode(id); mode != "enabled" {
		t.Fatalf("return paused approval: %s", mode)
	}
	next := f.leaseV2(1, 1, 1<<30, "")[0]
	if next.Attempt != task.Attempt+1 || !nasSpaceAuditJSONEqual(next.ApprovedProof, task.ApprovedProof) {
		t.Fatal("return changed approved proof or failed to advance attempt")
	}
	if code := call(body, session); code != 409 {
		t.Fatalf("old return affected new lease: %d", code)
	}
	if permit := f.observe([]cleanupTask{next}, nil)[0]; !permit.Delete {
		t.Fatal("returned work did not progress to execution")
	}
	body.Attempt = next.Attempt
	if code := call(body, session); code != 409 {
		t.Fatalf("issued permit was returned: %d", code)
	}
	var issued bool
	var state string
	if err := f.pool.QueryRow(f.ctx, `SELECT state,delete_ever_issued FROM nas_cleanup_items WHERE id=$1`, task.ItemID).Scan(&state, &issued); err != nil || !issued || state != "delete_issued" {
		t.Fatalf("lost issued authority: state=%s issued=%t err=%v", state, issued, err)
	}
}
