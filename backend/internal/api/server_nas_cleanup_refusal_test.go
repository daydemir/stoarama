package api

import (
	"fmt"
	"strings"
	"testing"
)

func TestNASCleanupDurableRefusalsDoNotPause(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, reason := range []string{"deadline_exceeded", "local_guard", "permit_expired", "pre_unlink_failure", "observation_replayed", "proof_mismatch", "execution_disabled", "", "attempted", "unknown", "sidecar", "legacy"} {
			t.Run(fmt.Sprintf("batch=%t/%s", batch, reason), func(t *testing.T) {
				f := newCleanupFixture(t, 24)
				id, sha, _ := f.admit("refusal", syntheticFiles(100, 2, 2))
				f.dryRun()
				f.s.cfg.NASCleanupDeletionEnabled = true
				if code, body := f.enableBatch([]int64{id}, []string{sha}, 2); code != 200 {
					t.Fatalf("enable: %d %s", code, body)
				}
				task := f.leaseV2(1, 1, 1<<30, "")[0]
				permit := f.observe([]cleanupTask{task}, nil)[0]
				receipt := map[string]any{"attempt": task.Attempt, "plan_sha256": sha, "proof_sha256": permit.ProofSHA, "outcome": "unverified", "error": "not unlinked", "unlink_attempted": false, "stage_timings": map[string]int64{"intent_journal_ms": 50000}}
				safe := reason != "" && reason != "attempted" && reason != "unknown" && reason != "sidecar" && reason != "legacy"
				if safe {
					receipt["refusal_reason"] = reason
					receipt["refusal_protocol"] = 1
				}
				if reason == "legacy" {
					receipt["refusal_reason"] = "observation_replayed"
				}
				if reason == "attempted" || reason == "sidecar" {
					receipt["unlink_attempted"] = true
				}
				if reason == "unknown" {
					delete(receipt, "unlink_attempted")
				}
				if reason == "sidecar" {
					receipt["outcome"] = "media_deleted_sidecar_failed"
				}
				for n := 0; n < 2; n++ {
					var code int
					var body string
					if batch {
						receipt["item_id"] = task.ItemID
						w := cleanupCall(t, f.s.handleAccountNASCleanupResultBatch, 47, 5, "", "", map[string]any{"client_version": "test", "receipts": []any{receipt}})
						code, body = w.Code, w.Body.String()
					} else {
						w := cleanupCall(t, f.s.handleAccountNASCleanupResult, 47, 5, "itemId", fmt.Sprint(task.ItemID), receipt)
						code, body = w.Code, w.Body.String()
					}
					if code != 200 || (safe && !strings.Contains(body, `"state":"keep"`)) {
						t.Fatalf("receipt/replay: %d %s", code, body)
					}
				}
				wantState, wantMode := "keep", "enabled"
				if !safe {
					wantState, wantMode = receipt["outcome"].(string), "paused"
				}
				var state string
				var issued bool
				var timing int64
				if err := f.pool.QueryRow(f.ctx, `SELECT state,delete_ever_issued,(outcome->'stage_timings'->>'intent_journal_ms')::bigint FROM nas_cleanup_items WHERE id=$1`, task.ItemID).Scan(&state, &issued, &timing); err != nil || state != wantState || !issued || timing != 50000 {
					t.Fatalf("state=%s issued=%t timing=%d err=%v", state, issued, timing, err)
				}
				if mode, _ := f.planMode(id); mode != wantMode {
					t.Fatalf("plan %s want %s", mode, wantMode)
				}
				if mode, _ := f.batchMode(); mode != wantMode {
					t.Fatalf("batch %s want %s", mode, wantMode)
				}
				next := f.leaseV2(1, 1, 1<<30, "")
				if safe && (len(next) != 1 || next[0].ItemID == task.ItemID) {
					t.Fatalf("refusal blocked unrelated execution or reissued reserved file: %+v", next)
				}
			})
		}
	}
}

func TestNASCleanupReceiptUncertaintyCannotBecomeKeep(t *testing.T) {
	yes, no := true, false
	for _, tc := range []struct {
		name, outcome, reason string
		attempted             *bool
		valid                 bool
	}{
		{"crash intent", "unverified", "", nil, true},
		{"unlink exception", "unverified", "", &yes, true},
		{"false without refusal", "unverified", "", &no, true},
		{"sidecar failure", "media_deleted_sidecar_failed", "", &yes, true},
		{"contradictory refusal", "unverified", "local_guard", &yes, false},
		{"unknown attempt refusal", "unverified", "local_guard", nil, false},
		{"contradictory deleted", "deleted", "local_guard", &no, false},
		{"contradictory sidecar", "media_deleted_sidecar_failed", "local_guard", &no, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := cleanupReceipt{Attempt: 1, PlanSHA: strings.Repeat("a", 64), ProofSHA: strings.Repeat("b", 64), Outcome: tc.outcome, RefusalReason: tc.reason, UnlinkAttempted: tc.attempted}
			if validCleanupReceipt(req) != tc.valid {
				t.Fatal("unexpected receipt validation")
			}
			if tc.valid && cleanupReceiptState(req) == "keep" {
				t.Fatal("uncertainty classified as keep")
			}
		})
	}
}
