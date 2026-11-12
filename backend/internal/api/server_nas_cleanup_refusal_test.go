package api

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// A certified refusal cannot remain certain when the same checkpoint later
// reports unlink activity. Exercise separate requests and duplicate batch rows.
func TestNASCleanupContradictoryRefusalBecomesUncertain(t *testing.T) {
	for _, transport := range []string{"single", "batch", "duplicate_batch"} {
		for _, outcome := range []string{"unverified", "deleted", "media_deleted_sidecar_failed", "contradictory_claim"} {
			t.Run(transport+"/"+outcome, func(t *testing.T) {
				f := newCleanupFixture(t, 24)
				pid, sha, _ := f.admit("contradictory-refusal", syntheticFiles(100, 2, 2))
				f.dryRun()
				f.s.cfg.NASCleanupDeletionEnabled = true
				if code, body := f.enableBatch([]int64{pid}, []string{sha}, 2); code != 200 {
					t.Fatalf("enable: %d %s", code, body)
				}
				task := f.leaseV2(1, 1, 1<<30, "")[0]
				permit := f.observe([]cleanupTask{task}, nil)[0]
				refusal := map[string]any{"attempt": task.Attempt, "plan_sha256": sha, "proof_sha256": permit.ProofSHA, "outcome": "unverified", "error": "not unlinked", "unlink_attempted": false, "refusal_protocol": 1, "refusal_reason": "local_guard"}
				contradiction := map[string]any{"attempt": task.Attempt, "plan_sha256": sha, "proof_sha256": permit.ProofSHA, "outcome": outcome, "error": "later evidence", "unlink_attempted": true}
				wantStatus := 409
				if outcome == "contradictory_claim" {
					contradiction["outcome"], contradiction["refusal_protocol"], contradiction["refusal_reason"] = "unverified", 1, "local_guard"
					wantStatus = 400 // reject the invalid claim, but retain its evidence
				}
				send := func(receipts ...map[string]any) int {
					t.Helper()
					if transport == "single" {
						return cleanupCall(t, f.s.handleAccountNASCleanupResult, 47, 5, "itemId", fmt.Sprint(task.ItemID), receipts[0]).Code
					}
					for _, receipt := range receipts {
						receipt["item_id"] = task.ItemID
					}
					w := cleanupCall(t, f.s.handleAccountNASCleanupResultBatch, 47, 5, "", "", map[string]any{"client_version": "test", "receipts": receipts})
					var body struct {
						Results []struct {
							Status int `json:"status"`
						} `json:"results"`
					}
					if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &body) != nil || len(body.Results) != len(receipts) {
						t.Fatalf("batch: %d %s", w.Code, w.Body.String())
					}
					return body.Results[len(body.Results)-1].Status
				}
				if transport == "duplicate_batch" {
					if code := send(refusal, contradiction); code != 409 {
						t.Fatalf("contradiction: %d", code)
					}
				} else {
					if code := send(refusal); code != 200 {
						t.Fatalf("refusal: %d", code)
					}
					if code := send(contradiction); code != wantStatus {
						t.Fatalf("contradiction: %d", code)
					}
				}
				var state string
				var issued bool
				if err := f.pool.QueryRow(f.ctx, `SELECT state,delete_ever_issued FROM nas_cleanup_items WHERE id=$1`, task.ItemID).Scan(&state, &issued); err != nil || state != "unverified" || !issued {
					t.Fatalf("contradiction stayed certain: %s issued=%t err=%v", state, issued, err)
				}
				var preserved bool
				if err := f.pool.QueryRow(f.ctx, `SELECT outcome->'certified_refusal'->>'refusal_protocol'='1' AND outcome->'contradictory_receipt'->>'unlink_attempted'='true' FROM nas_cleanup_items WHERE id=$1`, task.ItemID).Scan(&preserved); err != nil || !preserved {
					t.Fatalf("contradictory evidence lost: %t %v", preserved, err)
				}
				if mode, _ := f.planMode(pid); mode != "paused" {
					t.Fatalf("plan: %s", mode)
				}
				if mode, _ := f.batchMode(); mode != "paused" {
					t.Fatalf("batch: %s", mode)
				}
				if code := send(refusal); code != 409 {
					t.Fatalf("refusal restored certainty: %d", code)
				}
				if next := f.leaseV2(1, 1, 1<<30, ""); len(next) != 0 {
					t.Fatalf("uncertain execution re-leased: %+v", next)
				}
			})
		}
	}
}

func TestNASCleanupRefusalsAreTerminal(t *testing.T) {
	for _, batchPlan := range []bool{false, true} {
		for _, batchReceipt := range []bool{false, true} {
			for _, lastOutcome := range []string{"deleted", "keep"} {
				t.Run(fmt.Sprintf("batchPlan=%t/batchReceipt=%t/last=%s", batchPlan, batchReceipt, lastOutcome), func(t *testing.T) {
					f := newCleanupFixture(t, 24)
					pid, sha, _ := f.admit("terminal-refusal", syntheticFiles(100, 2, 2))
					f.dryRun()
					f.s.cfg.NASCleanupDeletionEnabled = true
					if batchPlan {
						if code, body := f.enableBatch([]int64{pid}, []string{sha}, 2); code != 200 {
							t.Fatalf("enable: %d %s", code, body)
						}
					} else if code, body := safeguardControl(t, f.s, pid, "enabled", map[string]any{"plan_sha256": sha}); code != 200 {
						t.Fatalf("enable: %d %s", code, body)
					}
					for _, outcome := range []string{"keep", lastOutcome} {
						tasks := f.leaseV2(1, 1, 1<<30, "")
						if len(tasks) != 1 {
							t.Fatalf("lease: %+v", tasks)
						}
						task := tasks[0]
						permit := f.observe(tasks, nil)[0]
						receipt := map[string]any{"attempt": task.Attempt, "plan_sha256": sha, "proof_sha256": permit.ProofSHA, "outcome": "deleted", "error": ""}
						if outcome == "keep" {
							receipt["outcome"], receipt["unlink_attempted"], receipt["refusal_protocol"], receipt["refusal_reason"] = "unverified", false, 1, "local_guard"
						}
						if batchReceipt {
							receipt["item_id"] = task.ItemID
							w := cleanupCall(t, f.s.handleAccountNASCleanupResultBatch, 47, 5, "", "", map[string]any{"client_version": "test", "receipts": []any{receipt}})
							if w.Code != 200 || !strings.Contains(w.Body.String(), `"status":200`) {
								t.Fatalf("receipt: %d %s", w.Code, w.Body.String())
							}
						} else {
							w := cleanupCall(t, f.s.handleAccountNASCleanupResult, 47, 5, "itemId", fmt.Sprint(task.ItemID), receipt)
							if w.Code != 200 {
								t.Fatalf("receipt: %d %s", w.Code, w.Body.String())
							}
						}
					}
					if mode, reason := f.planMode(pid); mode != "paused" || !(strings.HasPrefix(reason, "completed:") || strings.HasPrefix(reason, "batch-completed:")) {
						t.Fatalf("completion: %s %s", mode, reason)
					}
					if batchPlan {
						if mode, _ := f.batchMode(); mode != "completed" {
							t.Fatalf("batch: %s", mode)
						}
					}
					if w := closeCall(t, f.s, pid, sha); w.Code != 200 {
						t.Fatalf("close: %d %s", w.Code, w.Body.String())
					}
					var raw []byte
					var items []nasCleanupItem
					if err := f.pool.QueryRow(f.ctx, `SELECT manifest FROM nas_cleanup_plans WHERE id=$1`, pid).Scan(&raw); err != nil {
						t.Fatal(err)
					}
					if err := json.Unmarshal(raw, &items); err != nil {
						t.Fatal(err)
					}
					for _, item := range items {
						tx, err := f.pool.Begin(f.ctx)
						if err != nil {
							t.Fatal(err)
						}
						_, _, err = persistNASCleanupPlan(f.ctx, tx, "must-stay-reserved", []nasCleanupItem{item})
						tx.Rollback(f.ctx)
						if err == nil {
							t.Fatal("terminal issued identity became admissible")
						}
					}
					if next := f.leaseV2(1, 1, 1<<30, ""); len(next) != 0 {
						t.Fatalf("terminal item re-leased: %+v", next)
					}
				})
			}
		}
	}
}

func TestNASCleanupLateRefusalContradictionStopsAdvancedBatch(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprint("batch=", batch), func(t *testing.T) {
			f := newCleanupFixture(t, 24)
			p1, sha1, _ := f.admit("refused", syntheticFiles(100, 1, 2))
			p2, sha2, _ := f.admit("successor", syntheticFiles(200, 1, 2))
			f.dryRun()
			f.s.cfg.NASCleanupDeletionEnabled = true
			if code, body := f.enableBatch([]int64{p1, p2}, []string{sha1, sha2}, 2); code != 200 {
				t.Fatalf("enable: %d %s", code, body)
			}
			task := f.leaseV2(1, 1, 1<<30, "")[0]
			permit := f.observe([]cleanupTask{task}, nil)[0]
			receipt := map[string]any{"attempt": task.Attempt, "plan_sha256": sha1, "proof_sha256": permit.ProofSHA, "outcome": "unverified", "error": "", "unlink_attempted": false, "refusal_protocol": 1, "refusal_reason": "local_guard"}
			if w := cleanupCall(t, f.s.handleAccountNASCleanupResult, 47, 5, "itemId", fmt.Sprint(task.ItemID), receipt); w.Code != 200 {
				t.Fatalf("refusal: %d %s", w.Code, w.Body.String())
			}
			if mode, _ := f.planMode(p2); mode != "enabled" {
				t.Fatalf("did not advance: %s", mode)
			}
			receipt["outcome"], receipt["unlink_attempted"] = "deleted", true
			delete(receipt, "refusal_protocol")
			delete(receipt, "refusal_reason")
			if batch {
				receipt["item_id"] = task.ItemID
				w := cleanupCall(t, f.s.handleAccountNASCleanupResultBatch, 47, 5, "", "", map[string]any{"client_version": "test", "receipts": []any{receipt}})
				if w.Code != 200 || !strings.Contains(w.Body.String(), `"status":409`) {
					t.Fatalf("contradiction: %d %s", w.Code, w.Body.String())
				}
			} else if w := cleanupCall(t, f.s.handleAccountNASCleanupResult, 47, 5, "itemId", fmt.Sprint(task.ItemID), receipt); w.Code != 409 {
				t.Fatalf("contradiction: %d", w.Code)
			}
			for _, pid := range []int64{p1, p2} {
				if mode, _ := f.planMode(pid); mode != "paused" {
					t.Fatalf("plan %d: %s", pid, mode)
				}
			}
			if mode, _ := f.batchMode(); mode != "paused" {
				t.Fatalf("batch: %s", mode)
			}
			if next := f.leaseV2(1, 1, 1<<30, ""); len(next) != 0 {
				t.Fatalf("successor execution continued: %+v", next)
			}
		})
	}
}

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
				safe := reason != "" && reason != "attempted" && reason != "unknown" && reason != "sidecar" && reason != "legacy" && reason != "observation_replayed"
				if safe {
					receipt["refusal_reason"] = reason
					receipt["refusal_protocol"] = 1
				}
				if reason == "observation_replayed" {
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
