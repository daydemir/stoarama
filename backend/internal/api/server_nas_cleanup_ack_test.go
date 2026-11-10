package api

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// Both transports must bind fresh and replay acknowledgments to the exact
// item/attempt/plan/proof; a generic HTTP 200 is not a receipt acknowledgment.
func TestNASCleanupAcknowledgementsEchoExactCheckpoint(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprint("batch=", batch), func(t *testing.T) {
			f := newCleanupFixture(t, 24)
			f.admit("ack-fixture", syntheticFiles(100, 1, 10))
			task := f.leaseV2(1, 0, 0, "")[0]
			proof := fixtureProof(task.Item)
			encodedProof, _ := json.Marshal(proof)
			observation := map[string]any{"attempt": task.Attempt, "plan_sha256": task.PlanSHA, "outcome": "match", "proof": proof, "error": "", "client_version": "test"}
			assertAck := func(ack map[string]any, id int64, attempt int, plan, proof string) {
				t.Helper()
				if ack["item_id"] != float64(id) || ack["attempt"] != float64(attempt) || ack["plan_sha256"] != plan || ack["proof_sha256"] != proof {
					t.Fatalf("ack not bound to checkpoint: %v", ack)
				}
			}
			for replay := 0; replay < 2; replay++ {
				var ack map[string]any
				if batch {
					observation["item_id"] = task.ItemID
					w := cleanupCall(t, f.s.handleAccountNASCleanupObserveBatch, 47, 5, "", "", map[string]any{"client_version": "test", "observations": []any{observation}})
					var out struct {
						Results []map[string]any `json:"results"`
					}
					if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &out) != nil || len(out.Results) != 1 {
						t.Fatalf("observe: %d %s", w.Code, w.Body.String())
					}
					ack = out.Results[0]
				} else {
					w := cleanupCall(t, f.s.handleAccountNASCleanupObserve, 47, 5, "itemId", fmt.Sprint(task.ItemID), observation)
					if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &ack) != nil {
						t.Fatalf("observe: %d %s", w.Code, w.Body.String())
					}
				}
				assertAck(ack, task.ItemID, task.Attempt, task.PlanSHA, cleanupSHA(encodedProof))
				if ack["action"] != "would_delete" || ack["delete"] != false {
					t.Fatalf("unexpected dry run: %v", ack)
				}
			}
			safeguardPlan(t, f.ctx, f.s, 70, "enabled", 10)
			if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET state='delete_issued',delete_ever_issued=true,attempt=1,proof_sha256=repeat('c',64) WHERE plan_id=70`); err != nil {
				t.Fatal(err)
			}
			receipt := map[string]any{"attempt": 1, "plan_sha256": strings.Repeat("b", 64), "proof_sha256": strings.Repeat("c", 64), "outcome": "deleted", "error": ""}
			for replay := 0; replay < 2; replay++ {
				var ack map[string]any
				if batch {
					receipt["item_id"] = 70000
					w := cleanupCall(t, f.s.handleAccountNASCleanupResultBatch, 47, 5, "", "", map[string]any{"client_version": "test", "receipts": []any{receipt}})
					var out struct {
						Results []map[string]any `json:"results"`
					}
					if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &out) != nil || len(out.Results) != 1 {
						t.Fatalf("result: %d %s", w.Code, w.Body.String())
					}
					ack = out.Results[0]
				} else {
					w := cleanupCall(t, f.s.handleAccountNASCleanupResult, 47, 5, "itemId", "70000", receipt)
					if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &ack) != nil {
						t.Fatalf("result: %d %s", w.Code, w.Body.String())
					}
				}
				assertAck(ack, 70000, 1, strings.Repeat("b", 64), strings.Repeat("c", 64))
				if ack["ok"] != true {
					t.Fatalf("not acknowledged: %v", ack)
				}
			}
		})
	}
}
