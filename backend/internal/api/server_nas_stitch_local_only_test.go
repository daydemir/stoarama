package api

import (
	"encoding/json"
	"strings"
	"testing"
)

// The NAS stitch client always reports retirement_held=true for every input
// (owner rule: keep28 raw on the NAS is never deleted because of stitching).
// That sends readiness to completed_no_retirement, so the single lane is never
// wedged by a verified task whose cleanup plan never drains. This drives the
// real handlers with the client's exact production payload shape.
func TestNASStitchHeldInputsCompleteWithoutRetirementAndReleaseTheLane(t *testing.T) {
	f, p, q := newNASStitchTaskFixture(t)
	f.s.cfg.NASStitchEnabled = true
	f.s.cfg.NASStitchDeletionEnabled = true
	f.s.cfg.NASCleanupDeletionEnabled = true
	if len(q.Outputs[0].Inputs) != len(p.Clips)-1 {
		t.Fatalf("fixture no longer covers every included input: %d", len(q.Outputs[0].Inputs))
	}
	for n := range q.Outputs[0].Inputs {
		q.Outputs[0].Inputs[n].RetirementHeld = true
	}
	// Round-trip the body so the handler sees exactly the client's JSON shape,
	// including the always-present "retirement_held": true fields.
	raw, err := json.Marshal(q)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(raw), `"retirement_held":true`) != len(q.Outputs[0].Inputs) {
		t.Fatalf("fixture payload lost held inputs: %s", raw)
	}
	var body any
	if err = json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	if w := cleanupCall(t, f.s.handleAccountNASStitchResult, 47, 5, "id", "7", body); w.Code != 200 {
		t.Fatalf("held result rejected: %d %s", w.Code, w.Body.String())
	}
	// Held-only evidence still verifies the outputs; nothing is retirable.
	var state string
	if err = f.pool.QueryRow(f.ctx, `SELECT state FROM nas_stitch_tasks WHERE id=7`).Scan(&state); err != nil || state != "verified" {
		t.Fatalf("held result did not register the output: %q %v", state, err)
	}
	ready := func() string {
		t.Helper()
		w := cleanupCall(t, f.s.handleAccountNASStitchReady, 47, 5, "id", "7", map[string]any{"lease_token": q.Token, "plan_sha256": q.PlanSHA, "outputs_ready": true})
		if w.Code != 200 {
			t.Fatalf("readiness: %d %s", w.Code, w.Body.String())
		}
		return w.Body.String()
	}
	ready()
	// Idempotent: a lost ACK repeats the same terminal transition.
	ready()
	var planID *int64
	if err = f.pool.QueryRow(f.ctx, `SELECT state,cleanup_plan_id FROM nas_stitch_tasks WHERE id=7`).Scan(&state, &planID); err != nil {
		t.Fatal(err)
	}
	if state != "completed_no_retirement" || planID != nil {
		t.Fatalf("held completion created retirement authority: %q %v", state, planID)
	}
	var items int
	if err = f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_cleanup_items`).Scan(&items); err != nil || items != 0 {
		t.Fatalf("cleanup work existed without retirement: %d %v", items, err)
	}
	// The lane is free: the next pending hour is admitted immediately instead
	// of waiting on the finished task's cleanup plan forever.
	hourPlan, _ := json.Marshal(p)
	if _, err = f.pool.Exec(f.ctx, `INSERT INTO nas_stitch_tasks(id,scope_id,recording_id,hour_at,plan,plan_sha256) VALUES(8,1,2,'2026-09-20T01:00Z',$1,$2)`, string(hourPlan), q.PlanSHA); err != nil {
		t.Fatal(err)
	}
	w := cleanupCall(t, f.s.handleAccountNASStitchLease, 47, 5, "", "", map[string]any{"client_version": "r2-copy-v3"})
	if w.Code != 200 {
		t.Fatalf("lease after held completion: %d %s", w.Code, w.Body.String())
	}
	var lease struct {
		Task       *nasStitchTask `json:"task"`
		RetryAfter int            `json:"retry_after_sec"`
	}
	if err = json.Unmarshal(w.Body.Bytes(), &lease); err != nil {
		t.Fatal(err)
	}
	if lease.Task == nil || lease.Task.ID != 8 || lease.Task.Token == "" {
		t.Fatalf("next hour was not leased (retry_after=%d): %s", lease.RetryAfter, w.Body.String())
	}
}

// A verified task whose cleanup plan still exists and is undeleted is exactly
// the busy condition the held-input rule avoids. Prove the fixture would have
// wedged the lane without it, so this test keeps its meaning.
func TestNASStitchUnretiredPlanWouldBlockTheLane(t *testing.T) {
	f, p, q := newNASStitchTaskFixture(t)
	f.s.cfg.NASStitchEnabled = true
	// Retirable evidence creates a plan that keep28 never drains.
	p.Clips[0].PermitReason = nasStitchReason
	if w := cleanupCall(t, f.s.handleAccountNASStitchResult, 47, 5, "id", "7", q); w.Code != 200 {
		t.Fatalf("retirable result: %d %s", w.Code, w.Body.String())
	}
	if w := cleanupCall(t, f.s.handleAccountNASStitchReady, 47, 5, "id", "7", map[string]any{"lease_token": q.Token, "plan_sha256": q.PlanSHA, "outputs_ready": true}); w.Code != 200 {
		t.Fatalf("retirable readiness: %d %s", w.Code, w.Body.String())
	}
	var planID *int64
	if err := f.pool.QueryRow(f.ctx, `SELECT cleanup_plan_id FROM nas_stitch_tasks WHERE id=7`).Scan(&planID); err != nil || planID == nil {
		t.Fatalf("retirable readiness made no plan: %v %v", planID, err)
	}
	hourPlan, _ := json.Marshal(p)
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO nas_stitch_tasks(id,scope_id,recording_id,hour_at,plan,plan_sha256) VALUES(8,1,2,'2026-09-20T01:00Z',$1,$2)`, string(hourPlan), q.PlanSHA); err != nil {
		t.Fatal(err)
	}
	w := cleanupCall(t, f.s.handleAccountNASStitchLease, 47, 5, "", "", map[string]any{})
	var lease struct {
		Task *nasStitchTask `json:"task"`
	}
	if json.Unmarshal(w.Body.Bytes(), &lease) != nil || lease.Task != nil {
		t.Fatalf("undeleted plan did not hold the lane: %s", w.Body.String())
	}
}

// The client reports a deterministic per-task failure exactly once, with every
// input left out and every seam a hold (no outputs). That must be a valid,
// terminal `held` task, not a 400 the client would retry forever.
func TestNASStitchTaskDataHoldIsAcceptedAndTerminal(t *testing.T) {
	f, p, q := newNASStitchTaskFixture(t)
	f.s.cfg.NASStitchEnabled = true
	// Mirror the client's stitch_hold_task payload exactly.
	held := nasStitchResult{Policy: nasStitchPolicy, Token: q.Token, PlanSHA: q.PlanSHA,
		Outputs: []nasStitchOutput{}, Repairs: []nasStitchRepair{},
		LeftOut: []int64{p.Clips[0].SourceID, p.Clips[1].SourceID, p.Clips[2].SourceID},
		Seams: []nasStitchDecision{
			{Policy: nasStitchPolicy, LeftID: p.Clips[0].SourceID, RightID: p.Clips[1].SourceID, Status: "HELD_task_data", Decision: "hold", Reason: "stitch sidecar is invalid"},
			{Policy: nasStitchPolicy, LeftID: p.Clips[1].SourceID, RightID: p.Clips[2].SourceID, Status: "HELD_task_data", Decision: "hold", Reason: "stitch sidecar is invalid"},
		},
		Error: "stitch sidecar is invalid"}
	if items, err := validateNASStitchResult(p, held); err != nil || len(items) != 0 {
		t.Fatalf("held result not valid terminal evidence: %v %v", items, err)
	}
	raw, err := json.Marshal(held)
	if err != nil {
		t.Fatal(err)
	}
	var body any
	if err = json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	if w := cleanupCall(t, f.s.handleAccountNASStitchResult, 47, 5, "id", "7", body); w.Code != 200 {
		t.Fatalf("held result rejected: %d %s", w.Code, w.Body.String())
	}
	var state string
	var planID *int64
	if err = f.pool.QueryRow(f.ctx, `SELECT state,cleanup_plan_id FROM nas_stitch_tasks WHERE id=7`).Scan(&state, &planID); err != nil {
		t.Fatal(err)
	}
	if state != "held" || planID != nil {
		t.Fatalf("held task not terminal: %q %v", state, planID)
	}
	// The reason is recorded verbatim on the immutable result.
	var stored []byte
	if err = f.pool.QueryRow(f.ctx, `SELECT result FROM nas_stitch_tasks WHERE id=7`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	var round nasStitchResult
	if err = json.Unmarshal(stored, &round); err != nil || !strings.Contains(round.Error, "sidecar") {
		t.Fatalf("held reason not recorded: %s %v", stored, err)
	}
	// `held` is terminal for the hour: no re-lease, and the lane moves on.
	hourPlan, _ := json.Marshal(p)
	if _, err = f.pool.Exec(f.ctx, `INSERT INTO nas_stitch_tasks(id,scope_id,recording_id,hour_at,plan,plan_sha256) VALUES(8,1,2,'2026-09-20T01:00Z',$1,$2)`, string(hourPlan), q.PlanSHA); err != nil {
		t.Fatal(err)
	}
	w := cleanupCall(t, f.s.handleAccountNASStitchLease, 47, 5, "", "", map[string]any{})
	var lease struct {
		Task *nasStitchTask `json:"task"`
	}
	if json.Unmarshal(w.Body.Bytes(), &lease) != nil || lease.Task == nil || lease.Task.ID != 8 {
		t.Fatalf("held hour was re-leased or blocked the lane: %s", w.Body.String())
	}
}

// Held results are idempotent: a lost ACK repeats without mutating evidence.
func TestNASStitchTaskDataHoldReplayIsIdempotent(t *testing.T) {
	f, p, q := newNASStitchTaskFixture(t)
	f.s.cfg.NASStitchEnabled = true
	held := nasStitchResult{Policy: nasStitchPolicy, Token: q.Token, PlanSHA: q.PlanSHA,
		Outputs: []nasStitchOutput{}, Repairs: []nasStitchRepair{}, LeftOut: []int64{1, 2, 3},
		Seams: []nasStitchDecision{
			{Policy: nasStitchPolicy, LeftID: p.Clips[0].SourceID, RightID: p.Clips[1].SourceID, Status: "HELD_task_data", Decision: "hold", Reason: "bad media"},
			{Policy: nasStitchPolicy, LeftID: p.Clips[1].SourceID, RightID: p.Clips[2].SourceID, Status: "HELD_task_data", Decision: "hold", Reason: "bad media"},
		}, Error: "bad media"}
	for n := 0; n < 3; n++ {
		if w := cleanupCall(t, f.s.handleAccountNASStitchResult, 47, 5, "id", "7", held); w.Code != 200 {
			t.Fatalf("replay %d: %d %s", n, w.Code, w.Body.String())
		}
	}
}

// A real plan carries frozen October witnesses. The task-data hold must still be
// accepted then: a hold decision is exactly what a frozen difference requires,
// and every leaf is still disposed by left_out.
func TestNASStitchTaskDataHoldWithFrozenWitnessesIsAccepted(t *testing.T) {
	f, p, q := newNASStitchTaskFixture(t)
	f.s.cfg.NASStitchEnabled = true
	edge := map[string]any{"recording_id": 2,
		"source_key":               [][3]any{{int64(1), strings.Repeat("a", 64), int64(3)}, {int64(2), strings.Repeat("a", 64), int64(3)}},
		"nomination":               true,
		"status":                   "APPROVED85_nomination_requires_runtime_native_join_checks",
		"frozen_diagnostic_sha256": strings.Repeat("d", 64)}
	raw, _ := json.Marshal(edge)
	p.Frozen[0] = raw
	held := nasStitchResult{Policy: nasStitchPolicy, Token: q.Token, PlanSHA: q.PlanSHA,
		Outputs: []nasStitchOutput{}, Repairs: []nasStitchRepair{}, LeftOut: []int64{1, 2, 3},
		Seams: []nasStitchDecision{
			{Policy: nasStitchPolicy, LeftID: 1, RightID: 2, Decision: "hold", Status: "HELD_task_data", Reason: "unreadable source"},
			{Policy: nasStitchPolicy, LeftID: 2, RightID: 3, Decision: "hold", Status: "HELD_task_data", Reason: "unreadable source"},
		}, Error: "unreadable source"}
	if items, err := validateNASStitchResult(p, held); err != nil || len(items) != 0 {
		t.Fatalf("frozen-witness hold rejected: %v %v", items, err)
	}
}
