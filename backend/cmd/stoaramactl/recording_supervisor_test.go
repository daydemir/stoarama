package main

import (
	"testing"
	"time"
)

func TestMergeSupervisorIncidentDetailsPreservesNotificationStateAndAppendsTransition(t *testing.T) {
	now := time.Date(2026, 3, 27, 7, 0, 0, 0, time.UTC)
	existing := map[string]any{
		"current_state":        supervisionIncidentSpotty2h,
		"current_reason":       "loss_rate_2h",
		"last_notified_at":     "2026-03-27T06:00:00Z",
		"notify_count":         1,
		"last_remediated_at":   "2026-03-27T06:10:00Z",
		"remediation_count":    2,
		"state_transitions":    []any{map[string]any{"to_state": supervisionIncidentSpotty2h}},
		"last_state_change_at": "2026-03-27T06:00:00Z",
	}
	current := map[string]any{
		"current_state":  supervisionIncidentDown10m,
		"current_reason": "capture_runtime_stopped",
		"severity":       supervisorIncidentSeverity(supervisionIncidentDown10m),
	}

	merged := mergeSupervisorIncidentDetails(existing, current, supervisionIncidentSpotty2h, supervisionIncidentDown10m, now)

	if merged["notify_count"] != 1 {
		t.Fatalf("expected notify_count preserved, got %v", merged["notify_count"])
	}
	if merged["remediation_count"] != 2 {
		t.Fatalf("expected remediation_count preserved, got %v", merged["remediation_count"])
	}
	transitions := appendSupervisorIncidentTransition(merged["state_transitions"], map[string]any{})
	if len(transitions) != 3 {
		t.Fatalf("expected transition history to grow, got %d entries", len(transitions))
	}
	if merged["current_state"] != supervisionIncidentDown10m {
		t.Fatalf("expected current_state updated, got %v", merged["current_state"])
	}
}

func TestAppendSupervisorIncidentTransitionCapsHistory(t *testing.T) {
	raw := make([]any, 0, supervisionStateHistoryMax+5)
	for i := 0; i < supervisionStateHistoryMax+5; i++ {
		raw = append(raw, map[string]any{"idx": i})
	}
	out := appendSupervisorIncidentTransition(raw, map[string]any{"idx": 999})
	if len(out) != supervisionStateHistoryMax {
		t.Fatalf("expected capped history length %d, got %d", supervisionStateHistoryMax, len(out))
	}
	last := out[len(out)-1]["idx"]
	if last != 999 {
		t.Fatalf("expected latest transition retained, got %v", last)
	}
}
