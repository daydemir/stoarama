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

func TestShouldNotifySupervisorIncident(t *testing.T) {
	now := time.Date(2026, 4, 3, 12, 0, 0, 0, time.UTC)
	recent := now.Add(-30 * time.Minute)
	old := now.Add(-(supervisionSpottyNotifyCooldown + 5*time.Minute))
	current := now.Add(-5 * time.Minute)

	tests := []struct {
		name                 string
		incidentType         string
		currentLastNotified  *time.Time
		previousLastNotified *time.Time
		want                 bool
	}{
		{
			name:         "down incidents notify immediately",
			incidentType: supervisionIncidentDown10m,
			want:         true,
		},
		{
			name:         "new spotty incident notifies when no history exists",
			incidentType: supervisionIncidentSpotty2h,
			want:         true,
		},
		{
			name:                 "open incident does not renotify",
			incidentType:         supervisionIncidentSpotty2h,
			currentLastNotified:  &current,
			previousLastNotified: &old,
			want:                 false,
		},
		{
			name:                 "reopened spotty incident is suppressed within cooldown",
			incidentType:         supervisionIncidentSpotty2h,
			previousLastNotified: &recent,
			want:                 false,
		},
		{
			name:                 "reopened spotty incident notifies after cooldown",
			incidentType:         supervisionIncidentSpotty2h,
			previousLastNotified: &old,
			want:                 true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldNotifySupervisorIncident(now, tt.incidentType, tt.currentLastNotified, tt.previousLastNotified)
			if got != tt.want {
				t.Fatalf("shouldNotifySupervisorIncident()=%v want %v", got, tt.want)
			}
		})
	}
}

func TestShouldRemediateSupervisorIncident(t *testing.T) {
	now := time.Date(2026, 4, 3, 12, 0, 0, 0, time.UTC)
	recent := now.Add(-30 * time.Minute)
	old := now.Add(-2 * time.Hour)
	retry := time.Hour

	tests := []struct {
		name             string
		incidentType     string
		existing         bool
		existingDetails  map[string]any
		remediationRetry time.Duration
		want             bool
	}{
		{
			name:             "new down incident remediates immediately",
			incidentType:     supervisionIncidentDown10m,
			remediationRetry: retry,
			want:             true,
		},
		{
			name:         "existing down incident waits for retry window",
			incidentType: supervisionIncidentDown10m,
			existing:     true,
			existingDetails: map[string]any{
				"last_remediated_at": recent.Format(time.RFC3339Nano),
			},
			remediationRetry: retry,
			want:             false,
		},
		{
			name:         "existing down incident remediates after retry window",
			incidentType: supervisionIncidentDown10m,
			existing:     true,
			existingDetails: map[string]any{
				"last_remediated_at": old.Format(time.RFC3339Nano),
			},
			remediationRetry: retry,
			want:             true,
		},
		{
			name:             "spotty incident is alert only",
			incidentType:     supervisionIncidentSpotty2h,
			remediationRetry: retry,
			want:             false,
		},
		{
			name:         "existing spotty incident stays alert only",
			incidentType: supervisionIncidentSpotty2h,
			existing:     true,
			existingDetails: map[string]any{
				"last_remediated_at": old.Format(time.RFC3339Nano),
			},
			remediationRetry: retry,
			want:             false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldRemediateSupervisorIncident(now, tt.incidentType, tt.existing, tt.existingDetails, tt.remediationRetry)
			if got != tt.want {
				t.Fatalf("shouldRemediateSupervisorIncident()=%v want %v", got, tt.want)
			}
		})
	}
}
