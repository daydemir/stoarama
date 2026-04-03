package api

import (
	"testing"
	"time"
)

func TestClassifyRecordingSupervisionSkipsSpottyDuringWarmup(t *testing.T) {
	now := time.Date(2026, 4, 3, 4, 0, 0, 0, time.UTC)
	assignedAt := now.Add(-20 * time.Minute)
	lastFrameAt := now.Add(-30 * time.Second)
	streamUpdatedAt := now.Add(-22 * time.Minute)

	state, reason, unhealthySince := classifyRecordingSupervision(now, recordingSupervisionInput{
		RecordingState:  "on",
		ServerID:        "do-1",
		RuntimeStatus:   "running",
		AssignedAt:      &assignedAt,
		LastFrameAt:     &lastFrameAt,
		StreamUpdatedAt: streamUpdatedAt,
		Metrics: recordingSupervisionMetrics{
			LossRate2h:       99.0,
			ProcessIssues2h:  6,
			OutageEpisodes2h: 6,
		},
	})
	if state != "healthy" {
		t.Fatalf("expected healthy during warmup, got %q", state)
	}
	if reason != "fresh_captures" {
		t.Fatalf("expected fresh_captures during warmup, got %q", reason)
	}
	if unhealthySince != nil {
		t.Fatalf("expected no unhealthy_since during warmup, got %v", unhealthySince)
	}
}

func TestClassifyRecordingSupervisionTriggersSpottyAfterTwoHours(t *testing.T) {
	now := time.Date(2026, 4, 3, 4, 0, 0, 0, time.UTC)
	assignedAt := now.Add(-3 * time.Hour)
	lastFrameAt := now.Add(-15 * time.Second)
	streamUpdatedAt := now.Add(-3 * time.Hour)

	state, reason, unhealthySince := classifyRecordingSupervision(now, recordingSupervisionInput{
		RecordingState:  "on",
		ServerID:        "do-1",
		RuntimeStatus:   "running",
		AssignedAt:      &assignedAt,
		LastFrameAt:     &lastFrameAt,
		StreamUpdatedAt: streamUpdatedAt,
		Metrics: recordingSupervisionMetrics{
			LossRate2h: 99.0,
		},
	})
	if state != "spotty_2h" {
		t.Fatalf("expected spotty_2h, got %q", state)
	}
	if reason != "loss_rate_2h" {
		t.Fatalf("expected loss_rate_2h, got %q", reason)
	}
	if unhealthySince == nil || !unhealthySince.Equal(assignedAt) {
		t.Fatalf("expected unhealthy_since=%v, got %v", assignedAt, unhealthySince)
	}
}

func TestClassifyRecordingSupervisionUsesContinuousOnTimeNotRecentReassign(t *testing.T) {
	now := time.Date(2026, 4, 3, 4, 0, 0, 0, time.UTC)
	assignedAt := now.Add(-10 * time.Minute)
	lastFrameAt := now.Add(-20 * time.Second)
	streamUpdatedAt := now.Add(-3 * time.Hour)

	state, reason, unhealthySince := classifyRecordingSupervision(now, recordingSupervisionInput{
		RecordingState:  "on",
		ServerID:        "do-1",
		RuntimeStatus:   "running",
		AssignedAt:      &assignedAt,
		LastFrameAt:     &lastFrameAt,
		StreamUpdatedAt: streamUpdatedAt,
		Metrics: recordingSupervisionMetrics{
			ProcessIssues2h: 4,
		},
	})
	if state != "spotty_2h" {
		t.Fatalf("expected spotty_2h after recent reassign on long-running stream, got %q", state)
	}
	if reason != "process_restarts_2h" {
		t.Fatalf("expected process_restarts_2h, got %q", reason)
	}
	if unhealthySince == nil || !unhealthySince.Equal(streamUpdatedAt) {
		t.Fatalf("expected unhealthy_since=%v, got %v", streamUpdatedAt, unhealthySince)
	}
}
