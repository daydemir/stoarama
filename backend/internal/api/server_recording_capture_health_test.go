package api

import (
	"testing"
	"time"
)

func TestRecordingCoverageWindow(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	started := now.Add(-72 * time.Hour)
	paused := now.Add(-2 * time.Hour)
	start, end := recordingCoverageWindow("paused", started, nil, &paused, now)
	if !end.Equal(paused) || !start.Equal(paused.Add(-24*time.Hour)) {
		t.Fatalf("paused window = [%s,%s), want [%s,%s)", start, end, paused.Add(-24*time.Hour), paused)
	}

	completed := now.Add(-time.Hour)
	start, end = recordingCoverageWindow("completed", started, &completed, nil, now)
	if !start.Equal(started) || !end.Equal(completed) {
		t.Fatalf("completed window = [%s,%s), want [%s,%s)", start, end, started, completed)
	}
}

func TestRecordingCaptureHealthThresholds(t *testing.T) {
	tests := []struct {
		captured int64
		expected int64
		want     recordingCaptureHealthState
	}{
		{0, 0, recordingCaptureHealthNotExpected},
		{98, 100, recordingCaptureHealthHealthy},
		{90, 100, recordingCaptureHealthWarning},
		{89, 100, recordingCaptureHealthCritical},
	}
	for _, test := range tests {
		if got := recordingCaptureHealth("active", test.captured, test.expected); got != test.want {
			t.Fatalf("health(%d,%d) = %q, want %q", test.captured, test.expected, got, test.want)
		}
	}
}
