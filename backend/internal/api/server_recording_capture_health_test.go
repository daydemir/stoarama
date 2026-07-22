package api

import (
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/recsched"
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

func TestExpectedHealthBinsUseScheduledRecordingTime(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	spec := recordingHealthSpec{
		ID:               1,
		Mode:             "continuous",
		Timezone:         "UTC",
		DailyWindowStart: "08:00",
		DailyWindowEnd:   "10:00",
		ActiveWeekdays:   recsched.AllWeekdays,
		ClipDurationSec:  60,
		Status:           "active",
		StartAt:          now.AddDate(0, 0, -20),
	}
	bins, err := expectedHealthBins(spec, now, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(bins) != recentHealthBinCount {
		t.Fatalf("bin count=%d want=%d", len(bins), recentHealthBinCount)
	}
	for _, bin := range bins {
		if bin.Expected != 120 {
			t.Fatalf("expected clips=%d want=120 for bin %s", bin.Expected, bin.Start)
		}
	}
	if got := bins[len(bins)-1].Start; !got.Equal(time.Date(2026, 7, 22, 8, 0, 0, 0, time.UTC)) {
		t.Fatalf("latest bin=%s", got)
	}
}

func TestRecordingHealthBinSizeAdaptsForDetail(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if got := recordingHealthBinSize(start, start.Add(10*24*time.Hour), true); got != 2*time.Hour {
		t.Fatalf("10-day size=%s want=2h", got)
	}
	if got := recordingHealthBinSize(start, start.Add(20*24*time.Hour), true); got != 6*time.Hour {
		t.Fatalf("20-day size=%s want=6h", got)
	}
}

func TestDetailedHealthBinsCoverFullActiveRecording(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	spec := recordingHealthSpec{
		ID:               1,
		Mode:             "continuous",
		Timezone:         "UTC",
		DailyWindowStart: "00:00",
		DailyWindowEnd:   "00:00",
		ActiveWeekdays:   recsched.AllWeekdays,
		ClipDurationSec:  60,
		Status:           "active",
		StartAt:          now.Add(-48 * time.Hour),
	}
	bins, err := expectedHealthBins(spec, now, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(bins) != 24 {
		t.Fatalf("bin count=%d want=24", len(bins))
	}
}

func TestDetailedHealthBinsClipPartialIntervals(t *testing.T) {
	now := time.Date(2026, 7, 22, 10, 30, 0, 0, time.UTC)
	spec := recordingHealthSpec{
		ID:               1,
		Mode:             "continuous",
		Timezone:         "UTC",
		DailyWindowStart: "00:00",
		DailyWindowEnd:   "00:00",
		ActiveWeekdays:   recsched.AllWeekdays,
		ClipDurationSec:  60,
		Status:           "active",
		StartAt:          time.Date(2026, 7, 22, 8, 30, 0, 0, time.UTC),
	}
	bins, err := expectedHealthBins(spec, now, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(bins) != 2 {
		t.Fatalf("bin count=%d want=2", len(bins))
	}
	if !bins[0].Start.Equal(spec.StartAt) || !bins[0].End.Equal(time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC)) || bins[0].Expected != 90 {
		t.Fatalf("first bin=%+v", bins[0])
	}
	if !bins[1].Start.Equal(time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC)) || !bins[1].End.Equal(now) || bins[1].Expected != 30 {
		t.Fatalf("current bin=%+v", bins[1])
	}

	paused := time.Date(2026, 7, 22, 9, 30, 0, 0, time.UTC)
	spec.Status = "paused"
	spec.StartAt = time.Date(2026, 7, 22, 8, 0, 0, 0, time.UTC)
	spec.PausedAt = &paused
	bins, err = expectedHealthBins(spec, now, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(bins) != 1 || !bins[0].End.Equal(paused) || bins[0].Expected != 90 {
		t.Fatalf("paused bins=%+v", bins)
	}
	spec.Status = "completed"
	spec.PausedAt = nil
	spec.EndAt = &paused
	bins, err = expectedHealthBins(spec, now, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(bins) != 1 || !bins[0].End.Equal(paused) || bins[0].Expected != 90 {
		t.Fatalf("completed bins=%+v", bins)
	}
}

func TestExpectedClipsStartingInBinOwnsBoundaryCrossingClips(t *testing.T) {
	start := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	spec := recordingHealthSpec{
		Mode:            "sampled",
		CronExpr:        "0 */3 * * *",
		Timezone:        "UTC",
		ClipDurationSec: 90 * 60,
		StartAt:         start,
	}
	got, err := expectedClipsStartingInBin(spec, start.Add(2*time.Hour), start.Add(4*time.Hour), start.Add(6*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if got != 1 {
		t.Fatalf("expected=%d want=1", got)
	}
}

func TestRecentSampledHealthBinsJumpSparseSchedule(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	spec := recordingHealthSpec{
		Mode:            "sampled",
		CronExpr:        "0 0 1 * *",
		Timezone:        "UTC",
		ClipDurationSec: 60,
		Status:          "active",
		StartAt:         now.AddDate(-2, 0, 0),
	}
	bins, err := expectedHealthBins(spec, now, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(bins) != recentHealthBinCount {
		t.Fatalf("bin count=%d want=%d", len(bins), recentHealthBinCount)
	}
	for _, bin := range bins {
		if bin.Expected != 1 {
			t.Fatalf("expected clips=%d want=1", bin.Expected)
		}
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
