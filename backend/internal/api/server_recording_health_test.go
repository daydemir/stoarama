package api

import (
	"math"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
)

func TestExpectedCapturesPerHourUsesRecordingSemantics(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		class       string
		intervalSec int
		want        int64
	}{
		{
			name:        "clip_native_ignores_interval",
			class:       capture.ExecutionClassVideoLive,
			intervalSec: 7,
			want:        120,
		},
		{
			name:        "frame_based_uses_interval",
			class:       capture.ExecutionClassImagePoll,
			intervalSec: 7,
			want:        480,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := expectedCapturesPerHour(tc.class, tc.intervalSec); got != tc.want {
				t.Fatalf("expectedCapturesPerHour(%q, %d)=%d want %d", tc.class, tc.intervalSec, got, tc.want)
			}
		})
	}
}

func TestBuildRecordingHealthBuckets(t *testing.T) {
	t.Parallel()

	windowStart := time.Date(2026, time.March, 25, 17, 0, 0, 0, time.UTC)
	windowEnd := windowStart.Add(3 * time.Hour)
	counts := map[time.Time]recordingHealthHourlyCounts{
		windowStart: {
			Success: 120,
			Error:   5,
		},
		windowStart.Add(time.Hour): {
			Success: 90,
			Error:   1,
		},
	}

	buckets, summary := buildRecordingHealthBuckets(windowStart, windowEnd, 120, counts)
	if got, want := len(buckets), 3; got != want {
		t.Fatalf("bucket count=%d want %d", got, want)
	}

	first := buckets[0]
	if !first.HourStartUTC.Equal(windowStart) {
		t.Fatalf("first hour_start=%s want %s", first.HourStartUTC, windowStart)
	}
	if first.ExpectedCaptures != 120 || first.SuccessCaptures != 120 || first.ErrorCaptures != 5 || first.MissingCaptures != 0 {
		t.Fatalf("first bucket=%+v", first)
	}
	if first.LossRatePct != 0 {
		t.Fatalf("first loss_rate=%v want 0", first.LossRatePct)
	}

	second := buckets[1]
	if second.MissingCaptures != 30 {
		t.Fatalf("second missing=%d want 30", second.MissingCaptures)
	}
	if second.LossRatePct != 25 {
		t.Fatalf("second loss_rate=%v want 25", second.LossRatePct)
	}

	third := buckets[2]
	if third.SuccessCaptures != 0 || third.ErrorCaptures != 0 || third.MissingCaptures != 120 {
		t.Fatalf("third bucket=%+v", third)
	}
	if third.LossRatePct != 100 {
		t.Fatalf("third loss_rate=%v want 100", third.LossRatePct)
	}

	if summary.Buckets != 3 {
		t.Fatalf("summary buckets=%d want 3", summary.Buckets)
	}
	if summary.HoursWithLoss != 2 {
		t.Fatalf("summary hours_with_loss=%d want 2", summary.HoursWithLoss)
	}
	if summary.HoursWithErrors != 2 {
		t.Fatalf("summary hours_with_errors=%d want 2", summary.HoursWithErrors)
	}
	if summary.TotalExpectedCaptures != 360 {
		t.Fatalf("summary total_expected=%d want 360", summary.TotalExpectedCaptures)
	}
	if summary.TotalSuccessCaptures != 210 {
		t.Fatalf("summary total_success=%d want 210", summary.TotalSuccessCaptures)
	}
	if summary.TotalErrorCaptures != 6 {
		t.Fatalf("summary total_error=%d want 6", summary.TotalErrorCaptures)
	}
	if summary.TotalMissingCaptures != 150 {
		t.Fatalf("summary total_missing=%d want 150", summary.TotalMissingCaptures)
	}
	if math.Abs(summary.AvgLossRatePct-41.67) > 0.01 {
		t.Fatalf("summary avg_loss_rate=%v want about 41.67", summary.AvgLossRatePct)
	}
	if summary.MaxLossRatePct != 100 {
		t.Fatalf("summary max_loss_rate=%v want 100", summary.MaxLossRatePct)
	}
}
