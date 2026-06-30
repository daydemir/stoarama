package dropletpool

import (
	"testing"
	"time"
)

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	tm, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("parse time %q: %v", s, err)
	}
	return tm.UTC()
}

func TestPeakConcurrency(t *testing.T) {
	base := mustTime(t, "2026-06-24T12:00:00Z")
	tests := []struct {
		name      string
		intervals []jobInterval
		want      int
	}{
		{name: "empty", intervals: nil, want: 0},
		{
			name: "non-overlapping back to back",
			intervals: []jobInterval{
				{Start: base, End: base.Add(60 * time.Second)},
				{Start: base.Add(60 * time.Second), End: base.Add(120 * time.Second)},
			},
			// Half-open: a job ending exactly when the next starts does not overlap.
			want: 1,
		},
		{
			name: "two overlapping",
			intervals: []jobInterval{
				{Start: base, End: base.Add(60 * time.Second)},
				{Start: base.Add(30 * time.Second), End: base.Add(90 * time.Second)},
			},
			want: 2,
		},
		{
			name: "three stacked then decay",
			intervals: []jobInterval{
				{Start: base, End: base.Add(90 * time.Second)},
				{Start: base.Add(10 * time.Second), End: base.Add(90 * time.Second)},
				{Start: base.Add(20 * time.Second), End: base.Add(30 * time.Second)},
				{Start: base.Add(200 * time.Second), End: base.Add(260 * time.Second)},
			},
			want: 3,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := peakConcurrency(tc.intervals); got != tc.want {
				t.Fatalf("peakConcurrency=%d want %d", got, tc.want)
			}
		})
	}
}

func TestForecastFromRecordings_EveryMinutePeakIsClipFanout(t *testing.T) {
	now := mustTime(t, "2026-06-24T12:00:30Z")
	// Two recordings, each firing every minute with a 90s clip. Because the clip
	// (90s) outlives the 60s gap, at steady state each recording has 2 clips alive
	// at once => peak 4 across both recordings.
	recs := []forecastRecording{
		{cronExpr: "* * * * *", cronTimezone: "UTC", clipDurationSec: 90},
		{cronExpr: "* * * * *", cronTimezone: "UTC", clipDurationSec: 90},
	}
	fc := forecastFromRecordings(recs, now, 30*time.Minute)
	if fc.PeakConcurrent != 4 {
		t.Fatalf("peak=%d want 4 (2 recordings x 2 overlapping 90s clips)", fc.PeakConcurrent)
	}
	// Earliest fire after 12:00:30 is 12:01:00.
	wantNext := mustTime(t, "2026-06-24T12:01:00Z")
	if !fc.NextFireAt.Equal(wantNext) {
		t.Fatalf("next fire=%s want %s", fc.NextFireAt, wantNext)
	}
}

func TestForecastFromRecordings_IncludesPreviousSampledFireOverlap(t *testing.T) {
	now := mustTime(t, "2026-06-24T12:00:30Z")
	recs := []forecastRecording{
		{cronExpr: "* * * * *", cronTimezone: "UTC", clipDurationSec: 45},
	}
	fc := forecastFromRecordings(recs, now, 10*time.Second)
	if fc.PeakConcurrent != 1 {
		t.Fatalf("peak=%d want 1 (12:00 fire still overlaps now)", fc.PeakConcurrent)
	}
	if !fc.NextFireAt.IsZero() {
		t.Fatalf("next fire=%s want zero (next future fire is outside lookahead)", fc.NextFireAt)
	}
}

func TestForecastFromRecordings_SampledDelayMarginStacksCloseFires(t *testing.T) {
	now := mustTime(t, "2026-06-24T11:59:50Z")
	recs := []forecastRecording{
		{cronExpr: "0 * * * *", cronTimezone: "UTC", clipDurationSec: 60},
		{cronExpr: "1 * * * *", cronTimezone: "UTC", clipDurationSec: 60},
	}
	fc := forecastFromRecordings(recs, now, 2*time.Minute)
	if fc.PeakConcurrent != 2 {
		t.Fatalf("peak=%d want 2 (scheduler delay margin makes close fires overlap)", fc.PeakConcurrent)
	}
}

func TestForecastFromRecordings_DisjointSchedulesDoNotStack(t *testing.T) {
	now := mustTime(t, "2026-06-24T11:59:30Z")
	// Hourly fires with short clips never overlap -> peak 1 even with many recordings.
	recs := []forecastRecording{
		{cronExpr: "0 * * * *", cronTimezone: "UTC", clipDurationSec: 60},
		{cronExpr: "0 * * * *", cronTimezone: "UTC", clipDurationSec: 60},
		{cronExpr: "0 * * * *", cronTimezone: "UTC", clipDurationSec: 60},
	}
	fc := forecastFromRecordings(recs, now, 24*time.Hour)
	if fc.PeakConcurrent != 3 {
		t.Fatalf("peak=%d want 3 (three recordings all fire at the top of the hour)", fc.PeakConcurrent)
	}
}

func TestForecastFromRecordings_TimezoneRespected(t *testing.T) {
	// A daily 09:00 America/New_York fire. Pick a UTC "now" so the next NY 09:00 is
	// inside the window; assert the fire instant lands in the window and is counted.
	now := mustTime(t, "2026-06-24T00:00:00Z")
	recs := []forecastRecording{
		{cronExpr: "0 9 * * *", cronTimezone: "America/New_York", clipDurationSec: 60},
	}
	fc := forecastFromRecordings(recs, now, 24*time.Hour)
	if fc.PeakConcurrent != 1 {
		t.Fatalf("peak=%d want 1", fc.PeakConcurrent)
	}
	// 2026-06-24 09:00 EDT (UTC-4) == 13:00 UTC.
	wantNext := mustTime(t, "2026-06-24T13:00:00Z")
	if !fc.NextFireAt.Equal(wantNext) {
		t.Fatalf("next fire=%s want %s", fc.NextFireAt, wantNext)
	}
}

func TestForecastFromRecordings_BadCronSkipped(t *testing.T) {
	now := mustTime(t, "2026-06-24T12:00:00Z")
	recs := []forecastRecording{
		{cronExpr: "not a cron", cronTimezone: "UTC", clipDurationSec: 60},
		{cronExpr: "* * * * *", cronTimezone: "UTC", clipDurationSec: 30},
	}
	fc := forecastFromRecordings(recs, now, 5*time.Minute)
	// The bad recording contributes nothing; the good one (30s clip, 60s gap) never
	// overlaps itself -> peak 1.
	if fc.PeakConcurrent != 1 {
		t.Fatalf("peak=%d want 1 (bad cron skipped, good cron non-overlapping)", fc.PeakConcurrent)
	}
}

// TestForecastWithCandidate_LiftsPeak proves the create-time cap's demand model:
// adding the prospective recording to the existing capturing set lifts the
// forecast peak by exactly the candidate's own concurrency, reusing the same
// sweep-line the autoscaler uses (DRY). This is the pure composition
// ForecastPeakWithCandidate performs after loading existing recordings from the DB.
func TestForecastWithCandidate_LiftsPeak(t *testing.T) {
	now := mustTime(t, "2026-06-24T12:00:30Z")
	// One existing recording firing every minute with a 90s clip => peak 2 (two 90s
	// clips alive at steady state). Adding an identical candidate must lift peak to 4.
	existing := []forecastRecording{
		{cronExpr: "* * * * *", cronTimezone: "UTC", clipDurationSec: 90},
	}
	basePeak := forecastFromRecordings(existing, now, 30*time.Minute).PeakConcurrent
	if basePeak != 2 {
		t.Fatalf("base peak=%d want 2", basePeak)
	}
	candidate := forecastRecording{cronExpr: "* * * * *", cronTimezone: "UTC", clipDurationSec: 90}
	withCandidate := forecastFromRecordings(append(append([]forecastRecording{}, existing...), candidate), now, 30*time.Minute).PeakConcurrent
	if withCandidate != 4 {
		t.Fatalf("peak with candidate=%d want 4 (existing 2 + candidate 2)", withCandidate)
	}
	// A disjoint candidate (fires when nothing else does, short clip) does not stack.
	disjoint := forecastRecording{cronExpr: "30 3 * * *", cronTimezone: "UTC", clipDurationSec: 10}
	withDisjoint := forecastFromRecordings(append(append([]forecastRecording{}, existing...), disjoint), now, 30*time.Minute).PeakConcurrent
	if withDisjoint != 2 {
		t.Fatalf("peak with disjoint candidate=%d want 2 (no overlap in window)", withDisjoint)
	}
}

// TestForecastWithCandidates_BundleFanOutLiftsPeakByN proves the bundle preflight's
// demand model: a bundle's N members all share ONE cron/tz/clip, so they all fire
// at the same instants and the forecast peak rises by exactly N over the existing
// fleet at those instants. This is the composition ForecastPeakWithCandidates
// performs after loading existing recordings from the DB (DRY: same sweep-line).
func TestForecastWithCandidates_BundleFanOutLiftsPeakByN(t *testing.T) {
	now := mustTime(t, "2026-06-24T11:59:30Z")
	// One existing hourly recording with a short clip => base peak 1 at the top of
	// the hour (no self-overlap).
	existing := []forecastRecording{
		{cronExpr: "0 * * * *", cronTimezone: "UTC", clipDurationSec: 60},
	}
	basePeak := forecastFromRecordings(existing, now, 30*time.Minute).PeakConcurrent
	if basePeak != 1 {
		t.Fatalf("base peak=%d want 1", basePeak)
	}
	// A bundle of N identical members (same hourly cron/tz/clip) all fire at the
	// same hourly instant, so the peak rises by exactly N.
	const n = 8
	recs := append([]forecastRecording{}, existing...)
	for i := 0; i < n; i++ {
		recs = append(recs, forecastRecording{cronExpr: "0 * * * *", cronTimezone: "UTC", clipDurationSec: 60})
	}
	peak := forecastFromRecordings(recs, now, 30*time.Minute).PeakConcurrent
	if peak != basePeak+n {
		t.Fatalf("peak with bundle of %d=%d want %d (base %d + N)", n, peak, basePeak+n, basePeak)
	}
}

func TestRequiredDroplets_CapacityCeilAndSpendCap(t *testing.T) {
	tests := []struct {
		name                           string
		peak, capacity, min, max, want int
	}{
		{name: "zero demand zero min", peak: 0, capacity: 1, min: 0, max: 5, want: 0},
		{name: "zero demand min floor", peak: 0, capacity: 1, min: 1, max: 5, want: 1},
		{name: "ceil one per capacity", peak: 3, capacity: 1, min: 0, max: 5, want: 3},
		{name: "ceil capacity 2 rounds up", peak: 5, capacity: 2, min: 0, max: 5, want: 3},
		{name: "exact division", peak: 4, capacity: 2, min: 0, max: 5, want: 2},
		{name: "spend cap clamps", peak: 100, capacity: 1, min: 0, max: 5, want: 5},
		{name: "spend cap clamps with capacity", peak: 100, capacity: 4, min: 0, max: 3, want: 3},
		{name: "min above required raises", peak: 1, capacity: 4, min: 2, max: 5, want: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := RequiredDroplets(tc.peak, tc.capacity, tc.min, tc.max); got != tc.want {
				t.Fatalf("RequiredDroplets(peak=%d cap=%d min=%d max=%d)=%d want %d",
					tc.peak, tc.capacity, tc.min, tc.max, got, tc.want)
			}
		})
	}
}

// TestExpandContinuous_OneConstantSlot asserts a continuous recording whose daily
// window overlaps the lookahead expands to EXACTLY ONE interval clamped to
// [max(now,open), min(windowEnd,close)) with first == the window-open instant.
func TestExpandContinuous_OneConstantSlot(t *testing.T) {
	now := mustTime(t, "2026-06-30T10:00:00Z")
	windowEnd := now.Add(30 * time.Minute)
	r := forecastRecording{
		mode:             "continuous",
		cronTimezone:     "UTC",
		clipDurationSec:  60,
		dailyWindowStart: "09:00:00",
		dailyWindowEnd:   "21:00:00",
		envStart:         mustTime(t, "2026-06-30T00:00:00Z"),
	}
	ivals, first := expandRecording(r, now, windowEnd)
	if len(ivals) != 1 {
		t.Fatalf("continuous expansion produced %d intervals, want exactly 1", len(ivals))
	}
	// Window is open at now (09:00<=10:00<21:00), so the interval clamps to
	// [now, windowEnd) and first is the 09:00 open instant.
	if !ivals[0].Start.Equal(now) {
		t.Fatalf("interval start=%s want now=%s", ivals[0].Start, now)
	}
	if !ivals[0].End.Equal(windowEnd) {
		t.Fatalf("interval end=%s want windowEnd=%s", ivals[0].End, windowEnd)
	}
	wantOpen := mustTime(t, "2026-06-30T09:00:00Z")
	if !first.Equal(wantOpen) {
		t.Fatalf("first=%s want window open %s", first, wantOpen)
	}
}

// TestForecastContinuous_NConstantSlots asserts N identical continuous recordings
// produce peak concurrency == N (each a constant +1 slot for the shared window),
// distinct from the sampled cron fan-out, and a sampled recording is unchanged.
func TestForecastContinuous_NConstantSlots(t *testing.T) {
	now := mustTime(t, "2026-06-30T10:00:00Z")
	cont := func() forecastRecording {
		return forecastRecording{
			mode:             "continuous",
			cronTimezone:     "UTC",
			clipDurationSec:  60,
			dailyWindowStart: "09:00:00",
			dailyWindowEnd:   "21:00:00",
			envStart:         mustTime(t, "2026-06-30T00:00:00Z"),
		}
	}
	recs := []forecastRecording{cont(), cont(), cont()}
	fc := forecastFromRecordings(recs, now, 30*time.Minute)
	if fc.PeakConcurrent != 3 {
		t.Fatalf("continuous peak=%d want 3 (3 constant slots)", fc.PeakConcurrent)
	}
	// A continuous stream is ONE slot for the whole window, never a per-clip blip:
	// adding a 4th lifts the peak by exactly 1.
	recs = append(recs, cont())
	fc = forecastFromRecordings(recs, now, 30*time.Minute)
	if fc.PeakConcurrent != 4 {
		t.Fatalf("continuous peak=%d want 4", fc.PeakConcurrent)
	}
}

// TestExpandContinuous_WindowBeforeNow asserts a continuous recording whose daily
// window has already closed for the day (and the next opens past the lookahead)
// contributes ZERO intervals now (correct for the autoscaler: nothing to provision
// yet), while the sampled expansion is unaffected.
func TestExpandContinuous_WindowClosedContributesZero(t *testing.T) {
	now := mustTime(t, "2026-06-30T22:00:00Z") // after a 09:00-21:00 window
	windowEnd := now.Add(30 * time.Minute)
	r := forecastRecording{
		mode:             "continuous",
		cronTimezone:     "UTC",
		clipDurationSec:  60,
		dailyWindowStart: "09:00:00",
		dailyWindowEnd:   "21:00:00",
		envStart:         mustTime(t, "2026-06-30T00:00:00Z"),
	}
	ivals, _ := expandRecording(r, now, windowEnd)
	if len(ivals) != 0 {
		t.Fatalf("closed window produced %d intervals, want 0", len(ivals))
	}
}
