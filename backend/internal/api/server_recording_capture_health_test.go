package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/recsched"
)

func TestCaptureHealthPageUsesBoundedMetricAdmission(t *testing.T) {
	slots := make(chan struct{}, recordingMetricConcurrency)
	for range recordingMetricConcurrency {
		slots <- struct{}{}
	}
	s := &Server{recordingMetricSlots: slots}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/account/recordings/700/capture-health?from=2026-05-01&to=2026-05-31", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	started := time.Now()
	s.writeRecordingCaptureHealth(rec, req, 47, 700, false)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("capture health admission ignored request deadline: %s", elapsed)
	}
	if rec.Header().Get("Retry-After") != "10" {
		t.Fatalf("retry-after=%q", rec.Header().Get("Retry-After"))
	}

	authKey := recordingCaptureHealthCacheKey(req, 47, 700, false)
	publicKey := recordingCaptureHealthCacheKey(req, 47, 700, true)
	otherRange := recordingCaptureHealthCacheKey(httptest.NewRequest(http.MethodGet, "/?from=2026-04-01&to=2026-04-30", nil), 47, 700, false)
	if authKey == publicKey || authKey == otherRange {
		t.Fatalf("capture health cache keys are not scope-isolated: auth=%+v public=%+v range=%+v", authKey, publicKey, otherRange)
	}
}

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
	bins, err := expectedHealthBins(spec, now)
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

func TestExpectedClipsStartingInBinOwnsBoundaryCrossingClips(t *testing.T) {
	start := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	spec := recordingHealthSpec{
		Mode:            "sampled",
		CronExpr:        "0 */3 * * *",
		Timezone:        "UTC",
		ClipDurationSec: 90 * 60,
		StartAt:         start,
	}
	bins, err := expectedHealthBinsInRanges(spec, []captureHealthRange{{
		start: start.Add(2 * time.Hour),
		end:   start.Add(4 * time.Hour),
	}}, start.Add(6*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(bins) != 1 {
		t.Fatalf("bins=%+v", bins)
	}
	got := bins[0].Expected
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
	bins, err := expectedHealthBins(spec, now)
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

func TestRecentSampledHealthBinsBoundAncientSparseSchedule(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	spec := recordingHealthSpec{
		Mode:            "sampled",
		CronExpr:        "0 0 1 1 *",
		Timezone:        "UTC",
		ClipDurationSec: 60,
		Status:          "active",
		StartAt:         now.AddDate(-100, 0, 0),
	}
	bins, err := expectedHealthBins(spec, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(bins) != 1 || bins[0].Expected != 1 {
		t.Fatalf("bins=%+v", bins)
	}
	if bins[0].Start.Before(now.Add(-recentHealthLookback)) {
		t.Fatalf("bin %s exceeded bounded lookback", bins[0].Start)
	}
}

func TestExpectedHealthBinsInRangesExpandsScheduleOnceAcrossBoundaries(t *testing.T) {
	start := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	spec := recordingHealthSpec{
		Mode:            "sampled",
		CronExpr:        "0 */3 * * *",
		Timezone:        "UTC",
		ClipDurationSec: 90 * 60,
		StartAt:         start,
	}
	ranges := []captureHealthRange{
		{start: start, end: start.Add(2 * time.Hour)},
		{start: start.Add(2 * time.Hour), end: start.Add(4 * time.Hour)},
		{start: start.Add(4 * time.Hour), end: start.Add(6 * time.Hour)},
	}
	bins, err := expectedHealthBinsInRanges(spec, ranges, start.Add(8*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(bins) != 2 || bins[0].Expected != 1 || bins[1].Expected != 1 ||
		!bins[0].Start.Equal(start) || !bins[1].Start.Equal(start.Add(2*time.Hour)) {
		t.Fatalf("bins=%+v", bins)
	}
}

func TestCompletedFiveSecondContinuousRecentHealthAvoidsStartExpansion(t *testing.T) {
	end := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	start := end.AddDate(0, 0, -90)
	spec := recordingHealthSpec{
		Mode:             "continuous",
		Timezone:         "UTC",
		DailyWindowStart: "00:00",
		DailyWindowEnd:   "00:00",
		ActiveWeekdays:   recsched.AllWeekdays,
		ClipDurationSec:  5,
		Status:           "completed",
		StartAt:          start,
		EndAt:            &end,
	}
	bins, err := expectedHealthBins(spec, end.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(bins) != recentHealthBinCount {
		t.Fatalf("bins=%d want=%d", len(bins), recentHealthBinCount)
	}
	for _, bin := range bins {
		if bin.Expected != int64(2*time.Hour/(5*time.Second)) {
			t.Fatalf("expected=%d want=1440 for bin %s", bin.Expected, bin.Start)
		}
	}
}

func TestFiveSecondContinuousNinetyDayDetailHealthAvoidsStartExpansion(t *testing.T) {
	start := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 0, 90)
	spec := recordingHealthSpec{
		Mode:             "continuous",
		Timezone:         "UTC",
		DailyWindowStart: "00:00",
		DailyWindowEnd:   "00:00",
		ActiveWeekdays:   recsched.AllWeekdays,
		ClipDurationSec:  5,
		Status:           "completed",
		StartAt:          start,
		EndAt:            &end,
	}
	ranges := localCaptureHealthRanges(start, end, time.UTC)
	bins, err := expectedHealthBinsInRanges(spec, ranges, end)
	if err != nil {
		t.Fatal(err)
	}
	if len(bins) != 90*24 {
		t.Fatalf("bins=%d want=%d", len(bins), 90*24)
	}
	for _, bin := range bins {
		if bin.Expected != int64(time.Hour/(5*time.Second)) {
			t.Fatalf("expected=%d want=720 for bin %s", bin.Expected, bin.Start)
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

func TestSetCapturedHealthBinRejectsInvalidOrdinal(t *testing.T) {
	bins := make([]recordingHealthBin, 2)
	if err := setCapturedHealthBin(bins, 1, 7); err != nil {
		t.Fatal(err)
	}
	if bins[0].Captured != 7 {
		t.Fatalf("captured=%d want 7", bins[0].Captured)
	}
	for _, ordinal := range []int64{0, 3} {
		if err := setCapturedHealthBin(bins, ordinal, 1); err == nil {
			t.Fatalf("ordinal %d unexpectedly accepted", ordinal)
		}
	}
}

func TestLocalCaptureHealthRangesFollowLocalHourBoundaries(t *testing.T) {
	location, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 7, 28, 2, 45, 0, 0, time.UTC) // 08:15 local
	ranges := localCaptureHealthRanges(start, start.Add(2*time.Hour), location)
	if len(ranges) != 3 {
		t.Fatalf("ranges=%d want 3 partial/full/partial local hours", len(ranges))
	}
	if got := ranges[0].end.In(location).Format("15:04"); got != "09:00" {
		t.Fatalf("first local boundary=%s want 09:00", got)
	}
	if got := ranges[1].end.In(location).Format("15:04"); got != "10:00" {
		t.Fatalf("second local boundary=%s want 10:00", got)
	}
}

func TestLocalCaptureHealthRangesCoverDSTFallBackDay(t *testing.T) {
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 11, 1, 0, 0, 0, 0, location)
	end := start.AddDate(0, 0, 1)
	ranges := localCaptureHealthRanges(start.UTC(), end.UTC(), location)
	if len(ranges) != 25 {
		t.Fatalf("ranges=%d want 25 local hour cells", len(ranges))
	}
	var covered time.Duration
	cursor := start.UTC()
	for _, hour := range ranges {
		if !hour.start.Equal(cursor) || !hour.end.After(hour.start) {
			t.Fatalf("non-contiguous range [%s,%s) after %s", hour.start, hour.end, cursor)
		}
		covered += hour.end.Sub(hour.start)
		cursor = hour.end
	}
	if !cursor.Equal(end.UTC()) || covered != 25*time.Hour {
		t.Fatalf("covered through %s for %s, want %s and 25h", cursor, covered, end.UTC())
	}
	if got := ranges[1].start.In(location).Format("15:04 MST"); got != "01:00 EDT" {
		t.Fatalf("first repeated hour=%q", got)
	}
	if got := ranges[2].start.In(location).Format("15:04 MST"); got != "01:00 EST" {
		t.Fatalf("second repeated hour=%q", got)
	}
}

func TestRecordingHistoryWindowKeepsActiveAndPausedLifetime(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	start := now.AddDate(0, -2, 0)
	gotStart, gotEnd := recordingHistoryWindow("active", start, nil, nil, now)
	if !gotStart.Equal(start) || !gotEnd.Equal(now) {
		t.Fatalf("active history=[%s,%s)", gotStart, gotEnd)
	}
	paused := now.Add(-48 * time.Hour)
	gotStart, gotEnd = recordingHistoryWindow("paused", start, nil, &paused, now)
	if !gotStart.Equal(start) || !gotEnd.Equal(paused) {
		t.Fatalf("paused history=[%s,%s)", gotStart, gotEnd)
	}
}

func TestCaptureHealthLastDayTreatsHistoryEndAsExclusive(t *testing.T) {
	location, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 8, 1, 0, 0, 0, 0, location)
	end := start.AddDate(0, 0, 2)
	if got := captureHealthLastDay(start, end, location); !got.Equal(start.AddDate(0, 0, 1)) {
		t.Fatalf("last history day=%s want %s", got, start.AddDate(0, 0, 1))
	}
}

func TestCaptureHealthRangeMustOverlapRecordingCoverage(t *testing.T) {
	coverageStart := time.Date(2026, 8, 1, 8, 0, 0, 0, time.UTC)
	coverageEnd := time.Date(2026, 8, 18, 20, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name       string
		start, end time.Time
		want       bool
	}{
		{name: "before", start: coverageStart.Add(-48 * time.Hour), end: coverageStart, want: false},
		{name: "after", start: coverageEnd, end: coverageEnd.Add(48 * time.Hour), want: false},
		{name: "overlap start", start: coverageStart.Add(-time.Hour), end: coverageStart.Add(time.Hour), want: true},
		{name: "inside", start: coverageStart, end: coverageEnd, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := captureHealthRangeOverlaps(tc.start, tc.end, coverageStart, coverageEnd); got != tc.want {
				t.Fatalf("overlap=%t want=%t", got, tc.want)
			}
		})
	}
}
