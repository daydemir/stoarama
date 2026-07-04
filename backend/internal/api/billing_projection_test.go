package api

import (
	"testing"
	"time"
)

// ptime is a small helper for a fixed UTC instant in the tests.
func ptime(y int, mo time.Month, d, h int) time.Time {
	return time.Date(y, mo, d, h, 0, 0, 0, time.UTC)
}

// TestProjectRecordingHours checks the EXACT forward projection against the
// stored schedule. now is fixed and winEnd is chosen so the projected span is a
// clean N days, and projStart lands on an hour boundary so the current-hour ceil
// is a no-op.
func TestProjectRecordingHours(t *testing.T) {
	now := ptime(2026, time.July, 1, 0) // midnight so the hour-ceil is a no-op.

	cases := []struct {
		name    string
		rec     projectedRecording
		winEnd  time.Time
		wantHrs float64
	}{
		{
			// Sampled hourly, open-ended, 10 days => 24 distinct hrs/day * 10 = 240.
			name: "sampled hourly open-ended 10 days",
			rec: projectedRecording{
				Mode:         "sampled",
				CronExpr:     "0 * * * *",
				CronTimezone: "UTC",
				StartAt:      now.AddDate(0, 0, -5), // already started.
			},
			winEnd:  now.AddDate(0, 0, 10),
			wantHrs: 240,
		},
		{
			// Every 4 hours => 6 distinct hours/day * 10 days = 60.
			name: "sampled every 4h 10 days",
			rec: projectedRecording{
				Mode:         "sampled",
				CronExpr:     "0 */4 * * *",
				CronTimezone: "UTC",
				StartAt:      now.AddDate(0, 0, -5),
			},
			winEnd:  now.AddDate(0, 0, 10),
			wantHrs: 60,
		},
		{
			// Daily at 09:00 => 1 hr/day * 10 days = 10.
			name: "sampled daily 10 days",
			rec: projectedRecording{
				Mode:         "sampled",
				CronExpr:     "0 9 * * *",
				CronTimezone: "UTC",
				StartAt:      now.AddDate(0, 0, -5),
			},
			winEnd:  now.AddDate(0, 0, 10),
			wantHrs: 10,
		},
		{
			// Continuous 12h window (09:00-21:00), 5 days => 12 hrs/day * 5 = 60.
			name: "continuous 12h window 5 days",
			rec: projectedRecording{
				Mode:         "continuous",
				CronTimezone: "UTC",
				DailyStart:   "09:00:00",
				DailyEnd:     "21:00:00",
				StartAt:      now.AddDate(0, 0, -5),
			},
			winEnd:  now.AddDate(0, 0, 5),
			wantHrs: 60,
		},
		{
			// end_at caps the projection mid-window: hourly, but stops after 4 days
			// even though the billing window is 10 days => 24 * 4 = 96.
			name: "sampled hourly end_at caps at 4 days",
			rec: projectedRecording{
				Mode:         "sampled",
				CronExpr:     "0 * * * *",
				CronTimezone: "UTC",
				StartAt:      now.AddDate(0, 0, -2),
				EndAt:        ptr(now.AddDate(0, 0, 4)),
			},
			winEnd:  now.AddDate(0, 0, 10),
			wantHrs: 96,
		},
		{
			// start_at in the FUTURE (scheduled, not yet started): projection begins
			// at start_at, not now. Starts in 3 days, window ends in 10 => 7 days of
			// hourly capture => 24 * 7 = 168.
			name: "sampled hourly future start",
			rec: projectedRecording{
				Mode:         "sampled",
				CronExpr:     "0 * * * *",
				CronTimezone: "UTC",
				StartAt:      now.AddDate(0, 0, 3),
			},
			winEnd:  now.AddDate(0, 0, 10),
			wantHrs: 168,
		},
		{
			// Unparseable/custom cron: safe upper bound (every hour of the span) =>
			// 24 * 10 = 240, never 0.
			name: "unparseable cron upper bound",
			rec: projectedRecording{
				Mode:         "sampled",
				CronExpr:     "not a cron",
				CronTimezone: "UTC",
				StartAt:      now.AddDate(0, 0, -1),
			},
			winEnd:  now.AddDate(0, 0, 10),
			wantHrs: 240,
		},
		{
			// A window entirely in the past (end_at before now) projects 0.
			name: "already ended projects zero",
			rec: projectedRecording{
				Mode:         "sampled",
				CronExpr:     "0 * * * *",
				CronTimezone: "UTC",
				StartAt:      now.AddDate(0, 0, -10),
				EndAt:        ptr(now.AddDate(0, 0, -1)),
			},
			winEnd:  now.AddDate(0, 0, 10),
			wantHrs: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := projectRecordingHours(tc.rec, now, tc.winEnd)
			if got != tc.wantHrs {
				t.Fatalf("projectRecordingHours = %v, want %v", got, tc.wantHrs)
			}
		})
	}
}

// TestProjectRecordingHoursDistinctHoursPerDay pins the distinct record-hours a
// stored cron bills over exactly one day. The key exactness fix versus the old
// approximation: a sub-hourly cron (every 15/30 min) still bills only the 24
// distinct hours it lands in, not its raw fire count (96/48). A clustered cron
// (two fires in the same hour) bills that hour once.
func TestProjectRecordingHoursDistinctHoursPerDay(t *testing.T) {
	now := ptime(2026, time.July, 1, 0)
	winEnd := now.AddDate(0, 0, 1) // exactly one day.

	cases := []struct {
		expr string
		want float64
	}{
		{"*/15 * * * *", 24}, // every 15 min => 24 distinct hours, NOT 96 fires.
		{"*/30 * * * *", 24}, // every 30 min => 24 distinct hours, NOT 48 fires.
		{"0 * * * *", 24},    // hourly.
		{"0 */4 * * *", 6},   // every 4 hours.
		{"0 9 * * *", 1},     // daily.
		{"0,30 9 * * *", 1},  // clustered: 09:00 and 09:30 share one hour bucket.
	}
	for _, tc := range cases {
		rec := projectedRecording{
			Mode:         "sampled",
			CronExpr:     tc.expr,
			CronTimezone: "UTC",
			StartAt:      now.AddDate(0, 0, -1),
		}
		if got := projectRecordingHours(rec, now, winEnd); got != tc.want {
			t.Errorf("projectRecordingHours(%q, 1 day) = %v, want %v", tc.expr, got, tc.want)
		}
	}
}

// TestProjectRecordingHoursRestrictedCron checks that a restricted (weekly) cron
// projects only its firing days: a Mondays-only daily cron over a 14-day window
// that contains two Mondays bills 2 record-hours (1 per firing day).
func TestProjectRecordingHoursRestrictedCron(t *testing.T) {
	now := ptime(2026, time.July, 1, 0) // 2026-07-01 is a Wednesday (UTC).
	rec := projectedRecording{
		Mode:         "sampled",
		CronExpr:     "0 9 * * 1", // Mondays only at 09:00
		CronTimezone: "UTC",
		StartAt:      now.AddDate(0, 0, -1),
	}
	// A 14-day window from Wed 2026-07-01 contains Mondays 07-06 and 07-13.
	if got := projectRecordingHours(rec, now, now.AddDate(0, 0, 14)); got != 2 {
		t.Fatalf("projectRecordingHours(weekly Monday, 14d) = %v, want 2", got)
	}
}

// TestProjectRecordingHoursPartialFinalDay pins the fixed bug where a partial
// final day used to be ceiled to a full day. A continuous 06:00-18:00 window over
// 1.5 days bills 12 hours on the full day plus 6 hours on the half day = 18, NOT
// 2 * 12 = 24.
func TestProjectRecordingHoursPartialFinalDay(t *testing.T) {
	now := ptime(2026, time.July, 1, 0)
	rec := projectedRecording{
		Mode:         "continuous",
		CronTimezone: "UTC",
		DailyStart:   "06:00:00",
		DailyEnd:     "18:00:00",
		StartAt:      now.AddDate(0, 0, -1),
	}
	winEnd := time.Date(2026, time.July, 2, 12, 0, 0, 0, time.UTC) // 1.5 days out.
	if got := projectRecordingHours(rec, now, winEnd); got != 18 {
		t.Fatalf("projectRecordingHours(continuous 12h, 1.5 days) = %v, want 18", got)
	}
}

// TestProjectRecordingHoursMidWindowNow pins that when now is mid-hour, only the
// remaining whole hours of the span count and the current in-progress hour is
// excluded (it is billed on the to-date side). now = 14:30, winEnd = midnight, so
// the projected hours are 15:00..23:00 = 9, NOT a ceiled full day.
func TestProjectRecordingHoursMidWindowNow(t *testing.T) {
	now := time.Date(2026, time.July, 1, 14, 30, 0, 0, time.UTC)
	rec := projectedRecording{
		Mode:         "sampled",
		CronExpr:     "0 * * * *",
		CronTimezone: "UTC",
		StartAt:      ptime(2026, time.July, 1, 0),
	}
	winEnd := ptime(2026, time.July, 2, 0)
	if got := projectRecordingHours(rec, now, winEnd); got != 9 {
		t.Fatalf("projectRecordingHours(hourly, mid-window now) = %v, want 9", got)
	}
}

// TestProjectRecordingHoursHourBoundaryExclusive pins that a window ending exactly
// on an hour boundary does not touch the next bucket: a continuous 09:00-12:00
// window bills hours 09,10,11 = 3, not 4.
func TestProjectRecordingHoursHourBoundaryExclusive(t *testing.T) {
	now := ptime(2026, time.July, 1, 0)
	rec := projectedRecording{
		Mode:         "continuous",
		CronTimezone: "UTC",
		DailyStart:   "09:00:00",
		DailyEnd:     "12:00:00",
		StartAt:      now.AddDate(0, 0, -1),
	}
	winEnd := now.AddDate(0, 0, 1)
	if got := projectRecordingHours(rec, now, winEnd); got != 3 {
		t.Fatalf("projectRecordingHours(continuous 09-12) = %v, want 3", got)
	}
}

// TestProjectRecordingHoursDSTSpringForward checks a continuous window on a
// spring-forward day counts the real UTC hours (the skipped local hour is not
// billed) and does not crash. On 2026-03-08 America/New_York, local 00:00-06:00
// spans the 02:00->03:00 jump, so it is 05:00-10:00 UTC = 5 distinct hours.
func TestProjectRecordingHoursDSTSpringForward(t *testing.T) {
	now := time.Date(2026, time.March, 8, 0, 0, 0, 0, time.UTC)
	rec := projectedRecording{
		Mode:         "continuous",
		CronTimezone: "America/New_York",
		DailyStart:   "00:00:00",
		DailyEnd:     "06:00:00",
		StartAt:      now.AddDate(0, 0, -1),
	}
	winEnd := time.Date(2026, time.March, 9, 0, 0, 0, 0, time.UTC)
	if got := projectRecordingHours(rec, now, winEnd); got != 5 {
		t.Fatalf("projectRecordingHours(NY spring-forward 00-06) = %v, want 5", got)
	}
}

func ptr(t time.Time) *time.Time { return &t }

// TestProjectRecordingHoursOvernight checks an OVERNIGHT continuous window
// (22:00 -> 06:00, crossing local midnight) bills the real 8 distinct UTC hours,
// not 0 or a negative span. now is set before the occurrence opens so the whole
// [22:00, 06:00 next day) falls inside the projection span, and winEnd stops
// before the following night so exactly one occurrence is counted.
func TestProjectRecordingHoursOvernight(t *testing.T) {
	now := time.Date(2026, time.June, 30, 12, 0, 0, 0, time.UTC)
	rec := projectedRecording{
		Mode:         "continuous",
		CronTimezone: "UTC",
		DailyStart:   "22:00:00",
		DailyEnd:     "06:00:00",
		StartAt:      now.AddDate(0, 0, -5),
	}
	winEnd := time.Date(2026, time.July, 1, 12, 0, 0, 0, time.UTC)
	if got := projectRecordingHours(rec, now, winEnd); got != 8 {
		t.Fatalf("projectRecordingHours(overnight 22-06) = %v, want 8", got)
	}
}

// TestProjectRecordingHoursOvernight24h checks the end==start 24h window bills 24
// distinct hours over one clamped day.
func TestProjectRecordingHoursOvernight24h(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("load loc: %v", err)
	}
	// One full local day, clamped by the capture envelope to exactly [00:00, 24:00).
	start := time.Date(2026, time.June, 15, 0, 0, 0, 0, loc).UTC()
	end := time.Date(2026, time.June, 16, 0, 0, 0, 0, loc).UTC()
	rec := projectedRecording{
		Mode:         "continuous",
		CronTimezone: "America/New_York",
		DailyStart:   "00:00:00",
		DailyEnd:     "00:00:00",
		StartAt:      start,
		EndAt:        ptr(end),
	}
	now := start.AddDate(0, 0, -1)
	winEnd := end.AddDate(0, 0, 1)
	if got := projectRecordingHours(rec, now, winEnd); got != 24 {
		t.Fatalf("projectRecordingHours(24h window) = %v, want 24", got)
	}
}

// TestProjectRecordingHoursOvernightDST checks an overnight 22:00 -> 06:00 window
// across the 2026 US spring-forward night bills 7 real UTC hours (the skipped
// local hour is not billed) and across the fall-back night bills 9 (the repeated
// local hour is a second distinct UTC hour). The capture envelope pins the span to
// exactly the one occurrence.
func TestProjectRecordingHoursOvernightDST(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("load loc: %v", err)
	}
	cases := []struct {
		name       string
		openLocal  time.Time
		closeLocal time.Time
		wantHrs    float64
	}{
		{
			// Spring forward 2026-03-08 02:00->03:00: 22:00 Mar7 -> 06:00 Mar8 = 7h.
			name:       "spring forward night",
			openLocal:  time.Date(2026, time.March, 7, 22, 0, 0, 0, loc),
			closeLocal: time.Date(2026, time.March, 8, 6, 0, 0, 0, loc),
			wantHrs:    7,
		},
		{
			// Fall back 2026-11-01 02:00->01:00: 22:00 Oct31 -> 06:00 Nov1 = 9h.
			name:       "fall back night",
			openLocal:  time.Date(2026, time.October, 31, 22, 0, 0, 0, loc),
			closeLocal: time.Date(2026, time.November, 1, 6, 0, 0, 0, loc),
			wantHrs:    9,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := projectedRecording{
				Mode:         "continuous",
				CronTimezone: "America/New_York",
				DailyStart:   "22:00:00",
				DailyEnd:     "06:00:00",
				StartAt:      c.openLocal.UTC(),
				EndAt:        ptr(c.closeLocal.UTC()),
			}
			now := c.openLocal.UTC().AddDate(0, 0, -1)
			winEnd := c.closeLocal.UTC().AddDate(0, 0, 1)
			if got := projectRecordingHours(rec, now, winEnd); got != c.wantHrs {
				t.Fatalf("projectRecordingHours(%s) = %v, want %v", c.name, got, c.wantHrs)
			}
		})
	}
}

// TestProjectRecordingHoursDaytimeRegression pins the same-day (start < end)
// path: a 09:00 -> 17:00 window over 5 whole days bills 8 distinct hours/day = 40,
// unchanged by the overnight support.
func TestProjectRecordingHoursDaytimeRegression(t *testing.T) {
	now := time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC)
	rec := projectedRecording{
		Mode:         "continuous",
		CronTimezone: "UTC",
		DailyStart:   "09:00:00",
		DailyEnd:     "17:00:00",
		StartAt:      now.AddDate(0, 0, -2),
	}
	winEnd := now.AddDate(0, 0, 5)
	if got := projectRecordingHours(rec, now, winEnd); got != 40 {
		t.Fatalf("projectRecordingHours(daytime 09-17, 5d) = %v, want 40", got)
	}
}
