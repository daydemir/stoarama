package api

import (
	"testing"
	"time"
)

// ptime is a small helper for a fixed UTC instant in the tests.
func ptime(y int, mo time.Month, d, h int) time.Time {
	return time.Date(y, mo, d, h, 0, 0, 0, time.UTC)
}

// TestProjectRecordingHours checks the forward projection against the same
// scenarios the client composer's estimate would produce, driven by the stored
// schedule fields. now is fixed and winEnd is chosen so the window is a clean N
// days from now, so the rolling-day count is exactly N.
func TestProjectRecordingHours(t *testing.T) {
	now := ptime(2026, time.July, 1, 0) // midnight so day slices align cleanly.

	cases := []struct {
		name    string
		rec     projectedRecording
		winEnd  time.Time
		wantHrs float64
	}{
		{
			// Sampled hourly cron, open-ended, 10 days left => 24 hrs/day * 10 = 240.
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
			// Unparseable/custom cron: safe upper bound (fires every day, fills all
			// 24 admitted hours) => 24 * 10 = 240, never 0.
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

// TestCronRunsPerDay pins the fires-per-day the projection derives from the
// stored cron to the SAME values the client's estRunsPerDay preset map returns,
// which is the parity anchor between the two estimates.
func TestCronRunsPerDay(t *testing.T) {
	cases := []struct {
		expr string
		want int // client estRunsPerDay for the equivalent preset.
	}{
		{"*/15 * * * *", 96}, // every 15 min
		{"*/30 * * * *", 48}, // every 30 min
		{"0 * * * *", 24},    // hourly
		{"0 */4 * * *", 6},   // every 4 hours
		{"0 9 * * *", 1},     // daily
	}
	for _, tc := range cases {
		got := cronRunsPerDay(tc.expr, "UTC")
		if got != tc.want {
			t.Errorf("cronRunsPerDay(%q) = %d, want %d", tc.expr, got, tc.want)
		}
	}
	// An unparseable cron returns 0 so the caller applies the safe upper bound.
	if got := cronRunsPerDay("garbage", "UTC"); got != 0 {
		t.Errorf("cronRunsPerDay(garbage) = %d, want 0", got)
	}
}

// TestRemainingActiveDaysRestrictedCron checks that a restricted (weekly) cron
// projects only its firing days, matching the client's estFireDaysInWindow which
// walks the day-of-week field rather than counting every day.
func TestRemainingActiveDaysRestrictedCron(t *testing.T) {
	now := ptime(2026, time.July, 1, 0) // 2026-07-01 is a Wednesday (UTC).
	rec := projectedRecording{
		Mode:         "sampled",
		CronExpr:     "0 9 * * 1", // Mondays only at 09:00
		CronTimezone: "UTC",
		StartAt:      now.AddDate(0, 0, -1),
	}
	// A 14-day window starting Wed 2026-07-01 contains Mondays 2026-07-06 and
	// 2026-07-13 => 2 firing days.
	got := remainingActiveDays(rec, now, now.AddDate(0, 0, 14))
	if got != 2 {
		t.Fatalf("remainingActiveDays(weekly Monday, 14d) = %d, want 2", got)
	}
	// hoursPerActiveDay is 1 for a once-daily-on-Monday cron, so the projected
	// hours are 2 * 1 = 2.
	if hrs := projectRecordingHours(rec, now, now.AddDate(0, 0, 14)); hrs != 2 {
		t.Fatalf("projectRecordingHours(weekly Monday, 14d) = %v, want 2", hrs)
	}
}

func ptr(t time.Time) *time.Time { return &t }
