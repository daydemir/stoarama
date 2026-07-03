package api

import (
	"math"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/recsched"
)

// projectionMaxDays bounds the per-recording day scan so a pathological
// window (or a bug) can never spin the forward projection unbounded. A Stripe
// billing period is ~1 month, so 400 days is a generous ceiling.
const projectionMaxDays = 400

// projectedRecording is the minimal per-recording input the forward projection
// needs: the stored schedule fields as they live on the recordings row. Only
// status='active' recordings are projected; the caller filters status.
type projectedRecording struct {
	Mode         string     // "sampled" | "continuous"
	CronExpr     string     // 5-field cron (sampled); "" for continuous
	CronTimezone string     // IANA zone; "" defaults to UTC
	DailyStart   string     // "HH:MM[:SS]" (continuous); "" for sampled
	DailyEnd     string     // "HH:MM[:SS]" (continuous); "" for sampled
	StartAt      time.Time  // recording capture-window start (UTC)
	EndAt        *time.Time // recording capture-window stop (UTC); nil = open-ended
}

// projectRecordingHours estimates the ADDITIONAL distinct record-hours a single
// active recording is expected to bill between now and winEnd, mirroring the
// client composer's estimate math (estRunsPerDay / estFireDaysInWindow /
// continuous window hours) but driven by the recording's STORED schedule.
//
// A record-hour is a distinct UTC hour in which the recording captures at least
// one clip (the same unit recording_billing_hours counts). Per active day a
// sampled schedule firing N times lands in at most min(N,24) distinct hours; a
// continuous daily window of H hours contributes ~H record-hours. The projection
// counts, over the days in [max(now,start), min(end,winEnd)) on which the
// schedule admits a fire, hours_per_active_day for each such day.
//
// It is a pure forecast for display only. It never touches metering, Stripe, or
// what is charged. An unparseable/custom cron mirrors the client's safe upper
// bound (assume it fires and fills its admitted hours) rather than projecting 0.
func projectRecordingHours(rec projectedRecording, now, winEnd time.Time) float64 {
	// Project only the remaining part of the billing window: from the later of
	// now / the recording's own start, to the earlier of winEnd / its own stop.
	projStart := now
	if rec.StartAt.After(projStart) {
		projStart = rec.StartAt
	}
	projEnd := winEnd
	if rec.EndAt != nil && rec.EndAt.Before(projEnd) {
		projEnd = *rec.EndAt
	}
	if !projStart.Before(projEnd) {
		return 0
	}

	hoursPerActiveDay := hoursPerActiveDay(rec)
	if hoursPerActiveDay <= 0 {
		return 0
	}
	activeDays := remainingActiveDays(rec, projStart, projEnd)
	if activeDays <= 0 {
		return 0
	}
	return hoursPerActiveDay * float64(activeDays)
}

// hoursPerActiveDay is the distinct record-hours a recording bills on a day it
// fires, mirroring the client: continuous -> the daily window length in hours;
// sampled -> min(runsPerDay, 24), where runsPerDay is the true fires-per-day of
// the stored cron (an unparseable cron falls back to the full 24, the client's
// safe upper bound).
func hoursPerActiveDay(rec projectedRecording) float64 {
	if rec.Mode == "continuous" {
		start, err1 := recsched.ParseTimeOfDay(rec.DailyStart)
		end, err2 := recsched.ParseTimeOfDay(rec.DailyEnd)
		if err1 != nil || err2 != nil {
			return 0
		}
		secs := timeOfDaySeconds(end) - timeOfDaySeconds(start)
		if secs <= 0 {
			return 0
		}
		return float64(secs) / 3600.0
	}
	runs := cronRunsPerDay(rec.CronExpr, rec.CronTimezone)
	if runs <= 0 {
		// Unparseable/empty cron: the client treats an unknown cadence as firing,
		// so fall back to the full-day upper bound rather than 0.
		return 24
	}
	if runs > 24 {
		runs = 24
	}
	return float64(runs)
}

// timeOfDaySeconds is the seconds-past-local-midnight of a parsed daily-window
// time; the recsched field is unexported, so recompute it here (display only).
func timeOfDaySeconds(t recsched.TimeOfDay) int {
	return t.Hour*3600 + t.Minute*60 + t.Second
}

// cronRunsPerDay counts the fires the stored cron produces on a day it actually
// fires, in its own timezone. It anchors on the cron's first fire (rather than a
// fixed calendar day) so a restricted cron (weekly, monthly) is measured on a day
// it runs, not a day it is dormant: a Mondays-only cron still yields its per-day
// cadence (1), not 0. It uses the same ParseCron authority as the scheduler, so
// presets match the client exactly (hourly -> 24, every 15m -> 96, every 4h -> 6,
// daily -> 1). Returns 0 only for an unparseable/never-firing cron so the caller
// can apply the safe upper bound.
func cronRunsPerDay(expr, tz string) int {
	sched, err := recsched.ParseCron(expr)
	if err != nil {
		return 0
	}
	loc, err := recsched.LoadLocation(tz)
	if err != nil {
		return 0
	}
	// Anchor on the first fire after a fixed reference instant, then count fires
	// within the 24h starting at that fire's local midnight (a full firing day).
	first := sched.Next(time.Date(2025, time.January, 1, 0, 0, 0, 0, loc))
	if first.IsZero() {
		return 0
	}
	lf := first.In(loc)
	dayStart := time.Date(lf.Year(), lf.Month(), lf.Day(), 0, 0, 0, 0, loc)
	dayEnd := dayStart.AddDate(0, 0, 1)
	count := 0
	cursor := dayStart.Add(-time.Second) // Next() is strictly after the cursor.
	for {
		next := sched.Next(cursor)
		if next.IsZero() || !next.Before(dayEnd) {
			break
		}
		count++
		cursor = next
		if count > 24*60 { // one-per-minute ceiling; guards a pathological schedule.
			break
		}
	}
	return count
}

// remainingActiveDays counts the days in [projStart, projEnd) on which the
// recording's schedule admits at least one fire, over consecutive 24h steps from
// projStart (the client's rolling-window semantics: estFireDaysInWindow counts a
// fixed number of rolling days, not calendar-aligned dates). Continuous fires
// every day the window opens; a sampled preset/wildcard cron fires every day; a
// restricted cron (weekly, monthly) fires only on admitted days, evaluated with
// the same ParseCron authority as the scheduler so day-of-week/day-of-month
// Vixie OR-semantics match. An unparseable cron is treated as firing every day
// (the client's safe upper bound).
func remainingActiveDays(rec projectedRecording, projStart, projEnd time.Time) int {
	windowDays := rollingDays(projStart, projEnd)
	if windowDays <= 0 {
		return 0
	}
	// Continuous, or a preset/wildcard/unparseable sampled cron, fires every day.
	if rec.Mode == "continuous" || firesEveryDay(rec) {
		return windowDays
	}
	sched, err := recsched.ParseCron(rec.CronExpr)
	if err != nil {
		return windowDays // unparseable cron: safe upper bound, fires every day.
	}
	// Walk consecutive 24h slices from projStart and count the slices in which the
	// schedule fires at least once. robfig's Next already implements the Vixie
	// day-of-week/day-of-month OR-semantics the client's cronDayMatches ports.
	const dayDur = 24 * time.Hour
	count := 0
	for i := 0; i < windowDays; i++ {
		sliceStart := projStart.Add(time.Duration(i) * dayDur)
		sliceEnd := sliceStart.Add(dayDur)
		if sliceEnd.After(projEnd) {
			sliceEnd = projEnd
		}
		next := sched.Next(sliceStart.Add(-time.Second))
		if !next.IsZero() && next.Before(sliceEnd) {
			count++
		}
	}
	return count
}

// firesEveryDay reports whether the sampled cron's day-of-month and day-of-week
// fields are both wildcards (so it fires every day). It mirrors the client's
// wildcard short-circuit in estFireDaysInWindow; a non-5-field or restricted
// expression returns false so the caller walks day by day.
func firesEveryDay(rec projectedRecording) bool {
	fields := strings.Fields(rec.CronExpr)
	if len(fields) != 5 {
		return false
	}
	dom := fields[2]
	dow := fields[4]
	return (dom == "*" || dom == "?") && (dow == "*" || dow == "?")
}

// rollingDays is the number of consecutive 24h steps [projStart, projEnd) spans,
// rounded up so a partial final day still counts as an active day (matching the
// client, which counts each rolling day the schedule could fire in). Bounded by
// projectionMaxDays.
func rollingDays(projStart, projEnd time.Time) int {
	span := projEnd.Sub(projStart)
	if span <= 0 {
		return 0
	}
	days := int(math.Ceil(span.Hours() / 24.0))
	if days > projectionMaxDays {
		days = projectionMaxDays
	}
	return days
}
