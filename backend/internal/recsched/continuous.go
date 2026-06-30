package recsched

import (
	"fmt"
	"time"
)

// TimeOfDay is a wall-clock time-of-day (hour/minute/second) with no date, the
// shape of a recording's daily_window_start / daily_window_end. It is localized to
// a recording's timezone on a specific calendar day to produce a UTC instant.
type TimeOfDay struct {
	Hour   int
	Minute int
	Second int
}

// ParseTimeOfDay parses "HH:MM" or "HH:MM:SS" into a TimeOfDay.
func ParseTimeOfDay(s string) (TimeOfDay, error) {
	var h, m, sec int
	n, err := fmt.Sscanf(s, "%d:%d:%d", &h, &m, &sec)
	if err != nil || n < 2 {
		// Try HH:MM (Sscanf stops at the missing ":SS").
		if _, err2 := fmt.Sscanf(s, "%d:%d", &h, &m); err2 != nil {
			return TimeOfDay{}, fmt.Errorf("invalid time of day %q (want HH:MM)", s)
		}
		sec = 0
	}
	if h < 0 || h > 23 || m < 0 || m > 59 || sec < 0 || sec > 59 {
		return TimeOfDay{}, fmt.Errorf("time of day %q out of range", s)
	}
	return TimeOfDay{Hour: h, Minute: m, Second: sec}, nil
}

// secondsOfDay returns the number of seconds past local midnight.
func (t TimeOfDay) secondsOfDay() int {
	return t.Hour*3600 + t.Minute*60 + t.Second
}

// onDay localizes this time-of-day to the given calendar day in loc, returned as
// a UTC instant. y/mo/d are the calendar date in loc.
func (t TimeOfDay) onDay(y int, mo time.Month, d int, loc *time.Location) time.Time {
	return time.Date(y, mo, d, t.Hour, t.Minute, t.Second, 0, loc).UTC()
}

// ValidateContinuousWindowForCreate enforces the create-time invariants for a
// continuous recording: the daily window is non-empty and well-ordered (no
// midnight crossing in scope, so start must be strictly before end), and the
// segment length is sane (5..900s). This is the continuous analogue of
// ValidateCronForCreate; the 10-minute cron floor never applies to continuous.
func ValidateContinuousWindowForCreate(start, end TimeOfDay, clipDurationSec int) error {
	if clipDurationSec < 5 || clipDurationSec > 900 {
		return fmt.Errorf("segment length %ds must be between 5 and 900", clipDurationSec)
	}
	if start.secondsOfDay() >= end.secondsOfDay() {
		return fmt.Errorf("daily window end must be after start (a window crossing midnight is not supported)")
	}
	return nil
}

// currentOpenContinuousWindow returns, for now, whether a continuous recording's
// daily window is currently OPEN, and if so the open/close instants of that
// occurrence in UTC. The daily window [start, end) is localized to tz on now's
// local day and intersected with the recording's [envStart, envEnd) capture
// envelope (envEnd zero = open-ended). It is pure so the scheduler's enqueue gate
// is unit-testable.
func currentOpenContinuousWindow(tz string, start, end TimeOfDay, envStart, envEnd, now time.Time) (open bool, windowOpenUTC, windowEndUTC time.Time, err error) {
	loc, err := LoadLocation(tz)
	if err != nil {
		return false, time.Time{}, time.Time{}, err
	}
	localNow := now.In(loc)
	y, mo, d := localNow.Date()
	openUTC := start.onDay(y, mo, d, loc)
	closeUTC := end.onDay(y, mo, d, loc)
	if !now.Before(openUTC) && now.Before(closeUTC) {
		// Inside today's window occurrence; clamp to the capture envelope.
		if now.Before(envStart) {
			return false, time.Time{}, time.Time{}, nil
		}
		if !envEnd.IsZero() && !now.Before(envEnd) {
			return false, time.Time{}, time.Time{}, nil
		}
		return true, openUTC, closeUTC, nil
	}
	return false, time.Time{}, time.Time{}, nil
}

// NextWindowOpenUTC returns the next daily-window open instant strictly after
// `after`, localized to tz and clamped to the capture envelope. It scans forward a
// bounded number of days. A zero return means no further window opens within the
// envelope. Used for next_fire_at display and the create-time anchored preflight.
func NextWindowOpenUTC(tz string, start TimeOfDay, envStart, envEnd, after time.Time) (time.Time, error) {
	loc, err := LoadLocation(tz)
	if err != nil {
		return time.Time{}, err
	}
	cursor := after
	if cursor.Before(envStart) {
		cursor = envStart
	}
	localCursor := cursor.In(loc)
	y, mo, d := localCursor.Date()
	// Scan today plus up to ~370 days ahead so a window far in the future (or a
	// late envelope start) is still found, while bounding the loop.
	const maxDays = 372
	for i := 0; i < maxDays; i++ {
		dayOpen := start.onDay(y, mo, d, loc)
		if dayOpen.After(after) && !dayOpen.Before(envStart) {
			if !envEnd.IsZero() && !dayOpen.Before(envEnd) {
				return time.Time{}, nil
			}
			return dayOpen, nil
		}
		nd := time.Date(y, mo, d, 0, 0, 0, 0, loc).AddDate(0, 0, 1)
		y, mo, d = nd.Date()
	}
	return time.Time{}, nil
}
