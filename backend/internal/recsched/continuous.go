package recsched

import (
	"fmt"
	"time"
)

// WeekdaySet is a compact ISO-weekday mask (Monday=bit 0, Sunday=bit 6).
// Zero is invalid at the API boundary; AllWeekdays preserves pre-existing daily
// schedules and is the database default.
type WeekdaySet uint8

const AllWeekdays WeekdaySet = 0x7f

func NewWeekdaySet(days []int) (WeekdaySet, error) {
	var set WeekdaySet
	for _, day := range days {
		if day < 1 || day > 7 {
			return 0, fmt.Errorf("active_weekdays must contain ISO weekdays 1 through 7")
		}
		set |= 1 << (day - 1)
	}
	if set == 0 {
		return 0, fmt.Errorf("active_weekdays must not be empty")
	}
	return set, nil
}

func (s WeekdaySet) Contains(day time.Weekday) bool {
	iso := int(day)
	if iso == 0 {
		iso = 7
	}
	return s&(1<<(iso-1)) != 0
}

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

// IsOvernightWindow reports whether [start, end) is an OVERNIGHT continuous window
// (crosses local midnight): the window opens at start on one local day and closes
// at end the NEXT local day. This is true when end <= start; end == start is the
// 24h (full-day) window. start < end is a same-day daytime window (false). It is
// the single source of truth for the overnight predicate so the scheduler, demand
// forecast, and billing projection agree.
func IsOvernightWindow(start, end TimeOfDay) bool {
	return end.secondsOfDay() <= start.secondsOfDay()
}

// onDay localizes this time-of-day to the given calendar day in loc, returned as
// a UTC instant. y/mo/d are the calendar date in loc.
func (t TimeOfDay) onDay(y int, mo time.Month, d int, loc *time.Location) time.Time {
	return time.Date(y, mo, d, t.Hour, t.Minute, t.Second, 0, loc).UTC()
}

// ValidateContinuousWindowForCreate enforces the create-time invariants for a
// continuous recording: the segment length is sane (5..900s). Any ordering of
// start/end is accepted: start < end is a same-day daytime window; end <= start
// is an OVERNIGHT window that opens at start today and closes at end the next
// calendar day (crossing local midnight). end == start is the 24h (full-day)
// window, the useful continuous case, so it is accepted rather than rejected as a
// zero-length window. This is the continuous analogue of ValidateCronForCreate;
// the 10-minute cron floor never applies to continuous.
func ValidateContinuousWindowForCreate(start, end TimeOfDay, clipDurationSec int) error {
	if clipDurationSec < 5 || clipDurationSec > 900 {
		return fmt.Errorf("segment length %ds must be between 5 and 900", clipDurationSec)
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
	return currentOpenContinuousWindowOn(tz, start, end, AllWeekdays, envStart, envEnd, now)
}

func currentOpenContinuousWindowOn(tz string, start, end TimeOfDay, weekdays WeekdaySet, envStart, envEnd, now time.Time) (open bool, windowOpenUTC, windowEndUTC time.Time, err error) {
	loc, err := LoadLocation(tz)
	if err != nil {
		return false, time.Time{}, time.Time{}, err
	}
	localNow := now.In(loc)
	y, mo, d := localNow.Date()
	if !IsOvernightWindow(start, end) {
		// Same-day daytime window [start, end) on today's local date.
		openUTC := start.onDay(y, mo, d, loc)
		closeUTC := end.onDay(y, mo, d, loc)
		if weekdays.Contains(localNow.Weekday()) && !now.Before(openUTC) && now.Before(closeUTC) {
			return clampToEnvelope(openUTC, closeUTC, envStart, envEnd, now)
		}
		return false, time.Time{}, time.Time{}, nil
	}
	// Overnight window (end <= start): the occurrence opens at start on one local
	// day and closes at end the NEXT local day (end == start is the 24h window).
	// Two occurrences can cover `now`: the one opened today (if now >= start today)
	// and the one opened yesterday (if now < end today). Compute instants via the
	// location so DST is handled by the calendar math.
	openToday := start.onDay(y, mo, d, loc)
	if !now.Before(openToday) {
		// Opened today at start; closes at end tomorrow.
		ny, nmo, nd := nextLocalDate(y, mo, d, loc)
		closeUTC := end.onDay(ny, nmo, nd, loc)
		if weekdays.Contains(localNow.Weekday()) {
			return clampToEnvelope(openToday, closeUTC, envStart, envEnd, now)
		}
		return false, time.Time{}, time.Time{}, nil
	}
	// now is before today's open: the occurrence that opened yesterday may still be
	// open until end today.
	closeToday := end.onDay(y, mo, d, loc)
	if now.Before(closeToday) {
		py, pmo, pd := prevLocalDate(y, mo, d, loc)
		openYesterday := start.onDay(py, pmo, pd, loc)
		if weekdays.Contains(openYesterday.In(loc).Weekday()) {
			return clampToEnvelope(openYesterday, closeToday, envStart, envEnd, now)
		}
	}
	return false, time.Time{}, time.Time{}, nil
}

// clampToEnvelope returns the open/close instants for an occurrence that contains
// now, unless the capture envelope [envStart, envEnd) excludes now (envEnd zero =
// open-ended), in which case the window is reported closed.
func clampToEnvelope(openUTC, closeUTC, envStart, envEnd, now time.Time) (bool, time.Time, time.Time, error) {
	if now.Before(envStart) {
		return false, time.Time{}, time.Time{}, nil
	}
	if !envEnd.IsZero() && !now.Before(envEnd) {
		return false, time.Time{}, time.Time{}, nil
	}
	return true, openUTC, closeUTC, nil
}

// nextLocalDate / prevLocalDate return the calendar date one local day after /
// before y/mo/d in loc, using midnight arithmetic so DST transitions never skip a
// day.
func nextLocalDate(y int, mo time.Month, d int, loc *time.Location) (int, time.Month, int) {
	nd := time.Date(y, mo, d, 0, 0, 0, 0, loc).AddDate(0, 0, 1)
	ny, nmo, ndd := nd.Date()
	return ny, nmo, ndd
}

func prevLocalDate(y int, mo time.Month, d int, loc *time.Location) (int, time.Month, int) {
	pd := time.Date(y, mo, d, 0, 0, 0, 0, loc).AddDate(0, 0, -1)
	py, pmo, pdd := pd.Date()
	return py, pmo, pdd
}

// NextWindowOpenUTC returns the next daily-window open instant strictly after
// `after`, localized to tz and clamped to the capture envelope. It scans forward a
// bounded number of days. A zero return means no further window opens within the
// envelope. Used for next_fire_at display and the create-time anchored preflight.
func NextWindowOpenUTC(tz string, start TimeOfDay, envStart, envEnd, after time.Time) (time.Time, error) {
	return NextWindowOpenUTCOn(tz, start, AllWeekdays, envStart, envEnd, after)
}

func NextWindowOpenUTCOn(tz string, start TimeOfDay, weekdays WeekdaySet, envStart, envEnd, after time.Time) (time.Time, error) {
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
		if weekdays.Contains(dayOpen.In(loc).Weekday()) && dayOpen.After(after) && !dayOpen.Before(envStart) {
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
