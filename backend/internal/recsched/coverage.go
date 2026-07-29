package recsched

import (
	"errors"
	"fmt"
	"time"
)

var ErrCoverageTooLarge = errors.New("capture health range too large")

const (
	maxCoverageDays       = 36600
	maxSampledClips       = 500000
	maxExpandedClipStarts = 600000
)

// ExpectedClipCount returns the number of complete clip slots in [start, end).
// Sampled schedules count cron fires whose clips could have completed by end.
// Continuous schedules count complete segments inside localized daily windows.
func ExpectedClipCount(mode, cronExpr, timezone string, windowStart, windowEnd *TimeOfDay, weekdays WeekdaySet, clipDurationSec int, scheduleStart, start, end time.Time) (int64, error) {
	return walkExpectedClipStarts(mode, cronExpr, timezone, windowStart, windowEnd, weekdays, clipDurationSec, scheduleStart, start, end, maxSampledClips, nil)
}

// VisitExpectedClipStarts calls visit once for every complete scheduled clip
// start in [start, end), without retaining the expanded schedule.
func VisitExpectedClipStarts(mode, cronExpr, timezone string, windowStart, windowEnd *TimeOfDay, weekdays WeekdaySet, clipDurationSec int, scheduleStart, start, end time.Time, visit func(time.Time)) error {
	_, err := walkExpectedClipStarts(mode, cronExpr, timezone, windowStart, windowEnd, weekdays, clipDurationSec, scheduleStart, start, end, maxExpandedClipStarts, visit)
	return err
}

func walkExpectedClipStarts(mode, cronExpr, timezone string, windowStart, windowEnd *TimeOfDay, weekdays WeekdaySet, clipDurationSec int, scheduleStart, start, end time.Time, limit int64, yield func(time.Time)) (int64, error) {
	if clipDurationSec <= 0 {
		return 0, fmt.Errorf("clip duration must be positive")
	}
	if !start.Before(end) {
		return 0, nil
	}
	if mode == "sampled" {
		return walkExpectedSampledClipStarts(cronExpr, timezone, clipDurationSec, start, end, limit, yield)
	}
	if mode != "continuous" || windowStart == nil || windowEnd == nil {
		return 0, fmt.Errorf("invalid recording schedule shape")
	}
	return walkExpectedContinuousClipStarts(timezone, *windowStart, *windowEnd, weekdays, clipDurationSec, scheduleStart, start, end, limit, yield)
}

func walkExpectedSampledClipStarts(expr, timezone string, clipDurationSec int, start, end time.Time, limit int64, yield func(time.Time)) (int64, error) {
	schedule, err := ParseCron(expr)
	if err != nil {
		return 0, err
	}
	loc, err := LoadLocation(timezone)
	if err != nil {
		return 0, err
	}
	latestFire := end.Add(-time.Duration(clipDurationSec) * time.Second)
	if latestFire.Before(start) {
		return 0, nil
	}
	var count int64
	for fire := schedule.Next(start.Add(-time.Nanosecond).In(loc)); !fire.IsZero() && !fire.After(latestFire); fire = schedule.Next(fire) {
		count++
		if count > limit {
			return 0, fmt.Errorf("%w: exceeds %d sampled clips", ErrCoverageTooLarge, limit)
		}
		if yield != nil {
			yield(fire.UTC())
		}
	}
	return count, nil
}

func walkExpectedContinuousClipStarts(timezone string, windowStart, windowEnd TimeOfDay, weekdays WeekdaySet, clipDurationSec int, scheduleStart, start, end time.Time, limit int64, yield func(time.Time)) (int64, error) {
	loc, err := LoadLocation(timezone)
	if err != nil {
		return 0, err
	}
	if weekdays == 0 {
		weekdays = AllWeekdays
	}
	local := start.In(loc).AddDate(0, 0, -1)
	year, month, day := local.Date()
	var count int64
	for scannedDays := 0; scannedDays < maxCoverageDays; scannedDays++ {
		open := windowStart.onDay(year, month, day, loc)
		if !open.Before(end) {
			return count, nil
		}
		if weekdays.Contains(open.In(loc).Weekday()) {
			closeYear, closeMonth, closeDay := year, month, day
			if IsOvernightWindow(windowStart, windowEnd) {
				closeYear, closeMonth, closeDay = nextLocalDate(year, month, day, loc)
			}
			close := windowEnd.onDay(closeYear, closeMonth, closeDay, loc)
			segment := time.Duration(clipDurationSec) * time.Second
			anchor := maxTime(scheduleStart, open)
			first := int64(0)
			if start.After(anchor) {
				first = int64((start.Sub(anchor) + segment - 1) / segment)
			}
			last := int64(minTime(end, close).Sub(anchor) / segment)
			if last > first {
				if yield == nil {
					count += last - first
				} else {
					for slot := first; slot < last; slot++ {
						count++
						if count > limit {
							return 0, fmt.Errorf("%w: exceeds %d clips", ErrCoverageTooLarge, limit)
						}
						yield(anchor.Add(time.Duration(slot) * segment).UTC())
					}
				}
			}
		}
		next := time.Date(year, month, day, 0, 0, 0, 0, loc).AddDate(0, 0, 1)
		year, month, day = next.Date()
	}
	return 0, fmt.Errorf("%w: exceeds %d days", ErrCoverageTooLarge, maxCoverageDays)
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
