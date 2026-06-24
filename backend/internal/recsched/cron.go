// Package recsched parses and validates the 5-field cron expressions that drive
// the standalone stream recorder, and computes fire instants in a recording's
// IANA timezone as UTC. It is the single cron authority shared by the create
// handler, the scheduler, and (later) the autoscaler forecaster.
package recsched

import (
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

// cronProbeWindow bounds how far ahead ValidateCronForCreate walks the schedule
// when measuring the minimum gap between consecutive fires.
const cronProbeWindow = 7 * 24 * time.Hour

// ParseCron parses a standard 5-field cron expression. It rejects 6-field
// (seconds) forms and @-descriptors (@hourly, @daily, ...) so every recording
// uses the same minute-granularity grammar.
func ParseCron(expr string) (cron.Schedule, error) {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return nil, fmt.Errorf("cron expression is empty")
	}
	if strings.HasPrefix(trimmed, "@") {
		return nil, fmt.Errorf("cron descriptors (@hourly, @daily, ...) are not supported; use a 5-field expression")
	}
	if n := len(strings.Fields(trimmed)); n != 5 {
		return nil, fmt.Errorf("cron expression must have exactly 5 fields, got %d", n)
	}
	sched, err := cron.ParseStandard(trimmed)
	if err != nil {
		return nil, fmt.Errorf("invalid cron expression: %w", err)
	}
	return sched, nil
}

// LoadLocation resolves an IANA timezone name (empty defaults to UTC).
func LoadLocation(tz string) (*time.Location, error) {
	name := strings.TrimSpace(tz)
	if name == "" {
		return time.UTC, nil
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, fmt.Errorf("invalid cron_timezone %q: %w", name, err)
	}
	return loc, nil
}

// NextFireUTC returns the first fire instant strictly after `after`, evaluated
// in the recording's timezone and returned in UTC. A zero return time means the
// schedule produced no further fire (e.g. a one-shot date already in the past).
func NextFireUTC(expr, tz string, after time.Time) (time.Time, error) {
	sched, err := ParseCron(expr)
	if err != nil {
		return time.Time{}, err
	}
	loc, err := LoadLocation(tz)
	if err != nil {
		return time.Time{}, err
	}
	next := sched.Next(after.In(loc))
	if next.IsZero() {
		return time.Time{}, nil
	}
	return next.UTC(), nil
}

// ValidateCronForCreate enforces every create-time invariant for a recording's
// schedule: the expression parses, the timezone is a known IANA zone, the
// minimum gap between consecutive fires (probed over a 7-day window) is at least
// minIntervalSec, and the clip duration is shorter than that minimum gap (so a
// clip can never overrun the next fire).
func ValidateCronForCreate(expr, tz string, minIntervalSec, clipDurationSec int) error {
	if minIntervalSec <= 0 {
		return fmt.Errorf("min interval must be > 0")
	}
	if clipDurationSec <= 0 {
		return fmt.Errorf("clip duration must be > 0")
	}
	sched, err := ParseCron(expr)
	if err != nil {
		return err
	}
	loc, err := LoadLocation(tz)
	if err != nil {
		return err
	}

	minGap, err := minGapSeconds(sched, loc)
	if err != nil {
		return err
	}
	if minGap < int64(minIntervalSec) {
		return fmt.Errorf("cron fires too often: minimum gap %ds is below the allowed minimum of %ds", minGap, minIntervalSec)
	}
	if int64(clipDurationSec) >= minGap {
		return fmt.Errorf("clip duration %ds must be shorter than the minimum gap between fires (%ds)", clipDurationSec, minGap)
	}
	return nil
}

// minGapSeconds walks the schedule over a bounded probe window and returns the
// smallest gap (in whole seconds) between consecutive fires. It errors if the
// schedule never fires within the window.
func minGapSeconds(sched cron.Schedule, loc *time.Location) (int64, error) {
	start := time.Now().In(loc)
	deadline := start.Add(cronProbeWindow)
	prev := sched.Next(start)
	if prev.IsZero() || prev.After(deadline) {
		return 0, fmt.Errorf("cron schedule does not fire within the next 7 days")
	}
	var minGap int64 = -1
	cursor := prev
	for {
		next := sched.Next(cursor)
		if next.IsZero() || next.After(deadline) {
			break
		}
		gap := int64(next.Sub(cursor) / time.Second)
		if gap > 0 && (minGap < 0 || gap < minGap) {
			minGap = gap
		}
		cursor = next
	}
	if minGap < 0 {
		// Only a single fire occurred within the window; treat the gap as the
		// full probe window, which always satisfies any sane min-interval.
		return int64(cronProbeWindow / time.Second), nil
	}
	return minGap, nil
}
