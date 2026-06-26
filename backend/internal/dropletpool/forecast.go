package dropletpool

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/recsched"
)

// forecastRecording is one capturing recording's schedule, loaded for the demand
// forecast (status='active', window open now, card on file when billing is on).
type forecastRecording struct {
	cronExpr        string
	cronTimezone    string
	clipDurationSec int
}

// Forecast is the demand-forecast result over a lookahead window.
type Forecast struct {
	// PeakConcurrent is the maximum number of clip jobs that overlap at any instant
	// in [now, now+lookahead], modeling each cron fire as a job of clip_duration.
	PeakConcurrent int
	// NextFireAt is the earliest fire instant in the window (zero if none), used to
	// decide whether to provision ahead of demand within the provision lead.
	NextFireAt time.Time
}

// jobInterval models one scheduled clip as the half-open instant range
// [Start, End) during which a droplet slot is occupied.
type jobInterval struct {
	Start time.Time
	End   time.Time
}

// ForecastDemand loads every capturing recording (status='active', inside its
// [start_at, end_at) window, and, when billing is enabled, whose account has a
// card on file) and forecasts the peak concurrent clip count and the earliest
// fire in [now, now+lookahead]. It reads, never writes, the queue.
func ForecastDemand(ctx context.Context, pool *pgxpool.Pool, billingEnabled bool, now time.Time, lookahead time.Duration) (Forecast, error) {
	rows, err := pool.Query(ctx, `
		SELECT rec.cron_expr, rec.cron_timezone, rec.clip_duration_sec
		FROM recordings rec
		WHERE rec.status='active'
		  AND rec.start_at <= now()
		  AND (rec.end_at IS NULL OR now() < rec.end_at)
		  AND ($1 OR EXISTS (
		        SELECT 1 FROM account_billing b
		        WHERE b.account_id = rec.account_id
		          AND b.has_payment_method))
	`, !billingEnabled)
	if err != nil {
		return Forecast{}, fmt.Errorf("forecast: select capturing recordings: %w", err)
	}
	defer rows.Close()
	recs := make([]forecastRecording, 0, 64)
	for rows.Next() {
		var r forecastRecording
		if err := rows.Scan(&r.cronExpr, &r.cronTimezone, &r.clipDurationSec); err != nil {
			return Forecast{}, fmt.Errorf("forecast: scan recording: %w", err)
		}
		recs = append(recs, r)
	}
	if err := rows.Err(); err != nil {
		return Forecast{}, fmt.Errorf("forecast: iterate recordings: %w", err)
	}
	return forecastFromRecordings(recs, now, lookahead), nil
}

// forecastFromRecordings expands each recording's cron over the lookahead window
// into clip intervals and sweep-lines them for peak concurrency. A recording
// whose cron fails to parse is skipped (it can never have been created, but the
// forecast must never panic on bad data).
func forecastFromRecordings(recs []forecastRecording, now time.Time, lookahead time.Duration) Forecast {
	if lookahead <= 0 {
		return Forecast{}
	}
	windowEnd := now.Add(lookahead)
	intervals := make([]jobInterval, 0, len(recs)*4)
	var nextFire time.Time
	for _, r := range recs {
		ivals, first := expandRecording(r, now, windowEnd)
		intervals = append(intervals, ivals...)
		if !first.IsZero() && (nextFire.IsZero() || first.Before(nextFire)) {
			nextFire = first
		}
	}
	return Forecast{
		PeakConcurrent: peakConcurrency(intervals),
		NextFireAt:     nextFire,
	}
}

// expandRecording enumerates one recording's fires in (now, windowEnd], modeling
// each as a [fire, fire+clip_duration) interval, and returns the earliest fire.
// A fire that starts before windowEnd but whose clip extends past it still counts
// (the slot is occupied at windowEnd). A bounded iteration cap protects against a
// pathological schedule.
func expandRecording(r forecastRecording, now, windowEnd time.Time) ([]jobInterval, time.Time) {
	clip := time.Duration(r.clipDurationSec) * time.Second
	if clip <= 0 {
		clip = time.Second
	}
	out := make([]jobInterval, 0, 8)
	var first time.Time
	cursor := now
	// Hard cap on fires per recording per window so a malformed schedule cannot
	// blow up the sweep. A valid recording fires no more often than the configured
	// min interval, so this is never hit in practice.
	const maxFiresPerRecording = 10000
	for i := 0; i < maxFiresPerRecording; i++ {
		fire, err := recsched.NextFireUTC(r.cronExpr, r.cronTimezone, cursor)
		if err != nil || fire.IsZero() {
			break
		}
		if fire.After(windowEnd) {
			break
		}
		if first.IsZero() {
			first = fire
		}
		out = append(out, jobInterval{Start: fire, End: fire.Add(clip)})
		cursor = fire
	}
	return out, first
}

// peakConcurrency returns the maximum number of overlapping intervals via a
// sweep line over start (+1) and end (-1) events. End events at the same instant
// as start events are processed first (half-open intervals: a job ending exactly
// when another starts does not overlap), so the count reflects true concurrency.
func peakConcurrency(intervals []jobInterval) int {
	if len(intervals) == 0 {
		return 0
	}
	type event struct {
		at    time.Time
		delta int
	}
	events := make([]event, 0, len(intervals)*2)
	for _, iv := range intervals {
		events = append(events, event{at: iv.Start, delta: 1})
		events = append(events, event{at: iv.End, delta: -1})
	}
	sort.Slice(events, func(i, j int) bool {
		if !events[i].at.Equal(events[j].at) {
			return events[i].at.Before(events[j].at)
		}
		// Process ends (-1) before starts (+1) at the same instant.
		return events[i].delta < events[j].delta
	})
	cur, peak := 0, 0
	for _, e := range events {
		cur += e.delta
		if cur > peak {
			peak = cur
		}
	}
	return peak
}

// RequiredDroplets converts a forecast peak concurrency into a droplet count,
// dividing by per-droplet capacity (ceil) and clamping to [min, max]. The max is
// the hard spend cap. This is pure so the spend-cap clamp is unit-tested in
// isolation.
func RequiredDroplets(peakConcurrent, capacity, min, max int) int {
	if capacity <= 0 {
		capacity = 1
	}
	required := 0
	if peakConcurrent > 0 {
		required = (peakConcurrent + capacity - 1) / capacity
	}
	if required < min {
		required = min
	}
	if required > max {
		required = max
	}
	return required
}
