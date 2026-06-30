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
	mode            string
	cronExpr        string
	cronTimezone    string
	clipDurationSec int
	// dailyWindowStart/End are "HH:MM:SS" for a continuous recording, empty for a
	// sampled one. envStart/envEnd are the capture-window envelope ([start_at,end_at),
	// envEnd zero = open-ended) used to clamp continuous window occurrences.
	dailyWindowStart string
	dailyWindowEnd   string
	envStart         time.Time
	envEnd           time.Time
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

// sampledScheduleDelayMargin conservatively covers the scheduler's private
// fireJitter max (30s) when modeling sampled jobs from cron fire times. The
// forecast has no recording IDs, so it cannot reproduce per-recording jitter.
const sampledScheduleDelayMargin = 30 * time.Second

// loadCapturingRecordings reads every capturing recording (status='active',
// inside its [start_at, end_at) window, and, when billing is enabled, whose
// account has a card on file). It is the single SELECT both the autoscaler
// forecast and the create-time cap share, so the demand model has one source of
// truth (DRY). It reads, never writes, the queue.
func loadCapturingRecordings(ctx context.Context, pool *pgxpool.Pool, billingEnabled bool) ([]forecastRecording, error) {
	rows, err := pool.Query(ctx, `
		SELECT rec.mode, COALESCE(rec.cron_expr, ''), rec.cron_timezone, rec.clip_duration_sec,
		       COALESCE(to_char(rec.daily_window_start, 'HH24:MI:SS'), ''),
		       COALESCE(to_char(rec.daily_window_end, 'HH24:MI:SS'), ''),
		       rec.start_at, rec.end_at
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
		return nil, fmt.Errorf("forecast: select capturing recordings: %w", err)
	}
	defer rows.Close()
	recs := make([]forecastRecording, 0, 64)
	for rows.Next() {
		var r forecastRecording
		var envEnd *time.Time
		if err := rows.Scan(&r.mode, &r.cronExpr, &r.cronTimezone, &r.clipDurationSec,
			&r.dailyWindowStart, &r.dailyWindowEnd, &r.envStart, &envEnd); err != nil {
			return nil, fmt.Errorf("forecast: scan recording: %w", err)
		}
		if envEnd != nil {
			r.envEnd = envEnd.UTC()
		}
		recs = append(recs, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("forecast: iterate recordings: %w", err)
	}
	return recs, nil
}

// ForecastDemand loads every capturing recording and forecasts the peak
// concurrent clip count and the earliest fire in [now, now+lookahead]. It reads,
// never writes, the queue.
func ForecastDemand(ctx context.Context, pool *pgxpool.Pool, billingEnabled bool, now time.Time, lookahead time.Duration) (Forecast, error) {
	recs, err := loadCapturingRecordings(ctx, pool, billingEnabled)
	if err != nil {
		return Forecast{}, err
	}
	return forecastFromRecordings(recs, now, lookahead), nil
}

// ForecastPeakWithCandidate loads the current capturing recordings and forecasts
// the peak concurrent clip count over [now, now+lookahead] AS IF one more
// recording (the candidate being created) were already capturing. The create-time
// concurrency cap calls this and rejects a schedule whose prospective peak exceeds
// what the pool can serve (Max*Capacity). It reuses the exact sweep-line the
// autoscaler uses (forecastFromRecordings), so the cap and the scaler agree on the
// demand model (DRY). A candidate whose cron is unparseable contributes nothing to
// the peak (the create handler validates the cron separately and rejects first).
func ForecastPeakWithCandidate(ctx context.Context, pool *pgxpool.Pool, billingEnabled bool, candidateCronExpr, candidateCronTimezone string, candidateClipDurationSec int, now time.Time, lookahead time.Duration) (int, error) {
	return ForecastPeakWithCandidates(ctx, pool, billingEnabled, []ForecastCandidate{{
		CronExpr:        candidateCronExpr,
		CronTimezone:    candidateCronTimezone,
		ClipDurationSec: candidateClipDurationSec,
	}}, now, lookahead)
}

// ForecastCandidate is one prospective recording's schedule for the create-time
// capacity preflight. A bundle's N members are N identical candidates (they share
// one cron/tz/clip), so the sweep-line shows all N clips overlapping at each
// shared fire and PeakConcurrent rises by exactly N at those instants.
type ForecastCandidate struct {
	CronExpr        string
	CronTimezone    string
	ClipDurationSec int
	// Mode is 'sampled' (default; empty also treated as sampled) or 'continuous'.
	// A continuous candidate is a constant +1 slot for its daily window, so a
	// continuous bundle of N members is N identical candidates that all overlap.
	Mode             string
	DailyWindowStart string
	DailyWindowEnd   string
	EnvStart         time.Time
	EnvEnd           time.Time
}

// ForecastPeakWithCandidates loads the current capturing recordings and forecasts
// the peak concurrent clip count over [now, now+lookahead] AS IF every candidate
// were already capturing. It is the bundle-aware generalization of
// ForecastPeakWithCandidate: the whole bundle is added at once so the preflight
// can accept or reject it as a unit against the current cap, rather than emitting
// N separate decisions. It reuses the exact sweep-line the autoscaler uses
// (forecastFromRecordings), so the cap and the scaler agree on the demand model
// (DRY). A candidate whose cron is unparseable contributes nothing to the peak
// (create validates the cron separately and rejects first).
func ForecastPeakWithCandidates(ctx context.Context, pool *pgxpool.Pool, billingEnabled bool, candidates []ForecastCandidate, now time.Time, lookahead time.Duration) (int, error) {
	recs, err := loadCapturingRecordings(ctx, pool, billingEnabled)
	if err != nil {
		return 0, err
	}
	for _, c := range candidates {
		mode := c.Mode
		if mode == "" {
			mode = "sampled"
		}
		recs = append(recs, forecastRecording{
			mode:             mode,
			cronExpr:         c.CronExpr,
			cronTimezone:     c.CronTimezone,
			clipDurationSec:  c.ClipDurationSec,
			dailyWindowStart: c.DailyWindowStart,
			dailyWindowEnd:   c.DailyWindowEnd,
			envStart:         c.EnvStart,
			envEnd:           c.EnvEnd,
		})
	}
	return forecastFromRecordings(recs, now, lookahead).PeakConcurrent, nil
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

// expandRecording enumerates one recording's fires whose modeled clip intervals
// overlap [now, windowEnd], and returns the earliest future fire. Sampled clips
// are extended by sampledScheduleDelayMargin so scheduler jitter / scheduled_for
// delay cannot make close fires overlap in reality while the forecast undercounts
// them. A bounded iteration cap protects against a pathological schedule.
func expandRecording(r forecastRecording, now, windowEnd time.Time) ([]jobInterval, time.Time) {
	if r.mode == "continuous" {
		return expandContinuousRecording(r, now, windowEnd)
	}
	clip := time.Duration(r.clipDurationSec) * time.Second
	if clip <= 0 {
		clip = time.Second
	}
	modeledClip := clip + sampledScheduleDelayMargin
	out := make([]jobInterval, 0, 8)
	var first time.Time
	cursor := now.Add(-modeledClip).Add(-time.Nanosecond)
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
		end := fire.Add(modeledClip)
		if !end.After(now) {
			cursor = fire
			continue
		}
		if fire.After(now) && first.IsZero() {
			first = fire
		}
		out = append(out, jobInterval{Start: fire, End: end})
		cursor = fire
	}
	return out, first
}

// expandContinuousRecording emits ONE long interval per active daily-window
// occurrence overlapping (now, windowEnd]: a continuous stream is a constant slot
// for its whole window, not N per-clip blips. Each occurrence's [open, close) is
// localized to the recording's tz and clamped to both the lookahead and the
// capture envelope. first = the earliest window-open instant > now (drives the
// provision lead). A bounded day loop guards against pathological data.
func expandContinuousRecording(r forecastRecording, now, windowEnd time.Time) ([]jobInterval, time.Time) {
	start, errS := recsched.ParseTimeOfDay(r.dailyWindowStart)
	end, errE := recsched.ParseTimeOfDay(r.dailyWindowEnd)
	loc, errL := recsched.LoadLocation(r.cronTimezone)
	if errS != nil || errE != nil || errL != nil {
		return nil, time.Time{}
	}
	out := make([]jobInterval, 0, 4)
	var first time.Time
	// Walk each local day from now's day to windowEnd's day (lookahead is short, so
	// this is at most a couple of iterations; cap defends against bad data).
	const maxDays = 9
	day := now.In(loc)
	for i := 0; i < maxDays; i++ {
		y, mo, d := day.Date()
		openUTC := time.Date(y, mo, d, start.Hour, start.Minute, start.Second, 0, loc).UTC()
		closeUTC := time.Date(y, mo, d, end.Hour, end.Minute, end.Second, 0, loc).UTC()
		if openUTC.After(windowEnd) {
			break
		}
		// Clamp to the capture envelope.
		occOpen := openUTC
		occClose := closeUTC
		if occOpen.Before(r.envStart) {
			occOpen = r.envStart
		}
		if !r.envEnd.IsZero() && r.envEnd.Before(occClose) {
			occClose = r.envEnd
		}
		// Intersect with (now, windowEnd].
		ivStart := occOpen
		if ivStart.Before(now) {
			ivStart = now
		}
		ivEnd := occClose
		if ivEnd.After(windowEnd) {
			ivEnd = windowEnd
		}
		if ivStart.Before(ivEnd) {
			out = append(out, jobInterval{Start: ivStart, End: ivEnd})
			if openUTC.After(now) && (first.IsZero() || openUTC.Before(first)) {
				first = openUTC
			} else if first.IsZero() {
				// Occurrence already open at now: its open instant is in the past, so
				// the earliest forward fire is still this occurrence's open for lead.
				first = openUTC
			}
		}
		day = day.AddDate(0, 0, 1)
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
