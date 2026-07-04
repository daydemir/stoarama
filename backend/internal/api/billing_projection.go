package api

import (
	"time"

	"github.com/daydemir/stoarama/backend/internal/recsched"
)

// projectionMaxDays bounds the forward scan so a pathological open-ended window
// (or a bug) can never spin the projection unbounded. A Stripe billing period is
// ~1 month, so 400 days is a generous ceiling.
const projectionMaxDays = 400

// projectionMaxFires bounds the sampled cron enumeration. A one-per-minute cron
// over projectionMaxDays fires 400*1440 = 576,000 times, so 600,000 covers the
// worst valid schedule; on hitting it we return the buckets counted so far.
const projectionMaxFires = 600000

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

// projectRecordingHours computes the EXACT additional distinct record-hours a
// single active recording will bill between now and winEnd, driven by its stored
// schedule. A record-hour is a distinct UTC hour in which at least one clip
// starts (the unit recording_billing_hours counts as DISTINCT date_trunc('hour',
// clip_start_at)). Because the schedule is deterministic, the upcoming hours are
// enumerated exactly rather than approximated.
//
// The projected span is [max(now, start), min(winEnd, stop)), with projStart
// rounded UP to the next UTC hour boundary: the current in-progress hour is
// billed on the to-date side as soon as a clip lands in it, so projecting it too
// would double-count one hour. The span is then clamped to projStart +
// projectionMaxDays as an overall bound.
//
// Sampled: enumerate cron fires in the span (in the recording's timezone) and
// count the distinct UTC hour buckets they land in — clustered fires within one
// hour count once. Continuous: for each local day intersecting the span, localize
// the daily window to UTC (mirroring the scheduler exactly) and count the distinct
// UTC hour buckets with positive overlap. An unparseable cron or timezone falls
// back to the safe upper bound (every hour of the span). It is a pure display-only
// forecast: it never touches metering, Stripe, or what is charged.
func projectRecordingHours(rec projectedRecording, now, winEnd time.Time) float64 {
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
	// Exclude the current in-progress hour: it is billed to-date as soon as a clip
	// lands in it, so counting it here would double-count one hour.
	projStart = ceilHour(projStart.UTC())
	if maxEnd := projStart.AddDate(0, 0, projectionMaxDays); projEnd.After(maxEnd) {
		projEnd = maxEnd
	}
	if !projStart.Before(projEnd) {
		return 0
	}
	if rec.Mode == "continuous" {
		return float64(continuousRecordHours(rec, projStart, projEnd))
	}
	return float64(sampledRecordHours(rec, projStart, projEnd))
}

// ceilHour rounds a UTC instant up to the next whole hour boundary (an instant
// already on a boundary is unchanged). UTC hour boundaries coincide with absolute
// hour boundaries, so Truncate is exact here.
func ceilHour(t time.Time) time.Time {
	trunc := t.Truncate(time.Hour)
	if trunc.Equal(t) {
		return trunc
	}
	return trunc.Add(time.Hour)
}

// sampledRecordHours counts the distinct UTC hour buckets the stored cron fires
// into within [projStart, projEnd), evaluated in the recording's timezone using
// the same ParseCron/LoadLocation authority as the scheduler. Fires clustered in
// one hour (e.g. "0,30 9 * * *") collapse to a single record-hour. An unparseable
// cron/timezone falls back to the safe upper bound (every hour of the span).
func sampledRecordHours(rec projectedRecording, projStart, projEnd time.Time) int {
	sched, err := recsched.ParseCron(rec.CronExpr)
	if err != nil {
		return hourBucketsInSpan(projStart, projEnd)
	}
	loc, err := recsched.LoadLocation(rec.CronTimezone)
	if err != nil {
		return hourBucketsInSpan(projStart, projEnd)
	}
	buckets := make(map[int64]struct{})
	// Crons are minute-aligned, so stepping back one second includes a fire landing
	// exactly on projStart (an hour boundary) without pulling in an earlier one.
	cursor := projStart.Add(-time.Second).In(loc)
	for i := 0; i < projectionMaxFires; i++ {
		next := sched.Next(cursor)
		if next.IsZero() || !next.Before(projEnd) {
			break
		}
		buckets[next.UTC().Truncate(time.Hour).Unix()] = struct{}{}
		cursor = next
	}
	return len(buckets)
}

// continuousRecordHours counts the distinct UTC hour buckets a continuous
// recording's daily window covers within [projStart, projEnd). The daily window
// is governed by cron_timezone and its open/close instants are computed exactly as
// the scheduler does (time.Date in the recording's location, so DST is handled by
// the location math). A window ending exactly on an hour boundary does not touch
// the next bucket. An unparseable window/timezone projects 0.
func continuousRecordHours(rec projectedRecording, projStart, projEnd time.Time) int {
	start, err1 := recsched.ParseTimeOfDay(rec.DailyStart)
	end, err2 := recsched.ParseTimeOfDay(rec.DailyEnd)
	if err1 != nil || err2 != nil {
		return 0
	}
	loc, err := recsched.LoadLocation(rec.CronTimezone)
	if err != nil {
		return 0
	}
	buckets := make(map[int64]struct{})
	// An overnight window (end <= start) opens at start on one local day and closes
	// at end the NEXT local day (end == start is the 24h window); its occurrence can
	// begin the day before projStart's local day yet overlap the span, so start one
	// day earlier for overnight. Same-day windows iterate from projStart's date
	// exactly as before.
	overnight := recsched.IsOvernightWindow(start, end)
	y, mo, d := projStart.In(loc).Date()
	if overnight {
		pd := time.Date(y, mo, d, 0, 0, 0, 0, loc).AddDate(0, 0, -1)
		y, mo, d = pd.Date()
	}
	for i := 0; i < projectionMaxDays+2; i++ {
		openUTC := time.Date(y, mo, d, start.Hour, start.Minute, start.Second, 0, loc).UTC()
		if !openUTC.Before(projEnd) {
			break
		}
		var closeUTC time.Time
		if overnight {
			cd := time.Date(y, mo, d, 0, 0, 0, 0, loc).AddDate(0, 0, 1)
			cy, cmo, cdd := cd.Date()
			closeUTC = time.Date(cy, cmo, cdd, end.Hour, end.Minute, end.Second, 0, loc).UTC()
		} else {
			closeUTC = time.Date(y, mo, d, end.Hour, end.Minute, end.Second, 0, loc).UTC()
		}
		segStart := maxTime(openUTC, projStart)
		segEnd := minTime(closeUTC, projEnd)
		for b := segStart.Truncate(time.Hour); b.Before(segEnd); b = b.Add(time.Hour) {
			buckets[b.Unix()] = struct{}{}
		}
		nd := time.Date(y, mo, d, 0, 0, 0, 0, loc).AddDate(0, 0, 1)
		y, mo, d = nd.Date()
	}
	return len(buckets)
}

// hourBucketsInSpan counts the distinct UTC hour buckets a [start, end) span
// touches with positive overlap; start is assumed hour-aligned. It is the safe
// upper bound for an unparseable sampled schedule: at most one distinct hour per
// hour the window is open.
func hourBucketsInSpan(start, end time.Time) int {
	n := 0
	for b := start.Truncate(time.Hour); b.Before(end); b = b.Add(time.Hour) {
		n++
	}
	return n
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
