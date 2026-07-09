package recsched

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"
)

// Config configures the recorder cron scheduler. It runs on the dedicated
// single-instance control service, so there is no leader election.
type Config struct {
	TickInterval   time.Duration
	CatchupWindow  time.Duration
	MinIntervalSec int
	MaxJobsPerTick int
	// BillingEnabled gates capture on the account having a card on file. When false
	// (free mode / no Stripe), the gate is the open capture window alone.
	BillingEnabled bool
}

// Scheduler materializes one recording_jobs row per cron fire for every recording
// whose capture window is open (and, when billing is enabled, whose account has a
// card on file).
type Scheduler struct {
	pool *pgxpool.Pool
	cfg  Config
}

func New(pool *pgxpool.Pool, cfg Config) *Scheduler {
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = 15 * time.Second
	}
	if cfg.CatchupWindow <= 0 {
		cfg.CatchupWindow = 15 * time.Minute
	}
	if cfg.MinIntervalSec <= 0 {
		cfg.MinIntervalSec = 600
	}
	if cfg.MaxJobsPerTick <= 0 {
		cfg.MaxJobsPerTick = 500
	}
	return &Scheduler{pool: pool, cfg: cfg}
}

// Run drives the scheduler tick loop until ctx is canceled.
func (s *Scheduler) Run(ctx context.Context) error {
	log.Printf("recorder scheduler start tick=%s catchup=%s min_interval=%ds max_jobs_per_tick=%d billing_enabled=%t",
		s.cfg.TickInterval, s.cfg.CatchupWindow, s.cfg.MinIntervalSec, s.cfg.MaxJobsPerTick, s.cfg.BillingEnabled)
	ticker := time.NewTicker(s.cfg.TickInterval)
	defer ticker.Stop()
	if err := s.tick(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.tick(ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					return err
				}
				log.Printf("recorder scheduler tick error: %v", err)
			}
		}
	}
}

// tick auto-stops windows that have closed, reclaims expired leases, and enqueues
// due jobs in a single transaction.
func (s *Scheduler) tick(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin scheduler tick: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := s.autoStopExpiredRecordings(ctx, tx); err != nil {
		return err
	}
	if err := s.EnqueueDueRecordingJobs(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit scheduler tick: %w", err)
	}
	return nil
}

// autoStopExpiredRecordings enforces the window upper bound between cron ticks:
// any active recording whose end_at has passed flips to 'completed' (a terminal
// state distinct from 'canceled' = user deleted) with next_fire_at cleared, and
// its still-pending/leased jobs are canceled so the worker stops capturing it.
func (s *Scheduler) autoStopExpiredRecordings(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, `
		UPDATE recordings
		SET status='completed', next_fire_at=NULL, updated_at=now()
		WHERE status='active' AND end_at IS NOT NULL AND end_at <= now()
	`); err != nil {
		return fmt.Errorf("auto-stop expired recordings: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE recording_jobs
		SET status='canceled', updated_at=now()
		WHERE status IN ('pending','leased')
		  AND recording_id IN (SELECT id FROM recordings WHERE status='completed')
	`); err != nil {
		return fmt.Errorf("cancel jobs for completed recordings: %w", err)
	}
	return nil
}

type activeRecording struct {
	id                 int64
	mode               string
	cronExpr           *string
	cronTimezone       string
	clipDurationSec    int
	dailyWindowStart   *string
	dailyWindowEnd     *string
	startAt            time.Time
	endAt              *time.Time
	lastEnqueuedFireAt *time.Time
}

// EnqueueDueRecordingJobs is the core sweep: (a) reclaim expired leases,
// (b) per-recording forward catch-up over the catch-up window (each missed fire
// becomes its own job, capped), (c) advance last_enqueued_fire_at / next_fire_at.
// Each recording's enqueue runs in its own savepoint so one bad recording cannot
// abort the whole tick.
func (s *Scheduler) EnqueueDueRecordingJobs(ctx context.Context, tx pgx.Tx) error {
	// (a) reclaim expired leases.
	if _, err := tx.Exec(ctx, `
		UPDATE recording_jobs
		SET status='pending', lease_owner=NULL, lease_expires_at=NULL, updated_at=now()
		WHERE status='leased' AND lease_expires_at < now()
	`); err != nil {
		return fmt.Errorf("reclaim expired recording leases: %w", err)
	}

	// (a2) schedule-integrity freshness deadline: mark any pending job that can no
	// longer be captured on schedule as an HONEST miss (status='error') rather than
	// letting it be leased and recorded as a silently-wrong late clip. The window is
	// fire_at + clip_duration_sec + freshness grace, matching the lease gate's
	// predicate exactly so a job the lease query refuses is the same job marked here.
	// This only trips during a genuine transient overload / cold-boot (the autoscaler
	// keeps the common case on time and the create-time cap keeps accepted schedules
	// servable); each miss is user-visible on the recording and a signal to raise MAX.
	if err := s.markStaleJobsMissed(ctx, tx); err != nil {
		return err
	}

	// (b) select recordings whose window is open now and (when billing is enabled)
	// whose account has a card on file. Usage billing: starting a recording does not
	// charge, so the only capture gate is window + has_payment_method. A failed
	// stream writes no clip row, so a down day is simply never billed (no gate
	// change needed for "fails => free").
	rows, err := tx.Query(ctx, `
		SELECT rec.id, rec.mode, rec.cron_expr, rec.cron_timezone, rec.clip_duration_sec,
		       to_char(rec.daily_window_start, 'HH24:MI:SS'), to_char(rec.daily_window_end, 'HH24:MI:SS'),
		       rec.start_at, rec.end_at, rec.last_enqueued_fire_at
		FROM recordings rec
		WHERE rec.status='active'
		  AND rec.start_at <= now()
		  AND (rec.end_at IS NULL OR now() < rec.end_at)
		  AND ($1 OR EXISTS (
		        SELECT 1 FROM account_billing b
		        WHERE b.account_id = rec.account_id
		          AND b.has_payment_method))
		ORDER BY rec.id ASC
	`, !s.cfg.BillingEnabled)
	if err != nil {
		return fmt.Errorf("select active recordings: %w", err)
	}
	recs := make([]activeRecording, 0, 32)
	for rows.Next() {
		var rec activeRecording
		if err := rows.Scan(&rec.id, &rec.mode, &rec.cronExpr, &rec.cronTimezone, &rec.clipDurationSec,
			&rec.dailyWindowStart, &rec.dailyWindowEnd, &rec.startAt, &rec.endAt, &rec.lastEnqueuedFireAt); err != nil {
			rows.Close()
			return fmt.Errorf("scan active recording: %w", err)
		}
		recs = append(recs, rec)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate active recordings: %w", err)
	}
	rows.Close()

	now := time.Now().UTC()
	budget := s.cfg.MaxJobsPerTick
	for _, rec := range recs {
		if budget <= 0 {
			break
		}
		enqueued, err := s.enqueueRecording(ctx, tx, rec, now, budget)
		if err != nil {
			// One bad recording must not abort the whole tick (savepoint rolled back inside).
			log.Printf("recorder scheduler: recording %d enqueue skipped: %v", rec.id, err)
			continue
		}
		budget -= enqueued
	}
	return nil
}

// freshnessGraceSec is the slack added to a job's clip duration to form its
// schedule-integrity freshness window. It MUST match the API lease gate's grace
// (recordingFreshnessGraceSec) so the scheduler marks missed exactly the jobs the
// lease query refuses to hand out, never a job the worker could still capture on
// time.
const freshnessGraceSec = 30

// markStaleJobsMissed fails (status='error') every pending job past its freshness
// window and bumps the owning recording's health counters so the miss is visible
// in the recordings payload. The job moves to the terminal 'error' bucket (the
// recording_jobs CHECK allows pending/leased/done/error/canceled), distinct from a
// captured clip, so the schedule the user sees is truthful: on-time clip, or honest
// miss, never a silently-wrong on-schedule-looking clip.
func (s *Scheduler) markStaleJobsMissed(ctx context.Context, tx pgx.Tx) error {
	rows, err := tx.Query(ctx, `
		UPDATE recording_jobs
		SET status='error',
		    error_text='capacity: not captured on schedule (freshness deadline exceeded)',
		    lease_owner=NULL,
		    lease_expires_at=NULL,
		    completed_at=now(),
		    updated_at=now()
		WHERE status='pending'
		  AND kind='clip'
		  AND fire_at + make_interval(secs => (clip_duration_sec + $1)) <= now()
		RETURNING recording_id
	`, freshnessGraceSec)
	if err != nil {
		return fmt.Errorf("mark stale recording jobs missed: %w", err)
	}
	missedByRecording := make(map[int64]int)
	for rows.Next() {
		var recordingID int64
		if err := rows.Scan(&recordingID); err != nil {
			rows.Close()
			return fmt.Errorf("scan missed recording job: %w", err)
		}
		missedByRecording[recordingID]++
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate missed recording jobs: %w", err)
	}
	rows.Close()
	for recordingID, n := range missedByRecording {
		if _, err := tx.Exec(ctx, `
			UPDATE recordings
			SET consecutive_failures = consecutive_failures + $2,
			    last_error_text='capacity: not captured on schedule (freshness deadline exceeded)',
			    last_error_at=now(),
			    updated_at=now()
			WHERE id=$1
		`, recordingID, n); err != nil {
			return fmt.Errorf("bump recording health for missed jobs: %w", err)
		}
	}
	return nil
}

// enqueueRecording materializes due work for a single recording. A continuous
// recording materializes ONE window-long job per active daily-window occurrence;
// a sampled recording materializes one job per missed cron fire (the existing
// path). The 10-minute cron floor is the SAMPLED invariant and never applies to
// continuous, which validates its window instead.
func (s *Scheduler) enqueueRecording(ctx context.Context, tx pgx.Tx, rec activeRecording, now time.Time, budget int) (int, error) {
	if rec.mode == "continuous" {
		return s.enqueueContinuousRecording(ctx, tx, rec, now)
	}

	// Re-validate the clip-vs-interval invariant; skip+flag violators (S-DoS).
	if rec.cronExpr == nil {
		return 0, fmt.Errorf("sampled recording has no cron_expr")
	}
	if err := ValidateCronForCreate(*rec.cronExpr, rec.cronTimezone, s.cfg.MinIntervalSec, rec.clipDurationSec); err != nil {
		return 0, fmt.Errorf("cron invariant violated: %w", err)
	}

	if _, err := tx.Exec(ctx, `SAVEPOINT rec_enqueue`); err != nil {
		return 0, fmt.Errorf("savepoint: %w", err)
	}
	enqueued, lastFire, rollbackErr := s.enqueueRecordingFires(ctx, tx, rec, now, budget)
	if rollbackErr != nil {
		if _, err := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT rec_enqueue`); err != nil {
			return 0, fmt.Errorf("rollback to savepoint: %w", err)
		}
		_, _ = tx.Exec(ctx, `RELEASE SAVEPOINT rec_enqueue`)
		return 0, rollbackErr
	}
	if _, err := tx.Exec(ctx, `RELEASE SAVEPOINT rec_enqueue`); err != nil {
		return 0, fmt.Errorf("release savepoint: %w", err)
	}

	// (c) advance the cursor and next_fire_at outside the per-fire loop.
	nextFire, err := NextFireUTC(*rec.cronExpr, rec.cronTimezone, now)
	if err != nil {
		return enqueued, fmt.Errorf("compute next fire: %w", err)
	}
	var nextFireArg any
	if !nextFire.IsZero() {
		nextFireArg = nextFire
	}
	var lastFireArg any
	if !lastFire.IsZero() {
		lastFireArg = lastFire
	}
	if _, err := tx.Exec(ctx, `
		UPDATE recordings
		SET last_enqueued_fire_at = GREATEST(COALESCE(last_enqueued_fire_at, 'epoch'::timestamptz), COALESCE($2::timestamptz, COALESCE(last_enqueued_fire_at, 'epoch'::timestamptz))),
		    next_fire_at = $3,
		    updated_at = now()
		WHERE id=$1
	`, rec.id, lastFireArg, nextFireArg); err != nil {
		return enqueued, fmt.Errorf("advance recording cursor: %w", err)
	}
	return enqueued, nil
}

// enqueueContinuousRecording materializes ONE continuous_window job for the
// currently-open daily-window occurrence (if any) of a continuous recording. The
// job is idempotent on the window-open instant (idem key reccont:<id>:<open_unix>),
// so re-running the tick inside the same window is a no-op. When the window is not
// open, nothing is enqueued. next_fire_at advances to the next window-open instant
// for display. It does NOT call ValidateCronForCreate (no cron floor for continuous).
func (s *Scheduler) enqueueContinuousRecording(ctx context.Context, tx pgx.Tx, rec activeRecording, now time.Time) (int, error) {
	if rec.dailyWindowStart == nil || rec.dailyWindowEnd == nil {
		return 0, fmt.Errorf("continuous recording has no daily window")
	}
	start, err := ParseTimeOfDay(*rec.dailyWindowStart)
	if err != nil {
		return 0, err
	}
	end, err := ParseTimeOfDay(*rec.dailyWindowEnd)
	if err != nil {
		return 0, err
	}
	var envEnd time.Time
	if rec.endAt != nil {
		envEnd = rec.endAt.UTC()
	}
	open, windowOpenUTC, windowEndUTC, err := currentOpenContinuousWindow(rec.cronTimezone, start, end, rec.startAt.UTC(), envEnd, now)
	if err != nil {
		return 0, err
	}

	enqueued := 0
	var lastEnqueuedArg any
	if open {
		// Clamp the window end to the capture envelope upper bound so a window
		// straddling end_at stops the worker at end_at (autoStop also cancels it).
		effectiveEnd := windowEndUTC
		if !envEnd.IsZero() && envEnd.Before(effectiveEnd) {
			effectiveEnd = envEnd
		}
		idemKey := fmt.Sprintf("reccont:%d:%d", rec.id, windowOpenUTC.Unix())
		ct, err := tx.Exec(ctx, `
			UPDATE recording_jobs j
			SET status='pending',
			    scheduled_for=now(),
			    clip_duration_sec=$2,
			    lease_owner=NULL,
			    lease_expires_at=NULL,
			    attempt_count=0,
			    error_text='',
			    completed_at=NULL,
			    window_end_at=$3,
			    updated_at=now()
			WHERE j.recording_id=$1
			  AND j.idempotency_key=$4
			  AND j.kind='continuous_window'
			  AND j.status='done'
			  AND NOT EXISTS (SELECT 1 FROM recording_clips c WHERE c.recording_job_id=j.id)
		`, rec.id, rec.clipDurationSec, effectiveEnd, idemKey)
		if err != nil {
			return 0, fmt.Errorf("revive zero-clip continuous window job: %w", err)
		}
		if ct.RowsAffected() == 1 {
			enqueued = 1
		}
		ct, err = tx.Exec(ctx, `
			INSERT INTO recording_jobs (recording_id, fire_at, scheduled_for, clip_duration_sec, status, idempotency_key, kind, window_end_at)
			VALUES ($1, $2, $2, $3, 'pending', $4, 'continuous_window', $5)
			ON CONFLICT (idempotency_key) DO NOTHING
		`, rec.id, windowOpenUTC, rec.clipDurationSec, idemKey, effectiveEnd)
		if err != nil {
			return 0, fmt.Errorf("insert continuous window job: %w", err)
		}
		if ct.RowsAffected() == 1 {
			enqueued = 1
		}
		lastEnqueuedArg = windowOpenUTC
	}

	// next_fire_at = the next window-open instant (display + provision hint).
	nextOpen, err := NextWindowOpenUTC(rec.cronTimezone, start, rec.startAt.UTC(), envEnd, now)
	if err != nil {
		return enqueued, err
	}
	var nextFireArg any
	if !nextOpen.IsZero() {
		nextFireArg = nextOpen
	}
	if _, err := tx.Exec(ctx, `
		UPDATE recordings
		SET last_enqueued_fire_at = GREATEST(COALESCE(last_enqueued_fire_at, 'epoch'::timestamptz), COALESCE($2::timestamptz, COALESCE(last_enqueued_fire_at, 'epoch'::timestamptz))),
		    next_fire_at = $3,
		    updated_at = now()
		WHERE id=$1
	`, rec.id, lastEnqueuedArg, nextFireArg); err != nil {
		return enqueued, fmt.Errorf("advance continuous recording cursor: %w", err)
	}
	return enqueued, nil
}

// enqueueRecordingFires materializes one recording_jobs row per missed cron fire,
// idempotently. cursor = last_enqueued_fire_at, floored to now-CatchupWindow when
// the recording has been idle longer than the window. The set of fire instants is
// computed by the pure dueFireInstants helper; this method only performs the
// inserts and tracks how many new rows were written against the per-tick budget.
func (s *Scheduler) enqueueRecordingFires(ctx context.Context, tx pgx.Tx, rec activeRecording, now time.Time, budget int) (int, time.Time, error) {
	sched, err := ParseCron(*rec.cronExpr)
	if err != nil {
		return 0, time.Time{}, err
	}
	loc, err := LoadLocation(rec.cronTimezone)
	if err != nil {
		return 0, time.Time{}, err
	}

	cursor := catchupCursor(rec.lastEnqueuedFireAt, now, s.cfg.CatchupWindow)
	instants := dueFireInstants(sched, loc, cursor, now, catchupInstantCap(s.cfg.CatchupWindow, s.cfg.MinIntervalSec))

	jitter := fireJitter(rec.id, s.cfg.MinIntervalSec)
	enqueued := 0
	var lastFire time.Time
	for _, fireUTC := range instants {
		if enqueued >= budget {
			break
		}
		// fire_at is the exact cron instant (minute-aligned): it keys the idempotency
		// key (recjob:id:floor(epoch(fire_at))), the clip object key, and the freshness
		// deadline, so it must NOT carry jitter. scheduled_for is the leasable-at
		// instant: jittering it by a small deterministic per-recording offset breaks
		// the :00/:30 thundering herd (many same-cadence recordings no longer become
		// leasable on the same instant) without shifting the user-facing cadence.
		idemKey := fmt.Sprintf("recjob:%d:%d", rec.id, fireUTC.Unix())
		scheduledFor := fireUTC.Add(jitter)
		ct, err := tx.Exec(ctx, `
			INSERT INTO recording_jobs (recording_id, fire_at, scheduled_for, clip_duration_sec, status, idempotency_key)
			VALUES ($1, $2, $3, $4, 'pending', $5)
			ON CONFLICT (idempotency_key) DO NOTHING
		`, rec.id, fireUTC, scheduledFor, rec.clipDurationSec, idemKey)
		if err != nil {
			return enqueued, lastFire, fmt.Errorf("insert recording job: %w", err)
		}
		if ct.RowsAffected() == 1 {
			enqueued++
		}
		lastFire = fireUTC
	}
	return enqueued, lastFire, nil
}

// fireJitter returns a deterministic per-recording leasable-at offset that spreads
// same-cadence recordings off the exact cron boundary (the :00/:30 thundering
// herd). The span is small and bounded relative to the fire gap (at most 30s, and
// never more than minIntervalSec/20 so it stays cosmetically invisible on a fast
// cadence), so jitter alone never pushes a capture meaningfully off its expected
// time. It is a pure function of the recording id, so it is stable across restarts
// and never drifts (no randomness): the same fire always lands at the same
// scheduled_for, keeping reclaim/retry idempotent.
func fireJitter(recordingID int64, minIntervalSec int) time.Duration {
	span := 30
	if minIntervalSec > 0 && minIntervalSec/20 < span {
		span = minIntervalSec / 20
	}
	if span < 1 {
		return 0
	}
	offset := recordingID % int64(span)
	if offset < 0 {
		offset = -offset
	}
	return time.Duration(offset) * time.Second
}

// catchupCursor returns the lower bound (exclusive) of the catch-up window for a
// recording: its last enqueued fire instant, floored to now-window when it has
// been idle longer than the window (or never enqueued).
func catchupCursor(lastEnqueuedFireAt *time.Time, now time.Time, window time.Duration) time.Time {
	cursor := now.Add(-window)
	if lastEnqueuedFireAt != nil && lastEnqueuedFireAt.After(cursor) {
		cursor = lastEnqueuedFireAt.UTC()
	}
	return cursor
}

// catchupInstantCap bounds the number of fires materialized for one recording in
// a single tick to CatchupWindow/MinIntervalSec (at least 1). Because every valid
// recording fires no more often than MinIntervalSec (enforced at create and
// re-validated at enqueue), this cap always covers a full window's worth of fires
// while bounding per-recording work if a malformed schedule slips through.
func catchupInstantCap(window time.Duration, minIntervalSec int) int {
	if minIntervalSec <= 0 {
		return 1
	}
	n := int(window/time.Second) / minIntervalSec
	if n < 1 {
		n = 1
	}
	return n
}

// dueFireInstants returns, in ascending UTC order, every cron fire instant in the
// half-open interval (cursor, now], evaluated in loc, capped at maxInstants. It is
// pure (no DB) so the catch-up enumeration is unit-testable in isolation.
func dueFireInstants(sched cron.Schedule, loc *time.Location, cursor, now time.Time, maxInstants int) []time.Time {
	if maxInstants < 1 {
		return nil
	}
	out := make([]time.Time, 0, maxInstants)
	fire := sched.Next(cursor.In(loc))
	for !fire.IsZero() {
		fireUTC := fire.UTC()
		if fireUTC.After(now) {
			break
		}
		out = append(out, fireUTC)
		if len(out) >= maxInstants {
			break
		}
		fire = sched.Next(fire)
	}
	return out
}
