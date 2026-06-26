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
	// BillingEnabled gates capture on the billable predicate. When false (free
	// mode / no Stripe), the gate is status='active' alone.
	BillingEnabled bool
}

// Scheduler materializes one recording_jobs row per cron fire for every active
// (and, when billing is enabled, billable) recording.
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

// tick reclaims expired leases and enqueues due jobs in a single transaction.
func (s *Scheduler) tick(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin scheduler tick: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := s.EnqueueDueRecordingJobs(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit scheduler tick: %w", err)
	}
	return nil
}

type activeRecording struct {
	id                 int64
	cronExpr           string
	cronTimezone       string
	clipDurationSec    int
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

	// (b) select active (+ billable when enabled) recordings.
	rows, err := tx.Query(ctx, `
		SELECT rec.id, rec.cron_expr, rec.cron_timezone, rec.clip_duration_sec, rec.last_enqueued_fire_at
		FROM recordings rec
		WHERE rec.status='active'
		  AND ($1 OR EXISTS (
		        SELECT 1 FROM account_billing b
		        WHERE b.account_id = rec.account_id
		          AND b.subscription_status IN ('active','trialing','past_due')
		          AND (b.subscription_status <> 'past_due' OR b.current_period_end > now())
		          AND (SELECT count(*) FROM recordings r2
		                 WHERE r2.account_id = rec.account_id AND r2.status = 'active'
		                   AND (r2.created_at, r2.id) <= (rec.created_at, rec.id)) <= b.paid_quantity))
		ORDER BY rec.id ASC
	`, !s.cfg.BillingEnabled)
	if err != nil {
		return fmt.Errorf("select active recordings: %w", err)
	}
	recs := make([]activeRecording, 0, 32)
	for rows.Next() {
		var rec activeRecording
		if err := rows.Scan(&rec.id, &rec.cronExpr, &rec.cronTimezone, &rec.clipDurationSec, &rec.lastEnqueuedFireAt); err != nil {
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

// enqueueRecording materializes every missed fire for a single recording inside
// its own savepoint, capped by the catch-up window and the remaining tick budget.
func (s *Scheduler) enqueueRecording(ctx context.Context, tx pgx.Tx, rec activeRecording, now time.Time, budget int) (int, error) {
	// Re-validate the clip-vs-interval invariant; skip+flag violators (S-DoS).
	if err := ValidateCronForCreate(rec.cronExpr, rec.cronTimezone, s.cfg.MinIntervalSec, rec.clipDurationSec); err != nil {
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
	nextFire, err := NextFireUTC(rec.cronExpr, rec.cronTimezone, now)
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

// enqueueRecordingFires materializes one recording_jobs row per missed cron fire,
// idempotently. cursor = last_enqueued_fire_at, floored to now-CatchupWindow when
// the recording has been idle longer than the window. The set of fire instants is
// computed by the pure dueFireInstants helper; this method only performs the
// inserts and tracks how many new rows were written against the per-tick budget.
func (s *Scheduler) enqueueRecordingFires(ctx context.Context, tx pgx.Tx, rec activeRecording, now time.Time, budget int) (int, time.Time, error) {
	sched, err := ParseCron(rec.cronExpr)
	if err != nil {
		return 0, time.Time{}, err
	}
	loc, err := LoadLocation(rec.cronTimezone)
	if err != nil {
		return 0, time.Time{}, err
	}

	cursor := catchupCursor(rec.lastEnqueuedFireAt, now, s.cfg.CatchupWindow)
	instants := dueFireInstants(sched, loc, cursor, now, catchupInstantCap(s.cfg.CatchupWindow, s.cfg.MinIntervalSec))

	enqueued := 0
	var lastFire time.Time
	for _, fireUTC := range instants {
		if enqueued >= budget {
			break
		}
		idemKey := fmt.Sprintf("recjob:%d:%d", rec.id, fireUTC.Unix())
		ct, err := tx.Exec(ctx, `
			INSERT INTO recording_jobs (recording_id, fire_at, scheduled_for, clip_duration_sec, status, idempotency_key)
			VALUES ($1, $2, $2, $3, 'pending', $4)
			ON CONFLICT (idempotency_key) DO NOTHING
		`, rec.id, fireUTC, rec.clipDurationSec, idemKey)
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
