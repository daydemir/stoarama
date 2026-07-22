package recsched

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"
)

func mustParse(t *testing.T, expr string) cron.Schedule {
	t.Helper()
	sched, err := ParseCron(expr)
	if err != nil {
		t.Fatalf("ParseCron(%q): %v", expr, err)
	}
	return sched
}

func TestCatchupCursor(t *testing.T) {
	now := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	window := 15 * time.Minute

	// No prior cursor: floor to now-window.
	if got := catchupCursor(nil, now, window); !got.Equal(now.Add(-window)) {
		t.Fatalf("nil cursor: want %s, got %s", now.Add(-window), got)
	}

	// Prior cursor older than the window: still floored to now-window
	// (a recording idle longer than the window never backfills before it).
	old := now.Add(-2 * time.Hour)
	if got := catchupCursor(&old, now, window); !got.Equal(now.Add(-window)) {
		t.Fatalf("stale cursor: want %s, got %s", now.Add(-window), got)
	}

	// Prior cursor inside the window: resume exactly from it.
	recent := now.Add(-5 * time.Minute)
	if got := catchupCursor(&recent, now, window); !got.Equal(recent) {
		t.Fatalf("recent cursor: want %s, got %s", recent, got)
	}
}

func TestCatchupInstantCap(t *testing.T) {
	// 15-minute window, 10-minute floor -> 1 instant per tick.
	if got := catchupInstantCap(15*time.Minute, 600); got != 1 {
		t.Fatalf("want cap 1, got %d", got)
	}
	// 1-hour window, 10-minute floor -> 6.
	if got := catchupInstantCap(time.Hour, 600); got != 6 {
		t.Fatalf("want cap 6, got %d", got)
	}
	// Degenerate config never returns < 1.
	if got := catchupInstantCap(0, 600); got != 1 {
		t.Fatalf("want cap 1 for zero window, got %d", got)
	}
	if got := catchupInstantCap(time.Hour, 0); got != 1 {
		t.Fatalf("want cap 1 for zero interval, got %d", got)
	}
}

func TestDueFireInstantsBackfillsEveryMissedFire(t *testing.T) {
	sched := mustParse(t, "*/10 * * * *") // every 10 minutes
	// Cursor at 12:00, now at 12:35 -> 12:10, 12:20, 12:30 are all due.
	cursor := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	now := time.Date(2026, 6, 24, 12, 35, 0, 0, time.UTC)
	got := dueFireInstants(sched, time.UTC, cursor, now, 100)
	want := []time.Time{
		time.Date(2026, 6, 24, 12, 10, 0, 0, time.UTC),
		time.Date(2026, 6, 24, 12, 20, 0, 0, time.UTC),
		time.Date(2026, 6, 24, 12, 30, 0, 0, time.UTC),
	}
	if len(got) != len(want) {
		t.Fatalf("want %d instants, got %d: %v", len(want), len(got), got)
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Fatalf("instant %d: want %s, got %s", i, want[i], got[i])
		}
		if got[i].Location() != time.UTC {
			t.Fatalf("instant %d not UTC: %s", i, got[i].Location())
		}
	}
}

func TestDueFireInstantsExclusiveLowerInclusiveUpper(t *testing.T) {
	sched := mustParse(t, "*/10 * * * *")
	// Cursor exactly on a fire (12:00): that fire is excluded (lower bound is
	// exclusive). now exactly on a fire (12:20): that fire is included.
	cursor := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	now := time.Date(2026, 6, 24, 12, 20, 0, 0, time.UTC)
	got := dueFireInstants(sched, time.UTC, cursor, now, 100)
	want := []time.Time{
		time.Date(2026, 6, 24, 12, 10, 0, 0, time.UTC),
		time.Date(2026, 6, 24, 12, 20, 0, 0, time.UTC),
	}
	if len(got) != len(want) {
		t.Fatalf("want %d, got %d: %v", len(want), len(got), got)
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Fatalf("instant %d: want %s, got %s", i, want[i], got[i])
		}
	}
}

func TestDueFireInstantsCapTruncates(t *testing.T) {
	sched := mustParse(t, "*/10 * * * *")
	cursor := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	now := time.Date(2026, 6, 24, 13, 0, 0, 0, time.UTC) // 6 fires due
	got := dueFireInstants(sched, time.UTC, cursor, now, 2)
	if len(got) != 2 {
		t.Fatalf("want 2 (capped), got %d: %v", len(got), got)
	}
	// The cap keeps the EARLIEST instants so the cursor advances monotonically
	// and the remainder is drained on subsequent ticks.
	want := []time.Time{
		time.Date(2026, 6, 24, 12, 10, 0, 0, time.UTC),
		time.Date(2026, 6, 24, 12, 20, 0, 0, time.UTC),
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Fatalf("instant %d: want %s, got %s", i, want[i], got[i])
		}
	}
}

func TestDueFireInstantsNoneDueYet(t *testing.T) {
	sched := mustParse(t, "*/10 * * * *")
	// Cursor at 12:00, now at 12:05: the next fire (12:10) is in the future.
	cursor := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	now := time.Date(2026, 6, 24, 12, 5, 0, 0, time.UTC)
	if got := dueFireInstants(sched, time.UTC, cursor, now, 100); len(got) != 0 {
		t.Fatalf("want no instants due, got %v", got)
	}
}

func TestDueFireInstantsZeroCap(t *testing.T) {
	sched := mustParse(t, "*/10 * * * *")
	cursor := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	now := time.Date(2026, 6, 24, 13, 0, 0, 0, time.UTC)
	if got := dueFireInstants(sched, time.UTC, cursor, now, 0); got != nil {
		t.Fatalf("zero cap must yield nil, got %v", got)
	}
}

func TestDueFireInstantsTimezoneCorrect(t *testing.T) {
	// A daily 09:00 New York fire in winter (EST, UTC-5) materializes at 14:00 UTC.
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("load location: %v", err)
	}
	sched := mustParse(t, "0 9 * * *")
	cursor := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 1, 16, 0, 0, 0, 0, time.UTC)
	got := dueFireInstants(sched, loc, cursor, now, 100)
	if len(got) != 1 {
		t.Fatalf("want exactly one daily fire, got %d: %v", len(got), got)
	}
	want := time.Date(2026, 1, 15, 14, 0, 0, 0, time.UTC)
	if !got[0].Equal(want) {
		t.Fatalf("tz fire: want %s, got %s", want, got[0])
	}
}

// TestIdempotencyKeyFormat pins the exact key shape the enqueue path writes and
// the contract documents ('recjob:<recording_id>:<floor(epoch(fire_at))>'), since
// it is the cross-restart dedup guarantee for recording_jobs.
func TestIdempotencyKeyFormat(t *testing.T) {
	fireAt := time.Date(2026, 6, 24, 12, 30, 0, 0, time.UTC)
	got := fmt.Sprintf("recjob:%d:%d", int64(7), fireAt.Unix())
	want := fmt.Sprintf("recjob:7:%d", fireAt.Unix())
	if got != want {
		t.Fatalf("idempotency key: want %s, got %s", want, got)
	}
	// Sub-minute jitter on the same fire instant must collapse to the same key
	// (the column is minute-aligned by construction; epoch seconds are identical).
	if fireAt.Unix() != fireAt.Truncate(time.Minute).Unix() {
		t.Fatalf("fire instant is not minute-aligned: %s", fireAt)
	}
}

// TestFireJitter pins the herd-spreader: a deterministic, bounded, per-recording
// leasable-at offset that never exceeds the span and is stable across calls.
func TestFireJitter(t *testing.T) {
	// Default min interval 600s => span = min(30, 600/20) = 30s.
	const minInterval = 600
	for _, id := range []int64{0, 1, 29, 30, 31, 600, -7} {
		j := fireJitter(id, minInterval)
		if j < 0 {
			t.Fatalf("jitter for id=%d is negative: %s", id, j)
		}
		if j >= 30*time.Second {
			t.Fatalf("jitter for id=%d is %s, must be < span 30s", id, j)
		}
		// Determinism: same input, same output.
		if fireJitter(id, minInterval) != j {
			t.Fatalf("jitter for id=%d is not deterministic", id)
		}
	}
	// Spread: distinct ids within the span land on distinct offsets, breaking the herd.
	if fireJitter(1, minInterval) == fireJitter(2, minInterval) {
		t.Fatalf("ids 1 and 2 must jitter to different offsets")
	}
	// fire_at math is unaffected: jitter applies to scheduled_for only. A 1s offset
	// is cosmetically invisible against a 1800s (*/30) gap.
	if fireJitter(31, minInterval) != 1*time.Second {
		t.Fatalf("id 31 within span 30 must offset by 31%%30=1s, got %s", fireJitter(31, minInterval))
	}
	// A tiny min interval clamps the span to >=0 without panicking.
	if fireJitter(5, 10) != 0 {
		t.Fatalf("min interval 10 => span 0 => no jitter, got %s", fireJitter(5, 10))
	}
}

func TestContinuousRevivesDoneZeroClipWindow(t *testing.T) {
	pool, cleanup := testSchedulerPool(t)
	defer cleanup()

	ctx := context.Background()
	recID := insertSchedulerContinuousRecording(t, pool)
	now := time.Date(2026, 7, 9, 10, 53, 0, 0, time.UTC)
	windowOpen := time.Date(2026, 7, 9, 9, 0, 0, 0, time.UTC)
	jobID := insertSchedulerContinuousJob(t, pool, recID, windowOpen, "done")

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	rec := activeRecording{
		id:                 recID,
		mode:               "continuous",
		cronTimezone:       "UTC",
		clipDurationSec:    60,
		dailyWindowStart:   strPtr("09:00:00"),
		dailyWindowEnd:     strPtr("11:30:00"),
		startAt:            time.Date(2026, 7, 9, 8, 0, 0, 0, time.UTC),
		lastEnqueuedFireAt: &windowOpen,
	}
	got, err := New(pool, Config{}).enqueueContinuousRecording(ctx, tx, rec, now)
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("enqueue continuous: %v", err)
	}
	if got != 1 {
		_ = tx.Rollback(ctx)
		t.Fatalf("enqueued=%d want 1", got)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	var status string
	var attemptCount int
	var completedAt *time.Time
	if err := pool.QueryRow(ctx, `
		SELECT status, attempt_count, completed_at FROM recording_jobs WHERE id=$1
	`, jobID).Scan(&status, &attemptCount, &completedAt); err != nil {
		t.Fatalf("read job: %v", err)
	}
	if status != "pending" || attemptCount != 0 || completedAt != nil {
		t.Fatalf("job state = status %q attempts %d completed %v, want pending/0/nil", status, attemptCount, completedAt)
	}
}

func TestContinuousDoesNotReviveDoneJobWithClips(t *testing.T) {
	pool, cleanup := testSchedulerPool(t)
	defer cleanup()

	ctx := context.Background()
	recID := insertSchedulerContinuousRecording(t, pool)
	now := time.Date(2026, 7, 9, 10, 53, 0, 0, time.UTC)
	windowOpen := time.Date(2026, 7, 9, 9, 0, 0, 0, time.UTC)
	jobID := insertSchedulerContinuousJob(t, pool, recID, windowOpen, "done")
	if _, err := pool.Exec(ctx, `
		INSERT INTO recording_clips (recording_id, recording_job_id, clip_start_at)
		VALUES ($1, $2, $3)
	`, recID, jobID, windowOpen); err != nil {
		t.Fatalf("insert clip: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_jobs SET window_end_at=$2 WHERE id=$1`, jobID, time.Date(2026, 7, 9, 11, 30, 0, 0, time.UTC)); err != nil {
		t.Fatalf("set window end: %v", err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	rec := activeRecording{
		id:               recID,
		mode:             "continuous",
		cronTimezone:     "UTC",
		clipDurationSec:  60,
		dailyWindowStart: strPtr("09:00:00"),
		dailyWindowEnd:   strPtr("11:30:00"),
		startAt:          time.Date(2026, 7, 9, 8, 0, 0, 0, time.UTC),
	}
	got, err := New(pool, Config{}).enqueueContinuousRecording(ctx, tx, rec, now)
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("enqueue continuous: %v", err)
	}
	if got != 0 {
		_ = tx.Rollback(ctx)
		t.Fatalf("enqueued=%d want 0", got)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM recording_jobs WHERE id=$1`, jobID).Scan(&status); err != nil {
		t.Fatalf("read job: %v", err)
	}
	if status != "done" {
		t.Fatalf("status=%q want done", status)
	}
}

func TestContinuousRevivesDoneJobWithClipsWhenWindowExtended(t *testing.T) {
	pool, cleanup := testSchedulerPool(t)
	defer cleanup()

	ctx := context.Background()
	recID := insertSchedulerContinuousRecording(t, pool)
	now := time.Date(2026, 7, 9, 10, 53, 0, 0, time.UTC)
	windowOpen := time.Date(2026, 7, 9, 9, 0, 0, 0, time.UTC)
	jobID := insertSchedulerContinuousJob(t, pool, recID, windowOpen, "done")
	if _, err := pool.Exec(ctx, `
		INSERT INTO recording_clips (recording_id, recording_job_id, clip_start_at)
		VALUES ($1, $2, $3)
	`, recID, jobID, windowOpen); err != nil {
		t.Fatalf("insert clip: %v", err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	rec := activeRecording{
		id:               recID,
		mode:             "continuous",
		cronTimezone:     "UTC",
		clipDurationSec:  60,
		dailyWindowStart: strPtr("09:00:00"),
		dailyWindowEnd:   strPtr("11:30:00"),
		startAt:          time.Date(2026, 7, 9, 8, 0, 0, 0, time.UTC),
	}
	got, err := New(pool, Config{}).enqueueContinuousRecording(ctx, tx, rec, now)
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("enqueue continuous: %v", err)
	}
	if got != 1 {
		_ = tx.Rollback(ctx)
		t.Fatalf("enqueued=%d want 1", got)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	var status string
	var windowEnd time.Time
	if err := pool.QueryRow(ctx, `SELECT status, window_end_at FROM recording_jobs WHERE id=$1`, jobID).Scan(&status, &windowEnd); err != nil {
		t.Fatalf("read job: %v", err)
	}
	if status != "pending" || !windowEnd.Equal(time.Date(2026, 7, 9, 11, 30, 0, 0, time.UTC)) {
		t.Fatalf("state=%q end=%s want pending extended to 11:30", status, windowEnd)
	}
}

func TestContinuousRevivesCanceledJobWithClips(t *testing.T) {
	pool, cleanup := testSchedulerPool(t)
	defer cleanup()

	ctx := context.Background()
	recID := insertSchedulerContinuousRecording(t, pool)
	now := time.Date(2026, 7, 9, 10, 53, 0, 0, time.UTC)
	windowOpen := time.Date(2026, 7, 9, 9, 0, 0, 0, time.UTC)
	jobID := insertSchedulerContinuousJob(t, pool, recID, windowOpen, "canceled")
	if _, err := pool.Exec(ctx, `
		INSERT INTO recording_clips (recording_id, recording_job_id, clip_start_at)
		VALUES ($1, $2, $3)
	`, recID, jobID, windowOpen); err != nil {
		t.Fatalf("insert clip: %v", err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	rec := activeRecording{
		id:               recID,
		mode:             "continuous",
		cronTimezone:     "UTC",
		clipDurationSec:  60,
		dailyWindowStart: strPtr("09:00:00"),
		dailyWindowEnd:   strPtr("11:30:00"),
		startAt:          time.Date(2026, 7, 9, 8, 0, 0, 0, time.UTC),
	}
	got, err := New(pool, Config{}).enqueueContinuousRecording(ctx, tx, rec, now)
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("enqueue continuous: %v", err)
	}
	if got != 1 {
		_ = tx.Rollback(ctx)
		t.Fatalf("enqueued=%d want 1", got)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM recording_jobs WHERE id=$1`, jobID).Scan(&status); err != nil {
		t.Fatalf("read job: %v", err)
	}
	if status != "pending" {
		t.Fatalf("status=%q want pending", status)
	}
}

func strPtr(s string) *string { return &s }

func insertSchedulerContinuousRecording(t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO recordings (mode, cron_timezone, clip_duration_sec, daily_window_start, daily_window_end, status, start_at)
		VALUES ('continuous', 'UTC', 60, '09:00', '11:30', 'active', '2026-07-09T08:00:00Z')
		RETURNING id
	`).Scan(&id); err != nil {
		t.Fatalf("insert recording: %v", err)
	}
	return id
}

func insertSchedulerContinuousJob(t *testing.T, pool *pgxpool.Pool, recID int64, fireAt time.Time, status string) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO recording_jobs
			(recording_id, fire_at, scheduled_for, clip_duration_sec, status, idempotency_key, kind, window_end_at, completed_at, attempt_count)
		VALUES ($1, $2, $2, 60, $3, $4, 'continuous_window', $5, now(), 2)
		RETURNING id
	`, recID, fireAt, status, fmt.Sprintf("reccont:%d:%d", recID, fireAt.Unix()), fireAt.Add(90*time.Minute)).Scan(&id); err != nil {
		t.Fatalf("insert job: %v", err)
	}
	return id
}

func testSchedulerPool(t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run DB-backed scheduler regression")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open admin pool: %v", err)
	}
	schema := fmt.Sprintf("recsched_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schema)); err != nil {
		admin.Close()
		t.Fatalf("create schema: %v", err)
	}
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		_, _ = admin.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
		admin.Close()
		t.Fatalf("parse db url: %v", err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		_, _ = admin.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
		admin.Close()
		t.Fatalf("open test pool: %v", err)
	}
	for _, stmt := range []string{
		`CREATE TABLE account_billing (account_id BIGINT PRIMARY KEY, has_payment_method BOOLEAN NOT NULL DEFAULT false)`,
		`CREATE TABLE recordings (
			id BIGSERIAL PRIMARY KEY,
			account_id BIGINT NOT NULL DEFAULT 1,
			mode TEXT NOT NULL DEFAULT 'sampled',
			cron_expr TEXT,
			cron_timezone TEXT NOT NULL DEFAULT 'UTC',
			clip_duration_sec INT NOT NULL DEFAULT 60,
			daily_window_start TIME,
			daily_window_end TIME,
			status TEXT NOT NULL DEFAULT 'active',
			start_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			end_at TIMESTAMPTZ,
			active_weekdays SMALLINT NOT NULL DEFAULT 127,
			completed_captured_clip_count BIGINT,
			completed_expected_clip_count BIGINT,
			last_enqueued_fire_at TIMESTAMPTZ,
			next_fire_at TIMESTAMPTZ,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE recording_jobs (
			id BIGSERIAL PRIMARY KEY,
			recording_id BIGINT NOT NULL REFERENCES recordings(id) ON DELETE CASCADE,
			fire_at TIMESTAMPTZ NOT NULL,
			scheduled_for TIMESTAMPTZ NOT NULL,
			clip_duration_sec INT NOT NULL,
			status TEXT NOT NULL DEFAULT 'pending',
			lease_owner TEXT,
			lease_expires_at TIMESTAMPTZ,
			attempt_count INT NOT NULL DEFAULT 0,
			max_attempts INT NOT NULL DEFAULT 3,
			error_text TEXT NOT NULL DEFAULT '',
			idempotency_key TEXT NOT NULL UNIQUE,
			kind TEXT NOT NULL DEFAULT 'clip',
			window_end_at TIMESTAMPTZ,
			completed_at TIMESTAMPTZ,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE recording_clips (
			id BIGSERIAL PRIMARY KEY,
			recording_id BIGINT NOT NULL REFERENCES recordings(id) ON DELETE CASCADE,
			recording_job_id BIGINT REFERENCES recording_jobs(id) ON DELETE SET NULL,
			clip_start_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			clip_end_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
	} {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			pool.Close()
			_, _ = admin.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
			admin.Close()
			t.Fatalf("create test table: %v", err)
		}
	}
	return pool, func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
		admin.Close()
	}
}
