package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type CaptureSamplingPolicy struct {
	MinIntervalSec int
	MaxIntervalSec int
}

type CaptureJob struct {
	ID             int64
	StreamID       int64
	ScheduledFor   time.Time
	AttemptCount   int
	IdempotencyKey string
}

func EnqueueDueCaptureJobs(ctx context.Context, pool *pgxpool.Pool, policy CaptureSamplingPolicy) error {
	if err := policy.Validate(); err != nil {
		return err
	}
	if _, err := pool.Exec(ctx, `
		UPDATE capture_jobs
		SET status='pending', lease_owner=NULL, lease_expires_at=NULL, updated_at=now()
		WHERE status='leased' AND lease_expires_at < now()
	`); err != nil {
		return fmt.Errorf("release expired capture jobs: %w", err)
	}
	q := `
	WITH due AS (
	  SELECT s.id AS stream_id,
	         now() + make_interval(secs => floor($1::double precision + random() * (($2 - $1 + 1)::double precision))::int) AS scheduled_for
	  FROM streams s
	  WHERE s.recording_state = 'on'
	    AND COALESCE(NULLIF(s.capture_family, ''), 'continuous_video') = 'continuous_video'
	    AND NOT EXISTS (
	      SELECT 1 FROM capture_jobs j
	      WHERE j.stream_id=s.id
	        AND j.status IN ('pending','leased')
	    )
	)
	INSERT INTO capture_jobs (stream_id, scheduled_for, status, attempt_count, idempotency_key)
	SELECT stream_id, scheduled_for, 'pending', 0, concat('sampled-clip:', stream_id, ':', floor(extract(epoch from scheduled_for))::bigint)
	FROM due
	ON CONFLICT (idempotency_key) DO NOTHING
	`
	_, err := pool.Exec(ctx, q, policy.MinIntervalSec, policy.MaxIntervalSec)
	if err != nil {
		return fmt.Errorf("enqueue due capture jobs: %w", err)
	}
	return nil
}

func (p CaptureSamplingPolicy) Validate() error {
	if p.MinIntervalSec <= 0 {
		return fmt.Errorf("capture sampling min interval must be > 0")
	}
	if p.MaxIntervalSec < p.MinIntervalSec {
		return fmt.Errorf("capture sampling max interval must be >= min interval")
	}
	return nil
}

func LeaseOneCaptureJob(ctx context.Context, pool *pgxpool.Pool, workerID string, leaseSec int) (*CaptureJob, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	q := `
	WITH cte AS (
	  SELECT id
	  FROM capture_jobs
	  WHERE status='pending' AND scheduled_for <= now()
	  ORDER BY scheduled_for ASC, id ASC
	  LIMIT 1
	  FOR UPDATE SKIP LOCKED
	)
	UPDATE capture_jobs j
	SET status='leased',
	    lease_owner=$1,
	    lease_expires_at=now() + make_interval(secs => $2),
	    updated_at=now()
	FROM cte
	WHERE j.id = cte.id
	RETURNING j.id, j.stream_id, j.scheduled_for, j.attempt_count, j.idempotency_key
	`
	var job CaptureJob
	err = tx.QueryRow(ctx, q, workerID, leaseSec).Scan(&job.ID, &job.StreamID, &job.ScheduledFor, &job.AttemptCount, &job.IdempotencyKey)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("lease capture job: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit lease tx: %w", err)
	}
	return &job, nil
}

func CompleteCaptureJob(ctx context.Context, pool *pgxpool.Pool, jobID int64, nextDelaySec int) error {
	if nextDelaySec <= 0 {
		return fmt.Errorf("next delay must be > 0")
	}
	err := finishCaptureJob(ctx, pool, jobID, "done", "", nextDelaySec)
	if err != nil {
		return fmt.Errorf("complete capture job %d: %w", jobID, err)
	}
	return nil
}

func CompleteCaptureJobWithoutNext(ctx context.Context, pool *pgxpool.Pool, jobID int64) error {
	_, err := pool.Exec(ctx, `
		UPDATE capture_jobs
		SET status='done', updated_at=now(), completed_at=now(), lease_owner=NULL, lease_expires_at=NULL
		WHERE id=$1
	`, jobID)
	if err != nil {
		return fmt.Errorf("complete capture job without next %d: %w", jobID, err)
	}
	return nil
}

func FailCaptureJob(ctx context.Context, pool *pgxpool.Pool, jobID int64, errText string, nextDelaySec int) error {
	if nextDelaySec <= 0 {
		return fmt.Errorf("next delay must be > 0")
	}
	err := finishCaptureJob(ctx, pool, jobID, "error", errText, nextDelaySec)
	if err != nil {
		return fmt.Errorf("fail capture job %d: %w", jobID, err)
	}
	return nil
}

func finishCaptureJob(ctx context.Context, pool *pgxpool.Pool, jobID int64, status string, errText string, nextDelaySec int) error {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin finish tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var streamID int64
	switch status {
	case "done":
		err = tx.QueryRow(ctx, `
			UPDATE capture_jobs
			SET status='done', updated_at=now(), completed_at=now(), lease_owner=NULL, lease_expires_at=NULL
			WHERE id=$1
			RETURNING stream_id
		`, jobID).Scan(&streamID)
	case "error":
		err = tx.QueryRow(ctx, `
			UPDATE capture_jobs
			SET status='error', updated_at=now(), completed_at=now(), lease_owner=NULL, lease_expires_at=NULL,
			    attempt_count=attempt_count+1, error_text=$2
			WHERE id=$1
			RETURNING stream_id
		`, jobID, errText).Scan(&streamID)
	default:
		return fmt.Errorf("unsupported capture job status %q", status)
	}
	if err != nil {
		return fmt.Errorf("update capture job: %w", err)
	}
	nextScheduled := time.Now().UTC().Add(time.Duration(nextDelaySec) * time.Second)
	_, err = tx.Exec(ctx, `
		INSERT INTO capture_jobs (stream_id, scheduled_for, status, attempt_count, idempotency_key)
		SELECT s.id, $2, 'pending', 0, concat('sampled-clip:', s.id, ':', floor(extract(epoch from $2::timestamptz))::bigint)
		FROM streams s
		WHERE s.id=$1
		  AND s.recording_state='on'
		  AND COALESCE(NULLIF(s.capture_family, ''), 'continuous_video') = 'continuous_video'
		  AND NOT EXISTS (
		    SELECT 1 FROM capture_jobs j
		    WHERE j.stream_id=s.id
		      AND j.status IN ('pending','leased')
		  )
		ON CONFLICT (idempotency_key) DO NOTHING
	`, streamID, nextScheduled)
	if err != nil {
		return fmt.Errorf("schedule next capture job: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit finish tx: %w", err)
	}
	return nil
}
