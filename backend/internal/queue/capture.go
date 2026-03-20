package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type CaptureJob struct {
	ID             int64
	StreamID       int64
	ScheduledFor   time.Time
	AttemptCount   int
	IdempotencyKey string
}

func EnqueueDueCaptureJobs(ctx context.Context, pool *pgxpool.Pool) error {
	q := `
	WITH due AS (
	  SELECT s.id AS stream_id,
	         now() AS scheduled_for,
	         concat('stream:', s.id, ':', floor(extract(epoch from now()))::bigint) AS idem
	  FROM streams s
	  JOIN recording_settings rs ON rs.id=true
	  LEFT JOIN LATERAL (
	    SELECT f.captured_at
	    FROM frames f
	    WHERE f.stream_id = s.id AND f.capture_status='success'
	    ORDER BY f.captured_at DESC
	    LIMIT 1
	  ) lf ON true
	  WHERE s.recording_state = 'on'
	    AND (lf.captured_at IS NULL OR lf.captured_at <= now() - make_interval(secs => rs.capture_interval_sec))
	    AND NOT EXISTS (
	      SELECT 1 FROM capture_jobs j
	      WHERE j.stream_id=s.id
	        AND j.status IN ('pending','leased')
	    )
	)
	INSERT INTO capture_jobs (stream_id, scheduled_for, status, attempt_count, idempotency_key)
	SELECT stream_id, scheduled_for, 'pending', 0, idem
	FROM due
	ON CONFLICT (idempotency_key) DO NOTHING
	`
	_, err := pool.Exec(ctx, q)
	if err != nil {
		return fmt.Errorf("enqueue due capture jobs: %w", err)
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

func CompleteCaptureJob(ctx context.Context, pool *pgxpool.Pool, jobID int64) error {
	_, err := pool.Exec(ctx, `
		UPDATE capture_jobs
		SET status='done', updated_at=now(), lease_owner=NULL, lease_expires_at=NULL
		WHERE id=$1
	`, jobID)
	if err != nil {
		return fmt.Errorf("complete capture job %d: %w", jobID, err)
	}
	return nil
}

func FailCaptureJob(ctx context.Context, pool *pgxpool.Pool, jobID int64, errText string) error {
	_, err := pool.Exec(ctx, `
		UPDATE capture_jobs
		SET status='error', updated_at=now(), lease_owner=NULL, lease_expires_at=NULL,
		    attempt_count=attempt_count+1, error_text=$2
		WHERE id=$1
	`, jobID, errText)
	if err != nil {
		return fmt.Errorf("fail capture job %d: %w", jobID, err)
	}
	return nil
}
