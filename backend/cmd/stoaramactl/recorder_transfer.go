package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/secretbox"
)

// clipTransferTickInterval is the clip-transfer worker's poll cadence. Each
// copy can be multi-GB, so the loop processes a small bounded number of jobs per
// tick and relies on FOR UPDATE SKIP LOCKED so concurrent ticks / instances
// never double-lease.
const clipTransferTickInterval = 10 * time.Second

// clipTransferMaxPerTick bounds how many jobs one tick copies. Kept tiny because
// each job streams an entire clip; SKIP LOCKED spreads remaining work across the
// next ticks.
const clipTransferMaxPerTick = 2

// clipTransferLeaseSec is the lease window. It must outlast a single copy; a
// stalled worker's lease expires after this and another tick re-leases the job.
const clipTransferLeaseSec = 30 * 60

// clipTransferLeaseOwner names the lease owner so a re-lease of an expired lease
// is attributable to a host. recorder-control runs one replica, so a host + pid
// label is sufficient (the lease itself, not the owner string, guards against
// double-processing via SKIP LOCKED).
func clipTransferLeaseOwner() string {
	host, err := os.Hostname()
	if err != nil || strings.TrimSpace(host) == "" {
		host = "recorder-control"
	}
	return fmt.Sprintf("%s-%d", strings.TrimSpace(host), os.Getpid())
}

// clipTransferJob is one leased copy: the source clip's location + credentials
// and the target destination's location + credentials.
type clipTransferJob struct {
	id              int64
	targetObjectKey string
	attemptCount    int
	maxAttempts     int
	mimeType        string

	srcEndpoint    string
	srcRegion      string
	srcBucket      string
	srcObjectKey   string
	srcAccessKeyID string
	srcSecretEnc   []byte

	dstEndpoint    string
	dstRegion      string
	dstBucket      string
	dstAccessKeyID string
	dstSecretEnc   []byte
}

// runClipTransfer is the background loop that copies clips into accounts' own S3
// buckets. It runs under runWithBackoff in recorder-control. If the storage
// credential cipher is unset (STORAGE_CRED_KEY empty) it logs one clear warning
// and idles forever (it cannot decrypt any destination secret), rather than
// crashing the control process.
func runClipTransfer(ctx context.Context, pool *pgxpool.Pool, cipher *secretbox.Cipher) error {
	if cipher == nil {
		log.Printf("clip transfer: STORAGE_CRED_KEY is unset; clip-transfer worker idle (cannot decrypt destination credentials).")
		<-ctx.Done()
		return ctx.Err()
	}

	ticker := time.NewTicker(clipTransferTickInterval)
	defer ticker.Stop()

	runOnce := func() {
		for i := 0; i < clipTransferMaxPerTick; i++ {
			if ctx.Err() != nil {
				return
			}
			processed, err := transferOneClip(ctx, pool, cipher)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("clip transfer: %v", err)
				return
			}
			if !processed {
				return // no pending job this tick
			}
		}
	}

	runOnce()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			runOnce()
		}
	}
}

// transferOneClip leases one pending (or expired-leased) job, copies the clip
// via a streamed GET(source)->PutMultipart(target), and records the outcome.
// Returns (true, nil) if a job was leased and processed, (false, nil) if no job
// was due. Per-job copy errors are folded into the job row (retry or terminal
// error), not returned, so one bad clip never stalls the loop.
func transferOneClip(ctx context.Context, pool *pgxpool.Pool, cipher *secretbox.Cipher) (bool, error) {
	job, ok, err := leaseClipTransferJob(ctx, pool)
	if err != nil {
		return false, fmt.Errorf("lease clip transfer job: %w", err)
	}
	if !ok {
		return false, nil
	}

	n, copyErr := copyClip(ctx, cipher, job)
	if copyErr != nil {
		if ctx.Err() != nil {
			return true, nil
		}
		finishClipTransferError(ctx, pool, job, copyErr)
		return true, nil
	}

	if _, err := pool.Exec(ctx, `
		UPDATE clip_transfer_jobs
		SET status='done', bytes_copied=$2, error_text='', completed_at=now(),
		    lease_owner=NULL, lease_expires_at=NULL, updated_at=now()
		WHERE id=$1
	`, job.id, n); err != nil {
		return true, fmt.Errorf("mark clip transfer done (job %d): %w", job.id, err)
	}
	return true, nil
}

// leaseClipTransferJob atomically claims one job: a 'pending' row, or a 'leased'
// row whose lease has expired (the prior owner stalled). It uses FOR UPDATE SKIP
// LOCKED so concurrent ticks/instances never claim the same row, then loads the
// source clip's location + destination credentials and the target destination.
func leaseClipTransferJob(ctx context.Context, pool *pgxpool.Pool) (clipTransferJob, bool, error) {
	var job clipTransferJob
	err := pool.QueryRow(ctx, `
		WITH cte AS (
			SELECT id
			FROM clip_transfer_jobs
			WHERE status='pending'
			   OR (status='leased' AND lease_expires_at IS NOT NULL AND lease_expires_at < now())
			ORDER BY id ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE clip_transfer_jobs j
		SET status='leased',
		    lease_owner=$1,
		    lease_expires_at = now() + make_interval(secs => $2),
		    attempt_count = attempt_count + 1,
		    updated_at = now()
		FROM cte
		WHERE j.id = cte.id
		RETURNING j.id, j.target_object_key, j.attempt_count, j.max_attempts
	`, clipTransferLeaseOwner(), clipTransferLeaseSec).Scan(
		&job.id, &job.targetObjectKey, &job.attemptCount, &job.maxAttempts,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return clipTransferJob{}, false, nil
	}
	if err != nil {
		return clipTransferJob{}, false, err
	}

	// Load source clip location (+ its destination creds) and target destination
	// (+ creds). Loaded after the lease so the row is already claimed.
	err = pool.QueryRow(ctx, `
		SELECT c.endpoint, src.region, c.bucket, c.object_key, c.mime_type,
		       src.access_key_id, src.secret_access_key_enc,
		       dst.endpoint, dst.region, dst.bucket, dst.access_key_id, dst.secret_access_key_enc
		FROM clip_transfer_jobs j
		JOIN recording_clips c       ON c.id = j.recording_clip_id
		JOIN storage_destinations src ON src.id = c.storage_destination_id
		JOIN storage_destinations dst ON dst.id = j.target_storage_destination_id
		WHERE j.id=$1
	`, job.id).Scan(
		&job.srcEndpoint, &job.srcRegion, &job.srcBucket, &job.srcObjectKey, &job.mimeType,
		&job.srcAccessKeyID, &job.srcSecretEnc,
		&job.dstEndpoint, &job.dstRegion, &job.dstBucket, &job.dstAccessKeyID, &job.dstSecretEnc,
	)
	if err != nil {
		// The lease is held but we cannot load its inputs (e.g. a destination was
		// deleted). Record it as a job error so the lease does not just expire and
		// re-lease in a loop.
		finishClipTransferError(ctx, pool, job, fmt.Errorf("load transfer inputs: %w", err))
		return clipTransferJob{}, false, nil
	}
	return job, true, nil
}

// copyClip decrypts both secrets, builds the source + target r2 clients, and
// streams the source object into the target via a bounded-memory multipart PUT.
// Returns the number of bytes copied (the source clip size, re-headed so the
// recorded byte count is the object's real size, not a row estimate).
func copyClip(ctx context.Context, cipher *secretbox.Cipher, job clipTransferJob) (int64, error) {
	srcSecret, err := cipher.Decrypt(job.srcSecretEnc)
	if err != nil {
		return 0, fmt.Errorf("decrypt source secret: %w", err)
	}
	dstSecret, err := cipher.Decrypt(job.dstSecretEnc)
	if err != nil {
		return 0, fmt.Errorf("decrypt target secret: %w", err)
	}
	source, err := r2.New(ctx, r2.Config{
		AccessKey: job.srcAccessKeyID,
		SecretKey: string(srcSecret),
		Region:    job.srcRegion,
		Bucket:    job.srcBucket,
		Endpoint:  job.srcEndpoint,
	})
	if err != nil {
		return 0, fmt.Errorf("build source client: %w", err)
	}
	target, err := r2.New(ctx, r2.Config{
		AccessKey: job.dstAccessKeyID,
		SecretKey: string(dstSecret),
		Region:    job.dstRegion,
		Bucket:    job.dstBucket,
		Endpoint:  job.dstEndpoint,
	})
	if err != nil {
		return 0, fmt.Errorf("build target client: %w", err)
	}

	mimeType := job.mimeType
	if mimeType == "" {
		mimeType = "video/mp4"
	}

	rc, err := source.Open(ctx, job.srcObjectKey)
	if err != nil {
		return 0, fmt.Errorf("open source object: %w", err)
	}
	defer func() { _ = rc.Close() }()

	if _, err := target.PutMultipart(ctx, job.targetObjectKey, mimeType, rc); err != nil {
		return 0, fmt.Errorf("put target object: %w", err)
	}

	head, err := target.Head(ctx, job.targetObjectKey)
	if err != nil {
		return 0, fmt.Errorf("head target object: %w", err)
	}
	return head.SizeBytes, nil
}

// finishClipTransferError records a per-job copy failure: it retries (back to
// 'pending') while attempts remain, else marks the job a terminal 'error'.
// attempt_count was already incremented at lease time, so the comparison is
// against the post-increment count. Best-effort: a DB error here is logged, not
// returned, so the loop continues.
func finishClipTransferError(ctx context.Context, pool *pgxpool.Pool, job clipTransferJob, cause error) {
	errText := cause.Error()
	if _, err := pool.Exec(ctx, `
		UPDATE clip_transfer_jobs
		SET status = CASE WHEN attempt_count < max_attempts THEN 'pending' ELSE 'error' END,
		    error_text = $2,
		    lease_owner = NULL,
		    lease_expires_at = NULL,
		    completed_at = CASE WHEN attempt_count < max_attempts THEN NULL ELSE now() END,
		    updated_at = now()
		WHERE id = $1
	`, job.id, errText); err != nil {
		log.Printf("clip transfer: record job %d error failed: %v (original cause: %v)", job.id, err, cause)
		return
	}
	log.Printf("clip transfer: job %d failed (attempt %d/%d): %v", job.id, job.attemptCount, job.maxAttempts, cause)
}
