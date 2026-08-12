package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/cleanupverify"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/secretbox"
)

const cleanupVerificationLease = 20 * time.Minute

type cleanupVerificationJob struct {
	ID, DestinationID     int64
	Endpoint, Bucket, Key string
	ExpectedSize          int64
	ExpectedSHA           string
	AccessKey, Region     string
	SecretEnc             []byte
	LeaseToken            uuid.UUID
}

func runNASCleanupVerifier(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 || args[0] != "run-once" {
		log.Fatal("nas-cleanup-verifier requires run-once")
	}
	fs := flag.NewFlagSet("nas-cleanup-verifier run-once", flag.ExitOnError)
	maxBytes := fs.Int64("max-bytes", 50_000_000_000, "maximum expected bytes hashed per run")
	maxObjectBytes := fs.Int64("max-object-bytes", cleanupverify.DefaultMaxObjectBytes, "maximum bytes per object")
	objectTimeout := fs.Duration("object-timeout", 15*time.Minute, "timeout per object")
	workerID := fs.String("worker-id", cleanupVerifierWorkerID(), "lease owner")
	_ = fs.Parse(args[1:])
	if len(fs.Args()) != 0 || *maxBytes <= 0 || *maxObjectBytes <= 0 || *objectTimeout <= 0 || strings.TrimSpace(*workerID) == "" {
		log.Fatal("invalid verifier arguments")
	}
	if *objectTimeout >= cleanupVerificationLease-time.Minute {
		log.Fatalf("--object-timeout must be less than %s so the lease cannot expire during a read", cleanupVerificationLease-time.Minute)
	}
	if strings.TrimSpace(cfg.StorageCredKey) == "" {
		log.Fatal("STORAGE_CRED_KEY is required")
	}
	cipher, err := secretbox.NewFromBase64Key(cfg.StorageCredKey)
	if err != nil {
		log.Fatalf("storage cipher: %v", err)
	}
	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	var consumed int64
	var verified, unknown int
	for consumed < *maxBytes {
		job, ok, err := claimCleanupVerification(ctx, pool, strings.TrimSpace(*workerID))
		if err != nil {
			log.Fatalf("claim verification: %v", err)
		}
		if !ok {
			break
		}
		if job.ExpectedSize > *maxObjectBytes {
			if err := finishCleanupVerificationError(ctx, pool, job, "object_too_large", true); err != nil {
				log.Fatal(err)
			}
			unknown++
			continue
		}
		if consumed+job.ExpectedSize > *maxBytes {
			if err := returnCleanupVerification(ctx, pool, job); err != nil {
				log.Fatal(err)
			}
			break
		}
		consumed += job.ExpectedSize
		result, verifyErr := verifyCleanupObject(ctx, cipher, job, *maxObjectBytes, *objectTimeout)
		if verifyErr != nil {
			code := cleanupverify.ErrorCode(verifyErr)
			if err := finishCleanupVerificationError(ctx, pool, job, code, false); err != nil {
				log.Fatal(err)
			}
			unknown++
			log.Printf("nas cleanup verifier: id=%d status=unknown code=%s", job.ID, code)
		} else {
			if err := finishCleanupVerificationSuccess(ctx, pool, job, result); err != nil {
				log.Fatal(err)
			}
			verified++
		}
	}
	if err := refreshCleanupCandidateRuns(ctx, pool); err != nil {
		log.Fatalf("refresh runs: %v", err)
	}
	log.Printf("nas cleanup verifier: verified=%d unknown=%d bytes=%d", verified, unknown, consumed)
}

func cleanupVerifierWorkerID() string {
	if host, err := os.Hostname(); err == nil && strings.TrimSpace(host) != "" {
		return "cleanup-" + host
	}
	return "cleanup-verifier"
}

func claimCleanupVerification(ctx context.Context, pool *pgxpool.Pool, owner string) (cleanupVerificationJob, bool, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return cleanupVerificationJob{}, false, err
	}
	defer tx.Rollback(ctx)
	j := cleanupVerificationJob{LeaseToken: uuid.New()}
	err = tx.QueryRow(ctx, `WITH candidate AS (
	  SELECT id FROM r2_content_verifications WHERE
	    (status='queued' OR (status='leased' AND lease_expires_at<now()))
	    AND next_attempt_at<=now() AND attempt_count<max_attempts
	  ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1)
	UPDATE r2_content_verifications v SET status='leased',lease_token=$1,lease_owner=$2,
	  lease_expires_at=now()+$3::interval,attempt_count=attempt_count+1,updated_at=now()
	FROM candidate c,storage_destinations d
	WHERE v.id=c.id AND d.id=v.storage_destination_id
	  AND d.endpoint=v.endpoint_snapshot AND d.bucket=v.bucket
	  AND EXISTS(SELECT 1 FROM nas_cleanup_candidate_items item
	    JOIN nas_cleanup_candidate_runs run ON run.id=item.run_id
	    WHERE item.verification_id=v.id AND run.account_id=d.account_id)
	RETURNING v.id,v.storage_destination_id,v.endpoint_snapshot,v.bucket,v.object_key,
	  v.expected_size_bytes,v.expected_sha256,d.access_key_id,d.region,d.secret_access_key_enc,v.lease_token`,
		j.LeaseToken, owner, cleanupVerificationLease.String()).Scan(&j.ID, &j.DestinationID, &j.Endpoint, &j.Bucket, &j.Key, &j.ExpectedSize, &j.ExpectedSHA, &j.AccessKey, &j.Region, &j.SecretEnc, &j.LeaseToken)
	if errors.Is(err, pgx.ErrNoRows) {
		return cleanupVerificationJob{}, false, nil
	}
	if err != nil {
		return cleanupVerificationJob{}, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return cleanupVerificationJob{}, false, err
	}
	return j, true, nil
}

func verifyCleanupObject(parent context.Context, cipher *secretbox.Cipher, j cleanupVerificationJob, maxBytes int64, timeout time.Duration) (cleanupverify.Result, error) {
	secret, err := cipher.Decrypt(j.SecretEnc)
	if err != nil {
		return cleanupverify.Result{}, fmt.Errorf("decrypt credential: %w", err)
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	client, err := r2.New(ctx, r2.Config{AccessKey: j.AccessKey, SecretKey: string(secret), Region: j.Region, Bucket: j.Bucket, Endpoint: j.Endpoint})
	if err != nil {
		return cleanupverify.Result{}, fmt.Errorf("build client: %w", err)
	}
	return cleanupverify.Verify(ctx, client, j.Key, j.ExpectedSize, j.ExpectedSHA, maxBytes)
}

func finishCleanupVerificationSuccess(ctx context.Context, pool *pgxpool.Pool, j cleanupVerificationJob, result cleanupverify.Result) error {
	tag, err := pool.Exec(ctx, `UPDATE r2_content_verifications SET status='verified',observed_etag=$3,
	 observed_version_id=$4,observed_size_bytes=$5,observed_sha256=$6,verified_at=now(),last_head_at=now(),
	 lease_token=NULL,lease_owner=NULL,lease_expires_at=NULL,error_code='',updated_at=now()
	 WHERE id=$1 AND lease_token=$2 AND status='leased'`, j.ID, j.LeaseToken, result.ETag, result.VersionID, result.SizeBytes, result.SHA256)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("verification lease lost")
	}
	return nil
}

func finishCleanupVerificationError(ctx context.Context, pool *pgxpool.Pool, j cleanupVerificationJob, code string, terminal bool) error {
	tag, err := pool.Exec(ctx, `UPDATE r2_content_verifications SET
	  status=CASE WHEN $4 OR attempt_count>=max_attempts THEN 'unknown' ELSE 'queued' END,
	  next_attempt_at=now()+make_interval(secs=>LEAST(3600,60*(1<<LEAST(attempt_count,5)))),
	  lease_token=NULL,lease_owner=NULL,lease_expires_at=NULL,error_code=$3,updated_at=now()
	  WHERE id=$1 AND lease_token=$2 AND status='leased'`, j.ID, j.LeaseToken, code, terminal)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("verification lease lost")
	}
	return nil
}

func returnCleanupVerification(ctx context.Context, pool *pgxpool.Pool, j cleanupVerificationJob) error {
	tag, err := pool.Exec(ctx, `UPDATE r2_content_verifications SET status='queued',attempt_count=GREATEST(0,attempt_count-1),
	 lease_token=NULL,lease_owner=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND lease_token=$2 AND status='leased'`, j.ID, j.LeaseToken)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("verification lease lost")
	}
	return nil
}

func refreshCleanupCandidateRuns(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `UPDATE nas_cleanup_candidate_runs run SET unknown_count=facts.unknown_count,
	  state=CASE WHEN facts.unknown_count>0 THEN 'verifying' ELSE 'unknown' END,
	  nas_rehash_required=NOT(conn.inventory_scan_rows_skipped=0 AND conn.inventory_scan_completed_at>run.created_at
	    AND conn.inventory_scan_started_at>run.created_at AND conn.inventory_in_progress_generation=''
	    AND conn.inventory_live_revision=conn.inventory_tree_revision), finished_at=NULL
	FROM connections conn,LATERAL (SELECT count(*) FILTER(WHERE verification.status<>'verified')::bigint unknown_count
	  FROM nas_cleanup_candidate_items item JOIN r2_content_verifications verification ON verification.id=item.verification_id
	  WHERE item.run_id=run.id) facts
	WHERE conn.id=run.connection_id AND run.state IN('queued','verifying','unknown')`)
	if err != nil {
		return err
	}
	return finalizeReadyCandidateRuns(ctx, pool)
}

func finalizeReadyCandidateRuns(ctx context.Context, pool *pgxpool.Pool) error {
	rows, err := pool.Query(ctx, `SELECT run.id FROM nas_cleanup_candidate_runs run JOIN connections conn ON conn.id=run.connection_id
	 WHERE run.state IN('queued','verifying','unknown') AND run.unknown_count=0
	 AND conn.inventory_scan_rows_skipped=0 AND conn.inventory_scan_started_at>run.created_at
	 AND conn.inventory_scan_completed_at>run.created_at AND conn.inventory_scan_completed_at>=conn.inventory_scan_started_at
	 AND conn.inventory_in_progress_generation='' AND conn.inventory_generation<>''
	 AND conn.inventory_digest~'^[0-9a-f]{64}$' AND conn.inventory_live_revision=conn.inventory_tree_revision
	 AND NOT EXISTS(SELECT 1 FROM recording_qualification_members protected
	   WHERE protected.account_id=run.account_id AND protected.recording_id=ANY(run.recording_ids))
	 AND NOT EXISTS(SELECT 1 FROM nas_cleanup_candidate_items item
	   LEFT JOIN nas_inventory_files inv ON inv.connection_id=run.connection_id AND inv.clip_id=item.clip_id
	   JOIN r2_content_verifications verification ON verification.id=item.verification_id
	   WHERE item.run_id=run.id AND (verification.status IS DISTINCT FROM 'verified'
	    OR verification.verified_at IS NULL OR verification.verified_at<run.created_at
	    OR verification.observed_size_bytes IS DISTINCT FROM item.size_bytes
	    OR lower(verification.observed_sha256) IS DISTINCT FROM item.content_sha256
	    OR (item.recovery_etag<>'' AND verification.observed_etag IS DISTINCT FROM item.recovery_etag)
	    OR inv.clip_id IS NULL OR inv.state IS DISTINCT FROM 'present'
	    OR inv.seen_generation IS DISTINCT FROM conn.inventory_generation OR inv.relative_path IS DISTINCT FROM item.relative_path
	    OR inv.size_bytes IS DISTINCT FROM item.size_bytes OR lower(inv.sha256) IS DISTINCT FROM item.content_sha256
	    OR inv.file_mtime_ns IS DISTINCT FROM item.file_mtime_ns OR inv.file_ctime_ns IS DISTINCT FROM item.file_ctime_ns
	    OR inv.file_inode IS DISTINCT FROM item.file_inode OR inv.file_device IS DISTINCT FROM item.file_device
	    OR inv.sidecar_relative_path IS DISTINCT FROM item.sidecar_relative_path OR inv.sidecar_size_bytes IS DISTINCT FROM item.sidecar_size_bytes
	    OR lower(inv.sidecar_sha256) IS DISTINCT FROM item.sidecar_sha256)) ORDER BY run.id`)
	if err != nil {
		return err
	}
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, id := range ids {
		if err := finalizeReadyCandidateRun(ctx, pool, id); err != nil {
			return err
		}
	}
	return nil
}

func finalizeReadyCandidateRun(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) error {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var header string
	err = tx.QueryRow(ctx, `SELECT jsonb_build_array(run.id,run.account_id,run.connection_id,run.recording_ids,run.request_digest,
	 conn.inventory_generation,conn.inventory_digest,conn.inventory_scan_started_at,conn.inventory_scan_completed_at,
	 conn.inventory_live_revision,conn.inventory_tree_revision)::text FROM nas_cleanup_candidate_runs run JOIN connections conn ON conn.id=run.connection_id
	 WHERE run.id=$1 AND run.state IN('queued','verifying','unknown') AND run.unknown_count=0
	 AND conn.inventory_scan_rows_skipped=0 AND conn.inventory_scan_started_at>run.created_at AND conn.inventory_scan_completed_at>run.created_at
	 AND conn.inventory_scan_completed_at>=conn.inventory_scan_started_at AND conn.inventory_in_progress_generation=''
	 AND conn.inventory_generation<>'' AND conn.inventory_digest~'^[0-9a-f]{64}$'
	 AND conn.inventory_live_revision=conn.inventory_tree_revision
	 AND NOT EXISTS(SELECT 1 FROM recording_qualification_members protected
	   WHERE protected.account_id=run.account_id AND protected.recording_id=ANY(run.recording_ids))
	 FOR UPDATE OF run,conn`, id).Scan(&header)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "stoarama-nas-final-v1\n%d:%s\n", len(header), header)
	itemRows, err := tx.Query(ctx, `SELECT jsonb_build_array(item.ordinal,item.clip_id,item.recording_id,item.relative_path,item.size_bytes,item.content_sha256,
	 inv.seen_generation,inv.verified_at,inv.file_mtime_ns,inv.file_ctime_ns,inv.file_inode,inv.file_device,inv.sidecar_relative_path,inv.sidecar_size_bytes,inv.sidecar_sha256,
	 verification.storage_destination_id,verification.endpoint_snapshot,verification.bucket,verification.object_key,verification.expected_size_bytes,verification.expected_sha256,
	 verification.observed_etag,verification.observed_version_id,verification.observed_size_bytes,verification.observed_sha256,verification.verified_at)::text
	 FROM nas_cleanup_candidate_items item JOIN nas_cleanup_candidate_runs run ON run.id=item.run_id
	 JOIN connections conn ON conn.id=run.connection_id JOIN nas_inventory_files inv ON inv.connection_id=run.connection_id AND inv.clip_id=item.clip_id
	 JOIN r2_content_verifications verification ON verification.id=item.verification_id WHERE item.run_id=$1
	 AND verification.status='verified' AND verification.verified_at>=run.created_at
	 AND verification.observed_size_bytes IS NOT DISTINCT FROM item.size_bytes AND lower(verification.observed_sha256) IS NOT DISTINCT FROM item.content_sha256
	 AND (item.recovery_etag='' OR verification.observed_etag IS NOT DISTINCT FROM item.recovery_etag)
	 AND inv.state='present' AND inv.seen_generation IS NOT DISTINCT FROM conn.inventory_generation
	 AND inv.relative_path IS NOT DISTINCT FROM item.relative_path AND inv.size_bytes IS NOT DISTINCT FROM item.size_bytes AND lower(inv.sha256) IS NOT DISTINCT FROM item.content_sha256
	 AND inv.file_mtime_ns IS NOT DISTINCT FROM item.file_mtime_ns AND inv.file_ctime_ns IS NOT DISTINCT FROM item.file_ctime_ns
	 AND inv.file_inode IS NOT DISTINCT FROM item.file_inode AND inv.file_device IS NOT DISTINCT FROM item.file_device
	 AND inv.sidecar_relative_path IS NOT DISTINCT FROM item.sidecar_relative_path AND inv.sidecar_size_bytes IS NOT DISTINCT FROM item.sidecar_size_bytes
	 AND lower(inv.sidecar_sha256) IS NOT DISTINCT FROM item.sidecar_sha256 ORDER BY item.ordinal FOR SHARE OF item,inv,verification`, id)
	if err != nil {
		return err
	}
	count := int64(0)
	for itemRows.Next() {
		var line string
		if err := itemRows.Scan(&line); err != nil {
			itemRows.Close()
			return err
		}
		_, _ = fmt.Fprintf(h, "%d:%s\n", len(line), line)
		count++
	}
	if err := itemRows.Err(); err != nil {
		itemRows.Close()
		return err
	}
	itemRows.Close()
	var expected int64
	if err := tx.QueryRow(ctx, `SELECT item_count FROM nas_cleanup_candidate_runs WHERE id=$1`, id).Scan(&expected); err != nil {
		return err
	}
	if count != expected {
		return nil
	}
	digest := hex.EncodeToString(h.Sum(nil))
	tag, err := tx.Exec(ctx, `UPDATE nas_cleanup_candidate_runs SET state='ready',final_digest=$2,nas_rehash_required=false,finished_at=now() WHERE id=$1 AND final_digest IS NULL`, id, digest)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("candidate finalization race")
	}
	return tx.Commit(ctx)
}
