package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/util"
)

// NAS -> R2 restore, for Keep Everything originals only.
//
// Deniz, verbatim: "nas shoul dnot have any of the keep everything (except the
// keep 20)". R2 holds 100% of Keep Everything, so a Keep Everything clip whose
// bytes exist only on the NAS goes back to its original R2 key before the NAS
// copy can leave.
//
// stoaramactl nas-restore request queues rows in nas_restore_requests. A row
// is restorable only when nas_restore_ke_original holds: target 'original', the
// purged clip's own object key, and the clip's stream pinned keep_everything in
// nas_media_keep20_windows (the manifest cleanupEmbeddedPolicy embeds). Every
// other row stays as it is: never leased, requeued, verified or canceled here,
// and its report is refused with 409 before any verification, unpurge or row
// transition. Test restores stay retired. A clip under a NAS cleanup hold
// waits: its purged_at cannot change until the hold releases.
//
// The lease issues a presigned create-only PUT bound to the restore row: the
// key, content type, exact length, sha256 checksum and If-None-Match: * are
// signed. On report the API HEADs the object and re-hashes it in full; only
// then is the row verified and the clip's purged_at cleared. R2 deletes are
// retired, so a mismatched object is left in place and fails the row.

const (
	nasRestoreLeaseTTL      = 30 * time.Minute
	nasRestoreMaxLeaseTasks = 64
	nasRestoreIdleRetrySec  = 120
	nasRestoreBusyRetrySec  = 1
	nasRestoreMaxError      = 500
	nasRestoreMaxDuration   = 24 * time.Hour
	// The API's WriteTimeout is 60s. Clips are at most ~85 MB, which R2 serves
	// to the API in a few seconds; the verify and the write must both finish
	// inside the one report request.
	nasRestoreVerifyTimeout  = 40 * time.Second
	nasRestoreRecordTimeout  = 10 * time.Second
	nasRestoreMaxObjectBytes = r2.MaxConditionalPutBytes
)

// Client outcomes. uploaded/exists ask the API to verify the object;
// local_missing/local_mismatch are permanent; upload_failed is retried.
var nasRestoreOutcomes = map[string]bool{
	"uploaded": true, "exists": true, "local_missing": true, "local_mismatch": true, "upload_failed": true,
}

type nasRestoreLeaseRequest struct {
	ClientVersion string `json:"client_version"`
	MaxTasks      int    `json:"max_tasks"`
}

type nasRestoreSignedRequest struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
}

type nasRestoreTask struct {
	TaskID       int64                   `json:"task_id"`
	Attempt      int                     `json:"attempt"`
	ClipID       int64                   `json:"clip_id"`
	RelativePath string                  `json:"relative_path"`
	SizeBytes    int64                   `json:"size_bytes"`
	SHA256       string                  `json:"sha256"`
	ExpiresAt    time.Time               `json:"expires_at"`
	Put          nasRestoreSignedRequest `json:"put"`
}

type nasRestoreLeaseResponse struct {
	RetryAfterSec int              `json:"retry_after_sec"`
	Tasks         []nasRestoreTask `json:"tasks"`
}

type nasRestoreResultRequest struct {
	Attempt       int        `json:"attempt"`
	Outcome       string     `json:"outcome"`
	Error         string     `json:"error"`
	BytesUploaded int64      `json:"bytes_uploaded"`
	DurationMS    int64      `json:"duration_ms"`
	StartedAt     *time.Time `json:"started_at"`
	ClientVersion string     `json:"client_version"`
}

type nasRestoreRow struct {
	ID          int64
	ClipID      int64
	ObjectKey   string
	SizeBytes   int64
	SHA256      string
	State       string
	Attempts    int
	MaxAttempts int
}

func validateNASRestoreResult(req nasRestoreResultRequest, sizeBytes int64, now time.Time) error {
	if req.Attempt < 1 {
		return errors.New("attempt must be positive")
	}
	if !nasRestoreOutcomes[req.Outcome] {
		return errors.New("invalid outcome")
	}
	if req.BytesUploaded < 0 || req.BytesUploaded > sizeBytes {
		return errors.New("bytes_uploaded must be between 0 and the clip size")
	}
	if req.DurationMS < 0 || req.DurationMS > nasRestoreMaxDuration.Milliseconds() {
		return errors.New("duration_ms must be between 0 and 86400000")
	}
	if req.StartedAt != nil && req.StartedAt.After(now.Add(connectionHeartbeatFutureSkew)) {
		return errors.New("started_at may not be in the future")
	}
	if len(req.Error) > nasRestoreMaxError {
		return errors.New("error is too long")
	}
	if req.Outcome != "uploaded" && req.Outcome != "exists" && strings.TrimSpace(req.Error) == "" {
		return errors.New("a failed outcome requires an error")
	}
	if req.ClientVersion != "" && (len(req.ClientVersion) > 64 || !relayArtifactName.MatchString(req.ClientVersion)) {
		return errors.New("invalid client_version")
	}
	return nil
}

// nasRestoreConnection resolves the caller's NAS pull connection without
// locking it: heartbeats update the same row every 30 seconds.
func (s *Server) nasRestoreConnection(ctx context.Context, w http.ResponseWriter, r *http.Request) (int64, bool) {
	return s.nasConnection(ctx, w, r, nasConnectionMetadata)
}

// handleAccountConnectionRestoreLease hands the NAS client up to max_tasks
// pending Keep Everything original restores, each with its own create-only PUT.
func (s *Server) handleAccountConnectionRestoreLease(w http.ResponseWriter, r *http.Request) {
	var req nasRestoreLeaseRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.ClientVersion != "" && (len(req.ClientVersion) > 64 || !relayArtifactName.MatchString(req.ClientVersion)) {
		util.WriteError(w, http.StatusBadRequest, "invalid client_version")
		return
	}
	if req.MaxTasks < 1 || req.MaxTasks > nasRestoreMaxLeaseTasks {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("max_tasks must be between 1 and %d", nasRestoreMaxLeaseTasks))
		return
	}
	ctx := r.Context()
	connectionID, ok := s.nasRestoreConnection(ctx, w, r)
	if !ok {
		return
	}
	if s.r2 == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "object storage is unavailable")
		return
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin restore lease failed")
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// Expired leases go back to the queue (or fail once out of attempts). A
	// late upload from the old lease is harmless: the PUT is create-only, so
	// the next attempt reports 'exists' and the API verifies those bytes.
	if _, err := tx.Exec(ctx, `
		UPDATE nas_restore_requests SET
		  state=CASE WHEN attempts>=max_attempts THEN 'failed' ELSE 'pending' END,
		  completed_at=CASE WHEN attempts>=max_attempts THEN now() END,
		  lease_expires_at=NULL, outcome='lease_expired', last_error='lease expired before a result was reported'
		WHERE connection_id=$1 AND state='leased' AND lease_expires_at<now() AND nas_restore_ke_original(target,clip_id,object_key,size_bytes,sha256,relative_path)`, connectionID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "requeue expired restores failed")
		return
	}
	rows, err := tx.Query(ctx, `
		WITH picked AS (
		  SELECT id FROM nas_restore_requests
		  WHERE connection_id=$1 AND state='pending' AND nas_restore_ke_original(target,clip_id,object_key,size_bytes,sha256,relative_path)
		    AND NOT nas_cleanup_clip_held(clip_id)
		  ORDER BY id LIMIT $2 FOR UPDATE SKIP LOCKED)
		UPDATE nas_restore_requests q SET state='leased', attempts=q.attempts+1, leased_at=now(),
		  first_leased_at=COALESCE(q.first_leased_at, now()),
		  lease_expires_at=now()+$3::bigint*interval '1 microsecond',
		  client_version=CASE WHEN $4<>'' THEN $4 ELSE q.client_version END
		FROM picked WHERE q.id=picked.id
		RETURNING q.id, q.attempts, q.clip_id, q.relative_path, q.size_bytes, q.sha256, q.lease_expires_at`,
		connectionID, req.MaxTasks, nasRestoreLeaseTTL.Microseconds(), req.ClientVersion)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lease restores failed")
		return
	}
	tasks, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (nasRestoreTask, error) {
		var t nasRestoreTask
		err := row.Scan(&t.TaskID, &t.Attempt, &t.ClipID, &t.RelativePath, &t.SizeBytes, &t.SHA256, &t.ExpiresAt)
		return t, err
	})
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "scan leased restores failed")
		return
	}
	for i := range tasks {
		if tasks[i].SizeBytes > nasRestoreMaxObjectBytes {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("restore %d exceeds the single-PUT ceiling", tasks[i].TaskID))
			return
		}
		put, err := s.r2.PresignNASRestorePut(ctx, tx, tasks[i].TaskID, nasRestoreLeaseTTL)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, "presign restore failed")
			return
		}
		headers := map[string]string{}
		for name, values := range put.Headers {
			if http.CanonicalHeaderKey(name) == "Host" || len(values) == 0 {
				continue
			}
			headers[http.CanonicalHeaderKey(name)] = values[0]
		}
		tasks[i].Put = nasRestoreSignedRequest{URL: put.URL, Method: put.Method, Headers: headers}
	}
	if err := tx.Commit(ctx); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit restore lease failed")
		return
	}
	retry := nasRestoreIdleRetrySec
	if len(tasks) > 0 {
		retry = nasRestoreBusyRetrySec
	}
	util.WriteJSON(w, http.StatusOK, nasRestoreLeaseResponse{RetryAfterSec: retry, Tasks: tasks})
}

// verifyNASRestoreObject HEADs the object and re-hashes every byte of the
// exact generation it saw. mismatch is true only when the object exists and
// differs; err covers absence and transport failures.
func (s *Server) verifyNASRestoreObject(ctx context.Context, key string, size int64, sha string) (etag string, mismatch bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, nasRestoreVerifyTimeout)
	defer cancel()
	head, err := s.r2.Head(ctx, key)
	if err != nil {
		return "", false, err
	}
	if head.SizeBytes != size {
		return head.ETag, true, fmt.Errorf("r2 object is %d bytes, expected %d", head.SizeBytes, size)
	}
	body, err := s.r2.OpenExact(ctx, key, head.ETag, head.VersionID)
	if err != nil {
		return "", false, err
	}
	defer body.Close()
	h := sha256.New()
	n, err := io.Copy(h, body)
	if err != nil {
		return "", false, err
	}
	if got := hex.EncodeToString(h.Sum(nil)); n != size || got != sha {
		return head.ETag, true, fmt.Errorf("r2 object sha256 %s (%d bytes) differs from the recorded sha256", got, n)
	}
	return head.ETag, false, nil
}

// handleAccountConnectionRestoreResult verifies and records one Keep
// Everything original restore.
func (s *Server) handleAccountConnectionRestoreResult(w http.ResponseWriter, r *http.Request) {
	taskID, ok := parseInt64Path(w, r, "taskId")
	if !ok {
		return
	}
	var req nasRestoreResultRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	// Detached from the request: a client that disconnects mid-verify must not
	// leave a verified upload unrecorded. Bounded so it ends before WriteTimeout.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), nasRestoreVerifyTimeout+nasRestoreRecordTimeout)
	defer cancel()
	connectionID, ok := s.nasRestoreConnection(ctx, w, r)
	if !ok {
		return
	}
	var row nasRestoreRow
	var keOriginal bool
	var now time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT id,clip_id,object_key,size_bytes,sha256,state,attempts,max_attempts,nas_restore_ke_original(target,clip_id,object_key,size_bytes,sha256,relative_path),now()
		FROM nas_restore_requests WHERE id=$1 AND connection_id=$2`, taskID, connectionID).Scan(
		&row.ID, &row.ClipID, &row.ObjectKey, &row.SizeBytes, &row.SHA256,
		&row.State, &row.Attempts, &row.MaxAttempts, &keOriginal, &now)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "restore not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load restore failed")
		return
	}
	if !keOriginal {
		util.WriteError(w, http.StatusConflict, "only Keep Everything original restores are live; this request is preserved")
		return
	}
	if err := validateNASRestoreResult(req, row.SizeBytes, now); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if row.State != "leased" || row.Attempts != req.Attempt {
		util.WriteError(w, http.StatusConflict, "restore lease is no longer current")
		return
	}
	if s.r2 == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "object storage is unavailable")
		return
	}

	// Verify outside any transaction: a full re-hash can take seconds.
	next, outcome, lastError, etag := "", req.Outcome, strings.TrimSpace(req.Error), ""
	switch req.Outcome {
	case "uploaded", "exists":
		var mismatch bool
		var verifyErr error
		etag, mismatch, verifyErr = s.verifyNASRestoreObject(ctx, row.ObjectKey, row.SizeBytes, row.SHA256)
		switch {
		case verifyErr == nil:
			next, lastError = "verified", ""
		case mismatch:
			// Wrong bytes at the original key. Deletes are retired: the
			// object is left untouched and the row fails for an operator.
			next, outcome, lastError = "failed", "r2_mismatch", verifyErr.Error()
		default:
			next, outcome, lastError = "retry", "verify_failed", verifyErr.Error()
		}
	case "local_missing", "local_mismatch":
		next = "failed"
	default:
		next = "retry"
	}
	if next == "retry" {
		next = "pending"
		if row.Attempts >= row.MaxAttempts {
			next = "failed"
		}
	}
	if len(lastError) > nasRestoreMaxError {
		lastError = lastError[:nasRestoreMaxError]
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin restore result failed")
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var state string
	var attempts int
	if err := tx.QueryRow(ctx, `SELECT state,attempts FROM nas_restore_requests WHERE id=$1 FOR UPDATE`, row.ID).Scan(&state, &attempts); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock restore failed")
		return
	}
	if state != "leased" || attempts != req.Attempt {
		util.WriteError(w, http.StatusConflict, "restore lease is no longer current")
		return
	}
	clipRestored := false
	if next == "verified" {
		// The identity predicates make this a no-op if the clip row changed.
		tag, err := tx.Exec(ctx, `
			UPDATE recording_clips SET purged_at=NULL
			WHERE id=$1 AND purged_at IS NOT NULL AND object_key=$2 AND size_bytes=$3 AND lower(sha256)=$4`,
			row.ClipID, row.ObjectKey, row.SizeBytes, row.SHA256)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, "restore clip failed")
			return
		}
		clipRestored = tag.RowsAffected() == 1
	}
	if _, err := tx.Exec(ctx, `
		UPDATE nas_restore_requests SET state=$2, outcome=$3, last_error=$4,
		  lease_expires_at=NULL,
		  completed_at=CASE WHEN $2 IN ('verified','failed') THEN now() END,
		  upload_started_at=$5, upload_duration_ms=$6, bytes_uploaded=$7, verified_etag=$8,
		  clip_restored_at=CASE WHEN $9 THEN now() END,
		  client_version=CASE WHEN $10<>'' THEN $10 ELSE client_version END
		WHERE id=$1`,
		row.ID, next, outcome, lastError, req.StartedAt, req.DurationMS, req.BytesUploaded, etag, clipRestored, req.ClientVersion); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "record restore result failed")
		return
	}
	if err := tx.Commit(ctx); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit restore result failed")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true, "state": next, "clip_restored": clipRestored})
}
