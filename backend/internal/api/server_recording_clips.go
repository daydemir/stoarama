package api

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/util"
)

const (
	// recordingCaptureTimeoutMarginSec and recordingUploadMarginSec extend a
	// job's lease beyond its clip duration to cover ffmpeg startup/teardown and
	// the upload. lease = clip_duration + capture margin + upload margin.
	recordingCaptureTimeoutMarginSec = 90
	recordingUploadMarginSec         = 60
	// recordingMaxBitrateBytesPerSec bounds the presigned upload size (S-4): a
	// generous 8 MB/s, so a 900s clip caps at ~7.2 GB.
	recordingMaxBitrateBytesPerSec = 8 * 1024 * 1024
)

type recordingLeaseResponse struct {
	JobID                int64     `json:"job_id"`
	RecordingID          int64     `json:"recording_id"`
	SourceURL            string    `json:"source_url"`
	ClipDurationSec      int       `json:"clip_duration_sec"`
	StorageDestinationID int64     `json:"storage_destination_id"`
	FireAt               time.Time `json:"fire_at"`
	AttemptCount         int       `json:"attempt_count"`
	LeaseExpiresAt       time.Time `json:"lease_expires_at"`
}

// handleRecordingJobsLease leases at most one due recording job for the calling
// droplet. It locks ONLY recording_jobs in the CTE (mirroring the capture lease)
// and inlines the billable predicate non-windowed (the gate VIEW cannot be used
// under FOR UPDATE). Leasing is restricted to operator-managed pool droplets:
// recorder_droplets rows are created only by the autoscaler under the operator
// account, so a self-enrolled local_recorder node (which has no such row) can
// lease nothing and therefore cannot observe another tenant's stream_url or deny
// their capture.
func (s *Server) handleRecordingJobsLease(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	workerID := strings.TrimSpace(principal.DisplayName)
	if workerID == "" {
		util.WriteError(w, http.StatusBadRequest, "worker has no display name")
		return
	}
	billingDisabled := s.billing == nil

	var resp recordingLeaseResponse
	err := s.pool.QueryRow(r.Context(), `
		WITH cte AS (
		  SELECT j.id
		  FROM recording_jobs j
		  JOIN recordings rec ON rec.id = j.recording_id
		  WHERE j.status='pending' AND j.scheduled_for <= now()
		    AND rec.status='active'
		    AND ($2 OR EXISTS (
		          SELECT 1 FROM account_billing b
		          WHERE b.account_id = rec.account_id
		            AND b.subscription_status IN ('active','trialing','past_due')
		            AND (b.subscription_status <> 'past_due' OR b.current_period_end > now())
		            AND (SELECT count(*) FROM recordings r2
		                   WHERE r2.account_id = rec.account_id AND r2.status <> 'canceled'
		                     AND (r2.created_at, r2.id) <= (rec.created_at, rec.id)) <= b.paid_quantity))
		    AND EXISTS (
		          SELECT 1 FROM recorder_droplets d
		          WHERE d.name = $1 AND d.state <> 'draining')
		  ORDER BY j.scheduled_for ASC, j.id ASC
		  LIMIT 1
		  FOR UPDATE SKIP LOCKED
		)
		UPDATE recording_jobs j
		SET status='leased',
		    lease_owner=$1,
		    lease_expires_at = now() + make_interval(secs => (j.clip_duration_sec + $3)),
		    attempt_count = attempt_count + 1,
		    updated_at = now()
		FROM cte, recordings rec
		WHERE j.id = cte.id AND rec.id = j.recording_id
		RETURNING j.id, j.recording_id, rec.stream_url, j.clip_duration_sec,
		          rec.storage_destination_id, j.fire_at, j.attempt_count, j.lease_expires_at
	`, workerID, billingDisabled, recordingCaptureTimeoutMarginSec+recordingUploadMarginSec).Scan(
		&resp.JobID, &resp.RecordingID, &resp.SourceURL, &resp.ClipDurationSec,
		&resp.StorageDestinationID, &resp.FireAt, &resp.AttemptCount, &resp.LeaseExpiresAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteJSON(w, http.StatusOK, map[string]any{"job": nil})
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("lease recording job: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"job": resp})
}

type recordingUploadIntentRequest struct {
	JobID    int64  `json:"job_id"`
	MimeType string `json:"mime_type"`
}

// handleRecordingUploadIntent presigns a PUT against the USER's bucket for a clip
// belonging to a job the caller currently holds the lease on (S-2). User S3
// credentials never leave the API; the worker only receives a presigned URL.
func (s *Server) handleRecordingUploadIntent(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if !s.requireIdempotency(w, r, "POST:/api/v1/recording/upload-intents") {
		return
	}
	if s.secrets == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "storage credential key is unset")
		return
	}
	workerID := strings.TrimSpace(principal.DisplayName)
	var req recordingUploadIntentRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.JobID <= 0 {
		util.WriteError(w, http.StatusBadRequest, "job_id is required")
		return
	}
	mimeType := strings.TrimSpace(req.MimeType)
	if mimeType == "" {
		mimeType = "video/mp4"
	}

	// Load the job + recording + destination, asserting lease ownership (S-2).
	var (
		recordingID     int64
		clipDurationSec int
		fireAt          time.Time
		destID          int64
		endpoint        string
		region          string
		bucket          string
		keyPrefix       string
		accessKeyID     string
		secretEnc       []byte
	)
	err := s.pool.QueryRow(r.Context(), `
		SELECT j.recording_id, j.clip_duration_sec, j.fire_at,
		       sd.id, sd.endpoint, sd.region, sd.bucket, sd.key_prefix, sd.access_key_id, sd.secret_access_key_enc
		FROM recording_jobs j
		JOIN recordings rec ON rec.id = j.recording_id
		JOIN storage_destinations sd ON sd.id = rec.storage_destination_id
		WHERE j.id=$1 AND j.status='leased' AND j.lease_owner=$2 AND j.lease_expires_at > now()
		  AND rec.status='active'
	`, req.JobID, workerID).Scan(
		&recordingID, &clipDurationSec, &fireAt,
		&destID, &endpoint, &region, &bucket, &keyPrefix, &accessKeyID, &secretEnc,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusConflict, "job is not leased by this worker or recording is not active")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load recording job: %v", err))
		return
	}

	secret, err := s.secrets.Decrypt(secretEnc)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("decrypt destination secret: %v", err))
		return
	}
	client, err := r2.New(r.Context(), r2.Config{
		AccessKey: accessKeyID,
		SecretKey: string(secret),
		Region:    region,
		Bucket:    bucket,
		Endpoint:  endpoint,
	})
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("build destination client: %v", err))
		return
	}

	objectKey := buildRecordingClipObjectKey(keyPrefix, recordingID, req.JobID, fireAt)
	maxSize := int64(clipDurationSec) * recordingMaxBitrateBytesPerSec
	intentID := uuid.New()
	expiresAt := time.Now().UTC().Add(s.cfg.R2SignPutTTL)

	if _, err := s.pool.Exec(r.Context(), `
		INSERT INTO recording_upload_intents
			(id, recording_id, recording_job_id, storage_destination_id, endpoint, bucket, object_key, mime_type, max_size_bytes, status, expires_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'pending',$10)
	`, intentID, recordingID, req.JobID, destID, endpoint, bucket, objectKey, mimeType, maxSize, expiresAt); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("record upload intent: %v", err))
		return
	}

	uploadURL, err := client.PresignPut(r.Context(), objectKey, mimeType, s.cfg.R2SignPutTTL)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("presign upload: %v", err))
		return
	}

	util.WriteJSON(w, http.StatusCreated, map[string]any{
		"intent_id":      intentID.String(),
		"upload_url":     uploadURL,
		"object_key":     objectKey,
		"bucket":         bucket,
		"endpoint":       endpoint,
		"content_type":   mimeType,
		"max_size_bytes": maxSize,
		"expires_at":     expiresAt,
	})
}

type recordingClipIngestRequest struct {
	IntentID     string   `json:"intent_id"`
	JobID        int64    `json:"job_id"`
	SizeBytes    int64    `json:"size_bytes"`
	ETag         string   `json:"etag"`
	SHA256       string   `json:"sha256"`
	DurationMs   int64    `json:"duration_ms"`
	VideoCodec   string   `json:"video_codec"`
	AudioCodec   string   `json:"audio_codec"`
	AudioPresent bool     `json:"audio_present"`
	ActualFPS    *float64 `json:"actual_fps"`
	Container    string   `json:"container"`
	ResolvedURL  string   `json:"resolved_url"`
	ClipStartAt  string   `json:"clip_start_at"`
	ClipEndAt    string   `json:"clip_end_at"`
}

// handleRecordingClipIngest records a successfully uploaded clip. In one tx it
// re-verifies the recording is still active, enforces the presigned size cap via
// a Head (S-4), inserts the clip (a 0-row ON CONFLICT is treated as an error,
// not silent success, S-3), consumes the intent, and resets recording health.
func (s *Server) handleRecordingClipIngest(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if s.secrets == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "storage credential key is unset")
		return
	}
	workerID := strings.TrimSpace(principal.DisplayName)
	var req recordingClipIngestRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	intentID, err := uuid.Parse(strings.TrimSpace(req.IntentID))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, "intent_id must be a uuid")
		return
	}
	clipStartAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(req.ClipStartAt))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, "clip_start_at must be RFC3339")
		return
	}
	clipEndAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(req.ClipEndAt))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, "clip_end_at must be RFC3339")
		return
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin ingest tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	// Load the intent and assert ownership via the owning job's lease (S-2).
	var (
		recordingID     int64
		jobID           int64
		destID          int64
		endpoint        string
		region          string
		bucket          string
		objectKey       string
		mimeType        string
		maxSize         int64
		fireAt          time.Time
		recordingStatus string
		accessKeyID     string
		secretEnc       []byte
	)
	err = tx.QueryRow(r.Context(), `
		SELECT ui.recording_id, ui.recording_job_id, ui.storage_destination_id, ui.endpoint, sd.region,
		       ui.bucket, ui.object_key, ui.mime_type, ui.max_size_bytes, j.fire_at, rec.status,
		       sd.access_key_id, sd.secret_access_key_enc
		FROM recording_upload_intents ui
		JOIN recording_jobs j ON j.id = ui.recording_job_id
		JOIN recordings rec ON rec.id = ui.recording_id
		JOIN storage_destinations sd ON sd.id = ui.storage_destination_id
		WHERE ui.id=$1 AND ui.status='pending'
		  AND j.status='leased' AND j.lease_owner=$2 AND j.lease_expires_at > now()
		FOR UPDATE OF ui
	`, intentID, workerID).Scan(
		&recordingID, &jobID, &destID, &endpoint, &region,
		&bucket, &objectKey, &mimeType, &maxSize, &fireAt, &recordingStatus,
		&accessKeyID, &secretEnc,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusConflict, "upload intent not found, already consumed, or job not owned")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load upload intent: %v", err))
		return
	}
	if recordingStatus == "canceled" {
		util.WriteError(w, http.StatusGone, "recording was canceled")
		return
	}

	// Enforce the presigned size cap by Head-ing the object in the user bucket (S-4).
	secret, err := s.secrets.Decrypt(secretEnc)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("decrypt destination secret: %v", err))
		return
	}
	client, err := r2.New(r.Context(), r2.Config{
		AccessKey: accessKeyID,
		SecretKey: string(secret),
		Region:    region,
		Bucket:    bucket,
		Endpoint:  endpoint,
	})
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("build destination client: %v", err))
		return
	}
	head, err := client.Head(r.Context(), objectKey)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("uploaded object not found: %v", err))
		return
	}
	if head.SizeBytes > maxSize {
		util.WriteError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("uploaded clip %d bytes exceeds cap %d bytes", head.SizeBytes, maxSize))
		return
	}
	etag := strings.TrimSpace(req.ETag)
	if etag == "" {
		etag = head.ETag
	}

	container := strings.TrimSpace(req.Container)
	if container == "" {
		container = "mp4"
	}

	var clipID int64
	err = tx.QueryRow(r.Context(), `
		INSERT INTO recording_clips
			(recording_id, recording_job_id, storage_destination_id, endpoint, bucket, object_key,
			 mime_type, container, size_bytes, etag, sha256, duration_ms, video_codec, audio_codec,
			 audio_present, actual_fps, resolved_url, fire_at, clip_start_at, clip_end_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
		ON CONFLICT (bucket, object_key) DO NOTHING
		RETURNING id
	`, recordingID, jobID, destID, endpoint, bucket, objectKey,
		mimeType, container, head.SizeBytes, etag, strings.TrimSpace(req.SHA256), req.DurationMs,
		strings.TrimSpace(req.VideoCodec), strings.TrimSpace(req.AudioCodec), req.AudioPresent, req.ActualFPS,
		strings.TrimSpace(req.ResolvedURL), fireAt, clipStartAt, clipEndAt).Scan(&clipID)
	if errors.Is(err, pgx.ErrNoRows) {
		// 0-row insert means a clip already exists for this (bucket,object_key).
		// Treat as an error so the job is NOT marked done and the dropped clip
		// is never silently lost (S-3).
		util.WriteError(w, http.StatusConflict, "a clip already exists for this object key")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("insert recording clip: %v", err))
		return
	}

	if _, err := tx.Exec(r.Context(), `
		UPDATE recording_upload_intents SET status='consumed' WHERE id=$1
	`, intentID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("consume upload intent: %v", err))
		return
	}
	if _, err := tx.Exec(r.Context(), `
		UPDATE recordings
		SET last_clip_at=now(), consecutive_failures=0, last_error_text='', updated_at=now()
		WHERE id=$1
	`, recordingID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("update recording last clip: %v", err))
		return
	}

	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit ingest tx: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"clip_id": clipID})
}

// handleRecordingJobHeartbeat extends the lease (and touches the droplet
// liveness row). It returns 409 + a cancel signal if the job was canceled or is
// no longer owned, so the worker aborts ffmpeg and skips ingest (D-inflight).
func (s *Server) handleRecordingJobHeartbeat(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	workerID := strings.TrimSpace(principal.DisplayName)

	var leaseExpiresAt time.Time
	err := s.pool.QueryRow(r.Context(), `
		UPDATE recording_jobs j
		SET lease_expires_at = now() + make_interval(secs => (j.clip_duration_sec + $3)), updated_at = now()
		WHERE j.id=$1 AND j.status='leased' AND j.lease_owner=$2
		RETURNING j.lease_expires_at
	`, id, workerID, recordingCaptureTimeoutMarginSec+recordingUploadMarginSec).Scan(&leaseExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		// Not owned / not leased anymore (canceled, reclaimed, or completed).
		util.WriteJSON(w, http.StatusConflict, map[string]any{"cancel": true})
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("heartbeat recording job: %v", err))
		return
	}
	// Touch the droplet liveness row if this worker is a managed droplet.
	_, _ = s.pool.Exec(r.Context(), `UPDATE recorder_droplets SET last_seen_at=now() WHERE name=$1`, workerID)
	util.WriteJSON(w, http.StatusOK, map[string]any{"cancel": false, "lease_expires_at": leaseExpiresAt})
}

// handleRecordingDropletHeartbeat records droplet liveness independent of any
// held job. An idle managed worker (no leased job) still calls this on its own
// ticker so the autoscaler can tell the worker is alive, not merely powered on:
// promotion to active and failed-node detection both key off last_seen_at. For a
// manual node with no recorder_droplets row this is a harmless no-op update.
func (s *Server) handleRecordingDropletHeartbeat(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	workerID := strings.TrimSpace(principal.DisplayName)
	if workerID == "" {
		util.WriteError(w, http.StatusBadRequest, "worker has no display name")
		return
	}
	if _, err := s.pool.Exec(r.Context(), `UPDATE recorder_droplets SET last_seen_at=now() WHERE name=$1`, workerID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("touch droplet liveness: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handleRecordingJobComplete marks the job done. There is no self-reschedule;
// the scheduler owns the next fire.
func (s *Server) handleRecordingJobComplete(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	workerID := strings.TrimSpace(principal.DisplayName)
	ct, err := s.pool.Exec(r.Context(), `
		UPDATE recording_jobs
		SET status='done', completed_at=now(), lease_expires_at=NULL, updated_at=now()
		WHERE id=$1 AND status='leased' AND lease_owner=$2
	`, id, workerID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("complete recording job: %v", err))
		return
	}
	if ct.RowsAffected() == 0 {
		util.WriteError(w, http.StatusConflict, "job is not leased by this worker")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

type recordingJobFailRequest struct {
	ErrorText string `json:"error_text"`
}

// handleRecordingJobFail requeues the job (status=pending, scheduled now+60s) if
// attempts remain, else marks it error, and bumps recording health fields (B-6).
func (s *Server) handleRecordingJobFail(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	workerID := strings.TrimSpace(principal.DisplayName)
	var req recordingJobFailRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	errText := strings.TrimSpace(req.ErrorText)
	if errText == "" {
		errText = "recording capture failed"
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin fail tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	var recordingID int64
	err = tx.QueryRow(r.Context(), `
		UPDATE recording_jobs j
		SET status = CASE WHEN j.attempt_count < j.max_attempts THEN 'pending' ELSE 'error' END,
		    scheduled_for = CASE WHEN j.attempt_count < j.max_attempts THEN now() + interval '60 seconds' ELSE j.scheduled_for END,
		    lease_owner = NULL,
		    lease_expires_at = NULL,
		    error_text = $3,
		    completed_at = CASE WHEN j.attempt_count < j.max_attempts THEN NULL ELSE now() END,
		    updated_at = now()
		WHERE j.id=$1 AND j.status='leased' AND j.lease_owner=$2
		RETURNING j.recording_id
	`, id, workerID, errText).Scan(&recordingID)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusConflict, "job is not leased by this worker")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("fail recording job: %v", err))
		return
	}
	if _, err := tx.Exec(r.Context(), `
		UPDATE recordings
		SET consecutive_failures = consecutive_failures + 1, last_error_text=$2, last_error_at=now(), updated_at=now()
		WHERE id=$1
	`, recordingID, errText); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("bump recording health: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit fail tx: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// buildRecordingClipObjectKey is deterministic by the cron fire instant, so a
// re-leased fire overwrites the same object (at most one clip per fire) and the
// ON CONFLICT (bucket,object_key) dedup is real (D-clip-key / D-reclaim-dupe).
func buildRecordingClipObjectKey(keyPrefix string, recordingID, jobID int64, fireAt time.Time) string {
	parts := make([]string, 0, 5)
	// Defense in depth: key_prefix is validated by sanitizeStorageKeyPrefix at
	// storage-destination create time, but destinations created before that
	// guard existed may carry unsafe segments. Drop any empty, "." or ".."
	// segment so a stored prefix can never traverse out of the recordings
	// namespace, regardless of what is in the DB.
	for _, seg := range strings.Split(strings.Trim(strings.TrimSpace(keyPrefix), "/"), "/") {
		if seg == "" || seg == "." || seg == ".." {
			continue
		}
		parts = append(parts, seg)
	}
	parts = append(parts,
		"recordings",
		fmt.Sprintf("%d", recordingID),
		fmt.Sprintf("%d", jobID),
		fmt.Sprintf("%d.mp4", fireAt.UTC().UnixMilli()),
	)
	return strings.Join(parts, "/")
}
