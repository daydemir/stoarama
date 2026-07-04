package api

import (
	"errors"
	"fmt"
	"log"
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
	// recordingFreshnessGraceSec is the slack added to a job's clip duration to form
	// its schedule-integrity freshness window: a job must be leasable within
	// fire_at + clip_duration_sec + this grace, or it is an honest miss rather than a
	// silently-wrong late capture (capture has no seek-to-fire, so a late clip is the
	// wrong content). This is the single source of truth for the window; the
	// scheduler's miss-marking sweep uses the same value. The grace covers normal
	// lease/poll latency and a brief autoscaler cold-boot, never minutes.
	recordingFreshnessGraceSec = 30
)

// recorderWorkerID is the canonical lease_owner string for a recorder principal.
// For a relay node (node_type='relay') it is the server-derived 'node:{id}', which
// is spoof-proof (the id comes from the token lookup, never client input) and cannot
// collide with a user-chosen display name across accounts. For a cloud droplet
// (node_type='local_recorder') it is the operator-assigned display name, unchanged,
// so droplet lease/complete/ingest/heartbeat ownership is byte-identical to before.
func recorderWorkerID(principal nodePrincipal) string {
	if principal.NodeType == nodeTypeRelay {
		return fmt.Sprintf("node:%d", principal.NodeID)
	}
	return strings.TrimSpace(principal.DisplayName)
}

type recordingLeaseResponse struct {
	JobID                int64     `json:"job_id"`
	RecordingID          int64     `json:"recording_id"`
	SourceURL            string    `json:"source_url"`
	ClipDurationSec      int       `json:"clip_duration_sec"`
	StorageDestinationID int64     `json:"storage_destination_id"`
	FireAt               time.Time `json:"fire_at"`
	AttemptCount         int       `json:"attempt_count"`
	LeaseExpiresAt       time.Time `json:"lease_expires_at"`
	TargetFPS            *int      `json:"target_fps"`
	// Kind is 'clip' (default, per-cron-fire) or 'continuous_window' (one window-
	// long lease driving back-to-back segment capture). WindowEndAt is the
	// continuous window's close instant (zero for a clip job).
	Kind        string     `json:"kind"`
	WindowEndAt *time.Time `json:"window_end_at"`
}

// relayLeaseSQL is the relay branch of handleRecordingJobsLease, entered only for a
// node_type='relay' principal. It mirrors the cloud branch's capture gate but swaps
// the recorder_droplets liveness EXISTS for account-scoped relay-node liveness plus a
// per-node capacity bound, and partitions to capture_via='relay' recordings only.
//
// Security: n.id=$1 is the authenticated node id from the token lookup (never request
// input) and n.account_id=rec.account_id is the tenant wall, both enforced in SQL, so
// a relay can only ever lease its own account's relay recordings.
//
// Capacity ($1's active leases < n.relay_max_streams) is BEST-EFFORT / soft: FOR
// UPDATE SKIP LOCKED locks only the one candidate job row, while this COUNT(*) reads
// unlocked rows, so two concurrent lease calls sharing one node id can both pass and
// over-subscribe by a few. The AUTHORITATIVE bound is the relay worker's client-side
// semaphore (Concurrency = relay_max_streams); this subquery is a soft guard, not a
// race-proof cap. Params: $1=NodeID, $2=billingDisabled, $3=margin, $4=freshnessGrace.
const relayLeaseSQL = `
	WITH cte AS (
	  SELECT j.id
	  FROM recording_jobs j
	  JOIN recordings rec ON rec.id = j.recording_id
	  JOIN nodes n ON n.id = $1
	    AND n.account_id = rec.account_id
	    AND n.node_type = 'relay'
	    AND n.status = 'active'
	    AND n.last_heartbeat_at >= now() - interval '120 seconds'
	  WHERE j.status = 'pending'
	    AND j.scheduled_for <= now()
	    AND (j.kind = 'continuous_window'
	         OR j.fire_at + make_interval(secs => (j.clip_duration_sec + $4)) > now())
	    AND rec.status = 'active'
	    AND rec.start_at <= now()
	    AND (rec.end_at IS NULL OR now() < rec.end_at)
	    AND rec.capture_via = 'relay'
	    AND ($2 OR EXISTS (
	          SELECT 1 FROM account_billing b
	          WHERE b.account_id = rec.account_id
	            AND b.has_payment_method))
	    -- Best-effort capacity bound (soft): the client semaphore is authoritative.
	    AND (SELECT COUNT(*) FROM recording_jobs aj
	         WHERE aj.status = 'leased'
	           AND aj.lease_owner = 'node:' || $1::text
	           AND aj.lease_expires_at > now()) < n.relay_max_streams
	  ORDER BY j.scheduled_for ASC, j.id ASC
	  LIMIT 1
	  FOR UPDATE SKIP LOCKED
	)
	UPDATE recording_jobs j
	SET status = 'leased',
	    lease_owner = 'node:' || $1::text,
	    lease_expires_at = now() + make_interval(secs => (j.clip_duration_sec + $3)),
	    attempt_count = attempt_count + 1,
	    updated_at = now()
	FROM cte, recordings rec
	WHERE j.id = cte.id AND rec.id = j.recording_id
	RETURNING j.id, j.recording_id, rec.stream_url, j.clip_duration_sec,
	          rec.storage_destination_id, j.fire_at, j.attempt_count, j.lease_expires_at,
	          rec.target_fps, j.kind, j.window_end_at
`

// handleRecordingJobsLease leases at most one due recording job for the calling
// droplet. It locks ONLY recording_jobs in the CTE (mirroring the capture lease)
// and inlines the capture gate (status='active', window open, and, when billing is
// enabled, account has a card on file), matching the scheduler/forecast gate.
// Leasing is restricted to operator-managed pool droplets: recorder_droplets rows
// are created only by the autoscaler under the operator account, so a self-enrolled
// local_recorder node (which has no such row) can lease nothing and therefore
// cannot observe another tenant's stream_url or deny their capture.
func (s *Server) handleRecordingJobsLease(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	workerID := recorderWorkerID(principal)
	if workerID == "" {
		util.WriteError(w, http.StatusBadRequest, "worker has no display name")
		return
	}
	billingDisabled := s.billing == nil
	margin := recordingCaptureTimeoutMarginSec + recordingUploadMarginSec

	var resp recordingLeaseResponse
	var err error
	if principal.NodeType == nodeTypeRelay {
		// Relay branch (node_type='relay' only). The tenant wall (n.account_id =
		// rec.account_id) and the capture_via='relay' partition live entirely in SQL;
		// n.id is the authenticated principal's node id (token lookup), never request
		// input, so a relay can never lease another account's or a cloud recording's job.
		// $1=NodeID, $2=billingDisabled, $3=margin, $4=freshnessGrace.
		err = s.pool.QueryRow(r.Context(), relayLeaseSQL,
			principal.NodeID, billingDisabled, margin, recordingFreshnessGraceSec).Scan(
			&resp.JobID, &resp.RecordingID, &resp.SourceURL, &resp.ClipDurationSec,
			&resp.StorageDestinationID, &resp.FireAt, &resp.AttemptCount, &resp.LeaseExpiresAt,
			&resp.TargetFPS, &resp.Kind, &resp.WindowEndAt,
		)
	} else {
		err = s.pool.QueryRow(r.Context(), `
		WITH cte AS (
		  SELECT j.id
		  FROM recording_jobs j
		  JOIN recordings rec ON rec.id = j.recording_id
		  WHERE j.status='pending' AND j.scheduled_for <= now()
		    -- Schedule integrity (SAMPLED clip jobs only): never hand out a clip job
		    -- too late to be on-schedule. Capture has no seek-to-fire, so a clip job
		    -- leased past its fire_at + clip_duration + grace would record the stream
		    -- as-it-is-now (the wrong content) and store it as if it ran on schedule;
		    -- such a job is left for the scheduler's miss-marking sweep instead. A
		    -- continuous_window job's fire_at is the window-open instant (which may be
		    -- many minutes ago by design), so the freshness deadline is bypassed for it.
		    AND (j.kind='continuous_window'
		         OR j.fire_at + make_interval(secs => (j.clip_duration_sec + $4)) > now())
		    AND rec.status='active'
		    AND rec.start_at <= now()
		    AND (rec.end_at IS NULL OR now() < rec.end_at)
		    -- Cloud droplets never lease relay recordings (they would fail at yt-dlp on a
		    -- datacenter IP). capture_via is NOT NULL DEFAULT 'cloud', so every existing
		    -- row matches and this predicate is dark until a relay recording exists.
		    AND rec.capture_via = 'cloud'
		    AND ($2 OR EXISTS (
		          SELECT 1 FROM account_billing b
		          WHERE b.account_id = rec.account_id
		            AND b.has_payment_method))
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
		          rec.storage_destination_id, j.fire_at, j.attempt_count, j.lease_expires_at,
		          rec.target_fps, j.kind, j.window_end_at
	`, workerID, billingDisabled, margin, recordingFreshnessGraceSec).Scan(
			&resp.JobID, &resp.RecordingID, &resp.SourceURL, &resp.ClipDurationSec,
			&resp.StorageDestinationID, &resp.FireAt, &resp.AttemptCount, &resp.LeaseExpiresAt,
			&resp.TargetFPS, &resp.Kind, &resp.WindowEndAt,
		)
	}
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
	// SegmentStartMs, when > 0, is the UTC start instant (Unix millis) of a
	// continuous-capture segment. The per-segment object key is derived from it so
	// each back-to-back segment of one window job gets a unique, ordered,
	// idempotent key (a re-leased window overwrites the same per-second key). It is
	// 0/ignored for an ordinary clip job, which keys off the job's fire_at.
	SegmentStartMs int64 `json:"segment_start_ms"`
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
	workerID := recorderWorkerID(principal)
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

	// Load the job + recording + destination, asserting lease ownership (S-2). A
	// continuous_window job is leased window-long by this worker, so the same
	// ownership predicate covers each per-segment intent it raises.
	var (
		recordingID     int64
		clipDurationSec int
		fireAt          time.Time
		jobKind         string
		destID          int64
		endpoint        string
		region          string
		bucket          string
		keyPrefix       string
		accessKeyID     string
		secretEnc       []byte
	)
	err := s.pool.QueryRow(r.Context(), `
		SELECT j.recording_id, j.clip_duration_sec, j.fire_at, j.kind,
		       sd.id, sd.endpoint, sd.region, sd.bucket, sd.key_prefix, sd.access_key_id, sd.secret_access_key_enc
		FROM recording_jobs j
		JOIN recordings rec ON rec.id = j.recording_id
		JOIN storage_destinations sd ON sd.id = rec.storage_destination_id
		WHERE j.id=$1 AND j.status='leased' AND j.lease_owner=$2 AND j.lease_expires_at > now()
		  AND rec.status='active'
	`, req.JobID, workerID).Scan(
		&recordingID, &clipDurationSec, &fireAt, &jobKind,
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

	// A continuous_window job raises many per-segment intents under one lease, so
	// its object key is keyed on the SEGMENT START (unique, ordered, idempotent),
	// not the single window-open fire_at. A clip job keys on fire_at as before.
	var objectKey string
	if jobKind == "continuous_window" {
		if req.SegmentStartMs <= 0 {
			util.WriteError(w, http.StatusBadRequest, "segment_start_ms is required for a continuous window job")
			return
		}
		segStart := time.UnixMilli(req.SegmentStartMs).UTC()
		objectKey = buildRecordingClipObjectKeyContinuous(keyPrefix, recordingID, segStart)
	} else {
		objectKey = buildRecordingClipObjectKey(keyPrefix, recordingID, req.JobID, fireAt)
	}
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
	workerID := recorderWorkerID(principal)
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
		jobKind         string
		windowEndAt     *time.Time
		clipDurationSec int
		recordingStatus string
		accessKeyID     string
		secretEnc       []byte
	)
	err = tx.QueryRow(r.Context(), `
		SELECT ui.recording_id, ui.recording_job_id, ui.storage_destination_id, ui.endpoint, sd.region,
		       ui.bucket, ui.object_key, ui.mime_type, ui.max_size_bytes, j.fire_at,
		       j.kind, j.window_end_at, j.clip_duration_sec, rec.status,
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
		&bucket, &objectKey, &mimeType, &maxSize, &fireAt,
		&jobKind, &windowEndAt, &clipDurationSec, &recordingStatus,
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

	// Ingest sanity check (log only, never reject): a clip whose start lands far outside
	// its job's capture window signals a timezone bug on the recorder (e.g. a relay that
	// wrote segment strftime names in local time instead of UTC), which would otherwise
	// silently store misaligned clips. Window = [fire_at, window_end_at] for a
	// continuous_window job, else [fire_at, fire_at+clip_duration_sec]. A 15-minute slop
	// tolerates normal capture/upload latency while catching whole-hour UTC offsets.
	windowStart := fireAt
	windowEnd := fireAt.Add(time.Duration(clipDurationSec) * time.Second)
	if jobKind == "continuous_window" && windowEndAt != nil {
		windowEnd = *windowEndAt
	}
	const ingestWindowSlop = 15 * time.Minute
	if clipStartAt.Before(windowStart.Add(-ingestWindowSlop)) || clipStartAt.After(windowEnd.Add(ingestWindowSlop)) {
		log.Printf("WARNING clip ingest window sanity: clip_start_at=%s is outside job window [%s, %s] by >15m recording=%d job=%d worker=%q kind=%s (likely a recorder timezone bug)",
			clipStartAt.UTC().Format(time.RFC3339), windowStart.UTC().Format(time.RFC3339), windowEnd.UTC().Format(time.RFC3339),
			recordingID, jobID, workerID, jobKind)
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

	// Fallback so a probe-miss clip (recorder sent duration_ms<=0) still records a
	// real duration derived from the clip's own validated start/end span, mirroring
	// the legacy capture path. Metering never reads duration_ms (it uses the
	// timestamps), so this only corrects the stored/displayed duration.
	durationMs := req.DurationMs
	if durationMs <= 0 {
		durationMs = clipEndAt.Sub(clipStartAt).Milliseconds()
		if durationMs < 0 {
			durationMs = 0
		}
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
		mimeType, container, head.SizeBytes, etag, strings.TrimSpace(req.SHA256), durationMs,
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

	// Auto-enqueue a delivery transfer for a WebDAV recording. The clip was captured
	// into the account's managed staging area; if the recording has a delivery
	// destination, queue a clip_transfer_job to it (reusing the exact transfer
	// machinery as the user-initiated copy) with auto_purge_source=true so the worker
	// purges the managed staging copy after a confirmed delivery. ON CONFLICT DO
	// NOTHING keeps a retried ingest idempotent (the idempotency_key dedups on
	// clip+target). Only WebDAV recordings have a non-NULL delivery dest, so ordinary
	// recordings skip this entirely (no transfer, no purge).
	var (
		deliveryDestID *int64
		deliveryPrefix string
	)
	if err := tx.QueryRow(r.Context(), `
		SELECT rec.delivery_storage_destination_id, COALESCE(sd.key_prefix, '')
		FROM recordings rec
		LEFT JOIN storage_destinations sd ON sd.id = rec.delivery_storage_destination_id
		WHERE rec.id=$1
	`, recordingID).Scan(&deliveryDestID, &deliveryPrefix); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load delivery destination: %v", err))
		return
	}
	if deliveryDestID != nil {
		targetObjectKey := buildClipTransferObjectKey(deliveryPrefix, recordingID, clipID, objectKey)
		idempotencyKey := fmt.Sprintf("xfer:%d:%d", clipID, *deliveryDestID)
		if _, err := tx.Exec(r.Context(), `
			INSERT INTO clip_transfer_jobs
				(account_id, recording_clip_id, target_storage_destination_id, target_object_key, idempotency_key, auto_purge_source)
			SELECT rec.account_id, $1, $2, $3, $4, true
			FROM recordings rec WHERE rec.id=$5
			ON CONFLICT (idempotency_key) DO NOTHING
		`, clipID, *deliveryDestID, targetObjectKey, idempotencyKey, recordingID); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("enqueue delivery transfer: %v", err))
			return
		}
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
	workerID := recorderWorkerID(principal)

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
	workerID := recorderWorkerID(principal)
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
	workerID := recorderWorkerID(principal)
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
	workerID := recorderWorkerID(principal)
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

// buildRecordingClipObjectKeyContinuous keys a continuous-capture segment on its
// per-second start instant rather than a single fire+job, so every back-to-back
// segment of one window gets a unique, ordered key and a re-leased window
// overwrites the same key (idempotent re-capture; ON CONFLICT(bucket,object_key)
// dedups). It deliberately omits the job id so a re-leased window job (a new job
// id after reclaim) lands on the SAME key for the same wall-clock second.
func buildRecordingClipObjectKeyContinuous(keyPrefix string, recordingID int64, segStart time.Time) string {
	parts := make([]string, 0, 5)
	for _, seg := range strings.Split(strings.Trim(strings.TrimSpace(keyPrefix), "/"), "/") {
		if seg == "" || seg == "." || seg == ".." {
			continue
		}
		parts = append(parts, seg)
	}
	parts = append(parts,
		"recordings",
		fmt.Sprintf("%d", recordingID),
		"continuous",
		fmt.Sprintf("%d.mp4", segStart.UTC().Unix()),
	)
	return strings.Join(parts, "/")
}

// accountClipsCursorSQL forward-cursors the calling account's still-org-visible
// clips by the monotonic recording_clips.id (BIGSERIAL), so a NAS pull client can
// drain every clip exactly once and resume from its last seen id. object_key is
// never selected: the caller gets a download_path to the existing presign endpoint
// instead. Released clips (already pulled/detached) and purged clips are both
// excluded, so the working set stays small: the NAS releases each clip right after
// it pulls it, and an ordered id>cursor scan over the active partial index is cheap.
// ONLY nas_pull recordings' clips are ever handed out (r.delivery='nas_pull'), so a
// managed recording's clips can never be released by the NAS client. A commit
// watermark (accountClipsCommitWatermark) additionally hides very fresh clips until
// no older-id ingest tx can still be uncommitted.
//
// The commit-watermark interval: clip ids are BIGSERIAL, allocated at INSERT but
// only VISIBLE at COMMIT, so a raw id>cursor scan can skip a lower id whose ingest
// tx committed AFTER a higher id it already handed out (the client would advance its
// cursor past the lower id and never see it). Only offering clips whose created_at is
// at least this far in the past guarantees no older-id ingest tx can still be
// uncommitted. Clips are ~60s and the poll cadence is ~60s, so this latency is
// negligible.
const accountClipsCommitWatermark = `interval '90 seconds'`

const accountClipsCursorSQL = `
	SELECT c.id, c.recording_id, c.size_bytes, c.clip_start_at, c.clip_end_at
	FROM recording_clips c
	JOIN recordings r ON r.id = c.recording_id
	WHERE r.account_id = $1 AND c.purged_at IS NULL AND c.released_at IS NULL
	  AND r.delivery = 'nas_pull'
	  AND c.created_at < now() - ` + accountClipsCommitWatermark + `
	  AND c.id > $2
	ORDER BY c.id ASC
	LIMIT $3
`

// handleAccountClips returns one forward-cursored page of the calling account's
// unpurged clips, ordered by the monotonic clip id, for the NAS pull client.
// It is mounted under requireAccountAuth so a Bearer sir_ account API key can
// drain it. Each row carries a download_path to the existing per-recording clip
// download endpoint; object_key is never exposed. next_after_id is the max clip
// id in the page (the client's next cursor), or null when the page is empty.
func (s *Server) handleAccountClips(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	afterID := int64(parseIntQuery(r, "after_id", 0, 0, 1<<62))
	limit := parseIntQuery(r, "limit", 100, 1, 500)

	rows, err := s.pool.Query(r.Context(), accountClipsCursorSQL, principal.AccountID, afterID, limit)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list account clips: %v", err))
		return
	}
	defer rows.Close()

	clips := make([]map[string]any, 0, limit)
	var nextAfterID *int64
	for rows.Next() {
		var (
			clipID      int64
			recordingID int64
			sizeBytes   int64
			clipStartAt time.Time
			clipEndAt   *time.Time
		)
		if err := rows.Scan(&clipID, &recordingID, &sizeBytes, &clipStartAt, &clipEndAt); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan account clip: %v", err))
			return
		}
		var endAt any
		if clipEndAt != nil {
			endAt = clipEndAt.UTC()
		}
		clips = append(clips, map[string]any{
			"clip_id":       clipID,
			"recording_id":  recordingID,
			"size_bytes":    sizeBytes,
			"clip_start_at": clipStartAt.UTC(),
			"clip_end_at":   endAt,
			"download_path": fmt.Sprintf("/api/v1/account/recordings/%d/clips/%d/download", recordingID, clipID),
		})
		id := clipID
		nextAfterID = &id
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate account clips: %v", err))
		return
	}

	util.WriteJSON(w, http.StatusOK, map[string]any{
		"clips":         clips,
		"next_after_id": nextAfterID,
	})
}
