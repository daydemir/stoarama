package api

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/netguard"
	"github.com/daydemir/stoarama/backend/internal/recsched"
	"github.com/daydemir/stoarama/backend/internal/util"
)

// recordingProbeTimeout bounds the create-time ffmpeg reachability probe.
const recordingProbeTimeout = 8 * time.Second

// recordingResolveTimeout bounds the create/probe-time stream reference
// resolution (an HTTP fetch for indirect '!hls' sources; a passthrough for
// direct .m3u8).
const recordingResolveTimeout = 30 * time.Second

type recordingProbeRequest struct {
	StreamURL string `json:"stream_url"`
}

type recordingCreateRequest struct {
	Name      string `json:"name"`
	StreamURL string `json:"stream_url"`
	// StreamID, when set, links the recording to a catalog stream. The catalog's
	// source_url is then used as the stored stream_url (the stable reference the
	// worker re-resolves each fire); any client-sent stream_url is ignored.
	StreamID             *int64 `json:"stream_id"`
	StorageDestinationID int64  `json:"storage_destination_id"`
	CronExpr             string     `json:"cron_expr"`
	CronTimezone         string     `json:"cron_timezone"`
	ClipDurationSec      int        `json:"clip_duration_sec"`
	// Capture window. StartAt defaults to now() (start immediately); EndAt is
	// open-ended when nil. When both are set, EndAt must be strictly after StartAt.
	StartAt *time.Time `json:"start_at"`
	EndAt   *time.Time `json:"end_at"`
}

func (s *Server) handleAccountRecordingsList(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	rows, err := s.pool.Query(r.Context(), `
		SELECT
			rec.id, rec.name, rec.stream_url, rec.storage_destination_id, sd.name,
			rec.source_kind, rec.cron_expr, rec.cron_timezone, rec.clip_duration_sec,
			rec.status, rec.start_at, rec.end_at, rec.next_fire_at, rec.last_clip_at,
			rec.last_error_text, rec.last_error_at, rec.consecutive_failures,
			COALESCE((SELECT b.has_payment_method FROM account_billing b
			   WHERE b.account_id = rec.account_id), false) AS has_payment_method,
			(SELECT count(*) FROM recording_clips c
			   WHERE c.recording_id = rec.id AND c.clip_start_at > now() - interval '24 hours') AS recent_clip_count,
			rec.created_at, sd.managed,
			rec.stream_id, st.name, st.location_text
		FROM recordings rec
		JOIN storage_destinations sd ON sd.id = rec.storage_destination_id
		LEFT JOIN streams st ON st.id = rec.stream_id
		WHERE rec.account_id=$1 AND rec.status <> 'canceled'
		ORDER BY rec.created_at DESC, rec.id DESC
	`, principal.AccountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list recordings: %v", err))
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0, 8)
	for rows.Next() {
		item, err := scanRecordingListRow(rows, s.billing != nil)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan recording: %v", err))
			return
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate recordings: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) handleAccountRecordingsCreate(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var req recordingCreateRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		util.WriteError(w, http.StatusBadRequest, "name is required")
		return
	}
	// When linked to a catalog stream, the catalog's source_url is authoritative:
	// we store the stable catalog reference (re-resolved fresh each fire) so tokens
	// never expire mid-schedule. Any client-sent stream_url is ignored in this case.
	var streamIDArg any
	streamURL := strings.TrimSpace(req.StreamURL)
	if req.StreamID != nil {
		if *req.StreamID <= 0 {
			util.WriteError(w, http.StatusBadRequest, "stream_id is invalid")
			return
		}
		var catalogURL string
		err := s.pool.QueryRow(r.Context(), `SELECT source_url FROM streams WHERE id=$1`, *req.StreamID).Scan(&catalogURL)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				util.WriteError(w, http.StatusNotFound, "catalog stream not found")
				return
			}
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load catalog stream: %v", err))
			return
		}
		streamURL = strings.TrimSpace(catalogURL)
		if streamURL == "" {
			util.WriteError(w, http.StatusBadRequest, "catalog stream has no source_url")
			return
		}
		streamIDArg = *req.StreamID
	}
	if streamURL == "" {
		util.WriteError(w, http.StatusBadRequest, "stream_url is required")
		return
	}
	if req.StorageDestinationID <= 0 {
		util.WriteError(w, http.StatusBadRequest, "storage_destination_id is required")
		return
	}
	cronExpr := strings.TrimSpace(req.CronExpr)
	cronTimezone := strings.TrimSpace(req.CronTimezone)
	if cronTimezone == "" {
		cronTimezone = "UTC"
	}
	clipDuration := req.ClipDurationSec
	if clipDuration == 0 {
		clipDuration = 60
	}
	if clipDuration < 5 || clipDuration > 900 {
		util.WriteError(w, http.StatusBadRequest, "clip_duration_sec must be between 5 and 900")
		return
	}

	// Capture window: start_at defaults to now() (start immediately), end_at is
	// open-ended when nil. When both are present, end_at must be strictly after
	// start_at (mirrors the recordings_window_chk DB constraint).
	startAt := time.Now().UTC()
	if req.StartAt != nil {
		startAt = req.StartAt.UTC()
	}
	var endAtArg any
	if req.EndAt != nil {
		endAt := req.EndAt.UTC()
		if !endAt.After(startAt) {
			util.WriteError(w, http.StatusBadRequest, "end_at must be after start_at")
			return
		}
		endAtArg = endAt
	}

	// Resolve the pasted reference (e.g. a KBS '!hls' indirect URL) to the live
	// playable URL before validating/probing, so a reference ffmpeg cannot open
	// directly can still be scheduled. The raw reference is what gets stored
	// (below); the worker re-resolves it fresh on every capture.
	resolvedForProbe, err := resolveRecordingStreamURL(r.Context(), streamURL)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	// S-1: SSRF guard + HLS/HTTPS classify on the resolved URL before it ever
	// reaches ffmpeg. This is the validate half of the shared validate+probe path.
	validatedIP, sourceKind, err := validateRecordingStreamURL(resolvedForProbe)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Cron + timezone + min-interval + clip-vs-interval invariants.
	if err := recsched.ValidateCronForCreate(cronExpr, cronTimezone, s.cfg.RecSchedMinIntervalSec, clipDuration); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Verify the destination belongs to this account and is verified (S-IDOR).
	var destStatus string
	err = s.pool.QueryRow(r.Context(), `
		SELECT status FROM storage_destinations WHERE id=$1 AND account_id=$2
	`, req.StorageDestinationID, principal.AccountID).Scan(&destStatus)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusNotFound, "storage destination not found")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load storage destination: %v", err))
		return
	}
	if destStatus != "verified" {
		util.WriteError(w, http.StatusBadRequest, "storage destination is not verified")
		return
	}

	// Reachability probe pinned to the validated IP, so the probe socket cannot
	// be redirected by a DNS rebind between validation above and connect time.
	if err := probeRecordingStreamReachable(r.Context(), resolvedForProbe, validatedIP); err != nil {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("stream not reachable: %v", err))
		return
	}

	nextFire, err := recsched.NextFireUTC(cronExpr, cronTimezone, time.Now().UTC())
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	var nextFireArg any
	if !nextFire.IsZero() {
		nextFireArg = nextFire
	}

	var nameExists bool
	if err := s.pool.QueryRow(r.Context(), `
		SELECT EXISTS(SELECT 1 FROM recordings WHERE account_id=$1 AND lower(name)=lower($2) AND status <> 'canceled')
	`, principal.AccountID, name).Scan(&nameExists); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("check recording name: %v", err))
		return
	}
	if nameExists {
		util.WriteError(w, http.StatusConflict, "a recording with that name already exists")
		return
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin create tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	var (
		id        int64
		createdAt time.Time
		startOut  time.Time
		endOut    *time.Time
	)
	err = tx.QueryRow(r.Context(), `
		INSERT INTO recordings
			(account_id, storage_destination_id, name, stream_url, stream_id, source_kind, cron_expr, cron_timezone, clip_duration_sec, status, next_fire_at, start_at, end_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'active',$10,$11,$12)
		RETURNING id, created_at, start_at, end_at
	`, principal.AccountID, req.StorageDestinationID, name, streamURL, streamIDArg, sourceKind, cronExpr, cronTimezone, clipDuration, nextFireArg, startAt, endAtArg).Scan(&id, &createdAt, &startOut, &endOut)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			util.WriteError(w, http.StatusConflict, "a recording with that name already exists")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("create recording: %v", err))
		return
	}

	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit create tx: %v", err))
		return
	}

	_ = s.insertAccountAuthEvent(r.Context(), principal.AccountID, nil, "recording_created", "account", principal.Email, map[string]any{
		"recording_id":      id,
		"name":              name,
		"storage_dest_id":   req.StorageDestinationID,
		"source_kind":       sourceKind,
		"cron_expr":         cronExpr,
		"clip_duration_sec": clipDuration,
	})

	// Create just inserts: capture is held by the scheduler gate until a card is on
	// file (the list payload surfaces needs_card), and the new usage model never
	// charges at start, so there is no checkout_url here.
	util.WriteJSON(w, http.StatusCreated, map[string]any{
		"id":         id,
		"status":     "active",
		"created_at": createdAt.UTC(),
		"start_at":   startOut.UTC(),
		"end_at":     endOut,
	})
}

// handleAccountRecordingsProbe authoritatively validates a stream URL the same
// way create does (SSRF guard -> HLS/HTTPS classify -> IP-pinned ffmpeg
// reachability probe) so the frontend can verify a source before/while creating
// a recording. Because it shells ffmpeg on our dyno against a user-supplied URL,
// the SSRF guard is mandatory. A guard/classify/probe failure returns 200 with
// {"ok":false,"error":...} so the UI can show the reason inline; 4xx is reserved
// for malformed requests / a missing stream_url.
func (s *Server) handleAccountRecordingsProbe(w http.ResponseWriter, r *http.Request) {
	if _, ok := accountPrincipalFromContext(r.Context()); !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var req recordingProbeRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	streamURL := strings.TrimSpace(req.StreamURL)
	if streamURL == "" {
		util.WriteError(w, http.StatusBadRequest, "stream_url is required")
		return
	}

	resolved, err := resolveRecordingStreamURL(r.Context(), streamURL)
	if err != nil {
		util.WriteJSON(w, http.StatusOK, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	validatedIP, sourceKind, err := validateRecordingStreamURL(resolved)
	if err != nil {
		util.WriteJSON(w, http.StatusOK, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if err := probeRecordingStreamReachable(r.Context(), resolved, validatedIP); err != nil {
		util.WriteJSON(w, http.StatusOK, map[string]any{"ok": false, "error": fmt.Sprintf("stream not reachable: %v", err)})
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true, "source_kind": sourceKind, "resolved_url": resolved})
}

func (s *Server) handleAccountRecordingGet(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	row := s.pool.QueryRow(r.Context(), `
		SELECT
			rec.id, rec.name, rec.stream_url, rec.storage_destination_id, sd.name,
			rec.source_kind, rec.cron_expr, rec.cron_timezone, rec.clip_duration_sec,
			rec.status, rec.start_at, rec.end_at, rec.next_fire_at, rec.last_clip_at,
			rec.last_error_text, rec.last_error_at, rec.consecutive_failures,
			COALESCE((SELECT b.has_payment_method FROM account_billing b
			   WHERE b.account_id = rec.account_id), false) AS has_payment_method,
			(SELECT count(*) FROM recording_clips c
			   WHERE c.recording_id = rec.id AND c.clip_start_at > now() - interval '24 hours') AS recent_clip_count,
			rec.created_at, sd.managed,
			rec.stream_id, st.name, st.location_text
		FROM recordings rec
		JOIN storage_destinations sd ON sd.id = rec.storage_destination_id
		LEFT JOIN streams st ON st.id = rec.stream_id
		WHERE rec.id=$1 AND rec.account_id=$2 AND rec.status <> 'canceled'
	`, id, principal.AccountID)
	item, err := scanRecordingListRow(row, s.billing != nil)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusNotFound, "recording not found")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load recording: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, item)
}

// handleAccountRecordingClips returns the per-clip rows for one recording owned
// by the session account, newest fire first. Ownership is enforced by a SELECT
// scoped to account_id before any clips are read (404 when the recording is not
// the caller's). The list is capped; total is the unbounded count.
func (s *Server) handleAccountRecordingClips(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}

	var ownerOK bool
	err := s.pool.QueryRow(r.Context(), `
		SELECT EXISTS(SELECT 1 FROM recordings WHERE id=$1 AND account_id=$2 AND status <> 'canceled')
	`, id, principal.AccountID).Scan(&ownerOK)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load recording: %v", err))
		return
	}
	if !ownerOK {
		util.WriteError(w, http.StatusNotFound, "recording not found")
		return
	}

	var total int64
	if err := s.pool.QueryRow(r.Context(), `
		SELECT count(*) FROM recording_clips WHERE recording_id=$1
	`, id).Scan(&total); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("count clips: %v", err))
		return
	}

	rows, err := s.pool.Query(r.Context(), `
		SELECT fire_at, clip_start_at, clip_end_at, size_bytes, duration_ms, object_key
		FROM recording_clips
		WHERE recording_id=$1
		ORDER BY fire_at DESC
		LIMIT 200
	`, id)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list clips: %v", err))
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0, 16)
	for rows.Next() {
		var (
			fireAt      time.Time
			clipStartAt time.Time
			clipEndAt   time.Time
			sizeBytes   int64
			durationMs  int64
			objectKey   string
		)
		if err := rows.Scan(&fireAt, &clipStartAt, &clipEndAt, &sizeBytes, &durationMs, &objectKey); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan clip: %v", err))
			return
		}
		items = append(items, map[string]any{
			"fire_at":       fireAt.UTC(),
			"clip_start_at": clipStartAt.UTC(),
			"clip_end_at":   clipEndAt.UTC(),
			"size_bytes":    sizeBytes,
			"duration_ms":   durationMs,
			"object_key":    objectKey,
		})
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate clips: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items, "total": total})
}

func (s *Server) handleAccountRecordingPause(w http.ResponseWriter, r *http.Request) {
	s.setRecordingStatus(w, r, "active", "paused", "recording_paused")
}

func (s *Server) handleAccountRecordingResume(w http.ResponseWriter, r *http.Request) {
	s.setRecordingStatus(w, r, "paused", "active", "recording_resumed")
}

// setRecordingStatus enforces a single legal status transition (fromStatus ->
// toStatus) under the account scope and recomputes next_fire_at (NULL when not
// active). An illegal transition (wrong current status) returns 409.
func (s *Server) setRecordingStatus(w http.ResponseWriter, r *http.Request, fromStatus, toStatus, eventType string) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}

	var (
		curStatus    string
		cronExpr     string
		cronTimezone string
	)
	err := s.pool.QueryRow(r.Context(), `
		SELECT status, cron_expr, cron_timezone
		FROM recordings
		WHERE id=$1 AND account_id=$2 AND status <> 'canceled'
	`, id, principal.AccountID).Scan(&curStatus, &cronExpr, &cronTimezone)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusNotFound, "recording not found")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load recording: %v", err))
		return
	}
	if curStatus == toStatus {
		util.WriteJSON(w, http.StatusOK, map[string]any{"id": id, "status": toStatus})
		return
	}
	if curStatus != fromStatus {
		util.WriteError(w, http.StatusConflict, fmt.Sprintf("cannot %s a recording in status %q", eventType, curStatus))
		return
	}

	var nextFireArg any
	if toStatus == "active" {
		nextFire, err := recsched.NextFireUTC(cronExpr, cronTimezone, time.Now().UTC())
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("compute next fire: %v", err))
			return
		}
		if !nextFire.IsZero() {
			nextFireArg = nextFire
		}
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin status tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	ct, err := tx.Exec(r.Context(), `
		UPDATE recordings
		SET status=$3, next_fire_at=$4, updated_at=now()
		WHERE id=$1 AND account_id=$2 AND status=$5
	`, id, principal.AccountID, toStatus, nextFireArg, fromStatus)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("update recording status: %v", err))
		return
	}
	if ct.RowsAffected() == 0 {
		util.WriteError(w, http.StatusConflict, "recording status changed concurrently")
		return
	}

	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit status tx: %v", err))
		return
	}

	_ = s.insertAccountAuthEvent(r.Context(), principal.AccountID, nil, eventType, "account", principal.Email, map[string]any{
		"recording_id": id,
	})

	// Pause/resume only flip status; the usage model never charges at resume, and
	// capture is held by the scheduler gate until a card is on file.
	util.WriteJSON(w, http.StatusOK, map[string]any{"id": id, "status": toStatus})
}

func (s *Server) handleAccountRecordingDelete(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin delete tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	ct, err := tx.Exec(r.Context(), `
		UPDATE recordings
		SET status='canceled', next_fire_at=NULL, updated_at=now()
		WHERE id=$1 AND account_id=$2 AND status <> 'canceled'
	`, id, principal.AccountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("cancel recording: %v", err))
		return
	}
	if ct.RowsAffected() == 0 {
		util.WriteError(w, http.StatusNotFound, "recording not found")
		return
	}

	// Cancel any in-flight jobs so the worker stops capturing this recording.
	if _, err := tx.Exec(r.Context(), `
		UPDATE recording_jobs
		SET status='canceled', updated_at=now()
		WHERE recording_id=$1 AND status IN ('pending','leased')
	`, id); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("cancel recording jobs: %v", err))
		return
	}

	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit delete tx: %v", err))
		return
	}

	_ = s.insertAccountAuthEvent(r.Context(), principal.AccountID, nil, "recording_canceled", "account", principal.Email, map[string]any{
		"recording_id": id,
	})
	util.WriteJSON(w, http.StatusOK, map[string]any{"id": id, "status": "canceled"})
}

// classifyRecordingSource restricts recorder sources to HLS and direct-HTTPS
// streams, mapping the shared capture classifier onto the recordings source_kind
// enum and rejecting youtube/image/rtsp/unsupported inputs.
func classifyRecordingSource(streamURL string) (string, error) {
	mode := capture.ClassifyMode(capture.StreamSpec{StreamURL: streamURL})
	switch mode {
	case capture.ModeHLSLive:
		return "hls_live", nil
	case capture.ModeFFmpegDirect:
		return "ffmpeg_direct", nil
	default:
		return "", fmt.Errorf("stream_url must be an HLS (.m3u8) or direct HTTP(S) video stream")
	}
}

// validateRecordingStreamURL is the shared validate half of the create/probe
// path: it runs the SSRF guard (rejecting loopback/link-local/metadata/RFC1918)
// and the HLS/HTTPS-only classifier, returning the validated IP to pin the probe
// to and the resolved source_kind.
func validateRecordingStreamURL(streamURL string) (net.IP, string, error) {
	validatedIP, err := netguard.ValidatePublicURL(streamURL)
	if err != nil {
		return nil, "", err
	}
	sourceKind, err := classifyRecordingSource(streamURL)
	if err != nil {
		return nil, "", err
	}
	return validatedIP, sourceKind, nil
}

// probeRecordingStreamReachable is the shared probe half of the create/probe
// path: it pins the (already SSRF-validated) URL to validatedIP so the probe
// socket cannot be redirected by a DNS rebind between validation and connect
// time, then runs a bounded ffmpeg open to confirm the source is live. pinHost
// carries the original hostname for the Host header / SNI.
func probeRecordingStreamReachable(ctx context.Context, streamURL string, validatedIP net.IP) error {
	pinnedProbeURL, probeHost, err := netguard.PinnedURL(streamURL, validatedIP)
	if err != nil {
		return err
	}
	probeCtx, cancel := context.WithTimeout(ctx, recordingProbeTimeout)
	defer cancel()
	return capture.ProbeReachable(probeCtx, pinnedProbeURL, probeHost)
}

// resolveRecordingStreamURL resolves a pasted stream reference (e.g. a KBS '!hls'
// indirect URL) to the live playable URL so validation and the reachability probe
// run on the actual stream, and the composer previews the real stream. A direct
// .m3u8 passes through unchanged. The resolve fetch is SSRF-guarded inside
// capture.ResolveCaptureInput. Image sources are rejected (the recorder is
// video-only). The stored reference is left untouched; the worker re-resolves it
// fresh on every capture so expiring tokens never break a schedule.
func resolveRecordingStreamURL(ctx context.Context, streamURL string) (string, error) {
	resolveCtx, cancel := context.WithTimeout(ctx, recordingResolveTimeout)
	defer cancel()
	resolved, isImage, err := capture.ResolveCaptureInput(resolveCtx, "", streamURL, "")
	if err != nil {
		return "", fmt.Errorf("could not resolve stream reference: %w", err)
	}
	if isImage {
		return "", fmt.Errorf("image sources are not supported for recording")
	}
	return resolved, nil
}

// scanRecordingListRow scans one row of the list/get SELECT into the API payload.
// billingEnabled (s.billing != nil) is threaded in so needs_card can be surfaced:
// a recording is "live" only when it is active, inside its [start_at, end_at)
// window, and the account has a card on file; needs_card flags the account-level
// "add a card to capture" state.
func scanRecordingListRow(row pgx.Row, billingEnabled bool) (map[string]any, error) {
	var (
		id               int64
		name             string
		streamURL        string
		storageDestID    int64
		storageDestName  string
		sourceKind       string
		cronExpr         string
		cronTimezone     string
		clipDurationSec  int
		status           string
		startAt          time.Time
		endAt            *time.Time
		nextFireAt       *time.Time
		lastClipAt       *time.Time
		lastErrorText    string
		lastErrorAt      *time.Time
		consecutiveFails int
		hasPaymentMethod bool
		recentClipCount  int64
		createdAt        time.Time
		managed          bool
		streamID         *int64
		streamName       *string
		streamLocation   *string
	)
	if err := row.Scan(
		&id, &name, &streamURL, &storageDestID, &storageDestName,
		&sourceKind, &cronExpr, &cronTimezone, &clipDurationSec,
		&status, &startAt, &endAt, &nextFireAt, &lastClipAt,
		&lastErrorText, &lastErrorAt, &consecutiveFails,
		&hasPaymentMethod, &recentClipCount, &createdAt, &managed,
		&streamID, &streamName, &streamLocation,
	); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	inWindow := !startAt.After(now) && (endAt == nil || now.Before(*endAt))
	live := status == "active" && inWindow && hasPaymentMethod
	needsCard := billingEnabled && !hasPaymentMethod
	return map[string]any{
		"id":                       id,
		"name":                     name,
		"stream_url":               streamURL,
		"storage_destination_id":   storageDestID,
		"storage_destination_name": storageDestName,
		"storage_managed":          managed,
		"source_kind":              sourceKind,
		"cron_expr":                cronExpr,
		"cron_timezone":            cronTimezone,
		"clip_duration_sec":        clipDurationSec,
		"status":                   status,
		"start_at":                 startAt.UTC(),
		"end_at":                   endAt,
		"has_payment_method":       hasPaymentMethod,
		"live":                     live,
		"needs_card":               needsCard,
		"health":                   recordingHealth(status, consecutiveFails),
		"next_fire_at":             nextFireAt,
		"last_clip_at":             lastClipAt,
		"last_error_text":          lastErrorText,
		"last_error_at":            lastErrorAt,
		"consecutive_failures":     consecutiveFails,
		"recent_clip_count":        recentClipCount,
		"created_at":               createdAt.UTC(),
		"stream_id":                streamID,
		"stream_name":              streamName,
		"stream_location":          streamLocation,
	}, nil
}

// recordingHealth derives a coarse health badge from the failure counter. It is
// advisory UI only; the scheduler/worker never read it.
func recordingHealth(status string, consecutiveFailures int) string {
	if status != "active" {
		return "idle"
	}
	switch {
	case consecutiveFailures == 0:
		return "healthy"
	case consecutiveFailures < 3:
		return "degraded"
	default:
		return "failing"
	}
}
