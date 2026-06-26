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

type recordingProbeRequest struct {
	StreamURL string `json:"stream_url"`
}

type recordingCreateRequest struct {
	Name                 string `json:"name"`
	StreamURL            string `json:"stream_url"`
	StorageDestinationID int64  `json:"storage_destination_id"`
	CronExpr             string `json:"cron_expr"`
	CronTimezone         string `json:"cron_timezone"`
	ClipDurationSec      int    `json:"clip_duration_sec"`
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
			rec.status, rec.next_fire_at, rec.last_clip_at,
			rec.last_error_text, rec.last_error_at, rec.consecutive_failures,
			COALESCE(bs.billable, false) AS billable,
			(SELECT count(*) FROM recording_clips c
			   WHERE c.recording_id = rec.id AND c.clip_start_at > now() - interval '24 hours') AS recent_clip_count,
			rec.created_at
		FROM recordings rec
		JOIN storage_destinations sd ON sd.id = rec.storage_destination_id
		LEFT JOIN recording_billing_state bs ON bs.recording_id = rec.id
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
		item, err := scanRecordingListRow(rows)
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
	streamURL := strings.TrimSpace(req.StreamURL)
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

	// S-1: SSRF guard + HLS/HTTPS classify on the user-supplied URL before it ever
	// reaches ffmpeg. This is the validate half of the shared validate+probe path.
	validatedIP, sourceKind, err := validateRecordingStreamURL(streamURL)
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
	if err := probeRecordingStreamReachable(r.Context(), streamURL, validatedIP); err != nil {
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
	)
	err = tx.QueryRow(r.Context(), `
		INSERT INTO recordings
			(account_id, storage_destination_id, name, stream_url, source_kind, cron_expr, cron_timezone, clip_duration_sec, status, next_fire_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'active',$9)
		RETURNING id, created_at
	`, principal.AccountID, req.StorageDestinationID, name, streamURL, sourceKind, cronExpr, cronTimezone, clipDuration, nextFireArg).Scan(&id, &createdAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			util.WriteError(w, http.StatusConflict, "a recording with that name already exists")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("create recording: %v", err))
		return
	}

	// B-3: the Stripe quantity is the absolute live recording count, recomputed in
	// the same tx so a concurrent create/delete cannot push a stale seat count. A
	// missing billing client / subscription item is a no-op (free mode still works).
	if err := s.syncSubscriptionQuantity(r.Context(), tx, principal.AccountID); err != nil {
		util.WriteError(w, http.StatusBadGateway, fmt.Sprintf("sync billing quantity: %v", err))
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

	resp := map[string]any{
		"id":         id,
		"status":     "active",
		"created_at": createdAt.UTC(),
	}
	// Start = pay: if the account has no access-granting subscription, the new
	// recording is not billable and will not capture until Checkout completes, so
	// mint a Checkout session and return its url. If the sub already grants access,
	// the in-tx syncSubscriptionQuantity above already covered the new seat.
	grants, err := s.accountSubscriptionGrantsAccess(r.Context(), principal.AccountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load billing coverage: %v", err))
		return
	}
	if !grants {
		checkoutURL, err := s.ensureRecordingCheckoutURL(r.Context(), principal.AccountID, principal.Email)
		if err != nil {
			util.WriteError(w, http.StatusBadGateway, fmt.Sprintf("mint checkout session: %v", err))
			return
		}
		if checkoutURL != "" {
			resp["checkout_url"] = checkoutURL
		}
	}

	util.WriteJSON(w, http.StatusCreated, resp)
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

	validatedIP, sourceKind, err := validateRecordingStreamURL(streamURL)
	if err != nil {
		util.WriteJSON(w, http.StatusOK, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if err := probeRecordingStreamReachable(r.Context(), streamURL, validatedIP); err != nil {
		util.WriteJSON(w, http.StatusOK, map[string]any{"ok": false, "error": fmt.Sprintf("stream not reachable: %v", err)})
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true, "source_kind": sourceKind})
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
			rec.status, rec.next_fire_at, rec.last_clip_at,
			rec.last_error_text, rec.last_error_at, rec.consecutive_failures,
			COALESCE(bs.billable, false) AS billable,
			(SELECT count(*) FROM recording_clips c
			   WHERE c.recording_id = rec.id AND c.clip_start_at > now() - interval '24 hours') AS recent_clip_count,
			rec.created_at
		FROM recordings rec
		JOIN storage_destinations sd ON sd.id = rec.storage_destination_id
		LEFT JOIN recording_billing_state bs ON bs.recording_id = rec.id
		WHERE rec.id=$1 AND rec.account_id=$2 AND rec.status <> 'canceled'
	`, id, principal.AccountID)
	item, err := scanRecordingListRow(row)
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

	// Pause drops the Stripe seat (paused = not billed); resume bumps it back, in
	// the same tx so a concurrent change cannot push a stale seat count. A drop to
	// zero active recordings cancels the subscription (see syncSubscriptionQuantity).
	if err := s.syncSubscriptionQuantity(r.Context(), tx, principal.AccountID); err != nil {
		util.WriteError(w, http.StatusBadGateway, fmt.Sprintf("sync billing quantity: %v", err))
		return
	}

	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit status tx: %v", err))
		return
	}

	_ = s.insertAccountAuthEvent(r.Context(), principal.AccountID, nil, eventType, "account", principal.Email, map[string]any{
		"recording_id": id,
	})

	resp := map[string]any{"id": id, "status": toStatus}
	// Resume = pay: if resuming leaves the account with an active recording but no
	// access-granting subscription, the resumed recording is not billable and will
	// not capture until Checkout completes, so mint a Checkout session and return
	// its url (uniform with Start).
	if toStatus == "active" {
		grants, err := s.accountSubscriptionGrantsAccess(r.Context(), principal.AccountID)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load billing coverage: %v", err))
			return
		}
		if !grants {
			checkoutURL, err := s.ensureRecordingCheckoutURL(r.Context(), principal.AccountID, principal.Email)
			if err != nil {
				util.WriteError(w, http.StatusBadGateway, fmt.Sprintf("mint checkout session: %v", err))
				return
			}
			if checkoutURL != "" {
				resp["checkout_url"] = checkoutURL
			}
		}
	}
	util.WriteJSON(w, http.StatusOK, resp)
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

	// B-3: push the new absolute seat count (never paid_quantity-1) in the same tx.
	if err := s.syncSubscriptionQuantity(r.Context(), tx, principal.AccountID); err != nil {
		util.WriteError(w, http.StatusBadGateway, fmt.Sprintf("sync billing quantity: %v", err))
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

func scanRecordingListRow(row pgx.Row) (map[string]any, error) {
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
		nextFireAt       *time.Time
		lastClipAt       *time.Time
		lastErrorText    string
		lastErrorAt      *time.Time
		consecutiveFails int
		billable         bool
		recentClipCount  int64
		createdAt        time.Time
	)
	if err := row.Scan(
		&id, &name, &streamURL, &storageDestID, &storageDestName,
		&sourceKind, &cronExpr, &cronTimezone, &clipDurationSec,
		&status, &nextFireAt, &lastClipAt,
		&lastErrorText, &lastErrorAt, &consecutiveFails,
		&billable, &recentClipCount, &createdAt,
	); err != nil {
		return nil, err
	}
	live := status == "active" && billable
	return map[string]any{
		"id":                       id,
		"name":                     name,
		"stream_url":               streamURL,
		"storage_destination_id":   storageDestID,
		"storage_destination_name": storageDestName,
		"source_kind":              sourceKind,
		"cron_expr":                cronExpr,
		"cron_timezone":            cronTimezone,
		"clip_duration_sec":        clipDurationSec,
		"status":                   status,
		"billable":                 billable,
		"live":                     live,
		"health":                   recordingHealth(status, consecutiveFails),
		"next_fire_at":             nextFireAt,
		"last_clip_at":             lastClipAt,
		"last_error_text":          lastErrorText,
		"last_error_at":            lastErrorAt,
		"consecutive_failures":     consecutiveFails,
		"recent_clip_count":        recentClipCount,
		"created_at":               createdAt.UTC(),
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
