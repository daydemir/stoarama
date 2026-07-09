package api

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/daydemir/stoarama/backend/internal/billing"
	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/dropletpool"
	"github.com/daydemir/stoarama/backend/internal/netguard"
	"github.com/daydemir/stoarama/backend/internal/recordability"
	"github.com/daydemir/stoarama/backend/internal/recordingnaming"
	"github.com/daydemir/stoarama/backend/internal/recsched"
	"github.com/daydemir/stoarama/backend/internal/util"
)

// deliveryMode is a recording's storage-delivery mode: 'managed' (footage lives in
// managed/BYO/WebDAV storage and stays there) or 'nas_pull' (the account's NAS pull
// client drains and releases each clip). It is the single strict type for the
// recordings.delivery column; no loose string literal for the mode lives outside it.
type deliveryMode string

const (
	deliveryManaged deliveryMode = "managed"
	deliveryNASPull deliveryMode = "nas_pull"
)

// parseDeliveryMode fails fast on any value other than the two legal modes. An
// empty input is NOT accepted here (callers that want a default resolve it before
// calling), so a malformed client value can never silently fall through.
func parseDeliveryMode(s string) (deliveryMode, error) {
	switch deliveryMode(s) {
	case deliveryManaged:
		return deliveryManaged, nil
	case deliveryNASPull:
		return deliveryNASPull, nil
	default:
		return "", fmt.Errorf("delivery must be managed or nas_pull")
	}
}

// recordingListSelectSQL is the single SELECT + FROM/JOIN that feeds
// scanRecordingListRow. Its column list and order MUST stay exactly aligned with
// scanRecordingListRow's row.Scan; every consumer (the account list, the single
// get, and the CSV export) appends only its own WHERE/ORDER BY so the three can
// never drift out of sync again.
const recordingListSelectSQL = `
	SELECT
		rec.id, rec.name, rec.stream_url, rec.storage_destination_id, sd.name,
		rec.source_kind, COALESCE(rec.cron_expr,''), rec.cron_timezone, rec.clip_duration_sec, rec.target_fps,
		rec.status, rec.start_at, rec.end_at, rec.next_fire_at, rec.last_clip_at,
		rec.last_error_text, rec.last_error_at, rec.consecutive_failures,
		COALESCE((SELECT b.has_payment_method FROM account_billing b
		   WHERE b.account_id = rec.account_id), false) AS has_payment_method,
		(SELECT count(*) FROM recording_clips c
		   WHERE c.recording_id = rec.id AND c.clip_start_at > now() - interval '24 hours') AS recent_clip_count,
		rec.created_at, sd.managed,
		rec.stream_id, st.name, st.location_text,
		rec.mode, COALESCE(to_char(rec.daily_window_start,'HH24:MI'),''), COALESCE(to_char(rec.daily_window_end,'HH24:MI'),''),
		rec.bundle_id, (SELECT b.name FROM recording_bundles b WHERE b.id = rec.bundle_id) AS bundle_name,
		rec.storage_retention_tier, rec.delivery,
		rec.capture_via,
		rec.naming_profile, rec.folder_name, rec.naming_metadata_jsonb,
		-- Relay readiness (per-recording). Both expressions short-circuit to false/NULL
		-- for a cloud recording (capture_via='cloud'), so they are cheap and dark until a
		-- relay recording exists. has_relay_online: at least one online relay node in the
		-- account has spare capacity right now.
		(rec.capture_via = 'relay' AND EXISTS (
		  SELECT 1 FROM nodes n
		  WHERE n.account_id = rec.account_id
		    AND n.node_type = 'relay' AND n.status = 'active'
		    AND n.last_heartbeat_at >= now() - interval '120 seconds'
		    AND (SELECT COUNT(*) FROM recording_jobs aj
		         WHERE aj.status = 'leased'
		           AND aj.lease_owner = 'node:' || n.id::text
		           AND aj.lease_expires_at > now()) < n.relay_max_streams
		)) AS has_relay_online,
		-- relay_node_name: the relay currently holding an active lease on this recording,
		-- if any (derived, replaces persisted pinning). NULL for cloud recordings.
		CASE WHEN rec.capture_via = 'relay' THEN (
		  SELECT n2.display_name
		  FROM recording_jobs j2
		  JOIN nodes n2 ON j2.lease_owner = 'node:' || n2.id::text
		  WHERE j2.recording_id = rec.id AND j2.status = 'leased'
		    AND j2.lease_expires_at > now()
		  LIMIT 1
		) ELSE NULL END AS relay_node_name
	FROM recordings rec
	JOIN storage_destinations sd ON sd.id = rec.storage_destination_id
	LEFT JOIN streams st ON st.id = rec.stream_id`

// recordingProbeTimeout bounds the create-time ffmpeg reachability probe.
const recordingProbeTimeout = 8 * time.Second

// recordingResolveTimeout bounds the create/probe-time stream reference
// resolution (an HTTP fetch for indirect '!hls' sources; a passthrough for
// direct .m3u8).
const recordingResolveTimeout = 30 * time.Second

type recordingProbeRequest struct {
	StreamURL string `json:"stream_url"`
	// StreamID, when > 0, is the catalog stream this probe is for. It lets the probe
	// response carry relay_recommended (the recordability auto-route verdict) so the
	// composer can show the overridable "this stream needs a connected computer" note.
	// A raw pasted URL leaves it unset and gets no recommendation.
	StreamID int64 `json:"stream_id"`
	// RecommendOnly asks for ONLY the recordability recommendation (a pure DB read,
	// no resolve/ffmpeg). Requires StreamID.
	RecommendOnly bool `json:"recommend_only"`
}

type recordingCreateRequest struct {
	Name      string `json:"name"`
	StreamURL string `json:"stream_url"`
	// StreamID, when set, links the recording to a catalog stream. The catalog's
	// source_url is then used as the stored stream_url (the stable reference the
	// worker re-resolves each fire); any client-sent stream_url is ignored.
	StreamID             *int64 `json:"stream_id"`
	StorageDestinationID int64  `json:"storage_destination_id"`
	// DeliveryStorageDestinationID, when set, selects a WebDAV destination (the
	// account's own or a granted shared one) as the DELIVERY target. The clip is
	// captured into the account's managed staging area, then transferred to this
	// destination and the staging copy is purged. When set, any client-sent
	// storage_destination_id is ignored: capture is forced to managed staging.
	DeliveryStorageDestinationID int64  `json:"delivery_storage_destination_id"`
	CronExpr                     string `json:"cron_expr"`
	CronTimezone                 string `json:"cron_timezone"`
	ClipDurationSec              int    `json:"clip_duration_sec"`
	// Mode is 'sampled' (default; one clip per cron fire) or 'continuous' (gapless
	// back-to-back segments for a daily window). For continuous, cron_expr is
	// ignored and the daily window (HH:MM[:SS] in cron_timezone) is required;
	// clip_duration_sec is the segment length.
	Mode             string `json:"mode"`
	DailyWindowStart string `json:"daily_window_start"`
	DailyWindowEnd   string `json:"daily_window_end"`
	// TargetFPS normalizes each captured clip to that exact frame rate. nil =
	// Source/native (stream-copy, preserve source fps, no re-encode, the cheap
	// default). The composer offers 15 and 30; the server accepts only those.
	TargetFPS *int `json:"target_fps"`
	// Capture window. StartAt defaults to now() (start immediately); EndAt is
	// open-ended when nil. When both are set, EndAt must be strictly after StartAt.
	StartAt *time.Time `json:"start_at"`
	EndAt   *time.Time `json:"end_at"`
	// StorageRetentionTier is how the managed-storage footage is billed: 'monthly'
	// (default; metered $0.10/stream-hour-month in arrears) or 'yearly_prepaid'
	// ($0.05 effective, prepaid 12 months up front). yearly_prepaid is only allowed
	// when the capture destination is the account's managed destination AND the org
	// has a card on file; otherwise create 400s.
	StorageRetentionTier string `json:"storage_retention_tier"`
	// Delivery is the storage-delivery mode: 'managed' (default) or 'nas_pull'. A
	// nas_pull recording still STAGES capture into managed storage (the capture path
	// is unchanged); the flag only marks its clips as the account's NAS pull feed so
	// the puller may release them and the UI labels the recording as NAS-bound. Empty
	// is treated as 'managed'.
	Delivery string `json:"delivery"`
	// CaptureVia is which infrastructure runs the worker loop: 'cloud' (default; the
	// operator droplet pool) or 'relay' (an account-owned relay node on a user
	// machine). Empty is treated as 'cloud'. Orthogonal to delivery. P1 accepts and
	// validates it but never forces it (no YouTube-to-relay routing yet).
	CaptureVia string                  `json:"capture_via"`
	Naming     *recordingNamingRequest `json:"naming"`
}

type recordingNamingRequest struct {
	Profile    string                   `json:"profile"`
	FolderName string                   `json:"folder_name"`
	Metadata   recordingnaming.Metadata `json:"metadata"`
}

func resolveRecordingNaming(req *recordingNamingRequest, recordingID int64) (recordingnaming.Profile, string, []byte, error) {
	profile := recordingnaming.ProfileStoaramaV1
	metadata := recordingnaming.Metadata{}
	folderRaw := ""
	if req != nil {
		if strings.TrimSpace(req.Profile) != "" {
			parsed, err := recordingnaming.ParseProfile(req.Profile)
			if err != nil {
				return "", "", nil, err
			}
			profile = parsed
		}
		metadata = req.Metadata
		folderRaw = req.FolderName
	}
	folderName, err := recordingnaming.BuildFolderName(profile, recordingID, metadata, folderRaw)
	if err != nil {
		return "", "", nil, err
	}
	metadataBytes, err := recordingnaming.MarshalMetadata(metadata)
	if err != nil {
		return "", "", nil, err
	}
	return profile, folderName, metadataBytes, nil
}

func namingPayload(profile string, folderName string, metadataBytes []byte) (map[string]any, error) {
	metadata, err := recordingnaming.ParseMetadata(metadataBytes)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"profile":     profile,
		"folder_name": folderName,
		"metadata":    metadata,
	}, nil
}

// optionalActingAccountID resolves the acting org from an optional browser session
// on a PUBLIC request. It returns (0, false) when there is no session (an anonymous
// visitor), so the public streams endpoints can scope recording_id to the acting org
// without requiring auth. It never writes an error: a missing/invalid session is the
// anonymous case, not a failure.
func (s *Server) optionalActingAccountID(r *http.Request) (int64, bool) {
	principal, err := s.authenticateAccountSessionRequest(r)
	if err != nil || principal.AccountID <= 0 {
		return 0, false
	}
	return principal.AccountID, true
}

// actingRecordingIDsForStreams returns, per stream id, the acting org's latest
// non-canceled recording.id for that stream (NULL/absent when none). It is the batch
// resolver behind the streams list's recording_id field, scoped to the acting org so
// one org never sees another's recording. An empty input (or no acting org) yields an
// empty map, which renders every stream's recording_id as null.
func (s *Server) actingRecordingIDsForStreams(ctx context.Context, accountID int64, streamIDs []int64) (map[int64]int64, error) {
	out := map[int64]int64{}
	if accountID <= 0 || len(streamIDs) == 0 {
		return out, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT rec.stream_id, MAX(rec.id)
		FROM recordings rec
		WHERE rec.account_id=$1 AND rec.status <> 'canceled' AND rec.stream_id = ANY($2::bigint[])
		GROUP BY rec.stream_id
	`, accountID, streamIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var streamID, recID int64
		if err := rows.Scan(&streamID, &recID); err != nil {
			return nil, err
		}
		out[streamID] = recID
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// actingRecordingIDForStream returns the acting org's latest non-canceled
// recording.id for one stream (nil when none or no acting org). It backs the stream
// detail's recording_id field.
func (s *Server) actingRecordingIDForStream(ctx context.Context, accountID, streamID int64) (*int64, error) {
	if accountID <= 0 {
		return nil, nil
	}
	// MAX over zero matching rows returns a single NULL row (not ErrNoRows), so scan
	// into a nullable pointer and return it directly.
	var recID *int64
	if err := s.pool.QueryRow(ctx, `
		SELECT MAX(rec.id)
		FROM recordings rec
		WHERE rec.account_id=$1 AND rec.status <> 'canceled' AND rec.stream_id=$2
	`, accountID, streamID).Scan(&recID); err != nil {
		return nil, err
	}
	return recID, nil
}

func (s *Server) handleAccountRecordingsList(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	rows, err := s.pool.Query(r.Context(), recordingListSelectSQL+`
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
	// Fleet relay aggregate: total nodes, online nodes (heartbeat within 120s), live
	// leases across all relay nodes, available slots across ONLINE relays, and the
	// existing fleet_relay_warning (kept for rollout compatibility).
	var (
		fleetRelayTotal          int
		fleetRelayOnline         int
		fleetRelayLiveLeases     int
		fleetRelayAvailableSlots int
		fleetRelayWarning        bool
	)
	if err := s.pool.QueryRow(r.Context(), `
		WITH relay_nodes AS (
		  SELECT n.id, n.status, n.relay_max_streams, n.last_heartbeat_at,
		    (SELECT COUNT(*) FROM recording_jobs j
		     WHERE j.lease_owner='node:'||n.id::text AND j.status='leased' AND j.lease_expires_at > now()) AS live_leases
		  FROM nodes n
		  WHERE n.account_id=$1 AND n.node_type='relay'
		),
		fleet AS (
		  SELECT
		    COUNT(*)::int                                                                                                                               AS total,
		    COUNT(*) FILTER (WHERE status='active' AND last_heartbeat_at >= now()-interval '120 seconds')::int                                         AS online,
		    COALESCE(SUM(live_leases), 0)::int                                                                                                         AS live_leases,
		    COALESCE(SUM(GREATEST(relay_max_streams - live_leases, 0)) FILTER (WHERE status='active' AND last_heartbeat_at >= now()-interval '120 seconds'), 0)::int AS available_slots
		  FROM relay_nodes
		),
		has_active_relay_rec AS (
		  SELECT EXISTS (
		    SELECT 1 FROM recordings rec
		    WHERE rec.account_id=$1 AND rec.status='active' AND rec.capture_via='relay'
		      AND rec.start_at <= now() AND (rec.end_at IS NULL OR now() < rec.end_at)
		  ) AS val
		)
		SELECT f.total, f.online, f.live_leases, f.available_slots,
		       (f.online = 0 AND h.val) AS fleet_relay_warning
		FROM fleet f, has_active_relay_rec h
	`, principal.AccountID).Scan(&fleetRelayTotal, &fleetRelayOnline, &fleetRelayLiveLeases, &fleetRelayAvailableSlots, &fleetRelayWarning); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("compute fleet relay stats: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"items":                       items,
		"fleet_relay_warning":         fleetRelayWarning,
		"fleet_relay_total":           fleetRelayTotal,
		"fleet_relay_online":          fleetRelayOnline,
		"fleet_relay_live_leases":     fleetRelayLiveLeases,
		"fleet_relay_available_slots": fleetRelayAvailableSlots,
	})
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
	// catalogStreamID/catalogProvider are set only when the recording links to a
	// catalog stream; they drive the recordability auto-route below. A raw pasted URL
	// has neither, so it is never auto-routed (we have no probe verdict for it).
	var catalogStreamID int64
	var catalogProvider string
	var catalogSourcePageURL string
	streamURL := strings.TrimSpace(req.StreamURL)
	if req.StreamID != nil {
		if *req.StreamID <= 0 {
			util.WriteError(w, http.StatusBadRequest, "stream_id is invalid")
			return
		}
		var catalogURL, provider, sourcePageURL string
		err := s.pool.QueryRow(r.Context(), `SELECT source_url, COALESCE(provider,''), COALESCE(source_page_url,'') FROM streams WHERE id=$1 AND deleted_at IS NULL`, *req.StreamID).Scan(&catalogURL, &provider, &sourcePageURL)
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
		catalogStreamID = *req.StreamID
		catalogProvider = provider
		catalogSourcePageURL = sourcePageURL
	}
	if streamURL == "" {
		util.WriteError(w, http.StatusBadRequest, "stream_url is required")
		return
	}
	// Exactly one destination selector is required: storage_destination_id for an
	// S3/managed recording (captured straight there), or delivery_storage_destination_id
	// for a WebDAV recording (captured to managed staging, then transferred to the NAS).
	if req.DeliveryStorageDestinationID <= 0 && req.StorageDestinationID <= 0 {
		util.WriteError(w, http.StatusBadRequest, "storage_destination_id or delivery_storage_destination_id is required")
		return
	}
	cronExpr := strings.TrimSpace(req.CronExpr)
	cronTimezone := strings.TrimSpace(req.CronTimezone)
	if cronTimezone == "" {
		cronTimezone = "UTC"
	}
	mode := strings.TrimSpace(req.Mode)
	if mode == "" {
		mode = "sampled"
	}
	if mode != "sampled" && mode != "continuous" {
		util.WriteError(w, http.StatusBadRequest, "mode must be sampled or continuous")
		return
	}
	// Delivery mode (default managed). nas_pull marks this recording's clips as the
	// account's NAS pull feed; capture still stages into managed storage below. An
	// invalid client value fails fast (no fallback to managed).
	deliveryRaw := strings.TrimSpace(req.Delivery)
	if deliveryRaw == "" {
		deliveryRaw = string(deliveryManaged)
	}
	delivery, err := parseDeliveryMode(deliveryRaw)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	// A recording cannot both NAS-pull and deliver to an external (WebDAV) destination;
	// both would drain the same managed staging clips.
	if delivery == deliveryNASPull && req.DeliveryStorageDestinationID > 0 {
		util.WriteError(w, http.StatusBadRequest, "a NAS-pull recording cannot also deliver to an external destination")
		return
	}
	// Capture routing (default 'cloud'). 'relay' recordings are served by an
	// account-owned relay node instead of the droplet pool. P1 accepts and validates
	// the value but never forces it. An invalid value fails fast (no fallback).
	captureVia, ok := normalizeCaptureVia(req.CaptureVia)
	if !ok {
		util.WriteError(w, http.StatusBadRequest, "capture_via must be cloud or relay")
		return
	}
	// YouTube cannot be recorded from datacenter IPs, so YouTube URLs are always
	// captured via a relay regardless of what the client requested (Go mirror of
	// capture/resolve.go's host check). The relay resolves the watch URL with yt-dlp
	// and the user's local Chrome cookies at capture time. Non-YouTube URLs keep the
	// client's choice ('cloud' default, or an explicit 'relay' opt-in).
	if isYouTubeWatchURL(streamURL) {
		captureVia = "relay"
	}
	// Recordability auto-route: a catalog stream (or its provider) that the probe
	// classified as blocked/needs-relay DEFAULTS to relay, but the user can override.
	// This only applies when the client left capture_via BLANK: normalizeCaptureVia
	// collapses ""->"cloud", so we test the RAW request field to tell blank from an
	// explicit "cloud" (an explicit choice is always honored). Inert until a probe
	// writes a row: with both recordability tables empty, NeedsRelay returns false.
	if strings.TrimSpace(req.CaptureVia) == "" && captureVia != "relay" && catalogStreamID > 0 {
		needsRelay, err := recordability.NeedsRelay(r.Context(), s.pool, catalogStreamID, catalogProvider)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("recordability route: %v", err))
			return
		}
		if needsRelay {
			captureVia = "relay"
		}
	}
	clipDuration := req.ClipDurationSec
	if clipDuration == 0 {
		clipDuration = 60
	}
	if !recordingnaming.IsAllowedClipDuration(clipDuration) {
		util.WriteError(w, http.StatusBadRequest, "clip_duration_sec must be between 5 and 900")
		return
	}
	namingProfile, folderName, namingMetadata, err := resolveRecordingNaming(req.Naming, 0)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Parse the continuous daily window up front (used for validation, preflight,
	// next_fire, and the insert). Sampled recordings leave these empty.
	var (
		dailyStart    recsched.TimeOfDay
		dailyEnd      recsched.TimeOfDay
		dailyStartArg any
		dailyEndArg   any
	)
	if mode == "continuous" {
		ds, derr := recsched.ParseTimeOfDay(strings.TrimSpace(req.DailyWindowStart))
		if derr != nil {
			util.WriteError(w, http.StatusBadRequest, "daily_window_start must be HH:MM")
			return
		}
		de, derr := recsched.ParseTimeOfDay(strings.TrimSpace(req.DailyWindowEnd))
		if derr != nil {
			util.WriteError(w, http.StatusBadRequest, "daily_window_end must be HH:MM")
			return
		}
		if verr := recsched.ValidateContinuousWindowForCreate(ds, de, clipDuration); verr != nil {
			util.WriteError(w, http.StatusBadRequest, verr.Error())
			return
		}
		dailyStart, dailyEnd = ds, de
		dailyStartArg = strings.TrimSpace(req.DailyWindowStart)
		dailyEndArg = strings.TrimSpace(req.DailyWindowEnd)
	}
	if err := recordingnaming.ValidateSchedule(namingProfile, mode, cronExpr, clipDuration, strings.TrimSpace(req.DailyWindowStart), strings.TrimSpace(req.DailyWindowEnd)); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	// target_fps: NULL = Source/native (preserve source fps, no re-encode). The
	// composer offers Source/30/15 quick-picks plus a custom rate, so accept any
	// integer in 1..60 (the DB CHECK allows up to 240; 60 is the sensible ceiling
	// for a capture re-encode). Anything outside that range is rejected.
	var targetFPSArg any
	if req.TargetFPS != nil {
		if *req.TargetFPS < 1 || *req.TargetFPS > 60 {
			util.WriteError(w, http.StatusBadRequest, "target_fps must be between 1 and 60 (omit for Source)")
			return
		}
		targetFPSArg = *req.TargetFPS
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

	// Relay recordings are resolved by the relay on the user's machine at capture
	// time (yt-dlp + local Chrome cookies for YouTube; a direct/HLS open otherwise).
	// Render has no yt-dlp and cannot resolve or reach YouTube from a datacenter IP,
	// so the server-side resolve + SSRF classify + ffmpeg reachability probe are all
	// SKIPPED for capture_via='relay'. The raw reference is stored as-is and the relay
	// re-resolves it fresh on every capture; source_kind='auto' lets the relay's
	// capture layer classify the source at run time. Cloud recordings keep the same
	// resolve/validate/probe flow, with provider headers carried when required.
	var (
		resolvedForProbe string
		inputHeaders     string
		validatedIP      net.IP
		sourceKind       string
	)
	if captureVia == "relay" {
		sourceKind = "auto"
	} else {
		// Resolve the pasted reference (e.g. a KBS '!hls' indirect URL) to the live
		// playable URL before validating/probing, so a reference ffmpeg cannot open
		// directly can still be scheduled. The raw reference is what gets stored
		// (below); the worker re-resolves it fresh on every capture.
		resolvedForProbe, inputHeaders, err = resolveRecordingStreamURL(r.Context(), catalogProvider, streamURL, catalogSourcePageURL)
		if err != nil {
			util.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}

		// S-1: SSRF guard + HLS/HTTPS classify on the resolved URL before it ever
		// reaches ffmpeg. This is the validate half of the shared validate+probe path.
		validatedIP, sourceKind, err = validateRecordingStreamURL(resolvedForProbe)
		if err != nil {
			util.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	// Schedule invariants + create-time concurrency cap. Sampled validates the cron
	// floor + clip-vs-interval and models each fire as a clip slot; continuous
	// validates the window (already done above) and models the stream as ONE
	// constant slot for the window. Both reject a schedule whose forecast peak,
	// combined with everything already capturing, would exceed the pool ceiling
	// (Max*Capacity), reusing the autoscaler's exact sweep-line (DRY). A continuous
	// stream counts as a FULL slot for its whole window.
	// The create-time concurrency cap is a droplet-pool ceiling (Max*Capacity). Relay
	// recordings never consume droplet slots (they are excluded from the demand
	// forecast in loadCapturingRecordings), so the pool ceiling does not apply to them
	// and the preflight is skipped. Continuous still validates its window above and
	// sampled still validates its cron below regardless of capture_via.
	if mode == "continuous" {
		if captureVia != "relay" {
			if err := s.checkContinuousScheduleCapacity(r.Context(), cronTimezone, dailyStart, dailyEnd, clipDuration, startAt, endAtTime(endAtArg), 1); err != nil {
				util.WriteError(w, http.StatusConflict, err.Error())
				return
			}
		}
	} else {
		if err := recsched.ValidateCronForCreate(cronExpr, cronTimezone, s.cfg.RecSchedMinIntervalSec, clipDuration); err != nil {
			util.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		if captureVia != "relay" {
			if err := s.checkRecordingScheduleCapacity(r.Context(), cronExpr, cronTimezone, clipDuration); err != nil {
				util.WriteError(w, http.StatusConflict, err.Error())
				return
			}
		}
	}

	// Resolve the capture destination and (for a WebDAV target) the delivery
	// destination. Authorization is the single owner-or-granted predicate; a granted
	// shared destination is selectable exactly like an owned one.
	//
	//   captureDestID  = where clips are written at capture time (presign path).
	//   deliveryDestArg = the WebDAV delivery target, or NULL for ordinary recordings.
	//
	// For a WebDAV recording the chosen destination is the DELIVERY target, so capture
	// is forced into the account's managed staging area (the presign path is unchanged)
	// and the delivery target is recorded for the ingest auto-enqueue + auto-purge.
	captureDestID := req.StorageDestinationID
	var deliveryDestArg any
	if req.DeliveryStorageDestinationID > 0 {
		var (
			destStatus   string
			destProvider string
		)
		err = s.pool.QueryRow(r.Context(), fmt.Sprintf(`
			SELECT sd.status, sd.provider FROM storage_destinations sd WHERE sd.id=$1 AND %s
		`, fmt.Sprintf(storageDestAccessPredicate, "$2")), req.DeliveryStorageDestinationID, principal.AccountID).Scan(&destStatus, &destProvider)
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusNotFound, "delivery storage destination not found")
			return
		}
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load delivery storage destination: %v", err))
			return
		}
		if destProvider != "webdav" {
			util.WriteError(w, http.StatusBadRequest, "delivery_storage_destination_id must reference a webdav destination")
			return
		}
		if destStatus != "verified" {
			util.WriteError(w, http.StatusBadRequest, "delivery storage destination is not verified")
			return
		}
		// A WebDAV destination cannot be presigned, so stage the capture in the
		// account's managed destination, then transfer to the NAS on ingest.
		managedID, _, perr := s.provisionManagedDestination(r.Context(), principal.AccountID)
		if perr != nil {
			if errors.Is(perr, errManagedUnavailable) {
				util.WriteError(w, http.StatusServiceUnavailable, "managed staging is required for WebDAV delivery but managed storage is not available")
				return
			}
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("provision managed staging: %v", perr))
			return
		}
		captureDestID = managedID
		deliveryDestArg = req.DeliveryStorageDestinationID
	} else {
		// Ordinary S3/managed recording: the selected destination is the capture dest.
		// Owner-or-granted predicate + verified.
		var destStatus string
		err = s.pool.QueryRow(r.Context(), fmt.Sprintf(`
			SELECT sd.status FROM storage_destinations sd WHERE sd.id=$1 AND %s
		`, fmt.Sprintf(storageDestAccessPredicate, "$2")), req.StorageDestinationID, principal.AccountID).Scan(&destStatus)
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusNotFound, "storage destination not found")
			return
		}
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load storage destination: %v", err))
			return
		}
		if destStatus != "verified" {
			util.WriteError(w, http.StatusBadRequest, "storage destination is not verified")
			return
		}
	}

	// Storage retention tier. Default 'monthly' (existing metered model). 'yearly_prepaid'
	// is only allowed when the capture destination is the account's MANAGED destination
	// (never a BYO bucket or a WebDAV delivery) AND the org has a card on file, because
	// the prepay charges that card. A WebDAV delivery (deliveryDestArg set) stages in
	// managed but the footage lives on the NAS, so it is not prepay-eligible.
	retentionTier := strings.TrimSpace(req.StorageRetentionTier)
	if retentionTier == "" {
		retentionTier = "monthly"
	}
	if retentionTier != "monthly" && retentionTier != "yearly_prepaid" {
		util.WriteError(w, http.StatusBadRequest, "storage_retention_tier must be monthly or yearly_prepaid")
		return
	}
	if retentionTier == "yearly_prepaid" {
		if deliveryDestArg != nil {
			util.WriteError(w, http.StatusBadRequest, "yearly_prepaid storage is only available for managed storage, not NAS delivery")
			return
		}
		var destManaged bool
		if err := s.pool.QueryRow(r.Context(), `
			SELECT managed FROM storage_destinations WHERE id=$1
		`, captureDestID).Scan(&destManaged); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load destination managed flag: %v", err))
			return
		}
		if !destManaged {
			util.WriteError(w, http.StatusBadRequest, "yearly_prepaid storage is only available for Stoarama-managed storage")
			return
		}
		var hasCard bool
		if err := s.pool.QueryRow(r.Context(), `
			SELECT COALESCE(has_payment_method, false) FROM account_billing WHERE account_id=$1
		`, principal.AccountID).Scan(&hasCard); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load payment method: %v", err))
			return
		}
		if !hasCard {
			util.WriteError(w, http.StatusBadRequest, "add a payment method in Org settings before choosing yearly_prepaid storage")
			return
		}
	}

	// Reachability probe pinned to the validated IP, so the probe socket cannot
	// be redirected by a DNS rebind between validation above and connect time. Skipped
	// for relay recordings (the relay owns resolution + reachability at capture time).
	if captureVia != "relay" {
		if err := probeRecordingStreamReachable(r.Context(), resolvedForProbe, validatedIP, inputHeaders); err != nil {
			util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("stream not reachable: %v", err))
			return
		}
	}

	var nextFireArg any
	if mode == "continuous" {
		nextOpen, nerr := recsched.NextWindowOpenUTC(cronTimezone, dailyStart, startAt, endAtTime(endAtArg), time.Now().UTC())
		if nerr != nil {
			util.WriteError(w, http.StatusBadRequest, nerr.Error())
			return
		}
		if !nextOpen.IsZero() {
			nextFireArg = nextOpen
		}
	} else {
		nextFire, nerr := recsched.NextFireUTC(cronExpr, cronTimezone, time.Now().UTC())
		if nerr != nil {
			util.WriteError(w, http.StatusBadRequest, nerr.Error())
			return
		}
		if !nextFire.IsZero() {
			nextFireArg = nextFire
		}
	}

	var existingID *int64
	if err := s.pool.QueryRow(r.Context(), `
		SELECT id FROM recordings WHERE account_id=$1 AND lower(name)=lower($2) AND status <> 'canceled' LIMIT 1
	`, principal.AccountID, name).Scan(&existingID); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("check recording name: %v", err))
		return
	}
	if existingID != nil {
		util.WriteJSON(w, http.StatusConflict, map[string]any{
			"error":        "a recording with that name already exists",
			"recording_id": *existingID,
		})
		return
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin create tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	var cronExprArg any
	if mode != "continuous" {
		cronExprArg = cronExpr
	}
	id, createdAt, startOut, endOut, err := s.insertRecordingTx(r.Context(), tx, recordingInsertParams{
		accountID:           principal.AccountID,
		captureDestID:       captureDestID,
		deliveryDestArg:     deliveryDestArg,
		name:                name,
		streamURL:           streamURL,
		streamIDArg:         streamIDArg,
		sourceKind:          sourceKind,
		mode:                mode,
		cronExprArg:         cronExprArg,
		cronTimezone:        cronTimezone,
		clipDuration:        clipDuration,
		dailyWindowStartArg: dailyStartArg,
		dailyWindowEndArg:   dailyEndArg,
		targetFPSArg:        targetFPSArg,
		nextFireArg:         nextFireArg,
		startAt:             startAt,
		endAtArg:            endAtArg,
		bundleIDArg:         nil,
		retentionTier:       retentionTier,
		delivery:            delivery,
		captureVia:          captureVia,
		namingProfile:       namingProfile,
		folderName:          folderName,
		namingMetadata:      namingMetadata,
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			body := map[string]any{"error": "a recording with that name already exists"}
			var collidedID *int64
			if serr := s.pool.QueryRow(r.Context(), `
				SELECT id FROM recordings WHERE account_id=$1 AND lower(name)=lower($2) AND status <> 'canceled' LIMIT 1
			`, principal.AccountID, name).Scan(&collidedID); serr == nil && collidedID != nil {
				body["recording_id"] = *collidedID
			}
			util.WriteJSON(w, http.StatusConflict, body)
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
		"storage_dest_id":   captureDestID,
		"delivery_dest_id":  deliveryDestArg,
		"source_kind":       sourceKind,
		"cron_expr":         cronExpr,
		"clip_duration_sec": clipDuration,
		"naming_profile":    namingProfile.String(),
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

// isYouTubeWatchURL mirrors capture/resolve.go's isYouTubeURL host check. It is the
// server-side gate that forces capture_via='relay' for YouTube URLs, because YouTube
// blocks stream resolution from datacenter IPs (Render has no yt-dlp and no
// residential IP). Kept as a small local mirror so the api package needs no export
// from the capture package.
func isYouTubeWatchURL(raw string) bool {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return false
	}
	host := strings.ToLower(strings.TrimSpace(u.Hostname()))
	return host == "youtube.com" || host == "www.youtube.com" || host == "m.youtube.com" || host == "youtu.be" || strings.HasSuffix(host, ".youtube.com")
}

// normalizeCaptureVia validates and normalizes the create request's capture_via.
// Empty defaults to 'cloud'; 'cloud' and 'relay' are the only accepted values (the
// same set the recordings.capture_via CHECK constraint allows). ok=false on any other
// value so the create handler can fail fast with no fallback.
func normalizeCaptureVia(raw string) (string, bool) {
	switch strings.TrimSpace(raw) {
	case "":
		return "cloud", true
	case "cloud":
		return "cloud", true
	case "relay":
		return "relay", true
	default:
		return "", false
	}
}

// recordingInsertParams carries the fully-resolved column values for one
// recordings row. It is the single contract shared by single-recording create
// and bundle fan-out so the INSERT lives in exactly one place (DRY). Every *Arg
// field is an `any` that is either a concrete value or nil for SQL NULL.
type recordingInsertParams struct {
	accountID           int64
	captureDestID       int64
	deliveryDestArg     any
	name                string
	streamURL           string
	streamIDArg         any
	sourceKind          string
	mode                string
	cronExprArg         any
	cronTimezone        string
	clipDuration        int
	dailyWindowStartArg any
	dailyWindowEndArg   any
	targetFPSArg        any
	nextFireArg         any
	startAt             time.Time
	endAtArg            any
	bundleIDArg         any
	// retentionTier is 'monthly' (default) or 'yearly_prepaid'; empty is treated as
	// 'monthly'. Bundle fan-out leaves it empty so bundled recordings are metered.
	retentionTier string
	// delivery is the storage-delivery mode. Empty is treated as 'managed', so the
	// bundle fan-out (which leaves it empty) always writes 'managed' deliberately.
	delivery deliveryMode
	// captureVia is the capture-routing flag ('cloud' or 'relay'). Empty is treated as
	// 'cloud', so the bundle fan-out (which leaves it empty) always writes 'cloud'.
	captureVia     string
	namingProfile  recordingnaming.Profile
	folderName     string
	namingMetadata []byte
}

// insertRecordingTx runs the one canonical recordings INSERT inside the caller's
// transaction and returns the new row's id/created_at/start_at/end_at. It returns
// the pgx error unwrapped so each caller maps 23505 (name collision) on its own
// terms: single-create -> 409 "name exists"; bundle-create -> whole-bundle
// failure. The 15 existing columns keep the exact values they had before; the
// only addition is bundle_id (nil for a standalone recording).
func (s *Server) insertRecordingTx(ctx context.Context, tx pgx.Tx, p recordingInsertParams) (id int64, createdAt time.Time, startOut time.Time, endOut *time.Time, err error) {
	mode := p.mode
	if mode == "" {
		mode = "sampled"
	}
	retentionTier := p.retentionTier
	if retentionTier == "" {
		retentionTier = "monthly"
	}
	delivery := p.delivery
	if delivery == "" {
		delivery = deliveryManaged
	}
	captureVia := p.captureVia
	if captureVia == "" {
		captureVia = "cloud"
	}
	namingProfile := p.namingProfile
	if namingProfile == "" {
		namingProfile = recordingnaming.ProfileStoaramaV1
	}
	folderName := strings.TrimSpace(p.folderName)
	if folderName == "" {
		folderName = "recordings"
	}
	namingMetadata := p.namingMetadata
	if len(namingMetadata) == 0 {
		namingMetadata = []byte(`{}`)
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO recordings
			(account_id, storage_destination_id, delivery_storage_destination_id, name, stream_url, stream_id, source_kind, mode, cron_expr, cron_timezone, clip_duration_sec, daily_window_start, daily_window_end, target_fps, status, next_fire_at, start_at, end_at, bundle_id, storage_retention_tier, delivery, capture_via, naming_profile, folder_name, naming_metadata_jsonb)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,'active',$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)
		RETURNING id, created_at, start_at, end_at
	`, p.accountID, p.captureDestID, p.deliveryDestArg, p.name, p.streamURL, p.streamIDArg, p.sourceKind, mode, p.cronExprArg, p.cronTimezone, p.clipDuration, p.dailyWindowStartArg, p.dailyWindowEndArg, p.targetFPSArg, p.nextFireArg, p.startAt, p.endAtArg, p.bundleIDArg, retentionTier, string(delivery), captureVia, namingProfile.String(), folderName, namingMetadata).Scan(&id, &createdAt, &startOut, &endOut)
	return id, createdAt, startOut, endOut, err
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
	// Recommend-only mode: a catalog stream asks ONLY for the recordability
	// recommendation (a pure DB read), so the composer can show the overridable
	// "this stream needs a connected computer" note without shelling ffmpeg. A
	// needs-relay stream is resolved on the user's machine, so a server ffmpeg probe
	// would be pointless (and would fail from our datacenter IP, which is the point).
	if req.RecommendOnly {
		if req.StreamID <= 0 {
			util.WriteError(w, http.StatusBadRequest, "stream_id is required for recommend_only")
			return
		}
		resp := map[string]any{"ok": true}
		var provider string
		if err := s.pool.QueryRow(r.Context(), `SELECT COALESCE(provider,'') FROM streams WHERE id=$1 AND deleted_at IS NULL`, req.StreamID).Scan(&provider); err == nil {
			if needsRelay, rerr := recordability.NeedsRelay(r.Context(), s.pool, req.StreamID, provider); rerr == nil && needsRelay {
				resp["relay_recommended"] = true
				resp["relay_reason"] = "We could not record this stream from our servers, so it defaults to recording via your computer."
			}
		}
		util.WriteJSON(w, http.StatusOK, resp)
		return
	}
	streamURL := strings.TrimSpace(req.StreamURL)
	if streamURL == "" {
		util.WriteError(w, http.StatusBadRequest, "stream_url is required")
		return
	}

	provider, sourcePageURL := "", ""
	if req.StreamID > 0 {
		_ = s.pool.QueryRow(r.Context(), `SELECT COALESCE(provider,''), COALESCE(source_page_url,'') FROM streams WHERE id=$1 AND deleted_at IS NULL`, req.StreamID).Scan(&provider, &sourcePageURL)
	}
	resolved, inputHeaders, err := resolveRecordingStreamURL(r.Context(), provider, streamURL, sourcePageURL)
	if err != nil {
		util.WriteJSON(w, http.StatusOK, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	validatedIP, sourceKind, err := validateRecordingStreamURL(resolved)
	if err != nil {
		util.WriteJSON(w, http.StatusOK, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if err := probeRecordingStreamReachable(r.Context(), resolved, validatedIP, inputHeaders); err != nil {
		util.WriteJSON(w, http.StatusOK, map[string]any{"ok": false, "error": fmt.Sprintf("stream not reachable: %v", err)})
		return
	}
	// Recordability recommendation for a linked catalog stream: when the probe (or its
	// provider) says this stream needs relay, the composer shows an overridable note.
	// Inert until a probe writes a row (empty tables => relay_recommended=false).
	resp := map[string]any{"ok": true, "source_kind": sourceKind, "resolved_url": resolved}
	if req.StreamID > 0 {
		var provider string
		if err := s.pool.QueryRow(r.Context(), `SELECT COALESCE(provider,'') FROM streams WHERE id=$1 AND deleted_at IS NULL`, req.StreamID).Scan(&provider); err == nil {
			if needsRelay, rerr := recordability.NeedsRelay(r.Context(), s.pool, req.StreamID, provider); rerr == nil && needsRelay {
				resp["relay_recommended"] = true
				resp["relay_reason"] = "We could not record this stream from our servers, so it defaults to recording via your computer."
			}
		}
	}
	util.WriteJSON(w, http.StatusOK, resp)
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
	row := s.pool.QueryRow(r.Context(), recordingListSelectSQL+`
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

// writeRecordingJSON re-reads one recording under the account scope and writes it
// with the same shape as GET /recordings/{id}. It is the single place the PATCH
// handlers return their updated row (DRY), so a schedule/delivery edit and the get
// can never drift in shape.
func (s *Server) writeRecordingJSON(w http.ResponseWriter, r *http.Request, id, accountID int64) {
	row := s.pool.QueryRow(r.Context(), recordingListSelectSQL+`
		WHERE rec.id=$1 AND rec.account_id=$2 AND rec.status <> 'canceled'
	`, id, accountID)
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

// recordingScheduleRequest is the schedule-edit PATCH body. Only the fields
// relevant to the chosen mode are read: sampled uses cron_expr/cron_timezone;
// continuous uses daily_window_start/daily_window_end. Both share
// clip_duration_sec, target_fps, and the optional capture window (start_at/end_at).
type recordingScheduleRequest struct {
	Mode             string     `json:"mode"`
	CronExpr         string     `json:"cron_expr"`
	CronTimezone     string     `json:"cron_timezone"`
	ClipDurationSec  int        `json:"clip_duration_sec"`
	DailyWindowStart string     `json:"daily_window_start"`
	DailyWindowEnd   string     `json:"daily_window_end"`
	StartAt          *time.Time `json:"start_at"`
	EndAt            *time.Time `json:"end_at"`
	TargetFPS        *int       `json:"target_fps"`
}

// handleAccountRecordingSchedule edits one recording's schedule in place. It reuses
// the exact create-time validation (ValidateCronForCreate + the capacity checks +
// ValidateContinuousWindowForCreate) so an edit can never write a schedule create
// would reject, recomputes next_fire_at via nextFireForRecording, and returns the
// updated recording JSON. Only the owning account may edit; a canceled recording is
// not found.
func (s *Server) handleAccountRecordingSchedule(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req recordingScheduleRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	mode := strings.TrimSpace(req.Mode)
	if mode != "sampled" && mode != "continuous" {
		util.WriteError(w, http.StatusBadRequest, "mode must be sampled or continuous")
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
	if !recordingnaming.IsAllowedClipDuration(clipDuration) {
		util.WriteError(w, http.StatusBadRequest, "clip_duration_sec must be between 5 and 900")
		return
	}

	// target_fps: NULL = Source/native; otherwise 1..60 (mirrors create). Validated up
	// front with the other pure-input checks so a bad value fails fast before any DB work.
	var targetFPSArg any
	if req.TargetFPS != nil {
		if *req.TargetFPS < 1 || *req.TargetFPS > 60 {
			util.WriteError(w, http.StatusBadRequest, "target_fps must be between 1 and 60 (omit for Source)")
			return
		}
		targetFPSArg = *req.TargetFPS
	}

	// Confirm ownership and load the current capture window so an omitted start_at/
	// end_at keeps the recording's existing window (a schedule edit is not a window
	// reset). A canceled recording is not found.
	var (
		curStartAt       time.Time
		curEndAt         *time.Time
		namingProfileRaw string
	)
	if err := s.pool.QueryRow(r.Context(), `
		SELECT start_at, end_at, naming_profile FROM recordings
		WHERE id=$1 AND account_id=$2 AND status <> 'canceled'
	`, id, principal.AccountID).Scan(&curStartAt, &curEndAt, &namingProfileRaw); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusNotFound, "recording not found")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load recording: %v", err))
		return
	}
	namingProfile, err := recordingnaming.ParseProfile(namingProfileRaw)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}

	startAt := curStartAt.UTC()
	if req.StartAt != nil {
		startAt = req.StartAt.UTC()
	}
	endAtArg := any(nil)
	if curEndAt != nil {
		endAtArg = curEndAt.UTC()
	}
	if req.EndAt != nil {
		endAt := req.EndAt.UTC()
		if !endAt.After(startAt) {
			util.WriteError(w, http.StatusBadRequest, "end_at must be after start_at")
			return
		}
		endAtArg = endAt
	}

	// Parse + validate the schedule per mode, reusing the exact create-time helpers so
	// an edit can never write a schedule create would reject. Sampled clears the daily
	// window; continuous clears cron_expr. The nullable pointers feed both the UPDATE
	// args and the shared next-fire authority, so the two never drift.
	var (
		cronExprForNext *string
		dwStartForNext  *string
		dwEndForNext    *string
	)
	if mode == "continuous" {
		dwStart := strings.TrimSpace(req.DailyWindowStart)
		dwEnd := strings.TrimSpace(req.DailyWindowEnd)
		ds, derr := recsched.ParseTimeOfDay(dwStart)
		if derr != nil {
			util.WriteError(w, http.StatusBadRequest, "daily_window_start must be HH:MM")
			return
		}
		de, derr := recsched.ParseTimeOfDay(dwEnd)
		if derr != nil {
			util.WriteError(w, http.StatusBadRequest, "daily_window_end must be HH:MM")
			return
		}
		if verr := recsched.ValidateContinuousWindowForCreate(ds, de, clipDuration); verr != nil {
			util.WriteError(w, http.StatusBadRequest, verr.Error())
			return
		}
		if err := s.checkContinuousScheduleCapacity(r.Context(), cronTimezone, ds, de, clipDuration, startAt, endAtTime(endAtArg), 1); err != nil {
			util.WriteError(w, http.StatusConflict, err.Error())
			return
		}
		dwStartForNext, dwEndForNext = &dwStart, &dwEnd
	} else {
		if err := recsched.ValidateCronForCreate(cronExpr, cronTimezone, s.cfg.RecSchedMinIntervalSec, clipDuration); err != nil {
			util.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := s.checkRecordingScheduleCapacity(r.Context(), cronExpr, cronTimezone, clipDuration); err != nil {
			util.WriteError(w, http.StatusConflict, err.Error())
			return
		}
		cronExprForNext = &cronExpr
	}
	if err := recordingnaming.ValidateSchedule(namingProfile, mode, cronExpr, clipDuration, strings.TrimSpace(req.DailyWindowStart), strings.TrimSpace(req.DailyWindowEnd)); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Recompute next_fire_at with the mode-aware authority (NULL when the schedule has
	// no upcoming fire, e.g. a window that ends before now).
	var nextFireArg any
	var endAtForNext *time.Time
	if t, ok := endAtArg.(time.Time); ok {
		endAtForNext = &t
	}
	nextFire, nerr := nextFireForRecording(mode, cronExprForNext, cronTimezone, dwStartForNext, dwEndForNext, startAt, endAtForNext, time.Now().UTC())
	if nerr != nil {
		util.WriteError(w, http.StatusBadRequest, nerr.Error())
		return
	}
	if !nextFire.IsZero() {
		nextFireArg = nextFire
	}

	ct, err := s.pool.Exec(r.Context(), `
		UPDATE recordings
		SET mode=$3, cron_expr=$4, cron_timezone=$5, clip_duration_sec=$6,
		    daily_window_start=$7, daily_window_end=$8, target_fps=$9,
		    start_at=$10, end_at=$11, next_fire_at=$12, updated_at=now()
		WHERE id=$1 AND account_id=$2 AND status <> 'canceled'
	`, id, principal.AccountID, mode, cronExprForNext, cronTimezone, clipDuration,
		dwStartForNext, dwEndForNext, targetFPSArg, startAt, endAtArg, nextFireArg)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("update recording schedule: %v", err))
		return
	}
	if ct.RowsAffected() == 0 {
		util.WriteError(w, http.StatusNotFound, "recording not found")
		return
	}

	_ = s.insertAccountAuthEvent(r.Context(), principal.AccountID, nil, "recording_schedule_changed", "account", principal.Email, map[string]any{
		"recording_id":      id,
		"mode":              mode,
		"cron_expr":         cronExpr,
		"clip_duration_sec": clipDuration,
	})
	s.writeRecordingJSON(w, r, id, principal.AccountID)
}

func (s *Server) handleAccountRecordingNaming(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req recordingNamingRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	profile, folderName, metadataBytes, err := resolveRecordingNaming(&req, id)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	var mode, cronExpr, dailyWindowStart, dailyWindowEnd string
	var clipDuration int
	if err := s.pool.QueryRow(r.Context(), `
		SELECT mode, COALESCE(cron_expr, ''), clip_duration_sec,
		       COALESCE(to_char(daily_window_start, 'HH24:MI:SS'), ''),
		       COALESCE(to_char(daily_window_end, 'HH24:MI:SS'), '')
		FROM recordings
		WHERE id=$1 AND account_id=$2 AND status <> 'canceled'
	`, id, principal.AccountID).Scan(&mode, &cronExpr, &clipDuration, &dailyWindowStart, &dailyWindowEnd); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusNotFound, "recording not found")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load recording: %v", err))
		return
	}
	if err := recordingnaming.ValidateSchedule(profile, mode, cronExpr, clipDuration, dailyWindowStart, dailyWindowEnd); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	ct, err := s.pool.Exec(r.Context(), `
		UPDATE recordings
		SET naming_profile=$3, folder_name=$4, naming_metadata_jsonb=$5, updated_at=now()
		WHERE id=$1 AND account_id=$2 AND status <> 'canceled'
	`, id, principal.AccountID, profile.String(), folderName, metadataBytes)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("update recording naming: %v", err))
		return
	}
	if ct.RowsAffected() == 0 {
		util.WriteError(w, http.StatusNotFound, "recording not found")
		return
	}
	s.writeRecordingJSON(w, r, id, principal.AccountID)
}

// recordingDeliveryRequest is the delivery-mode PATCH body.
type recordingDeliveryRequest struct {
	Delivery string `json:"delivery"`
}

// handleAccountRecordingDelivery switches one recording's storage-delivery mode.
// nas_pull is rejected when the account has no nas_pull connection (there would be
// no puller to drain the clips). Capture already stages to managed for both modes,
// so no capture-destination change is needed. Only the owning account may edit.
func (s *Server) handleAccountRecordingDelivery(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req recordingDeliveryRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	delivery, err := parseDeliveryMode(strings.TrimSpace(req.Delivery))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	// nas_pull requires an existing NAS pull connection on the account; without one
	// there is no puller to drain the staged clips, so the switch is rejected.
	if delivery == deliveryNASPull {
		var hasConnection bool
		if err := s.pool.QueryRow(r.Context(), `
			SELECT EXISTS(SELECT 1 FROM connections WHERE account_id=$1 AND kind='nas_pull')
		`, principal.AccountID).Scan(&hasConnection); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("check nas pull connection: %v", err))
			return
		}
		if !hasConnection {
			util.WriteError(w, http.StatusBadRequest, "connect a NAS pull client before switching a recording to NAS delivery")
			return
		}
		// A recording that already delivers to an external (WebDAV) destination must
		// not also NAS-pull: both would drain the same managed staging clips.
		var hasExternalDelivery bool
		if err := s.pool.QueryRow(r.Context(), `
			SELECT EXISTS(SELECT 1 FROM recordings WHERE id=$1 AND account_id=$2 AND delivery_storage_destination_id IS NOT NULL)
		`, id, principal.AccountID).Scan(&hasExternalDelivery); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("check recording delivery target: %v", err))
			return
		}
		if hasExternalDelivery {
			util.WriteError(w, http.StatusBadRequest, "this recording already delivers to an external destination and cannot also pull to a NAS")
			return
		}
	}

	ct, err := s.pool.Exec(r.Context(), `
		UPDATE recordings SET delivery=$3, updated_at=now()
		WHERE id=$1 AND account_id=$2 AND status <> 'canceled'
	`, id, principal.AccountID, string(delivery))
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("update recording delivery: %v", err))
		return
	}
	if ct.RowsAffected() == 0 {
		util.WriteError(w, http.StatusNotFound, "recording not found")
		return
	}

	_ = s.insertAccountAuthEvent(r.Context(), principal.AccountID, nil, "recording_delivery_changed", "account", principal.Email, map[string]any{
		"recording_id": id,
		"delivery":     string(delivery),
	})
	s.writeRecordingJSON(w, r, id, principal.AccountID)
}

// recordingRetentionRequest is the tier-change PATCH body.
type recordingRetentionRequest struct {
	Tier string `json:"tier"`
}

// handleAccountRecordingRetention switches one recording's storage_retention_tier
// (retroactively). It is owner/billing_admin only (principalCanManageBilling).
//
//   - monthly -> yearly_prepaid: card required. It prepays the recording's
//     CURRENTLY-STORED footage NOW as an immediate standalone batch
//     (batch_key "prepay-switch:rec-<id>:<YYYY-MM-DD>"), sets the tier, and future
//     footage prepays via the monthly pass. The credit grant is created later on the
//     invoice.paid webhook (same path as the monthly pass), so the switch is not
//     "done" billing-wise until Stripe confirms payment.
//   - yearly_prepaid -> monthly: sets the tier so future footage reverts to metered.
//     Any already-granted credit RUNS TO ITS EXPIRY and is NOT refunded (stated in the
//     response and the UI copy).
//
// Only managed footage is prepay-eligible; a recording whose destination is not
// managed cannot be set to yearly_prepaid.
func (s *Server) handleAccountRecordingRetention(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if !principalCanManageBilling(principal) {
		util.WriteError(w, http.StatusForbidden, "only an org owner or billing admin can change storage retention")
		return
	}
	if s.billing == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "billing is not enabled")
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req recordingRetentionRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	newTier := strings.TrimSpace(req.Tier)
	if newTier != "monthly" && newTier != "yearly_prepaid" {
		util.WriteError(w, http.StatusBadRequest, "tier must be monthly or yearly_prepaid")
		return
	}

	// Load the recording (owned by this account), its current tier, and whether its
	// capture destination is managed.
	var (
		curTier     string
		destManaged bool
	)
	err := s.pool.QueryRow(r.Context(), `
		SELECT rec.storage_retention_tier, sd.managed
		FROM recordings rec
		JOIN storage_destinations sd ON sd.id = rec.storage_destination_id
		WHERE rec.id=$1 AND rec.account_id=$2 AND rec.status <> 'canceled'
	`, id, principal.AccountID).Scan(&curTier, &destManaged)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "recording not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load recording: %v", err))
		return
	}

	if newTier == curTier {
		util.WriteJSON(w, http.StatusOK, map[string]any{"id": id, "storage_retention_tier": curTier, "changed": false})
		return
	}

	if newTier == "monthly" {
		// yearly_prepaid -> monthly: future footage reverts to metered. Existing granted
		// credit runs to expiry; NO refund.
		if _, err := s.pool.Exec(r.Context(), `
			UPDATE recordings SET storage_retention_tier='monthly', updated_at=now()
			WHERE id=$1 AND account_id=$2
		`, id, principal.AccountID); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("set monthly tier: %v", err))
			return
		}
		util.WriteJSON(w, http.StatusOK, map[string]any{
			"id":                     id,
			"storage_retention_tier": "monthly",
			"changed":                true,
			"note":                   "Future footage will be metered at $0.10 per stream-hour-month. Any prepaid credit already granted runs until it expires and is not refunded.",
		})
		return
	}

	// monthly -> yearly_prepaid: managed-only + card required + immediate retroactive
	// prepay of currently-stored footage.
	if !destManaged {
		util.WriteError(w, http.StatusBadRequest, "yearly_prepaid storage is only available for Stoarama-managed storage")
		return
	}
	var (
		customerID *string
		hasCard    bool
	)
	if err := s.pool.QueryRow(r.Context(), `
		SELECT stripe_customer_id, COALESCE(has_payment_method, false)
		FROM account_billing WHERE account_id=$1
	`, principal.AccountID).Scan(&customerID, &hasCard); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load billing: %v", err))
		return
	}
	if !hasCard || customerID == nil || strings.TrimSpace(*customerID) == "" {
		util.WriteError(w, http.StatusBadRequest, "add a payment method in Org settings before switching to yearly_prepaid")
		return
	}

	// Currently-stored managed stream-hours for THIS recording (purged/released
	// excluded), computed directly from recording_clips.
	var streamHours float64
	if err := s.pool.QueryRow(r.Context(), `
		SELECT COALESCE(SUM(GREATEST(EXTRACT(EPOCH FROM (c.clip_end_at - c.clip_start_at)), 0) / 3600.0), 0)
		FROM recording_clips c
		JOIN storage_destinations sd ON sd.id = c.storage_destination_id
		WHERE c.recording_id=$1 AND sd.managed AND c.purged_at IS NULL AND c.released_at IS NULL
	`, id).Scan(&streamHours); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("compute stored stream-hours: %v", err))
		return
	}

	// Set the tier first so future footage prepays via the monthly pass regardless of
	// whether there is any currently-stored footage to retroactively charge.
	if _, err := s.pool.Exec(r.Context(), `
		UPDATE recordings SET storage_retention_tier='yearly_prepaid', updated_at=now()
		WHERE id=$1 AND account_id=$2
	`, id, principal.AccountID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("set yearly tier: %v", err))
		return
	}

	cents := billing.PrepaidBatchCents(streamHours)
	if cents <= 0 {
		// No stored footage yet (or rounds to $0): nothing to charge now; future
		// footage prepays via the monthly pass.
		util.WriteJSON(w, http.StatusOK, map[string]any{
			"id":                     id,
			"storage_retention_tier": "yearly_prepaid",
			"changed":                true,
			"prepaid_now":            false,
			"note":                   "Future footage is prepaid at $0.05 per stream-hour-month. No stored footage to prepay right now.",
		})
		return
	}

	batchKey := fmt.Sprintf("prepay-switch:rec-%d:%s", id, time.Now().UTC().Format("2006-01-02"))
	if err := s.chargeRetroactivePrepay(r.Context(), retroactivePrepay{
		batchKey:    batchKey,
		accountID:   principal.AccountID,
		recordingID: id,
		customerID:  strings.TrimSpace(*customerID),
		streamHours: streamHours,
		cents:       cents,
	}); err != nil {
		// The tier is already set (future footage is covered). Surface the charge
		// failure so the caller can retry; the ledger row is marked 'failed' by the
		// helper so a retry uses a fresh batch_key path only if the caller changes date.
		util.WriteError(w, http.StatusBadGateway, fmt.Sprintf("prepay stored footage: %v", err))
		return
	}

	_ = s.insertAccountAuthEvent(r.Context(), principal.AccountID, nil, "recording_retention_changed", "account", principal.Email, map[string]any{
		"recording_id": id,
		"from":         curTier,
		"to":           "yearly_prepaid",
		"batch_key":    batchKey,
		"cents":        cents,
	})
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"id":                     id,
		"storage_retention_tier": "yearly_prepaid",
		"changed":                true,
		"prepaid_now":            true,
		"charged_cents":          cents,
		"stream_hours":           strconv.FormatFloat(streamHours, 'f', 4, 64),
		"note":                   "Currently-stored footage was prepaid for 12 months. A standalone invoice was charged to your card; the credit applies once payment confirms.",
	})
}

// retroactivePrepay carries the resolved inputs for an immediate per-recording prepay
// charge (the monthly->yearly switch). It mirrors the metering pass's chargeBatch but
// lives in the api package because the switch runs in the request path.
type retroactivePrepay struct {
	batchKey    string
	accountID   int64
	recordingID int64
	customerID  string
	streamHours float64
	cents       int64
}

// chargeRetroactivePrepay inserts the pending ledger row and charges the standalone
// prepay invoice for a monthly->yearly switch. It is the same no-double-charge
// discipline as the monthly pass: batch_key is UNIQUE (a re-submit on the same day
// no-ops on the INSERT), and ChargePrepaidBatch sets the Stripe idempotency key.
// On a Stripe error it marks the row 'failed' and returns the error. The credit grant
// is created later on the invoice.paid webhook.
func (s *Server) chargeRetroactivePrepay(ctx context.Context, p retroactivePrepay) error {
	ct, err := s.pool.Exec(ctx, `
		INSERT INTO prepaid_storage_batches
			(batch_key, account_id, recording_id, stream_hours, charged_cents, status)
		VALUES ($1,$2,$3,$4,$5,'pending')
		ON CONFLICT (batch_key) DO NOTHING
	`, p.batchKey, p.accountID, p.recordingID, p.streamHours, p.cents)
	if err != nil {
		return fmt.Errorf("insert prepay batch: %w", err)
	}
	if ct.RowsAffected() == 0 {
		// Already submitted today (idempotent): do not charge again.
		return nil
	}
	res, err := s.billing.ChargePrepaidBatch(ctx, p.customerID, p.batchKey, p.cents, map[string]string{
		"account_id":   strconv.FormatInt(p.accountID, 10),
		"recording_id": strconv.FormatInt(p.recordingID, 10),
		"stream_hours": strconv.FormatFloat(p.streamHours, 'f', 4, 64),
		"kind":         "yearly_prepaid_storage_switch",
	})
	if err != nil {
		if _, uerr := s.pool.Exec(ctx, `
			UPDATE prepaid_storage_batches SET status='failed', updated_at=now() WHERE batch_key=$1
		`, p.batchKey); uerr != nil {
			return fmt.Errorf("charge prepay batch: %v; mark failed: %w", err, uerr)
		}
		return fmt.Errorf("charge prepay batch: %w", err)
	}
	if _, err := s.pool.Exec(ctx, `
		UPDATE prepaid_storage_batches
		SET status='charged', stripe_invoice_id=$2, stripe_invoice_item_id=$3, charged_at=now(), updated_at=now()
		WHERE batch_key=$1
	`, p.batchKey, res.InvoiceID, res.InvoiceItemID); err != nil {
		return fmt.Errorf("record charged batch: %w", err)
	}
	return nil
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

	// Released clips (sent to the NAS) and purged clips both stay in this per-recording
	// listing, each flagged for the UI. The NAS pull FEED (accountClipsCursorSQL) is the
	// only place that must exclude released clips; the researcher's own clip browser
	// shows the full history so a "Sent to NAS" clip is visible, not silently gone.
	var total int64
	if err := s.pool.QueryRow(r.Context(), `
		SELECT count(*) FROM recording_clips WHERE recording_id=$1
	`, id).Scan(&total); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("count clips: %v", err))
		return
	}

	// Page the list so a recording with hundreds of clips is browsable a page at a
	// time. limit defaults to 100 and is clamped to 1..500; offset defaults to 0.
	limit := parseIntQuery(r, "limit", 100, 1, 500)
	offset := parseIntQuery(r, "offset", 0, 0, 1<<30)

	rows, err := s.pool.Query(r.Context(), `
		SELECT id, fire_at, clip_start_at, clip_end_at, size_bytes, duration_ms, actual_fps, object_key, display_path, storage_destination_id, purged_at, released_at
		FROM recording_clips
		WHERE recording_id=$1
		ORDER BY fire_at DESC
		LIMIT $2 OFFSET $3
	`, id, limit, offset)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list clips: %v", err))
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0, 16)
	for rows.Next() {
		var (
			clipID       int64
			fireAt       time.Time
			clipStartAt  time.Time
			clipEndAt    time.Time
			sizeBytes    int64
			durationMs   int64
			actualFPS    *float64
			objectKey    string
			displayPath  string
			sourceDestID int64
			purgedAt     *time.Time
			releasedAt   *time.Time
		)
		if err := rows.Scan(&clipID, &fireAt, &clipStartAt, &clipEndAt, &sizeBytes, &durationMs, &actualFPS, &objectKey, &displayPath, &sourceDestID, &purgedAt, &releasedAt); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan clip: %v", err))
			return
		}
		items = append(items, map[string]any{
			"id":                     clipID,
			"fire_at":                fireAt.UTC(),
			"clip_start_at":          clipStartAt.UTC(),
			"clip_end_at":            clipEndAt.UTC(),
			"size_bytes":             sizeBytes,
			"duration_ms":            durationMs,
			"actual_fps":             actualFPS,
			"object_key":             objectKey,
			"display_path":           displayPath,
			"storage_destination_id": sourceDestID,
			"purged":                 purgedAt != nil,
			"released":               releasedAt != nil,
		})
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate clips: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"items":  items,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
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
		mode         string
		cronExpr     *string
		cronTimezone string
		dwStart      *string
		dwEnd        *string
		startAt      time.Time
		endAt        *time.Time
	)
	err := s.pool.QueryRow(r.Context(), `
		SELECT status, mode, cron_expr, cron_timezone,
		       to_char(daily_window_start, 'HH24:MI:SS'), to_char(daily_window_end, 'HH24:MI:SS'), start_at, end_at
		FROM recordings
		WHERE id=$1 AND account_id=$2 AND status <> 'canceled'
	`, id, principal.AccountID).Scan(&curStatus, &mode, &cronExpr, &cronTimezone, &dwStart, &dwEnd, &startAt, &endAt)
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
		nextFire, nerr := nextFireForRecording(mode, cronExpr, cronTimezone, dwStart, dwEnd, startAt, endAt, time.Now().UTC())
		if nerr != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("compute next fire: %v", nerr))
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
// path: it runs a bounded ffmpeg open on the already SSRF-validated streamURL to
// confirm the source is live. The original hostname URL is handed to ffmpeg
// directly (no host->IP rewrite) so TLS SNI + Host routing work for SNI/Host-
// routed CDNs; ValidatePublicURL has already rejected any host that resolves to a
// private/metadata address. validatedIP is retained by the caller as the proof
// the host resolved public.
func probeRecordingStreamReachable(ctx context.Context, streamURL string, validatedIP net.IP, inputHeaders string) error {
	_ = validatedIP
	probeCtx, cancel := context.WithTimeout(ctx, recordingProbeTimeout)
	defer cancel()
	return capture.ProbeReachableWithHeaders(probeCtx, streamURL, "", inputHeaders)
}

// resolveRecordingStreamURL resolves a pasted stream reference (e.g. a KBS '!hls'
// indirect URL) to the live playable URL so validation and the reachability probe
// run on the actual stream, and the composer previews the real stream. A direct
// .m3u8 passes through unchanged. The resolve fetch is SSRF-guarded inside
// capture.ResolveCaptureInput. Image sources are rejected (the recorder is
// video-only). The stored reference is left untouched; the worker re-resolves it
// fresh on every capture so expiring tokens never break a schedule.
func resolveRecordingStreamURL(ctx context.Context, provider, streamURL, sourcePageURL string) (string, string, error) {
	resolveCtx, cancel := context.WithTimeout(ctx, recordingResolveTimeout)
	defer cancel()
	resolved, isImage, inputHeaders, err := capture.ResolveCaptureInputWithHeaders(resolveCtx, provider, streamURL, sourcePageURL)
	if err != nil {
		return "", "", fmt.Errorf("could not resolve stream reference: %w", err)
	}
	if isImage {
		return "", "", fmt.Errorf("image sources are not supported for recording")
	}
	return resolved, inputHeaders, nil
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
		targetFPS        *int
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
		mode             string
		dailyWindowStart string
		dailyWindowEnd   string
		bundleID         *int64
		bundleName       *string
		retentionTier    string
		delivery         string
		captureVia       string
		namingProfile    string
		folderName       string
		namingMetadata   []byte
		hasRelayOnline   bool
		relayNodeName    *string
	)
	if err := row.Scan(
		&id, &name, &streamURL, &storageDestID, &storageDestName,
		&sourceKind, &cronExpr, &cronTimezone, &clipDurationSec, &targetFPS,
		&status, &startAt, &endAt, &nextFireAt, &lastClipAt,
		&lastErrorText, &lastErrorAt, &consecutiveFails,
		&hasPaymentMethod, &recentClipCount, &createdAt, &managed,
		&streamID, &streamName, &streamLocation,
		&mode, &dailyWindowStart, &dailyWindowEnd,
		&bundleID, &bundleName, &retentionTier, &delivery,
		&captureVia, &namingProfile, &folderName, &namingMetadata,
		&hasRelayOnline, &relayNodeName,
	); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	inWindow := !startAt.After(now) && (endAt == nil || now.Before(*endAt))
	live := status == "active" && inWindow && hasPaymentMethod
	needsCard := billingEnabled && !hasPaymentMethod
	naming, err := namingPayload(namingProfile, folderName, namingMetadata)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"id":                       id,
		"name":                     name,
		"stream_url":               streamURL,
		"storage_destination_id":   storageDestID,
		"storage_destination_name": storageDestName,
		"storage_managed":          managed,
		"source_kind":              sourceKind,
		"mode":                     mode,
		"cron_expr":                cronExpr,
		"cron_timezone":            cronTimezone,
		"clip_duration_sec":        clipDurationSec,
		"daily_window_start":       dailyWindowStart,
		"daily_window_end":         dailyWindowEnd,
		"target_fps":               targetFPS,
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
		"bundle_id":                bundleID,
		"bundle_name":              bundleName,
		"storage_retention_tier":   retentionTier,
		"delivery":                 delivery,
		"capture_via":              captureVia,
		"naming":                   naming,
		// Derived relay readiness. has_relay_assigned = a relay currently holds the
		// lease (relay_node_name is non-null). All false/null for cloud recordings.
		"has_relay_online":   hasRelayOnline,
		"has_relay_assigned": relayNodeName != nil,
		"relay_node_name":    relayNodeName,
	}, nil
}

// checkRecordingScheduleCapacity rejects a prospective schedule whose forecast
// peak simultaneous clip count (the existing capturing fleet plus this candidate)
// would exceed the pool ceiling Max*Capacity, i.e. more concurrent clips than the
// autoscaler could ever stand up. It reuses the dropletpool forecast sweep-line so
// the cap and the scaler share one demand model. The lookahead matches the
// autoscaler's so a schedule accepted here is one Decide can actually provision
// for; the error is user-facing (no em dashes).
func (s *Server) checkRecordingScheduleCapacity(ctx context.Context, cronExpr, cronTimezone string, clipDurationSec int) error {
	ceiling := s.cfg.DropletPoolMax * s.cfg.DropletPoolCapacity
	if ceiling <= 0 {
		// Pool config not set on this service: no meaningful ceiling to enforce.
		return nil
	}
	billingEnabled := s.billing != nil
	lookahead := time.Duration(s.cfg.DropletPoolLookaheadSec) * time.Second
	if lookahead <= 0 {
		lookahead = 30 * time.Minute
	}
	peak, err := dropletpool.ForecastPeakWithCandidate(ctx, s.pool, billingEnabled, cronExpr, cronTimezone, clipDurationSec, time.Now().UTC(), lookahead)
	if err != nil {
		return fmt.Errorf("forecast schedule capacity: %v", err)
	}
	if peak > ceiling {
		return fmt.Errorf("at capacity: this schedule would need %d clips recording at once, above the recorder limit of %d. Stagger the schedule or contact the operator to raise the cap.", peak, ceiling)
	}
	return nil
}

// nextFireForRecording computes the next_fire_at instant for a recording given
// its mode: for sampled it is the next cron fire; for continuous it is the next
// daily-window open instant. The daily window strings are "HH:MM:SS" (nil for a
// sampled recording). It is the single mode-aware next-fire authority shared by
// the create, resume, and (bundle) member-resume paths.
func nextFireForRecording(mode string, cronExpr *string, cronTimezone string, dwStart, dwEnd *string, startAt time.Time, endAt *time.Time, now time.Time) (time.Time, error) {
	if mode == "continuous" {
		if dwStart == nil {
			return time.Time{}, fmt.Errorf("continuous recording has no daily window")
		}
		start, err := recsched.ParseTimeOfDay(*dwStart)
		if err != nil {
			return time.Time{}, err
		}
		var env time.Time
		if endAt != nil {
			env = endAt.UTC()
		}
		return recsched.NextWindowOpenUTC(cronTimezone, start, startAt.UTC(), env, now)
	}
	if cronExpr == nil {
		return time.Time{}, fmt.Errorf("sampled recording has no cron_expr")
	}
	return recsched.NextFireUTC(*cronExpr, cronTimezone, now)
}

// endAtTime extracts the time.Time from an end_at insert arg (an any that is
// either a time.Time or nil for open-ended), returning the zero time for nil.
func endAtTime(endAtArg any) time.Time {
	if endAtArg == nil {
		return time.Time{}
	}
	if t, ok := endAtArg.(time.Time); ok {
		return t.UTC()
	}
	return time.Time{}
}

// checkContinuousScheduleCapacity rejects a prospective continuous schedule (or a
// continuous bundle of memberCount identical streams) whose forecast peak, with
// everything already capturing, would exceed the pool ceiling Max*Capacity. A
// continuous stream is a constant +1 slot for its whole window, so memberCount
// streams add +memberCount at the shared window. To count all members
// simultaneously regardless of how far the window is from now, the forecast is
// anchored just after the NEXT window-open occurrence (where every member's
// constant slot overlaps), not at now. No ceiling/Capacity/Max change.
func (s *Server) checkContinuousScheduleCapacity(ctx context.Context, cronTimezone string, start, end recsched.TimeOfDay, clipDurationSec int, envStart, envEnd time.Time, memberCount int) error {
	ceiling := s.cfg.DropletPoolMax * s.cfg.DropletPoolCapacity
	if ceiling <= 0 {
		return nil
	}
	if memberCount <= 0 {
		return nil
	}
	billingEnabled := s.billing != nil

	// Anchor the evaluation just after the next window-open so all N members'
	// constant slots overlap and the +N is fully counted vs the ceiling.
	nextOpen, err := recsched.NextWindowOpenUTC(cronTimezone, start, envStart, envEnd, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("forecast continuous capacity: %v", err)
	}
	anchor := time.Now().UTC()
	if !nextOpen.IsZero() {
		anchor = nextOpen.Add(time.Second)
	}
	// A lookahead long enough to cover the window so the constant interval registers.
	lookahead := time.Duration(end.Hour-start.Hour+1)*time.Hour + 5*time.Minute

	dws := timeOfDayString(start)
	dwe := timeOfDayString(end)
	candidates := make([]dropletpool.ForecastCandidate, memberCount)
	for i := range candidates {
		candidates[i] = dropletpool.ForecastCandidate{
			Mode:             "continuous",
			CronTimezone:     cronTimezone,
			ClipDurationSec:  clipDurationSec,
			DailyWindowStart: dws,
			DailyWindowEnd:   dwe,
			EnvStart:         envStart,
			EnvEnd:           envEnd,
		}
	}
	peak, err := dropletpool.ForecastPeakWithCandidates(ctx, s.pool, billingEnabled, candidates, anchor, lookahead)
	if err != nil {
		return fmt.Errorf("forecast continuous capacity: %v", err)
	}
	if peak > ceiling {
		return fmt.Errorf("this continuous selection peaks at %d concurrent streams, above the recorder limit of %d. Reduce the number of streams or contact the operator to raise the cap.", peak, ceiling)
	}
	return nil
}

// timeOfDayString renders a TimeOfDay as HH:MM:SS for the forecast candidate.
func timeOfDayString(t recsched.TimeOfDay) string {
	return fmt.Sprintf("%02d:%02d:%02d", t.Hour, t.Minute, t.Second)
}

// checkBundleScheduleCapacity rejects a prospective bundle whose forecast peak
// simultaneous clip count, combined with the existing capturing fleet, would
// exceed the pool ceiling Max*Capacity. Because every bundle member shares ONE
// cron/tz/clip, the memberCount candidates are identical and all fire together,
// so the honest peak rises by memberCount at the bundle's fire instants. It
// reuses the same sweep-line as the single-recording cap (DRY) and rejects the
// WHOLE bundle up front with one clear message, so an over-cap bundle leaves zero
// rows behind. The error is user-facing (no em dashes).
func (s *Server) checkBundleScheduleCapacity(ctx context.Context, cronExpr, cronTimezone string, clipDurationSec, memberCount int) error {
	ceiling := s.cfg.DropletPoolMax * s.cfg.DropletPoolCapacity
	if ceiling <= 0 {
		// Pool config not set on this service: no meaningful ceiling to enforce.
		return nil
	}
	if memberCount <= 0 {
		return nil
	}
	billingEnabled := s.billing != nil
	lookahead := time.Duration(s.cfg.DropletPoolLookaheadSec) * time.Second
	if lookahead <= 0 {
		lookahead = 30 * time.Minute
	}
	candidates := make([]dropletpool.ForecastCandidate, memberCount)
	for i := range candidates {
		candidates[i] = dropletpool.ForecastCandidate{
			CronExpr:        cronExpr,
			CronTimezone:    cronTimezone,
			ClipDurationSec: clipDurationSec,
		}
	}
	peak, err := dropletpool.ForecastPeakWithCandidates(ctx, s.pool, billingEnabled, candidates, time.Now().UTC(), lookahead)
	if err != nil {
		return fmt.Errorf("forecast bundle capacity: %v", err)
	}
	if peak > ceiling {
		return fmt.Errorf("this bundle peaks at %d concurrent clips, above the recorder limit of %d. Reduce the number of streams, stagger the schedule, or contact the operator to raise the cap.", peak, ceiling)
	}
	return nil
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
