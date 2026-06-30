package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/daydemir/stoarama/backend/internal/recsched"
	"github.com/daydemir/stoarama/backend/internal/util"
)

// bundleCreateRequest is the create payload for a recording bundle: a stream
// selection (explicit ids and/or a single tag the server resolves) plus ONE
// shared schedule that every fanned-out member recording inherits unchanged. The
// timezone is user-chosen and applied uniformly; it is NEVER resolved per-stream.
type bundleCreateRequest struct {
	Name string `json:"name"`
	// StreamIDs is an explicit catalog-stream selection. Tag (optional) is unioned
	// in: the server resolves it to stream ids via streams.tags && ARRAY[tag]. The
	// resolved member set is the de-duplicated union of both.
	StreamIDs []int64 `json:"stream_ids"`
	Tag       string  `json:"tag"`
	// Shared schedule, identical to a single recording's fields. cron_timezone is
	// the user's uniform choice applied to all members.
	CronExpr        string `json:"cron_expr"`
	CronTimezone    string `json:"cron_timezone"`
	ClipDurationSec int    `json:"clip_duration_sec"`
	// Mode is 'sampled' (default) or 'continuous'. A continuous bundle fans out
	// continuous members sharing the daily window; cron_expr is ignored.
	Mode                         string     `json:"mode"`
	DailyWindowStart             string     `json:"daily_window_start"`
	DailyWindowEnd               string     `json:"daily_window_end"`
	TargetFPS                    *int       `json:"target_fps"`
	StartAt                      *time.Time `json:"start_at"`
	EndAt                        *time.Time `json:"end_at"`
	StorageDestinationID         int64      `json:"storage_destination_id"`
	DeliveryStorageDestinationID int64      `json:"delivery_storage_destination_id"`
}

// bundleMember is one resolved catalog stream selected for a bundle, captured
// before the fan-out tx so each member recording can be inserted from a stable
// reference (the catalog source_url is authoritative and re-resolved each fire).
type bundleMember struct {
	streamID   int64
	streamName string
	sourceURL  string
	sourceKind string
}

// handleAccountBundleStreams lists recordable catalog streams for the bundle
// composer's multi-select: id, name, location, and tags, optionally narrowed by a
// case-insensitive name/location search (q) and/or a tag filter. Only HLS/HTTPS
// streams with a non-empty source_url are returned (the same recordability gate
// the fan-out applies), so the picker never offers a stream the create would
// reject. Capped so the picker stays light.
func (s *Server) handleAccountBundleStreams(w http.ResponseWriter, r *http.Request) {
	if _, ok := accountPrincipalFromContext(r.Context()); !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	tag := strings.TrimSpace(r.URL.Query().Get("tag"))
	limit := parseIntQuery(r, "limit", 500, 1, 2000)

	rows, err := s.pool.Query(r.Context(), `
		SELECT id, name, location_text, source_url, tags
		FROM streams
		WHERE source_url <> ''
		  AND ($1 = '' OR name ILIKE '%'||$1||'%' OR location_text ILIKE '%'||$1||'%')
		  AND ($2 = '' OR tags && ARRAY[$2]::text[])
		ORDER BY name ASC, id ASC
		LIMIT $3
	`, q, tag, limit)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list bundle streams: %v", err))
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0, 64)
	for rows.Next() {
		var (
			id        int64
			nm        string
			loc       string
			sourceURL string
			tags      []string
		)
		if err := rows.Scan(&id, &nm, &loc, &sourceURL, &tags); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan stream: %v", err))
			return
		}
		// Only surface streams the fan-out would accept (HLS/HTTPS).
		if _, kerr := classifyRecordingSource(strings.TrimSpace(sourceURL)); kerr != nil {
			continue
		}
		items = append(items, map[string]any{
			"id":            id,
			"name":          nm,
			"location_text": loc,
			"tags":          tags,
		})
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate streams: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) handleAccountBundlesCreate(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var req bundleCreateRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		util.WriteError(w, http.StatusBadRequest, "name is required")
		return
	}
	tag := strings.TrimSpace(req.Tag)
	if len(req.StreamIDs) == 0 && tag == "" {
		util.WriteError(w, http.StatusBadRequest, "select streams by stream_ids and/or a tag")
		return
	}

	// Exactly one destination selector is required, mirroring single-create.
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
	clipDuration := req.ClipDurationSec
	if clipDuration == 0 {
		clipDuration = 60
	}
	if clipDuration < 5 || clipDuration > 900 {
		util.WriteError(w, http.StatusBadRequest, "clip_duration_sec must be between 5 and 900")
		return
	}

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

	var targetFPSArg any
	if req.TargetFPS != nil {
		if *req.TargetFPS != 15 && *req.TargetFPS != 30 {
			util.WriteError(w, http.StatusBadRequest, "target_fps must be 15 or 30 (omit for Source)")
			return
		}
		targetFPSArg = *req.TargetFPS
	}

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

	// Validate the shared schedule ONCE for the whole bundle. Continuous validated
	// its window above; sampled validates the cron floor here.
	if mode != "continuous" {
		if err := recsched.ValidateCronForCreate(cronExpr, cronTimezone, s.cfg.RecSchedMinIntervalSec, clipDuration); err != nil {
			util.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	// Resolve the member set: union of explicit ids and the tag filter, dedup by
	// id, ordered by id. A bundle may be up to ~1000 members, so each stream is
	// validated statically (existence + non-empty source_url + recordable classify
	// via classifyRecordingSource) rather than probed; an ffmpeg probe per member
	// is infeasible at create time and the worker re-resolves each fire anyway.
	members, err := s.resolveBundleMembers(r.Context(), req.StreamIDs, tag)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(members) == 0 {
		util.WriteError(w, http.StatusBadRequest, "no recordable streams matched the selection")
		return
	}

	// Capacity preflight over ALL N members at once BEFORE any insert: reject the
	// whole bundle if its forecast peak exceeds the current cap, so an over-cap
	// bundle leaves zero rows behind. A continuous bundle counts each member as a
	// FULL constant slot for the shared window (so N members = +N at the window).
	if mode == "continuous" {
		if err := s.checkContinuousScheduleCapacity(r.Context(), cronTimezone, dailyStart, dailyEnd, clipDuration, startAt, endAtTime(endAtArg), len(members)); err != nil {
			util.WriteError(w, http.StatusConflict, err.Error())
			return
		}
	} else {
		if err := s.checkBundleScheduleCapacity(r.Context(), cronExpr, cronTimezone, clipDuration, len(members)); err != nil {
			util.WriteError(w, http.StatusConflict, err.Error())
			return
		}
	}

	// Resolve the capture (and optional WebDAV delivery) destination exactly as
	// single-create does: owner-or-granted predicate + verified. A WebDAV delivery
	// target forces capture into the account's managed staging area.
	captureDestID := req.StorageDestinationID
	var deliveryDestArg any
	if req.DeliveryStorageDestinationID > 0 {
		var destStatus, destProvider string
		err := s.pool.QueryRow(r.Context(), fmt.Sprintf(`
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
		var destStatus string
		err := s.pool.QueryRow(r.Context(), fmt.Sprintf(`
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

	// One next_fire computed from the shared schedule, applied to every member.
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

	// Bundle name uniqueness (account_id, lower(name)).
	var bundleNameExists bool
	if err := s.pool.QueryRow(r.Context(), `
		SELECT EXISTS(SELECT 1 FROM recording_bundles WHERE account_id=$1 AND lower(name)=lower($2) AND status <> 'canceled')
	`, principal.AccountID, name).Scan(&bundleNameExists); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("check bundle name: %v", err))
		return
	}
	if bundleNameExists {
		util.WriteError(w, http.StatusConflict, "a bundle with that name already exists")
		return
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin bundle create tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	var (
		bundleID  int64
		createdAt time.Time
	)
	var bundleCronExprArg any
	if mode != "continuous" {
		bundleCronExprArg = cronExpr
	}
	err = tx.QueryRow(r.Context(), `
		INSERT INTO recording_bundles
			(account_id, name, mode, cron_expr, cron_timezone, clip_duration_sec, daily_window_start, daily_window_end, target_fps, start_at, end_at, storage_destination_id, delivery_storage_destination_id, status)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'active')
		RETURNING id, created_at
	`, principal.AccountID, name, mode, bundleCronExprArg, cronTimezone, clipDuration, dailyStartArg, dailyEndArg, targetFPSArg, startAt, endAtArg, captureDestID, deliveryDestArg).Scan(&bundleID, &createdAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			util.WriteError(w, http.StatusConflict, "a bundle with that name already exists")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("create bundle: %v", err))
		return
	}

	// Fan out: one normal recording per member via the shared insert helper, all
	// carrying this bundle_id and the SAME shared schedule/tz/clip/fps/dest. The
	// member recording name is derived deterministically from the bundle name and
	// the stream so it satisfies the per-account name uniqueness index. A name
	// collision (23505) fails the WHOLE bundle (fail-fast, no silent rename).
	var memberCronExprArg any
	if mode != "continuous" {
		memberCronExprArg = cronExpr
	}
	for _, m := range members {
		memberName := bundleMemberRecordingName(name, m.streamName, m.streamID)
		_, _, _, _, ierr := s.insertRecordingTx(r.Context(), tx, recordingInsertParams{
			accountID:           principal.AccountID,
			captureDestID:       captureDestID,
			deliveryDestArg:     deliveryDestArg,
			name:                memberName,
			streamURL:           m.sourceURL,
			streamIDArg:         m.streamID,
			sourceKind:          m.sourceKind,
			mode:                mode,
			cronExprArg:         memberCronExprArg,
			cronTimezone:        cronTimezone,
			clipDuration:        clipDuration,
			dailyWindowStartArg: dailyStartArg,
			dailyWindowEndArg:   dailyEndArg,
			targetFPSArg:        targetFPSArg,
			nextFireArg:         nextFireArg,
			startAt:             startAt,
			endAtArg:            endAtArg,
			bundleIDArg:         bundleID,
		})
		if ierr != nil {
			var pgErr *pgconn.PgError
			if errors.As(ierr, &pgErr) && pgErr.Code == "23505" {
				util.WriteError(w, http.StatusConflict, fmt.Sprintf("a recording named %q already exists; pick a different bundle name", memberName))
				return
			}
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("fan out member for stream %d: %v", m.streamID, ierr))
			return
		}
	}

	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit bundle create tx: %v", err))
		return
	}

	_ = s.insertAccountAuthEvent(r.Context(), principal.AccountID, nil, "bundle_created", "account", principal.Email, map[string]any{
		"bundle_id":         bundleID,
		"member_count":      len(members),
		"cron_expr":         cronExpr,
		"cron_timezone":     cronTimezone,
		"clip_duration_sec": clipDuration,
	})

	util.WriteJSON(w, http.StatusCreated, map[string]any{
		"id":           bundleID,
		"name":         name,
		"status":       "active",
		"member_count": len(members),
		"created_at":   createdAt.UTC(),
		"start_at":     startAt,
		"end_at":       endAtArg,
	})
}

// resolveBundleMembers loads the de-duplicated union of the explicit stream ids
// and the tag-filtered streams, validating each as recordable. It rejects the
// whole selection (naming the bad stream) if any matched stream has no
// source_url or is not an HLS/HTTPS source. Returned in ascending id order so the
// fan-out is deterministic.
func (s *Server) resolveBundleMembers(ctx context.Context, streamIDs []int64, tag string) ([]bundleMember, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, name, source_url
		FROM streams
		WHERE (id = ANY($1) OR ($2 <> '' AND tags && ARRAY[$2]::text[]))
		ORDER BY id ASC
	`, streamIDs, tag)
	if err != nil {
		return nil, fmt.Errorf("resolve bundle streams: %v", err)
	}
	defer rows.Close()
	matched := make(map[int64]bool, 16)
	members := make([]bundleMember, 0, 16)
	for rows.Next() {
		var (
			id        int64
			nm        string
			sourceURL string
		)
		if err := rows.Scan(&id, &nm, &sourceURL); err != nil {
			return nil, fmt.Errorf("scan stream: %v", err)
		}
		if matched[id] {
			continue
		}
		matched[id] = true
		sourceURL = strings.TrimSpace(sourceURL)
		if sourceURL == "" {
			return nil, fmt.Errorf("stream %d (%s) has no source_url and cannot be recorded", id, nm)
		}
		kind, kerr := classifyRecordingSource(sourceURL)
		if kerr != nil {
			return nil, fmt.Errorf("stream %d (%s) is not recordable: %v", id, nm, kerr)
		}
		members = append(members, bundleMember{streamID: id, streamName: nm, sourceURL: sourceURL, sourceKind: kind})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate streams: %v", err)
	}
	// Any explicit id that matched nothing is a hard error (the user named a
	// stream that does not exist).
	for _, id := range streamIDs {
		if id > 0 && !matched[id] {
			return nil, fmt.Errorf("stream %d not found", id)
		}
	}
	return members, nil
}

// bundleMemberRecordingName derives a deterministic per-member recording name
// that satisfies the per-account name uniqueness index. It is "<bundle> :: <stream
// or stream-<id>>"; the stream id is appended when the stream has no name so two
// unnamed streams cannot collide.
func bundleMemberRecordingName(bundleName, streamName string, streamID int64) string {
	sn := strings.TrimSpace(streamName)
	if sn == "" {
		sn = fmt.Sprintf("stream-%d", streamID)
	}
	return fmt.Sprintf("%s :: %s", bundleName, sn)
}

// handleAccountBundlesList returns the caller's bundles with a schedule summary,
// live member count (members not individually canceled), and record-day count +
// cost-to-date aggregated from recording_billing_days across all members. The
// bundle's own status is the aggregate status. Newest first.
func (s *Server) handleAccountBundlesList(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	rows, err := s.pool.Query(r.Context(), `
		SELECT
			b.id, b.name, COALESCE(b.cron_expr,''), b.cron_timezone, b.clip_duration_sec, b.target_fps,
			b.start_at, b.end_at, b.status, b.created_at,
			(SELECT count(*) FROM recordings rec WHERE rec.bundle_id=b.id AND rec.status <> 'canceled') AS member_count,
			(SELECT count(*) FROM recording_billing_days d
			   JOIN recordings rec ON rec.id = d.recording_id
			   WHERE rec.bundle_id = b.id) AS record_days,
			b.mode, COALESCE(to_char(b.daily_window_start,'HH24:MI'),''), COALESCE(to_char(b.daily_window_end,'HH24:MI'),'')
		FROM recording_bundles b
		WHERE b.account_id=$1 AND b.status <> 'canceled'
		ORDER BY b.created_at DESC, b.id DESC
	`, principal.AccountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list bundles: %v", err))
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0, 8)
	for rows.Next() {
		item, err := scanBundleListRow(rows)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan bundle: %v", err))
			return
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate bundles: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

// recordDayCost is the per-record-day rate in USD cents, mirroring the existing
// usage-billing rate ($0.50/record-day). Cost-to-date = record_days * this.
const recordDayCostCents = 50

func scanBundleListRow(row pgx.Row) (map[string]any, error) {
	var (
		id              int64
		name            string
		cronExpr        string
		cronTimezone    string
		clipDurationSec int
		targetFPS       *int
		startAt         time.Time
		endAt           *time.Time
		status          string
		createdAt       time.Time
		memberCount     int64
		recordDays      int64
		mode            string
		dwStart         string
		dwEnd           string
	)
	if err := row.Scan(
		&id, &name, &cronExpr, &cronTimezone, &clipDurationSec, &targetFPS,
		&startAt, &endAt, &status, &createdAt, &memberCount, &recordDays,
		&mode, &dwStart, &dwEnd,
	); err != nil {
		return nil, err
	}
	return map[string]any{
		"id":                 id,
		"name":               name,
		"mode":               mode,
		"cron_expr":          cronExpr,
		"cron_timezone":      cronTimezone,
		"clip_duration_sec":  clipDurationSec,
		"daily_window_start": dwStart,
		"daily_window_end":   dwEnd,
		"target_fps":         targetFPS,
		"start_at":           startAt.UTC(),
		"end_at":             endAt,
		"status":             status,
		"created_at":         createdAt.UTC(),
		"member_count":       memberCount,
		"record_days":        recordDays,
		"cost_cents":         recordDays * recordDayCostCents,
	}, nil
}

// handleAccountBundleGet returns one bundle (the shared schedule) plus each member
// recording with its status/health/last-clip/next-fire/clip-count, joined by
// bundle_id and scoped to the caller. 404 when the bundle is not the caller's.
func (s *Server) handleAccountBundleGet(w http.ResponseWriter, r *http.Request) {
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
		name            string
		cronExpr        string
		cronTimezone    string
		clipDurationSec int
		targetFPS       *int
		startAt         time.Time
		endAt           *time.Time
		status          string
		createdAt       time.Time
		storageDestID   int64
		deliveryDestID  *int64
		mode            string
		dwStart         string
		dwEnd           string
	)
	err := s.pool.QueryRow(r.Context(), `
		SELECT name, COALESCE(cron_expr,''), cron_timezone, clip_duration_sec, target_fps, start_at, end_at, status, created_at, storage_destination_id, delivery_storage_destination_id,
		       mode, COALESCE(to_char(daily_window_start,'HH24:MI'),''), COALESCE(to_char(daily_window_end,'HH24:MI'),'')
		FROM recording_bundles
		WHERE id=$1 AND account_id=$2 AND status <> 'canceled'
	`, id, principal.AccountID).Scan(&name, &cronExpr, &cronTimezone, &clipDurationSec, &targetFPS, &startAt, &endAt, &status, &createdAt, &storageDestID, &deliveryDestID, &mode, &dwStart, &dwEnd)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "bundle not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load bundle: %v", err))
		return
	}

	rows, err := s.pool.Query(r.Context(), `
		SELECT rec.id, rec.name, rec.stream_id, st.name, st.location_text,
		       rec.status, rec.consecutive_failures, rec.last_clip_at, rec.next_fire_at,
		       (SELECT count(*) FROM recording_clips c WHERE c.recording_id = rec.id) AS clip_count
		FROM recordings rec
		LEFT JOIN streams st ON st.id = rec.stream_id
		WHERE rec.bundle_id=$1 AND rec.account_id=$2 AND rec.status <> 'canceled'
		ORDER BY rec.id ASC
	`, id, principal.AccountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load bundle members: %v", err))
		return
	}
	defer rows.Close()
	members := make([]map[string]any, 0, 16)
	for rows.Next() {
		var (
			recID          int64
			recName        string
			streamID       *int64
			streamName     *string
			streamLocation *string
			recStatus      string
			consecFails    int
			lastClipAt     *time.Time
			nextFireAt     *time.Time
			clipCount      int64
		)
		if err := rows.Scan(&recID, &recName, &streamID, &streamName, &streamLocation, &recStatus, &consecFails, &lastClipAt, &nextFireAt, &clipCount); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan bundle member: %v", err))
			return
		}
		members = append(members, map[string]any{
			"recording_id":         recID,
			"name":                 recName,
			"stream_id":            streamID,
			"stream_name":          streamName,
			"stream_location":      streamLocation,
			"status":               recStatus,
			"health":               recordingHealth(recStatus, consecFails),
			"consecutive_failures": consecFails,
			"last_clip_at":         lastClipAt,
			"next_fire_at":         nextFireAt,
			"clip_count":           clipCount,
		})
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate bundle members: %v", err))
		return
	}

	util.WriteJSON(w, http.StatusOK, map[string]any{
		"id":                              id,
		"name":                            name,
		"mode":                            mode,
		"cron_expr":                       cronExpr,
		"cron_timezone":                   cronTimezone,
		"clip_duration_sec":               clipDurationSec,
		"daily_window_start":              dwStart,
		"daily_window_end":                dwEnd,
		"target_fps":                      targetFPS,
		"start_at":                        startAt.UTC(),
		"end_at":                          endAt,
		"status":                          status,
		"created_at":                      createdAt.UTC(),
		"storage_destination_id":          storageDestID,
		"delivery_storage_destination_id": deliveryDestID,
		"members":                         members,
	})
}

// handleAccountBundleClips enumerates the bundle's member clips, newest fire
// first, with the member recording name so the dense table can label the source.
// It is account-scoped (404 if the bundle is not the caller's) and paginated with
// the same 100/page contract as the per-recording clip list.
func (s *Server) handleAccountBundleClips(w http.ResponseWriter, r *http.Request) {
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
	if err := s.pool.QueryRow(r.Context(), `
		SELECT EXISTS(SELECT 1 FROM recording_bundles WHERE id=$1 AND account_id=$2 AND status <> 'canceled')
	`, id, principal.AccountID).Scan(&ownerOK); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load bundle: %v", err))
		return
	}
	if !ownerOK {
		util.WriteError(w, http.StatusNotFound, "bundle not found")
		return
	}

	var total int64
	if err := s.pool.QueryRow(r.Context(), `
		SELECT count(*) FROM recording_clips c
		JOIN recordings rec ON rec.id = c.recording_id
		WHERE rec.bundle_id=$1 AND rec.account_id=$2
	`, id, principal.AccountID).Scan(&total); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("count bundle clips: %v", err))
		return
	}

	limit := parseIntQuery(r, "limit", 100, 1, 500)
	offset := parseIntQuery(r, "offset", 0, 0, 1<<30)

	rows, err := s.pool.Query(r.Context(), `
		SELECT c.id, c.recording_id, rec.name, c.fire_at, c.clip_start_at, c.clip_end_at,
		       c.size_bytes, c.duration_ms, c.actual_fps, c.object_key, c.storage_destination_id, c.purged_at
		FROM recording_clips c
		JOIN recordings rec ON rec.id = c.recording_id
		WHERE rec.bundle_id=$1 AND rec.account_id=$2
		ORDER BY c.fire_at DESC, c.id DESC
		LIMIT $3 OFFSET $4
	`, id, principal.AccountID, limit, offset)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list bundle clips: %v", err))
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0, 16)
	for rows.Next() {
		var (
			clipID       int64
			recordingID  int64
			recordingNm  string
			fireAt       time.Time
			clipStartAt  time.Time
			clipEndAt    time.Time
			sizeBytes    int64
			durationMs   int64
			actualFPS    *float64
			objectKey    string
			sourceDestID int64
			purgedAt     *time.Time
		)
		if err := rows.Scan(&clipID, &recordingID, &recordingNm, &fireAt, &clipStartAt, &clipEndAt, &sizeBytes, &durationMs, &actualFPS, &objectKey, &sourceDestID, &purgedAt); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan bundle clip: %v", err))
			return
		}
		items = append(items, map[string]any{
			"id":                     clipID,
			"recording_id":           recordingID,
			"recording_name":         recordingNm,
			"fire_at":                fireAt.UTC(),
			"clip_start_at":          clipStartAt.UTC(),
			"clip_end_at":            clipEndAt.UTC(),
			"size_bytes":             sizeBytes,
			"duration_ms":            durationMs,
			"actual_fps":             actualFPS,
			"object_key":             objectKey,
			"storage_destination_id": sourceDestID,
			"purged":                 purgedAt != nil,
		})
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate bundle clips: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"items":  items,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

func (s *Server) handleAccountBundlePause(w http.ResponseWriter, r *http.Request) {
	s.setBundleStatus(w, r, "active", "paused", "bundle_paused")
}

func (s *Server) handleAccountBundleResume(w http.ResponseWriter, r *http.Request) {
	s.setBundleStatus(w, r, "paused", "active", "bundle_resumed")
}

// setBundleStatus flips the bundle between active and paused and cascades the same
// flip to every member recording in ONE tx, mirroring setRecordingStatus. Pause
// stops scheduling (status='paused', next_fire_at=NULL); resume re-enables
// (status='active', next_fire_at recomputed once from the shared cron/tz and
// applied to all resumed members). Members individually canceled stay canceled.
func (s *Server) setBundleStatus(w http.ResponseWriter, r *http.Request, fromStatus, toStatus, eventType string) {
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
		FROM recording_bundles
		WHERE id=$1 AND account_id=$2 AND status <> 'canceled'
	`, id, principal.AccountID).Scan(&curStatus, &mode, &cronExpr, &cronTimezone, &dwStart, &dwEnd, &startAt, &endAt)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "bundle not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load bundle: %v", err))
		return
	}
	if curStatus == toStatus {
		util.WriteJSON(w, http.StatusOK, map[string]any{"id": id, "status": toStatus})
		return
	}
	if curStatus != fromStatus {
		util.WriteError(w, http.StatusConflict, fmt.Sprintf("cannot %s a bundle in status %q", eventType, curStatus))
		return
	}

	var nextFireArg any
	if toStatus == "active" {
		nextFire, ferr := nextFireForRecording(mode, cronExpr, cronTimezone, dwStart, dwEnd, startAt, endAt, time.Now().UTC())
		if ferr != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("compute next fire: %v", ferr))
			return
		}
		if !nextFire.IsZero() {
			nextFireArg = nextFire
		}
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin bundle status tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	ct, err := tx.Exec(r.Context(), `
		UPDATE recording_bundles SET status=$3, updated_at=now()
		WHERE id=$1 AND account_id=$2 AND status=$4
	`, id, principal.AccountID, toStatus, fromStatus)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("update bundle status: %v", err))
		return
	}
	if ct.RowsAffected() == 0 {
		util.WriteError(w, http.StatusConflict, "bundle status changed concurrently")
		return
	}

	// Cascade the same flip to members that are themselves in the from-status.
	if _, err := tx.Exec(r.Context(), `
		UPDATE recordings SET status=$3, next_fire_at=$4, updated_at=now()
		WHERE bundle_id=$1 AND account_id=$2 AND status=$5
	`, id, principal.AccountID, toStatus, nextFireArg, fromStatus); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("cascade member status: %v", err))
		return
	}

	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit bundle status tx: %v", err))
		return
	}

	_ = s.insertAccountAuthEvent(r.Context(), principal.AccountID, nil, eventType, "account", principal.Email, map[string]any{
		"bundle_id": id,
	})
	util.WriteJSON(w, http.StatusOK, map[string]any{"id": id, "status": toStatus})
}

// handleAccountBundleCancel terminates the whole bundle and cascades the
// single-recording cancel to every member in ONE tx: cancel the bundle, cancel
// all non-canceled member recordings (clearing next_fire_at), and cancel their
// pending/leased jobs so the worker stops capturing. Mirrors
// handleAccountRecordingDelete applied set-wide.
func (s *Server) handleAccountBundleCancel(w http.ResponseWriter, r *http.Request) {
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
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin bundle cancel tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	ct, err := tx.Exec(r.Context(), `
		UPDATE recording_bundles SET status='canceled', updated_at=now()
		WHERE id=$1 AND account_id=$2 AND status <> 'canceled'
	`, id, principal.AccountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("cancel bundle: %v", err))
		return
	}
	if ct.RowsAffected() == 0 {
		util.WriteError(w, http.StatusNotFound, "bundle not found")
		return
	}

	if _, err := tx.Exec(r.Context(), `
		UPDATE recordings SET status='canceled', next_fire_at=NULL, updated_at=now()
		WHERE bundle_id=$1 AND account_id=$2 AND status <> 'canceled'
	`, id, principal.AccountID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("cancel bundle members: %v", err))
		return
	}

	if _, err := tx.Exec(r.Context(), `
		UPDATE recording_jobs SET status='canceled', updated_at=now()
		WHERE recording_id IN (SELECT id FROM recordings WHERE bundle_id=$1 AND account_id=$2)
		  AND status IN ('pending','leased')
	`, id, principal.AccountID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("cancel bundle member jobs: %v", err))
		return
	}

	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit bundle cancel tx: %v", err))
		return
	}

	_ = s.insertAccountAuthEvent(r.Context(), principal.AccountID, nil, "bundle_canceled", "account", principal.Email, map[string]any{
		"bundle_id": id,
	})
	util.WriteJSON(w, http.StatusOK, map[string]any{"id": id, "status": "canceled"})
}

// handleAccountBundleExport zips one page of the bundle's member clips into the
// operator export bucket, reusing the day-zip job machinery + lock exactly like
// the per-recording export. The page is selected by limit/offset matching the
// bundle clips list. The job is polled via the shared GET /clips-zip/{jobId}.
func (s *Server) handleAccountBundleExport(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if s.secrets == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "storage credential key is unset")
		return
	}
	if s.r2 == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "export storage is not configured")
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req clipZipRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	offset := req.Offset
	if offset < 0 {
		offset = 0
	}

	var bundleName string
	err := s.pool.QueryRow(r.Context(), `
		SELECT name FROM recording_bundles WHERE id=$1 AND account_id=$2 AND status <> 'canceled'
	`, id, principal.AccountID).Scan(&bundleName)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "bundle not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load bundle: %v", err))
		return
	}

	rows, err := s.pool.Query(r.Context(), `
		SELECT c.id, c.clip_start_at, c.clip_end_at, c.duration_ms, c.size_bytes, c.object_key,
		       sd.region, sd.bucket, sd.endpoint, sd.access_key_id, sd.secret_access_key_enc
		FROM recording_clips c
		JOIN recordings rec ON rec.id = c.recording_id
		JOIN storage_destinations sd ON sd.id = c.storage_destination_id
		WHERE rec.bundle_id=$1 AND rec.account_id=$2 AND c.purged_at IS NULL
		ORDER BY c.fire_at DESC, c.id DESC
		LIMIT $3 OFFSET $4
	`, id, principal.AccountID, limit, offset)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list bundle clips: %v", err))
		return
	}
	defer rows.Close()
	var clips []clipZipRow
	var totalBytes int64
	for rows.Next() {
		var (
			z       clipZipRow
			clipID  int64
			startAt time.Time
			endAt   time.Time
		)
		if err := rows.Scan(&clipID, &startAt, &endAt, &z.row.DurationMs, &z.row.SizeBytes, &z.row.ObjectKey,
			&z.dest.region, &z.dest.bucket, &z.dest.endpoint, &z.dest.accessKeyID, &z.dest.secretEnc); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan clip: %v", err))
			return
		}
		end := endAt.UTC()
		z.row.ID = clipID
		z.row.SegmentStartAt = startAt.UTC()
		z.row.ClipEndAt = &end
		z.row.MIMEType = "video/mp4"
		totalBytes += z.row.SizeBytes
		clips = append(clips, z)
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate clips: %v", err))
		return
	}
	if len(clips) == 0 {
		util.WriteError(w, http.StatusNotFound, "no clips on this page")
		return
	}
	if totalBytes > dayZipMaxBytes {
		util.WriteError(w, http.StatusBadRequest, "page too large")
		return
	}

	select {
	case s.dayZipSlot <- struct{}{}:
	default:
		util.WriteError(w, http.StatusConflict, "busy")
		return
	}

	slug := slugifyName(bundleName)
	if slug == "" {
		slug = fmt.Sprintf("bundle-%d", id)
	}
	jobID := uuid.NewString()
	zipKey := fmt.Sprintf("exports/account/%d/bundle-%d-%s.zip", principal.AccountID, id, jobID)
	s.setDayZipJob(&dayZipJob{
		ID:        jobID,
		StreamID:  id,
		Status:    "pending",
		ZipKey:    zipKey,
		ItemCount: len(clips),
		SizeBytes: totalBytes,
	})
	go s.runClipsZipJob(jobID, slug, id, clips)

	util.WriteJSON(w, http.StatusAccepted, map[string]any{
		"job_id": jobID,
		"status": "pending",
	})
}
