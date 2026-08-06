package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/dropletpool"
	"github.com/daydemir/stoarama/backend/internal/model"
	"github.com/daydemir/stoarama/backend/internal/recordingnaming"
	"github.com/daydemir/stoarama/backend/internal/recsched"
	"github.com/daydemir/stoarama/backend/internal/util"
)

type batchScheduleMode string

const (
	batchSampled    batchScheduleMode = "sampled"
	batchContinuous batchScheduleMode = "continuous"

	batchEffectiveTimezoneSQL = `COALESCE(NULLIF(st.local_timezone,''), (SELECT rec.cron_timezone FROM recordings rec WHERE rec.account_id=$2 AND rec.stream_id=st.id AND rec.status <> 'canceled' ORDER BY rec.id DESC LIMIT 1), '')`
	batchTimezoneMissingSQL   = `st.local_timezone=''`
)

func parseBatchScheduleMode(raw string) (batchScheduleMode, error) {
	switch batchScheduleMode(strings.TrimSpace(raw)) {
	case batchSampled:
		return batchSampled, nil
	case batchContinuous:
		return batchContinuous, nil
	default:
		return "", fmt.Errorf("mode must be sampled or continuous")
	}
}

type streamTimezoneInput struct {
	StreamID int64  `json:"stream_id"`
	Timezone string `json:"timezone"`
}

type batchScheduleRequest struct {
	TargetAccountID              int64                 `json:"target_account_id"`
	StreamIDs                    []int64               `json:"stream_ids"`
	StreamTimezones              []streamTimezoneInput `json:"stream_timezones"`
	NamingProfile                string                `json:"naming_profile"`
	Mode                         string                `json:"mode"`
	CronExpr                     string                `json:"cron_expr"`
	ClipDurationSec              int                   `json:"clip_duration_sec"`
	DailyWindowStart             string                `json:"daily_window_start"`
	DailyWindowEnd               string                `json:"daily_window_end"`
	ActiveWeekdays               []int                 `json:"active_weekdays"`
	TargetFPS                    *int                  `json:"target_fps"`
	StartAt                      *time.Time            `json:"start_at"`
	EndAt                        *time.Time            `json:"end_at"`
	StorageDestinationID         int64                 `json:"storage_destination_id"`
	DeliveryStorageDestinationID int64                 `json:"delivery_storage_destination_id"`
	Delivery                     string                `json:"delivery"`
	DryRun                       bool                  `json:"dry_run"`
	RequiredRelaySlots           int                   `json:"required_relay_slots"`
}

type batchStream struct {
	id, recordingID, recordingCount                 int64
	name, sourceURL, provider, timezone, captureVia string
	timezoneMissing                                 bool
	namingDefaults                                  catalogNamingDefaults
}

func batchCaptureVia(sourceURL, provider, existing string) string {
	if isYouTubeWatchURL(sourceURL) || model.StreamRequiresRelay(provider, sourceURL) {
		return "relay"
	}
	if existing != "" {
		return existing
	}
	return "cloud"
}

type batchScheduleItem struct {
	StreamID    int64  `json:"stream_id"`
	RecordingID int64  `json:"recording_id"`
	Action      string `json:"action"`
	Timezone    string `json:"timezone"`
}

type batchScheduleResponse struct {
	Items              []batchScheduleItem `json:"items"`
	Created            int                 `json:"created"`
	Updated            int                 `json:"updated"`
	DryRun             bool                `json:"dry_run"`
	RelayStreams       int                 `json:"relay_streams"`
	OnlineRelaySlots   int                 `json:"online_relay_slots"`
	RequiredRelaySlots int                 `json:"required_relay_slots"`
}

func (s *Server) handleAccountRecordingsBatchSchedule(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var req batchScheduleRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	accountID := principal.AccountID
	if req.TargetAccountID < 0 {
		util.WriteError(w, http.StatusBadRequest, "target_account_id must be positive")
		return
	}
	if req.TargetAccountID > 0 {
		if principal.Role != accountRoleAdmin {
			util.WriteError(w, http.StatusForbidden, "target_account_id requires platform operator access")
			return
		}
		var status string
		if err := s.pool.QueryRow(r.Context(), `SELECT status FROM accounts WHERE id=$1`, req.TargetAccountID).Scan(&status); errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusBadRequest, "target account not found")
			return
		} else if err != nil {
			util.WriteError(w, http.StatusInternalServerError, "load target account")
			return
		} else if status != "active" {
			util.WriteError(w, http.StatusBadRequest, "target account is not active")
			return
		}
		accountID = req.TargetAccountID
	}
	ids, err := uniqueBatchStreamIDs(req.StreamIDs)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	mode, err := parseBatchScheduleMode(req.Mode)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	namingProfile, err := recordingnaming.ParseProfile(req.NamingProfile)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	delivery, err := parseDeliveryMode(strings.TrimSpace(req.Delivery))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if delivery == deliveryNASPull && req.DeliveryStorageDestinationID > 0 {
		util.WriteError(w, http.StatusBadRequest, "a NAS-pull recording cannot also deliver to an external destination")
		return
	}
	if (req.StorageDestinationID > 0) == (req.DeliveryStorageDestinationID > 0) {
		util.WriteError(w, http.StatusBadRequest, "exactly one storage destination is required")
		return
	}
	weekdays := recsched.AllWeekdays
	if req.ActiveWeekdays != nil {
		weekdays, err = recsched.NewWeekdaySet(req.ActiveWeekdays)
		if err != nil {
			util.WriteError(w, http.StatusBadRequest, err.Error())
			return
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
	if req.TargetFPS != nil && (*req.TargetFPS < 1 || *req.TargetFPS > 60) {
		util.WriteError(w, http.StatusBadRequest, "target_fps must be between 1 and 60 (omit for Source)")
		return
	}
	if req.RequiredRelaySlots < 0 {
		util.WriteError(w, http.StatusBadRequest, "required_relay_slots cannot be negative")
		return
	}
	requestNow := time.Now().UTC()
	startAt := effectiveRecordingStart(req.StartAt, requestNow)
	var endAt *time.Time
	if req.EndAt != nil {
		t := req.EndAt.UTC()
		if !t.After(startAt) {
			util.WriteError(w, http.StatusBadRequest, "end_at must be after start_at")
			return
		}
		endAt = &t
	}
	cronExpr := strings.TrimSpace(req.CronExpr)
	dailyStartRaw, dailyEndRaw := strings.TrimSpace(req.DailyWindowStart), strings.TrimSpace(req.DailyWindowEnd)
	var dailyStart, dailyEnd recsched.TimeOfDay
	if mode == batchContinuous {
		dailyStart, err = recsched.ParseTimeOfDay(dailyStartRaw)
		if err == nil {
			dailyEnd, err = recsched.ParseTimeOfDay(dailyEndRaw)
		}
		if err != nil {
			util.WriteError(w, http.StatusBadRequest, "daily_window_start and daily_window_end must be HH:MM")
			return
		}
		if err = recsched.ValidateContinuousWindowForCreate(dailyStart, dailyEnd, clipDuration); err != nil {
			util.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if err := recordingnaming.ValidateSchedule(namingProfile, string(mode), cronExpr, clipDuration, dailyStartRaw, dailyEndRaw); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	timezoneByID := make(map[int64]string, len(req.StreamTimezones))
	selected := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		selected[id] = struct{}{}
	}
	for _, item := range req.StreamTimezones {
		zone := strings.TrimSpace(item.Timezone)
		if _, ok := selected[item.StreamID]; !ok || zone == "" {
			util.WriteError(w, http.StatusBadRequest, "stream_timezones must reference selected streams and contain a timezone")
			return
		}
		if _, exists := timezoneByID[item.StreamID]; exists {
			util.WriteError(w, http.StatusBadRequest, "stream_timezones contains a duplicate stream_id")
			return
		}
		if _, err := recsched.LoadLocation(zone); err != nil {
			util.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		timezoneByID[item.StreamID] = zone
	}

	txOptions := pgx.TxOptions{}
	if req.DryRun {
		txOptions.AccessMode = pgx.ReadOnly
	}
	tx, err := s.pool.BeginTx(r.Context(), txOptions)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin batch schedule")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	streamLock := "FOR UPDATE"
	if req.DryRun {
		streamLock = ""
	}
	rows, err := tx.Query(r.Context(), fmt.Sprintf(`
		SELECT st.id, st.name, st.source_url, st.provider,
		       %s,
		       %s,
		       COALESCE(NULLIF(st.metadata_jsonb->>'continent',''), NULLIF(st.metadata_jsonb#>>'{csv_values,continent}',''), ''),
		       COALESCE(st.location_country,''), COALESCE(st.location_city,''), st.name,
		       COALESCE((SELECT rec.id FROM recordings rec WHERE rec.account_id=$2 AND rec.stream_id=st.id AND rec.status <> 'canceled' ORDER BY rec.id DESC LIMIT 1),0),
		       COALESCE((SELECT rec.capture_via FROM recordings rec WHERE rec.account_id=$2 AND rec.stream_id=st.id AND rec.status <> 'canceled' ORDER BY rec.id DESC LIMIT 1),''),
		       (SELECT count(*) FROM recordings rec WHERE rec.account_id=$2 AND rec.stream_id=st.id AND rec.status <> 'canceled')
		FROM streams st WHERE st.id=ANY($1::bigint[]) AND st.deleted_at IS NULL
		ORDER BY st.id %s
	`, batchEffectiveTimezoneSQL, batchTimezoneMissingSQL, streamLock), ids, accountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load batch streams")
		return
	}
	streams := make([]batchStream, 0, len(ids))
	for rows.Next() {
		var st batchStream
		if err := rows.Scan(&st.id, &st.name, &st.sourceURL, &st.provider, &st.timezone, &st.timezoneMissing, &st.namingDefaults.Continent, &st.namingDefaults.Country, &st.namingDefaults.City, &st.namingDefaults.PlazaName, &st.recordingID, &st.captureVia, &st.recordingCount); err != nil {
			rows.Close()
			util.WriteError(w, http.StatusInternalServerError, "load batch streams")
			return
		}
		streams = append(streams, st)
	}
	rows.Close()
	if len(streams) != len(ids) {
		util.WriteError(w, http.StatusNotFound, "one or more catalog streams were not found")
		return
	}
	for i := range streams {
		st := &streams[i]
		if st.recordingCount > 1 {
			util.WriteError(w, http.StatusConflict, fmt.Sprintf("stream %d has multiple active recordings; resolve them before batch scheduling", st.id))
			return
		}
		if st.timezoneMissing {
			if supplied := timezoneByID[st.id]; supplied != "" {
				st.timezone = supplied
			}
			if st.timezone == "" {
				util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("stream %d requires a local timezone", st.id))
				return
			}
			if !req.DryRun {
				if _, err := tx.Exec(r.Context(), `UPDATE streams SET local_timezone=$2, updated_at=now() WHERE id=$1 AND local_timezone=''`, st.id, st.timezone); err != nil {
					util.WriteError(w, http.StatusInternalServerError, "set stream timezone")
					return
				}
			}
		} else if supplied := timezoneByID[st.id]; supplied != "" && supplied != st.timezone {
			util.WriteError(w, http.StatusConflict, fmt.Sprintf("stream %d already has timezone %s", st.id, st.timezone))
			return
		}
		if _, err := recsched.LoadLocation(st.timezone); err != nil {
			util.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		if mode == batchSampled {
			if err := recsched.ValidateCronForCreate(cronExpr, st.timezone, s.cfg.RecSchedMinIntervalSec, clipDuration); err != nil {
				util.WriteError(w, http.StatusBadRequest, err.Error())
				return
			}
		}
	}
	relayStreams := 0
	for _, st := range streams {
		if batchCaptureVia(st.sourceURL, st.provider, st.captureVia) == "relay" {
			relayStreams++
		}
	}
	requiredRelaySlots := req.RequiredRelaySlots
	if requiredRelaySlots < relayStreams {
		requiredRelaySlots = relayStreams
	}
	// This is a preflight snapshot, not a reservation. The recording worker's
	// lease limits remain the enforcement backstop if capacity changes.
	onlineRelaySlots, err := availableRelayCapacity(r.Context(), tx, accountID, ids)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load relay capacity")
		return
	}
	if requiredRelaySlots > onlineRelaySlots {
		util.WriteError(w, http.StatusConflict, fmt.Sprintf("campaign requires %d relay slots, but only %d are available", requiredRelaySlots, onlineRelaySlots))
		return
	}
	ceiling := s.cfg.DropletPoolMax * s.cfg.DropletPoolCapacity
	if ceiling > 0 {
		candidates := make([]dropletpool.ForecastCandidate, 0, len(streams))
		excluded := make([]int64, 0, len(streams))
		for _, st := range streams {
			captureVia := batchCaptureVia(st.sourceURL, st.provider, st.captureVia)
			if st.recordingID > 0 {
				excluded = append(excluded, st.recordingID)
			}
			if captureVia == "relay" {
				continue
			}
			candidates = append(candidates, dropletpool.ForecastCandidate{Mode: string(mode), CronExpr: cronExpr, CronTimezone: st.timezone, ClipDurationSec: clipDuration, DailyWindowStart: dailyStartRaw, DailyWindowEnd: dailyEndRaw, EnvStart: startAt, EnvEnd: timeOrZero(endAt), ActiveWeekdays: weekdays})
		}
		peak, ferr := dropletpool.ForecastPeakWithCandidatesExcluding(r.Context(), s.pool, s.billing != nil, candidates, excluded, time.Now().UTC(), 8*24*time.Hour)
		if ferr != nil {
			util.WriteError(w, http.StatusInternalServerError, "forecast batch capacity")
			return
		}
		if peak > ceiling {
			util.WriteError(w, http.StatusConflict, fmt.Sprintf("this schedule peaks at %d concurrent streams, above the recorder limit of %d", peak, ceiling))
			return
		}
	}

	captureDestID := req.StorageDestinationID
	var deliveryDestArg any
	if delivery == deliveryNASPull {
		var hasConnection bool
		if err := tx.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM connections WHERE account_id=$1 AND kind='nas_pull')`, accountID).Scan(&hasConnection); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "check nas pull connection")
			return
		}
		if !hasConnection {
			util.WriteError(w, http.StatusBadRequest, "connect a NAS pull client before scheduling recordings to your NAS")
			return
		}
	}
	if req.DeliveryStorageDestinationID > 0 {
		var status, provider string
		err := tx.QueryRow(r.Context(), fmt.Sprintf(`SELECT status, provider FROM storage_destinations sd WHERE sd.id=$1 AND %s`, fmt.Sprintf(storageDestAccessPredicate, "$2")), req.DeliveryStorageDestinationID, accountID).Scan(&status, &provider)
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusBadRequest, "a verified WebDAV delivery_storage_destination_id is required")
			return
		}
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, "check delivery storage destination")
			return
		}
		if status != "verified" || provider != "webdav" {
			util.WriteError(w, http.StatusBadRequest, "a verified WebDAV delivery_storage_destination_id is required")
			return
		}
		if req.DryRun {
			if s.r2 == nil || s.secrets == nil || s.cfg.ValidateR2() != nil {
				util.WriteError(w, http.StatusServiceUnavailable, "managed staging unavailable")
				return
			}
		} else {
			managedID, _, err := s.provisionManagedDestination(r.Context(), tx, accountID)
			if err != nil {
				util.WriteError(w, http.StatusServiceUnavailable, "managed staging unavailable")
				return
			}
			captureDestID = managedID
		}
		deliveryDestArg = req.DeliveryStorageDestinationID
	} else {
		var verified, managed bool
		err := tx.QueryRow(r.Context(), fmt.Sprintf(`SELECT status='verified', managed FROM storage_destinations sd WHERE sd.id=$1 AND %s`, fmt.Sprintf(storageDestAccessPredicate, "$2")), req.StorageDestinationID, accountID).Scan(&verified, &managed)
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusBadRequest, "a verified storage_destination_id is required")
			return
		}
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, "check storage destination")
			return
		}
		if !verified {
			util.WriteError(w, http.StatusBadRequest, "a verified storage_destination_id is required")
			return
		}
		if delivery == deliveryNASPull && !managed {
			util.WriteError(w, http.StatusBadRequest, "NAS pull recordings require Stoarama-managed staging")
			return
		}
	}

	items := make([]batchScheduleItem, 0, len(streams))
	created, updated := 0, 0
	now := time.Now().UTC()
	startAt = effectiveRecordingStart(req.StartAt, now)
	if endAt != nil && !endAt.After(startAt) {
		util.WriteError(w, http.StatusBadRequest, "end_at must be after start_at")
		return
	}
	for _, st := range streams {
		captureVia := batchCaptureVia(st.sourceURL, st.provider, st.captureVia)
		namingRequest := &recordingNamingRequest{Profile: namingProfile.String(), Metadata: recordingnaming.Metadata{
			Continent: st.namingDefaults.Continent,
			Country:   st.namingDefaults.Country,
			City:      st.namingDefaults.City,
			PlazaName: st.namingDefaults.PlazaName,
		}}
		if namingProfile == recordingnaming.ProfilePlazaHourlyV1 && !req.DryRun {
			namingRequest.Metadata.PlazaID, err = recordingnaming.EnsureStreamPlazaID(r.Context(), tx, accountID, st.id)
			if err != nil {
				util.WriteError(w, http.StatusInternalServerError, "allocate plaza id")
				return
			}
		}
		var resolvedProfile recordingnaming.Profile
		var folderName string
		var namingMetadata []byte
		var namingErr error
		if req.DryRun {
			resolvedProfile, folderName, namingMetadata, namingErr = resolveRecordingNamingForValidation(namingRequest, st.id)
		} else {
			resolvedProfile, folderName, namingMetadata, namingErr = resolveRecordingNaming(namingRequest, 0)
		}
		if namingErr != nil {
			util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("stream %d: %v", st.id, namingErr))
			return
		}
		var cronArg, dailyStartArg, dailyEndArg, nextArg any
		if mode == batchSampled {
			cronArg = cronExpr
			next, nextErr := recsched.NextFireUTC(cronExpr, st.timezone, now)
			if nextErr != nil {
				util.WriteError(w, http.StatusBadRequest, nextErr.Error())
				return
			}
			if !next.IsZero() {
				nextArg = next
			}
		} else {
			dailyStartArg, dailyEndArg = dailyStartRaw, dailyEndRaw
			next, nextErr := recsched.NextWindowOpenUTCOn(st.timezone, dailyStart, weekdays, startAt, timeOrZero(endAt), now)
			if nextErr != nil {
				util.WriteError(w, http.StatusBadRequest, nextErr.Error())
				return
			}
			if !next.IsZero() {
				nextArg = next
			}
		}
		action := "updated"
		recordingID := st.recordingID
		if recordingID == 0 {
			action = "created"
			if req.DryRun && captureVia == "cloud" {
				if _, classifyErr := classifyRecordingSource(strings.TrimSpace(st.sourceURL)); classifyErr != nil {
					util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("stream %d: %v", st.id, classifyErr))
					return
				}
			}
		}
		if req.DryRun {
			if recordingID == 0 {
				created++
			} else {
				updated++
			}
			items = append(items, batchScheduleItem{StreamID: st.id, RecordingID: recordingID, Action: action, Timezone: st.timezone})
			continue
		}
		if recordingID != 0 {
			updatedRecording, updateErr := tx.Exec(r.Context(), `UPDATE recordings SET mode=$3, cron_expr=$4, cron_timezone=$5, clip_duration_sec=$6, daily_window_start=$7, daily_window_end=$8, active_weekdays=$9, target_fps=$10, start_at=$11, end_at=$12, next_fire_at=$13, storage_destination_id=$14, delivery_storage_destination_id=$15, delivery=$16, capture_via=$17, naming_profile=$18, folder_name=$19, naming_metadata_jsonb=$20, last_enqueued_fire_at=NULL, status='active', paused_at=NULL, completed_captured_clip_count=NULL, completed_expected_clip_count=NULL, consecutive_failures=0, last_error_text='', last_error_at=NULL, updated_at=now() WHERE id=$1 AND account_id=$2 AND status <> 'canceled'`, recordingID, accountID, mode, cronArg, st.timezone, clipDuration, dailyStartArg, dailyEndArg, weekdays, req.TargetFPS, startAt, endAt, nextArg, captureDestID, deliveryDestArg, delivery, captureVia, resolvedProfile.String(), folderName, namingMetadata)
			err = updateErr
			if err == nil && updatedRecording.RowsAffected() != 1 {
				err = fmt.Errorf("recording was canceled while scheduling")
			}
			if err == nil {
				_, err = tx.Exec(r.Context(), `
					UPDATE recording_jobs
					SET status='canceled', lease_owner=NULL, lease_expires_at=NULL, updated_at=now()
					WHERE recording_id=$1 AND status IN ('pending','leased')
				`, recordingID)
			}
			updated++
		} else {
			sourceKind := "auto"
			if captureVia == "cloud" {
				sourceKind, err = classifyRecordingSource(strings.TrimSpace(st.sourceURL))
			}
			if err == nil {
				recordingID, _, _, _, err = s.insertRecordingTx(r.Context(), tx, recordingInsertParams{accountID: accountID, captureDestID: captureDestID, deliveryDestArg: deliveryDestArg, name: fmt.Sprintf("%s [%d]", st.name, st.id), streamURL: st.sourceURL, streamIDArg: st.id, sourceKind: sourceKind, mode: string(mode), cronExprArg: cronArg, cronTimezone: st.timezone, clipDuration: clipDuration, dailyWindowStartArg: dailyStartArg, dailyWindowEndArg: dailyEndArg, activeWeekdays: weekdays, targetFPSArg: req.TargetFPS, nextFireArg: nextArg, startAt: startAt, endAtArg: endAt, delivery: delivery, captureVia: captureVia, namingProfile: resolvedProfile, folderName: folderName, namingMetadata: namingMetadata})
			}
			created++
		}
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, "schedule recording")
			return
		}
		items = append(items, batchScheduleItem{StreamID: st.id, RecordingID: recordingID, Action: action, Timezone: st.timezone})
	}
	if req.DryRun {
		util.WriteJSON(w, http.StatusOK, batchScheduleResponse{
			Items: items, Created: created, Updated: updated, DryRun: true,
			RelayStreams: relayStreams, OnlineRelaySlots: onlineRelaySlots, RequiredRelaySlots: requiredRelaySlots,
		})
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit batch schedule")
		return
	}
	util.WriteJSON(w, http.StatusOK, batchScheduleResponse{
		Items: items, Created: created, Updated: updated,
		RelayStreams: relayStreams, OnlineRelaySlots: onlineRelaySlots, RequiredRelaySlots: requiredRelaySlots,
	})
}

func availableRelayCapacity(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, accountID int64, selectedStreamIDs []int64) (int, error) {
	stats, err := loadRelayAvailability(ctx, q, accountID, selectedStreamIDs)
	return stats.available, err
}

type relayAvailability struct {
	total     int
	online    int
	live      int
	available int
}

func loadRelayAvailability(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, accountID int64, excludedStreamIDs []int64) (relayAvailability, error) {
	var stats relayAvailability
	err := q.QueryRow(ctx, `
		WITH live_leases AS (
			SELECT n.id AS node_id, COUNT(j.id)::int AS count
			FROM nodes n
			JOIN recording_jobs j ON j.lease_owner='node:'||n.id::text
			JOIN recordings r ON r.id=j.recording_id
			WHERE n.account_id=$1 AND j.status='leased' AND j.lease_expires_at > now()
			  AND (r.stream_id IS NULL OR NOT (r.stream_id=ANY($2::bigint[])))
			GROUP BY n.id
		),
		relay_nodes AS (
			SELECT n.id, n.relay_group_id, n.relay_max_streams,
			       n.status, n.last_heartbeat_at, g.max_streams AS group_max,
			       COALESCE(l.count, 0) AS live_leases,
			       n.status='active' AND n.last_heartbeat_at >= now()-interval '120 seconds' AS online
			FROM nodes n
			LEFT JOIN relay_groups g ON g.id=n.relay_group_id AND g.account_id=n.account_id
			LEFT JOIN live_leases l ON l.node_id=n.id
			WHERE n.account_id=$1 AND n.node_type='relay' AND `+visibleNodeSQL+`
		),
		grouped AS (
			SELECT relay_group_id,
			       GREATEST(
			         LEAST(
			           MAX(group_max)-SUM(live_leases),
			           COALESCE(
			             SUM(GREATEST(relay_max_streams-live_leases, 0)) FILTER (WHERE online),
			             0
			           )
			         ),
			         0
			       )::int AS slots
			FROM relay_nodes WHERE relay_group_id IS NOT NULL GROUP BY relay_group_id
		)
		SELECT
			COUNT(*)::int,
			COUNT(*) FILTER (WHERE online)::int,
			COALESCE(SUM(live_leases), 0)::int,
			(
			COALESCE((
			  SELECT SUM(GREATEST(0, relay_max_streams-live_leases))
			  FROM relay_nodes
			  WHERE relay_group_id IS NULL AND online
			), 0)
			+ COALESCE((SELECT SUM(slots) FROM grouped), 0)
			)::int
		FROM relay_nodes
	`, accountID, excludedStreamIDs).Scan(&stats.total, &stats.online, &stats.live, &stats.available)
	return stats, err
}

func uniqueBatchStreamIDs(input []int64) ([]int64, error) {
	if len(input) == 0 || len(input) > 200 {
		return nil, fmt.Errorf("stream_ids must contain between 1 and 200 items")
	}
	seen := make(map[int64]struct{}, len(input))
	ids := append([]int64(nil), input...)
	for _, id := range ids {
		if id <= 0 {
			return nil, fmt.Errorf("stream_ids must contain positive integers")
		}
		if _, ok := seen[id]; ok {
			return nil, fmt.Errorf("stream_ids contains duplicate %d", id)
		}
		seen[id] = struct{}{}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

func timeOrZero(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.UTC()
}
