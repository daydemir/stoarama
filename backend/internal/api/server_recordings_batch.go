package api

import (
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
	StreamIDs                    []int64               `json:"stream_ids"`
	StreamTimezones              []streamTimezoneInput `json:"stream_timezones"`
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
}

type batchStream struct {
	id, recordingID, recordingCount                 int64
	name, sourceURL, provider, timezone, captureVia string
	timezoneMissing                                 bool
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
	Items   []batchScheduleItem `json:"items"`
	Created int                 `json:"created"`
	Updated int                 `json:"updated"`
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
	startAt := time.Now().UTC()
	if req.StartAt != nil {
		startAt = req.StartAt.UTC()
	}
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

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin batch schedule: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	rows, err := tx.Query(r.Context(), fmt.Sprintf(`
		SELECT st.id, st.name, st.source_url, st.provider,
		       %s,
		       %s,
		       COALESCE((SELECT rec.id FROM recordings rec WHERE rec.account_id=$2 AND rec.stream_id=st.id AND rec.status <> 'canceled' ORDER BY rec.id DESC LIMIT 1),0),
		       COALESCE((SELECT rec.capture_via FROM recordings rec WHERE rec.account_id=$2 AND rec.stream_id=st.id AND rec.status <> 'canceled' ORDER BY rec.id DESC LIMIT 1),''),
		       (SELECT count(*) FROM recordings rec WHERE rec.account_id=$2 AND rec.stream_id=st.id AND rec.status <> 'canceled')
		FROM streams st WHERE st.id=ANY($1::bigint[]) AND st.deleted_at IS NULL
		ORDER BY st.id FOR UPDATE
	`, batchEffectiveTimezoneSQL, batchTimezoneMissingSQL), ids, principal.AccountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load batch streams: %v", err))
		return
	}
	streams := make([]batchStream, 0, len(ids))
	for rows.Next() {
		var st batchStream
		if err := rows.Scan(&st.id, &st.name, &st.sourceURL, &st.provider, &st.timezone, &st.timezoneMissing, &st.recordingID, &st.captureVia, &st.recordingCount); err != nil {
			rows.Close()
			util.WriteError(w, http.StatusInternalServerError, err.Error())
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
			if _, err := tx.Exec(r.Context(), `UPDATE streams SET local_timezone=$2, updated_at=now() WHERE id=$1 AND local_timezone=''`, st.id, st.timezone); err != nil {
				util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("set stream timezone: %v", err))
				return
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
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("forecast batch capacity: %v", ferr))
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
		if err := tx.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM connections WHERE account_id=$1 AND kind='nas_pull')`, principal.AccountID).Scan(&hasConnection); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("check nas pull connection: %v", err))
			return
		}
		if !hasConnection {
			util.WriteError(w, http.StatusBadRequest, "connect a NAS pull client before scheduling recordings to your NAS")
			return
		}
	}
	if req.DeliveryStorageDestinationID > 0 {
		var status, provider string
		err := tx.QueryRow(r.Context(), fmt.Sprintf(`SELECT status, provider FROM storage_destinations sd WHERE sd.id=$1 AND %s`, fmt.Sprintf(storageDestAccessPredicate, "$2")), req.DeliveryStorageDestinationID, principal.AccountID).Scan(&status, &provider)
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusBadRequest, "a verified WebDAV delivery_storage_destination_id is required")
			return
		}
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if status != "verified" || provider != "webdav" {
			util.WriteError(w, http.StatusBadRequest, "a verified WebDAV delivery_storage_destination_id is required")
			return
		}
		managedID, _, err := s.provisionManagedDestination(r.Context(), tx, principal.AccountID)
		if err != nil {
			util.WriteError(w, http.StatusServiceUnavailable, fmt.Sprintf("provision managed staging: %v", err))
			return
		}
		captureDestID, deliveryDestArg = managedID, req.DeliveryStorageDestinationID
	} else {
		var verified, managed bool
		err := tx.QueryRow(r.Context(), fmt.Sprintf(`SELECT status='verified', managed FROM storage_destinations sd WHERE sd.id=$1 AND %s`, fmt.Sprintf(storageDestAccessPredicate, "$2")), req.StorageDestinationID, principal.AccountID).Scan(&verified, &managed)
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusBadRequest, "a verified storage_destination_id is required")
			return
		}
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, err.Error())
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
	for _, st := range streams {
		captureVia := batchCaptureVia(st.sourceURL, st.provider, st.captureVia)
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
		if recordingID != 0 {
			updatedRecording, updateErr := tx.Exec(r.Context(), `UPDATE recordings SET mode=$3, cron_expr=$4, cron_timezone=$5, clip_duration_sec=$6, daily_window_start=$7, daily_window_end=$8, active_weekdays=$9, target_fps=$10, start_at=$11, end_at=$12, next_fire_at=$13, storage_destination_id=$14, delivery_storage_destination_id=$15, delivery=$16, capture_via=$17, last_enqueued_fire_at=NULL, status='active', consecutive_failures=0, last_error_text='', last_error_at=NULL, updated_at=now() WHERE id=$1 AND account_id=$2 AND status <> 'canceled'`, recordingID, principal.AccountID, mode, cronArg, st.timezone, clipDuration, dailyStartArg, dailyEndArg, weekdays, req.TargetFPS, startAt, endAt, nextArg, captureDestID, deliveryDestArg, delivery, captureVia)
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
				recordingID, _, _, _, err = s.insertRecordingTx(r.Context(), tx, recordingInsertParams{accountID: principal.AccountID, captureDestID: captureDestID, deliveryDestArg: deliveryDestArg, name: fmt.Sprintf("%s [%d]", st.name, st.id), streamURL: st.sourceURL, streamIDArg: st.id, sourceKind: sourceKind, mode: string(mode), cronExprArg: cronArg, cronTimezone: st.timezone, clipDuration: clipDuration, dailyWindowStartArg: dailyStartArg, dailyWindowEndArg: dailyEndArg, activeWeekdays: weekdays, targetFPSArg: req.TargetFPS, nextFireArg: nextArg, startAt: startAt, endAtArg: endAt, delivery: delivery, captureVia: captureVia})
			}
			action = "created"
			created++
		}
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("schedule stream %d: %v", st.id, err))
			return
		}
		items = append(items, batchScheduleItem{StreamID: st.id, RecordingID: recordingID, Action: action, Timezone: st.timezone})
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit batch schedule: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, batchScheduleResponse{Items: items, Created: created, Updated: updated})
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
