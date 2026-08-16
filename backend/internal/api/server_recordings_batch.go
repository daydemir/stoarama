package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

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
	CampaignAdmissionApprovalID  string                `json:"campaign_admission_approval_id"`
}

type batchStream struct {
	id, recordingID, recordingCount                 int64
	name, sourceURL, provider, timezone, captureVia string
	timezoneMissing                                 bool
	namingDefaults                                  catalogNamingDefaults
	admissionEvidenceID, admissionEvidenceID2       string
	sceneIdentity, evidenceSHA, scheduleSHA         string
	evidenceObservedAt                              time.Time
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
	Items               []batchScheduleItem `json:"items"`
	Created             int                 `json:"created"`
	Updated             int                 `json:"updated"`
	DryRun              bool                `json:"dry_run"`
	RelayStreams        int                 `json:"relay_streams"`
	OnlineRelaySlots    int                 `json:"online_relay_slots"`
	RequiredRelaySlots  int                 `json:"required_relay_slots"`
	CampaignTrackID     int64               `json:"campaign_track_id,omitempty"`
	AdmissionApproval   string              `json:"campaign_admission_approval_id,omitempty"`
	CapacityObservation string              `json:"campaign_capacity_observation_id,omitempty"`
	StorageObservation  string              `json:"campaign_storage_observation_id,omitempty"`
	ForecastPeakSlots   int                 `json:"forecast_peak_slots,omitempty"`
	UsableAfterLoss     int                 `json:"usable_after_worker_loss,omitempty"`
	RelayActiveDemand   int                 `json:"relay_active_demand,omitempty"`
	RelayFailureDomains int                 `json:"relay_failure_domains,omitempty"`
	RelayEffectiveSlots int                 `json:"relay_effective_capacity,omitempty"`
	RelayAfterLoss      int                 `json:"relay_usable_after_largest_loss,omitempty"`
	RequiredFreeBytes   int64               `json:"required_free_bytes,omitempty"`
	ProjectedFreeBytes  int64               `json:"projected_free_after_bytes,omitempty"`
}

type campaignAdmissionReplayRequest struct {
	ApprovalID      string `json:"approval_id"`
	TargetAccountID int64  `json:"target_account_id"`
}

// A committed response remains retrievable with the exact historical browser
// session secret after logout/revocation. This endpoint cannot mutate or reveal
// anything beyond that already-sealed response, and only a credential hash is
// sent to PostgreSQL.
func (s *Server) handleRecordingCampaignAdmissionReplay(w http.ResponseWriter, r *http.Request) {
	if s.admissionPool == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "campaign admission executor is unavailable")
		return
	}
	var req campaignAdmissionReplayRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	approvalID, err := uuid.Parse(strings.TrimSpace(req.ApprovalID))
	cookie, cookieErr := r.Cookie(accountSessionCookie)
	if err != nil || cookieErr != nil || req.TargetAccountID <= 0 || strings.TrimSpace(cookie.Value) == "" {
		util.WriteError(w, http.StatusUnauthorized, "exact historical admission replay credential required")
		return
	}
	var replayJSON []byte
	err = s.admissionPool.QueryRow(r.Context(), `SELECT recording_campaign_replay($1,$2,$3)`, approvalID, req.TargetAccountID, hashSecret(strings.TrimSpace(cookie.Value))).Scan(&replayJSON)
	if err != nil || len(replayJSON) == 0 {
		util.WriteError(w, http.StatusUnauthorized, "sealed admission replay credential does not match")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(replayJSON)
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
	var err error
	if req.TargetAccountID < 0 {
		util.WriteError(w, http.StatusBadRequest, "target_account_id must be non-negative")
		return
	}
	if req.TargetAccountID > 0 {
		if req.TargetAccountID != principal.AccountID && principal.Role != accountRoleAdmin {
			util.WriteError(w, http.StatusForbidden, "target_account_id requires platform operator access")
			return
		}
		accountID = req.TargetAccountID
	}
	var admissionApprovalID uuid.UUID
	var admissionScheduleSpec []byte
	admissionRequested := strings.TrimSpace(req.CampaignAdmissionApprovalID) != ""
	if admissionRequested {
		if s.admissionPool == nil {
			util.WriteError(w, http.StatusServiceUnavailable, "campaign admission executor is unavailable")
			return
		}
		if principal.UserID == 0 || principal.SessionID == nil || principal.Role != accountRoleAdmin || (principal.MemberRole != "owner" && principal.MemberRole != "admin") {
			util.WriteError(w, http.StatusForbidden, "campaign admission requires an account owner/admin browser session")
			return
		}
		admissionApprovalID, err = uuid.Parse(strings.TrimSpace(req.CampaignAdmissionApprovalID))
		if err != nil {
			util.WriteError(w, http.StatusBadRequest, "campaign_admission_approval_id must be a UUID")
			return
		}
		// Resolve a sealed terminal response before account/source/provider/capacity
		// freshness. The approval UUID is the immutable request identity.
		var replayJSON []byte
		replayErr := s.admissionPool.QueryRow(r.Context(), `SELECT recording_campaign_replay($1,$2,$3)`, admissionApprovalID, accountID, principal.credentialSHA256).Scan(&replayJSON)
		if replayErr == nil && len(replayJSON) > 0 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(replayJSON)
			return
		}
		if replayErr != nil {
			util.WriteError(w, http.StatusInternalServerError, "load sealed campaign admission replay")
			return
		}
	}
	if req.TargetAccountID > 0 {
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
	if admissionRequested && delivery != deliveryNASPull {
		util.WriteError(w, http.StatusBadRequest, "campaign admission supports only nas_pull delivery")
		return
	}
	if admissionRequested && (req.StorageDestinationID <= 0 || req.DeliveryStorageDestinationID != 0) {
		util.WriteError(w, http.StatusBadRequest, "campaign admission requires one server-owned managed capture destination")
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
	if req.TargetFPS != nil {
		util.WriteError(w, http.StatusBadRequest, "target_fps is not supported; recordings preserve the source without re-encoding")
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
	var admissionCloudCapacity campaignCloudCapacityObservation
	if admissionRequested {
		admissionCloudCapacity, err = s.observeCampaignCloudCapacity(r.Context())
		if err != nil {
			util.WriteError(w, http.StatusConflict, "campaign admission lacks fresh one-worker-loss cloud capacity")
			return
		}
	}

	txOptions := pgx.TxOptions{}
	if admissionRequested {
		txOptions.IsoLevel = pgx.Serializable
	}
	if req.DryRun {
		txOptions.AccessMode = pgx.ReadOnly
	}
	tx, err := s.pool.BeginTx(r.Context(), txOptions)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin batch schedule")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if !req.DryRun {
		if _, err := tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock(hashtextextended('campaign-admission-capacity-v1',0))`); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "lock global campaign capacity")
			return
		}
		// Campaign admission, ordinary batch scheduling, and roster occupancy all
		// serialize account -> stream -> recording so neither commit order can
		// bypass a pending scene reservation.
		if _, err := tx.Exec(r.Context(), `SELECT 1 FROM accounts WHERE id=$1 FOR UPDATE`, accountID); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "lock account scheduling occupancy")
			return
		}
	}
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
	if admissionRequested {
		approvedSchedule := req
		approvedSchedule.CampaignAdmissionApprovalID = ""
		approvedSchedule.DryRun = false
		approvedSchedule.TargetAccountID = accountID
		var marshalErr error
		admissionScheduleSpec, marshalErr = json.Marshal(approvedSchedule)
		if marshalErr != nil {
			util.WriteError(w, http.StatusInternalServerError, "encode exact campaign admission schedule")
			return
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
	cloudForecastPeak := 0
	ceiling := s.cfg.DropletPoolMax * s.cfg.DropletPoolCapacity
	if admissionRequested {
		ceiling = admissionCloudCapacity.UsableAfterWorkerLoss
	}
	if ceiling > 0 {
		candidates := make([]dropletpool.ForecastCandidate, 0, len(streams))
		excluded := make([]int64, 0, len(streams))
		for _, st := range streams {
			captureVia := batchCaptureVia(st.sourceURL, st.provider, st.captureVia)
			if st.recordingID > 0 && !admissionRequested {
				excluded = append(excluded, st.recordingID)
			}
			if captureVia == "relay" {
				continue
			}
			candidates = append(candidates, dropletpool.ForecastCandidate{Mode: string(mode), CronExpr: cronExpr, CronTimezone: st.timezone, ClipDurationSec: clipDuration, DailyWindowStart: dailyStartRaw, DailyWindowEnd: dailyEndRaw, EnvStart: startAt, EnvEnd: timeOrZero(endAt), ActiveWeekdays: weekdays})
		}
		forecastHorizon := 8 * 24 * time.Hour
		if admissionRequested {
			forecastHorizon = 62 * 24 * time.Hour
		}
		peak, ferr := dropletpool.ForecastPeakWithCandidatesExcluding(r.Context(), s.pool, s.billing != nil, candidates, excluded, time.Now().UTC(), forecastHorizon)
		if ferr != nil {
			util.WriteError(w, http.StatusInternalServerError, "forecast batch capacity")
			return
		}
		cloudForecastPeak = peak
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

	if admissionRequested {
		if s.admissionPool == nil || principal.SessionID == nil || !lowerSHA256(principal.credentialSHA256) {
			util.WriteError(w, http.StatusServiceUnavailable, "campaign admission executor is unavailable")
			return
		}
		type admissionNextFire struct {
			StreamID   int64     `json:"stream_id"`
			NextFireAt time.Time `json:"next_fire_at"`
		}
		nextFires := make([]admissionNextFire, 0, len(streams))
		now := time.Now().UTC()
		startAt = effectiveRecordingStart(req.StartAt, now)
		for _, st := range streams {
			next, nextErr := recsched.NextWindowOpenUTCOn(st.timezone, dailyStart, weekdays, startAt, timeOrZero(endAt), now)
			if nextErr != nil || next.IsZero() {
				util.WriteError(w, http.StatusConflict, fmt.Sprintf("stream %d has no next complete approved window", st.id))
				return
			}
			nextFires = append(nextFires, admissionNextFire{StreamID: st.id, NextFireAt: next.UTC()})
		}
		var currentActive int
		if err := tx.QueryRow(r.Context(), `SELECT count(*) FROM recordings WHERE account_id=$1 AND status='active'`, accountID).Scan(&currentActive); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "load campaign roster head")
			return
		}
		activeRosterAfter := currentActive + len(streams)
		if activeRosterAfter > 60 {
			util.WriteError(w, http.StatusConflict, "campaign admission exceeds the reviewed roster cap")
			return
		}
		var connectionID, nasTotalBytes, nasFreeBytes, measured24hBytes int64
		var nasReportedAt time.Time
		var measuredStreams int
		if err := tx.QueryRow(r.Context(), `SELECT id,nas_storage_total_bytes,nas_storage_free_bytes,nas_storage_reported_at FROM connections WHERE account_id=$1 AND kind='nas_pull' AND nas_capacity_blocked=false AND last_seen_at>=transaction_timestamp()-interval '5 minutes' AND nas_storage_reported_at>=transaction_timestamp()-interval '5 minutes' ORDER BY nas_storage_reported_at DESC,id DESC LIMIT 1 FOR SHARE`, accountID).Scan(&connectionID, &nasTotalBytes, &nasFreeBytes, &nasReportedAt); err != nil {
			util.WriteError(w, http.StatusConflict, "campaign admission requires fresh healthy NAS capacity telemetry")
			return
		}
		if err := tx.QueryRow(r.Context(), `SELECT COALESCE(sum(c.size_bytes),0),count(DISTINCT c.recording_id)::int FROM recording_clips c JOIN recordings r ON r.id=c.recording_id WHERE r.account_id=$1 AND c.created_at>=transaction_timestamp()-interval '24 hours' AND c.purged_at IS NULL`, accountID).Scan(&measured24hBytes, &measuredStreams); err != nil || measured24hBytes <= 0 || measuredStreams <= 0 {
			util.WriteError(w, http.StatusConflict, "campaign admission lacks a measured 24-hour NAS runway baseline")
			return
		}
		campaignDaysWithReserve := int((endAt.UTC().Sub(now)+24*time.Hour-1)/(24*time.Hour)) + 7
		const maxInt64 = int64(^uint64(0) >> 1)
		if campaignDaysWithReserve < 8 || campaignDaysWithReserve > 60 || measured24hBytes > maxInt64/int64(activeRosterAfter)/125 {
			util.WriteError(w, http.StatusConflict, "campaign admission storage projection is invalid")
			return
		}
		projectionNumerator := measured24hBytes * int64(activeRosterAfter) * 125
		projectionDenominator := int64(measuredStreams) * 100
		projectedDailyBytes := (projectionNumerator + projectionDenominator - 1) / projectionDenominator
		if projectedDailyBytes <= 0 || projectedDailyBytes > maxInt64/int64(campaignDaysWithReserve) {
			util.WriteError(w, http.StatusConflict, "campaign admission storage projection is invalid")
			return
		}
		requiredFreeBytes := projectedDailyBytes * int64(campaignDaysWithReserve)
		if requiredFreeBytes > nasFreeBytes {
			util.WriteError(w, http.StatusConflict, "campaign admission would violate NAS campaign-plus-7d runway")
			return
		}
		projectedFreeAfterBytes := nasFreeBytes - requiredFreeBytes
		warningThresholdBytes := (nasTotalBytes + 9) / 10
		capacityJSON, marshalErr := json.Marshal(map[string]any{
			"observation_started_at":      admissionCloudCapacity.ObservationStartedAt.UTC(),
			"observed_at":                 admissionCloudCapacity.ObservedAt.UTC(),
			"provider_observation_sha256": admissionCloudCapacity.ProviderObservationSHA256,
			"build_sha":                   strings.ToLower(strings.TrimSpace(s.cfg.DropletPoolBuildSHA)),
			"ready_workers":               admissionCloudCapacity.ReadyWorkers, "total_slots": admissionCloudCapacity.TotalSlots,
			"largest_worker_slots": admissionCloudCapacity.LargestWorkerSlots, "usable_after_worker_loss": admissionCloudCapacity.UsableAfterWorkerLoss,
			"largest_region": admissionCloudCapacity.LargestRegion, "largest_region_slots": admissionCloudCapacity.LargestRegionSlots,
			"provider_project_sha256": hashSecret(s.cfg.DropletPoolProjectID), "provider_firewall_sha256": hashSecret(s.cfg.DropletPoolFirewallID),
			"size_slug": admissionCloudCapacity.SizeSlug, "pool_identity_sha256": admissionCloudCapacity.PoolIdentitySHA256,
			"facts_sha256": admissionCloudCapacity.FactsSHA256, "forecast_peak_slots": cloudForecastPeak,
		})
		storageJSON, storageMarshalErr := json.Marshal(map[string]any{
			"connection_id": connectionID, "nas_reported_at": nasReportedAt.UTC(), "nas_total_bytes": nasTotalBytes,
			"nas_free_bytes": nasFreeBytes, "measured_24h_bytes": measured24hBytes, "measured_streams": measuredStreams,
			"projected_daily_bytes": projectedDailyBytes, "campaign_days_with_reserve": campaignDaysWithReserve,
			"required_free_bytes": requiredFreeBytes, "projected_free_after_bytes": projectedFreeAfterBytes,
			"warning_threshold_bytes": warningThresholdBytes, "warning_after_reservation": projectedFreeAfterBytes < warningThresholdBytes,
		})
		nextFireJSON, nextMarshalErr := json.Marshal(nextFires)
		if marshalErr != nil || storageMarshalErr != nil || nextMarshalErr != nil {
			util.WriteError(w, http.StatusInternalServerError, "encode campaign admission executor request")
			return
		}
		// Release every ordinary-runtime lock before entering the single executor
		// statement. The definer function reacquires the canonical global/account/
		// stream order and revalidates all preflight facts under those locks.
		if err := tx.Rollback(r.Context()); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "release campaign admission preflight")
			return
		}
		var sealedResponse []byte
		err = s.admissionPool.QueryRow(r.Context(), `SELECT recording_campaign_admit($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8::jsonb,$9::jsonb)`, admissionApprovalID, accountID, principal.UserID, *principal.SessionID, principal.credentialSHA256, admissionScheduleSpec, nextFireJSON, capacityJSON, storageJSON).Scan(&sealedResponse)
		if err != nil {
			util.WriteError(w, http.StatusConflict, "campaign admission atomic executor rejected the transition")
			return
		}
		var response batchScheduleResponse
		if err := json.Unmarshal(sealedResponse, &response); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "decode DB-canonical campaign admission response")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(sealedResponse)
		return
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
		sourceKind := "auto"
		if captureVia == "cloud" {
			sourceKind, err = classifyRecordingSource(strings.TrimSpace(st.sourceURL))
			if err != nil {
				util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("stream %d: %v", st.id, err))
				return
			}
		}
		if recordingID == 0 {
			action = "created"
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
			updatedRecording, updateErr := tx.Exec(r.Context(), `UPDATE recordings SET mode=$3, cron_expr=$4, cron_timezone=$5, clip_duration_sec=$6, daily_window_start=$7, daily_window_end=$8, active_weekdays=$9, target_fps=$10, start_at=$11, end_at=$12, next_fire_at=$13, storage_destination_id=$14, delivery_storage_destination_id=$15, delivery=$16, capture_via=$17, naming_profile=$18, folder_name=$19, naming_metadata_jsonb=$20, stream_url=$21, source_kind=$22, last_enqueued_fire_at=NULL, status='active', paused_at=NULL, completed_captured_clip_count=NULL, completed_expected_clip_count=NULL, consecutive_failures=0, last_error_text='', last_error_at=NULL, updated_at=now() WHERE id=$1 AND account_id=$2 AND status <> 'canceled'`, recordingID, accountID, mode, cronArg, st.timezone, clipDuration, dailyStartArg, dailyEndArg, weekdays, req.TargetFPS, startAt, endAt, nextArg, captureDestID, deliveryDestArg, delivery, captureVia, resolvedProfile.String(), folderName, namingMetadata, st.sourceURL, sourceKind)
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
			if err == nil {
				recordingID, _, _, _, err = s.insertRecordingTx(r.Context(), tx, recordingInsertParams{accountID: accountID, captureDestID: captureDestID, deliveryDestArg: deliveryDestArg, name: fmt.Sprintf("%s [%d]", st.name, st.id), streamURL: st.sourceURL, streamIDArg: st.id, sourceKind: sourceKind, mode: string(mode), cronExprArg: cronArg, cronTimezone: st.timezone, clipDuration: clipDuration, dailyWindowStartArg: dailyStartArg, dailyWindowEndArg: dailyEndArg, activeWeekdays: weekdays, targetFPSArg: req.TargetFPS, nextFireArg: nextArg, startAt: startAt, endAtArg: endAt, delivery: delivery, captureVia: captureVia, namingProfile: resolvedProfile, folderName: folderName, namingMetadata: namingMetadata})
			}
			created++
		}
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) {
				log.Printf("batch schedule failed account_id=%d stream_id=%d sqlstate=%s table=%s constraint=%s routine=%s", accountID, st.id, pgErr.Code, pgErr.TableName, pgErr.ConstraintName, pgErr.Routine)
			} else {
				log.Printf("batch schedule failed account_id=%d stream_id=%d error=%v", accountID, st.id, err)
			}
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
	response := batchScheduleResponse{
		Items: items, Created: created, Updated: updated,
		RelayStreams: relayStreams, OnlineRelaySlots: onlineRelaySlots, RequiredRelaySlots: requiredRelaySlots,
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit batch schedule")
		return
	}
	util.WriteJSON(w, http.StatusOK, response)
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
