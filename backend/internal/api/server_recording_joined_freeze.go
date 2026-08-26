package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/daydemir/stoarama/backend/internal/recordingnaming"
	"github.com/daydemir/stoarama/backend/internal/util"
)

const joinedTier1SelectionBasis = "operator_approved_ordered_cohort_v1"

const joinedTier1ExclusionEvidenceSQL = `, evidence AS (
	SELECT c.recording_id,c.id clip_id,CASE WHEN c.created_at>d.cutoff THEN 'after_cutoff'
	 WHEN c.clip_end_at<=d.window_start OR c.clip_start_at>=d.window_end THEN 'outside_qualification_window'
	 ELSE 'nonpositive_source_size' END reason_code,
	 encode(sha256(convert_to(jsonb_build_object('clip_id',c.id,'recording_id',c.recording_id,
	 'recording_job_id',c.recording_job_id,'created_at',c.created_at,'clip_start_at',c.clip_start_at,
	 'clip_end_at',c.clip_end_at,'size_bytes',c.size_bytes,'window_start_at',d.window_start,
	 'window_end_at',d.window_end,'cutoff',d.cutoff)::text,'UTF8')),'hex') evidence_sha256
	FROM selected d JOIN recording_clips c ON c.recording_id=d.recording_id AND c.recording_job_id=d.job_id
	WHERE c.id<=d.high_water_clip_id AND c.purged_at IS NULL AND (c.created_at>d.cutoff OR c.clip_end_at<=d.window_start
	 OR c.clip_start_at>=d.window_end OR c.size_bytes<=0))`

func joinedTier1ExclusionQuery(scopeSQL, projectionSQL string) string {
	return "WITH selected AS (" + scopeSQL + ")" + joinedTier1ExclusionEvidenceSQL + projectionSQL
}

var joinedBatchIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)

type joinedTier1FreezeRequest struct {
	ProtocolVersion          int       `json:"protocol_version"`
	ConnectionID             int64     `json:"connection_id"`
	BatchID                  string    `json:"batch_id"`
	Generation               int       `json:"generation"`
	SourceEndpoint           string    `json:"source_endpoint"`
	QualificationRunID       int64     `json:"qualification_run_id"`
	RecordingIDs             []int64   `json:"recording_ids"`
	OrderedRecordingIDSHA256 string    `json:"ordered_recording_ids_sha256"`
	EligibilityCutoff        time.Time `json:"eligibility_cutoff"`
	Apply                    bool      `json:"apply"`
	ExpectedRequestSHA256    string    `json:"expected_request_sha256,omitempty"`
}

type joinedTier1FreezePlan struct {
	SchemaVersion           int                                `json:"schema_version"`
	BatchID                 string                             `json:"batch_id"`
	Generation              int                                `json:"generation"`
	AccountID               int64                              `json:"account_id"`
	ConnectionID            int64                              `json:"connection_id"`
	SourceEndpoint          string                             `json:"source_endpoint"`
	RecordingIDs            []int64                            `json:"recording_ids"`
	SelectionAuthority      joinedrecording.SelectionAuthority `json:"selection_authority"`
	QualificationJobsSHA256 string                             `json:"qualification_jobs_sha256"`
	PolicyVersion           string                             `json:"policy_version"`
	MediaTool               joinedrecording.MediaToolEvidence  `json:"media_tool"`
	Recordings              []joinedTier1FreezeRecording       `json:"recordings"`
	ExpectedStreamDays      int                                `json:"expected_stream_days"`
	ExpectedScheduledHours  int                                `json:"expected_scheduled_hours"`
	ProvisionalSourceClips  int64                              `json:"provisional_source_clips"`
	ProvisionalSourceBytes  int64                              `json:"provisional_source_bytes"`
	ProvisionalExclusions   int64                              `json:"provisional_exclusions"`
	FrozenDenominatorSHA256 string                             `json:"frozen_denominator_sha256"`
	FreezeExclusionsSHA256  string                             `json:"freeze_exclusions_sha256"`
	RequestSHA256           string                             `json:"request_sha256,omitempty"`
}

type joinedTier1FreezeRecording struct {
	Frozen                   joinedrecording.FrozenRecording     `json:"frozen_recording"`
	Qualification            joinedrecording.QualificationWindow `json:"qualification"`
	SnapshotDays             []joinedTier1FreezeDayScope         `json:"snapshot_days"`
	ExpectedSourceClips      int64                               `json:"expected_source_clips"`
	ExpectedSourceBytes      int64                               `json:"expected_source_bytes"`
	ExpectedExclusions       int64                               `json:"expected_exclusions"`
	ExpectedExclusionsSHA256 string                              `json:"expected_exclusions_sha256"`
}

type joinedTier1FreezeDayScope struct {
	LocalDate            string `json:"local_date"`
	DateOrdinal          int    `json:"date_ordinal"`
	RecordingJobID       int64  `json:"recording_job_id"`
	HighWaterClipID      int64  `json:"high_water_clip_id"`
	ExpectedSourceClips  int    `json:"expected_source_clips"`
	ExpectedSourceBytes  int64  `json:"expected_source_bytes"`
	ExpectedSourceSHA256 string `json:"expected_source_sha256"`
}

type joinedTier1FreezeProgress struct {
	State               string `json:"state"`
	CompletedRecordings int    `json:"completed_recordings"`
	ExpectedRecordings  int    `json:"expected_recordings"`
	CompletedStreamDays int    `json:"completed_stream_days"`
	ExpectedStreamDays  int    `json:"expected_stream_days"`
	NextPriorityOrdinal *int   `json:"next_priority_ordinal,omitempty"`
}

type joinedTier1FreezeQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

func (r joinedTier1FreezeRequest) validate() error {
	cutoff, err := time.Parse(time.RFC3339Nano, joinedrecording.Tier1FrozenAt)
	if err != nil {
		return err
	}
	if r.ProtocolVersion != joinedrecording.JoinedProtocolVersion || r.ConnectionID <= 0 || r.QualificationRunID <= 0 ||
		r.Generation <= 0 || !joinedBatchIDPattern.MatchString(r.BatchID) || len(r.RecordingIDs) != len(joinedrecording.Tier1RecordingIDs) ||
		r.OrderedRecordingIDSHA256 != joinedrecording.Tier1RecordingIDSHA || !r.EligibilityCutoff.Equal(cutoff) ||
		!strings.HasSuffix(r.BatchID, fmt.Sprintf("-generation-%d", r.Generation)) {
		return errors.New("invalid Tier-1 freeze request")
	}
	if _, err := joinedrecording.CanonicalSourceEndpointAuthority(r.SourceEndpoint); err != nil {
		return errors.New("invalid Tier-1 source endpoint")
	}
	for i := range r.RecordingIDs {
		if r.RecordingIDs[i] != joinedrecording.Tier1RecordingIDs[i] {
			return errors.New("Tier-1 recording order differs")
		}
	}
	if r.Apply && !lowerHexSHA256(r.ExpectedRequestSHA256) {
		return errors.New("expected_request_sha256 is required for apply")
	}
	return nil
}

func joinedTier1FreezeRequestMatchesPlan(req joinedTier1FreezeRequest, plan joinedTier1FreezePlan) bool {
	return req.BatchID == plan.BatchID && req.Generation == plan.Generation && req.ConnectionID == plan.ConnectionID &&
		req.SourceEndpoint == plan.SourceEndpoint && req.QualificationRunID == plan.SelectionAuthority.QualificationRunID &&
		req.OrderedRecordingIDSHA256 == plan.SelectionAuthority.OrderedRecordingIDSHA256 &&
		req.EligibilityCutoff.Equal(plan.SelectionAuthority.Cutoff) && slices.Equal(req.RecordingIDs, plan.RecordingIDs)
}

func (s *Server) handleAdminJoinedFreezeTier1(w http.ResponseWriter, r *http.Request) {
	if !s.joinedControlPlaneReady() {
		util.WriteError(w, http.StatusServiceUnavailable, "joined recording is disabled")
		return
	}
	var req joinedTier1FreezeRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := req.validate(); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.ConnectionID != int64(s.cfg.JoinedRecordingConnectionID) || req.BatchID != s.cfg.JoinedRecordingBatchID {
		util.WriteError(w, http.StatusConflict, "Tier-1 connection differs from configured cloud authority")
		return
	}
	if !req.Apply {
		plan, _, err := s.buildJoinedTier1FreezePlan(r.Context(), s.pool, req)
		if err != nil {
			util.WriteError(w, http.StatusConflict, err.Error())
			return
		}
		util.WriteJSON(w, http.StatusOK, map[string]any{"dry_run": true, "plan": plan})
		return
	}
	plan, created, err := s.applyJoinedTier1FreezeResumable(r.Context(), req)
	if err != nil {
		util.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	progress, err := s.joinedTier1FreezeProgress(r.Context(), req.BatchID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"dry_run": false, "created": created, "plan": plan, "progress": progress})
}

func (s *Server) buildJoinedTier1FreezePlan(ctx context.Context, q joinedTier1FreezeQuerier, req joinedTier1FreezeRequest) (joinedTier1FreezePlan, []byte, error) {
	if req.ConnectionID != int64(s.cfg.JoinedRecordingConnectionID) || req.BatchID != s.cfg.JoinedRecordingBatchID {
		return joinedTier1FreezePlan{}, nil, errors.New("Tier-1 freeze authority differs")
	}
	tool, err := joinedrecording.InspectMediaToolEvidence(ctx)
	if err != nil {
		return joinedTier1FreezePlan{}, nil, fmt.Errorf("inspect joined media tool: %w", err)
	}
	return s.buildJoinedTier1FreezePlanWithTool(ctx, q, req, tool, true)
}

func (s *Server) buildJoinedTier1FreezePlanWithTool(ctx context.Context, q joinedTier1FreezeQuerier, req joinedTier1FreezeRequest,
	tool joinedrecording.MediaToolEvidence, countSources bool) (joinedTier1FreezePlan, []byte, error) {
	var plan joinedTier1FreezePlan
	plan.SchemaVersion = 2
	plan.BatchID, plan.Generation = req.BatchID, req.Generation
	plan.ConnectionID, plan.SourceEndpoint = req.ConnectionID, req.SourceEndpoint
	plan.RecordingIDs = append([]int64(nil), req.RecordingIDs...)
	plan.SelectionAuthority = joinedrecording.SelectionAuthority{SelectionBasis: joinedTier1SelectionBasis,
		OrderedRecordingIDSHA256: req.OrderedRecordingIDSHA256, Cutoff: req.EligibilityCutoff.UTC(),
		QualificationRunID: req.QualificationRunID}
	plan.PolicyVersion = joinedrecording.PlanPolicyVersion
	plan.ExpectedStreamDays = len(req.RecordingIDs) * 14
	plan.ExpectedScheduledHours = plan.ExpectedStreamDays * 12
	if err := q.QueryRow(ctx, `SELECT c.account_id,q.definition_version,q.cohort_sha256,q.windows_sha256,q.frozen_at
		FROM connections c JOIN recording_qualification_runs q ON q.id=$2 AND q.account_id=c.account_id
		WHERE c.id=$1 AND q.status='active'`, req.ConnectionID, req.QualificationRunID).
		Scan(&plan.AccountID, &plan.SelectionAuthority.QualificationRuleVersion,
			&plan.SelectionAuthority.QualificationCohortSHA256, &plan.SelectionAuthority.QualificationWindowsSHA256,
			&plan.SelectionAuthority.QualificationRunFrozenAt); err != nil {
		return plan, nil, errors.New("Tier-1 connection or active qualification run differs")
	}
	plan.MediaTool = tool
	historical := historicalQualificationVersion(plan.SelectionAuthority.QualificationRuleVersion)
	historicalImportedAfterCutoff := historicalAuthorityImportedAfterCutoff(
		plan.SelectionAuthority.QualificationRuleVersion,
		plan.SelectionAuthority.QualificationRunFrozenAt, req.EligibilityCutoff)
	qualificationQuery := `SELECT chosen.ord,m.recording_id,m.cron_timezone,r.naming_profile,r.folder_name,
		r.naming_metadata_jsonb,w.ordinal,to_char(w.local_open_at,'YYYY-MM-DD'),w.window_start_at,w.window_end_at,
		matched.id,matched.scheduled_for,matched.status,matched.completed_at
		FROM unnest($3::bigint[]) WITH ORDINALITY chosen(recording_id,ord)
		JOIN recording_qualification_members m ON m.run_id=$2 AND m.recording_id=chosen.recording_id AND m.account_id=$1
		JOIN recordings r ON r.id=m.recording_id AND r.account_id=$1 AND r.mode='continuous' AND r.delivery='nas_pull'
		  AND r.cron_timezone=m.cron_timezone AND r.daily_window_start='08:00'::time AND r.daily_window_end='20:00'::time
		JOIN recording_qualification_windows w ON w.run_id=m.run_id AND w.recording_id=m.recording_id
		JOIN LATERAL (SELECT min(j.id) id,min(j.scheduled_for) scheduled_for,min(j.status) status,
		  min(j.completed_at) completed_at,count(*) matches
		  FROM recording_jobs j WHERE j.recording_id=m.recording_id AND j.fire_at=w.window_start_at
		    AND j.scheduled_for=w.window_start_at AND j.kind='continuous_window' AND j.window_end_at=w.window_end_at
		    AND j.status='done' AND j.completed_at>=w.window_end_at AND j.completed_at<=$4) matched ON matched.matches=1
		ORDER BY chosen.ord,w.ordinal`
	if historical {
		qualificationQuery = `SELECT chosen.ord,m.recording_id,m.cron_timezone,r.naming_profile,r.folder_name,
			r.naming_metadata_jsonb,w.ordinal,to_char(w.local_open_at,'YYYY-MM-DD'),w.window_start_at,w.window_end_at,
			(day.item->>'job_id')::BIGINT,(day.item->>'scheduled_for')::TIMESTAMPTZ,
			day.item->>'job_status',(day.item->>'completed_at')::TIMESTAMPTZ
			FROM unnest($3::bigint[]) WITH ORDINALITY chosen(recording_id,ord)
			JOIN recording_qualification_runs q ON q.id=$2 AND q.account_id=$1
			JOIN recording_qualification_members m ON m.run_id=q.id AND m.recording_id=chosen.recording_id AND m.account_id=$1
			JOIN recordings r ON r.id=m.recording_id AND r.account_id=$1 AND r.mode='continuous' AND r.delivery='nas_pull'
			  AND r.cron_timezone=m.cron_timezone AND r.daily_window_start='08:00'::time AND r.daily_window_end='20:00'::time
			JOIN recording_qualification_windows w ON w.run_id=m.run_id AND w.recording_id=m.recording_id
			JOIN LATERAL (SELECT q.definition_jsonb->'canonical_plan'->'members'->((chosen.ord-1)::INTEGER) item) imported
			  ON (imported.item->>'recording_id')::BIGINT=m.recording_id
			JOIN LATERAL (SELECT imported.item->'qualification'->'days'->(w.ordinal-1) item) day
			  ON (day.item->>'qualification_window_ordinal')::INTEGER=w.ordinal
			  AND (day.item->>'window_start')::TIMESTAMPTZ=w.window_start_at
			  AND (day.item->>'window_end')::TIMESTAMPTZ=w.window_end_at
			  AND (day.item->>'completed_at')::TIMESTAMPTZ<=$4
			ORDER BY chosen.ord,w.ordinal`
	}
	rows, err := q.Query(ctx, qualificationQuery, plan.AccountID, req.QualificationRunID, req.RecordingIDs, req.EligibilityCutoff)
	if err != nil {
		return plan, nil, fmt.Errorf("load Tier-1 qualification facts: %w", err)
	}
	defer rows.Close()
	var jobs []joinedrecording.QualifiedDay
	for rows.Next() {
		var ordinal, windowOrdinal int
		var recordingID, jobID int64
		var timezone, profileRaw, folderRaw, localDate, jobStatus string
		var metadataRaw []byte
		var windowStart, windowEnd, scheduledFor, completedAt time.Time
		if err := rows.Scan(&ordinal, &recordingID, &timezone, &profileRaw, &folderRaw, &metadataRaw,
			&windowOrdinal, &localDate, &windowStart, &windowEnd, &jobID, &scheduledFor, &jobStatus, &completedAt); err != nil {
			return plan, nil, err
		}
		if ordinal < 1 || ordinal > len(req.RecordingIDs) || req.RecordingIDs[ordinal-1] != recordingID || windowOrdinal < 1 || windowOrdinal > 14 {
			return plan, nil, errors.New("Tier-1 qualification order differs")
		}
		if len(plan.Recordings)+1 == ordinal {
			profile, parseErr := recordingnaming.ParseProfile(profileRaw)
			metadata, metadataErr := recordingnaming.ParseMetadata(metadataRaw)
			folder, folderErr := recordingnaming.BuildFolderName(profile, recordingID, metadata, folderRaw)
			if parseErr != nil || metadataErr != nil || folderErr != nil {
				return plan, nil, fmt.Errorf("Tier-1 recording %d naming differs", recordingID)
			}
			plan.Recordings = append(plan.Recordings, joinedTier1FreezeRecording{
				Frozen: joinedrecording.FrozenRecording{RecordingID: recordingID, PriorityOrdinal: ordinal,
					SelectionTier: "good+", Timezone: timezone, FolderName: folder, NamingMetadata: metadata},
				Qualification: joinedrecording.QualificationWindow{RecordingID: recordingID, Timezone: timezone,
					FrozenAt: req.EligibilityCutoff.UTC(), AuthorityKind: historicalAuthorityKind(historical)},
			})
		}
		if len(plan.Recordings) != ordinal {
			return plan, nil, errors.New("Tier-1 qualification member order differs")
		}
		recording := &plan.Recordings[ordinal-1]
		location, locationErr := time.LoadLocation(timezone)
		if locationErr != nil {
			return plan, nil, errors.New("Tier-1 recording timezone differs")
		}
		date, dateErr := time.ParseInLocation("2006-01-02", localDate, location)
		startLocal, endLocal := windowStart.In(location), windowEnd.In(location)
		var previousDate time.Time
		if len(recording.Qualification.Days) > 0 {
			previousDate, _ = time.ParseInLocation("2006-01-02",
				recording.Qualification.Days[len(recording.Qualification.Days)-1].LocalDate, location)
		}
		if recording.Frozen.RecordingID != recordingID || len(recording.Qualification.Days)+1 != windowOrdinal ||
			dateErr != nil || windowEnd.Sub(windowStart) != 12*time.Hour ||
			startLocal.Format("2006-01-02") != localDate || endLocal.Format("2006-01-02") != localDate ||
			startLocal.Hour() != 8 || startLocal.Minute() != 0 || startLocal.Second() != 0 || startLocal.Nanosecond() != 0 ||
			endLocal.Hour() != 20 || endLocal.Minute() != 0 || endLocal.Second() != 0 || endLocal.Nanosecond() != 0 ||
			(windowOrdinal > 1 && !date.Equal(previousDate.AddDate(0, 0, 1))) || completedAt.After(req.EligibilityCutoff) ||
			(!historical && (plan.SelectionAuthority.QualificationRunFrozenAt.After(windowStart) || completedAt.Before(windowEnd))) ||
			(historical && !historicalImportedAfterCutoff) {
			return plan, nil, errors.New("Tier-1 completed job/window facts differ")
		}
		day := joinedrecording.QualifiedDay{QualificationWindowOrdinal: windowOrdinal, LocalDate: localDate,
			WindowStart: windowStart.UTC(), WindowEnd: windowEnd.UTC(), JobID: jobID, CompletedAt: completedAt.UTC()}
		if historical {
			scheduledUTC := scheduledFor.UTC()
			day.ScheduledFor, day.JobStatus = &scheduledUTC, jobStatus
			if !scheduledFor.Equal(windowStart) {
				day.ReasonCodes = append(day.ReasonCodes, "scheduled_for_drift")
			}
			if jobStatus == "error" {
				day.ReasonCodes = append(day.ReasonCodes, "terminal_job_error")
			}
		}
		recording.Qualification.Days = append(recording.Qualification.Days, day)
		jobs = append(jobs, day)
	}
	if err := rows.Err(); err != nil {
		return plan, nil, err
	}
	if len(plan.Recordings) != len(req.RecordingIDs) || len(jobs) != plan.ExpectedStreamDays {
		return plan, nil, errors.New("Tier-1 run lacks exactly 33 members and 462 completed jobs")
	}
	plan.SelectionAuthority.QualificationRunFrozenAt = plan.SelectionAuthority.QualificationRunFrozenAt.UTC()
	frozenRecordings := make([]joinedrecording.FrozenRecording, len(plan.Recordings))
	for i := range plan.Recordings {
		sealed, sealErr := joinedrecording.SealQualificationWindow(plan.Recordings[i].Qualification)
		if sealErr != nil {
			return plan, nil, fmt.Errorf("seal Tier-1 recording %d qualification: %w", sealed.RecordingID, sealErr)
		}
		plan.Recordings[i].Qualification = sealed
		plan.Recordings[i].Frozen.QualificationSHA256 = sealed.EvidenceSHA
		for _, day := range sealed.Days {
			if day.CompletedAt.After(plan.Recordings[i].Frozen.CompletedAt) {
				plan.Recordings[i].Frozen.CompletedAt = day.CompletedAt
			}
		}
		frozenRecordings[i] = plan.Recordings[i].Frozen
	}
	selectedWindowsSHA, err := joinedrecording.SelectedQualificationWindowsSHA256(frozenRecordings)
	if err != nil {
		return plan, nil, err
	}
	plan.SelectionAuthority.SelectedQualificationWindowsSHA256 = selectedWindowsSHA
	if err := joinedrecording.ValidateSelectionAuthority(plan.SelectionAuthority, plan.RecordingIDs); err != nil {
		return plan, nil, err
	}
	jobsBytes, err := json.Marshal(jobs)
	if err != nil {
		return plan, nil, fmt.Errorf("marshal qualification jobs: %w", err)
	}
	plan.QualificationJobsSHA256 = sha256Bytes(jobsBytes)
	if !countSources {
		return plan, nil, nil
	}
	if _, err := populateJoinedTier1FrozenEvidence(ctx, q, &plan, false); err != nil {
		return plan, nil, err
	}
	return sealJoinedTier1FreezePlan(plan)
}

func sealJoinedTier1FreezePlan(plan joinedTier1FreezePlan) (joinedTier1FreezePlan, []byte, error) {
	plan.RequestSHA256 = ""
	requestBytes, err := json.Marshal(plan)
	if err != nil {
		return plan, nil, err
	}
	plan.RequestSHA256 = sha256Bytes(requestBytes)
	return plan, requestBytes, nil
}

func joinedTier1JobIDs(days []joinedrecording.QualifiedDay) []int64 {
	ids := make([]int64, len(days))
	for i := range days {
		ids[i] = days[i].JobID
	}
	return ids
}

func populateJoinedTier1FrozenEvidence(ctx context.Context, q joinedTier1FreezeQuerier, plan *joinedTier1FreezePlan, fromApplySnapshot bool) ([]joinedrecording.FrozenDenominatorDayProjection, error) {
	return populateJoinedTier1FrozenEvidenceWithWatermarks(ctx, q, plan, fromApplySnapshot, nil)
}

func populateJoinedTier1FrozenEvidenceWithWatermarks(ctx context.Context, q joinedTier1FreezeQuerier, plan *joinedTier1FreezePlan,
	fromApplySnapshot bool, fixedWatermarks map[int64]int64) ([]joinedrecording.FrozenDenominatorDayProjection, error) {
	recordingIDs, jobIDs := make([]int64, 0, plan.ExpectedStreamDays), make([]int64, 0, plan.ExpectedStreamDays)
	windowStarts, windowEnds := make([]time.Time, 0, plan.ExpectedStreamDays), make([]time.Time, 0, plan.ExpectedStreamDays)
	for _, recording := range plan.Recordings {
		for _, day := range recording.Qualification.Days {
			recordingIDs, jobIDs = append(recordingIDs, recording.Frozen.RecordingID), append(jobIDs, day.JobID)
			windowStarts, windowEnds = append(windowStarts, day.WindowStart), append(windowEnds, day.WindowEnd)
		}
	}
	watermarks := fixedWatermarks
	if watermarks == nil {
		watermarks = make(map[int64]int64, len(jobIDs))
	}
	if !fromApplySnapshot && fixedWatermarks == nil {
		watermarkRows, err := q.Query(ctx, `WITH selected AS (SELECT * FROM unnest($1::bigint[],$2::bigint[]) AS d(recording_id,job_id))
			SELECT d.job_id,COALESCE(max(c.id),0)::bigint FROM selected d LEFT JOIN recording_clips c
			  ON c.recording_id=d.recording_id AND c.recording_job_id=d.job_id GROUP BY d.job_id`, recordingIDs, jobIDs)
		if err != nil {
			return nil, fmt.Errorf("load Tier-1 clip watermarks: %w", err)
		}
		for watermarkRows.Next() {
			var jobID, watermark int64
			if err := watermarkRows.Scan(&jobID, &watermark); err != nil {
				watermarkRows.Close()
				return nil, err
			}
			watermarks[jobID] = watermark
		}
		if err := watermarkRows.Err(); err != nil {
			watermarkRows.Close()
			return nil, err
		}
		watermarkRows.Close()
	}
	watermarkValues := make([]int64, len(jobIDs))
	for i, jobID := range jobIDs {
		watermarkValues[i] = watermarks[jobID]
	}
	sourceQuery := `WITH selected AS (SELECT * FROM unnest($1::bigint[],$2::bigint[],$3::timestamptz[],$4::timestamptz[],$5::bigint[])
		AS d(recording_id,job_id,window_start,window_end,high_water_clip_id))
		SELECT c.id,c.recording_id,c.recording_job_id,c.storage_destination_id,sd.account_id,sd.provider,c.endpoint,
			sd.endpoint,sd.region,c.bucket,sd.bucket,c.object_key,c.etag,c.size_bytes,c.sha256,c.clip_start_at,c.clip_end_at,c.released_at
		FROM selected d JOIN recording_clips c ON c.recording_id=d.recording_id AND c.recording_job_id=d.job_id
		JOIN storage_destinations sd ON sd.id=c.storage_destination_id
		WHERE c.id<=d.high_water_clip_id AND c.purged_at IS NULL AND c.size_bytes>0 AND c.created_at<=$6
		  AND c.clip_end_at>d.window_start AND c.clip_start_at<d.window_end
		ORDER BY c.recording_id,c.recording_job_id,c.clip_start_at,c.id`
	args := []any{recordingIDs, jobIDs, windowStarts, windowEnds, watermarkValues, plan.SelectionAuthority.Cutoff}
	if fromApplySnapshot {
		sourceQuery = `SELECT clip_id,recording_id,recording_job_id,storage_destination_id,account_id,provider,endpoint,
			destination_endpoint,region,bucket,destination_bucket,object_key,ingest_etag,size_bytes,sha256,start_at,end_at,released_at
			FROM joined_tier1_apply_sources ORDER BY recording_id,recording_job_id,day_ordinal`
		args = nil
	}
	rows, err := q.Query(ctx, sourceQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("load Tier-1 frozen sources: %w", err)
	}
	type dayEvidence struct {
		index            int
		recordingID      int64
		qualificationSHA string
		day              joinedrecording.QualifiedDay
	}
	daysByJob := make(map[int64]dayEvidence, plan.ExpectedStreamDays)
	dayProjections := make([]joinedrecording.FrozenDenominatorDayProjection, plan.ExpectedStreamDays)
	projected := make([]bool, plan.ExpectedStreamDays)
	dayIndex := 0
	for _, recording := range plan.Recordings {
		for _, day := range recording.Qualification.Days {
			daysByJob[day.JobID] = dayEvidence{dayIndex, recording.Frozen.RecordingID, recording.Frozen.QualificationSHA256, day}
			dayIndex++
		}
	}
	var currentJobID int64
	var currentSources []joinedrecording.FrozenSourceSnapshot
	flush := func() error {
		if currentJobID == 0 {
			return nil
		}
		evidence, ok := daysByJob[currentJobID]
		if !ok || projected[evidence.index] {
			return errors.New("Tier-1 source job differs from qualification evidence")
		}
		canonicalSources := append([]joinedrecording.FrozenSourceSnapshot(nil), currentSources...)
		// released_at is mutable bookkeeping. V2 records it in the retained source
		// snapshot, but it is deliberately outside denominator identity.
		for i := range canonicalSources {
			canonicalSources[i].ReleasedAt = nil
		}
		projection, err := joinedrecording.BuildFrozenDenominatorDayProjection(evidence.recordingID,
			evidence.day, evidence.qualificationSHA, canonicalSources)
		if err != nil {
			return err
		}
		dayProjections[evidence.index], projected[evidence.index] = projection, true
		currentSources = currentSources[:0]
		return nil
	}
	for rows.Next() {
		var source joinedrecording.FrozenSourceSnapshot
		var storageDestinationID, sourceAccountID int64
		var destinationEndpoint, destinationBucket, ingestETag string
		if err := rows.Scan(&source.ClipID, &source.RecordingID, &source.RecordingJobID, &storageDestinationID,
			&sourceAccountID, &source.Provider, &source.Endpoint, &destinationEndpoint, &source.Region, &source.Bucket,
			&destinationBucket, &source.ObjectKey, &ingestETag, &source.SizeBytes, &source.IngestSHA256,
			&source.StartUTC, &source.EndUTC, &source.ReleasedAt); err != nil {
			rows.Close()
			return nil, err
		}
		if storageDestinationID <= 0 || sourceAccountID != plan.AccountID || source.Endpoint != plan.SourceEndpoint ||
			destinationEndpoint != plan.SourceEndpoint || source.Bucket != destinationBucket || strings.TrimSpace(ingestETag) == "" ||
			source.SizeBytes <= 0 || !lowerHexSHA256(source.IngestSHA256) {
			rows.Close()
			return nil, errors.New("Tier-1 source differs from the approved storage authority or identity contract")
		}
		source.StorageDestinationID = storageDestinationID
		source.StartUTC, source.EndUTC = source.StartUTC.UTC(), source.EndUTC.UTC()
		if source.ReleasedAt != nil {
			released := source.ReleasedAt.UTC()
			source.ReleasedAt = &released
		}
		if currentJobID != 0 && source.RecordingJobID != currentJobID {
			if err := flush(); err != nil {
				rows.Close()
				return nil, err
			}
		}
		currentJobID = source.RecordingJobID
		currentSources = append(currentSources, source)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	if err := flush(); err != nil {
		return nil, err
	}

	frozenRecordings := make([]joinedrecording.FrozenRecording, len(plan.Recordings))
	var sourceCount, sourceBytes int64
	for i := range plan.Recordings {
		recording := plan.Recordings[i]
		frozenRecordings[i] = recording.Frozen
		for _, day := range recording.Qualification.Days {
			evidence := daysByJob[day.JobID]
			if !projected[evidence.index] {
				projection, err := joinedrecording.BuildFrozenDenominatorDayProjection(recording.Frozen.RecordingID,
					day, recording.Frozen.QualificationSHA256, nil)
				if err != nil {
					return nil, err
				}
				dayProjections[evidence.index], projected[evidence.index] = projection, true
			}
			projection := dayProjections[evidence.index]
			sourceCount += int64(projection.SourceCount)
			sourceBytes += projection.SourceBytes
			plan.Recordings[i].SnapshotDays = append(plan.Recordings[i].SnapshotDays, joinedTier1FreezeDayScope{
				LocalDate: day.LocalDate, DateOrdinal: day.QualificationWindowOrdinal, RecordingJobID: day.JobID,
				HighWaterClipID: watermarks[day.JobID], ExpectedSourceClips: projection.SourceCount,
				ExpectedSourceBytes: projection.SourceBytes, ExpectedSourceSHA256: projection.FrozenSourceSHA256,
			})
			plan.Recordings[i].ExpectedSourceClips += int64(projection.SourceCount)
			plan.Recordings[i].ExpectedSourceBytes += projection.SourceBytes
		}
	}
	plan.ProvisionalSourceClips, plan.ProvisionalSourceBytes = sourceCount, sourceBytes
	if len(plan.Recordings) == len(joinedrecording.Tier1RecordingIDs) {
		denominatorSHA, err := joinedrecording.ComputeFrozenDenominatorSHA256(plan.SelectionAuthority, frozenRecordings, dayProjections)
		if err != nil {
			return nil, err
		}
		plan.FrozenDenominatorSHA256 = denominatorSHA
	}

	exclusionScopeSQL := `SELECT d.*,$6::timestamptz cutoff FROM unnest($1::bigint[],$2::bigint[],$3::timestamptz[],
		$4::timestamptz[],$5::bigint[]) AS d(recording_id,job_id,window_start,window_end,high_water_clip_id)`
	exclusionQuery := joinedTier1ExclusionQuery(exclusionScopeSQL, `
		SELECT count(*)::bigint,encode(sha256(convert_to(COALESCE(string_agg(format('%s\n%s\n%s\n%s\n',recording_id,
		 clip_id,reason_code,evidence_sha256),'' ORDER BY recording_id,clip_id,reason_code,evidence_sha256),''),'UTF8')),'hex') FROM evidence`)
	exclusionArgs := []any{recordingIDs, jobIDs, windowStarts, windowEnds, watermarkValues, plan.SelectionAuthority.Cutoff}
	if fromApplySnapshot {
		exclusionQuery = `SELECT count(*)::bigint,encode(sha256(convert_to(COALESCE(string_agg(format('%s\n%s\n%s\n%s\n',
			recording_id,clip_id,reason_code,evidence_sha256),'' ORDER BY recording_id,clip_id,reason_code,evidence_sha256),''),'UTF8')),'hex')
			FROM joined_tier1_apply_exclusions`
		exclusionArgs = nil
	}
	if err := q.QueryRow(ctx, exclusionQuery, exclusionArgs...).Scan(&plan.ProvisionalExclusions, &plan.FreezeExclusionsSHA256); err != nil {
		return nil, fmt.Errorf("load Tier-1 freeze exclusions: %w", err)
	}
	if !fromApplySnapshot {
		exclusionRows, err := q.Query(ctx, joinedTier1ExclusionQuery(exclusionScopeSQL, `
			SELECT recording_id,count(*)::bigint,encode(sha256(convert_to(COALESCE(string_agg(format('%s\n%s\n%s\n%s\n',recording_id,
			 clip_id,reason_code,evidence_sha256),'' ORDER BY recording_id,clip_id,reason_code,evidence_sha256),''),'UTF8')),'hex')
			FROM evidence GROUP BY recording_id`), recordingIDs, jobIDs, windowStarts, windowEnds, watermarkValues, plan.SelectionAuthority.Cutoff)
		if err != nil {
			return nil, fmt.Errorf("load Tier-1 recording exclusions: %w", err)
		}
		byID := make(map[int64]int, len(plan.Recordings))
		for i := range plan.Recordings {
			byID[plan.Recordings[i].Frozen.RecordingID] = i
			plan.Recordings[i].ExpectedExclusionsSHA256 = sha256Bytes(nil)
		}
		for exclusionRows.Next() {
			var recordingID, count int64
			var digest string
			if err := exclusionRows.Scan(&recordingID, &count, &digest); err != nil {
				exclusionRows.Close()
				return nil, err
			}
			i, ok := byID[recordingID]
			if !ok {
				exclusionRows.Close()
				return nil, errors.New("Tier-1 exclusion recording differs")
			}
			plan.Recordings[i].ExpectedExclusions = count
			plan.Recordings[i].ExpectedExclusionsSHA256 = digest
		}
		if err := exclusionRows.Err(); err != nil {
			exclusionRows.Close()
			return nil, err
		}
		exclusionRows.Close()
	}
	return dayProjections, nil
}

func (s *Server) applyJoinedTier1Freeze(ctx context.Context, req joinedTier1FreezeRequest) (joinedTier1FreezePlan, bool, error) {
	tool, err := joinedrecording.InspectMediaToolEvidence(ctx)
	if err != nil {
		return joinedTier1FreezePlan{}, false, fmt.Errorf("inspect joined media tool: %w", err)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return joinedTier1FreezePlan{}, false, err
	}
	defer tx.Rollback(ctx)
	// This must be the first transactional operation: it linearizes the raw
	// membership snapshot against application purge/delete without row-locking
	// the roughly 403k candidate clips.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(137,1)`); err != nil {
		return joinedTier1FreezePlan{}, false, err
	}
	var existingBytes []byte
	var existingSHA string
	err = tx.QueryRow(ctx, `SELECT b.freeze_request_bytes,b.freeze_request_sha256
		FROM recording_joined_batches b JOIN connections c ON c.id=b.connection_id
		WHERE b.batch_id=$1 AND c.id=$2 FOR SHARE OF b,c`, req.BatchID,
		s.cfg.JoinedRecordingConnectionID).Scan(&existingBytes, &existingSHA)
	if err == nil {
		if existingSHA != req.ExpectedRequestSHA256 {
			return joinedTier1FreezePlan{}, false, errors.New("Tier-1 batch key already has different immutable evidence")
		}
		var existing joinedTier1FreezePlan
		if json.Unmarshal(existingBytes, &existing) != nil {
			return joinedTier1FreezePlan{}, false, errors.New("stored Tier-1 request is not canonical")
		}
		existing.RequestSHA256 = existingSHA
		if !joinedTier1FreezeRequestMatchesPlan(req, existing) {
			return joinedTier1FreezePlan{}, false, errors.New("Tier-1 batch key differs from the explicit immutable request")
		}
		if err := tx.Commit(ctx); err != nil {
			return joinedTier1FreezePlan{}, false, err
		}
		return existing, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return joinedTier1FreezePlan{}, false, err
	}
	plan, _, err := s.buildJoinedTier1FreezePlanWithTool(ctx, tx, req, tool, false)
	if err != nil {
		return joinedTier1FreezePlan{}, false, err
	}
	jobIDs, recordingIDs := make([]int64, 0, plan.ExpectedStreamDays), make([]int64, 0, plan.ExpectedStreamDays)
	windowStarts, windowEnds := make([]time.Time, 0, plan.ExpectedStreamDays), make([]time.Time, 0, plan.ExpectedStreamDays)
	for _, recording := range plan.Recordings {
		for _, day := range recording.Qualification.Days {
			jobIDs, recordingIDs = append(jobIDs, day.JobID), append(recordingIDs, recording.Frozen.RecordingID)
			windowStarts, windowEnds = append(windowStarts, day.WindowStart), append(windowEnds, day.WindowEnd)
		}
	}
	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE joined_tier1_apply_candidates ON COMMIT DROP AS
		WITH selected AS (SELECT * FROM unnest($1::bigint[],$2::bigint[],$3::timestamptz[],$4::timestamptz[])
			AS d(recording_id,job_id,window_start,window_end))
		SELECT c.id clip_id,c.recording_id,c.recording_job_id,c.storage_destination_id,
			sd.account_id,sd.provider,c.endpoint,sd.endpoint destination_endpoint,sd.region,c.bucket,
			sd.bucket destination_bucket,c.object_key,c.etag ingest_etag,
			c.size_bytes,c.sha256,c.clip_start_at start_at,c.clip_end_at end_at,c.created_at clip_created_at,
			c.released_at,c.capture_lease_token,c.capture_sequence,c.capture_attempt_id,c.timestamp_contract_version,
			c.timestamp_contract,c.timestamp_contract_status,c.timestamp_contract_reason,
			d.window_start,d.window_end,$5::timestamptz cutoff
		FROM selected d JOIN recording_clips c ON c.recording_job_id=d.job_id AND c.recording_id=d.recording_id
		JOIN storage_destinations sd ON sd.id=c.storage_destination_id
		WHERE c.purged_at IS NULL`,
		recordingIDs, jobIDs, windowStarts, windowEnds, plan.SelectionAuthority.Cutoff); err != nil {
		return joinedTier1FreezePlan{}, false, err
	}
	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE joined_tier1_apply_sources ON COMMIT DROP AS
		SELECT c.clip_id,c.recording_id,c.recording_job_id,c.storage_destination_id,c.account_id,c.provider,c.endpoint,
			c.destination_endpoint,
			c.region,c.bucket,c.object_key,c.ingest_etag,c.size_bytes,c.sha256,c.start_at,c.end_at,c.clip_created_at,
			c.destination_bucket,
			c.released_at,c.capture_lease_token,c.capture_sequence,c.capture_attempt_id,c.timestamp_contract_version,
			c.timestamp_contract,c.timestamp_contract_status,c.timestamp_contract_reason,
			row_number() OVER (PARTITION BY c.recording_job_id ORDER BY c.start_at,c.clip_id)::integer day_ordinal
		FROM joined_tier1_apply_candidates c
		WHERE c.clip_created_at<=c.cutoff AND c.end_at>c.window_start AND c.start_at<c.window_end
		  AND c.size_bytes>0`); err != nil {
		return joinedTier1FreezePlan{}, false, err
	}
	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE joined_tier1_apply_exclusions ON COMMIT DROP AS
		SELECT evidence.*,encode(sha256(convert_to(evidence.canonical_evidence::text,'UTF8')),'hex') evidence_sha256 FROM (
		SELECT c.clip_id,c.recording_id,
			CASE WHEN c.clip_created_at>c.cutoff THEN 'after_cutoff'
			  WHEN c.end_at<=c.window_start OR c.start_at>=c.window_end THEN 'outside_qualification_window'
			  ELSE 'nonpositive_source_size' END reason_code,
			jsonb_build_object('clip_id',c.clip_id,'recording_id',c.recording_id,'recording_job_id',c.recording_job_id,
			  'created_at',c.clip_created_at,'clip_start_at',c.start_at,'clip_end_at',c.end_at,'size_bytes',c.size_bytes,
			  'window_start_at',c.window_start,'window_end_at',c.window_end,'cutoff',c.cutoff) canonical_evidence
		FROM joined_tier1_apply_candidates c
		WHERE c.clip_created_at>c.cutoff OR c.end_at<=c.window_start OR c.start_at>=c.window_end
		  OR c.size_bytes<=0) evidence`); err != nil {
		return joinedTier1FreezePlan{}, false, err
	}
	dayProjections, err := populateJoinedTier1FrozenEvidence(ctx, tx, &plan, true)
	if err != nil {
		return joinedTier1FreezePlan{}, false, err
	}
	plan, requestBytes, err := sealJoinedTier1FreezePlan(plan)
	if err != nil {
		return joinedTier1FreezePlan{}, false, err
	}
	if req.ExpectedRequestSHA256 != plan.RequestSHA256 {
		return joinedTier1FreezePlan{}, false, errors.New("expected_request_sha256 differs from current Tier-1 plan")
	}
	mediaToolJSON, err := json.Marshal(plan.MediaTool)
	if err != nil {
		return joinedTier1FreezePlan{}, false, fmt.Errorf("marshal media tool evidence: %w", err)
	}
	var batchRecordID int64
	if err := tx.QueryRow(ctx, `INSERT INTO recording_joined_batches(account_id,connection_id,batch_id,generation,
		source_endpoint,qualification_run_id,qualification_cohort_sha256,qualification_windows_sha256,
		selected_qualification_windows_sha256,qualification_jobs_sha256,qualification_frozen_at,
		ordered_recording_ids_sha256,selection_basis,policy_version,eligibility_cutoff,media_tool,media_tool_sha256,
		freeze_request_bytes,freeze_request_sha256,frozen_denominator_sha256,freeze_exclusions_sha256,
		expected_recordings,expected_stream_days,expected_scheduled_hours,expected_source_clips,expected_source_bytes,
		expected_freeze_exclusions)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,33,462,5544,$22,$23,$24)
		RETURNING id`, plan.AccountID, plan.ConnectionID, plan.BatchID, plan.Generation, plan.SourceEndpoint,
		plan.SelectionAuthority.QualificationRunID, plan.SelectionAuthority.QualificationCohortSHA256,
		plan.SelectionAuthority.QualificationWindowsSHA256, plan.SelectionAuthority.SelectedQualificationWindowsSHA256,
		plan.QualificationJobsSHA256, plan.SelectionAuthority.QualificationRunFrozenAt,
		plan.SelectionAuthority.OrderedRecordingIDSHA256, plan.SelectionAuthority.SelectionBasis, plan.PolicyVersion,
		plan.SelectionAuthority.Cutoff, mediaToolJSON, plan.MediaTool.IdentitySHA256, requestBytes, plan.RequestSHA256,
		plan.FrozenDenominatorSHA256, plan.FreezeExclusionsSHA256, plan.ProvisionalSourceClips,
		plan.ProvisionalSourceBytes, plan.ProvisionalExclusions).Scan(&batchRecordID); err != nil {
		return joinedTier1FreezePlan{}, false, err
	}
	for _, recording := range plan.Recordings {
		qualificationJSON, err := json.Marshal(recording.Qualification)
		if err != nil {
			return joinedTier1FreezePlan{}, false, fmt.Errorf("marshal qualification for recording %d: %w", recording.Frozen.RecordingID, err)
		}
		jobIDs := joinedTier1JobIDs(recording.Qualification.Days)
		var batchRecordingID int64
		if err := tx.QueryRow(ctx, `INSERT INTO recording_joined_batch_recordings(batch_record_id,account_id,connection_id,
			batch_id,qualification_run_id,selection_tier,recording_id,priority_ordinal,timezone,folder_name,naming_metadata,
			first_local_date,last_local_date,qualification,qualification_sha256,qualification_policy_version,completed_at,
			authoritative_job_ids) VALUES($1,$2,$3,$4,$5,'good+',$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17) RETURNING id`,
			batchRecordID, plan.AccountID, plan.ConnectionID, plan.BatchID, plan.SelectionAuthority.QualificationRunID,
			recording.Frozen.RecordingID, recording.Frozen.PriorityOrdinal, recording.Frozen.Timezone,
			recording.Frozen.FolderName, recording.Frozen.NamingMetadata, recording.Qualification.Days[0].LocalDate,
			recording.Qualification.Days[13].LocalDate, qualificationJSON, recording.Frozen.QualificationSHA256,
			plan.SelectionAuthority.QualificationRuleVersion, recording.Frozen.CompletedAt, jobIDs).Scan(&batchRecordingID); err != nil {
			return joinedTier1FreezePlan{}, false, err
		}
		for _, day := range recording.Qualification.Days {
			projection := dayProjections[(recording.Frozen.PriorityOrdinal-1)*14+day.QualificationWindowOrdinal-1]
			if _, err := tx.Exec(ctx, `INSERT INTO recording_joined_stream_days(batch_record_id,batch_recording_id,
				account_id,connection_id,batch_id,recording_id,local_date,date_ordinal,recording_job_id,scheduled_start_at,
				scheduled_end_at,completed_at,source_clip_count,source_bytes,source_snapshot_sha256)
				VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`, batchRecordID, batchRecordingID,
				plan.AccountID, plan.ConnectionID, plan.BatchID, recording.Frozen.RecordingID, day.LocalDate,
				day.QualificationWindowOrdinal, day.JobID, day.WindowStart, day.WindowEnd, day.CompletedAt,
				projection.SourceCount, projection.SourceBytes, projection.FrozenSourceSHA256); err != nil {
				return joinedTier1FreezePlan{}, false, err
			}
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO recording_joined_freeze_exclusions(batch_record_id,batch_recording_id,
		recording_id,clip_id,reason_code,evidence_sha256,canonical_evidence)
		SELECT $1,br.id,e.recording_id,e.clip_id,e.reason_code,
			e.evidence_sha256,e.canonical_evidence
		FROM joined_tier1_apply_exclusions e JOIN recording_joined_batch_recordings br
			ON br.batch_record_id=$1 AND br.recording_id=e.recording_id`, batchRecordID); err != nil {
		return joinedTier1FreezePlan{}, false, err
	}
	if err := s.insertJoinedTier1SourceSnapshots(ctx, tx, batchRecordID, plan.AccountID, plan.ConnectionID); err != nil {
		return joinedTier1FreezePlan{}, false, err
	}
	if err := ctx.Err(); err != nil {
		return joinedTier1FreezePlan{}, false, err
	}
	if _, err := tx.Exec(ctx, `UPDATE recording_joined_batches SET state='building' WHERE id=$1 AND state='snapshotting'`, batchRecordID); err != nil {
		return joinedTier1FreezePlan{}, false, err
	}
	if err := ctx.Err(); err != nil {
		return joinedTier1FreezePlan{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return joinedTier1FreezePlan{}, false, err
	}
	return plan, true, nil
}

func (s *Server) insertJoinedTier1SourceSnapshots(ctx context.Context, tx pgx.Tx, batchRecordID, accountID, connectionID int64) error {
	type streamDay struct {
		id          int64
		sourceCount int64
		priority    int
	}
	rows, err := tx.Query(ctx, `SELECT d.id,d.source_clip_count,br.priority_ordinal
		FROM recording_joined_stream_days d JOIN recording_joined_batch_recordings br ON br.id=d.batch_recording_id
		WHERE d.batch_record_id=$1 ORDER BY br.priority_ordinal,d.date_ordinal,d.id`, batchRecordID)
	if err != nil {
		return fmt.Errorf("load Tier-1 source snapshot stream days: %w", err)
	}
	streamDays := make([]streamDay, 0, len(joinedrecording.Tier1RecordingIDs)*14)
	for rows.Next() {
		var day streamDay
		if err := rows.Scan(&day.id, &day.sourceCount, &day.priority); err != nil {
			rows.Close()
			return fmt.Errorf("load Tier-1 source snapshot stream day: %w", err)
		}
		streamDays = append(streamDays, day)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("load Tier-1 source snapshot stream days: %w", err)
	}

	if len(streamDays) != len(joinedrecording.Tier1RecordingIDs)*14 {
		return fmt.Errorf("load Tier-1 source snapshot stream days: got %d want %d",
			len(streamDays), len(joinedrecording.Tier1RecordingIDs)*14)
	}
	for start := 0; start < len(streamDays); start += 14 {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := min(start+14, len(streamDays))
		streamDayIDs := make([]int64, 0, end-start)
		var expectedSources int64
		priority := streamDays[start].priority
		for _, day := range streamDays[start:end] {
			if day.priority != priority {
				return errors.New("Tier-1 source snapshot recording chunk differs")
			}
			streamDayIDs = append(streamDayIDs, day.id)
			expectedSources += day.sourceCount
		}
		command, err := tx.Exec(ctx, `INSERT INTO recording_joined_source_snapshots(batch_record_id,stream_day_id,account_id,
		connection_id,recording_id,recording_job_id,clip_id,storage_destination_id,day_ordinal,provider,endpoint,region,
		bucket,object_key,ingest_etag,size_bytes,sha256,start_at,end_at,clip_created_at,released_at,capture_lease_token,
		capture_sequence,capture_attempt_id,timestamp_contract_version,timestamp_contract,timestamp_contract_status,
		timestamp_contract_reason)
		SELECT $1,d.id,$2,$3,t.recording_id,t.recording_job_id,t.clip_id,t.storage_destination_id,t.day_ordinal,
			t.provider,t.endpoint,t.region,t.bucket,t.object_key,t.ingest_etag,t.size_bytes,t.sha256,t.start_at,t.end_at,
			t.clip_created_at,t.released_at,t.capture_lease_token,t.capture_sequence,t.capture_attempt_id,
			t.timestamp_contract_version,t.timestamp_contract,t.timestamp_contract_status,t.timestamp_contract_reason
		FROM joined_tier1_apply_sources t JOIN recording_joined_stream_days d
			ON d.batch_record_id=$1 AND d.recording_id=t.recording_id AND d.recording_job_id=t.recording_job_id
		JOIN recording_joined_batch_recordings br ON br.id=d.batch_recording_id
		WHERE d.id=ANY($4::bigint[]) ORDER BY br.priority_ordinal,d.date_ordinal,t.day_ordinal`,
			batchRecordID, accountID, connectionID, streamDayIDs)
		if err != nil {
			return fmt.Errorf("insert Tier-1 source snapshot chunk: %w", err)
		}
		if command.RowsAffected() != expectedSources {
			return fmt.Errorf("insert Tier-1 source snapshot chunk: rows=%d want=%d", command.RowsAffected(), expectedSources)
		}
		if s.joinedFreezeChunkHook != nil {
			if err := s.joinedFreezeChunkHook(ctx, priority); err != nil {
				return fmt.Errorf("after Tier-1 source snapshot chunk: %w", err)
			}
		}
	}
	return nil
}

func sha256Bytes(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

func lowerHexSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && value == strings.ToLower(value)
}
