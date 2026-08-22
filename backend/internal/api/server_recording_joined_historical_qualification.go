package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

type joinedHistoricalQualificationJobs struct {
	RecordingID int64   `json:"recording_id"`
	JobIDs      []int64 `json:"job_ids"`
}

type joinedHistoricalQualificationRequest struct {
	ProtocolVersion       int                                 `json:"protocol_version"`
	ConnectionID          int64                               `json:"connection_id"`
	BatchID               string                              `json:"batch_id"`
	Generation            int                                 `json:"generation"`
	RecordingJobs         []joinedHistoricalQualificationJobs `json:"recording_jobs"`
	Apply                 bool                                `json:"apply"`
	ExpectedRequestSHA256 string                              `json:"expected_request_sha256,omitempty"`
}

type joinedHistoricalQualificationMember struct {
	RecordingID    int64                               `json:"recording_id"`
	StreamID       int64                               `json:"stream_id"`
	RecordingName  string                              `json:"recording_name"`
	StreamName     string                              `json:"stream_name"`
	Timezone       string                              `json:"timezone"`
	ScheduleStart  time.Time                           `json:"schedule_start_at"`
	ScheduleEnd    *time.Time                          `json:"schedule_end_at,omitempty"`
	ActiveWeekdays int16                               `json:"active_weekdays"`
	Qualification  joinedrecording.QualificationWindow `json:"qualification"`
}

type joinedHistoricalQualificationPlan struct {
	SchemaVersion            int                                   `json:"schema_version"`
	AuthorityKind            string                                `json:"authority_kind"`
	BatchID                  string                                `json:"batch_id"`
	Generation               int                                   `json:"generation"`
	AccountID                int64                                 `json:"account_id"`
	ConnectionID             int64                                 `json:"connection_id"`
	Cutoff                   time.Time                             `json:"cutoff"`
	OrderedRecordingIDSHA256 string                                `json:"ordered_recording_ids_sha256"`
	QualificationRuleVersion string                                `json:"qualification_rule_version"`
	QualificationJobsSHA256  string                                `json:"qualification_jobs_sha256"`
	Members                  []joinedHistoricalQualificationMember `json:"members"`
	RequestSHA256            string                                `json:"request_sha256,omitempty"`
}

type joinedHistoricalQualificationQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

func (r joinedHistoricalQualificationRequest) validate() error {
	if r.ProtocolVersion != joinedrecording.JoinedProtocolVersion || r.ConnectionID <= 0 ||
		r.BatchID != joinedrecording.Tier1BatchID || r.Generation != 1 ||
		len(r.RecordingJobs) != len(joinedrecording.Tier1RecordingIDs) {
		return errors.New("invalid exact Tier-1 historical qualification request")
	}
	seenJobs := make(map[int64]bool, len(r.RecordingJobs)*14)
	for i, recording := range r.RecordingJobs {
		if recording.RecordingID != joinedrecording.Tier1RecordingIDs[i] || len(recording.JobIDs) != 14 {
			return errors.New("historical qualification recording order or cardinality differs")
		}
		for _, jobID := range recording.JobIDs {
			if jobID <= 0 || seenJobs[jobID] {
				return errors.New("historical qualification job IDs must be unique positive integers")
			}
			seenJobs[jobID] = true
		}
	}
	if r.Apply && !lowerHexSHA256(r.ExpectedRequestSHA256) {
		return errors.New("expected_request_sha256 is required for apply")
	}
	return nil
}

func buildJoinedHistoricalQualificationPlan(ctx context.Context, q joinedHistoricalQualificationQuerier,
	req joinedHistoricalQualificationRequest) (joinedHistoricalQualificationPlan, error) {
	if err := req.validate(); err != nil {
		return joinedHistoricalQualificationPlan{}, err
	}
	cutoff, err := time.Parse(time.RFC3339Nano, joinedrecording.Tier1FrozenAt)
	if err != nil {
		return joinedHistoricalQualificationPlan{}, err
	}
	plan := joinedHistoricalQualificationPlan{
		SchemaVersion: 1, AuthorityKind: joinedrecording.Tier1HistoricalAuthorityKind, BatchID: req.BatchID,
		Generation: req.Generation, ConnectionID: req.ConnectionID, Cutoff: cutoff,
		OrderedRecordingIDSHA256: joinedrecording.Tier1RecordingIDSHA,
		QualificationRuleVersion: joinedrecording.Tier1HistoricalQualificationVersion,
		Members:                  make([]joinedHistoricalQualificationMember, 0, len(req.RecordingJobs)),
	}
	if err := q.QueryRow(ctx, `SELECT account_id FROM connections WHERE id=$1 AND joined_protocol_version=1`,
		req.ConnectionID).Scan(&plan.AccountID); err != nil {
		return plan, errors.New("Tier-1 connection differs")
	}
	recordingIDs := make([]int64, 0, len(req.RecordingJobs)*14)
	jobIDs := make([]int64, 0, len(req.RecordingJobs)*14)
	dayOrdinals := make([]int32, 0, len(req.RecordingJobs)*14)
	for _, recording := range req.RecordingJobs {
		for i, jobID := range recording.JobIDs {
			recordingIDs = append(recordingIDs, recording.RecordingID)
			jobIDs = append(jobIDs, jobID)
			dayOrdinals = append(dayOrdinals, int32(i+1))
		}
	}
	jobBytes, _ := json.Marshal(req.RecordingJobs)
	plan.QualificationJobsSHA256 = sha256Hex(jobBytes)
	rows, err := q.Query(ctx, `SELECT expected.recording_id,expected.day_ordinal,r.stream_id,r.name,s.name,
		r.cron_timezone,r.active_weekdays,r.start_at,r.end_at,j.id,j.fire_at,j.scheduled_for,j.window_end_at,j.completed_at,j.status
		FROM unnest($2::bigint[],$3::bigint[],$4::integer[]) expected(recording_id,job_id,day_ordinal)
		JOIN recordings r ON r.id=expected.recording_id AND r.account_id=$1 AND r.status='active'
		  AND r.mode='continuous' AND r.delivery='nas_pull' AND r.daily_window_start='08:00'::time
		  AND r.daily_window_end='20:00'::time
		JOIN streams s ON s.id=r.stream_id
		JOIN recording_jobs j ON j.id=expected.job_id AND j.recording_id=r.id AND j.kind='continuous_window'
		  AND j.status IN('done','error') AND j.completed_at IS NOT NULL
		ORDER BY array_position($5::bigint[],expected.recording_id),expected.day_ordinal
		FOR SHARE OF r,s,j`, plan.AccountID, recordingIDs, jobIDs, dayOrdinals, joinedrecording.Tier1RecordingIDs)
	if err != nil {
		return plan, fmt.Errorf("resolve historical qualification jobs: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var recordingID, streamID, jobID int64
		var dayOrdinal int
		var recordingName, streamName, timezone, jobStatus string
		var activeWeekdays int16
		var scheduleStart, fireAt, scheduledFor, windowEnd, completedAt time.Time
		var scheduleEnd *time.Time
		if err := rows.Scan(&recordingID, &dayOrdinal, &streamID, &recordingName, &streamName, &timezone,
			&activeWeekdays, &scheduleStart, &scheduleEnd, &jobID, &fireAt, &scheduledFor, &windowEnd, &completedAt, &jobStatus); err != nil {
			return plan, err
		}
		recordingOrdinal := len(plan.Members)
		if dayOrdinal == 1 {
			if recordingOrdinal >= len(joinedrecording.Tier1RecordingIDs) ||
				recordingID != joinedrecording.Tier1RecordingIDs[recordingOrdinal] {
				return plan, errors.New("historical qualification recording order differs")
			}
			plan.Members = append(plan.Members, joinedHistoricalQualificationMember{
				RecordingID: recordingID, StreamID: streamID, RecordingName: recordingName, StreamName: streamName,
				Timezone: timezone, ScheduleStart: scheduleStart.UTC(), ScheduleEnd: utcTimePointer(scheduleEnd),
				ActiveWeekdays: activeWeekdays,
				Qualification: joinedrecording.QualificationWindow{RecordingID: recordingID, Timezone: timezone, FrozenAt: cutoff,
					AuthorityKind: joinedrecording.Tier1HistoricalAuthorityKind},
			})
		}
		if len(plan.Members) == 0 {
			return plan, errors.New("historical qualification starts after ordinal one")
		}
		member := &plan.Members[len(plan.Members)-1]
		location, locationErr := time.LoadLocation(member.Timezone)
		if locationErr != nil {
			return plan, errors.New("historical qualification timezone differs")
		}
		startLocal, endLocal := fireAt.In(location), windowEnd.In(location)
		if member.RecordingID != recordingID || member.StreamID != streamID ||
			dayOrdinal != len(member.Qualification.Days)+1 ||
			windowEnd.Sub(fireAt) != 12*time.Hour || startLocal.Hour() != 8 || startLocal.Minute() != 0 ||
			startLocal.Second() != 0 || startLocal.Nanosecond() != 0 || endLocal.Hour() != 20 ||
			endLocal.Minute() != 0 || endLocal.Second() != 0 || endLocal.Nanosecond() != 0 ||
			startLocal.Format("2006-01-02") != endLocal.Format("2006-01-02") ||
			completedAt.After(cutoff) {
			return plan, errors.New("historical qualification completed job facts differ")
		}
		if dayOrdinal > 1 {
			previous, _ := time.ParseInLocation("2006-01-02", member.Qualification.Days[dayOrdinal-2].LocalDate, location)
			current, _ := time.ParseInLocation("2006-01-02", startLocal.Format("2006-01-02"), location)
			if !current.Equal(previous.AddDate(0, 0, 1)) {
				return plan, errors.New("historical qualification dates are not consecutive")
			}
		}
		reasons := make([]string, 0, 2)
		if !scheduledFor.Equal(fireAt) {
			reasons = append(reasons, "scheduled_for_drift")
		}
		if jobStatus == "error" {
			reasons = append(reasons, "terminal_job_error")
		}
		scheduledUTC := scheduledFor.UTC()
		member.Qualification.Days = append(member.Qualification.Days, joinedrecording.QualifiedDay{
			LocalDate: startLocal.Format("2006-01-02"), QualificationWindowOrdinal: dayOrdinal, JobID: jobID,
			ScheduledFor: &scheduledUTC, JobStatus: jobStatus, ReasonCodes: reasons,
			WindowStart: fireAt.UTC(), WindowEnd: windowEnd.UTC(), CompletedAt: completedAt.UTC(),
		})
	}
	if err := rows.Err(); err != nil {
		return plan, err
	}
	if len(plan.Members) != len(joinedrecording.Tier1RecordingIDs) {
		return plan, errors.New("historical qualification lacks exactly 33 recordings")
	}
	for i := range plan.Members {
		sealed, err := joinedrecording.SealQualificationWindow(plan.Members[i].Qualification)
		if err != nil {
			return plan, fmt.Errorf("seal historical qualification recording %d: %w", plan.Members[i].RecordingID, err)
		}
		plan.Members[i].Qualification = sealed
	}
	canonical := joinedHistoricalQualificationApprovalBytes(plan)
	plan.RequestSHA256 = sha256Hex(canonical)
	return plan, nil
}

func joinedHistoricalQualificationApprovalBytes(plan joinedHistoricalQualificationPlan) []byte {
	plan.RequestSHA256 = ""
	canonical, _ := json.Marshal(plan)
	return canonical
}

func utcTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	utc := value.UTC()
	return &utc
}

func (s *Server) handleAdminJoinedHistoricalQualification(w http.ResponseWriter, r *http.Request) {
	if !s.joinedControlPlaneReady() {
		util.WriteError(w, http.StatusServiceUnavailable, "joined recording is disabled")
		return
	}
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok || principal.Role != accountRoleAdmin {
		util.WriteError(w, http.StatusForbidden, "admin access required")
		return
	}
	var req joinedHistoricalQualificationRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := req.validate(); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !req.Apply {
		plan, err := buildJoinedHistoricalQualificationPlan(r.Context(), s.pool, req)
		if err != nil {
			util.WriteError(w, http.StatusConflict, err.Error())
			return
		}
		util.WriteJSON(w, http.StatusOK, map[string]any{"dry_run": true, "plan": plan})
		return
	}
	plan, runID, frozenAt, created, err := s.applyJoinedHistoricalQualification(r.Context(), req)
	if err != nil {
		util.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"dry_run": false, "created": created, "run_id": runID,
		"imported_at": frozenAt, "plan": plan})
}

func (s *Server) applyJoinedHistoricalQualification(ctx context.Context, req joinedHistoricalQualificationRequest) (
	joinedHistoricalQualificationPlan, int64, time.Time, bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return joinedHistoricalQualificationPlan{}, 0, time.Time{}, false, err
	}
	defer tx.Rollback(ctx)
	var accountID int64
	if err := tx.QueryRow(ctx, `SELECT account_id FROM connections WHERE id=$1 AND joined_protocol_version=1`,
		req.ConnectionID).Scan(&accountID); err != nil {
		return joinedHistoricalQualificationPlan{}, 0, time.Time{}, false, errors.New("Tier-1 connection differs")
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('recording-historical-qualification:'||($1::bigint)::text,0))`, accountID); err != nil {
		return joinedHistoricalQualificationPlan{}, 0, time.Time{}, false, err
	}
	var existingID int64
	var existingVersion, existingSHA string
	var existingFrozen time.Time
	var existingPlanJSON, existingJobsJSON []byte
	err = tx.QueryRow(ctx, `SELECT id,definition_version,COALESCE(definition_jsonb->>'request_sha256',''),frozen_at,
		definition_jsonb->'canonical_plan',definition_jsonb->'recording_jobs'
		FROM recording_qualification_runs WHERE account_id=$1 AND status='active'`, accountID).
		Scan(&existingID, &existingVersion, &existingSHA, &existingFrozen, &existingPlanJSON, &existingJobsJSON)
	if err == nil {
		jobBytes, _ := json.Marshal(req.RecordingJobs)
		var existingPlan joinedHistoricalQualificationPlan
		planDecoder := json.NewDecoder(bytes.NewReader(existingPlanJSON))
		planDecoder.DisallowUnknownFields()
		var existingJobs []joinedHistoricalQualificationJobs
		jobsDecoder := json.NewDecoder(bytes.NewReader(existingJobsJSON))
		jobsDecoder.DisallowUnknownFields()
		jobsDecodeErr := jobsDecoder.Decode(&existingJobs)
		existingJobBytes, _ := json.Marshal(existingJobs)
		if existingVersion != joinedrecording.Tier1HistoricalQualificationVersion ||
			existingSHA != req.ExpectedRequestSHA256 || jobsDecodeErr != nil || jobsDecoder.Decode(&struct{}{}) != io.EOF ||
			!bytes.Equal(existingJobBytes, jobBytes) || planDecoder.Decode(&existingPlan) != nil ||
			planDecoder.Decode(&struct{}{}) != io.EOF ||
			existingPlan.RequestSHA256 != existingSHA || sha256Hex(joinedHistoricalQualificationApprovalBytes(existingPlan)) != existingSHA ||
			existingPlan.AccountID != accountID || existingPlan.ConnectionID != req.ConnectionID ||
			existingPlan.BatchID != req.BatchID || existingPlan.Generation != req.Generation {
			return existingPlan, 0, time.Time{}, false, errors.New("a different active qualification authority already exists")
		}
		if err := tx.QueryRow(ctx, `SELECT account_id FROM connections WHERE id=$1 AND account_id=$2
			AND joined_protocol_version=1 FOR SHARE`, req.ConnectionID, accountID).Scan(&accountID); err != nil {
			return existingPlan, 0, time.Time{}, false, errors.New("Tier-1 connection changed during historical replay")
		}
		if err := tx.Commit(ctx); err != nil {
			return existingPlan, 0, time.Time{}, false, err
		}
		return existingPlan, existingID, existingFrozen.UTC(), false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return joinedHistoricalQualificationPlan{}, 0, time.Time{}, false, err
	}
	plan, err := buildJoinedHistoricalQualificationPlan(ctx, tx, req)
	if err != nil {
		return plan, 0, time.Time{}, false, err
	}
	if plan.AccountID != accountID || !strings.EqualFold(req.ExpectedRequestSHA256, plan.RequestSHA256) {
		return plan, 0, time.Time{}, false, errors.New("expected_request_sha256 differs from current historical authority")
	}
	jobBytes, _ := json.Marshal(req.RecordingJobs)
	definition := map[string]any{
		"version": joinedrecording.Tier1HistoricalQualificationVersion, "authority_kind": joinedrecording.Tier1HistoricalAuthorityKind,
		"batch_id": plan.BatchID, "generation": plan.Generation, "cutoff": plan.Cutoff,
		"ordered_recording_ids_sha256": plan.OrderedRecordingIDSHA256,
		"qualification_jobs_sha256":    plan.QualificationJobsSHA256, "request_sha256": plan.RequestSHA256,
		"recording_jobs": req.RecordingJobs, "canonical_plan": plan,
		"qualification_jobs_canonical": string(jobBytes),
		"request_canonical":            string(joinedHistoricalQualificationApprovalBytes(plan)),
		"historical_scene_claim":       false, "historical_per_day_grade_claim": false,
	}
	firstWindow := plan.Members[0].Qualification.Days[0].WindowStart
	var runID int64
	if err := tx.QueryRow(ctx, `INSERT INTO recording_qualification_runs(account_id,definition_version,definition_jsonb,
		target_recording_count,window_sequence_start_at) VALUES($1,$2,$3,33,$4) RETURNING id`, plan.AccountID,
		joinedrecording.Tier1HistoricalQualificationVersion, definition, firstWindow).Scan(&runID); err != nil {
		return plan, 0, time.Time{}, false, err
	}
	memberBatch, windowBatch := &pgx.Batch{}, &pgx.Batch{}
	for i, member := range plan.Members {
		memberBatch.Queue(`INSERT INTO recording_qualification_members(run_id,account_id,recording_id,ordinal,stream_id,
			recording_name,stream_name,scene_identity_sha256,scene_frame_evidence_id,cron_timezone,daily_window_start,
			daily_window_end,active_weekdays,schedule_start_at,schedule_end_at,window_generator_version)
			VALUES($1,$2,$3,$4,$5,$6,$7,NULL,NULL,$8,'08:00','20:00',$9,$10,$11,'historical-explicit-jobs-v1')`,
			runID, plan.AccountID, member.RecordingID, i+1, member.StreamID, member.RecordingName, member.StreamName,
			member.Timezone, member.ActiveWeekdays, member.ScheduleStart, member.ScheduleEnd)
		location, _ := time.LoadLocation(member.Timezone)
		for _, day := range member.Qualification.Days {
			startLocal, endLocal := day.WindowStart.In(location), day.WindowEnd.In(location)
			_, startOffset := startLocal.Zone()
			_, endOffset := endLocal.Zone()
			windowBatch.Queue(`INSERT INTO recording_qualification_windows(run_id,recording_id,ordinal,local_open_at,
				local_end_at,open_utc_offset_seconds,end_utc_offset_seconds,window_start_at,window_end_at,expected_seconds)
				VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,43200)`, runID, member.RecordingID, day.QualificationWindowOrdinal,
				startLocal.Format("2006-01-02 15:04:05"), endLocal.Format("2006-01-02 15:04:05"), startOffset, endOffset,
				day.WindowStart, day.WindowEnd)
		}
	}
	if err := execQualificationBatch(ctx, tx, memberBatch); err != nil {
		return plan, 0, time.Time{}, false, err
	}
	if err := execQualificationBatch(ctx, tx, windowBatch); err != nil {
		return plan, 0, time.Time{}, false, err
	}
	var frozenAt time.Time
	if err := tx.QueryRow(ctx, `UPDATE recording_qualification_runs SET status='active' WHERE id=$1 AND status='building'
		RETURNING frozen_at`, runID).Scan(&frozenAt); err != nil {
		return plan, 0, time.Time{}, false, err
	}
	if err := tx.QueryRow(ctx, `SELECT account_id FROM connections WHERE id=$1 AND account_id=$2
		AND joined_protocol_version=1 FOR SHARE`, req.ConnectionID, accountID).Scan(&accountID); err != nil {
		return plan, 0, time.Time{}, false, errors.New("Tier-1 connection changed during historical import")
	}
	if err := tx.Commit(ctx); err != nil {
		return plan, 0, time.Time{}, false, err
	}
	return plan, runID, frozenAt.UTC(), true, nil
}

func historicalQualificationVersion(version string) bool {
	return version == joinedrecording.Tier1HistoricalQualificationVersion
}

func historicalAuthorityKind(historical bool) string {
	if historical {
		return joinedrecording.Tier1HistoricalAuthorityKind
	}
	return ""
}

func historicalAuthorityImportedAfterCutoff(version string, importedAt, cutoff time.Time) bool {
	return historicalQualificationVersion(version) && !importedAt.IsZero() && importedAt.After(cutoff)
}
