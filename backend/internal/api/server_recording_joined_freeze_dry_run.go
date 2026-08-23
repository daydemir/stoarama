package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/daydemir/stoarama/backend/internal/util"
)

type joinedTier1DryRunStepRequest struct {
	RunID           string `json:"run_id"`
	PriorityOrdinal int    `json:"priority_ordinal"`
}

type joinedTier1DryRunProgress struct {
	RunID               string  `json:"run_id"`
	State               string  `json:"state"`
	CompletedRecordings int     `json:"completed_recordings"`
	ExpectedRecordings  int     `json:"expected_recordings"`
	NextPriorityOrdinal *int    `json:"next_priority_ordinal,omitempty"`
	RequestSHA256       *string `json:"request_sha256,omitempty"`
}

type joinedTier1DryRunRecordingEvidence struct {
	Recording     joinedTier1FreezeRecording      `json:"recording"`
	ExclusionRows []joinedTier1DryRunExclusionRow `json:"exclusion_rows"`
}

type joinedTier1DryRunExclusionRow struct {
	RecordingID    int64  `json:"recording_id"`
	ClipID         int64  `json:"clip_id"`
	ReasonCode     string `json:"reason_code"`
	EvidenceSHA256 string `json:"evidence_sha256"`
}

func (s *Server) handleAdminJoinedFreezeTier1DryRunStart(w http.ResponseWriter, r *http.Request) {
	if !s.joinedControlPlaneReady() {
		util.WriteError(w, http.StatusServiceUnavailable, "joined recording is disabled")
		return
	}
	var req joinedTier1FreezeRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Apply || req.ExpectedRequestSHA256 != "" {
		util.WriteError(w, http.StatusBadRequest, "dry-run start cannot apply")
		return
	}
	if err := req.validate(); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	progress, err := s.startJoinedTier1DryRun(r.Context(), req)
	if err != nil {
		util.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusAccepted, progress)
}

func (s *Server) startJoinedTier1DryRun(ctx context.Context, req joinedTier1FreezeRequest) (joinedTier1DryRunProgress, error) {
	inputBytes, err := json.Marshal(req)
	if err != nil {
		return joinedTier1DryRunProgress{}, err
	}
	inputSHA := sha256Bytes(inputBytes)
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return joinedTier1DryRunProgress{}, err
	}
	defer tx.Rollback(ctx)
	// Keep the control-plane fence bounded below the API/proxy deadline. A
	// timeout rolls back the start transaction; the operator can reconcile by
	// batch/generation without risking a half-created authority.
	if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='45s'`); err != nil {
		return joinedTier1DryRunProgress{}, err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(137,1)`); err != nil {
		return joinedTier1DryRunProgress{}, err
	}
	var existingRunID, existingInputSHA string
	err = tx.QueryRow(ctx, `SELECT id::text,input_sha256 FROM recording_joined_dry_runs
		WHERE batch_id=$1 AND generation=$2 FOR SHARE`, req.BatchID, req.Generation).Scan(&existingRunID, &existingInputSHA)
	if err == nil {
		if existingInputSHA != inputSHA {
			return joinedTier1DryRunProgress{}, errors.New("Tier-1 dry-run key already has different immutable input")
		}
		if err := tx.Commit(ctx); err != nil {
			return joinedTier1DryRunProgress{}, err
		}
		return s.joinedTier1DryRunStatus(ctx, existingRunID)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return joinedTier1DryRunProgress{}, err
	}
	tool, err := joinedrecording.InspectMediaToolEvidence(ctx)
	if err != nil {
		return joinedTier1DryRunProgress{}, fmt.Errorf("inspect joined media tool: %w", err)
	}
	plan, _, err := s.buildJoinedTier1FreezePlanWithTool(ctx, tx, req, tool, false)
	if err != nil {
		return joinedTier1DryRunProgress{}, err
	}
	recordingIDs, jobIDs := make([]int64, 0, plan.ExpectedStreamDays), make([]int64, 0, plan.ExpectedStreamDays)
	for _, recording := range plan.Recordings {
		for _, day := range recording.Qualification.Days {
			recordingIDs, jobIDs = append(recordingIDs, recording.Frozen.RecordingID), append(jobIDs, day.JobID)
		}
	}
	watermarkRows, err := tx.Query(ctx, `WITH selected AS (SELECT * FROM unnest($1::bigint[],$2::bigint[]) d(recording_id,job_id))
		SELECT d.job_id,COALESCE(max(c.id),0)::bigint FROM selected d LEFT JOIN recording_clips c
		 ON c.recording_id=d.recording_id AND c.recording_job_id=d.job_id GROUP BY d.job_id`, recordingIDs, jobIDs)
	if err != nil {
		return joinedTier1DryRunProgress{}, err
	}
	watermarks := make(map[int64]int64, len(jobIDs))
	for watermarkRows.Next() {
		var jobID, watermark int64
		if err := watermarkRows.Scan(&jobID, &watermark); err != nil {
			watermarkRows.Close()
			return joinedTier1DryRunProgress{}, err
		}
		watermarks[jobID] = watermark
	}
	if err := watermarkRows.Err(); err != nil {
		watermarkRows.Close()
		return joinedTier1DryRunProgress{}, err
	}
	watermarkRows.Close()
	if len(watermarks) != plan.ExpectedStreamDays {
		return joinedTier1DryRunProgress{}, errors.New("Tier-1 dry-run watermark authority is incomplete")
	}
	for i := range plan.Recordings {
		for _, day := range plan.Recordings[i].Qualification.Days {
			plan.Recordings[i].SnapshotDays = append(plan.Recordings[i].SnapshotDays, joinedTier1FreezeDayScope{
				LocalDate: day.LocalDate, DateOrdinal: day.QualificationWindowOrdinal,
				RecordingJobID: day.JobID, HighWaterClipID: watermarks[day.JobID],
			})
		}
	}
	skeletonBytes, err := json.Marshal(plan)
	if err != nil {
		return joinedTier1DryRunProgress{}, err
	}
	var runID, storedInputSHA, storedSkeletonSHA string
	err = tx.QueryRow(ctx, `INSERT INTO recording_joined_dry_runs(account_id,connection_id,batch_id,generation,
		qualification_run_id,input_bytes,input_sha256,skeleton_bytes,skeleton_sha256)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT(batch_id,generation) DO UPDATE SET batch_id=EXCLUDED.batch_id
		RETURNING id::text,input_sha256,skeleton_sha256`, plan.AccountID, plan.ConnectionID, plan.BatchID, plan.Generation,
		plan.SelectionAuthority.QualificationRunID, inputBytes, inputSHA, skeletonBytes, sha256Bytes(skeletonBytes)).
		Scan(&runID, &storedInputSHA, &storedSkeletonSHA)
	if err != nil {
		return joinedTier1DryRunProgress{}, err
	}
	if storedInputSHA != inputSHA || storedSkeletonSHA != sha256Bytes(skeletonBytes) {
		return joinedTier1DryRunProgress{}, errors.New("Tier-1 dry-run key already has different immutable input")
	}
	command, err := tx.Exec(ctx, `WITH selected AS (
		SELECT r.recording_id,r.priority_ordinal,d.local_date,d.date_ordinal,d.recording_job_id,d.high_water_clip_id
		FROM jsonb_array_elements(convert_from($2,'UTF8')::jsonb->'recordings') WITH ORDINALITY item(value,priority_ordinal)
		CROSS JOIN LATERAL jsonb_to_record(item.value->'frozen_recording') r(recording_id bigint,priority_ordinal integer)
		CROSS JOIN LATERAL jsonb_to_recordset(item.value->'snapshot_days')
		  d(local_date date,date_ordinal integer,recording_job_id bigint,high_water_clip_id bigint)
	)
	INSERT INTO recording_joined_dry_run_scopes(dry_run_id,recording_id,priority_ordinal,local_date,date_ordinal,
		recording_job_id,high_water_clip_id)
	SELECT $1::uuid,recording_id,priority_ordinal,local_date,date_ordinal,recording_job_id,high_water_clip_id FROM selected
	ON CONFLICT DO NOTHING`, runID, skeletonBytes)
	if err != nil {
		return joinedTier1DryRunProgress{}, err
	}
	if command.RowsAffected() != 462 && command.RowsAffected() != 0 {
		return joinedTier1DryRunProgress{}, fmt.Errorf("create dry-run scopes: rows=%d", command.RowsAffected())
	}
	if err := tx.Commit(ctx); err != nil {
		return joinedTier1DryRunProgress{}, err
	}
	return s.joinedTier1DryRunStatus(ctx, runID)
}

func (s *Server) handleAdminJoinedFreezeTier1DryRunStep(w http.ResponseWriter, r *http.Request) {
	if !s.joinedControlPlaneReady() {
		util.WriteError(w, http.StatusServiceUnavailable, "joined recording is disabled")
		return
	}
	var req joinedTier1DryRunStepRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if _, err := uuid.Parse(req.RunID); err != nil || req.PriorityOrdinal < 1 || req.PriorityOrdinal > 33 {
		util.WriteError(w, http.StatusBadRequest, "invalid Tier-1 dry-run step")
		return
	}
	progress, err := s.stepJoinedTier1DryRun(r.Context(), req)
	if err != nil {
		util.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusOK, progress)
}

func (s *Server) stepJoinedTier1DryRun(ctx context.Context, req joinedTier1DryRunStepRequest) (joinedTier1DryRunProgress, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return joinedTier1DryRunProgress{}, err
	}
	defer tx.Rollback(ctx)
	// A single recording is intentionally bounded below the 55-second client
	// and platform request budgets. Failure rolls back this ordinal only.
	if _, err := tx.Exec(ctx, `SET LOCAL statement_timeout='45s'`); err != nil {
		return joinedTier1DryRunProgress{}, err
	}
	var skeletonBytes []byte
	var state string
	var completed int
	if err := tx.QueryRow(ctx, `SELECT skeleton_bytes,state,completed_recordings FROM recording_joined_dry_runs
		WHERE id=$1 FOR UPDATE`, req.RunID).Scan(&skeletonBytes, &state, &completed); err != nil {
		return joinedTier1DryRunProgress{}, err
	}
	if state == "ready" {
		if err := tx.Commit(ctx); err != nil {
			return joinedTier1DryRunProgress{}, err
		}
		return s.joinedTier1DryRunStatus(ctx, req.RunID)
	}
	if state == "invalidated" {
		return joinedTier1DryRunProgress{}, errors.New("Tier-1 dry-run is invalidated and remains retention protected")
	}
	if req.PriorityOrdinal != completed+1 {
		if req.PriorityOrdinal <= completed {
			if err := tx.Commit(ctx); err != nil {
				return joinedTier1DryRunProgress{}, err
			}
			return s.joinedTier1DryRunStatus(ctx, req.RunID)
		}
		return joinedTier1DryRunProgress{}, errors.New("Tier-1 dry-run step is not the next ordinal")
	}
	var skeleton joinedTier1FreezePlan
	if err := json.Unmarshal(skeletonBytes, &skeleton); err != nil {
		return joinedTier1DryRunProgress{}, err
	}
	mini := skeleton
	mini.Recordings = []joinedTier1FreezeRecording{skeleton.Recordings[req.PriorityOrdinal-1]}
	mini.ExpectedStreamDays = 14
	mini.Recordings[0].SnapshotDays = nil
	mini.Recordings[0].ExpectedSourceClips = 0
	mini.Recordings[0].ExpectedSourceBytes = 0
	mini.Recordings[0].ExpectedExclusions = 0
	mini.Recordings[0].ExpectedExclusionsSHA256 = ""
	watermarks := make(map[int64]int64, 14)
	rows, err := tx.Query(ctx, `SELECT recording_job_id,high_water_clip_id FROM recording_joined_dry_run_scopes
		WHERE dry_run_id=$1 AND priority_ordinal=$2 ORDER BY date_ordinal`, req.RunID, req.PriorityOrdinal)
	if err != nil {
		return joinedTier1DryRunProgress{}, err
	}
	for rows.Next() {
		var jobID, watermark int64
		if err := rows.Scan(&jobID, &watermark); err != nil {
			rows.Close()
			return joinedTier1DryRunProgress{}, err
		}
		watermarks[jobID] = watermark
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return joinedTier1DryRunProgress{}, err
	}
	rows.Close()
	if len(watermarks) != 14 {
		return joinedTier1DryRunProgress{}, errors.New("Tier-1 dry-run scope is incomplete")
	}
	if _, err := populateJoinedTier1FrozenEvidenceWithWatermarks(ctx, tx, &mini, false, watermarks); err != nil {
		return joinedTier1DryRunProgress{}, err
	}
	evidence := joinedTier1DryRunRecordingEvidence{Recording: mini.Recordings[0]}
	qualificationBytes, err := json.Marshal(mini.Recordings[0].Qualification)
	if err != nil {
		return joinedTier1DryRunProgress{}, err
	}
	exclusionScopeSQL := `
		SELECT s.recording_id,s.recording_job_id job_id,s.high_water_clip_id,
		 (q->>'window_start')::timestamptz window_start,(q->>'window_end')::timestamptz window_end,$4::timestamptz cutoff
		FROM recording_joined_dry_run_scopes s
		JOIN LATERAL jsonb_array_elements($3::jsonb->'days') q ON (q->>'job_id')::bigint=s.recording_job_id
		WHERE s.dry_run_id=$1 AND s.priority_ordinal=$2`
	exclusionRows, err := tx.Query(ctx, joinedTier1ExclusionQuery(exclusionScopeSQL, `
		SELECT recording_id,clip_id,reason_code,evidence_sha256
		FROM evidence ORDER BY recording_id,clip_id,reason_code,evidence_sha256`), req.RunID, req.PriorityOrdinal,
		qualificationBytes, skeleton.SelectionAuthority.Cutoff)
	if err != nil {
		return joinedTier1DryRunProgress{}, err
	}
	for exclusionRows.Next() {
		var row joinedTier1DryRunExclusionRow
		if err := exclusionRows.Scan(&row.RecordingID, &row.ClipID, &row.ReasonCode, &row.EvidenceSHA256); err != nil {
			exclusionRows.Close()
			return joinedTier1DryRunProgress{}, err
		}
		evidence.ExclusionRows = append(evidence.ExclusionRows, row)
	}
	if err := exclusionRows.Err(); err != nil {
		exclusionRows.Close()
		return joinedTier1DryRunProgress{}, err
	}
	exclusionRows.Close()
	evidenceBytes, err := json.Marshal(evidence)
	if err != nil {
		return joinedTier1DryRunProgress{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO recording_joined_dry_run_recordings(dry_run_id,priority_ordinal,recording_id,
		evidence_bytes,evidence_sha256,source_clips,source_bytes,exclusions,exclusions_sha256)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, req.RunID, req.PriorityOrdinal, evidence.Recording.Frozen.RecordingID,
		evidenceBytes, sha256Bytes(evidenceBytes), evidence.Recording.ExpectedSourceClips,
		evidence.Recording.ExpectedSourceBytes, evidence.Recording.ExpectedExclusions, evidence.Recording.ExpectedExclusionsSHA256)
	if err != nil {
		return joinedTier1DryRunProgress{}, err
	}
	if req.PriorityOrdinal == len(joinedrecording.Tier1RecordingIDs) {
		if err := finalizeJoinedTier1DryRun(ctx, tx, req.RunID, &skeleton); err != nil {
			return joinedTier1DryRunProgress{}, err
		}
	} else if _, err := tx.Exec(ctx, `UPDATE recording_joined_dry_runs SET completed_recordings=$2 WHERE id=$1`, req.RunID, req.PriorityOrdinal); err != nil {
		return joinedTier1DryRunProgress{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return joinedTier1DryRunProgress{}, err
	}
	return s.joinedTier1DryRunStatus(ctx, req.RunID)
}

func finalizeJoinedTier1DryRun(ctx context.Context, tx pgx.Tx, runID string, plan *joinedTier1FreezePlan) error {
	rows, err := tx.Query(ctx, `SELECT evidence_bytes FROM recording_joined_dry_run_recordings WHERE dry_run_id=$1 ORDER BY priority_ordinal`, runID)
	if err != nil {
		return err
	}
	var recordings []joinedTier1FreezeRecording
	var exclusionRows []joinedTier1DryRunExclusionRow
	var clips, sourceBytes, exclusions int64
	for rows.Next() {
		var raw []byte
		var evidence joinedTier1DryRunRecordingEvidence
		if err := rows.Scan(&raw); err != nil {
			rows.Close()
			return err
		}
		if err := json.Unmarshal(raw, &evidence); err != nil {
			rows.Close()
			return err
		}
		recordings = append(recordings, evidence.Recording)
		exclusionRows = append(exclusionRows, evidence.ExclusionRows...)
		clips += evidence.Recording.ExpectedSourceClips
		sourceBytes += evidence.Recording.ExpectedSourceBytes
		exclusions += evidence.Recording.ExpectedExclusions
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	if len(recordings) != len(joinedrecording.Tier1RecordingIDs) {
		return errors.New("Tier-1 dry-run recordings are incomplete")
	}
	plan.Recordings = recordings
	plan.ProvisionalSourceClips = clips
	plan.ProvisionalSourceBytes = sourceBytes
	plan.ProvisionalExclusions = exclusions
	sort.Slice(exclusionRows, func(i, j int) bool {
		a, b := exclusionRows[i], exclusionRows[j]
		if a.RecordingID != b.RecordingID {
			return a.RecordingID < b.RecordingID
		}
		if a.ClipID != b.ClipID {
			return a.ClipID < b.ClipID
		}
		if a.ReasonCode != b.ReasonCode {
			return a.ReasonCode < b.ReasonCode
		}
		return a.EvidenceSHA256 < b.EvidenceSHA256
	})
	var exclusionCanonical strings.Builder
	// Keep the literal backslash-n separators identical to the SQL
	// canonicalization; this is load-bearing for exclusion-hash parity.
	for _, row := range exclusionRows {
		fmt.Fprintf(&exclusionCanonical, "%d\\n%d\\n%s\\n%s\\n", row.RecordingID, row.ClipID, row.ReasonCode, row.EvidenceSHA256)
	}
	plan.FreezeExclusionsSHA256 = sha256Bytes([]byte(exclusionCanonical.String()))
	frozen := make([]joinedrecording.FrozenRecording, len(recordings))
	days := make([]joinedrecording.FrozenDenominatorDayProjection, 0, 462)
	for i, recording := range recordings {
		frozen[i] = recording.Frozen
		for _, day := range recording.SnapshotDays {
			days = append(days, joinedrecording.FrozenDenominatorDayProjection{RecordingID: recording.Frozen.RecordingID, LocalDate: day.LocalDate, QualificationSHA256: recording.Frozen.QualificationSHA256, FrozenSourceSHA256: day.ExpectedSourceSHA256, SourceCount: day.ExpectedSourceClips, SourceBytes: day.ExpectedSourceBytes})
		}
	}
	plan.FrozenDenominatorSHA256, err = joinedrecording.ComputeFrozenDenominatorSHA256(plan.SelectionAuthority, frozen, days)
	if err != nil {
		return err
	}
	sealed, raw, err := sealJoinedTier1FreezePlan(*plan)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE recording_joined_dry_runs SET state='ready',completed_recordings=33,final_plan_bytes=$2,
		final_plan_sha256=$3,ready_at=clock_timestamp() WHERE id=$1 AND state='building'`, runID, raw, sealed.RequestSHA256)
	return err
}

func (s *Server) handleAdminJoinedFreezeTier1DryRunStatus(w http.ResponseWriter, r *http.Request) {
	if !s.joinedControlPlaneReady() {
		util.WriteError(w, http.StatusServiceUnavailable, "joined recording is disabled")
		return
	}
	runID := r.URL.Query().Get("run_id")
	if runID == "" {
		batchID := r.URL.Query().Get("batch_id")
		generation, parseErr := strconv.Atoi(r.URL.Query().Get("generation"))
		if !joinedBatchIDPattern.MatchString(batchID) || parseErr != nil || generation <= 0 {
			util.WriteError(w, http.StatusBadRequest, "invalid Tier-1 dry-run key")
			return
		}
		if err := s.pool.QueryRow(r.Context(), `SELECT id::text FROM recording_joined_dry_runs WHERE batch_id=$1 AND generation=$2`,
			batchID, generation).Scan(&runID); err != nil {
			util.WriteError(w, http.StatusNotFound, "Tier-1 dry-run not found")
			return
		}
	} else if _, err := uuid.Parse(runID); err != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid Tier-1 dry-run id")
		return
	}
	progress, err := s.joinedTier1DryRunStatus(r.Context(), runID)
	if err != nil {
		util.WriteError(w, http.StatusNotFound, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusOK, progress)
}

func (s *Server) handleAdminJoinedFreezeTier1DryRunInvalidate(w http.ResponseWriter, r *http.Request) {
	if !s.joinedControlPlaneReady() {
		util.WriteError(w, http.StatusServiceUnavailable, "joined recording is disabled")
		return
	}
	var req struct {
		RunID string `json:"run_id"`
	}
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if _, err := uuid.Parse(req.RunID); err != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid Tier-1 dry-run id")
		return
	}
	command, err := s.pool.Exec(r.Context(), `UPDATE recording_joined_dry_runs SET state='invalidated',invalidated_at=clock_timestamp()
		WHERE id=$1 AND state='building'`, req.RunID)
	if err != nil {
		util.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	if command.RowsAffected() != 1 {
		util.WriteError(w, http.StatusConflict, "Tier-1 dry-run cannot be invalidated")
		return
	}
	progress, err := s.joinedTier1DryRunStatus(r.Context(), req.RunID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusOK, progress)
}

func (s *Server) joinedTier1DryRunStatus(ctx context.Context, runID string) (joinedTier1DryRunProgress, error) {
	var p joinedTier1DryRunProgress
	var sha *string
	if err := s.pool.QueryRow(ctx, `SELECT id::text,state,completed_recordings,final_plan_sha256 FROM recording_joined_dry_runs WHERE id=$1`, runID).
		Scan(&p.RunID, &p.State, &p.CompletedRecordings, &sha); err != nil {
		return p, err
	}
	p.ExpectedRecordings = len(joinedrecording.Tier1RecordingIDs)
	p.RequestSHA256 = sha
	if p.State == "building" {
		next := p.CompletedRecordings + 1
		p.NextPriorityOrdinal = &next
	}
	return p, nil
}
