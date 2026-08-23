package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/daydemir/stoarama/backend/internal/util"
)

// A preflight step is deliberately bounded below the API/proxy deadline. The
// step persists no row until every HEAD in its range has passed exact identity
// checks, so a failed request can be retried without partial evidence.
const joinedStreamDayHeadPreflightMaxBatch = 16

type joinedStreamDayHeadPreflightStartRequest struct {
	ProtocolVersion int    `json:"protocol_version"`
	BatchID         string `json:"batch_id"`
	RecordingID     int64  `json:"recording_id"`
	LocalDate       string `json:"local_date"`
}

type joinedStreamDayHeadPreflightStepRequest struct {
	RunID string `json:"run_id"`
	Limit int    `json:"limit,omitempty"`
}

type joinedStreamDayHeadPreflightSealRequest struct {
	RunID string `json:"run_id"`
}

type joinedStreamDayHeadPreflightProgress struct {
	RunID                 string `json:"run_id"`
	BatchID               string `json:"batch_id"`
	RecordingID           int64  `json:"recording_id"`
	LocalDate             string `json:"local_date"`
	State                 string `json:"state"`
	CompletedObservations int    `json:"completed_observations"`
	ExpectedObservations  int    `json:"expected_observations"`
	NextDayOrdinal        *int   `json:"next_day_ordinal,omitempty"`
	SourceSnapshotSHA256  string `json:"source_snapshot_sha256"`
	ExpectedSourceBytes   int64  `json:"expected_source_bytes"`
}

type joinedStreamDayHeadPreflightRun struct {
	Progress    joinedStreamDayHeadPreflightProgress
	ID          uuid.UUID
	StreamDayID int64
}

func (s *Server) joinedStreamDayHeadPreflightExists(ctx context.Context, req joinedSealStreamDayRequest) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM recording_joined_stream_day_head_preflights
		WHERE batch_id=$1 AND recording_id=$2 AND local_date=$3::date)`, req.BatchID, req.RecordingID, req.LocalDate).Scan(&exists)
	return exists, err
}

func (r joinedStreamDayHeadPreflightStartRequest) sealRequest() joinedSealStreamDayRequest {
	return joinedSealStreamDayRequest{ProtocolVersion: r.ProtocolVersion, BatchID: r.BatchID,
		RecordingID: r.RecordingID, LocalDate: r.LocalDate}
}

func (r joinedStreamDayHeadPreflightStartRequest) validate() error {
	return r.sealRequest().validate()
}

func (r joinedStreamDayHeadPreflightStepRequest) validate() (uuid.UUID, int, error) {
	id, err := uuid.Parse(r.RunID)
	if err != nil {
		return uuid.Nil, 0, errors.New("invalid joined HEAD preflight run")
	}
	limit := r.Limit
	if limit == 0 {
		limit = joinedStreamDayHeadPreflightMaxBatch
	}
	if limit < 1 || limit > joinedStreamDayHeadPreflightMaxBatch {
		return uuid.Nil, 0, fmt.Errorf("joined HEAD preflight batch must be between 1 and %d", joinedStreamDayHeadPreflightMaxBatch)
	}
	return id, limit, nil
}

func (r joinedStreamDayHeadPreflightSealRequest) validate() (uuid.UUID, error) {
	id, err := uuid.Parse(r.RunID)
	if err != nil {
		return uuid.Nil, errors.New("invalid joined HEAD preflight run")
	}
	return id, nil
}

func (s *Server) handleAdminJoinedStreamDayHeadPreflightStart(w http.ResponseWriter, r *http.Request) {
	if !s.joinedControlPlaneReady() {
		util.WriteError(w, http.StatusServiceUnavailable, "joined recording is disabled")
		return
	}
	var req joinedStreamDayHeadPreflightStartRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := req.validate(); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	progress, err := s.startJoinedStreamDayHeadPreflight(r.Context(), req)
	if err != nil {
		util.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusAccepted, progress)
}

func (s *Server) handleAdminJoinedStreamDayHeadPreflightStep(w http.ResponseWriter, r *http.Request) {
	if !s.joinedControlPlaneReady() {
		util.WriteError(w, http.StatusServiceUnavailable, "joined recording is disabled")
		return
	}
	var req joinedStreamDayHeadPreflightStepRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	runID, limit, err := req.validate()
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	progress, err := s.stepJoinedStreamDayHeadPreflight(r.Context(), runID, limit)
	if err != nil {
		util.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusOK, progress)
}

func (s *Server) handleAdminJoinedStreamDayHeadPreflightSeal(w http.ResponseWriter, r *http.Request) {
	if !s.joinedControlPlaneReady() {
		util.WriteError(w, http.StatusServiceUnavailable, "joined recording is disabled")
		return
	}
	var req joinedStreamDayHeadPreflightSealRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	runID, err := req.validate()
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	response, err := s.sealJoinedStreamDayHeadPreflight(r.Context(), runID)
	if err != nil {
		util.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusOK, response)
}

func (s *Server) startJoinedStreamDayHeadPreflight(ctx context.Context,
	req joinedStreamDayHeadPreflightStartRequest) (joinedStreamDayHeadPreflightProgress, error) {
	sealReq := req.sealRequest()
	if existing, found, err := s.loadJoinedStreamDayHeadPreflightByRequest(ctx, sealReq); err != nil {
		return joinedStreamDayHeadPreflightProgress{}, err
	} else if found {
		return existing.progress, nil
	}
	plan, err := s.loadPendingJoinedStreamDay(ctx, sealReq)
	if err != nil {
		return joinedStreamDayHeadPreflightProgress{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return joinedStreamDayHeadPreflightProgress{}, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='5s'`); err != nil {
		return joinedStreamDayHeadPreflightProgress{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO recording_joined_stream_day_head_preflights
		(batch_record_id,stream_day_id,account_id,connection_id,batch_id,recording_id,local_date,
		 source_snapshot_sha256,expected_source_count,expected_source_bytes)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (stream_day_id) DO NOTHING`, plan.BatchRecordID, plan.StreamDayID, plan.AccountID,
		plan.ConnectionID, plan.BatchID, plan.RecordingID, plan.LocalDate, plan.SourceSnapshotSHA,
		len(plan.Sources), planSourceBytes(plan))
	if err != nil {
		return joinedStreamDayHeadPreflightProgress{}, fmt.Errorf("create joined HEAD preflight: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return joinedStreamDayHeadPreflightProgress{}, err
	}
	run, found, err := s.loadJoinedStreamDayHeadPreflightByRequest(ctx, sealReq)
	if err != nil || !found {
		if err == nil {
			err = errors.New("joined HEAD preflight was not persisted")
		}
		return joinedStreamDayHeadPreflightProgress{}, err
	}
	return run.progress, nil
}

func planSourceBytes(plan joinedStreamDayPlan) int64 {
	var total int64
	for _, source := range plan.Sources {
		total += source.Source.Object.SizeBytes
	}
	return total
}

type joinedStreamDayHeadPreflightLookup struct {
	progress joinedStreamDayHeadPreflightProgress
	found    bool
}

func (s *Server) loadJoinedStreamDayHeadPreflightByRequest(ctx context.Context,
	req joinedSealStreamDayRequest) (joinedStreamDayHeadPreflightLookup, bool, error) {
	var run joinedStreamDayHeadPreflightLookup
	var next *int
	var runID string
	err := s.pool.QueryRow(ctx, `SELECT p.id::text,p.batch_id,p.recording_id,p.local_date::text,p.state,
		p.source_snapshot_sha256,p.expected_source_count,p.expected_source_bytes,
		(SELECT count(*)::int FROM recording_joined_stream_day_head_observations o WHERE o.preflight_id=p.id),
		(SELECT min(s.day_ordinal)::int FROM recording_joined_source_snapshots s
		 LEFT JOIN recording_joined_stream_day_head_observations o ON o.preflight_id=p.id AND o.source_snapshot_id=s.id
		 WHERE s.stream_day_id=p.stream_day_id AND o.id IS NULL)
		FROM recording_joined_stream_day_head_preflights p
		WHERE p.batch_id=$1 AND p.recording_id=$2 AND p.local_date=$3::date`, req.BatchID, req.RecordingID, req.LocalDate).Scan(
		&runID, &run.progress.BatchID, &run.progress.RecordingID, &run.progress.LocalDate, &run.progress.State,
		&run.progress.SourceSnapshotSHA256, &run.progress.ExpectedObservations, &run.progress.ExpectedSourceBytes,
		&run.progress.CompletedObservations, &next)
	if errors.Is(err, pgx.ErrNoRows) {
		return run, false, nil
	}
	if err != nil {
		return run, false, fmt.Errorf("load joined HEAD preflight: %w", err)
	}
	_, err = uuid.Parse(runID)
	if err != nil {
		return run, false, err
	}
	run.progress.RunID = runID
	if next != nil {
		run.progress.NextDayOrdinal = next
	}
	return run, true, nil
}

func (s *Server) loadJoinedStreamDayHeadPreflight(ctx context.Context, id uuid.UUID) (joinedStreamDayHeadPreflightRun, error) {
	var run joinedStreamDayHeadPreflightRun
	var next *int
	var runID string
	err := s.pool.QueryRow(ctx, `SELECT p.id::text,p.stream_day_id,p.batch_id,p.recording_id,p.local_date::text,p.state,
		p.source_snapshot_sha256,p.expected_source_count,p.expected_source_bytes,
		(SELECT count(*)::int FROM recording_joined_stream_day_head_observations o WHERE o.preflight_id=p.id),
		(SELECT min(s.day_ordinal)::int FROM recording_joined_source_snapshots s
		 LEFT JOIN recording_joined_stream_day_head_observations o ON o.preflight_id=p.id AND o.source_snapshot_id=s.id
		 WHERE s.stream_day_id=p.stream_day_id AND o.id IS NULL)
		FROM recording_joined_stream_day_head_preflights p WHERE p.id=$1`, id).Scan(
		&runID, &run.StreamDayID, &run.Progress.BatchID, &run.Progress.RecordingID, &run.Progress.LocalDate,
		&run.Progress.State, &run.Progress.SourceSnapshotSHA256, &run.Progress.ExpectedObservations,
		&run.Progress.ExpectedSourceBytes, &run.Progress.CompletedObservations, &next)
	if err != nil {
		return run, fmt.Errorf("load joined HEAD preflight: %w", err)
	}
	parsed, err := uuid.Parse(runID)
	if err != nil {
		return run, err
	}
	run.ID, run.Progress.RunID = parsed, runID
	run.Progress.NextDayOrdinal = next
	return run, nil
}

func (s *Server) stepJoinedStreamDayHeadPreflight(ctx context.Context, runID uuid.UUID, limit int) (joinedStreamDayHeadPreflightProgress, error) {
	run, err := s.loadJoinedStreamDayHeadPreflight(ctx, runID)
	if err != nil {
		return joinedStreamDayHeadPreflightProgress{}, err
	}
	if run.Progress.State == "sealed" || run.Progress.State == "ready" {
		return run.Progress, nil
	}
	req := joinedSealStreamDayRequest{ProtocolVersion: joinedrecording.JoinedProtocolVersion,
		BatchID: run.Progress.BatchID, RecordingID: run.Progress.RecordingID, LocalDate: run.Progress.LocalDate}
	release, err := s.acquireJoinedStreamDayHeadLock(ctx, req.BatchID)
	if err != nil {
		return joinedStreamDayHeadPreflightProgress{}, err
	}
	defer release()
	plan, err := s.loadPendingJoinedStreamDay(ctx, req)
	if err != nil {
		return joinedStreamDayHeadPreflightProgress{}, err
	}
	if plan.SourceSnapshotSHA != run.Progress.SourceSnapshotSHA256 || len(plan.Sources) != run.Progress.ExpectedObservations ||
		planSourceBytes(plan) != run.Progress.ExpectedSourceBytes {
		return joinedStreamDayHeadPreflightProgress{}, errors.New("joined HEAD preflight source authority differs")
	}
	snapshots, err := s.nextJoinedStreamDayHeadPreflightSnapshots(ctx, runID, plan.StreamDayID, limit)
	if err != nil {
		return joinedStreamDayHeadPreflightProgress{}, err
	}
	if len(snapshots) == 0 {
		return s.markJoinedStreamDayHeadPreflightReady(ctx, runID)
	}
	observations, err := s.headJoinedStreamDaySources(ctx, joinedStreamDayPlan{Sources: snapshots})
	if err != nil {
		return joinedStreamDayHeadPreflightProgress{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return joinedStreamDayHeadPreflightProgress{}, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='5s'`); err != nil {
		return joinedStreamDayHeadPreflightProgress{}, err
	}
	var state, sourceSHA string
	var expectedCount int
	if err := tx.QueryRow(ctx, `SELECT state,source_snapshot_sha256,expected_source_count
		FROM recording_joined_stream_day_head_preflights WHERE id=$1 FOR UPDATE`, runID).Scan(&state, &sourceSHA, &expectedCount); err != nil {
		return joinedStreamDayHeadPreflightProgress{}, err
	}
	if state != "building" || sourceSHA != plan.SourceSnapshotSHA || expectedCount != len(plan.Sources) {
		return joinedStreamDayHeadPreflightProgress{}, errors.New("joined HEAD preflight changed during step")
	}
	for _, observation := range observations {
		if _, err := tx.Exec(ctx, `INSERT INTO recording_joined_stream_day_head_observations
			(preflight_id,stream_day_id,source_snapshot_id,day_ordinal,clip_id,storage_destination_id,
			 provider,endpoint,region,bucket,object_key,version_id,etag,size_bytes,sha256,start_at,end_at,observed_at)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
			ON CONFLICT (preflight_id,source_snapshot_id) DO NOTHING`, runID, plan.StreamDayID,
			observation.SourceSnapshotID, sourceOrdinal(plan, observation.SourceSnapshotID), observation.ClipID,
			observation.StorageDestinationID, observation.Provider, observation.Endpoint, observation.Region,
			observation.Bucket, observation.ObjectKey, observation.VersionID, observation.ETag,
			observation.SizeBytes, observation.SHA256, observation.StartUTC, observation.EndUTC, time.Now().UTC()); err != nil {
			return joinedStreamDayHeadPreflightProgress{}, fmt.Errorf("persist joined HEAD observation: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE recording_joined_stream_day_head_preflights
		SET state='ready',ready_at=clock_timestamp() WHERE id=$1 AND state='building'
		AND (SELECT count(*) FROM recording_joined_stream_day_head_observations WHERE preflight_id=$1)=expected_source_count`, runID); err != nil {
		return joinedStreamDayHeadPreflightProgress{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return joinedStreamDayHeadPreflightProgress{}, err
	}
	finished, err := s.loadJoinedStreamDayHeadPreflight(ctx, runID)
	if err != nil {
		return joinedStreamDayHeadPreflightProgress{}, err
	}
	return finished.Progress, nil
}

func sourceOrdinal(plan joinedStreamDayPlan, snapshotID int64) int {
	for _, snapshot := range plan.Sources {
		if snapshot.ID == snapshotID {
			return snapshot.DayOrdinal
		}
	}
	return 0
}

func (s *Server) nextJoinedStreamDayHeadPreflightSnapshots(ctx context.Context, runID uuid.UUID, streamDayID int64, limit int) ([]joinedStreamDaySnapshot, error) {
	rows, err := s.pool.Query(ctx, `SELECT s.id,s.day_ordinal,s.clip_id,s.recording_id,s.recording_job_id,s.storage_destination_id,s.provider,
		s.endpoint,s.region,s.bucket,s.object_key,s.ingest_etag,s.size_bytes,s.sha256,s.start_at,s.end_at,s.clip_created_at,s.released_at,
		s.capture_lease_token::text,s.capture_sequence,s.capture_attempt_id::text,s.timestamp_contract_version,
		s.timestamp_contract,s.timestamp_contract_status,s.timestamp_contract_reason
		FROM recording_joined_source_snapshots s
		WHERE s.stream_day_id=$1 AND NOT EXISTS (SELECT 1 FROM recording_joined_stream_day_head_observations o
			WHERE o.preflight_id=$2 AND o.source_snapshot_id=s.id)
		ORDER BY s.day_ordinal LIMIT $3`, streamDayID, runID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var snapshots []joinedStreamDaySnapshot
	for rows.Next() {
		var snapshot joinedStreamDaySnapshot
		if err := rows.Scan(&snapshot.ID, &snapshot.DayOrdinal, &snapshot.Source.ClipID, &snapshot.Source.RecordingID,
			&snapshot.Source.RecordingJobID, &snapshot.Source.StorageDestinationID, &snapshot.Source.Provider,
			&snapshot.Source.Endpoint, &snapshot.Source.Region, &snapshot.Source.Bucket, &snapshot.Source.Object.Key,
			&snapshot.Source.Object.ETag, &snapshot.Source.Object.SizeBytes, &snapshot.Source.Object.SHA256,
			&snapshot.Source.StartUTC, &snapshot.Source.EndUTC, &snapshot.ClipCreatedAt, &snapshot.Source.ReleasedAt,
			&snapshot.CaptureLeaseToken, &snapshot.CaptureSequence, &snapshot.CaptureAttemptID, &snapshot.TimestampVersion,
			&snapshot.TimestampContract, &snapshot.TimestampStatus, &snapshot.TimestampReason); err != nil {
			return nil, err
		}
		snapshot.Source.StartUTC, snapshot.Source.EndUTC = snapshot.Source.StartUTC.UTC(), snapshot.Source.EndUTC.UTC()
		if snapshot.Source.ReleasedAt != nil {
			released := snapshot.Source.ReleasedAt.UTC()
			snapshot.Source.ReleasedAt = &released
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots, rows.Err()
}

func (s *Server) markJoinedStreamDayHeadPreflightReady(ctx context.Context, runID uuid.UUID) (joinedStreamDayHeadPreflightProgress, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return joinedStreamDayHeadPreflightProgress{}, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='5s'`); err != nil {
		return joinedStreamDayHeadPreflightProgress{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE recording_joined_stream_day_head_preflights
		SET state='ready',ready_at=clock_timestamp() WHERE id=$1 AND state='building'
		AND (SELECT count(*) FROM recording_joined_stream_day_head_observations WHERE preflight_id=$1)=expected_source_count`, runID); err != nil {
		return joinedStreamDayHeadPreflightProgress{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return joinedStreamDayHeadPreflightProgress{}, err
	}
	run, err := s.loadJoinedStreamDayHeadPreflight(ctx, runID)
	if err != nil {
		return joinedStreamDayHeadPreflightProgress{}, err
	}
	return run.Progress, nil
}

func (s *Server) loadJoinedStreamDayHeadPreflightObservations(ctx context.Context, runID uuid.UUID) ([]joinedStreamDayHeadObservation, error) {
	rows, err := s.pool.Query(ctx, `SELECT source_snapshot_id,clip_id,storage_destination_id,provider,endpoint,region,bucket,
		object_key,version_id,etag,size_bytes,sha256,start_at,end_at
		FROM recording_joined_stream_day_head_observations WHERE preflight_id=$1 ORDER BY day_ordinal`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var observations []joinedStreamDayHeadObservation
	for rows.Next() {
		var observation joinedStreamDayHeadObservation
		if err := rows.Scan(&observation.SourceSnapshotID, &observation.ClipID, &observation.StorageDestinationID,
			&observation.Provider, &observation.Endpoint, &observation.Region, &observation.Bucket, &observation.ObjectKey,
			&observation.VersionID, &observation.ETag, &observation.SizeBytes, &observation.SHA256,
			&observation.StartUTC, &observation.EndUTC); err != nil {
			return nil, err
		}
		observation.StartUTC, observation.EndUTC = observation.StartUTC.UTC(), observation.EndUTC.UTC()
		observations = append(observations, observation)
	}
	return observations, rows.Err()
}

func (s *Server) sealJoinedStreamDayHeadPreflight(ctx context.Context, runID uuid.UUID) (joinedSealStreamDayResponse, error) {
	run, err := s.loadJoinedStreamDayHeadPreflight(ctx, runID)
	if err != nil {
		return joinedSealStreamDayResponse{}, err
	}
	if run.Progress.State != "ready" {
		return joinedSealStreamDayResponse{}, errors.New("joined HEAD preflight is not ready")
	}
	req := joinedSealStreamDayRequest{ProtocolVersion: joinedrecording.JoinedProtocolVersion,
		BatchID: run.Progress.BatchID, RecordingID: run.Progress.RecordingID, LocalDate: run.Progress.LocalDate}
	release, err := s.acquireJoinedStreamDayHeadLock(ctx, req.BatchID)
	if err != nil {
		return joinedSealStreamDayResponse{}, err
	}
	defer release()
	plan, err := s.loadPendingJoinedStreamDay(ctx, req)
	if err != nil {
		return joinedSealStreamDayResponse{}, err
	}
	if plan.StreamDayID != run.StreamDayID || plan.SourceSnapshotSHA != run.Progress.SourceSnapshotSHA256 ||
		len(plan.Sources) != run.Progress.ExpectedObservations || planSourceBytes(plan) != run.Progress.ExpectedSourceBytes {
		return joinedSealStreamDayResponse{}, errors.New("joined HEAD preflight source authority differs")
	}
	observations, err := s.loadJoinedStreamDayHeadPreflightObservations(ctx, runID)
	if err != nil {
		return joinedSealStreamDayResponse{}, err
	}
	if len(observations) != len(plan.Sources) {
		return joinedSealStreamDayResponse{}, errors.New("joined HEAD preflight observations are incomplete")
	}
	return s.sealJoinedStreamDayWithPreflight(ctx, req, plan, observations, &runID)
}
