package api

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/daydemir/stoarama/backend/internal/util"
)

type joinedFinalFreezeRequest struct {
	ProtocolVersion                 int    `json:"protocol_version"`
	BatchID                         string `json:"batch_id"`
	ExpectedFrozenDenominatorSHA256 string `json:"expected_frozen_denominator_sha256"`
}

type joinedFinalFreezeResponse struct {
	ProtocolVersion         int       `json:"protocol_version"`
	BatchID                 string    `json:"batch_id"`
	State                   string    `json:"state"`
	FrozenAt                time.Time `json:"frozen_at"`
	FrozenDenominatorSHA256 string    `json:"frozen_denominator_sha256"`
	RecordingCount          int       `json:"recording_count"`
	StreamDayCount          int       `json:"stream_day_count"`
	ScheduledHourCount      int       `json:"scheduled_hour_count"`
	AlreadyFrozen           bool      `json:"already_frozen"`
}

const (
	joinedFinalFreezeOperationTimeout = 10 * time.Second
	joinedFinalFreezeLockTimeout      = 5 * time.Second
	joinedFinalFreezeStatementTimeout = 5 * time.Second
)

func (r joinedFinalFreezeRequest) validate() error {
	if r.ProtocolVersion != joinedrecording.JoinedProtocolVersion || !joinedBatchIDPattern.MatchString(r.BatchID) ||
		!lowerHexSHA256(r.ExpectedFrozenDenominatorSHA256) {
		return errors.New("invalid joined final-freeze request")
	}
	return nil
}

func (s *Server) handleAdminJoinedFinalFreeze(w http.ResponseWriter, r *http.Request) {
	if !s.joinedControlPlaneReady() {
		util.WriteError(w, http.StatusServiceUnavailable, "joined recording is disabled")
		return
	}
	var req joinedFinalFreezeRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := req.validate(); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	response, err := s.finalFreezeJoinedBatch(r.Context(), req)
	if err != nil {
		util.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusOK, response)
}

func (s *Server) finalFreezeJoinedBatch(ctx context.Context, req joinedFinalFreezeRequest) (joinedFinalFreezeResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, joinedFinalFreezeOperationTimeout)
	defer cancel()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return joinedFinalFreezeResponse{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT set_config('lock_timeout',$1,true),set_config('statement_timeout',$2,true)`,
		joinedFinalFreezeLockTimeout.String(), joinedFinalFreezeStatementTimeout.String()); err != nil {
		return joinedFinalFreezeResponse{}, fmt.Errorf("bound joined final freeze: %w", err)
	}

	response := joinedFinalFreezeResponse{ProtocolVersion: joinedrecording.JoinedProtocolVersion, BatchID: req.BatchID}
	var batchRecordID, connectionID int64
	var frozenAt, freezeStartedAt sql.NullTime
	var authority joinedrecording.SelectionAuthority
	err = tx.QueryRow(ctx, `SELECT b.id,b.connection_id,b.state,b.frozen_at,b.freeze_started_at,b.frozen_denominator_sha256,
		b.expected_recordings,b.expected_stream_days,b.expected_scheduled_hours,b.selection_basis,
		b.ordered_recording_ids_sha256,b.eligibility_cutoff,b.qualification_run_id,b.qualification_frozen_at,
		q.definition_version,b.qualification_cohort_sha256,b.qualification_windows_sha256,
		b.selected_qualification_windows_sha256
		FROM recording_joined_batches b
		JOIN connections c ON c.id=b.connection_id AND c.joined_protocol_version=1
		JOIN recording_qualification_runs q ON q.id=b.qualification_run_id AND q.account_id=b.account_id
		WHERE b.batch_id=$1 FOR UPDATE OF b FOR SHARE OF q`, req.BatchID).Scan(&batchRecordID, &connectionID, &response.State,
		&frozenAt, &freezeStartedAt, &response.FrozenDenominatorSHA256, &response.RecordingCount,
		&response.StreamDayCount, &response.ScheduledHourCount, &authority.SelectionBasis,
		&authority.OrderedRecordingIDSHA256, &authority.Cutoff, &authority.QualificationRunID,
		&authority.QualificationRunFrozenAt, &authority.QualificationRuleVersion,
		&authority.QualificationCohortSHA256, &authority.QualificationWindowsSHA256,
		&authority.SelectedQualificationWindowsSHA256)
	if err != nil {
		return response, fmt.Errorf("lock joined batch for final freeze: %w", err)
	}
	if response.FrozenDenominatorSHA256 != req.ExpectedFrozenDenominatorSHA256 {
		return response, errors.New("joined final-freeze denominator differs")
	}
	if joinedBatchHasFinalFreeze(response.State) {
		if !frozenAt.Valid {
			return response, errors.New("joined frozen batch lacks frozen time")
		}
		response.FrozenAt, response.AlreadyFrozen = frozenAt.Time.UTC(), true
		if err := tx.Commit(ctx); err != nil {
			return response, err
		}
		return response, nil
	}
	if response.State != "building" || frozenAt.Valid || freezeStartedAt.Valid {
		return response, errors.New("joined batch is not ready for final freeze")
	}
	command, err := tx.Exec(ctx, `UPDATE recording_joined_batches SET freeze_started_at=clock_timestamp()
		WHERE id=$1 AND state='building' AND freeze_started_at IS NULL`, batchRecordID)
	if err != nil {
		return response, fmt.Errorf("start joined final freeze: %w", err)
	}
	if command.RowsAffected() != 1 {
		return response, fmt.Errorf("start joined final freeze: rows=%d", command.RowsAffected())
	}

	recordings, err := loadJoinedFinalFreezeRecordings(ctx, tx, batchRecordID)
	if err != nil {
		return response, err
	}
	days, err := loadJoinedFinalFreezeDays(ctx, tx, batchRecordID)
	if err != nil {
		return response, err
	}
	authority.Cutoff, authority.QualificationRunFrozenAt = authority.Cutoff.UTC(), authority.QualificationRunFrozenAt.UTC()
	denominator, err := joinedrecording.ComputeFrozenDenominatorSHA256(authority, recordings, days)
	if err != nil || denominator != response.FrozenDenominatorSHA256 || len(recordings) != response.RecordingCount ||
		len(days) != response.StreamDayCount || response.ScheduledHourCount != response.StreamDayCount*12 {
		return response, errors.New("joined final-freeze evidence differs")
	}
	var protocolVersion int
	if err := tx.QueryRow(ctx, `SELECT joined_protocol_version FROM connections WHERE id=$1
		AND joined_protocol_version=1 FOR SHARE`, connectionID).Scan(&protocolVersion); err != nil ||
		protocolVersion != joinedrecording.JoinedProtocolVersion {
		return response, errors.New("joined connection protocol changed before final freeze")
	}
	if err := tx.QueryRow(ctx, `UPDATE recording_joined_batches SET state='frozen',frozen_at=clock_timestamp()
		WHERE id=$1 AND state='building' AND freeze_started_at IS NOT NULL RETURNING frozen_at`, batchRecordID).
		Scan(&response.FrozenAt); err != nil {
		return response, fmt.Errorf("finish joined final freeze: %w", err)
	}
	response.FrozenAt, response.State = response.FrozenAt.UTC(), "frozen"
	if err := tx.Commit(ctx); err != nil {
		return response, err
	}
	return response, nil
}

func joinedBatchHasFinalFreeze(state string) bool {
	switch state {
	case "frozen", "index_sealed", "published":
		return true
	default:
		return false
	}
}

func loadJoinedFinalFreezeRecordings(ctx context.Context, tx pgx.Tx, batchRecordID int64) ([]joinedrecording.FrozenRecording, error) {
	rows, err := tx.Query(ctx, `SELECT recording_id,priority_ordinal,selection_tier,qualification_sha256,completed_at
		FROM recording_joined_batch_recordings WHERE batch_record_id=$1 ORDER BY priority_ordinal`, batchRecordID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var recordings []joinedrecording.FrozenRecording
	for rows.Next() {
		var recording joinedrecording.FrozenRecording
		if err := rows.Scan(&recording.RecordingID, &recording.PriorityOrdinal, &recording.SelectionTier,
			&recording.QualificationSHA256, &recording.CompletedAt); err != nil {
			return nil, err
		}
		recording.CompletedAt = recording.CompletedAt.UTC()
		recordings = append(recordings, recording)
	}
	return recordings, rows.Err()
}

func loadJoinedFinalFreezeDays(ctx context.Context, tx pgx.Tx, batchRecordID int64) ([]joinedrecording.FrozenDenominatorDayProjection, error) {
	rows, err := tx.Query(ctx, `SELECT d.recording_id,d.local_date::text,br.qualification_sha256,
		d.source_snapshot_sha256,d.source_clip_count,d.source_bytes
		FROM recording_joined_stream_days d
		JOIN recording_joined_batch_recordings br ON br.id=d.batch_recording_id
		WHERE d.batch_record_id=$1 AND d.state='sealed'
		ORDER BY br.priority_ordinal,d.date_ordinal`, batchRecordID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var days []joinedrecording.FrozenDenominatorDayProjection
	for rows.Next() {
		var day joinedrecording.FrozenDenominatorDayProjection
		if err := rows.Scan(&day.RecordingID, &day.LocalDate, &day.QualificationSHA256, &day.FrozenSourceSHA256,
			&day.SourceCount, &day.SourceBytes); err != nil {
			return nil, err
		}
		days = append(days, day)
	}
	return days, rows.Err()
}
