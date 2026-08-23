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

const joinedFinalValidationVersion = "stream-day-v1"

type joinedFinalValidationStartRequest struct {
	ProtocolVersion                 int    `json:"protocol_version"`
	BatchID                         string `json:"batch_id"`
	ExpectedFrozenDenominatorSHA256 string `json:"expected_frozen_denominator_sha256"`
}

type joinedFinalValidationStepRequest struct {
	ProtocolVersion int    `json:"protocol_version"`
	RunID           string `json:"run_id"`
	Ordinal         int    `json:"ordinal,omitempty"`
}

type joinedFinalValidationProgress struct {
	RunID                     string     `json:"run_id"`
	BatchID                   string     `json:"batch_id"`
	Generation                int        `json:"generation"`
	State                     string     `json:"state"`
	ValidatorVersion          string     `json:"validator_version"`
	ExpectedDenominatorSHA256 string     `json:"expected_frozen_denominator_sha256"`
	CompletedScopes           int        `json:"completed_scopes"`
	ExpectedScopes            int        `json:"expected_scopes"`
	NextOrdinal               *int       `json:"next_ordinal,omitempty"`
	ReceiptSetSHA256          *string    `json:"receipt_set_sha256,omitempty"`
	CompletedAt               *time.Time `json:"completed_at,omitempty"`
}

func (r joinedFinalValidationStartRequest) validate() error {
	if r.ProtocolVersion != joinedrecording.JoinedProtocolVersion || !joinedBatchIDPattern.MatchString(r.BatchID) || !lowerHexSHA256(r.ExpectedFrozenDenominatorSHA256) {
		return errors.New("invalid joined final-validation start request")
	}
	return nil
}

func (s *Server) handleAdminJoinedFinalValidationStart(w http.ResponseWriter, r *http.Request) {
	if !s.joinedControlPlaneReady() {
		util.WriteError(w, http.StatusServiceUnavailable, "joined recording is disabled")
		return
	}
	var req joinedFinalValidationStartRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := req.validate(); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	progress, err := s.startJoinedFinalValidation(r.Context(), req)
	if err != nil {
		util.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusAccepted, progress)
}

func (s *Server) startJoinedFinalValidation(ctx context.Context, req joinedFinalValidationStartRequest) (joinedFinalValidationProgress, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return joinedFinalValidationProgress{}, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='15s'`); err != nil {
		return joinedFinalValidationProgress{}, err
	}
	var batchRecordID int64
	var generation int
	var state string
	var expectedDays int
	var denominator string
	if err := tx.QueryRow(ctx, `SELECT id,generation,state,expected_stream_days,frozen_denominator_sha256
		FROM recording_joined_batches WHERE batch_id=$1 FOR UPDATE`, req.BatchID).
		Scan(&batchRecordID, &generation, &state, &expectedDays, &denominator); err != nil {
		return joinedFinalValidationProgress{}, fmt.Errorf("lock joined batch for final validation: %w", err)
	}
	if denominator != req.ExpectedFrozenDenominatorSHA256 {
		return joinedFinalValidationProgress{}, errors.New("joined final-validation denominator differs")
	}
	if state != "building" {
		return joinedFinalValidationProgress{}, errors.New("joined batch is not building")
	}
	var runID string
	err = tx.QueryRow(ctx, `SELECT id::text FROM recording_joined_final_validation_runs
		WHERE batch_record_id=$1 FOR SHARE`, batchRecordID).Scan(&runID)
	if err == nil {
		if err := tx.Commit(ctx); err != nil {
			return joinedFinalValidationProgress{}, err
		}
		return s.joinedFinalValidationStatus(ctx, runID)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return joinedFinalValidationProgress{}, err
	}
	runID = uuid.NewString()
	if _, err := tx.Exec(ctx, `INSERT INTO recording_joined_final_validation_runs
		(id,batch_record_id,batch_id,generation,validator_version,expected_denominator_sha256,expected_stream_days)
		VALUES($1,$2,$3,$4,$5,$6,$7)`, runID, batchRecordID, req.BatchID, generation,
		joinedFinalValidationVersion, denominator, expectedDays); err != nil {
		return joinedFinalValidationProgress{}, fmt.Errorf("create joined final-validation run: %w", err)
	}
	command, err := tx.Exec(ctx, `INSERT INTO recording_joined_final_validation_scopes
		(run_id,ordinal,batch_record_id,stream_day_id,recording_id,local_date,date_ordinal,
		source_snapshot_sha256,source_clip_count,source_bytes,ledger_sha256)
		SELECT $1,row_number() OVER (ORDER BY br.priority_ordinal,d.date_ordinal)::INTEGER,
			d.batch_record_id,d.id,d.recording_id,d.local_date,d.date_ordinal,d.source_snapshot_sha256,
			d.source_clip_count,d.source_bytes,d.ledger_sha256
		FROM recording_joined_stream_days d
		JOIN recording_joined_batch_recordings br ON br.id=d.batch_recording_id
		WHERE d.batch_record_id=$2 AND d.state='sealed'`, runID, batchRecordID)
	if err != nil {
		return joinedFinalValidationProgress{}, fmt.Errorf("capture joined final-validation scope: %w", err)
	}
	if int(command.RowsAffected()) != expectedDays {
		return joinedFinalValidationProgress{}, fmt.Errorf("joined final-validation scope is incomplete: rows=%d expected=%d", command.RowsAffected(), expectedDays)
	}
	if err := tx.Commit(ctx); err != nil {
		return joinedFinalValidationProgress{}, err
	}
	return s.joinedFinalValidationStatus(ctx, runID)
}

func (s *Server) handleAdminJoinedFinalValidationStep(w http.ResponseWriter, r *http.Request) {
	if !s.joinedControlPlaneReady() {
		util.WriteError(w, http.StatusServiceUnavailable, "joined recording is disabled")
		return
	}
	var req joinedFinalValidationStepRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.ProtocolVersion != joinedrecording.JoinedProtocolVersion {
		util.WriteError(w, http.StatusBadRequest, "invalid joined final-validation protocol")
		return
	}
	if _, err := uuid.Parse(req.RunID); err != nil || req.Ordinal < 0 {
		util.WriteError(w, http.StatusBadRequest, "invalid joined final-validation step")
		return
	}
	progress, err := s.stepJoinedFinalValidation(r.Context(), req)
	if err != nil {
		util.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusOK, progress)
}

func (s *Server) stepJoinedFinalValidation(ctx context.Context, req joinedFinalValidationStepRequest) (joinedFinalValidationProgress, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return joinedFinalValidationProgress{}, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='8s'`); err != nil {
		return joinedFinalValidationProgress{}, err
	}
	var state string
	if err := tx.QueryRow(ctx, `SELECT state FROM recording_joined_final_validation_runs WHERE id=$1 FOR UPDATE`, req.RunID).Scan(&state); err != nil {
		return joinedFinalValidationProgress{}, err
	}
	if state == "ready" {
		if err := tx.Commit(ctx); err != nil {
			return joinedFinalValidationProgress{}, err
		}
		return s.joinedFinalValidationStatus(ctx, req.RunID)
	}
	var ordinal int
	var streamDayID int64
	if err := tx.QueryRow(ctx, `SELECT s.ordinal,s.stream_day_id
		FROM recording_joined_final_validation_scopes s
		LEFT JOIN recording_joined_final_validation_receipts r
			ON r.run_id=s.run_id AND r.ordinal=s.ordinal
		WHERE s.run_id=$1 AND r.ordinal IS NULL ORDER BY s.ordinal LIMIT 1`, req.RunID).
		Scan(&ordinal, &streamDayID); err != nil {
		return joinedFinalValidationProgress{}, fmt.Errorf("find joined final-validation scope: %w", err)
	}
	if req.Ordinal != 0 && req.Ordinal != ordinal {
		if req.Ordinal < ordinal {
			if err := tx.Commit(ctx); err != nil {
				return joinedFinalValidationProgress{}, err
			}
			return s.joinedFinalValidationStatus(ctx, req.RunID)
		}
		return joinedFinalValidationProgress{}, errors.New("joined final-validation step is not the next ordinal")
	}
	var valid bool
	if err := tx.QueryRow(ctx, `SELECT validate_recording_joined_stream_day($1)`, streamDayID).Scan(&valid); err != nil {
		return joinedFinalValidationProgress{}, fmt.Errorf("validate joined stream day %d: %w", streamDayID, err)
	}
	if !valid {
		return joinedFinalValidationProgress{}, fmt.Errorf("joined stream day %d failed final validation", streamDayID)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO recording_joined_final_validation_receipts
		(run_id,ordinal,stream_day_id,recording_id,local_date,date_ordinal,source_snapshot_sha256,
		source_clip_count,source_bytes,ledger_sha256,validator_version,receipt_sha256)
		SELECT s.run_id,s.ordinal,s.stream_day_id,s.recording_id,s.local_date,s.date_ordinal,
		s.source_snapshot_sha256,s.source_clip_count,s.source_bytes,s.ledger_sha256,$3,
		encode(sha256(convert_to(concat_ws(E'\n',s.run_id::TEXT,s.ordinal::TEXT,s.stream_day_id::TEXT,
			s.recording_id::TEXT,s.local_date::TEXT,s.date_ordinal::TEXT,s.source_snapshot_sha256,
			s.source_clip_count::TEXT,s.source_bytes::TEXT,s.ledger_sha256,$3),'UTF8')),'hex')
		FROM recording_joined_final_validation_scopes s WHERE s.run_id=$1 AND s.ordinal=$2`,
		req.RunID, ordinal, joinedFinalValidationVersion); err != nil {
		return joinedFinalValidationProgress{}, fmt.Errorf("record joined final-validation receipt: %w", err)
	}
	if _, err := tx.Exec(ctx, `WITH c AS (
		SELECT count(*)::INTEGER AS n, encode(sha256(convert_to(COALESCE(string_agg(receipt_sha256 || E'\n','' ORDER BY ordinal),''),'UTF8')),'hex') AS digest
		FROM recording_joined_final_validation_receipts WHERE run_id=$1
	)
	UPDATE recording_joined_final_validation_runs r SET completed_scopes=c.n,
		state=CASE WHEN c.n=r.expected_stream_days THEN 'ready' ELSE 'running' END,
		receipt_set_sha256=CASE WHEN c.n=r.expected_stream_days THEN c.digest ELSE NULL END,
		completed_at=CASE WHEN c.n=r.expected_stream_days THEN clock_timestamp() ELSE NULL END
	FROM c WHERE r.id=$1 AND r.state='running'`, req.RunID); err != nil {
		return joinedFinalValidationProgress{}, fmt.Errorf("advance joined final-validation run: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return joinedFinalValidationProgress{}, err
	}
	return s.joinedFinalValidationStatus(ctx, req.RunID)
}

func (s *Server) handleAdminJoinedFinalValidationStatus(w http.ResponseWriter, r *http.Request) {
	if !s.joinedControlPlaneReady() {
		util.WriteError(w, http.StatusServiceUnavailable, "joined recording is disabled")
		return
	}
	runID := r.URL.Query().Get("run_id")
	if _, err := uuid.Parse(runID); err != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid joined final-validation id")
		return
	}
	progress, err := s.joinedFinalValidationStatus(r.Context(), runID)
	if err != nil {
		util.WriteError(w, http.StatusNotFound, "joined final-validation run not found")
		return
	}
	util.WriteJSON(w, http.StatusOK, progress)
}

func (s *Server) joinedFinalValidationStatus(ctx context.Context, runID string) (joinedFinalValidationProgress, error) {
	var p joinedFinalValidationProgress
	var next *int
	var receiptSet *string
	var completedAt *time.Time
	if err := s.pool.QueryRow(ctx, `SELECT id::text,batch_id,generation,state,validator_version,
		expected_denominator_sha256,completed_scopes,expected_stream_days,receipt_set_sha256,completed_at
		FROM recording_joined_final_validation_runs WHERE id=$1`, runID).
		Scan(&p.RunID, &p.BatchID, &p.Generation, &p.State, &p.ValidatorVersion, &p.ExpectedDenominatorSHA256,
			&p.CompletedScopes, &p.ExpectedScopes, &receiptSet, &completedAt); err != nil {
		return p, err
	}
	p.ReceiptSetSHA256, p.CompletedAt = receiptSet, completedAt
	if p.State == "running" {
		n := p.CompletedScopes + 1
		next = &n
	}
	p.NextOrdinal = next
	return p, nil
}
