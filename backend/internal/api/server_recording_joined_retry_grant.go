package api

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

func (s *Server) handleJoinedExactRetryGrant(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	var req joinedrecording.ExactRetryGrantRequest
	if err := util.DecodeJSON(r, &req); err != nil || req.Validate() != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid exact joined retry grant")
		return
	}
	if req.BatchID != s.cfg.JoinedRecordingBatchID || !s.joinedFrozenBatchScope() {
		util.WriteError(w, http.StatusForbidden, "exact retries require the configured frozen batch")
		return
	}
	if s.pool == nil || !s.joinedControlPlaneReady() {
		util.WriteError(w, http.StatusServiceUnavailable, "joined control plane is unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin exact retry grant")
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, `SET LOCAL lock_timeout='1s'; SET LOCAL statement_timeout='4s'`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "bound exact retry grant")
		return
	}
	var hourRecordID int64
	// Lock the same hour row used by claims and seal; concurrent grants serialize.
	err = tx.QueryRow(ctx, `SELECT h.id FROM recording_joined_hours h
		JOIN recording_joined_batches b ON b.id=h.batch_record_id
		WHERE h.batch_id=$1 AND h.hour_id=$2 AND h.connection_id=$3 AND b.state='frozen'
		FOR UPDATE OF h`, req.BatchID, req.HourID, s.cfg.JoinedRecordingConnectionID).Scan(&hourRecordID)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "exact retry hour is unavailable")
		return
	}
	grant := joinedrecording.ExactRetryGrant{BatchID: req.BatchID, HourID: req.HourID, ExpectedAttemptCount: req.ExpectedAttemptCount}
	readGrant := func() error {
		return tx.QueryRow(ctx, `SELECT failure_id,reason,granted_at,consumed_at FROM recording_joined_exact_retry_grants
			WHERE hour_record_id=$1 AND expected_attempt_count=$2`, hourRecordID, req.ExpectedAttemptCount).
			Scan(&grant.FailureID, &grant.Reason, &grant.GrantedAt, &grant.ConsumedAt)
	}
	err = readGrant()
	if err == nil {
		// Idempotent replay reports the existing grant, including consumption;
		// it never creates new authority for the now-current attempt.
		if grant.Reason != req.Reason {
			util.WriteError(w, http.StatusConflict, "exact retry grant differs")
			return
		}
	} else if errors.Is(err, pgx.ErrNoRows) {
		tag, insertErr := tx.Exec(ctx, `INSERT INTO recording_joined_exact_retry_grants
			(hour_record_id,expected_attempt_count,expected_claim_token,failure_id,reason)
			SELECT h.id,h.attempt_count,h.claim_token,f.id,$3 FROM recording_joined_hours h
			JOIN recording_joined_worker_failures f ON f.hour_record_id=h.id AND f.attempt_count=h.attempt_count
			  AND f.claim_token=h.claim_token
			WHERE h.id=$1 AND h.attempt_count=$2 AND h.state='leased' AND h.lease_expires_at<=now()
			  AND h.next_attempt_at<=now()
			  AND h.source_clip_count>0 AND h.source_only_sha256 IS NULL AND h.canonical_plan IS NULL
			  AND h.manifest_bytes IS NULL AND h.manifest_sha256 IS NULL AND h.sealed_at IS NULL
			  AND f.disposition='retry' AND f.retry_at<=now() AND f.failure_class IN ('transient','resource')
			  AND NOT EXISTS(SELECT 1 FROM recording_joined_artifacts a WHERE a.hour_record_id=h.id)
			  AND NOT EXISTS(SELECT 1 FROM recording_joined_hour_dispositions d WHERE d.hour_record_id=h.id)`,
			hourRecordID, req.ExpectedAttemptCount, req.Reason)
		if insertErr != nil {
			util.WriteError(w, http.StatusInternalServerError, "record exact retry grant")
			return
		}
		if tag.RowsAffected() != 1 {
			util.WriteError(w, http.StatusConflict, "hour is not an expired output-free due retry at the expected attempt")
			return
		}
		if err = readGrant(); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "read exact retry grant")
			return
		}
	} else {
		util.WriteError(w, http.StatusInternalServerError, "read exact retry grant")
		return
	}
	if err := tx.Commit(ctx); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit exact retry grant")
		return
	}
	util.WriteJSON(w, http.StatusOK, grant)
}
