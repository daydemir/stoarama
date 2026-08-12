package api

import (
	"errors"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/util"
)

type recordingGracefulHandoffRequest struct {
	AccountID            int64  `json:"account_id"`
	JobID                int64  `json:"job_id"`
	ExpectedOwner        string `json:"expected_owner"`
	ExpectedAttemptCount int    `json:"expected_attempt_count"`
	RequestID            string `json:"request_id"`
	Reason               string `json:"reason"`
}

// handleAdminRecordingGracefulHandoff requests, but never forces, a handoff.
// The exact live generation keeps renewing until its worker has stopped capture
// and acknowledged every accepted segment through the ordinary surrender call.
func (s *Server) handleAdminRecordingGracefulHandoff(w http.ResponseWriter, r *http.Request) {
	recordingID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req recordingGracefulHandoffRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	req.ExpectedOwner = strings.TrimSpace(req.ExpectedOwner)
	reason := sanitizeRecordingSurrenderError(req.Reason, "operator graceful handoff")
	requestID, err := uuid.Parse(strings.TrimSpace(req.RequestID))
	if req.AccountID <= 0 || req.JobID <= 0 || req.ExpectedAttemptCount <= 0 || req.ExpectedOwner == "" || strings.TrimSpace(req.Reason) == "" || err != nil {
		util.WriteError(w, http.StatusBadRequest, "account_id, job_id, expected_owner, expected_attempt_count, request_id, and reason are required")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin graceful handoff")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	var accountID int64
	var owner string
	var attempt int
	var groupID *int64
	var existingID *uuid.UUID
	err = tx.QueryRow(r.Context(), `
		SELECT rec.account_id,j.lease_owner,j.attempt_count,n.relay_group_id,j.graceful_handoff_request_id
		FROM recording_jobs j
		JOIN recordings rec ON rec.id=j.recording_id
		JOIN nodes n ON j.lease_owner='node:'||n.id::text AND n.account_id=rec.account_id
		WHERE j.id=$1 AND j.recording_id=$2 AND j.kind='continuous_window'
		  AND j.status='leased' AND j.window_end_at>now() AND j.lease_expires_at>now()
		FOR UPDATE OF j`, req.JobID, recordingID).Scan(&accountID, &owner, &attempt, &groupID, &existingID)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusConflict, "graceful handoff requires a live relay continuous lease")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock graceful handoff job")
		return
	}
	if accountID != req.AccountID || owner != req.ExpectedOwner || attempt != req.ExpectedAttemptCount {
		util.WriteError(w, http.StatusConflict, "graceful handoff lease precondition changed")
		return
	}
	if existingID != nil {
		if *existingID != requestID {
			util.WriteError(w, http.StatusConflict, "another graceful handoff is already requested")
			return
		}
		if err := tx.Commit(r.Context()); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "commit idempotent graceful handoff")
			return
		}
		util.WriteJSON(w, http.StatusOK, map[string]any{"recording_id": recordingID, "job_id": req.JobID, "request_id": requestID, "status": "drain_requested", "idempotent": true})
		return
	}
	// Fail closed unless a fresh, active relay in a different failure domain has
	// a free slot right now. Admission is checked again by the ordinary lease SQL.
	var alternate bool
	err = tx.QueryRow(r.Context(), `SELECT EXISTS(
		SELECT 1 FROM nodes candidate
		WHERE candidate.account_id=$1 AND candidate.node_type='relay' AND candidate.status='active'
		  AND candidate.last_heartbeat_at>=now()-interval '120 seconds'
		  AND 'node:'||candidate.id::text <> $2
		  AND candidate.relay_group_id IS DISTINCT FROM $3
		  AND (SELECT count(*) FROM recording_jobs active
		       WHERE active.status='leased' AND active.lease_owner='node:'||candidate.id::text
		         AND active.lease_expires_at>now()) < candidate.relay_max_streams
	)`, accountID, owner, groupID).Scan(&alternate)
	if err != nil || !alternate {
		util.WriteError(w, http.StatusConflict, "no alternate relay failure-domain capacity is currently available")
		return
	}
	_, err = tx.Exec(r.Context(), `UPDATE recording_jobs SET
		graceful_handoff_request_id=$2,graceful_handoff_requested_at=now(),
		graceful_handoff_reason=$3,graceful_handoff_owner=lease_owner,
		graceful_handoff_lease_token=lease_token,
		graceful_handoff_excluded_relay_group_id=$4,updated_at=now()
		WHERE id=$1`, req.JobID, requestID, reason, groupID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "request graceful handoff")
		return
	}
	if err := s.insertAccountAuthEventTx(r.Context(), tx, accountID, nil, "recording_graceful_handoff_requested", "service", "stoaramactl", map[string]any{"recording_id": recordingID, "job_id": req.JobID, "request_id": requestID, "owner": owner, "attempt_count": attempt, "reason": reason}); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "audit graceful handoff")
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit graceful handoff")
		return
	}
	util.WriteJSON(w, http.StatusAccepted, map[string]any{"recording_id": recordingID, "job_id": req.JobID, "request_id": requestID, "status": "drain_requested"})
}
