package api

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/daydemir/stoarama/backend/internal/util"
)

// joinedClaimAdmissionAllowed is called inside each claim transaction. The
// shared row lock makes a completed pause linearizable with claim commits,
// while heartbeat, seal, upload, and finalize never consult this control.
func (s *Server) joinedClaimAdmissionAllowed(ctx context.Context, tx pgx.Tx, batchID string) (bool, error) {
	var paused bool
	err := tx.QueryRow(ctx, `SELECT claims_paused FROM recording_joined_admission_controls
		WHERE batch_id=$1 FOR SHARE`, batchID).Scan(&paused)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return !paused, err
}

func (s *Server) handleJoinedAdmissionStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	batchID, ok := joinedAdmissionBatchID(w, r)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin joined admission status")
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()
	status, err := joinedAdmissionStatus(ctx, tx, batchID)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusConflict, "joined admission control is unavailable")
		return
	}
	if err != nil || status.Validate() != nil {
		util.WriteError(w, http.StatusInternalServerError, "read joined admission status")
		return
	}
	util.WriteJSON(w, http.StatusOK, status)
}

func (s *Server) handleJoinedAdmissionSet(w http.ResponseWriter, r *http.Request) {
	var req joinedrecording.ClaimAdmissionRequest
	if err := util.DecodeJSON(r, &req); err != nil || req.Validate() != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid joined claim admission request")
		return
	}
	if req.BatchID != s.cfg.JoinedRecordingBatchID {
		util.WriteError(w, http.StatusForbidden, "joined batch scope differs")
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
		util.WriteError(w, http.StatusInternalServerError, "begin joined admission update")
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, `SET LOCAL lock_timeout='3s'; SET LOCAL statement_timeout='5s'`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "bound joined admission update")
		return
	}
	var updatedAt time.Time
	err = tx.QueryRow(ctx, `UPDATE recording_joined_admission_controls
		SET claims_paused=$2,updated_at=clock_timestamp() WHERE batch_id=$1 RETURNING updated_at`,
		req.BatchID, req.ClaimsPaused).Scan(&updatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusConflict, "joined admission control is unavailable")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "update joined admission control")
		return
	}
	status, err := joinedAdmissionStatus(ctx, tx, req.BatchID)
	if err != nil || !status.UpdatedAt.Equal(updatedAt) || status.ClaimsPaused != req.ClaimsPaused || status.Validate() != nil {
		util.WriteError(w, http.StatusInternalServerError, "verify joined admission update")
		return
	}
	if err := tx.Commit(ctx); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit joined admission update")
		return
	}
	util.WriteJSON(w, http.StatusOK, status)
}

func joinedAdmissionBatchID(w http.ResponseWriter, r *http.Request) (string, bool) {
	values, ok := r.URL.Query()["batch_id"]
	if !ok || len(values) != 1 || len(r.URL.Query()) != 1 || !joinedrecording.ValidBatchID(values[0]) {
		util.WriteError(w, http.StatusBadRequest, "one canonical joined batch_id is required")
		return "", false
	}
	return values[0], true
}

func joinedAdmissionStatus(ctx context.Context, tx pgx.Tx, batchID string) (joinedrecording.ClaimAdmissionStatus, error) {
	status := joinedrecording.ClaimAdmissionStatus{ProtocolVersion: joinedrecording.JoinedProtocolVersion, BatchID: batchID}
	err := tx.QueryRow(ctx, `SELECT c.claims_paused,c.updated_at,
		(SELECT count(*) FROM recording_joined_hours h WHERE h.batch_record_id=c.batch_record_id
		 AND h.state='leased' AND h.lease_expires_at>now()),
		(SELECT count(*) FROM recording_joined_artifacts a WHERE a.batch_record_id=c.batch_record_id
		 AND a.publication_state='publishing' AND a.publication_lease_expires_at>now())
		FROM recording_joined_admission_controls c WHERE c.batch_id=$1`, batchID).Scan(
		&status.ClaimsPaused, &status.UpdatedAt, &status.ActiveHourLeases, &status.ActivePublicationLeases)
	status.ActiveLeaseCount = status.ActiveHourLeases + status.ActivePublicationLeases
	return status, err
}
