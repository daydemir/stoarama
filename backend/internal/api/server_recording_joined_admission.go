package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/daydemir/stoarama/backend/internal/util"
)

// joinedClaimAdmissionAllowed is called inside each claim transaction. The
// exclusive control-row lock serializes claim admission. One-shot admission
// also locks every expected active task until the new claim commits, so a
// terminal transition cannot substitute another task under the same count.
func (s *Server) joinedClaimAdmissionAllowed(ctx context.Context, tx pgx.Tx, batchID string) (bool, bool, bool, error) {
	var paused bool
	var expected *string
	var remaining int
	err := tx.QueryRow(ctx, `SELECT c.claims_paused,c.one_shot_expected_active_claims_sha256,c.one_shot_claims_remaining
		FROM recording_joined_admission_controls c
		JOIN recording_joined_batches b ON b.id=c.batch_record_id
		WHERE c.batch_id=$1 AND b.connection_id=$2 FOR UPDATE OF c`, batchID,
		s.cfg.JoinedRecordingConnectionID).Scan(&paused, &expected, &remaining)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, false, false, nil
	}
	if err != nil {
		return false, false, false, err
	}
	if remaining == 0 {
		return !paused, false, false, nil
	}
	_, _, digest, err := joinedActiveClaims(ctx, tx, batchID, true)
	if err != nil {
		return false, false, false, err
	}
	if expected == nil || digest != *expected {
		_, err = tx.Exec(ctx, `UPDATE recording_joined_admission_controls SET claims_paused=TRUE,
			one_shot_expected_active_claims_sha256=NULL,one_shot_claims_remaining=0,updated_at=clock_timestamp()
			WHERE batch_id=$1`, batchID)
		return false, false, true, err
	}
	return true, true, false, nil
}

func joinedActiveClaims(ctx context.Context, tx pgx.Tx, batchID string, lock bool) (int64, int64, string, error) {
	suffix := ""
	if lock {
		suffix = " FOR UPDATE"
	}
	rows, err := tx.Query(ctx, `SELECT id,claim_token::text FROM recording_joined_hours
		WHERE batch_id=$1 AND state='leased' AND lease_expires_at>now()`+suffix, batchID)
	if err != nil {
		return 0, 0, "", err
	}
	var identities []string
	var hours, publications int64
	for rows.Next() {
		var id int64
		var token string
		if err := rows.Scan(&id, &token); err != nil {
			rows.Close()
			return 0, 0, "", err
		}
		identities = append(identities, fmt.Sprintf("hour:%d:%s", id, token))
		hours++
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, 0, "", err
	}
	rows.Close()
	rows, err = tx.Query(ctx, `SELECT id,publication_token::text FROM recording_joined_artifacts
		WHERE batch_id=$1 AND publication_state='publishing' AND publication_lease_expires_at>now()`+suffix, batchID)
	if err != nil {
		return 0, 0, "", err
	}
	for rows.Next() {
		var id int64
		var token string
		if err := rows.Scan(&id, &token); err != nil {
			rows.Close()
			return 0, 0, "", err
		}
		identities = append(identities, fmt.Sprintf("publication:%d:%s", id, token))
		publications++
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, 0, "", err
	}
	rows.Close()
	sort.Strings(identities)
	sum := sha256.Sum256([]byte(strings.Join(identities, "\n")))
	return hours, publications, hex.EncodeToString(sum[:]), nil
}

func consumeJoinedOneShotClaim(ctx context.Context, tx pgx.Tx, batchID string) error {
	tag, err := tx.Exec(ctx, `UPDATE recording_joined_admission_controls SET
		one_shot_expected_active_claims_sha256=NULL,one_shot_claims_remaining=0,updated_at=clock_timestamp()
		WHERE batch_id=$1 AND claims_paused=TRUE AND one_shot_claims_remaining=1`, batchID)
	if err != nil {
		return fmt.Errorf("consume joined one-shot claim: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("consume joined one-shot claim: rows=%d", tag.RowsAffected())
	}
	return nil
}

func (s *Server) handleJoinedAdmissionStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	batchID, ok := joinedAdmissionBatchID(w, r)
	if !ok {
		return
	}
	if batchID != s.cfg.JoinedRecordingBatchID {
		util.WriteError(w, http.StatusNotFound, "joined admission control is unavailable")
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
	status, err := s.joinedAdmissionStatus(ctx, tx, batchID)
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
	if req.MaxNewClaims == 1 {
		var paused bool
		if err := tx.QueryRow(ctx, `SELECT claims_paused FROM recording_joined_admission_controls
			WHERE batch_id=$1 FOR UPDATE`, req.BatchID).Scan(&paused); err != nil || !paused {
			util.WriteError(w, http.StatusConflict, "joined one-shot admission requires paused claims")
			return
		}
		_, _, digest, digestErr := joinedActiveClaims(ctx, tx, req.BatchID, true)
		if digestErr != nil || digest != req.ExpectedActiveClaimsSHA256 {
			util.WriteError(w, http.StatusConflict, "joined active claim identity differs")
			return
		}
	}
	err = tx.QueryRow(ctx, `UPDATE recording_joined_admission_controls c
		SET claims_paused=$2,one_shot_expected_active_claims_sha256=$4,
			one_shot_claims_remaining=$5,updated_at=clock_timestamp() FROM recording_joined_batches b
		WHERE c.batch_id=$1 AND b.id=c.batch_record_id AND b.connection_id=$3 RETURNING c.updated_at`,
		req.BatchID, req.ClaimsPaused, s.cfg.JoinedRecordingConnectionID,
		nullIfEmpty(req.ExpectedActiveClaimsSHA256), req.MaxNewClaims).Scan(&updatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusConflict, "joined admission control is unavailable")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "update joined admission control")
		return
	}
	status, err := s.joinedAdmissionStatus(ctx, tx, req.BatchID)
	if err != nil || !status.UpdatedAt.Equal(updatedAt) || status.ClaimsPaused != req.ClaimsPaused ||
		status.OneShotClaimsRemaining != req.MaxNewClaims || status.Validate() != nil {
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

func (s *Server) joinedAdmissionStatus(ctx context.Context, tx pgx.Tx, batchID string) (joinedrecording.ClaimAdmissionStatus, error) {
	status := joinedrecording.ClaimAdmissionStatus{ProtocolVersion: joinedrecording.JoinedProtocolVersion, BatchID: batchID}
	err := tx.QueryRow(ctx, `SELECT c.claims_paused,c.updated_at,c.one_shot_claims_remaining
		FROM recording_joined_admission_controls c JOIN recording_joined_batches b ON b.id=c.batch_record_id
		WHERE c.batch_id=$1 AND b.connection_id=$2`, batchID, s.cfg.JoinedRecordingConnectionID).Scan(
		&status.ClaimsPaused, &status.UpdatedAt, &status.OneShotClaimsRemaining)
	if err != nil {
		return status, err
	}
	status.ActiveHourLeases, status.ActivePublicationLeases, status.ActiveClaimsSHA256, err = joinedActiveClaims(ctx, tx, batchID, false)
	status.ActiveLeaseCount = status.ActiveHourLeases + status.ActivePublicationLeases
	return status, err
}

func nullIfEmpty(value string) any {
	if value == "" {
		return nil
	}
	return value
}
