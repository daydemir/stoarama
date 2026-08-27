package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/joinedauth"
	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/daydemir/stoarama/backend/internal/util"
)

const (
	joinedWorkerProtocolVersion = 1
	joinedLeaseDuration         = 5 * time.Minute
	joinedMaxAttempts           = 8
	joinedFailureBackoffMax     = 30 * time.Minute
)

func joinedClaimCapacity(req joinedrecording.WorkClaimRequest, broad bool) (int64, error) {
	if !broad && req.ScratchAvailableBytes == 0 && req.TaskBudgetBytes == 0 {
		return 1<<63 - 1, nil
	}
	if req.ScratchAvailableBytes <= 0 || req.TaskBudgetBytes <= 0 {
		return 0, errors.New("frozen-batch claims require scratch_available_bytes and task_budget_bytes")
	}
	if req.ScratchAvailableBytes < req.TaskBudgetBytes {
		return req.ScratchAvailableBytes, nil
	}
	return req.TaskBudgetBytes, nil
}

func joinedFailureDisposition(class string, attempt int, token uuid.UUID, now time.Time) (string, time.Time) {
	if class == "deterministic" || attempt >= joinedMaxAttempts {
		return "terminal", time.Time{}
	}
	base := 15 * time.Second
	if class == "resource" {
		base = time.Minute
	}
	delay := base
	for i := 1; i < attempt && delay < joinedFailureBackoffMax; i++ {
		delay *= 2
	}
	if delay > joinedFailureBackoffMax {
		delay = joinedFailureBackoffMax
	}
	sum := sha256.Sum256(token[:])
	// Stable 75%..125% jitter avoids synchronized retries without trusting a
	// worker clock or making an idempotent report change its answer.
	delay = time.Duration(int64(delay) * int64(75+int(sum[0])%51) / 100)
	if delay > joinedFailureBackoffMax {
		delay = joinedFailureBackoffMax
	}
	return "retry", now.Add(delay)
}

func joinedFailureDispositionAfterLease(class string, attempt int, token uuid.UUID, leaseExpires, now time.Time) (string, time.Time) {
	disposition, retry := joinedFailureDisposition(class, attempt, token, now)
	if disposition != "retry" {
		return disposition, retry
	}
	// A retry row fences the existing lease rather than releasing it. Start
	// the backoff after that lease expires so the same high-priority task
	// cannot be reclaimed immediately ahead of untouched work.
	if leaseExpires.After(now) {
		retry = leaseExpires.Add(retry.Sub(now))
	}
	return disposition, retry.UTC().Truncate(time.Second)
}

func (s *Server) joinedClaimSlotAvailable(ctx context.Context, tx pgx.Tx, batchID string, connectionID int) (bool, error) {
	cap := s.cfg.JoinedRecordingMaxActiveTasks
	if cap == 0 && !s.joinedFrozenBatchScope() {
		cap = 1
	}
	if cap < 1 {
		return false, nil
	}
	var batchRecordID int64
	if err := tx.QueryRow(ctx, `SELECT id FROM recording_joined_batches WHERE batch_id=$1 AND connection_id=$2 FOR UPDATE`,
		batchID, connectionID).Scan(&batchRecordID); err != nil {
		return false, err
	}
	var active int
	if err := tx.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM recording_joined_hours WHERE batch_record_id=$1 AND state='leased' AND lease_expires_at>now())+
		(SELECT count(*) FROM recording_joined_artifacts WHERE batch_record_id=$1 AND publication_state='publishing'
		 AND publication_lease_expires_at>now())`, batchRecordID).Scan(&active); err != nil {
		return false, err
	}
	return active < cap, nil
}

// Temporary source aliases keep local API tests compiling while their wire
// fixtures move from the former item wrapper to the canonical root DTO.
type joinedClaimRequest = joinedrecording.WorkClaimRequest
type joinedClaimItem = joinedrecording.PreflightHourClaim

func validateJoinedWorkerID(workerID string) (string, error) {
	workerID = strings.TrimSpace(workerID)
	if workerID == "" || len(workerID) > 256 {
		return "", errors.New("worker_id is required and must not exceed 256 bytes")
	}
	return workerID, nil
}

func (s *Server) joinedControlPlaneReady() bool {
	return s.cfg.JoinedRecordingControlPlaneEnabled && s.cfg.ValidateJoined() == nil
}

func (s *Server) joinedCanaryHourIDs() []string {
	hours, _ := s.cfg.JoinedCanaryHourIDs()
	return hours
}

func (s *Server) joinedFrozenBatchScope() bool {
	scope, err := s.cfg.JoinedWorkScope()
	return err == nil && scope == "frozen_batch"
}

func (s *Server) joinedWorkScopeIdentity() (joinedrecording.WorkScopeIdentity, error) {
	scope, err := s.cfg.JoinedWorkScope()
	if err != nil {
		return joinedrecording.WorkScopeIdentity{}, err
	}
	var hours []string
	if joinedrecording.IsCanaryWorkScope(scope) {
		hours, err = s.cfg.JoinedCanaryHourIDs()
		if err != nil {
			return joinedrecording.WorkScopeIdentity{}, err
		}
	}
	return joinedrecording.NewWorkScopeIdentity(s.cfg.JoinedRecordingBatchID, scope, hours)
}

func (s *Server) joinedClaimMatchesCurrentScope(claims joinedauth.Claims) bool {
	current, err := s.joinedWorkScopeIdentity()
	return err == nil && claims.BatchID == s.cfg.JoinedRecordingBatchID && claims.WorkScopeIdentity.Equal(current)
}

func (s *Server) handleJoinedToken(w http.ResponseWriter, r *http.Request) {
	var req joinedrecording.WorkerBootstrapRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := req.Validate(); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !s.joinedControlPlaneReady() {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	workScope, err := s.joinedWorkScopeIdentity()
	if err != nil || req.BatchID != s.cfg.JoinedRecordingBatchID || !req.WorkScopeIdentity.Equal(workScope) {
		util.WriteError(w, http.StatusForbidden, "joined batch scope differs")
		return
	}
	canaryHours := s.joinedCanaryHourIDs()
	frozenBatch := s.joinedFrozenBatchScope()
	workScopeSHA, err := workScope.SHA256(req.BatchID)
	if err != nil {
		util.WriteError(w, http.StatusServiceUnavailable, "joined work scope is unavailable")
		return
	}
	var availableBatchID string
	err = s.pool.QueryRow(r.Context(), `
		SELECT b.batch_id FROM recording_joined_batches b
		JOIN connections c ON c.id=b.connection_id AND c.id=$5
		WHERE b.batch_id=$1 AND b.state IN ('frozen','index_sealed') AND (EXISTS(SELECT 1 FROM recording_joined_hours h WHERE h.batch_record_id=b.id
		    AND ($3 OR h.hour_id=ANY($2::text[]))
		    AND h.source_clip_count>0 AND h.attempt_count<$4
		    AND NOT EXISTS(SELECT 1 FROM recording_joined_worker_failures f WHERE f.hour_record_id=h.id
		      AND f.attempt_count=h.attempt_count AND f.disposition='retry' AND f.retry_at>now())
		    AND ((h.state='pending' AND h.next_attempt_at<=now())
		      OR (h.state='leased' AND h.lease_expires_at<=now()))
		    AND EXISTS(SELECT 1 FROM recording_joined_artifacts ledger WHERE ledger.stream_day_id=h.stream_day_id
		      AND ledger.artifact_kind='allocation_ledger' AND ledger.publication_state='published'))
		  OR EXISTS(SELECT 1 FROM recording_joined_artifacts a WHERE a.batch_record_id=b.id
		    AND a.publication_attempt_count<$4
		    AND NOT EXISTS(SELECT 1 FROM recording_joined_worker_failures f WHERE f.artifact_id=a.id
		      AND f.attempt_count=a.publication_attempt_count AND f.disposition='retry' AND f.retry_at>now())
		    AND ((a.publication_state='sealed' AND a.publication_next_attempt_at<=now())
		      OR (a.publication_state='publishing' AND a.publication_lease_expires_at<=now()))
			    AND (a.artifact_kind<>'batch_index' OR ($7='frozen_batch' AND NOT EXISTS(SELECT 1
			      FROM recording_joined_batch_index_refs ref
			      JOIN recording_joined_artifacts target ON target.id=ref.referenced_artifact_id
			      JOIN recording_joined_hours gap_hour ON gap_hour.id=target.hour_record_id
			      WHERE ref.index_artifact_id=a.id AND ref.reference_kind='hour_manifest' AND gap_hour.source_clip_count=0
			        AND NOT EXISTS(SELECT 1 FROM recording_joined_gap_only_scope_authorizations ga
			          WHERE ga.artifact_id=target.id AND ga.batch_record_id=target.batch_record_id
			            AND ga.batch_id=target.batch_id AND ga.hour_record_id=target.hour_record_id
			            AND ga.hour_id=target.scope_id AND ga.work_scope='frozen_batch'
			            AND ga.work_scope_identity_sha256=$6
			            AND ga.authorization_source IN ('server_seal','operator_frozen')))))
			    AND (a.artifact_kind<>'hour_manifest' OR COALESCE((SELECT source_clip_count FROM recording_joined_hours WHERE id=a.hour_record_id),0)>0
			      OR EXISTS(SELECT 1 FROM recording_joined_gap_only_scope_authorizations ga WHERE ga.artifact_id=a.id
			        AND ga.batch_record_id=a.batch_record_id AND ga.batch_id=a.batch_id AND ga.hour_record_id=a.hour_record_id
			        AND ga.hour_id=a.scope_id AND ga.work_scope=$7 AND ga.work_scope_identity_sha256=$6
			        AND (($7='frozen_batch' AND ga.authorization_source IN ('server_seal','operator_frozen'))
			          OR ($7 IN ('canary','canary_single','allowlist_50') AND ga.authorization_source='server_seal'))))
			    AND (($3 AND (a.artifact_kind<>'hour_manifest' OR EXISTS(SELECT 1 FROM recording_joined_artifacts published_ledger
		          WHERE published_ledger.stream_day_id=a.stream_day_id AND published_ledger.artifact_kind='allocation_ledger'
		            AND published_ledger.publication_state='published'))) OR (a.artifact_kind='allocation_ledger' AND EXISTS(SELECT 1 FROM recording_joined_hours allowed
		          WHERE allowed.stream_day_id=a.stream_day_id AND allowed.hour_id=ANY($2::text[])))
		      OR (a.artifact_kind='hour_manifest' AND EXISTS(SELECT 1 FROM recording_joined_hours allowed
		          WHERE allowed.id=a.hour_record_id AND allowed.hour_id=ANY($2::text[]))
		        AND EXISTS(SELECT 1 FROM recording_joined_artifacts ledger WHERE ledger.stream_day_id=a.stream_day_id
		          AND ledger.artifact_kind='allocation_ledger' AND ledger.publication_state='published')))))
		LIMIT 1`, req.BatchID, canaryHours, frozenBatch, joinedMaxAttempts,
		s.cfg.JoinedRecordingConnectionID, workScopeSHA, workScope.WorkScope).Scan(&availableBatchID)
	if errors.Is(err, pgx.ErrNoRows) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("select joined batch: %v", err))
		return
	}
	expiresAt := time.Now().UTC().Truncate(time.Second).Add(10 * time.Minute)
	token, err := joinedauth.MintClaim(s.cfg.JoinedWorkerSigningKey, availableBatchID, workScope, expiresAt)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("mint joined claim token: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, joinedrecording.WorkerBootstrapResponse{ProtocolVersion: joinedWorkerProtocolVersion,
		BatchID: availableBatchID, ClaimToken: token, ExpiresAt: expiresAt, WorkScopeIdentity: workScope})
}

func (s *Server) recordJoinedExpiredAttemptEvidence(ctx context.Context, batchID string, canaryHours []string, frozenBatch bool) (int64, int64, error) {
	workScope, err := s.joinedWorkScopeIdentity()
	if err != nil {
		return 0, 0, err
	}
	workScopeSHA, err := workScope.SHA256(batchID)
	if err != nil {
		return 0, 0, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout='500ms'; SET LOCAL statement_timeout='1500ms'`); err != nil {
		return 0, 0, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO recording_joined_worker_failures(batch_record_id,hour_record_id,batch_id,scope_kind,scope_id,
		 claim_token,attempt_count,failure_class,reason_code,disposition)
		 SELECT h.batch_record_id,h.id,h.batch_id,'hour',h.hour_id,h.claim_token,h.attempt_count,
		   'transient','worker_lease_expired','terminal' FROM recording_joined_hours h
		 WHERE h.batch_id=$1 AND ($4 OR h.hour_id=ANY($3::text[])) AND h.state='leased'
		   AND EXISTS(SELECT 1 FROM recording_joined_batches b WHERE b.id=h.batch_record_id AND b.connection_id=$5)
		   AND h.lease_expires_at<=now() AND h.attempt_count>= $2
		 ON CONFLICT DO NOTHING`, batchID, joinedMaxAttempts, canaryHours, frozenBatch, s.cfg.JoinedRecordingConnectionID); err != nil {
		return 0, 0, err
	}
	hours, err := tx.Exec(ctx, `UPDATE recording_joined_hours h SET state='terminal_failed',claim_token=NULL,claimed_by=NULL,
		 lease_expires_at=NULL,heartbeat_at=NULL,failure_reason_code='worker_lease_expired'
		 WHERE h.batch_id=$1 AND ($4 OR h.hour_id=ANY($3::text[])) AND h.state='leased'
		   AND EXISTS(SELECT 1 FROM recording_joined_batches b WHERE b.id=h.batch_record_id AND b.connection_id=$5)
		   AND h.lease_expires_at<=now() AND h.attempt_count>= $2
		   AND EXISTS(SELECT 1 FROM recording_joined_worker_failures f WHERE f.hour_record_id=h.id
		     AND f.claim_token=h.claim_token AND f.attempt_count=h.attempt_count AND f.disposition='terminal'
		     AND f.reason_code='worker_lease_expired')`, batchID, joinedMaxAttempts, canaryHours, frozenBatch,
		s.cfg.JoinedRecordingConnectionID)
	if err != nil {
		return 0, 0, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO recording_joined_worker_failures(batch_record_id,artifact_id,batch_id,scope_kind,scope_id,
		 claim_token,attempt_count,failure_class,reason_code,disposition)
		 SELECT a.batch_record_id,a.id,a.batch_id,a.scope_kind,a.scope_id,a.publication_token,a.publication_attempt_count,
		   'transient','worker_lease_expired','terminal' FROM recording_joined_artifacts a
		 WHERE a.batch_id=$1 AND a.artifact_kind<>'media' AND a.publication_state='publishing'
		   AND EXISTS(SELECT 1 FROM recording_joined_batches b WHERE b.id=a.batch_record_id AND b.connection_id=$5)
		   AND (a.artifact_kind<>'batch_index' OR ($7='frozen_batch' AND NOT EXISTS(SELECT 1
		     FROM recording_joined_batch_index_refs ref
		     JOIN recording_joined_artifacts target ON target.id=ref.referenced_artifact_id
		     JOIN recording_joined_hours gap_hour ON gap_hour.id=target.hour_record_id
		     WHERE ref.index_artifact_id=a.id AND ref.reference_kind='hour_manifest' AND gap_hour.source_clip_count=0
		       AND NOT EXISTS(SELECT 1 FROM recording_joined_gap_only_scope_authorizations ga
		         WHERE ga.artifact_id=target.id AND ga.batch_record_id=target.batch_record_id
		           AND ga.batch_id=target.batch_id AND ga.hour_record_id=target.hour_record_id
		           AND ga.hour_id=target.scope_id AND ga.work_scope='frozen_batch'
		           AND ga.work_scope_identity_sha256=$6
		           AND ga.authorization_source IN ('server_seal','operator_frozen')))))
		   AND (a.artifact_kind<>'hour_manifest' OR COALESCE((SELECT source_clip_count FROM recording_joined_hours
		     WHERE id=a.hour_record_id),0)>0 OR EXISTS(SELECT 1 FROM recording_joined_gap_only_scope_authorizations ga
		       WHERE ga.artifact_id=a.id AND ga.batch_record_id=a.batch_record_id AND ga.batch_id=a.batch_id
		         AND ga.hour_record_id=a.hour_record_id AND ga.hour_id=a.scope_id AND ga.work_scope=$7
		         AND ga.work_scope_identity_sha256=$6
		         AND (($7='frozen_batch' AND ga.authorization_source IN ('server_seal','operator_frozen'))
		           OR ($7 IN ('canary','canary_single','allowlist_50') AND ga.authorization_source='server_seal'))))
		   AND ($4 OR (a.artifact_kind='allocation_ledger' AND EXISTS(SELECT 1 FROM recording_joined_hours allowed
		     WHERE allowed.stream_day_id=a.stream_day_id AND allowed.hour_id=ANY($3::text[])))
		     OR (a.artifact_kind='hour_manifest' AND EXISTS(SELECT 1 FROM recording_joined_hours allowed
		       WHERE allowed.id=a.hour_record_id AND allowed.hour_id=ANY($3::text[]))))
		   AND a.publication_lease_expires_at<=now() AND a.publication_attempt_count>= $2
		 ON CONFLICT DO NOTHING`, batchID, joinedMaxAttempts, canaryHours, frozenBatch, s.cfg.JoinedRecordingConnectionID,
		workScopeSHA, workScope.WorkScope); err != nil {
		return 0, 0, err
	}
	artifacts, err := tx.Exec(ctx, `UPDATE recording_joined_artifacts a SET publication_state='terminal_failed',
		 publication_token=NULL,publication_claimed_by=NULL,publication_lease_expires_at=NULL,
		 publication_heartbeat_at=NULL,failure_reason_code='worker_lease_expired'
		 WHERE a.batch_id=$1 AND a.artifact_kind<>'media' AND a.publication_state='publishing'
		   AND EXISTS(SELECT 1 FROM recording_joined_batches b WHERE b.id=a.batch_record_id AND b.connection_id=$5)
		   AND (a.artifact_kind<>'batch_index' OR ($7='frozen_batch' AND NOT EXISTS(SELECT 1
		     FROM recording_joined_batch_index_refs ref
		     JOIN recording_joined_artifacts target ON target.id=ref.referenced_artifact_id
		     JOIN recording_joined_hours gap_hour ON gap_hour.id=target.hour_record_id
		     WHERE ref.index_artifact_id=a.id AND ref.reference_kind='hour_manifest' AND gap_hour.source_clip_count=0
		       AND NOT EXISTS(SELECT 1 FROM recording_joined_gap_only_scope_authorizations ga
		         WHERE ga.artifact_id=target.id AND ga.batch_record_id=target.batch_record_id
		           AND ga.batch_id=target.batch_id AND ga.hour_record_id=target.hour_record_id
		           AND ga.hour_id=target.scope_id AND ga.work_scope='frozen_batch'
		           AND ga.work_scope_identity_sha256=$6
		           AND ga.authorization_source IN ('server_seal','operator_frozen')))))
		   AND (a.artifact_kind<>'hour_manifest' OR COALESCE((SELECT source_clip_count FROM recording_joined_hours
		     WHERE id=a.hour_record_id),0)>0 OR EXISTS(SELECT 1 FROM recording_joined_gap_only_scope_authorizations ga
		       WHERE ga.artifact_id=a.id AND ga.batch_record_id=a.batch_record_id AND ga.batch_id=a.batch_id
		         AND ga.hour_record_id=a.hour_record_id AND ga.hour_id=a.scope_id AND ga.work_scope=$7
		         AND ga.work_scope_identity_sha256=$6
		         AND (($7='frozen_batch' AND ga.authorization_source IN ('server_seal','operator_frozen'))
		           OR ($7 IN ('canary','canary_single','allowlist_50') AND ga.authorization_source='server_seal'))))
		   AND ($4 OR (a.artifact_kind='allocation_ledger' AND EXISTS(SELECT 1 FROM recording_joined_hours allowed
		     WHERE allowed.stream_day_id=a.stream_day_id AND allowed.hour_id=ANY($3::text[])))
		     OR (a.artifact_kind='hour_manifest' AND EXISTS(SELECT 1 FROM recording_joined_hours allowed
		       WHERE allowed.id=a.hour_record_id AND allowed.hour_id=ANY($3::text[]))))
		   AND a.publication_lease_expires_at<=now() AND a.publication_attempt_count>= $2
		   AND EXISTS(SELECT 1 FROM recording_joined_worker_failures f WHERE f.artifact_id=a.id
		     AND f.claim_token=a.publication_token AND f.attempt_count=a.publication_attempt_count
		     AND f.disposition='terminal' AND f.reason_code='worker_lease_expired')`, batchID, joinedMaxAttempts, canaryHours,
		frozenBatch, s.cfg.JoinedRecordingConnectionID, workScopeSHA, workScope.WorkScope)
	if err != nil {
		return 0, 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, 0, err
	}
	return hours.RowsAffected(), artifacts.RowsAffected(), nil
}

func (s *Server) handleJoinedClaim(w http.ResponseWriter, r *http.Request) {
	var req joinedrecording.WorkClaimRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := req.Validate(); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	workerID := req.WorkerID
	if !s.joinedControlPlaneReady() {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	canaryHours := s.joinedCanaryHourIDs()
	frozenBatch := s.joinedFrozenBatchScope()
	claims, ok := joinedWorkerClaimsFromContext(r.Context())
	if !ok || claims.Kind != joinedauth.KindClaim || req.BatchID != claims.BatchID || !s.joinedClaimMatchesCurrentScope(claims) {
		util.WriteError(w, http.StatusForbidden, "joined claim token scope differs")
		return
	}
	capacityBytes, err := joinedClaimCapacity(req, frozenBatch)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin joined claim: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	admissionAllowed, err := s.joinedClaimAdmissionAllowed(r.Context(), tx, claims.BatchID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "read joined claim admission")
		return
	}
	if !admissionAllowed {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	available, err := s.joinedClaimSlotAvailable(r.Context(), tx, claims.BatchID, s.cfg.JoinedRecordingConnectionID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "read joined active-task cap")
		return
	}
	if !available {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	var hourRecordID int64
	err = tx.QueryRow(r.Context(), `
		SELECT h.id FROM recording_joined_hours h
		JOIN recording_joined_artifacts ledger ON ledger.stream_day_id=h.stream_day_id
		  AND ledger.artifact_kind='allocation_ledger' AND ledger.publication_state='published'
		JOIN connections c ON c.id=h.connection_id AND c.id=$7
		WHERE h.batch_id=$1 AND ($3 OR h.hour_id=ANY($2::text[]))
		  AND EXISTS(SELECT 1 FROM recording_joined_batches b WHERE b.id=h.batch_record_id AND b.state='frozen')
		  AND h.source_clip_count>0 AND h.attempt_count<$5
		  AND h.source_bytes<=GREATEST(($4::bigint-$6::bigint)/2,-1::bigint)
		  AND NOT EXISTS(SELECT 1 FROM recording_joined_worker_failures f WHERE f.hour_record_id=h.id
		    AND f.attempt_count=h.attempt_count AND f.disposition='retry' AND f.retry_at>now())
		  AND ((h.state='pending' AND h.next_attempt_at<=now()) OR (h.state='leased' AND h.lease_expires_at<=now()))
		ORDER BY h.priority_ordinal,h.next_attempt_at,h.id
		FOR UPDATE OF h,c SKIP LOCKED LIMIT 1`, claims.BatchID, canaryHours, frozenBatch, capacityBytes,
		joinedMaxAttempts, joinedrecording.JoinedScratchFixedBytes, s.cfg.JoinedRecordingConnectionID).Scan(&hourRecordID)
	if errors.Is(err, pgx.ErrNoRows) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("select joined claim: %v", err))
		return
	}
	claimToken := uuid.New()
	var item joinedrecording.PreflightHourClaim
	var metadataJSON, qualificationJSON, mediaToolJSON []byte
	err = tx.QueryRow(r.Context(), `
		UPDATE recording_joined_hours SET state='leased',attempt_count=attempt_count+1,claim_token=$2,claimed_by=$3,
		  lease_expires_at=date_trunc('second',now()+$4::interval),heartbeat_at=now()
		WHERE id=$1 AND batch_id=$5 AND ($7 OR hour_id=ANY($6::text[]))
		  AND EXISTS(SELECT 1 FROM connections c WHERE c.id=recording_joined_hours.connection_id AND c.id=$8)
		RETURNING hour_id,lease_expires_at`, hourRecordID, claimToken, workerID, joinedLeaseDuration.String(), claims.BatchID,
		canaryHours, frozenBatch, s.cfg.JoinedRecordingConnectionID).
		Scan(&item.HourID, &item.LeaseExpires)
	if err != nil {
		util.WriteError(w, http.StatusConflict, fmt.Sprintf("claim joined hour: %v", err))
		return
	}
	err = tx.QueryRow(r.Context(), `
		SELECT h.batch_id,b.generation,h.recording_id,br.timezone,h.local_date::text,h.delivery_hour,
		  br.folder_name,br.naming_metadata,day.ledger_sha256,br.qualification,b.media_tool,h.source_claim_sha256
		FROM recording_joined_hours h
		JOIN recording_joined_batches b ON b.id=h.batch_record_id
		JOIN recording_joined_batch_recordings br ON br.batch_record_id=b.id AND br.recording_id=h.recording_id
		JOIN recording_joined_stream_days day ON day.id=h.stream_day_id
		WHERE h.id=$1`, hourRecordID).Scan(&item.BatchID, &item.Generation, &item.RecordingID, &item.Timezone,
		&item.LocalDate, &item.LocalHour, &item.FolderName, &metadataJSON, &item.AllocationLedgerSHA,
		&qualificationJSON, &mediaToolJSON, &item.SourceClaimSHA256)
	if err != nil || json.Unmarshal(metadataJSON, &item.Metadata) != nil || json.Unmarshal(qualificationJSON, &item.Qualification) != nil ||
		json.Unmarshal(mediaToolJSON, &item.MediaTool) != nil {
		util.WriteError(w, http.StatusInternalServerError, "load canonical joined claim facts")
		return
	}
	rows, err := tx.Query(r.Context(), `
		SELECT clip_id,recording_id,recording_job_id,storage_destination_id,provider,endpoint,region,bucket,start_at,end_at,
		  released_at,object_key,version_id,etag,size_bytes,sha256
		FROM recording_joined_sources WHERE hour_record_id=$1 ORDER BY hour_ordinal`, hourRecordID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load joined claim sources: %v", err))
		return
	}
	defer rows.Close()
	item.Sources = []joinedrecording.SourceClip{}
	for rows.Next() {
		var source joinedrecording.SourceClip
		if err := rows.Scan(&source.ClipID, &source.RecordingID, &source.RecordingJobID, &source.StorageDestinationID, &source.Provider,
			&source.Endpoint, &source.Region, &source.Bucket, &source.StartUTC, &source.EndUTC,
			&source.ReleasedAt, &source.Object.Key, &source.Object.VersionID, &source.Object.ETag, &source.Object.SizeBytes,
			&source.Object.SHA256); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan joined claim source: %v", err))
			return
		}
		source.StartUTC, source.EndUTC = source.StartUTC.UTC(), source.EndUTC.UTC()
		if source.ReleasedAt != nil {
			releasedAt := source.ReleasedAt.UTC()
			source.ReleasedAt = &releasedAt
		}
		item.Sources = append(item.Sources, source)
	}
	rows.Close()
	if rows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, "load joined claim sources")
		return
	}
	item.LeaseID = joinedauth.LeaseID(claimToken)
	item.ProtocolVersion = joinedWorkerProtocolVersion
	item.OperationToken, err = joinedauth.MintOperation(s.cfg.JoinedWorkerSigningKey, item.BatchID, joinedauth.SubjectHour,
		item.HourID, claimToken, joinedauth.OperationPreflight, item.LeaseExpires)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("mint joined job token: %v", err))
		return
	}
	if err := item.Validate(time.Now().UTC()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("validate canonical joined claim: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit joined claim: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, item)
}

func (s *Server) handleJoinedPublicationClaim(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), joinedBatchIndexOperationTimeout)
	defer cancel()
	r = r.WithContext(ctx)
	var req joinedrecording.PublicationClaimRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := req.Validate(); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !s.joinedControlPlaneReady() {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	canaryHours := s.joinedCanaryHourIDs()
	frozenBatch := s.joinedFrozenBatchScope()
	currentScope, scopeErr := s.joinedWorkScopeIdentity()
	if scopeErr != nil {
		util.WriteError(w, http.StatusServiceUnavailable, "joined work scope is unavailable")
		return
	}
	currentScopeSHA, scopeErr := currentScope.SHA256(req.BatchID)
	if scopeErr != nil {
		util.WriteError(w, http.StatusServiceUnavailable, "joined work scope is unavailable")
		return
	}
	claims, ok := joinedWorkerClaimsFromContext(r.Context())
	if !ok || claims.Kind != joinedauth.KindClaim || claims.BatchID != req.BatchID || !s.joinedClaimMatchesCurrentScope(claims) {
		util.WriteError(w, http.StatusForbidden, "joined claim token scope differs")
		return
	}
	capacityBytes, err := joinedClaimCapacity(req, frozenBatch)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	workerID, err := validateJoinedWorkerID(req.WorkerID)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin joined publication claim")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	admissionAllowed, err := s.joinedClaimAdmissionAllowed(r.Context(), tx, claims.BatchID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "read joined publication claim admission")
		return
	}
	if !admissionAllowed {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	available, err := s.joinedClaimSlotAvailable(r.Context(), tx, claims.BatchID, s.cfg.JoinedRecordingConnectionID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "read joined active-task cap")
		return
	}
	if !available {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	var artifactID int64
	var selectedKind string
	err = tx.QueryRow(r.Context(), `SELECT a.id,a.artifact_kind FROM recording_joined_artifacts a
		JOIN connections c ON c.id=a.connection_id AND c.id=$7
		LEFT JOIN recording_joined_artifacts ledger ON a.artifact_kind='hour_manifest'
		  AND ledger.stream_day_id=a.stream_day_id AND ledger.artifact_kind='allocation_ledger'
		JOIN recording_joined_batches b ON b.id=a.batch_record_id
		WHERE a.batch_id=$1 AND b.state IN ('frozen','index_sealed') AND a.artifact_kind<>'media'
		  AND a.publication_attempt_count<$5
		  AND (a.artifact_kind<>'hour_manifest' OR COALESCE((SELECT h.source_bytes FROM recording_joined_hours h WHERE h.id=a.hour_record_id),0)<=GREATEST(($4::bigint-$6::bigint)/2,-1::bigint))
		  AND NOT EXISTS(SELECT 1 FROM recording_joined_worker_failures f WHERE f.artifact_id=a.id
		    AND f.attempt_count=a.publication_attempt_count AND f.disposition='retry' AND f.retry_at>now())
		  AND ((a.publication_state='sealed' AND a.publication_next_attempt_at<=now())
		    OR (a.publication_state='publishing' AND a.publication_lease_expires_at<=now()))
		  AND (a.artifact_kind<>'batch_index' OR ($9='frozen_batch' AND NOT EXISTS(SELECT 1
		    FROM recording_joined_batch_index_refs ref
		    JOIN recording_joined_artifacts target ON target.id=ref.referenced_artifact_id
		    JOIN recording_joined_hours gap_hour ON gap_hour.id=target.hour_record_id
		    WHERE ref.index_artifact_id=a.id AND ref.reference_kind='hour_manifest' AND gap_hour.source_clip_count=0
		      AND NOT EXISTS(SELECT 1 FROM recording_joined_gap_only_scope_authorizations ga
		        WHERE ga.artifact_id=target.id AND ga.batch_record_id=target.batch_record_id
		          AND ga.batch_id=target.batch_id AND ga.hour_record_id=target.hour_record_id
		          AND ga.hour_id=target.scope_id AND ga.work_scope='frozen_batch'
		          AND ga.work_scope_identity_sha256=$8
		          AND ga.authorization_source IN ('server_seal','operator_frozen')))))
		  AND (a.artifact_kind<>'hour_manifest' OR COALESCE((SELECT source_clip_count FROM recording_joined_hours WHERE id=a.hour_record_id),0)>0
		    OR EXISTS(SELECT 1 FROM recording_joined_gap_only_scope_authorizations ga WHERE ga.artifact_id=a.id
		      AND ga.batch_record_id=a.batch_record_id AND ga.batch_id=a.batch_id AND ga.hour_record_id=a.hour_record_id
		      AND ga.hour_id=a.scope_id AND ga.work_scope=$9 AND ga.work_scope_identity_sha256=$8
		      AND (($9='frozen_batch' AND ga.authorization_source IN ('server_seal','operator_frozen'))
		        OR ($9 IN ('canary','canary_single','allowlist_50') AND ga.authorization_source='server_seal'))))
		  AND (($3 AND (a.artifact_kind<>'hour_manifest' OR ledger.publication_state='published')) OR (a.artifact_kind='allocation_ledger' AND EXISTS(SELECT 1 FROM recording_joined_hours allowed
		      WHERE allowed.stream_day_id=a.stream_day_id AND allowed.hour_id=ANY($2::text[])))
		    OR (a.artifact_kind='hour_manifest' AND ledger.publication_state='published'
		      AND EXISTS(SELECT 1 FROM recording_joined_hours allowed
		        WHERE allowed.id=a.hour_record_id AND allowed.hour_id=ANY($2::text[]))))
		ORDER BY CASE a.artifact_kind WHEN 'allocation_ledger' THEN 0 WHEN 'hour_manifest' THEN 1 ELSE 2 END,
		  COALESCE((SELECT h.priority_ordinal FROM recording_joined_hours h WHERE h.id=a.hour_record_id),0),a.id
		FOR UPDATE OF a SKIP LOCKED LIMIT 1`, claims.BatchID, canaryHours, frozenBatch, capacityBytes,
		joinedMaxAttempts, joinedrecording.JoinedScratchFixedBytes, s.cfg.JoinedRecordingConnectionID, currentScopeSHA,
		currentScope.WorkScope).Scan(&artifactID, &selectedKind)
	if errors.Is(err, pgx.ErrNoRows) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err != nil {
		log.Printf("joined publication claim selection failed batch_id=%s worker_id=%s: %v", claims.BatchID, workerID, err)
		util.WriteError(w, http.StatusInternalServerError, "select joined publication claim")
		return
	}
	if selectedKind == "batch_index" {
		if buildErr := configureJoinedBatchIndexTransaction(r.Context(), tx); buildErr != nil {
			util.WriteError(w, http.StatusConflict, "configure joined batch-index evidence transaction")
			return
		}
		canonical, state, existingID, buildErr := loadJoinedCanonicalBatchIndex(r.Context(), tx, claims.BatchID,
			s.cfg.JoinedRecordingConnectionID, true)
		if buildErr != nil || state != "index_sealed" || existingID != artifactID || canonical.SHA256 == "" {
			util.WriteError(w, http.StatusConflict, "joined batch index evidence differs")
			return
		}
	}
	var lockedConnection int64
	if err := tx.QueryRow(r.Context(), `SELECT c.id FROM recording_joined_artifacts a JOIN connections c
		ON c.id=a.connection_id AND c.id=$2 WHERE a.id=$1 FOR SHARE OF c`, artifactID,
		s.cfg.JoinedRecordingConnectionID).
		Scan(&lockedConnection); err != nil {
		util.WriteError(w, http.StatusConflict, "joined publication protocol changed")
		return
	}
	leaseToken := uuid.New()
	var kind, scopeID string
	var leaseExpires time.Time
	err = tx.QueryRow(r.Context(), `UPDATE recording_joined_artifacts SET publication_state='publishing',
		publication_attempt_count=publication_attempt_count+1,publication_token=$2,publication_claimed_by=$3,
		publication_lease_expires_at=date_trunc('second',now()+$4::interval),publication_heartbeat_at=now()
		WHERE id=$1 AND batch_id=$5 AND EXISTS(SELECT 1 FROM connections c
		  WHERE c.id=recording_joined_artifacts.connection_id AND c.id=$6)
		RETURNING artifact_kind,scope_id,publication_lease_expires_at`, artifactID, leaseToken, workerID,
		joinedLeaseDuration.String(), claims.BatchID, s.cfg.JoinedRecordingConnectionID).Scan(&kind, &scopeID, &leaseExpires)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "claim joined publication")
		return
	}
	operationToken, err := joinedauth.MintOperation(s.cfg.JoinedWorkerSigningKey, claims.BatchID,
		map[string]string{"allocation_ledger": joinedauth.SubjectLedger, "hour_manifest": joinedauth.SubjectHour,
			"batch_index": joinedauth.SubjectBatchIndex}[kind], scopeID, leaseToken, joinedauth.OperationPublish, leaseExpires)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "mint joined publication token")
		return
	}
	authority, err := joinedOutputAuthority(s.cfg.R2Endpoint)
	if err != nil {
		util.WriteError(w, http.StatusServiceUnavailable, "joined output storage authority is invalid")
		return
	}
	response := joinedrecording.PublicationClaimResponse{ProtocolVersion: joinedWorkerProtocolVersion}
	switch kind {
	case "allocation_ledger":
		var raw []byte
		if err = tx.QueryRow(r.Context(), `SELECT d.ledger_bytes FROM recording_joined_artifacts a
		  JOIN recording_joined_stream_days d ON d.id=a.stream_day_id WHERE a.id=$1`, artifactID).Scan(&raw); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "load joined ledger claim")
			return
		}
		claim := joinedrecording.LedgerPublicationClaim{ProtocolVersion: joinedWorkerProtocolVersion, ArtifactID: artifactID,
			ScopeID: scopeID, LeaseID: joinedauth.LeaseID(leaseToken), OperationToken: operationToken,
			LeaseExpires: leaseExpires, StorageAuthority: authority, StorageBucket: s.cfg.R2Bucket,
			BatchID: claims.BatchID}
		if err = json.Unmarshal(raw, &claim.Ledger); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "decode joined ledger claim")
			return
		}
		if err = tx.QueryRow(r.Context(), `SELECT expected_size_bytes,expected_sha256 FROM recording_joined_artifacts WHERE id=$1`, artifactID).
			Scan(&claim.ExpectedSize, &claim.ExpectedSHA256); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "load joined ledger identity")
			return
		}
		response.Kind, response.Ledger = "ledger", &claim
	case "hour_manifest":
		claim, loadErr := loadJoinedHourPublicationClaim(r.Context(), tx, artifactID, leaseToken, operationToken,
			leaseExpires, authority, s.cfg.R2Bucket)
		if loadErr != nil {
			util.WriteError(w, http.StatusInternalServerError, "load joined hour publication claim")
			return
		}
		response.Kind, response.Hour = "hour", &claim
	case "batch_index":
		var raw []byte
		claim := joinedrecording.BatchIndexPublicationClaim{ProtocolVersion: joinedWorkerProtocolVersion, ScopeID: scopeID,
			ArtifactID: artifactID, LeaseID: joinedauth.LeaseID(leaseToken), OperationToken: operationToken,
			LeaseExpires: leaseExpires, StorageAuthority: authority, StorageBucket: s.cfg.R2Bucket}
		if err = tx.QueryRow(r.Context(), `SELECT canonical_bytes,expected_size_bytes,expected_sha256
		  FROM recording_joined_artifacts WHERE id=$1`, artifactID).Scan(&raw, &claim.ExpectedSize, &claim.ExpectedSHA256); err != nil ||
			json.Unmarshal(raw, &claim.Index) != nil {
			util.WriteError(w, http.StatusInternalServerError, "load joined batch index claim")
			return
		}
		response.Kind, response.BatchIndex = "batch_index", &claim
	default:
		util.WriteError(w, http.StatusInternalServerError, "invalid joined publication kind")
		return
	}
	if err := response.Validate(time.Now().UTC()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("validate joined publication claim: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusConflict, "commit joined publication claim")
		return
	}
	writeJoinedWorkerJSON(w, http.StatusOK, response)
}

func (s *Server) handleJoinedFailure(w http.ResponseWriter, r *http.Request) {
	if !s.joinedControlPlaneReady() {
		util.WriteError(w, http.StatusServiceUnavailable, "joined recording is disabled")
		return
	}
	var req joinedrecording.WorkFailureRequest
	if err := util.DecodeJSON(r, &req); err != nil || req.Validate() != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid joined work failure")
		return
	}
	claims, ok := joinedWorkerClaimsFromContext(r.Context())
	if !ok || claims.Kind != joinedauth.KindOperation || claims.BatchID != s.cfg.JoinedRecordingBatchID ||
		claims.SubjectKind != req.ScopeKind || claims.SubjectID != req.ScopeID ||
		(claims.Operation != joinedauth.OperationPreflight && claims.Operation != joinedauth.OperationPublish) {
		util.WriteError(w, http.StatusForbidden, "joined failure token scope differs")
		return
	}
	claimToken, err := joinedCapabilityToken(claims.LeaseToken)
	if err != nil {
		util.WriteError(w, http.StatusForbidden, "joined failure lease differs")
		return
	}
	var workScopeSHA, workScopeName string
	if claims.Operation == joinedauth.OperationPublish {
		workScope, scopeErr := s.joinedWorkScopeIdentity()
		if scopeErr != nil {
			util.WriteError(w, http.StatusServiceUnavailable, "joined work scope is unavailable")
			return
		}
		workScopeSHA, scopeErr = workScope.SHA256(claims.BatchID)
		if scopeErr != nil {
			util.WriteError(w, http.StatusServiceUnavailable, "joined work scope is unavailable")
			return
		}
		workScopeName = workScope.WorkScope
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin joined failure report")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if _, err := tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, claimToken.String()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock joined failure report")
		return
	}
	var existingClass, existingReason, disposition string
	var attempt int
	var retryAt *time.Time
	err = tx.QueryRow(r.Context(), `SELECT f.failure_class,f.reason_code,f.disposition,f.attempt_count,f.retry_at
		FROM recording_joined_worker_failures f
		JOIN recording_joined_batches b ON b.id=f.batch_record_id AND b.connection_id=$5
		WHERE f.claim_token=$1 AND f.batch_id=$2 AND f.scope_kind=$3 AND f.scope_id=$4`, claimToken,
		claims.BatchID, req.ScopeKind, req.ScopeID, s.cfg.JoinedRecordingConnectionID).
		Scan(&existingClass, &existingReason, &disposition, &attempt, &retryAt)
	if err == nil {
		if existingClass != req.FailureClass || existingReason != req.ReasonCode {
			util.WriteError(w, http.StatusConflict, "joined failure retry differs")
			return
		}
		util.WriteJSON(w, http.StatusOK, joinedrecording.WorkFailureResponse{ProtocolVersion: 1,
			State: disposition, AttemptCount: attempt, NextAttemptAt: retryAt})
		return
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusInternalServerError, "read joined failure report")
		return
	}
	var batchRecordID, targetID int64
	var leaseExpires time.Time
	if claims.Operation == joinedauth.OperationPreflight && req.ScopeKind == joinedauth.SubjectHour {
		err = tx.QueryRow(r.Context(), `SELECT batch_record_id,id,attempt_count,lease_expires_at FROM recording_joined_hours
			WHERE batch_id=$1 AND hour_id=$2 AND state='leased' AND claim_token=$3 AND lease_expires_at>now()
			  AND connection_id=$4
			FOR UPDATE`, claims.BatchID, req.ScopeID, claimToken, s.cfg.JoinedRecordingConnectionID).Scan(&batchRecordID, &targetID, &attempt, &leaseExpires)
	} else if claims.Operation == joinedauth.OperationPublish {
		// Bind the live scope before accepting a stale publication token.
		err = tx.QueryRow(r.Context(), `SELECT a.batch_record_id,a.id,a.publication_attempt_count,a.publication_lease_expires_at
			FROM recording_joined_artifacts a LEFT JOIN recording_joined_hours h ON h.id=a.hour_record_id
			WHERE a.batch_id=$1 AND a.scope_kind=$2 AND a.scope_id=$3 AND a.artifact_kind<>'media'
			  AND a.publication_state='publishing' AND a.publication_token=$4 AND a.publication_lease_expires_at>now()
			  AND a.connection_id=$5
			  AND (a.artifact_kind<>'batch_index' OR ($7='frozen_batch' AND NOT EXISTS(SELECT 1
			    FROM recording_joined_batch_index_refs ref
			    JOIN recording_joined_artifacts target ON target.id=ref.referenced_artifact_id
			    JOIN recording_joined_hours gap_hour ON gap_hour.id=target.hour_record_id
			    WHERE ref.index_artifact_id=a.id AND ref.reference_kind='hour_manifest' AND gap_hour.source_clip_count=0
			      AND NOT EXISTS(SELECT 1 FROM recording_joined_gap_only_scope_authorizations ga
			        WHERE ga.artifact_id=target.id AND ga.batch_record_id=target.batch_record_id
			          AND ga.batch_id=target.batch_id AND ga.hour_record_id=target.hour_record_id
			          AND ga.hour_id=target.scope_id AND ga.work_scope='frozen_batch'
			          AND ga.work_scope_identity_sha256=$6
			          AND ga.authorization_source IN ('server_seal','operator_frozen')))))
			  AND (a.artifact_kind<>'hour_manifest' OR h.source_clip_count>0 OR EXISTS(SELECT 1
			    FROM recording_joined_gap_only_scope_authorizations ga WHERE ga.artifact_id=a.id
			      AND ga.batch_record_id=a.batch_record_id AND ga.batch_id=a.batch_id
			      AND ga.hour_record_id=a.hour_record_id AND ga.hour_id=a.scope_id
			      AND ga.work_scope=$7 AND ga.work_scope_identity_sha256=$6
			      AND (($7='frozen_batch' AND ga.authorization_source IN ('server_seal','operator_frozen'))
			        OR ($7 IN ('canary','canary_single','allowlist_50') AND ga.authorization_source='server_seal'))))
			FOR UPDATE OF a`, claims.BatchID, req.ScopeKind, req.ScopeID, claimToken, s.cfg.JoinedRecordingConnectionID,
			workScopeSHA, workScopeName).
			Scan(&batchRecordID, &targetID, &attempt, &leaseExpires)
	} else {
		err = pgx.ErrNoRows
	}
	if err != nil {
		util.WriteError(w, http.StatusConflict, "joined failure lease is stale or foreign")
		return
	}
	disposition, retry := joinedFailureDispositionAfterLease(req.FailureClass, attempt, claimToken, leaseExpires, time.Now().UTC())
	if disposition == "retry" {
		retryAt = &retry
	}
	var hourID, artifactID any
	if claims.Operation == joinedauth.OperationPreflight {
		hourID = targetID
	} else {
		artifactID = targetID
	}
	_, err = tx.Exec(r.Context(), `INSERT INTO recording_joined_worker_failures
		(batch_record_id,hour_record_id,artifact_id,batch_id,scope_kind,scope_id,claim_token,attempt_count,
		 failure_class,reason_code,disposition,retry_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, batchRecordID, hourID, artifactID,
		claims.BatchID, req.ScopeKind, req.ScopeID, claimToken, attempt, req.FailureClass, req.ReasonCode, disposition, retryAt)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "record joined failure")
		return
	}
	if disposition == "terminal" {
		if claims.Operation == joinedauth.OperationPreflight {
			_, err = tx.Exec(r.Context(), `UPDATE recording_joined_hours SET state='terminal_failed',claim_token=NULL,
				claimed_by=NULL,lease_expires_at=NULL,heartbeat_at=NULL,failure_reason_code=$2 WHERE id=$1 AND connection_id=$3`, targetID, req.ReasonCode, s.cfg.JoinedRecordingConnectionID)
		} else {
			_, err = tx.Exec(r.Context(), `UPDATE recording_joined_artifacts SET publication_state='terminal_failed',
				publication_token=NULL,publication_claimed_by=NULL,publication_lease_expires_at=NULL,
				publication_heartbeat_at=NULL,failure_reason_code=$2 WHERE id=$1 AND connection_id=$3`, targetID, req.ReasonCode, s.cfg.JoinedRecordingConnectionID)
		}
		if err != nil {
			util.WriteError(w, http.StatusConflict, "terminate joined failure")
			return
		}
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusConflict, "commit joined failure")
		return
	}
	util.WriteJSON(w, http.StatusOK, joinedrecording.WorkFailureResponse{ProtocolVersion: 1,
		State: disposition, AttemptCount: attempt, NextAttemptAt: retryAt})
}

func (s *Server) handleJoinedLeaseStatus(w http.ResponseWriter, r *http.Request) {
	var req joinedrecording.LeaseStatusRequest
	if err := util.DecodeJSON(r, &req); err != nil || req.Validate() != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid joined lease status request")
		return
	}
	if !s.joinedControlPlaneReady() || req.BatchID != s.cfg.JoinedRecordingBatchID {
		util.WriteError(w, http.StatusForbidden, "joined lease status scope differs")
		return
	}
	rows, err := s.pool.Query(r.Context(), `SELECT lease_id,expires_at FROM (
		SELECT h.claim_token AS lease_id,h.lease_expires_at AS expires_at FROM recording_joined_hours h
		JOIN recording_joined_batches b ON b.id=h.batch_record_id AND b.connection_id=$2
		WHERE h.batch_id=$1 AND h.state='leased' AND h.claim_token IS NOT NULL
		UNION ALL SELECT a.publication_token,a.publication_lease_expires_at FROM recording_joined_artifacts a
		JOIN recording_joined_batches b ON b.id=a.batch_record_id AND b.connection_id=$2
		WHERE a.batch_id=$1 AND a.publication_state='publishing' AND a.publication_token IS NOT NULL) active`,
		req.BatchID, s.cfg.JoinedRecordingConnectionID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "read joined lease status")
		return
	}
	defer rows.Close()
	active := make(map[string]time.Time)
	for rows.Next() {
		var id uuid.UUID
		var expires time.Time
		if err := rows.Scan(&id, &expires); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "scan joined lease status")
			return
		}
		active[joinedauth.LeaseID(id)] = expires
	}
	if rows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, "iterate joined lease status")
		return
	}
	response := joinedrecording.LeaseStatusResponse{ProtocolVersion: 1, Leases: make([]joinedrecording.LeaseStatus, 0, len(req.LeaseIDs))}
	now := time.Now().UTC()
	for _, leaseID := range req.LeaseIDs {
		status := joinedrecording.LeaseStatus{LeaseID: leaseID}
		if expires, ok := active[leaseID]; ok {
			status.ExpiresAt = &expires
			status.Active = expires.After(now)
		}
		response.Leases = append(response.Leases, status)
	}
	util.WriteJSON(w, http.StatusOK, response)
}

func loadJoinedHourPublicationClaim(ctx context.Context, tx pgx.Tx, manifestID int64, leaseToken uuid.UUID,
	operationToken string, leaseExpires time.Time, authority, bucket string) (joinedrecording.WorkerClaim, error) {
	var claim joinedrecording.WorkerClaim
	var planJSON, ledgerJSON, manifestJSON []byte
	err := tx.QueryRow(ctx, `SELECT h.hour_id,h.canonical_plan,d.ledger_bytes,a.expected_size_bytes,a.expected_sha256,a.canonical_bytes
		FROM recording_joined_artifacts a JOIN recording_joined_hours h ON h.id=a.hour_record_id
		JOIN recording_joined_stream_days d ON d.id=a.stream_day_id WHERE a.id=$1`, manifestID).
		Scan(&claim.HourID, &planJSON, &ledgerJSON, &claim.HourManifestExpectedSize, &claim.HourManifestExpectedSHA, &manifestJSON)
	if err != nil || json.Unmarshal(planJSON, &claim.Plan) != nil || json.Unmarshal(ledgerJSON, &claim.AllocationLedger) != nil {
		return joinedrecording.WorkerClaim{}, errors.New("decode sealed joined hour")
	}
	var manifest joinedrecording.HourManifest
	if json.Unmarshal(manifestJSON, &manifest) != nil {
		return joinedrecording.WorkerClaim{}, errors.New("decode joined hour manifest")
	}
	claim.ProtocolVersion, claim.LeaseID, claim.OperationToken, claim.LeaseExpires = joinedWorkerProtocolVersion,
		joinedauth.LeaseID(leaseToken), operationToken, leaseExpires
	claim.StorageAuthority, claim.StorageBucket = authority, bucket
	claim.Allocation = manifest.Allocation
	claim.HourManifest = manifest
	claim.HourManifestArtifactID = manifestID
	rows, err := tx.Query(ctx, `SELECT id FROM recording_joined_artifacts WHERE hour_record_id=(SELECT hour_record_id
		FROM recording_joined_artifacts WHERE id=$1) AND artifact_kind='media' ORDER BY ordinal`, manifestID)
	if err != nil {
		return joinedrecording.WorkerClaim{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return joinedrecording.WorkerClaim{}, err
		}
		claim.MediaArtifactIDs = append(claim.MediaArtifactIDs, id)
	}
	return claim, rows.Err()
}

func (s *Server) joinedPublishedObjectMatches(ctx context.Context, objectKey, etag, versionID string, sizeBytes int64) bool {
	store := s.joinedOutputStore()
	if store == nil || objectKey == "" || etag == "" || sizeBytes <= 0 {
		return false
	}
	head, err := store.Head(ctx, objectKey)
	return err == nil && head.ETag == etag && head.VersionID == versionID && head.SizeBytes == sizeBytes
}

func (s *Server) handleJoinedFinalizeLedger(w http.ResponseWriter, r *http.Request) {
	var req joinedrecording.FinalizeLedgerRequest
	if err := util.DecodeJSON(r, &req); err != nil || req.Validate() != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid joined ledger finalize")
		return
	}
	// Operation tokens deliberately carry no rollout scope. Bind the current
	// server scope once for both identity validation and gap-only sealing so a
	// single request cannot observe two different scope decisions.
	workScope, err := s.joinedWorkScopeIdentity()
	if err != nil {
		util.WriteError(w, http.StatusConflict, "joined work scope is invalid")
		return
	}
	if err := s.validateJoinedRootFinalizeIdentity(r.Context(), joinedauth.SubjectLedger, req.Published.ArtifactID,
		req.Published.ObjectKey, req.Published.SizeBytes, req.Published.SHA256); err != nil {
		util.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	if !s.joinedPublishedObjectMatches(r.Context(), req.Published.ObjectKey, req.Published.ETag,
		req.Published.VersionID, req.Published.SizeBytes) {
		util.WriteError(w, http.StatusConflict, "joined ledger storage identity differs")
		return
	}
	if err := s.finalizeJoinedRoot(r.Context(), joinedauth.SubjectLedger, req.Published.ArtifactID,
		req.Published.ObjectKey, req.Published.ETag, req.Published.VersionID, req.Published.SizeBytes, req.Published.SHA256, workScope); err != nil {
		util.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleJoinedFinalizeBatchIndex(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), joinedBatchIndexOperationTimeout)
	defer cancel()
	r = r.WithContext(ctx)
	var req joinedrecording.FinalizeBatchIndexRequest
	if err := util.DecodeJSON(r, &req); err != nil || req.Validate() != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid joined batch index finalize")
		return
	}
	if err := s.validateJoinedRootFinalizeIdentity(r.Context(), joinedauth.SubjectBatchIndex, req.Published.ArtifactID,
		req.Published.ObjectKey, req.Published.SizeBytes, req.Published.SHA256); err != nil {
		util.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	if !s.joinedPublishedObjectMatches(r.Context(), req.Published.ObjectKey, req.Published.ETag,
		req.Published.VersionID, req.Published.SizeBytes) {
		util.WriteError(w, http.StatusConflict, "joined batch index storage identity differs")
		return
	}
	if err := s.finalizeJoinedRoot(r.Context(), joinedauth.SubjectBatchIndex, req.Published.ArtifactID,
		req.Published.ObjectKey, req.Published.ETag, req.Published.VersionID, req.Published.SizeBytes, req.Published.SHA256,
		joinedrecording.WorkScopeIdentity{}); err != nil {
		util.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) validateJoinedRootFinalizeIdentity(ctx context.Context, scopeKind string, artifactID int64,
	objectKey string, sizeBytes int64, sha256 string) error {
	if !s.joinedControlPlaneReady() {
		return errors.New("joined recording is disabled")
	}
	claims, ok := joinedWorkerClaimsFromContext(ctx)
	if !ok || claims.Kind != joinedauth.KindOperation || claims.Operation != joinedauth.OperationPublish ||
		claims.SubjectKind != scopeKind {
		return errors.New("joined publication token scope differs")
	}
	lease, err := joinedCapabilityToken(claims.LeaseToken)
	if err != nil {
		return errors.New("joined publication lease differs")
	}
	currentScope, err := s.joinedWorkScopeIdentity()
	if err != nil {
		return errors.New("joined work scope is unavailable")
	}
	currentScopeSHA, err := currentScope.SHA256(claims.BatchID)
	if err != nil {
		return errors.New("joined work scope is unavailable")
	}
	var valid bool
	err = s.pool.QueryRow(ctx, `SELECT a.scope_id=$4 AND a.object_key=$5 AND a.expected_size_bytes=$6 AND a.expected_sha256=$7
		AND ((a.publication_state='publishing' AND a.publication_token=$8 AND a.publication_lease_expires_at>now())
		  OR (a.publication_state='published' AND a.finalized_token=$8))
		FROM recording_joined_artifacts a JOIN connections c ON c.id=a.connection_id
		WHERE a.id=$1 AND a.artifact_kind<>'media' AND a.scope_kind=$2 AND a.batch_id=$3 AND c.id=$9
		  AND (a.artifact_kind<>'batch_index' OR ($11='frozen_batch' AND NOT EXISTS(SELECT 1
		    FROM recording_joined_batch_index_refs ref
		    JOIN recording_joined_artifacts target ON target.id=ref.referenced_artifact_id
		    JOIN recording_joined_hours gap_hour ON gap_hour.id=target.hour_record_id
		    WHERE ref.index_artifact_id=a.id AND ref.reference_kind='hour_manifest' AND gap_hour.source_clip_count=0
		      AND NOT EXISTS(SELECT 1 FROM recording_joined_gap_only_scope_authorizations ga
		        WHERE ga.artifact_id=target.id AND ga.batch_record_id=target.batch_record_id
		          AND ga.batch_id=target.batch_id AND ga.hour_record_id=target.hour_record_id
		          AND ga.hour_id=target.scope_id AND ga.work_scope='frozen_batch'
		          AND ga.work_scope_identity_sha256=$10
		          AND ga.authorization_source IN ('server_seal','operator_frozen')))))
		FOR SHARE OF a,c`, artifactID, scopeKind, claims.BatchID, claims.SubjectID, objectKey, sizeBytes, sha256, lease,
		s.cfg.JoinedRecordingConnectionID, currentScopeSHA, currentScope.WorkScope).Scan(&valid)
	if err != nil || !valid {
		return errors.New("joined publication identity differs")
	}
	return nil
}

func (s *Server) finalizeJoinedRoot(ctx context.Context, scopeKind string, artifactID int64, objectKey, etag,
	versionID string, sizeBytes int64, sha256 string, workScope joinedrecording.WorkScopeIdentity) error {
	if scopeKind == joinedauth.SubjectBatchIndex {
		return s.finalizeJoinedBatchIndexCanonical(ctx, artifactID, objectKey, etag, versionID, sizeBytes, sha256)
	}
	if !s.joinedControlPlaneReady() {
		return errors.New("joined recording is disabled")
	}
	claims, ok := joinedWorkerClaimsFromContext(ctx)
	if !ok || claims.Kind != joinedauth.KindOperation || claims.Operation != joinedauth.OperationPublish ||
		claims.SubjectKind != scopeKind {
		return errors.New("joined publication token scope differs")
	}
	if scopeKind == joinedauth.SubjectLedger {
		if err := workScope.Validate(claims.BatchID); err != nil {
			return errors.New("joined work scope is invalid")
		}
	}
	lease, err := joinedCapabilityToken(claims.LeaseToken)
	if err != nil {
		return errors.New("joined publication lease differs")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var state, scopeID, expectedKey, expectedSHA, currentETag, currentVersion string
	var expectedSize int64
	var currentFinalized *uuid.UUID
	err = tx.QueryRow(ctx, `SELECT a.publication_state,a.scope_id,a.object_key,a.expected_size_bytes,a.expected_sha256,
		COALESCE(a.etag,''),COALESCE(a.version_id,''),a.finalized_token
		FROM recording_joined_artifacts a JOIN connections c ON c.id=a.connection_id
		WHERE a.id=$1 AND a.artifact_kind<>'media' AND a.scope_kind=$2 AND a.batch_id=$3 AND c.id=$4 FOR UPDATE OF a,c`,
		artifactID, scopeKind, claims.BatchID, s.cfg.JoinedRecordingConnectionID).Scan(&state, &scopeID, &expectedKey, &expectedSize, &expectedSHA,
		&currentETag, &currentVersion, &currentFinalized)
	if err != nil || scopeID != claims.SubjectID || expectedKey != objectKey || expectedSize != sizeBytes || expectedSHA != sha256 {
		return errors.New("joined publication identity differs")
	}
	if state == "published" {
		if currentFinalized == nil || *currentFinalized != lease || currentETag != etag || currentVersion != versionID {
			return errors.New("joined publication retry differs")
		}
		if scopeKind == joinedauth.SubjectLedger {
			if err := sealJoinedGapOnlyHoursTx(ctx, tx, artifactID, workScope, s.cfg.JoinedRecordingConnectionID); err != nil {
				return errors.New("seal joined gap-only hours")
			}
		}
		return tx.Commit(ctx)
	}
	if state != "publishing" {
		return errors.New("joined publication lease is stale")
	}
	var updated int64
	if err := tx.QueryRow(ctx, `UPDATE recording_joined_artifacts SET publication_state='published',
		publication_token=NULL,publication_claimed_by=NULL,publication_lease_expires_at=NULL,publication_heartbeat_at=NULL,
		finalized_token=$2,etag=$3,version_id=$4,published_at=now()
		WHERE id=$1 AND publication_token=$2 AND publication_lease_expires_at>now()
		  AND connection_id=$5
		RETURNING id`, artifactID, lease, etag, versionID, s.cfg.JoinedRecordingConnectionID).Scan(&updated); err != nil {
		return errors.New("joined publication lease is stale")
	}
	if scopeKind == joinedauth.SubjectLedger {
		if err := sealJoinedGapOnlyHoursTx(ctx, tx, artifactID, workScope, s.cfg.JoinedRecordingConnectionID); err != nil {
			return errors.New("seal joined gap-only hours")
		}
	}
	return tx.Commit(ctx)
}

func (s *Server) finalizeJoinedBatchIndexCanonical(ctx context.Context, artifactID int64, objectKey, etag,
	versionID string, sizeBytes int64, sha256 string) error {
	ctx, cancel := context.WithTimeout(ctx, joinedBatchIndexOperationTimeout)
	defer cancel()
	if !s.joinedControlPlaneReady() || !s.joinedFrozenBatchScope() {
		return errors.New("joined frozen-batch work is disabled")
	}
	claims, ok := joinedWorkerClaimsFromContext(ctx)
	if !ok || claims.Kind != joinedauth.KindOperation || claims.Operation != joinedauth.OperationPublish ||
		claims.SubjectKind != joinedauth.SubjectBatchIndex || claims.BatchID != s.cfg.JoinedRecordingBatchID {
		return errors.New("joined batch-index token scope differs")
	}
	lease, err := joinedCapabilityToken(claims.LeaseToken)
	if err != nil {
		return errors.New("joined batch-index lease differs")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := configureJoinedBatchIndexTransaction(ctx, tx); err != nil {
		return err
	}
	canonical, batchState, existingID, err := loadJoinedCanonicalBatchIndex(ctx, tx, claims.BatchID,
		s.cfg.JoinedRecordingConnectionID, true)
	if err != nil || existingID != artifactID || (batchState != "index_sealed" && batchState != "published") ||
		canonical.SHA256 != sha256 || int64(len(canonical.Bytes)) != sizeBytes {
		return errors.New("joined batch-index canonical evidence differs")
	}
	var state, scopeID, expectedKey, expectedSHA, currentETag, currentVersion string
	var expectedSize int64
	var currentFinalized *uuid.UUID
	err = tx.QueryRow(ctx, `SELECT publication_state,scope_id,object_key,expected_size_bytes,expected_sha256,
		COALESCE(etag,''),COALESCE(version_id,''),finalized_token FROM recording_joined_artifacts
		WHERE id=$1 AND batch_record_id=$2 AND artifact_kind='batch_index' FOR UPDATE`, artifactID, canonical.BatchRecordID).
		Scan(&state, &scopeID, &expectedKey, &expectedSize, &expectedSHA, &currentETag, &currentVersion, &currentFinalized)
	if err != nil || scopeID != claims.SubjectID || expectedKey != objectKey || expectedSize != sizeBytes || expectedSHA != sha256 {
		return errors.New("joined batch-index publication identity differs")
	}
	if state == "published" {
		if batchState != "published" || currentFinalized == nil || *currentFinalized != lease || currentETag != etag || currentVersion != versionID {
			return errors.New("joined batch-index publication retry differs")
		}
		return tx.Commit(ctx)
	}
	if state != "publishing" || batchState != "index_sealed" {
		return errors.New("joined batch-index publication lease is stale")
	}
	var lockedConnection int64
	if err := tx.QueryRow(ctx, `SELECT id FROM connections WHERE id=$1 FOR SHARE`,
		canonical.ConnectionID).Scan(&lockedConnection); err != nil {
		return errors.New("joined batch-index protocol changed")
	}
	var updated int64
	if err := tx.QueryRow(ctx, `UPDATE recording_joined_artifacts SET publication_state='published',publication_token=NULL,
		publication_claimed_by=NULL,publication_lease_expires_at=NULL,publication_heartbeat_at=NULL,finalized_token=$2,
		etag=$3,version_id=$4,published_at=now() WHERE id=$1 AND publication_token=$2 AND publication_lease_expires_at>now()
		RETURNING id`, artifactID, lease, etag, versionID).Scan(&updated); err != nil {
		return errors.New("joined batch-index publication lease is stale")
	}
	command, err := tx.Exec(ctx, `UPDATE recording_joined_batches SET state='published',published_at=now()
		WHERE id=$1 AND state='index_sealed' AND index_artifact_id=$2`, canonical.BatchRecordID, artifactID)
	if err != nil || command.RowsAffected() != 1 {
		return errors.New("joined batch-index state differs")
	}
	return tx.Commit(ctx)
}

func (s *Server) handleJoinedFinalizeHour(w http.ResponseWriter, r *http.Request) {
	var req joinedrecording.FinalizeHourRequest
	if err := util.DecodeJSON(r, &req); err != nil || req.Validate() != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid joined hour finalize")
		return
	}
	if err := s.validateJoinedHourFinalizeIdentity(r.Context(), req.Published); err != nil {
		util.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	for _, output := range req.Published.Outputs {
		if !s.joinedPublishedObjectMatches(r.Context(), output.ObjectKey, output.ETag, output.VersionID, output.SizeBytes) {
			util.WriteError(w, http.StatusConflict, "joined media storage identity differs")
			return
		}
	}
	if !s.joinedPublishedObjectMatches(r.Context(), req.Published.HourManifestObjectKey, req.Published.HourManifestETag,
		req.Published.HourManifestVersionID, req.Published.HourManifestSizeBytes) {
		util.WriteError(w, http.StatusConflict, "joined hour manifest storage identity differs")
		return
	}
	if err := s.finalizeJoinedHour(r.Context(), req.Published); err != nil {
		util.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) validateJoinedHourFinalizeIdentity(ctx context.Context, published joinedrecording.PublishedHour) error {
	if !s.joinedControlPlaneReady() {
		return errors.New("joined recording is disabled")
	}
	claims, ok := joinedWorkerClaimsFromContext(ctx)
	if !ok || claims.Kind != joinedauth.KindOperation || claims.Operation != joinedauth.OperationPublish ||
		claims.SubjectKind != joinedauth.SubjectHour || claims.SubjectID != published.HourID {
		return errors.New("joined hour publication token scope differs")
	}
	lease, err := joinedCapabilityToken(claims.LeaseToken)
	if err != nil {
		return errors.New("joined hour publication lease differs")
	}
	currentScope, err := s.joinedWorkScopeIdentity()
	if err != nil {
		return errors.New("joined work scope is unavailable")
	}
	currentScopeSHA, err := currentScope.SHA256(claims.BatchID)
	if err != nil {
		return errors.New("joined work scope is unavailable")
	}
	var manifestID int64
	var key, sha, localDate string
	var size, recordingID int64
	var localHour int
	var valid bool
	err = s.pool.QueryRow(ctx, `SELECT root.id,root.object_key,root.expected_size_bytes,root.expected_sha256,
		h.recording_id,h.local_date::text,h.delivery_hour,
		((root.publication_state='publishing' AND root.publication_token=$3 AND root.publication_lease_expires_at>now())
		 OR (root.publication_state='published' AND root.finalized_token=$3))
		FROM recording_joined_artifacts root JOIN recording_joined_hours h ON h.id=root.hour_record_id
		JOIN connections c ON c.id=root.connection_id
		WHERE root.batch_id=$1 AND root.scope_id=$2 AND root.artifact_kind='hour_manifest' AND c.id=$4
		  AND (h.source_clip_count>0 OR EXISTS(SELECT 1 FROM recording_joined_gap_only_scope_authorizations ga
		    WHERE ga.artifact_id=root.id AND ga.batch_record_id=root.batch_record_id AND ga.batch_id=root.batch_id
		      AND ga.hour_record_id=root.hour_record_id AND ga.hour_id=root.scope_id
		      AND ga.work_scope=$6 AND ga.work_scope_identity_sha256=$5
		      AND (($6='frozen_batch' AND ga.authorization_source IN ('server_seal','operator_frozen'))
		        OR ($6 IN ('canary','canary_single','allowlist_50') AND ga.authorization_source='server_seal'))))

		FOR SHARE OF root,h,c`, claims.BatchID, published.HourID, lease, s.cfg.JoinedRecordingConnectionID, currentScopeSHA,
		currentScope.WorkScope).Scan(
		&manifestID, &key, &size, &sha, &recordingID, &localDate, &localHour, &valid)
	if err != nil || !valid || key != published.HourManifestObjectKey || size != published.HourManifestSizeBytes ||
		sha != published.HourManifestSHA256 || recordingID != published.RecordingID || localDate != published.LocalDate ||
		localHour != published.LocalHour {
		return errors.New("joined hour publication identity differs")
	}
	rows, err := s.pool.Query(ctx, `SELECT id,object_key,expected_size_bytes,expected_sha256 FROM recording_joined_artifacts
		WHERE hour_record_id=(SELECT hour_record_id FROM recording_joined_artifacts WHERE id=$1)
		  AND artifact_kind='media' ORDER BY ordinal`, manifestID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for i := 0; rows.Next(); i++ {
		if i >= len(published.Outputs) {
			return errors.New("joined hour media cardinality differs")
		}
		var id, expectedSize int64
		var expectedKey, expectedSHA string
		if err := rows.Scan(&id, &expectedKey, &expectedSize, &expectedSHA); err != nil {
			return err
		}
		got := published.Outputs[i]
		if got.ArtifactID != id || got.ObjectKey != expectedKey || got.SizeBytes != expectedSize || got.SHA256 != expectedSHA {
			return errors.New("joined hour media identity differs")
		}
		if i == len(published.Outputs)-1 {
			if rows.Next() {
				return errors.New("joined hour media cardinality differs")
			}
			return rows.Err()
		}
	}
	if len(published.Outputs) != 0 {
		return errors.New("joined hour media cardinality differs")
	}
	return rows.Err()
}

func (s *Server) finalizeJoinedHour(ctx context.Context, published joinedrecording.PublishedHour) error {
	if !s.joinedControlPlaneReady() {
		return errors.New("joined recording is disabled")
	}
	claims, ok := joinedWorkerClaimsFromContext(ctx)
	if !ok || claims.Kind != joinedauth.KindOperation || claims.Operation != joinedauth.OperationPublish ||
		claims.SubjectKind != joinedauth.SubjectHour || claims.SubjectID != published.HourID {
		return errors.New("joined hour publication token scope differs")
	}
	lease, err := joinedCapabilityToken(claims.LeaseToken)
	if err != nil {
		return errors.New("joined hour publication lease differs")
	}
	currentScope, err := s.joinedWorkScopeIdentity()
	if err != nil {
		return errors.New("joined work scope is unavailable")
	}
	currentScopeSHA, err := currentScope.SHA256(claims.BatchID)
	if err != nil {
		return errors.New("joined work scope is unavailable")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var manifestID, recordingID int64
	var state, manifestKey, manifestSHA, currentETag, currentVersion string
	var localDate string
	var localHour int
	var manifestSize int64
	var finalized *uuid.UUID
	err = tx.QueryRow(ctx, `SELECT a.id,a.publication_state,a.object_key,a.expected_size_bytes,a.expected_sha256,
		COALESCE(a.etag,''),COALESCE(a.version_id,''),a.finalized_token,h.recording_id,h.local_date::text,h.delivery_hour
		FROM recording_joined_artifacts a JOIN recording_joined_hours h ON h.id=a.hour_record_id
		JOIN connections c ON c.id=a.connection_id
		WHERE a.scope_kind='hour' AND a.scope_id=$1 AND a.batch_id=$2 AND a.artifact_kind='hour_manifest' AND c.id=$3
		  AND (h.source_clip_count>0 OR EXISTS(SELECT 1 FROM recording_joined_gap_only_scope_authorizations ga
		    WHERE ga.artifact_id=a.id AND ga.batch_record_id=a.batch_record_id AND ga.batch_id=a.batch_id
		      AND ga.hour_record_id=a.hour_record_id AND ga.hour_id=a.scope_id
		      AND ga.work_scope=$5 AND ga.work_scope_identity_sha256=$4
		      AND (($5='frozen_batch' AND ga.authorization_source IN ('server_seal','operator_frozen'))
		        OR ($5 IN ('canary','canary_single','allowlist_50') AND ga.authorization_source='server_seal'))))
		FOR UPDATE OF a,c`, published.HourID, claims.BatchID, s.cfg.JoinedRecordingConnectionID, currentScopeSHA,
		currentScope.WorkScope).Scan(&manifestID, &state, &manifestKey, &manifestSize,
		&manifestSHA, &currentETag, &currentVersion, &finalized, &recordingID, &localDate, &localHour)
	if err != nil || manifestKey != published.HourManifestObjectKey || manifestSize != published.HourManifestSizeBytes ||
		manifestSHA != published.HourManifestSHA256 || recordingID != published.RecordingID || localDate != published.LocalDate ||
		localHour != published.LocalHour {
		return errors.New("joined hour manifest identity differs")
	}
	if state != "publishing" && state != "published" {
		return errors.New("joined hour publication lease is stale")
	}
	rows, err := tx.Query(ctx, `SELECT id,object_key,expected_size_bytes,expected_sha256,
		COALESCE(etag,''),COALESCE(version_id,''),finalized_token FROM recording_joined_artifacts
		WHERE hour_record_id=(SELECT hour_record_id FROM recording_joined_artifacts WHERE id=$1)
		  AND artifact_kind='media' ORDER BY ordinal FOR UPDATE`, manifestID)
	if err != nil {
		return err
	}
	type expectedOutput struct {
		id, size                int64
		key, sha, etag, version string
		finalized               *uuid.UUID
	}
	expected := []expectedOutput{}
	for rows.Next() {
		var out expectedOutput
		if err := rows.Scan(&out.id, &out.key, &out.size, &out.sha, &out.etag, &out.version, &out.finalized); err != nil {
			rows.Close()
			return err
		}
		expected = append(expected, out)
	}
	rows.Close()
	if len(expected) != len(published.Outputs) {
		return errors.New("joined hour media cardinality differs")
	}
	if state == "published" {
		if finalized == nil || *finalized != lease || currentETag != published.HourManifestETag ||
			currentVersion != published.HourManifestVersionID {
			return errors.New("joined hour publication retry differs")
		}
		for i, output := range published.Outputs {
			want := expected[i]
			if output.ArtifactID != want.id || output.ObjectKey != want.key || output.SizeBytes != want.size ||
				output.SHA256 != want.sha || output.ETag != want.etag || output.VersionID != want.version ||
				want.finalized == nil || *want.finalized != lease {
				return errors.New("joined hour media retry differs")
			}
		}
		return tx.Commit(ctx)
	}
	for i, output := range published.Outputs {
		want := expected[i]
		if output.ArtifactID != want.id || output.ObjectKey != want.key || output.SizeBytes != want.size || output.SHA256 != want.sha {
			return errors.New("joined hour media identity differs")
		}
		if _, err := tx.Exec(ctx, `UPDATE recording_joined_artifacts SET finalized_token=$2,etag=$3,version_id=$4,published_at=now()
		  WHERE id=$1 AND published_at IS NULL AND connection_id=$5`, want.id, lease, output.ETag, output.VersionID, s.cfg.JoinedRecordingConnectionID); err != nil {
			return err
		}
	}
	var updated int64
	if err := tx.QueryRow(ctx, `UPDATE recording_joined_artifacts SET publication_state='published',
		publication_token=NULL,publication_claimed_by=NULL,publication_lease_expires_at=NULL,publication_heartbeat_at=NULL,
		finalized_token=$2,etag=$3,version_id=$4,published_at=now()
		WHERE id=$1 AND publication_token=$2 AND publication_lease_expires_at>now()
		  AND connection_id=$5
		RETURNING id`, manifestID, lease, published.HourManifestETag, published.HourManifestVersionID, s.cfg.JoinedRecordingConnectionID).Scan(&updated); err != nil {
		return errors.New("joined hour publication lease is stale")
	}
	return tx.Commit(ctx)
}

type joinedHeartbeatRequest = joinedrecording.HeartbeatRequest

func (s *Server) handleJoinedHeartbeat(w http.ResponseWriter, r *http.Request) {
	if !s.joinedControlPlaneReady() {
		util.WriteError(w, http.StatusServiceUnavailable, "joined recording is disabled")
		return
	}
	var req joinedHeartbeatRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := req.Validate(); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	claims, ok := joinedWorkerClaimsFromContext(r.Context())
	if !ok || claims.Kind != joinedauth.KindOperation || claims.SubjectKind != req.ScopeKind || claims.SubjectID != req.ScopeID {
		util.WriteError(w, http.StatusForbidden, "joined worker token scope differs")
		return
	}
	token, err := joinedCapabilityToken(claims.LeaseToken)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid joined heartbeat lease")
		return
	}
	var workScopeSHA, workScopeName string
	if claims.Operation == joinedauth.OperationPublish {
		workScope, scopeErr := s.joinedWorkScopeIdentity()
		if scopeErr != nil {
			util.WriteError(w, http.StatusServiceUnavailable, "joined work scope is unavailable")
			return
		}
		workScopeSHA, scopeErr = workScope.SHA256(claims.BatchID)
		if scopeErr != nil {
			util.WriteError(w, http.StatusServiceUnavailable, "joined work scope is unavailable")
			return
		}
		workScopeName = workScope.WorkScope
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin joined heartbeat: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	var locked int
	err = tx.QueryRow(r.Context(), `
		SELECT c.id FROM connections c
		JOIN recording_joined_batches b ON b.connection_id=c.id
		WHERE b.batch_id=$1 AND c.id=$4 AND
		  (($2='hour' AND EXISTS(SELECT 1 FROM recording_joined_hours h WHERE h.batch_record_id=b.id AND h.hour_id=$3))
		    OR ($2<>'hour' AND EXISTS(SELECT 1 FROM recording_joined_artifacts a
		      WHERE a.batch_record_id=b.id AND a.scope_kind=$2 AND a.scope_id=$3)))
		FOR UPDATE OF c`, claims.BatchID, req.ScopeKind, req.ScopeID, s.cfg.JoinedRecordingConnectionID).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusConflict, "joined heartbeat lease is stale or foreign")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("lock joined heartbeat: %v", err))
		return
	}
	var leaseExpires time.Time
	if claims.Operation == joinedauth.OperationPreflight && req.ScopeKind == joinedauth.SubjectHour {
		err = tx.QueryRow(r.Context(), `UPDATE recording_joined_hours SET
		  lease_expires_at=GREATEST(lease_expires_at+interval '1 second',date_trunc('second',now()+$3::interval)),heartbeat_at=now()
		  WHERE hour_id=$1 AND batch_id=$4 AND state='leased' AND claim_token=$2 AND lease_expires_at>now()
		    AND connection_id=$5
		  RETURNING lease_expires_at`, req.ScopeID, token, joinedLeaseDuration.String(), claims.BatchID, s.cfg.JoinedRecordingConnectionID).Scan(&leaseExpires)
	} else if claims.Operation == joinedauth.OperationPublish {
		err = tx.QueryRow(r.Context(), `UPDATE recording_joined_artifacts a SET
		  publication_lease_expires_at=GREATEST(a.publication_lease_expires_at+interval '1 second',date_trunc('second',now()+$4::interval)),publication_heartbeat_at=now()
		  WHERE a.scope_kind=$1 AND a.scope_id=$2 AND a.batch_id=$5 AND a.artifact_kind<>'media'
		    AND a.publication_state='publishing' AND a.publication_token=$3 AND a.publication_lease_expires_at>now()
		    AND a.connection_id=$6
		    AND (a.artifact_kind<>'batch_index' OR ($8='frozen_batch' AND NOT EXISTS(SELECT 1
		      FROM recording_joined_batch_index_refs ref
		      JOIN recording_joined_artifacts target ON target.id=ref.referenced_artifact_id
		      JOIN recording_joined_hours gap_hour ON gap_hour.id=target.hour_record_id
		      WHERE ref.index_artifact_id=a.id AND ref.reference_kind='hour_manifest' AND gap_hour.source_clip_count=0
		        AND NOT EXISTS(SELECT 1 FROM recording_joined_gap_only_scope_authorizations ga
		          WHERE ga.artifact_id=target.id AND ga.batch_record_id=target.batch_record_id
		            AND ga.batch_id=target.batch_id AND ga.hour_record_id=target.hour_record_id
		            AND ga.hour_id=target.scope_id AND ga.work_scope='frozen_batch'
		            AND ga.work_scope_identity_sha256=$7
		            AND ga.authorization_source IN ('server_seal','operator_frozen')))))
		    AND (a.artifact_kind<>'hour_manifest' OR COALESCE((SELECT source_clip_count FROM recording_joined_hours
		      WHERE id=a.hour_record_id),0)>0 OR EXISTS(SELECT 1
		      FROM recording_joined_gap_only_scope_authorizations ga WHERE ga.artifact_id=a.id
		        AND ga.batch_record_id=a.batch_record_id AND ga.batch_id=a.batch_id
		        AND ga.hour_record_id=a.hour_record_id AND ga.hour_id=a.scope_id
		        AND ga.work_scope=$8 AND ga.work_scope_identity_sha256=$7
		        AND (($8='frozen_batch' AND ga.authorization_source IN ('server_seal','operator_frozen'))
		          OR ($8 IN ('canary','canary_single','allowlist_50') AND ga.authorization_source='server_seal'))))
		  RETURNING a.publication_lease_expires_at`, req.ScopeKind, req.ScopeID, token, joinedLeaseDuration.String(),
			claims.BatchID, s.cfg.JoinedRecordingConnectionID, workScopeSHA, workScopeName).Scan(&leaseExpires)
	} else {
		err = pgx.ErrNoRows
	}
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusConflict, "joined heartbeat lease is stale or foreign")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("renew joined heartbeat: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusConflict, "joined heartbeat lease changed")
		return
	}
	jobToken, err := joinedauth.MintOperation(s.cfg.JoinedWorkerSigningKey, claims.BatchID, claims.SubjectKind,
		req.ScopeID, token, claims.Operation, leaseExpires)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("renew joined job token: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, joinedrecording.HeartbeatResponse{ProtocolVersion: joinedWorkerProtocolVersion,
		ScopeKind: req.ScopeKind, ScopeID: req.ScopeID, LeaseID: joinedauth.LeaseID(token),
		ExpiresAt: leaseExpires, OperationToken: jobToken})
}

func (s *Server) handleJoinedStatus(w http.ResponseWriter, r *http.Request) {
	batchIDs, ok := r.URL.Query()["batch_id"]
	if !ok || len(batchIDs) != 1 || len(r.URL.Query()) != 1 || !joinedBatchIDPattern.MatchString(batchIDs[0]) {
		util.WriteError(w, http.StatusBadRequest, "one canonical joined batch_id is required")
		return
	}
	batchID := batchIDs[0]
	if batchID != s.cfg.JoinedRecordingBatchID {
		util.WriteError(w, http.StatusNotFound, "joined batch not found")
		return
	}
	var authorized bool
	if err := s.pool.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM recording_joined_batches
		WHERE batch_id=$1 AND connection_id=$2)`, batchID, s.cfg.JoinedRecordingConnectionID).Scan(&authorized); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load joined status authority")
		return
	}
	if !authorized {
		util.WriteError(w, http.StatusNotFound, "joined batch not found")
		return
	}
	counts := map[string]int64{}
	rows, err := s.pool.Query(r.Context(), `SELECT h.state,count(*) FROM recording_joined_hours h
		JOIN recording_joined_batches b ON b.id=h.batch_record_id AND b.connection_id=$2
		WHERE h.batch_id=$1 GROUP BY h.state ORDER BY h.state`, batchID, s.cfg.JoinedRecordingConnectionID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load joined status: %v", err))
		return
	}
	defer rows.Close()
	for rows.Next() {
		var state string
		var count int64
		if err := rows.Scan(&state, &count); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan joined status: %v", err))
			return
		}
		counts[state] = count
	}
	workScope, _ := s.cfg.JoinedWorkScope()
	util.WriteJSON(w, http.StatusOK, map[string]any{"protocol_version": joinedWorkerProtocolVersion,
		"enabled":  s.joinedControlPlaneReady() && batchID == s.cfg.JoinedRecordingBatchID,
		"batch_id": batchID, "work_scope": workScope,
		"canary_hour_ids": s.joinedCanaryHourIDs(), "hours": counts})
}

type joinedConnectionStatusResponse struct {
	ConnectionID                    int64      `json:"connection_id"`
	ControlPlaneEnabled             bool       `json:"control_plane_enabled"`
	NASDeliveryEnabled              bool       `json:"nas_delivery_enabled"`
	ExpectedProtocolVersion         int        `json:"expected_protocol_version"`
	ExpectedProtocolGeneration      int        `json:"expected_protocol_generation"`
	ServerDesiredProtocolVersion    int        `json:"server_desired_protocol_version"`
	ServerDesiredProtocolGeneration int        `json:"server_desired_protocol_generation"`
	ObservedProtocolVersion         int        `json:"observed_protocol_version"`
	LastSeenAt                      *time.Time `json:"last_seen_at,omitempty"`
	HeartbeatAgeSeconds             *int64     `json:"heartbeat_age_seconds,omitempty"`
	HeartbeatStale                  bool       `json:"heartbeat_stale"`
	PollIntervalSeconds             int        `json:"poll_interval_seconds"`
	ClientVersion                   string     `json:"client_version,omitempty"`
	ClientPhase                     string     `json:"client_phase,omitempty"`
	ClientPreviousExit              string     `json:"client_previous_exit,omitempty"`
	ClientErrorClass                string     `json:"client_error_class,omitempty"`
	ClientErrorSHA256               string     `json:"client_error_sha256,omitempty"`
	ClientErrorAt                   *time.Time `json:"client_error_at,omitempty"`
	ClientErrorPresent              bool       `json:"client_error_present"`
}

func boundedJoinedDiagnosticText(value string) string {
	value = strings.TrimSpace(value)
	runes := []rune(value)
	if len(runes) > 128 {
		runes = runes[:128]
	}
	return string(runes)
}

func joinedClientErrorClass(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if strings.HasPrefix(value, "joined delivery:") {
		return "joined_delivery"
	}
	return "nas_pull"
}

func joinedClientErrorSHA256(value string) string {
	if value == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(value))
	return fmt.Sprintf("%x", sum)
}

// joinedConnectionStatusAllowed rate-limits the authenticated diagnostic to
// one read per instance every five seconds. The endpoint is intentionally
// narrow and read-only, but it must not become a high-rate database probe.
func (s *Server) joinedConnectionStatusAllowed(now time.Time) bool {
	s.joinedConnectionStatusMu.Lock()
	defer s.joinedConnectionStatusMu.Unlock()
	if !s.joinedConnectionStatusAt.IsZero() && now.Sub(s.joinedConnectionStatusAt) < 5*time.Second {
		return false
	}
	s.joinedConnectionStatusAt = now
	return true
}

func (s *Server) handleJoinedConnectionStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	batchIDs, ok := r.URL.Query()["batch_id"]
	if !ok || len(batchIDs) != 1 || len(r.URL.Query()) != 1 ||
		!joinedBatchIDPattern.MatchString(batchIDs[0]) {
		util.WriteError(w, http.StatusBadRequest, "one canonical joined batch_id is required")
		return
	}
	if batchIDs[0] != s.cfg.JoinedRecordingBatchID {
		util.WriteError(w, http.StatusForbidden, "joined batch scope differs")
		return
	}
	if !s.joinedConnectionStatusAllowed(time.Now().UTC()) {
		util.WriteError(w, http.StatusTooManyRequests, "joined connection status is rate limited")
		return
	}
	connectionID := s.cfg.JoinedRecordingConnectionID
	if connectionID <= 0 {
		util.WriteError(w, http.StatusServiceUnavailable, "joined connection target is not configured")
		return
	}
	if s.pool == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "joined connection status is unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	var (
		observedID                                                  int64
		observedProtocol, pollInterval                              int
		lastSeen, clientErrorAt                                     *time.Time
		clientVersion, clientPhase, clientPreviousExit, clientError string
	)
	err := s.pool.QueryRow(ctx, `SELECT c.id,c.joined_protocol_version,c.last_seen_at,c.poll_interval_sec,
		client_version,client_phase,client_previous_exit,client_last_error,client_last_error_at
		FROM connections c
		WHERE c.id=$1 AND c.kind='nas_pull' AND EXISTS (
			SELECT 1 FROM recording_joined_batches b WHERE b.batch_id=$2 AND b.connection_id=c.id
		)`, connectionID, batchIDs[0]).Scan(
		&observedID, &observedProtocol, &lastSeen, &pollInterval,
		&clientVersion, &clientPhase, &clientPreviousExit, &clientError, &clientErrorAt)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "joined connection target not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load joined connection status failed")
		return
	}
	desiredProtocol, desiredGeneration := desiredJoinedProtocol(s.cfg, observedID)
	now := time.Now().UTC()
	var ageSeconds *int64
	stale := true
	if lastSeen != nil {
		age := int64(now.Sub(lastSeen.UTC()).Seconds())
		if age < 0 {
			age = 0
		}
		ageSeconds = &age
		interval := pollInterval
		if interval < 10 {
			interval = 10
		}
		stale = now.After(lastSeen.UTC().Add(time.Duration(interval*3) * time.Second))
	}
	log.Printf("joined connection status read connection_id=%d observed_protocol=%d stale=%t", observedID, observedProtocol, stale)
	util.WriteJSON(w, http.StatusOK, joinedConnectionStatusResponse{
		ConnectionID: observedID, ControlPlaneEnabled: s.cfg.JoinedRecordingControlPlaneEnabled,
		NASDeliveryEnabled:           s.cfg.JoinedRecordingNASDeliveryEnabled,
		ExpectedProtocolVersion:      s.cfg.JoinedRecordingProtocolVersion,
		ExpectedProtocolGeneration:   s.cfg.JoinedRecordingProtocolGeneration,
		ServerDesiredProtocolVersion: desiredProtocol, ServerDesiredProtocolGeneration: desiredGeneration,
		ObservedProtocolVersion: observedProtocol, LastSeenAt: lastSeen,
		HeartbeatAgeSeconds: ageSeconds, HeartbeatStale: stale,
		PollIntervalSeconds: pollInterval, ClientVersion: boundedJoinedDiagnosticText(clientVersion),
		ClientPhase: boundedJoinedDiagnosticText(clientPhase), ClientPreviousExit: boundedJoinedDiagnosticText(clientPreviousExit),
		ClientErrorClass: joinedClientErrorClass(clientError), ClientErrorSHA256: joinedClientErrorSHA256(clientError),
		ClientErrorAt: clientErrorAt, ClientErrorPresent: clientError != "" || clientErrorAt != nil,
	})
}
