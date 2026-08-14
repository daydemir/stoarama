package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	recordingRecoveryIntentHeader = "X-Stoarama-Recording-Recovery-Intent"
	recordingRecoverySecretHeader = "X-Stoarama-Recording-Recovery-Secret"
)

var recordingSurrenderObservationClassRE = regexp.MustCompile(`^[a-z0-9_]{0,64}$`)

type recordingRecoveryPrincipal struct {
	GrantID    uuid.UUID
	IntentID   uuid.UUID
	ProducerID uuid.UUID
	JobID      int64
	LeaseToken uuid.UUID
	WorkerID   string
	NodeID     int64
	AccountID  int64
	ExpiresAt  time.Time
}

type recordingRecoveryContextKey struct{}

func recordingRecoveryFromContext(ctx context.Context) (recordingRecoveryPrincipal, bool) {
	principal, ok := ctx.Value(recordingRecoveryContextKey{}).(recordingRecoveryPrincipal)
	return principal, ok
}

func (s *Server) requireRecorderOrRecoveryAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// An explicit, valid recovery capability takes precedence over the ordinary
		// node credential that the same host may still carry. After lease expiry the
		// ordinary credential must not accidentally route an upload through the
		// current-lease branch and strand the fenced bytes the grant was created for.
		if strings.TrimSpace(r.Header.Get(recordingRecoveryIntentHeader)) != "" || strings.TrimSpace(r.Header.Get(recordingRecoverySecretHeader)) != "" {
			if recovery, recoveryErr := s.authenticateRecordingRecovery(r); recoveryErr == nil {
				node := nodePrincipal{NodeID: recovery.NodeID, AccountID: recovery.AccountID, NodeType: nodeTypeLocalRecorder, DisplayName: recovery.WorkerID}
				ctx := context.WithValue(r.Context(), nodePrincipalContextKey, node)
				ctx = context.WithValue(ctx, recordingRecoveryContextKey{}, recovery)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
		}
		principal, err := s.authenticateNodeRequest(r)
		nodeType := strings.TrimSpace(principal.NodeType)
		if err == nil && (nodeType == nodeTypeLocalRecorder || nodeType == nodeTypeRelay) {
			if nodeType != nodeTypeLocalRecorder {
				ctx := context.WithValue(r.Context(), nodePrincipalContextKey, principal)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
			managed, managedErr := s.isManagedCloudRecorder(r.Context(), principal)
			if managedErr == nil && managed {
				ctx := context.WithValue(r.Context(), nodePrincipalContextKey, principal)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
		}
		recovery, recoveryErr := s.authenticateRecordingRecovery(r)
		if recoveryErr != nil {
			util.WriteError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		node := nodePrincipal{NodeID: recovery.NodeID, AccountID: recovery.AccountID, NodeType: nodeTypeLocalRecorder, DisplayName: recovery.WorkerID}
		ctx := context.WithValue(r.Context(), nodePrincipalContextKey, node)
		ctx = context.WithValue(ctx, recordingRecoveryContextKey{}, recovery)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) authenticateRecordingRecovery(r *http.Request) (recordingRecoveryPrincipal, error) {
	intentID, err := uuid.Parse(strings.TrimSpace(r.Header.Get(recordingRecoveryIntentHeader)))
	secret := strings.TrimSpace(r.Header.Get(recordingRecoverySecretHeader))
	if err != nil || len(secret) != 64 {
		return recordingRecoveryPrincipal{}, errors.New("invalid recovery capability")
	}
	secretBytes, err := hex.DecodeString(secret)
	if err != nil || len(secretBytes) != 32 {
		return recordingRecoveryPrincipal{}, errors.New("invalid recovery capability")
	}
	secretHash := sha256.Sum256(secretBytes)
	var out recordingRecoveryPrincipal
	err = s.pool.QueryRow(r.Context(), `
		SELECT g.id,g.upload_intent_id,g.producer_id,g.recording_job_id,g.lease_token,p.worker_id,p.node_id,n.account_id,g.upload_grace_until
		FROM recording_job_recovery_grants g
		JOIN recording_capture_producers p ON p.id=g.producer_id
		JOIN nodes n ON n.id=p.node_id
		WHERE g.upload_intent_id=$1 AND g.recovery_secret_sha256=$2
		  AND ((g.revoked_at IS NULL AND g.upload_grace_until>transaction_timestamp())
		       OR g.revoke_reason='recovery_completed')
	`, intentID, hex.EncodeToString(secretHash[:])).Scan(&out.GrantID, &out.IntentID, &out.ProducerID, &out.JobID, &out.LeaseToken, &out.WorkerID, &out.NodeID, &out.AccountID, &out.ExpiresAt)
	if err != nil {
		return recordingRecoveryPrincipal{}, err
	}
	return out, nil
}

type recordingRecoveryArtifact struct {
	IntentID        string `json:"intent_id"`
	CaptureSequence int64  `json:"capture_sequence"`
	SegmentStartMs  int64  `json:"segment_start_ms"`
	SizeBytes       int64  `json:"size_bytes"`
	SHA256          string `json:"sha256"`
	Result          string `json:"result,omitempty"`
}

func (s *Server) handleRecordingRecoveryStatus(w http.ResponseWriter, r *http.Request) {
	recovery, ok := recordingRecoveryFromContext(r.Context())
	intentID, parseErr := uuid.Parse(strings.TrimSpace(chiURLParam(r, "intentId")))
	if !ok || parseErr != nil || intentID != recovery.IntentID {
		util.WriteError(w, http.StatusForbidden, "recovery capability does not authorize this intent")
		return
	}
	var out recordingRecoveryArtifact
	var result, producerResult string
	var segmentStart, size *int64
	var sha *string
	err := s.pool.QueryRow(r.Context(), `
		SELECT artifact.capture_sequence,seal.segment_start_ms,seal.size_bytes,seal.sha256,
		       COALESCE(result.result,''),COALESCE(producer_result.result,'')
		FROM recording_capture_artifact_intents artifact
		LEFT JOIN recording_capture_artifact_seals seal ON seal.upload_intent_id=artifact.upload_intent_id
		LEFT JOIN recording_capture_artifact_results result ON result.upload_intent_id=artifact.upload_intent_id
		LEFT JOIN recording_capture_producer_results producer_result ON producer_result.producer_id=artifact.producer_id
		WHERE artifact.upload_intent_id=$1 AND artifact.producer_id=$2
	`, intentID, recovery.ProducerID).Scan(&out.CaptureSequence, &segmentStart, &size, &sha, &result, &producerResult)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "recovery intent is unavailable")
		return
	}
	out.IntentID = intentID.String()
	out.Result = result
	if segmentStart != nil {
		out.SegmentStartMs = *segmentStart
		out.SizeBytes = *size
		out.SHA256 = *sha
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"intent_id": intentID, "producer_id": recovery.ProducerID, "job_id": recovery.JobID,
		"lease_token": recovery.LeaseToken, "expires_at": recovery.ExpiresAt,
		"authority": "recovery_grant", "producer_result": producerResult,
		"artifacts": []recordingRecoveryArtifact{out},
	})
}

func (s *Server) handleRecordingRecoveryFinish(w http.ResponseWriter, r *http.Request) {
	recovery, ok := recordingRecoveryFromContext(r.Context())
	intentID, parseErr := uuid.Parse(strings.TrimSpace(chiURLParam(r, "intentId")))
	if !ok || parseErr != nil || intentID != recovery.IntentID {
		util.WriteError(w, http.StatusForbidden, "recovery capability does not authorize this intent")
		return
	}
	var req struct {
		Result string `json:"result"`
	}
	if err := util.DecodeJSON(r, &req); err != nil || (req.Result != "abandoned_unsealed" && req.Result != "unrecoverable_partial") {
		util.WriteError(w, http.StatusBadRequest, "invalid reserved-unsealed recovery result")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin recovery intent finish")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, recordingSurrenderJobLockKey(recovery.JobID)); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock recovery intent finish")
		return
	}
	var current bool
	var priorReason, priorResult string
	if err = tx.QueryRow(r.Context(), `
		SELECT grant_row.revoked_at IS NULL AND grant_row.upload_grace_until>transaction_timestamp(),
		       COALESCE(grant_row.revoke_reason,''),COALESCE(result.result,'')
		FROM recording_job_recovery_grants grant_row
		LEFT JOIN recording_capture_artifact_results result ON result.upload_intent_id=grant_row.upload_intent_id
		WHERE grant_row.id=$1 AND grant_row.upload_intent_id=$2 FOR UPDATE OF grant_row
	`, recovery.GrantID, intentID).Scan(&current, &priorReason, &priorResult); err != nil {
		util.WriteError(w, http.StatusConflict, "recovery capability is unavailable")
		return
	}
	if priorReason == "recovery_completed" && priorResult == req.Result {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if !current {
		util.WriteError(w, http.StatusConflict, "recovery capability expired before finish")
		return
	}
	if _, err = tx.Exec(r.Context(), `
		INSERT INTO recording_capture_artifact_results(upload_intent_id,result)
		SELECT $1,$2 WHERE NOT EXISTS(SELECT 1 FROM recording_capture_artifact_seals WHERE upload_intent_id=$1)
		ON CONFLICT(upload_intent_id) DO NOTHING
	`, intentID, req.Result); err != nil {
		util.WriteError(w, http.StatusConflict, "finish reserved-unsealed recovery intent")
		return
	}
	if _, err = tx.Exec(r.Context(), `UPDATE recording_job_recovery_grants SET revoked_at=transaction_timestamp(),revoke_reason='recovery_completed' WHERE id=$1 AND revoked_at IS NULL`, recovery.GrantID); err != nil {
		util.WriteError(w, http.StatusConflict, "close recovery intent capability")
		return
	}
	if err = terminalizeRecoveredProducer(r.Context(), tx, recovery.ProducerID); err != nil {
		util.WriteError(w, http.StatusConflict, "finish recovered producer")
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit recovery intent finish")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func terminalizeRecoveredProducer(ctx context.Context, tx pgx.Tx, producerID uuid.UUID) error {
	var total, unresolved, accepted, security, expired int
	if err := tx.QueryRow(ctx, `
		SELECT count(*),count(*) FILTER(WHERE result.upload_intent_id IS NULL),
		       count(*) FILTER(WHERE result.result IN('accepted_unique','exact_replay')),
		       count(*) FILTER(WHERE result.result='security_revoked'),
		       count(*) FILTER(WHERE result.result='host_unreachable_unrecoverable')
		FROM recording_capture_artifact_intents artifact
		LEFT JOIN recording_capture_artifact_results result ON result.upload_intent_id=artifact.upload_intent_id
		WHERE artifact.producer_id=$1
	`, producerID).Scan(&total, &unresolved, &accepted, &security, &expired); err != nil || total == 0 || unresolved != 0 {
		return err
	}
	result, detail := "abandoned_empty", ""
	if security > 0 {
		result, detail = "security_revoked", "recovery_capability_revoked"
	} else if expired > 0 {
		result, detail = "host_unreachable_unrecoverable", "recovery_grace_expired"
	} else if accepted > 0 {
		result = "completed"
	}
	_, err := tx.Exec(ctx, `INSERT INTO recording_capture_producer_results(producer_id,result,detail_class) VALUES($1,$2,$3) ON CONFLICT(producer_id) DO NOTHING`, producerID, result, detail)
	return err
}

var recordingSurrenderDetailClassRE = regexp.MustCompile(`^[a-z0-9_]{1,64}$`)

type recordingCaptureProducerReserveRequest struct {
	ProducerID        string `json:"producer_id"`
	CaptureOrdinal    int64  `json:"capture_ordinal"`
	SealedIntentLimit int    `json:"sealed_intent_limit"`
}

type recordingCaptureProducerFinishRequest struct {
	Result      string `json:"result"`
	DetailClass string `json:"detail_class"`
}

func recordingSurrenderJobLockKey(jobID int64) string {
	return "recording-surrender-job:" + strconv.FormatInt(jobID, 10)
}

func validLowerSHA256(v string) bool {
	if len(v) != 64 || strings.ToLower(v) != v {
		return false
	}
	_, err := hex.DecodeString(v)
	return err == nil
}

func surrenderRequestSHA(req recordingJobSurrenderRequest) string {
	values := []string{
		strconv.Itoa(req.TransportVersion), req.AttemptID, string(req.Reason), strings.TrimSpace(req.ErrorText),
		strconv.FormatInt(req.ExpectedHeadVersion, 10), req.ExpectedUploadIntentID,
		strconv.FormatInt(req.ExpectedClipID, 10), strconv.Itoa(req.SpoolCount),
		strconv.FormatInt(req.SpoolBytes, 10), strconv.Itoa(req.InFlightCount),
	}
	h := sha256.New()
	_, _ = h.Write([]byte("recording-surrender-request-v1\n"))
	for _, value := range values {
		_, _ = fmt.Fprintf(h, "%d:%s\n", len([]byte(value)), value)
	}
	return hex.EncodeToString(h.Sum(nil))
}

type recordingSurrenderV1Result struct {
	Result                string     `json:"result"`
	HandoffUntil          *time.Time `json:"handoff_until,omitempty"`
	NextRetryAt           *time.Time `json:"next_retry_at,omitempty"`
	HadClips              *bool      `json:"had_clips,omitempty"`
	AlternateAvailable    *bool      `json:"alternate_available,omitempty"`
	CurrentHeadVersion    int64      `json:"current_head_version"`
	CurrentUploadIntentID string     `json:"current_upload_intent_id,omitempty"`
	CurrentClipID         int64      `json:"current_clip_id,omitempty"`
}

func (s *Server) handleRecordingJobSurrenderV1(w http.ResponseWriter, r *http.Request, principal nodePrincipal, jobID int64, leaseToken *uuid.UUID, req recordingJobSurrenderRequest) {
	if (principal.NodeType != nodeTypeLocalRecorder && principal.NodeType != nodeTypeRelay) || leaseToken == nil {
		util.WriteError(w, http.StatusBadRequest, "surrender transport v1 requires a fenced recorder generation")
		return
	}
	attemptID, err := uuid.Parse(strings.TrimSpace(req.AttemptID))
	if err != nil || req.ExpectedHeadVersion < 0 || req.SpoolCount != 0 || req.SpoolBytes != 0 || req.InFlightCount != 0 {
		util.WriteError(w, http.StatusBadRequest, "invalid surrender transport v1 request")
		return
	}
	var expectedIntent *uuid.UUID
	if strings.TrimSpace(req.ExpectedUploadIntentID) != "" {
		parsed, parseErr := uuid.Parse(strings.TrimSpace(req.ExpectedUploadIntentID))
		if parseErr != nil {
			util.WriteError(w, http.StatusBadRequest, "invalid expected upload intent")
			return
		}
		expectedIntent = &parsed
	}
	if (req.ExpectedHeadVersion == 0) != (expectedIntent == nil && req.ExpectedClipID == 0) || (req.ExpectedHeadVersion > 0 && (expectedIntent == nil || req.ExpectedClipID <= 0)) {
		util.WriteError(w, http.StatusBadRequest, "incomplete expected accepted-unique head")
		return
	}
	errorText := sanitizeRecordingSurrenderError(req.ErrorText, string(req.Reason))
	req.ErrorText = errorText
	requestSHA := surrenderRequestSHA(req)
	workerID := recorderWorkerID(principal)
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin surrender decision")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock(hashtextextended('recording-surrender-cloud-capacity-v1',0))`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock surrender capacity")
		return
	}
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, recordingSurrenderJobLockKey(jobID)); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock surrender job")
		return
	}
	// Exact request replay returns the already-frozen result even after the job's
	// fence moved. A reused UUID with different bytes never reaches job mutation.
	var priorSHA string
	var prior recordingSurrenderV1Result
	err = tx.QueryRow(r.Context(), `
		SELECT a.request_sha256,r.result,r.handoff_until,r.next_retry_at,r.had_clips,r.alternate_available,r.current_head_version,COALESCE(r.current_upload_intent_id::text,''),COALESCE(r.current_clip_id,0)
		FROM recording_job_surrender_attempts a JOIN recording_job_surrender_results r ON r.attempt_id=a.id
		WHERE a.id=$1
	`, attemptID).Scan(&priorSHA, &prior.Result, &prior.HandoffUntil, &prior.NextRetryAt, &prior.HadClips, &prior.AlternateAvailable, &prior.CurrentHeadVersion, &prior.CurrentUploadIntentID, &prior.CurrentClipID)
	if err == nil {
		if priorSHA != requestSHA {
			util.WriteError(w, http.StatusConflict, "surrender attempt replay differs")
			return
		}
		if err = tx.Commit(r.Context()); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "commit surrender replay")
			return
		}
		util.WriteJSON(w, http.StatusOK, prior)
		return
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusInternalServerError, "load surrender replay")
		return
	}

	var generationOwner string
	var generationNode int64
	var currentStatus, currentOwner, captureVia string
	var currentToken *uuid.UUID
	var windowOpen bool
	var attemptCount int
	var currentHead int64
	var currentIntent *uuid.UUID
	var currentClip *int64
	err = tx.QueryRow(r.Context(), `
			SELECT g.lease_owner,COALESCE(g.node_id,0),j.status,COALESCE(j.lease_owner,''),j.lease_token,transaction_timestamp()<j.window_end_at,j.attempt_count,recording.capture_via,
			       h.version,h.upload_intent_id,h.clip_id
		FROM recording_job_lease_generations g
		JOIN recording_jobs j ON j.id=g.recording_job_id
		JOIN recordings recording ON recording.id=j.recording_id
		JOIN recording_job_unique_heads h ON h.recording_job_id=g.recording_job_id AND h.lease_token=g.lease_token
		WHERE g.recording_job_id=$1 AND g.lease_token=$2
			  AND j.kind='continuous_window' AND j.window_end_at IS NOT NULL AND recording.capture_via IN('cloud','relay')
			FOR UPDATE OF j,h
		`, jobID, leaseToken).Scan(&generationOwner, &generationNode, &currentStatus, &currentOwner, &currentToken, &windowOpen, &attemptCount, &captureVia, &currentHead, &currentIntent, &currentClip)
	if errors.Is(err, pgx.ErrNoRows) || generationOwner != workerID || generationNode != principal.NodeID {
		util.WriteError(w, http.StatusConflict, "unknown surrender lease generation")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load surrender generation")
		return
	}
	resultName := ""
	if currentStatus != "leased" || currentOwner != workerID || currentToken == nil || *currentToken != *leaseToken {
		resultName = "stale_fence"
	} else if !windowOpen {
		resultName = "window_closed"
	} else if currentHead != req.ExpectedHeadVersion || (currentHead > 0 && (currentIntent == nil || expectedIntent == nil || *currentIntent != *expectedIntent || currentClip == nil || *currentClip != req.ExpectedClipID)) {
		resultName = "stale_progress"
	}
	// Server-side empty-spool proof: every producer is terminal and every sealed
	// artifact has a terminal result. Caller zeroes alone are insufficient.
	if resultName == "" {
		var unsafe bool
		if err = tx.QueryRow(r.Context(), `SELECT EXISTS(
			SELECT 1 FROM recording_capture_producers p
			WHERE p.recording_job_id=$1 AND p.lease_token=$2
			  AND NOT EXISTS(SELECT 1 FROM recording_capture_producer_results pr WHERE pr.producer_id=p.id)
			UNION ALL
			SELECT 1 FROM recording_capture_artifact_seals a JOIN recording_capture_producers p ON p.id=a.producer_id
			WHERE p.recording_job_id=$1 AND p.lease_token=$2
			  AND NOT EXISTS(SELECT 1 FROM recording_capture_artifact_results ar WHERE ar.upload_intent_id=a.upload_intent_id)
		)`, jobID, leaseToken).Scan(&unsafe); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "validate surrender spool barrier")
			return
		} else if unsafe {
			resultName = "ineligible_spool"
		}
	}

	if _, err = tx.Exec(r.Context(), `
		INSERT INTO recording_job_surrender_attempts(id,recording_job_id,lease_token,worker_id,node_id,reason,error_text,expected_head_version,expected_upload_intent_id,expected_clip_id,spool_count,spool_bytes,in_flight_count,request_sha256)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,NULLIF($10,0),0,0,0,$11)
	`, attemptID, jobID, leaseToken, workerID, principal.NodeID, req.Reason, errorText, req.ExpectedHeadVersion, expectedIntent, req.ExpectedClipID, requestSHA); err != nil {
		util.WriteError(w, http.StatusConflict, fmt.Sprintf("record surrender attempt: %v", err))
		return
	}
	if resultName != "" {
		if _, err = tx.Exec(r.Context(), `INSERT INTO recording_job_surrender_results(attempt_id,result,current_head_version,current_upload_intent_id,current_clip_id) VALUES($1,$2,$3,$4,$5)`, attemptID, resultName, currentHead, currentIntent, currentClip); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "record surrender rejection")
			return
		}
		if err = tx.Commit(r.Context()); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "commit surrender rejection")
			return
		}
		response := recordingSurrenderV1Result{Result: resultName, CurrentHeadVersion: currentHead}
		if currentIntent != nil {
			response.CurrentUploadIntentID = currentIntent.String()
		}
		if currentClip != nil {
			response.CurrentClipID = *currentClip
		}
		util.WriteJSON(w, http.StatusOK, response)
		return
	}

	var alternate bool
	if captureVia == "cloud" {
		err = tx.QueryRow(r.Context(), `SELECT EXISTS(
		SELECT 1 FROM recorder_droplets d
		WHERE d.name<>$1 AND d.state IN ('provisioning','active') AND d.last_seen_at>=transaction_timestamp()-interval '2 minutes'
		  AND (SELECT count(*) FROM recording_jobs live WHERE live.status='leased' AND live.lease_owner=d.name AND live.lease_expires_at>transaction_timestamp()) < d.capacity
	)`, workerID).Scan(&alternate)
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "compute surrender alternate capacity")
		return
	}
	errorText = sanitizeRecordingSurrenderError(req.ErrorText, string(req.Reason))
	var handoffUntil, nextRetryAt time.Time
	var hadClips bool
	err = tx.QueryRow(r.Context(), `
		UPDATE recording_jobs
		SET status='pending',scheduled_for=transaction_timestamp()+CASE
		      WHEN NOT $4 THEN interval '0'
		      WHEN $5>0 THEN interval '0'
		      WHEN attempt_count<=1 THEN interval '1 minute'
		      WHEN attempt_count=2 THEN interval '2 minutes'
		      ELSE interval '5 minutes' END,
		    lease_owner=NULL,lease_expires_at=NULL,lease_token=NULL,
		    handoff_owner=$2,handoff_until=transaction_timestamp()+CASE WHEN $4 THEN interval '5 minutes' ELSE interval '0' END,
		    error_text=$3,completed_at=NULL,updated_at=transaction_timestamp()
		WHERE id=$1 AND status='leased' AND lease_owner=$2 AND lease_token=$6
		RETURNING handoff_until,scheduled_for,$5>0
	`, jobID, workerID, errorText, alternate, currentHead, leaseToken).Scan(&handoffUntil, &nextRetryAt, &hadClips)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "surrender lease changed before commit")
		return
	}
	if _, err = tx.Exec(r.Context(), `
		INSERT INTO recording_job_surrender_results(attempt_id,result,next_retry_at,handoff_until,had_clips,alternate_available,current_head_version,current_upload_intent_id,current_clip_id)
		VALUES($1,'committed',$2,$3,$4,$5,$6,$7,$8)
	`, attemptID, nextRetryAt, handoffUntil, hadClips, alternate, currentHead, currentIntent, currentClip); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "record surrender commit result")
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit surrender decision")
		return
	}
	response := recordingSurrenderV1Result{Result: "committed", HandoffUntil: &handoffUntil, NextRetryAt: &nextRetryAt, HadClips: &hadClips, AlternateAvailable: &alternate, CurrentHeadVersion: currentHead}
	if currentIntent != nil {
		response.CurrentUploadIntentID = currentIntent.String()
	}
	if currentClip != nil {
		response.CurrentClipID = *currentClip
	}
	util.WriteJSON(w, http.StatusOK, response)
}

func (s *Server) handleRecordingSurrenderTransportObservations(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	if !ok || (principal.NodeType != nodeTypeLocalRecorder && principal.NodeType != nodeTypeRelay) {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	jobID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req struct {
		Observations []struct {
			ID            string    `json:"id"`
			LeaseToken    string    `json:"lease_token"`
			AttemptID     string    `json:"attempt_id"`
			Type          string    `json:"type"`
			ErrorClass    string    `json:"error_class"`
			ObservedAt    time.Time `json:"observed_at"`
			RequestSHA256 string    `json:"request_sha256"`
		} `json:"observations"`
	}
	if err := util.DecodeJSON(r, &req); err != nil || len(req.Observations) == 0 || len(req.Observations) > 64 {
		util.WriteError(w, http.StatusBadRequest, "invalid surrender transport observation batch")
		return
	}
	workerID := recorderWorkerID(principal)
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin surrender transport observations")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, recordingSurrenderJobLockKey(jobID)); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock surrender transport observations")
		return
	}
	for _, observation := range req.Observations {
		id, idErr := uuid.Parse(strings.TrimSpace(observation.ID))
		leaseToken, tokenErr := uuid.Parse(strings.TrimSpace(observation.LeaseToken))
		attemptID, attemptErr := uuid.Parse(strings.TrimSpace(observation.AttemptID))
		validType := observation.Type == "request_started" || observation.Type == "request_transport_failed" || observation.Type == "request_result_received" || observation.Type == "transport_budget_exhausted"
		validClass := ((observation.Type == "request_started" || observation.Type == "request_result_received") && observation.ErrorClass == "") ||
			((observation.Type == "request_transport_failed" || observation.Type == "transport_budget_exhausted") && recordingSurrenderObservationClassRE.MatchString(observation.ErrorClass) && observation.ErrorClass != "")
		if idErr != nil || tokenErr != nil || attemptErr != nil || !validType || !validClass || !validLowerSHA256(observation.RequestSHA256) || observation.ObservedAt.IsZero() {
			util.WriteError(w, http.StatusBadRequest, "invalid surrender transport observation")
			return
		}
		tag, insertErr := tx.Exec(r.Context(), `
			INSERT INTO recording_surrender_transport_observations(id,recording_job_id,lease_token,worker_id,node_id,observation_type,attempt_id,error_class,observed_at,request_sha256)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(id) DO NOTHING
		`, id, jobID, leaseToken, workerID, principal.NodeID, observation.Type, attemptID, observation.ErrorClass, observation.ObservedAt.UTC(), observation.RequestSHA256)
		if insertErr != nil {
			util.WriteError(w, http.StatusConflict, "record surrender transport observation")
			return
		}
		inserted := tag.RowsAffected() == 1
		if !inserted {
			var exact bool
			if err = tx.QueryRow(r.Context(), `SELECT recording_job_id=$2 AND lease_token=$3 AND worker_id=$4 AND node_id=$5 AND observation_type=$6 AND attempt_id=$7 AND error_class=$8 AND observed_at=$9 AND request_sha256=$10 FROM recording_surrender_transport_observations WHERE id=$1`, id, jobID, leaseToken, workerID, principal.NodeID, observation.Type, attemptID, observation.ErrorClass, observation.ObservedAt.UTC(), observation.RequestSHA256).Scan(&exact); err != nil || !exact {
				util.WriteError(w, http.StatusConflict, "surrender transport observation replay differs")
				return
			}
		}
		if observation.Type == "transport_budget_exhausted" && inserted {
			var episodeKey uuid.UUID
			if err = tx.QueryRow(r.Context(), `
				INSERT INTO recording_surrender_transport_episodes(recording_job_id,episode_key,lease_token,state,reason)
				VALUES($1,$2,$3,'open','transport_budget_exhausted')
				ON CONFLICT(recording_job_id) DO UPDATE SET lease_token=EXCLUDED.lease_token,state='open',reason='transport_budget_exhausted',last_observed_at=transaction_timestamp(),resolved_at=NULL
				RETURNING episode_key
			`, jobID, id, leaseToken).Scan(&episodeKey); err != nil {
				util.WriteError(w, http.StatusConflict, "open surrender transport episode")
				return
			}
			if _, err = tx.Exec(r.Context(), `INSERT INTO recording_surrender_transport_episode_events(event_key,episode_key,recording_job_id,lease_token,event_type,reason) VALUES($1,$2,$3,$4,'opened','transport_budget_exhausted') ON CONFLICT DO NOTHING`, id, episodeKey, jobID, leaseToken); err != nil {
				util.WriteError(w, http.StatusConflict, "seal surrender transport episode event")
				return
			}
		}
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit surrender transport observations")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleRecordingCaptureProducerReserve(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	jobID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	leaseToken, err := recordingLeaseToken(r)
	if err != nil || leaseToken == nil {
		util.WriteError(w, http.StatusBadRequest, "generation-fenced lease token is required")
		return
	}
	var req recordingCaptureProducerReserveRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	producerID, err := uuid.Parse(strings.TrimSpace(req.ProducerID))
	if err != nil || req.CaptureOrdinal <= 0 || req.SealedIntentLimit < 1 || req.SealedIntentLimit > 8 {
		util.WriteError(w, http.StatusBadRequest, "invalid capture producer reservation")
		return
	}
	workerID := recorderWorkerID(principal)
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin capture producer reservation")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, recordingSurrenderJobLockKey(jobID)); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock capture producer reservation")
		return
	}
	var snapshotRecordingID, snapshotStreamID int64
	if err = tx.QueryRow(r.Context(), `SELECT recording.id,recording.stream_id FROM recording_jobs job JOIN recordings recording ON recording.id=job.recording_id WHERE job.id=$1 AND recording.stream_id IS NOT NULL`, jobID).Scan(&snapshotRecordingID, &snapshotStreamID); err != nil {
		util.WriteError(w, http.StatusConflict, "capture producer source binding is unavailable")
		return
	}
	if _, err = tx.Exec(r.Context(), `SELECT id FROM streams WHERE id=$1 FOR SHARE`, snapshotStreamID); err != nil {
		util.WriteError(w, http.StatusConflict, "lock capture producer stream binding")
		return
	}
	if _, err = tx.Exec(r.Context(), `SELECT id FROM recordings WHERE id=$1 AND stream_id=$2 FOR SHARE`, snapshotRecordingID, snapshotStreamID); err != nil {
		util.WriteError(w, http.StatusConflict, "lock capture producer recording binding")
		return
	}
	var snapshot, configSHA string
	var revisionID *int64
	err = tx.QueryRow(r.Context(), `
		SELECT encode(sha256(convert_to(
		         'recording-capture-producer-v1'||chr(10)
		         ||octet_length(r.account_id::text)::text||':'||r.account_id::text||chr(10)
		         ||octet_length(r.id::text)::text||':'||r.id::text||chr(10)
		         ||octet_length(r.stream_id::text)::text||':'||r.stream_id::text||chr(10)
		         ||octet_length(r.stream_url)::text||':'||r.stream_url||chr(10)
		         ||octet_length(r.capture_via)::text||':'||r.capture_via||chr(10)
		         ||octet_length(r.mode)::text||':'||r.mode||chr(10)
		         ||octet_length(r.source_kind)::text||':'||r.source_kind||chr(10)
		         ||octet_length(COALESCE(r.target_fps,0)::text)::text||':'||COALESCE(r.target_fps,0)::text||chr(10)
		         ||octet_length(COALESCE(r.naming_profile,''))::text||':'||COALESCE(r.naming_profile,'')||chr(10)
		         ||octet_length(COALESCE(r.folder_name,''))::text||':'||COALESCE(r.folder_name,'')||chr(10)
		         ||octet_length(j.clip_duration_sec::text)::text||':'||j.clip_duration_sec::text||chr(10)
		         ||octet_length(j.kind)::text||':'||j.kind||chr(10)
		         ||octet_length(COALESCE(j.window_end_at::text,''))::text||':'||COALESCE(j.window_end_at::text,'')||chr(10)
		         ||octet_length(st.source_url)::text||':'||st.source_url||chr(10)
		         ||octet_length(st.source_page_url)::text||':'||st.source_page_url||chr(10)
		         ||octet_length(st.provider)::text||':'||st.provider||chr(10)
		         ||octet_length(st.external_id)::text||':'||st.external_id||chr(10)
		         ||octet_length(st.source_family)::text||':'||st.source_family||chr(10)
		         ||octet_length(st.capture_type)::text||':'||st.capture_type||chr(10)
		         ||octet_length(st.execution_class)::text||':'||st.execution_class||chr(10)
		         ||octet_length(COALESCE(st.execution_config_jsonb,'{}'::jsonb)::text)::text||':'||COALESCE(st.execution_config_jsonb,'{}'::jsonb)::text||chr(10)
		         ||octet_length(COALESCE((SELECT admission.policy_version FROM recording_timestamp_contract_admissions admission WHERE admission.recording_job_id=j.id AND admission.lease_token=j.lease_token),''))::text||':'||COALESCE((SELECT admission.policy_version FROM recording_timestamp_contract_admissions admission WHERE admission.recording_job_id=j.id AND admission.lease_token=j.lease_token),'')||chr(10)
		         ||octet_length(COALESCE((SELECT max(sr.id) FROM stream_source_revisions sr WHERE sr.stream_id=st.id),0)::text)::text||':'||COALESCE((SELECT max(sr.id) FROM stream_source_revisions sr WHERE sr.stream_id=st.id),0)::text||chr(10)
		       ,'UTF8')),'hex'),
		       (SELECT max(sr.id) FROM stream_source_revisions sr WHERE sr.stream_id=st.id),
		       encode(sha256(convert_to(jsonb_build_array(r.capture_via,r.mode,r.source_kind,r.target_fps,r.clip_duration_sec,j.clip_duration_sec,st.source_family,st.capture_type,st.execution_class,COALESCE(st.execution_config_jsonb,'{}'::jsonb),COALESCE((SELECT admission.policy_version FROM recording_timestamp_contract_admissions admission WHERE admission.recording_job_id=j.id AND admission.lease_token=j.lease_token),''))::text,'UTF8')),'hex')
		FROM recording_jobs j
		JOIN recordings r ON r.id=j.recording_id
		JOIN streams st ON st.id=r.stream_id
		JOIN recording_job_lease_generations g ON g.recording_job_id=j.id AND g.lease_token=j.lease_token
		WHERE j.id=$1 AND j.status='leased' AND j.lease_owner=$2 AND j.lease_token=$3 AND j.lease_expires_at>transaction_timestamp()
		FOR UPDATE OF j,g
	`, jobID, workerID, leaseToken).Scan(&snapshot, &revisionID, &configSHA)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusConflict, "capture producer lease is stale")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load capture producer lease")
		return
	}
	var existingID uuid.UUID
	var existingWorker string
	var existingNode int64
	var existingLimit int
	var existingTerminal bool
	err = tx.QueryRow(r.Context(), `SELECT p.id,p.worker_id,p.node_id,p.sealed_intent_limit,EXISTS(SELECT 1 FROM recording_capture_producer_results result WHERE result.producer_id=p.id) FROM recording_capture_producers p WHERE p.recording_job_id=$1 AND p.lease_token=$2 AND p.capture_ordinal=$3`, jobID, leaseToken, req.CaptureOrdinal).Scan(&existingID, &existingWorker, &existingNode, &existingLimit, &existingTerminal)
	if err == nil {
		if existingTerminal || existingID != producerID || existingWorker != workerID || existingNode != principal.NodeID || existingLimit != req.SealedIntentLimit {
			util.WriteError(w, http.StatusConflict, "capture producer replay differs from reserved identity")
			return
		}
		if err = tx.Commit(r.Context()); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "commit capture producer replay")
			return
		}
		util.WriteJSON(w, http.StatusOK, map[string]any{"producer_id": producerID, "capture_ordinal": req.CaptureOrdinal})
		return
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusInternalServerError, "load capture producer replay")
		return
	}
	commandTag, err := tx.Exec(r.Context(), `
		INSERT INTO recording_capture_producers(id,recording_job_id,lease_token,capture_ordinal,worker_id,node_id,sealed_intent_limit,source_snapshot_sha256,source_revision_head_id,capture_config_sha256)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT(recording_job_id,lease_token,capture_ordinal) DO NOTHING
	`, producerID, jobID, leaseToken, req.CaptureOrdinal, workerID, principal.NodeID, req.SealedIntentLimit, snapshot, revisionID, configSHA)
	if err != nil {
		util.WriteError(w, http.StatusConflict, fmt.Sprintf("reserve capture producer: %v", err))
		return
	}
	if commandTag.RowsAffected() == 0 {
		if err = tx.QueryRow(r.Context(), `SELECT p.id,p.worker_id,p.node_id,p.sealed_intent_limit,EXISTS(SELECT 1 FROM recording_capture_producer_results result WHERE result.producer_id=p.id) FROM recording_capture_producers p WHERE p.recording_job_id=$1 AND p.lease_token=$2 AND p.capture_ordinal=$3`, jobID, leaseToken, req.CaptureOrdinal).Scan(&existingID, &existingWorker, &existingNode, &existingLimit, &existingTerminal); err != nil || existingTerminal || existingID != producerID || existingWorker != workerID || existingNode != principal.NodeID || existingLimit != req.SealedIntentLimit {
			util.WriteError(w, http.StatusConflict, "capture producer replay differs from reserved identity")
			return
		}
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit capture producer reservation")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"producer_id": producerID, "capture_ordinal": req.CaptureOrdinal})
}

func (s *Server) handleRecordingCaptureProducerStatus(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	if !ok || (principal.NodeType != nodeTypeLocalRecorder && principal.NodeType != nodeTypeRelay) {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	jobID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	producerID, err := uuid.Parse(strings.TrimSpace(chiURLParam(r, "producerId")))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid capture producer")
		return
	}
	var result string
	var currentLease bool
	var intentCount int
	err = s.pool.QueryRow(r.Context(), `
		SELECT COALESCE(result.result,''),
		       job.status='leased' AND job.lease_token=producer.lease_token
		         AND job.lease_owner=producer.worker_id AND job.lease_expires_at>transaction_timestamp(),
		       count(artifact.upload_intent_id)
		FROM recording_capture_producers producer
		JOIN recording_jobs job ON job.id=producer.recording_job_id
		LEFT JOIN recording_capture_producer_results result ON result.producer_id=producer.id
		LEFT JOIN recording_capture_artifact_intents artifact ON artifact.producer_id=producer.id
		WHERE producer.id=$1 AND producer.recording_job_id=$2 AND producer.node_id=$3 AND producer.worker_id=$4
		GROUP BY producer.id,result.result,job.status,job.lease_token,job.lease_owner,job.lease_expires_at
	`, producerID, jobID, principal.NodeID, recorderWorkerID(principal)).Scan(&result, &currentLease, &intentCount)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteJSON(w, http.StatusOK, map[string]any{"producer_id": producerID, "found": false, "intent_count": 0})
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load capture producer status")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"producer_id": producerID, "found": true, "current_lease": currentLease, "intent_count": intentCount, "result": result})
}

func (s *Server) handleRecordingCaptureArtifactsReserve(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	jobID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	producerID, err := uuid.Parse(strings.TrimSpace(chiURLParam(r, "producerId")))
	leaseToken, tokenErr := recordingLeaseToken(r)
	var req struct {
		Artifacts []struct {
			IntentID             string `json:"intent_id"`
			RecoverySecretSHA256 string `json:"recovery_secret_sha256"`
			CaptureSequence      int64  `json:"capture_sequence"`
		} `json:"artifacts"`
	}
	if err != nil || tokenErr != nil || leaseToken == nil || util.DecodeJSON(r, &req) != nil || len(req.Artifacts) < 1 || len(req.Artifacts) > 2048 {
		util.WriteError(w, http.StatusBadRequest, "invalid pre-byte artifact reservation batch")
		return
	}
	seenID, seenSeq, seenSecret := map[uuid.UUID]struct{}{}, map[int64]struct{}{}, map[string]struct{}{}
	for _, artifact := range req.Artifacts {
		id, parseErr := uuid.Parse(strings.TrimSpace(artifact.IntentID))
		if parseErr != nil || artifact.CaptureSequence <= 0 || !validLowerSHA256(artifact.RecoverySecretSHA256) {
			util.WriteError(w, http.StatusBadRequest, "invalid pre-byte artifact identity")
			return
		}
		if _, exists := seenID[id]; exists {
			util.WriteError(w, http.StatusBadRequest, "duplicate pre-byte artifact intent")
			return
		}
		if _, exists := seenSeq[artifact.CaptureSequence]; exists {
			util.WriteError(w, http.StatusBadRequest, "duplicate pre-byte artifact sequence")
			return
		}
		if _, exists := seenSecret[artifact.RecoverySecretSHA256]; exists {
			util.WriteError(w, http.StatusBadRequest, "duplicate pre-byte artifact recovery secret")
			return
		}
		seenID[id] = struct{}{}
		seenSeq[artifact.CaptureSequence] = struct{}{}
		seenSecret[artifact.RecoverySecretSHA256] = struct{}{}
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin pre-byte artifact reservation")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, recordingSurrenderJobLockKey(jobID)); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock pre-byte artifact reservation")
		return
	}
	var recordingID, destID int64
	var endpoint, bucket, keyPrefix string
	var clipDuration int
	err = tx.QueryRow(r.Context(), `
		SELECT job.recording_id,destination.id,destination.endpoint,destination.bucket,destination.key_prefix,job.clip_duration_sec
		FROM recording_capture_producers producer
		JOIN recording_jobs job ON job.id=producer.recording_job_id
		JOIN recordings recording ON recording.id=job.recording_id
		JOIN storage_destinations destination ON destination.id=recording.storage_destination_id
		WHERE producer.id=$1 AND producer.recording_job_id=$2 AND producer.lease_token=$3 AND producer.node_id=$4
		 AND producer.worker_id=$5 AND job.status='leased' AND job.lease_token=producer.lease_token
		 AND job.lease_owner=producer.worker_id AND job.lease_expires_at>transaction_timestamp()
		 AND NOT EXISTS(SELECT 1 FROM recording_capture_producer_results result WHERE result.producer_id=producer.id)
		FOR UPDATE OF producer,job
	`, producerID, jobID, leaseToken, principal.NodeID, recorderWorkerID(principal)).Scan(&recordingID, &destID, &endpoint, &bucket, &keyPrefix, &clipDuration)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "capture producer is stale")
		return
	}
	maxSize := int64(clipDuration) * recordingMaxBitrateBytesPerSec
	for _, artifact := range req.Artifacts {
		intentID := uuid.MustParse(artifact.IntentID)
		displayPath := fmt.Sprintf("capture-v1/%d/%s.mp4", jobID, intentID)
		objectKey := storageObjectKey(keyPrefix, displayPath)
		if _, err = tx.Exec(r.Context(), `
			INSERT INTO recording_upload_intents(id,recording_id,recording_job_id,storage_destination_id,endpoint,bucket,object_key,display_path,mime_type,max_size_bytes,status,expires_at)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,'video/mp4',$9,'pending',transaction_timestamp()+interval '15 minutes')
			ON CONFLICT(id) DO NOTHING
		`, intentID, recordingID, jobID, destID, endpoint, bucket, objectKey, displayPath, maxSize); err != nil {
			util.WriteError(w, http.StatusConflict, "reserve upload intent before bytes")
			return
		}
		var exactUpload bool
		if err = tx.QueryRow(r.Context(), `
			SELECT recording_id=$2 AND recording_job_id=$3 AND storage_destination_id=$4
			 AND endpoint=$5 AND bucket=$6 AND object_key=$7 AND display_path=$8
			 AND mime_type='video/mp4' AND max_size_bytes=$9 AND status='pending'
			FROM recording_upload_intents WHERE id=$1
		`, intentID, recordingID, jobID, destID, endpoint, bucket, objectKey, displayPath, maxSize).Scan(&exactUpload); err != nil || !exactUpload {
			util.WriteError(w, http.StatusConflict, "pre-byte upload intent replay differs")
			return
		}
		tag, insertErr := tx.Exec(r.Context(), `
			INSERT INTO recording_capture_artifact_intents(upload_intent_id,producer_id,capture_sequence,recovery_secret_sha256,max_size_bytes)
			VALUES($1,$2,$3,$4,$5) ON CONFLICT(upload_intent_id) DO NOTHING
		`, intentID, producerID, artifact.CaptureSequence, artifact.RecoverySecretSHA256, maxSize)
		if insertErr != nil {
			util.WriteError(w, http.StatusConflict, "bind upload intent before bytes")
			return
		}
		if tag.RowsAffected() == 0 {
			var exact bool
			if err = tx.QueryRow(r.Context(), `SELECT producer_id=$2 AND capture_sequence=$3 AND recovery_secret_sha256=$4 AND max_size_bytes=$5 FROM recording_capture_artifact_intents WHERE upload_intent_id=$1`, intentID, producerID, artifact.CaptureSequence, artifact.RecoverySecretSHA256, maxSize).Scan(&exact); err != nil || !exact {
				util.WriteError(w, http.StatusConflict, "pre-byte artifact batch replay differs")
				return
			}
		}
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit pre-byte artifact reservation")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleRecordingCaptureProducerFinish(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	jobID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	producerID, err := uuid.Parse(strings.TrimSpace(chiURLParam(r, "producerId")))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid producer id")
		return
	}
	leaseToken, err := recordingLeaseToken(r)
	if err != nil || leaseToken == nil {
		util.WriteError(w, http.StatusBadRequest, "generation-fenced lease token is required")
		return
	}
	var req recordingCaptureProducerFinishRequest
	if err = util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Result != "completed" && req.Result != "abandoned_empty" {
		util.WriteError(w, http.StatusBadRequest, "invalid capture producer result")
		return
	}
	if req.DetailClass != "" && !recordingSurrenderDetailClassRE.MatchString(req.DetailClass) {
		util.WriteError(w, http.StatusBadRequest, "invalid capture producer detail class")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin capture producer finish")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, recordingSurrenderJobLockKey(jobID)); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock capture producer finish")
		return
	}
	var owner string
	err = tx.QueryRow(r.Context(), `
		SELECT producer.worker_id
		FROM recording_capture_producers producer
		JOIN recording_jobs job ON job.id=producer.recording_job_id
		WHERE producer.id=$1 AND producer.recording_job_id=$2 AND producer.lease_token=$3 AND producer.node_id=$4
		  AND job.status='leased' AND job.lease_token=producer.lease_token
		  AND job.lease_owner=producer.worker_id AND job.lease_expires_at>transaction_timestamp()
		FOR UPDATE OF producer,job
	`, producerID, jobID, leaseToken, principal.NodeID).Scan(&owner)
	if errors.Is(err, pgx.ErrNoRows) || owner != recorderWorkerID(principal) {
		util.WriteError(w, http.StatusConflict, "capture producer is not owned by this lease")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load capture producer")
		return
	}
	if _, err = tx.Exec(r.Context(), `
		INSERT INTO recording_capture_artifact_results(upload_intent_id,result)
		SELECT artifact.upload_intent_id,'abandoned_unsealed'
		FROM recording_capture_artifact_intents artifact
		WHERE artifact.producer_id=$1
		  AND NOT EXISTS(SELECT 1 FROM recording_capture_artifact_seals seal WHERE seal.upload_intent_id=artifact.upload_intent_id)
		  AND NOT EXISTS(SELECT 1 FROM recording_capture_artifact_results result WHERE result.upload_intent_id=artifact.upload_intent_id)
	`, producerID); err != nil {
		util.WriteError(w, http.StatusConflict, "terminalize unused capture artifact reservations")
		return
	}
	var intentCount, acceptedCount, unresolvedCount int
	if err = tx.QueryRow(r.Context(), `
		SELECT count(*),count(*) FILTER(WHERE ar.result IN('accepted_unique','exact_replay')),count(*) FILTER(WHERE ar.upload_intent_id IS NULL)
		FROM recording_capture_artifact_intents a
		LEFT JOIN recording_capture_artifact_results ar ON ar.upload_intent_id=a.upload_intent_id
		WHERE a.producer_id=$1
	`, producerID).Scan(&intentCount, &acceptedCount, &unresolvedCount); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load capture producer artifacts")
		return
	}
	zeroReservation := intentCount == 0 && req.Result == "abandoned_empty" && req.DetailClass == "no_artifact_reservation"
	if !zeroReservation && (intentCount == 0 || unresolvedCount != 0 ||
		(req.Result == "completed" && acceptedCount == 0) ||
		(req.Result == "abandoned_empty" && acceptedCount != 0)) {
		util.WriteError(w, http.StatusConflict, "capture producer result does not match artifact state")
		return
	}
	tag, err := tx.Exec(r.Context(), `INSERT INTO recording_capture_producer_results(producer_id,result,detail_class) VALUES($1,$2,$3) ON CONFLICT(producer_id) DO NOTHING`, producerID, req.Result, req.DetailClass)
	if err != nil {
		util.WriteError(w, http.StatusConflict, fmt.Sprintf("finish capture producer: %v", err))
		return
	}
	if tag.RowsAffected() == 0 {
		var priorResult, priorDetail string
		if err = tx.QueryRow(r.Context(), `SELECT result,detail_class FROM recording_capture_producer_results WHERE producer_id=$1`, producerID).Scan(&priorResult, &priorDetail); err != nil || priorResult != req.Result || priorDetail != req.DetailClass {
			util.WriteError(w, http.StatusConflict, "capture producer result replay differs")
			return
		}
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit capture producer finish")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// chiURLParam is kept local to this file so producer path parsing remains
// explicit without widening the generic path helper surface.
func chiURLParam(r *http.Request, name string) string {
	return chi.URLParam(r, name)
}
