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
	recordingRecoveryProducerHeader = "X-Stoarama-Recording-Recovery-Producer"
	recordingRecoverySecretHeader   = "X-Stoarama-Recording-Recovery-Secret"
)

type recordingRecoveryPrincipal struct {
	GrantID    uuid.UUID
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
		if strings.TrimSpace(r.Header.Get(recordingRecoveryProducerHeader)) != "" || strings.TrimSpace(r.Header.Get(recordingRecoverySecretHeader)) != "" {
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
	producerID, err := uuid.Parse(strings.TrimSpace(r.Header.Get(recordingRecoveryProducerHeader)))
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
		SELECT g.id,g.producer_id,g.recording_job_id,g.lease_token,p.worker_id,p.node_id,n.account_id,g.upload_grace_until
		FROM recording_job_recovery_grants g
		JOIN recording_capture_producers p ON p.id=g.producer_id
		JOIN nodes n ON n.id=p.node_id
		WHERE g.producer_id=$1 AND g.recovery_secret_sha256=$2
		  AND ((g.revoked_at IS NULL AND g.upload_grace_until>transaction_timestamp())
		       OR g.revoke_reason='recovery_completed')
	`, producerID, hex.EncodeToString(secretHash[:])).Scan(&out.GrantID, &out.ProducerID, &out.JobID, &out.LeaseToken, &out.WorkerID, &out.NodeID, &out.AccountID, &out.ExpiresAt)
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
	producerID, parseErr := uuid.Parse(strings.TrimSpace(chiURLParam(r, "producerId")))
	if parseErr != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid recovery producer")
		return
	}
	terminalWithoutGrant := false
	authority := "recovery_grant"
	producerResult := ""
	if !ok {
		principal, nodeOK := nodePrincipalFromContext(r.Context())
		if !nodeOK || principal.NodeType != nodeTypeLocalRecorder {
			util.WriteError(w, http.StatusForbidden, "recovery capability does not authorize this producer")
			return
		}
		var currentLease bool
		var leaseExpiresAt time.Time
		err := s.pool.QueryRow(r.Context(), `
			SELECT producer.recording_job_id,producer.lease_token,COALESCE(result.result,''),
			       job.status='leased' AND job.lease_token=producer.lease_token
			         AND job.lease_owner=producer.worker_id AND job.lease_expires_at>transaction_timestamp(),
			       COALESCE(job.lease_expires_at,transaction_timestamp())
			FROM recording_capture_producers producer
			JOIN recording_jobs job ON job.id=producer.recording_job_id
			LEFT JOIN recording_capture_producer_results result ON result.producer_id=producer.id
			WHERE producer.id=$1 AND producer.node_id=$2 AND producer.worker_id=$3
		`, producerID, principal.NodeID, recorderWorkerID(principal)).Scan(&recovery.JobID, &recovery.LeaseToken, &producerResult, &currentLease, &leaseExpiresAt)
		if err != nil || (!currentLease && producerResult == "") {
			util.WriteError(w, http.StatusConflict, "producer has no active recovery grant or terminal result")
			return
		}
		recovery.ProducerID = producerID
		if currentLease && producerResult == "" {
			authority = "current_lease"
			recovery.ExpiresAt = leaseExpiresAt
		} else {
			authority = "terminal"
			terminalWithoutGrant = true
		}
	}
	if producerID != recovery.ProducerID {
		util.WriteError(w, http.StatusForbidden, "recovery capability does not authorize this producer")
		return
	}
	rows, err := s.pool.Query(r.Context(), `
		SELECT seal.upload_intent_id,seal.capture_sequence,seal.segment_start_ms,seal.size_bytes,seal.sha256,COALESCE(result.result,'')
		FROM recording_capture_artifact_seals seal
		LEFT JOIN recording_capture_artifact_results result ON result.upload_intent_id=seal.upload_intent_id
		WHERE seal.producer_id=$1 ORDER BY seal.capture_sequence
	`, recovery.ProducerID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load recovery artifacts")
		return
	}
	defer rows.Close()
	artifacts := make([]recordingRecoveryArtifact, 0)
	var nextSequence int64 = 1
	for rows.Next() {
		var intent uuid.UUID
		var artifact recordingRecoveryArtifact
		if err = rows.Scan(&intent, &artifact.CaptureSequence, &artifact.SegmentStartMs, &artifact.SizeBytes, &artifact.SHA256, &artifact.Result); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "scan recovery artifacts")
			return
		}
		artifact.IntentID = intent.String()
		if artifact.CaptureSequence >= nextSequence {
			nextSequence = artifact.CaptureSequence + 1
		}
		artifacts = append(artifacts, artifact)
	}
	if err = rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "read recovery artifacts")
		return
	}
	var expiresAt time.Time
	if terminalWithoutGrant {
		expiresAt = time.Now().UTC()
	} else if authority == "current_lease" {
		expiresAt = recovery.ExpiresAt
	} else {
		if err = s.pool.QueryRow(r.Context(), `
			SELECT grant_row.upload_grace_until,COALESCE(result.result,'')
			FROM recording_job_recovery_grants grant_row
			LEFT JOIN recording_capture_producer_results result ON result.producer_id=grant_row.producer_id
			WHERE grant_row.id=$1
		`, recovery.GrantID).Scan(&expiresAt, &producerResult); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "load recovery deadline")
			return
		}
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"producer_id": recovery.ProducerID, "job_id": recovery.JobID, "lease_token": recovery.LeaseToken,
		"expires_at": expiresAt, "authority": authority, "producer_result": producerResult, "next_capture_sequence": nextSequence, "artifacts": artifacts,
	})
}

func (s *Server) handleRecordingRecoveryFinish(w http.ResponseWriter, r *http.Request) {
	recovery, ok := recordingRecoveryFromContext(r.Context())
	if !ok || chiURLParam(r, "producerId") != recovery.ProducerID.String() {
		util.WriteError(w, http.StatusForbidden, "recovery capability does not authorize this producer")
		return
	}
	var req recordingCaptureProducerFinishRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Result != "completed" && req.Result != "abandoned_empty" {
		util.WriteError(w, http.StatusBadRequest, "invalid recovery producer result")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin recovery finish")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, recordingSurrenderJobLockKey(recovery.JobID)); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock recovery finish")
		return
	}
	var priorResult, priorRevoke string
	err = tx.QueryRow(r.Context(), `
		SELECT COALESCE(result.result,''),COALESCE(grant_row.revoke_reason,'')
		FROM recording_job_recovery_grants grant_row
		LEFT JOIN recording_capture_producer_results result ON result.producer_id=grant_row.producer_id
		WHERE grant_row.id=$1
	`, recovery.GrantID).Scan(&priorResult, &priorRevoke)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "load recovery finish replay")
		return
	}
	if priorResult != "" {
		if priorResult == req.Result && priorRevoke == "recovery_completed" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		util.WriteError(w, http.StatusConflict, "recovery producer already terminated differently")
		return
	}
	var grantCurrent bool
	if err = tx.QueryRow(r.Context(), `
		SELECT revoked_at IS NULL AND upload_grace_until>transaction_timestamp()
		FROM recording_job_recovery_grants WHERE id=$1 FOR UPDATE
	`, recovery.GrantID).Scan(&grantCurrent); err != nil || !grantCurrent {
		util.WriteError(w, http.StatusConflict, "recovery capability expired before finish")
		return
	}
	var sealCount, unresolved int
	if err = tx.QueryRow(r.Context(), `
		SELECT count(*),count(*) FILTER(WHERE result.upload_intent_id IS NULL)
		FROM recording_capture_artifact_seals seal
		LEFT JOIN recording_capture_artifact_results result ON result.upload_intent_id=seal.upload_intent_id
		WHERE seal.producer_id=$1
	`, recovery.ProducerID).Scan(&sealCount, &unresolved); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load recovery finish artifacts")
		return
	}
	if unresolved != 0 || (req.Result == "abandoned_empty" && sealCount != 0) || (req.Result == "completed" && sealCount == 0) {
		util.WriteError(w, http.StatusConflict, "recovery producer still has durable artifacts")
		return
	}
	tag, err := tx.Exec(r.Context(), `INSERT INTO recording_capture_producer_results(producer_id,result,detail_class) VALUES($1,$2,'') ON CONFLICT(producer_id) DO NOTHING`, recovery.ProducerID, req.Result)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "finish recovery producer")
		return
	}
	if tag.RowsAffected() == 0 {
		var prior string
		if err = tx.QueryRow(r.Context(), `SELECT result FROM recording_capture_producer_results WHERE producer_id=$1`, recovery.ProducerID).Scan(&prior); err != nil || prior != req.Result {
			util.WriteError(w, http.StatusConflict, "recovery producer result replay differs")
			return
		}
	}
	if _, err = tx.Exec(r.Context(), `
		UPDATE recording_job_recovery_grants
		SET revoked_at=transaction_timestamp(),revoke_reason='recovery_completed'
		WHERE id=$1 AND revoked_at IS NULL
	`, recovery.GrantID); err != nil {
		util.WriteError(w, http.StatusConflict, "close recovery capability")
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit recovery finish")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

var recordingSurrenderDetailClassRE = regexp.MustCompile(`^[a-z0-9_]{1,64}$`)

type recordingCaptureProducerReserveRequest struct {
	ProducerID           string `json:"producer_id"`
	CaptureOrdinal       int64  `json:"capture_ordinal"`
	SealedIntentLimit    int    `json:"sealed_intent_limit"`
	RecoverySecretSHA256 string `json:"recovery_secret_sha256"`
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

func producerSourceSnapshot(values ...string) string {
	h := sha256.New()
	_, _ = h.Write([]byte("recording-capture-producer-v1\n"))
	for _, value := range values {
		_, _ = fmt.Fprintf(h, "%d:%s\n", len([]byte(value)), value)
	}
	return hex.EncodeToString(h.Sum(nil))
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
	if principal.NodeType != nodeTypeLocalRecorder || leaseToken == nil {
		util.WriteError(w, http.StatusBadRequest, "surrender transport v1 requires a cloud generation fence")
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
	var currentStatus, currentOwner string
	var currentToken *uuid.UUID
	var windowOpen bool
	var attemptCount int
	var currentHead int64
	var currentIntent *uuid.UUID
	var currentClip *int64
	err = tx.QueryRow(r.Context(), `
		SELECT g.lease_owner,COALESCE(g.node_id,0),j.status,COALESCE(j.lease_owner,''),j.lease_token,transaction_timestamp()<j.window_end_at,j.attempt_count,
		       h.version,h.upload_intent_id,h.clip_id
		FROM recording_job_lease_generations g
		JOIN recording_jobs j ON j.id=g.recording_job_id
		JOIN recordings recording ON recording.id=j.recording_id
		JOIN recording_job_unique_heads h ON h.recording_job_id=g.recording_job_id AND h.lease_token=g.lease_token
		WHERE g.recording_job_id=$1 AND g.lease_token=$2
		  AND j.kind='continuous_window' AND j.window_end_at IS NOT NULL AND recording.capture_via='cloud'
		FOR UPDATE OF j,h
	`, jobID, leaseToken).Scan(&generationOwner, &generationNode, &currentStatus, &currentOwner, &currentToken, &windowOpen, &attemptCount, &currentHead, &currentIntent, &currentClip)
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
	if err = tx.QueryRow(r.Context(), `SELECT EXISTS(
		SELECT 1 FROM recorder_droplets d
		WHERE d.name<>$1 AND d.state IN ('provisioning','active') AND d.last_seen_at>=transaction_timestamp()-interval '2 minutes'
		  AND (SELECT count(*) FROM recording_jobs live WHERE live.status='leased' AND live.lease_owner=d.name AND live.lease_expires_at>transaction_timestamp()) < d.capacity
	)`, workerID).Scan(&alternate); err != nil {
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
	if err != nil || req.CaptureOrdinal <= 0 || req.SealedIntentLimit < 1 || req.SealedIntentLimit > 8 || !validLowerSHA256(req.RecoverySecretSHA256) {
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
	var recordingID, streamID int64
	var streamURL, sourceURL, sourcePageURL, provider, externalID string
	err = tx.QueryRow(r.Context(), `
		SELECT j.recording_id,COALESCE(r.stream_id,0),r.stream_url,COALESCE(st.source_url,''),COALESCE(st.source_page_url,''),
		       COALESCE(st.provider,''),COALESCE(st.external_id,'')
		FROM recording_jobs j
		JOIN recordings r ON r.id=j.recording_id
		LEFT JOIN streams st ON st.id=r.stream_id
		JOIN recording_job_lease_generations g ON g.recording_job_id=j.id AND g.lease_token=j.lease_token
		WHERE j.id=$1 AND j.status='leased' AND j.lease_owner=$2 AND j.lease_token=$3 AND j.lease_expires_at>transaction_timestamp()
		FOR UPDATE OF j,g
	`, jobID, workerID, leaseToken).Scan(&recordingID, &streamID, &streamURL, &sourceURL, &sourcePageURL, &provider, &externalID)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusConflict, "capture producer lease is stale")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load capture producer lease")
		return
	}
	snapshot := producerSourceSnapshot(strconv.FormatInt(recordingID, 10), strconv.FormatInt(streamID, 10), streamURL, sourceURL, sourcePageURL, provider, externalID)
	var existingID uuid.UUID
	var existingWorker, existingSecret string
	var existingNode int64
	var existingLimit int
	var existingTerminal bool
	err = tx.QueryRow(r.Context(), `SELECT p.id,p.worker_id,p.node_id,p.sealed_intent_limit,p.recovery_secret_sha256,EXISTS(SELECT 1 FROM recording_capture_producer_results result WHERE result.producer_id=p.id) FROM recording_capture_producers p WHERE p.recording_job_id=$1 AND p.lease_token=$2 AND p.capture_ordinal=$3`, jobID, leaseToken, req.CaptureOrdinal).Scan(&existingID, &existingWorker, &existingNode, &existingLimit, &existingSecret, &existingTerminal)
	if err == nil {
		if existingTerminal || existingID != producerID || existingWorker != workerID || existingNode != principal.NodeID || existingLimit != req.SealedIntentLimit || existingSecret != req.RecoverySecretSHA256 {
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
		INSERT INTO recording_capture_producers(id,recording_job_id,lease_token,capture_ordinal,worker_id,node_id,sealed_intent_limit,recovery_secret_sha256,source_snapshot_sha256)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT(recording_job_id,lease_token,capture_ordinal) DO NOTHING
	`, producerID, jobID, leaseToken, req.CaptureOrdinal, workerID, principal.NodeID, req.SealedIntentLimit, req.RecoverySecretSHA256, snapshot)
	if err != nil {
		util.WriteError(w, http.StatusConflict, fmt.Sprintf("reserve capture producer: %v", err))
		return
	}
	if commandTag.RowsAffected() == 0 {
		if err = tx.QueryRow(r.Context(), `SELECT p.id,p.worker_id,p.node_id,p.sealed_intent_limit,p.recovery_secret_sha256,EXISTS(SELECT 1 FROM recording_capture_producer_results result WHERE result.producer_id=p.id) FROM recording_capture_producers p WHERE p.recording_job_id=$1 AND p.lease_token=$2 AND p.capture_ordinal=$3`, jobID, leaseToken, req.CaptureOrdinal).Scan(&existingID, &existingWorker, &existingNode, &existingLimit, &existingSecret, &existingTerminal); err != nil || existingTerminal || existingID != producerID || existingWorker != workerID || existingNode != principal.NodeID || existingLimit != req.SealedIntentLimit || existingSecret != req.RecoverySecretSHA256 {
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
	var sealCount, unresolvedCount int
	if err = tx.QueryRow(r.Context(), `
		SELECT count(*),count(*) FILTER(WHERE ar.upload_intent_id IS NULL)
		FROM recording_capture_artifact_seals a
		LEFT JOIN recording_capture_artifact_results ar ON ar.upload_intent_id=a.upload_intent_id
		WHERE a.producer_id=$1
	`, producerID).Scan(&sealCount, &unresolvedCount); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load capture producer artifacts")
		return
	}
	if (req.Result != "abandoned_empty" && unresolvedCount != 0) ||
		(req.Result == "completed" && sealCount == 0) ||
		(req.Result == "abandoned_empty" && sealCount != 0) {
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
