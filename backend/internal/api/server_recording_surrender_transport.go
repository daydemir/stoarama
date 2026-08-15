package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/recordingapi"
	"github.com/daydemir/stoarama/backend/internal/surrenderplan"
	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	recordingRecoveryIntentHeader  = "X-Stoarama-Recording-Recovery-Intent"
	recordingRecoverySecretHeader  = "X-Stoarama-Recording-Recovery-Secret"
	recordingRecoverySessionHeader = "X-Stoarama-Recording-Recovery-Session"
)

var recordingSurrenderObservationClassRE = regexp.MustCompile(`^[a-z0-9_]{0,64}$`)
var recordingCaptureSegmentLeafRE = regexp.MustCompile(`^seg-[0-9]{8}-[0-9]{6}\.mp4$`)

type recordingRecoveryPrincipal struct {
	GrantID    uuid.UUID
	IntentID   uuid.UUID
	ProducerID uuid.UUID
	SetID      uuid.UUID
	Ordinal    int
	Authority  string
	JobID      int64
	LeaseToken uuid.UUID
	WorkerID   string
	NodeID     int64
	AccountID  int64
	NodeType   string
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
				node := nodePrincipal{NodeID: recovery.NodeID, AccountID: recovery.AccountID, NodeType: recovery.NodeType, DisplayName: recovery.WorkerID}
				ctx := context.WithValue(r.Context(), nodePrincipalContextKey, node)
				ctx = context.WithValue(ctx, recordingRecoveryContextKey{}, recovery)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
			util.WriteError(w, http.StatusUnauthorized, "unauthorized")
			return
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
		node := nodePrincipal{NodeID: recovery.NodeID, AccountID: recovery.AccountID, NodeType: recovery.NodeType, DisplayName: recovery.WorkerID}
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
		SELECT set_grant.id,artifact.artifact_id,plan.producer_id,artifact.set_id,artifact.ordinal,
		       plan.recording_job_id,plan.lease_token,generation.lease_owner,generation.node_id,node.account_id,
		       node.node_type,set_grant.upload_grace_until
		FROM recording_capture_materialized_artifacts artifact
		JOIN recording_capture_set_grants set_grant ON set_grant.set_id=artifact.set_id
		JOIN recording_capture_reservation_sets capture_set ON capture_set.id=artifact.set_id
		JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
		JOIN recording_job_lease_generations generation
		  ON generation.recording_job_id=plan.recording_job_id AND generation.lease_token=plan.lease_token
		JOIN nodes node ON node.id=generation.node_id
		WHERE artifact.artifact_id=$1 AND artifact.recovery_secret_sha256=$2
		  AND set_grant.upload_grace_until>transaction_timestamp()
		  AND NOT EXISTS(SELECT 1 FROM recording_capture_security_events event
		                 WHERE event.set_id=artifact.set_id AND (event.ordinal IS NULL OR event.ordinal=artifact.ordinal))
	`, intentID, hex.EncodeToString(secretHash[:])).Scan(
		&out.GrantID, &out.IntentID, &out.ProducerID, &out.SetID, &out.Ordinal,
		&out.JobID, &out.LeaseToken, &out.WorkerID, &out.NodeID, &out.AccountID, &out.NodeType, &out.ExpiresAt)
	if err == nil {
		out.Authority = "capture_set"
		return out, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return recordingRecoveryPrincipal{}, err
	}
	err = s.pool.QueryRow(r.Context(), `
		SELECT g.id,g.upload_intent_id,g.producer_id,g.recording_job_id,g.lease_token,p.worker_id,p.node_id,n.account_id,n.node_type,g.upload_grace_until
		FROM recording_job_recovery_grants g
		JOIN recording_capture_producers p ON p.id=g.producer_id
		JOIN nodes n ON n.id=p.node_id
		WHERE g.upload_intent_id=$1 AND g.recovery_secret_sha256=$2
		  AND g.upload_grace_until>transaction_timestamp()
		  AND NOT EXISTS(SELECT 1 FROM recording_job_recovery_grant_results result WHERE result.grant_id=g.id)
	`, intentID, hex.EncodeToString(secretHash[:])).Scan(&out.GrantID, &out.IntentID, &out.ProducerID, &out.JobID, &out.LeaseToken, &out.WorkerID, &out.NodeID, &out.AccountID, &out.NodeType, &out.ExpiresAt)
	if err != nil {
		return recordingRecoveryPrincipal{}, err
	}
	out.Authority = "legacy_intent"
	return out, nil
}

type minimumRateReader struct {
	reader    io.Reader
	startedAt time.Time
	read      int64
}

const (
	recoveryProxyGlobalConcurrency  = 16
	recoveryProxyAccountConcurrency = 4
	recoveryProxyNodeConcurrency    = 2
	recoveryProxyGlobalBytes        = int64(512 << 20)
	recoveryProxyAccountBytes       = int64(256 << 20)
	recoveryProxyNodeBytes          = int64(64 << 20)
	recoveryProxyMinimumRate        = int64(64 << 10)
	recoveryProxyRateGrace          = 10 * time.Second
	recoveryProxySessionLimit       = 5 * time.Minute
)

type recordingRecoveryObjectStore interface {
	PutReader(context.Context, string, string, io.Reader) (string, error)
	Copy(context.Context, string, string, string, map[string]string) (r2.ObjectHead, error)
	Head(context.Context, string) (r2.ObjectHead, error)
	DeleteObjects(context.Context, []string) error
}

func recoveryPromotionMetadata(setID uuid.UUID, ordinal int, sessionID uuid.UUID, size int64, sha string) map[string]string {
	return map[string]string{
		"stoarama-schema":     "recording-recovery-promotion-v1",
		"stoarama-set-id":     setID.String(),
		"stoarama-ordinal":    strconv.Itoa(ordinal),
		"stoarama-session-id": sessionID.String(),
		"stoarama-size":       strconv.FormatInt(size, 10),
		"stoarama-sha256":     sha,
	}
}

func recoveryPromotionMetadataSHA(metadata map[string]string) string {
	keys := make([]string, 0, len(metadata))
	for key := range metadata {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	h := sha256.New()
	_, _ = h.Write([]byte("stoarama.recording.recovery-provider-metadata.v1\x00"))
	for _, key := range keys {
		_, _ = fmt.Fprintf(h, "%d:%s%d:%s\n", len(key), key, len(metadata[key]), metadata[key])
	}
	return hex.EncodeToString(h.Sum(nil))
}

func recoveryPromotionHeadExact(head, copied r2.ObjectHead, expectedSize int64, expectedContentType string, expectedMetadata map[string]string) bool {
	if head.SizeBytes != expectedSize || strings.TrimSpace(head.ETag) == "" || head.ETag != copied.ETag || strings.TrimSpace(head.VersionID) != strings.TrimSpace(copied.VersionID) || strings.TrimSpace(head.ContentType) != strings.TrimSpace(expectedContentType) || len(head.Metadata) != len(expectedMetadata) {
		return false
	}
	for key, value := range expectedMetadata {
		if head.Metadata[key] != value {
			return false
		}
	}
	return true
}

func appendRecoveryPromotionResult(ctx context.Context, tx pgx.Tx, sessionID uuid.UUID, result string, size int64, sha string, provider r2.ObjectHead, metadataSHA string) error {
	var sizeValue any
	if size >= 0 {
		sizeValue = size
	}
	var shaValue any
	if sha != "" {
		shaValue = sha
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO recording_recovery_upload_session_results(
		 id,session_id,phase,result,observed_size,observed_sha256,provider_etag,provider_version_id,provider_metadata_sha256)
		VALUES($1,$2,'promotion',$3,$4,$5,$6,$7,$8)
		ON CONFLICT(session_id,phase) DO NOTHING
	`, uuid.New(), sessionID, result, sizeValue, shaValue, strings.TrimSpace(provider.ETag), strings.TrimSpace(provider.VersionID), metadataSHA)
	if err != nil || tag.RowsAffected() == 1 {
		return err
	}
	var exact bool
	err = tx.QueryRow(ctx, `
		SELECT result=$2 AND observed_size IS NOT DISTINCT FROM $3::bigint
		 AND observed_sha256 IS NOT DISTINCT FROM $4::text AND provider_etag IS NOT DISTINCT FROM $5::text
		 AND provider_version_id IS NOT DISTINCT FROM $6::text AND provider_metadata_sha256 IS NOT DISTINCT FROM $7::text
		FROM recording_recovery_upload_session_results WHERE session_id=$1 AND phase='promotion'
	`, sessionID, result, sizeValue, shaValue, strings.TrimSpace(provider.ETag), strings.TrimSpace(provider.VersionID), metadataSHA).Scan(&exact)
	if err != nil || !exact {
		return fmt.Errorf("recovery promotion result replay differs")
	}
	return nil
}

func (s *Server) newRecordingRecoveryObjectStore(ctx context.Context, cfg r2.Config) (recordingRecoveryObjectStore, error) {
	if s.recoveryStorageFactory != nil {
		return s.recoveryStorageFactory(ctx, cfg)
	}
	return r2.New(ctx, cfg)
}

func (r *minimumRateReader) Read(buffer []byte) (int, error) {
	n, err := r.reader.Read(buffer)
	r.read += int64(n)
	elapsed := time.Since(r.startedAt)
	if elapsed >= recoveryProxyRateGrace && r.read*int64(time.Second) < recoveryProxyMinimumRate*int64(elapsed) {
		return n, fmt.Errorf("recovery upload fell below minimum rate")
	}
	return n, err
}

func appendRecoveryUploadSessionResult(ctx context.Context, pool interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}, sessionID uuid.UUID, phase, result string, size int64, sha string) error {
	var sizeValue any
	if size >= 0 {
		sizeValue = size
	}
	var shaValue any
	if sha != "" {
		shaValue = sha
	}
	tag, err := pool.Exec(ctx, `
		INSERT INTO recording_recovery_upload_session_results(id,session_id,phase,result,observed_size,observed_sha256)
		VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(session_id,phase) DO NOTHING
	`, uuid.New(), sessionID, phase, result, sizeValue, shaValue)
	if err != nil || tag.RowsAffected() == 1 {
		return err
	}
	var exact bool
	err = pool.QueryRow(ctx, `
		SELECT result=$3 AND observed_size IS NOT DISTINCT FROM $4::bigint
		       AND observed_sha256 IS NOT DISTINCT FROM $5::text
		FROM recording_recovery_upload_session_results WHERE session_id=$1 AND phase=$2
	`, sessionID, phase, result, sizeValue, shaValue).Scan(&exact)
	if err != nil || !exact {
		return fmt.Errorf("recovery upload session result replay differs")
	}
	return nil
}

func appendRecoveryUploadSessionResultDetached(pool interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}, sessionID uuid.UUID, phase, result string, size int64, sha string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return appendRecoveryUploadSessionResult(ctx, pool, sessionID, phase, result, size, sha)
}

func recoveryUploadFailureResult(err error, observedBytes, expectedBytes int64, observedSHA, expectedSHA string) string {
	if err == nil {
		return "hash_mismatch"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, io.ErrUnexpectedEOF) {
		return "disconnect"
	}
	if strings.Contains(strings.ToLower(err.Error()), "minimum rate") {
		return "slow"
	}
	if observedBytes == expectedBytes && observedSHA == expectedSHA {
		return "response_ambiguous"
	}
	return "storage_5xx"
}

func (s *Server) handleRecordingRecoveryProxyUpload(w http.ResponseWriter, r *http.Request) {
	recovery, ok := recordingRecoveryFromContext(r.Context())
	intentID, intentErr := uuid.Parse(strings.TrimSpace(chiURLParam(r, "intentId")))
	sessionID, sessionErr := uuid.Parse(strings.TrimSpace(r.Header.Get(recordingRecoverySessionHeader)))
	if !ok || s.secrets == nil || recovery.Authority != "capture_set" || intentErr != nil || intentID != recovery.IntentID || sessionErr != nil || r.ContentLength <= 0 || r.ContentLength > surrenderplan.RecoveryArtifactMaxBytes {
		util.WriteError(w, http.StatusBadRequest, "invalid bounded recovery upload")
		return
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin recovery upload")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock_shared(hashtextextended('recording-worker-claim-v1',0))`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock recovery upload claim authority")
		return
	}
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock(hashtextextended('recording-recovery-proxy-quota-v1',0))`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock recovery upload quota")
		return
	}
	if _, err = tx.Exec(r.Context(), `SELECT recording_surrender_reconcile_expired_upload_sessions()`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "reconcile expired recovery upload sessions")
		return
	}
	var endpoint, region, bucket, objectKey, mimeType, accessKeyID string
	var secretEnc []byte
	var expectedSize int64
	var expectedSHA string
	var revision int
	var grantDeadline, sessionDeadline time.Time
	err = tx.QueryRow(r.Context(), `
		SELECT intent.endpoint,plan.destination_naming_snapshot->>'region',intent.bucket,intent.object_key,intent.mime_type,
		       destination.access_key_id,destination.secret_access_key_enc,seal.size_bytes,seal.sha256,set_grant.upload_grace_until,
		       COALESCE((SELECT max(session.revision) FROM recording_recovery_upload_sessions session
		                 WHERE session.set_id=artifact.set_id AND session.ordinal=artifact.ordinal),0)+1
		FROM recording_capture_materialized_artifacts artifact
		JOIN recording_capture_set_grants set_grant ON set_grant.set_id=artifact.set_id
		JOIN recording_capture_reservation_sets capture_set ON capture_set.id=artifact.set_id
		JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
		JOIN recording_capture_materialized_artifact_seals seal
		  ON seal.set_id=artifact.set_id AND seal.ordinal=artifact.ordinal AND seal.artifact_id=artifact.artifact_id
		JOIN recording_upload_intents intent ON intent.id=artifact.artifact_id AND intent.status='pending'
		JOIN storage_destinations destination ON destination.id=intent.storage_destination_id
		WHERE artifact.artifact_id=$1 AND artifact.set_id=$2 AND artifact.ordinal=$3 AND set_grant.id=$4
		  AND set_grant.upload_grace_until>transaction_timestamp()
		  AND NOT EXISTS(SELECT 1 FROM recording_capture_security_events event
		                 WHERE event.set_id=artifact.set_id AND (event.ordinal IS NULL OR event.ordinal=artifact.ordinal))
		  AND NOT EXISTS(SELECT 1 FROM recording_capture_artifact_grant_results result
		                 WHERE result.set_id=artifact.set_id AND result.ordinal=artifact.ordinal)
		FOR UPDATE OF artifact,set_grant,seal,intent,destination
	`, intentID, recovery.SetID, recovery.Ordinal, recovery.GrantID).Scan(
		&endpoint, &region, &bucket, &objectKey, &mimeType, &accessKeyID, &secretEnc,
		&expectedSize, &expectedSHA, &grantDeadline, &revision)
	if err != nil || expectedSize != r.ContentLength {
		util.WriteError(w, http.StatusConflict, "recovery upload authority is stale")
		return
	}
	// Exact same-session replay can resume a quarantined upload. A different
	// session may start only after the latest revision is terminal. A previously
	// promoted artifact is reconciled by object HEAD below without reading or
	// overwriting the request body.
	var replayExisting, resumePromotion, priorPromoted bool
	var replayQuarantine string
	var replayRevision int
	var replayExact bool
	var replayUploadResult, replayPromotionResult string
	replayErr := tx.QueryRow(r.Context(), `
		SELECT session.set_id=$2 AND session.ordinal=$3 AND session.node_id=$4
		       AND session.account_id=$5 AND session.declared_bytes=$6,
		       session.revision,session.quarantine_key,
		       COALESCE((SELECT result.result FROM recording_recovery_upload_session_results result
		                 WHERE result.session_id=session.id AND result.phase='upload'),''),
		       COALESCE((SELECT result.result FROM recording_recovery_upload_session_results result
		                 WHERE result.session_id=session.id AND result.phase='promotion'),'')
		FROM recording_recovery_upload_sessions session WHERE session.id=$1 FOR UPDATE
	`, sessionID, recovery.SetID, recovery.Ordinal, recovery.NodeID, recovery.AccountID, expectedSize).Scan(
		&replayExact, &replayRevision, &replayQuarantine, &replayUploadResult, &replayPromotionResult)
	if replayErr == nil {
		if !replayExact {
			util.WriteError(w, http.StatusConflict, "recovery upload session replay differs")
			return
		}
		replayExisting = true
		resumePromotion = replayUploadResult == "quarantined" && replayPromotionResult == ""
		priorPromoted = replayPromotionResult == "promoted"
		revision = replayRevision
		if !resumePromotion && !priorPromoted {
			util.WriteError(w, http.StatusConflict, "terminal recovery upload session requires a new revision")
			return
		}
	} else if !errors.Is(replayErr, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusInternalServerError, "load recovery upload session replay")
		return
	} else {
		var latestUpload, latestPromotion string
		latestErr := tx.QueryRow(r.Context(), `
			SELECT COALESCE((SELECT result.result FROM recording_recovery_upload_session_results result
			                 WHERE result.session_id=session.id AND result.phase='upload'),''),
			       COALESCE((SELECT result.result FROM recording_recovery_upload_session_results result
			                 WHERE result.session_id=session.id AND result.phase='promotion'),'')
			FROM recording_recovery_upload_sessions session
			WHERE session.set_id=$1 AND session.ordinal=$2 ORDER BY session.revision DESC LIMIT 1 FOR UPDATE
		`, recovery.SetID, recovery.Ordinal).Scan(&latestUpload, &latestPromotion)
		if latestErr == nil {
			priorPromoted = latestPromotion == "promoted"
			terminal := latestPromotion != "" || (latestUpload != "" && latestUpload != "quarantined")
			if !terminal && !priorPromoted {
				util.WriteError(w, http.StatusConflict, "recovery upload session is already in progress")
				return
			}
		} else if !errors.Is(latestErr, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusInternalServerError, "load latest recovery upload session")
			return
		}
	}
	var activeGlobal, activeNode, activeAccount int
	var activeGlobalBytes, activeNodeBytes, activeAccountBytes int64
	if err = tx.QueryRow(r.Context(), `
		SELECT count(*),count(*) FILTER(WHERE node_id=$1),count(*) FILTER(WHERE account_id=$2),
		       COALESCE(sum(declared_bytes),0),COALESCE(sum(declared_bytes) FILTER(WHERE node_id=$1),0),
		       COALESCE(sum(declared_bytes) FILTER(WHERE account_id=$2),0)
		FROM recording_recovery_upload_sessions session
		WHERE session.deadline_at>transaction_timestamp()
		  AND NOT EXISTS(SELECT 1 FROM recording_recovery_upload_session_results result
		                 WHERE result.session_id=session.id
		                   AND (result.phase='promotion' OR (result.phase='upload' AND result.result<>'quarantined')))
	`, recovery.NodeID, recovery.AccountID).Scan(&activeGlobal, &activeNode, &activeAccount, &activeGlobalBytes, &activeNodeBytes, &activeAccountBytes); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load recovery upload quota")
		return
	}
	if !replayExisting && !priorPromoted {
		if activeGlobal >= recoveryProxyGlobalConcurrency || activeNode >= recoveryProxyNodeConcurrency || activeAccount >= recoveryProxyAccountConcurrency || activeGlobalBytes+expectedSize > recoveryProxyGlobalBytes || activeNodeBytes+expectedSize > recoveryProxyNodeBytes || activeAccountBytes+expectedSize > recoveryProxyAccountBytes {
			util.WriteError(w, http.StatusTooManyRequests, "recovery upload quota is full")
			return
		}
		sessionDeadline = time.Now().UTC().Add(recoveryProxySessionLimit)
		if grantDeadline.Before(sessionDeadline) {
			sessionDeadline = grantDeadline
		}
		replayQuarantine = fmt.Sprintf(".stoarama-recovery/v1/%s/%d/%s", recovery.SetID, recovery.Ordinal, sessionID)
		if _, err = tx.Exec(r.Context(), `
			INSERT INTO recording_recovery_upload_sessions(id,set_id,ordinal,revision,node_id,account_id,declared_bytes,quarantine_key,deadline_at)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
		`, sessionID, recovery.SetID, recovery.Ordinal, revision, recovery.NodeID, recovery.AccountID, expectedSize, replayQuarantine, sessionDeadline); err != nil {
			util.WriteError(w, http.StatusConflict, "create recovery upload session")
			return
		}
	} else if replayExisting {
		if err = tx.QueryRow(r.Context(), `SELECT deadline_at FROM recording_recovery_upload_sessions WHERE id=$1`, sessionID).Scan(&sessionDeadline); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "load recovery upload session deadline")
			return
		}
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit recovery upload session")
		return
	}

	secret, err := s.secrets.Decrypt(secretEnc)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "decrypt recovery destination")
		return
	}
	client, err := s.newRecordingRecoveryObjectStore(r.Context(), r2.Config{AccessKey: accessKeyID, SecretKey: string(secret), Region: region, Bucket: bucket, Endpoint: endpoint})
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "build recovery destination")
		return
	}
	if priorPromoted {
		var promotedSessionID uuid.UUID
		var promotedETag, promotedVersion, promotedMetadataSHA string
		err = s.pool.QueryRow(r.Context(), `
			SELECT session.id,result.provider_etag,result.provider_version_id,result.provider_metadata_sha256
			FROM recording_recovery_upload_sessions session
			JOIN recording_recovery_upload_session_results result ON result.session_id=session.id
			WHERE session.set_id=$1 AND session.ordinal=$2 AND result.phase='promotion' AND result.result='promoted'
			ORDER BY session.revision DESC LIMIT 1
		`, recovery.SetID, recovery.Ordinal).Scan(&promotedSessionID, &promotedETag, &promotedVersion, &promotedMetadataSHA)
		if err != nil {
			util.WriteError(w, http.StatusConflict, "promoted recovery identity is unavailable")
			return
		}
		expectedMetadata := recoveryPromotionMetadata(recovery.SetID, recovery.Ordinal, promotedSessionID, expectedSize, expectedSHA)
		head, headErr := client.Head(r.Context(), objectKey)
		persistedIdentity := r2.ObjectHead{ETag: promotedETag, VersionID: promotedVersion, ContentType: mimeType}
		if headErr != nil || promotedMetadataSHA != recoveryPromotionMetadataSHA(expectedMetadata) || !recoveryPromotionHeadExact(head, persistedIdentity, expectedSize, mimeType, expectedMetadata) {
			util.WriteError(w, http.StatusConflict, "promoted recovery object cannot be reconciled")
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	observedSHA := expectedSHA
	observedBytes := expectedSize
	if !resumePromotion {
		uploadCtx, cancelUpload := context.WithDeadline(r.Context(), sessionDeadline)
		hash := sha256.New()
		counting := &countingReader{reader: io.TeeReader(io.LimitReader(r.Body, expectedSize+1), hash)}
		rateReader := &minimumRateReader{reader: counting, startedAt: time.Now()}
		_, uploadErr := client.PutReader(uploadCtx, replayQuarantine, mimeType, rateReader)
		cancelUpload()
		observedBytes = counting.read
		observedSHA = hex.EncodeToString(hash.Sum(nil))
		if uploadErr != nil || counting.read != expectedSize || observedSHA != expectedSHA {
			result := recoveryUploadFailureResult(uploadErr, counting.read, expectedSize, observedSHA, expectedSHA)
			if uploadErr == nil {
				result = "hash_mismatch"
			}
			_ = appendRecoveryUploadSessionResultDetached(s.pool, sessionID, "upload", result, counting.read, observedSHA)
			_ = client.DeleteObjects(context.Background(), []string{replayQuarantine})
			util.WriteError(w, http.StatusConflict, "recovery upload verification failed")
			return
		}
		if err = appendRecoveryUploadSessionResultDetached(s.pool, sessionID, "upload", "quarantined", counting.read, observedSHA); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "seal recovery quarantine result")
			return
		}
	}

	// Copy is intentionally outside every DB transaction and claim fence. A
	// security revocation can commit while storage is copying; finalization below
	// then rejects and quarantines/deletes the unaccepted object.
	promotionMetadata := recoveryPromotionMetadata(recovery.SetID, recovery.Ordinal, sessionID, expectedSize, expectedSHA)
	promotionMetadataSHA := recoveryPromotionMetadataSHA(promotionMetadata)
	promotionCtx, cancelPromotion := context.WithDeadline(r.Context(), sessionDeadline)
	promotionCopy, err := client.Copy(promotionCtx, replayQuarantine, objectKey, mimeType, promotionMetadata)
	var promotionIdentity r2.ObjectHead
	if err == nil {
		promotionIdentity, err = client.Head(promotionCtx, objectKey)
		if err == nil && !recoveryPromotionHeadExact(promotionIdentity, promotionCopy, expectedSize, mimeType, promotionMetadata) {
			err = fmt.Errorf("promoted recovery object identity differs")
		}
	}
	cancelPromotion()
	if err != nil {
		_ = appendRecoveryUploadSessionResultDetached(s.pool, sessionID, "promotion", "storage_5xx", observedBytes, observedSHA)
		util.WriteError(w, http.StatusBadGateway, "promote recovery upload")
		return
	}
	promotionTx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin recovery promotion")
		return
	}
	defer func() { _ = promotionTx.Rollback(r.Context()) }()
	if _, err = promotionTx.Exec(r.Context(), `SELECT pg_advisory_xact_lock_shared(hashtextextended('recording-worker-claim-v1',0))`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock recovery promotion authority")
		return
	}
	var stillAuthorized bool
	err = promotionTx.QueryRow(r.Context(), `
		SELECT set_grant.upload_grace_until>transaction_timestamp()
		 AND NOT EXISTS(SELECT 1 FROM recording_capture_security_events event
		                WHERE event.set_id=$2 AND (event.ordinal IS NULL OR event.ordinal=$3))
		 AND session.revision=(SELECT max(latest.revision) FROM recording_recovery_upload_sessions latest
		                       WHERE latest.set_id=$2 AND latest.ordinal=$3)
		FROM recording_capture_set_grants set_grant
		JOIN recording_recovery_upload_sessions session ON session.id=$1 AND session.set_id=set_grant.set_id
		WHERE set_grant.id=$4 FOR SHARE OF set_grant,session
	`, sessionID, recovery.SetID, recovery.Ordinal, recovery.GrantID).Scan(&stillAuthorized)
	if err != nil || !stillAuthorized {
		result := "aborted"
		var revoked bool
		_ = promotionTx.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM recording_capture_security_events event WHERE event.set_id=$1 AND (event.ordinal IS NULL OR event.ordinal=$2))`, recovery.SetID, recovery.Ordinal).Scan(&revoked)
		if revoked {
			result = "security_revoked"
		}
		if appendErr := appendRecoveryUploadSessionResult(r.Context(), promotionTx, sessionID, "promotion", result, observedBytes, observedSHA); appendErr == nil {
			_ = promotionTx.Commit(r.Context())
		}
		// The provider copy may have completed after this session's DB deadline.
		// Never issue an unversioned delete against the ordinary key: a newer exact
		// retry may already own it. The unaccepted object is unreachable from clip
		// authority and a later exact retry overwrites only this same artifact key.
		_ = client.DeleteObjects(context.Background(), []string{replayQuarantine})
		util.WriteError(w, http.StatusConflict, "recovery upload was revoked")
		return
	}
	if err = appendRecoveryPromotionResult(r.Context(), promotionTx, sessionID, "promoted", observedBytes, observedSHA, promotionIdentity, promotionMetadataSHA); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "seal recovery promotion result")
		return
	}
	if err = promotionTx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit recovery promotion")
		return
	}
	_ = client.DeleteObjects(context.Background(), []string{replayQuarantine})
	w.WriteHeader(http.StatusNoContent)
}

type countingReader struct {
	reader io.Reader
	read   int64
}

func (r *countingReader) Read(buffer []byte) (int, error) {
	n, err := r.reader.Read(buffer)
	r.read += int64(n)
	return n, err
}

type recordingCaptureRecoveryReportRequest struct {
	ReportID        string    `json:"report_id"`
	ReportType      string    `json:"report_type"`
	SizeBytes       *int64    `json:"size_bytes,omitempty"`
	SHA256          string    `json:"sha256,omitempty"`
	LocalObservedAt time.Time `json:"local_observed_at"`
}

func (s *Server) handleRecordingRecoveryReport(w http.ResponseWriter, r *http.Request) {
	recovery, ok := recordingRecoveryFromContext(r.Context())
	intentID, intentErr := uuid.Parse(strings.TrimSpace(chiURLParam(r, "intentId")))
	var req recordingCaptureRecoveryReportRequest
	reportID, reportErr := uuid.Nil, error(nil)
	if decodeErr := util.DecodeJSON(r, &req); decodeErr == nil {
		reportID, reportErr = uuid.Parse(strings.TrimSpace(req.ReportID))
	} else {
		reportErr = decodeErr
	}
	validShape := req.ReportType == "no_bytes" && req.SizeBytes == nil && req.SHA256 == ""
	validShape = validShape || ((req.ReportType == "partial_bytes" || req.ReportType == "sealed_bytes") && req.SizeBytes != nil && *req.SizeBytes > 0 && validLowerSHA256(req.SHA256))
	if !ok || recovery.Authority != "capture_set" || intentErr != nil || intentID != recovery.IntentID || reportErr != nil || !validShape || req.LocalObservedAt.IsZero() {
		util.WriteError(w, http.StatusBadRequest, "invalid recovery artifact report")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin recovery artifact report")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock_shared(hashtextextended('recording-worker-claim-v1',0))`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock recovery report claim authority")
		return
	}
	var current bool
	if err = tx.QueryRow(r.Context(), `
		SELECT set_grant.id=$4 AND set_grant.upload_grace_until>transaction_timestamp()
		  AND NOT EXISTS(SELECT 1 FROM recording_capture_security_events event
		                 WHERE event.set_id=artifact.set_id AND (event.ordinal IS NULL OR event.ordinal=artifact.ordinal))
		FROM recording_capture_materialized_artifacts artifact
		JOIN recording_capture_set_grants set_grant ON set_grant.set_id=artifact.set_id
		WHERE artifact.artifact_id=$1 AND artifact.set_id=$2 AND artifact.ordinal=$3
		FOR UPDATE OF artifact,set_grant
	`, intentID, recovery.SetID, recovery.Ordinal, recovery.GrantID).Scan(&current); err != nil || !current {
		util.WriteError(w, http.StatusConflict, "recovery artifact report authority is stale")
		return
	}
	var reportSHA any
	if req.SHA256 != "" {
		reportSHA = req.SHA256
	}
	tag, err := tx.Exec(r.Context(), `
		INSERT INTO recording_capture_recovery_reports(id,set_id,ordinal,report_type,size_bytes,sha256,local_observed_at)
		VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(id) DO NOTHING
	`, reportID, recovery.SetID, recovery.Ordinal, req.ReportType, req.SizeBytes, reportSHA, req.LocalObservedAt.UTC())
	if err != nil {
		util.WriteError(w, http.StatusConflict, "seal recovery artifact report")
		return
	}
	if tag.RowsAffected() == 0 {
		var exact bool
		if err = tx.QueryRow(r.Context(), `SELECT set_id=$2 AND ordinal=$3 AND report_type=$4 AND size_bytes IS NOT DISTINCT FROM $5 AND sha256 IS NOT DISTINCT FROM $6 AND local_observed_at=$7 FROM recording_capture_recovery_reports WHERE id=$1`, reportID, recovery.SetID, recovery.Ordinal, req.ReportType, req.SizeBytes, reportSHA, req.LocalObservedAt.UTC()).Scan(&exact); err != nil || !exact {
			util.WriteError(w, http.StatusConflict, "recovery artifact report replay differs")
			return
		}
	}
	if req.ReportType != "sealed_bytes" {
		result := "abandoned_no_bytes"
		if req.ReportType == "partial_bytes" {
			result = "unrecoverable_partial"
		}
		if _, err = tx.Exec(r.Context(), `INSERT INTO recording_capture_artifact_grant_results(set_id,ordinal,result,report_id) VALUES($1,$2,$3,$4) ON CONFLICT(set_id,ordinal) DO NOTHING`, recovery.SetID, recovery.Ordinal, result, reportID); err != nil {
			util.WriteError(w, http.StatusConflict, "seal recovery artifact terminal result")
			return
		}
		if _, err = sealCaptureSetTerminalTx(r.Context(), tx, recovery.SetID); err != nil {
			util.WriteError(w, http.StatusConflict, "seal recovery capture set")
			return
		}
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit recovery artifact report")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func sealCaptureSetTerminalTx(ctx context.Context, tx pgx.Tx, setID uuid.UUID) (bool, error) {
	var artifactCount int
	if err := tx.QueryRow(ctx, `SELECT artifact_count FROM recording_capture_reservation_sets WHERE id=$1 FOR UPDATE`, setID).Scan(&artifactCount); err != nil {
		return false, err
	}
	rows, err := tx.Query(ctx, `
		SELECT artifact.ordinal,result.result
		FROM recording_capture_materialized_artifacts artifact
		LEFT JOIN recording_capture_artifact_grant_results result
		  ON result.set_id=artifact.set_id AND result.ordinal=artifact.ordinal
		WHERE artifact.set_id=$1 ORDER BY artifact.ordinal FOR UPDATE OF artifact
	`, setID)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	var materialized []int
	setResult := "completed"
	var setSecurity bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM recording_capture_security_events event WHERE event.set_id=$1 AND event.ordinal IS NULL)`, setID).Scan(&setSecurity); err != nil {
		return false, err
	}
	if setSecurity {
		setResult = "security_revoked"
	}
	for rows.Next() {
		var ordinal int
		var result *string
		if err = rows.Scan(&ordinal, &result); err != nil {
			return false, err
		}
		if result == nil {
			return false, nil
		}
		materialized = append(materialized, ordinal)
		switch *result {
		case "security_revoked":
			setResult = "security_revoked"
		case "host_unreachable":
			if setResult != "security_revoked" {
				setResult = "host_unreachable"
			}
		case "abandoned_no_bytes", "unrecoverable_partial":
			if setResult == "completed" {
				setResult = "abandoned"
			}
		}
	}
	if err = rows.Err(); err != nil {
		return false, err
	}
	coverage := struct {
		ArtifactCount int      `json:"artifact_count"`
		Materialized  []int    `json:"materialized_ordinals"`
		Unused        [][2]int `json:"unused_ranges"`
	}{artifactCount, materialized, captureSetUnusedRanges(artifactCount, materialized)}
	coverageJSON, err := json.Marshal(coverage)
	if err != nil {
		return false, err
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO recording_capture_set_results(set_id,result,coverage_ranges,coverage_sha256)
		VALUES($1,$2,$3,encode(sha256(convert_to($3::jsonb::text,'UTF8')),'hex')) ON CONFLICT(set_id) DO NOTHING
	`, setID, setResult, coverageJSON)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

type recordingRecoverySecurityRevokeRequest struct {
	SetID          string `json:"set_id"`
	Ordinal        *int   `json:"ordinal,omitempty"`
	IdempotencyKey string `json:"idempotency_key"`
	Reason         string `json:"reason"`
}

func (s *Server) handleAdminRecordingRecoverySecurityRevoke(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	var req recordingRecoverySecurityRevokeRequest
	setID, setErr := uuid.Nil, error(nil)
	idempotencyKey, keyErr := uuid.Nil, error(nil)
	if decodeErr := util.DecodeJSON(r, &req); decodeErr == nil {
		setID, setErr = uuid.Parse(strings.TrimSpace(req.SetID))
		idempotencyKey, keyErr = uuid.Parse(strings.TrimSpace(req.IdempotencyKey))
	} else {
		setErr = decodeErr
		keyErr = decodeErr
	}
	if !ok || !adminOverrideFromContext(r.Context()) || principal.UserID <= 0 && principal.APIKeyID == nil || setErr != nil || keyErr != nil || req.Reason != "suspected_capability_compromise" && req.Reason != "suspected_seed_compromise" || req.Ordinal != nil && *req.Ordinal <= 0 || req.Reason == "suspected_seed_compromise" && req.Ordinal != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid recovery security revocation")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin recovery security revocation")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock(hashtextextended('recording-worker-claim-v1',0))`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock recovery security authority")
		return
	}
	var accountID int64
	var setTerminal bool
	if err = tx.QueryRow(r.Context(), `
		SELECT plan.account_id,EXISTS(SELECT 1 FROM recording_capture_set_results result WHERE result.set_id=capture_set.id)
		FROM recording_capture_reservation_sets capture_set
		JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
		WHERE capture_set.id=$1 FOR UPDATE OF capture_set,plan
	`, setID).Scan(&accountID, &setTerminal); err != nil {
		util.WriteError(w, http.StatusNotFound, "capture recovery set not found")
		return
	}
	if principal.AccountID != accountID {
		util.WriteError(w, http.StatusForbidden, "recovery security target belongs to another account")
		return
	}
	var eventID uuid.UUID
	var priorSet uuid.UUID
	var priorOrdinal *int
	var priorReason string
	err = tx.QueryRow(r.Context(), `SELECT id,set_id,ordinal,reason FROM recording_capture_security_events WHERE idempotency_key=$1`, idempotencyKey).Scan(&eventID, &priorSet, &priorOrdinal, &priorReason)
	if err == nil {
		if priorSet != setID || priorReason != req.Reason || (priorOrdinal == nil) != (req.Ordinal == nil) || priorOrdinal != nil && *priorOrdinal != *req.Ordinal {
			util.WriteError(w, http.StatusConflict, "security revocation replay differs")
			return
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusInternalServerError, "load recovery security replay")
		return
	} else {
		if setTerminal {
			util.WriteError(w, http.StatusConflict, "terminal recovery set cannot be revoked")
			return
		}
		eventID = uuid.New()
		var actorUserID any
		if principal.UserID > 0 {
			actorUserID = principal.UserID
		}
		if _, err = tx.Exec(r.Context(), `
			INSERT INTO recording_capture_security_events(id,set_id,ordinal,actor_user_id,actor_api_key_id,reason,idempotency_key)
			VALUES($1,$2,$3,$4,$5,$6,$7)
		`, eventID, setID, req.Ordinal, actorUserID, principal.APIKeyID, req.Reason, idempotencyKey); err != nil {
			util.WriteError(w, http.StatusConflict, "seal recovery security event")
			return
		}
	}
	if req.Ordinal == nil {
		_, err = tx.Exec(r.Context(), `
			INSERT INTO recording_capture_artifact_grant_results(set_id,ordinal,result,security_event_id)
			SELECT artifact.set_id,artifact.ordinal,'security_revoked',$2
			FROM recording_capture_materialized_artifacts artifact
			WHERE artifact.set_id=$1 ON CONFLICT(set_id,ordinal) DO NOTHING
		`, setID, eventID)
	} else {
		_, err = tx.Exec(r.Context(), `
			INSERT INTO recording_capture_artifact_grant_results(set_id,ordinal,result,security_event_id)
			SELECT artifact.set_id,artifact.ordinal,'security_revoked',$3
			FROM recording_capture_materialized_artifacts artifact
			WHERE artifact.set_id=$1 AND artifact.ordinal=$2
			ON CONFLICT(set_id,ordinal) DO NOTHING
		`, setID, *req.Ordinal, eventID)
	}
	if err != nil {
		util.WriteError(w, http.StatusConflict, "seal recovery security result")
		return
	}
	if _, err = tx.Exec(r.Context(), `
		INSERT INTO recording_capture_recovery_alert_events(id,set_id,event_type,dedupe_key)
		VALUES(gen_random_uuid(),$1,'security_revoked','capture-set:'||$1::text)
		ON CONFLICT(dedupe_key,event_type) DO NOTHING
	`, setID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "record recovery security alert")
		return
	}
	_, _ = sealCaptureSetTerminalTx(r.Context(), tx, setID)
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit recovery security revocation")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"event_id": eventID, "set_id": setID, "account_id": accountID})
}

type recordingClaimSuccessorProposalRequest struct {
	ProposalID   string `json:"proposal_id"`
	KeyPrefix    string `json:"key_prefix"`
	SecretSHA256 string `json:"secret_sha256"`
}

func claimSuccessorFactsSHA(nodeID, predecessor, successor int64, proposalID uuid.UUID, tokenID int64, tokenSHA string) string {
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "recording-claim-successor-v1\x00%d\x00%d\x00%d\x00%s\x00%d\x00%s", nodeID, predecessor, successor, proposalID, tokenID, tokenSHA)
	return hex.EncodeToString(h.Sum(nil))
}

func (s *Server) handleRecordingClaimSuccessorPropose(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	var req recordingClaimSuccessorProposalRequest
	proposalID, proposalErr := uuid.Nil, error(nil)
	if decodeErr := util.DecodeJSON(r, &req); decodeErr == nil {
		proposalID, proposalErr = uuid.Parse(strings.TrimSpace(req.ProposalID))
	} else {
		proposalErr = decodeErr
	}
	if !ok || principal.NodeTokenID <= 0 || proposalErr != nil || len(req.KeyPrefix) < 8 || len(req.KeyPrefix) > 32 || !validLowerSHA256(req.SecretSHA256) {
		util.WriteError(w, http.StatusBadRequest, "invalid claim successor proposal")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin claim successor proposal")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock(hashtextextended('recording-worker-claim-v1',0))`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock claim successor proposal")
		return
	}
	var predecessor int64
	var predecessorTokenID int64
	var headState string
	if err = tx.QueryRow(r.Context(), `SELECT generation,claim_token_id,state FROM recording_worker_claim_heads WHERE node_id=$1 FOR UPDATE`, principal.NodeID).Scan(&predecessor, &predecessorTokenID, &headState); err != nil || predecessorTokenID != principal.NodeTokenID || headState != "recovery_blocked" {
		var replay recordingapi.ClaimSuccessor
		if replayErr := tx.QueryRow(r.Context(), `
			SELECT proposal.id,proposal.node_id,proposal.predecessor_generation,proposal.successor_generation,proposal.successor_token_id
			FROM recording_worker_claim_successor_proposals proposal
			WHERE proposal.id=$1 AND proposal.node_id=$2 AND proposal.predecessor_token_id=$3
			  AND proposal.successor_key_prefix=$4 AND proposal.successor_secret_sha256=$5
		`, proposalID, principal.NodeID, principal.NodeTokenID, req.KeyPrefix, req.SecretSHA256).Scan(
			&replay.ProposalID, &replay.NodeID, &replay.PredecessorGeneration, &replay.SuccessorGeneration, &replay.SuccessorTokenID); replayErr == nil {
			_ = tx.Commit(r.Context())
			util.WriteJSON(w, http.StatusOK, replay)
			return
		}
		util.WriteError(w, http.StatusConflict, "claim successor authority is not blocked on this credential")
		return
	}
	successor := predecessor + 1
	var recoveryPending bool
	if err = tx.QueryRow(r.Context(), `
		SELECT EXISTS(
		  SELECT 1 FROM recording_capture_set_grants set_grant
		  JOIN recording_capture_reservation_sets capture_set ON capture_set.id=set_grant.set_id
		  JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
		  JOIN recording_job_lease_generations generation
		    ON generation.recording_job_id=plan.recording_job_id AND generation.lease_token=plan.lease_token
		  WHERE generation.node_id=$1 AND plan.origin_claim_generation=$2
		    AND NOT EXISTS(SELECT 1 FROM recording_capture_set_results result WHERE result.set_id=set_grant.set_id)
		)
	`, principal.NodeID, predecessor).Scan(&recoveryPending); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load claim recovery state")
		return
	}
	if recoveryPending {
		util.WriteError(w, http.StatusTooEarly, "durable recovery remains nonterminal")
		return
	}
	var successorTokenID int64
	if err = tx.QueryRow(r.Context(), `
		INSERT INTO node_tokens(node_id,key_prefix,secret_hash,last_used_at,recording_claim_generation,recording_claim_purpose)
		VALUES($1,$2,$3,transaction_timestamp(),$4,'existing_fence_only') RETURNING id
	`, principal.NodeID, req.KeyPrefix, req.SecretSHA256, successor).Scan(&successorTokenID); err != nil {
		util.WriteError(w, http.StatusConflict, "reserve claim successor credential")
		return
	}
	if _, err = tx.Exec(r.Context(), `
		INSERT INTO recording_worker_claim_successor_proposals
		  (id,node_id,predecessor_generation,successor_generation,predecessor_token_id,successor_token_id,successor_key_prefix,successor_secret_sha256)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8)
	`, proposalID, principal.NodeID, predecessor, successor, predecessorTokenID, successorTokenID, req.KeyPrefix, req.SecretSHA256); err != nil {
		util.WriteError(w, http.StatusConflict, "seal claim successor proposal")
		return
	}
	factsSHA := claimSuccessorFactsSHA(principal.NodeID, predecessor, successor, proposalID, successorTokenID, req.SecretSHA256)
	if _, err = tx.Exec(r.Context(), `
		INSERT INTO recording_worker_claim_generation_events(node_id,generation,predecessor_generation,claim_token_id,event_type,facts_sha256)
		VALUES($1,$2,$3,$4,'successor_proposed',$5);
		UPDATE node_tokens SET recording_claim_purpose='existing_fence_only' WHERE id=$6;
		UPDATE recording_worker_claim_heads
		SET generation=$2,claim_token_id=$4,state='successor_pending'
		WHERE node_id=$1 AND generation=$3 AND claim_token_id=$6 AND state='recovery_blocked'
	`, principal.NodeID, successor, predecessor, successorTokenID, factsSHA, predecessorTokenID); err != nil {
		util.WriteError(w, http.StatusConflict, "advance claim successor proposal")
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit claim successor proposal")
		return
	}
	util.WriteJSON(w, http.StatusOK, recordingapi.ClaimSuccessor{ProposalID: proposalID.String(), NodeID: principal.NodeID, PredecessorGeneration: predecessor, SuccessorGeneration: successor, SuccessorTokenID: successorTokenID})
}

func (s *Server) handleRecordingClaimSuccessorAck(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	proposalID, proposalErr := uuid.Parse(strings.TrimSpace(chiURLParam(r, "proposalId")))
	if !ok || principal.NodeTokenID <= 0 || proposalErr != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid claim successor acknowledgment")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin claim successor acknowledgment")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock(hashtextextended('recording-worker-claim-v1',0))`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock claim successor acknowledgment")
		return
	}
	var nodeID, predecessor, successor, predecessorTokenID, successorTokenID int64
	var secretSHA string
	err = tx.QueryRow(r.Context(), `
		SELECT node_id,predecessor_generation,successor_generation,predecessor_token_id,successor_token_id,successor_secret_sha256
		FROM recording_worker_claim_successor_proposals WHERE id=$1 FOR UPDATE
	`, proposalID).Scan(&nodeID, &predecessor, &successor, &predecessorTokenID, &successorTokenID, &secretSHA)
	if err != nil || nodeID != principal.NodeID || successorTokenID != principal.NodeTokenID {
		util.WriteError(w, http.StatusConflict, "claim successor acknowledgment authority differs")
		return
	}
	var alreadyEnabled bool
	if err = tx.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM recording_worker_claim_successor_results WHERE proposal_id=$1)`, proposalID).Scan(&alreadyEnabled); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load claim successor acknowledgment")
		return
	}
	if !alreadyEnabled {
		if _, err = tx.Exec(r.Context(), `INSERT INTO recording_worker_claim_successor_results(proposal_id,result) VALUES($1,'enabled')`, proposalID); err != nil {
			util.WriteError(w, http.StatusConflict, "seal claim successor acknowledgment")
			return
		}
		factsSHA := claimSuccessorFactsSHA(nodeID, predecessor, successor, proposalID, successorTokenID, secretSHA)
		if _, err = tx.Exec(r.Context(), `
			UPDATE node_tokens SET recording_claim_purpose='claim_current' WHERE id=$1;
			UPDATE recording_worker_claim_heads
			SET state='enabled',blocked_at=NULL,block_reason=NULL
			WHERE node_id=$2 AND generation=$3 AND claim_token_id=$1 AND state='successor_pending';
			INSERT INTO recording_worker_claim_generation_events(node_id,generation,predecessor_generation,claim_token_id,event_type,facts_sha256)
			VALUES($2,$3,$4,$1,'enabled',$5)
		`, successorTokenID, nodeID, successor, predecessor, factsSHA); err != nil {
			util.WriteError(w, http.StatusConflict, "enable claim successor")
			return
		}
	}
	if _, err = tx.Exec(r.Context(), `
		WITH retired AS (
		  UPDATE node_tokens token SET revoked_at=transaction_timestamp(),updated_at=transaction_timestamp()
		  WHERE token.id=$1 AND token.revoked_at IS NULL
		    AND NOT EXISTS(SELECT 1 FROM recording_jobs job
		                   WHERE job.lease_node_token_id=token.id AND job.status='leased'
		                     AND job.lease_expires_at>transaction_timestamp())
		  RETURNING token.id
		)
		INSERT INTO recording_worker_claim_generation_events
		  (node_id,generation,predecessor_generation,claim_token_id,event_type,facts_sha256)
		SELECT $2,$3,CASE WHEN $3=1 THEN NULL ELSE $3-1 END,$1,'retired',
		       encode(sha256(convert_to('recording-worker-claim-retired-v1','UTF8')
		         ||decode('00','hex')||convert_to($2::text,'UTF8')
		         ||decode('00','hex')||convert_to($3::text,'UTF8')
		         ||decode('00','hex')||convert_to($1::text,'UTF8')),'hex')
		FROM retired ON CONFLICT DO NOTHING
	`, predecessorTokenID, nodeID, predecessor); err != nil {
		util.WriteError(w, http.StatusConflict, "retire drained predecessor credential")
		return
	}
	var predecessorRetired bool
	if err = tx.QueryRow(r.Context(), `SELECT revoked_at IS NOT NULL FROM node_tokens WHERE id=$1`, predecessorTokenID).Scan(&predecessorRetired); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load predecessor retirement result")
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit claim successor acknowledgment")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"predecessor_retired": predecessorRetired})
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
	if recovery.Authority == "capture_set" {
		tx, err := s.pool.Begin(r.Context())
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, "begin recovery status")
			return
		}
		defer func() { _ = tx.Rollback(r.Context()) }()
		if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock_shared(hashtextextended('recording-worker-claim-v1',0))`); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "lock recovery status authority")
			return
		}
		var captureSequence, segmentStartMicro, size int64
		var sha, result string
		err = tx.QueryRow(r.Context(), `
			SELECT artifact.capture_sequence,COALESCE(seal.segment_start_microseconds,0),
			       COALESCE(seal.size_bytes,0),COALESCE(seal.sha256,''),COALESCE(result.result,'')
			FROM recording_capture_materialized_artifacts artifact
			LEFT JOIN recording_capture_materialized_artifact_seals seal
			  ON seal.set_id=artifact.set_id AND seal.ordinal=artifact.ordinal
			LEFT JOIN recording_capture_artifact_grant_results result
			  ON result.set_id=artifact.set_id AND result.ordinal=artifact.ordinal
			JOIN recording_capture_set_grants set_grant ON set_grant.set_id=artifact.set_id
			WHERE artifact.artifact_id=$1 AND artifact.set_id=$2 AND artifact.ordinal=$3
			  AND set_grant.id=$4 AND set_grant.upload_grace_until>transaction_timestamp()
			  AND NOT EXISTS(SELECT 1 FROM recording_capture_security_events event
			                 WHERE event.set_id=artifact.set_id AND (event.ordinal IS NULL OR event.ordinal=artifact.ordinal))
			FOR SHARE OF artifact,set_grant
		`, intentID, recovery.SetID, recovery.Ordinal, recovery.GrantID).Scan(&captureSequence, &segmentStartMicro, &size, &sha, &result)
		if err != nil {
			util.WriteError(w, http.StatusConflict, "recovery artifact is unavailable")
			return
		}
		if err = tx.Commit(r.Context()); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "commit recovery status")
			return
		}
		util.WriteJSON(w, http.StatusOK, map[string]any{
			"intent_id": intentID, "producer_id": recovery.ProducerID, "job_id": recovery.JobID,
			"lease_token": recovery.LeaseToken, "expires_at": recovery.ExpiresAt, "authority": "capture_set_grant",
			"artifacts": []recordingRecoveryArtifact{{IntentID: intentID.String(), CaptureSequence: captureSequence,
				SegmentStartMs: segmentStartMicro / 1000, SizeBytes: size, SHA256: sha, Result: result}},
		})
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin legacy recovery status")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock_shared(hashtextextended('recording-worker-claim-v1',0))`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock legacy recovery status authority")
		return
	}
	var out recordingRecoveryArtifact
	var result, producerResult string
	var segmentStart, size *int64
	var sha *string
	err = tx.QueryRow(r.Context(), `
		SELECT artifact.capture_sequence,seal.segment_start_ms,seal.size_bytes,seal.sha256,
		       COALESCE(result.result,''),COALESCE(producer_result.result,'')
		FROM recording_capture_artifact_intents artifact
		LEFT JOIN recording_capture_artifact_seals seal ON seal.upload_intent_id=artifact.upload_intent_id
		LEFT JOIN recording_capture_artifact_results result ON result.upload_intent_id=artifact.upload_intent_id
		LEFT JOIN recording_capture_producer_results producer_result ON producer_result.producer_id=artifact.producer_id
		JOIN recording_job_recovery_grants grant_row ON grant_row.id=$3 AND grant_row.upload_intent_id=artifact.upload_intent_id
		WHERE artifact.upload_intent_id=$1 AND artifact.producer_id=$2
		  AND grant_row.upload_grace_until>transaction_timestamp()
	`, intentID, recovery.ProducerID, recovery.GrantID).Scan(&out.CaptureSequence, &segmentStart, &size, &sha, &result, &producerResult)
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
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit legacy recovery status")
		return
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
	if err := util.DecodeJSON(r, &req); err != nil || (req.Result != "abandoned_unsealed" && req.Result != "unrecoverable_partial" && req.Result != "acknowledged_terminal") {
		util.WriteError(w, http.StatusBadRequest, "invalid reserved-unsealed recovery result")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin recovery intent finish")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock_shared(hashtextextended('recording-worker-claim-v1',0))`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock recovery intent claim authority")
		return
	}
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, recordingSurrenderJobLockKey(recovery.JobID)); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock recovery intent finish")
		return
	}
	var current bool
	var priorReason, priorResult string
	if err = tx.QueryRow(r.Context(), `
		SELECT grant_row.upload_grace_until>transaction_timestamp() AND grant_result.grant_id IS NULL,
		       COALESCE(grant_result.result,''),COALESCE(result.result,'')
		FROM recording_job_recovery_grants grant_row
		LEFT JOIN recording_capture_artifact_results result ON result.upload_intent_id=grant_row.upload_intent_id
		LEFT JOIN recording_job_recovery_grant_results grant_result ON grant_result.grant_id=grant_row.id
		WHERE grant_row.id=$1 AND grant_row.upload_intent_id=$2 FOR UPDATE OF grant_row
	`, recovery.GrantID, intentID).Scan(&current, &priorReason, &priorResult); err != nil {
		util.WriteError(w, http.StatusConflict, "recovery capability is unavailable")
		return
	}
	if priorReason == "recovery_completed" && (priorResult == req.Result || (req.Result == "acknowledged_terminal" && priorResult != "")) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if !current {
		util.WriteError(w, http.StatusConflict, "recovery capability expired before finish")
		return
	}
	if req.Result == "acknowledged_terminal" {
		if priorResult != "accepted_unique" && priorResult != "exact_replay" && priorResult != "abandoned_unsealed" && priorResult != "unrecoverable_partial" {
			util.WriteError(w, http.StatusConflict, "recovery acknowledgment lacks a terminal artifact result")
			return
		}
	} else {
		if _, err = tx.Exec(r.Context(), `
			INSERT INTO recording_capture_artifact_results(upload_intent_id,result)
			SELECT $1,$2 WHERE NOT EXISTS(SELECT 1 FROM recording_capture_artifact_seals WHERE upload_intent_id=$1)
			ON CONFLICT(upload_intent_id) DO NOTHING
		`, intentID, req.Result); err != nil {
			util.WriteError(w, http.StatusConflict, "finish reserved-unsealed recovery intent")
			return
		}
	}
	if _, err = tx.Exec(r.Context(), `INSERT INTO recording_job_recovery_grant_results(grant_id,result) VALUES($1,'recovery_completed') ON CONFLICT(grant_id) DO NOTHING`, recovery.GrantID); err != nil {
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

func revalidateRecordingNodeCredential(ctx context.Context, tx pgx.Tx, principal nodePrincipal) error {
	var valid bool
	err := tx.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM node_tokens token JOIN nodes node ON node.id=token.node_id
			WHERE token.id=$1 AND token.node_id=$2 AND token.revoked_at IS NULL
			  AND token.recording_claim_purpose IN('claim_current','existing_fence_only')
			  AND node.status=ANY($3::text[])
		)
	`, principal.NodeTokenID, principal.NodeID, nodeTokenAllowedStatuses()).Scan(&valid)
	if err != nil {
		return err
	}
	if !valid {
		return fmt.Errorf("recording node credential is stale")
	}
	return nil
}

func revalidateRecordingLeaseCredential(ctx context.Context, tx pgx.Tx, principal nodePrincipal, jobID int64, leaseToken *uuid.UUID) error {
	var valid bool
	err := tx.QueryRow(ctx, `
		SELECT CASE
		  WHEN job.lease_credential_state='exact' THEN
		    recording_surrender_token_can_access_lease($1,$2,job.lease_node_token_id,job.lease_claim_generation)
		  WHEN job.lease_credential_state='legacy_unknown' THEN job.lease_owner=$5 AND EXISTS(
		    SELECT 1 FROM node_tokens token WHERE token.id=$1 AND token.node_id=$2 AND token.revoked_at IS NULL)
		  ELSE false END
		FROM recording_jobs job
		WHERE job.id=$3 AND job.lease_token IS NOT DISTINCT FROM $4
	`, principal.NodeTokenID, principal.NodeID, jobID, leaseToken, recorderWorkerID(principal)).Scan(&valid)
	if err != nil || !valid {
		return fmt.Errorf("recording lease credential is stale")
	}
	return nil
}

func validLowerSHA256(v string) bool {
	if len(v) != 64 || strings.ToLower(v) != v {
		return false
	}
	_, err := hex.DecodeString(v)
	return err == nil
}

type recordingCaptureSetPlanRequest struct {
	PlanID               string `json:"plan_id"`
	SetID                string `json:"set_id"`
	ProducerID           string `json:"producer_id"`
	CaptureOrdinal       int64  `json:"capture_ordinal"`
	FirstCaptureSequence int64  `json:"first_capture_sequence"`
}

type recordingCaptureSetPlanResponse struct {
	PlanID                  uuid.UUID `json:"plan_id"`
	SetID                   uuid.UUID `json:"set_id"`
	ProducerID              uuid.UUID `json:"producer_id"`
	CaptureOrdinal          int64     `json:"capture_ordinal"`
	FirstCaptureSequence    int64     `json:"first_capture_sequence"`
	AccountID               int64     `json:"account_id"`
	RecordingID             int64     `json:"recording_id"`
	JobID                   int64     `json:"job_id"`
	LeaseToken              uuid.UUID `json:"lease_token"`
	OriginClaimGeneration   int64     `json:"origin_claim_generation"`
	SnapshotGeneration      int64     `json:"snapshot_generation"`
	SourceSnapshotSHA256    string    `json:"source_snapshot_sha256"`
	DestinationNamingSHA256 string    `json:"destination_naming_sha256"`
	PlanAt                  time.Time `json:"plan_at"`
	WindowEndAt             time.Time `json:"window_end_at"`
	DurationMicroseconds    int64     `json:"duration_microseconds"`
	ClipDurationSeconds     int       `json:"clip_duration_seconds"`
	ArtifactCount           int       `json:"artifact_count"`
	SegmentTimesArgument    string    `json:"segment_times_argument"`
	MaxArtifactBytes        int64     `json:"max_artifact_bytes"`
	ExpiresAt               time.Time `json:"expires_at"`
}

func scanRecordingCaptureSetPlan(row pgx.Row, out *recordingCaptureSetPlanResponse) error {
	return row.Scan(&out.PlanID, &out.SetID, &out.ProducerID, &out.CaptureOrdinal, &out.FirstCaptureSequence, &out.AccountID, &out.RecordingID,
		&out.JobID, &out.LeaseToken, &out.OriginClaimGeneration, &out.SnapshotGeneration,
		&out.SourceSnapshotSHA256, &out.DestinationNamingSHA256, &out.PlanAt, &out.WindowEndAt,
		&out.DurationMicroseconds, &out.ClipDurationSeconds, &out.ArtifactCount, &out.SegmentTimesArgument,
		&out.MaxArtifactBytes, &out.ExpiresAt)
}

const recordingCaptureSetPlanSelect = `
	SELECT id,set_id,producer_id,capture_ordinal,first_capture_sequence,account_id,recording_id,recording_job_id,lease_token,
	       COALESCE(origin_claim_generation,0),snapshot_generation,source_snapshot_sha256,destination_naming_sha256,
	       plan_at,window_end_at,duration_microseconds,clip_duration_seconds,artifact_count,
	       segment_times_argument,max_artifact_bytes,expires_at
	FROM recording_capture_set_plans WHERE id=$1`

func (s *Server) handleRecordingCaptureSetPlan(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	if !ok || principal.NodeTokenID <= 0 {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	jobID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	leaseToken, err := recordingLeaseToken(r)
	var req recordingCaptureSetPlanRequest
	if err != nil || leaseToken == nil || util.DecodeJSON(r, &req) != nil || req.CaptureOrdinal <= 0 || req.FirstCaptureSequence <= 0 {
		util.WriteError(w, http.StatusBadRequest, "invalid capture set plan request")
		return
	}
	planID, planErr := uuid.Parse(strings.TrimSpace(req.PlanID))
	setID, setErr := uuid.Parse(strings.TrimSpace(req.SetID))
	producerID, producerErr := uuid.Parse(strings.TrimSpace(req.ProducerID))
	if planErr != nil || setErr != nil || producerErr != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid capture set plan identity")
		return
	}

	// Discover tenant before row locks. The authoritative reselect occurs after
	// accounts are locked in ascending identity order.
	var workerAccountID, targetAccountID int64
	if err = s.pool.QueryRow(r.Context(), `SELECT $2::bigint,recording.account_id FROM recording_jobs job JOIN recordings recording ON recording.id=job.recording_id WHERE job.id=$1`, jobID, principal.AccountID).Scan(&workerAccountID, &targetAccountID); err != nil {
		util.WriteError(w, http.StatusConflict, "capture set plan job is unavailable")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin capture set plan")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock_shared(hashtextextended('recording-worker-claim-v1',0))`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock capture claim authority")
		return
	}
	if _, err = tx.Exec(r.Context(), `SELECT recording_surrender_expire_set_plans()`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "expire stale capture set plans")
		return
	}
	lo, hi := workerAccountID, targetAccountID
	if lo > hi {
		lo, hi = hi, lo
	}
	if _, err = tx.Exec(r.Context(), `SELECT id FROM accounts WHERE id=ANY($1::bigint[]) ORDER BY id FOR SHARE`, []int64{lo, hi}); err != nil {
		util.WriteError(w, http.StatusConflict, "lock capture set accounts")
		return
	}
	var tokenValid bool
	if err = tx.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM node_tokens token JOIN nodes node ON node.id=token.node_id WHERE token.id=$1 AND token.node_id=$2 AND token.revoked_at IS NULL AND node.status=ANY($3::text[]))`, principal.NodeTokenID, principal.NodeID, nodeTokenAllowedStatuses()).Scan(&tokenValid); err != nil || !tokenValid {
		util.WriteError(w, http.StatusUnauthorized, "capture credential is stale")
		return
	}
	var existing recordingCaptureSetPlanResponse
	err = scanRecordingCaptureSetPlan(tx.QueryRow(r.Context(), recordingCaptureSetPlanSelect, planID), &existing)
	if err == nil {
		var terminalResult string
		if resultErr := tx.QueryRow(r.Context(), `SELECT COALESCE((SELECT result FROM recording_capture_set_plan_results WHERE plan_id=$1),'')`, planID).Scan(&terminalResult); resultErr != nil || terminalResult != "" {
			util.WriteError(w, http.StatusConflict, "capture set plan is already terminal")
			return
		}
		if existing.SetID != setID || existing.ProducerID != producerID || existing.CaptureOrdinal != req.CaptureOrdinal || existing.FirstCaptureSequence != req.FirstCaptureSequence || existing.JobID != jobID || existing.LeaseToken != *leaseToken {
			util.WriteError(w, http.StatusConflict, "capture set plan replay differs")
			return
		}
		if err = tx.Commit(r.Context()); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "commit capture set plan replay")
			return
		}
		util.WriteJSON(w, http.StatusOK, existing)
		return
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusInternalServerError, "load capture set plan replay")
		return
	}
	var recordingID, accountID, streamID, destinationID int64
	var clipDuration int
	var windowEnd, dbNow time.Time
	var originGeneration *int64
	var credentialState string
	var sourceSnapshot, destinationSnapshot []byte
	err = tx.QueryRow(r.Context(), `
		SELECT recording.id,recording.account_id,recording.stream_id,recording.storage_destination_id,
		       job.clip_duration_sec,job.window_end_at,transaction_timestamp(),job.lease_claim_generation,
		       job.lease_credential_state,
		       recording_surrender_source_snapshot(recording.id),
		       recording_surrender_destination_snapshot(recording.id)
		FROM recording_jobs job
		JOIN recordings recording ON recording.id=job.recording_id
		JOIN streams stream ON stream.id=recording.stream_id
		JOIN storage_destinations destination ON destination.id=recording.storage_destination_id
		WHERE job.id=$1 AND job.status='leased' AND job.lease_owner=$2 AND job.lease_token=$3
		  AND job.lease_expires_at>transaction_timestamp() AND job.kind='continuous_window'
		  AND job.window_end_at>transaction_timestamp() AND recording.account_id=$4
		FOR UPDATE OF job,recording,stream,destination
	`, jobID, recorderWorkerID(principal), leaseToken, targetAccountID).Scan(&recordingID, &accountID, &streamID, &destinationID, &clipDuration, &windowEnd, &dbNow, &originGeneration, &credentialState, &sourceSnapshot, &destinationSnapshot)
	if err != nil || credentialState != "exact" || originGeneration == nil {
		util.WriteError(w, http.StatusConflict, "capture set requires an exact current lease credential")
		return
	}
	plan, err := surrenderplan.Build(dbNow, windowEnd, clipDuration)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "capture set plan is unsupported")
		return
	}
	sourceHash := sha256.Sum256(sourceSnapshot)
	destinationHash := sha256.Sum256(destinationSnapshot)
	snapshotGeneration := int64(1)
	if err = tx.QueryRow(r.Context(), `SELECT COALESCE(max(id),0)+1 FROM stream_source_revisions WHERE stream_id=$1`, streamID).Scan(&snapshotGeneration); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load source snapshot generation")
		return
	}
	segmentHash := sha256.Sum256([]byte(plan.SplitTimesArgument))
	_, err = tx.Exec(r.Context(), `
		INSERT INTO recording_capture_set_plans(id,set_id,account_id,recording_id,storage_destination_id,recording_job_id,lease_token,
		 origin_claim_generation,producer_id,capture_ordinal,first_capture_sequence,snapshot_generation,source_snapshot,source_snapshot_sha256,
		 destination_naming_snapshot,destination_naming_sha256,plan_at,window_end_at,duration_microseconds,
		 clip_duration_seconds,artifact_count,segment_times_argument,segment_times_sha256,max_artifact_bytes,expires_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$17+interval '30 seconds')
	`, planID, setID, accountID, recordingID, destinationID, jobID, leaseToken, originGeneration, producerID, req.CaptureOrdinal,
		req.FirstCaptureSequence, snapshotGeneration, sourceSnapshot, hex.EncodeToString(sourceHash[:]), destinationSnapshot, hex.EncodeToString(destinationHash[:]),
		dbNow, windowEnd, plan.DurationMicro, clipDuration, plan.ArtifactCount, plan.SplitTimesArgument,
		hex.EncodeToString(segmentHash[:]), surrenderplan.RecoveryArtifactMaxBytes)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "create capture set plan")
		return
	}
	if err = scanRecordingCaptureSetPlan(tx.QueryRow(r.Context(), recordingCaptureSetPlanSelect, planID), &existing); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load capture set plan")
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit capture set plan")
		return
	}
	util.WriteJSON(w, http.StatusOK, existing)
}

func (s *Server) handleRecordingCaptureSetCommit(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	jobID, jobOK := parseInt64Path(w, r, "id")
	planID, planErr := uuid.Parse(strings.TrimSpace(chiURLParam(r, "planId")))
	leaseToken, leaseErr := recordingLeaseToken(r)
	var req struct {
		MerkleRootSHA256 string `json:"merkle_root_sha256"`
	}
	if !ok || principal.NodeTokenID <= 0 || !jobOK || planErr != nil || leaseErr != nil || leaseToken == nil || util.DecodeJSON(r, &req) != nil || !validLowerSHA256(req.MerkleRootSHA256) {
		util.WriteError(w, http.StatusBadRequest, "invalid capture set commitment")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin capture set commitment")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock(hashtextextended('recording-worker-claim-v1',0))`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock capture set commitment")
		return
	}
	var setID uuid.UUID
	var count int
	var exact bool
	var priorResult string
	err = tx.QueryRow(r.Context(), `
		SELECT plan.set_id,plan.artifact_count,
		 job.id=$2 AND job.status='leased' AND job.lease_token=$3 AND job.lease_owner=$4
		 AND job.lease_expires_at>transaction_timestamp() AND plan.expires_at>transaction_timestamp()
		 AND job.lease_credential_state='exact'
		 AND recording_surrender_token_can_access_lease($5,$6,job.lease_node_token_id,job.lease_claim_generation)
		 AND plan.source_snapshot=recording_surrender_source_snapshot(recording.id)
		 AND plan.destination_naming_snapshot=recording_surrender_destination_snapshot(recording.id),
		 COALESCE(result.result,'')
		FROM recording_capture_set_plans plan
		JOIN recording_jobs job ON job.id=plan.recording_job_id
		JOIN recordings recording ON recording.id=job.recording_id
		JOIN streams stream ON stream.id=recording.stream_id
		JOIN storage_destinations destination ON destination.id=recording.storage_destination_id
		JOIN node_tokens token ON token.id=$5
		LEFT JOIN recording_capture_set_plan_results result ON result.plan_id=plan.id
		WHERE plan.id=$1 AND plan.recording_job_id=$2 AND plan.lease_token=$3
		FOR UPDATE OF plan,job,recording,stream,destination,token
	`, planID, jobID, leaseToken, recorderWorkerID(principal), principal.NodeTokenID, principal.NodeID).Scan(&setID, &count, &exact, &priorResult)
	if err != nil || !exact {
		util.WriteError(w, http.StatusConflict, "capture set plan is stale")
		return
	}
	if priorResult != "" {
		var priorRoot string
		if priorResult != "accepted_set" || tx.QueryRow(r.Context(), `SELECT merkle_root_sha256 FROM recording_capture_reservation_sets WHERE plan_id=$1`, planID).Scan(&priorRoot) != nil || priorRoot != req.MerkleRootSHA256 {
			util.WriteError(w, http.StatusConflict, "capture set commitment replay differs")
			return
		}
		if err = tx.Commit(r.Context()); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "commit capture set replay")
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if _, err = tx.Exec(r.Context(), `INSERT INTO recording_capture_reservation_sets(id,plan_id,merkle_root_sha256,artifact_count) VALUES($1,$2,$3,$4)`, setID, planID, req.MerkleRootSHA256, count); err == nil {
		_, err = tx.Exec(r.Context(), `INSERT INTO recording_capture_set_plan_results(plan_id,result,set_id) VALUES($1,'accepted_set',$2)`, planID, setID)
	}
	if err != nil {
		util.WriteError(w, http.StatusConflict, "commit capture set")
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit capture set transaction")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func parseCaptureMerkleProof(values []string, ordinal int) (surrenderplan.Proof, string, error) {
	proof := surrenderplan.Proof{Ordinal: ordinal, Siblings: make([][32]byte, len(values))}
	h := sha256.New()
	_, _ = h.Write([]byte("stoarama.recording.capture-artifact-proof.v1\x00"))
	for index, value := range values {
		if !validLowerSHA256(value) {
			return surrenderplan.Proof{}, "", fmt.Errorf("invalid proof hash")
		}
		decoded, _ := hex.DecodeString(value)
		copy(proof.Siblings[index][:], decoded)
		_, _ = h.Write(decoded)
	}
	return proof, hex.EncodeToString(h.Sum(nil)), nil
}

func (s *Server) handleRecordingCaptureArtifactMaterialize(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	jobID, jobOK := parseInt64Path(w, r, "id")
	setID, setErr := uuid.Parse(strings.TrimSpace(chiURLParam(r, "setId")))
	ordinal64, ordinalErr := strconv.ParseInt(strings.TrimSpace(chiURLParam(r, "ordinal")), 10, 32)
	leaseToken, leaseErr := recordingLeaseToken(r)
	var req recordingapiCaptureArtifactMaterialization
	if !ok || principal.NodeTokenID <= 0 || !jobOK || setErr != nil || ordinalErr != nil || ordinal64 <= 0 || leaseErr != nil || leaseToken == nil || util.DecodeJSON(r, &req) != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid capture artifact materialization")
		return
	}
	artifactID, artifactErr := uuid.Parse(strings.TrimSpace(req.ArtifactID))
	secretBytes, secretErr := hex.DecodeString(strings.TrimSpace(req.RecoverySecretSHA256))
	ordinal := int(ordinal64)
	proof, proofSHA, proofErr := parseCaptureMerkleProof(req.Proof, ordinal)
	if artifactErr != nil || secretErr != nil || len(secretBytes) != sha256.Size || !validLowerSHA256(req.RecoverySecretSHA256) || req.CaptureSequence <= 0 || proofErr != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid capture artifact proof")
		return
	}
	var secretHash [32]byte
	copy(secretHash[:], secretBytes)

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin capture artifact materialization")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock_shared(hashtextextended('recording-worker-claim-v1',0))`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock capture artifact authority")
		return
	}
	var set surrenderplan.SetIdentity
	var rootHex string
	var firstSequence int64
	var tokenValid, leaseCurrent bool
	err = tx.QueryRow(r.Context(), `
		SELECT plan.account_id,plan.recording_id,plan.recording_job_id,plan.lease_token,
		       COALESCE(plan.origin_claim_generation,0),plan.producer_id,plan.source_snapshot_sha256,
		       plan.destination_naming_sha256,reservation.artifact_count,reservation.merkle_root_sha256,
		       plan.first_capture_sequence,
		       (recording_surrender_token_can_access_lease($6,$7,job.lease_node_token_id,job.lease_claim_generation)
		           OR (token.recording_claim_generation=plan.origin_claim_generation
		             AND EXISTS(SELECT 1 FROM recording_capture_set_grants set_grant
		                        WHERE set_grant.set_id=reservation.id AND set_grant.upload_grace_until>transaction_timestamp())
		             AND EXISTS(SELECT 1 FROM recording_job_lease_generations generation
		                        WHERE generation.recording_job_id=plan.recording_job_id AND generation.lease_token=plan.lease_token
		                          AND generation.node_id=$7)))),
		       (job.status='leased' AND job.lease_owner=$5 AND job.lease_token=$4
		         AND job.lease_expires_at>transaction_timestamp() AND job.lease_credential_state='exact'
		         AND NOT EXISTS(SELECT 1 FROM recording_capture_producer_stop_events stop WHERE stop.set_id=reservation.id))
		       OR EXISTS(SELECT 1 FROM recording_capture_set_grants set_grant
		                 WHERE set_grant.set_id=reservation.id AND set_grant.upload_grace_until>transaction_timestamp())
		FROM recording_capture_reservation_sets reservation
		JOIN recording_capture_set_plans plan ON plan.id=reservation.plan_id
		JOIN recording_jobs job ON job.id=plan.recording_job_id
		JOIN node_tokens token ON token.id=$6 AND token.node_id=$7
		WHERE reservation.id=$1 AND plan.recording_job_id=$2 AND plan.lease_token=$3
		FOR UPDATE OF reservation,plan,job,token
	`, setID, jobID, leaseToken, leaseToken, recorderWorkerID(principal), principal.NodeTokenID, principal.NodeID).Scan(
		&set.AccountID, &set.RecordingID, &set.JobID, &set.LeaseToken, &set.OriginClaimGeneration,
		&set.ProducerID, &set.SnapshotSHA256, &set.DestinationNamingSHA256, &set.ArtifactCount,
		&rootHex, &firstSequence, &tokenValid, &leaseCurrent)
	if err != nil || !tokenValid || !leaseCurrent || ordinal > set.ArtifactCount || req.CaptureSequence != firstSequence+int64(ordinal-1) {
		util.WriteError(w, http.StatusConflict, "capture artifact authority is stale")
		return
	}
	set.SetID, set.MIME, set.MaxBytes = setID, "video/mp4", surrenderplan.RecoveryArtifactMaxBytes
	rootBytes, _ := hex.DecodeString(rootHex)
	var root [32]byte
	copy(root[:], rootBytes)
	if len(rootBytes) != sha256.Size || !surrenderplan.VerifyCommittedProof(root, set, ordinal, artifactID, secretHash, proof) {
		util.WriteError(w, http.StatusConflict, "capture artifact proof does not match committed set")
		return
	}
	tag, err := tx.Exec(r.Context(), `
		INSERT INTO recording_capture_materialized_artifacts(set_id,ordinal,artifact_id,recovery_secret_sha256,capture_sequence,proof,proof_sha256)
		VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(set_id,ordinal) DO NOTHING
	`, setID, ordinal, artifactID, req.RecoverySecretSHA256, req.CaptureSequence, req.Proof, proofSHA)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "materialize capture artifact")
		return
	}
	if tag.RowsAffected() == 0 {
		var exact bool
		if err = tx.QueryRow(r.Context(), `SELECT artifact_id=$3 AND recovery_secret_sha256=$4 AND capture_sequence=$5 AND proof_sha256=$6 FROM recording_capture_materialized_artifacts WHERE set_id=$1 AND ordinal=$2`, setID, ordinal, artifactID, req.RecoverySecretSHA256, req.CaptureSequence, proofSHA).Scan(&exact); err != nil || !exact {
			util.WriteError(w, http.StatusConflict, "capture artifact replay differs")
			return
		}
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit capture artifact materialization")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type recordingapiCaptureArtifactMaterialization struct {
	ArtifactID           string   `json:"artifact_id"`
	CaptureSequence      int64    `json:"capture_sequence"`
	RecoverySecretSHA256 string   `json:"recovery_secret_sha256"`
	Proof                []string `json:"proof"`
}

func (s *Server) handleRecordingCaptureSetStopAck(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	jobID, jobOK := parseInt64Path(w, r, "id")
	setID, setErr := uuid.Parse(strings.TrimSpace(chiURLParam(r, "setId")))
	leaseToken, leaseErr := recordingLeaseToken(r)
	var req recordingapi.CaptureStopAck
	if !ok || principal.NodeTokenID <= 0 || !jobOK || setErr != nil || leaseErr != nil || leaseToken == nil || util.DecodeJSON(r, &req) != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid capture stop acknowledgment")
		return
	}
	ackID, ackErr := uuid.Parse(strings.TrimSpace(req.AckID))
	calculatedSHA, digestErr := recordingapi.CaptureStopInventorySHA(req)
	if ackErr != nil || digestErr != nil || calculatedSHA != req.InventorySHA256 || req.RetainedDirectoryInode == 0 || req.RetainedDirectoryDevice > uint64(^uint64(0)>>1) || req.RetainedDirectoryInode > uint64(^uint64(0)>>1) || len(req.Members) > surrenderplan.MaxArtifacts {
		util.WriteError(w, http.StatusBadRequest, "invalid capture stop inventory")
		return
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin capture stop acknowledgment")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock_shared(hashtextextended('recording-worker-claim-v1',0))`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock capture stop acknowledgment")
		return
	}
	var stopEventID uuid.UUID
	var set surrenderplan.SetIdentity
	var rootHex string
	var firstSequence int64
	var artifactCount int
	currentFenceAuthority := true
	scanAuthority := func(row pgx.Row) error {
		return row.Scan(&stopEventID, &set.AccountID, &set.RecordingID, &set.JobID, &set.LeaseToken,
			&set.OriginClaimGeneration, &set.ProducerID, &set.SnapshotSHA256,
			&set.DestinationNamingSHA256, &artifactCount, &rootHex, &firstSequence)
	}
	err = scanAuthority(tx.QueryRow(r.Context(), `
		SELECT stop.id,plan.account_id,plan.recording_id,plan.recording_job_id,plan.lease_token,
		       COALESCE(plan.origin_claim_generation,0),plan.producer_id,plan.source_snapshot_sha256,
		       plan.destination_naming_sha256,capture_set.artifact_count,capture_set.merkle_root_sha256,
		       plan.first_capture_sequence
		FROM recording_capture_producer_stop_events stop
		JOIN recording_capture_reservation_sets capture_set ON capture_set.id=stop.set_id
		JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
		JOIN recording_jobs job ON job.id=plan.recording_job_id
		JOIN node_tokens token ON token.id=$5
		WHERE capture_set.id=$1 AND plan.recording_job_id=$2 AND plan.lease_token=$3
		  AND job.status='leased' AND job.lease_owner=$4 AND job.lease_token=$3
		  AND job.lease_expires_at>transaction_timestamp()
		  AND job.lease_credential_state='exact'
		  AND recording_surrender_token_can_access_lease($5,$6,job.lease_node_token_id,job.lease_claim_generation)
		  AND NOT EXISTS(SELECT 1 FROM recording_capture_producer_stop_acks ack WHERE ack.stop_event_id=stop.id)
		ORDER BY stop.required_at,stop.id LIMIT 1
		FOR UPDATE OF stop,capture_set,plan,job,token
	`, setID, jobID, leaseToken, recorderWorkerID(principal), principal.NodeTokenID, principal.NodeID))
	if errors.Is(err, pgx.ErrNoRows) {
		currentFenceAuthority = false
		// A stop ACK is fsynced locally before its request. If that response is
		// lost and the lease expires, the exact set recovery grant and immutable
		// origin generation remain authority to replay the inventory. The current
		// same-node successor token may service the old fence, but no other node,
		// set, generation, or expired grant can author an ACK.
		err = scanAuthority(tx.QueryRow(r.Context(), `
			SELECT stop.id,plan.account_id,plan.recording_id,plan.recording_job_id,plan.lease_token,
			       COALESCE(plan.origin_claim_generation,0),plan.producer_id,plan.source_snapshot_sha256,
			       plan.destination_naming_sha256,capture_set.artifact_count,capture_set.merkle_root_sha256,
			       plan.first_capture_sequence
			FROM recording_capture_producer_stop_events stop
			JOIN recording_capture_reservation_sets capture_set ON capture_set.id=stop.set_id
			JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
			JOIN recording_capture_set_grants set_grant ON set_grant.set_id=capture_set.id
			JOIN recording_job_lease_generations lease_generation
			  ON lease_generation.recording_job_id=plan.recording_job_id
			 AND lease_generation.lease_token=plan.lease_token
			JOIN recording_worker_claim_heads head ON head.node_id=lease_generation.node_id
			JOIN node_tokens origin_token
			  ON origin_token.node_id=lease_generation.node_id
			 AND origin_token.recording_claim_generation=plan.origin_claim_generation
			JOIN node_tokens presented_token ON presented_token.id=$4
			WHERE capture_set.id=$1 AND plan.recording_job_id=$2 AND plan.lease_token=$3
			  AND lease_generation.node_id=$5
			  AND set_grant.origin_claim_generation IS NOT DISTINCT FROM plan.origin_claim_generation
			  AND set_grant.recovery_block_generation=plan.origin_claim_generation
			  AND set_grant.upload_grace_until>transaction_timestamp()
			  AND head.generation>=set_grant.recovery_block_generation
			  AND head.state IN('recovery_blocked','successor_pending','enabled')
			  AND recording_surrender_token_can_access_lease(
			        presented_token.id,lease_generation.node_id,origin_token.id,plan.origin_claim_generation)
			  AND NOT EXISTS(SELECT 1 FROM recording_capture_producer_stop_acks ack WHERE ack.stop_event_id=stop.id)
			ORDER BY stop.required_at,stop.id LIMIT 1
			FOR UPDATE OF stop,capture_set,plan,set_grant,lease_generation,head,origin_token,presented_token
		`, setID, jobID, leaseToken, principal.NodeTokenID, principal.NodeID))
	}
	if errors.Is(err, pgx.ErrNoRows) {
		var exact bool
		if replayErr := tx.QueryRow(r.Context(), `
			SELECT ack.set_id=$2 AND ack.inventory_sha256=$3
			FROM recording_capture_producer_stop_acks ack WHERE ack.id=$1
		`, ackID, setID, req.InventorySHA256).Scan(&exact); replayErr == nil && exact {
			rows, queryErr := tx.Query(r.Context(), `
				SELECT ordinal FROM recording_capture_artifact_grant_results
				WHERE stop_ack_id=$1 AND result='acknowledged_no_bytes' ORDER BY ordinal
			`, ackID)
			if queryErr != nil {
				util.WriteError(w, http.StatusInternalServerError, "load capture stop replay result")
				return
			}
			var noBytes []int
			for rows.Next() {
				var ordinal int
				if queryErr = rows.Scan(&ordinal); queryErr != nil {
					break
				}
				noBytes = append(noBytes, ordinal)
			}
			queryErr = errors.Join(queryErr, rows.Err())
			rows.Close()
			if queryErr != nil {
				util.WriteError(w, http.StatusInternalServerError, "scan capture stop replay result")
				return
			}
			_ = tx.Commit(r.Context())
			util.WriteJSON(w, http.StatusOK, recordingapi.CaptureStopAckResult{NoBytesOrdinals: noBytes})
			return
		}
		util.WriteError(w, http.StatusConflict, "capture stop authority is stale")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load capture stop authority")
		return
	}
	set.SetID, set.ArtifactCount, set.MIME, set.MaxBytes = setID, artifactCount, "video/mp4", surrenderplan.RecoveryArtifactMaxBytes
	rootBytes, rootErr := hex.DecodeString(rootHex)
	var root [32]byte
	copy(root[:], rootBytes)
	seenOrdinals := make(map[int]struct{}, len(req.Members))
	seenFiles := make(map[string]struct{}, len(req.Members))
	for _, member := range req.Members {
		artifactID, artifactErr := uuid.Parse(strings.TrimSpace(member.ArtifactID))
		secretBytes, secretErr := hex.DecodeString(strings.TrimSpace(member.RecoverySecretSHA256))
		proof, proofSHA, proofErr := parseCaptureMerkleProof(member.Proof, member.Ordinal)
		if artifactErr != nil || secretErr != nil || len(secretBytes) != sha256.Size || proofErr != nil || member.Ordinal < 1 || member.Ordinal > artifactCount || member.CaptureSequence != firstSequence+int64(member.Ordinal-1) || member.Device > uint64(^uint64(0)>>1) || member.Inode == 0 || member.Inode > uint64(^uint64(0)>>1) || member.SizeBytes < 0 || member.SizeBytes > surrenderplan.RecoveryArtifactMaxBytes || !recordingCaptureSegmentLeafRE.MatchString(member.RelativeName) {
			util.WriteError(w, http.StatusBadRequest, "invalid capture stop member")
			return
		}
		if _, duplicate := seenOrdinals[member.Ordinal]; duplicate {
			util.WriteError(w, http.StatusBadRequest, "duplicate capture stop member")
			return
		}
		if _, duplicate := seenFiles[member.RelativeName]; duplicate {
			util.WriteError(w, http.StatusBadRequest, "duplicate capture stop file")
			return
		}
		seenOrdinals[member.Ordinal], seenFiles[member.RelativeName] = struct{}{}, struct{}{}
		var secretHash [32]byte
		copy(secretHash[:], secretBytes)
		if rootErr != nil || len(rootBytes) != sha256.Size || !surrenderplan.VerifyCommittedProof(root, set, member.Ordinal, artifactID, secretHash, proof) {
			util.WriteError(w, http.StatusConflict, "capture stop member proof differs")
			return
		}
		tag, insertErr := tx.Exec(r.Context(), `
			INSERT INTO recording_capture_materialized_artifacts(set_id,ordinal,artifact_id,recovery_secret_sha256,capture_sequence,proof,proof_sha256)
			VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(set_id,ordinal) DO NOTHING
		`, setID, member.Ordinal, artifactID, member.RecoverySecretSHA256, member.CaptureSequence, member.Proof, proofSHA)
		if insertErr != nil {
			util.WriteError(w, http.StatusConflict, "materialize stopped capture artifact")
			return
		}
		if tag.RowsAffected() == 0 {
			var exact bool
			if insertErr = tx.QueryRow(r.Context(), `SELECT artifact_id=$3 AND recovery_secret_sha256=$4 AND capture_sequence=$5 AND proof_sha256=$6 FROM recording_capture_materialized_artifacts WHERE set_id=$1 AND ordinal=$2`, setID, member.Ordinal, artifactID, member.RecoverySecretSHA256, member.CaptureSequence, proofSHA).Scan(&exact); insertErr != nil || !exact {
				util.WriteError(w, http.StatusConflict, "stopped capture artifact replay differs")
				return
			}
		}
	}
	if _, err = tx.Exec(r.Context(), `
		INSERT INTO recording_capture_producer_stop_acks(id,stop_event_id,set_id,inventory_sha256,retained_directory_device,retained_directory_inode)
		VALUES($1,$2,$3,$4,$5,$6)
	`, ackID, stopEventID, setID, req.InventorySHA256, int64(req.RetainedDirectoryDevice), int64(req.RetainedDirectoryInode)); err != nil {
		util.WriteError(w, http.StatusConflict, "seal capture stop acknowledgment")
		return
	}
	for _, member := range req.Members {
		if _, err = tx.Exec(r.Context(), `
			INSERT INTO recording_capture_stop_ack_members(stop_ack_id,ordinal,artifact_id,device,inode,size_bytes,relative_name)
			VALUES($1,$2,$3,$4,$5,$6,$7)
		`, ackID, member.Ordinal, member.ArtifactID, int64(member.Device), int64(member.Inode), member.SizeBytes, member.RelativeName); err != nil {
			util.WriteError(w, http.StatusConflict, "seal capture stop inventory member")
			return
		}
		if currentFenceAuthority && member.SizeBytes == 0 {
			if _, err = tx.Exec(r.Context(), `
				INSERT INTO recording_capture_artifact_grant_results(set_id,ordinal,result,stop_ack_id)
				VALUES($1,$2,'acknowledged_no_bytes',$3)
			`, setID, member.Ordinal, ackID); err != nil {
				util.WriteError(w, http.StatusConflict, "terminalize zero-byte stopped capture artifact")
				return
			}
		}
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit capture stop acknowledgment")
		return
	}
	var noBytes []int
	if currentFenceAuthority {
		for _, member := range req.Members {
			if member.SizeBytes == 0 {
				noBytes = append(noBytes, member.Ordinal)
			}
		}
	}
	util.WriteJSON(w, http.StatusOK, recordingapi.CaptureStopAckResult{NoBytesOrdinals: noBytes})
}

func captureSetUnusedRanges(count int, materialized []int) [][2]int {
	used := make(map[int]struct{}, len(materialized))
	for _, ordinal := range materialized {
		used[ordinal] = struct{}{}
	}
	var ranges [][2]int
	for ordinal := 1; ordinal <= count; {
		if _, ok := used[ordinal]; ok {
			ordinal++
			continue
		}
		start := ordinal
		for ordinal <= count {
			if _, ok := used[ordinal]; ok {
				break
			}
			ordinal++
		}
		ranges = append(ranges, [2]int{start, ordinal - 1})
	}
	return ranges
}

func (s *Server) handleRecordingCaptureSetFinish(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	jobID, jobOK := parseInt64Path(w, r, "id")
	setID, setErr := uuid.Parse(strings.TrimSpace(chiURLParam(r, "setId")))
	leaseToken, leaseErr := recordingLeaseToken(r)
	if !ok || principal.NodeTokenID <= 0 || !jobOK || setErr != nil || leaseErr != nil || leaseToken == nil {
		util.WriteError(w, http.StatusBadRequest, "invalid capture set finish")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin capture set finish")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock_shared(hashtextextended('recording-worker-claim-v1',0))`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock capture set finish")
		return
	}
	var artifactCount int
	var exact bool
	err = tx.QueryRow(r.Context(), `
		SELECT capture_set.artifact_count,
		       job.id=$2 AND job.lease_token=$3 AND job.lease_owner=$4
		         AND job.lease_credential_state='exact'
		         AND recording_surrender_token_can_access_lease($5,$6,job.lease_node_token_id,job.lease_claim_generation)
		FROM recording_capture_reservation_sets capture_set
		JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
		JOIN recording_jobs job ON job.id=plan.recording_job_id
		JOIN node_tokens token ON token.id=$5
		WHERE capture_set.id=$1 AND plan.recording_job_id=$2 AND plan.lease_token=$3
		FOR UPDATE OF capture_set,plan,job,token
	`, setID, jobID, leaseToken, recorderWorkerID(principal), principal.NodeTokenID, principal.NodeID).Scan(&artifactCount, &exact)
	if err != nil || !exact {
		util.WriteError(w, http.StatusConflict, "capture set finish authority is stale")
		return
	}
	rows, err := tx.Query(r.Context(), `
		SELECT artifact.ordinal,result.result
		FROM recording_capture_materialized_artifacts artifact
		LEFT JOIN recording_capture_artifact_grant_results result
		  ON result.set_id=artifact.set_id AND result.ordinal=artifact.ordinal
		WHERE artifact.set_id=$1 ORDER BY artifact.ordinal FOR UPDATE OF artifact
	`, setID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load capture set coverage")
		return
	}
	var materialized []int
	hasNoBytes := false
	for rows.Next() {
		var ordinal int
		var result *string
		if err = rows.Scan(&ordinal, &result); err != nil {
			rows.Close()
			util.WriteError(w, http.StatusInternalServerError, "scan capture set coverage")
			return
		}
		if result == nil || (*result != "accepted_unique" && *result != "exact_replay" && *result != "acknowledged_no_bytes") {
			rows.Close()
			util.WriteError(w, http.StatusConflict, "capture set has a nonterminal materialized artifact")
			return
		}
		hasNoBytes = hasNoBytes || *result == "acknowledged_no_bytes"
		materialized = append(materialized, ordinal)
	}
	rows.Close()
	coverage := struct {
		ArtifactCount int      `json:"artifact_count"`
		Materialized  []int    `json:"materialized_ordinals"`
		Unused        [][2]int `json:"unused_ranges"`
	}{artifactCount, materialized, captureSetUnusedRanges(artifactCount, materialized)}
	coverageJSON, _ := json.Marshal(coverage)
	setResult := "completed"
	if len(materialized) == 0 || hasNoBytes {
		setResult = "abandoned"
	}
	var coverageSHA string
	tag, err := tx.Exec(r.Context(), `INSERT INTO recording_capture_set_results(set_id,result,coverage_ranges,coverage_sha256) VALUES($1,$2,$3,encode(sha256(convert_to($3::jsonb::text,'UTF8')),'hex')) ON CONFLICT(set_id) DO NOTHING`, setID, setResult, coverageJSON)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "seal capture set result")
		return
	}
	if tag.RowsAffected() == 0 {
		var replay bool
		if err = tx.QueryRow(r.Context(), `SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex')`, coverageJSON).Scan(&coverageSHA); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "hash capture set coverage")
			return
		}
		if err = tx.QueryRow(r.Context(), `SELECT result=$2 AND coverage_sha256=$3 FROM recording_capture_set_results WHERE set_id=$1`, setID, setResult, coverageSHA).Scan(&replay); err != nil || !replay {
			util.WriteError(w, http.StatusConflict, "capture set finish replay differs")
			return
		}
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit capture set finish")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleRecordingCaptureSetEmptyRecovery(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	jobID, jobOK := parseInt64Path(w, r, "id")
	setID, setErr := uuid.Parse(strings.TrimSpace(chiURLParam(r, "setId")))
	leaseToken, leaseErr := recordingLeaseToken(r)
	var req struct {
		ReportID string `json:"report_id"`
	}
	if !ok || principal.NodeTokenID <= 0 || !jobOK || setErr != nil || leaseErr != nil || leaseToken == nil || util.DecodeJSON(r, &req) != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid empty capture set recovery")
		return
	}
	reportID, reportErr := uuid.Parse(strings.TrimSpace(req.ReportID))
	if reportErr != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid empty capture set report")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin empty capture set recovery")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock_shared(hashtextextended('recording-worker-claim-v1',0))`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock empty capture set recovery")
		return
	}
	var grantID uuid.UUID
	var artifactCount int
	var authorized bool
	err = tx.QueryRow(r.Context(), `
		SELECT set_grant.id,capture_set.artifact_count,
		       generation.node_id=$4 AND (
		         EXISTS(SELECT 1 FROM node_tokens token
		                WHERE token.id=$5 AND token.node_id=generation.node_id AND token.revoked_at IS NULL
		                  AND token.recording_claim_generation=plan.origin_claim_generation
		                  AND token.recording_claim_purpose='existing_fence_only')
		         OR EXISTS(SELECT 1 FROM recording_worker_claim_heads head
		                   JOIN node_tokens token ON token.id=head.claim_token_id
		                   WHERE head.node_id=generation.node_id AND head.claim_token_id=$5
		                     AND head.state='enabled' AND head.generation>=set_grant.recovery_block_generation
		                     AND token.revoked_at IS NULL AND token.recording_claim_purpose='claim_current'))
		FROM recording_capture_set_grants set_grant
		JOIN recording_capture_reservation_sets capture_set ON capture_set.id=set_grant.set_id
		JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
		JOIN recording_job_lease_generations generation
		  ON generation.recording_job_id=plan.recording_job_id AND generation.lease_token=plan.lease_token
		WHERE set_grant.set_id=$1 AND plan.recording_job_id=$2 AND plan.lease_token=$3
		  AND set_grant.upload_grace_until>transaction_timestamp()
		  AND NOT EXISTS(SELECT 1 FROM recording_capture_materialized_artifacts artifact WHERE artifact.set_id=capture_set.id)
		FOR UPDATE OF set_grant,capture_set,plan
	`, setID, jobID, leaseToken, principal.NodeID, principal.NodeTokenID).Scan(&grantID, &artifactCount, &authorized)
	if err != nil || !authorized {
		util.WriteError(w, http.StatusConflict, "empty capture set recovery authority is unavailable")
		return
	}
	tag, err := tx.Exec(r.Context(), `
		INSERT INTO recording_capture_empty_set_reports(set_id,grant_id,node_id,report_id,result)
		VALUES($1,$2,$3,$4,'no_bytes') ON CONFLICT(set_id) DO NOTHING
	`, setID, grantID, principal.NodeID, reportID)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "record empty capture set recovery")
		return
	}
	if tag.RowsAffected() == 0 {
		var exact bool
		if err = tx.QueryRow(r.Context(), `
			SELECT grant_id=$2 AND node_id=$3 AND report_id=$4 AND result='no_bytes'
			FROM recording_capture_empty_set_reports WHERE set_id=$1
		`, setID, grantID, principal.NodeID, reportID).Scan(&exact); err != nil || !exact {
			util.WriteError(w, http.StatusConflict, "empty capture set recovery replay differs")
			return
		}
	}
	coverage := struct {
		ArtifactCount int      `json:"artifact_count"`
		Materialized  []int    `json:"materialized_ordinals"`
		Unused        [][2]int `json:"unused_ranges"`
	}{artifactCount, []int{}, [][2]int{{1, artifactCount}}}
	coverageJSON, _ := json.Marshal(coverage)
	tag, err = tx.Exec(r.Context(), `
		INSERT INTO recording_capture_set_results(set_id,result,coverage_ranges,coverage_sha256)
		VALUES($1,'abandoned',$2,encode(sha256(convert_to($2::jsonb::text,'UTF8')),'hex'))
		ON CONFLICT(set_id) DO NOTHING
	`, setID, coverageJSON)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "seal empty capture set recovery")
		return
	}
	if tag.RowsAffected() == 0 {
		var exact bool
		if err = tx.QueryRow(r.Context(), `
			SELECT result='abandoned' AND coverage_ranges=$2::jsonb
			FROM recording_capture_set_results WHERE set_id=$1
		`, setID, coverageJSON).Scan(&exact); err != nil || !exact {
			util.WriteError(w, http.StatusConflict, "empty capture set result replay differs")
			return
		}
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit empty capture set recovery")
		return
	}
	w.WriteHeader(http.StatusNoContent)
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
	// Discover the capacity domain without locks, then reselect the exact job
	// after taking the global fence and capacity rows.  Relay claim uses
	// capacity->job order; surrender must never invert that as job->capacity.
	var discoveredCaptureVia string
	var discoveredAccountID int64
	if err = s.pool.QueryRow(r.Context(), `
		SELECT recording.capture_via,recording.account_id
		FROM recording_jobs job JOIN recordings recording ON recording.id=job.recording_id
		WHERE job.id=$1
	`, jobID).Scan(&discoveredCaptureVia, &discoveredAccountID); err != nil || (discoveredCaptureVia != "cloud" && discoveredCaptureVia != "relay") {
		util.WriteError(w, http.StatusConflict, "surrender capacity domain is unavailable")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin surrender decision")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock_shared(hashtextextended('recording-worker-claim-v1',0))`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock surrender claim authority")
		return
	}
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock(hashtextextended('recording-surrender-cloud-capacity-v1',0))`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock surrender capacity")
		return
	}
	if _, err = tx.Exec(r.Context(), `SELECT id FROM accounts WHERE id=$1 FOR SHARE`, discoveredAccountID); err != nil {
		util.WriteError(w, http.StatusConflict, "lock surrender account")
		return
	}
	if discoveredCaptureVia == "relay" {
		if err = lockRelayCapacityDomain(r.Context(), tx, discoveredAccountID); err != nil {
			util.WriteError(w, http.StatusConflict, "lock surrender relay capacity")
			return
		}
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
	var recordingAccountID int64
	var currentToken *uuid.UUID
	var windowOpen bool
	var attemptCount int
	var currentHead int64
	var currentIntent *uuid.UUID
	var currentClip *int64
	err = tx.QueryRow(r.Context(), `
			SELECT g.lease_owner,COALESCE(g.node_id,0),j.status,COALESCE(j.lease_owner,''),j.lease_token,transaction_timestamp()<j.window_end_at,j.attempt_count,recording.capture_via,recording.account_id,
			       h.version,h.upload_intent_id,h.clip_id
		FROM recording_job_lease_generations g
		JOIN recording_jobs j ON j.id=g.recording_job_id
		JOIN recordings recording ON recording.id=j.recording_id
		JOIN recording_job_unique_heads h ON h.recording_job_id=g.recording_job_id AND h.lease_token=g.lease_token
			WHERE g.recording_job_id=$1 AND g.lease_token=$2
			  AND j.lease_claim_generation IS NOT NULL AND j.lease_credential_state='exact'
			  AND recording_surrender_token_can_access_lease($3,$4,j.lease_node_token_id,j.lease_claim_generation)
			  AND j.kind='continuous_window' AND j.window_end_at IS NOT NULL AND recording.capture_via IN('cloud','relay')
			FOR UPDATE OF j,h
		`, jobID, leaseToken, principal.NodeTokenID, principal.NodeID).Scan(&generationOwner, &generationNode, &currentStatus, &currentOwner, &currentToken, &windowOpen, &attemptCount, &captureVia, &recordingAccountID, &currentHead, &currentIntent, &currentClip)
	if errors.Is(err, pgx.ErrNoRows) || generationOwner != workerID || generationNode != principal.NodeID {
		util.WriteError(w, http.StatusConflict, "unknown surrender lease generation")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load surrender generation")
		return
	}
	if captureVia != discoveredCaptureVia || recordingAccountID != discoveredAccountID {
		util.WriteError(w, http.StatusConflict, "surrender capacity domain changed")
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
			UNION ALL
			SELECT 1 FROM recording_capture_reservation_sets capture_set
			JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
			WHERE plan.recording_job_id=$1 AND plan.lease_token=$2
			  AND NOT EXISTS(SELECT 1 FROM recording_capture_set_results result WHERE result.set_id=capture_set.id)
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
		  AND EXISTS(SELECT 1 FROM recording_worker_claim_heads head
		             JOIN node_tokens token ON token.id=head.claim_token_id
		             WHERE head.node_id=d.node_id AND head.state='enabled' AND token.revoked_at IS NULL
		               AND token.recording_claim_generation=head.generation AND token.recording_claim_purpose='claim_current')
		  AND (SELECT count(*) FROM recording_jobs live WHERE live.status='leased' AND live.lease_owner=d.name AND live.lease_expires_at>transaction_timestamp()) < d.capacity
	)`, workerID).Scan(&alternate)
	} else {
		err = tx.QueryRow(r.Context(), `SELECT recording_surrender_relay_alternate($1,$2)`, jobID, workerID).Scan(&alternate)
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
		SET status='pending',scheduled_for=transaction_timestamp(),
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
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock_shared(hashtextextended('recording-worker-claim-v1',0))`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock surrender observation claim authority")
		return
	}
	if err = revalidateRecordingNodeCredential(r.Context(), tx, principal); err != nil {
		util.WriteError(w, http.StatusUnauthorized, "surrender observation credential is stale")
		return
	}
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
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock_shared(hashtextextended('recording-worker-claim-v1',0))`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock capture producer claim authority")
		return
	}
	if err = revalidateRecordingNodeCredential(r.Context(), tx, principal); err != nil {
		util.WriteError(w, http.StatusUnauthorized, "capture producer credential is stale")
		return
	}
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
		SELECT encode(sha256(convert_to(recording_surrender_source_snapshot(r.id)::text,'UTF8')),'hex'),
		       (SELECT max(sr.id) FROM stream_source_revisions sr WHERE sr.stream_id=st.id),
		       encode(sha256(convert_to(recording_surrender_capture_config_snapshot(r.id,j.id,j.lease_token)::text,'UTF8')),'hex')
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
	if !ok || principal.NodeTokenID <= 0 || (principal.NodeType != nodeTypeLocalRecorder && principal.NodeType != nodeTypeRelay) {
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
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin capture producer status")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock_shared(hashtextextended('recording-worker-claim-v1',0))`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock producer status claim authority")
		return
	}
	err = tx.QueryRow(r.Context(), `
		SELECT COALESCE(result.result,''),
		       job.status='leased' AND job.lease_token=producer.lease_token
		         AND job.lease_owner=producer.worker_id AND job.lease_expires_at>transaction_timestamp(),
		       count(artifact.upload_intent_id)
		FROM recording_capture_producers producer
		JOIN recording_jobs job ON job.id=producer.recording_job_id
		LEFT JOIN recording_capture_producer_results result ON result.producer_id=producer.id
		LEFT JOIN recording_capture_artifact_intents artifact ON artifact.producer_id=producer.id
		JOIN node_tokens token ON token.id=$5 AND token.node_id=producer.node_id AND token.revoked_at IS NULL
		 AND token.recording_claim_purpose IN('claim_current','existing_fence_only')
		WHERE producer.id=$1 AND producer.recording_job_id=$2 AND producer.node_id=$3 AND producer.worker_id=$4
		GROUP BY producer.id,result.result,job.status,job.lease_token,job.lease_owner,job.lease_expires_at
	`, producerID, jobID, principal.NodeID, recorderWorkerID(principal), principal.NodeTokenID).Scan(&result, &currentLease, &intentCount)
	if errors.Is(err, pgx.ErrNoRows) {
		if err = tx.Commit(r.Context()); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "commit missing producer status")
			return
		}
		util.WriteJSON(w, http.StatusOK, map[string]any{"producer_id": producerID, "found": false, "intent_count": 0})
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load capture producer status")
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit capture producer status")
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
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock_shared(hashtextextended('recording-worker-claim-v1',0))`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock artifact reservation claim authority")
		return
	}
	if err = revalidateRecordingNodeCredential(r.Context(), tx, principal); err != nil {
		util.WriteError(w, http.StatusUnauthorized, "artifact reservation credential is stale")
		return
	}
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
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock_shared(hashtextextended('recording-worker-claim-v1',0))`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock producer finish claim authority")
		return
	}
	if err = revalidateRecordingNodeCredential(r.Context(), tx, principal); err != nil {
		util.WriteError(w, http.StatusUnauthorized, "producer finish credential is stale")
		return
	}
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
