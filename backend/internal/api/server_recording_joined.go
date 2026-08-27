package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/joinedauth"
	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/util"
)

type joinedArtifactItem struct {
	ArtifactID               int64   `json:"artifact_id"`
	ConnectionID             int64   `json:"connection_id"`
	BatchID                  string  `json:"batch_id"`
	HourID                   *string `json:"hour_id"`
	Kind                     string  `json:"kind"`
	ContentType              string  `json:"content_type"`
	RelativePath             string  `json:"relative_path"`
	SizeBytes                int64   `json:"size_bytes"`
	SHA256                   string  `json:"sha256"`
	DownloadPath             string  `json:"download_path"`
	HourManifestID           *int64  `json:"hour_manifest_id"`
	HourManifestRelativePath *string `json:"hour_manifest_relative_path"`
	HourManifestSHA256       *string `json:"hour_manifest_sha256"`
	LedgerArtifactID         *int64  `json:"ledger_artifact_id"`
	LedgerRelativePath       *string `json:"ledger_relative_path"`
	LedgerSHA256             *string `json:"ledger_sha256"`
}

// joinedFeedHeadFromWhere is shared by the NAS feed and the read-only
// connection diagnostic. Keep the eligibility predicate and ordering in one
// place so an operator sees the same head item the NAS sees.
const joinedFeedHeadFromWhere = `
		FROM recording_joined_artifacts a
		LEFT JOIN recording_joined_hours h ON h.id=a.hour_record_id
		LEFT JOIN recording_joined_artifacts manifest ON a.artifact_kind='media'
		  AND manifest.hour_record_id=a.hour_record_id AND manifest.artifact_kind='hour_manifest'
		LEFT JOIN recording_joined_artifacts ledger ON a.artifact_kind='hour_manifest'
		  AND ledger.stream_day_id=a.stream_day_id AND ledger.artifact_kind='allocation_ledger'
		LEFT JOIN recording_joined_artifact_acks own_ack ON own_ack.artifact_id=a.id AND own_ack.connection_id=a.connection_id
		JOIN connections c ON c.id=a.connection_id AND c.joined_protocol_version=1
		WHERE a.connection_id=$1 AND a.batch_id=$2 AND own_ack.artifact_id IS NULL
		  AND (a.artifact_kind<>'hour_manifest' OR h.source_clip_count>0 OR EXISTS(
		    SELECT 1 FROM recording_joined_gap_only_scope_authorizations ga WHERE ga.artifact_id=a.id
		      AND ga.batch_record_id=a.batch_record_id AND ga.batch_id=a.batch_id AND ga.hour_record_id=a.hour_record_id
		      AND ga.hour_id=a.scope_id AND ga.work_scope='frozen_batch' AND ga.authorization_source IN ('server_seal','operator_frozen')
		      AND ga.work_scope_identity_sha256=$3))
		  AND ((a.artifact_kind<>'media' AND a.publication_state='published')
		    OR (a.artifact_kind='media' AND a.published_at IS NOT NULL))
		  AND (a.artifact_kind='allocation_ledger'
		    OR (a.artifact_kind='hour_manifest' AND EXISTS(SELECT 1 FROM recording_joined_artifact_acks ack
		      WHERE ack.artifact_id=ledger.id AND ack.connection_id=a.connection_id))
		    OR (a.artifact_kind='media' AND EXISTS(SELECT 1 FROM recording_joined_artifact_acks ack
		      WHERE ack.artifact_id=manifest.id AND ack.connection_id=a.connection_id))
		    OR (a.artifact_kind='batch_index' AND NOT EXISTS(SELECT 1 FROM recording_joined_artifacts prior
		      LEFT JOIN recording_joined_artifact_acks ack ON ack.artifact_id=prior.id AND ack.connection_id=prior.connection_id
		      WHERE prior.batch_record_id=a.batch_record_id AND prior.artifact_kind<>'batch_index' AND ack.artifact_id IS NULL)))
		ORDER BY a.batch_record_id,h.priority_ordinal NULLS LAST,
		  CASE a.artifact_kind WHEN 'allocation_ledger' THEN 0 WHEN 'hour_manifest' THEN 1 WHEN 'media' THEN 2 ELSE 3 END,
		  a.ordinal,a.id
		LIMIT 1`

type joinedOutputObjectStore interface {
	Head(context.Context, string) (r2.ObjectHead, error)
	OpenExact(context.Context, string, string, string) (io.ReadCloser, error)
	PresignPutCreateOnlyRequest(context.Context, string, string, int64, string, time.Duration) (r2.PresignedRequest, error)
	PresignGetExactRequest(context.Context, string, string, string, time.Duration) (r2.PresignedRequest, error)
}

func (s *Server) joinedOutputStore() joinedOutputObjectStore {
	if s.joinedOutputStorage != nil {
		return s.joinedOutputStorage
	}
	return s.r2
}

func joinedFrozenScopeSHA(batchID string) (string, error) {
	scope, err := joinedrecording.NewWorkScopeIdentity(batchID, joinedrecording.WorkScopeFrozenBatch, nil)
	if err != nil {
		return "", err
	}
	return scope.SHA256(batchID)
}

func pullConnectionID(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, principal accountPrincipal, forUpdate bool) (int64, error) {
	if principal.APIKeyID == nil {
		return 0, pgx.ErrNoRows
	}
	suffix := ""
	if forUpdate {
		suffix = " FOR UPDATE"
	}
	var id int64
	err := q.QueryRow(ctx, `SELECT id FROM connections WHERE account_id=$1 AND api_key_id=$2 AND kind='nas_pull'`+suffix,
		principal.AccountID, *principal.APIKeyID).Scan(&id)
	return id, err
}

var errJoinedProtocolDisabled = errors.New("joined protocol is not currently authorized")

func (s *Server) pullJoinedConnectionID(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, principal accountPrincipal, forUpdate bool) (int64, error) {
	connectionID, err := pullConnectionID(ctx, q, principal, forUpdate)
	if err != nil {
		return 0, err
	}
	protocol, generation := desiredJoinedProtocol(s.cfg, connectionID)
	if protocol != 1 || generation != s.cfg.JoinedRecordingProtocolGeneration {
		return 0, errJoinedProtocolDisabled
	}
	return connectionID, nil
}

// handleAccountJoined returns exactly one sealed, highest-priority unacked
// artifact. There is deliberately no numeric cursor: a blocked head item repeats
// until its exact NAS acknowledgment arrives.
func (s *Server) handleAccountJoined(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin joined feed: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	frozenScopeSHA, err := joinedFrozenScopeSHA(s.cfg.JoinedRecordingBatchID)
	if err != nil {
		util.WriteError(w, http.StatusServiceUnavailable, "joined delivery scope is unavailable")
		return
	}
	connectionID, err := s.pullJoinedConnectionID(r.Context(), tx, principal, true)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusForbidden, "joined delivery requires a NAS pull key")
		return
	}
	if errors.Is(err, errJoinedProtocolDisabled) {
		util.WriteJSON(w, http.StatusOK, map[string]any{"item": nil})
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load NAS connection: %v", err))
		return
	}

	var item joinedArtifactItem
	err = tx.QueryRow(r.Context(), `
		SELECT a.id,a.connection_id,a.batch_id,h.hour_id,a.artifact_kind,a.content_type,a.relative_path,a.expected_size_bytes,a.expected_sha256,
		       manifest.id,manifest.relative_path,manifest.expected_sha256,
		       ledger.id,ledger.relative_path,ledger.expected_sha256
		`+joinedFeedHeadFromWhere+` FOR SHARE OF a`, connectionID, s.cfg.JoinedRecordingBatchID, frozenScopeSHA).Scan(
		&item.ArtifactID, &item.ConnectionID, &item.BatchID, &item.HourID, &item.Kind, &item.ContentType, &item.RelativePath,
		&item.SizeBytes, &item.SHA256, &item.HourManifestID, &item.HourManifestRelativePath, &item.HourManifestSHA256,
		&item.LedgerArtifactID, &item.LedgerRelativePath, &item.LedgerSHA256)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteJSON(w, http.StatusOK, map[string]any{"item": nil})
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list joined output: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusConflict, "joined feed eligibility changed")
		return
	}
	item.DownloadPath = fmt.Sprintf("/api/v1/account/joined/%d/download", item.ArtifactID)
	util.WriteJSON(w, http.StatusOK, map[string]any{"item": item})
}

func (s *Server) handleAccountJoinedDownload(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	outputID, ok := parseInt64Path(w, r, "joinedId")
	if !ok {
		return
	}
	frozenScopeSHA, err := joinedFrozenScopeSHA(s.cfg.JoinedRecordingBatchID)
	if err != nil {
		util.WriteError(w, http.StatusServiceUnavailable, "joined delivery scope is unavailable")
		return
	}
	connectionID, err := s.pullJoinedConnectionID(r.Context(), s.pool, principal, false)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusForbidden, "joined delivery requires a NAS pull key")
		return
	}
	if errors.Is(err, errJoinedProtocolDisabled) {
		util.WriteError(w, http.StatusNotFound, "joined output not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load NAS connection: %v", err))
		return
	}
	var objectKey, etag, versionID, sha256, contentType string
	var sizeBytes int64
	err = s.pool.QueryRow(r.Context(), `
		SELECT a.object_key,a.etag,a.version_id,a.expected_size_bytes,a.expected_sha256,a.content_type
		FROM recording_joined_artifacts a
		LEFT JOIN recording_joined_hours h ON h.id=a.hour_record_id
		LEFT JOIN recording_joined_artifacts manifest ON a.artifact_kind='media'
		  AND manifest.hour_record_id=a.hour_record_id AND manifest.artifact_kind='hour_manifest'
		LEFT JOIN recording_joined_artifacts ledger ON a.artifact_kind='hour_manifest'
		  AND ledger.stream_day_id=a.stream_day_id AND ledger.artifact_kind='allocation_ledger'
		JOIN connections c ON c.id=a.connection_id AND c.joined_protocol_version=1
		WHERE a.id=$1 AND a.connection_id=$2 AND a.account_id=$3 AND a.batch_id=$4
		  AND (a.artifact_kind<>'hour_manifest' OR h.source_clip_count>0 OR EXISTS(SELECT 1
		    FROM recording_joined_gap_only_scope_authorizations ga WHERE ga.artifact_id=a.id
		      AND ga.batch_record_id=a.batch_record_id AND ga.batch_id=a.batch_id AND ga.hour_record_id=a.hour_record_id
		      AND ga.hour_id=a.scope_id AND ga.work_scope='frozen_batch' AND ga.authorization_source IN ('server_seal','operator_frozen')
		      AND ga.work_scope_identity_sha256=$5))
		  AND ((a.artifact_kind<>'media' AND a.publication_state='published') OR (a.artifact_kind='media' AND a.published_at IS NOT NULL))
		  AND (a.artifact_kind='allocation_ledger'
		    OR (a.artifact_kind='hour_manifest' AND EXISTS(SELECT 1 FROM recording_joined_artifact_acks ack
		      WHERE ack.artifact_id=ledger.id AND ack.connection_id=a.connection_id))
		    OR (a.artifact_kind='media' AND EXISTS(SELECT 1 FROM recording_joined_artifact_acks ack
		      WHERE ack.artifact_id=manifest.id AND ack.connection_id=a.connection_id))
		    OR (a.artifact_kind='batch_index' AND NOT EXISTS(SELECT 1 FROM recording_joined_artifacts prior
		      LEFT JOIN recording_joined_artifact_acks ack ON ack.artifact_id=prior.id AND ack.connection_id=prior.connection_id
		      WHERE prior.batch_record_id=a.batch_record_id AND prior.artifact_kind<>'batch_index' AND ack.artifact_id IS NULL)))`,
		outputID, connectionID, principal.AccountID, s.cfg.JoinedRecordingBatchID, frozenScopeSHA).Scan(
		&objectKey, &etag, &versionID, &sizeBytes, &sha256, &contentType)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "joined output not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load joined output: %v", err))
		return
	}
	if s.r2 == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "joined object storage is unavailable")
		return
	}
	head, err := s.r2.Head(r.Context(), objectKey)
	if err != nil || head.SizeBytes != sizeBytes || head.ETag != etag || head.VersionID != versionID {
		util.WriteError(w, http.StatusConflict, "joined object identity no longer matches its sealed output")
		return
	}
	ttl := s.cfg.R2SignGetTTL
	if ttl <= 0 || ttl > time.Hour {
		ttl = time.Hour
	}
	url, err := s.r2.PresignGetExact(r.Context(), objectKey, etag, versionID, ttl)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("presign joined output: %v", err))
		return
	}
	if err := s.revalidateAccountJoinedDownload(r.Context(), principal, outputID, objectKey, etag, versionID, sizeBytes, sha256, frozenScopeSHA); err != nil {
		util.WriteError(w, http.StatusConflict, "joined download eligibility changed")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"url": url, "etag": etag, "if_match": `"` + etag + `"`, "version_id": versionID,
		"size_bytes": sizeBytes, "sha256": sha256, "content_type": contentType,
		"expires_in_sec": int(ttl.Seconds()),
	})
}

func (s *Server) revalidateAccountJoinedDownload(ctx context.Context, principal accountPrincipal, outputID int64,
	objectKey, etag, versionID string, sizeBytes int64, sha256, frozenScopeSHA string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	connectionID, err := s.pullJoinedConnectionID(ctx, tx, principal, true)
	if err != nil {
		return err
	}
	var ok int
	err = tx.QueryRow(ctx, `
		SELECT 1 FROM recording_joined_artifacts a
		LEFT JOIN recording_joined_hours h ON h.id=a.hour_record_id
		LEFT JOIN recording_joined_artifacts manifest ON a.artifact_kind='media'
		  AND manifest.hour_record_id=a.hour_record_id AND manifest.artifact_kind='hour_manifest'
		LEFT JOIN recording_joined_artifacts ledger ON a.artifact_kind='hour_manifest'
		  AND ledger.stream_day_id=a.stream_day_id AND ledger.artifact_kind='allocation_ledger'
		JOIN connections c ON c.id=a.connection_id AND c.joined_protocol_version=1
		WHERE a.id=$1 AND a.connection_id=$2 AND a.account_id=$3 AND a.batch_id=$9
		  AND (a.artifact_kind<>'hour_manifest' OR h.source_clip_count>0 OR EXISTS(SELECT 1
		    FROM recording_joined_gap_only_scope_authorizations ga WHERE ga.artifact_id=a.id
		      AND ga.batch_record_id=a.batch_record_id AND ga.batch_id=a.batch_id AND ga.hour_record_id=a.hour_record_id
		      AND ga.hour_id=a.scope_id AND ga.work_scope='frozen_batch' AND ga.authorization_source IN ('server_seal','operator_frozen')
		      AND ga.work_scope_identity_sha256=$10))
		  AND (a.object_key,a.etag,a.version_id,a.expected_size_bytes,a.expected_sha256)=($4,$5,$6,$7,$8)
		  AND ((a.artifact_kind<>'media' AND a.publication_state='published') OR (a.artifact_kind='media' AND a.published_at IS NOT NULL))
		  AND (a.artifact_kind='allocation_ledger'
		    OR (a.artifact_kind='hour_manifest' AND EXISTS(SELECT 1 FROM recording_joined_artifact_acks ack
		      WHERE ack.artifact_id=ledger.id AND ack.connection_id=a.connection_id))
		    OR (a.artifact_kind='media' AND EXISTS(SELECT 1 FROM recording_joined_artifact_acks ack
		      WHERE ack.artifact_id=manifest.id AND ack.connection_id=a.connection_id))
		    OR (a.artifact_kind='batch_index' AND NOT EXISTS(SELECT 1 FROM recording_joined_artifacts prior
		      LEFT JOIN recording_joined_artifact_acks ack ON ack.artifact_id=prior.id AND ack.connection_id=prior.connection_id
		      WHERE prior.batch_record_id=a.batch_record_id AND prior.artifact_kind<>'batch_index' AND ack.artifact_id IS NULL)))
		FOR SHARE OF a`, outputID, connectionID, principal.AccountID, objectKey, etag, versionID, sizeBytes, sha256,
		s.cfg.JoinedRecordingBatchID, frozenScopeSHA).Scan(&ok)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type joinedAckRequest struct {
	ArtifactID   int64  `json:"artifact_id"`
	RelativePath string `json:"relative_path"`
	SizeBytes    int64  `json:"size_bytes"`
	SHA256       string `json:"sha256"`
}

func validateJoinedAck(req joinedAckRequest) error {
	if req.ArtifactID <= 0 || req.SizeBytes <= 0 || req.RelativePath != strings.TrimSpace(req.RelativePath) ||
		!validNASRelativePath(req.RelativePath) || len(req.SHA256) != 64 || !lowerHex(req.SHA256) {
		return errors.New("invalid joined acknowledgment identity")
	}
	return nil
}

func (s *Server) handleAccountJoinedAck(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var req joinedAckRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateJoinedAck(req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	frozenScopeSHA, err := joinedFrozenScopeSHA(s.cfg.JoinedRecordingBatchID)
	if err != nil {
		util.WriteError(w, http.StatusServiceUnavailable, "joined delivery scope is unavailable")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin joined acknowledgment: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	connectionID, err := s.pullJoinedConnectionID(r.Context(), tx, principal, true)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusForbidden, "joined delivery requires a NAS pull key")
		return
	}
	if errors.Is(err, errJoinedProtocolDisabled) {
		util.WriteError(w, http.StatusNotFound, "joined output not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load NAS connection: %v", err))
		return
	}
	var relativePath, sha256 string
	var sizeBytes int64
	var verifiedAt *time.Time
	err = tx.QueryRow(r.Context(), `
		SELECT a.relative_path,a.expected_size_bytes,a.expected_sha256,ack.verified_at
		FROM recording_joined_artifacts a
		LEFT JOIN recording_joined_hours h ON h.id=a.hour_record_id
		LEFT JOIN recording_joined_artifacts manifest ON a.artifact_kind='media'
		  AND manifest.hour_record_id=a.hour_record_id AND manifest.artifact_kind='hour_manifest'
		LEFT JOIN recording_joined_artifacts ledger ON a.artifact_kind='hour_manifest'
		  AND ledger.stream_day_id=a.stream_day_id AND ledger.artifact_kind='allocation_ledger'
		LEFT JOIN recording_joined_artifact_acks ack ON ack.artifact_id=a.id AND ack.connection_id=a.connection_id
		JOIN connections c ON c.id=a.connection_id AND c.joined_protocol_version=1
		WHERE a.id=$1 AND a.connection_id=$2 AND a.account_id=$3 AND a.batch_id=$4
		  AND (a.artifact_kind<>'hour_manifest' OR h.source_clip_count>0 OR EXISTS(SELECT 1
		    FROM recording_joined_gap_only_scope_authorizations ga WHERE ga.artifact_id=a.id
		      AND ga.batch_record_id=a.batch_record_id AND ga.batch_id=a.batch_id AND ga.hour_record_id=a.hour_record_id
		      AND ga.hour_id=a.scope_id AND ga.work_scope='frozen_batch' AND ga.authorization_source IN ('server_seal','operator_frozen')
		      AND ga.work_scope_identity_sha256=$5))
		  AND ((a.artifact_kind<>'media' AND a.publication_state='published') OR (a.artifact_kind='media' AND a.published_at IS NOT NULL))
		  AND (a.artifact_kind='allocation_ledger'
		    OR (a.artifact_kind='hour_manifest' AND EXISTS(SELECT 1 FROM recording_joined_artifact_acks dep
		      WHERE dep.artifact_id=ledger.id AND dep.connection_id=a.connection_id))
		    OR (a.artifact_kind='media' AND EXISTS(SELECT 1 FROM recording_joined_artifact_acks dep
		      WHERE dep.artifact_id=manifest.id AND dep.connection_id=a.connection_id))
		    OR (a.artifact_kind='batch_index' AND NOT EXISTS(SELECT 1 FROM recording_joined_artifacts prior
		      LEFT JOIN recording_joined_artifact_acks dep ON dep.artifact_id=prior.id AND dep.connection_id=prior.connection_id
		      WHERE prior.batch_record_id=a.batch_record_id AND prior.artifact_kind<>'batch_index' AND dep.artifact_id IS NULL)))
		FOR SHARE OF a`, req.ArtifactID, connectionID, principal.AccountID, s.cfg.JoinedRecordingBatchID, frozenScopeSHA).Scan(
		&relativePath, &sizeBytes, &sha256, &verifiedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "joined output not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load joined acknowledgment target: %v", err))
		return
	}
	if relativePath != req.RelativePath || sizeBytes != req.SizeBytes || sha256 != req.SHA256 {
		util.WriteError(w, http.StatusConflict, "joined acknowledgment identity differs")
		return
	}
	if verifiedAt == nil {
		if _, err := tx.Exec(r.Context(), `INSERT INTO recording_joined_artifact_acks
		  (artifact_id,connection_id,relative_path,size_bytes,sha256,verified_at) VALUES($1,$2,$3,$4,$5,now())`,
			req.ArtifactID, connectionID, relativePath, sizeBytes, sha256); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("acknowledge joined output: %v", err))
			return
		}
		if _, err := tx.Exec(r.Context(), `
			UPDATE connections SET joined_files_pulled=joined_files_pulled+1,
			  joined_bytes_pulled=joined_bytes_pulled+$2,
			  joined_last_attempt_artifact_id=NULL,joined_last_blocker='',joined_last_attempt_at=NULL,joined_retry_at=NULL
			WHERE id=$1`, connectionID, sizeBytes); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("advance joined totals: %v", err))
			return
		}
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit joined acknowledgment: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true, "already_verified": verifiedAt != nil})
}

type joinedSourceCapabilityRequest = joinedrecording.SourceCapabilityRequest
type joinedArtifactCapabilityRequest = joinedrecording.ArtifactCapabilityRequest
type joinedSignedRequest = joinedrecording.SignedRequest

func (s *Server) revalidateJoinedSourceCapability(ctx context.Context, hourID, batchID string, clipID int64, lease uuid.UUID, operation string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var ok int
	err = tx.QueryRow(ctx, `
		SELECT c.id FROM connections c JOIN recording_joined_hours h ON h.connection_id=c.id
		WHERE h.hour_id=$1 AND h.batch_id=$2 AND c.id=$3
		FOR SHARE OF c`, hourID, batchID, s.cfg.JoinedRecordingConnectionID).Scan(&ok)
	if err != nil {
		return err
	}
	switch operation {
	case joinedauth.OperationPreflight:
		err = tx.QueryRow(ctx, `
			SELECT 1 FROM recording_joined_hours h
			JOIN recording_joined_sources src ON src.hour_record_id=h.id AND src.clip_id=$3
			WHERE h.hour_id=$1 AND h.batch_id=$2 AND h.state='leased'
			  AND h.claim_token=$4 AND h.lease_expires_at>now()
			FOR SHARE OF h,src`, hourID, batchID, clipID, lease).Scan(&ok)
	case joinedauth.OperationPublish:
		err = tx.QueryRow(ctx, `
			SELECT 1 FROM recording_joined_hours h
			JOIN recording_joined_sources src ON src.hour_record_id=h.id AND src.clip_id=$3
			JOIN recording_joined_artifacts root ON root.hour_record_id=h.id AND root.artifact_kind='hour_manifest'
			WHERE h.hour_id=$1 AND h.batch_id=$2 AND h.state='sealed'
			  AND root.publication_state='publishing' AND root.publication_token=$4
			  AND root.publication_lease_expires_at>now()
			FOR SHARE OF h,src,root`, hourID, batchID, clipID, lease).Scan(&ok)
	default:
		return errors.New("joined source capability operation differs")
	}
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Server) revalidateJoinedArtifactCapability(ctx context.Context, scopeKind, scopeID, batchID string, artifactID int64, lease uuid.UUID) error {
	workScope, err := s.joinedWorkScopeIdentity()
	if err != nil {
		return err
	}
	workScopeSHA, err := workScope.SHA256(batchID)
	if err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var ok int
	err = tx.QueryRow(ctx, `SELECT c.id FROM connections c JOIN recording_joined_batches b ON b.connection_id=c.id
		WHERE b.batch_id=$1 AND c.id=$2 FOR SHARE OF c`, batchID, s.cfg.JoinedRecordingConnectionID).Scan(&ok)
	if err != nil {
		return err
	}
	err = tx.QueryRow(ctx, `
		SELECT 1 FROM recording_joined_artifacts target
		JOIN recording_joined_artifacts root ON root.batch_record_id=target.batch_record_id
		  AND root.scope_kind=target.scope_kind AND root.scope_id=target.scope_id AND root.artifact_kind<>'media'
		LEFT JOIN recording_joined_hours h ON h.id=root.hour_record_id
		WHERE target.id=$1 AND target.scope_kind=$2 AND target.scope_id=$3 AND target.batch_id=$4
		  AND root.publication_state='publishing' AND root.publication_token=$5 AND root.publication_lease_expires_at>now()
		  AND (root.artifact_kind<>'batch_index' OR ($7='frozen_batch' AND NOT EXISTS(SELECT 1
		    FROM recording_joined_batch_index_refs ref
		    JOIN recording_joined_artifacts referenced ON referenced.id=ref.referenced_artifact_id
		    JOIN recording_joined_hours gap_hour ON gap_hour.id=referenced.hour_record_id
		    WHERE ref.index_artifact_id=root.id AND ref.reference_kind='hour_manifest' AND gap_hour.source_clip_count=0
		      AND NOT EXISTS(SELECT 1 FROM recording_joined_gap_only_scope_authorizations ga
		        WHERE ga.artifact_id=referenced.id AND ga.batch_record_id=referenced.batch_record_id
		          AND ga.batch_id=referenced.batch_id AND ga.hour_record_id=referenced.hour_record_id
		          AND ga.hour_id=referenced.scope_id AND ga.work_scope='frozen_batch'
		          AND ga.work_scope_identity_sha256=$6
		          AND ga.authorization_source IN ('server_seal','operator_frozen')))))
		  AND (root.artifact_kind<>'hour_manifest' OR h.source_clip_count>0 OR EXISTS(SELECT 1
		    FROM recording_joined_gap_only_scope_authorizations ga WHERE ga.artifact_id=root.id
		      AND ga.batch_record_id=root.batch_record_id AND ga.batch_id=root.batch_id
		      AND ga.hour_record_id=root.hour_record_id AND ga.hour_id=root.scope_id
		      AND ga.work_scope=$7 AND ga.work_scope_identity_sha256=$6
		      AND (($7='frozen_batch' AND ga.authorization_source IN ('server_seal','operator_frozen'))
		        OR ($7 IN ('canary','canary_single','allowlist_50') AND ga.authorization_source='server_seal'))))
		FOR SHARE OF target,root`, artifactID, scopeKind, scopeID, batchID, lease, workScopeSHA, workScope.WorkScope).Scan(&ok)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func joinedCapabilityToken(raw string) (uuid.UUID, error) {
	token, err := uuid.Parse(strings.TrimSpace(raw))
	if err != nil || token == uuid.Nil {
		return uuid.Nil, errors.New("claim_token must be a UUID")
	}
	return token, nil
}

func joinedCapabilityTTL(ttl time.Duration, leaseExpires, databaseNow time.Time, createOnly bool) (time.Duration, error) {
	remaining := leaseExpires.Sub(databaseNow)
	if remaining < time.Second {
		return 0, errors.New("joined publication lease is expired or too close to expiry")
	}
	if ttl <= 0 || ttl > time.Hour {
		return 0, errors.New("joined capability lifetime is invalid")
	}
	if !createOnly && ttl > remaining {
		ttl = remaining
	}
	return ttl, nil
}

func joinedSignedRequestFrom(capability r2.PresignedRequest, expectedAuthority string) (joinedSignedRequest, error) {
	parsed, err := url.Parse(capability.URL)
	if err != nil || parsed.Scheme != "https" || parsed.User != nil || parsed.Host == "" || parsed.Host != expectedAuthority {
		return joinedSignedRequest{}, errors.New("presigned storage authority differs from its frozen destination")
	}
	headers := make(map[string]string, len(capability.Headers))
	for key, values := range capability.Headers {
		if len(values) != 1 {
			return joinedSignedRequest{}, errors.New("presigned storage capability has an ambiguous required header")
		}
		if strings.EqualFold(key, "Host") {
			if values[0] != expectedAuthority {
				return joinedSignedRequest{}, errors.New("presigned storage host differs from its frozen destination")
			}
			continue
		}
		headers[key] = values[0]
	}
	return joinedSignedRequest{Method: capability.Method, URL: capability.URL, Scheme: parsed.Scheme, Authority: parsed.Host,
		EscapedPath: parsed.EscapedPath(), RawQuery: parsed.RawQuery, RequiredHeaders: headers}, nil
}

func joinedSignedRequestExpiry(request joinedSignedRequest, upperBound time.Time) (time.Time, error) {
	query, err := url.ParseQuery(request.RawQuery)
	if err != nil || len(query["X-Amz-Date"]) != 1 || len(query["X-Amz-Expires"]) != 1 {
		return time.Time{}, errors.New("presigned storage capability expiry is missing or ambiguous")
	}
	signedAt, err := time.Parse("20060102T150405Z", query.Get("X-Amz-Date"))
	if err != nil {
		return time.Time{}, errors.New("presigned storage capability date is invalid")
	}
	seconds, err := strconv.Atoi(query.Get("X-Amz-Expires"))
	if err != nil || seconds < 1 || seconds > 3600 {
		return time.Time{}, errors.New("presigned storage capability lifetime is invalid")
	}
	signedExpiry := signedAt.Add(time.Duration(seconds) * time.Second)
	if signedExpiry.After(upperBound) {
		return time.Time{}, errors.New("presigned storage capability outlives its authority bound")
	}
	return signedExpiry, nil
}

// handleJoinedSourceCapability mints reads only for a source already frozen
// into the exact fenced hour. A renewed sealed publication lease may request
// the same source solely to rebuild the same sealed bytes after scratch loss.
func (s *Server) handleJoinedSourceCapability(w http.ResponseWriter, r *http.Request) {
	if !s.joinedControlPlaneReady() {
		util.WriteError(w, http.StatusServiceUnavailable, "joined recording is disabled")
		return
	}
	var req joinedSourceCapabilityRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := req.Validate(); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	claims, ok := joinedWorkerClaimsFromContext(r.Context())
	if !ok || claims.Kind != joinedauth.KindOperation || claims.SubjectKind != joinedauth.SubjectHour || claims.SubjectID != req.HourID {
		util.WriteError(w, http.StatusForbidden, "joined worker token scope differs")
		return
	}
	token, err := joinedCapabilityToken(claims.LeaseToken)
	if err != nil {
		util.WriteError(w, http.StatusForbidden, "joined worker token lease differs")
		return
	}
	var d clipDestination
	var provider, etag, versionID, sha256, leaseState string
	var leaseExpires, databaseNow time.Time
	err = s.pool.QueryRow(r.Context(), `
		SELECT src.object_key,src.size_bytes,src.region,src.bucket,src.endpoint,sd.access_key_id,sd.secret_access_key_enc,
		       src.provider,src.etag,src.version_id,src.sha256,
		       CASE WHEN h.state='leased' THEN h.lease_expires_at ELSE root.publication_lease_expires_at END,
		       CASE WHEN h.state='leased' THEN 'leased' ELSE root.publication_state END,now()
		FROM recording_joined_sources src
		JOIN recording_joined_hours h ON h.id=src.hour_record_id
		JOIN recording_joined_batches b ON b.id=h.batch_record_id AND b.batch_id=h.batch_id
		  AND b.source_endpoint=src.endpoint
		LEFT JOIN recording_joined_artifacts root ON root.hour_record_id=h.id AND root.artifact_kind='hour_manifest'
		JOIN connections c ON c.id=h.connection_id AND c.id=$5
		JOIN storage_destinations sd ON sd.id=src.storage_destination_id AND sd.provider=src.provider
		  AND sd.endpoint=src.endpoint AND sd.region=src.region AND sd.bucket=src.bucket
		WHERE h.hour_id=$1 AND src.clip_id=$2 AND h.batch_id=$4
		  AND ((h.state='leased' AND h.claim_token=$3 AND h.lease_expires_at>now())
		    OR (h.state='sealed' AND root.publication_state='publishing' AND root.publication_token=$3
		      AND root.publication_lease_expires_at>now()))
		  AND h.failure_reason_code=''`, req.HourID, req.ClipID, token, claims.BatchID, s.cfg.JoinedRecordingConnectionID).Scan(
		&d.objectKey, &d.sizeBytes, &d.region, &d.bucket, &d.endpoint, &d.accessKeyID, &d.secretEnc,
		&provider, &etag, &versionID, &sha256, &leaseExpires, &leaseState, &databaseNow)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusConflict, "joined source capability lease is stale or foreign")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load frozen joined source: %v", err))
		return
	}
	if (leaseState == "leased" && claims.Operation != joinedauth.OperationPreflight) ||
		(leaseState == "publishing" && claims.Operation != joinedauth.OperationPublish) {
		util.WriteError(w, http.StatusForbidden, "joined worker token operation differs")
		return
	}
	if s.secrets == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "storage credential key is unset")
		return
	}
	secret, err := s.secrets.Decrypt(d.secretEnc)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "decrypt joined source storage credential")
		return
	}
	bootstrap, signing := strings.TrimSpace(s.cfg.JoinedWorkerBootstrapToken), strings.TrimSpace(s.cfg.JoinedWorkerSigningKey)
	if d.accessKeyID == bootstrap || d.accessKeyID == signing || string(secret) == bootstrap || string(secret) == signing {
		util.WriteError(w, http.StatusServiceUnavailable, "joined worker and source storage credentials must be distinct")
		return
	}
	sourceAuthority, authorityErr := joinedrecording.CanonicalSourceEndpointAuthority(d.endpoint)
	if provider == "" || authorityErr != nil {
		util.WriteError(w, http.StatusBadGateway, "frozen joined source storage authority is invalid")
		return
	}
	client, err := s.buildClipClient(r, d)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("build joined source storage client: %v", err))
		return
	}
	ttl, err := joinedCapabilityTTL(15*time.Minute, leaseExpires, databaseNow, false)
	if err != nil {
		util.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	expiresAt := time.Now().UTC().Add(ttl)
	if expiresAt.After(leaseExpires) {
		expiresAt = leaseExpires
	}
	var capability r2.PresignedRequest
	if req.Operation == "head" {
		capability, err = client.PresignHeadExactRequest(r.Context(), d.objectKey, etag, versionID, ttl)
	} else {
		capability, err = client.PresignGetExactRequest(r.Context(), d.objectKey, etag, versionID, ttl)
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("presign joined source %s: %v", req.Operation, err))
		return
	}
	requestDTO, err := joinedSignedRequestFrom(capability, sourceAuthority)
	if err != nil {
		util.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	expiresAt, err = joinedSignedRequestExpiry(requestDTO, expiresAt)
	if err != nil {
		util.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	if err := s.revalidateJoinedSourceCapability(r.Context(), req.HourID, claims.BatchID, req.ClipID, token, claims.Operation); err != nil {
		util.WriteError(w, http.StatusConflict, "joined source capability lease changed")
		return
	}
	response := joinedrecording.SourceReadCapability{ProtocolVersion: joinedWorkerProtocolVersion, Operation: req.Operation,
		ObjectKey: d.objectKey, SizeBytes: d.sizeBytes, SHA256: sha256, ETag: etag, VersionID: versionID,
		ExpiresAt: expiresAt, Request: requestDTO}
	if err := response.Validate(joinedrecording.SourceClip{Provider: provider, Endpoint: d.endpoint, Region: d.region,
		Bucket: d.bucket, Object: joinedrecording.ObjectIdentity{Key: d.objectKey, SizeBytes: d.sizeBytes, SHA256: sha256,
			ETag: etag, VersionID: versionID}}, req.Operation, sourceAuthority, databaseNow); err != nil {
		util.WriteError(w, http.StatusBadGateway, "joined source capability response differs")
		return
	}
	util.WriteJSON(w, http.StatusOK, response)
}

// handleJoinedArtifactCapability derives every object coordinate from one
// immutable sealed artifact under the current renewable publication lease.
func (s *Server) handleJoinedArtifactCapability(w http.ResponseWriter, r *http.Request) {
	if !s.joinedControlPlaneReady() {
		util.WriteError(w, http.StatusServiceUnavailable, "joined recording is disabled")
		return
	}
	var req joinedArtifactCapabilityRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := req.Validate(); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	operation := req.Operation
	claims, ok := joinedWorkerClaimsFromContext(r.Context())
	if !ok || claims.Kind != joinedauth.KindOperation || claims.SubjectKind != req.ScopeKind || claims.SubjectID != req.ScopeID ||
		claims.Operation != joinedauth.OperationPublish {
		util.WriteError(w, http.StatusForbidden, "joined worker token scope differs")
		return
	}
	token, err := joinedCapabilityToken(claims.LeaseToken)
	if err != nil {
		util.WriteError(w, http.StatusForbidden, "joined worker token lease differs")
		return
	}
	workScope, err := s.joinedWorkScopeIdentity()
	if err != nil {
		util.WriteError(w, http.StatusServiceUnavailable, "joined work scope is unavailable")
		return
	}
	workScopeSHA, err := workScope.SHA256(claims.BatchID)
	if err != nil {
		util.WriteError(w, http.StatusServiceUnavailable, "joined work scope is unavailable")
		return
	}
	var objectKey, contentType, expectedSHA string
	var expectedSize int64
	var leaseExpires, databaseNow time.Time
	err = s.pool.QueryRow(r.Context(), `
		SELECT target.object_key,target.content_type,target.expected_size_bytes,target.expected_sha256,
		  root.publication_lease_expires_at,now()
		FROM recording_joined_artifacts target
		JOIN recording_joined_artifacts root ON root.batch_record_id=target.batch_record_id
		  AND root.scope_kind=target.scope_kind AND root.scope_id=target.scope_id AND root.artifact_kind<>'media'
		LEFT JOIN recording_joined_hours h ON h.id=root.hour_record_id
		JOIN connections c ON c.id=target.connection_id AND c.id=$6
		WHERE target.id=$1 AND target.scope_id=$2 AND target.scope_kind=$5 AND root.publication_state='publishing'
		  AND root.publication_token=$3 AND root.publication_lease_expires_at>now() AND target.batch_id=$4
		  AND (root.artifact_kind<>'batch_index' OR ($8='frozen_batch' AND NOT EXISTS(SELECT 1
		    FROM recording_joined_batch_index_refs ref
		    JOIN recording_joined_artifacts referenced ON referenced.id=ref.referenced_artifact_id
		    JOIN recording_joined_hours gap_hour ON gap_hour.id=referenced.hour_record_id
		    WHERE ref.index_artifact_id=root.id AND ref.reference_kind='hour_manifest' AND gap_hour.source_clip_count=0
		      AND NOT EXISTS(SELECT 1 FROM recording_joined_gap_only_scope_authorizations ga
		        WHERE ga.artifact_id=referenced.id AND ga.batch_record_id=referenced.batch_record_id
		          AND ga.batch_id=referenced.batch_id AND ga.hour_record_id=referenced.hour_record_id
		          AND ga.hour_id=referenced.scope_id AND ga.work_scope='frozen_batch'
		          AND ga.work_scope_identity_sha256=$7
		          AND ga.authorization_source IN ('server_seal','operator_frozen')))))
		  AND (root.artifact_kind<>'hour_manifest' OR h.source_clip_count>0 OR EXISTS(SELECT 1
		    FROM recording_joined_gap_only_scope_authorizations ga WHERE ga.artifact_id=root.id
		      AND ga.batch_record_id=root.batch_record_id AND ga.batch_id=root.batch_id
		      AND ga.hour_record_id=root.hour_record_id AND ga.hour_id=root.scope_id
		      AND ga.work_scope=$8 AND ga.work_scope_identity_sha256=$7
		      AND (($8='frozen_batch' AND ga.authorization_source IN ('server_seal','operator_frozen'))
		        OR ($8 IN ('canary','canary_single','allowlist_50') AND ga.authorization_source='server_seal'))))`,
		req.ArtifactID, req.ScopeID, token, claims.BatchID, req.ScopeKind, s.cfg.JoinedRecordingConnectionID, workScopeSHA,
		workScope.WorkScope).Scan(
		&objectKey, &contentType, &expectedSize, &expectedSHA, &leaseExpires, &databaseNow)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusConflict, "joined artifact capability lease is stale or foreign")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load sealed joined artifact: %v", err))
		return
	}
	store := s.joinedOutputStore()
	if store == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "joined object storage is unavailable")
		return
	}
	configuredTTL := 15 * time.Minute
	if operation == "put" {
		configuredTTL = time.Hour
	}
	ttl, err := joinedCapabilityTTL(configuredTTL, leaseExpires, databaseNow, operation == "put")
	if err != nil {
		util.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	outputAuthority, err := joinedOutputAuthority(s.cfg.R2Endpoint)
	if err != nil {
		util.WriteError(w, http.StatusServiceUnavailable, "joined output storage authority is invalid")
		return
	}
	if operation == "put" {
		expiresAt := time.Now().UTC().Add(ttl)
		put, err := store.PresignPutCreateOnlyRequest(r.Context(), objectKey, contentType, expectedSize, expectedSHA, ttl)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("presign joined artifact put: %v", err))
			return
		}
		putDTO, err := joinedSignedRequestFrom(put, outputAuthority)
		if err != nil {
			util.WriteError(w, http.StatusBadGateway, err.Error())
			return
		}
		expiresAt, err = joinedSignedRequestExpiry(putDTO, expiresAt)
		if err != nil {
			util.WriteError(w, http.StatusBadGateway, err.Error())
			return
		}
		if err := s.revalidateJoinedArtifactCapability(r.Context(), req.ScopeKind, req.ScopeID, claims.BatchID, req.ArtifactID, token); err != nil {
			util.WriteError(w, http.StatusConflict, "joined artifact capability lease changed")
			return
		}
		response := joinedrecording.ObjectCreateCapability{ProtocolVersion: joinedWorkerProtocolVersion, ArtifactID: req.ArtifactID,
			ObjectKey: objectKey, ContentType: contentType, SizeBytes: expectedSize, SHA256: expectedSHA,
			ExpiresAt: expiresAt, Request: putDTO}
		if err := response.Validate(req.ArtifactID, s.cfg.R2Bucket, objectKey, contentType, expectedSize, expectedSHA,
			outputAuthority, databaseNow); err != nil {
			util.WriteError(w, http.StatusBadGateway, "joined artifact create capability response differs")
			return
		}
		util.WriteJSON(w, http.StatusOK, response)
		return
	}
	expiresAt := time.Now().UTC().Add(ttl)
	if expiresAt.After(leaseExpires) {
		expiresAt = leaseExpires
	}
	identity, err := store.Head(r.Context(), objectKey)
	if err != nil || identity.ETag == "" || identity.SizeBytes != expectedSize {
		util.WriteError(w, http.StatusConflict, "joined artifact storage identity differs from its sealed bytes")
		return
	}
	get, err := store.PresignGetExactRequest(r.Context(), objectKey, identity.ETag, identity.VersionID, ttl)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("presign joined artifact get: %v", err))
		return
	}
	getDTO, err := joinedSignedRequestFrom(get, outputAuthority)
	if err != nil {
		util.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	expiresAt, err = joinedSignedRequestExpiry(getDTO, expiresAt)
	if err != nil {
		util.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	if err := s.revalidateJoinedArtifactCapability(r.Context(), req.ScopeKind, req.ScopeID, claims.BatchID, req.ArtifactID, token); err != nil {
		util.WriteError(w, http.StatusConflict, "joined artifact capability lease changed")
		return
	}
	response := joinedrecording.ObjectReadCapability{ProtocolVersion: joinedWorkerProtocolVersion, ArtifactID: req.ArtifactID,
		ObjectKey: objectKey, SizeBytes: expectedSize, SHA256: expectedSHA, ETag: identity.ETag,
		VersionID: identity.VersionID, ExpiresAt: expiresAt, Request: getDTO}
	if err := response.Validate(req.ArtifactID, s.cfg.R2Bucket, objectKey, expectedSize, expectedSHA, identity.ETag,
		identity.VersionID, outputAuthority, databaseNow); err != nil {
		util.WriteError(w, http.StatusBadGateway, "joined artifact read capability response differs")
		return
	}
	util.WriteJSON(w, http.StatusOK, response)
}
