package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/stitchcert"
	"github.com/daydemir/stoarama/backend/internal/util"
)

type joinedGapOnlyFrozenAuthorizationRequest struct {
	ProtocolVersion      int    `json:"protocol_version"`
	BatchID              string `json:"batch_id"`
	ArtifactID           int64  `json:"artifact_id"`
	HourID               string `json:"hour_id"`
	ObjectKey            string `json:"object_key"`
	ExpectedSizeBytes    int64  `json:"expected_size_bytes"`
	ExpectedSHA256       string `json:"expected_sha256"`
	ReviewEvidenceSHA256 string `json:"review_evidence_sha256"`
	IncidentID           string `json:"incident_id"`
	Apply                bool   `json:"apply"`
}

var joinedGapAuthorizationIncidentIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)

type joinedGapOnlyFrozenAuthorizationResponse struct {
	ProtocolVersion         int       `json:"protocol_version"`
	ArtifactID              int64     `json:"artifact_id"`
	HourID                  string    `json:"hour_id"`
	WorkScopeIdentitySHA256 string    `json:"work_scope_identity_sha256"`
	RequestSHA256           string    `json:"request_sha256"`
	CreatedAt               time.Time `json:"created_at"`
	AlreadyAuthorized       bool      `json:"already_authorized"`
	ReviewEvidenceSHA256    string    `json:"review_evidence_sha256"`
	Applied                 bool      `json:"applied"`
}

func (r joinedGapOnlyFrozenAuthorizationRequest) validate() error {
	if r.ProtocolVersion != joinedrecording.JoinedProtocolVersion || !joinedrecording.ValidBatchID(r.BatchID) ||
		r.ArtifactID <= 0 || !joinedGapAuthorizationIncidentIDPattern.MatchString(r.IncidentID) ||
		strings.TrimSpace(r.HourID) != r.HourID || r.HourID == "" ||
		strings.TrimSpace(r.ObjectKey) != r.ObjectKey || r.ObjectKey == "" || r.ExpectedSizeBytes <= 0 ||
		!lowerHex64(r.ExpectedSHA256) || (r.Apply && !lowerHex64(r.ReviewEvidenceSHA256)) || (!r.Apply && r.ReviewEvidenceSHA256 != "") {
		return errors.New("invalid joined gap-only frozen authorization")
	}
	return nil
}

func (s *Server) handleJoinedGapOnlyFrozenAuthorization(w http.ResponseWriter, r *http.Request) {
	var req joinedGapOnlyFrozenAuthorizationRequest
	if err := util.DecodeJSON(r, &req); err != nil || req.validate() != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid joined gap-only frozen authorization")
		return
	}
	if !s.joinedControlPlaneReady() || !s.joinedFrozenBatchScope() {
		util.WriteError(w, http.StatusConflict, "joined frozen-batch scope is not active")
		return
	}
	if req.BatchID != s.cfg.JoinedRecordingBatchID {
		util.WriteError(w, http.StatusForbidden, "joined batch scope differs")
		return
	}
	response, err := s.authorizeJoinedGapOnlyFrozen(r.Context(), req)
	if err != nil {
		util.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusOK, response)
}

func (s *Server) authorizeJoinedGapOnlyFrozen(ctx context.Context, req joinedGapOnlyFrozenAuthorizationRequest) (joinedGapOnlyFrozenAuthorizationResponse, error) {
	var response joinedGapOnlyFrozenAuthorizationResponse
	var preState, preRelativePath, preObjectKey, preSHA, preETag, preVersion string
	var preSize int64
	var preCanonical []byte
	if err := s.pool.QueryRow(ctx, `SELECT publication_state,relative_path,object_key,expected_size_bytes,expected_sha256,
		canonical_bytes,COALESCE(etag,''),COALESCE(version_id,'') FROM recording_joined_artifacts
		WHERE id=$1 AND batch_id=$2 AND scope_id=$3 AND artifact_kind='hour_manifest'`, req.ArtifactID, req.BatchID, req.HourID).Scan(
		&preState, &preRelativePath, &preObjectKey, &preSize, &preSHA, &preCanonical, &preETag, &preVersion); err != nil {
		return response, errors.New("joined gap-only artifact identity differs")
	}
	if (preState != "sealed" && preState != "published") || preObjectKey != req.ObjectKey || preSize != req.ExpectedSizeBytes || preSHA != req.ExpectedSHA256 {
		return response, errors.New("joined gap-only artifact identity differs")
	}
	reviewEvidenceSHA, err := verifyJoinedGapAuthorizationStorage(ctx, s.joinedOutputStore(), req.ArtifactID,
		preState, preObjectKey, preSize, preSHA, preETag, preVersion, preCanonical)
	if err != nil || (req.Apply && reviewEvidenceSHA != req.ReviewEvidenceSHA256) {
		return response, errors.New("joined gap-only storage review differs")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return response, errors.New("begin joined gap-only authorization")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var batchRecordID, hourRecordID, size int64
	var batchState, hourID, relativePath, objectKey, expectedSHA, publicationState, etag, versionID string
	var canonical []byte
	if err := tx.QueryRow(ctx, `SELECT a.batch_record_id,a.hour_record_id,b.state,h.hour_id,a.relative_path,a.object_key,
		a.expected_size_bytes,a.expected_sha256,a.canonical_bytes,a.publication_state,COALESCE(a.etag,''),COALESCE(a.version_id,'')
		FROM recording_joined_artifacts a JOIN recording_joined_batches b ON b.id=a.batch_record_id
		JOIN recording_joined_hours h ON h.id=a.hour_record_id
		JOIN connections c ON c.id=a.connection_id AND c.id=$4
		WHERE a.id=$1 AND a.batch_id=$2 AND a.artifact_kind='hour_manifest' AND a.scope_kind='hour'
		  AND h.state='sealed' AND h.source_clip_count=0 AND h.hour_id=$3
		FOR UPDATE OF a,b,h,c`, req.ArtifactID, req.BatchID, req.HourID, s.cfg.JoinedRecordingConnectionID).Scan(
		&batchRecordID, &hourRecordID, &batchState, &hourID, &relativePath, &objectKey, &size, &expectedSHA, &canonical,
		&publicationState, &etag, &versionID); err != nil {
		return response, errors.New("joined gap-only artifact identity differs")
	}
	if (batchState != "frozen" && batchState != "index_sealed") || publicationState != preState || relativePath != preRelativePath ||
		objectKey != req.ObjectKey || size != req.ExpectedSizeBytes || expectedSHA != req.ExpectedSHA256 || !bytes.Equal(canonical, preCanonical) ||
		etag != preETag || versionID != preVersion {
		return response, errors.New("joined gap-only artifact identity differs")
	}
	var paused bool
	if err := tx.QueryRow(ctx, `SELECT claims_paused FROM recording_joined_admission_controls
		WHERE batch_record_id=$1 AND batch_id=$2 FOR UPDATE`, batchRecordID, req.BatchID).Scan(&paused); err != nil || !paused {
		return response, errors.New("joined claims must be paused")
	}
	var active, indexed int
	if err := tx.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM recording_joined_hours WHERE batch_record_id=$1 AND state='leased' AND lease_expires_at>now())+
		(SELECT count(*) FROM recording_joined_artifacts WHERE batch_record_id=$1 AND publication_state='publishing' AND publication_lease_expires_at>now()),
		(SELECT count(*) FROM recording_joined_batch_index_refs WHERE referenced_artifact_id=$2)`, batchRecordID, req.ArtifactID).Scan(&active, &indexed); err != nil || active != 0 || indexed != 0 {
		return response, errors.New("joined gap-only authorization requires a drained unindexed batch")
	}
	var manifest joinedrecording.HourManifest
	if json.Unmarshal(canonical, &manifest) != nil || manifest.Status != "gap_only" || manifest.HourID != hourID ||
		manifest.SourceCount != 0 || len(manifest.Sources) != 0 || len(manifest.Media) != 0 {
		return response, errors.New("joined gap-only canonical manifest differs")
	}
	rebuilt, rebuiltSHA, err := joinedrecording.CanonicalHourManifestArtifact(manifest)
	if err != nil || !bytes.Equal(rebuilt, canonical) || int64(len(rebuilt)) != size || rebuiltSHA != expectedSHA {
		return response, errors.New("joined gap-only canonical manifest differs")
	}
	var ledgerArtifactID int64
	var ledgerBytes []byte
	if err := tx.QueryRow(ctx, `SELECT ledger.id,ledger.canonical_bytes FROM recording_joined_artifacts target
		JOIN recording_joined_artifacts ledger ON ledger.stream_day_id=target.stream_day_id
		  AND ledger.artifact_kind='allocation_ledger' AND ledger.publication_state='published'
		WHERE target.id=$1 FOR SHARE OF ledger`, req.ArtifactID).Scan(&ledgerArtifactID, &ledgerBytes); err != nil {
		return response, errors.New("joined gap-only allocation ledger differs")
	}
	var ledger joinedrecording.StreamDayAllocation
	if json.Unmarshal(ledgerBytes, &ledger) != nil {
		return response, errors.New("joined gap-only allocation ledger differs")
	}
	ledgerRef, err := joinedrecording.BuildAllocationLedgerRef(ledgerArtifactID, ledger)
	if err != nil || joinedrecording.ValidateHourManifestLedgerBinding(manifest, ledgerRef, ledger) != nil {
		return response, errors.New("joined gap-only allocation ledger differs")
	}
	frozenScope, err := joinedrecording.NewWorkScopeIdentity(req.BatchID, joinedrecording.WorkScopeFrozenBatch, nil)
	if err != nil {
		return response, errors.New("joined frozen scope differs")
	}
	scopeSHA, scopeBytes, err := frozenScope.Canonical(req.BatchID)
	if err != nil {
		return response, errors.New("joined frozen scope differs")
	}
	requestSHA, _, err := stitchcert.CanonicalSHA(req)
	if err != nil {
		return response, errors.New("joined gap-only request identity differs")
	}
	if !req.Apply {
		return joinedGapOnlyFrozenAuthorizationResponse{ProtocolVersion: joinedrecording.JoinedProtocolVersion,
			ArtifactID: req.ArtifactID, HourID: hourID, WorkScopeIdentitySHA256: scopeSHA, RequestSHA256: requestSHA,
			ReviewEvidenceSHA256: reviewEvidenceSHA, Applied: false}, nil
	}
	var createdAt time.Time
	var storedRequestSHA string
	err = tx.QueryRow(ctx, `INSERT INTO recording_joined_gap_only_scope_authorizations
		(artifact_id,batch_record_id,batch_id,hour_record_id,hour_id,work_scope,work_scope_identity_sha256,work_scope_identity_bytes,
		 canary_hour_ids_sha256,authorization_source,request_sha256,relative_path,object_key,expected_size_bytes,expected_sha256,
		 review_evidence_sha256,incident_id,verification_policy_version,verified_publication_state,verified_etag,verified_version_id)
		VALUES($1,$2,$3,$4,$5,'frozen_batch',$6,$7,NULL,'operator_frozen',$8,$9,$10,$11,$12,$13,
		 $14,'joined-gap-authorization-v1',$15,NULLIF($16,''),NULLIF($17,''))
		ON CONFLICT (artifact_id,work_scope_identity_sha256) DO NOTHING
		RETURNING request_sha256,created_at`, req.ArtifactID, batchRecordID, req.BatchID, hourRecordID, hourID, scopeSHA, scopeBytes,
		requestSHA, relativePath, objectKey, size, expectedSHA, reviewEvidenceSHA, req.IncidentID, publicationState, etag, versionID).Scan(
		&storedRequestSHA, &createdAt)
	already := false
	if errors.Is(err, pgx.ErrNoRows) {
		already = true
		err = tx.QueryRow(ctx, `SELECT request_sha256,created_at FROM recording_joined_gap_only_scope_authorizations
			WHERE artifact_id=$1 AND work_scope_identity_sha256=$2 FOR SHARE`, req.ArtifactID, scopeSHA).Scan(&storedRequestSHA, &createdAt)
	}
	if err != nil || storedRequestSHA != requestSHA {
		return response, errors.New("joined gap-only authorization retry differs")
	}
	if err := tx.Commit(ctx); err != nil {
		return response, errors.New("commit joined gap-only authorization")
	}
	return joinedGapOnlyFrozenAuthorizationResponse{ProtocolVersion: joinedrecording.JoinedProtocolVersion,
		ArtifactID: req.ArtifactID, HourID: hourID, WorkScopeIdentitySHA256: scopeSHA, RequestSHA256: requestSHA,
		CreatedAt: createdAt.UTC(), AlreadyAuthorized: already, ReviewEvidenceSHA256: reviewEvidenceSHA, Applied: true}, nil
}

func verifyJoinedGapAuthorizationStorage(ctx context.Context, store joinedOutputObjectStore, artifactID int64, state, key string,
	size int64, sha, etag, versionID string, canonical []byte) (string, error) {
	if store == nil {
		return "", errors.New("joined output storage is unavailable")
	}
	verification := struct {
		Policy           string `json:"policy"`
		ArtifactID       int64  `json:"artifact_id"`
		PublicationState string `json:"publication_state"`
		ObjectKey        string `json:"object_key"`
		SizeBytes        int64  `json:"size_bytes"`
		SHA256           string `json:"sha256"`
		ETag             string `json:"etag,omitempty"`
		VersionID        string `json:"version_id,omitempty"`
		StorageStatus    string `json:"storage_status"`
	}{"joined-gap-authorization-v1", artifactID, state, key, size, sha, etag, versionID, ""}
	if state == "sealed" {
		if _, err := store.Head(ctx, key); err == nil || !r2.IsNotFound(err) {
			return "", errors.New("sealed joined gap target is not absent")
		}
		verification.StorageStatus = "absent"
	} else {
		reader, err := store.OpenExact(ctx, key, etag, versionID)
		if err != nil {
			return "", err
		}
		defer reader.Close()
		body, err := io.ReadAll(io.LimitReader(reader, size+1))
		if err != nil || int64(len(body)) != size || !bytes.Equal(body, canonical) {
			return "", errors.New("published joined gap object differs")
		}
		sum := sha256.Sum256(body)
		if hex.EncodeToString(sum[:]) != sha {
			return "", errors.New("published joined gap hash differs")
		}
		verification.StorageStatus = "exact"
	}
	digest, _, err := stitchcert.CanonicalSHA(verification)
	return digest, err
}
