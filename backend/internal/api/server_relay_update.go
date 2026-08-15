package api

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/daydemir/stoarama/backend/internal/util"
)

const (
	relayManifestMaxBytes = 1 << 20
	relayArchiveMaxBytes  = 256 << 20
)

type relayUpdateIntentRequest struct {
	RequestID               string `json:"request_id"`
	ExpectedPromotedVersion string `json:"expected_promoted_version"`
}

type relayUpdateIntent struct {
	RequestID           string `json:"request_id"`
	NodeID              int64  `json:"node_id"`
	TargetVersion       string `json:"target_version"`
	ManifestName        string `json:"manifest_name"`
	ManifestSHA256      string `json:"manifest_sha256"`
	RelayArtifactSHA256 string `json:"relay_artifact_sha256"`
	InstalledSHA256     string `json:"installed_binary_sha256"`
	Status              string `json:"status"`
}

type relayUpdateAppliedResult struct {
	RequestID           string `json:"request_id"`
	TargetVersion       string `json:"target_version"`
	ManifestName        string `json:"manifest_name"`
	ManifestSHA256      string `json:"manifest_sha256"`
	RelayArtifactSHA256 string `json:"relay_artifact_sha256"`
	InstalledSHA256     string `json:"installed_binary_sha256"`
	AppliedAt           string `json:"applied_at"`
}

type relayUpdateHeartbeatResult struct {
	Intent       *relayUpdateIntent `json:"intent,omitempty"`
	AckRequestID string             `json:"ack_request_id,omitempty"`
}

type promotedRelayArtifact struct {
	Artifact string `json:"artifact"`
	SHA256   string `json:"sha256"`
}

type promotedRelayManifest struct {
	Version         string                           `json:"version"`
	PreviousVersion string                           `json:"previous_version"`
	Relay           map[string]promotedRelayArtifact `json:"relay"`
}

type relayManifestSnapshot struct {
	Version         string
	Name            string
	SHA256          string
	PreviousVersion string
	RuntimeTarget   string
	ArtifactSHA256  string
	InstalledSHA256 string
}

var errRelayUpdateReplayConflict = errors.New("relay update request replay differs")

func (s *Server) snapshotPromotedRelayManifest(ctx context.Context, expected, fromVersion, runtimeTarget string) (relayManifestSnapshot, error) {
	return snapshotPromotedRelayManifest(ctx, expected, fromVersion, runtimeTarget, func(ctx context.Context, key string) (io.ReadCloser, error) {
		return s.r2.Open(ctx, key)
	})
}

func snapshotPromotedRelayManifest(ctx context.Context, expected, fromVersion, runtimeTarget string, open func(context.Context, string) (io.ReadCloser, error)) (relayManifestSnapshot, error) {
	expected, runtimeTarget = strings.TrimSpace(expected), strings.TrimSpace(runtimeTarget)
	name := "latest-" + expected + ".json"
	if expected == "" || runtimeTarget == "" || !relayArtifactName.MatchString(name) || strings.Contains(expected, "..") {
		return relayManifestSnapshot{}, fmt.Errorf("invalid expected promoted version or runtime target")
	}
	read := func(key string, max int64) ([]byte, error) {
		body, err := open(ctx, relayReleasePrefix+key)
		if err != nil {
			return nil, err
		}
		defer body.Close()
		data, err := io.ReadAll(io.LimitReader(body, max+1))
		if err != nil {
			return nil, err
		}
		if int64(len(data)) > max {
			return nil, fmt.Errorf("release object exceeds %d bytes", max)
		}
		return data, nil
	}
	live, err := read("latest.json", relayManifestMaxBytes)
	if err != nil {
		return relayManifestSnapshot{}, fmt.Errorf("read promoted relay manifest: %w", err)
	}
	var manifest promotedRelayManifest
	if err := json.Unmarshal(live, &manifest); err != nil || manifest.Version != expected {
		return relayManifestSnapshot{}, fmt.Errorf("current promoted relay version differs")
	}
	if manifest.PreviousVersion != fromVersion || strings.TrimSpace(fromVersion) == "" {
		return relayManifestSnapshot{}, fmt.Errorf("current promoted relay version is not the node's immediate successor")
	}
	artifact, ok := manifest.Relay[runtimeTarget]
	artifact.Artifact = strings.TrimSpace(artifact.Artifact)
	artifact.SHA256 = strings.TrimSpace(artifact.SHA256)
	if !ok || !relayArtifactName.MatchString(artifact.Artifact) || strings.Contains(artifact.Artifact, "..") || !lowerSHA256(artifact.SHA256) {
		return relayManifestSnapshot{}, fmt.Errorf("promoted manifest has no exact relay artifact for %s", runtimeTarget)
	}
	immutable, err := read(name, relayManifestMaxBytes)
	if err != nil {
		return relayManifestSnapshot{}, fmt.Errorf("read immutable relay manifest: %w", err)
	}
	if !bytes.Equal(live, immutable) {
		return relayManifestSnapshot{}, fmt.Errorf("immutable relay manifest differs from promoted manifest")
	}
	if signature, err := read(name+".sig", 1<<10); err != nil || len(bytes.TrimSpace(signature)) == 0 {
		return relayManifestSnapshot{}, fmt.Errorf("immutable relay manifest signature unavailable")
	}
	liveAgain, err := read("latest.json", relayManifestMaxBytes)
	if err != nil || !bytes.Equal(live, liveAgain) {
		return relayManifestSnapshot{}, fmt.Errorf("promoted relay manifest changed during snapshot")
	}
	archive, err := read(artifact.Artifact, relayArchiveMaxBytes)
	if err != nil {
		return relayManifestSnapshot{}, fmt.Errorf("read promoted relay artifact: %w", err)
	}
	archiveSum := sha256.Sum256(archive)
	if hex.EncodeToString(archiveSum[:]) != artifact.SHA256 {
		return relayManifestSnapshot{}, fmt.Errorf("promoted relay artifact digest differs")
	}
	installedSHA, err := relayInstalledBinarySHA256(archive)
	if err != nil {
		return relayManifestSnapshot{}, fmt.Errorf("inspect promoted relay artifact: %w", err)
	}
	sum := sha256.Sum256(immutable)
	return relayManifestSnapshot{Version: expected, Name: name, SHA256: hex.EncodeToString(sum[:]), PreviousVersion: manifest.PreviousVersion, RuntimeTarget: runtimeTarget, ArtifactSHA256: strings.TrimSpace(artifact.SHA256), InstalledSHA256: installedSHA}, nil
}

func relayInstalledBinarySHA256(archive []byte) (string, error) {
	gz, err := gzip.NewReader(bytes.NewReader(archive))
	if err != nil {
		return "", err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			return "", fmt.Errorf("stoarama-relay entry is absent")
		}
		if err != nil {
			return "", err
		}
		if header.Name != "stoarama-relay" {
			continue
		}
		if header.Typeflag != tar.TypeReg || header.Size < 1 || header.Size > relayArchiveMaxBytes {
			return "", fmt.Errorf("stoarama-relay entry is invalid")
		}
		hash := sha256.New()
		if _, err := io.CopyN(hash, tr, header.Size); err != nil {
			return "", err
		}
		return hex.EncodeToString(hash.Sum(nil)), nil
	}
}

func relayUpdateActorArgs(principal accountPrincipal) (userID, sessionID, apiKeyID any) {
	if principal.APIKeyID != nil {
		return nil, nil, *principal.APIKeyID
	}
	if principal.SessionID == nil {
		return principal.UserID, nil, nil
	}
	return principal.UserID, *principal.SessionID, nil
}

func (s *Server) handleAccountRelayUpdateIntentCreate(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if !principalIsOwner(principal) {
		util.WriteError(w, http.StatusForbidden, "owner role required")
		return
	}
	if s.admissionPool == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "relay update authority unavailable")
		return
	}
	nodeID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req relayUpdateIntentRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	requestID, err := uuid.Parse(strings.TrimSpace(req.RequestID))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, "request_id must be a UUID")
		return
	}
	expected := strings.TrimSpace(req.ExpectedPromotedVersion)
	userID, sessionID, apiKeyID := relayUpdateActorArgs(principal)
	var replayJSON []byte
	err = s.admissionPool.QueryRow(r.Context(), `SELECT relay_update_intent_replay($1,$2,$3,$4,$5,$6,$7,$8)`,
		principal.AccountID, nodeID, requestID, expected, userID, sessionID, apiKeyID, principal.credentialSHA256).Scan(&replayJSON)
	if err == nil && len(replayJSON) != 0 && string(replayJSON) != "null" {
		var replay relayUpdateIntent
		if json.Unmarshal(replayJSON, &replay) != nil {
			util.WriteError(w, http.StatusInternalServerError, "decode relay update replay")
			return
		}
		util.WriteJSON(w, http.StatusOK, replay)
		return
	}
	if err != nil {
		writeRelayUpdateAuthorityError(w, err)
		return
	}
	var nodeVersion, nodeType, status, runtimeTarget string
	var supports bool
	err = s.pool.QueryRow(r.Context(), `
		SELECT COALESCE(capabilities_jsonb->>'relay_version',''),node_type,status,
		       COALESCE(capabilities_jsonb->>'relay_update_runtime_target',''),
		       COALESCE(capabilities_jsonb->>'relay_update_intents_v1'='true',false)
		FROM nodes WHERE id=$1 AND account_id=$2
	`, nodeID, principal.AccountID).Scan(&nodeVersion, &nodeType, &status, &runtimeTarget, &supports)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "relay node not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load relay node")
		return
	}
	if nodeType != nodeTypeRelay || status != "active" || nodeVersion == "" || runtimeTarget == "" || !supports {
		util.WriteError(w, http.StatusConflict, "relay node must be active and support typed update intents")
		return
	}
	snapshotter := s.snapshotPromotedRelayManifest
	if s.relayUpdateSnapshot != nil {
		snapshotter = s.relayUpdateSnapshot
	}
	snapshot, err := snapshotter(r.Context(), expected, nodeVersion, runtimeTarget)
	if err != nil {
		log.Printf("relay update manifest snapshot failed account_id=%d node_id=%d: %v", principal.AccountID, nodeID, err)
		util.WriteError(w, http.StatusConflict, "expected promoted relay version is not current and immutable")
		return
	}
	var createdJSON []byte
	err = s.admissionPool.QueryRow(r.Context(), `SELECT relay_update_intent_create($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
		requestID, principal.AccountID, nodeID, nodeVersion, runtimeTarget, snapshot.Version, snapshot.Name,
		snapshot.SHA256, snapshot.ArtifactSHA256, snapshot.InstalledSHA256, userID, sessionID, apiKeyID, principal.credentialSHA256).Scan(&createdJSON)
	if err != nil {
		writeRelayUpdateAuthorityError(w, err)
		return
	}
	var envelope struct {
		Created bool              `json:"created"`
		Intent  relayUpdateIntent `json:"intent"`
	}
	if err := json.Unmarshal(createdJSON, &envelope); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "decode relay update authority response")
		return
	}
	statusCode := http.StatusOK
	if envelope.Created {
		statusCode = http.StatusCreated
	}
	util.WriteJSON(w, statusCode, envelope.Intent)
}

func writeRelayUpdateAuthorityError(w http.ResponseWriter, err error) {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch {
		case strings.Contains(pgErr.Message, "authorization changed"):
			util.WriteError(w, http.StatusUnauthorized, "authorization changed")
		case strings.Contains(pgErr.Message, "replay differs"):
			util.WriteError(w, http.StatusConflict, "request_id was already used with different input")
		case strings.Contains(pgErr.Message, "live capture authority"), strings.Contains(pgErr.Message, "updater-capable"):
			util.WriteError(w, http.StatusConflict, "relay node is not fresh, idle, and updater-capable")
		default:
			util.WriteError(w, http.StatusInternalServerError, "relay update authority failed")
		}
		return
	}
	util.WriteError(w, http.StatusInternalServerError, "relay update authority failed")
}

func (s *Server) resolveRelayUpdateHeartbeat(ctx context.Context, principal nodePrincipal, reportedVersion string, supports bool, result *relayUpdateAppliedResult) (relayUpdateHeartbeatResult, error) {
	if s.admissionPool == nil || principal.NodeType != nodeTypeRelay {
		return relayUpdateHeartbeatResult{}, nil
	}
	var resultJSON any
	if result != nil {
		encoded, err := json.Marshal(result)
		if err != nil {
			return relayUpdateHeartbeatResult{}, err
		}
		resultJSON = string(encoded)
	}
	var responseJSON []byte
	err := s.admissionPool.QueryRow(ctx, `SELECT relay_update_intent_heartbeat($1,$2,$3,$4,$5,$6,$7::jsonb)`,
		principal.NodeID, principal.NodeTokenID, principal.NodeClaimGeneration, principal.credentialSHA256,
		strings.TrimSpace(reportedVersion), supports, resultJSON).Scan(&responseJSON)
	if err != nil {
		return relayUpdateHeartbeatResult{}, err
	}
	var response relayUpdateHeartbeatResult
	if err := json.Unmarshal(responseJSON, &response); err != nil {
		return relayUpdateHeartbeatResult{}, err
	}
	return response, nil
}
