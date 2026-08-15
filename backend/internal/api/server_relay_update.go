package api

import (
	"bytes"
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

const relayManifestMaxBytes = 1 << 20

type relayUpdateIntentRequest struct {
	RequestID               string `json:"request_id"`
	ExpectedPromotedVersion string `json:"expected_promoted_version"`
}

type relayUpdateIntent struct {
	RequestID      string `json:"request_id"`
	NodeID         int64  `json:"node_id"`
	TargetVersion  string `json:"target_version"`
	ManifestName   string `json:"manifest_name"`
	ManifestSHA256 string `json:"manifest_sha256"`
	Status         string `json:"status"`
}

type promotedRelayManifest struct {
	Version         string `json:"version"`
	PreviousVersion string `json:"previous_version"`
}

type relayManifestSnapshot struct {
	Version         string
	Name            string
	SHA256          string
	PreviousVersion string
}

var errRelayUpdateReplayConflict = errors.New("relay update request replay differs")

func (s *Server) snapshotPromotedRelayManifest(ctx context.Context, expected, fromVersion string) (relayManifestSnapshot, error) {
	return snapshotPromotedRelayManifest(ctx, expected, fromVersion, func(ctx context.Context, key string) (io.ReadCloser, error) {
		return s.r2.Open(ctx, key)
	})
}

func snapshotPromotedRelayManifest(ctx context.Context, expected, fromVersion string, open func(context.Context, string) (io.ReadCloser, error)) (relayManifestSnapshot, error) {
	expected = strings.TrimSpace(expected)
	name := "latest-" + expected + ".json"
	if expected == "" || !relayArtifactName.MatchString(name) || strings.Contains(expected, "..") {
		return relayManifestSnapshot{}, fmt.Errorf("invalid expected promoted version")
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
	sum := sha256.Sum256(immutable)
	return relayManifestSnapshot{Version: expected, Name: name, SHA256: hex.EncodeToString(sum[:]), PreviousVersion: manifest.PreviousVersion}, nil
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
	if replay, found, err := s.loadAuthorizedRelayUpdateReplay(r.Context(), principal, nodeID, requestID, expected); errors.Is(err, errRelayUpdateReplayConflict) {
		util.WriteError(w, http.StatusConflict, "request_id was already used with different input")
		return
	} else if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusUnauthorized, "authorization changed")
		return
	} else if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load relay update intent")
		return
	} else if found {
		util.WriteJSON(w, http.StatusOK, replay)
		return
	}
	var observedNodeVersion, observedNodeType, observedStatus string
	var supportsUpdateIntents bool
	if err := s.pool.QueryRow(r.Context(), `
		SELECT COALESCE(capabilities_jsonb->>'relay_version',''),node_type,status,
		       COALESCE(capabilities_jsonb->>'relay_update_intents_v1'='true',false)
		FROM nodes WHERE id=$1 AND account_id=$2
	`, nodeID, principal.AccountID).Scan(&observedNodeVersion, &observedNodeType, &observedStatus, &supportsUpdateIntents); errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "relay node not found")
		return
	} else if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load relay node")
		return
	}
	if observedNodeType != nodeTypeRelay || observedStatus != "active" || observedNodeVersion == "" || !supportsUpdateIntents {
		util.WriteError(w, http.StatusConflict, "relay node must be active and support typed update intents")
		return
	}

	snapshotter := s.snapshotPromotedRelayManifest
	if s.relayUpdateSnapshot != nil {
		snapshotter = s.relayUpdateSnapshot
	}
	snapshot, err := snapshotter(r.Context(), expected, observedNodeVersion)
	if err != nil {
		log.Printf("relay update manifest snapshot failed account_id=%d node_id=%d: %v", principal.AccountID, nodeID, err)
		util.WriteError(w, http.StatusConflict, "expected promoted relay version is not current and immutable")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin relay update intent")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if err := authorizeRelayUpdatePrincipalTx(r.Context(), tx, principal); errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusUnauthorized, "authorization changed")
		return
	} else if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock relay update authorization")
		return
	}
	if _, err := tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, relayUpdateRequestLock(principal.AccountID, requestID)); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock relay update request")
		return
	}
	if replay, found, err := loadRelayUpdateIntentTx(r.Context(), tx, principal.AccountID, nodeID, requestID, expected); errors.Is(err, errRelayUpdateReplayConflict) {
		util.WriteError(w, http.StatusConflict, "request_id was already used with different input")
		return
	} else if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "reload relay update intent")
		return
	} else if found {
		util.WriteJSON(w, http.StatusOK, replay)
		return
	}

	var nodeVersion, nodeType, status string
	var stillSupportsUpdateIntents bool
	if err := tx.QueryRow(r.Context(), `
		SELECT COALESCE(capabilities_jsonb->>'relay_version',''), node_type, status,
		       COALESCE(capabilities_jsonb->>'relay_update_intents_v1'='true',false)
		FROM nodes WHERE id=$1 AND account_id=$2 FOR UPDATE
	`, nodeID, principal.AccountID).Scan(&nodeVersion, &nodeType, &status, &stillSupportsUpdateIntents); errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "relay node not found")
		return
	} else if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock relay node")
		return
	}
	if nodeType != nodeTypeRelay || status != "active" || nodeVersion != observedNodeVersion || !stillSupportsUpdateIntents || snapshot.PreviousVersion != nodeVersion {
		util.WriteError(w, http.StatusConflict, "relay node changed during request")
		return
	}
	if nodeVersion == snapshot.Version {
		util.WriteError(w, http.StatusConflict, "relay node already runs the promoted version")
		return
	}

	var userID, apiKeyID any
	actorType, actorRef := "session", principal.Email
	if principal.APIKeyID != nil {
		apiKeyID = *principal.APIKeyID
		actorType, actorRef = "api_key", fmt.Sprintf("key:%d", *principal.APIKeyID)
	} else {
		userID = principal.UserID
	}
	var out relayUpdateIntent
	err = tx.QueryRow(r.Context(), `
		INSERT INTO relay_update_intents(
			account_id,node_id,request_id,node_version_at_request,target_version,
			manifest_name,manifest_sha256,requested_by_user_id,requested_by_api_key_id)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		RETURNING request_id::text,node_id,target_version,manifest_name,manifest_sha256,status
	`, principal.AccountID, nodeID, requestID, nodeVersion, snapshot.Version, snapshot.Name, snapshot.SHA256, userID, apiKeyID).Scan(
		&out.RequestID, &out.NodeID, &out.TargetVersion, &out.ManifestName, &out.ManifestSHA256, &out.Status)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			util.WriteError(w, http.StatusConflict, "relay node already has a pending update intent")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, "create relay update intent")
		return
	}
	if err := s.insertAccountAuthEventTx(r.Context(), tx, principal.AccountID, principal.APIKeyID, "relay_update_intent_created", actorType, actorRef, map[string]any{
		"node_id": nodeID, "request_id": requestID.String(), "target_version": snapshot.Version, "manifest_sha256": snapshot.SHA256,
	}); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "audit relay update intent")
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit relay update intent")
		return
	}
	util.WriteJSON(w, http.StatusCreated, out)
}

func (s *Server) loadAuthorizedRelayUpdateReplay(ctx context.Context, principal accountPrincipal, nodeID int64, requestID uuid.UUID, expected string) (relayUpdateIntent, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return relayUpdateIntent{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authorizeRelayUpdatePrincipalTx(ctx, tx, principal); err != nil {
		return relayUpdateIntent{}, false, err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, relayUpdateRequestLock(principal.AccountID, requestID)); err != nil {
		return relayUpdateIntent{}, false, err
	}
	out, found, err := loadRelayUpdateIntentTx(ctx, tx, principal.AccountID, nodeID, requestID, expected)
	if err != nil {
		return relayUpdateIntent{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return relayUpdateIntent{}, false, err
	}
	return out, found, nil
}

func relayUpdateRequestLock(accountID int64, requestID uuid.UUID) string {
	return fmt.Sprintf("relay-update-intent:%d:%s", accountID, requestID.String())
}

func authorizeRelayUpdatePrincipalTx(ctx context.Context, tx pgx.Tx, principal accountPrincipal) error {
	if principal.APIKeyID != nil {
		return tx.QueryRow(ctx, `
			SELECT 1 FROM account_api_keys k JOIN accounts a ON a.id=k.account_id
			WHERE k.id=$1 AND k.account_id=$2 AND k.revoked_at IS NULL
			  AND (k.expires_at IS NULL OR k.expires_at>transaction_timestamp()) AND a.status='active'
			FOR SHARE OF k,a
		`, *principal.APIKeyID, principal.AccountID).Scan(new(int))
	}
	if principal.SessionID == nil || principal.UserID <= 0 {
		return pgx.ErrNoRows
	}
	return tx.QueryRow(ctx, `
		SELECT 1
		FROM account_sessions s
		JOIN accounts a ON a.id=s.current_org_id
		JOIN users u ON u.id=s.user_id
		JOIN memberships m ON m.user_id=u.id AND m.org_id=a.id
		WHERE s.id=$1 AND s.user_id=$2 AND s.current_org_id=$3
		  AND s.revoked_at IS NULL AND s.expires_at>transaction_timestamp()
		  AND a.status='active' AND m.role='owner'
		FOR SHARE OF s,a,u,m
	`, *principal.SessionID, principal.UserID, principal.AccountID).Scan(new(int))
}

func loadRelayUpdateIntentTx(ctx context.Context, tx pgx.Tx, accountID, nodeID int64, requestID uuid.UUID, expected string) (relayUpdateIntent, bool, error) {
	row := tx.QueryRow(ctx, `
		SELECT request_id::text,node_id,target_version,manifest_name,manifest_sha256,status
		FROM relay_update_intents WHERE account_id=$1 AND request_id=$2
		FOR SHARE
	`, accountID, requestID)
	return scanRelayUpdateIntent(row, nodeID, expected)
}

func scanRelayUpdateIntent(row pgx.Row, nodeID int64, expected string) (relayUpdateIntent, bool, error) {
	var out relayUpdateIntent
	if err := row.Scan(&out.RequestID, &out.NodeID, &out.TargetVersion, &out.ManifestName, &out.ManifestSHA256, &out.Status); errors.Is(err, pgx.ErrNoRows) {
		return relayUpdateIntent{}, false, nil
	} else if err != nil {
		return relayUpdateIntent{}, false, err
	}
	if out.NodeID != nodeID || out.TargetVersion != expected {
		return relayUpdateIntent{}, false, errRelayUpdateReplayConflict
	}
	return out, true, nil
}

func resolveRelayUpdateHeartbeat(ctx context.Context, tx pgx.Tx, principal nodePrincipal, reportedVersion string) (*relayUpdateIntent, error) {
	if principal.NodeType != nodeTypeRelay || reportedVersion == "" {
		return nil, nil
	}
	var out relayUpdateIntent
	var nodeVersionAtRequest string
	err := tx.QueryRow(ctx, `
		SELECT request_id::text,node_id,target_version,manifest_name,manifest_sha256,status,node_version_at_request
		FROM relay_update_intents
		WHERE account_id=$1 AND node_id=$2 AND status='pending'
		FOR UPDATE
	`, principal.AccountID, principal.NodeID).Scan(
		&out.RequestID, &out.NodeID, &out.TargetVersion, &out.ManifestName, &out.ManifestSHA256, &out.Status, &nodeVersionAtRequest)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if reportedVersion == out.TargetVersion {
		if _, err := tx.Exec(ctx, `
			UPDATE relay_update_intents
			SET status='completed',completed_at=transaction_timestamp(),completed_version=$2
			WHERE account_id=$1 AND request_id=$3 AND status='pending'
		`, principal.AccountID, out.TargetVersion, out.RequestID); err != nil {
			return nil, err
		}
		if err := insertRelayUpdateNodeEvent(ctx, tx, principal, "relay_update_intent_completed", out, reportedVersion); err != nil {
			return nil, err
		}
		return nil, nil
	}
	if reportedVersion != nodeVersionAtRequest {
		if _, err := tx.Exec(ctx, `
			UPDATE relay_update_intents
			SET status='superseded',superseded_at=transaction_timestamp(),superseded_by_version=$2
			WHERE account_id=$1 AND request_id=$3 AND status='pending'
		`, principal.AccountID, reportedVersion, out.RequestID); err != nil {
			return nil, err
		}
		if err := insertRelayUpdateNodeEvent(ctx, tx, principal, "relay_update_intent_superseded", out, reportedVersion); err != nil {
			return nil, err
		}
		return nil, nil
	}
	return &out, nil
}

func insertRelayUpdateNodeEvent(ctx context.Context, tx pgx.Tx, principal nodePrincipal, eventType string, intent relayUpdateIntent, reportedVersion string) error {
	detail, err := json.Marshal(map[string]any{
		"node_id": principal.NodeID, "request_id": intent.RequestID,
		"target_version": intent.TargetVersion, "reported_version": reportedVersion,
	})
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO account_auth_events(account_id,event_type,actor_type,actor_ref,detail_jsonb)
		VALUES ($1,$2,'node',$3,$4::jsonb)
	`, principal.AccountID, eventType, fmt.Sprintf("node:%d", principal.NodeID), string(detail))
	return err
}
