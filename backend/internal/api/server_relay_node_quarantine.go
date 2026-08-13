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
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/util"
)

type relayNodeQuarantineRequest struct {
	IdempotencyKey          string `json:"idempotency_key"`
	ExpectedAccountID       int64  `json:"expected_account_id"`
	ExpectedRelayGroupID    int64  `json:"expected_relay_group_id"`
	ExpectedRelayMaxStreams int    `json:"expected_relay_max_streams"`
	ExpectedStatus          string `json:"expected_status"`
	ExpectedRelayVersion    string `json:"expected_relay_version"`
	ExpectedSourceRevision  string `json:"expected_source_revision"`
	ExpectedRelayStartedAt  string `json:"expected_relay_started_at"`
	ExpectedHeartbeatAt     string `json:"expected_heartbeat_at"`
	ReasonCode              string `json:"reason_code"`
	EvidenceSHA256          string `json:"evidence_sha256"`
}

type relayNodeQuarantineResponse struct {
	OK             bool      `json:"ok"`
	EventID        uuid.UUID `json:"event_id"`
	IdempotencyKey uuid.UUID `json:"idempotency_key"`
	NodeID         int64     `json:"node_id"`
	AccountID      int64     `json:"account_id"`
	RelayGroupID   int64     `json:"relay_group_id"`
	Status         string    `json:"status"`
	ReasonCode     string    `json:"reason_code"`
	EvidenceSHA256 string    `json:"evidence_sha256"`
	RequestDigest  string    `json:"request_digest"`
	CreatedAt      time.Time `json:"created_at"`
}

var relayNodeQuarantineReasons = map[string]bool{
	"tls_input_incompatibility":   true,
	"runtime_attestation_failure": true,
	"repeated_no_progress":        true,
}

func canonicalRelayNodeQuarantineRequest(nodeID int64, req relayNodeQuarantineRequest) (string, error) {
	canonical := struct {
		NodeID  int64                      `json:"node_id"`
		Request relayNodeQuarantineRequest `json:"request"`
	}{nodeID, req}
	b, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func parseRelayNodeQuarantineRequest(nodeID int64, req relayNodeQuarantineRequest) (relayNodeQuarantineRequest, uuid.UUID, time.Time, time.Time, string, error) {
	req.IdempotencyKey = strings.TrimSpace(req.IdempotencyKey)
	req.ExpectedStatus = strings.TrimSpace(req.ExpectedStatus)
	req.ExpectedRelayVersion = strings.TrimSpace(req.ExpectedRelayVersion)
	req.ExpectedSourceRevision = strings.TrimSpace(req.ExpectedSourceRevision)
	req.ExpectedRelayStartedAt = strings.TrimSpace(req.ExpectedRelayStartedAt)
	req.ExpectedHeartbeatAt = strings.TrimSpace(req.ExpectedHeartbeatAt)
	req.ReasonCode = strings.TrimSpace(req.ReasonCode)
	req.EvidenceSHA256 = strings.TrimSpace(req.EvidenceSHA256)
	key, err := uuid.Parse(strings.TrimSpace(req.IdempotencyKey))
	if err != nil || key == uuid.Nil {
		return req, uuid.Nil, time.Time{}, time.Time{}, "", fmt.Errorf("idempotency_key must be a non-zero UUID")
	}
	req.IdempotencyKey = key.String()
	if nodeID <= 0 || req.ExpectedAccountID <= 0 || req.ExpectedRelayGroupID <= 0 || req.ExpectedRelayMaxStreams < 1 || req.ExpectedRelayMaxStreams > 20 {
		return req, uuid.Nil, time.Time{}, time.Time{}, "", fmt.Errorf("expected node, account, group, and cap are required")
	}
	if req.ExpectedStatus != "active" {
		return req, uuid.Nil, time.Time{}, time.Time{}, "", fmt.Errorf("expected_status must be active")
	}
	if !relayNodeQuarantineReasons[req.ReasonCode] {
		return req, uuid.Nil, time.Time{}, time.Time{}, "", fmt.Errorf("reason_code is not supported")
	}
	if !quarantineLowerHex(req.EvidenceSHA256, 64) || !quarantineLowerHex(req.ExpectedSourceRevision, 40) || req.ExpectedRelayVersion == "" {
		return req, uuid.Nil, time.Time{}, time.Time{}, "", fmt.Errorf("runtime revision and evidence must be canonical")
	}
	started, err := time.Parse(time.RFC3339Nano, req.ExpectedRelayStartedAt)
	if err != nil {
		return req, uuid.Nil, time.Time{}, time.Time{}, "", fmt.Errorf("expected_relay_started_at must be RFC3339")
	}
	heartbeat, err := time.Parse(time.RFC3339Nano, req.ExpectedHeartbeatAt)
	if err != nil {
		return req, uuid.Nil, time.Time{}, time.Time{}, "", fmt.Errorf("expected_heartbeat_at must be RFC3339")
	}
	started, heartbeat = started.UTC(), heartbeat.UTC()
	req.ExpectedRelayStartedAt = started.Format(time.RFC3339Nano)
	req.ExpectedHeartbeatAt = heartbeat.Format(time.RFC3339Nano)
	digest, err := canonicalRelayNodeQuarantineRequest(nodeID, req)
	return req, key, started, heartbeat, digest, err
}

func decodeRelayNodeQuarantineRequest(r *http.Request, dst *relayNodeQuarantineRequest) error {
	defer r.Body.Close()
	dec := json.NewDecoder(io.LimitReader(r.Body, 64<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("decode json: %w", err)
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		return fmt.Errorf("decode json: exactly one value is required")
	}
	return nil
}

func quarantineLowerHex(v string, n int) bool {
	if len(v) != n {
		return false
	}
	for _, c := range v {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

func (s *Server) handleAdminRelayNodeQuarantine(w http.ResponseWriter, r *http.Request) {
	p, ok := accountPrincipalFromContext(r.Context())
	actorIsSession := p.UserID > 0 && p.APIKeyID == nil && p.AuthType == "session"
	actorIsAPIKey := p.UserID == 0 && p.APIKeyID != nil && p.AuthType == "api_key"
	if !ok || p.Role != accountRoleAdmin || actorIsSession == actorIsAPIKey {
		util.WriteError(w, http.StatusForbidden, "typed operator session or API key required")
		return
	}
	nodeID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req relayNodeQuarantineRequest
	if err := decodeRelayNodeQuarantineRequest(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	req, key, expectedStarted, expectedHeartbeat, digest, err := parseRelayNodeQuarantineRequest(nodeID, req)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	tx, err := s.pool.BeginTx(r.Context(), pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin relay quarantine")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	var txNow time.Time
	if err = tx.QueryRow(r.Context(), `SELECT transaction_timestamp()`).Scan(&txNow); err != nil {
		util.WriteError(w, 500, "load transaction time")
		return
	}
	var lockedAccount int64
	if err = tx.QueryRow(r.Context(), `SELECT id FROM accounts WHERE id=$1 FOR UPDATE`, req.ExpectedAccountID).Scan(&lockedAccount); errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, 404, "account not found")
		return
	} else if err != nil {
		util.WriteError(w, 500, "lock account")
		return
	}
	actorKind := "operator_session"
	var operatorUserID, operatorAPIKeyID any = p.UserID, nil
	var actorKeyPrefix *string
	if actorIsAPIKey {
		actorKind, operatorUserID, operatorAPIKeyID = "operator_api_key", nil, *p.APIKeyID
		var prefix string
		if err = tx.QueryRow(r.Context(), `SELECT k.key_prefix FROM account_api_keys k JOIN accounts a ON a.id=k.account_id WHERE k.id=$1 AND k.revoked_at IS NULL AND (k.expires_at IS NULL OR k.expires_at>$2) AND a.status='active' AND a.role='admin' FOR SHARE OF k,a`, *p.APIKeyID, txNow).Scan(&prefix); err != nil {
			util.WriteError(w, http.StatusForbidden, "active operator API key required")
			return
		}
		actorKeyPrefix = &prefix
	}

	var accountID, groupID int64
	var displayName, status string
	var relayMax int
	var heartbeat time.Time
	var relayVersion, sourceRevision, startedRaw string
	err = tx.QueryRow(r.Context(), `SELECT account_id,display_name,status,relay_group_id,relay_max_streams,last_heartbeat_at,COALESCE(capabilities_jsonb->>'relay_version',''),COALESCE(capabilities_jsonb->>'relay_source_revision',''),COALESCE(capabilities_jsonb->>'relay_started_at','') FROM nodes WHERE id=$1 AND node_type='relay' FOR UPDATE`, nodeID).Scan(&accountID, &displayName, &status, &groupID, &relayMax, &heartbeat, &relayVersion, &sourceRevision, &startedRaw)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, 404, "relay node not found")
		return
	} else if err != nil {
		util.WriteError(w, 500, "lock relay node")
		return
	}
	var lockedGroup int64
	if err = tx.QueryRow(r.Context(), `SELECT id FROM relay_groups WHERE id=$1 AND account_id=$2 FOR UPDATE`, groupID, accountID).Scan(&lockedGroup); err != nil {
		util.WriteError(w, 409, "relay group unavailable")
		return
	}

	var priorDigest string
	var priorResponse []byte
	var priorActorKind string
	var priorUserID, priorAPIKeyID *int64
	err = tx.QueryRow(r.Context(), `SELECT request_digest,response_bytes,actor_kind,operator_user_id,operator_api_key_id FROM relay_node_quarantine_events WHERE idempotency_key=$1`, key).Scan(&priorDigest, &priorResponse, &priorActorKind, &priorUserID, &priorAPIKeyID)
	if err == nil {
		sameActor := (actorIsSession && priorActorKind == "operator_session" && priorUserID != nil && *priorUserID == p.UserID) ||
			(actorIsAPIKey && priorActorKind == "operator_api_key" && priorAPIKeyID != nil && *priorAPIKeyID == *p.APIKeyID)
		if priorDigest != digest || !sameActor {
			util.WriteError(w, 409, "idempotency key conflicts with prior quarantine")
			return
		}
		if err = tx.Commit(r.Context()); err != nil {
			util.WriteError(w, 500, "commit quarantine replay")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(priorResponse)
		return
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, 500, "load quarantine replay")
		return
	}
	var already bool
	if err = tx.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM relay_node_quarantine_events WHERE node_id=$1)`, nodeID).Scan(&already); err != nil {
		util.WriteError(w, 500, "check quarantine")
		return
	}
	started, parseErr := time.Parse(time.RFC3339Nano, startedRaw)
	if already {
		util.WriteError(w, 409, "relay node is already quarantined")
		return
	}
	if parseErr != nil || accountID != req.ExpectedAccountID || groupID != req.ExpectedRelayGroupID || relayMax != req.ExpectedRelayMaxStreams || status != "active" || relayVersion != req.ExpectedRelayVersion || sourceRevision != req.ExpectedSourceRevision || !started.Equal(expectedStarted) || !heartbeat.Equal(expectedHeartbeat) || heartbeat.Before(txNow.Add(-120*time.Second)) || heartbeat.After(txNow.Add(30*time.Second)) {
		util.WriteError(w, 409, "relay node precondition changed")
		return
	}

	// Claim and canary paths take account -> node -> group locks. Lock their
	// mutable rows in stable order before proving the group can lose this node.
	rows, err := tx.Query(r.Context(), `SELECT r.id FROM recording_canary_reservations r JOIN nodes n ON n.id=r.node_id WHERE n.account_id=$1 AND n.relay_group_id=$2 AND r.expires_at>$3 ORDER BY r.id FOR UPDATE OF r`, accountID, groupID, txNow)
	if err != nil {
		util.WriteError(w, 500, "lock relay reservations")
		return
	}
	reservationCount := 0
	for rows.Next() {
		var id uuid.UUID
		if rows.Scan(&id) != nil {
			rows.Close()
			util.WriteError(w, 500, "scan relay reservations")
			return
		}
		reservationCount++
	}
	rows.Close()
	jobRows, err := tx.Query(r.Context(), `SELECT j.id FROM recording_jobs j JOIN recordings rec ON rec.id=j.recording_id LEFT JOIN nodes owner ON j.lease_owner='node:'||owner.id::text WHERE ((j.status='leased' AND j.lease_expires_at>$3 AND owner.account_id=$1 AND owner.relay_group_id=$2) OR (j.status='pending' AND j.handoff_owner='node:'||$4::bigint::text AND j.handoff_until>$3) OR (j.status='pending' AND j.scheduled_for<=$3+interval '15 minutes' AND rec.account_id=$1 AND rec.status='active' AND rec.capture_via='relay' AND (rec.preferred_relay_group_id IS NULL OR rec.preferred_relay_group_id=$2) AND ((j.kind='continuous_window' AND j.window_end_at>$3) OR j.kind<>'continuous_window'))) ORDER BY j.id FOR UPDATE OF j`, accountID, groupID, txNow, nodeID)
	if err != nil {
		util.WriteError(w, 500, "lock relay jobs")
		return
	}
	for jobRows.Next() {
		var id int64
		if jobRows.Scan(&id) != nil {
			jobRows.Close()
			util.WriteError(w, 500, "scan relay jobs")
			return
		}
	}
	jobRows.Close()
	var live, handoff, required, available int
	err = tx.QueryRow(r.Context(), `WITH live AS (SELECT DISTINCT j.recording_id FROM recording_jobs j JOIN nodes n ON j.lease_owner='node:'||n.id::text WHERE j.status='leased' AND j.lease_expires_at>$3 AND n.account_id=$1 AND n.relay_group_id=$2), reserved AS (SELECT DISTINCT r.recording_id FROM recording_canary_reservations r JOIN nodes n ON n.id=r.node_id WHERE r.expires_at>$3 AND n.account_id=$1 AND n.relay_group_id=$2), imminent AS (SELECT DISTINCT j.recording_id FROM recording_jobs j JOIN recordings rec ON rec.id=j.recording_id WHERE j.status='pending' AND j.scheduled_for<=$3+interval '15 minutes' AND rec.account_id=$1 AND rec.status='active' AND rec.capture_via='relay' AND (rec.preferred_relay_group_id IS NULL OR rec.preferred_relay_group_id=$2) AND ((j.kind='continuous_window' AND j.window_end_at>$3) OR j.kind<>'continuous_window')), demand AS (SELECT recording_id FROM live UNION SELECT recording_id FROM reserved UNION SELECT recording_id FROM imminent) SELECT (SELECT count(*) FROM recording_jobs WHERE status='leased' AND lease_expires_at>$3 AND lease_owner='node:'||$4::bigint::text),(SELECT count(*) FROM recording_jobs WHERE status='pending' AND handoff_owner='node:'||$4::bigint::text AND handoff_until>$3),(SELECT count(*) FROM demand),LEAST((SELECT max_streams FROM relay_groups WHERE id=$2),(SELECT COALESCE(sum(relay_max_streams),0) FROM nodes WHERE account_id=$1 AND relay_group_id=$2 AND node_type='relay' AND status='active' AND id<>$4 AND last_heartbeat_at BETWEEN $3-interval '120 seconds' AND $3+interval '30 seconds'))`, accountID, groupID, txNow, nodeID).Scan(&live, &handoff, &required, &available)
	if err != nil {
		util.WriteError(w, 500, "check relay quarantine capacity")
		return
	}
	if live > 0 || handoff > 0 || reservationCount > 0 || available < required {
		util.WriteError(w, 409, "relay node has work or remaining capacity is insufficient")
		return
	}

	eventID := uuid.New()
	createdAt := txNow.UTC()
	resp := relayNodeQuarantineResponse{true, eventID, key, nodeID, accountID, groupID, "disabled", req.ReasonCode, req.EvidenceSHA256, digest, createdAt}
	responseBytes, err := json.Marshal(resp)
	if err != nil {
		util.WriteError(w, 500, "marshal quarantine response")
		return
	}
	if _, err = tx.Exec(r.Context(), `UPDATE nodes SET status='disabled',updated_at=$2 WHERE id=$1`, nodeID, txNow); err != nil {
		util.WriteError(w, 500, "disable relay node")
		return
	}
	_, err = tx.Exec(r.Context(), `INSERT INTO relay_node_quarantine_events(id,idempotency_key,request_digest,account_id,node_id,relay_group_id,relay_max_streams,display_name,relay_version,relay_source_revision,relay_started_at,heartbeat_at,reason_code,evidence_sha256,actor_kind,operator_user_id,operator_api_key_id,actor_email,actor_key_prefix,response_bytes,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)`, eventID, key, digest, accountID, nodeID, groupID, relayMax, displayName, relayVersion, sourceRevision, expectedStarted, heartbeat, req.ReasonCode, req.EvidenceSHA256, actorKind, operatorUserID, operatorAPIKeyID, p.Email, actorKeyPrefix, responseBytes, txNow)
	if err != nil {
		util.WriteError(w, 500, "persist relay quarantine")
		return
	}
	if err = s.insertAccountAuthEventTx(r.Context(), tx, accountID, p.APIKeyID, "relay_node_quarantined", "operator", p.Email, map[string]any{"event_id": eventID, "node_id": nodeID, "reason_code": req.ReasonCode, "evidence_sha256": req.EvidenceSHA256, "request_digest": digest}); err != nil {
		util.WriteError(w, 500, "audit relay quarantine")
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, 500, "commit relay quarantine")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(responseBytes)
}

func relayNodeIsQuarantined(ctx context.Context, tx pgx.Tx, nodeID int64) (bool, error) {
	var exists bool
	err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM relay_node_quarantine_events WHERE node_id=$1)`, nodeID).Scan(&exists)
	return exists, err
}
