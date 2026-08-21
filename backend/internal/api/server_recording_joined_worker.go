package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/joinedauth"
	"github.com/daydemir/stoarama/backend/internal/util"
)

const (
	joinedWorkerProtocolVersion = 1
	joinedLeaseDuration         = 5 * time.Minute
)

type joinedClaimRequest struct {
	ProtocolVersion int    `json:"protocol_version"`
	BatchID         string `json:"batch_id"`
	WorkerID        string `json:"worker_id"`
}

type joinedClaimSource struct {
	Ordinal            int             `json:"ordinal"`
	ClipID             int64           `json:"clip_id"`
	RecordingJobID     int64           `json:"recording_job_id"`
	StorageDestination int64           `json:"storage_destination_id"`
	Provider           string          `json:"provider"`
	Endpoint           string          `json:"endpoint"`
	Region             string          `json:"region"`
	Bucket             string          `json:"bucket"`
	ObjectKey          string          `json:"object_key"`
	SizeBytes          int64           `json:"size_bytes"`
	SHA256             string          `json:"sha256"`
	ETag               string          `json:"etag"`
	VersionID          string          `json:"version_id"`
	StartUTC           time.Time       `json:"start_utc"`
	EndUTC             time.Time       `json:"end_utc"`
	ReleasedAt         *time.Time      `json:"released_at"`
	AdjacencyFacts     json.RawMessage `json:"adjacency_facts"`
	AllocationFacts    json.RawMessage `json:"allocation_facts"`
}

type joinedClaimItem struct {
	ProtocolVersion     int                 `json:"protocol_version"`
	HourID              string              `json:"hour_id"`
	BatchID             string              `json:"batch_id"`
	RecordingID         int64               `json:"recording_id"`
	LocalDate           string              `json:"local_date"`
	DeliveryHour        int                 `json:"delivery_hour"`
	Timezone            string              `json:"timezone"`
	ScheduledStartUTC   time.Time           `json:"scheduled_start_utc"`
	ScheduledEndUTC     time.Time           `json:"scheduled_end_utc"`
	SourceClaimSHA256   string              `json:"source_claim_sha256"`
	QualificationSHA256 string              `json:"qualification_sha256"`
	QualificationFacts  json.RawMessage     `json:"qualification_facts"`
	ExpiresAt           time.Time           `json:"expires_at"`
	LeaseID             string              `json:"lease_id"`
	OperationToken      string              `json:"operation_token"`
	Sources             []joinedClaimSource `json:"sources"`
}

func validateJoinedWorkerID(workerID string) (string, error) {
	workerID = strings.TrimSpace(workerID)
	if workerID == "" || len(workerID) > 256 {
		return "", errors.New("worker_id is required and must not exceed 256 bytes")
	}
	return workerID, nil
}

func (s *Server) joinedControlPlaneReady() bool {
	return s.cfg.JoinedRecordingEnabled && s.cfg.ValidateJoined() == nil
}

func (s *Server) handleJoinedToken(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ProtocolVersion int    `json:"protocol_version"`
		BatchID         string `json:"batch_id"`
	}
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.ProtocolVersion != joinedWorkerProtocolVersion {
		util.WriteError(w, http.StatusBadRequest, "unsupported joined protocol version")
		return
	}
	batchID := strings.TrimSpace(req.BatchID)
	if batchID == "" || batchID != req.BatchID || len(batchID) > 128 {
		util.WriteError(w, http.StatusBadRequest, "batch_id is required")
		return
	}
	if !s.joinedControlPlaneReady() {
		util.WriteJSON(w, http.StatusOK, map[string]any{"item": nil})
		return
	}
	var availableBatchID string
	err := s.pool.QueryRow(r.Context(), `
		SELECT h.batch_id FROM recording_joined_hours h JOIN connections c ON c.id=h.connection_id AND c.joined_protocol_version=1
		WHERE h.batch_id=$1 AND ((h.state='pending' AND h.next_attempt_at<=now())
		   OR (h.state='leased' AND h.sealed_at IS NULL AND h.lease_expires_at<=now()))
		ORDER BY h.batch_queue_order,h.priority_tier,h.priority_order,h.id LIMIT 1`, batchID).Scan(&availableBatchID)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteJSON(w, http.StatusOK, map[string]any{"item": nil})
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("select joined batch: %v", err))
		return
	}
	expiresAt := time.Now().UTC().Add(10 * time.Minute)
	token, err := joinedauth.MintClaim(s.cfg.JoinedWorkerSigningKey, availableBatchID, expiresAt)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("mint joined claim token: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"item": map[string]any{
		"protocol_version": joinedWorkerProtocolVersion, "batch_id": availableBatchID, "claim_token": token, "expires_at": expiresAt,
	}})
}

func (s *Server) handleJoinedClaim(w http.ResponseWriter, r *http.Request) {
	var req joinedClaimRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.ProtocolVersion != joinedWorkerProtocolVersion {
		util.WriteError(w, http.StatusBadRequest, "unsupported joined protocol version")
		return
	}
	workerID, err := validateJoinedWorkerID(req.WorkerID)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !s.joinedControlPlaneReady() {
		util.WriteJSON(w, http.StatusOK, map[string]any{"item": nil})
		return
	}
	claims, ok := joinedWorkerClaimsFromContext(r.Context())
	if !ok || claims.Kind != joinedauth.KindClaim || req.BatchID != claims.BatchID {
		util.WriteError(w, http.StatusForbidden, "joined claim token scope differs")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin joined claim: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	var hourRecordID int64
	err = tx.QueryRow(r.Context(), `
		SELECT h.id FROM recording_joined_hours h JOIN connections c ON c.id=h.connection_id AND c.joined_protocol_version=1
		WHERE h.batch_id=$1 AND ((h.state='pending' AND h.next_attempt_at<=now())
		   OR (h.state='leased' AND h.sealed_at IS NULL AND h.lease_expires_at<=now()))
		ORDER BY h.priority_tier,h.priority_order,h.next_attempt_at,h.id
		FOR UPDATE OF h,c SKIP LOCKED LIMIT 1`, claims.BatchID).Scan(&hourRecordID)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteJSON(w, http.StatusOK, map[string]any{"item": nil})
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("select joined claim: %v", err))
		return
	}
	claimToken := uuid.New()
	var item joinedClaimItem
	err = tx.QueryRow(r.Context(), `
		UPDATE recording_joined_hours SET state='leased',attempt_count=attempt_count+1,claim_token=$2,claimed_by=$3,
		  lease_expires_at=date_trunc('second',now()+$4::interval),heartbeat_at=now()
		WHERE id=$1 AND batch_id=$5
		  AND EXISTS(SELECT 1 FROM connections c WHERE c.id=recording_joined_hours.connection_id AND c.joined_protocol_version=1)
		RETURNING canonical_hour_id,batch_id,recording_id,local_date::text,delivery_hour,local_timezone,
		  hour_start_at,hour_end_at,source_manifest_sha256,qualification_sha256,qualification_facts,lease_expires_at`,
		hourRecordID, claimToken, workerID, joinedLeaseDuration.String(), claims.BatchID).Scan(&item.HourID, &item.BatchID, &item.RecordingID,
		&item.LocalDate, &item.DeliveryHour, &item.Timezone, &item.ScheduledStartUTC, &item.ScheduledEndUTC,
		&item.SourceClaimSHA256, &item.QualificationSHA256, &item.QualificationFacts, &item.ExpiresAt)
	if err != nil {
		util.WriteError(w, http.StatusConflict, fmt.Sprintf("claim joined hour: %v", err))
		return
	}
	rows, err := tx.Query(r.Context(), `
		SELECT ordinal,clip_id,recording_job_id,storage_destination_id,provider,endpoint,region,bucket,object_key,
		  size_bytes,sha256,etag,version_id,clip_start_at,clip_end_at,released_at,adjacency_facts,allocation_facts
		FROM recording_joined_hour_sources WHERE hour_id=$1 ORDER BY ordinal`, hourRecordID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load joined claim sources: %v", err))
		return
	}
	defer rows.Close()
	item.Sources = []joinedClaimSource{}
	for rows.Next() {
		var source joinedClaimSource
		if err := rows.Scan(&source.Ordinal, &source.ClipID, &source.RecordingJobID, &source.StorageDestination,
			&source.Provider, &source.Endpoint, &source.Region, &source.Bucket, &source.ObjectKey, &source.SizeBytes,
			&source.SHA256, &source.ETag, &source.VersionID, &source.StartUTC, &source.EndUTC, &source.ReleasedAt,
			&source.AdjacencyFacts, &source.AllocationFacts); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan joined claim source: %v", err))
			return
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
		item.HourID, claimToken, joinedauth.OperationPreflight, item.ExpiresAt)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("mint joined job token: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit joined claim: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"item": item})
}

type joinedHeartbeatRequest struct {
	ProtocolVersion int    `json:"protocol_version"`
	ScopeKind       string `json:"scope_kind"`
	ScopeID         string `json:"scope_id"`
}

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
	if req.ProtocolVersion != joinedWorkerProtocolVersion {
		util.WriteError(w, http.StatusBadRequest, "unsupported joined protocol version")
		return
	}
	claims, ok := joinedWorkerClaimsFromContext(r.Context())
	if !ok || claims.Kind != joinedauth.KindOperation || claims.SubjectKind != req.ScopeKind || claims.SubjectID != req.ScopeID {
		util.WriteError(w, http.StatusForbidden, "joined worker token scope differs")
		return
	}
	token, err := joinedCapabilityToken(claims.LeaseToken)
	if err != nil || req.ScopeKind != joinedauth.SubjectHour {
		util.WriteError(w, http.StatusBadRequest, "unsupported joined heartbeat scope")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin joined heartbeat: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	var locked int
	err = tx.QueryRow(r.Context(), `
		SELECT c.id FROM connections c JOIN recording_joined_hours h ON h.connection_id=c.id
		WHERE h.canonical_hour_id=$1 AND h.batch_id=$2 AND c.joined_protocol_version=1
		FOR UPDATE OF c`, req.ScopeID, claims.BatchID).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusConflict, "joined heartbeat lease is stale or foreign")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("lock joined heartbeat: %v", err))
		return
	}
	var leaseExpires time.Time
	err = tx.QueryRow(r.Context(), `
		UPDATE recording_joined_hours SET
		  lease_expires_at=CASE WHEN state='leased' THEN date_trunc('second',now()+$3::interval) ELSE lease_expires_at END,
		  heartbeat_at=CASE WHEN state='leased' THEN now() ELSE heartbeat_at END,
		  publish_lease_expires_at=CASE WHEN state='publishing' THEN date_trunc('second',now()+$3::interval) ELSE publish_lease_expires_at END,
		  publish_heartbeat_at=CASE WHEN state='publishing' THEN now() ELSE publish_heartbeat_at END
		WHERE canonical_hour_id=$1 AND batch_id=$5
		  AND EXISTS(SELECT 1 FROM connections c WHERE c.id=recording_joined_hours.connection_id AND c.joined_protocol_version=1)
		  AND ((state='leased' AND $4='preflight' AND claim_token=$2 AND lease_expires_at>now())
		    OR (state='publishing' AND $4='publish' AND publish_claim_token=$2 AND publish_lease_expires_at>now()))
		RETURNING CASE WHEN state='leased' THEN lease_expires_at ELSE publish_lease_expires_at END`,
		req.ScopeID, token, joinedLeaseDuration.String(), claims.Operation, claims.BatchID).Scan(&leaseExpires)
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
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"protocol_version": joinedWorkerProtocolVersion, "scope_kind": req.ScopeKind, "scope_id": req.ScopeID,
		"lease_id": joinedauth.LeaseID(token), "expires_at": leaseExpires, "operation_token": jobToken,
	})
}

func (s *Server) handleJoinedStatus(w http.ResponseWriter, r *http.Request) {
	counts := map[string]int64{}
	rows, err := s.pool.Query(r.Context(), `SELECT state,count(*) FROM recording_joined_hours GROUP BY state ORDER BY state`)
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
	util.WriteJSON(w, http.StatusOK, map[string]any{"enabled": s.joinedControlPlaneReady(), "hours": counts})
}
