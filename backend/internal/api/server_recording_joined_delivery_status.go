package api

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/util"
)

const joinedDeliveryStatusInterval = time.Second

type joinedDeliveryStatusResponse struct {
	BatchID            string     `json:"batch_id"`
	ArtifactID         int64      `json:"artifact_id"`
	ArtifactKind       string     `json:"artifact_kind"`
	HourID             string     `json:"hour_id"`
	RelativePath       string     `json:"relative_path"`
	ExpectedSizeBytes  int64      `json:"expected_size_bytes"`
	ExpectedSHA256     string     `json:"expected_sha256"`
	PublicationState   string     `json:"publication_state"`
	PublishedAt        *time.Time `json:"published_at,omitempty"`
	Acknowledged       bool       `json:"acknowledged"`
	VerifiedAt         *time.Time `json:"verified_at,omitempty"`
	AcknowledgedPath   string     `json:"acknowledged_relative_path,omitempty"`
	AcknowledgedSize   *int64     `json:"acknowledged_size_bytes,omitempty"`
	AcknowledgedSHA256 string     `json:"acknowledged_sha256,omitempty"`
	IdentityMatches    bool       `json:"identity_matches"`
	ConnectionID       int64      `json:"connection_id"`
	ConnectionProtocol int        `json:"connection_protocol_version"`
}

func (s *Server) joinedDeliveryStatusAllowed(now time.Time) bool {
	s.joinedDeliveryStatusMu.Lock()
	defer s.joinedDeliveryStatusMu.Unlock()
	if !s.joinedDeliveryStatusAt.IsZero() && now.Sub(s.joinedDeliveryStatusAt) < joinedDeliveryStatusInterval {
		return false
	}
	s.joinedDeliveryStatusAt = now
	return true
}

// handleJoinedDeliveryStatus exposes the append-only NAS acknowledgement for
// one exact artifact in the configured canary scope. It never contacts the NAS
// and cannot create, retry, or acknowledge a delivery.
func (s *Server) handleJoinedDeliveryStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	query := r.URL.Query()
	batchValues, batchOK := query["batch_id"]
	artifactValues, artifactOK := query["artifact_id"]
	if !batchOK || !artifactOK || len(query) != 2 || len(batchValues) != 1 || len(artifactValues) != 1 ||
		!joinedBatchIDPattern.MatchString(batchValues[0]) {
		util.WriteError(w, http.StatusBadRequest, "one canonical batch_id and artifact_id are required")
		return
	}
	artifactID, err := strconv.ParseInt(artifactValues[0], 10, 64)
	if err != nil || artifactID <= 0 || strconv.FormatInt(artifactID, 10) != artifactValues[0] {
		util.WriteError(w, http.StatusBadRequest, "one canonical batch_id and artifact_id are required")
		return
	}
	if batchValues[0] != s.cfg.JoinedRecordingBatchID {
		util.WriteError(w, http.StatusForbidden, "joined batch scope differs")
		return
	}
	if !s.joinedDeliveryStatusAllowed(time.Now().UTC()) {
		util.WriteError(w, http.StatusTooManyRequests, "joined delivery status is rate limited")
		return
	}
	if s.pool == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "joined delivery status is unavailable")
		return
	}
	if !s.joinedControlPlaneReady() {
		util.WriteError(w, http.StatusConflict, "joined control plane is not ready")
		return
	}
	workScope, err := s.cfg.JoinedWorkScope()
	if err != nil {
		util.WriteError(w, http.StatusServiceUnavailable, "joined work scope is unavailable")
		return
	}
	canaryIDs, err := s.cfg.JoinedCanaryHourIDs()
	if err != nil || !config.IsJoinedCanaryWorkScope(workScope) {
		util.WriteError(w, http.StatusConflict, "joined delivery status requires an exact canary scope")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin joined delivery status read failed")
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SET LOCAL statement_timeout='5s'"); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "set joined delivery status timeout failed")
		return
	}

	response := joinedDeliveryStatusResponse{BatchID: batchValues[0], ArtifactID: artifactID}
	var acknowledgedPath, acknowledgedSHA *string
	var acknowledgedSize *int64
	err = tx.QueryRow(ctx, `
		SELECT a.artifact_kind,h.hour_id,a.relative_path,a.expected_size_bytes,a.expected_sha256,
		       CASE WHEN a.artifact_kind='media' AND a.published_at IS NOT NULL THEN 'published'
		            ELSE COALESCE(a.publication_state,'') END,a.published_at,ack.verified_at,
		       ack.relative_path,ack.size_bytes,ack.sha256,a.connection_id,c.joined_protocol_version
		FROM recording_joined_artifacts a
		JOIN recording_joined_batches b ON b.id=a.batch_record_id AND b.batch_id=$1 AND b.connection_id=$2
		JOIN recording_joined_hours h ON h.id=a.hour_record_id AND h.batch_record_id=b.id
		JOIN connections c ON c.id=a.connection_id AND c.id=$2
		LEFT JOIN recording_joined_artifact_acks ack ON ack.artifact_id=a.id AND ack.connection_id=a.connection_id
		WHERE a.id=$3 AND a.batch_id=$1 AND a.batch_record_id=b.id
		  AND a.artifact_kind IN ('hour_manifest','media') AND h.hour_id=ANY($4::text[])`,
		batchValues[0], s.cfg.JoinedRecordingConnectionID, artifactID, canaryIDs).Scan(
		&response.ArtifactKind, &response.HourID, &response.RelativePath, &response.ExpectedSizeBytes,
		&response.ExpectedSHA256, &response.PublicationState, &response.PublishedAt, &response.VerifiedAt,
		&acknowledgedPath, &acknowledgedSize, &acknowledgedSHA, &response.ConnectionID, &response.ConnectionProtocol)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "joined artifact not found in configured scope")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "read joined delivery status failed")
		return
	}
	response.Acknowledged = response.VerifiedAt != nil
	response.AcknowledgedSize = acknowledgedSize
	if acknowledgedPath != nil {
		response.AcknowledgedPath = *acknowledgedPath
	}
	if acknowledgedSHA != nil {
		response.AcknowledgedSHA256 = *acknowledgedSHA
	}
	response.IdentityMatches = response.Acknowledged && acknowledgedPath != nil && acknowledgedSize != nil && acknowledgedSHA != nil &&
		*acknowledgedPath == response.RelativePath && *acknowledgedSize == response.ExpectedSizeBytes && *acknowledgedSHA == response.ExpectedSHA256
	if !validNASRelativePath(response.RelativePath) || len(response.ExpectedSHA256) != 64 || !lowerHex(response.ExpectedSHA256) ||
		(response.Acknowledged && (!validNASRelativePath(response.AcknowledgedPath) || len(response.AcknowledgedSHA256) != 64 ||
			!lowerHex(response.AcknowledgedSHA256))) {
		util.WriteError(w, http.StatusConflict, "joined delivery identity is invalid")
		return
	}
	if err := tx.Commit(ctx); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "finish joined delivery status read failed")
		return
	}
	util.WriteJSON(w, http.StatusOK, response)
}
