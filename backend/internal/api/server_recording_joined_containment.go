package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/daydemir/stoarama/backend/internal/util"
)

const joinedContainmentArtifactSampleLimit = 64

const joinedContainmentOutsideScopeWhere = `a.batch_id=$1 AND a.batch_record_id=$2
			AND NOT COALESCE((
				(a.artifact_kind='allocation_ledger' AND EXISTS (
					SELECT 1 FROM recording_joined_hours allowed
					WHERE allowed.batch_record_id=a.batch_record_id AND allowed.stream_day_id=a.stream_day_id
					  AND allowed.hour_id=ANY($3::text[])))
				OR (a.artifact_kind IN ('hour_manifest','media') AND h.batch_record_id=a.batch_record_id
					AND h.id=a.hour_record_id AND h.hour_id=ANY($3::text[]))
			), false)`

type joinedContainmentHour struct {
	HourID           string     `json:"hour_id"`
	RecordingID      int64      `json:"recording_id"`
	State            string     `json:"state"`
	AttemptCount     int        `json:"attempt_count"`
	SourceClipCount  int        `json:"source_clip_count"`
	SourceBytes      int64      `json:"source_bytes"`
	LeaseExpiresAt   *time.Time `json:"lease_expires_at,omitempty"`
	LeaseExpired     bool       `json:"lease_expired"`
	ActiveLease      bool       `json:"active_lease"`
	ClaimedByPresent bool       `json:"claimed_by_present"`
	HeartbeatAt      *time.Time `json:"heartbeat_at,omitempty"`
}

type joinedContainmentArtifactState struct {
	ArtifactKind     string `json:"artifact_kind"`
	PublicationState string `json:"publication_state"`
	Count            int64  `json:"count"`
	ActiveLeases     int64  `json:"active_leases"`
}

type joinedContainmentArtifactSample struct {
	ID                    int64      `json:"id"`
	ArtifactKind          string     `json:"artifact_kind"`
	ScopeID               string     `json:"scope_id"`
	HourID                string     `json:"hour_id,omitempty"`
	StreamDayID           *int64     `json:"stream_day_id,omitempty"`
	HourRecordID          *int64     `json:"hour_record_id,omitempty"`
	PublicationState      string     `json:"publication_state"`
	PublicationLeaseUntil *time.Time `json:"publication_lease_expires_at,omitempty"`
	PublishedAt           *time.Time `json:"published_at,omitempty"`
}

type joinedContainmentResponse struct {
	ProtocolVersion                         int                               `json:"protocol_version"`
	BatchID                                 string                            `json:"batch_id"`
	BatchState                              string                            `json:"batch_state"`
	BatchGeneration                         int                               `json:"batch_generation"`
	ConnectionProtocolVersion               int                               `json:"connection_protocol_version"`
	WorkScope                               string                            `json:"work_scope"`
	CanaryHourIDs                           []string                          `json:"canary_hour_ids"`
	DatabaseNow                             time.Time                         `json:"database_now"`
	Hours                                   []joinedContainmentHour           `json:"hours"`
	ArtifactStates                          []joinedContainmentArtifactState  `json:"artifact_states"`
	OutsideScopePublishedOrPublishingCount  int64                             `json:"outside_scope_published_or_publishing_count"`
	OutsideScopeActivePublicationLeaseCount int64                             `json:"outside_scope_active_publication_lease_count"`
	OutsideScopeMediaCount                  int64                             `json:"outside_scope_media_count"`
	OutsideScopePublishedSampleTruncated    bool                              `json:"outside_scope_published_sample_truncated"`
	OutsideScopeSample                      []joinedContainmentArtifactSample `json:"outside_scope_sample"`
	InScopeMediaCount                       int64                             `json:"in_scope_media_count"`
	InScopeMediaSampleTruncated             bool                              `json:"in_scope_media_sample_truncated"`
	InScopeMediaSample                      []joinedContainmentArtifactSample `json:"in_scope_media_sample"`
	UnauthorizedGapOnlyCount                int64                             `json:"unauthorized_gap_only_count"`
	UnauthorizedGapOnlyArtifactIDs          []int64                           `json:"unauthorized_gap_only_artifact_ids"`
	UnauthorizedGapOnlySampleTruncated      bool                              `json:"unauthorized_gap_only_sample_truncated"`
}

// joinedContainmentAllowed reuses the diagnostic's process-local brake. This
// view is a bounded, authenticated read-only database probe and must not be a
// high-rate production query.
func (s *Server) joinedContainmentAllowed(now time.Time) bool {
	s.joinedContainmentMu.Lock()
	defer s.joinedContainmentMu.Unlock()
	if !s.joinedContainmentAt.IsZero() && now.Sub(s.joinedContainmentAt) < 5*time.Second {
		return false
	}
	s.joinedContainmentAt = now
	return true
}

func (s *Server) handleJoinedContainment(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	batchIDs, ok := r.URL.Query()["batch_id"]
	if !ok || len(batchIDs) != 1 || len(r.URL.Query()) != 1 || !joinedBatchIDPattern.MatchString(batchIDs[0]) {
		util.WriteError(w, http.StatusBadRequest, "one canonical joined batch_id is required")
		return
	}
	if batchIDs[0] != s.cfg.JoinedRecordingBatchID {
		util.WriteError(w, http.StatusForbidden, "joined batch scope differs")
		return
	}
	if !s.joinedContainmentAllowed(time.Now().UTC()) {
		util.WriteError(w, http.StatusTooManyRequests, "joined containment is rate limited")
		return
	}
	if s.pool == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "joined containment is unavailable")
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
		util.WriteError(w, http.StatusConflict, "joined containment requires an exact canary scope")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin joined containment read failed")
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SET LOCAL statement_timeout='5s'"); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "set joined containment timeout failed")
		return
	}

	response := joinedContainmentResponse{
		ProtocolVersion:                s.cfg.JoinedRecordingProtocolVersion,
		BatchID:                        batchIDs[0],
		WorkScope:                      workScope,
		CanaryHourIDs:                  append([]string(nil), canaryIDs...),
		Hours:                          make([]joinedContainmentHour, 0, len(canaryIDs)),
		ArtifactStates:                 make([]joinedContainmentArtifactState, 0),
		OutsideScopeSample:             make([]joinedContainmentArtifactSample, 0),
		InScopeMediaSample:             make([]joinedContainmentArtifactSample, 0),
		UnauthorizedGapOnlyArtifactIDs: make([]int64, 0),
	}
	scopeIdentity, err := joinedrecording.NewWorkScopeIdentity(batchIDs[0], workScope, canaryIDs)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "joined containment scope differs")
		return
	}
	scopeSHA, err := scopeIdentity.SHA256(batchIDs[0])
	if err != nil {
		util.WriteError(w, http.StatusConflict, "joined containment scope differs")
		return
	}
	var batchRecordID int64
	if err := tx.QueryRow(ctx, `SELECT b.id,b.state,b.generation,c.joined_protocol_version
		FROM recording_joined_batches b JOIN connections c ON c.id=b.connection_id
		WHERE b.batch_id=$1 AND b.connection_id=$2`,
		batchIDs[0], s.cfg.JoinedRecordingConnectionID).Scan(&batchRecordID, &response.BatchState, &response.BatchGeneration,
		&response.ConnectionProtocolVersion); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusNotFound, "joined batch not found")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, "read joined batch state failed")
		return
	}
	if response.BatchState != "frozen" {
		util.WriteError(w, http.StatusConflict, "joined batch is not frozen")
		return
	}
	if !strings.HasSuffix(response.BatchID, fmt.Sprintf("-generation-%d", response.BatchGeneration)) {
		util.WriteError(w, http.StatusConflict, "joined batch generation identity is invalid")
		return
	}
	if err := tx.QueryRow(ctx, `SELECT now()`).Scan(&response.DatabaseNow); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "read joined database clock failed")
		return
	}
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM recording_joined_artifacts a
		JOIN recording_joined_hours h ON h.id=a.hour_record_id
		WHERE a.batch_record_id=$1 AND a.artifact_kind='hour_manifest' AND h.source_clip_count=0
		  AND NOT EXISTS(SELECT 1 FROM recording_joined_gap_only_scope_authorizations ga
		    WHERE ga.artifact_id=a.id AND ga.batch_record_id=a.batch_record_id AND ga.batch_id=a.batch_id
		      AND ga.hour_record_id=a.hour_record_id AND ga.hour_id=a.scope_id AND ga.work_scope=$3
		      AND ga.work_scope_identity_sha256=$2 AND (($3='frozen_batch' AND ga.authorization_source IN ('server_seal','operator_frozen'))
		        OR ($3 IN ('canary','canary_single') AND ga.authorization_source='server_seal')))`,
		batchRecordID, scopeSHA, workScope).Scan(&response.UnauthorizedGapOnlyCount); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "read unauthorized joined gap-only artifacts failed")
		return
	}
	unauthorizedRows, err := tx.Query(ctx, `SELECT a.id FROM recording_joined_artifacts a
		JOIN recording_joined_hours h ON h.id=a.hour_record_id
		WHERE a.batch_record_id=$1 AND a.artifact_kind='hour_manifest' AND h.source_clip_count=0
		  AND NOT EXISTS(SELECT 1 FROM recording_joined_gap_only_scope_authorizations ga
		    WHERE ga.artifact_id=a.id AND ga.batch_record_id=a.batch_record_id AND ga.batch_id=a.batch_id
		      AND ga.hour_record_id=a.hour_record_id AND ga.hour_id=a.scope_id AND ga.work_scope=$3
		      AND ga.work_scope_identity_sha256=$2 AND (($3='frozen_batch' AND ga.authorization_source IN ('server_seal','operator_frozen'))
		        OR ($3 IN ('canary','canary_single') AND ga.authorization_source='server_seal')))
		ORDER BY a.id LIMIT $4`, batchRecordID, scopeSHA, workScope, joinedContainmentArtifactSampleLimit)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "read unauthorized joined gap-only sample failed")
		return
	}
	for unauthorizedRows.Next() {
		var id int64
		if err := unauthorizedRows.Scan(&id); err != nil {
			unauthorizedRows.Close()
			util.WriteError(w, http.StatusInternalServerError, "scan unauthorized joined gap-only sample failed")
			return
		}
		response.UnauthorizedGapOnlyArtifactIDs = append(response.UnauthorizedGapOnlyArtifactIDs, id)
	}
	if err := unauthorizedRows.Err(); err != nil {
		unauthorizedRows.Close()
		util.WriteError(w, http.StatusInternalServerError, "read unauthorized joined gap-only sample failed")
		return
	}
	unauthorizedRows.Close()
	response.UnauthorizedGapOnlySampleTruncated = response.UnauthorizedGapOnlyCount > int64(len(response.UnauthorizedGapOnlyArtifactIDs))
	rows, err := tx.Query(ctx, `
		SELECT h.hour_id,h.recording_id,h.state,h.attempt_count,h.source_clip_count,h.source_bytes,
		       h.lease_expires_at,COALESCE(h.lease_expires_at<=now(),false),COALESCE(h.lease_expires_at>now(),false),
		       h.claimed_by IS NOT NULL,h.heartbeat_at
		FROM recording_joined_hours h
		JOIN recording_joined_batches b ON b.id=h.batch_record_id AND b.batch_id=$1 AND b.connection_id=$2
		JOIN connections c ON c.id=h.connection_id AND c.id=$2
		WHERE h.batch_id=$1 AND h.batch_record_id=b.id AND h.hour_id=ANY($3::text[])
		ORDER BY array_position($3::text[],h.hour_id)`, batchIDs[0], s.cfg.JoinedRecordingConnectionID, canaryIDs)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "read joined canary hours failed")
		return
	}
	for rows.Next() {
		var hour joinedContainmentHour
		if err := rows.Scan(&hour.HourID, &hour.RecordingID, &hour.State, &hour.AttemptCount, &hour.SourceClipCount,
			&hour.SourceBytes, &hour.LeaseExpiresAt, &hour.LeaseExpired, &hour.ActiveLease,
			&hour.ClaimedByPresent, &hour.HeartbeatAt); err != nil {
			rows.Close()
			util.WriteError(w, http.StatusInternalServerError, "scan joined canary hour failed")
			return
		}
		response.Hours = append(response.Hours, hour)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		util.WriteError(w, http.StatusInternalServerError, "iterate joined canary hours failed")
		return
	}
	rows.Close()
	if len(response.Hours) != len(canaryIDs) {
		util.WriteError(w, http.StatusConflict, "joined canary hour evidence is incomplete")
		return
	}

	stateRows, err := tx.Query(ctx, `
		SELECT artifact_kind,COALESCE(publication_state,''),count(*)::bigint,
		       count(*) FILTER (WHERE publication_state='publishing' AND publication_lease_expires_at>now())::bigint
		FROM recording_joined_artifacts
		WHERE batch_id=$1 AND batch_record_id=(SELECT id FROM recording_joined_batches WHERE batch_id=$1 AND connection_id=$2)
		GROUP BY artifact_kind,publication_state
		ORDER BY artifact_kind,publication_state`, batchIDs[0], s.cfg.JoinedRecordingConnectionID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "read joined artifact states failed")
		return
	}
	for stateRows.Next() {
		var state joinedContainmentArtifactState
		if err := stateRows.Scan(&state.ArtifactKind, &state.PublicationState, &state.Count, &state.ActiveLeases); err != nil {
			stateRows.Close()
			util.WriteError(w, http.StatusInternalServerError, "scan joined artifact states failed")
			return
		}
		response.ArtifactStates = append(response.ArtifactStates, state)
	}
	if err := stateRows.Err(); err != nil {
		stateRows.Close()
		util.WriteError(w, http.StatusInternalServerError, "iterate joined artifact states failed")
		return
	}
	stateRows.Close()

	if err := tx.QueryRow(ctx, `WITH outside_scope AS (
			SELECT a.* FROM recording_joined_artifacts a
			LEFT JOIN recording_joined_hours h ON h.id=a.hour_record_id
			WHERE `+joinedContainmentOutsideScopeWhere+`
		)
		SELECT count(*) FILTER (WHERE artifact_kind<>'media' AND publication_state IN ('published','publishing'))::bigint,
		       count(*) FILTER (WHERE artifact_kind<>'media' AND publication_state='publishing' AND publication_lease_expires_at>now())::bigint,
		       count(*) FILTER (WHERE artifact_kind='media')::bigint
		FROM outside_scope`, batchIDs[0], batchRecordID, canaryIDs).
		Scan(&response.OutsideScopePublishedOrPublishingCount, &response.OutsideScopeActivePublicationLeaseCount,
			&response.OutsideScopeMediaCount); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "read joined outside-scope artifact counts failed")
		return
	}

	sampleRows, err := tx.Query(ctx, `
		SELECT a.id,a.artifact_kind,a.scope_id,COALESCE(h.hour_id,''),a.stream_day_id,a.hour_record_id,COALESCE(a.publication_state,''),
		       a.publication_lease_expires_at,a.published_at
		FROM recording_joined_artifacts a
		LEFT JOIN recording_joined_hours h ON h.id=a.hour_record_id
		WHERE `+joinedContainmentOutsideScopeWhere+`
		  AND a.artifact_kind<>'media' AND a.publication_state IN ('published','publishing')
		ORDER BY a.id
		LIMIT $4`, batchIDs[0], batchRecordID, canaryIDs, joinedContainmentArtifactSampleLimit)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "read joined outside-scope artifact sample failed")
		return
	}
	for sampleRows.Next() {
		var sample joinedContainmentArtifactSample
		if err := sampleRows.Scan(&sample.ID, &sample.ArtifactKind, &sample.ScopeID, &sample.HourID, &sample.StreamDayID,
			&sample.HourRecordID, &sample.PublicationState,
			&sample.PublicationLeaseUntil, &sample.PublishedAt); err != nil {
			sampleRows.Close()
			util.WriteError(w, http.StatusInternalServerError, "scan joined outside-scope artifact sample failed")
			return
		}
		response.OutsideScopeSample = append(response.OutsideScopeSample, sample)
	}
	if err := sampleRows.Err(); err != nil {
		sampleRows.Close()
		util.WriteError(w, http.StatusInternalServerError, "iterate joined outside-scope artifact sample failed")
		return
	}
	sampleRows.Close()
	response.OutsideScopePublishedSampleTruncated = response.OutsideScopePublishedOrPublishingCount > int64(len(response.OutsideScopeSample))

	mediaRows, err := tx.Query(ctx, `
		SELECT a.id,a.artifact_kind,a.scope_id,COALESCE(h.hour_id,''),a.stream_day_id,a.hour_record_id,
		       COALESCE(a.publication_state,''),a.publication_lease_expires_at,a.published_at,
		       count(*) OVER ()
		FROM recording_joined_artifacts a
		JOIN recording_joined_hours h ON h.id=a.hour_record_id
		WHERE a.batch_id=$1 AND a.batch_record_id=$2 AND a.artifact_kind='media'
		  AND h.hour_id=ANY($3::text[])
		ORDER BY a.id
		LIMIT $4`, batchIDs[0], batchRecordID, canaryIDs, joinedContainmentArtifactSampleLimit)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "read joined in-scope media sample failed")
		return
	}
	var mediaTotal int64
	for mediaRows.Next() {
		var sample joinedContainmentArtifactSample
		if err := mediaRows.Scan(&sample.ID, &sample.ArtifactKind, &sample.ScopeID, &sample.HourID,
			&sample.StreamDayID, &sample.HourRecordID, &sample.PublicationState,
			&sample.PublicationLeaseUntil, &sample.PublishedAt, &mediaTotal); err != nil {
			mediaRows.Close()
			util.WriteError(w, http.StatusInternalServerError, "scan joined in-scope media sample failed")
			return
		}
		response.InScopeMediaSample = append(response.InScopeMediaSample, sample)
	}
	if err := mediaRows.Err(); err != nil {
		mediaRows.Close()
		util.WriteError(w, http.StatusInternalServerError, "iterate joined in-scope media sample failed")
		return
	}
	mediaRows.Close()
	response.InScopeMediaCount = mediaTotal
	response.InScopeMediaSampleTruncated = mediaTotal > int64(len(response.InScopeMediaSample))
	if err := tx.Commit(ctx); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "finish joined containment read failed")
		return
	}
	util.WriteJSON(w, http.StatusOK, response)
}
