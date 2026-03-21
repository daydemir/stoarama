package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/util"
)

type youTubeRelaySourceHeartbeatRequest struct {
	ServerID     string         `json:"server_id"`
	ShardID      string         `json:"shard_id"`
	MaxActive    int            `json:"max_active"`
	Draining     bool           `json:"draining"`
	LeaseSec     int            `json:"lease_sec"`
	MetadataJSON map[string]any `json:"metadata_json"`
}

type youTubeRelaySourceStoppedRequest struct {
	ServerID string `json:"server_id"`
}

type youTubeRelayRouteStatusRequest struct {
	Actor        string         `json:"actor"`
	Status       string         `json:"status"`
	Reason       string         `json:"reason"`
	RelayPullURL string         `json:"relay_pull_url"`
	ErrorText    string         `json:"error_text"`
	MetadataJSON map[string]any `json:"metadata_json"`
}

func youTubeRelaySourceServerIDForNode(nodeID int64) string {
	return fmt.Sprintf("node-%d-yt-relay-source", nodeID)
}

func (s *Server) handleYouTubeRelaySourceHeartbeat(w http.ResponseWriter, r *http.Request) {
	var req youTubeRelaySourceHeartbeatRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	serverID := strings.TrimSpace(req.ServerID)
	if serverID == "" {
		util.WriteError(w, http.StatusBadRequest, "server_id is required")
		return
	}
	shardID := strings.TrimSpace(req.ShardID)
	if shardID == "" {
		util.WriteError(w, http.StatusBadRequest, "shard_id is required")
		return
	}
	if req.MaxActive <= 0 {
		util.WriteError(w, http.StatusBadRequest, "max_active must be > 0")
		return
	}
	leaseSec := req.LeaseSec
	if leaseSec <= 0 {
		leaseSec = 45
	}
	if leaseSec > 3600 {
		util.WriteError(w, http.StatusBadRequest, "lease_sec must be <= 3600")
		return
	}
	metaBytes, err := json.Marshal(nonNilMap(req.MetadataJSON))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid metadata_json: %v", err))
		return
	}
	if err := s.upsertYouTubeRelaySource(r.Context(), 0, serverID, shardID, req.MaxActive, req.Draining, leaseSec, string(metaBytes)); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("upsert youtube relay source: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"ok":         true,
		"server_id":  serverID,
		"shard_id":   shardID,
		"max_active": req.MaxActive,
		"draining":   req.Draining,
	})
}

func (s *Server) handleNodeYouTubeRelaySourceHeartbeat(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if principal.NodeType != nodeTypeYTRelaySource {
		util.WriteError(w, http.StatusForbidden, "yt relay source node required")
		return
	}
	var req youTubeRelaySourceHeartbeatRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	shardID := strings.TrimSpace(req.ShardID)
	if shardID == "" {
		util.WriteError(w, http.StatusBadRequest, "shard_id is required")
		return
	}
	if req.MaxActive <= 0 {
		util.WriteError(w, http.StatusBadRequest, "max_active must be > 0")
		return
	}
	leaseSec := req.LeaseSec
	if leaseSec <= 0 {
		leaseSec = 45
	}
	if leaseSec > 3600 {
		util.WriteError(w, http.StatusBadRequest, "lease_sec must be <= 3600")
		return
	}
	meta := nonNilMap(req.MetadataJSON)
	meta["node_id"] = principal.NodeID
	meta["node_display_name"] = principal.DisplayName
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid metadata_json: %v", err))
		return
	}
	serverID := youTubeRelaySourceServerIDForNode(principal.NodeID)
	if err := s.upsertYouTubeRelaySource(r.Context(), principal.NodeID, serverID, shardID, req.MaxActive, req.Draining, leaseSec, string(metaBytes)); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("upsert youtube relay source: %v", err))
		return
	}
	if _, err := s.pool.Exec(r.Context(), `
		UPDATE nodes
		SET
			last_heartbeat_at=now(),
			capabilities_jsonb=COALESCE(capabilities_jsonb, '{}'::jsonb) || jsonb_build_object(
				'yt_relay_source', true,
				'yt_relay_max_active', $2::int,
				'yt_relay_shard_id', $3::text
			),
			metadata_jsonb=COALESCE(metadata_jsonb, '{}'::jsonb) || $4::jsonb,
			updated_at=now()
		WHERE id=$1
	`, principal.NodeID, req.MaxActive, shardID, string(metaBytes)); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("update node relay heartbeat: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"ok":         true,
		"server_id":  serverID,
		"shard_id":   shardID,
		"max_active": req.MaxActive,
		"draining":   req.Draining,
	})
}

func (s *Server) upsertYouTubeRelaySource(ctx context.Context, nodeID int64, serverID string, shardID string, maxActive int, draining bool, leaseSec int, metadataJSON string) error {
	var nodeIDArg any
	if nodeID > 0 {
		nodeIDArg = nodeID
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO youtube_relay_sources (
			server_id, node_id, shard_id, max_active, draining, heartbeat_at, lease_expires_at, metadata_jsonb, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, now(), now() + make_interval(secs => $6), $7::jsonb, now())
		ON CONFLICT (server_id)
		DO UPDATE SET
			node_id=EXCLUDED.node_id,
			shard_id=EXCLUDED.shard_id,
			max_active=EXCLUDED.max_active,
			draining=EXCLUDED.draining,
			heartbeat_at=EXCLUDED.heartbeat_at,
			lease_expires_at=EXCLUDED.lease_expires_at,
			metadata_jsonb=EXCLUDED.metadata_jsonb,
			updated_at=now()
	`, serverID, nodeIDArg, shardID, maxActive, draining, leaseSec, metadataJSON)
	return err
}

func (s *Server) handleYouTubeRelaySourceStopped(w http.ResponseWriter, r *http.Request) {
	var req youTubeRelaySourceStoppedRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	serverID := strings.TrimSpace(req.ServerID)
	if serverID == "" {
		util.WriteError(w, http.StatusBadRequest, "server_id is required")
		return
	}
	if err := s.markYouTubeRelaySourceStopped(r.Context(), serverID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true, "server_id": serverID})
}

func (s *Server) handleNodeYouTubeRelaySourceStopped(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if principal.NodeType != nodeTypeYTRelaySource {
		util.WriteError(w, http.StatusForbidden, "yt relay source node required")
		return
	}
	serverID := youTubeRelaySourceServerIDForNode(principal.NodeID)
	if err := s.markYouTubeRelaySourceStopped(r.Context(), serverID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true, "server_id": serverID})
}

func (s *Server) markYouTubeRelaySourceStopped(ctx context.Context, serverID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin youtube relay source stop tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
		WITH failed AS (
			UPDATE youtube_relay_routes
			SET
				status='failed',
				error_text='source server stopped',
				stopped_at=COALESCE(stopped_at, now()),
				updated_at=now()
			WHERE source_server_id=$1
			  AND status IN ('assigned', 'source_ready', 'running')
			RETURNING stream_id, source_server_id, sink_server_id
		)
		INSERT INTO youtube_relay_events (
			stream_id, source_server_id, sink_server_id, status, actor, reason, error_text, metadata_jsonb
		)
		SELECT stream_id, source_server_id, sink_server_id, 'failed', 'api.youtube_relay_source_stopped', 'source server stopped', 'source server stopped', '{}'::jsonb
		FROM failed
	`, serverID); err != nil {
		return fmt.Errorf("mark relay routes failed for source stop: %v", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM youtube_relay_sources WHERE server_id=$1`, serverID); err != nil {
		return fmt.Errorf("delete youtube relay source: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit youtube relay source stop tx: %v", err)
	}
	return nil
}

func (s *Server) handleYouTubeRelayRoutesList(w http.ResponseWriter, r *http.Request) {
	sourceServerID := strings.TrimSpace(r.URL.Query().Get("source_server_id"))
	sinkServerID := strings.TrimSpace(r.URL.Query().Get("sink_server_id"))
	status := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("status")))
	limit := parseIntQuery(r, "limit", 500, 1, 2000)
	offset := parseIntQuery(r, "offset", 0, 0, 1000000)
	if status != "" && !validYouTubeRelayRouteStatus(status) {
		util.WriteError(w, http.StatusBadRequest, "status must be one of assigned|source_ready|running|stopped|failed")
		return
	}
	where := []string{"1=1"}
	args := make([]any, 0, 6)
	if sourceServerID != "" {
		args = append(args, sourceServerID)
		where = append(where, fmt.Sprintf("r.source_server_id=$%d", len(args)))
	}
	if sinkServerID != "" {
		args = append(args, sinkServerID)
		where = append(where, fmt.Sprintf("r.sink_server_id=$%d", len(args)))
	}
	if status != "" {
		args = append(args, status)
		where = append(where, fmt.Sprintf("r.status=$%d", len(args)))
	}
	args = append(args, limit, offset)
	rows, err := s.pool.Query(r.Context(), fmt.Sprintf(`
		SELECT
			r.stream_id,
			r.source_server_id,
			r.sink_server_id,
			r.assignment_revision,
			r.status,
			r.relay_pull_url,
			r.error_text,
			s.source_url,
			s.source_page_url,
			r.metadata_jsonb,
			r.created_at,
			r.updated_at
		FROM youtube_relay_routes r
		JOIN streams s ON s.id=r.stream_id
		WHERE %s
		ORDER BY r.updated_at DESC, r.stream_id ASC
		LIMIT $%d OFFSET $%d
	`, strings.Join(where, " AND "), len(args)-1, len(args)), args...)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query youtube relay routes: %v", err))
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0, limit)
	for rows.Next() {
		var streamID int64
		var sourceID, sinkID, routeStatus, relayPullURL, errorText, streamURL, sourcePageURL string
		var assignmentRevision int64
		var metadataBytes []byte
		var createdAt, updatedAt time.Time
		if err := rows.Scan(
			&streamID, &sourceID, &sinkID, &assignmentRevision,
			&routeStatus, &relayPullURL, &errorText, &streamURL, &sourcePageURL,
			&metadataBytes, &createdAt, &updatedAt,
		); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan youtube relay route: %v", err))
			return
		}
		meta := map[string]any{}
		if len(metadataBytes) > 0 {
			if err := json.Unmarshal(metadataBytes, &meta); err != nil {
				util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("decode youtube relay route metadata: %v", err))
				return
			}
		}
		items = append(items, map[string]any{
			"stream_id":           streamID,
			"source_server_id":    sourceID,
			"sink_server_id":      sinkID,
			"assignment_revision": assignmentRevision,
			"status":              routeStatus,
			"relay_pull_url":      relayPullURL,
			"error_text":          errorText,
			"source_url":          streamURL,
			"source_page_url":     sourcePageURL,
			"metadata_json":       meta,
			"created_at":          createdAt.UTC(),
			"updated_at":          updatedAt.UTC(),
		})
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate youtube relay routes: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"items":  items,
		"limit":  limit,
		"offset": offset,
		"total":  len(items),
	})
}

func (s *Server) handleNodeYouTubeRelayRoutesList(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if principal.NodeType != nodeTypeYTRelaySource {
		util.WriteError(w, http.StatusForbidden, "yt relay source node required")
		return
	}
	q := r.URL.Query()
	q.Set("source_server_id", youTubeRelaySourceServerIDForNode(principal.NodeID))
	r.URL.RawQuery = q.Encode()
	s.handleYouTubeRelayRoutesList(w, r)
}

func (s *Server) handleYouTubeRelayRouteStatus(w http.ResponseWriter, r *http.Request) {
	streamID, ok := parseInt64Path(w, r, "stream_id")
	if !ok {
		return
	}
	var req youTubeRelayRouteStatusRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	actor := strings.TrimSpace(req.Actor)
	if actor == "" {
		util.WriteError(w, http.StatusBadRequest, "actor is required")
		return
	}
	status := strings.TrimSpace(strings.ToLower(req.Status))
	if !validYouTubeRelayRouteStatus(status) {
		util.WriteError(w, http.StatusBadRequest, "status must be one of assigned|source_ready|running|stopped|failed")
		return
	}
	metaBytes, err := json.Marshal(nonNilMap(req.MetadataJSON))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid metadata_json: %v", err))
		return
	}
	reason := strings.TrimSpace(req.Reason)
	errText := strings.TrimSpace(req.ErrorText)
	relayPullURL := strings.TrimSpace(req.RelayPullURL)

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin youtube relay route status tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	var sourceServerID, sinkServerID, currentPullURL string
	if err := tx.QueryRow(r.Context(), `
		SELECT source_server_id, sink_server_id, relay_pull_url
		FROM youtube_relay_routes
		WHERE stream_id=$1
		FOR UPDATE
	`, streamID).Scan(&sourceServerID, &sinkServerID, &currentPullURL); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusNotFound, "youtube relay route not found")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load youtube relay route: %v", err))
		return
	}
	if relayPullURL == "" {
		relayPullURL = strings.TrimSpace(currentPullURL)
	}
	if (status == youtubeRelayRouteStatusSourceReady || status == youtubeRelayRouteStatusRunning) && relayPullURL == "" {
		util.WriteError(w, http.StatusBadRequest, "relay_pull_url is required for status source_ready|running")
		return
	}

	if _, err := tx.Exec(r.Context(), `
		UPDATE youtube_relay_routes
		SET
			status=$2,
			relay_pull_url=$3,
			error_text=$4,
			metadata_jsonb=$5::jsonb,
			started_at=CASE
				WHEN $2='running' THEN COALESCE(started_at, now())
				WHEN $2 IN ('stopped', 'failed') THEN started_at
				ELSE NULL
			END,
			stopped_at=CASE
				WHEN $2 IN ('stopped', 'failed') THEN COALESCE(stopped_at, now())
				ELSE NULL
			END,
			updated_at=now()
		WHERE stream_id=$1
	`, streamID, status, relayPullURL, errText, string(metaBytes)); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("update youtube relay route status: %v", err))
		return
	}
	if _, err := tx.Exec(r.Context(), `
		INSERT INTO youtube_relay_events (
			stream_id, source_server_id, sink_server_id, status, actor, reason, error_text, metadata_jsonb
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
	`, streamID, sourceServerID, sinkServerID, status, actor, reason, errText, string(metaBytes)); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("insert youtube relay event: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit youtube relay route status tx: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"ok":               true,
		"stream_id":        streamID,
		"status":           status,
		"source_server_id": sourceServerID,
		"sink_server_id":   sinkServerID,
		"relay_pull_url":   relayPullURL,
	})
}

func (s *Server) handleNodeYouTubeRelayRouteStatus(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if principal.NodeType != nodeTypeYTRelaySource {
		util.WriteError(w, http.StatusForbidden, "yt relay source node required")
		return
	}
	streamID, ok := parseInt64Path(w, r, "stream_id")
	if !ok {
		return
	}
	expectedSourceServerID := youTubeRelaySourceServerIDForNode(principal.NodeID)
	var sourceServerID string
	if err := s.pool.QueryRow(r.Context(), `
		SELECT source_server_id
		FROM youtube_relay_routes
		WHERE stream_id=$1
	`, streamID).Scan(&sourceServerID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusNotFound, "youtube relay route not found")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load youtube relay route: %v", err))
		return
	}
	if strings.TrimSpace(sourceServerID) != expectedSourceServerID {
		util.WriteError(w, http.StatusNotFound, "youtube relay route not found")
		return
	}
	s.handleYouTubeRelayRouteStatus(w, r)
}
