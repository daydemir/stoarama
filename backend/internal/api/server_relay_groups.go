package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/daydemir/stoarama/backend/internal/util"
)

const (
	relayGroupDefaultMaxStreams = 8
	relayGroupMinMaxStreams     = 1
	relayGroupMaxMaxStreams     = 200
)

func lockRelayNodeRow(ctx context.Context, tx pgx.Tx, nodeID, accountID int64) (*int64, error) {
	var groupID *int64
	if err := tx.QueryRow(ctx, `
		SELECT n.relay_group_id
		FROM nodes n
		WHERE n.id=$1 AND n.account_id=$2 AND n.node_type='relay'
		FOR UPDATE
	`, nodeID, accountID).Scan(&groupID); err != nil {
		return nil, err
	}
	return groupID, nil
}

func lockRelayNode(ctx context.Context, tx pgx.Tx, nodeID, accountID int64) (*int64, int, error) {
	groupID, err := lockRelayNodeRow(ctx, tx, nodeID, accountID)
	if err != nil {
		return nil, 0, err
	}
	var liveLeases int
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*) FROM recording_jobs
		WHERE lease_owner=$1
		  AND status='leased' AND lease_expires_at > now()
	`, fmt.Sprintf("node:%d", nodeID)).Scan(&liveLeases); err != nil {
		return nil, 0, err
	}
	return groupID, liveLeases, nil
}

func relayGroupChangeAllowed(current, target *int64, liveLeases int) bool {
	return liveLeases == 0 || current == nil || (target != nil && *current == *target)
}

const relayGroupSelectSQL = `
	SELECT g.id, g.name, g.max_streams,
	       COALESCE((
	         SELECT jsonb_agg(n.id ORDER BY n.id)
	         FROM nodes n
	         WHERE n.account_id=g.account_id AND n.node_type='relay' AND n.relay_group_id=g.id
	       ), '[]'::jsonb) AS node_ids,
	       (SELECT COUNT(*)
	        FROM recording_jobs j
	        JOIN nodes n ON j.lease_owner='node:'||n.id::text
	        WHERE n.account_id=g.account_id AND n.relay_group_id=g.id
	          AND j.status='leased' AND j.lease_expires_at > now())::int AS live_leases
	FROM relay_groups g
`

type relayGroupCreateRequest struct {
	Name       string `json:"name"`
	MaxStreams *int   `json:"max_streams"`
}

type relayGroupPatchRequest struct {
	Name       *string `json:"name"`
	MaxStreams *int    `json:"max_streams"`
}

func scanRelayGroup(row pgx.Row) (map[string]any, error) {
	var (
		id, liveLeases int64
		name           string
		maxStreams     int
		nodeIDsRaw     json.RawMessage
	)
	if err := row.Scan(&id, &name, &maxStreams, &nodeIDsRaw, &liveLeases); err != nil {
		return nil, err
	}
	var nodeIDs []int64
	if err := json.Unmarshal(nodeIDsRaw, &nodeIDs); err != nil {
		return nil, err
	}
	return map[string]any{
		"id": id, "name": name, "max_streams": maxStreams,
		"node_ids": nodeIDs, "live_leases": liveLeases,
	}, nil
}

func validateRelayGroupMaxStreams(max int) error {
	if max < relayGroupMinMaxStreams || max > relayGroupMaxMaxStreams {
		return fmt.Errorf("max_streams must be between %d and %d", relayGroupMinMaxStreams, relayGroupMaxMaxStreams)
	}
	return nil
}

func relayGroupConflict(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func (s *Server) handleAccountRelayGroupsList(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	rows, err := s.pool.Query(r.Context(), relayGroupSelectSQL+` WHERE g.account_id=$1 ORDER BY lower(g.name), g.id`, principal.AccountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list relay groups: %v", err))
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0, 4)
	for rows.Next() {
		item, err := scanRelayGroup(rows)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan relay group: %v", err))
			return
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate relay groups: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) handleAccountRelayGroupsCreate(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var req relayGroupCreateRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		util.WriteError(w, http.StatusBadRequest, "name is required")
		return
	}
	maxStreams := relayGroupDefaultMaxStreams
	if req.MaxStreams != nil {
		maxStreams = *req.MaxStreams
	}
	if err := validateRelayGroupMaxStreams(maxStreams); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	var id int64
	err := s.pool.QueryRow(r.Context(), `
		INSERT INTO relay_groups (account_id, name, max_streams)
		VALUES ($1, $2, $3)
		RETURNING id
	`, principal.AccountID, name, maxStreams).Scan(&id)
	if relayGroupConflict(err) {
		util.WriteError(w, http.StatusConflict, "relay group name already exists")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("create relay group: %v", err))
		return
	}
	group, err := scanRelayGroup(s.pool.QueryRow(r.Context(), relayGroupSelectSQL+` WHERE g.account_id=$1 AND g.id=$2`, principal.AccountID, id))
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load relay group: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusCreated, map[string]any{"group": group})
}

func (s *Server) handleAccountRelayGroupPatch(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req relayGroupPatchRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Name == nil && req.MaxStreams == nil {
		util.WriteError(w, http.StatusBadRequest, "name or max_streams is required")
		return
	}
	var name any
	if req.Name != nil {
		trimmed := strings.TrimSpace(*req.Name)
		if trimmed == "" {
			util.WriteError(w, http.StatusBadRequest, "name must not be empty")
			return
		}
		name = trimmed
	}
	var maxStreams any
	if req.MaxStreams != nil {
		if err := validateRelayGroupMaxStreams(*req.MaxStreams); err != nil {
			util.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		maxStreams = *req.MaxStreams
	}
	ct, err := s.pool.Exec(r.Context(), `
		UPDATE relay_groups
		SET name=COALESCE($3, name), max_streams=COALESCE($4, max_streams)
		WHERE id=$1 AND account_id=$2
	`, id, principal.AccountID, name, maxStreams)
	if relayGroupConflict(err) {
		util.WriteError(w, http.StatusConflict, "relay group name already exists")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("update relay group: %v", err))
		return
	}
	if ct.RowsAffected() == 0 {
		util.WriteError(w, http.StatusNotFound, "relay group not found")
		return
	}
	group, err := scanRelayGroup(s.pool.QueryRow(r.Context(), relayGroupSelectSQL+` WHERE g.account_id=$1 AND g.id=$2`, principal.AccountID, id))
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load relay group: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"group": group})
}

func (s *Server) handleAccountNodeRelayGroupPut(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	nodeID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	groupID, ok := parseInt64Path(w, r, "group_id")
	if !ok {
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin relay group assignment: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	currentGroupID, liveLeases, err := lockRelayNode(r.Context(), tx, nodeID, principal.AccountID)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "relay node not found")
		return
	} else if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("lock relay node: %v", err))
		return
	}
	if !relayGroupChangeAllowed(currentGroupID, &groupID, liveLeases) {
		util.WriteError(w, http.StatusConflict, "wait for this computer's active recordings to finish")
		return
	}
	var lockedGroupID int64
	if err := tx.QueryRow(r.Context(), `
		SELECT id FROM relay_groups WHERE id=$1 AND account_id=$2 FOR UPDATE
	`, groupID, principal.AccountID).Scan(&lockedGroupID); errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "relay group not found")
		return
	} else if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("lock relay group: %v", err))
		return
	}
	if _, err := tx.Exec(r.Context(), `
		UPDATE nodes SET relay_group_id=$3, updated_at=now()
		WHERE id=$1 AND account_id=$2 AND node_type='relay'
	`, nodeID, principal.AccountID, groupID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("assign relay group: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit relay group assignment: %v", err))
		return
	}
	node, err := s.fetchNodeByID(r.Context(), nodeID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load relay node: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"node": node})
}

func (s *Server) handleAccountNodeRelayGroupDelete(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	nodeID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin relay group clear: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	currentGroupID, liveLeases, err := lockRelayNode(r.Context(), tx, nodeID, principal.AccountID)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "relay node not found")
		return
	} else if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("lock relay node: %v", err))
		return
	}
	if !relayGroupChangeAllowed(currentGroupID, nil, liveLeases) {
		util.WriteError(w, http.StatusConflict, "wait for this computer's active recordings to finish")
		return
	}
	if _, err := tx.Exec(r.Context(), `
		UPDATE nodes SET relay_group_id=NULL, updated_at=now()
		WHERE id=$1 AND account_id=$2 AND node_type='relay'
	`, nodeID, principal.AccountID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("clear relay group: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit relay group clear: %v", err))
		return
	}
	node, err := s.fetchNodeByID(r.Context(), nodeID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load relay node: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"node": node})
}

func (s *Server) handleAccountRelayGroupDelete(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	ct, err := s.pool.Exec(r.Context(), `DELETE FROM relay_groups WHERE id=$1 AND account_id=$2`, id, principal.AccountID)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23503" {
		util.WriteError(w, http.StatusConflict, "remove all computers from the relay group first")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("delete relay group: %v", err))
		return
	}
	if ct.RowsAffected() == 0 {
		util.WriteError(w, http.StatusNotFound, "relay group not found")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}
