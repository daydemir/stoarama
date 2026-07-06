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

const (
	nodeTypeInferenceNode = "inference_node"
	nodeTypeLocalRecorder = "local_recorder"
	// nodeTypeRelay is an account-owned relay node running on a user machine. It is
	// distinct from nodeTypeLocalRecorder (which cloud droplets also enroll as), so
	// relay-vs-droplet branch selection keys off a real typed field, never a
	// heuristic. Only relay principals take the node:{id} lease-owner canonical form
	// and the relay lease branch; droplet principals are byte-identical to before.
	nodeTypeRelay = "relay"

	relayDefaultMaxStreams = 6
	relayMinMaxStreams     = 1
	relayMaxMaxStreams     = 20
)

type nodePrincipal struct {
	NodeID      int64
	AccountID   int64
	NodeType    string
	DisplayName string
}

type nodeContextKey string

const nodePrincipalContextKey nodeContextKey = "node_principal"

func normalizeNodeType(raw string) (string, bool) {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case nodeTypeInferenceNode:
		return nodeTypeInferenceNode, true
	case nodeTypeLocalRecorder:
		return nodeTypeLocalRecorder, true
	case nodeTypeRelay:
		return nodeTypeRelay, true
	default:
		return "", false
	}
}

func (s *Server) requireNodeAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, err := s.authenticateNodeRequest(r)
		if err != nil {
			util.WriteError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		ctx := context.WithValue(r.Context(), nodePrincipalContextKey, principal)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// requireRecorderNodeAuth authenticates a recorder node token and authorizes ONLY
// the recorder endpoints. It never accepts the shared SERVICE_TOKEN and never
// grants the capture/inference/media worker surface, so a recorder's blast radius
// is limited to its own recording jobs. It accepts both node types that run the
// recorder loop: 'local_recorder' (operator-owned cloud droplets, unchanged) and
// 'relay' (account-owned relay nodes on user machines). The two are still fully
// partitioned downstream by the typed node_type discriminator in every handler.
func (s *Server) requireRecorderNodeAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, err := s.authenticateNodeRequest(r)
		nodeType := strings.TrimSpace(principal.NodeType)
		if err != nil || (nodeType != nodeTypeLocalRecorder && nodeType != nodeTypeRelay) {
			util.WriteError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		ctx := context.WithValue(r.Context(), nodePrincipalContextKey, principal)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func nodePrincipalFromContext(ctx context.Context) (nodePrincipal, bool) {
	if ctx == nil {
		return nodePrincipal{}, false
	}
	v := ctx.Value(nodePrincipalContextKey)
	principal, ok := v.(nodePrincipal)
	return principal, ok
}

func (s *Server) authenticateNodeRequest(r *http.Request) (nodePrincipal, error) {
	if r == nil {
		return nodePrincipal{}, fmt.Errorf("request is nil")
	}
	got := strings.TrimSpace(r.Header.Get("Authorization"))
	if !strings.HasPrefix(got, "Bearer ") {
		return nodePrincipal{}, fmt.Errorf("missing node bearer token")
	}
	token := strings.TrimSpace(strings.TrimPrefix(got, "Bearer "))
	if token == "" {
		return nodePrincipal{}, fmt.Errorf("missing node bearer token")
	}
	return s.lookupNodeToken(r.Context(), token)
}

func (s *Server) lookupNodeToken(ctx context.Context, raw string) (nodePrincipal, error) {
	hash := hashSecret(raw)
	var principal nodePrincipal
	var tokenID int64
	err := s.pool.QueryRow(ctx, `
		SELECT n.id, n.account_id, n.node_type, n.display_name, t.id
		FROM node_tokens t
		JOIN nodes n ON n.id=t.node_id
		JOIN accounts a ON a.id=n.account_id
		WHERE t.secret_hash=$1
		  AND t.revoked_at IS NULL
		  AND n.status='active'
		  AND a.status='active'
	`, hash).Scan(&principal.NodeID, &principal.AccountID, &principal.NodeType, &principal.DisplayName, &tokenID)
	if err != nil {
		return nodePrincipal{}, err
	}
	_, _ = s.pool.Exec(ctx, `UPDATE node_tokens SET last_used_at=now() WHERE id=$1`, tokenID)
	return principal, nil
}

type nodeEnrollmentCreateRequest struct {
	OwnerAccountID *int64 `json:"owner_account_id,omitempty"`
	OwnerEmail     string `json:"owner_email,omitempty"`
	NodeType       string `json:"node_type"`
	Label          string `json:"label"`
	ExpiresAt      string `json:"expires_at"`
}

func (s *Server) createNodeEnrollmentToken(ctx context.Context, accountID int64, accountEmail string, req nodeEnrollmentCreateRequest) (map[string]any, error) {
	nodeType, ok := normalizeNodeType(req.NodeType)
	if !ok {
		return nil, newAPIStatusError(http.StatusBadRequest, "invalid node_type")
	}
	label := strings.TrimSpace(req.Label)
	if label == "" {
		label = nodeType
	}
	expiresAt := time.Now().UTC().Add(24 * time.Hour)
	if raw := strings.TrimSpace(req.ExpiresAt); raw != "" {
		tm, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return nil, newAPIStatusError(http.StatusBadRequest, "expires_at must be RFC3339")
		}
		if !tm.After(time.Now().UTC()) {
			return nil, newAPIStatusError(http.StatusBadRequest, "expires_at must be in the future")
		}
		expiresAt = tm.UTC()
	}
	rawToken, err := generateSecret(36)
	if err != nil {
		return nil, fmt.Errorf("generate node enrollment token: %w", err)
	}
	token := "sie_" + rawToken
	tokenHash := hashSecret(token)
	tokenPrefix := token
	if len(tokenPrefix) > 16 {
		tokenPrefix = tokenPrefix[:16]
	}
	var id int64
	err = s.pool.QueryRow(ctx, `
		INSERT INTO node_enrollment_tokens (account_id, token_prefix, token_hash, node_type, label, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id
	`, accountID, tokenPrefix, tokenHash, nodeType, label, expiresAt).Scan(&id)
	if err != nil {
		return nil, fmt.Errorf("create node enrollment token: %w", err)
	}
	_ = s.insertAccountAuthEvent(ctx, accountID, nil, "node_enrollment_token_created", "account", accountEmail, map[string]any{
		"token_id":     id,
		"token_prefix": tokenPrefix,
		"node_type":    nodeType,
		"label":        label,
	})
	return map[string]any{
		"id":           id,
		"token":        token,
		"token_prefix": tokenPrefix,
		"node_type":    nodeType,
		"label":        label,
		"expires_at":   expiresAt.UTC(),
		// install_command is the show-once, ready-to-paste line the org-settings
		// "Connect a computer" flow surfaces: it fetches the public relay installer and
		// runs it with the enrollment token. The token is only in this create response
		// (never listed again), so this is the one place the full command exists.
		"install_command": relayInstallCommand(token, label),
	}, nil
}

// relayInstallPublicBase is the user-facing host that serves /relay/install.sh (the
// installer registered at the router root). It mirrors the hardcoded public host
// used by the NAS connection compose snippet (connectionPublicAPIBase) so the
// copyable install command always points at the same host the docs reference.
const relayInstallPublicBase = "https://stoarama.com"

// relayInstallCommand builds the show-once curl|bash install line for an enrollment
// token, matching the shipped installer's invocation contract.
func relayInstallCommand(token, label string) string {
	cmd := fmt.Sprintf("curl -fsSL %s/relay/install.sh | bash -s -- --token %s",
		relayInstallPublicBase, shellQuote(token))
	if strings.TrimSpace(label) != "" {
		cmd += " --name " + shellQuote(strings.TrimSpace(label))
	}
	return cmd
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

func (s *Server) handleAccountNodeEnrollmentTokensList(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	rows, err := s.pool.Query(r.Context(), `
		SELECT id, token_prefix, node_type, label, expires_at, consumed_at, revoked_at, created_at
		FROM node_enrollment_tokens
		WHERE account_id=$1
		ORDER BY created_at DESC, id DESC
	`, principal.AccountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list node enrollment tokens: %v", err))
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0, 8)
	for rows.Next() {
		var (
			id         int64
			tokenPref  string
			nodeType   string
			label      string
			expiresAt  time.Time
			consumedAt *time.Time
			revokedAt  *time.Time
			createdAt  time.Time
		)
		if err := rows.Scan(&id, &tokenPref, &nodeType, &label, &expiresAt, &consumedAt, &revokedAt, &createdAt); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan node enrollment token: %v", err))
			return
		}
		items = append(items, map[string]any{
			"id":           id,
			"token_prefix": tokenPref,
			"node_type":    nodeType,
			"label":        label,
			"expires_at":   expiresAt.UTC(),
			"consumed_at":  consumedAt,
			"revoked_at":   revokedAt,
			"created_at":   createdAt.UTC(),
		})
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate node enrollment tokens: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) handleAccountNodeEnrollmentTokensCreate(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var req nodeEnrollmentCreateRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp, err := s.createNodeEnrollmentToken(r.Context(), principal.AccountID, principal.Email, req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	util.WriteJSON(w, http.StatusCreated, resp)
}

func (s *Server) handleServiceNodeEnrollmentTokensCreate(w http.ResponseWriter, r *http.Request) {
	var req nodeEnrollmentCreateRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	ownerAccountID, err := s.resolveAccountRef(r.Context(), req.OwnerAccountID, req.OwnerEmail)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	if ownerAccountID == nil || *ownerAccountID <= 0 {
		util.WriteError(w, http.StatusBadRequest, "owner_account_id or owner_email is required")
		return
	}
	var accountEmail string
	if err := s.pool.QueryRow(r.Context(), `SELECT email FROM accounts WHERE id=$1`, *ownerAccountID).Scan(&accountEmail); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusBadRequest, "account not found")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load account email: %v", err))
		return
	}
	resp, err := s.createNodeEnrollmentToken(r.Context(), *ownerAccountID, accountEmail, req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	util.WriteJSON(w, http.StatusCreated, resp)
}

func (s *Server) handleAccountNodeEnrollmentTokenRevoke(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	ct, err := s.pool.Exec(r.Context(), `
		UPDATE node_enrollment_tokens
		SET revoked_at=COALESCE(revoked_at, now()), updated_at=now()
		WHERE id=$1 AND account_id=$2
	`, id, principal.AccountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("revoke node enrollment token: %v", err))
		return
	}
	if ct.RowsAffected() == 0 {
		util.WriteError(w, http.StatusNotFound, "node enrollment token not found")
		return
	}
	_ = s.insertAccountAuthEvent(r.Context(), principal.AccountID, nil, "node_enrollment_token_revoked", "account", principal.Email, map[string]any{
		"token_id": id,
	})
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleAccountNodesList(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	rows, err := s.pool.Query(r.Context(), `
		SELECT n.id, n.account_id, n.node_type, n.display_name, n.hostname, n.platform, n.status, n.enrolled_at, n.last_heartbeat_at, n.capabilities_jsonb, n.metadata_jsonb, n.created_at, n.updated_at, n.relay_max_streams,
		       (SELECT COUNT(*) FROM recording_jobs j WHERE j.lease_owner='node:'||n.id::text AND j.status='leased' AND j.lease_expires_at > now())::int AS live_leases
		FROM nodes n
		WHERE n.account_id=$1
		ORDER BY n.created_at DESC, n.id DESC
	`, principal.AccountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list nodes: %v", err))
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0, 8)
	for rows.Next() {
		item, err := scanNodeRow(rows)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan node: %v", err))
			return
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate nodes: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

type nodePatchRequest struct {
	DisplayName     *string `json:"display_name"`
	RelayMaxStreams *int    `json:"relay_max_streams"`
}

// handleAccountNodePatch renames a relay node and/or adjusts its per-node stream cap.
// Both fields are optional; only the ones present in the body are updated (COALESCE
// against the current value). Scoped to node_type='relay' AND the caller's account, so
// a user can never touch an operator droplet or another tenant's node. relay_max_streams
// is bounded 1..20 (the relay mirrors it as its worker semaphore; the DB CHECK enforces
// >= 1 as the floor).
func (s *Server) handleAccountNodePatch(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req nodePatchRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.DisplayName == nil && req.RelayMaxStreams == nil {
		util.WriteError(w, http.StatusBadRequest, "display_name or relay_max_streams is required")
		return
	}
	var displayNameArg any
	if req.DisplayName != nil {
		name := strings.TrimSpace(*req.DisplayName)
		if name == "" {
			util.WriteError(w, http.StatusBadRequest, "display_name must not be empty")
			return
		}
		if isReservedNodeDisplayName(name) {
			util.WriteError(w, http.StatusBadRequest, "display_name must not start with 'node:'")
			return
		}
		displayNameArg = name
	}
	var relayMaxArg any
	if req.RelayMaxStreams != nil {
		if *req.RelayMaxStreams < relayMinMaxStreams || *req.RelayMaxStreams > relayMaxMaxStreams {
			util.WriteError(w, http.StatusBadRequest, "relay_max_streams must be between 1 and 20")
			return
		}
		relayMaxArg = *req.RelayMaxStreams
	}
	ct, err := s.pool.Exec(r.Context(), `
		UPDATE nodes
		SET display_name=COALESCE($3, display_name),
		    relay_max_streams=COALESCE($4, relay_max_streams),
		    updated_at=now()
		WHERE id=$1 AND account_id=$2 AND node_type='relay'
	`, id, principal.AccountID, displayNameArg, relayMaxArg)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("update node: %v", err))
		return
	}
	if ct.RowsAffected() == 0 {
		util.WriteError(w, http.StatusNotFound, "node not found")
		return
	}
	node, err := s.fetchNodeByID(r.Context(), id)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load node: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true, "node": node})
}

// handleAccountNodeDelete removes a relay node: it disables the node row and revokes
// all of its tokens, so the very next lease attempt is gated out (SQL) and the very
// next API call from that node 401s. In-flight leases bleed at most one
// clip_duration + reclaim window before the expired-lease sweep recovers them
// (DECISIONS #4, no immediate eviction in v1). Scoped to node_type='relay' AND the
// caller's account.
func (s *Server) handleAccountNodeDelete(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin node delete tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	ct, err := tx.Exec(r.Context(), `
		UPDATE nodes SET status='disabled', updated_at=now()
		WHERE id=$1 AND account_id=$2 AND node_type='relay'
	`, id, principal.AccountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("disable node: %v", err))
		return
	}
	if ct.RowsAffected() == 0 {
		util.WriteError(w, http.StatusNotFound, "node not found")
		return
	}
	if _, err := tx.Exec(r.Context(), `
		UPDATE node_tokens SET revoked_at=COALESCE(revoked_at, now())
		WHERE node_id=$1 AND revoked_at IS NULL
	`, id); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("revoke node tokens: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit node delete tx: %v", err))
		return
	}
	_ = s.insertAccountAuthEvent(r.Context(), principal.AccountID, nil, "node_removed", "account", principal.Email, map[string]any{
		"node_id": id,
	})
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// nodeEnrollAction is the decision for how an enrollment maps onto the nodes table
// given the existing rows in the same account that share lower(display_name).
type nodeEnrollAction int

const (
	// nodeEnrollInsert: no matching non-disabled row, no disabled row of the same
	// node_type -> insert a genuinely new node row.
	nodeEnrollInsert nodeEnrollAction = iota
	// nodeEnrollReactivate: a disabled row in the same account with the same
	// lower(display_name) AND the same node_type exists -> reactivate that row.
	nodeEnrollReactivate
	// nodeEnrollConflict: an active (non-disabled) row already holds this name in the
	// account -> a real conflict, reject.
	nodeEnrollConflict
)

// nodeNameMatch is one existing node row in the enrolling account whose
// lower(display_name) equals the requested name. account_id scoping is applied by the
// caller's query, so every match here is already tenant-local.
type nodeNameMatch struct {
	ID       int64
	NodeType string
	Status   string
}

// decideNodeEnroll picks the enrollment action for a requested node_type against the
// existing same-account, same-lower(name) rows. An active row (any type) holding the
// name is a hard conflict; otherwise a disabled row of the same node_type is reactivated;
// otherwise a new row is inserted. Droplet enrollment does not use this path (dropletpool
// inserts nodes directly), so operator-assigned droplet names are unaffected.
func decideNodeEnroll(matches []nodeNameMatch, nodeType string) (nodeEnrollAction, int64) {
	for _, m := range matches {
		if m.Status != "disabled" {
			return nodeEnrollConflict, 0
		}
	}
	for _, m := range matches {
		if m.Status == "disabled" && m.NodeType == nodeType {
			return nodeEnrollReactivate, m.ID
		}
	}
	return nodeEnrollInsert, 0
}

type nodeEnrollRequest struct {
	Token            string         `json:"token"`
	NodeType         string         `json:"node_type"`
	DisplayName      string         `json:"display_name"`
	Hostname         string         `json:"hostname"`
	Platform         string         `json:"platform"`
	RelayMaxStreams  *int           `json:"relay_max_streams"`
	CapabilitiesJSON map[string]any `json:"capabilities_json"`
	MetadataJSON     map[string]any `json:"metadata_json"`
}

func (s *Server) handleNodeEnroll(w http.ResponseWriter, r *http.Request) {
	var req nodeEnrollRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	rawToken := strings.TrimSpace(req.Token)
	if rawToken == "" {
		util.WriteError(w, http.StatusBadRequest, "token is required")
		return
	}
	displayName := strings.TrimSpace(req.DisplayName)
	if displayName == "" {
		util.WriteError(w, http.StatusBadRequest, "display_name is required")
		return
	}
	// 'node:' is the reserved lease_owner namespace for relay nodes (workerID is the
	// server-derived 'node:{id}'). Reject a display_name in that namespace so no node
	// can present an identity that collides with the canonical lease-owner form.
	if isReservedNodeDisplayName(displayName) {
		util.WriteError(w, http.StatusBadRequest, "display_name must not start with 'node:'")
		return
	}
	nodeType, ok := normalizeNodeType(req.NodeType)
	if !ok {
		util.WriteError(w, http.StatusBadRequest, "invalid node_type")
		return
	}
	relayMaxStreams := relayDefaultMaxStreams
	if req.RelayMaxStreams != nil {
		if nodeType != nodeTypeRelay {
			util.WriteError(w, http.StatusBadRequest, "relay_max_streams is only valid for relay nodes")
			return
		}
		if *req.RelayMaxStreams < relayMinMaxStreams || *req.RelayMaxStreams > relayMaxMaxStreams {
			util.WriteError(w, http.StatusBadRequest, "relay_max_streams must be between 1 and 20")
			return
		}
		relayMaxStreams = *req.RelayMaxStreams
	}
	tokenHash := hashSecret(rawToken)
	capBytes, err := json.Marshal(nonNilMap(req.CapabilitiesJSON))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("marshal capabilities_json: %v", err))
		return
	}
	metaBytes, err := json.Marshal(nonNilMap(req.MetadataJSON))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("marshal metadata_json: %v", err))
		return
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin node enroll tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	var (
		enrollmentID int64
		accountID    int64
		tokenType    string
		expiresAt    time.Time
		consumedAt   *time.Time
		revokedAt    *time.Time
		accountEmail string
		accountState string
	)
	err = tx.QueryRow(r.Context(), `
		SELECT t.id, t.account_id, t.node_type, t.expires_at, t.consumed_at, t.revoked_at, a.email, a.status
		FROM node_enrollment_tokens t
		JOIN accounts a ON a.id=t.account_id
		WHERE t.token_hash=$1
		FOR UPDATE
	`, tokenHash).Scan(&enrollmentID, &accountID, &tokenType, &expiresAt, &consumedAt, &revokedAt, &accountEmail, &accountState)
	if err != nil {
		util.WriteError(w, http.StatusUnauthorized, "invalid enrollment token")
		return
	}
	if accountState != "active" {
		util.WriteError(w, http.StatusForbidden, "account disabled")
		return
	}
	if tokenType != nodeType {
		util.WriteError(w, http.StatusBadRequest, "node_type does not match enrollment token")
		return
	}
	if revokedAt != nil || consumedAt != nil || !expiresAt.After(time.Now().UTC()) {
		util.WriteError(w, http.StatusUnauthorized, "enrollment token expired")
		return
	}

	// Removing a computer disables its node row (status='disabled') but keeps it. With the
	// partial unique index (0080) a disabled row no longer reserves its name, so re-enrolling
	// the same name must reactivate that disabled row rather than insert a duplicate. Scope
	// strictly to this account + lower(display_name); FOR UPDATE serializes concurrent enrolls.
	matchRows, err := tx.Query(r.Context(), `
		SELECT id, node_type, status
		FROM nodes
		WHERE account_id=$1 AND lower(display_name)=lower($2)
		FOR UPDATE
	`, accountID, displayName)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("lookup existing node: %v", err))
		return
	}
	var matches []nodeNameMatch
	for matchRows.Next() {
		var m nodeNameMatch
		if err := matchRows.Scan(&m.ID, &m.NodeType, &m.Status); err != nil {
			matchRows.Close()
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan existing node: %v", err))
			return
		}
		matches = append(matches, m)
	}
	matchRows.Close()
	if err := matchRows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate existing node: %v", err))
		return
	}

	action, reactivateID := decideNodeEnroll(matches, nodeType)
	var nodeID int64
	switch action {
	case nodeEnrollConflict:
		util.WriteError(w, http.StatusConflict, "a node with this name is already active")
		return
	case nodeEnrollReactivate:
		nodeID = reactivateID
		if _, err := tx.Exec(r.Context(), `
			UPDATE nodes
			SET status='active',
			    hostname=$2,
			    platform=$3,
			    capabilities_jsonb=$4::jsonb,
			    metadata_jsonb=$5::jsonb,
			    relay_max_streams=$6,
			    enrolled_at=now(),
			    last_heartbeat_at=now(),
			    updated_at=now()
			WHERE id=$1
		`, nodeID, strings.TrimSpace(req.Hostname), strings.TrimSpace(req.Platform), string(capBytes), string(metaBytes), relayMaxStreams); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("reactivate node: %v", err))
			return
		}
	default:
		err = tx.QueryRow(r.Context(), `
			INSERT INTO nodes (
				account_id, node_type, display_name, hostname, platform, status, enrolled_at, last_heartbeat_at, capabilities_jsonb, metadata_jsonb, relay_max_streams
			)
			VALUES ($1, $2, $3, $4, $5, 'active', now(), now(), $6::jsonb, $7::jsonb, $8)
			RETURNING id
		`, accountID, nodeType, displayName, strings.TrimSpace(req.Hostname), strings.TrimSpace(req.Platform), string(capBytes), string(metaBytes), relayMaxStreams).Scan(&nodeID)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("create node: %v", err))
			return
		}
	}

	if _, err := tx.Exec(r.Context(), `
		UPDATE node_enrollment_tokens
		SET consumed_at=now(), updated_at=now()
		WHERE id=$1
	`, enrollmentID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("consume enrollment token: %v", err))
		return
	}

	rawNodeToken, err := generateSecret(36)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("generate node token: %v", err))
		return
	}
	nodeToken := "sin_" + rawNodeToken
	nodeTokenHash := hashSecret(nodeToken)
	nodeTokenPrefix := nodeToken
	if len(nodeTokenPrefix) > 16 {
		nodeTokenPrefix = nodeTokenPrefix[:16]
	}
	var nodeTokenID int64
	err = tx.QueryRow(r.Context(), `
		INSERT INTO node_tokens (node_id, key_prefix, secret_hash, last_used_at)
		VALUES ($1, $2, $3, now())
		RETURNING id
	`, nodeID, nodeTokenPrefix, nodeTokenHash).Scan(&nodeTokenID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("create node token: %v", err))
		return
	}

	if err := s.insertAccountAuthEventTx(r.Context(), tx, accountID, nil, "node_enrolled", "account", accountEmail, map[string]any{
		"node_id":                 nodeID,
		"node_type":               nodeType,
		"display_name":            displayName,
		"node_token_id":           nodeTokenID,
		"node_token_prefix":       nodeTokenPrefix,
		"enrollment_token_id":     enrollmentID,
		"enrollment_token_prefix": rawToken[:minInt(len(rawToken), 16)],
		"reactivated":             action == nodeEnrollReactivate,
	}); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("insert node enroll event: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit node enroll tx: %v", err))
		return
	}

	node, err := s.fetchNodeByID(r.Context(), nodeID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load node: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusCreated, map[string]any{
		"node":       node,
		"node_token": nodeToken,
	})
}

func (s *Server) handleNodeMe(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	node, err := s.fetchNodeByID(r.Context(), principal.NodeID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load node: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"node": node})
}

type nodeHeartbeatRequest struct {
	CapabilitiesJSON map[string]any `json:"capabilities_json"`
	MetadataJSON     map[string]any `json:"metadata_json"`
}

func (s *Server) handleNodeHeartbeat(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var req nodeHeartbeatRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	var capArg any
	if req.CapabilitiesJSON != nil {
		b, err := json.Marshal(nonNilMap(req.CapabilitiesJSON))
		if err != nil {
			util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("marshal capabilities_json: %v", err))
			return
		}
		capArg = string(b)
	}
	var metaArg any
	if req.MetadataJSON != nil {
		b, err := json.Marshal(nonNilMap(req.MetadataJSON))
		if err != nil {
			util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("marshal metadata_json: %v", err))
			return
		}
		metaArg = string(b)
	}
	// Merge (not replace) the reported capability keys into capabilities_jsonb so a
	// relay heartbeat that reports only its relay keys (youtube_mode, youtube_ready,
	// youtube_error, active_jobs, relay_version, ...) preserves any pre-existing keys.
	// A null capabilities payload leaves the column untouched (concat with '{}').
	ct, err := s.pool.Exec(r.Context(), `
		UPDATE nodes
		SET
			last_heartbeat_at=now(),
			capabilities_jsonb=COALESCE(nodes.capabilities_jsonb, '{}'::jsonb) || COALESCE($2::jsonb, '{}'::jsonb),
			metadata_jsonb=COALESCE($3::jsonb, nodes.metadata_jsonb),
			updated_at=now()
		WHERE nodes.id=$1 AND nodes.status='active'
	`, principal.NodeID, capArg, metaArg)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("update node heartbeat: %v", err))
		return
	}
	if ct.RowsAffected() == 0 {
		util.WriteError(w, http.StatusNotFound, "node not found")
		return
	}
	node, err := s.fetchNodeByID(r.Context(), principal.NodeID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load node: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true, "node": node})
}

func (s *Server) fetchNodeByID(ctx context.Context, id int64) (map[string]any, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT n.id, n.account_id, n.node_type, n.display_name, n.hostname, n.platform, n.status, n.enrolled_at, n.last_heartbeat_at, n.capabilities_jsonb, n.metadata_jsonb, n.created_at, n.updated_at, n.relay_max_streams,
		       (SELECT COUNT(*) FROM recording_jobs j WHERE j.lease_owner='node:'||n.id::text AND j.status='leased' AND j.lease_expires_at > now())::int AS live_leases
		FROM nodes n
		WHERE n.id=$1
	`, id)
	return scanNodeRow(row)
}

type nodeScanner interface {
	Scan(dest ...any) error
}

func scanNodeRow(row nodeScanner) (map[string]any, error) {
	var (
		id              int64
		accountID       int64
		nodeType        string
		displayName     string
		hostname        string
		platform        string
		status          string
		enrolledAt      *time.Time
		lastHeartbeatAt *time.Time
		capabilitiesRaw []byte
		metadataRaw     []byte
		createdAt       time.Time
		updatedAt       time.Time
		relayMaxStreams int
		liveLeases      int
	)
	if err := row.Scan(
		&id,
		&accountID,
		&nodeType,
		&displayName,
		&hostname,
		&platform,
		&status,
		&enrolledAt,
		&lastHeartbeatAt,
		&capabilitiesRaw,
		&metadataRaw,
		&createdAt,
		&updatedAt,
		&relayMaxStreams,
		&liveLeases,
	); err != nil {
		return nil, err
	}
	var capabilities map[string]any
	if len(capabilitiesRaw) > 0 {
		_ = json.Unmarshal(capabilitiesRaw, &capabilities)
	}
	var metadata map[string]any
	if len(metadataRaw) > 0 {
		_ = json.Unmarshal(metadataRaw, &metadata)
	}
	// healthy is server-derived from last_heartbeat_at with the same 120s liveness
	// threshold the relay lease gate uses, so the fleet UX (green/red dot) never
	// depends on client clock skew.
	healthy := lastHeartbeatAt != nil && time.Since(*lastHeartbeatAt) < 120*time.Second
	// seconds_since_heartbeat is the server-computed age of the last heartbeat, so the
	// UI can render an honest "last seen X ago" label without trusting the client clock.
	// Null when the node has never heartbeat.
	var secondsSinceHeartbeat any
	if lastHeartbeatAt != nil {
		secondsSinceHeartbeat = int64(time.Since(*lastHeartbeatAt).Seconds())
	}
	return map[string]any{
		"id":                      id,
		"account_id":              accountID,
		"node_type":               nodeType,
		"display_name":            displayName,
		"hostname":                hostname,
		"platform":                platform,
		"status":                  status,
		"enrolled_at":             enrolledAt,
		"last_heartbeat_at":       lastHeartbeatAt,
		"seconds_since_heartbeat": secondsSinceHeartbeat,
		"healthy":                 healthy,
		"relay_max_streams":       relayMaxStreams,
		"live_leases":             liveLeases,
		"capabilities_json":       nonNilMap(capabilities),
		"metadata_json":           nonNilMap(metadata),
		"created_at":              createdAt.UTC(),
		"updated_at":              updatedAt.UTC(),
	}, nil
}

// isReservedNodeDisplayName reports whether a node/droplet display name falls in the
// reserved 'node:' lease_owner namespace. Relay principals derive their canonical
// lease_owner as 'node:{id}', so any user- or operator-chosen name in that namespace
// is rejected at enrollment / droplet registration to keep the namespace disjoint.
// The typed node_type discriminator is the primary partition; this is defense in depth.
func isReservedNodeDisplayName(name string) bool {
	return strings.HasPrefix(strings.TrimSpace(name), "node:")
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
