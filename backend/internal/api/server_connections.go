package api

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/util"
)

// The two pull endpoints that carry numeric path params. r.URL.Path is reliable in
// middleware (chi's RoutePattern is not, for nested-group middleware), so the
// allowlist matches on the raw path with precompiled, anchored regexps for the
// param routes and literal+method checks for the rest. Default is DENY: any account
// route not in this allowlist is 403d for a pull-scoped key automatically.
var (
	pullDownloadPathRe = regexp.MustCompile(`^/api/v1/account/recordings/\d+/clips/\d+/download$`)
	pullDeletePathRe   = regexp.MustCompile(`^/api/v1/account/recordings/\d+/clips/\d+$`)
)

// pullPathAllowed reports whether a pull-scoped key may call (method, path). It is
// the single source of truth for pull confinement and is exercised directly by the
// table tests. The 4 allowed shapes:
//   - GET    /api/v1/account/clips                                   (cursor list)
//   - POST   /api/v1/account/connections/heartbeat                  (heartbeat)
//   - GET    /api/v1/account/recordings/{id}/clips/{clipId}/download (presign)
//   - DELETE /api/v1/account/recordings/{id}/clips/{clipId}          (purge one clip)
func pullPathAllowed(method, path string) bool {
	switch {
	case method == http.MethodGet && path == "/api/v1/account/clips":
		return true
	case method == http.MethodPost && path == "/api/v1/account/connections/heartbeat":
		return true
	case method == http.MethodGet && pullDownloadPathRe.MatchString(path):
		return true
	case method == http.MethodDelete && pullDeletePathRe.MatchString(path):
		return true
	default:
		return false
	}
}

// confineAccountScope is registered immediately after requireAccountAuth on the
// account group, so the principal is already in context. A session or full/read
// key passes through untouched; a pull-scoped key is allowed ONLY on the 4 pull
// endpoints and 403d everywhere else. Default-DENY means a newly added account
// route is automatically out of reach for a leaked NAS key.
func (s *Server) confineAccountScope(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := accountPrincipalFromContext(r.Context())
		if !ok {
			util.WriteError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		if !isPullScopedPrincipal(principal) {
			next.ServeHTTP(w, r)
			return
		}
		if !pullPathAllowed(r.Method, r.URL.Path) {
			util.WriteError(w, http.StatusForbidden, "this key is limited to the NAS pull")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func clampPollIntervalSec(v int) int {
	if v == 0 {
		return 90
	}
	if v < 10 {
		return 10
	}
	if v > 3600 {
		return 3600
	}
	return v
}

// connectionComposeSnippet renders the ready-to-paste docker-compose stanza for the
// NAS pull client, prefilled with the request-derived API base and the one-time
// token. It mirrors clients/nas-pull/docker-compose.yml's env block.
func connectionComposeSnippet(apiBase, token string, pollIntervalSec int) string {
	return fmt.Sprintf(`services:
  stoarama-pull:
    build: .
    restart: always
    environment:
      STOARAMA_API_BASE: "%s"
      STOARAMA_API_KEY: "%s"
      STOARAMA_OUTPUT_DIR: "/clips"
      STOARAMA_STATE_FILE: "/state/cursor.json"
      STOARAMA_POLL_INTERVAL_SEC: "%d"
      STOARAMA_DRY_RUN: "0"
    volumes:
      - /volume1/stoarama-clips:/clips
      - /volume1/stoarama-state:/state
`, apiBase, token, pollIntervalSec)
}

// connectionAPIBase derives the public /api/v1 base for the compose snippet from
// the configured AppBaseURL, falling back to the request scheme+host (same logic as
// buildAccountMagicLink, plus the /api/v1 suffix the client expects).
func (s *Server) connectionAPIBase(r *http.Request) string {
	base := strings.TrimRight(strings.TrimSpace(s.cfg.AppBaseURL), "/")
	if base == "" && r != nil {
		scheme := "http"
		if requestIsHTTPS(r) {
			scheme = "https"
		}
		if host := strings.TrimSpace(r.Host); host != "" {
			base = scheme + "://" + host
		}
	}
	if base == "" {
		base = "http://localhost:8080"
	}
	return base + "/api/v1"
}

type connectionCreateRequest struct {
	Label           string `json:"label"`
	PollIntervalSec int    `json:"poll_interval_sec"`
}

// handleAccountConnectionsCreate mints a stoarama.pull-scoped key and inserts a
// connection row referencing it, in ONE tx, then returns the sir_ token ONCE plus a
// ready-to-paste docker-compose snippet. Member-visible (session group), no owner
// gate. The minted key can do nothing but the pull loop (confineAccountScope).
func (s *Server) handleAccountConnectionsCreate(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var req connectionCreateRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	label := strings.TrimSpace(req.Label)
	if label == "" {
		label = "NAS"
	}
	pollInterval := clampPollIntervalSec(req.PollIntervalSec)

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin connection tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	keyID, prefix, token, err := mintAccountAPIKey(r.Context(), tx, principal.AccountID, label, accountScopePull, nil)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("mint pull key: %v", err))
		return
	}
	var connID int64
	if err := tx.QueryRow(r.Context(), `
		INSERT INTO connections (account_id, kind, label, api_key_id, poll_interval_sec, created_by)
		VALUES ($1, 'nas_pull', $2, $3, $4, $5)
		RETURNING id
	`, principal.AccountID, label, keyID, pollInterval, principal.AccountID).Scan(&connID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("create connection: %v", err))
		return
	}
	if err := s.insertAccountAuthEventTx(r.Context(), tx, principal.AccountID, &keyID, "connection_created", "account", accountActorLabel(principal, ""), map[string]any{
		"connection_id": connID,
		"label":         label,
		"key_prefix":    prefix,
	}); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("audit connection: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit connection: %v", err))
		return
	}

	apiBase := s.connectionAPIBase(r)
	util.WriteJSON(w, http.StatusCreated, map[string]any{
		"id":                connID,
		"label":             label,
		"poll_interval_sec": pollInterval,
		"token":             token,
		"compose_snippet":   connectionComposeSnippet(apiBase, token, pollInterval),
	})
}

// handleAccountConnectionsList returns the account's connections with a derived
// health: 'never' until the first heartbeat, then 'healthy' if last_seen_at is
// within 3x the poll interval else 'stale'. Never returns the token.
func (s *Server) handleAccountConnectionsList(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	rows, err := s.pool.Query(r.Context(), `
		SELECT id, label, last_seen_at, clips_pulled, last_cursor_id, poll_interval_sec, created_at
		FROM connections
		WHERE account_id=$1
		ORDER BY created_at DESC, id DESC
	`, principal.AccountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list connections: %v", err))
		return
	}
	defer rows.Close()
	now := time.Now().UTC()
	items := make([]map[string]any, 0, 8)
	for rows.Next() {
		var (
			id              int64
			label           string
			lastSeenAt      *time.Time
			clipsPulled     int64
			lastCursorID    int64
			pollIntervalSec int
			createdAt       time.Time
		)
		if err := rows.Scan(&id, &label, &lastSeenAt, &clipsPulled, &lastCursorID, &pollIntervalSec, &createdAt); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan connection: %v", err))
			return
		}
		health := "never"
		var lastSeen any
		if lastSeenAt != nil {
			lastSeen = lastSeenAt.UTC()
			staleAfter := time.Duration(pollIntervalSec) * 3 * time.Second
			if now.Sub(*lastSeenAt) <= staleAfter {
				health = "healthy"
			} else {
				health = "stale"
			}
		}
		items = append(items, map[string]any{
			"id":                id,
			"label":             label,
			"last_seen_at":      lastSeen,
			"clips_pulled":      clipsPulled,
			"last_cursor_id":    lastCursorID,
			"poll_interval_sec": pollIntervalSec,
			"health":            health,
			"created_at":        createdAt.UTC(),
		})
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate connections: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

// handleAccountConnectionRotate mints a fresh pull key, points the connection at it,
// and revokes the old key, in ONE tx. Returns the new token ONCE plus a refreshed
// compose snippet. 404 if the connection is not owned by the caller's account.
func (s *Server) handleAccountConnectionRotate(w http.ResponseWriter, r *http.Request) {
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
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin rotate tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	var label string
	var oldKeyID int64
	var pollInterval int
	err = tx.QueryRow(r.Context(), `
		SELECT label, api_key_id, poll_interval_sec
		FROM connections
		WHERE id=$1 AND account_id=$2
		FOR UPDATE
	`, id, principal.AccountID).Scan(&label, &oldKeyID, &pollInterval)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "connection not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load connection: %v", err))
		return
	}

	newKeyID, prefix, token, err := mintAccountAPIKey(r.Context(), tx, principal.AccountID, label, accountScopePull, nil)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("mint pull key: %v", err))
		return
	}
	if _, err := tx.Exec(r.Context(), `
		UPDATE connections SET api_key_id=$1, updated_at=now() WHERE id=$2 AND account_id=$3
	`, newKeyID, id, principal.AccountID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("point connection at new key: %v", err))
		return
	}
	if _, err := tx.Exec(r.Context(), `
		UPDATE account_api_keys SET revoked_at=COALESCE(revoked_at, now()), updated_at=now()
		WHERE id=$1 AND account_id=$2
	`, oldKeyID, principal.AccountID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("revoke old key: %v", err))
		return
	}
	if err := s.insertAccountAuthEventTx(r.Context(), tx, principal.AccountID, &newKeyID, "connection_rotated", "account", accountActorLabel(principal, ""), map[string]any{
		"connection_id": id,
		"old_key_id":    oldKeyID,
		"key_prefix":    prefix,
	}); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("audit rotate: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit rotate: %v", err))
		return
	}

	apiBase := s.connectionAPIBase(r)
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"id":                id,
		"label":             label,
		"poll_interval_sec": pollInterval,
		"token":             token,
		"compose_snippet":   connectionComposeSnippet(apiBase, token, pollInterval),
	})
}

// handleAccountConnectionDelete revokes the connection's key and deletes the row.
// 404 if not owned by the caller's account.
func (s *Server) handleAccountConnectionDelete(w http.ResponseWriter, r *http.Request) {
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
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin delete tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	var keyID int64
	err = tx.QueryRow(r.Context(), `
		DELETE FROM connections WHERE id=$1 AND account_id=$2 RETURNING api_key_id
	`, id, principal.AccountID).Scan(&keyID)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "connection not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("delete connection: %v", err))
		return
	}
	if _, err := tx.Exec(r.Context(), `
		UPDATE account_api_keys SET revoked_at=COALESCE(revoked_at, now()), updated_at=now()
		WHERE id=$1 AND account_id=$2
	`, keyID, principal.AccountID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("revoke connection key: %v", err))
		return
	}
	if err := s.insertAccountAuthEventTx(r.Context(), tx, principal.AccountID, &keyID, "connection_deleted", "account", accountActorLabel(principal, ""), map[string]any{
		"connection_id": id,
	}); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("audit delete: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit delete: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

type connectionHeartbeatRequest struct {
	CursorID    int64 `json:"cursor_id"`
	ClipsPulled int64 `json:"clips_pulled"`
}

// handleAccountConnectionHeartbeat is called by the pull client with its scoped
// key. It resolves the connection by the calling api_key_id (+ account_id) and
// advances last_seen_at/last_cursor_id and the monotonic clips_pulled. A session
// principal (no api_key_id) or a key with no connection row gets 403; the heartbeat
// is machine-only.
func (s *Server) handleAccountConnectionHeartbeat(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if principal.APIKeyID == nil {
		util.WriteError(w, http.StatusForbidden, "heartbeat requires a NAS pull key")
		return
	}
	var req connectionHeartbeatRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.CursorID < 0 || req.ClipsPulled < 0 {
		util.WriteError(w, http.StatusBadRequest, "cursor_id and clips_pulled must be non-negative")
		return
	}
	ct, err := s.pool.Exec(r.Context(), `
		UPDATE connections
		SET last_seen_at=now(),
		    last_cursor_id=GREATEST(last_cursor_id, $1),
		    clips_pulled=GREATEST(clips_pulled, $2),
		    updated_at=now()
		WHERE api_key_id=$3 AND account_id=$4
	`, req.CursorID, req.ClipsPulled, *principal.APIKeyID, principal.AccountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("heartbeat: %v", err))
		return
	}
	if ct.RowsAffected() == 0 {
		util.WriteError(w, http.StatusForbidden, "no connection for this key")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}
