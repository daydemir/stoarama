package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/email"
	"github.com/daydemir/stoarama/backend/internal/util"
)

const researchScopeRead = "stoarama.read"

type researchPrincipal struct {
	AccountID int64
	Email     string
	Name      string
	AuthType  string
	SessionID *int64
	APIKeyID  *int64
}

type researchContextKey string

const researchPrincipalContextKey researchContextKey = "research_principal"

func (s *Server) handleResearchApp(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(s.researchHTML)
}

func loadResearchHTML() ([]byte, error) {
	candidates := []string{
		"backend/web/account.html",
		"web/account.html",
		"../backend/web/account.html",
		"../web/account.html",
	}
	for _, c := range candidates {
		if b, err := os.ReadFile(c); err == nil {
			return b, nil
		}
	}
	cwd, _ := os.Getwd()
	if cwd != "" {
		for _, rel := range candidates {
			p := filepath.Join(cwd, rel)
			if b, err := os.ReadFile(p); err == nil {
				return b, nil
			}
		}
	}
	return nil, fmt.Errorf("research html not found")
}

func (s *Server) requireResearchAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, err := s.authenticateResearchRequest(r)
		if err != nil {
			util.WriteError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		ctx := context.WithValue(r.Context(), researchPrincipalContextKey, principal)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func researchPrincipalFromContext(ctx context.Context) (researchPrincipal, bool) {
	if ctx == nil {
		return researchPrincipal{}, false
	}
	v := ctx.Value(researchPrincipalContextKey)
	principal, ok := v.(researchPrincipal)
	return principal, ok
}

func (s *Server) authenticateResearchRequest(r *http.Request) (researchPrincipal, error) {
	if r == nil {
		return researchPrincipal{}, fmt.Errorf("request is nil")
	}
	if got := strings.TrimSpace(r.Header.Get("Authorization")); strings.HasPrefix(got, "Bearer ") {
		token := strings.TrimSpace(strings.TrimPrefix(got, "Bearer "))
		if token != "" {
			return s.lookupResearchAPIKey(r.Context(), token)
		}
	}
	if c, err := r.Cookie(researchSessionCookie); err == nil {
		token := strings.TrimSpace(c.Value)
		if token != "" {
			return s.lookupResearchSession(r.Context(), token)
		}
	}
	return researchPrincipal{}, fmt.Errorf("missing researcher auth")
}

func (s *Server) lookupResearchSession(ctx context.Context, raw string) (researchPrincipal, error) {
	hash := hashResearchSecret(raw)
	var p researchPrincipal
	var sessionID int64
	err := s.pool.QueryRow(ctx, `
		SELECT a.id, a.email, a.name, rs.id
		FROM research_sessions rs
		JOIN research_accounts a ON a.id=rs.account_id
		WHERE rs.session_hash=$1
		  AND rs.revoked_at IS NULL
		  AND rs.expires_at > now()
		  AND a.status='active'
	`, hash).Scan(&p.AccountID, &p.Email, &p.Name, &sessionID)
	if err != nil {
		return researchPrincipal{}, err
	}
	p.AuthType = "session"
	p.SessionID = &sessionID
	_, _ = s.pool.Exec(ctx, `UPDATE research_sessions SET last_used_at=now() WHERE id=$1`, sessionID)
	return p, nil
}

func (s *Server) lookupResearchAPIKey(ctx context.Context, raw string) (researchPrincipal, error) {
	hash := hashResearchSecret(raw)
	var p researchPrincipal
	var keyID int64
	err := s.pool.QueryRow(ctx, `
		SELECT a.id, a.email, a.name, k.id
		FROM research_api_keys k
		JOIN research_accounts a ON a.id=k.account_id
		WHERE k.secret_hash=$1
		  AND k.revoked_at IS NULL
		  AND (k.expires_at IS NULL OR k.expires_at > now())
		  AND a.status='active'
	`, hash).Scan(&p.AccountID, &p.Email, &p.Name, &keyID)
	if err != nil {
		return researchPrincipal{}, err
	}
	p.AuthType = "api_key"
	p.APIKeyID = &keyID
	_, _ = s.pool.Exec(ctx, `UPDATE research_api_keys SET last_used_at=now() WHERE id=$1`, keyID)
	return p, nil
}

type researchAuthRequest struct {
	Email        string `json:"email"`
	Name         string `json:"name"`
	RedirectPath string `json:"redirect_path"`
}

func (s *Server) handleResearchAuthRequestLink(w http.ResponseWriter, r *http.Request) {
	var req researchAuthRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	email := normalizeResearchEmail(req.Email)
	if !looksLikeEmail(email) {
		util.WriteError(w, http.StatusBadRequest, "valid email is required")
		return
	}
	name := strings.TrimSpace(req.Name)
	redirectPath := sanitizeResearchRedirectPath(req.RedirectPath)
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin research auth tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	var accountID int64
	var status string
	if err := tx.QueryRow(r.Context(), `
		INSERT INTO research_accounts (email, name, status)
		VALUES ($1, $2, 'active')
		ON CONFLICT (email)
		DO UPDATE SET
			name=CASE
				WHEN EXCLUDED.name <> '' AND research_accounts.name = '' THEN EXCLUDED.name
				ELSE research_accounts.name
			END,
			updated_at=now()
		RETURNING id, status
	`, email, name).Scan(&accountID, &status); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("upsert research account: %v", err))
		return
	}
	if status == "disabled" {
		_ = tx.Commit(r.Context())
		util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
		return
	}
	rawToken, err := generateResearchSecret(32)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("generate magic link: %v", err))
		return
	}
	hash := hashResearchSecret(rawToken)
	expiresAt := time.Now().UTC().Add(s.cfg.ResearchMagicLinkTTL)
	var linkID int64
	if err := tx.QueryRow(r.Context(), `
		INSERT INTO research_magic_links (
			account_id, token_hash, purpose, redirect_path, requester_ip, user_agent, expires_at
		)
		VALUES ($1, $2, 'login', $3, $4, $5, $6)
		RETURNING id
	`, accountID, hash, redirectPath, requesterIP(r), requestUserAgent(r), expiresAt).Scan(&linkID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("insert magic link: %v", err))
		return
	}
	if err := s.insertResearchAuthEventTx(r.Context(), tx, accountID, nil, "magic_link_created", "account", email, map[string]any{
		"magic_link_id":  linkID,
		"redirect_path":  redirectPath,
		"requester_ip":   requesterIP(r),
		"request_origin": r.Header.Get("Origin"),
	}); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("insert research auth event: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit research auth tx: %v", err))
		return
	}

	linkURL := s.buildResearchMagicLink(r, rawToken)
	if err := s.sendResearchMagicLink(r.Context(), email, linkURL); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("send magic link email: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleResearchAuthComplete(w http.ResponseWriter, r *http.Request) {
	rawToken := strings.TrimSpace(r.URL.Query().Get("token"))
	if rawToken == "" {
		http.Redirect(w, r, "/account?error=missing_token", http.StatusFound)
		return
	}
	hash := hashResearchSecret(rawToken)
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		http.Redirect(w, r, "/account?error=server_error", http.StatusFound)
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	var (
		linkID       int64
		accountID    int64
		email        string
		name         string
		status       string
		expiresAt    time.Time
		consumedAt   *time.Time
		redirectPath string
	)
	err = tx.QueryRow(r.Context(), `
		SELECT ml.id, a.id, a.email, a.name, a.status, ml.expires_at, ml.consumed_at, ml.redirect_path
		FROM research_magic_links ml
		JOIN research_accounts a ON a.id=ml.account_id
		WHERE ml.token_hash=$1
		FOR UPDATE
	`, hash).Scan(&linkID, &accountID, &email, &name, &status, &expiresAt, &consumedAt, &redirectPath)
	if err != nil {
		http.Redirect(w, r, "/account?error=invalid_token", http.StatusFound)
		return
	}
	if status != "active" {
		http.Redirect(w, r, "/account?error=account_disabled", http.StatusFound)
		return
	}
	if consumedAt != nil || !expiresAt.After(time.Now().UTC()) {
		http.Redirect(w, r, "/account?error=expired_token", http.StatusFound)
		return
	}
	if _, err := tx.Exec(r.Context(), `
		UPDATE research_magic_links
		SET consumed_at=now()
		WHERE id=$1
	`, linkID); err != nil {
		http.Redirect(w, r, "/account?error=server_error", http.StatusFound)
		return
	}
	if _, err := tx.Exec(r.Context(), `
		UPDATE research_accounts
		SET email_verified_at=COALESCE(email_verified_at, now())
		WHERE id=$1
	`, accountID); err != nil {
		http.Redirect(w, r, "/account?error=server_error", http.StatusFound)
		return
	}
	rawSession, err := generateResearchSecret(32)
	if err != nil {
		http.Redirect(w, r, "/account?error=server_error", http.StatusFound)
		return
	}
	sessionHash := hashResearchSecret(rawSession)
	sessionExpiresAt := time.Now().UTC().Add(s.cfg.ResearchSessionTTL)
	var sessionID int64
	if err := tx.QueryRow(r.Context(), `
		INSERT INTO research_sessions (account_id, session_hash, expires_at, last_used_at)
		VALUES ($1, $2, $3, now())
		RETURNING id
	`, accountID, sessionHash, sessionExpiresAt).Scan(&sessionID); err != nil {
		http.Redirect(w, r, "/account?error=server_error", http.StatusFound)
		return
	}
	if err := s.insertResearchAuthEventTx(r.Context(), tx, accountID, nil, "session_created", "account", email, map[string]any{
		"session_id":    sessionID,
		"magic_link_id": linkID,
	}); err != nil {
		http.Redirect(w, r, "/account?error=server_error", http.StatusFound)
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		http.Redirect(w, r, "/account?error=server_error", http.StatusFound)
		return
	}
	setResearchSessionCookie(w, r, rawSession, sessionExpiresAt)
	http.Redirect(w, r, sanitizeResearchRedirectPath(redirectPath), http.StatusFound)
}

func (s *Server) handleResearchMe(w http.ResponseWriter, r *http.Request) {
	principal, ok := researchPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"account": map[string]any{
			"id":        principal.AccountID,
			"email":     principal.Email,
			"name":      principal.Name,
			"auth_type": principal.AuthType,
		},
	})
}

func (s *Server) handleResearchLogout(w http.ResponseWriter, r *http.Request) {
	principal, ok := researchPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if principal.SessionID != nil {
		_, _ = s.pool.Exec(r.Context(), `UPDATE research_sessions SET revoked_at=now() WHERE id=$1`, *principal.SessionID)
		_ = s.insertResearchAuthEvent(r.Context(), principal.AccountID, nil, "session_revoked", "account", principal.Email, map[string]any{
			"session_id": *principal.SessionID,
		})
	}
	clearResearchSessionCookie(w, r)
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleResearchAPIKeysList(w http.ResponseWriter, r *http.Request) {
	principal, ok := researchPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	rows, err := s.pool.Query(r.Context(), `
		SELECT id, key_prefix, label, scopes, expires_at, last_used_at, revoked_at, created_at
		FROM research_api_keys
		WHERE account_id=$1
		ORDER BY created_at DESC, id DESC
	`, principal.AccountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list research api keys: %v", err))
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0, 8)
	for rows.Next() {
		var (
			id         int64
			prefix     string
			label      string
			scopes     []string
			expiresAt  *time.Time
			lastUsedAt *time.Time
			revokedAt  *time.Time
			createdAt  time.Time
		)
		if err := rows.Scan(&id, &prefix, &label, &scopes, &expiresAt, &lastUsedAt, &revokedAt, &createdAt); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan research api key: %v", err))
			return
		}
		items = append(items, map[string]any{
			"id":           id,
			"key_prefix":   prefix,
			"label":        label,
			"scopes":       scopes,
			"expires_at":   expiresAt,
			"last_used_at": lastUsedAt,
			"revoked_at":   revokedAt,
			"created_at":   createdAt.UTC(),
		})
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate research api keys: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

type researchAPIKeyCreateRequest struct {
	Label     string `json:"label"`
	ExpiresAt string `json:"expires_at"`
}

func (s *Server) handleResearchAPIKeysCreate(w http.ResponseWriter, r *http.Request) {
	principal, ok := researchPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var req researchAPIKeyCreateRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	label := strings.TrimSpace(req.Label)
	if label == "" {
		label = "default"
	}
	var expiresAt *time.Time
	if raw := strings.TrimSpace(req.ExpiresAt); raw != "" {
		tm, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			util.WriteError(w, http.StatusBadRequest, "expires_at must be RFC3339")
			return
		}
		if !tm.After(time.Now().UTC()) {
			util.WriteError(w, http.StatusBadRequest, "expires_at must be in the future")
			return
		}
		v := tm.UTC()
		expiresAt = &v
	}
	rawKey, err := generateResearchSecret(36)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("generate api key: %v", err))
		return
	}
	token := "sir_" + rawKey
	hash := hashResearchSecret(token)
	prefix := token
	if len(prefix) > 16 {
		prefix = prefix[:16]
	}
	var keyID int64
	if err := s.pool.QueryRow(r.Context(), `
		INSERT INTO research_api_keys (account_id, key_prefix, secret_hash, label, scopes, expires_at)
		VALUES ($1, $2, $3, $4, ARRAY[$5]::text[], $6)
		RETURNING id
	`, principal.AccountID, prefix, hash, label, researchScopeRead, expiresAt).Scan(&keyID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("create research api key: %v", err))
		return
	}
	_ = s.insertResearchAuthEvent(r.Context(), principal.AccountID, &keyID, "api_key_created", "account", principal.Email, map[string]any{
		"label":      label,
		"key_prefix": prefix,
	})
	util.WriteJSON(w, http.StatusCreated, map[string]any{
		"id":         keyID,
		"key_prefix": prefix,
		"label":      label,
		"token":      token,
		"scopes":     []string{researchScopeRead},
		"expires_at": expiresAt,
	})
}

func (s *Server) handleResearchAPIKeyRevoke(w http.ResponseWriter, r *http.Request) {
	principal, ok := researchPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	tag, err := s.revokeResearchAPIKey(r.Context(), id, principal.AccountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("revoke research api key: %v", err))
		return
	}
	if !tag {
		util.WriteError(w, http.StatusNotFound, "research api key not found")
		return
	}
	_ = s.insertResearchAuthEvent(r.Context(), principal.AccountID, &id, "api_key_revoked", "account", principal.Email, map[string]any{})
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleResearchAdminAccountsList(w http.ResponseWriter, r *http.Request) {
	rows, err := s.pool.Query(r.Context(), `
		SELECT id, email, name, status, email_verified_at, created_at, updated_at
		FROM research_accounts
		ORDER BY created_at DESC, id DESC
		LIMIT 500
	`)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list research accounts: %v", err))
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0, 64)
	for rows.Next() {
		var (
			id              int64
			email           string
			name            string
			status          string
			emailVerifiedAt *time.Time
			createdAt       time.Time
			updatedAt       time.Time
		)
		if err := rows.Scan(&id, &email, &name, &status, &emailVerifiedAt, &createdAt, &updatedAt); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan research account: %v", err))
			return
		}
		items = append(items, map[string]any{
			"id":                id,
			"email":             email,
			"name":              name,
			"status":            status,
			"email_verified_at": emailVerifiedAt,
			"created_at":        createdAt.UTC(),
			"updated_at":        updatedAt.UTC(),
		})
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) handleResearchAdminAccountDisable(w http.ResponseWriter, r *http.Request) {
	s.handleResearchAdminAccountStatus(w, r, "disabled")
}

func (s *Server) handleResearchAdminAccountEnable(w http.ResponseWriter, r *http.Request) {
	s.handleResearchAdminAccountStatus(w, r, "active")
}

func (s *Server) handleResearchAdminAccountStatus(w http.ResponseWriter, r *http.Request, status string) {
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	ct, err := s.pool.Exec(r.Context(), `
		UPDATE research_accounts
		SET status=$2, updated_at=now()
		WHERE id=$1
	`, id, status)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("update research account status: %v", err))
		return
	}
	if ct.RowsAffected() == 0 {
		util.WriteError(w, http.StatusNotFound, "research account not found")
		return
	}
	_ = s.insertResearchAuthEvent(r.Context(), id, nil, "account_status_updated", "operator", "dashboard", map[string]any{"status": status})
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true, "status": status})
}

func (s *Server) handleResearchAdminAccountAPIKeys(w http.ResponseWriter, r *http.Request) {
	accountID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	rows, err := s.pool.Query(r.Context(), `
		SELECT id, key_prefix, label, scopes, expires_at, last_used_at, revoked_at, created_at
		FROM research_api_keys
		WHERE account_id=$1
		ORDER BY created_at DESC, id DESC
	`, accountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list research api keys: %v", err))
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0, 8)
	for rows.Next() {
		var (
			id         int64
			prefix     string
			label      string
			scopes     []string
			expiresAt  *time.Time
			lastUsedAt *time.Time
			revokedAt  *time.Time
			createdAt  time.Time
		)
		if err := rows.Scan(&id, &prefix, &label, &scopes, &expiresAt, &lastUsedAt, &revokedAt, &createdAt); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan research api key: %v", err))
			return
		}
		items = append(items, map[string]any{
			"id":           id,
			"key_prefix":   prefix,
			"label":        label,
			"scopes":       scopes,
			"expires_at":   expiresAt,
			"last_used_at": lastUsedAt,
			"revoked_at":   revokedAt,
			"created_at":   createdAt.UTC(),
		})
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) handleResearchAdminAPIKeyRevoke(w http.ResponseWriter, r *http.Request) {
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	revoked, err := s.revokeResearchAPIKey(r.Context(), id, 0)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("revoke research api key: %v", err))
		return
	}
	if !revoked {
		util.WriteError(w, http.StatusNotFound, "research api key not found")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) revokeResearchAPIKey(ctx context.Context, keyID int64, accountID int64) (bool, error) {
	sql := `
		UPDATE research_api_keys
		SET revoked_at=COALESCE(revoked_at, now()), updated_at=now()
		WHERE id=$1
	`
	args := []any{keyID}
	if accountID > 0 {
		sql += ` AND account_id=$2`
		args = append(args, accountID)
	}
	ct, err := s.pool.Exec(ctx, sql, args...)
	if err != nil {
		return false, err
	}
	return ct.RowsAffected() == 1, nil
}

func setResearchSessionCookie(w http.ResponseWriter, r *http.Request, token string, expiresAt time.Time) {
	http.SetCookie(w, &http.Cookie{
		Name:     researchSessionCookie,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		Secure:   requestIsHTTPS(r),
		SameSite: http.SameSiteLaxMode,
		Expires:  expiresAt,
		MaxAge:   int(time.Until(expiresAt).Seconds()),
	})
}

func clearResearchSessionCookie(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     researchSessionCookie,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		Secure:   requestIsHTTPS(r),
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
		Expires:  time.Unix(0, 0),
	})
}

func normalizeResearchEmail(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

func looksLikeEmail(raw string) bool {
	return strings.Count(strings.TrimSpace(raw), "@") == 1
}

func sanitizeResearchRedirectPath(raw string) string {
	v := strings.TrimSpace(raw)
	if v == "" {
		return "/account"
	}
	if !strings.HasPrefix(v, "/") || strings.HasPrefix(v, "//") {
		return "/account"
	}
	return v
}

func (s *Server) buildResearchMagicLink(r *http.Request, token string) string {
	base := strings.TrimRight(strings.TrimSpace(s.cfg.ResearchAppBaseURL), "/")
	if base == "" && r != nil {
		scheme := "http"
		if requestIsHTTPS(r) {
			scheme = "https"
		}
		host := strings.TrimSpace(r.Host)
		if host != "" {
			base = scheme + "://" + host
		}
	}
	if base == "" {
		base = "http://localhost:8080"
	}
	return fmt.Sprintf("%s/auth/complete?token=%s", base, url.QueryEscape(token))
}

func buildResearchMagicLinkEmail(emailAddr, linkURL string) email.Message {
	subject := "Your Stoarama sign-in link"
	text := fmt.Sprintf("Use this sign-in link to access your Stoarama account:\n\n%s\n\nIf you did not request this link, you can ignore this email.", linkURL)
	html := fmt.Sprintf(`<p>Use this sign-in link to access your Stoarama account:</p><p><a href="%s">%s</a></p><p>If you did not request this link, you can ignore this email.</p>`, htmlEscape(linkURL), htmlEscape(linkURL))
	return email.Message{
		To:          emailAddr,
		Subject:     subject,
		PlainText:   text,
		HTML:        html,
		MessageType: "account_magic_link",
	}
}

func htmlEscape(v string) string {
	repl := strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		`"`, "&quot;",
		"'", "&#39;",
	)
	return repl.Replace(v)
}

func generateResearchSecret(numBytes int) (string, error) {
	b := make([]byte, numBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return strings.TrimRight(base64.RawURLEncoding.EncodeToString(b), "="), nil
}

func hashResearchSecret(raw string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(raw)))
	return hex.EncodeToString(sum[:])
}

func requestUserAgent(r *http.Request) string {
	if r == nil {
		return ""
	}
	return strings.TrimSpace(r.UserAgent())
}

func requesterIP(r *http.Request) string {
	if r == nil {
		return ""
	}
	if xff := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); xff != "" {
		parts := strings.Split(xff, ",")
		if len(parts) > 0 {
			return strings.TrimSpace(parts[0])
		}
	}
	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err == nil {
		return strings.TrimSpace(host)
	}
	return strings.TrimSpace(r.RemoteAddr)
}

func (s *Server) insertResearchAuthEvent(ctx context.Context, accountID int64, apiKeyID *int64, eventType, actorType, actorRef string, detail map[string]any) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := s.insertResearchAuthEventTx(ctx, tx, accountID, apiKeyID, eventType, actorType, actorRef, detail); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Server) insertResearchAuthEventTx(ctx context.Context, tx pgx.Tx, accountID int64, apiKeyID *int64, eventType, actorType, actorRef string, detail map[string]any) error {
	var keyID any
	if apiKeyID != nil {
		keyID = *apiKeyID
	}
	b, err := json.Marshal(nonNilMap(detail))
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO research_auth_events (account_id, api_key_id, event_type, actor_type, actor_ref, detail_jsonb)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb)
	`, accountID, keyID, strings.TrimSpace(eventType), strings.TrimSpace(actorType), strings.TrimSpace(actorRef), string(b))
	return err
}

func (s *Server) sendResearchMagicLink(ctx context.Context, emailAddr, linkURL string) error {
	msg := buildResearchMagicLinkEmail(emailAddr, linkURL)
	msg.From = strings.TrimSpace(s.cfg.ResearchEmailFrom)
	msg.ReplyTo = strings.TrimSpace(s.cfg.ResearchEmailReplyTo)
	return s.mailer.Send(ctx, msg)
}
