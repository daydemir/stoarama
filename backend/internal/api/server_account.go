package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/email"
	"github.com/daydemir/stoarama/backend/internal/util"
)

const (
	accountScopeRead  = "stoarama.read"
	accountScopePull  = "stoarama.pull"
	accountRoleMember = "member"
	accountRoleAdmin  = "admin"
)

type accountPrincipal struct {
	// AccountID is the CURRENT org id (account_sessions.current_org_id for a
	// session; the key's account_id for an API key). Every account_id-scoped
	// downstream query keys on this.
	AccountID int64
	// UserID is the acting user (users.id). Zero for API-key callers, which are
	// org-scoped and not tied to a user row.
	UserID    int64
	Email     string
	Name      string
	Role      string // operator/admin flag ('admin' when users.is_operator); NOT the team role
	AuthType  string
	SessionID *int64
	APIKeyID  *int64
	// MemberEmail is the email the magic link was issued for (the acting member's
	// own email), which differs from the account owner email for invited members.
	// Empty for legacy sessions and API-key callers.
	MemberEmail string
	// MemberRole is the team role (memberships.role: 'owner'|'billing_admin'|
	// 'member'), independent of Role. Defaults to 'owner' for API-key callers.
	MemberRole string
	// KeyScopes are the scopes of the calling API key (account_api_keys.scopes).
	// Nil for browser-session principals. A key whose scopes contain
	// accountScopePull is confined to the NAS pull endpoints by confineAccountScope.
	KeyScopes []string
}

// isPullScopedPrincipal reports whether the caller is an API key limited to the
// 'stoarama.pull' scope. Such a key may ONLY call the NAS pull endpoints; it is
// 403d on every other /api/v1/account route by confineAccountScope. Sessions and
// full/read keys are never pull-scoped, so they keep full access.
func isPullScopedPrincipal(p accountPrincipal) bool {
	if p.APIKeyID == nil {
		return false
	}
	return slices.Contains(p.KeyScopes, accountScopePull)
}

// principalIsOwner gates owner-only org actions (member management). A session's
// MemberRole comes from the memberships row for (user, current org); an API key
// is set to "owner" explicitly. Empty is treated as owner only as a defensive
// default (session resolution fails-fast before this on a missing membership).
func principalIsOwner(p accountPrincipal) bool {
	return p.MemberRole == "" || p.MemberRole == "owner"
}

// principalCanManageBilling gates billing WRITES (add/change card, open portal)
// and the org role change. An owner or a billing_admin may manage billing; a
// plain member may only READ the org bill. Empty MemberRole (legacy session /
// API key) is treated as owner, matching principalIsOwner.
func principalCanManageBilling(p accountPrincipal) bool {
	return p.MemberRole == "" || p.MemberRole == "owner" || p.MemberRole == "billing_admin"
}

type accountContextKey string

const accountPrincipalContextKey accountContextKey = "account_principal"

func (s *Server) handleAccountApp(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(s.accountHTML)
}

func loadAccountHTML() ([]byte, error) {
	return loadHTMLPage("account.html")
}

func (s *Server) requireAccountAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, err := s.authenticateAccountRequest(r)
		if err != nil {
			util.WriteError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		ctx := context.WithValue(r.Context(), accountPrincipalContextKey, principal)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) requireAccountSessionAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, err := s.authenticateAccountSessionRequest(r)
		if err != nil {
			util.WriteError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		ctx := context.WithValue(r.Context(), accountPrincipalContextKey, principal)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func accountPrincipalFromContext(ctx context.Context) (accountPrincipal, bool) {
	if ctx == nil {
		return accountPrincipal{}, false
	}
	v := ctx.Value(accountPrincipalContextKey)
	principal, ok := v.(accountPrincipal)
	return principal, ok
}

func accountSessionCapabilities(principal accountPrincipal) map[string]any {
	hasBrowserSession := principal.SessionID != nil
	return map[string]any{
		"can_toggle_recording": hasBrowserSession,
		"can_manage_api_keys":  hasBrowserSession,
		"can_download_clips":   hasBrowserSession,
		"can_edit_tags":        hasBrowserSession,
		"can_edit_notes":       hasBrowserSession,
	}
}

func accountActorLabel(principal accountPrincipal, fallback string) string {
	email := strings.TrimSpace(principal.Email)
	if email != "" {
		return "account:" + email
	}
	if principal.AccountID > 0 {
		return fmt.Sprintf("account:%d", principal.AccountID)
	}
	return strings.TrimSpace(fallback)
}

func (s *Server) authenticateAccountRequest(r *http.Request) (accountPrincipal, error) {
	if r == nil {
		return accountPrincipal{}, fmt.Errorf("request is nil")
	}
	if got := strings.TrimSpace(r.Header.Get("Authorization")); strings.HasPrefix(got, "Bearer ") {
		token := strings.TrimSpace(strings.TrimPrefix(got, "Bearer "))
		if token != "" {
			return s.lookupAccountAPIKey(r.Context(), token)
		}
	}
	if c, err := r.Cookie(accountSessionCookie); err == nil {
		token := strings.TrimSpace(c.Value)
		if token != "" {
			return s.lookupAccountSession(r.Context(), token)
		}
	}
	return accountPrincipal{}, fmt.Errorf("missing account auth")
}

func (s *Server) authenticateAccountSessionRequest(r *http.Request) (accountPrincipal, error) {
	if r == nil {
		return accountPrincipal{}, fmt.Errorf("request is nil")
	}
	c, err := r.Cookie(accountSessionCookie)
	if err != nil {
		return accountPrincipal{}, fmt.Errorf("missing account session")
	}
	token := strings.TrimSpace(c.Value)
	if token == "" {
		return accountPrincipal{}, fmt.Errorf("missing account session")
	}
	return s.lookupAccountSession(r.Context(), token)
}

func (s *Server) lookupAccountSession(ctx context.Context, raw string) (accountPrincipal, error) {
	hash := hashSecret(raw)
	var p accountPrincipal
	var sessionID int64
	var isOperator bool
	var memberEmail *string
	// Resolve the session through user -> current org -> membership. The INNER
	// JOIN on memberships is the fail-fast: if the acting user has no membership in
	// their current org (revoked/removed), no row returns and requireAccountAuth
	// 401s. This replaces the old COALESCE(role,'owner') fallback, which was only
	// safe under one-team-per-email.
	err := s.pool.QueryRow(ctx, `
		SELECT o.id, u.id, u.email, u.name, u.is_operator, m.role, rs.id, rs.member_email
		FROM account_sessions rs
		JOIN users u ON u.id=rs.user_id
		JOIN accounts o ON o.id=rs.current_org_id
		JOIN memberships m ON m.user_id=rs.user_id AND m.org_id=rs.current_org_id
		WHERE rs.session_hash=$1
		  AND rs.revoked_at IS NULL
		  AND rs.expires_at > now()
		  AND o.status='active'
	`, hash).Scan(&p.AccountID, &p.UserID, &p.Email, &p.Name, &isOperator, &p.MemberRole, &sessionID, &memberEmail)
	if err != nil {
		return accountPrincipal{}, err
	}
	// requireAdminAuth keys on Role=="admin"; drive it from the platform operator
	// flag so operator access is preserved after the users move.
	if isOperator {
		p.Role = accountRoleAdmin
	} else {
		p.Role = accountRoleMember
	}
	p.AuthType = "session"
	p.SessionID = &sessionID
	if memberEmail != nil {
		p.MemberEmail = *memberEmail
	}
	_, _ = s.pool.Exec(ctx, `UPDATE account_sessions SET last_used_at=now() WHERE id=$1`, sessionID)
	return p, nil
}

func (s *Server) lookupAccountAPIKey(ctx context.Context, raw string) (accountPrincipal, error) {
	hash := hashSecret(raw)
	var p accountPrincipal
	var keyID int64
	err := s.pool.QueryRow(ctx, `
		SELECT a.id, a.email, a.name, a.role, k.id, k.scopes
		FROM account_api_keys k
		JOIN accounts a ON a.id=k.account_id
		WHERE k.secret_hash=$1
		  AND k.revoked_at IS NULL
		  AND (k.expires_at IS NULL OR k.expires_at > now())
		  AND a.status='active'
	`, hash).Scan(&p.AccountID, &p.Email, &p.Name, &p.Role, &keyID, &p.KeyScopes)
	if err != nil {
		return accountPrincipal{}, err
	}
	p.AuthType = "api_key"
	p.APIKeyID = &keyID
	// Account API keys are account-scoped full access; never treat them as a
	// restricted team member. Set MemberRole explicitly to avoid empty-string
	// ambiguity (team endpoints are session-only anyway).
	p.MemberRole = "owner"
	_, _ = s.pool.Exec(ctx, `UPDATE account_api_keys SET last_used_at=now() WHERE id=$1`, keyID)
	return p, nil
}

type accountAuthRequest struct {
	Email        string `json:"email"`
	RedirectPath string `json:"redirect_path"`
}

func (s *Server) handleAccountAuthRequestLink(w http.ResponseWriter, r *http.Request) {
	var req accountAuthRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	email := normalizeAccountEmail(req.Email)
	if !looksLikeEmail(email) {
		util.WriteError(w, http.StatusBadRequest, "valid email is required")
		return
	}
	// Basic abuse protection: cap how many links a single requester IP or email
	// can trigger in a short window. Over-limit requests return the SAME neutral
	// OK as a normal request so this never reveals whether an email exists and
	// cannot be used to spray sign-in emails.
	if !s.authLinkLimiter.allow(requesterIP(r)) || !s.authLinkLimiter.allow("email:"+email) {
		util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
		return
	}
	redirectPath := sanitizeAccountRedirectPath(req.RedirectPath)
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin account auth tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	// Find-or-create the USER for this email. A user with zero memberships is a
	// brand-new sign-up: create a PERSONAL org (accounts row, is_personal=true,
	// name=email localpart) and an owner membership so the standard shape holds
	// from the first sign-in. The default org the link targets is the personal org
	// if present, else the earliest membership.
	var userID int64
	if err := tx.QueryRow(r.Context(), `
		INSERT INTO users (email, name)
		VALUES ($1, '')
		ON CONFLICT (email) DO UPDATE SET updated_at=now()
		RETURNING id
	`, email).Scan(&userID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("upsert user: %v", err))
		return
	}

	var accountID int64
	if err := tx.QueryRow(r.Context(), `
		SELECT o.id
		FROM memberships m
		JOIN accounts o ON o.id=m.org_id
		WHERE m.user_id=$1
		ORDER BY o.is_personal DESC, m.invited_at, o.id
		LIMIT 1
	`, userID).Scan(&accountID); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("resolve default org: %v", err))
			return
		}
		// Zero memberships: create the personal org + owner membership.
		if err := tx.QueryRow(r.Context(), `
			INSERT INTO accounts (email, name, role, status, is_personal)
			VALUES ($1, $2, $3, 'active', true)
			ON CONFLICT (email)
			DO UPDATE SET updated_at=now()
			RETURNING id
		`, email, emailLocalPart(email), accountRoleMember).Scan(&accountID); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("create personal org: %v", err))
			return
		}
		if _, err := tx.Exec(r.Context(), `
			INSERT INTO memberships (user_id, org_id, role, accepted_at)
			VALUES ($1, $2, 'owner', now())
			ON CONFLICT (user_id, org_id) DO NOTHING
		`, userID, accountID); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("seed owner membership: %v", err))
			return
		}
	}

	var status string
	if err := tx.QueryRow(r.Context(), `
		SELECT status FROM accounts WHERE id=$1
	`, accountID).Scan(&status); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load default org: %v", err))
		return
	}
	if status == "disabled" {
		_ = tx.Commit(r.Context())
		util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
		return
	}
	rawToken, err := generateSecret(32)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("generate magic link: %v", err))
		return
	}
	hash := hashSecret(rawToken)
	expiresAt := time.Now().UTC().Add(s.cfg.MagicLinkTTL)
	var linkID int64
	// member_email is kept populated (KEPT this phase) so a link minted now still
	// resolves through the legacy complete path if needed; user_id + target_org_id
	// are the forward-looking binding.
	if err := tx.QueryRow(r.Context(), `
		INSERT INTO account_magic_links (
			account_id, token_hash, purpose, redirect_path, requester_ip, user_agent, expires_at, member_email, user_id, target_org_id
		)
		VALUES ($1, $2, 'login', $3, $4, $5, $6, $7, $8, $9)
		RETURNING id
	`, accountID, hash, redirectPath, requesterIP(r), requestUserAgent(r), expiresAt, email, userID, accountID).Scan(&linkID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("insert magic link: %v", err))
		return
	}
	if err := s.insertAccountAuthEventTx(r.Context(), tx, accountID, nil, "magic_link_created", "account", email, map[string]any{
		"magic_link_id":  linkID,
		"redirect_path":  redirectPath,
		"requester_ip":   requesterIP(r),
		"request_origin": r.Header.Get("Origin"),
	}); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("insert account auth event: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit account auth tx: %v", err))
		return
	}

	linkURL, err := s.buildAccountMagicLink(rawToken)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("build magic link: %v", err))
		return
	}
	if err := s.sendAccountMagicLink(r.Context(), email, linkURL); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("send magic link email: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleAccountAuthComplete(w http.ResponseWriter, r *http.Request) {
	if redirectURL, ok := s.canonicalAccountAuthCompleteURL(r); ok {
		http.Redirect(w, r, redirectURL, http.StatusFound)
		return
	}
	rawToken := strings.TrimSpace(r.URL.Query().Get("token"))
	if rawToken == "" {
		log.Printf("account auth complete missing_token host=%s ip=%s ua=%q", strings.TrimSpace(r.Host), requesterIP(r), requestUserAgent(r))
		http.Redirect(w, r, "/account?error=missing_token", http.StatusFound)
		return
	}
	hash := hashSecret(rawToken)
	maskedToken := maskSecretForLog(rawToken)
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		log.Printf("account auth complete server_error token=%s host=%s ip=%s ua=%q err=%v", maskedToken, strings.TrimSpace(r.Host), requesterIP(r), requestUserAgent(r), err)
		http.Redirect(w, r, "/account?error=server_error", http.StatusFound)
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	var (
		linkID       int64
		orgID        int64
		userID       int64
		email        string
		isOperator   bool
		status       string
		expiresAt    time.Time
		redirectPath string
		memberEmail  *string
	)
	// Resolve the acting user + target org from the link. target_org_id/user_id are
	// the forward-looking binding (backfilled for pre-cutover links). The user's
	// email + operator flag drive verification and bootstrap.
	err = tx.QueryRow(r.Context(), `
		SELECT ml.id, o.id, u.id, u.email, u.is_operator, o.status, ml.expires_at, ml.redirect_path, ml.member_email
		FROM account_magic_links ml
		JOIN accounts o ON o.id=ml.target_org_id
		JOIN users u ON u.id=ml.user_id
		WHERE ml.token_hash=$1
	`, hash).Scan(&linkID, &orgID, &userID, &email, &isOperator, &status, &expiresAt, &redirectPath, &memberEmail)
	if err != nil {
		log.Printf("account auth complete invalid_token token=%s host=%s ip=%s ua=%q", maskedToken, strings.TrimSpace(r.Host), requesterIP(r), requestUserAgent(r))
		http.Redirect(w, r, "/account?error=invalid_token", http.StatusFound)
		return
	}
	if status != "active" {
		log.Printf("account auth complete disabled_account token=%s link_id=%d email=%s host=%s ip=%s ua=%q", maskedToken, linkID, email, strings.TrimSpace(r.Host), requesterIP(r), requestUserAgent(r))
		http.Redirect(w, r, "/account?error=account_disabled", http.StatusFound)
		return
	}
	if !expiresAt.After(time.Now().UTC()) {
		log.Printf("account auth complete expired_token token=%s link_id=%d email=%s expires_at=%s host=%s ip=%s ua=%q", maskedToken, linkID, email, expiresAt.UTC().Format(time.RFC3339), strings.TrimSpace(r.Host), requesterIP(r), requestUserAgent(r))
		http.Redirect(w, r, "/account?error=expired_token", http.StatusFound)
		return
	}
	if _, err := tx.Exec(r.Context(), `
		UPDATE users
		SET email_verified_at=COALESCE(email_verified_at, now()), updated_at=now()
		WHERE id=$1
	`, userID); err != nil {
		log.Printf("account auth complete verify_failed token=%s link_id=%d email=%s host=%s ip=%s ua=%q err=%v", maskedToken, linkID, email, strings.TrimSpace(r.Host), requesterIP(r), requestUserAgent(r), err)
		http.Redirect(w, r, "/account?error=server_error", http.StatusFound)
		return
	}
	// Mark the acting user's membership in the target org accepted on first sign-in.
	if _, err := tx.Exec(r.Context(), `
		UPDATE memberships
		SET accepted_at=COALESCE(accepted_at, now())
		WHERE user_id=$1 AND org_id=$2
	`, userID, orgID); err != nil {
		log.Printf("account auth complete accept_membership_failed token=%s link_id=%d email=%s host=%s ip=%s ua=%q err=%v", maskedToken, linkID, email, strings.TrimSpace(r.Host), requesterIP(r), requestUserAgent(r), err)
		http.Redirect(w, r, "/account?error=server_error", http.StatusFound)
		return
	}
	if s.shouldBootstrapAdmin(email) {
		var otherOperatorExists bool
		if err := tx.QueryRow(r.Context(), `
			SELECT EXISTS(
				SELECT 1
				FROM users
				WHERE is_operator=true AND id<>$1
			)
		`, userID).Scan(&otherOperatorExists); err != nil {
			http.Redirect(w, r, "/account?error=server_error", http.StatusFound)
			return
		}
		if !otherOperatorExists && !isOperator {
			if _, err := tx.Exec(r.Context(), `
				UPDATE users
				SET is_operator=true, updated_at=now()
				WHERE id=$1
			`, userID); err != nil {
				http.Redirect(w, r, "/account?error=server_error", http.StatusFound)
				return
			}
			isOperator = true
			if err := s.insertAccountAuthEventTx(r.Context(), tx, orgID, nil, "account_bootstrap_admin", "system", email, map[string]any{
				"bootstrap_admin_email": normalizeAccountEmail(s.cfg.BootstrapAdminEmail),
			}); err != nil {
				http.Redirect(w, r, "/account?error=server_error", http.StatusFound)
				return
			}
		}
	}
	rawSession, err := generateSecret(32)
	if err != nil {
		http.Redirect(w, r, "/account?error=server_error", http.StatusFound)
		return
	}
	sessionHash := hashSecret(rawSession)
	sessionExpiresAt := time.Now().UTC().Add(s.cfg.SessionTTL)
	var sessionID int64
	// account_id is kept populated (NOT NULL FK) as the org; current_org_id + user_id
	// are the authoritative binding session resolution keys on.
	if err := tx.QueryRow(r.Context(), `
		INSERT INTO account_sessions (account_id, session_hash, expires_at, last_used_at, member_email, user_id, current_org_id)
		VALUES ($1, $2, $3, now(), $4, $5, $6)
		RETURNING id
	`, orgID, sessionHash, sessionExpiresAt, memberEmail, userID, orgID).Scan(&sessionID); err != nil {
		http.Redirect(w, r, "/account?error=server_error", http.StatusFound)
		return
	}
	if err := s.insertAccountAuthEventTx(r.Context(), tx, orgID, nil, "session_created", "account", email, map[string]any{
		"session_id":    sessionID,
		"magic_link_id": linkID,
	}); err != nil {
		http.Redirect(w, r, "/account?error=server_error", http.StatusFound)
		return
	}
	if err := s.insertAccountAuthEventTx(r.Context(), tx, orgID, nil, "magic_link_used", "account", email, map[string]any{
		"magic_link_id":  linkID,
		"session_id":     sessionID,
		"redirect_path":  sanitizeAccountRedirectPath(redirectPath),
		"request_host":   strings.TrimSpace(r.Host),
		"requester_ip":   requesterIP(r),
		"request_origin": r.Header.Get("Origin"),
		"user_agent":     requestUserAgent(r),
		"token_prefix":   maskedToken,
	}); err != nil {
		http.Redirect(w, r, "/account?error=server_error", http.StatusFound)
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		log.Printf("account auth complete commit_failed token=%s link_id=%d email=%s session_id=%d host=%s ip=%s ua=%q err=%v", maskedToken, linkID, email, sessionID, strings.TrimSpace(r.Host), requesterIP(r), requestUserAgent(r), err)
		http.Redirect(w, r, "/account?error=server_error", http.StatusFound)
		return
	}
	setAccountSessionCookie(w, r, rawSession, sessionExpiresAt)
	finalRedirectPath := buildAccountPostAuthRedirectPath(redirectPath)
	log.Printf("account auth complete success token=%s link_id=%d email=%s session_id=%d redirect=%q host=%s ip=%s ua=%q", maskedToken, linkID, email, sessionID, finalRedirectPath, strings.TrimSpace(r.Host), requesterIP(r), requestUserAgent(r))
	http.Redirect(w, r, finalRedirectPath, http.StatusFound)
}

func (s *Server) handleAccountMe(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	hasBrowserSession := principal.SessionID != nil
	// has_keys_or_nodes gates the operator-only Developer/nodes link for non-admin
	// accounts that already enrolled nodes or minted API keys.
	var hasKeysOrNodes bool
	if err := s.pool.QueryRow(r.Context(), `
		SELECT EXISTS(SELECT 1 FROM account_api_keys WHERE account_id=$1 AND revoked_at IS NULL)
		    OR EXISTS(SELECT 1 FROM nodes WHERE account_id=$1)
	`, principal.AccountID).Scan(&hasKeysOrNodes); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load account flags: %v", err))
		return
	}
	// current_org + orgs power the topbar org switcher. Only browser sessions carry
	// a UserID; API-key callers get just the current org (no membership list).
	currentOrgName := principal.Name
	orgs := []map[string]any{}
	if principal.UserID > 0 {
		if err := s.pool.QueryRow(r.Context(), `
			SELECT name FROM accounts WHERE id=$1
		`, principal.AccountID).Scan(&currentOrgName); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load current org: %v", err))
			return
		}
		rows, err := s.pool.Query(r.Context(), `
			SELECT o.id, o.name, m.role
			FROM memberships m
			JOIN accounts o ON o.id=m.org_id
			WHERE m.user_id=$1 AND o.status='active'
			ORDER BY o.is_personal DESC, o.name, o.id
		`, principal.UserID)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load orgs: %v", err))
			return
		}
		defer rows.Close()
		for rows.Next() {
			var (
				id   int64
				name string
				role string
			)
			if err := rows.Scan(&id, &name, &role); err != nil {
				util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan org: %v", err))
				return
			}
			orgs = append(orgs, map[string]any{"id": id, "name": name, "role": role})
		}
		if err := rows.Err(); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate orgs: %v", err))
			return
		}
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"authenticated": true,
		"account": map[string]any{
			"id":                principal.AccountID,
			"email":             principal.Email,
			"name":              principal.Name,
			"role":              principal.Role,
			"auth_type":         principal.AuthType,
			"has_keys_or_nodes": hasKeysOrNodes,
		},
		"current_org": map[string]any{
			"id":                 principal.AccountID,
			"name":               currentOrgName,
			"member_role":        principal.MemberRole,
			"can_manage_billing": principalCanManageBilling(principal),
		},
		"orgs":         orgs,
		"capabilities": accountSessionCapabilities(principal),
		"session": map[string]any{
			"auth_type":       principal.AuthType,
			"browser_session": hasBrowserSession,
		},
	})
}

func (s *Server) handleAccountLogout(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if principal.SessionID != nil {
		_, _ = s.pool.Exec(r.Context(), `UPDATE account_sessions SET revoked_at=now() WHERE id=$1`, *principal.SessionID)
		_ = s.insertAccountAuthEvent(r.Context(), principal.AccountID, nil, "session_revoked", "account", principal.Email, map[string]any{
			"session_id": *principal.SessionID,
		})
	}
	clearAccountSessionCookie(w, r)
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleAccountAPIKeysList(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	rows, err := s.pool.Query(r.Context(), `
		SELECT id, key_prefix, label, scopes, expires_at, last_used_at, revoked_at, created_at
		FROM account_api_keys
		WHERE account_id=$1
		ORDER BY created_at DESC, id DESC
	`, principal.AccountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list account api keys: %v", err))
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
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan account api key: %v", err))
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
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate account api keys: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

type accountAPIKeyCreateRequest struct {
	Label     string `json:"label"`
	ExpiresAt string `json:"expires_at"`
}

func (s *Server) handleAccountAPIKeysCreate(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var req accountAPIKeyCreateRequest
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
	keyID, prefix, token, err := mintAccountAPIKey(r.Context(), s.pool, principal.AccountID, label, accountScopeRead, expiresAt)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("create account api key: %v", err))
		return
	}
	_ = s.insertAccountAuthEvent(r.Context(), principal.AccountID, &keyID, "api_key_created", "account", principal.Email, map[string]any{
		"label":      label,
		"key_prefix": prefix,
	})
	util.WriteJSON(w, http.StatusCreated, map[string]any{
		"id":         keyID,
		"key_prefix": prefix,
		"label":      label,
		"token":      token,
		"scopes":     []string{accountScopeRead},
		"expires_at": expiresAt,
	})
}

// accountAPIKeyMinter is the minimal querier mintAccountAPIKey needs, satisfied by
// both *pgxpool.Pool and pgx.Tx, so connection-create can mint inside one tx.
type accountAPIKeyMinter interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// mintAccountAPIKey generates a sir_ token, stores its hash under the given single
// scope, and returns (keyID, key_prefix, token). The token is shown ONCE by the
// caller and never persisted in cleartext. The caller writes the audit event.
func mintAccountAPIKey(ctx context.Context, q accountAPIKeyMinter, accountID int64, label, scope string, expiresAt *time.Time) (int64, string, string, error) {
	rawKey, err := generateSecret(36)
	if err != nil {
		return 0, "", "", fmt.Errorf("generate api key: %w", err)
	}
	token := "sir_" + rawKey
	hash := hashSecret(token)
	prefix := token
	if len(prefix) > 16 {
		prefix = prefix[:16]
	}
	var keyID int64
	if err := q.QueryRow(ctx, `
		INSERT INTO account_api_keys (account_id, key_prefix, secret_hash, label, scopes, expires_at)
		VALUES ($1, $2, $3, $4, ARRAY[$5]::text[], $6)
		RETURNING id
	`, accountID, prefix, hash, label, scope, expiresAt).Scan(&keyID); err != nil {
		return 0, "", "", err
	}
	return keyID, prefix, token, nil
}

func (s *Server) handleAccountAPIKeyRevoke(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	tag, err := s.revokeAccountAPIKey(r.Context(), id, principal.AccountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("revoke account api key: %v", err))
		return
	}
	if !tag {
		util.WriteError(w, http.StatusNotFound, "account api key not found")
		return
	}
	_ = s.insertAccountAuthEvent(r.Context(), principal.AccountID, &id, "api_key_revoked", "account", principal.Email, map[string]any{})
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleAdminAccountsList(w http.ResponseWriter, r *http.Request) {
	rows, err := s.pool.Query(r.Context(), `
		SELECT id, email, name, role, status, email_verified_at, created_at, updated_at
		FROM accounts
		ORDER BY created_at DESC, id DESC
		LIMIT 500
	`)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list accounts: %v", err))
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0, 64)
	for rows.Next() {
		var (
			id              int64
			email           string
			name            string
			role            string
			status          string
			emailVerifiedAt *time.Time
			createdAt       time.Time
			updatedAt       time.Time
		)
		if err := rows.Scan(&id, &email, &name, &role, &status, &emailVerifiedAt, &createdAt, &updatedAt); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan account: %v", err))
			return
		}
		items = append(items, map[string]any{
			"id":                id,
			"email":             email,
			"name":              name,
			"role":              role,
			"status":            status,
			"email_verified_at": emailVerifiedAt,
			"created_at":        createdAt.UTC(),
			"updated_at":        updatedAt.UTC(),
		})
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) handleAdminAccountDisable(w http.ResponseWriter, r *http.Request) {
	s.handleAdminAccountStatus(w, r, "disabled")
}

func (s *Server) handleAdminAccountEnable(w http.ResponseWriter, r *http.Request) {
	s.handleAdminAccountStatus(w, r, "active")
}

func (s *Server) handleAdminAccountPromote(w http.ResponseWriter, r *http.Request) {
	s.handleAdminAccountRole(w, r, "admin")
}

func (s *Server) handleAdminAccountDemote(w http.ResponseWriter, r *http.Request) {
	s.handleAdminAccountRole(w, r, "member")
}

func (s *Server) handleAdminAccountStatus(w http.ResponseWriter, r *http.Request, status string) {
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	ct, err := s.pool.Exec(r.Context(), `
		UPDATE accounts
		SET status=$2, updated_at=now()
		WHERE id=$1
	`, id, status)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("update account status: %v", err))
		return
	}
	if ct.RowsAffected() == 0 {
		util.WriteError(w, http.StatusNotFound, "account not found")
		return
	}
	_ = s.insertAccountAuthEvent(r.Context(), id, nil, "account_status_updated", "operator", "dashboard", map[string]any{"status": status})
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true, "status": status})
}

func (s *Server) handleAdminAccountRole(w http.ResponseWriter, r *http.Request, role string) {
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin account role update: %v", err))
		return
	}
	defer tx.Rollback(r.Context())
	ct, err := tx.Exec(r.Context(), `
		UPDATE accounts
		SET role=$2, updated_at=now()
		WHERE id=$1
	`, id, role)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("update account role: %v", err))
		return
	}
	if ct.RowsAffected() == 0 {
		util.WriteError(w, http.StatusNotFound, "account not found")
		return
	}
	_, err = tx.Exec(r.Context(), `
		UPDATE users
		SET is_operator = $2, updated_at = now()
		WHERE email = (
			SELECT lower(trim(email)) FROM accounts WHERE id=$1
		)
	`, id, role == accountRoleAdmin)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("update operator role: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit account role update: %v", err))
		return
	}
	_ = s.insertAccountAuthEvent(r.Context(), id, nil, "account_role_updated", "operator", "dashboard", map[string]any{"role": role})
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true, "role": role})
}

func (s *Server) handleAdminAccountAPIKeys(w http.ResponseWriter, r *http.Request) {
	accountID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	rows, err := s.pool.Query(r.Context(), `
		SELECT id, key_prefix, label, scopes, expires_at, last_used_at, revoked_at, created_at
		FROM account_api_keys
		WHERE account_id=$1
		ORDER BY created_at DESC, id DESC
	`, accountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list account api keys: %v", err))
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
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan account api key: %v", err))
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

func (s *Server) handleAdminAPIKeyRevoke(w http.ResponseWriter, r *http.Request) {
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	revoked, err := s.revokeAccountAPIKey(r.Context(), id, 0)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("revoke account api key: %v", err))
		return
	}
	if !revoked {
		util.WriteError(w, http.StatusNotFound, "account api key not found")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) revokeAccountAPIKey(ctx context.Context, keyID int64, accountID int64) (bool, error) {
	sql := `
		UPDATE account_api_keys
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

func setAccountSessionCookie(w http.ResponseWriter, r *http.Request, token string, expiresAt time.Time) {
	http.SetCookie(w, &http.Cookie{
		Name:     accountSessionCookie,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		Secure:   requestIsHTTPS(r),
		SameSite: http.SameSiteLaxMode,
		Expires:  expiresAt,
		MaxAge:   int(time.Until(expiresAt).Seconds()),
	})
}

func clearAccountSessionCookie(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     accountSessionCookie,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		Secure:   requestIsHTTPS(r),
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
		Expires:  time.Unix(0, 0),
	})
}

func normalizeAccountEmail(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

// emailLocalPart returns the part before '@', used as the default name for an
// auto-created personal org. Falls back to the whole (normalized) email if there
// is no '@'.
func emailLocalPart(email string) string {
	if i := strings.IndexByte(email, '@'); i > 0 {
		return email[:i]
	}
	return email
}

func (s *Server) shouldBootstrapAdmin(email string) bool {
	bootstrapEmail := normalizeAccountEmail(s.cfg.BootstrapAdminEmail)
	if bootstrapEmail == "" {
		return false
	}
	return normalizeAccountEmail(email) == bootstrapEmail
}

func looksLikeEmail(raw string) bool {
	return strings.Count(strings.TrimSpace(raw), "@") == 1
}

func sanitizeAccountRedirectPath(raw string) string {
	v := strings.TrimSpace(raw)
	if v == "" {
		return "/account"
	}
	if !strings.HasPrefix(v, "/") || strings.HasPrefix(v, "//") {
		return "/account"
	}
	u, err := url.Parse(v)
	if err != nil || u.IsAbs() || u.Host != "" || u.Path == "" {
		return "/account"
	}
	params := u.Query()
	for _, key := range []string{"auth", "error", "redirect_path", "token"} {
		params.Del(key)
	}
	u.RawQuery = params.Encode()
	u.Fragment = ""
	return u.String()
}

func buildAccountPostAuthRedirectPath(redirectPath string) string {
	u, err := url.Parse(sanitizeAccountRedirectPath(redirectPath))
	if err != nil {
		return "/account?auth=complete"
	}
	params := u.Query()
	params.Set("auth", "complete")
	u.RawQuery = params.Encode()
	return u.String()
}

func (s *Server) canonicalAccountAuthCompleteURL(r *http.Request) (string, bool) {
	base, err := url.Parse(strings.TrimRight(strings.TrimSpace(s.cfg.AppBaseURL), "/"))
	if err != nil || base.Scheme == "" || base.Host == "" {
		return "", false
	}
	if sameHost(strings.TrimSpace(r.Host), base.Host) && accountAuthCompleteSchemeMatches(r, base.Scheme) {
		return "", false
	}
	u := *r.URL
	u.Scheme = base.Scheme
	u.Host = base.Host
	return u.String(), true
}

func sameHost(a, b string) bool {
	ahost, _, err := net.SplitHostPort(a)
	if err == nil {
		a = ahost
	}
	bhost, _, err := net.SplitHostPort(b)
	if err == nil {
		b = bhost
	}
	return strings.EqualFold(a, b)
}

func accountAuthCompleteSchemeMatches(r *http.Request, scheme string) bool {
	switch strings.ToLower(strings.TrimSpace(scheme)) {
	case "https":
		return requestIsHTTPS(r)
	case "http":
		return !requestIsHTTPS(r)
	default:
		return false
	}
}

func maskSecretForLog(raw string) string {
	v := strings.TrimSpace(raw)
	if v == "" {
		return ""
	}
	if len(v) <= 10 {
		return v[:2] + "..." + v[len(v)-2:]
	}
	return v[:6] + "..." + v[len(v)-4:]
}

func (s *Server) buildAccountMagicLink(token string) (string, error) {
	base := strings.TrimRight(strings.TrimSpace(s.cfg.AppBaseURL), "/")
	if base == "" {
		log.Printf("account magic link build failed: AppBaseURL is empty")
		return "", fmt.Errorf("AppBaseURL is not configured")
	}
	return fmt.Sprintf("%s/auth/complete?token=%s", base, url.QueryEscape(token)), nil
}

func buildAccountMagicLinkEmail(emailAddr, linkURL string) email.Message {
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

func generateSecret(numBytes int) (string, error) {
	b := make([]byte, numBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return strings.TrimRight(base64.RawURLEncoding.EncodeToString(b), "="), nil
}

func hashSecret(raw string) string {
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

func (s *Server) insertAccountAuthEvent(ctx context.Context, accountID int64, apiKeyID *int64, eventType, actorType, actorRef string, detail map[string]any) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := s.insertAccountAuthEventTx(ctx, tx, accountID, apiKeyID, eventType, actorType, actorRef, detail); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Server) insertAccountAuthEventTx(ctx context.Context, tx pgx.Tx, accountID int64, apiKeyID *int64, eventType, actorType, actorRef string, detail map[string]any) error {
	var keyID any
	if apiKeyID != nil {
		keyID = *apiKeyID
	}
	b, err := json.Marshal(nonNilMap(detail))
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO account_auth_events (account_id, api_key_id, event_type, actor_type, actor_ref, detail_jsonb)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb)
	`, accountID, keyID, strings.TrimSpace(eventType), strings.TrimSpace(actorType), strings.TrimSpace(actorRef), string(b))
	return err
}

func (s *Server) sendAccountMagicLink(ctx context.Context, emailAddr, linkURL string) error {
	msg := buildAccountMagicLinkEmail(emailAddr, linkURL)
	msg.From = strings.TrimSpace(s.cfg.EmailFrom)
	msg.ReplyTo = strings.TrimSpace(s.cfg.EmailReplyTo)
	_, err := s.mailer.Send(ctx, msg)
	return err
}
