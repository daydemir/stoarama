package api

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/util"
)

// Org switching + org creation for a signed-in user. An org IS an accounts row;
// a user's orgs are its memberships. These endpoints are browser-session only
// (they need principal.UserID, which API keys do not carry).

type accountOrgItem struct {
	ID       int64  `json:"id"`
	Name     string `json:"name"`
	Role     string `json:"role"`
	Personal bool   `json:"is_personal"`
	Current  bool   `json:"is_current"`
}

// handleAccountOrgsList lists the orgs the caller is a member of, flagging the
// current one.
func (s *Server) handleAccountOrgsList(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok || principal.UserID == 0 {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	rows, err := s.pool.Query(r.Context(), `
		SELECT o.id, o.name, m.role, o.is_personal
		FROM memberships m
		JOIN accounts o ON o.id=m.org_id
		WHERE m.user_id=$1 AND o.status='active'
		ORDER BY o.is_personal DESC, o.name, o.id
	`, principal.UserID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list orgs: %v", err))
		return
	}
	defer rows.Close()
	items := []accountOrgItem{}
	for rows.Next() {
		var it accountOrgItem
		if err := rows.Scan(&it.ID, &it.Name, &it.Role, &it.Personal); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan org: %v", err))
			return
		}
		it.Current = it.ID == principal.AccountID
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("read orgs: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

// handleAccountOrgSwitch points the caller's session at a different org. It only
// succeeds if the caller has a membership in the target org (else 403), so a user
// can never switch into an org they do not belong to.
func (s *Server) handleAccountOrgSwitch(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok || principal.UserID == 0 || principal.SessionID == nil {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	orgID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var isMember bool
	if err := s.pool.QueryRow(r.Context(), `
		SELECT EXISTS(
			SELECT 1
			FROM memberships m
			JOIN accounts o ON o.id=m.org_id
			WHERE m.user_id=$1 AND m.org_id=$2 AND o.status='active'
		)
	`, principal.UserID, orgID).Scan(&isMember); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("verify org membership: %v", err))
		return
	}
	if !isMember {
		util.WriteError(w, http.StatusForbidden, "not a member of that org")
		return
	}
	// account_id is kept in lockstep with current_org_id so both stay the org id.
	if _, err := s.pool.Exec(r.Context(), `
		UPDATE account_sessions
		SET current_org_id=$2, account_id=$2, updated_at=now()
		WHERE id=$1
	`, *principal.SessionID, orgID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("switch org: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true, "org_id": orgID})
}

type accountOrgCreateRequest struct {
	Name string `json:"name"`
}

// handleAccountOrgCreate creates a new TEAM org (is_personal=false) owned by the
// caller. The accounts row still needs a UNIQUE email, so the team org is keyed on
// a synthetic org-scoped address derived from the caller's user id; billing keys
// on the accounts.id (org id) exactly as for personal orgs.
func (s *Server) handleAccountOrgCreate(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok || principal.UserID == 0 {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var req accountOrgCreateRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		util.WriteError(w, http.StatusBadRequest, "org name is required")
		return
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin create org tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	// accounts.email is UNIQUE NOT NULL; a team org has no real deliverable address,
	// so generate a unique, non-deliverable placeholder. A random suffix guarantees
	// uniqueness even if the same owner makes two orgs with the same name.
	suffix, err := generateSecret(6)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("generate org id: %v", err))
		return
	}
	var orgID int64
	if err := tx.QueryRow(r.Context(), `
		INSERT INTO accounts (email, name, role, status, is_personal)
		VALUES ($1, $2, $3, 'active', false)
		RETURNING id
	`, orgEmailPlaceholder(principal.UserID, name, suffix), name, accountRoleMember).Scan(&orgID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("create org: %v", err))
		return
	}
	if _, err := tx.Exec(r.Context(), `
		INSERT INTO memberships (user_id, org_id, role, accepted_at)
		VALUES ($1, $2, 'owner', now())
	`, principal.UserID, orgID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("seed owner membership: %v", err))
		return
	}
	if err := s.insertAccountAuthEventTx(r.Context(), tx, orgID, nil, "org_created", "account", principal.Email, map[string]any{
		"created_by_user_id": principal.UserID,
		"name":               name,
	}); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("insert org event: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit create org tx: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusCreated, map[string]any{"id": orgID, "name": name})
}

// orgEmailPlaceholder builds a unique, non-deliverable address for a team org's
// accounts.email (which is UNIQUE NOT NULL). It embeds the owner user id, an
// org-name slug, and a random suffix so it is human-legible in the accounts table
// and never collides with a real user's personal-org email or another team org.
func orgEmailPlaceholder(userID int64, orgName, suffix string) string {
	return fmt.Sprintf("org+%d-%s@%s.stoarama.internal", userID, strings.ToLower(suffix), orgSlug(orgName))
}

// orgSlug lowercases and reduces an org name to [a-z0-9-] so it is safe inside the
// placeholder host. Empty result falls back to "team".
func orgSlug(name string) string {
	var b strings.Builder
	prevDash := false
	for _, r := range strings.ToLower(strings.TrimSpace(name)) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			prevDash = false
		default:
			if !prevDash && b.Len() > 0 {
				b.WriteByte('-')
				prevDash = true
			}
		}
	}
	slug := strings.Trim(b.String(), "-")
	if slug == "" {
		return "team"
	}
	return slug
}
