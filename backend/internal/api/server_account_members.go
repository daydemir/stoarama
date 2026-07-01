package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/util"
)

// Org members. A user's role within an org is a memberships row; a person can now
// belong to several orgs (multi-org). These endpoints operate on the CURRENT org
// (principal.AccountID) and gate on the org role (principal.MemberRole via
// memberships.role), which is fully separate from the platform operator flag
// (principal.Role via users.is_operator).

// pathEmailParam reads the {email} path param and URL-decodes it. chi routes on
// the raw (percent-encoded) path and returns URLParam values still encoded, so an
// email arriving as "user%40host" must be unescaped to "user@host" before it can
// pass looksLikeEmail. Applies to the member role and remove routes.
func pathEmailParam(r *http.Request) string {
	raw := chi.URLParam(r, "email")
	if decoded, err := url.PathUnescape(raw); err == nil {
		return decoded
	}
	return raw
}

type accountMemberInviteRequest struct {
	Email string `json:"email"`
}

type accountMemberItem struct {
	Email      string     `json:"email"`
	Role       string     `json:"role"`
	InvitedAt  time.Time  `json:"invited_at"`
	AcceptedAt *time.Time `json:"accepted_at"`
}

// handleAccountMembersList lists the current org's members. Visible to any member;
// can_manage tells the UI whether to show invite/remove controls.
func (s *Server) handleAccountMembersList(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	rows, err := s.pool.Query(r.Context(), `
		SELECT u.email, m.role, m.invited_at, m.accepted_at
		FROM memberships m
		JOIN users u ON u.id=m.user_id
		WHERE m.org_id=$1
		ORDER BY m.role DESC, m.invited_at
	`, principal.AccountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list members: %v", err))
		return
	}
	defer rows.Close()
	items := []accountMemberItem{}
	for rows.Next() {
		var it accountMemberItem
		if err := rows.Scan(&it.Email, &it.Role, &it.InvitedAt, &it.AcceptedAt); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan member: %v", err))
			return
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("read members: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"items":      items,
		"can_manage": principalIsOwner(principal),
	})
}

// handleAccountMembersInvite invites an email to the CURRENT org. Owner only.
// Multi-org is allowed: an email already in another org is fine; it is only a 409
// if that user is already a member of THIS org.
func (s *Server) handleAccountMembersInvite(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if !principalIsOwner(principal) {
		util.WriteError(w, http.StatusForbidden, "only an org owner can invite members")
		return
	}
	var req accountMemberInviteRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	email := normalizeAccountEmail(req.Email)
	if !looksLikeEmail(email) {
		util.WriteError(w, http.StatusBadRequest, "valid email is required")
		return
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin invite tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	// Find-or-create the invited USER (they may already exist in other orgs).
	var inviteeUserID int64
	if err := tx.QueryRow(r.Context(), `
		INSERT INTO users (email, name)
		VALUES ($1, '')
		ON CONFLICT (email) DO UPDATE SET updated_at=now()
		RETURNING id
	`, email).Scan(&inviteeUserID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("upsert invited user: %v", err))
		return
	}

	// Insert the membership in the current org. ON CONFLICT DO NOTHING makes a
	// re-invite of an existing member of THIS org a no-op we detect as a 409.
	var it accountMemberItem
	err = tx.QueryRow(r.Context(), `
		INSERT INTO memberships (user_id, org_id, role, invited_by)
		VALUES ($1, $2, 'member', $3)
		ON CONFLICT (user_id, org_id) DO NOTHING
		RETURNING role, invited_at, accepted_at
	`, inviteeUserID, principal.AccountID, principal.UserID).Scan(&it.Role, &it.InvitedAt, &it.AcceptedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusConflict, "that user is already a member of this org")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("insert membership: %v", err))
		return
	}
	it.Email = email

	// Issue a sign-in link bound to the current org, threading the invited email
	// through member_email plus user_id/target_org_id so the session resolves the
	// member's identity in this org.
	rawToken, linkID, err := s.createAccountMagicLinkTx(r.Context(), tx, principal.AccountID, email, inviteeUserID, "/account", requesterIP(r), requestUserAgent(r))
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("create invite link: %v", err))
		return
	}
	if err := s.insertAccountAuthEventTx(r.Context(), tx, principal.AccountID, nil, "member_invited", "account", email, map[string]any{
		"magic_link_id": linkID,
		"invited_by":    principal.UserID,
	}); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("insert invite event: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit invite tx: %v", err))
		return
	}

	linkURL, err := s.buildAccountMagicLink(rawToken)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("build invite link: %v", err))
		return
	}
	if err := s.sendAccountMagicLink(r.Context(), email, linkURL); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("send invite email: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusCreated, it)
}

// handleAccountMembersRemove removes a member from the CURRENT org and revokes
// only that member's sessions IN THIS ORG (their sessions in other orgs survive).
// Owner only. Refuses to remove the last remaining owner (which also blocks a sole
// owner removing themselves).
func (s *Server) handleAccountMembersRemove(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if !principalIsOwner(principal) {
		util.WriteError(w, http.StatusForbidden, "only an org owner can remove members")
		return
	}
	email := normalizeAccountEmail(pathEmailParam(r))
	if !looksLikeEmail(email) {
		util.WriteError(w, http.StatusBadRequest, "valid email is required")
		return
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin remove tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	var (
		targetUserID int64
		targetRole   string
	)
	if err := tx.QueryRow(r.Context(), `
		SELECT m.user_id, m.role
		FROM memberships m
		JOIN users u ON u.id=m.user_id
		WHERE m.org_id=$1 AND u.email=$2
	`, principal.AccountID, email).Scan(&targetUserID, &targetRole); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusNotFound, "member not found")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load member: %v", err))
		return
	}

	var ownerCount int
	if err := tx.QueryRow(r.Context(), `
		SELECT count(*) FROM memberships WHERE org_id=$1 AND role='owner'
	`, principal.AccountID).Scan(&ownerCount); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("count owners: %v", err))
		return
	}
	if !canRemoveMember(targetRole, ownerCount) {
		util.WriteError(w, http.StatusConflict, "cannot remove the last owner")
		return
	}

	if _, err := tx.Exec(r.Context(), `
		DELETE FROM memberships WHERE org_id=$1 AND user_id=$2
	`, principal.AccountID, targetUserID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("delete membership: %v", err))
		return
	}
	// Revoke only the removed user's sessions IN THIS ORG (matched on
	// current_org_id AND user_id), so their sessions in other orgs are untouched.
	if _, err := tx.Exec(r.Context(), `
		UPDATE account_sessions
		SET revoked_at=now()
		WHERE current_org_id=$1 AND user_id=$2 AND revoked_at IS NULL
	`, principal.AccountID, targetUserID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("revoke member sessions: %v", err))
		return
	}
	if err := s.insertAccountAuthEventTx(r.Context(), tx, principal.AccountID, nil, "member_removed", "account", email, map[string]any{
		"removed_by":      principal.UserID,
		"removed_user_id": targetUserID,
	}); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("insert remove event: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit remove tx: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// canRemoveMember returns false only when removing the target would orphan the
// account by leaving zero owners (target is the sole remaining owner).
func canRemoveMember(targetRole string, ownerCount int) bool {
	if targetRole == "owner" && ownerCount <= 1 {
		return false
	}
	return true
}

type accountMemberRoleRequest struct {
	Role string `json:"role"`
}

// validateMemberRoleChange decides whether an owner may set targetRole -> newRole.
// It gates the assignable billing role (billing_admin|member) so this endpoint can
// never mint or demote an owner: owner promotion/demotion stays owner-only and is
// out of scope here, so demoting the sole owner (which would orphan the org) is
// refused. Returns (ok, publicReason) where reason is empty on success.
func validateMemberRoleChange(targetRole, newRole string, ownerCount int) (bool, string) {
	if newRole != "member" && newRole != "billing_admin" {
		return false, "role must be member or billing_admin"
	}
	if targetRole == "owner" && ownerCount <= 1 {
		return false, "cannot change the role of the last owner"
	}
	return true, ""
}

// handleAccountMemberRoleSet sets a member's org role to member|billing_admin.
// Owner only. It never touches the owner role (promotion/demotion stays out of
// scope) and refuses to demote the last remaining owner, so an org always keeps an
// owner. Revokes nothing: a role change does not sign the member out.
func (s *Server) handleAccountMemberRoleSet(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if !principalIsOwner(principal) {
		util.WriteError(w, http.StatusForbidden, "only an org owner can change member roles")
		return
	}
	email := normalizeAccountEmail(pathEmailParam(r))
	if !looksLikeEmail(email) {
		util.WriteError(w, http.StatusBadRequest, "valid email is required")
		return
	}
	var req accountMemberRoleRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	newRole := strings.TrimSpace(req.Role)

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin role tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	var (
		targetUserID int64
		targetRole   string
	)
	if err := tx.QueryRow(r.Context(), `
		SELECT m.user_id, m.role
		FROM memberships m
		JOIN users u ON u.id=m.user_id
		WHERE m.org_id=$1 AND u.email=$2
	`, principal.AccountID, email).Scan(&targetUserID, &targetRole); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusNotFound, "member not found")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load member: %v", err))
		return
	}

	var ownerCount int
	if err := tx.QueryRow(r.Context(), `
		SELECT count(*) FROM memberships WHERE org_id=$1 AND role='owner'
	`, principal.AccountID).Scan(&ownerCount); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("count owners: %v", err))
		return
	}
	if ok, reason := validateMemberRoleChange(targetRole, newRole, ownerCount); !ok {
		util.WriteError(w, http.StatusConflict, reason)
		return
	}

	if _, err := tx.Exec(r.Context(), `
		UPDATE memberships SET role=$3 WHERE org_id=$1 AND user_id=$2
	`, principal.AccountID, targetUserID, newRole); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("update member role: %v", err))
		return
	}
	if err := s.insertAccountAuthEventTx(r.Context(), tx, principal.AccountID, nil, "member_role_changed", "account", email, map[string]any{
		"changed_by":      principal.UserID,
		"changed_user_id": targetUserID,
		"from_role":       targetRole,
		"to_role":         newRole,
	}); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("insert role event: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit role tx: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"email": email, "role": newRole})
}

// createAccountMagicLinkTx inserts a login magic link bound to org orgID for user
// userID, with the member_email threaded on (KEPT this phase), and returns the raw
// token + link id. Shared by member invites.
func (s *Server) createAccountMagicLinkTx(ctx context.Context, tx pgx.Tx, orgID int64, memberEmail string, userID int64, redirectPath, requesterIP, userAgent string) (string, int64, error) {
	rawToken, err := generateSecret(32)
	if err != nil {
		return "", 0, err
	}
	hash := hashSecret(rawToken)
	expiresAt := time.Now().UTC().Add(s.cfg.MagicLinkTTL)
	var linkID int64
	if err := tx.QueryRow(ctx, `
		INSERT INTO account_magic_links (
			account_id, token_hash, purpose, redirect_path, requester_ip, user_agent, expires_at, member_email, user_id, target_org_id
		)
		VALUES ($1, $2, 'login', $3, $4, $5, $6, $7, $8, $9)
		RETURNING id
	`, orgID, hash, sanitizeAccountRedirectPath(redirectPath), strings.TrimSpace(requesterIP), strings.TrimSpace(userAgent), expiresAt, memberEmail, userID, orgID).Scan(&linkID); err != nil {
		return "", 0, err
	}
	return rawToken, linkID, nil
}
