package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/daydemir/stoarama/backend/internal/util"
)

// Team members. A LEAN multi-user layer over the single-account model: a person
// belongs to exactly one account (member_email is globally unique), so sign-in
// stays unambiguous. These endpoints gate on the TEAM role (principal.MemberRole
// via account_members.role), which is fully separate from the operator/admin
// flag (principal.Role via accounts.role).

type accountMemberInviteRequest struct {
	Email string `json:"email"`
}

type accountMemberItem struct {
	Email      string     `json:"email"`
	Role       string     `json:"role"`
	InvitedAt  time.Time  `json:"invited_at"`
	AcceptedAt *time.Time `json:"accepted_at"`
}

// handleAccountMembersList lists the caller account's team members. Visible to
// any member; can_manage tells the UI whether to show invite/remove controls.
func (s *Server) handleAccountMembersList(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	rows, err := s.pool.Query(r.Context(), `
		SELECT member_email, role, invited_at, accepted_at
		FROM account_members
		WHERE account_id=$1
		ORDER BY role DESC, invited_at
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

// handleAccountMembersInvite invites an email to the caller's team. Owner only.
// Global uniqueness is enforced: an email that already belongs to any account or
// team is rejected with 409 (multi-team membership is out of scope for now).
func (s *Server) handleAccountMembersInvite(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if !principalIsOwner(principal) {
		util.WriteError(w, http.StatusForbidden, "only a team owner can invite members")
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

	// Global-uniqueness pre-check: an email that is already any account or member
	// cannot be invited (the DB UNIQUE(member_email) is the backstop below).
	var taken bool
	if err := tx.QueryRow(r.Context(), `
		SELECT EXISTS(SELECT 1 FROM accounts WHERE email=$1)
		    OR EXISTS(SELECT 1 FROM account_members WHERE member_email=$1)
	`, email).Scan(&taken); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("check email uniqueness: %v", err))
		return
	}
	if taken {
		util.WriteError(w, http.StatusConflict, "that email already belongs to an account or team (multi-team membership is not supported)")
		return
	}

	var it accountMemberItem
	if err := tx.QueryRow(r.Context(), `
		INSERT INTO account_members (account_id, member_email, role, invited_by)
		VALUES ($1, $2, 'member', $3)
		RETURNING member_email, role, invited_at, accepted_at
	`, principal.AccountID, email, principal.AccountID).Scan(&it.Email, &it.Role, &it.InvitedAt, &it.AcceptedAt); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			util.WriteError(w, http.StatusConflict, "that email already belongs to an account or team (multi-team membership is not supported)")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("insert member: %v", err))
		return
	}

	// Issue a sign-in link bound to the caller's account, threading the invited
	// email through member_email so the session resolves the member's identity.
	rawToken, linkID, err := s.createAccountMagicLinkTx(r.Context(), tx, principal.AccountID, email, "/account", requesterIP(r), requestUserAgent(r))
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("create invite link: %v", err))
		return
	}
	if err := s.insertAccountAuthEventTx(r.Context(), tx, principal.AccountID, nil, "member_invited", "account", email, map[string]any{
		"magic_link_id": linkID,
		"invited_by":    principal.AccountID,
	}); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("insert invite event: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit invite tx: %v", err))
		return
	}

	linkURL := s.buildAccountMagicLink(r, rawToken)
	if err := s.sendAccountMagicLink(r.Context(), email, linkURL); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("send invite email: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusCreated, it)
}

// handleAccountMembersRemove removes a member from the caller's team and revokes
// their active sessions immediately. Owner only. Refuses to remove the last
// remaining owner (which also blocks a sole owner removing themselves).
func (s *Server) handleAccountMembersRemove(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if !principalIsOwner(principal) {
		util.WriteError(w, http.StatusForbidden, "only a team owner can remove members")
		return
	}
	email := normalizeAccountEmail(chi.URLParam(r, "email"))
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

	var targetRole string
	if err := tx.QueryRow(r.Context(), `
		SELECT role FROM account_members WHERE account_id=$1 AND member_email=$2
	`, principal.AccountID, email).Scan(&targetRole); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusNotFound, "member not found")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load member: %v", err))
		return
	}

	var ownerCount int
	if err := tx.QueryRow(r.Context(), `
		SELECT count(*) FROM account_members WHERE account_id=$1 AND role='owner'
	`, principal.AccountID).Scan(&ownerCount); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("count owners: %v", err))
		return
	}
	if !canRemoveMember(targetRole, ownerCount) {
		util.WriteError(w, http.StatusConflict, "cannot remove the last owner")
		return
	}

	if _, err := tx.Exec(r.Context(), `
		DELETE FROM account_members WHERE account_id=$1 AND member_email=$2
	`, principal.AccountID, email); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("delete member: %v", err))
		return
	}
	// Revoke only the removed member's sessions (matched on account_id AND
	// member_email), so legacy owner sessions (member_email NULL) are untouched.
	if _, err := tx.Exec(r.Context(), `
		UPDATE account_sessions
		SET revoked_at=now()
		WHERE account_id=$1 AND member_email=$2 AND revoked_at IS NULL
	`, principal.AccountID, email); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("revoke member sessions: %v", err))
		return
	}
	if err := s.insertAccountAuthEventTx(r.Context(), tx, principal.AccountID, nil, "member_removed", "account", email, map[string]any{
		"removed_by": principal.AccountID,
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

// createAccountMagicLinkTx inserts a login magic link bound to accountID with the
// given member_email threaded onto it, and returns the raw token + link id. This
// is the shared link-issuing core used by both self-signup and member invites.
func (s *Server) createAccountMagicLinkTx(ctx context.Context, tx pgx.Tx, accountID int64, memberEmail, redirectPath, requesterIP, userAgent string) (string, int64, error) {
	rawToken, err := generateSecret(32)
	if err != nil {
		return "", 0, err
	}
	hash := hashSecret(rawToken)
	expiresAt := time.Now().UTC().Add(s.cfg.MagicLinkTTL)
	var linkID int64
	if err := tx.QueryRow(ctx, `
		INSERT INTO account_magic_links (
			account_id, token_hash, purpose, redirect_path, requester_ip, user_agent, expires_at, member_email
		)
		VALUES ($1, $2, 'login', $3, $4, $5, $6, $7)
		RETURNING id
	`, accountID, hash, sanitizeAccountRedirectPath(redirectPath), strings.TrimSpace(requesterIP), strings.TrimSpace(userAgent), expiresAt, memberEmail).Scan(&linkID); err != nil {
		return "", 0, err
	}
	return rawToken, linkID, nil
}
