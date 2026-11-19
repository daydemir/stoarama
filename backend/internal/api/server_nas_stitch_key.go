package api

import (
	"context"
	"net/http"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/util"
)

const (
	nasStitchAccountID     int64 = 47
	nasStitchKeyHashPrefix       = "nas-stitch:"
)

func isNASStitchKey(p accountPrincipal) bool {
	return p.APIKeyID != nil && slices.Contains(p.KeyScopes, string(accountScopeNASStitch))
}

func validNASStitchKey(p accountPrincipal) bool {
	return isNASStitchKey(p) && p.AccountID == nasStitchAccountID && len(p.KeyScopes) == 1
}

func nasStitchKeyPathAllowed(method, path string) bool {
	return method == http.MethodPost && (path == "/api/v1/account/connections/stitch/lease" || pullStitchTaskRe.MatchString(path))
}

func (s *Server) handleAdminNASStitchKeyMint(w http.ResponseWriter, r *http.Request) {
	p, ok := accountPrincipalFromContext(r.Context())
	if !ok || p.SessionID == nil || p.Role != accountRoleAdmin {
		util.WriteError(w, http.StatusForbidden, "operator session required")
		return
	}
	ctx := r.Context()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "could not begin stitch key issuance")
		return
	}
	defer tx.Rollback(ctx)
	const label = "Durham NAS stitch"
	id, prefix, token, err := mintAccountAPIKey(ctx, tx, nasStitchAccountID, label, accountScopeNASStitch, nil)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "could not mint the dedicated stitch key")
		return
	}
	if err := s.insertAccountAuthEventTx(ctx, tx, nasStitchAccountID, &id, "api_key_created", "operator", p.Email, map[string]any{
		"label": label, "key_prefix": prefix, "scope": accountScopeNASStitch,
	}); err != nil || tx.Commit(ctx) != nil {
		util.WriteError(w, http.StatusInternalServerError, "could not commit stitch key issuance")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	util.WriteJSON(w, http.StatusCreated, map[string]any{"id": id, "key_prefix": prefix, "token": token, "label": label, "scopes": []accountAPIKeyScope{accountScopeNASStitch}})
}

// Abandonment frees the shared lane after credential loss without accepting
// an absent receipt, changing the evidence, or making the hour eligible again.
func (s *Server) handleAdminNASStitchAbandonRegistration(w http.ResponseWriter, r *http.Request) {
	p, ok := accountPrincipalFromContext(r.Context())
	if !ok || p.SessionID == nil || p.Role != accountRoleAdmin {
		util.WriteError(w, http.StatusForbidden, "operator session required")
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req struct {
		PlanSHA string `json:"plan_sha256"`
		Reason  string `json:"reason"`
	}
	if util.DecodeJSON(r, &req) != nil || !stitchSHA(req.PlanSHA) {
		util.WriteError(w, http.StatusBadRequest, "exact stitch plan digest and reason required")
		return
	}
	req.Reason = strings.TrimSpace(req.Reason)
	if len(req.Reason) == 0 || len(req.Reason) > 500 {
		util.WriteError(w, http.StatusBadRequest, "stitch abandonment reason must be 1 to 500 bytes")
		return
	}

	ctx := r.Context()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, 503, "stitch recovery unavailable")
		return
	}
	defer tx.Rollback(ctx)
	var owner int64
	var oldReason *string
	err = tx.QueryRow(ctx, `SELECT lease_api_key_id,registration_abandoned_reason
 FROM nas_stitch_tasks WHERE id=$1 AND plan_sha256=$2 AND state='verified'
 AND cleanup_plan_id IS NULL AND lease_api_key_id IS NOT NULL FOR UPDATE`, id, req.PlanSHA).Scan(&owner, &oldReason)
	if err != nil {
		util.WriteError(w, 409, "no matching verified dedicated stitch task")
		return
	}
	caller := nasStitchCaller{keyID: owner, accountID: nasStitchAccountID, dedicated: true}
	active, err := caller.keyActive(ctx, tx)
	if err != nil || active {
		util.WriteError(w, 409, "stitch recovery requires a revoked or expired dedicated key")
		return
	}
	if oldReason != nil && *oldReason != req.Reason {
		util.WriteError(w, 409, "stitch abandonment already recorded")
		return
	}
	if oldReason == nil {
		if _, err = tx.Exec(ctx, `UPDATE nas_stitch_tasks SET registration_abandoned_at=clock_timestamp(),
 registration_abandoned_reason=$2,updated_at=clock_timestamp() WHERE id=$1`, id, req.Reason); err != nil {
			util.WriteError(w, 503, "stitch abandonment could not be recorded")
			return
		}
		err = s.insertAccountAuthEventTx(ctx, tx, nasStitchAccountID, &owner, "nas_stitch_registration_abandoned", "operator", p.Email, map[string]any{
			"task_id": id, "plan_sha256": req.PlanSHA, "reason": req.Reason, "operator_user_id": p.UserID,
		})
	}
	if err != nil || tx.Commit(ctx) != nil {
		util.WriteError(w, 503, "stitch abandonment could not be committed")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"id": id, "registration_abandoned": true})
}

type nasStitchCaller struct {
	keyID        int64
	accountID    int64
	connectionID int64
	dedicated    bool
}

func (s *Server) ordinaryNASStitchCaller(w http.ResponseWriter, r *http.Request) (nasStitchCaller, bool) {
	p, ok := accountPrincipalFromContext(r.Context())
	if !ok || p.APIKeyID == nil {
		util.WriteError(w, http.StatusForbidden, "stitch API key required")
		return nasStitchCaller{}, false
	}
	caller := nasStitchCaller{keyID: *p.APIKeyID, accountID: p.AccountID}
	if isNASStitchKey(p) {
		if !validNASStitchKey(p) {
			util.WriteError(w, http.StatusForbidden, "invalid dedicated stitch key scope")
			return nasStitchCaller{}, false
		}
		caller.dedicated = true
		return caller, true
	}
	cid, ok := s.nasRestoreConnection(r.Context(), w, r)
	if !ok {
		return nasStitchCaller{}, false
	}
	caller.connectionID = cid
	return caller, true
}

func (caller nasStitchCaller) allowed() bool {
	return caller.dedicated || caller.connectionID == 13
}

func (caller nasStitchCaller) owns(key *int64) bool {
	if key == nil {
		return caller.connectionID == 13
	}
	return caller.allowed() && *key == caller.keyID
}

// Hold the key row through the task commit. The clock is read after lock
// acquisition, so waiting for a concurrent revocation cannot revive a key.
func (caller nasStitchCaller) lockActive(ctx context.Context, tx pgx.Tx) bool {
	active, err := caller.keyActive(ctx, tx)
	return err == nil && active
}

func (caller nasStitchCaller) keyActive(ctx context.Context, tx pgx.Tx) (bool, error) {
	var active bool
	err := tx.QueryRow(ctx, `
WITH key AS MATERIALIZED (
 SELECT expires_at,revoked_at FROM account_api_keys
 WHERE id=$1 AND account_id=$2
   AND (NOT $3::boolean OR (scopes=ARRAY[$4]::text[] AND secret_hash ~ ('^'||$5||'[0-9a-f]{64}$')))
 FOR SHARE
)
SELECT revoked_at IS NULL AND (expires_at IS NULL OR expires_at>clock_timestamp()) FROM key`,
		caller.keyID, caller.accountID, caller.dedicated, string(accountScopeNASStitch), nasStitchKeyHashPrefix).Scan(&active)
	return active, err
}
