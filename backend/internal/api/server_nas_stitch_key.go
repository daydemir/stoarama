package api

import (
	"context"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/util"
)

const (
	nasStitchAccountID     int64 = 47
	nasStitchKeyHashPrefix       = "nas-stitch:"
	nasStitchKeyLabel            = "Durham NAS stitch"
	nasStitchKeyMintLock   int64 = 7343226011191
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
	key, err := issueDurhamStitchKey(r.Context(), s.pool, p.Email, "", durhamMintBrowser)
	if errors.Is(err, errDurhamStitchKeyActive) {
		util.WriteError(w, http.StatusConflict, "an active Durham stitch key already exists")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "Durham stitch key issuance unavailable")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	util.WriteJSON(w, http.StatusCreated, map[string]any{"id": key.id, "key_prefix": key.prefix,
		"token": key.token, "label": nasStitchKeyLabel, "scopes": []accountAPIKeyScope{accountScopeNASStitch}})
}

type durhamStitchMintMethod string

const (
	durhamMintBrowser durhamStitchMintMethod = "admin_session"
	durhamMintCLI     durhamStitchMintMethod = "stoaramactl"
)

var errDurhamStitchKeyActive = errors.New("an active Durham stitch key already exists")

type durhamStitchKey struct {
	id            int64
	prefix, token string
}

type durhamStitchMintDB interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

func issueDurhamStitchKey(
	ctx context.Context, db durhamStitchMintDB, actor, authorization string, method durhamStitchMintMethod,
) (durhamStitchKey, error) {
	var key durhamStitchKey
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return key, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasStitchKeyMintLock); err != nil {
		return key, err
	}
	var active bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM account_api_keys
 WHERE account_id=$1 AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at>clock_timestamp())
  AND ($2=ANY(scopes) OR starts_with(secret_hash,$3)))`, nasStitchAccountID, string(accountScopeNASStitch),
		nasStitchKeyHashPrefix).Scan(&active); err != nil {
		return key, err
	}
	if active {
		return key, errDurhamStitchKeyActive
	}
	var databaseActor string
	if err := tx.QueryRow(ctx, `SELECT current_user`).Scan(&databaseActor); err != nil {
		return key, err
	}
	key.id, key.prefix, key.token, err = mintAccountAPIKey(ctx, tx, nasStitchAccountID, nasStitchKeyLabel, accountScopeNASStitch, nil)
	if err != nil {
		return durhamStitchKey{}, err
	}
	if err := insertAccountAuthEventTx(ctx, tx, nasStitchAccountID, &key.id, "api_key_created", "operator", actor, map[string]any{
		"label": nasStitchKeyLabel, "key_prefix": key.prefix, "scope": accountScopeNASStitch,
		"method": method, "database_actor": databaseActor, "authorization_reference": authorization,
	}); err != nil {
		return durhamStitchKey{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return durhamStitchKey{id: key.id}, err
	}
	return key, nil
}

// MintDurhamStitchKeyTo writes only the newly committed token to the caller's
// private credential sink. The returned ID allows reconciliation on delivery
// or ambiguous commit failure. Only the operator CLI supplies this sink.
func MintDurhamStitchKeyTo(ctx context.Context, db durhamStitchMintDB, actor, authorization string, sink io.Writer) (int64, error) {
	actor, authorization = strings.TrimSpace(actor), strings.TrimSpace(authorization)
	if actor == "" || len(actor) > 200 || authorization == "" || len(authorization) > 500 || sink == nil {
		return 0, errors.New("operator identity, owner authorization and private sink required")
	}
	key, err := issueDurhamStitchKey(ctx, db, actor, authorization, durhamMintCLI)
	if err != nil {
		return key.id, err
	}
	if n, err := io.WriteString(sink, key.token+"\n"); err != nil || n != len(key.token)+1 {
		return key.id, errors.New("Durham credential delivery failed")
	}
	return key.id, nil
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
 AND cleanup_plan_id IS NULL AND lease_api_key_id IS NOT NULL AND lease_api_key_attempt=attempt FOR UPDATE`, id, req.PlanSHA).Scan(&owner, &oldReason)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, 409, "no matching verified dedicated stitch task")
		return
	}
	if err != nil {
		util.WriteError(w, 503, "stitch recovery unavailable")
		return
	}
	caller := nasStitchCaller{keyID: owner, accountID: nasStitchAccountID, dedicated: true}
	active, err := caller.keyActive(ctx, tx)
	if errors.Is(err, pgx.ErrNoRows) || active {
		util.WriteError(w, 409, "stitch recovery requires a revoked or expired dedicated key")
		return
	}
	if err != nil {
		util.WriteError(w, 503, "stitch recovery key lookup unavailable")
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
	if cid == 13 {
		var version string
		if err := s.pool.QueryRow(r.Context(), `SELECT client_version FROM connections WHERE id=$1`, cid).Scan(&version); err != nil {
			util.WriteError(w, 503, "stitch client version unavailable")
			return nasStitchCaller{}, false
		}
		if nasLowMemoryRawCopyOnly(version) {
			util.WriteError(w, 409, "low-memory client is copy-only")
			return nasStitchCaller{}, false
		}
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
