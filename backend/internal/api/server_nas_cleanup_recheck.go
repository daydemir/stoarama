package api

import (
	"log"
	"net/http"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/util"
)

// Recheck only queues new read-only observations. It never arms a plan or
// changes approval evidence. Paused plans must explicitly resume their dry run
// through the existing control (which restores source holds) first.
func (s *Server) handleAdminNASCleanupRecheck(w http.ResponseWriter, r *http.Request) {
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req struct {
		PlanSHA string `json:"expected_plan_sha256"`
		Reason  string `json:"reason"`
		Scope   string `json:"scope"`
	}
	if err := util.DecodeJSON(r, &req); err != nil || len(req.PlanSHA) != 64 || !lowerHex(req.PlanSHA) || strings.TrimSpace(req.Reason) == "" || len(req.Reason) > 500 {
		util.WriteError(w, 400, "exact expected_plan_sha256 and reason (1..500 bytes) required")
		return
	}
	if req.Scope == "" {
		req.Scope = "unverified"
	}
	if req.Scope != "unverified" && req.Scope != "all" {
		util.WriteError(w, 400, "scope must be unverified or all")
		return
	}
	ctx := r.Context()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, 500, "begin cleanup recheck failed")
		return
	}
	defer tx.Rollback(ctx)
	// Same canonical scope as plan control: drain in-flight observations
	// before resetting any item evidence.
	if err = cleanupLockPlans(ctx, tx, []int64{id}); err != nil {
		util.WriteError(w, 409, "cleanup recheck fence unavailable")
		return
	}
	var sha, mode, binding string
	if err = tx.QueryRow(ctx, `SELECT plan_sha256,mode,report_sha256 FROM nas_cleanup_plans WHERE id=$1 AND account_id=47 AND connection_id=13 FOR UPDATE`, id).Scan(&sha, &mode, &binding); err != nil || sha != req.PlanSHA {
		util.WriteError(w, 409, "plan identity differs")
		return
	}
	if mode != "dry_run" || binding != cleanupPolicySHA {
		util.WriteError(w, 409, "recheck requires a dry_run plan bound to the current cleanup policy")
		return
	}
	var issued bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nas_cleanup_items WHERE plan_id=$1 AND delete_ever_issued)`, id).Scan(&issued); err != nil {
		util.WriteError(w, 500, "cleanup recheck evidence unavailable")
		return
	}
	if issued {
		util.WriteError(w, 409, "a deletion was previously issued; reconcile instead")
		return
	}
	// Keep attempt monotonic to reject old observation/receipt replays. The
	// baseline grants three new expired-lease attempts for this observation
	// cycle. Clear prior proof/recovery so only a full new dry run can enable.
	result, err := tx.Exec(ctx, `UPDATE nas_cleanup_items SET state='pending',
 recheck_attempt_base=attempt,lease_expires_at=NULL,lease_request_id=NULL,
 proof=NULL,proof_sha256=NULL,recovery='unchecked',recovery_evidence=NULL,
 policy_reason='',outcome=NULL,stage_timings='{}',updated_at=now()
 WHERE plan_id=$1 AND (state='unverified' OR (state='would_delete' AND ($2='all' OR recovery NOT IN ('r2_sha_verified','r2_md5_etag_matched'))))`, id, req.Scope)
	if err != nil || tx.Commit(ctx) != nil {
		util.WriteError(w, 500, "cleanup recheck failed")
		return
	}
	// %q escapes newlines/control characters in operator-supplied reasons.
	// Do not overwrite approval_reason: this action grants no approval.
	log.Printf("nas cleanup recheck plan_id=%d plan_sha256=%s scope=%s reset_items=%d reason=%q", id, sha, req.Scope, result.RowsAffected(), req.Reason)
	util.WriteJSON(w, 200, map[string]any{"id": id, "plan_sha256": sha, "mode": mode, "scope": req.Scope, "reset_items": result.RowsAffected(), "deletion_enabled": false})
}
