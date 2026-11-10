package api

import (
	"net/http"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/util"
)

// Close reuses the terminal retired state. It grants no deletion authority and
// never rewrites item/receipt evidence or removes hold rows. Migration 0170
// conservatively retains ALL recovery holds if any item ever issued a permit.
// Only never-issued file identities become available to a fresh dry-run plan.
func (s *Server) handleAdminNASCleanupClose(w http.ResponseWriter, r *http.Request) {
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req struct {
		PlanSHA string `json:"expected_plan_sha256"`
		Reason  string `json:"reason"`
	}
	if util.DecodeJSON(r, &req) != nil || len(req.PlanSHA) != 64 || !lowerHex(req.PlanSHA) || strings.TrimSpace(req.Reason) == "" || len(req.Reason) > 500 {
		util.WriteError(w, 400, "exact expected_plan_sha256 and reason (1..500 bytes) required")
		return
	}
	ctx := r.Context()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, 500, "begin cleanup close failed")
		return
	}
	defer tx.Rollback(ctx)
	// Match enable/batch order: public fence before plan locks. Observations
	// take the per-plan fence before any row lock; close drains those too.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasCleanupPublicFence); err == nil {
		_, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasCleanupPlanLockBase+id)
	}
	if err != nil {
		util.WriteError(w, 409, "cleanup close fence unavailable")
		return
	}
	var sha, mode string
	if err = tx.QueryRow(ctx, `SELECT plan_sha256,mode FROM nas_cleanup_plans WHERE id=$1 AND account_id=47 AND connection_id=13 FOR UPDATE`, id).Scan(&sha, &mode); err != nil || sha != req.PlanSHA {
		util.WriteError(w, 409, "plan identity differs")
		return
	}
	if mode != "paused" && mode != "retired" {
		util.WriteError(w, 409, "close requires a paused plan")
		return
	}
	var busy bool
	var issued, released int
	// Drain item writes too: lease issuance locks items without locking the
	// plan. Paused plans cannot acquire new work. Re-read the busy evidence
	// after those transactions commit; a newly committed live lease refuses
	// close. No item evidence is rewritten.
	rows, err := tx.Query(ctx, `SELECT id FROM nas_cleanup_items WHERE plan_id=$1 ORDER BY id FOR UPDATE`, id)
	if err != nil {
		util.WriteError(w, 409, "cleanup close item fence unavailable")
		return
	}
	for rows.Next() {
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		util.WriteError(w, 409, "cleanup close item fence unavailable")
		return
	}
	// Expired permits do not prove unlink did not happen: delete_issued always
	// blocks, and unverified stays unverified (held forever).
	err = tx.QueryRow(ctx, `SELECT coalesce(bool_or(state='delete_issued' OR (state='leased' AND lease_expires_at>now())),false),count(*) FILTER(WHERE delete_ever_issued),count(*) FILTER(WHERE NOT delete_ever_issued) FROM nas_cleanup_items WHERE plan_id=$1`, id).Scan(&busy, &issued, &released)
	if err != nil {
		util.WriteError(w, 500, "cleanup close evidence unavailable")
		return
	}
	if busy {
		util.WriteError(w, 409, "outstanding lease or delete_issued item; wait for leases and resolve receipts before close")
		return
	}
	if mode == "paused" {
		_, err = tx.Exec(ctx, `UPDATE nas_cleanup_plans SET mode='retired',approved_until=NULL,allow_nas_only=false,approval_reason=$2 WHERE id=$1`, id, req.Reason)
		if err == nil {
			_, err = tx.Exec(ctx, `UPDATE nas_cleanup_batches SET mode='paused',approved_until=NULL,approval_reason=left('closed plan '||$1::text||': '||$2::text,500) WHERE mode='enabled' AND $1::bigint=ANY(plan_ids)`, id, req.Reason)
		}
	}
	if err != nil || tx.Commit(ctx) != nil {
		util.WriteError(w, 500, "cleanup close failed")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"id": id, "plan_sha256": sha, "mode": "retired", "issued_items_held": issued, "never_issued_items": released, "deletion_enabled": false})
}
