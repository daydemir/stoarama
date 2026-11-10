package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

// This endpoint consumes only an owned OBSERVE journal checkpoint. The client
// durably replaces observe with intent before any unlink. It must never call
// the ordinary observation handler here, even when no decision was recorded.
func (s *Server) handleAccountNASCleanupReconcile(w http.ResponseWriter, r *http.Request) {
	id, ok := parseInt64Path(w, r, "itemId")
	if !ok {
		return
	}
	var req cleanupObserve
	if util.DecodeJSON(r, &req) != nil || cleanupObserveRequestError(req) != "" {
		util.WriteError(w, 400, "invalid cleanup reconciliation observation")
		return
	}
	ctx, cancel := cleanupExecutionContext(r.Context())
	defer cancel()
	cid, ok := s.nasRestoreConnection(ctx, w, r)
	if !ok {
		return
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, 500, "cleanup reconciliation begin failed")
		return
	}
	defer tx.Rollback(ctx)
	var planID int64
	if err = tx.QueryRow(ctx, `SELECT i.plan_id FROM nas_cleanup_items i JOIN nas_cleanup_plans p ON p.id=i.plan_id WHERE i.id=$1 AND p.connection_id=$2`, id, cid).Scan(&planID); err != nil {
		status := 503
		if errors.Is(err, pgx.ErrNoRows) {
			status = 409
		}
		cleanupObservationFailure(w, ctx, status, "cleanup reconciliation identity unavailable")
		return
	}
	// Same lock order as issuance/close. A request still in flight must commit
	// or roll back before deciding that no permit was ever issued.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasCleanupPlanLockBase+planID); err != nil {
		cleanupObservationFailure(w, ctx, 503, "cleanup reconciliation fence unavailable")
		return
	}
	var sha, state string
	var attempt int
	var issued bool
	var oldProof []byte
	if err = tx.QueryRow(ctx, `SELECT p.plan_sha256,i.attempt,i.state,i.delete_ever_issued,i.proof FROM nas_cleanup_items i JOIN nas_cleanup_plans p ON p.id=i.plan_id WHERE i.id=$1 AND p.connection_id=$2 FOR UPDATE OF i FOR SHARE OF p`, id, cid).Scan(&sha, &attempt, &state, &issued, &oldProof); err != nil {
		cleanupObservationFailure(w, ctx, 503, "cleanup reconciliation state unavailable")
		return
	}
	if sha != req.PlanSHA || attempt != req.Attempt {
		util.WriteError(w, 409, "cleanup task identity differs")
		return
	}
	action := state
	if !issued && (state == "leased" || state == "pending") {
		// Keep approved proof/recovery and the monotonic attempt intact. A fresh
		// lease increments attempt, rejecting any delayed original observation.
		action = "pending"
		_, err = tx.Exec(ctx, `UPDATE nas_cleanup_items SET state='pending',lease_expires_at=NULL,lease_request_id=NULL,recheck_attempt_base=attempt,updated_at=now() WHERE id=$1`, id)
	} else if !nasSpaceAuditJSONEqual(oldProof, req.Proof) {
		util.WriteError(w, 409, "cleanup reconciliation proof differs")
		return
	} else if issued && state == "delete_issued" {
		// Observe proves no unlink by this journal owner, but already-issued
		// authority remains reserved until its explicit receipt is reconciled.
		action = "reconcile"
	}
	body, _ := json.Marshal(req)
	if err == nil {
		_, err = tx.Exec(ctx, `INSERT INTO nas_cleanup_reconciliations(item_id,attempt,observation,action) VALUES($1,$2,$3::jsonb,$4) ON CONFLICT DO NOTHING`, id, attempt, string(body), action)
	}
	if err != nil || tx.Commit(ctx) != nil {
		cleanupObservationFailure(w, ctx, 503, "cleanup reconciliation save failed")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"action": action, "delete": false, "item_id": id, "attempt": attempt, "plan_sha256": sha, "proof_sha256": cleanupSHA(req.Proof)})
}
