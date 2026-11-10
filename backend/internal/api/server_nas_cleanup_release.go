package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/util"
)

// These exact diagnostics identify paths whose durable result was written
// before any unlink. A boolean alone, a legacy generic error, or an interrupted
// intent is insufficient. In particular pre_unlink_failure is not allowlisted.
func cleanupNoUnlinkReceipt(raw []byte, attempt int, planSHA, proofSHA string) bool {
	var receipt cleanupReceipt
	if json.Unmarshal(raw, &receipt) != nil || !validCleanupReceipt(receipt) || receipt.Outcome != "unverified" ||
		receipt.UnlinkAttempted == nil || *receipt.UnlinkAttempted || receipt.Attempt != attempt ||
		receipt.PlanSHA != planSHA || receipt.ProofSHA != proofSHA || !lowerHex(proofSHA) {
		return false
	}
	switch receipt.RefusalReason {
	case "observation_replayed":
		// The owner CAS holds the exact observe snapshot through its conversion
		// to result. Any possible unlink would have replaced observe with intent.
		return receipt.Error == "recovered observation permit was not executed"
	case "deadline_exceeded":
		return receipt.Error == "cleanup observation deadline exceeded (30 s); not unlinked"
	case "permit_expired":
		return receipt.Error == "cleanup deletion permit expired; not unlinked"
	case "proof_mismatch":
		return receipt.Error == "cleanup deletion permit does not match exact current proof; not unlinked"
	case "execution_disabled":
		return receipt.Error == "cleanup deletion execution is disabled; not unlinked"
	case "local_guard":
		return strings.HasPrefix(receipt.Error, "client deletion guard refused; not unlinked: ")
	}
	return false
}

// Releasing a reservation is not rescheduling an issued attempt. Require close
// first so no path can re-enable the original plan; a different plan must do a
// new dry run and obtain owner approval. No item/receipt/hold is modified.
func (s *Server) handleAdminNASCleanupReleaseUnexecuted(w http.ResponseWriter, r *http.Request) {
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
		util.WriteError(w, 500, "cleanup release begin failed")
		return
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasCleanupPlanLockBase+id); err != nil {
		util.WriteError(w, 409, "cleanup release fence unavailable")
		return
	}
	var sha, mode string
	if err = tx.QueryRow(ctx, `SELECT plan_sha256,mode FROM nas_cleanup_plans WHERE id=$1 AND account_id=47 AND connection_id=13 FOR UPDATE`, id).Scan(&sha, &mode); err != nil || sha != req.PlanSHA {
		util.WriteError(w, 409, "plan identity differs")
		return
	}
	if mode != "retired" {
		util.WriteError(w, 409, "close the paused plan before releasing reservations")
		return
	}
	type candidate struct {
		id       int64
		attempt  int
		proofSHA string
		receipt  []byte
		expires  *time.Time
		released bool
	}
	candidates := []candidate{}
	rows, err := tx.Query(ctx, `SELECT i.id,i.attempt,coalesce(i.proof_sha256,''),i.outcome,i.permit_expires_at,u.item_id IS NOT NULL FROM nas_cleanup_items i LEFT JOIN nas_cleanup_unexecuted_releases u ON u.item_id=i.id WHERE i.plan_id=$1 AND i.delete_ever_issued AND i.state='unverified' ORDER BY i.id FOR UPDATE OF i`, id)
	if err != nil {
		util.WriteError(w, 500, "cleanup release evidence unavailable")
		return
	}
	for rows.Next() {
		var c candidate
		if err = rows.Scan(&c.id, &c.attempt, &c.proofSHA, &c.receipt, &c.expires, &c.released); err != nil {
			break
		}
		candidates = append(candidates, c)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		util.WriteError(w, 500, "cleanup release evidence unavailable")
		return
	}
	released := []int64{}
	protected := []int64{}
	for _, c := range candidates {
		if c.expires == nil || !c.expires.Before(time.Now()) || !cleanupNoUnlinkReceipt(c.receipt, c.attempt, sha, c.proofSHA) {
			protected = append(protected, c.id)
			continue
		}
		if !c.released {
			_, err = tx.Exec(ctx, `INSERT INTO nas_cleanup_unexecuted_releases(item_id,plan_sha256,attempt,receipt,reason) VALUES($1,$2,$3,$4::jsonb,$5)`, c.id, sha, c.attempt, string(c.receipt), req.Reason)
			if err != nil {
				util.WriteError(w, 500, "cleanup reservation release failed")
				return
			}
		}
		released = append(released, c.id)
	}
	if len(released) == 0 {
		util.WriteJSON(w, 409, map[string]any{"error": "no provably unexecuted items; reservations remain protected", "protected_item_ids": protected})
		return
	}
	if tx.Commit(ctx) != nil {
		util.WriteError(w, 500, "cleanup reservation release failed")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"id": id, "plan_sha256": sha, "released_item_ids": released, "protected_item_ids": protected, "deletion_enabled": false})
}
