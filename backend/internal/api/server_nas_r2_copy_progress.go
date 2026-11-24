package api

import (
	"context"
	"log"
	"net/http"

	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

type NASR2CopyCompletion string

const (
	NASR2CopyVerifiedInPlace NASR2CopyCompletion = "verified_in_place"
	NASR2CopyDownloaded      NASR2CopyCompletion = "downloaded"
)

type NASR2CopyBlockReason string

const (
	NASR2CopyVerifyMissing    NASR2CopyBlockReason = "verify_missing"
	NASR2CopySourceChanged    NASR2CopyBlockReason = "source_changed"
	NASR2CopyR2Identity       NASR2CopyBlockReason = "r2_identity"
	NASR2CopyFinalConflict    NASR2CopyBlockReason = "final_conflict"
	NASR2CopySidecarConflict  NASR2CopyBlockReason = "sidecar_conflict"
	NASR2CopyPartialConflict  NASR2CopyBlockReason = "partial_conflict"
	NASR2CopyPathConflict     NASR2CopyBlockReason = "path_conflict"
	NASR2CopyChecksumMismatch NASR2CopyBlockReason = "checksum_mismatch"
)

func (reason NASR2CopyBlockReason) client() bool {
	switch reason {
	case NASR2CopyVerifyMissing, NASR2CopyFinalConflict, NASR2CopySidecarConflict, NASR2CopyPartialConflict, NASR2CopyPathConflict, NASR2CopyChecksumMismatch:
		return true
	default:
		return false
	}
}
func blockNASR2Copy(ctx context.Context, tx pgx.Tx, id int64, reason NASR2CopyBlockReason) error {
	_, err := tx.Exec(ctx, `UPDATE nas_r2_copy_items SET state='blocked',blocked_reason=$2,last_error=$2,lease_token=NULL,lease_expires_at=NULL,range_until=NULL,range_id=NULL,updated_at=clock_timestamp() WHERE id=$1`, id, reason)
	return err
}
func (s *Server) nasR2CopyBlockResponse(w http.ResponseWriter, r *http.Request, tx pgx.Tx, id int64, reason NASR2CopyBlockReason) {
	if err := blockNASR2Copy(r.Context(), tx, id, reason); err != nil {
		util.WriteError(w, 503, "copy block failed")
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, 503, "copy block commit failed")
		return
	}
	log.Printf("R2 copy blocked id=%d reason=%s; key protected", id, reason)
	util.WriteJSON(w, 200, map[string]any{"task": nil, "blocked": true, "id": id, "reason": reason})
}

// Failure reporting deliberately does not reload source identity, policy,
// maintenance or resource gates: none may prevent persisting a custody conflict.
// The exact owning token must still match; expired-but-unreassigned owners may
// latch their evidence, while a reassigned token never can. A fast lease clears
// earlier fast tokens at once; the client keeps its local latch and reports it
// with the item's next token.
func (s *Server) handleNASR2CopyBlock(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.nasR2CopyConnection(w, r, nasConnectionMetadata); !ok {
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req nasR2CopyControl
	if util.DecodeJSON(r, &req) != nil || req.Token == "" || !req.Reason.client() {
		util.WriteError(w, 400, "invalid copy block reason")
		return
	}
	tx, err := BeginNASR2CopyTx(r.Context(), s.pool)
	if err != nil {
		util.WriteError(w, 503, "copy database unavailable")
		return
	}
	defer tx.Rollback(r.Context())
	var owned bool
	err = tx.QueryRow(r.Context(), `SELECT true FROM nas_r2_copy_items WHERE id=$1 AND connection_id=13 AND state IN ('leased','refresh_leased') AND lease_token=$2 FOR UPDATE`, id, req.Token).Scan(&owned)
	if err != nil || !owned {
		util.WriteError(w, 409, "copy block owner changed")
		return
	}
	s.nasR2CopyBlockResponse(w, r, tx, id, req.Reason)
}
