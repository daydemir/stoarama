package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

const deliveryNASHandoffCheckPath = "/api/v1/account/collated/handoff-check"
const deliveryNASHandoffCheckMaxBytes = 8 << 20

// Both methods use the active roster and its latest readback round. The
// challenge is issued only after that round has every exact NAS receipt.
const deliveryNASHandoffActiveCheckSQL = `
 FROM delivery_nas_handoff_checks q
 JOIN connections c ON c.id=13 AND c.delivery_nas_handoff_enabled
  AND c.delivery_nas_handoff_roster_sha256=q.roster_sha256
 JOIN delivery_nas_handoff_mounts m ON m.roster_sha256=q.roster_sha256
 WHERE NOT EXISTS(SELECT 1 FROM delivery_nas_handoff_rounds newer
  WHERE newer.roster_sha256=q.roster_sha256 AND newer.round>q.round)
 AND NOT EXISTS(SELECT 1 FROM delivery_nas_handoff_checks superseded
  WHERE superseded.roster_sha256=q.roster_sha256 AND superseded.round=q.round AND superseded.id>q.id)`

type deliveryNASHandoffCheckResult struct {
	ID           int64    `json:"id"`
	ManifestSHA  string   `json:"manifest_sha256"`
	Mount        [2]int64 `json:"mount"`
	FailedOutput int64    `json:"failed_output_id,omitempty"`
}

func (s *Server) handleAccountDeliveryNASHandoffCheck(w http.ResponseWriter, r *http.Request) {
	connection, _, ok := s.collatedPullConnection(w, r)
	if !ok {
		return
	}
	if connection != 13 {
		util.WriteError(w, http.StatusForbidden, "NAS handoff check requires connection 13")
		return
	}
	ctx := r.Context()
	if r.Method == http.MethodGet {
		var result struct {
			deliveryNASHandoffCheckResult
			Files json.RawMessage `json:"files"`
		}
		err := s.pool.QueryRow(ctx, `
 SELECT q.id,q.manifest_sha256,m.device,m.inode,q.manifest
 `+deliveryNASHandoffActiveCheckSQL+` AND q.checked_at IS NULL`).Scan(
			&result.ID, &result.ManifestSHA, &result.Mount[0], &result.Mount[1], &result.Files)
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteJSON(w, http.StatusOK, map[string]any{"check": nil})
			return
		}
		if err != nil || len(result.Files) > deliveryNASHandoffCheckMaxBytes {
			util.WriteError(w, http.StatusServiceUnavailable, "read NAS handoff path check")
			return
		}
		util.WriteJSON(w, http.StatusOK, map[string]any{"check": result})
		return
	}
	var req deliveryNASHandoffCheckResult
	if err := util.DecodeJSON(r, &req); err != nil || req.ID <= 0 || req.FailedOutput < 0 ||
		len(req.ManifestSHA) != 64 || !lowerHex(req.ManifestSHA) || req.Mount[0] < 0 || req.Mount[1] <= 0 {
		util.WriteError(w, http.StatusBadRequest, "invalid NAS handoff path check result")
		return
	}
	tag, err := s.pool.Exec(ctx, `
 WITH active AS (
  SELECT q.id `+deliveryNASHandoffActiveCheckSQL+`
   AND q.id=$1 AND q.manifest_sha256=$2 AND m.device=$3 AND m.inode=$4
   AND ($5::bigint=0 OR EXISTS(SELECT 1 FROM delivery_nas_handoff_outputs p
     WHERE p.output_id=$5 AND p.roster_sha256=q.roster_sha256))
 )
 UPDATE delivery_nas_handoff_checks q SET checked_at=clock_timestamp(),failed_output_id=nullif($5,0)
 FROM active WHERE q.id=active.id AND q.checked_at IS NULL`,
		req.ID, req.ManifestSHA, req.Mount[0], req.Mount[1], req.FailedOutput)
	if err != nil {
		util.WriteError(w, http.StatusServiceUnavailable, "record NAS handoff path check")
		return
	}
	if tag.RowsAffected() == 0 {
		var same bool
		err = s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 `+deliveryNASHandoffActiveCheckSQL+`
 AND q.id=$1 AND q.manifest_sha256=$2 AND m.device=$3 AND m.inode=$4 AND q.checked_at IS NOT NULL
 AND q.failed_output_id IS NOT DISTINCT FROM nullif($5::bigint,0))`,
			req.ID, req.ManifestSHA, req.Mount[0], req.Mount[1], req.FailedOutput).Scan(&same)
		if err != nil || !same {
			util.WriteError(w, http.StatusConflict, "NAS handoff path check is no longer current")
			return
		}
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}
