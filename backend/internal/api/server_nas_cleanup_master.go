package api

import (
	"context"
	"net/http"
	"time"

	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

// cleanupMasterOn reads the durable NAS cleanup deletion master switch live.
// Inside a transaction FOR SHARE holds it until commit, so a flip waits for
// every in-flight decision that read it. Any error or a missing row is OFF.
func cleanupMasterOn(ctx context.Context, q queryRower) bool {
	var on bool
	return q.QueryRow(ctx, `SELECT enabled FROM nas_cleanup_master FOR SHARE`).Scan(&on) == nil && on
}

type cleanupMasterState struct {
	Enabled   bool      `json:"enabled"`
	ChangedAt time.Time `json:"changed_at"`
	Actor     string    `json:"actor"`
	Reason    string    `json:"reason"`
}

const cleanupMasterStateSQL = `SELECT enabled,changed_at,actor,reason FROM nas_cleanup_master`

func scanCleanupMaster(row pgx.Row) (cleanupMasterState, error) {
	var m cleanupMasterState
	return m, row.Scan(&m.Enabled, &m.ChangedAt, &m.Actor, &m.Reason)
}

func (s *Server) handleAdminNASCleanupMasterStatus(w http.ResponseWriter, r *http.Request) {
	m, err := scanCleanupMaster(s.pool.QueryRow(r.Context(), cleanupMasterStateSQL))
	if err != nil {
		util.WriteError(w, 500, "cleanup master control unavailable")
		return
	}
	util.WriteJSON(w, 200, m)
}

// handleAdminNASCleanupMasterSet flips the master switch for the audited
// operator CLI. It is idempotent: an unchanged state writes nothing. The
// history row is appended by the table trigger in the same transaction.
func (s *Server) handleAdminNASCleanupMasterSet(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Enabled *bool  `json:"enabled"`
		Actor   string `json:"actor"`
		Reason  string `json:"reason"`
	}
	if util.DecodeJSON(r, &req) != nil || req.Enabled == nil || len(req.Actor) < 1 || len(req.Actor) > 200 || len(req.Reason) < 1 || len(req.Reason) > 500 {
		util.WriteError(w, 400, "enabled, actor and reason required")
		return
	}
	ctx := r.Context()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, 500, "begin cleanup master change failed")
		return
	}
	defer tx.Rollback(ctx)
	before, err := scanCleanupMaster(tx.QueryRow(ctx, cleanupMasterStateSQL+` FOR UPDATE`))
	if err != nil {
		util.WriteError(w, 500, "cleanup master control unavailable")
		return
	}
	after := before
	if before.Enabled != *req.Enabled {
		after, err = scanCleanupMaster(tx.QueryRow(ctx, `UPDATE nas_cleanup_master SET enabled=$1,actor=$2,reason=$3 RETURNING enabled,changed_at,actor,reason`, *req.Enabled, req.Actor, req.Reason))
		if err == nil {
			err = tx.Commit(ctx)
		}
		if err != nil {
			util.WriteError(w, 500, "cleanup master change failed")
			return
		}
	}
	util.WriteJSON(w, 200, map[string]any{"before": before, "after": after, "changed": before.Enabled != after.Enabled})
}
