package api

import (
	"context"
	"crypto/subtle"
	"net/http"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

// nasCleanupControllerActor is the server-side identity recorded for a master
// flip. It comes from the authenticated controller principal, never from the
// request body; the operator-supplied name is stored only as claimed_by.
const nasCleanupControllerActor = "nas-cleanup-controller"

type cleanupControllerPrincipalKey struct{}

// requireCleanupControllerAuth fences the master control behind its own
// credential so the runner's service token cannot flip deletion authority.
// An unconfigured, colliding or incorrect credential fails closed: no request
// reaches the handler.
func (s *Server) requireCleanupControllerAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		expected := strings.TrimSpace(s.cfg.NASCleanupControllerToken)
		if expected == "" || expected == strings.TrimSpace(s.cfg.ServiceToken) || expected == strings.TrimSpace(s.cfg.APIToken) || expected == strings.TrimSpace(s.cfg.JoinedOperatorToken) {
			util.WriteError(w, http.StatusServiceUnavailable, "cleanup controller credential unavailable")
			return
		}
		authorization := strings.TrimSpace(r.Header.Get("Authorization"))
		if !strings.HasPrefix(authorization, "Bearer ") ||
			subtle.ConstantTimeCompare([]byte(strings.TrimSpace(strings.TrimPrefix(authorization, "Bearer "))), []byte(expected)) != 1 {
			util.WriteError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), cleanupControllerPrincipalKey{}, nasCleanupControllerActor)))
	})
}

func cleanupControllerActor(ctx context.Context) string {
	if ctx != nil {
		if actor, ok := ctx.Value(cleanupControllerPrincipalKey{}).(string); ok && actor != "" {
			return actor
		}
	}
	return nasCleanupControllerActor
}

// cleanupMasterOn reads the durable NAS cleanup deletion master switch live.
// Inside a transaction FOR SHARE holds it until commit, so a flip waits for
// every in-flight decision that read it. Any error or a missing row is OFF.
func cleanupMasterOn(ctx context.Context, q queryRower) bool {
	if tx, ok := q.(pgx.Tx); ok {
		return cleanupMasterOnTx(ctx, tx)
	}
	var on bool
	return q.QueryRow(ctx, `SELECT enabled FROM nas_cleanup_master FOR SHARE`).Scan(&on) == nil && on
}

// cleanupMasterOnTx reads the master switch inside a savepoint. A read error
// (or a missing row) rolls back only that savepoint, so the caller's
// transaction is never left aborted: read-only dry-run work and batch
// auto-pause can still commit. The decision treats a failed read as OFF, and a
// successful read keeps its FOR SHARE row lock until the transaction ends.
func cleanupMasterOnTx(ctx context.Context, tx pgx.Tx) bool {
	save, err := tx.Begin(ctx)
	if err != nil {
		return false
	}
	var on bool
	if err = save.QueryRow(ctx, `SELECT enabled FROM nas_cleanup_master FOR SHARE`).Scan(&on); err != nil {
		_ = save.Rollback(ctx)
		return false
	}
	if err = save.Commit(ctx); err != nil {
		return false
	}
	return on
}

// cleanupMasterLockedSQL reports whether this backend still holds the FOR SHARE
// row lock on the master control. to_regclass is NULL (never an error) when the
// table is missing, so an unavailable control fails closed without aborting the
// caller's transaction. A row lock taken after a savepoint is released by that
// savepoint's rollback, which is exactly what a cached decision must not
// outlive.
const cleanupMasterLockedSQL = `SELECT EXISTS(SELECT 1 FROM pg_locks
 WHERE locktype='relation' AND relation=to_regclass('nas_cleanup_master')
 AND mode='RowShareLock' AND granted AND pid=pg_backend_pid())`

// cleanupLockProbe answers a yes/no pg_locks question without endangering the
// caller's transaction: the probe runs in its own savepoint, so any error (a
// missing table, a permission problem, an aborted statement) rolls back only
// the probe and answers "not held". Every caller treats that as OFF.
func cleanupLockProbe(ctx context.Context, tx pgx.Tx, query string, args ...any) bool {
	save, err := tx.Begin(ctx)
	if err != nil {
		return false
	}
	var held bool
	if err = save.QueryRow(ctx, query, args...).Scan(&held); err != nil {
		_ = save.Rollback(ctx)
		return false
	}
	if save.Commit(ctx) != nil {
		return false
	}
	return held
}

// cleanupMasterLockHeld reports whether the master row lock is held on this
// connection right now.
func cleanupMasterLockHeld(ctx context.Context, tx pgx.Tx) bool {
	return cleanupLockProbe(ctx, tx, cleanupMasterLockedSQL)
}

// cleanupMasterMaybeOn is the cheap, non-blocking pre-check: it reads the
// switch in a savepoint without any row lock, so it can be used before the
// fences that an emergency OFF must not wait behind. It is advisory only; the
// authoritative decision still reads cleanupMasterOn last, immediately before
// the guarded write. Errors and a missing row are OFF.
func cleanupMasterMaybeOn(ctx context.Context, tx pgx.Tx) bool {
	save, err := tx.Begin(ctx)
	if err != nil {
		return false
	}
	var on bool
	if err = save.QueryRow(ctx, `SELECT enabled FROM nas_cleanup_master`).Scan(&on); err != nil {
		_ = save.Rollback(ctx)
		return false
	}
	if err = save.Commit(ctx); err != nil {
		return false
	}
	return on
}

type cleanupMasterState struct {
	Enabled   bool      `json:"enabled"`
	ChangedAt time.Time `json:"changed_at"`
	Actor     string    `json:"actor"`
	ClaimedBy string    `json:"claimed_by"`
	Reason    string    `json:"reason"`
}

const cleanupMasterStateSQL = `SELECT enabled,changed_at,actor,claimed_by,reason FROM nas_cleanup_master`

func scanCleanupMaster(row pgx.Row) (cleanupMasterState, error) {
	var m cleanupMasterState
	return m, row.Scan(&m.Enabled, &m.ChangedAt, &m.Actor, &m.ClaimedBy, &m.Reason)
}

// cleanupMasterEnabled reports the readable master state. A read failure is an
// error so reports never claim OFF when they simply could not look.
func cleanupMasterEnabled(ctx context.Context, q queryRower) (bool, error) {
	var on bool
	if err := q.QueryRow(ctx, `SELECT enabled FROM nas_cleanup_master`).Scan(&on); err != nil {
		return false, err
	}
	return on, nil
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
// controller CLI. It is idempotent: an unchanged state writes nothing. The
// actor is the authenticated controller principal; the body may only claim a
// name. The history row is appended by the table trigger in the same
// transaction.
func (s *Server) handleAdminNASCleanupMasterSet(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Enabled   *bool  `json:"enabled"`
		ClaimedBy string `json:"claimed_by"`
		Reason    string `json:"reason"`
	}
	if util.DecodeJSON(r, &req) != nil || req.Enabled == nil {
		util.WriteError(w, 400, "enabled, claimed_by and reason required")
		return
	}
	claimed := strings.TrimSpace(req.ClaimedBy)
	reason := strings.TrimSpace(req.Reason)
	if len(claimed) < 1 || len(claimed) > 200 || len(reason) < 1 || len(reason) > 500 {
		util.WriteError(w, 400, "enabled, claimed_by and reason required")
		return
	}
	actor := cleanupControllerActor(r.Context())
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
		after, err = scanCleanupMaster(tx.QueryRow(ctx, `UPDATE nas_cleanup_master SET enabled=$1,actor=$2,claimed_by=$3,reason=$4 RETURNING enabled,changed_at,actor,claimed_by,reason`, *req.Enabled, actor, claimed, reason))
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
