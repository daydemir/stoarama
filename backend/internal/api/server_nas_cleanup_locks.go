package api

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// Global cleanup lock order (including receipt/observation batches, controls,
// expiry, close, advance and lease): public advisory fence (exclusive for new
// authority, shared otherwise), ascending plan advisory fences, ascending plan
// rows, ascending batch rows, then item rows. Take the strongest required plan
// lock up front: an item writer must never upgrade a shared parent lock.
// Lease/budget advisory locks follow plan/batch scope locks; no later path
// may acquire a new parent scope while holding them.
// Resolve immutable membership without row locks, then lock the entire scope
// before re-reading mutable mode/approval/receipt evidence in a new statement.
// Batch insertion requires the exclusive public fence; its membership cannot
// change. The shared fence keeps this resolution stable without serializing
// independent plans or blocking public readers. Admission inserts new identities
// under its separate admission fence and never locks existing cleanup children.
func cleanupLockPlans(ctx context.Context, tx pgx.Tx, source []int64) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared($1)`, nasCleanupPublicFence); err != nil {
		return err
	}
	var plans []int64
	if err := tx.QueryRow(ctx, `SELECT coalesce(array_agg(id ORDER BY id),'{}') FROM (
 SELECT unnest($1::bigint[]) id UNION SELECT unnest(plan_ids) FROM nas_cleanup_batches WHERE plan_ids && $1) scope`, source).Scan(&plans); err != nil {
		return err
	}
	if err := cleanupDrainLocks(ctx, tx, `WITH ordered AS MATERIALIZED (SELECT unnest($1::bigint[]) id ORDER BY id)
 SELECT pg_advisory_xact_lock($2+id) FROM ordered`, plans, nasCleanupPlanLockBase); err != nil {
		return err
	}
	for _, lock := range []struct {
		query string
		ids   []int64
	}{
		{`SELECT id FROM nas_cleanup_plans WHERE id=ANY($1) ORDER BY id FOR UPDATE`, plans},
		{`SELECT id FROM nas_cleanup_batches WHERE plan_ids && $1 ORDER BY id FOR UPDATE`, source},
	} {
		if err := cleanupDrainLocks(ctx, tx, lock.query, lock.ids); err != nil {
			return err
		}
	}
	return nil
}

func cleanupLockConnection(ctx context.Context, tx pgx.Tx, cid int64) ([]int64, error) {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared($1)`, nasCleanupPublicFence); err != nil {
		return nil, err
	}
	var plans []int64
	// Retired is terminal: without an outstanding lease it has no queue or
	// expiry work. Locking that entire immutable history on every serial lease
	// made cleanup progressively slower. Keep paused plans for resume/retirement
	// and every leased item for expiry, regardless of mode or policy. Batch
	// membership is still expanded by cleanupLockPlans before taking any rows.
	if err := tx.QueryRow(ctx, `SELECT coalesce(array_agg(p.id ORDER BY p.id),'{}') FROM nas_cleanup_plans p WHERE connection_id=$1
 AND (p.mode<>'retired' OR EXISTS(SELECT 1 FROM nas_cleanup_items i WHERE i.plan_id=p.id AND i.state='leased'))`, cid).Scan(&plans); err != nil {
		return nil, err
	}
	return plans, cleanupLockPlans(ctx, tx, plans)
}

func cleanupLockItems(ctx context.Context, tx pgx.Tx, cid int64, ids []int64) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared($1)`, nasCleanupPublicFence); err != nil {
		return err
	}
	var plans []int64
	if err := tx.QueryRow(ctx, `SELECT coalesce(array_agg(DISTINCT p.id ORDER BY p.id),'{}') FROM nas_cleanup_items i JOIN nas_cleanup_plans p ON p.id=i.plan_id WHERE i.id=ANY($1) AND p.connection_id=$2`, ids, cid).Scan(&plans); err != nil {
		return err
	}
	if err := cleanupLockPlans(ctx, tx, plans); err != nil {
		return err
	}
	return cleanupDrainLocks(ctx, tx, `SELECT i.id FROM nas_cleanup_items i JOIN nas_cleanup_plans p ON p.id=i.plan_id WHERE i.id=ANY($1) AND p.connection_id=$2 ORDER BY i.id FOR UPDATE OF i`, ids, cid)
}

func cleanupDrainLocks(ctx context.Context, tx pgx.Tx, query string, args ...any) error {
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
	}
	return rows.Err()
}
