package api

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestNASCleanupHistoryScopePreservesExpiryAndPausedFences(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256,mode)
 SELECT n,47,13,'history-'||n,repeat('a',64),'[]',repeat('b',64),
 CASE WHEN n=1 THEN 'dry_run' WHEN n IN (2,3) THEN 'paused' ELSE 'retired' END
 FROM generate_series(1,1399) n;
 INSERT INTO nas_cleanup_items(id,plan_id,ordinal,state,attempt,lease_expires_at) VALUES
 (1,1,1,'pending',0,NULL),(3,3,1,'leased',1,now()-interval '1 minute'),
 (4,4,1,'leased',3,now()-interval '1 minute');`); err != nil {
		t.Fatal(err)
	}
	// A receipt/control holding an irrelevant retired plan cannot stall the
	// execution queue. Check both the canonical advisory fence and row lock.
	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(ctx)
	if err = cleanupLockPlans(ctx, blocker, []int64{1399}); err != nil {
		t.Fatal(err)
	}
	bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tx, err := pool.Begin(bounded)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	plans, err := cleanupLockConnection(bounded, tx, 13)
	if err != nil || !reflect.DeepEqual(plans, []int64{1, 2, 3, 4}) {
		t.Fatalf("queue scope %v: %v", plans, err)
	}
	// Paused/no-lease plans remain fenced for the superseded retirement path.
	probe, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Rollback(ctx)
	var acquired bool
	if err = probe.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1)`, nasCleanupPlanLockBase+2).Scan(&acquired); err != nil || acquired {
		t.Fatalf("paused plan lost canonical fence: %t %v", acquired, err)
	}
	if _, err = tx.Exec(bounded, cleanupExpireLeasesSQL, int64(13), "expired fixture", plans); err != nil {
		t.Fatal(err)
	}
	var states []string
	if err = tx.QueryRow(bounded, `SELECT array_agg(state ORDER BY id) FROM nas_cleanup_items`).Scan(&states); err != nil || !reflect.DeepEqual(states, []string{"pending", "pending", "unverified"}) {
		t.Fatalf("paused/retired lease expiry %v: %v", states, err)
	}
}

// Run the old and new lock scopes against the observed production history
// size. Log timings rather than asserting a host-dependent performance ratio.
func TestNASCleanupHistoryLockCost(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256,mode)
 SELECT n,47,13,'history-'||n,repeat('a',64),'[]',repeat('b',64),
 CASE WHEN n<=4 THEN 'paused' ELSE 'retired' END FROM generate_series(1,1399) n`); err != nil {
		t.Fatal(err)
	}
	measure := func(lock func(pgx.Tx) error) time.Duration {
		t.Helper()
		started := time.Now()
		for n := 0; n < 20; n++ {
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			err = lock(tx)
			tx.Rollback(ctx)
			if err != nil {
				t.Fatal(err)
			}
		}
		return time.Since(started) / 20
	}
	old := measure(func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared($1)`, nasCleanupPublicFence); err != nil {
			return err
		}
		var plans []int64
		if err := tx.QueryRow(ctx, `SELECT array_agg(id ORDER BY id) FROM nas_cleanup_plans WHERE connection_id=13`).Scan(&plans); err != nil {
			return err
		}
		return cleanupLockPlans(ctx, tx, plans)
	})
	current := measure(func(tx pgx.Tx) error { _, err := cleanupLockConnection(ctx, tx, 13); return err })
	t.Logf("1399 plans / 1395 retired: old scope %s, current scope %s per call", old, current)
}

func TestNASCleanupHistoryScopeStillPausesExpiredExecutionBatch(t *testing.T) {
	f := newCleanupFixture(t, 24)
	first, sha, ids := f.admit("expiry-current", syntheticFiles(100, 1, 2))
	next, nextSHA, _ := f.admit("expiry-next", syntheticFiles(200, 1, 2))
	f.dryRun()
	f.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := f.enableBatch([]int64{first, next}, []string{sha, nextSHA}, 2); code != 200 {
		t.Fatalf("enable: %d %s", code, body)
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET state='leased',attempt=2,lease_expires_at=now()-interval '1 minute' WHERE id=$1`, ids[0]); err != nil {
		t.Fatal(err)
	}
	if err := f.s.cleanupExpireLeases(f.ctx, 13); err != nil {
		t.Fatal(err)
	}
	if mode, _ := f.planMode(first); mode != "paused" {
		t.Fatalf("expired execution still %s", mode)
	}
	if mode, _ := f.batchMode(); mode != "paused" {
		t.Fatalf("expired batch still %s", mode)
	}
}
