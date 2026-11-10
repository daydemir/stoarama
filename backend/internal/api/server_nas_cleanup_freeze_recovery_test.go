package api

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// Complete the real freeze publication/drain/replay, then lose the activation
// transaction before enable. Deliberately discard its in-memory release callback.
func abandonCleanupActivation(t *testing.T, f *cleanupFixture, id int64) int64 {
	t.Helper()
	tx, err := f.pool.BeginTx(f.ctx, nasCleanupAdmissionOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(f.ctx)
	if _, err = tx.Exec(f.ctx, `SELECT pg_advisory_xact_lock($1)`, nasCleanupPublicFence); err != nil {
		t.Fatal(err)
	}
	var attempt int64
	if err = tx.QueryRow(f.ctx, `SELECT txid_current() FROM nas_cleanup_plans WHERE id=$1 FOR UPDATE`, id).Scan(&attempt); err != nil {
		t.Fatal(err)
	}
	if _, err = f.s.cleanupActivateRankings(f.ctx, tx, id); err != nil {
		t.Fatal(err)
	}
	var owner int64
	var created time.Time
	var validated bool
	if err = f.pool.QueryRow(f.ctx, `SELECT activation_attempt_id,created_at,validated FROM nas_cleanup_frozen_streams WHERE plan_id=$1`, id).Scan(&owner, &created, &validated); err != nil || owner != attempt || created.IsZero() || validated {
		t.Fatalf("publication ownership not durable before enable: owner=%d attempt=%d created=%v validated=%v err=%v", owner, attempt, created, validated, err)
	}
	// Recovery must skip an activation even when its committed plan is still
	// dry_run and its freeze looks abandoned. No elapsed-time heuristic is safe.
	if err = f.s.cleanupRecoverFreezes(f.ctx); err != nil {
		t.Fatal(err)
	}
	requireCleanupFreezeCount(t, f, id, 1)
	if err = tx.Rollback(f.ctx); err != nil {
		t.Fatal(err)
	}
	return attempt
}

func requireCleanupFreezeCount(t *testing.T, f *cleanupFixture, id int64, want int) {
	t.Helper()
	var count int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_cleanup_frozen_streams WHERE plan_id=$1`, id).Scan(&count); err != nil || count != want {
		t.Fatalf("plan %d freezes=%d want=%d err=%v", id, count, want, err)
	}
}

func TestNASCleanupSourceFreezeRecoversAbandonedActivation(t *testing.T) {
	for _, recovery := range []string{"housekeeping", "next-single-enable", "next-batch-enable"} {
		t.Run(recovery, func(t *testing.T) {
			f := newCleanupFixture(t, 24)
			id, sha, _ := f.admit("abandoned", syntheticFiles(100, 1, 10))
			f.dryRun()
			attempt := abandonCleanupActivation(t, f, id)
			requireCleanupFreezeCount(t, f, id, 1)
			_, err := f.pool.Exec(f.ctx, `INSERT INTO recording_clips(id,recording_id) VALUES(999,2)`)
			requireCleanupFrozen(t, err)
			f.s = &Server{pool: f.pool, r2: f.s.r2, cfg: f.s.cfg, nasCleanupReportCache: f.s.nasCleanupReportCache}
			if recovery == "housekeeping" {
				f.s.cleanupHousekeeping(f.ctx, 13)
				requireCleanupFreezeCount(t, f, id, 0)
				if _, err = f.pool.Exec(f.ctx, `INSERT INTO recording_clips(id,recording_id) VALUES(999,2)`); err != nil {
					t.Fatalf("recovery did not restore ingest: %v", err)
				}
				return
			}
			// Also recover a different abandoned owner, so overwriting just the
			// current plan's generation cannot accidentally satisfy this test.
			if _, err = f.pool.Exec(f.ctx, `INSERT INTO nas_cleanup_frozen_streams(plan_id,stream_id) VALUES(999999,4)`); err != nil {
				t.Fatal(err)
			}
			f.s.cfg.NASCleanupDeletionEnabled = true
			code, body := 0, ""
			if recovery == "next-single-enable" {
				code, body = safeguardControl(t, f.s, id, "enabled", map[string]any{"plan_sha256": sha})
			} else {
				code, body = f.enableBatch([]int64{id}, []string{sha}, 2)
			}
			if code != 200 {
				t.Fatalf("next activation: %d %s", code, body)
			}
			requireCleanupFreezeCount(t, f, 999999, 0)
			var owner int64
			if err = f.pool.QueryRow(f.ctx, `SELECT activation_attempt_id FROM nas_cleanup_frozen_streams WHERE plan_id=$1 AND validated`, id).Scan(&owner); err != nil || owner == attempt {
				t.Fatalf("new attempt did not replace abandoned ownership: %d %v", owner, err)
			}
			f.s.cleanupHousekeeping(f.ctx, 13)
			requireCleanupFreezeCount(t, f, id, 1)
		})
	}
}

func TestNASCleanupSourceFreezeEnabledRestartWithColdCache(t *testing.T) {
	f, id, tasks := cleanupFenceFixture(t, true)
	// Retain only the fixture's synthetic reviewed-report substitute; actual
	// execution rankings and freeze ownership must reload from the database.
	f.s = &Server{pool: f.pool, r2: f.s.r2, cfg: f.s.cfg, nasCleanupReportCache: f.s.nasCleanupReportCache}
	f.s.cleanupHousekeeping(f.ctx, 13)
	requireCleanupFreezeCount(t, f, id, 1)
	if result := f.observe(tasks, map[int64]string{tasks[0].ItemID: strings.Repeat("a", 32)})[0]; !result.Delete {
		t.Fatalf("enabled restart failed to replay cold cache: %+v", result)
	}
	var cache string
	if err := f.pool.QueryRow(f.ctx, `SELECT stage_timings->>'rankings_cache' FROM nas_cleanup_items WHERE id=$1`, tasks[0].ItemID).Scan(&cache); err != nil || cache != "replay" {
		t.Fatalf("cold cache was not rebuilt: %s %v", cache, err)
	}
}

func TestNASCleanupSourceFreezeRecoverySkipsConcurrentRelease(t *testing.T) {
	f := newCleanupFixture(t, 24)
	id, _, _ := f.admit("concurrent-release", syntheticFiles(100, 1, 10))
	f.dryRun()
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO nas_cleanup_frozen_streams(plan_id,stream_id) VALUES(999999,4)`); err != nil {
		t.Fatal(err)
	}
	releasing, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer releasing.Rollback(f.ctx)
	// A concurrent pause may own this freeze row and then wait for the batch
	// row that activation holds. Recovery on its separate connection must not
	// wait on that release, or it introduces an application-level lock cycle.
	if _, err = releasing.Exec(f.ctx, `DELETE FROM nas_cleanup_frozen_streams WHERE plan_id=999999`); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 2*time.Second)
	defer cancel()
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(f.ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasCleanupPublicFence); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `SELECT id FROM nas_cleanup_plans WHERE id=$1 FOR UPDATE`, id); err != nil {
		t.Fatal(err)
	}
	if _, err = f.s.cleanupActivateRankings(ctx, tx, id); err != nil {
		t.Fatalf("activation waited for an unrelated freeze release: %v", err)
	}
}

func TestNASCleanupSourceFreezeHousekeepingRecoversEndedPlansWithoutCallbacks(t *testing.T) {
	for _, mode := range []string{"paused", "retired"} {
		t.Run(mode, func(t *testing.T) {
			f := newCleanupFixture(t, 24)
			id, _, _ := f.admit("ended", syntheticFiles(100, 1, 10))
			if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_plans SET mode=$2 WHERE id=$1`, id, mode); err != nil {
				t.Fatal(err)
			}
			// Simulate stale persisted state from a maintenance/legacy writer.
			if _, err := f.pool.Exec(f.ctx, `INSERT INTO nas_cleanup_frozen_streams(plan_id,stream_id,validated) VALUES($1,3,true)`, id); err != nil {
				t.Fatal(err)
			}
			if _, err := f.pool.Exec(f.ctx, cleanupRankingsCacheStoreSQL, id, 3, 1, "2026-09-19", "stale", time.Now()); err != nil {
				t.Fatal(err)
			}
			f.s.cleanupHousekeeping(f.ctx, 13)
			requireCleanupFreezeCount(t, f, id, 0)
			var cache int
			if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_cleanup_rankings_cache WHERE plan_id=$1`, id).Scan(&cache); err != nil || cache != 0 {
				t.Fatalf("ended plan retained cache: %d %v", cache, err)
			}
		})
	}
}

func TestNASCleanupMaintenanceBlocksEnableAndPermits(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			f := newCleanupFixture(t, 24)
			id, sha, _ := f.admit("maintenance", syntheticFiles(100, 1, 10))
			f.dryRun()
			f.s.cfg.NASCleanupDeletionEnabled = true
			var tasks []cleanupTask
			if enabled {
				if code, body := f.enableBatch([]int64{id}, []string{sha}, 2); code != 200 {
					t.Fatalf("enable: %d %s", code, body)
				}
				tasks = f.leaseV2(64, 64, 1<<40, "")
			}
			if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_maintenance SET active=true,reason='test maintenance',started_at=clock_timestamp()`); err != nil {
				t.Fatal(err)
			}
			if !enabled {
				if code, body := safeguardControl(t, f.s, id, "enabled", map[string]any{"plan_sha256": sha}); code != 409 || !strings.Contains(body, "maintenance") {
					t.Fatalf("maintenance allowed single enable: %d %s", code, body)
				}
				if code, body := f.enableBatch([]int64{id}, []string{sha}, 2); code != 409 || !strings.Contains(body, "maintenance") {
					t.Fatalf("maintenance allowed batch enable: %d %s", code, body)
				}
				requireCleanupFreezeCount(t, f, id, 0)
				return
			}
			if len(tasks) != 1 {
				t.Fatalf("lease: %+v", tasks)
			}
			result := f.observe(tasks, map[int64]string{tasks[0].ItemID: strings.Repeat("a", 32)})[0]
			if result.Delete || result.Status != 503 || !strings.Contains(result.Error, "maintenance") {
				t.Fatalf("maintenance allowed permit: %+v", result)
			}
		})
	}
}

func TestNASCleanupMaintenanceWaitsForInFlightGrant(t *testing.T) {
	f, _, tasks := cleanupFenceFixture(t, false)
	tr := cleanupFenceTrace(t, f, cleanupRankingsCacheLoadSQL, true)
	done := cleanupFenceObserve(f, tasks)
	cleanupFenceReached(t, tr)
	maintenance, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer maintenance.Rollback(f.ctx)
	// NOWAIT proves the permit holds the maintenance row through the grant;
	// no sleeps or lock-timeout assumptions are needed.
	_, err = maintenance.Exec(f.ctx, `SELECT singleton FROM nas_cleanup_maintenance FOR UPDATE NOWAIT`)
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "55P03" {
		t.Fatalf("maintenance did not wait for in-flight grant: %v", err)
	}
	tr.resume <- struct{}{}
	if result := cleanupFenceResult(t, done); !result.Delete {
		t.Fatalf("in-flight grant failed: %+v", result)
	}
	maintenance.Rollback(f.ctx)
	if _, err = f.pool.Exec(f.ctx, `UPDATE nas_cleanup_maintenance SET active=true`); err != nil {
		t.Fatal(err)
	}
	if result := cleanupFenceResult(t, cleanupFenceObserve(f, tasks)); result.Delete || result.Status != 503 || !strings.Contains(result.Error, "maintenance") {
		t.Fatalf("maintenance replayed a permit response: %+v", result)
	}
}

func TestNASCleanupMaintenanceWaitsForActivation(t *testing.T) {
	f := newCleanupFixture(t, 24)
	id, sha, _ := f.admit("maintenance-activation", syntheticFiles(100, 1, 10))
	f.dryRun()
	f.s.cfg.NASCleanupDeletionEnabled = true
	tr := cleanupFenceTrace(t, f, cleanupFreezeDrainSQL, false)
	done := make(chan int, 1)
	go func() {
		code, _ := f.enableBatch([]int64{id}, []string{sha}, 2)
		done <- code
	}()
	cleanupFenceReached(t, tr)
	maintenance, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer maintenance.Rollback(f.ctx)
	_, err = maintenance.Exec(f.ctx, `SELECT singleton FROM nas_cleanup_maintenance FOR UPDATE NOWAIT`)
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "55P03" {
		t.Fatalf("maintenance did not wait for activation: %v", err)
	}
	tr.resume <- struct{}{}
	select {
	case code := <-done:
		if code != 200 {
			t.Fatalf("in-flight activation failed: %d", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("activation did not finish")
	}
}
