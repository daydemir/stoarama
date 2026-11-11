package api

import (
	"fmt"
	"testing"
)

func TestNASCleanupRankingCacheUsesFrozenVerdictAcrossLongGaps(t *testing.T) {
	f := newCleanupFixture(t, 24)
	id, sha, _ := f.admit("cache-invalidation", syntheticFiles(100, 5, 10))
	f.dryRun()
	f.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := f.enableBatch([]int64{id}, []string{sha}, 2); code != 200 {
		t.Fatalf("enable: %d %s", code, body)
	}
	execute := func(want string) {
		t.Helper()
		tasks := f.leaseV2(64, 64, 1<<40, "")
		if len(tasks) != 1 {
			t.Fatalf("lease: %+v", tasks)
		}
		permit := f.observe(tasks, map[int64]string{tasks[0].ItemID: fmt.Sprintf("%032x", tasks[0].ItemID)})
		if len(permit) != 1 || !permit[0].Delete {
			t.Fatalf("permit: %+v", permit)
		}
		var cache string
		if err := f.pool.QueryRow(f.ctx, `SELECT stage_timings->>'rankings_cache' FROM nas_cleanup_items WHERE id=$1`, tasks[0].ItemID).Scan(&cache); err != nil || cache != want {
			t.Fatalf("permit without required %s: cache=%s err=%v", want, cache, err)
		}
		f.receipts(tasks, permit, "deleted")
	}
	execute("hit")
	if _, err := f.pool.Exec(f.ctx, `UPDATE recording_clips SET size_bytes=size_bytes+1 WHERE id=5000001`); err == nil {
		t.Fatal("frozen source changed")
	}
	execute("hit")
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_rankings_cache SET checked_at=clock_timestamp()-interval '61 seconds' WHERE plan_id=$1`, id); err != nil {
		t.Fatal(err)
	}
	execute("hit")
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_rankings_cache SET checked_at=clock_timestamp()-interval '23 hours' WHERE plan_id=$1`, id); err != nil {
		t.Fatal(err)
	}
	execute("hit")
	// A deployed #449 activation has no generation in execution_rankings;
	// its persisted cache row still binds the verdict to the live freeze.
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_plans SET execution_rankings=jsonb_build_object('3',(execution_rankings->'3')-'freeze_generation') WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	execute("hit")
}

func TestNASCleanupRankingCacheFailsClosedOnFreezeChange(t *testing.T) {
	for _, change := range []string{
		`UPDATE nas_cleanup_frozen_streams SET generation=generation+1000 WHERE plan_id=$1`,
		`UPDATE nas_cleanup_rankings_cache SET freeze_generation=freeze_generation+1000 WHERE plan_id=$1`,
		`DELETE FROM nas_cleanup_frozen_streams WHERE plan_id=$1`,
		`WITH removed AS (DELETE FROM nas_cleanup_rankings_cache WHERE plan_id=$1) UPDATE nas_cleanup_frozen_streams SET generation=generation+1000 WHERE plan_id=$1`,
	} {
		t.Run(change, func(t *testing.T) {
			f, id, tasks := cleanupFenceFixture(t, false)
			if _, err := f.pool.Exec(f.ctx, change, id); err != nil {
				t.Fatal(err)
			}
			result := f.observe(tasks, map[int64]string{tasks[0].ItemID: fmt.Sprintf("%032x", tasks[0].ItemID)})[0]
			if result.Delete {
				t.Fatalf("changed freeze minted permit: %+v", result)
			}
			var issued bool
			if err := f.pool.QueryRow(f.ctx, `SELECT delete_ever_issued FROM nas_cleanup_items WHERE id=$1`, tasks[0].ItemID).Scan(&issued); err != nil || issued {
				t.Fatalf("changed freeze recorded authority: %t %v", issued, err)
			}
		})
	}
}

func TestNASCleanupRankingCacheRollsBackWithObservation(t *testing.T) {
	f := newCleanupFixture(t, 24)
	id, sha, _ := f.admit("cache-rollback", syntheticFiles(100, 2, 10))
	f.dryRun()
	f.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := f.enableBatch([]int64{id}, []string{sha}, 2); code != 200 {
		t.Fatalf("enable: %d %s", code, body)
	}
	var stored []byte
	if err := f.pool.QueryRow(f.ctx, `SELECT execution_rankings FROM nas_cleanup_plans WHERE id=$1`, id).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(f.ctx, `DELETE FROM nas_cleanup_rankings_cache WHERE plan_id=$1`, id); err != nil {
		t.Fatal(err)
	}
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(f.ctx)
	var version int64
	if err := tx.QueryRow(f.ctx, cleanupRankingsFreezeSQL, id, 3).Scan(&version); err != nil {
		t.Fatal(err)
	}
	check := f.s.cleanupExecutionRankingsUnchanged(f.ctx, tx, id, version, "enabled", stored, nil)
	if check == nil {
		t.Fatal("replay unavailable")
	}
	if verified, err := check(3, "2026-09-19"); err != nil || !verified {
		t.Fatalf("fresh replay failed: %v", err)
	}
	if err := tx.Rollback(f.ctx); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_cleanup_rankings_cache WHERE plan_id=$1`, id).Scan(&count); err != nil || count != 0 {
		t.Fatalf("uncommitted verdict survived: %d %v", count, err)
	}
}
