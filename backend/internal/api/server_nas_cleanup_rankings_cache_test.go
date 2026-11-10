package api

import (
	"fmt"
	"testing"
)

func TestNASCleanupRankingCacheInvalidatesBeforeNextPermit(t *testing.T) {
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
	execute("hit") // activation seeded the versioned replay
	// Any clip row change must replay, even if the scored inputs/fingerprint
	// happen to be identical (metadata update, or an ABA interval edit).
	if _, err := f.pool.Exec(f.ctx, `UPDATE recording_clips SET size_bytes=size_bytes+1 WHERE id=5000001`); err != nil {
		t.Fatal(err)
	}
	execute("replay")
	execute("hit")
	if _, err := f.pool.Exec(f.ctx, `UPDATE recording_clips SET clip_end_at=clip_end_at+interval '1 microsecond' WHERE id=5000002; UPDATE recording_clips SET clip_end_at=clip_end_at-interval '1 microsecond' WHERE id=5000002`); err != nil {
		t.Fatal(err)
	}
	execute("replay")
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_rankings_cache SET checked_at=clock_timestamp()-interval '61 seconds' WHERE plan_id=$1`, id); err != nil {
		t.Fatal(err)
	}
	execute("replay")
}

func TestNASCleanupRankingVersionsCoverAllReplayDependencies(t *testing.T) {
	f := newCleanupFixture(t, 24)
	version := func() int64 {
		t.Helper()
		var v int64
		if err := f.pool.QueryRow(f.ctx, `SELECT `+cleanupRankingsVersionExpr, 3).Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	for _, change := range []string{
		`UPDATE recording_clips SET clip_end_at=clip_end_at+interval '1 microsecond' WHERE id=5000000`,
		`INSERT INTO recording_clips(id,recording_id) VALUES(9,2)`,
		`DELETE FROM recording_clips WHERE id=9`,
		`INSERT INTO recording_collation_hours VALUES(77,2,1,'{"clips":[]}')`,
		`UPDATE recording_collation_hours SET manifest='{"clips":[{"clip":{"clip_id":5000001},"disposition":"quarantined"}]}' WHERE id=77`,
		`DELETE FROM recording_collation_hours WHERE id=77`,
		`UPDATE recordings SET cron_timezone='UTC' WHERE id=2`,
		`UPDATE streams SET local_timezone='Asia/Seoul' WHERE id=3`,
		`INSERT INTO recordings VALUES(5,47,3,'UTC')`,
		`UPDATE recordings SET stream_id=4 WHERE id=5`,
		`UPDATE recordings SET stream_id=3 WHERE id=5`,
		`DELETE FROM recordings WHERE id=5`,
		`TRUNCATE recording_clips CASCADE`,
		`TRUNCATE recording_collation_hours`,
		`TRUNCATE recordings`,
		`TRUNCATE streams`,
	} {
		before := version()
		if _, err := f.pool.Exec(f.ctx, change); err != nil {
			t.Fatal(change, err)
		}
		if after := version(); after <= before {
			t.Fatalf("version did not increase: %s (%d -> %d)", change, before, after)
		}
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
	check := f.s.cleanupExecutionRankingsUnchanged(f.ctx, tx, id, "enabled", stored, nil)
	if check == nil || !check(3, "2026-09-19") {
		t.Fatal("fresh replay failed")
	}
	if err := tx.Rollback(f.ctx); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_cleanup_rankings_cache WHERE plan_id=$1`, id).Scan(&count); err != nil || count != 0 {
		t.Fatalf("uncommitted verdict survived: %d %v", count, err)
	}
}
