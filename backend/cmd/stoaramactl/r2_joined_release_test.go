package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/db"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

// TestR2JoinedBatchRelease proves the owner's release of a frozen joined batch
// frees its sources for the R2 RAW lane only: the purge guard admits a purge of
// a clip whose every snapshot is released, the RAW hold drops it, and the
// keep-28 lane, an unreleased snapshot, deletion and scopes all stay protected.
func TestR2JoinedBatchRelease(t *testing.T) {
	ctx := context.Background()
	pool := joinedPurgeTestPool(t, "joined_release")
	if err := db.MigrateUp(ctx, pool, joinedPurgeMigrationsDir()); err != nil {
		t.Fatal(err)
	}
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, query, args...); err != nil {
			t.Fatal(query, err)
		}
	}
	exec(`INSERT INTO accounts(id,email) VALUES(47,'release@test.invalid');
 INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,local_timezone,capture_type,source_family,execution_class,capture_family,expected_fps) VALUES(3,'test','3','release','release','https://test.invalid','off','Asia/Tokyo','hls','video_manifest','video_live','continuous_video',30);
 INSERT INTO storage_destinations(id,account_id,name,endpoint,region,bucket,access_key_id,secret_access_key_enc) VALUES(1,47,'test','https://test.invalid','auto','stoarama','fixture',decode('00','hex'));
 INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at) VALUES(30,47,1,'release','https://test.invalid','* * * * *','Asia/Tokyo','paused',3,now());`)
	for id, key := range map[int64]string{1: "managed/acct-47/released.mp4", 2: "managed/acct-47/mixed.mp4", 3: "managed/acct-47/held.mp4"} {
		exec(`INSERT INTO recording_clips(id,recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,etag,sha256,fire_at,clip_start_at,clip_end_at,display_path)
 VALUES($1,30,1,'https://test.invalid','stoarama',$2,100,'etag',repeat('b',64),'2026-06-01','2026-06-01','2026-06-01 00:01:00','release/'||$1::bigint)`, id, key)
	}
	// Only the batch and snapshot identities matter here; this schema is
	// disposable, so its unrelated joined constraints are relaxed for seeding.
	exec(`DO $$ DECLARE c record; BEGIN
 FOR c IN SELECT table_name,column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name IN ('recording_joined_batches','recording_joined_source_snapshots') AND is_nullable='NO' AND column_name NOT IN ('id','batch_record_id','clip_id') LOOP
  EXECUTE format('ALTER TABLE %I ALTER COLUMN %I DROP NOT NULL',c.table_name,c.column_name); END LOOP;
 FOR c IN SELECT conrelid::regclass::text t,conname FROM pg_constraint WHERE conrelid IN ('recording_joined_batches'::regclass,'recording_joined_source_snapshots'::regclass) AND contype IN ('c','f') LOOP
  EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I',c.t,c.conname); END LOOP; END $$`)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{`SET LOCAL session_replication_role=replica`,
		`INSERT INTO recording_joined_batches(id,batch_id,generation,state) VALUES(5,'goodplus-20260821-generation-1',1,'frozen'),(6,'other',1,'frozen')`,
		`INSERT INTO recording_joined_source_snapshots(batch_record_id,clip_id) VALUES(5,1),(5,2),(6,2),(6,3)`,
		`INSERT INTO r2_joined_batch_releases(batch_record_id,decision) VALUES(5,'test release')`} {
		if _, err = tx.Exec(ctx, q); err != nil {
			t.Fatal(q, err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	released := func(id int64) bool {
		var ok bool
		if err := pool.QueryRow(ctx, `SELECT r2_joined_source_released($1)`, id).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		return ok
	}
	if !released(1) || released(2) || released(3) {
		t.Fatalf("release: only-released %v mixed %v unreleased %v", released(1), released(2), released(3))
	}
	held := func(id int64, key string, raw bool) bool {
		var h bool
		if err := pool.QueryRow(ctx, inverseRawHeldSQL, id, "stoarama", key, "", "release/x", strings.Repeat("b", 64), int64(100), raw).Scan(&h); err != nil {
			t.Fatal(err)
		}
		return h
	}
	if held(1, "managed/acct-47/released.mp4", true) || !held(1, "managed/acct-47/released.mp4", false) || !held(2, "managed/acct-47/mixed.mp4", true) {
		t.Fatal("RAW lane hold does not follow the release, or the keep-28 lane lost its hold")
	}
	keys := func(sql string) map[string]bool {
		rows, err := pool.Query(ctx, sql, "stoarama")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		out := map[string]bool{}
		for rows.Next() {
			var k string
			if err = rows.Scan(&k); err != nil {
				t.Fatal(err)
			}
			out[k] = true
		}
		return out
	}
	partitionHeld, partitionReleased := keys(inverseHeldSQL), keys(inverseJoinedReleasedSQL)
	if partitionHeld["managed/acct-47/released.mp4"] || !partitionHeld["managed/acct-47/mixed.mp4"] || !partitionHeld["managed/acct-47/held.mp4"] || len(partitionReleased) != 1 || !partitionReleased["managed/acct-47/released.mp4"] {
		t.Fatalf("partition held %v released %v", partitionHeld, partitionReleased)
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_clips SET purged_at=now() WHERE id=3`); err == nil {
		t.Fatal("unreleased frozen source was purged")
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_clips SET purged_at=now() WHERE id=2`); err == nil {
		t.Fatal("source with an unreleased snapshot was purged")
	}
	if _, err = pool.Exec(ctx, `DELETE FROM recording_clips WHERE id=1`); err == nil {
		t.Fatal("released source was deleted outright")
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_clips SET purged_at=now() WHERE id=1`); err != nil {
		t.Fatalf("released source purge refused: %v", err)
	}
	for _, q := range []string{`UPDATE r2_joined_batch_releases SET decision='x'`, `DELETE FROM r2_joined_batch_releases`, `TRUNCATE r2_joined_batch_releases`} {
		if _, err = pool.Exec(ctx, q); err == nil {
			t.Fatal("release record changed:", q)
		}
	}
}

// A released joined source inside a pinned keep-28 window stays held, so the
// keep-28 lane never takes it; outside every window it is ordinary RAW.
func TestR2JoinedReleasedKeepsKeep28Held(t *testing.T) {
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(28 * 24 * time.Hour)
	p := inverseProtection{Held: map[string]bool{}, Delivery: map[string]bool{}, KEFolders: map[string]bool{},
		JoinedReleased: map[string]bool{"in": true, "out": true},
		Windows:        map[int64]api.Keep28Window{3: {StreamID: 3, Timezone: "Asia/Tokyo", Start: start, End: end}},
		Picks:          r2RetirePicks{Choices: map[int64]r2RetirePick{3: {Choice: "none", KeepStartUTC: start.Format(time.RFC3339), KeepEndUTC: end.Format(time.RFC3339)}}}}
	inside, insideEnd := start.Add(time.Hour), start.Add(time.Hour+time.Minute)
	outside, outsideEnd := start.Add(-30*24*time.Hour), start.Add(-30*24*time.Hour+time.Minute)
	p.Identities = map[string][]inverseIdentity{
		"in":  {{StreamID: 3, Path: "s/in.mp4", Start: &inside, End: &insideEnd, Timezone: "Asia/Tokyo", Kind: "raw"}},
		"out": {{StreamID: 3, Path: "s/out.mp4", Start: &outside, End: &outsideEnd, Timezone: "Asia/Tokyo", Kind: "raw"}},
	}
	if class, reason := p.classify(r2.ObjectInfo{Key: "in", SizeBytes: 1}); class != inverseHeld || reason != "joined_source_snapshot" {
		t.Fatalf("released in-window source classified %s (%s)", class, reason)
	}
	if class, _ := p.classify(r2.ObjectInfo{Key: "out", SizeBytes: 1}); class != inverseDeletable {
		t.Fatalf("released outside-window source classified %s", class)
	}
}
