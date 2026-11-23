package main

import (
	"context"
	"strings"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/db"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

// TestR2OwnerKeepPrefixes pins Deniz's thesis keep: every key under the two
// legacy stream prefixes is refused by the retirement guards and classified
// keep, whether or not a catalog row exists, and the hold is append-only.
func TestR2OwnerKeepPrefixes(t *testing.T) {
	ctx := context.Background()
	pool := joinedPurgeTestPool(t, "owner_keep")
	if err := db.MigrateUp(ctx, pool, joinedPurgeMigrationsDir()); err != nil {
		t.Fatal(err)
	}
	for prefix, words := range map[string]string{"raw/stream/854/": "for my thesis", "raw/stream/537/": "for my thesis", "raw/stream/78/": "yes keep for now", "raw/stream/415/": "yes keep for now"} {
		var decision string
		if err := pool.QueryRow(ctx, `SELECT decision FROM r2_owner_keep_prefixes WHERE bucket='stoarama' AND prefix=$1`, prefix).Scan(&decision); err != nil || !strings.Contains(decision, words) {
			t.Fatalf("%s hold %q %v", prefix, decision, err)
		}
	}
	if _, err := pool.Exec(ctx, `INSERT INTO r2_owner_keep_prefixes(bucket,prefix,decision) VALUES('stoarama','Raw/no-slash','x')`); err == nil {
		t.Fatal("malformed prefix accepted")
	}
	rr, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = rr.Exec(ctx, `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ`); err != nil {
		t.Fatal(err)
	}
	if _, err = rr.Exec(ctx, `INSERT INTO r2_owner_keep_prefixes(bucket,prefix,decision) VALUES('stoarama','raw/stream/999/','x')`); err == nil || !strings.Contains(err.Error(), "READ COMMITTED") {
		t.Fatalf("repeatable-read admission: %v", err)
	}
	rr.Rollback(ctx)
	for _, key := range []string{"raw/stream/854/2026/04/12/segment-1775969171803.mp4", "raw/stream/537/2026/04/18/segment-1776481123345.mp4", "raw/stream/78/2026/05/01/segment-1777600000000.mp4", "raw/stream/415/2026/06/01/segment-1780300000000.mp4"} {
		for _, table := range []string{"r2_inverse_attempts", "r2_keep28_attempts"} {
			if _, err := pool.Exec(ctx, `INSERT INTO `+table+`(bucket,object_key) VALUES('stoarama',$1)`, key); err == nil || !strings.Contains(err.Error(), "OWNER KEEP HOLD") {
				t.Fatalf("%s accepted %s: %v", table, key, err)
			}
		}
		if _, err := pool.Exec(ctx, `INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256) VALUES('raw',1,'stoarama',$1,1,repeat('a',64),repeat('b',64))`, key); err == nil || !strings.Contains(err.Error(), "OWNER KEEP HOLD") {
			t.Fatalf("reservation of %s: %v", key, err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO r2_object_retirements(kind,source_id,stream_id,bucket,object_key,size_bytes,sha256,reason,choice,manifest_sha256,policy_sha256) VALUES('raw',1,854,'stoarama',$1,1,repeat('a',64),'inverse_non_keep28','none',repeat('b',64),repeat('c',64))`, key); err == nil || !strings.Contains(err.Error(), "OWNER KEEP HOLD") {
			t.Fatalf("retirement of %s: %v", key, err)
		}
	}
	var held, other, elsewhere bool
	if err := pool.QueryRow(ctx, `SELECT r2_owner_keep_held('stoarama','raw/stream/854/x.mp4'),r2_owner_keep_held('stoarama','raw/stream/8540/x.mp4'),r2_owner_keep_held('other','raw/stream/854/x.mp4')`).Scan(&held, &other, &elsewhere); err != nil || !held || other || elsewhere {
		t.Fatalf("prefix match held %v neighbor %v other bucket %v %v", held, other, elsewhere, err)
	}
	for _, q := range []string{`UPDATE r2_owner_keep_prefixes SET decision='x'`, `DELETE FROM r2_owner_keep_prefixes`, `TRUNCATE r2_owner_keep_prefixes`} {
		if _, err := pool.Exec(ctx, q); err == nil {
			t.Fatal("owner keep hold changed:", q)
		}
	}
	prefixes, err := loadInverseOwnerKeepPrefixes(ctx, pool, "stoarama")
	if err != nil || len(prefixes) != 4 {
		t.Fatalf("prefixes %v %v", prefixes, err)
	}
	p := inverseTestProtection()
	p.OwnerKeepPrefixes = prefixes
	p.Held["raw/stream/537/held.mp4"] = true
	for _, key := range []string{"raw/stream/854/2026/03/23/segment-1.mp4", "raw/stream/537/held.mp4", "raw/stream/78/2026/04/01/segment-1.mp4", "raw/stream/415/2026/06/28/segment-1.mp4"} {
		if class, reason := p.classify(r2.ObjectInfo{Key: key, SizeBytes: 1}); class != inverseKE || reason != "owner_keep_prefix" {
			t.Fatalf("%s classified %s (%s)", key, class, reason)
		}
	}
	if class, _ := p.classify(r2.ObjectInfo{Key: "raw/stream/8540/segment-1.mp4", SizeBytes: 1}); class == inverseKE {
		t.Fatal("neighbor stream prefix was kept")
	}
}

func TestR2OwnerKeepCatalogStream(t *testing.T) {
	f := newKeep28Fixture(t)
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := f.pool.Exec(f.ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	var held bool
	if err := f.pool.QueryRow(f.ctx, `SELECT r2_owner_keep_held('stoarama',$1)`, f.key).Scan(&held); err != nil || held {
		t.Fatalf("ordinary stream held=%v: %v", held, err)
	}
	p := inverseTestProtection()
	loaded, err := loadInverseApplyProtection(f.ctx, f.pool, "stoarama", p.Picks, p.Windows)
	if err != nil || !loaded.OwnerKeepStreams[415] || !loaded.OwnerKeepStreams[17318] || loaded.OwnerKeepStreams[3] {
		t.Fatalf("wrong pinned stream protection %v: %v", loaded.OwnerKeepStreams, err)
	}
	loaded.Identities[f.key] = []inverseIdentity{{Kind: "raw", StreamID: 415}}
	if class, reason := loaded.classify(r2.ObjectInfo{Key: f.key, SizeBytes: 100}); class != inverseKE || reason != "owner_keep_stream" {
		t.Fatalf("managed key classified %s (%s)", class, reason)
	}
	// Seed a historical derivative under the fixture's existing writer fence.
	exec(`SET LOCAL session_replication_role=replica;
 INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,local_timezone,capture_type,source_family,execution_class,capture_family,expected_fps)
 SELECT 415,provider,'415','owner-ke','owner-ke',source_url,recording_state,'Asia/Seoul',capture_type,source_family,execution_class,capture_family,expected_fps FROM streams WHERE id=3;
 UPDATE recordings SET stream_id=415 WHERE id=30;
 INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at)
 SELECT 31,account_id,storage_destination_id,'ordinary',stream_url,cron_expr,cron_timezone,status,3,paused_at FROM recordings WHERE id=30;
 INSERT INTO recording_collation_hours(id,hour_id,batch_id,generation,recording_id,local_date,delivery_hour,status,manifest_object_key,manifest_sha256,manifest,source_clip_count,included_clip_count,excluded_clip_count,join_count,split_count)
 VALUES(1,'test__recording-31__date-2026-09-01__hour-01__generation-2','test',2,31,'2026-09-01',1,'collated','test-manifest',repeat('c',64),'{}',1,1,0,0,0);
 INSERT INTO recording_collation_outputs(collation_hour_id,part,parts,object_key,nas_relative_path,size_bytes,sha256,start_at,end_at,source_clip_ids,r2_verified_at)
 VALUES(1,1,1,'joined/test/objects/'||repeat('d',64)||'.mp4','test/joined.mp4',100,repeat('d',64),'2026-09-01','2026-09-02',ARRAY[9000001::bigint],now())`)
	joinedKey := "joined/test/objects/" + strings.Repeat("d", 64) + ".mp4"
	for _, key := range []string{f.key, joinedKey} {
		var otherBucket bool
		if err := f.pool.QueryRow(f.ctx, `SELECT r2_owner_keep_held('stoarama',$1), r2_owner_keep_held('other',$1)`, key).Scan(&held, &otherBucket); err != nil || !held || otherBucket {
			t.Fatalf("catalog keep for %s: held=%v other bucket=%v: %v", key, held, otherBucket, err)
		}
	}
	for _, entry := range []struct{ table, key string }{
		{"r2_inverse_attempts", f.key}, {"r2_keep28_attempts", f.key}, {"r2_nonraw_attempts", joinedKey},
	} {
		if _, err := f.pool.Exec(f.ctx, `INSERT INTO `+entry.table+`(bucket,object_key) VALUES('stoarama',$1)`, entry.key); err == nil || !strings.Contains(err.Error(), "OWNER KEEP HOLD") {
			t.Fatalf("%s accepted a protected key: %v", entry.table, err)
		}
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE recording_clips SET purged_at=now() WHERE object_key=$1`, f.key); err == nil || !strings.Contains(err.Error(), "OWNER KEEP HOLD") {
		t.Fatalf("catalog purge accepted a protected key: %v", err)
	}
	if _, err := applyInverse(f.ctx, f.pool, f.sdk, f.options); err == nil || f.deletes() != 0 {
		t.Fatalf("protected key reached apply: err=%v deletes=%d", err, f.deletes())
	}
}

// TestR2OwnerKeepStopsTheExecutor drives the real apply path: a hold that
// already covers a key stops the run before any DELETE, and a hold cannot be
// added over a key whose retirement is pending.
func TestR2OwnerKeepStopsTheExecutor(t *testing.T) {
	t.Run("held_key_never_reaches_wire", func(t *testing.T) {
		f := newKeep28Fixture(t)
		if _, err := f.pool.Exec(f.ctx, `INSERT INTO r2_owner_keep_prefixes(bucket,prefix,decision) VALUES('stoarama','managed/acct-47/keep28/','test hold')`); err != nil {
			t.Fatal(err)
		}
		if _, err := applyInverse(f.ctx, f.pool, f.sdk, f.options); err == nil || f.deletes() != 0 {
			t.Fatalf("held key: err %v deletes %d", err, f.deletes())
		}
		var intents int
		if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM r2_keep28_intents WHERE object_key=$1`, f.key).Scan(&intents); err != nil || intents != 0 {
			t.Fatalf("held key left intents %d %v", intents, err)
		}
	})
	t.Run("hold_cannot_land_over_a_pending_retirement", func(t *testing.T) {
		f := newKeep28Fixture(t)
		tx, err := f.pool.Begin(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(f.ctx, `SET LOCAL session_replication_role=replica`); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(f.ctx, `INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256) VALUES('raw',9000001,'stoarama',$1,100,repeat('a',64),repeat('b',64))`, f.key); err != nil {
			t.Fatal(err)
		}
		if err = tx.Commit(f.ctx); err != nil {
			t.Fatal(err)
		}
		if _, err = f.pool.Exec(f.ctx, `INSERT INTO r2_owner_keep_prefixes(bucket,prefix,decision) VALUES('stoarama','managed/acct-47/keep28/','late hold')`); err == nil || !strings.Contains(err.Error(), "overlaps a pending R2 retirement") {
			t.Fatalf("late hold over a pending reservation: %v", err)
		}
	})
	t.Run("replica_mode_and_catalog_purge_cannot_bypass", func(t *testing.T) {
		f := newKeep28Fixture(t)
		if _, err := f.pool.Exec(f.ctx, `INSERT INTO r2_owner_keep_prefixes(bucket,prefix,decision) VALUES('stoarama','managed/acct-47/keep28/','test hold')`); err != nil {
			t.Fatal(err)
		}
		tx, err := f.pool.Begin(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(f.ctx)
		if _, err = tx.Exec(f.ctx, `SET LOCAL session_replication_role=replica`); err != nil {
			t.Fatal(err)
		}
		for _, q := range []string{
			`INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256) VALUES('raw',9000001,'stoarama','managed/acct-47/keep28/1.mp4',100,repeat('a',64),repeat('b',64))`,
			`DELETE FROM r2_owner_keep_prefixes`,
		} {
			if _, err = tx.Exec(f.ctx, `SAVEPOINT s`); err != nil {
				t.Fatal(err)
			}
			if _, err = tx.Exec(f.ctx, q); err == nil {
				t.Fatal("replica mode bypassed the owner hold:", q)
			}
			if _, err = tx.Exec(f.ctx, `ROLLBACK TO SAVEPOINT s`); err != nil {
				t.Fatal(err)
			}
		}
		if _, err = f.pool.Exec(f.ctx, `UPDATE recording_clips SET purged_at=now() WHERE object_key=$1`, f.key); err == nil || !strings.Contains(err.Error(), "OWNER KEEP HOLD") {
			t.Fatalf("held clip purge: %v", err)
		}
		// Moving the key out from under the hold in the same update still trips it.
		moved, err := f.pool.Begin(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer moved.Rollback(f.ctx)
		if _, err = moved.Exec(f.ctx, `SET LOCAL session_replication_role=replica`); err != nil {
			t.Fatal(err)
		}
		if _, err = moved.Exec(f.ctx, `UPDATE recording_clips SET object_key='managed/acct-47/elsewhere/1.mp4',purged_at=now() WHERE object_key=$1`, f.key); err == nil || !strings.Contains(err.Error(), "OWNER KEEP HOLD") {
			t.Fatalf("held clip moved and purged: %v", err)
		}
	})
	t.Run("stage_retirement_under_a_held_prefix_is_refused", func(t *testing.T) {
		f := newKeep28Fixture(t)
		if _, err := f.pool.Exec(f.ctx, `INSERT INTO r2_owner_keep_prefixes(bucket,prefix,decision) VALUES('stoarama','stitch-staging/','test hold')`); err != nil {
			t.Fatal(err)
		}
		tx, err := f.pool.Begin(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(f.ctx)
		if _, err = tx.Exec(f.ctx, `SET LOCAL session_replication_role=replica`); err != nil {
			t.Fatal(err)
		}
		var id int64
		if err = tx.QueryRow(f.ctx, `INSERT INTO nas_stitch_r2_stage_objects(hour_id,attempt,api_key_id,bucket,endpoint_sha256,relative_path,sha256,size_bytes) VALUES(1,1,1,'stoarama',repeat('a',64),'h/1.mp4',repeat('b',64),10) RETURNING id`).Scan(&id); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(f.ctx, `INSERT INTO nas_stitch_r2_stage_retirements(object_id) VALUES($1)`, id); err == nil || !strings.Contains(err.Error(), "OWNER KEEP HOLD") {
			t.Fatalf("stage retirement under a held prefix: %v", err)
		}
	})
}

// A staging retirement in flight blocks a new hold over it, a held object
// cannot gain a staging retirement by reassignment, and guarded writes under
// Repeatable Read are refused before they rely on a stale snapshot.
func TestR2OwnerKeepStagingAndIsolation(t *testing.T) {
	f := newKeep28Fixture(t)
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(f.ctx)
	if _, err = tx.Exec(f.ctx, `SET LOCAL session_replication_role=replica`); err != nil {
		t.Fatal(err)
	}
	var free, held int64
	if err = tx.QueryRow(f.ctx, `INSERT INTO nas_stitch_r2_stage_objects(hour_id,attempt,api_key_id,bucket,endpoint_sha256,relative_path,sha256,size_bytes) VALUES(1,1,1,'stoarama',repeat('a',64),'h/1.mp4',repeat('b',64),10) RETURNING id`).Scan(&free); err != nil {
		t.Fatal(err)
	}
	if err = tx.QueryRow(f.ctx, `INSERT INTO nas_stitch_r2_stage_objects(hour_id,attempt,api_key_id,bucket,endpoint_sha256,relative_path,sha256,size_bytes) VALUES(1,1,1,'stoarama',repeat('a',64),'h/2.mp4',repeat('c',64),10) RETURNING id`).Scan(&held); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(f.ctx, `INSERT INTO nas_stitch_r2_stage_retirements(object_id) VALUES($1)`, free); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	// An unresolved staging retirement blocks a hold over its key.
	if _, err = f.pool.Exec(f.ctx, `INSERT INTO r2_owner_keep_prefixes(bucket,prefix,decision) VALUES('stoarama','stitch-staging/','late hold')`); err == nil || !strings.Contains(err.Error(), "overlaps a pending R2 retirement") {
		t.Fatalf("hold over an unresolved staging retirement: %v", err)
	}
	// Repeatable Read writers are refused by the guards themselves.
	rr, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rr.Rollback(f.ctx)
	if _, err = rr.Exec(f.ctx, `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ`); err != nil {
		t.Fatal(err)
	}
	if _, err = rr.Exec(f.ctx, `INSERT INTO r2_inverse_attempts(bucket,object_key) VALUES('stoarama','managed/acct-47/other/rr.mp4')`); err == nil || !strings.Contains(err.Error(), "READ COMMITTED") {
		t.Fatalf("repeatable-read ledger write: %v", err)
	}
	rr.Rollback(f.ctx)
	// Reassigning a staging retirement to a held object is refused, even in replica mode.
	if _, err = f.pool.Exec(f.ctx, `INSERT INTO r2_owner_keep_prefixes(bucket,prefix,decision) SELECT 'stoarama',left(object_key,length('stitch-staging/')+37),'test hold' FROM nas_stitch_r2_stage_objects WHERE id=$1`, held); err != nil {
		t.Fatal(err)
	}
	move, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer move.Rollback(f.ctx)
	if _, err = move.Exec(f.ctx, `SET LOCAL session_replication_role=replica`); err != nil {
		t.Fatal(err)
	}
	if _, err = move.Exec(f.ctx, `UPDATE nas_stitch_r2_stage_retirements SET object_id=$1 WHERE object_id=$2`, held, free); err == nil || !strings.Contains(err.Error(), "OWNER KEEP HOLD") {
		t.Fatalf("staging retirement reassigned to a held object: %v", err)
	}
	move.Rollback(f.ctx)
	// Nor away from its object, which would hide an in-flight DELETE from admission.
	var spare int64
	away, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer away.Rollback(f.ctx)
	if _, err = away.Exec(f.ctx, `SET LOCAL session_replication_role=replica`); err != nil {
		t.Fatal(err)
	}
	if err = away.QueryRow(f.ctx, `INSERT INTO nas_stitch_r2_stage_objects(hour_id,attempt,api_key_id,bucket,endpoint_sha256,relative_path,sha256,size_bytes) VALUES(1,1,1,'stoarama',repeat('a',64),'h/3.mp4',repeat('d',64),10) RETURNING id`).Scan(&spare); err != nil {
		t.Fatal(err)
	}
	if _, err = away.Exec(f.ctx, `UPDATE nas_stitch_r2_stage_retirements SET object_id=$1 WHERE object_id=$2`, spare, free); err == nil || !strings.Contains(err.Error(), "cannot change its object") {
		t.Fatalf("staging retirement moved away from its object: %v", err)
	}
}
