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
	var decision string
	if err := pool.QueryRow(ctx, `SELECT decision FROM r2_owner_keep_prefixes WHERE bucket='stoarama' AND prefix='raw/stream/854/'`).Scan(&decision); err != nil || !strings.Contains(decision, "for my thesis") {
		t.Fatalf("854 hold %q %v", decision, err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO r2_owner_keep_prefixes(bucket,prefix,decision) VALUES('stoarama','Raw/no-slash','x')`); err == nil {
		t.Fatal("malformed prefix accepted")
	}
	for _, key := range []string{"raw/stream/854/2026/04/12/segment-1775969171803.mp4", "raw/stream/537/2026/04/18/segment-1776481123345.mp4"} {
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
	if err != nil || len(prefixes) != 2 {
		t.Fatalf("prefixes %v %v", prefixes, err)
	}
	p := inverseTestProtection()
	p.OwnerKeepPrefixes = prefixes
	p.Held["raw/stream/537/held.mp4"] = true
	for _, key := range []string{"raw/stream/854/2026/03/23/segment-1.mp4", "raw/stream/537/held.mp4"} {
		if class, reason := p.classify(r2.ObjectInfo{Key: key, SizeBytes: 1}); class != inverseKE || reason != "owner_keep_prefix" {
			t.Fatalf("%s classified %s (%s)", key, class, reason)
		}
	}
	if class, _ := p.classify(r2.ObjectInfo{Key: "raw/stream/8540/segment-1.mp4", SizeBytes: 1}); class == inverseKE {
		t.Fatal("neighbor stream prefix was kept")
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
	})
}
