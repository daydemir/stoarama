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
	for _, key := range []string{"raw/stream/854/2026/04/12/segment-1775969171803.mp4", "raw/stream/537/2026/04/18/segment-1776481123345.mp4"} {
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
