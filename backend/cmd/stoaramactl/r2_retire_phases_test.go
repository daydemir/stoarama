package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
)

func TestR2RetirePhasesCannotExecuteCopyOrLater(t *testing.T) {
	p := newR2RetirePhases()
	c := r2tCandidate("raw", 3, "Asia/Tokyo", r2tUTC("2026-09-01T00:00:00Z"), r2tUTC("2026-09-01T00:01:00Z"))
	c.ETag = "e"
	c.RelativePath = "p/1.mp4"
	p.add(c, r2RetireReason, "none", true)
	c.SourceID = 2
	c.ObjectKey = "managed/2.mp4"
	p.add(c, r2RetireNASReason, "none", true)
	c.SourceID = 3
	c.ObjectKey = "managed/3.mp4"
	p.add(c, "copy_to_nas", "none", false)
	c.SourceID = 4
	c.ObjectKey = "managed/4.mp4"
	p.add(c, "after_nas_cleanup", "none", false)
	dir := t.TempDir()
	h := r2RetireHeader{DeliveryKeepSHA256: r2tSHA, Version: r2RetireManifestVersion, NASBacked: true, Reason: r2RetireReason, Bucket: "stoarama", Choices: map[string]string{"3": "none"}, PolicySHA256: r2tPolicy, CreatedAt: time.Now()}
	if err := p.write(dir, h); err != nil {
		t.Fatal(err)
	}
	if p.Phase1.Objects != 1 || p.Phase1.Bytes != 100 || len(p.CopyBatches) != 1 {
		t.Fatalf("phase accounting %+v", p.Totals)
	}
	_, items, sha, err := readR2RetireManifest(filepath.Join(dir, "phase1.jsonl"))
	if err != nil || len(items) != 1 || sha != p.Phase1SHA256 {
		t.Fatalf("phase1 %v %s %v", items, sha, err)
	}
	for _, name := range []string{"later.jsonl.gz", "copy.jsonl.gz", "blocked.jsonl.gz", "copy-batches/0000.jsonl"} {
		if _, _, _, err := readR2RetireManifest(filepath.Join(dir, name)); err == nil {
			t.Fatalf("planning-only %s accepted for deletion", name)
		}
	}
}

func TestR2RetireListingDigestBucketAndAge(t *testing.T) {
	file := filepath.Join(t.TempDir(), "listing.jsonl")
	write := func(bucket string, at time.Time) string {
		raw := []byte(fmt.Sprintf("{\"bucket\":%q,\"listed_at\":%q}\n{\"Key\":\"managed/a\",\"ETag\":\"e\",\"SizeBytes\":100}\n", bucket, at.Format(time.RFC3339Nano)))
		if err := os.WriteFile(file, raw, 0600); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		return hex.EncodeToString(sum[:])
	}
	sha := write("stoarama", time.Now())
	objects, err := readR2RetireListing(file, sha, "stoarama")
	if err != nil || len(objects) != 1 {
		t.Fatalf("listing %v %v", objects, err)
	}
	if _, err := readR2RetireListing(file, r2tSHA, "stoarama"); err == nil {
		t.Fatal("wrong digest accepted")
	}
	for _, tt := range []struct {
		bucket string
		at     time.Time
	}{{"foreign", time.Now()}, {"stoarama", time.Now().Add(-3 * time.Hour)}, {"stoarama", time.Now().Add(time.Hour)}} {
		sha := write(tt.bucket, tt.at)
		if _, err := readR2RetireListing(file, sha, "stoarama"); err == nil {
			t.Fatal("bucket/age drift accepted")
		}
	}
}

type r2RetireTestLister struct{}

func (r2RetireTestLister) ListPrefix(ctx context.Context, prefix string, _ int, fn func(r2.ObjectInfo) error) error {
	if prefix == "broken/" {
		return errors.New("listing failed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return fn(r2.ObjectInfo{Key: prefix + "a", SizeBytes: 100, ETag: "e"})
}
func TestR2RetireParallelListingFailsClosed(t *testing.T) {
	objects, err := r2RetireLiveObjects(context.Background(), r2RetireTestLister{}, []string{"managed/a/", "managed/b/", "joined/v/"})
	if err != nil || len(objects) != 3 {
		t.Fatalf("objects=%d err=%v", len(objects), err)
	}
	if objects, err := r2RetireLiveObjects(context.Background(), r2RetireTestLister{}, []string{"managed/a/", "broken/"}); err == nil || objects != nil {
		t.Fatal("partial listing accepted")
	}
}

func TestR2RetireDeleteProofCannotCrossEndpoints(t *testing.T) {
	file := filepath.Join(t.TempDir(), "proof.json")
	raw := []byte(fmt.Sprintf(`{"bucket":"stoarama","endpoint_sha256":%q,"created_at":%q,"wrong_etag_rejected":true,"object_preserved":true,"matching_delete_confirmed":true}`, r2tSHA, time.Now().Format(time.RFC3339)))
	if err := os.WriteFile(file, raw, 0600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	digest := hex.EncodeToString(sum[:])
	if _, err := r2RetireDeleteProofExpiry(file, digest, "stoarama", r2tSHA, false, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := r2RetireDeleteProofExpiry(file, digest, "stoarama", r2tPolicy, false, time.Now().UTC()); err == nil {
		t.Fatal("proof from another endpoint accepted")
	}
}

func TestR2RetireAdvancingCustodyAndLockedCatalogETagPostgres(t *testing.T) {
	pool := joinedPurgeTestPool(t, "r2_deadlines")
	ctx := context.Background()
	// A real PostgreSQL SQL/lock fixture, not a fake apply or proof authority.
	if _, err := pool.Exec(ctx, `CREATE TABLE recording_clips(id bigint PRIMARY KEY,display_path text,sha256 text,size_bytes bigint,etag text);
 CREATE TABLE nas_inventory_files(connection_id bigint,clip_id bigint,relative_path text,state text,sha256 text,size_bytes bigint,verified_at timestamptz,server_received_at timestamptz,sidecar_relative_path text,sidecar_size_bytes bigint,sidecar_sha256 text,PRIMARY KEY(connection_id,clip_id));
 INSERT INTO recording_clips VALUES(1,'p/1.mp4',repeat('a',64),10,'original')`); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `INSERT INTO nas_inventory_files VALUES(13,1,'p/1.mp4','present',repeat('a',64),10,now()-interval '24 hours',now(),'p/1.mp4.stoarama.json',10,repeat('b',64))`); err != nil {
		t.Fatal(err)
	}
	var oldFresh, advanced bool
	if err := tx.QueryRow(ctx, `SELECT verified_at BETWEEN now()-interval '24 hours' AND now()+interval '5 minutes',clock_timestamp()>now() FROM nas_inventory_files`).Scan(&oldFresh, &advanced); err != nil || !oldFresh || !advanced {
		t.Fatal("fixed transaction clock did not expose retained bug", err)
	}
	var exact bool
	var id int64
	var path string
	if err := tx.QueryRow(ctx, `SELECT c.id`+r2RetireRawExactSQL+` FROM recording_clips c WHERE c.id=$1`, int64(1), int64(13)).Scan(&id, &exact, &path); err != nil || exact {
		t.Fatal("stale custody selected as exact", exact, err)
	}
	it := r2RetireItem{r2RetireCandidate: r2RetireCandidate{Kind: "raw", SourceID: 1, RelativePath: "p/1.mp4", SHA256: strings.Repeat("a", 64), SizeBytes: 10, ETag: "original"}}
	if _, err := r2RetireLockNASIdentity(ctx, tx, it, 13); err == nil {
		t.Fatal("expired custody locked using transaction start time")
	}
	if _, err := tx.Exec(ctx, `UPDATE nas_inventory_files SET verified_at=clock_timestamp(),server_received_at=clock_timestamp()-interval '23 hours'`); err != nil {
		t.Fatal(err)
	}
	expiry, err := r2RetireLockNASIdentity(ctx, tx, it, 13)
	if err != nil {
		t.Fatal(err)
	}
	var expected time.Time
	if err := tx.QueryRow(ctx, `SELECT server_received_at+interval '24 hours' FROM nas_inventory_files`).Scan(&expected); err != nil || !expiry.Equal(expected) {
		t.Fatal("wrong custody deadline", expiry, expected, err)
	}
	if _, err := tx.Exec(ctx, `UPDATE nas_inventory_files SET sidecar_sha256=''`); err != nil {
		t.Fatal(err)
	}
	if _, err := r2RetireLockNASIdentity(ctx, tx, it, 13); err == nil {
		t.Fatal("lock omitted required sidecar proof")
	}
	// Preflight saw original; the generation changed before the apply row lock.
	if _, err := tx.Exec(ctx, `UPDATE recording_clips SET etag='replacement' WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRow(ctx, `SELECT id FROM recording_clips WHERE id=1 FOR UPDATE`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if _, err := r2RetireRequireCatalogETag(ctx, tx, it); err == nil {
		t.Fatal("absent reconciliation adopted a changed catalog generation")
	}
	if _, err := tx.Exec(ctx, `UPDATE recording_clips SET etag='original' WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	if _, err := r2RetireRequireCatalogETag(ctx, tx, it); err != nil {
		t.Fatal(err)
	}
}
