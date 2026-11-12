package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"os"
	"path/filepath"
	"testing"
	"time"
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
	h := r2RetireHeader{Version: r2RetireManifestVersion, NASBacked: true, Reason: r2RetireReason, Bucket: "stoarama", Choices: map[string]string{"3": "none"}, PolicySHA256: r2tPolicy, CreatedAt: time.Now()}
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
	for _, name := range []string{"later.jsonl", "copy.jsonl", "blocked.jsonl", "copy-batches/0000.jsonl"} {
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
	if err := checkR2RetireDeleteProof(file, digest, "stoarama", r2tSHA); err != nil {
		t.Fatal(err)
	}
	if err := checkR2RetireDeleteProof(file, digest, "stoarama", r2tPolicy); err == nil {
		t.Fatal("proof from another endpoint accepted")
	}
}
