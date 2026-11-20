package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/db"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

func TestR2InverseJobEvidenceAndHeldIntentGate(t *testing.T) {
	ctx := context.Background()
	pool := joinedPurgeTestPool(t, "inverse_job")
	if err := db.MigrateUp(ctx, pool, joinedPurgeMigrationsDir()); err != nil {
		t.Fatal(err)
	}
	store := &inverseFakeStore{endpointSHA: strings.Repeat("c", 64), objects: map[string]r2.ObjectInfo{}}
	if _, err := pool.Exec(ctx, `INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256) VALUES('stoarama',$1)`, store.EndpointSHA256()); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SET LOCAL session_replication_role=replica; UPDATE r2_retirement_writer_fences SET activated_at='2026-11-12T14:28:47.037031Z', drain_not_before=clock_timestamp()-interval '1 hour'`); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	// Evidence is observed from the exact live fence row and bound to this partition.
	owner := filepath.Join("..", "..", "..", "docs", "records", "r2-owner-fence-policy-20261118.md")
	o := inverseApplyOptions{ManifestSHA: strings.Repeat("a", 64), SummarySHA: strings.Repeat("b", 64)}
	path := filepath.Join(t.TempDir(), "evidence.json")
	digest, err := writeInverseOwnerEvidence(ctx, pool, store, o, owner, path)
	if err != nil {
		t.Fatal(err)
	}
	var e inverseDrainEvidence
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &e); err != nil {
		t.Fatal(err)
	}
	if got, _ := inverseFileSHA(path); got != digest || e.validateProof(o) != nil || !e.ObservedAt.After(e.DrainNotBefore) || e.FencePolicy.PartitionSHA256 != o.ManifestSHA {
		t.Fatalf("evidence %+v digest %s", e, digest)
	}
	if _, err = writeInverseOwnerEvidence(ctx, pool, store, o, owner, path); err == nil {
		t.Fatal("evidence file was overwritten")
	}
	if _, err = writeInverseOwnerEvidence(ctx, pool, &inverseFakeStore{endpointSHA: strings.Repeat("d", 64)}, o, owner, path+".other"); err == nil {
		t.Fatal("evidence accepted a different endpoint's fence")
	}

}

// The job's partition step refuses picks the owner did not approve, even when
// the file on disk changed after approval: the partition records the digest it
// actually classified with, and the post-read check compares it. Bypassing that
// check at the job's call site would seal and return this partition.
func TestR2InverseJobPartitionRequiresTheApprovedPicks(t *testing.T) {
	ctx := context.Background()
	pool := joinedPurgeTestPool(t, "inverse_job_picks")
	if err := db.MigrateUp(ctx, pool, joinedPurgeMigrationsDir()); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "picks.json")
	write := func(choice string) error {
		return os.WriteFile(path, []byte(`{"choices":{"3":{"choice":"`+choice+`"}}}`), 0600)
	}
	if err := write("none"); err != nil {
		t.Fatal(err)
	}
	approved, err := inverseFileSHA(path)
	if err != nil {
		t.Fatal(err)
	}
	// The file the job reads is no longer the one the owner approved.
	if err := write("all"); err != nil {
		t.Fatal(err)
	}
	f := inverseJobFlags{picks: path, picksSHA: approved, owner: "owner.md", work: t.TempDir(), workers: 1, items: 1, maxBytes: 1, batches: 1}
	store := inverseTestListing{{Key: "managed/acct-47/x.mp4", SizeBytes: 10}}
	if _, _, err = inverseJobPartition(ctx, config.Config{R2Bucket: "bucket"}, pool, store, f, filepath.Join(f.work, "partition"), false); err == nil || !strings.Contains(err.Error(), "approved digest") {
		t.Fatalf("job sealed a partition from picks the owner did not approve: %v", err)
	}
}

// The job binds its apply steps to the digest the partition step returned. A
// partition file changed after it was sealed must fail the existing apply-time
// hash check instead of being re-derived and accepted from disk.
func TestR2InverseJobBindsApplyToTheSealedDigest(t *testing.T) {
	ctx := context.Background()
	pool := joinedPurgeTestPool(t, "inverse_job_digest")
	if err := db.MigrateUp(ctx, pool, joinedPurgeMigrationsDir()); err != nil {
		t.Fatal(err)
	}
	store := &inverseFakeStore{endpointSHA: strings.Repeat("c", 64), objects: map[string]r2.ObjectInfo{}}
	dir := filepath.Join(t.TempDir(), "partition")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	manifest := filepath.Join(dir, "partition.jsonl")
	entry, err := json.Marshal(inverseEntry{ObjectInfo: r2.ObjectInfo{Key: "managed/acct-47/x.mp4", ETag: "etag", SizeBytes: 100, LastModified: inverseRetirementCutoff().Add(-time.Second)}, Class: inverseDeletable})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(manifest, append(entry, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	digest, err := inverseFileSHA(manifest)
	if err != nil {
		t.Fatal(err)
	}
	summary, _ := json.Marshal(inverseSummary{Version: 1, Bucket: "stoarama", EndpointSHA256: store.EndpointSHA256(), CreatedAt: time.Now().UTC(), PolicySHA256: strings.Repeat("a", 64), PicksSHA256: strings.Repeat("b", 64), ReadOnly: true})
	if err = os.WriteFile(filepath.Join(dir, "summary.json"), summary, 0600); err != nil {
		t.Fatal(err)
	}
	f := inverseJobFlags{picks: filepath.Join(dir, "picks.json"), work: t.TempDir(), workers: 1, items: 1, maxBytes: 100, batches: 1}
	// A file changed after the partition step printed its sealed digest.
	if err = os.WriteFile(manifest, append(entry, entry...), 0600); err != nil {
		t.Fatal(err)
	}
	o, err := inverseJobApplyOptions(f, inverseOutsideKeep28, dir, digest)
	if err != nil {
		t.Fatal(err)
	}
	if o.ManifestSHA != digest {
		t.Fatalf("apply options carry %q, want the sealed %q", o.ManifestSHA, digest)
	}
	if _, err = applyInverse(ctx, pool, store, o); err == nil || !strings.Contains(err.Error(), "partition digest differs") {
		t.Fatalf("apply accepted a partition changed after it was sealed: %v", err)
	}
}
