package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

	// A held intent must still exist at the provider before any batch.
	if n, err := requireInversePendingIntentsPresent(ctx, pool, store, &inverseApplier{opts: inverseApplyOptions{Lane: inverseOutsideKeep28}}); err != nil || n != 0 {
		t.Fatalf("no intents: %d %v", n, err)
	}
	const key = "managed/acct-47/held/0001.mp4"
	store.objects[key] = r2.ObjectInfo{Key: key, ETag: "etag", SizeBytes: 100, LastModified: inverseRetirementCutoff().Add(-time.Hour)}
	if _, err = pool.Exec(ctx, `INSERT INTO r2_inverse_intents(bucket,object_key,source_id,size_bytes,etag,last_modified,manifest_sha256,policy_sha256,picks_sha256,drain_evidence_sha256,choice,endpoint_sha256,stream_id)
 VALUES('stoarama',$1,5399,100,'etag','2026-11-01',repeat('a',64),'060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811',repeat('b',64),repeat('c',64),'none',$2,3)`, key, store.EndpointSHA256()); err != nil {
		t.Fatal(err)
	}
	if n, err := requireInversePendingIntentsPresent(ctx, pool, store, &inverseApplier{opts: inverseApplyOptions{Lane: inverseOutsideKeep28}}); err != nil || n != 1 {
		t.Fatalf("held intent present: %d %v", n, err)
	}
	delete(store.objects, key)
	if _, err := requireInversePendingIntentsPresent(ctx, pool, store, &inverseApplier{opts: inverseApplyOptions{Lane: inverseOutsideKeep28}}); err == nil || !strings.Contains(err.Error(), "is absent at the provider") {
		t.Fatalf("held intent absent: %v", err)
	}
}
