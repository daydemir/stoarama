package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
)

func TestNASR2CopyPlanIncludesBlockedCollatedAndPinnedBatches(t *testing.T) {
	policy, w, _ := api.Keep28Policy()
	start := w[3].Start
	item := api.NASR2CopyItem{Kind: "raw", SourceID: 1, RecordingID: 2, StreamID: 3, Bucket: "bucket", ObjectKey: "managed/c.mp4", RelativePath: "p/a.mp4", SizeBytes: 10, SHA256: strings.Repeat("a", 64), StartAt: start, EndAt: start.Add(time.Minute)}
	collated := item
	collated.Kind = "collated"
	collated.SourceID = 2
	header := map[string]any{"version": 2, "phase": "copy", "policy_sha256": policy, "items": 2, "bytes": 20, "connection_id": 13, "account_id": 47}
	dir := t.TempDir()
	path := filepath.Join(dir, "copy.jsonl")
	f, _ := os.Create(path)
	enc := json.NewEncoder(f)
	enc.Encode(map[string]any{"header": header})
	for _, i := range []api.NASR2CopyItem{item, collated} {
		data, _ := json.Marshal(i)
		var row map[string]any
		json.Unmarshal(data, &row)
		row["reason"] = "copy_to_nas"
		enc.Encode(row)
	}
	f.Close()
	data, _ := os.ReadFile(path)
	hash := sha256.Sum256(data)
	digest := hex.EncodeToString(hash[:])
	out := filepath.Join(dir, "plan")
	sum, err := planNASR2Copy(path, out, digest)
	if err != nil {
		t.Fatal(err)
	}
	if sum.Raw.Objects != 1 || sum.CollatedBlocked.Objects != 1 || sum.Raw.Bytes != 10 || sum.CollatedBlocked.Bytes != 10 {
		t.Fatalf("totals %+v", sum)
	}
	_, items, err := readNASCopyBatch(filepath.Join(out, "raw-0000.jsonl"), sum.Batches["raw-0000.jsonl"])
	if err != nil || len(items) != 1 {
		t.Fatalf("batch %v %v", items, err)
	}
	if _, _, err = readNASCopyBatch(filepath.Join(out, "collated-blocked.jsonl"), sum.Batches["collated-blocked.jsonl"]); err == nil {
		t.Fatal("blocked collated accepted")
	}
	if _, err = planNASR2Copy(path, filepath.Join(dir, "bad"), strings.Repeat("0", 64)); err == nil {
		t.Fatal("unverified source accepted")
	}
	if _, err = os.Stat(filepath.Join(dir, "bad")); !os.IsNotExist(err) {
		t.Fatal("invalid input created output")
	}
}

// Exercise retirement's unchanged exact-NAS SQL against the inventory shape
// this worker publishes. This is a read of retirement code, never an edit.
func TestNASR2CopyPublishedInventorySatisfiesRetireProof(t *testing.T) {
	pool := joinedPurgeTestPool(t, "nas_copy_proof")
	ctx := context.Background()
	_, err := pool.Exec(ctx, `CREATE TABLE recording_clips(id bigint PRIMARY KEY,display_path text,sha256 text,size_bytes bigint);
 CREATE TABLE nas_inventory_files(connection_id bigint,clip_id bigint,relative_path text,state text,sha256 text,size_bytes bigint,verified_at timestamptz,server_received_at timestamptz,sidecar_relative_path text,sidecar_size_bytes bigint,sidecar_sha256 text);
 INSERT INTO recording_clips VALUES(901,'p/copy.mp4',repeat('a',64),100);
 INSERT INTO nas_inventory_files VALUES(13,901,'p/copy.mp4','present',repeat('a',64),100,now(),now(),'p/copy.mp4.stoarama.json',100,repeat('b',64));`)
	if err != nil {
		t.Fatal(err)
	}
	var exact bool
	var path string
	var id int64
	err = pool.QueryRow(ctx, `SELECT c.id`+r2RetireRawExactSQL+` FROM recording_clips c WHERE c.id=$1`, int64(901), int64(13)).Scan(&id, &exact, &path)
	if err != nil || !exact || path != "p/copy.mp4" {
		t.Fatalf("retire proof %t %s %v", exact, path, err)
	}
}
