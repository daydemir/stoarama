package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/smithy-go"

	"github.com/daydemir/stoarama/backend/internal/r2"
)

func TestParseNASPurgeArgsDefaultsToSafeDryRun(t *testing.T) {
	opts, err := parseNASPurgeArgs([]string{"--log", "/tmp/x.log"})
	if err != nil {
		t.Fatal(err)
	}
	if opts.apply || opts.checkR2 || opts.grace != 7*24*time.Hour || opts.batchSize != 1000 || opts.workers != 16 ||
		opts.rate != 200 || opts.shaEvery != 1000 || opts.headMode != "sample" || opts.limit != 0 || opts.logPath != "/tmp/x.log" {
		t.Fatalf("unexpected defaults: %+v", opts)
	}
	opts, err = parseNASPurgeArgs([]string{"--from", "2026-08-01", "--to", "2026-08-02T00:00:00Z", "--recording-id", "7"})
	if err != nil || opts.from.Format(time.RFC3339) != "2026-08-01T00:00:00Z" || opts.recordingID != 7 {
		t.Fatalf("range parse %+v err=%v", opts, err)
	}
	for _, bad := range [][]string{
		{"--grace", "1h"}, {"--head", "none"}, {"--after-clip-id", "5", "--until-clip-id", "5"}, {"--exclude-recording-ids", "x"}, {"--exclude-clip-ids-file", "/nonexistent"}, {"--rate", "0"}, {"--batch-size", "0"}, {"--batch-size", "5000"}, {"--workers", "99"},
		{"--apply", "--check-r2"}, {"--from", "2026-08-02", "--to", "2026-08-01"}, {"--from", "yesterday"},
		{"--limit", "-1"}, {"--sha-every", "-1"}, {"extra"},
	} {
		if _, err := parseNASPurgeArgs(bad); err == nil {
			t.Fatalf("expected error for %v", bad)
		}
	}
}

func TestNASPurgeSkipReasonOrder(t *testing.T) {
	c := nasPurgeCandidate{Facts: map[string]bool{}}
	for _, name := range nasPurgeFactNames {
		c.Facts[name] = true
	}
	if got := c.skipReason(); got != "" {
		t.Fatalf("all facts true skipped %q", got)
	}
	c.Facts["nas_ok"] = false
	c.Facts["no_qualification_window"] = false
	if got := c.skipReason(); got != "qualification_window" {
		t.Fatalf("joined/qualification rules must win over NAS: %q", got)
	}
	for _, name := range nasPurgeFactNames {
		if nasPurgeFactReasons[name] == "" {
			t.Fatalf("fact %s has no reason", name)
		}
	}
}

// fakeNASPurgeStore is an in-memory object store; beforeHead lets a test change
// the database between the candidate scan and the locked re-check.
type fakeNASPurgeStore struct {
	mu         sync.Mutex
	objects    map[string][]byte
	deleted    []string
	failKeys   map[string]bool
	heads      int
	beforeHead func(key string)
}

func (f *fakeNASPurgeStore) Bucket() string { return "stoarama" }

func (f *fakeNASPurgeStore) Head(_ context.Context, key string) (r2.ObjectHead, error) {
	if f.beforeHead != nil {
		f.beforeHead(key)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.heads++
	b, ok := f.objects[key]
	if !ok {
		return r2.ObjectHead{}, &smithy.GenericAPIError{Code: "NotFound"}
	}
	return r2.ObjectHead{ETag: "e-" + key, SizeBytes: int64(len(b))}, nil
}

func (f *fakeNASPurgeStore) OpenExact(_ context.Context, key, etag, _ string) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.objects[key]
	if !ok || etag != "e-"+key {
		return nil, fmt.Errorf("precondition failed for %s", key)
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (f *fakeNASPurgeStore) DeleteObjectsEach(_ context.Context, keys []string) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	failed := map[string]string{}
	for _, k := range keys {
		if f.failKeys[k] {
			failed[k] = "InternalError try again"
			continue
		}
		delete(f.objects, k)
		f.deleted = append(f.deleted, k)
	}
	return failed, nil
}

func TestNASVerifiedPurgeEndToEnd(t *testing.T) {
	pool := joinedPurgeFixture(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
ALTER TABLE recording_joined_snapshot_scopes ADD COLUMN scheduled_start_at timestamptz, ADD COLUMN scheduled_end_at timestamptz;
CREATE TABLE recordings(id bigint PRIMARY KEY, account_id bigint NOT NULL, status text NOT NULL, delivery text NOT NULL);
CREATE TABLE connections(id bigint PRIMARY KEY, account_id bigint NOT NULL, kind text NOT NULL);
CREATE TABLE nas_inventory_unmatched_files(connection_id bigint NOT NULL, relative_path text NOT NULL, state text NOT NULL, PRIMARY KEY(connection_id, relative_path));
CREATE TABLE recording_qualification_runs(id bigint PRIMARY KEY, status text NOT NULL);
CREATE TABLE recording_qualification_windows(run_id bigint NOT NULL, recording_id bigint NOT NULL, window_start_at timestamptz NOT NULL, window_end_at timestamptz NOT NULL);
INSERT INTO connections VALUES (13,47,'nas_pull'),(14,48,'nas_pull');
INSERT INTO recordings VALUES (1,47,'completed','nas_pull'),(52,47,'completed','nas_pull'),(50,47,'completed','nas_pull'),(51,47,'active','nas_pull'),(60,48,'completed','nas_pull');
INSERT INTO recording_qualification_runs VALUES (1,'active'),(2,'canceled');
`); err != nil {
		t.Fatal(err)
	}
	migration, err := os.ReadFile(filepath.Join(joinedPurgeMigrationsDir(), "0157_nas_restore_requests.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		t.Fatalf("apply 0157: %v", err)
	}
	store := &fakeNASPurgeStore{objects: map[string][]byte{}, failKeys: map[string]bool{"managed/acct-47/115.mp4": true}}
	old := time.Now().Add(-30 * 24 * time.Hour)
	type spec struct {
		id, rec, job, dest int64
		endAgo             time.Duration
		nas                string // "ok", "badsha", "none"
		r2                 string // "ok", "absent", "short", "wrongsha"
	}
	specs := []spec{
		{101, 50, 5000, 20, 0, "ok", "ok"},                  // eligible
		{102, 50, 5000, 20, 0, "ok", "absent"},              // eligible, R2 object already gone
		{103, 50, 5000, 20, 0, "badsha", "ok"},              // NAS copy differs
		{104, 50, 5000, 20, 24 * time.Hour, "ok", "ok"},     // inside the grace period
		{105, 50, 5000, 20, 0, "ok", "ok"},                  // NAS path claimed by another clip row
		{106, 50, 5000, 20, 0, "ok", "ok"},                  // unmatched NAS file at the same path
		{107, 50, 5000, 20, 0, "ok", "ok"},                  // inside an active qualification window
		{108, 50, 5000, 20, 0, "ok", "ok"},                  // inside a joined snapshot scope window
		{109, 50, 5000, 21, 0, "ok", "ok"},                  // not managed storage
		{110, 50, 5000, 20, 0, "ok", "ok"},                  // restore queued
		{111, 50, 5000, 20, 0, "ok", "short"},               // R2 size differs
		{112, 50, 5002, 20, 0, "ok", "ok"},                  // covered by a dry-run scope
		{113, 50, 5000, 20, 0, "ok", "wrongsha"},            // R2 bytes differ from the recorded sha
		{114, 50, 5003, 20, 0, "ok", "ok"},                  // snapshotting scope appears before the lock
		{120, 51, 5100, 20, 3 * 24 * time.Hour, "ok", "ok"}, // last 7 days of an active recording
		{121, 51, 5100, 20, 0, "ok", "ok"},
		{115, 50, 5000, 20, 0, "ok", "ok"}, // eligible, but R2 refuses its delete
		{116, 50, 5000, 20, 0, "ok", "ok"}, // listed in the exclusion file
		{140, 52, 5200, 20, 0, "ok", "ok"}, // excluded recording                  // active recording, old clip: eligible
		{130, 60, 6000, 20, 0, "ok", "ok"}, // another account's recording
	}
	bodies := map[int64][]byte{}
	for _, s := range specs {
		body := []byte(fmt.Sprintf("clip-body-%d", s.id))
		bodies[s.id] = body
		end := old.Add(time.Duration(s.id) * time.Hour)
		if s.endAgo > 0 {
			end = time.Now().Add(-s.endAgo)
		}
		key, path := fmt.Sprintf("managed/acct-47/%d.mp4", s.id), fmt.Sprintf("r/%d.mp4", s.id)
		if _, err := pool.Exec(ctx, `INSERT INTO recording_clips(id,recording_id,recording_job_id,storage_destination_id,bucket,object_key,size_bytes,sha256,display_path,clip_start_at,clip_end_at,created_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::timestamptz-interval '1 minute',$10,$10)`,
			s.id, s.rec, s.job, s.dest, map[int64]string{20: "stoarama", 21: "byo"}[s.dest], key, len(body), joinedPurgeSHA(string(body)), path, end); err != nil {
			t.Fatal(err)
		}
		sha := joinedPurgeSHA(string(body))
		if s.nas == "badsha" {
			sha = strings.Repeat("f", 64)
		}
		conn := int64(13)
		if s.rec == 60 {
			conn = 14
		}
		if _, err := pool.Exec(ctx, `INSERT INTO nas_inventory_files(connection_id,clip_id,state,sha256,size_bytes,relative_path) VALUES ($1,$2,'present',$3,$4,$5)`,
			conn, s.id, sha, len(body), path); err != nil {
			t.Fatal(err)
		}
		switch s.r2 {
		case "ok":
			store.objects[key] = body
		case "short":
			store.objects[key] = body[:len(body)-1]
		case "wrongsha":
			store.objects[key] = bytes.ToUpper(body)
		}
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO nas_inventory_files(connection_id,clip_id,state,sha256,size_bytes,relative_path) VALUES (13,999,'present',repeat('a',64),1,'r/105.mp4');
INSERT INTO nas_inventory_unmatched_files VALUES (13,'r/106.mp4','present');
INSERT INTO recording_qualification_windows VALUES
  (1,50,(SELECT clip_start_at FROM recording_clips WHERE id=107),(SELECT clip_end_at FROM recording_clips WHERE id=107)),
  (2,51,(SELECT clip_start_at-interval '1 hour' FROM recording_clips WHERE id=121),(SELECT clip_end_at+interval '1 hour' FROM recording_clips WHERE id=121));
INSERT INTO recording_joined_snapshot_scopes(batch_record_id,recording_id,recording_job_id,high_water_clip_id,scheduled_start_at,scheduled_end_at)
  SELECT 1,50,5001,0,clip_start_at+interval '10 seconds',clip_end_at+interval '1 hour' FROM recording_clips WHERE id=108;
INSERT INTO recording_joined_dry_run_scopes VALUES ('00000000-0000-0000-0000-00000000000a',50,5002,1000);
INSERT INTO nas_restore_requests(connection_id,clip_id,recording_id,target,object_key,source_object_key,relative_path,content_type,size_bytes,sha256,label)
  SELECT 13,id,recording_id,'test','nas-restore-test/x/110.mp4',object_key,display_path,'video/mp4',size_bytes,sha256,'x' FROM recording_clips WHERE id=110;
`); err != nil {
		t.Fatal(err)
	}

	logPath := filepath.Join(t.TempDir(), "purge.log")
	base := nasPurgeOptions{connectionID: 13, grace: 48 * time.Hour, pageSize: 4, batchSize: 3, workers: 4, rate: 500,
		shaEvery: 1, maxErrors: 5, logPath: logPath, logSkips: true, headMode: "all",
		excludeClips: map[int64]bool{116: true}, excludeRecs: map[int64]bool{52: true}}
	run := func(opts nasPurgeOptions) nasPurgeSummary {
		t.Helper()
		f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		s, err := runNASVerifiedPurgePass(ctx, pool, store, opts, f)
		if err != nil {
			t.Fatalf("pass: %v (summary %+v)", err, s)
		}
		return s
	}
	wantSkips := map[string]int64{
		"nas_unverified": 3, "within_grace": 1, "qualification_window": 1, "joined_window": 1, "not_managed_storage": 1,
		"restore_recent": 1, "joined_scope": 1, "active_recording_recent": 1, "joined_snapshot": 9,
		"excluded_clip": 1, "excluded_recording": 1,
	}
	dry := run(base)
	if dry.Mode != "dry_run" || dry.Eligible != 7 || dry.Purged != 0 || len(store.deleted) != 0 {
		t.Fatalf("dry run summary %+v", dry)
	}
	for reason, n := range wantSkips {
		if dry.Skipped[reason] != n {
			t.Fatalf("dry run skipped[%s]=%d want %d (all: %v)", reason, dry.Skipped[reason], n, dry.Skipped)
		}
	}

	// --limit stops early; the resume point never skips unsettled clips.
	limited := base
	limited.limit = 1
	if s := run(limited); !s.LimitReached || s.Eligible != 1 {
		t.Fatalf("limited summary %+v", s)
	}
	// Between the scan and the lock, a joined batch starts snapshotting clip
	// 114's job; the locked re-check must leave it alone.
	store.beforeHead = func(key string) {
		if strings.HasSuffix(key, "/114.mp4") {
			_, _ = pool.Exec(ctx, `INSERT INTO recording_joined_snapshot_scopes(batch_record_id,recording_id,recording_job_id,high_water_clip_id) VALUES (2,50,5003,1000)`)
		}
	}
	apply := base
	apply.apply = true
	got := run(apply)
	store.beforeHead = nil
	if got.Purged != 3 || got.SourceAbsent != 1 || got.Skipped["source_size_mismatch"] != 1 || got.Skipped["source_sha_mismatch"] != 1 ||
		got.Skipped["changed_before_lock"] != 1 || got.DeleteFailed != 1 || got.Errors != 1 || got.ShaChecks < 3 {
		t.Fatalf("apply summary %+v", got)
	}
	var purged []int64
	rows, err := pool.Query(ctx, `SELECT id FROM recording_clips WHERE purged_at IS NOT NULL AND id>=100 ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int64
		_ = rows.Scan(&id)
		purged = append(purged, id)
	}
	rows.Close()
	if fmt.Sprint(purged) != "[101 102 121]" {
		t.Fatalf("purged clips %v", purged)
	}
	sort.Strings(store.deleted)
	if fmt.Sprint(store.deleted) != "[managed/acct-47/101.mp4 managed/acct-47/102.mp4 managed/acct-47/121.mp4]" {
		t.Fatalf("deleted objects %v", store.deleted)
	}
	for _, id := range []int64{103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 120, 130, 140} {
		if _, ok := store.objects[fmt.Sprintf("managed/acct-47/%d.mp4", id)]; !ok {
			t.Fatalf("object of protected clip %d was deleted", id)
		}
	}
	logged, err := os.ReadFile(logPath)
	if err != nil || !strings.Contains(string(logged), `"action":"purged"`) || !strings.Contains(string(logged), `"reason":"source_already_absent"`) {
		t.Fatalf("log missing purge lines: %v", err)
	}

	// Reruns are idempotent; a key whose delete failed is retried and, once
	// the store accepts it, purged.
	store.failKeys = nil
	again := run(apply)
	if again.Purged != 1 || len(store.deleted) != 4 {
		t.Fatalf("rerun summary %+v deleted=%v", again, store.deleted)
	}
	// Sample mode purges without a HEAD per clip.
	var pre int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM recording_clips WHERE id=115 AND purged_at IS NOT NULL`).Scan(&pre); err != nil || pre != 1 {
		t.Fatalf("clip 115 not purged after retry: %d %v", pre, err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_clips(id,recording_id,recording_job_id,storage_destination_id,bucket,object_key,size_bytes,sha256,display_path,clip_start_at,clip_end_at,created_at)
		VALUES (150,50,5000,20,'stoarama','managed/acct-47/150.mp4',3,$1,'r/150.mp4',now()-interval '20 days',now()-interval '20 days',now()-interval '20 days')`, joinedPurgeSHA("abc")); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO nas_inventory_files(connection_id,clip_id,state,sha256,size_bytes,relative_path) VALUES (13,150,'present',$1,3,'r/150.mp4')`, joinedPurgeSHA("abc")); err != nil {
		t.Fatal(err)
	}
	store.objects["managed/acct-47/150.mp4"] = []byte("abc")
	sample := apply
	sample.headMode, sample.shaEvery = "sample", 0
	store.heads = 0
	// Without a HEAD, 111 (short R2 copy) and 113 (R2 bytes differ) are purged
	// too: the NAS holds the exact recorded bytes, so no good copy is lost.
	if s := run(sample); s.Purged != 3 || store.heads != 0 {
		t.Fatalf("sample mode summary %+v heads=%d", s, store.heads)
	}
}
