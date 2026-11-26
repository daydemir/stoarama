package main

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/db"
)

func TestParseNASRestoreRequestArgs(t *testing.T) {
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	file := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	ids, dup, empty := file("ids", "7\n9, 8\n"), file("dup", "7 7"), file("empty", "\n")
	if opts, err := parseNASRestoreRequestArgs([]string{"--output-ids-file", ids}, now); err != nil || !slices.Equal(opts.outputIDs, []int64{7, 9, 8}) {
		t.Fatalf("output opts=%+v err=%v", opts, err)
	}
	opts, err := parseNASRestoreRequestArgs([]string{"--hour-record-id", "85"}, now)
	if err != nil || opts.hourRecordID != 85 || opts.test || opts.label != "restore-20260924t120000" || opts.maxAttempts != 5 {
		t.Fatalf("hour opts=%+v err=%v", opts, err)
	}
	opts, err = parseNASRestoreRequestArgs([]string{"--clip-ids", "3, 4", "--test-prefix", "--label", "rt-1", "--max-bytes", "100"}, now)
	if err != nil || len(opts.clipIDs) != 2 || !opts.test || opts.label != "rt-1" || opts.maxBytes != 100 {
		t.Fatalf("clip opts=%+v err=%v", opts, err)
	}
	for _, bad := range [][]string{
		nil, {"--clip-ids", "1", "--hour-record-id", "2"}, {"--recording-id", "1"}, {"--recording-id", "1", "--from", "2026-08-02", "--to", "2026-08-01"},
		{"--clip-ids", "x"}, {"--clip-ids", "1", "--label", "Bad Label"}, {"--clip-ids", "1", "--from", "2026-08-01"},
		{"--clip-ids", "1", "--max-attempts", "0"}, {"--clip-ids", "1", "extra"},
		{"--output-ids-file", ids, "--clip-ids", "1"}, {"--output-ids-file", ids, "--test-prefix"},
		{"--output-ids-file", dup}, {"--output-ids-file", empty}, {"--output-ids-file", filepath.Join(dir, "absent")},
	} {
		if _, err := parseNASRestoreRequestArgs(bad, now); err == nil {
			t.Fatalf("expected error for %v", bad)
		}
	}
}

func TestClassifyNASRestoreCandidate(t *testing.T) {
	ok := nasRestoreCandidate{ClipID: 1, ObjectKey: "k", DisplayPath: "a/b.mp4", SizeBytes: 5, SHA256: strings.Repeat("a", 64),
		Purged: true, Bucket: "stoarama", Managed: true, ConnectionID: 13, NASVerified: true, NASConnection: 1}
	if got := classifyNASRestoreCandidate(ok, "stoarama", false); got != "" {
		t.Fatalf("eligible classified %q", got)
	}
	for want, mutate := range map[string]func(*nasRestoreCandidate){
		"no_nas_connection":        func(c *nasRestoreCandidate) { c.NASConnection = 0 },
		"ambiguous_nas_connection": func(c *nasRestoreCandidate) { c.NASConnection = 2 },
		"clip_identity_incomplete": func(c *nasRestoreCandidate) { c.SHA256 = "" },
		"too_large":                func(c *nasRestoreCandidate) { c.SizeBytes = 6 << 30 },
		"nas_unverified":           func(c *nasRestoreCandidate) { c.NASVerified = false },
		"not_managed_storage":      func(c *nasRestoreCandidate) { c.Bucket = "other" },
		"not_purged":               func(c *nasRestoreCandidate) { c.Purged = false },
	} {
		c := ok
		mutate(&c)
		if got := classifyNASRestoreCandidate(c, "stoarama", false); got != want {
			t.Fatalf("want %q got %q", want, got)
		}
	}
	live := ok
	live.Purged, live.Bucket = false, "other"
	if got := classifyNASRestoreCandidate(live, "stoarama", true); got != "" {
		t.Fatalf("test restore of a live clip classified %q", got)
	}
	// A stream-day inventory's exact match is verified for the restore, purged
	// or not, and is read from where the inventory found it.
	inventoried := ok
	inventoried.NASVerified, inventoried.Purged, inventoried.InventoriedPath = false, false, "plaza/September/25-Friday/b.mp4"
	if got := classifyNASRestoreCandidate(inventoried, "stoarama", false); got != "" || inventoried.sourcePath() != "plaza/September/25-Friday/b.mp4" {
		t.Fatalf("inventoried clip classified %q from %q", got, inventoried.sourcePath())
	}
	if inventoried.Bucket = "other"; classifyNASRestoreCandidate(inventoried, "stoarama", false) != "not_managed_storage" {
		t.Fatal("inventoried clip in foreign storage queued")
	}
	if ok.sourcePath() != "a/b.mp4" {
		t.Fatalf("verified clip reads %q", ok.sourcePath())
	}
	if got := nasRestoreTestKey("rt-1", 9, "x/y/z.mp4"); got != "nas-restore-test/rt-1/9/z.mp4" {
		t.Fatalf("test key %q", got)
	}
}

func TestRequestNASRestoresQueuesOnlyVerifiedClips(t *testing.T) {
	pool := joinedPurgeFixture(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
ALTER TABLE recording_clips ADD COLUMN mime_type text;
CREATE TABLE recordings(id bigint PRIMARY KEY, account_id bigint NOT NULL, status text NOT NULL DEFAULT 'completed', delivery text NOT NULL DEFAULT 'nas_pull');
CREATE TABLE connections(id bigint PRIMARY KEY, account_id bigint NOT NULL, kind text NOT NULL);
INSERT INTO connections VALUES (13,47,'nas_pull');
INSERT INTO recordings(id,account_id) VALUES (1,47),(2,47),(3,47),(9,47);
UPDATE recording_clips SET purged_at=now() WHERE id IN (1,2,7);`); err != nil {
		t.Fatal(err)
	}
	migration, err := os.ReadFile(filepath.Join(joinedPurgeMigrationsDir(), "0157_nas_restore_requests.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		t.Fatalf("apply 0157: %v", err)
	}
	// 0263's column and 0272's inventory scope; their gates need the full
	// schema (see the output and stream-day inventory tests). Nothing here is inventoried.
	if _, err := pool.Exec(ctx, `ALTER TABLE nas_restore_requests ADD COLUMN output_id bigint;
CREATE TABLE nas_day_inventory_files(clip_id bigint, relative_path text);
CREATE FUNCTION nas_restore_inventoried_original(text,bigint,bigint,text,bigint,text,text) RETURNS boolean LANGUAGE sql AS 'SELECT false'`); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	// Hour 11 sources: 1,2 purged+NAS; 3 unpurged; 6 unpurged; 7 purged, no NAS; 8,10 unpurged.
	opts, _ := parseNASRestoreRequestArgs([]string{"--hour-record-id", "11", "--label", "h11"}, now)
	dry := opts
	dry.dryRun = true
	s, err := requestNASRestores(ctx, pool, "stoarama", dry)
	if err != nil || s.Queued != 2 || s.Skipped["not_purged"] != 4 || s.Skipped["nas_unverified"] != 1 {
		t.Fatalf("dry run %+v err=%v", s, err)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM nas_restore_requests`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("dry run queued %d rows err=%v", n, err)
	}
	s, err = requestNASRestores(ctx, pool, "stoarama", opts)
	if err != nil || s.Queued != 2 || s.QueuedBytes != 10 {
		t.Fatalf("request %+v err=%v", s, err)
	}
	var key, target, ctype string
	if err := pool.QueryRow(ctx, `SELECT object_key,target,content_type FROM nas_restore_requests WHERE clip_id=1`).Scan(&key, &target, &ctype); err != nil ||
		key != joinedPurgeSourceKey(1) || target != "original" || ctype != "video/mp4" {
		t.Fatalf("queued row key=%s target=%s type=%s err=%v", key, target, ctype, err)
	}
	if s, err = requestNASRestores(ctx, pool, "stoarama", opts); err != nil || s.Queued != 0 || s.AlreadyQueued != 2 {
		t.Fatalf("re-request %+v err=%v", s, err)
	}
	test, _ := parseNASRestoreRequestArgs([]string{"--hour-record-id", "11", "--test-prefix", "--label", "t11", "--max-bytes", "12"}, now)
	if s, err = requestNASRestores(ctx, pool, "stoarama", test); err != nil || s.Queued != 2 || !s.CapReached {
		t.Fatalf("test request %+v err=%v", s, err)
	}
	statuses, err := loadNASRestoreStatus(ctx, pool, "")
	if err != nil || len(statuses) != 2 || statuses[0].Label != "t11" || statuses[0].States["pending"] != 2 {
		t.Fatalf("status %+v err=%v", statuses, err)
	}
}

// Output restores queue only scoped outputs the NAS acknowledged, in file
// order, as rows the API's lease gate admits.
func TestRequestNASRestoresQueuesOnlyScopedAcknowledgedOutputs(t *testing.T) {
	ctx := context.Background()
	pool := joinedPurgeTestPool(t, "nas_restore_outputs")
	if err := db.MigrateUp(ctx, pool, joinedPurgeMigrationsDir()); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SET LOCAL session_replication_role=replica`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO accounts(id,email) VALUES(47,'nas-restore@example.invalid');
INSERT INTO account_api_keys(id,account_id,key_prefix,secret_hash) VALUES(47,47,'fixture-key','fixture-key');
INSERT INTO connections(id,account_id,kind,api_key_id) VALUES(13,47,'nas_pull',47);
INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,local_timezone,capture_type,source_family,execution_class,capture_family,expected_fps)
 VALUES(3,'test','3','test','test','https://storage.example.test','off','Asia/Tokyo','hls','video_manifest','video_live','continuous_video',30);
INSERT INTO storage_destinations(id,account_id,name,endpoint,region,bucket,access_key_id,secret_access_key_enc)
 VALUES(1,47,'test','https://storage.example.test','auto','bucket','fixture',decode('00','hex'));
INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at)
 VALUES(30,47,1,'rec','https://storage.example.test','* * * * *','Asia/Tokyo','paused',3,now());
INSERT INTO recording_collation_hours(id,hour_id,batch_id,generation,recording_id,local_date,delivery_hour,status,manifest_object_key,manifest_sha256,
 manifest,source_clip_count,included_clip_count,excluded_clip_count,join_count,split_count)
 VALUES(1,'nightly-collation-v2__recording-30__date-2026-10-01__hour-05__generation-2','nightly-collation-v2',2,30,'2026-10-01',5,'collated','m/1',repeat('a',64),'{}',1,1,0,0,0);
INSERT INTO recording_collation_outputs(id,collation_hour_id,part,parts,object_key,nas_relative_path,size_bytes,sha256,start_at,end_at,source_clip_ids,r2_verified_at)
 SELECT i,1,i,3,'joined/nightly-collation-v2/objects/'||repeat(i::text,64)||'.mp4','Rec/October/01-Wednesday/Rec_hour_05_part_0'||i||'_050000-060000.mp4',
  100*i,repeat(i::text,64),'2026-09-30T20:00:00Z','2026-09-30T21:00:00Z',ARRAY[1],now() FROM generate_series(1,3) i;
INSERT INTO recording_collation_output_acks(output_id,connection_id,nas_relative_path,size_bytes,sha256)
 SELECT id,13,nas_relative_path,size_bytes,sha256 FROM recording_collation_outputs WHERE id IN (1,2);
INSERT INTO nas_restore_output_scope(output_id) VALUES(2),(3);`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	ids := filepath.Join(t.TempDir(), "ids")
	if err := os.WriteFile(ids, []byte("999 1 3 2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	opts, err := parseNASRestoreRequestArgs([]string{"--output-ids-file", ids, "--label", "o1"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	s, err := requestNASRestores(ctx, pool, "", opts)
	if err != nil || s.Target != "output" || s.Selected != 4 || s.Queued != 1 || s.QueuedBytes != 200 ||
		s.Skipped["unknown_output"] != 1 || s.Skipped["not_scoped"] != 1 || s.Skipped["nas_unacknowledged"] != 1 {
		t.Fatalf("request %+v err=%v", s, err)
	}
	var outputID int64
	var clip *int64
	var key, path string
	var admitted bool
	if err := pool.QueryRow(ctx, `SELECT output_id,clip_id,object_key,relative_path,nas_restore_admitted(q) FROM nas_restore_requests q`).Scan(
		&outputID, &clip, &key, &path, &admitted); err != nil || outputID != 2 || clip != nil || !admitted ||
		key != "joined/nightly-collation-v2/objects/"+strings.Repeat("2", 64)+".mp4" || path != "joined/Rec/October/01-Wednesday/Rec_hour_05_part_02_050000-060000.mp4" {
		t.Fatalf("queued output=%d clip=%v key=%s path=%s admitted=%v err=%v", outputID, clip, key, path, admitted, err)
	}
	if s, err = requestNASRestores(ctx, pool, "", opts); err != nil || s.Queued != 0 || s.AlreadyQueued != 1 {
		t.Fatalf("re-request %+v err=%v", s, err)
	}
	// A lease whose scope was withdrawn is never requeued by the API; cancel
	// ends it once expired, and leaves a live lease alone.
	if _, err := pool.Exec(ctx, `UPDATE nas_restore_requests SET state='leased',attempts=1,lease_expires_at=now()+interval '1 minute'`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM nas_restore_output_scope WHERE output_id=2`); err != nil {
		t.Fatal(err)
	}
	if n, err := cancelNASRestores(ctx, pool, "o1"); err != nil || n != 0 {
		t.Fatalf("cancel of a live lease canceled=%d err=%v", n, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE nas_restore_requests SET lease_expires_at=now()-interval '1 minute'`); err != nil {
		t.Fatal(err)
	}
	var state string
	if n, err := cancelNASRestores(ctx, pool, "o1"); err != nil || n != 1 ||
		pool.QueryRow(ctx, `SELECT state FROM nas_restore_requests`).Scan(&state) != nil || state != "canceled" {
		t.Fatalf("cancel of an expired lease canceled=%d state=%s err=%v", n, state, err)
	}
}
