package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseNASRestoreRequestArgs(t *testing.T) {
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
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
