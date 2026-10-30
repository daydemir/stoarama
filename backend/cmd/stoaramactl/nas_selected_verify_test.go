package main

import (
	"context"
	"os"
	"strings"
	"testing"
)

func TestParseNASSelectedVerifyRequestArgs(t *testing.T) {
	opts, err := parseNASSelectedVerifyRequestArgs([]string{"--clip-ids", "3,1", "--label", "chosen-28", "--expected-plan-sha256", strings.Repeat("a", 64)})
	if err != nil || len(opts.clipIDs) != 2 || opts.clipIDs[0] != 3 || opts.label != "chosen-28" {
		t.Fatalf("opts=%+v err=%v", opts, err)
	}
	for _, args := range [][]string{nil, {"--clip-ids", "1"}, {"--clip-ids", "x", "--label", "x"}, {"--clip-ids", "1", "--label", "Bad Label"}, {"--clip-ids", "1", "--label", "x", "--expected-plan-sha256", "bad"}} {
		if _, err := parseNASSelectedVerifyRequestArgs(args); err == nil {
			t.Errorf("accepted %v", args)
		}
	}
}

func TestNASSelectedVerifyCanonicalKindsAndBounds(t *testing.T) {
	for _, kind := range []string{"raw", "collated", "joined"} {
		p := "folder/file.mp4"
		if kind != "raw" {
			p = "joined/folder/file.mp4"
		}
		i := nasSelectedVerifyManifestItem{Kind: kind, SourceID: 1, RelativePath: p, SizeBytes: 3, SHA256: strings.Repeat("a", 64), R2Source: "object"}
		if err := validateNASSelectedVerifyIdentity(i); err != nil {
			t.Fatal(err)
		}
		for _, bad := range []string{"/file.mp4", "../file.mp4", "folder/../file.mp4", "folder//file.mp4", "#recycle/f.mp4", "folder\\file.mp4"} {
			j := i
			j.RelativePath = bad
			if err := validateNASSelectedVerifyIdentity(j); err == nil {
				t.Fatalf("accepted %s", bad)
			}
		}
	}
	for _, args := range [][]string{{"--collated-ids", "2", "--joined-ids", "3", "--label", "x", "--dry-run"}, {"--clip-ids", "1", "--label", "x", "--dry-run"}} {
		if _, err := parseNASSelectedVerifyRequestArgs(args); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := parseNASSelectedVerifyRequestArgs([]string{"--clip-ids", "1,1", "--label", "x", "--dry-run"}); err == nil {
		t.Fatal("accepted duplicate IDs")
	}
}

func TestNASSelectedVerifyRequestAndReport(t *testing.T) {
	pool := joinedPurgeTestPool(t, "nas_selected_verify")
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
 CREATE TABLE accounts(id bigint PRIMARY KEY);
 CREATE TABLE connections(id bigint PRIMARY KEY,account_id bigint,kind text);
 CREATE TABLE recordings(id bigint PRIMARY KEY,account_id bigint);
 CREATE TABLE recording_clips(id bigint PRIMARY KEY,recording_id bigint,display_path text,size_bytes bigint,sha256 text,object_key text);
 CREATE TABLE recording_collation_hours(id bigint PRIMARY KEY,recording_id bigint);
 CREATE TABLE recording_collation_outputs(id bigint PRIMARY KEY,collation_hour_id bigint,nas_relative_path text,size_bytes bigint,sha256 text,object_key text);
 CREATE TABLE recording_joined_artifacts(id bigint PRIMARY KEY,account_id bigint,connection_id bigint,artifact_kind text,batch_id text,relative_path text,expected_size_bytes bigint,expected_sha256 text,object_key text);
 INSERT INTO accounts VALUES(47),(48);
 INSERT INTO connections VALUES(13,47,'nas_pull');
 INSERT INTO recordings VALUES(1,47),(2,48);
 INSERT INTO recording_clips VALUES(1,1,'folder/a.mp4',3,repeat('a',64),'raw/a'),(2,2,'folder/b.mp4',3,repeat('b',64),'raw/b');
 INSERT INTO recording_collation_hours VALUES(1,1);
 INSERT INTO recording_collation_outputs VALUES(1,1,'folder/hour.mp4',10,repeat('c',64),'collated/hour');
 INSERT INTO recording_joined_artifacts VALUES(1,47,13,'media','batch','folder/media.mp4',20,repeat('d',64),'joined/media');
 `); err != nil {
		t.Fatal(err)
	}
	migration, err := os.ReadFile("../../../infra/sql/migrations/0165_nas_selected_verify.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		t.Fatal(err)
	}
	o := nasSelectedVerifyOptions{clipIDs: []int64{1}, collatedIDs: []int64{1}, joinedIDs: []int64{1}, label: "first", dryRun: true}
	dry, err := requestNASSelectedVerify(ctx, pool, o)
	if err != nil || dry.Items != 3 || dry.Bytes != 33 {
		t.Fatalf("dry=%+v err=%v", dry, err)
	}
	var count int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM nas_selected_verify_plans`).Scan(&count)
	if count != 0 {
		t.Fatal("dry-run wrote")
	}
	o.dryRun = false
	o.expectedSHA = dry.PlanSHA256
	if _, err := requestNASSelectedVerify(ctx, pool, o); err != nil {
		t.Fatal(err)
	}
	if _, err := requestNASSelectedVerify(ctx, pool, o); err != nil {
		t.Fatalf("retry: %v", err)
	}
	report, err := reportNASSelectedVerify(ctx, pool, "first")
	if err != nil {
		t.Fatal(err)
	}
	items := report["items"].([]nasSelectedVerifyReportItem)
	if len(items) != 3 || items[0].RelativePath != "joined/folder/hour.mp4" || items[1].RelativePath != "joined/batch/folder/media.mp4" {
		t.Fatalf("items=%+v", items)
	}
	o.label = "later"
	if _, err := requestNASSelectedVerify(ctx, pool, o); err != nil {
		t.Fatalf("fresh repeat: %v", err)
	}
	o.label = "foreign"
	o.clipIDs = []int64{2}
	if _, err := requestNASSelectedVerify(ctx, pool, o); err == nil {
		t.Fatal("accepted foreign clip")
	}
	if _, err := pool.Exec(ctx, `UPDATE nas_selected_verify_items SET relative_path='other.mp4'`); err == nil {
		t.Fatal("identity mutable")
	}
	if _, err := pool.Exec(ctx, `UPDATE nas_selected_verify_plans SET label='changed'`); err == nil {
		t.Fatal("plan mutable")
	}
}
