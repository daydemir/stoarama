package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestNASSelectedVerifyCopyRowsPreserveTypedIdentityAndOrdinal(t *testing.T) {
	items := []nasSelectedVerifyManifestItem{
		{Kind: "raw", SourceID: 11, RelativePath: "folder/a.mp4", SizeBytes: 13, SHA256: strings.Repeat("a", 64)},
		{Kind: "joined", SourceID: 17, RelativePath: "joined/folder/b.mp4", SizeBytes: 19, SHA256: strings.Repeat("b", 64)},
	}
	source := nasSelectedVerifyCopySource(23, items)
	want := [][]any{
		{int64(23), int32(1), "raw", int64(11), "folder/a.mp4", int64(13), strings.Repeat("a", 64)},
		{int64(23), int32(2), "joined", int64(17), "joined/folder/b.mp4", int64(19), strings.Repeat("b", 64)},
	}
	for n := range want {
		if !source.Next() {
			t.Fatalf("row %d missing: %v", n, source.Err())
		}
		got, err := source.Values()
		if err != nil || !reflect.DeepEqual(got, want[n]) {
			t.Fatalf("row %d got=%#v want=%#v err=%v", n, got, want[n], err)
		}
	}
	if source.Next() || source.Err() != nil {
		t.Fatalf("unexpected trailing row/error: %v", source.Err())
	}
}

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
	migration, err = os.ReadFile("../../../infra/sql/migrations/0166_nas_original_witness.sql")
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
	if _, err := pool.Exec(ctx, `
 CREATE TABLE streams(id bigint PRIMARY KEY,local_timezone text);
 INSERT INTO streams VALUES(1,'UTC');
 ALTER TABLE recordings ADD COLUMN stream_id bigint,ADD COLUMN cron_timezone text;
 UPDATE recordings SET stream_id=1,cron_timezone='UTC';
 ALTER TABLE recording_clips ADD COLUMN clip_start_at timestamptz,ADD COLUMN clip_end_at timestamptz;
 UPDATE recording_clips SET clip_start_at='2026-07-20T12:00:00Z',clip_end_at='2026-07-20T12:01:00Z';
 `); err != nil {
		t.Fatal(err)
	}
	witness := nasSelectedVerifyOptions{clipIDs: []int64{1}, label: "original", dryRun: true, originalWitness: true}
	wd, err := requestNASSelectedVerify(ctx, pool, witness)
	if err != nil {
		t.Fatalf("original dry-run: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE streams SET local_timezone='Europe/London'`); err != nil {
		t.Fatal(err)
	}
	shifted, err := requestNASSelectedVerify(ctx, pool, witness)
	if err != nil || shifted.PlanSHA256 == wd.PlanSHA256 {
		t.Fatalf("timezone not digest-bound: %+v %v", shifted, err)
	}
	witness.dryRun = false
	witness.expectedSHA = wd.PlanSHA256
	if _, err := requestNASSelectedVerify(ctx, pool, witness); err == nil {
		t.Fatal("stale canonical timezone admitted")
	}
	if _, err := pool.Exec(ctx, `UPDATE streams SET local_timezone='UTC'`); err != nil {
		t.Fatal(err)
	}
	if _, err := requestNASSelectedVerify(ctx, pool, witness); err != nil {
		t.Fatalf("original admission: %v", err)
	}
	wr, err := reportNASSelectedVerify(ctx, pool, "original")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(wr["witness_manifest"].(json.RawMessage)), "registered_start_utc") {
		t.Fatal("immutable registered context absent from report")
	}
	if _, err := pool.Exec(ctx, `UPDATE nas_selected_verify_items SET relative_path='other.mp4'`); err == nil {
		t.Fatal("identity mutable")
	}
	if _, err := pool.Exec(ctx, `UPDATE nas_selected_verify_plans SET label='changed'`); err == nil {
		t.Fatal("plan mutable")
	}
}

func TestOriginalWitnessRequestModeAndBinding(t *testing.T) {
	if _, err := parseNASSelectedVerifyRequestArgs([]string{"--original-witness", "--collated-ids", "1", "--label", "w", "--dry-run"}); err == nil {
		t.Fatal("collated original witness accepted")
	}
	o, err := parseNASSelectedVerifyRequestArgs([]string{"--original-witness", "--clip-ids", "1", "--label", "w", "--dry-run"})
	if err != nil || !o.originalWitness {
		t.Fatalf("raw witness: %+v %v", o, err)
	}
	start := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	context, err := originalWitnessContext(start, start.Add(time.Minute), "UTC")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := originalWitnessContext(start, start.Add(time.Minute), ""); err == nil {
		t.Fatal("empty timezone accepted")
	}
	if _, err := originalWitnessContext(start, start.Add(16*time.Minute), "UTC"); err == nil {
		t.Fatal("oversized span accepted")
	}
	midnight := time.Date(2026, 7, 20, 23, 59, 30, 0, time.UTC)
	if _, err := originalWitnessContext(midnight, midnight.Add(time.Minute), "UTC"); err == nil {
		t.Fatal("cross-midnight attribution accepted")
	}
	plain := nasSelectedVerifyManifestItem{Kind: "raw", SourceID: 1, RelativePath: "f.mp4", SizeBytes: 3, SHA256: strings.Repeat("a", 64), R2Source: "raw"}
	a, _ := json.Marshal(plain)
	if strings.Contains(string(a), "original_witness") {
		t.Fatal("legacy manifest hash would change")
	}
	plain.OriginalWitness = context
	b, _ := json.Marshal(plain)
	if sha256.Sum256(a) == sha256.Sum256(b) {
		t.Fatal("mode absent from digest")
	}
	plain.OriginalWitness.Timezone = "Europe/London"
	d, _ := json.Marshal(plain)
	if sha256.Sum256(b) == sha256.Sum256(d) {
		t.Fatal("timezone absent from digest")
	}
}
