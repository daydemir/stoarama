package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestDeliverySourceCoverageRequiresLatestProofAndExactNASReadback(t *testing.T) {
	pool := joinedPurgeTestPool(t, "delivery_source_coverage")
	ctx := context.Background()
	execSQL := func(sql string) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	execSQL(`
 CREATE TABLE connections(id bigint PRIMARY KEY,account_id bigint,kind text);
 CREATE TABLE recordings(id bigint PRIMARY KEY,account_id bigint,stream_id bigint);
 CREATE TABLE recording_collation_hours(id bigint PRIMARY KEY,recording_id bigint,generation int,status text,
   manifest_object_key text,manifest_sha256 text,hour_id text,batch_id text);
 CREATE TABLE recording_collation_outputs(id bigint PRIMARY KEY,collation_hour_id bigint,part int,parts int,object_key text,
   nas_relative_path text,sha256 text,size_bytes bigint,source_clip_ids bigint[],start_at timestamptz,end_at timestamptz);
 CREATE TABLE delivery_keep_holds(bucket text,object_key text,kind text,etag text,size_bytes bigint,sha256 text);
 CREATE TABLE recording_collation_output_acks(output_id bigint,connection_id bigint,nas_relative_path text,size_bytes bigint,sha256 text);
 CREATE TABLE recording_clips(id bigint PRIMARY KEY,recording_id bigint,bucket text,object_key text,etag text,
   sha256 text,size_bytes bigint,display_path text,clip_start_at timestamptz,clip_end_at timestamptz);
 CREATE TABLE r2_object_retirements(kind text,source_id bigint,stream_id bigint,bucket text,object_key text,sha256 text,size_bytes bigint);
 INSERT INTO connections VALUES(13,47,'nas_pull');
 INSERT INTO recordings VALUES(2,47,123);
 INSERT INTO recording_collation_hours VALUES(3,2,2,'collated','manifest.json',repeat('b',64),'fixture-hour','fixture-batch');
 INSERT INTO recording_collation_outputs VALUES(4,3,1,1,'joined/goodplus-test/objects/demo.mp4',
   'f/July/26-Sunday/f_hour_01_000000-005900.mp4',repeat('c',64),100,ARRAY[5],'2026-08-05T00:00Z','2026-08-05T00:01Z');
 INSERT INTO recording_clips VALUES(5,2,'stoarama','raw/demo.mp4','raw-etag',repeat('d',64),88,
   'test/raw.mp4','2026-08-05T00:00Z','2026-08-05T00:01Z');
 `)
	for _, name := range []string{"0224_delivery_nas_handoff.sql", "0227_delivery_source_coverage.sql"} {
		data, err := os.ReadFile("../../../infra/sql/migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		execSQL(string(data))
	}
	execSQL(`
 INSERT INTO delivery_nas_handoff_mounts VALUES(repeat('a',64),5,6);
 INSERT INTO delivery_nas_handoff_outputs(output_id,recording_id,roster_sha256,picks_sha256,bucket,object_key,
  etag,size_bytes,sha256,nas_relative_path)
 SELECT id,2,repeat('a',64),repeat('b',64),'stoarama',object_key,'demo-etag',size_bytes,sha256,nas_relative_path
 FROM recording_collation_outputs;
 INSERT INTO recording_collation_output_acks SELECT id,13,nas_relative_path,size_bytes,sha256 FROM recording_collation_outputs;
 INSERT INTO delivery_nas_handoff_readbacks(output_id,round,connection_id,size_bytes,mtime_ns,ctime_ns,inode,device,root_inode)
 VALUES(4,0,13,100,2,3,4,5,6);
 `)
	target, err := readDeliveryCoverageTarget(ctx, pool, 4)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC)
	row := deliveryCoverageObservation{
		Source: deliveryCoverageSource{ID: 5, RecordingID: 2, Bucket: "stoarama", Key: "raw/demo.mp4", ETag: "raw-etag",
			SHA: strings.Repeat("d", 64), Bytes: 88, Start: start, End: start.Add(time.Minute)},
		Ordinal: 1, Verdict: deliveryCoverageProven,
		Tracks: map[string]deliveryCoverageTrack{
			"video": {PacketCount: 60, PacketChain: strings.Repeat("1", 64), FrameCount: 60, FrameChain: strings.Repeat("2", 64)},
		},
	}
	report := deliveryCoverageReport{SchemaVersion: 1, ManifestSHA: target.ManifestSHA, HourID: target.HourID,
		BatchID: target.BatchID, Output: target.Output, VerifierSHA: deliveryCoverageVerifierSHA,
		SupportSHA: deliveryCoverageSupportSHA, FFmpegSHA: deliveryCoverageFFmpegSHA, FFprobeSHA: deliveryCoverageFFprobeSHA}
	reserve := func() int64 {
		t.Helper()
		id, err := reserveDeliveryCoverageRun(ctx, pool)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	record := func(runID int64, row deliveryCoverageObservation) error {
		t.Helper()
		report.Sources = []deliveryCoverageObservation{row}
		raw, err := json.Marshal(report)
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := parseDeliveryCoverageReport(raw, target)
		if err != nil {
			t.Fatal(err)
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if err := storeDeliveryCoverage(ctx, tx, runID, target, parsed, raw); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	write := func(row deliveryCoverageObservation) {
		t.Helper()
		if err := record(reserve(), row); err != nil {
			t.Fatal(err)
		}
	}
	want := func(count int) {
		t.Helper()
		var got int
		err := pool.QueryRow(ctx, "SELECT count(*) FROM delivery_source_coverage_certified").Scan(&got)
		if err != nil || got != count {
			t.Fatalf("certified=%d, want %d, err=%v", got, count, err)
		}
	}
	write(row)
	want(0)
	execSQL(`
 INSERT INTO delivery_nas_handoff_rounds(roster_sha256,round) VALUES(repeat('a',64),1);
 INSERT INTO delivery_nas_handoff_readbacks(output_id,round,connection_id,size_bytes,mtime_ns,ctime_ns,inode,device,root_inode)
 VALUES(4,1,13,100,2,3,4,5,6);
 `)
	want(0)
	execSQL(`
 WITH m(body) AS (
  SELECT jsonb_build_array(jsonb_build_array(output_id,nas_relative_path,jsonb_build_array(100,2,3,4,5)))
  FROM delivery_nas_handoff_outputs WHERE output_id=4
 )
 INSERT INTO delivery_nas_handoff_checks(roster_sha256,round,manifest,manifest_sha256,requested_at,checked_at)
 SELECT repeat('a',64),1,body,encode(sha256(convert_to(body::text,'UTF8')),'hex'),
  clock_timestamp(),clock_timestamp() FROM m;
 `)

	want(1)
	execSQL("UPDATE recording_clips SET display_path='other/raw.mp4' WHERE id=5")
	want(0)
	execSQL("UPDATE recording_clips SET display_path='test/raw.mp4' WHERE id=5")
	want(1)
	execSQL("UPDATE recording_collation_outputs SET source_clip_ids=ARRAY[5,6] WHERE id=4")
	want(0)
	execSQL("UPDATE recording_collation_outputs SET source_clip_ids=ARRAY[5] WHERE id=4")
	want(1)
	for _, drift := range []struct{ change, restore string }{
		{"UPDATE recording_collation_outputs SET parts=2 WHERE id=4", "UPDATE recording_collation_outputs SET parts=1 WHERE id=4"},
		{"UPDATE recording_collation_outputs SET part=2,parts=2 WHERE id=4", "UPDATE recording_collation_outputs SET part=1,parts=1 WHERE id=4"},
		{"UPDATE recording_collation_outputs SET end_at=end_at+interval '1 second' WHERE id=4", "UPDATE recording_collation_outputs SET end_at=end_at-interval '1 second' WHERE id=4"},
		{"UPDATE recording_collation_hours SET hour_id='other-hour' WHERE id=3", "UPDATE recording_collation_hours SET hour_id='fixture-hour' WHERE id=3"},
		{"UPDATE recording_collation_hours SET batch_id='other-batch' WHERE id=3", "UPDATE recording_collation_hours SET batch_id='fixture-batch' WHERE id=3"},
	} {
		execSQL(drift.change)
		want(0)
		if err := record(reserve(), row); err == nil || !strings.Contains(err.Error(), "catalog changed") {
			t.Fatalf("recording did not reject catalog drift after %s: %v", drift.change, err)
		}
		execSQL(drift.restore)
		want(1)
	}
	execSQL(`INSERT INTO recording_collation_outputs
	 SELECT 6,collation_hour_id,2,2,object_key||'.other',nas_relative_path||'.other',sha256,size_bytes,
	        source_clip_ids,start_at,end_at FROM recording_collation_outputs WHERE id=4`)
	want(0)
	execSQL("DELETE FROM recording_collation_outputs WHERE id=6")
	want(1)
	execSQL(`INSERT INTO r2_object_retirements
	 SELECT 'collated',4,123,bucket,object_key,sha256,size_bytes FROM delivery_nas_handoff_outputs WHERE output_id=4`)
	want(1)
	execSQL("UPDATE r2_object_retirements SET stream_id=124")
	want(0)
	execSQL("UPDATE r2_object_retirements SET stream_id=123,size_bytes=99")
	want(0)
	execSQL("UPDATE r2_object_retirements SET size_bytes=100,source_id=6")
	want(0)
	execSQL("UPDATE r2_object_retirements SET source_id=4,object_key='other-key'")
	want(0)
	execSQL("DELETE FROM r2_object_retirements")
	want(1)
	execSQL(`INSERT INTO r2_object_retirements
	 SELECT 'raw',id,123,bucket,object_key,sha256,size_bytes FROM recording_clips WHERE id=5`)
	want(1)
	execSQL("UPDATE r2_object_retirements SET stream_id=124")
	want(0)
	execSQL("UPDATE r2_object_retirements SET stream_id=123,size_bytes=87")
	want(0)
	execSQL("UPDATE r2_object_retirements SET size_bytes=88,source_id=6")
	want(0)
	execSQL("DELETE FROM r2_object_retirements")
	want(1)

	passed := row
	earlier := reserve()
	row.Verdict, row.Tracks, row.Reason = deliveryCoverageUnproven, nil, "visible audio mismatch"
	write(row)
	want(0)
	if err := record(earlier, passed); err != nil {
		t.Fatal(err)
	}
	want(0)
	write(passed)
	want(1)
}

func TestDeliveryCoverageCopiesOnlyPinnedRegularExecutables(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "tool")
	copy := filepath.Join(dir, "verified")
	payload := []byte("reviewed media tool")
	digest := fmt.Sprintf("%x", sha256.Sum256(payload))
	if err := os.WriteFile(source, payload, 0700); err != nil {
		t.Fatal(err)
	}
	if err := copyPinnedCoverageTool(source, copy, digest); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(source, []byte("different tool"), 0700); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(copy); err != nil || !bytes.Equal(data, payload) {
		t.Fatalf("the verified executable followed source mutation: %q, %v", data, err)
	}
	fifo := filepath.Join(dir, "fifo")
	if err := unix.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	if err := copyPinnedCoverageTool(fifo, filepath.Join(dir, "rejected"), digest); err == nil {
		t.Fatal("accepted a non-regular executable")
	}
	if _, err := readCoveragePinnedDocument(fifo, digest); err == nil {
		t.Fatal("accepted a non-regular document")
	}
}
