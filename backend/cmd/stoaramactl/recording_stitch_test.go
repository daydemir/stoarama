package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/stitchcert"
	"github.com/jackc/pgx/v5"
)

func TestPlanOneNativeStitchPostgresHistoricalPartialAndIntentFence(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	schema := fmt.Sprintf("stitch_plan_%d", time.Now().UnixNano())
	if _, err = conn.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	defer conn.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
	if _, err = conn.Exec(ctx, "SET search_path TO "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	ddl := `
CREATE TABLE accounts(id bigint primary key);
CREATE TABLE connections(id bigint primary key);
CREATE TABLE recordings(id bigint primary key,account_id bigint not null,mode text not null);
CREATE TABLE recording_jobs(id bigint primary key,recording_id bigint,status text,kind text,completed_at timestamptz,scheduled_for timestamptz,fire_at timestamptz,window_end_at timestamptz,clip_duration_sec integer,idempotency_key text);
CREATE TABLE recording_window_health(recording_id bigint,job_id bigint,calculated_at timestamptz,metric_version integer,expected_seconds bigint,covered_seconds double precision,coverage_pct double precision,largest_gap_seconds double precision,gap_count integer,gap_over_30s_count integer,gap_over_5m_count integer,overlap_count integer,overlap_seconds double precision,layout_change_count integer,clip_count integer,primary key(recording_id,job_id));
CREATE TABLE recording_upload_intents(recording_job_id bigint,status text,expires_at timestamptz,recording_id bigint,storage_destination_id bigint,endpoint text,bucket text,object_key text,display_path text,max_size_bytes bigint);
CREATE TABLE recording_clips(id bigint primary key,recording_id bigint,recording_job_id bigint,display_path text,size_bytes bigint,sha256 text,clip_start_at timestamptz,clip_end_at timestamptz,capture_lease_token uuid,capture_sequence bigint,purged_at timestamptz,capture_attempt_id uuid,timestamp_contract_version text,timestamp_contract jsonb,timestamp_contract_status text,timestamp_contract_reason text,storage_destination_id bigint,endpoint text,bucket text,object_key text,created_at timestamptz);
CREATE TABLE recording_campaign_tracks(id bigint primary key,account_id bigint,campaign_key text,state text,deadline_at timestamptz);
CREATE TABLE recording_campaign_roster_entries(track_id bigint,recording_id bigint,rank integer,role text,status text);
INSERT INTO accounts VALUES(47); INSERT INTO connections VALUES(8); INSERT INTO recordings VALUES(7,47,'continuous'),(8,47,'continuous');`
	if _, err = conn.Exec(ctx, ddl); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "infra", "sql", "migrations", "0133_recording_native_stitch_certification.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(ctx, string(raw)); err != nil {
		t.Fatalf("apply native stitch migration: %v", err)
	}
	start := time.Date(2026, 8, 10, 8, 0, 0, 0, time.UTC)
	end := start.Add(12 * time.Hour)
	now := end.Add(time.Hour)
	if _, err = conn.Exec(ctx, `
INSERT INTO recording_jobs VALUES(9,7,'done','continuous_window',$3,$3,$1,$2,60,'reccont:7:'||extract(epoch from $1)::bigint);
INSERT INTO recording_window_health VALUES(7,9,$3,2,43200,43200,100,0,0,0,0,0,0,0,1);
INSERT INTO recording_clips VALUES(11,7,9,'safe/clip.mp4',4,repeat('a',64),$1,$2,'00000000-0000-4000-8000-000000000001',1,NULL,NULL,NULL,NULL,NULL,NULL,3,'https://storage.example','bucket','clip', $3-interval '20 minutes')`, start, end, now); err != nil {
		t.Fatal(err)
	}
	// Candidate selection is one best historical GOOD window per protected
	// recording before a second wave. A revived job may have scheduled_for after
	// fire_at; reccont fire/window identity is the immutable occurrence contract.
	if _, err = conn.Exec(ctx, `
INSERT INTO recording_campaign_tracks VALUES(1,47,'delivery30','active',$1+interval '9 days');
INSERT INTO recording_campaign_roster_entries VALUES(1,7,1,'primary','protect'),(1,8,2,'primary','protect');
INSERT INTO recording_jobs VALUES
 (19,7,'done','continuous_window',$2,$2,$3,$1,60,'reccont:7:'||extract(epoch from $3)::bigint),
 (20,8,'done','continuous_window',$2,$2,$3,$1,60,'reccont:8:'||extract(epoch from $3)::bigint);
INSERT INTO recording_window_health VALUES
 (7,19,$2,2,43200,43200,100,0,0,0,0,0,0,0,1),
 (8,20,$2,2,43200,43200,100,0,0,0,0,0,0,0,1)`, end, now, start); err != nil {
		t.Fatal(err)
	}
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		t.Fatal(err)
	}
	selected, err := selectNativeStitchCandidates(ctx, tx, 47, 2)
	_ = tx.Rollback(ctx)
	if err != nil || len(selected) != 2 || selected[0].RecordingID == selected[1].RecordingID {
		t.Fatalf("first-wave candidates=%+v err=%v", selected, err)
	}
	// Existing top-ranked tasks are excluded before LIMIT, so later protected
	// recordings cannot starve behind the same first page forever.
	if _, err = conn.Exec(ctx, `INSERT INTO accounts VALUES(48); INSERT INTO recording_campaign_tracks VALUES(2,48,'delivery30','active',$1+interval '9 days')`, end); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 22; i++ {
		recordingID, jobID := int64(100+i), int64(1000+i)
		if _, err = conn.Exec(ctx, `INSERT INTO recordings VALUES($1,48,'continuous'); INSERT INTO recording_campaign_roster_entries VALUES(2,$1,$2,'primary','protect'); INSERT INTO recording_jobs VALUES($3,$1,'done','continuous_window',$6,$6,$4,$5,60,'reccont:'||$1||':'||extract(epoch from $4)::bigint); INSERT INTO recording_window_health VALUES($1,$3,$6,2,43200,43200,100,0,0,0,0,0,0,0,1)`, recordingID, i+1, jobID, start, end, now); err != nil {
			t.Fatal(err)
		}
	}
	tx, _ = conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	firstPage, err := selectNativeStitchCandidates(ctx, tx, 48, 20)
	_ = tx.Rollback(ctx)
	if err != nil || len(firstPage) != 20 {
		t.Fatalf("first bounded page=%d err=%v", len(firstPage), err)
	}
	for _, candidate := range firstPage {
		if _, err = conn.Exec(ctx, `INSERT INTO recording_native_stitch_tasks(account_id,recording_id,recording_job_id,window_start_at,window_end_at,health_calculated_at,health_metric_version,health_facts,job_schedule_facts,clip_manifest,clip_manifest_sha256,clip_count,source_bytes,policy_version) VALUES(48,$1,$2,$3,$4,$5,2,'{}','{}','[{}]',repeat('a',64),1,1,$6)`, candidate.RecordingID, candidate.JobID, candidate.FireAt, candidate.WindowEnd, candidate.HealthCalculated, stitchcert.PolicyVersion); err != nil {
			t.Fatal(err)
		}
	}
	tx, _ = conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	secondPage, err := selectNativeStitchCandidates(ctx, tx, 48, 20)
	_ = tx.Rollback(ctx)
	if err != nil || len(secondPage) != 2 {
		t.Fatalf("second bounded page=%d want=2 err=%v", len(secondPage), err)
	}
	tx, err = conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		t.Fatal(err)
	}
	candidate := stitchPlanCandidate{AccountID: 47, RecordingID: 7, JobID: 9, Rank: 1, FireAt: start, WindowEnd: end, DBNow: now}
	if outcome, reason := planOneNativeStitch(ctx, tx, candidate, true); outcome != "created" || reason != "" {
		t.Fatalf("historical bytes/run task outcome=%s reason=%s", outcome, reason)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var tasks, taskClips int
	if err = conn.QueryRow(ctx, `SELECT count(*),(SELECT count(*) FROM recording_native_stitch_task_clips) FROM recording_native_stitch_tasks`).Scan(&tasks, &taskClips); err != nil || tasks != 1 || taskClips != 1 {
		t.Fatalf("tasks=%d task_clips=%d err=%v", tasks, taskClips, err)
	}

	// A fresh intent blocks planning under the transaction's DB-derived time.
	if _, err = conn.Exec(ctx, `INSERT INTO recording_jobs VALUES(10,7,'done','continuous_window',$3,$3,$1,$2,60,'reccont:7:'||extract(epoch from $1)::bigint); INSERT INTO recording_window_health SELECT 7,10,$3,2,43200,43200,100,0,0,0,0,0,0,0,1; UPDATE recording_clips SET recording_job_id=10 WHERE id=11; INSERT INTO recording_upload_intents VALUES(10,'pending',$3+interval '1 minute',7,3,'https://storage.example','bucket','next','safe/next.mp4',100)`, start, end, now); err != nil {
		t.Fatal(err)
	}
	tx, _ = conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	candidate.JobID = 10
	if outcome, reason := planOneNativeStitch(ctx, tx, candidate, true); outcome != "rejected" || reason != "active_upload_intent" {
		t.Fatalf("active intent outcome=%s reason=%s", outcome, reason)
	}
	_ = tx.Rollback(ctx)

	// A consumed reservation without its exact object identity also fails closed.
	if _, err = conn.Exec(ctx, `UPDATE recording_upload_intents SET status='consumed',expires_at=$1-interval '1 minute' WHERE recording_job_id=10`, now); err != nil {
		t.Fatal(err)
	}
	tx, _ = conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if outcome, reason := planOneNativeStitch(ctx, tx, candidate, true); outcome != "rejected" || reason != "consumed_intent_without_exact_clip" {
		t.Fatalf("consumed race outcome=%s reason=%s", outcome, reason)
	}
	_ = tx.Rollback(ctx)
}
