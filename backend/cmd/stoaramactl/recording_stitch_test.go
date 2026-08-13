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
CREATE TABLE recording_qualification_runs(id bigint primary key,account_id bigint,status text);
CREATE TABLE recording_qualification_members(run_id bigint,account_id bigint,recording_id bigint,cron_timezone text,daily_window_start time,daily_window_end time,active_weekdays smallint,schedule_start_at timestamptz,schedule_end_at timestamptz,window_generator_version text,schedule_config_sha256 text,window_sequence_sha256 text,primary key(run_id,recording_id));
CREATE TABLE recording_qualification_windows(run_id bigint,recording_id bigint,ordinal integer,local_open_at timestamp,local_end_at timestamp,open_utc_offset_seconds integer,end_utc_offset_seconds integer,window_start_at timestamptz,window_end_at timestamptz,primary key(run_id,recording_id,ordinal));
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
	for _, fixture := range []struct {
		query string
		args  []any
	}{
		{`INSERT INTO recording_jobs VALUES(9,7,'done','continuous_window',$3,$3,$1,$2,60,'reccont:7:'||extract(epoch from $1::timestamptz)::bigint)`, []any{start, end, now}},
		{`INSERT INTO recording_window_health VALUES(7,9,$1,2,43200,43200,100,0,0,0,0,0,0,0,1)`, []any{now}},
		{`INSERT INTO recording_clips VALUES(11,7,9,'safe/clip.mp4',4,repeat('a',64),$1,$2,'00000000-0000-4000-8000-000000000001',1,NULL,NULL,NULL,NULL,NULL,NULL,3,'https://storage.example','bucket','clip',$3::timestamptz-interval '20 minutes')`, []any{start, end, now}},
	} {
		if _, err = conn.Exec(ctx, fixture.query, fixture.args...); err != nil {
			t.Fatal(err)
		}
	}
	// Candidate selection is one best historical GOOD window per protected
	// recording before a second wave. A revived job may have scheduled_for after
	// fire_at; reccont fire/window identity is the immutable occurrence contract.
	for _, fixture := range []struct {
		query string
		args  []any
	}{
		{`INSERT INTO recording_campaign_tracks VALUES(1,47,'delivery30','active',$1::timestamptz+interval '9 days')`, []any{end}},
		{`INSERT INTO recording_campaign_roster_entries VALUES(1,7,1,'primary','protect'),(1,8,2,'primary','protect')`, nil},
		{`INSERT INTO recording_jobs VALUES (19,7,'done','continuous_window',$2,$2,$3,$1,60,'reccont:7:'||extract(epoch from $3::timestamptz)::bigint),(20,8,'done','continuous_window',$2,$2,$3,$1,60,'reccont:8:'||extract(epoch from $3::timestamptz)::bigint)`, []any{end, now, start}},
		{`INSERT INTO recording_window_health VALUES (7,19,$1,2,43200,43200,100,0,0,0,0,0,0,0,1),(8,20,$1,2,43200,43200,100,0,0,0,0,0,0,0,1)`, []any{now}},
	} {
		if _, err = conn.Exec(ctx, fixture.query, fixture.args...); err != nil {
			t.Fatal(err)
		}
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
	if _, err = conn.Exec(ctx, `INSERT INTO accounts VALUES(48)`); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(ctx, `INSERT INTO recording_campaign_tracks VALUES(2,48,'delivery30','active',$1::timestamptz+interval '9 days')`, end); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 22; i++ {
		recordingID, jobID := int64(100+i), int64(1000+i)
		for _, fixture := range []struct {
			query string
			args  []any
		}{
			{`INSERT INTO recordings VALUES($1,48,'continuous')`, []any{recordingID}},
			{`INSERT INTO recording_campaign_roster_entries VALUES(2,$1,$2,'primary','protect')`, []any{recordingID, i + 1}},
			{`INSERT INTO recording_jobs VALUES($1,$2,'done','continuous_window',$5,$5,$3,$4,60,'reccont:'||$2::bigint::text||':'||extract(epoch from $3::timestamptz)::bigint)`, []any{jobID, recordingID, start, end, now}},
			{`INSERT INTO recording_window_health VALUES($1,$2,$3,2,43200,43200,100,0,0,0,0,0,0,0,1)`, []any{recordingID, jobID, now}},
		} {
			if _, err = conn.Exec(ctx, fixture.query, fixture.args...); err != nil {
				t.Fatal(err)
			}
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
	var qualificationScope string
	if err = conn.QueryRow(ctx, `SELECT qualification_scope FROM recording_native_stitch_tasks WHERE recording_job_id=9`).Scan(&qualificationScope); err != nil || qualificationScope != "byte_run_audit" {
		t.Fatalf("unfrozen historical job scope=%q err=%v", qualificationScope, err)
	}
	var tasks, taskClips int
	if err = conn.QueryRow(ctx, `SELECT count(*),(
		SELECT count(*)
		FROM recording_native_stitch_task_clips tc
		JOIN recording_native_stitch_tasks scoped ON scoped.id=tc.task_id
		WHERE scoped.recording_job_id=9
	) FROM recording_native_stitch_tasks WHERE recording_job_id=9`).Scan(&tasks, &taskClips); err != nil || tasks != 1 || taskClips != 1 {
		t.Fatalf("tasks=%d task_clips=%d err=%v", tasks, taskClips, err)
	}

	// Only a frozen qualification occurrence rebuilt by the shared scheduler
	// may be surfaced as qualification evidence. A wrong frozen UTC offset is
	// rejected rather than inferred from the current recording schedule.
	for _, fixture := range []struct {
		query string
		args  []any
	}{
		{`INSERT INTO recording_qualification_runs VALUES(1,47,'active')`, nil},
		{`INSERT INTO recording_qualification_members VALUES(1,47,7,'UTC','08:00','20:00',127,$1::timestamptz-interval '1 day',NULL,'qualification-windows-v1',repeat('b',64),repeat('c',64))`, []any{start}},
		{`INSERT INTO recording_qualification_windows VALUES(1,7,1,$1::timestamptz::timestamp,$2::timestamptz::timestamp,0,0,$1::timestamptz,$2::timestamptz)`, []any{start, end}},
	} {
		if _, err = conn.Exec(ctx, fixture.query, fixture.args...); err != nil {
			t.Fatal(err)
		}
	}
	tx, _ = conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	authority, authorityErr := loadStitchOccurrenceAuthority(ctx, tx, 47, 7, start, end)
	_ = tx.Rollback(ctx)
	if authorityErr != nil || authority.Scope != "authoritative_occurrence" {
		t.Fatalf("frozen occurrence authority=%+v err=%v", authority, authorityErr)
	}
	if _, err = conn.Exec(ctx, `UPDATE recording_qualification_windows SET open_utc_offset_seconds=3600 WHERE run_id=1`); err != nil {
		t.Fatal(err)
	}
	tx, _ = conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	_, authorityErr = loadStitchOccurrenceAuthority(ctx, tx, 47, 7, start, end)
	_ = tx.Rollback(ctx)
	if authorityErr == nil {
		t.Fatal("mismatched frozen occurrence offset was accepted")
	}
	if _, err = conn.Exec(ctx, `UPDATE recording_qualification_windows SET open_utc_offset_seconds=0 WHERE run_id=1`); err != nil {
		t.Fatal(err)
	}
	dstStart := time.Date(2026, 3, 8, 12, 0, 0, 0, time.UTC)
	dstEnd := dstStart.Add(12 * time.Hour)
	for _, fixture := range []struct {
		query string
		args  []any
	}{
		{`INSERT INTO recording_qualification_runs VALUES(2,47,'active')`, nil},
		{`INSERT INTO recording_qualification_members VALUES(2,47,8,'America/New_York','08:00','20:00',127,$1::timestamptz-interval '1 day',NULL,'qualification-windows-v1',repeat('d',64),repeat('e',64))`, []any{dstStart}},
		{`INSERT INTO recording_qualification_windows VALUES(2,8,1,'2026-03-08 08:00:00','2026-03-08 20:00:00',-18000,-14400,$1,$2)`, []any{dstStart, dstEnd}},
	} {
		if _, err = conn.Exec(ctx, fixture.query, fixture.args...); err != nil {
			t.Fatal(err)
		}
	}
	tx, _ = conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	_, authorityErr = loadStitchOccurrenceAuthority(ctx, tx, 47, 8, dstStart, dstEnd)
	_ = tx.Rollback(ctx)
	if authorityErr == nil {
		t.Fatal("DST occurrence with a pre-transition open offset was accepted")
	}

	// A fresh intent blocks planning under the transaction's DB-derived time.
	for _, fixture := range []struct {
		query string
		args  []any
	}{
		{`INSERT INTO recording_jobs VALUES(10,7,'done','continuous_window',$3,$3,$1,$2,60,'reccont:7:'||extract(epoch from $1::timestamptz)::bigint)`, []any{start, end, now}},
		{`INSERT INTO recording_window_health SELECT 7,10,$1,2,43200,43200,100,0,0,0,0,0,0,0,1`, []any{now}},
		{`UPDATE recording_clips SET recording_job_id=10 WHERE id=11`, nil},
		{`INSERT INTO recording_upload_intents VALUES(10,'pending',$1::timestamptz+interval '1 minute',7,3,'https://storage.example','bucket','next','safe/next.mp4',100)`, []any{now}},
	} {
		if _, err = conn.Exec(ctx, fixture.query, fixture.args...); err != nil {
			t.Fatal(err)
		}
	}
	tx, _ = conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	candidate.JobID = 10
	if outcome, reason := planOneNativeStitch(ctx, tx, candidate, true); outcome != "rejected" || reason != "active_upload_intent" {
		t.Fatalf("active intent outcome=%s reason=%s", outcome, reason)
	}
	_ = tx.Rollback(ctx)

	// A consumed reservation without its exact object identity also fails closed.
	if _, err = conn.Exec(ctx, `UPDATE recording_upload_intents SET status='consumed',expires_at=$1::timestamptz-interval '1 minute' WHERE recording_job_id=10`, now); err != nil {
		t.Fatal(err)
	}
	tx, _ = conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if outcome, reason := planOneNativeStitch(ctx, tx, candidate, true); outcome != "rejected" || reason != "consumed_intent_without_exact_clip" {
		t.Fatalf("consumed race outcome=%s reason=%s", outcome, reason)
	}
	_ = tx.Rollback(ctx)
}
