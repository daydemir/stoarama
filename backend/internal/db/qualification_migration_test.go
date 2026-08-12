package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestRecordingQualificationMigrationFreezesCompleteCohort(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run qualification migration regression")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect test database: %v", err)
	}
	defer conn.Close(context.Background())

	schema := fmt.Sprintf("qualification_%d", time.Now().UnixNano())
	if _, err := conn.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), `SET search_path TO public`)
		_, _ = conn.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
	}()
	if _, err := conn.Exec(ctx, `SET search_path TO `+pgx.Identifier{schema}.Sanitize()+`; SET TIME ZONE 'UTC'`); err != nil {
		t.Fatalf("set schema: %v", err)
	}
	if _, err := conn.Exec(ctx, `
		CREATE TABLE memberships (user_id BIGINT NOT NULL,org_id BIGINT NOT NULL,accepted_at TIMESTAMPTZ);
		CREATE TABLE media_objects (id BIGINT PRIMARY KEY,sha256 TEXT);
		CREATE TABLE frames (
		  id BIGINT PRIMARY KEY,stream_id BIGINT NOT NULL,raw_media_object_id BIGINT,
		  captured_at TIMESTAMPTZ NOT NULL,capture_status TEXT NOT NULL
		);
		CREATE TABLE recordings (
		  id BIGINT PRIMARY KEY,account_id BIGINT NOT NULL,stream_id BIGINT,name TEXT NOT NULL,
		  mode TEXT NOT NULL,status TEXT NOT NULL,cron_timezone TEXT NOT NULL,
		  daily_window_start TIME,daily_window_end TIME,active_weekdays SMALLINT NOT NULL,
		  start_at TIMESTAMPTZ NOT NULL,end_at TIMESTAMPTZ
		);
		INSERT INTO memberships VALUES (7,47,now());
		INSERT INTO recordings
		SELECT 1000+n,47,2000+n,'recording-'||n,'continuous','active','UTC',
		       '08:00','20:00',127,'2026-08-01 00:00:00+00',NULL
		FROM generate_series(1,50) n;
	`); err != nil {
		t.Fatalf("create evidence prerequisites: %v", err)
	}

	migration, err := os.ReadFile(filepath.Join("..", "..", "..", "infra", "sql", "migrations", "0122_recording_qualification_cohorts.sql"))
	if err != nil {
		t.Fatalf("read qualification migration: %v", err)
	}
	if _, err := conn.Exec(ctx, string(migration)); err != nil {
		t.Fatalf("apply qualification migration: %v", err)
	}
	if _, err := conn.Exec(ctx, `
		INSERT INTO media_objects
		SELECT 5000+n,lpad(to_hex(5000+n),64,'0') FROM generate_series(1,50) n;
		INSERT INTO frames
		SELECT 3000+n,2000+n,5000+n,now()-interval '1 hour','success' FROM generate_series(1,50) n;
		INSERT INTO recording_scene_frame_evidence (
		  account_id,stream_id,frame_id,media_object_id,captured_at,frame_sha256,
		  scene_identity_sha256,verification_method,verified_by_user_id,verified_at,evidence_sha256
		)
		SELECT 47,f.stream_id,f.id,f.raw_media_object_id,f.captured_at,m.sha256,
		       lpad(to_hex(n),64,'0'),'operator_visual',7,now(),repeat('0',64)
		FROM generate_series(1,50) n
		JOIN frames f ON f.id=3000+n
		JOIN media_objects m ON m.id=f.raw_media_object_id
	`); err != nil {
		t.Fatalf("insert scene evidence: %v", err)
	}
	if _, err := conn.Exec(ctx, `
		INSERT INTO recording_scene_frame_evidence (
		  account_id,stream_id,frame_id,media_object_id,captured_at,frame_sha256,
		  scene_identity_sha256,verification_method,verified_by_user_id,verified_at,evidence_sha256
		)
		SELECT 47,f.stream_id,f.id,f.raw_media_object_id,f.captured_at,m.sha256,
		       repeat('f',64),'operator_visual',7,now(),repeat('0',64)
		FROM frames f JOIN media_objects m ON m.id=f.raw_media_object_id WHERE f.id=3001
	`); err == nil {
		t.Fatal("same authoritative frame qualified as a second scene")
	}

	var runID int64
	err = conn.QueryRow(ctx, `
		INSERT INTO recording_qualification_runs (
		  account_id,definition_version,definition_jsonb,target_recording_count,window_sequence_start_at
		) VALUES (47,'timeline-14-v1','{"scope":"timeline_and_certification"}'::jsonb,50,'2026-08-13 08:00:00+00')
		RETURNING id
	`).Scan(&runID)
	if err != nil {
		t.Fatalf("insert run: %v", err)
	}

	if _, err := conn.Exec(ctx, `
		INSERT INTO recording_qualification_members (
		  run_id,account_id,recording_id,ordinal,stream_id,recording_name,stream_name,
		  scene_identity_sha256,scene_frame_evidence_id,cron_timezone,daily_window_start,daily_window_end,
		  active_weekdays,schedule_start_at,window_generator_version
		)
		SELECT $1,47,1000+n,n,2000+n,'recording-'||n,'stream-'||n,
		       lpad(to_hex(n),64,'0'),e.id,'UTC','08:00','20:00',127,
		       '2026-08-01 00:00:00+00','recsched-next-full-v1'
		FROM generate_series(1,50) n
		JOIN recording_scene_frame_evidence e ON e.stream_id=2000+n AND e.account_id=47
	`, runID); err != nil {
		t.Fatalf("insert members: %v", err)
	}

	if _, err := conn.Exec(ctx, `
		INSERT INTO recording_qualification_windows (
		  run_id,recording_id,ordinal,local_open_at,local_end_at,
		  open_utc_offset_seconds,end_utc_offset_seconds,window_start_at,window_end_at,expected_seconds
		)
		SELECT $1,1000+member,ordinal,
		       timestamp '2026-08-13 08:00:00' + (ordinal-1)*interval '1 day',
		       timestamp '2026-08-13 20:00:00' + (ordinal-1)*interval '1 day',
		       0,0,
		       timestamptz '2026-08-13 08:00:00+00' + (ordinal-1)*interval '1 day',
		       timestamptz '2026-08-13 20:00:00+00' + (ordinal-1)*interval '1 day',43200
		FROM generate_series(1,50) member
		CROSS JOIN generate_series(1,14) ordinal
	`, runID); err != nil {
		t.Fatalf("insert expected windows: %v", err)
	}
	if _, err := conn.Exec(ctx, `UPDATE recordings SET stream_id=999999 WHERE id=1001`); err != nil {
		t.Fatalf("prepare authoritative recording: %v", err)
	}
	if _, err := conn.Exec(ctx, `UPDATE recordings SET stream_id=2001 WHERE id=1001`); err != nil {
		t.Fatalf("restore authoritative recording: %v", err)
	}

	// Hold an uncommitted schedule mutation on a second connection. Activation
	// must wait for that authoritative row lock, then validate the committed new
	// version and reject it rather than freezing the old statement snapshot.
	conn2, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect concurrent editor: %v", err)
	}
	defer conn2.Close(context.Background())
	if _, err := conn2.Exec(ctx, `SET search_path TO `+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatalf("set concurrent schema: %v", err)
	}
	tx2, err := conn2.Begin(ctx)
	if err != nil {
		t.Fatalf("begin concurrent edit: %v", err)
	}
	if _, err := tx2.Exec(ctx, `UPDATE recordings SET stream_id=999999 WHERE id=1001`); err != nil {
		t.Fatalf("concurrent authoritative edit: %v", err)
	}
	activationResult := make(chan error, 1)
	go func() {
		_, activateErr := conn.Exec(ctx, `UPDATE recording_qualification_runs SET status='active' WHERE id=$1`, runID)
		activationResult <- activateErr
	}()
	select {
	case err := <-activationResult:
		t.Fatalf("activation did not serialize behind authoritative edit: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("commit concurrent edit: %v", err)
	}
	if err := <-activationResult; err == nil {
		t.Fatal("activation ignored committed concurrent recording mutation")
	}
	if _, err := conn.Exec(ctx, `UPDATE recordings SET stream_id=2001 WHERE id=1001`); err != nil {
		t.Fatalf("restore after concurrent edit: %v", err)
	}

	if _, err := conn.Exec(ctx, `UPDATE recording_qualification_runs SET status='active' WHERE id=$1`, runID); err != nil {
		t.Fatalf("activate complete cohort: %v", err)
	}
	var status, definitionSHA, cohortSHA, windowsSHA string
	var dueAt, frozenAt time.Time
	if err := conn.QueryRow(ctx, `
		SELECT status,definition_sha256,cohort_sha256,windows_sha256,qualification_due_at,frozen_at
		FROM recording_qualification_runs WHERE id=$1
	`, runID).Scan(&status, &definitionSHA, &cohortSHA, &windowsSHA, &dueAt, &frozenAt); err != nil {
		t.Fatalf("read activated run: %v", err)
	}
	if status != "active" || len(definitionSHA) != 64 || len(cohortSHA) != 64 || len(windowsSHA) != 64 {
		t.Fatalf("activation evidence status=%q hashes=%d/%d/%d", status, len(definitionSHA), len(cohortSHA), len(windowsSHA))
	}
	var missingHashes int
	if err := conn.QueryRow(ctx, `
		SELECT count(*) FROM recording_qualification_members
		WHERE run_id=$1 AND (schedule_config_sha256 IS NULL OR window_sequence_sha256 IS NULL)
	`, runID).Scan(&missingHashes); err != nil || missingHashes != 0 {
		t.Fatalf("member schedule evidence missing=%d err=%v", missingHashes, err)
	}
	if want := time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC); !dueAt.Equal(want) {
		t.Fatalf("qualification due=%s want %s", dueAt, want)
	}
	if frozenAt.IsZero() {
		t.Fatal("activation did not freeze cohort")
	}

	assertRejected := func(label, sql string, args ...any) {
		t.Helper()
		if _, err := conn.Exec(ctx, sql, args...); err == nil {
			t.Fatalf("%s unexpectedly succeeded", label)
		}
	}
	assertRejected("mutate member", `UPDATE recording_qualification_members SET stream_name='rewritten' WHERE run_id=$1 AND ordinal=1`, runID)
	assertRejected("mutate expected window", `DELETE FROM recording_qualification_windows WHERE run_id=$1 AND ordinal=14`, runID)
	assertRejected("reopen run", `UPDATE recording_qualification_runs SET status='building' WHERE id=$1`, runID)
	assertRejected("delete activated run", `DELETE FROM recording_qualification_runs WHERE id=$1`, runID)
	assertRejected("truncate members", `TRUNCATE recording_qualification_members CASCADE`)
	assertRejected("mutate scene evidence", `UPDATE recording_scene_frame_evidence SET verification_method='approved_automated' WHERE account_id=47`)

	var underfilledID int64
	if err := conn.QueryRow(ctx, `
		INSERT INTO recording_qualification_runs (
		  account_id,definition_version,definition_jsonb,target_recording_count,window_sequence_start_at
		) VALUES (47,'timeline-14-v1','{}'::jsonb,50,'2026-08-13 08:00:00+00') RETURNING id
	`).Scan(&underfilledID); err != nil {
		t.Fatalf("insert underfilled run: %v", err)
	}
	assertRejected("activate underfilled run", `UPDATE recording_qualification_runs SET status='active' WHERE id=$1`, underfilledID)
	assertRejected("reparent active member", `UPDATE recording_qualification_members SET run_id=$2 WHERE run_id=$1 AND ordinal=1`, runID, underfilledID)

	if _, err := conn.Exec(ctx, `UPDATE recording_qualification_runs SET status='canceled' WHERE id=$1`, runID); err != nil {
		t.Fatalf("cancel active run: %v", err)
	}
	assertRejected("reopen canceled run", `UPDATE recording_qualification_runs SET status='active' WHERE id=$1`, runID)
}
