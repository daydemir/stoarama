package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestRecordingNativeStitchMigrationAndLifecycle(t *testing.T) {
	url := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if url == "" {
		t.Skip("test database required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	schema := fmt.Sprintf("native_stitch_%d", time.Now().UnixNano())
	if _, err = c.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	defer c.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
	if _, err = c.Exec(ctx, "SET search_path TO "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, `CREATE TABLE accounts(id bigint primary key); CREATE TABLE connections(id bigint primary key); CREATE TABLE recording_clips(id bigint primary key); INSERT INTO accounts VALUES(47); INSERT INTO connections VALUES(8); INSERT INTO recording_clips VALUES(11)`); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "infra", "sql", "migrations", "0133_recording_native_stitch_certification.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, string(raw)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}
	manifest := `[{"ordinal":1,"clip_id":11,"recording_id":7,"recording_job_id":9,"relative_path":"safe/clip.mp4","size_bytes":4,"sha256":"` + strings.Repeat("a", 64) + `","clip_start_at":"2026-08-10T08:00:00Z","clip_end_at":"2026-08-10T08:01:00Z","capture_generation":"sha256:g","capture_sequence":1,"capture_attempt_id":"","timestamp_contract_version":"","timestamp_contract_status":"","timestamp_contract_reason":"","timestamp_contract_sha256":""}]`
	var task int64
	err = c.QueryRow(ctx, `INSERT INTO recording_native_stitch_tasks(account_id,recording_id,recording_job_id,window_start_at,window_end_at,health_calculated_at,health_metric_version,health_facts,job_schedule_facts,clip_manifest,clip_manifest_sha256,clip_count,source_bytes,policy_version) VALUES(47,7,9,'2026-08-10 08:00Z','2026-08-10 20:00Z',now(),2,'{}','{}',$1,repeat('b',64),1,4,'native-window-v1') RETURNING id`, manifest).Scan(&task)
	if err != nil {
		t.Fatal(err)
	}
	unsupported := manifest
	var historicalTask int64
	if err = c.QueryRow(ctx, `INSERT INTO recording_native_stitch_tasks(account_id,recording_id,recording_job_id,window_start_at,window_end_at,health_calculated_at,health_metric_version,health_facts,job_schedule_facts,clip_manifest,clip_manifest_sha256,clip_count,source_bytes,policy_version) VALUES(47,7,19,'2026-08-10 08:00Z','2026-08-10 20:00Z',now(),2,'{}','{}',$1,repeat('d',64),1,4,'native-window-v1') RETURNING id`, unsupported).Scan(&historicalTask); err != nil {
		t.Fatalf("historical byte/run task was incorrectly rejected: %v", err)
	}
	if _, err = c.Exec(ctx, `INSERT INTO recording_native_stitch_task_clips(task_id,ordinal,clip_id,relative_path,size_bytes,sha256) VALUES($1,1,11,'safe/clip.mp4',4,repeat('a',64))`, historicalTask); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, `INSERT INTO recording_native_stitch_task_clips(task_id,ordinal,clip_id,relative_path,size_bytes,sha256) VALUES($1,1,11,'safe/other.mp4',4,repeat('a',64))`, task); err == nil {
		t.Fatal("task clip differing from frozen manifest was accepted")
	}
	if _, err = c.Exec(ctx, `INSERT INTO recording_native_stitch_tasks(account_id,recording_id,recording_job_id,window_start_at,window_end_at,health_calculated_at,health_metric_version,health_facts,job_schedule_facts,clip_manifest,clip_manifest_sha256,clip_count,source_bytes,policy_version,state) VALUES(47,8,10,now(),now()+interval '1 hour',now(),2,'{}','{}','[]',repeat('c',64),1,1,'native-window-v1','passed')`); err == nil {
		t.Fatal("task bypassed pending lifecycle")
	}
	token := uuid.New()
	if _, err = c.Exec(ctx, `UPDATE recording_native_stitch_tasks SET state='leased',claim_token=$2,claimed_connection_id=8,lease_expires_at=now()+interval '45 minutes',attempt_count=attempt_count+1 WHERE id=$1`, task, token); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, `UPDATE recording_native_stitch_tasks SET claim_token=$2 WHERE id=$1`, task, uuid.New()); err == nil {
		t.Fatal("leased token swap succeeded")
	}
	var certID int64
	if err = c.QueryRow(ctx, `INSERT INTO recording_native_stitch_certifications(task_id,claim_token,connection_id,status,nas_byte_decode_status,native_run_concat_status,within_run_frame_adjacency_status,within_run_audio_sample_continuity_status,window_continuity_status,run_count,seam_count,audio_seam_count,inventory_generation,inventory_digest,inventory_completed_at,report,report_sha256,policy_version,client_version,ffmpeg_version,ffprobe_version,reason_codes,started_at,completed_at) VALUES($1,$2,8,'partial','passed','passed','unknown','not_present','unknown',1,0,0,'g',repeat('d',64),now(),'{}',repeat('e',64),'native-window-v1','test','ffmpeg test','ffprobe test',ARRAY['continuous_source_pts_unavailable'],now(),now()) RETURNING id`, task, token).Scan(&certID); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, `INSERT INTO recording_native_stitch_certification_clips(certification_id,ordinal,clip_id,recording_job_id,relative_path,size_bytes,sha256,sidecar_sha256,clip_start_at,clip_end_at,capture_generation,capture_sequence,capture_attempt_id,timestamp_contract_version,timestamp_contract_status,timestamp_contract_sha256,file_identity,native_signature,native_signature_sha256,strict_decode_status,audio_present) VALUES($1,1,11,9,'safe/clip.mp4',4,repeat('a',64),repeat('b',64),'2026-08-10 08:00Z','2026-08-10 08:01Z','sha256:g',1,$2,'continuous-source-pts-v1','per_clip_probe_complete',repeat('c',64),'{}','{}',repeat('f',64),'passed',false)`, certID, uuid.New()); err == nil {
		t.Fatal("complete timestamp provenance without exact-byte recomputation was accepted")
	}
	if _, err = c.Exec(ctx, `INSERT INTO recording_native_stitch_certification_clips(certification_id,ordinal,clip_id,recording_job_id,relative_path,size_bytes,sha256,sidecar_sha256,clip_start_at,clip_end_at,capture_generation,capture_sequence,file_identity,native_signature,native_signature_sha256,strict_decode_status,audio_present) VALUES($1,1,11,9,'safe/clip.mp4',4,repeat('a',64),repeat('b',64),'2026-08-10 08:00Z','2026-08-10 08:01Z','sha256:g',1,'{}','{}',repeat('f',64),'passed',false)`, certID); err != nil {
		t.Fatalf("historical exact-byte evidence was rejected: %v", err)
	}
	if _, err = c.Exec(ctx, `UPDATE recording_native_stitch_tasks SET state='passed',claim_token=NULL,claimed_connection_id=NULL,lease_expires_at=NULL WHERE id=$1`, task); err != nil {
		t.Fatal(err)
	}
	partialToken := uuid.New()
	if _, err = c.Exec(ctx, `UPDATE recording_native_stitch_tasks SET state='leased',claim_token=$2,claimed_connection_id=8,lease_expires_at=now()+interval '45 minutes',attempt_count=attempt_count+1 WHERE id=$1`, historicalTask, partialToken); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, `UPDATE recording_native_stitch_tasks SET state='partial',claim_token=NULL,claimed_connection_id=NULL,lease_expires_at=NULL WHERE id=$1`, historicalTask); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, `UPDATE recording_native_stitch_tasks SET state='pending' WHERE id=$1`, historicalTask); err == nil {
		t.Fatal("terminal historical partial was reopened for endless retry")
	}
	if _, err = c.Exec(ctx, `UPDATE recording_native_stitch_tasks SET state='pending' WHERE id=$1`, task); err == nil {
		t.Fatal("terminal task reopened")
	}
	if _, err = c.Exec(ctx, `DELETE FROM recording_native_stitch_tasks WHERE id=$1`, task); err == nil {
		t.Fatal("task deletion succeeded")
	}
}
