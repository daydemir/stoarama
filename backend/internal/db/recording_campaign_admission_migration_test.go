package db

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRecordingCampaignAdmissionMigrationFencesAndSealsActivation(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run campaign admission migration regression")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("campaign_admission_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(context.Background(), `DROP SCHEMA `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)

	baseConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if baseConfig.ConnConfig.RuntimeParams == nil {
		baseConfig.ConnConfig.RuntimeParams = map[string]string{}
	}
	baseConfig.ConnConfig.RuntimeParams["search_path"] = schema
	migrator, err := pgxpool.NewWithConfig(ctx, baseConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer migrator.Close()
	if err := migrateUpForDBTest(ctx, migrator, filepath.Join("..", "..", "..", "infra", "sql", "migrations")); err != nil {
		t.Fatalf("migrate 0139 -> 0140 with split roles: %v", err)
	}

	connectRole := func(role string) *pgxpool.Pool {
		t.Helper()
		cfg := baseConfig.Copy()
		cfg.ConnConfig.User = role
		pool, connectErr := pgxpool.NewWithConfig(ctx, cfg)
		if connectErr != nil {
			t.Fatalf("connect %s: %v", role, connectErr)
		}
		if pingErr := pool.Ping(ctx); pingErr != nil {
			pool.Close()
			t.Fatalf("ping %s: %v", role, pingErr)
		}
		return pool
	}
	runtimePool := connectRole("stoarama_test_runtime")
	defer runtimePool.Close()
	executorPool := connectRole("stoarama_test_admission_executor")
	defer executorPool.Close()
	if err := ValidateCampaignRuntimePrivileges(ctx, runtimePool, "stoarama_test_runtime", "stoarama_test_admission_authority"); err != nil {
		t.Fatalf("runtime privilege manifest: %v", err)
	}
	if err := ValidateCampaignExecutorPrivileges(ctx, executorPool, "stoarama_test_admission_executor", "stoarama_test_admission_authority"); err != nil {
		t.Fatalf("executor privilege manifest: %v", err)
	}
	if _, err := executorPool.Exec(ctx, `SELECT count(*) FROM recording_campaign_admission_approvals`); err == nil {
		t.Fatal("executor read an authority table directly")
	}
	if _, err := runtimePool.Exec(ctx, `INSERT INTO recording_campaign_admission_tx_authorizations(transaction_id,action,account_id) VALUES(txid_current(),'approve',1)`); err == nil {
		t.Fatal("runtime forged an admission authorization directly")
	}
	if _, err := executorPool.Exec(ctx, `SELECT recording_campaign_authorize_account('approve',NULL,1,1,1,$1)`, strings.Repeat("0", 64)); err == nil {
		t.Fatal("executor invoked a low-level authority function")
	}
	if _, err := runtimePool.Exec(ctx, `SELECT recording_campaign_now()`); err == nil {
		t.Fatal("runtime invoked the authority clock directly")
	}
	if _, err := executorPool.Exec(ctx, `SELECT recording_campaign_now()`); err == nil {
		t.Fatal("executor invoked the authority clock directly")
	}

	var accountID, userID, sessionID, destinationID, streamID, revisionID, mediaID, frameID, sceneID, recordingID int64
	const rawSession = "campaign-admission-session"
	credentialSHA := fmt.Sprintf("%x", sha256.Sum256([]byte(rawSession)))
	if err := migrator.QueryRow(ctx, `INSERT INTO accounts(email,name,status,role,is_personal) VALUES('deniz@example.test','Campaign','active','admin',true) RETURNING id`).Scan(&accountID); err != nil {
		t.Fatal(err)
	}
	if err := migrator.QueryRow(ctx, `INSERT INTO users(email,name,is_operator) VALUES('deniz@example.test','Deniz',true) RETURNING id`).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if _, err := migrator.Exec(ctx, `INSERT INTO memberships(user_id,org_id,role,accepted_at) VALUES($1,$2,'owner',transaction_timestamp())`, userID, accountID); err != nil {
		t.Fatal(err)
	}
	if err := migrator.QueryRow(ctx, `INSERT INTO account_sessions(account_id,session_hash,expires_at,last_used_at,user_id,current_org_id) VALUES($1,$2,transaction_timestamp()+interval '1 day',transaction_timestamp(),$3,$1) RETURNING id`, accountID, credentialSHA, userID).Scan(&sessionID); err != nil {
		t.Fatal(err)
	}
	if err := migrator.QueryRow(ctx, `INSERT INTO storage_destinations(account_id,name,provider,endpoint,region,bucket,access_key_id,secret_access_key_enc,status,managed) VALUES($1,'campaign','s3_compatible','https://storage.example.test','auto','campaign','key',decode('00','hex'),'verified',true) RETURNING id`, accountID).Scan(&destinationID); err != nil {
		t.Fatal(err)
	}
	if err := migrator.QueryRow(ctx, `INSERT INTO streams(provider,external_id,name,slug,source_url,source_page_url,capture_type,source_family,execution_class,capture_family,expected_fps,local_timezone,tags) VALUES('publisher','fd-17235','FD scene','fd-scene-17235','https://source.example/live.m3u8','https://source.example/page','hls','video_manifest','video_live','continuous_video',30,'Europe/Berlin',ARRAY['FD']) RETURNING id`).Scan(&streamID); err != nil {
		t.Fatal(err)
	}
	if streamID != 17235 {
		if _, err := migrator.Exec(ctx, `UPDATE streams SET id=17235 WHERE id=$1`, streamID); err != nil {
			t.Fatalf("bind immutable FD decision member: %v", err)
		}
		streamID = 17235
	}
	if err := migrator.QueryRow(ctx, `INSERT INTO stream_source_revisions(stream_id,actor,reason,new_source_url,new_source_page_url) VALUES($1,'test','bind','https://source.example/live.m3u8','https://source.example/page') RETURNING id`, streamID).Scan(&revisionID); err != nil {
		t.Fatal(err)
	}
	frameSHA := strings.Repeat("1", 64)
	sceneSHA := strings.Repeat("2", 64)
	if err := migrator.QueryRow(ctx, `INSERT INTO media_objects(storage_provider,bucket,object_key,mime_type,size_bytes,sha256) VALUES('r2','campaign','scene.jpg','image/jpeg',1,$1) RETURNING id`, frameSHA).Scan(&mediaID); err != nil {
		t.Fatal(err)
	}
	capturedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	if err := migrator.QueryRow(ctx, `INSERT INTO frames(stream_id,captured_at,raw_media_object_id,capture_status,source_kind) VALUES($1,$2,$3,'success','live') RETURNING id`, streamID, capturedAt, mediaID).Scan(&frameID); err != nil {
		t.Fatal(err)
	}
	if err := migrator.QueryRow(ctx, `INSERT INTO recording_scene_frame_evidence(account_id,stream_id,frame_id,media_object_id,captured_at,frame_sha256,scene_identity_sha256,verification_method,verified_by_user_id,evidence_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,'operator_visual',$8,$9) RETURNING id`, accountID, streamID, frameID, mediaID, capturedAt, frameSHA, sceneSHA, userID, strings.Repeat("0", 64)).Scan(&sceneID); err != nil {
		t.Fatal(err)
	}
	startAt := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	endAt := startAt.Add(24 * time.Hour)
	if err := migrator.QueryRow(ctx, `INSERT INTO recordings(account_id,storage_destination_id,name,stream_url,stream_id,source_kind,cron_expr,cron_timezone,clip_duration_sec,status,start_at,end_at,capture_via,target_fps,mode,daily_window_start,daily_window_end,active_weekdays,delivery,naming_profile,folder_name,naming_metadata_jsonb,storage_retention_tier) VALUES($1,$2,'FD scene [17235]','https://source.example/live.m3u8',$3,'hls_live','','Europe/Berlin',60,'completed',$4,$5,'cloud',NULL,'continuous','06:00','18:00',127,'nas_pull','stoarama_v1','recordings','{}','monthly') RETURNING id`, accountID, destinationID, streamID, startAt, endAt).Scan(&recordingID); err != nil {
		t.Fatal(err)
	}
	var sourceUpdated time.Time
	if err := migrator.QueryRow(ctx, `SELECT updated_at FROM streams WHERE id=$1`, streamID).Scan(&sourceUpdated); err != nil {
		t.Fatal(err)
	}
	hashText := func(v string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(v))) }
	entries := []map[string]any{{
		"stream_id": streamID, "recording_id": recordingID, "source_revision_id": revisionID,
		"source_url_sha256": hashText("https://source.example/live.m3u8"), "source_page_url_sha256": hashText("https://source.example/page"),
		"source_updated_at_unix_micros": sourceUpdated.UTC().UnixMicro(), "provider": "publisher", "external_id": "fd-17235",
		"normalized_label": "fdscene", "scene_frame_evidence_id": sceneID, "scene_identity_sha256": sceneSHA,
	}}
	schedule := map[string]any{
		"target_account_id": accountID, "stream_ids": []int64{streamID}, "stream_timezones": []map[string]any{{"stream_id": streamID, "timezone": "Europe/Berlin"}},
		"naming_profile": "stoarama_v1", "mode": "continuous", "cron_expr": "", "clip_duration_sec": 60,
		"daily_window_start": "06:00", "daily_window_end": "18:00", "active_weekdays": []int{1, 2, 3, 4, 5, 6, 7},
		"target_fps": nil, "start_at": startAt, "end_at": endAt, "storage_destination_id": destinationID,
		"delivery_storage_destination_id": 0, "delivery": "nas_pull", "dry_run": false, "required_relay_slots": 0,
		"campaign_admission_approval_id": "",
	}
	entryJSON, _ := json.Marshal(entries)
	scheduleJSON, _ := json.Marshal(schedule)
	requestID := uuid.New()
	requestSHA := strings.Repeat("a", 64)
	var approvalID uuid.UUID
	var approvalSHA string
	if err := executorPool.QueryRow(ctx, `SELECT approval_id,approval_sha256 FROM recording_campaign_approve($1,$2,$3,$4,$5,$6,'deniz_fd_restore_20260814','FD',$7,$8::jsonb,$9::jsonb,$10)`, requestID, accountID, userID, sessionID, credentialSHA, "deniz@example.test", endAt, entryJSON, scheduleJSON, requestSHA).Scan(&approvalID, &approvalSHA); err != nil {
		t.Fatalf("atomic approval entry point: %v", err)
	}
	if approvalID == uuid.Nil || len(approvalSHA) != 64 {
		t.Fatalf("invalid approval result id=%s sha=%q", approvalID, approvalSHA)
	}
	if _, err := runtimePool.Exec(ctx, `UPDATE recordings SET status='active' WHERE id=$1`, recordingID); err == nil || !strings.Contains(err.Error(), "typed campaign admission") {
		t.Fatalf("generic completed activation bypassed reservation: %v", err)
	}
	var status string
	if err := migrator.QueryRow(ctx, `SELECT status FROM recordings WHERE id=$1`, recordingID).Scan(&status); err != nil || status != "completed" {
		t.Fatalf("rejected activation changed recording status=%q err=%v", status, err)
	}
	if _, err := migrator.Exec(ctx, `UPDATE account_sessions SET revoked_at=transaction_timestamp() WHERE id=$1`, sessionID); err != nil {
		t.Fatal(err)
	}
	var replayID uuid.UUID
	if err := executorPool.QueryRow(ctx, `SELECT approval_id FROM recording_campaign_approve($1,$2,$3,$4,$5,$6,'deniz_fd_restore_20260814','FD',$7,$8::jsonb,$9::jsonb,$10)`, requestID, accountID, userID, sessionID, credentialSHA, "deniz@example.test", endAt, entryJSON, scheduleJSON, requestSHA).Scan(&replayID); err != nil || replayID != approvalID {
		t.Fatalf("terminal approval replay depended on revoked session: id=%s err=%v", replayID, err)
	}
	if _, err := executorPool.Exec(ctx, `SELECT approval_id FROM recording_campaign_approve($1,$2,$3,$4,$5,$6,'deniz_fd_restore_20260814','FD',$7,$8::jsonb,$9::jsonb,$10)`, requestID, accountID, userID, sessionID, credentialSHA, "deniz@example.test", endAt, entryJSON, scheduleJSON, strings.Repeat("b", 64)); err == nil || !strings.Contains(err.Error(), "idempotency conflict") {
		t.Fatalf("mismatched approval replay was accepted: %v", err)
	}
	if _, err := migrator.Exec(ctx, `UPDATE streams SET source_url='https://source.example/changed.m3u8' WHERE id=$1; UPDATE streams SET source_url='https://source.example/live.m3u8' WHERE id=$1`, streamID); err != nil {
		t.Fatal(err)
	}
	var fenceEvents int
	if err := migrator.QueryRow(ctx, `SELECT count(*) FROM recording_campaign_admission_source_fence_events WHERE stream_id=$1`, streamID).Scan(&fenceEvents); err != nil || fenceEvents != 2 {
		t.Fatalf("A-B-A source mutation did not append two permanent fence events: count=%d err=%v", fenceEvents, err)
	}
}

func TestRecordingCampaignAdmissionMigrationClosesCrossBoundaryBypasses(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "infra", "sql", "migrations", "0140_targeted_campaign_admission.sql"))
	if err != nil {
		t.Fatal(err)
	}
	sql := string(raw)
	for _, required := range []string{
		"hashtextextended('campaign-admission-capacity-v1',0)",
		"GRANT INSERT ON TABLE %I.recording_campaign_tracks,%I.recording_campaign_roster_entries,%I.recording_campaign_roster_events,%I.recording_campaign_track_events",
		"GRANT UPDATE ON TABLE %I.recording_campaign_tracks",
		"recording_campaign_roster_events_id_seq",
		"recording_campaign_track_events_id_seq",
		"JOIN %I.recording_campaign_admission_approvals approval ON approval.id=$5 AND approval.account_id=$3",
		"campaign capacity witness differs from authority rows",
		"campaign NAS witness differs from authority rows",
		"recording_targeted_probe_scene_presentations",
		"presentation.presented_at<recording_campaign_now()-interval '30 minutes'",
		"campaign_key='delivery30-2026q3'",
		"recording_targeted_probe_attempt_terminal_events",
		"expired_without_evidence",
		"head.state='enabled'",
		"pool_identity_sha256",
		"recording_campaign_read_probe_attempt",
		"recording_campaign_read_probe_scene",
		"recording_campaign_read_baseline_scene",
		"campaign activation requires a fresh typed capacity observation",
		"campaign one-worker-loss capacity head is permanently enforced",
		"recording_campaign_relay_failure_capacity",
		"campaign largest-relay-domain-loss capacity head is permanently enforced",
		"relay_usable_after_largest_loss",
		"admitted recording next-fire must advance by one exact scheduled window",
		"approval.schedule_spec->>'delivery'<>'nas_pull'",
		"recording_campaign_replay(p_approval_id,p_account_id,p_credential_sha256)",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("0140 omitted reviewed cross-boundary closure %q", required)
		}
	}
	if strings.Contains(sql, "campaign_admission_capacity_v1") {
		t.Fatal("0140 retained a second advisory-lock namespace")
	}
	if strings.Contains(sql, "AND n.account_id=requested_account") || strings.Contains(sql, "AND n.account_id=$3") {
		t.Fatal("0140 still binds infrastructure cloud nodes to the customer account")
	}
	if strings.Contains(sql, "admitted.id IS NULL") {
		t.Fatal("0140 lets completed admissions disappear from reciprocal occupancy fencing")
	}
	for _, source := range []string{
		"../api/server_recording_campaign_admission.go",
		"../api/server_recording_qualification.go",
		"../api/server_recordings_batch.go",
	} {
		rawSource, readErr := os.ReadFile(source)
		if readErr != nil {
			t.Fatal(readErr)
		}
		for lineNo, line := range strings.Split(string(rawSource), "\n") {
			if strings.Contains(line, "s.pool.") && (strings.Contains(line, "recording_campaign_") || strings.Contains(line, "recording_targeted_")) {
				t.Fatalf("ordinary runtime pool reads admission authority at %s:%d", source, lineNo+1)
			}
		}
	}
	renderRaw, err := os.ReadFile(filepath.Join("..", "..", "..", "render.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(renderRaw), "key: STOARAMA_ADMISSION_EXECUTOR_ROLE\n        value: stoarama_admission_executor") {
		t.Fatal("Render API blueprint omits the reviewed executor role identity")
	}
}
