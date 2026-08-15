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
	unreviewedRole := fmt.Sprintf("campaign_unreviewed_%d", time.Now().UnixNano())
	unreviewedIdentifier := pgx.Identifier{unreviewedRole}.Sanitize()
	if _, err := admin.Exec(ctx, `CREATE ROLE `+unreviewedIdentifier+`; GRANT stoarama_test_admission_authority TO `+unreviewedIdentifier); err != nil {
		t.Fatalf("seed unreviewed authority member: %v", err)
	}
	defer func() {
		_, _ = admin.Exec(context.Background(), `REVOKE stoarama_test_admission_authority FROM `+unreviewedIdentifier+`; DROP ROLE IF EXISTS `+unreviewedIdentifier)
	}()
	if err := ValidateCampaignRuntimePrivileges(ctx, runtimePool, "stoarama_test_runtime", "stoarama_test_admission_authority"); err == nil {
		t.Fatal("runtime startup accepted an unreviewed authority-role member")
	}
	if _, err := admin.Exec(ctx, `REVOKE stoarama_test_admission_authority FROM `+unreviewedIdentifier+`; DROP ROLE `+unreviewedIdentifier); err != nil {
		t.Fatalf("remove unreviewed authority member fixture: %v", err)
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
	if _, err := migrator.Exec(ctx, `UPDATE recording_campaign_authority_decisions SET decision_sha256=repeat('0',64) WHERE code='deniz_fd_restore_20260814'`); err == nil {
		t.Fatal("migration authority mutated the semantic Deniz decision digest")
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
	var relayStreamID int64
	if err := migrator.QueryRow(ctx, `INSERT INTO streams(provider,external_id,name,slug,source_url,source_page_url,capture_type,source_family,execution_class,capture_family,expected_fps,local_timezone) VALUES('relay-fixture','off-window-relay','Off-window relay','off-window-relay','https://relay.example/live.m3u8','https://relay.example/page','hls','video_manifest','video_live','continuous_video',30,'UTC') RETURNING id`).Scan(&relayStreamID); err != nil {
		t.Fatal(err)
	}
	if _, err := migrator.Exec(ctx, `
		WITH groups AS (
		  INSERT INTO relay_groups(account_id,name,max_streams) VALUES($1,'domain-a',4),($1,'domain-b',3) RETURNING id,name
		)
		INSERT INTO nodes(account_id,display_name,node_type,status,last_heartbeat_at,relay_max_streams,relay_group_id)
		SELECT $1,'relay-'||name,'relay','active',recording_campaign_now(),CASE WHEN name='domain-a' THEN 4 ELSE 3 END,id FROM groups
	`, accountID); err != nil {
		t.Fatal(err)
	}
	if _, err := migrator.Exec(ctx, `INSERT INTO recordings(account_id,storage_destination_id,name,stream_url,stream_id,source_kind,mode,cron_expr,cron_timezone,clip_duration_sec,status,start_at,end_at,capture_via,next_fire_at) VALUES($1,$2,'future relay demand','https://relay.example/live.m3u8',$3,'hls_live','sampled','0 23 * * *','UTC',60,'active',recording_campaign_now()+interval '6 hours',recording_campaign_now()+interval '1 day','relay',recording_campaign_now()+interval '12 hours')`, accountID, destinationID, relayStreamID); err != nil {
		t.Fatal(err)
	}
	var relayDemand, relayDomains, relayCapacity, relayAfterLoss int
	if err := migrator.QueryRow(ctx, `SELECT active_demand,failure_domains,effective_capacity,usable_after_largest_loss FROM recording_campaign_relay_failure_capacity($1)`, accountID).Scan(&relayDemand, &relayDomains, &relayCapacity, &relayAfterLoss); err != nil || relayDemand != 1 || relayDomains != 2 || relayCapacity != 7 || relayAfterLoss != 3 {
		t.Fatalf("future relay schedule peak was not fenced: demand=%d domains=%d capacity=%d after_loss=%d err=%v", relayDemand, relayDomains, relayCapacity, relayAfterLoss, err)
	}
	var futureCloudStreamID int64
	if err := migrator.QueryRow(ctx, `INSERT INTO streams(provider,external_id,name,slug,source_url,source_page_url,capture_type,source_family,execution_class,capture_family,expected_fps,local_timezone) VALUES('cloud-fixture','future-cloud','Future cloud','future-cloud','https://cloud.example/live.m3u8','https://cloud.example/page','hls','video_manifest','video_live','continuous_video',30,'UTC') RETURNING id`).Scan(&futureCloudStreamID); err != nil {
		t.Fatal(err)
	}
	if _, err := migrator.Exec(ctx, `INSERT INTO recordings(account_id,storage_destination_id,name,stream_url,stream_id,source_kind,mode,cron_timezone,clip_duration_sec,daily_window_start,daily_window_end,active_weekdays,status,start_at,end_at,capture_via,next_fire_at) VALUES($1,$2,'future cloud demand','https://cloud.example/live.m3u8',$3,'hls_live','continuous','UTC',60,'06:00','18:00',127,'active',recording_campaign_now()+interval '10 days',recording_campaign_now()+interval '20 days','cloud',recording_campaign_now()+interval '10 days')`, accountID, destinationID, futureCloudStreamID); err != nil {
		t.Fatal(err)
	}
	var cloudPeak int
	if err := migrator.QueryRow(ctx, `SELECT recording_campaign_forecast_peak_slots($1)`, accountID).Scan(&cloudPeak); err != nil || cloudPeak < 1 {
		t.Fatalf("future cloud schedule peak was not fenced: peak=%d err=%v", cloudPeak, err)
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
	// A second, independent approval exercises the probe-vs-expiration fence in
	// both commit orders without consuming the activation-reservation fixture.
	const probeRaceStreamID int64 = 17237
	probeRaceSource := "https://source.example/probe-race.m3u8"
	probeRacePage := "https://source.example/probe-race"
	if _, err := migrator.Exec(ctx, `INSERT INTO streams(id,provider,external_id,name,slug,source_url,source_page_url,capture_type,source_family,execution_class,capture_family,expected_fps,local_timezone,tags) VALUES($1,'publisher','fd-probe-race','FD probe race','fd-probe-race',$2,$3,'hls','video_manifest','video_live','continuous_video',30,'Europe/Berlin',ARRAY['FD'])`, probeRaceStreamID, probeRaceSource, probeRacePage); err != nil {
		t.Fatal(err)
	}
	var probeRaceRevisionID, probeRaceMediaID, probeRaceFrameID, probeRaceSceneID, probeRaceRecordingID int64
	if err := migrator.QueryRow(ctx, `INSERT INTO stream_source_revisions(stream_id,actor,reason,new_source_url,new_source_page_url) VALUES($1,'test','probe-race',$2,$3) RETURNING id`, probeRaceStreamID, probeRaceSource, probeRacePage).Scan(&probeRaceRevisionID); err != nil {
		t.Fatal(err)
	}
	probeRaceFrameSHA := strings.Repeat("3", 64)
	probeRaceSceneSHA := strings.Repeat("4", 64)
	if err := migrator.QueryRow(ctx, `INSERT INTO media_objects(storage_provider,bucket,object_key,mime_type,size_bytes,sha256) VALUES('r2','campaign','probe-race-scene.jpg','image/jpeg',1,$1) RETURNING id`, probeRaceFrameSHA).Scan(&probeRaceMediaID); err != nil {
		t.Fatal(err)
	}
	if err := migrator.QueryRow(ctx, `INSERT INTO frames(stream_id,captured_at,raw_media_object_id,capture_status,source_kind) VALUES($1,$2,$3,'success','live') RETURNING id`, probeRaceStreamID, capturedAt, probeRaceMediaID).Scan(&probeRaceFrameID); err != nil {
		t.Fatal(err)
	}
	if err := migrator.QueryRow(ctx, `INSERT INTO recording_scene_frame_evidence(account_id,stream_id,frame_id,media_object_id,captured_at,frame_sha256,scene_identity_sha256,verification_method,verified_by_user_id,evidence_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,'operator_visual',$8,$9) RETURNING id`, accountID, probeRaceStreamID, probeRaceFrameID, probeRaceMediaID, capturedAt, probeRaceFrameSHA, probeRaceSceneSHA, userID, strings.Repeat("0", 64)).Scan(&probeRaceSceneID); err != nil {
		t.Fatal(err)
	}
	if err := migrator.QueryRow(ctx, `INSERT INTO recordings(account_id,storage_destination_id,name,stream_url,stream_id,source_kind,cron_expr,cron_timezone,clip_duration_sec,status,start_at,end_at,capture_via,target_fps,mode,daily_window_start,daily_window_end,active_weekdays,delivery,naming_profile,folder_name,naming_metadata_jsonb,storage_retention_tier) VALUES($1,$2,'FD probe race [17237]',$3,$4,'hls_live','','Europe/Berlin',60,'completed',$5,$6,'cloud',NULL,'continuous','06:00','18:00',127,'nas_pull','stoarama_v1','recordings','{}','monthly') RETURNING id`, accountID, destinationID, probeRaceSource, probeRaceStreamID, startAt, endAt).Scan(&probeRaceRecordingID); err != nil {
		t.Fatal(err)
	}
	var probeRaceSourceUpdated time.Time
	if err := migrator.QueryRow(ctx, `SELECT updated_at FROM streams WHERE id=$1`, probeRaceStreamID).Scan(&probeRaceSourceUpdated); err != nil {
		t.Fatal(err)
	}
	probeRaceEntries := []map[string]any{{
		"stream_id": probeRaceStreamID, "recording_id": probeRaceRecordingID, "source_revision_id": probeRaceRevisionID,
		"source_url_sha256": hashText(probeRaceSource), "source_page_url_sha256": hashText(probeRacePage),
		"source_updated_at_unix_micros": probeRaceSourceUpdated.UTC().UnixMicro(), "provider": "publisher", "external_id": "fd-probe-race",
		"normalized_label": "fdproberace", "scene_frame_evidence_id": probeRaceSceneID, "scene_identity_sha256": probeRaceSceneSHA,
	}}
	probeRaceSchedule := map[string]any{}
	for key, value := range schedule {
		probeRaceSchedule[key] = value
	}
	probeRaceSchedule["stream_ids"] = []int64{probeRaceStreamID}
	probeRaceSchedule["stream_timezones"] = []map[string]any{{"stream_id": probeRaceStreamID, "timezone": "Europe/Berlin"}}
	probeRaceEntriesJSON, _ := json.Marshal(probeRaceEntries)
	probeRaceScheduleJSON, _ := json.Marshal(probeRaceSchedule)
	var probeRaceApprovalID uuid.UUID
	if err := executorPool.QueryRow(ctx, `SELECT approval_id FROM recording_campaign_approve($1,$2,$3,$4,$5,$6,'deniz_fd_restore_20260814','FD',$7,$8::jsonb,$9::jsonb,$10)`, uuid.New(), accountID, userID, sessionID, credentialSHA, "deniz@example.test", endAt, probeRaceEntriesJSON, probeRaceScheduleJSON, strings.Repeat("c", 64)).Scan(&probeRaceApprovalID); err != nil {
		t.Fatalf("create probe/expiration race approval: %v", err)
	}
	var probeRaceOrderID uuid.UUID
	if err := executorPool.QueryRow(ctx, `SELECT order_id FROM recording_campaign_queue_probe($1,$2,$3,$4,$5,$6,$7)`, uuid.New(), probeRaceApprovalID, accountID, userID, sessionID, credentialSHA, probeRaceStreamID).Scan(&probeRaceOrderID); err != nil {
		t.Fatalf("queue probe/expiration race order: %v", err)
	}
	var probeNodeID, probeTokenID, probeGeneration, probeDropletID int64
	probeCredentialSHA := hashText("probe-race-token")
	const probeBuildSHA = "5555555555555555555555555555555555555555"
	if err := migrator.QueryRow(ctx, `INSERT INTO nodes(account_id,display_name,node_type,status,last_heartbeat_at,relay_max_streams) VALUES($1,'probe-race-worker','local_recorder','active',recording_campaign_now(),1) RETURNING id`, accountID).Scan(&probeNodeID); err != nil {
		t.Fatal(err)
	}
	if err := migrator.QueryRow(ctx, `INSERT INTO node_tokens(node_id,key_prefix,secret_hash) VALUES($1,'probe-race',$2) RETURNING id,recording_claim_generation`, probeNodeID, probeCredentialSHA).Scan(&probeTokenID, &probeGeneration); err != nil {
		t.Fatal(err)
	}
	if err := migrator.QueryRow(ctx, `INSERT INTO recorder_droplets(name,node_id,do_droplet_id,region,size,capacity,state,last_seen_at,build_sha) VALUES('probe-race-worker',$1,170237,'nyc1','s-2vcpu-4gb',5,'active',recording_campaign_now(),$2) RETURNING id`, probeNodeID, probeBuildSHA).Scan(&probeDropletID); err != nil {
		t.Fatal(err)
	}
	probeProjectSHA := strings.Repeat("6", 64)
	probeFirewallSHA := strings.Repeat("7", 64)
	probePoolSHA := hashText("s-2vcpu-4gb\n" + probeBuildSHA + "\n5\n" + probeProjectSHA + "\n" + probeFirewallSHA)
	probeAttemptID := uuid.New()
	probeRequestID := uuid.New()
	probeLeaseTx, err := executorPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var leasedOrderID uuid.UUID
	if err := probeLeaseTx.QueryRow(ctx, `SELECT order_id FROM recording_campaign_lease_probe($1,$2,$3,$4,$5,170237,'nyc1',$6,$7,$8,'s-2vcpu-4gb',$9,$10,$11,$12,$13,$14,$15,134217728,8388608)`, probeNodeID, probeTokenID, probeGeneration, probeCredentialSHA, probeDropletID, probeBuildSHA, probeProjectSHA, probeFirewallSHA, probePoolSHA, probeAttemptID, probeRequestID, strings.Repeat("8", 64), strings.Repeat("9", 64), "quarantine/campaign-probe/"+probeAttemptID.String()+"/media.zip", "quarantine/campaign-probe/"+probeAttemptID.String()+"/frame.jpg").Scan(&leasedOrderID); err != nil || leasedOrderID != probeRaceOrderID {
		_ = probeLeaseTx.Rollback(ctx)
		t.Fatalf("lease probe/expiration race order: got=%s want=%s err=%v", leasedOrderID, probeRaceOrderID, err)
	}
	if err := probeLeaseTx.Commit(ctx); err != nil {
		t.Fatalf("commit probe-first side of expiration race: %v", err)
	}
	var draftTrackID int64
	if err := migrator.QueryRow(ctx, `INSERT INTO recording_campaign_tracks(account_id,campaign_key,label,deadline_at,target_count,grade_floor,required_consecutive_windows,created_by_user_id,reporting_grade_floor,reporting_required_consecutive_windows) VALUES($1,$2,'reservation collision fixture',recording_campaign_now()+interval '3 days',1,'GOOD',14,$3,'ACCEPTABLE',14) RETURNING id`, accountID, "draft-collision-"+uuid.NewString(), userID).Scan(&draftTrackID); err != nil {
		t.Fatal(err)
	}
	if _, err := migrator.Exec(ctx, `INSERT INTO recording_campaign_roster_entries(track_id,recording_id,stream_id,scene_identity_sha256,role,rank,status,reason_codes,effective_at,decision_at,evidence_observed_at,evidence_sha256,updated_by_user_id) VALUES($1,$2,$3,$4,'primary',1,'probation',ARRAY['fixture'],recording_campaign_now(),recording_campaign_now(),recording_campaign_now(),$5,$6)`, draftTrackID, recordingID, streamID, sceneSHA, frameSHA, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := runtimePool.Exec(ctx, `SELECT transition_recording_campaign_track($1,'active',ARRAY['forged_runtime'],$2,recording_campaign_now())`, draftTrackID, userID); err == nil {
		t.Fatal("runtime invoked the authority-owned campaign transition")
	}
	if _, err := executorPool.Exec(ctx, `SELECT transition_recording_campaign_track($1,'active',ARRAY['forged_executor'],$2,recording_campaign_now())`, draftTrackID, userID); err == nil {
		t.Fatal("admission executor invoked the internal campaign transition")
	}
	var unwitnessedTrackID int64
	if err := migrator.QueryRow(ctx, `INSERT INTO recording_campaign_tracks(account_id,campaign_key,label,deadline_at,target_count,grade_floor,required_consecutive_windows,created_by_user_id,reporting_grade_floor,reporting_required_consecutive_windows) VALUES($1,$2,'unwitnessed transition fixture',recording_campaign_now()+interval '3 days',0,'GOOD',14,$3,'ACCEPTABLE',14) RETURNING id`, accountID, "unwitnessed-"+uuid.NewString(), userID).Scan(&unwitnessedTrackID); err != nil {
		t.Fatal(err)
	}
	unwitnessedTx, err := runtimePool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = unwitnessedTx.Exec(ctx, `SELECT set_config('stoarama.campaign_transition','1',true)`); err != nil {
		_ = unwitnessedTx.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err = unwitnessedTx.Exec(ctx, `UPDATE recording_campaign_tracks SET state='active' WHERE id=$1`, unwitnessedTrackID); err == nil || !strings.Contains(err.Error(), "typed transition") {
		_ = unwitnessedTx.Rollback(ctx)
		t.Fatalf("runtime GUC forged an unwitnessed track transition: %v", err)
	}
	_ = unwitnessedTx.Rollback(ctx)
	forgedTrackTx, err := migrator.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = forgedTrackTx.Exec(ctx, `SELECT set_config('stoarama.campaign_transition','1',true)`); err != nil {
		_ = forgedTrackTx.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err = forgedTrackTx.Exec(ctx, `UPDATE recording_campaign_tracks SET state='active' WHERE id=$1`, draftTrackID); err == nil || !strings.Contains(err.Error(), "collides") {
		_ = forgedTrackTx.Rollback(ctx)
		t.Fatalf("forged transition GUC bypassed track occupancy guard: %v", err)
	}
	_ = forgedTrackTx.Rollback(ctx)
	if _, err := migrator.Exec(ctx, `SELECT transition_recording_campaign_track($1,'active',ARRAY['fixture'],$2,recording_campaign_now())`, draftTrackID, userID); err == nil || !strings.Contains(err.Error(), "collides") {
		t.Fatalf("draft roster activation crossed pending reservation: %v", err)
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
	if _, err := migrator.Exec(ctx, `CREATE OR REPLACE FUNCTION recording_campaign_now() RETURNS timestamptz LANGUAGE sql STABLE SET search_path=pg_catalog AS $$ SELECT transaction_timestamp()+interval '2 days' $$`); err != nil {
		t.Fatalf("advance isolated campaign authority clock: %v", err)
	}
	expireSessionRaw := "campaign-expire-session"
	expireCredentialSHA := hashText(expireSessionRaw)
	var expireSessionID int64
	if err := migrator.QueryRow(ctx, `INSERT INTO account_sessions(account_id,session_hash,expires_at,last_used_at,user_id,current_org_id) VALUES($1,$2,recording_campaign_now()+interval '1 day',recording_campaign_now(),$3,$1) RETURNING id`, accountID, expireCredentialSHA, userID).Scan(&expireSessionID); err != nil {
		t.Fatal(err)
	}
	expireRequestID := uuid.New()
	var terminalSHA string
	if _, err := migrator.Exec(ctx, `INSERT INTO recording_campaign_admission_reservation_terminal_events(approval_id,account_id,request_id,result,actor_user_id,event_sha256) VALUES($1,$2,$3,'expired_unadmitted',$4,repeat('0',64))`, approvalID, accountID, uuid.New(), userID); err == nil {
		t.Fatal("direct SQL forged a reservation terminal event")
	}
	// Probe-first: the committed server attempt is visible to expiration and is
	// terminalized under the same approval fence. Expiry-first: while that
	// terminal event is uncommitted, another lease blocks; after commit it
	// observes the terminal approval and returns no target without authorizing
	// the node or creating a second attempt.
	probeExpireTx, err := executorPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var probeTerminalSHA string
	if err := probeExpireTx.QueryRow(ctx, `SELECT event_sha256 FROM recording_campaign_expire_approval($1,$2,$3,$4,$5,$6)`, uuid.New(), probeRaceApprovalID, accountID, userID, expireSessionID, expireCredentialSHA).Scan(&probeTerminalSHA); err != nil || len(probeTerminalSHA) != 64 {
		_ = probeExpireTx.Rollback(ctx)
		t.Fatalf("probe-first expiration did not terminalize exact approval: sha=%q err=%v", probeTerminalSHA, err)
	}
	leaseAfterTerminal := make(chan struct {
		count int
		err   error
	}, 1)
	go func() {
		rows, queryErr := executorPool.Query(ctx, `SELECT order_id FROM recording_campaign_lease_probe($1,$2,$3,$4,$5,170237,'nyc1',$6,$7,$8,'s-2vcpu-4gb',$9,$10,$11,$12,$13,$14,$15,134217728,8388608)`, probeNodeID, probeTokenID, probeGeneration, probeCredentialSHA, probeDropletID, probeBuildSHA, probeProjectSHA, probeFirewallSHA, probePoolSHA, uuid.New(), uuid.New(), strings.Repeat("a", 64), strings.Repeat("b", 64), "quarantine/campaign-probe/"+uuid.NewString()+"/media.zip", "quarantine/campaign-probe/"+uuid.NewString()+"/frame.jpg")
		if queryErr != nil {
			leaseAfterTerminal <- struct {
				count int
				err   error
			}{err: queryErr}
			return
		}
		defer rows.Close()
		count := 0
		for rows.Next() {
			count++
		}
		leaseAfterTerminal <- struct {
			count int
			err   error
		}{count: count, err: rows.Err()}
	}()
	select {
	case early := <-leaseAfterTerminal:
		_ = probeExpireTx.Rollback(ctx)
		t.Fatalf("probe lease crossed uncommitted approval terminal: count=%d err=%v", early.count, early.err)
	case <-time.After(50 * time.Millisecond):
	}
	if err := probeExpireTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case after := <-leaseAfterTerminal:
		if after.err != nil || after.count != 0 {
			t.Fatalf("terminal approval leased another probe: count=%d err=%v", after.count, after.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("probe lease did not resume after approval terminal commit")
	}
	if _, err := executorPool.Exec(ctx, `SELECT order_id FROM recording_campaign_queue_probe($1,$2,$3,$4,$5,$6,$7)`, uuid.New(), probeRaceApprovalID, accountID, userID, expireSessionID, expireCredentialSHA, probeRaceStreamID); err == nil {
		t.Fatal("terminal approval accepted a new probe order")
	}
	// Genuine two-connection, both-order serialization. First the ordinary
	// activation owns the global fence: it deterministically rejects the live
	// reservation while expiration waits. Then expiration owns the fence with
	// its terminal row still uncommitted: a second activation waits and succeeds
	// only after the terminal event commits.
	activationFirst, err := runtimePool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := activationFirst.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('campaign-admission-capacity-v1',0))`); err != nil {
		t.Fatal(err)
	}
	expireTx, err := executorPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	expireDone := make(chan error, 1)
	go func() {
		expireDone <- expireTx.QueryRow(ctx, `SELECT event_sha256 FROM recording_campaign_expire_approval($1,$2,$3,$4,$5,$6)`, expireRequestID, approvalID, accountID, userID, expireSessionID, expireCredentialSHA).Scan(&terminalSHA)
	}()
	select {
	case err := <-expireDone:
		_ = activationFirst.Rollback(ctx)
		_ = expireTx.Rollback(ctx)
		t.Fatalf("expiration crossed activation-first fence: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if _, err := activationFirst.Exec(ctx, `SAVEPOINT track_activation_first`); err != nil {
		t.Fatal(err)
	}
	if _, err := activationFirst.Exec(ctx, `SELECT transition_recording_campaign_track($1,'active',ARRAY['fixture'],$2,recording_campaign_now())`, draftTrackID, userID); err == nil || !strings.Contains(err.Error(), "collides") {
		_ = activationFirst.Rollback(ctx)
		_ = expireTx.Rollback(ctx)
		t.Fatalf("track-activation-first reservation guard failed: %v", err)
	}
	if _, err := activationFirst.Exec(ctx, `ROLLBACK TO SAVEPOINT track_activation_first`); err != nil {
		t.Fatal(err)
	}
	if _, err := activationFirst.Exec(ctx, `SAVEPOINT recording_activation_first`); err != nil {
		t.Fatal(err)
	}
	if _, err := activationFirst.Exec(ctx, `UPDATE recordings SET status='active' WHERE id=$1`, recordingID); err == nil || !strings.Contains(err.Error(), "typed campaign admission") {
		_ = activationFirst.Rollback(ctx)
		_ = expireTx.Rollback(ctx)
		t.Fatalf("activation-first reservation guard failed: %v", err)
	}
	if _, err := activationFirst.Exec(ctx, `ROLLBACK TO SAVEPOINT recording_activation_first`); err != nil {
		t.Fatal(err)
	}
	_ = activationFirst.Rollback(ctx)
	select {
	case err := <-expireDone:
		if err != nil || len(terminalSHA) != 64 {
			_ = expireTx.Rollback(ctx)
			t.Fatalf("typed expired-reservation terminal failed: sha=%q err=%v", terminalSHA, err)
		}
	case <-time.After(5 * time.Second):
		_ = expireTx.Rollback(ctx)
		t.Fatal("expiration did not acquire the released activation fence")
	}
	activationAfterTerminal := make(chan error, 1)
	go func() {
		_, err := runtimePool.Exec(ctx, `UPDATE recordings SET status='active' WHERE id=$1`, recordingID)
		activationAfterTerminal <- err
	}()
	trackAfterTerminal := make(chan error, 1)
	go func() {
		_, err := migrator.Exec(ctx, `SELECT transition_recording_campaign_track($1,'active',ARRAY['terminal_release'],$2,recording_campaign_now())`, draftTrackID, userID)
		trackAfterTerminal <- err
	}()
	select {
	case err := <-activationAfterTerminal:
		_ = expireTx.Rollback(ctx)
		t.Fatalf("activation crossed uncommitted reservation terminal: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	select {
	case err := <-trackAfterTerminal:
		_ = expireTx.Rollback(ctx)
		t.Fatalf("track activation crossed uncommitted reservation terminal: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if err := expireTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-activationAfterTerminal:
		if err != nil {
			t.Fatalf("typed terminal did not release expired reservation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("activation did not resume after reservation terminal commit")
	}
	select {
	case err := <-trackAfterTerminal:
		if err != nil {
			t.Fatalf("terminal reservation did not release draft roster activation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("track activation did not resume after reservation terminal commit")
	}
	if _, err := migrator.Exec(ctx, `UPDATE account_sessions SET revoked_at=recording_campaign_now() WHERE id=$1`, expireSessionID); err != nil {
		t.Fatal(err)
	}
	var terminalReplaySHA string
	if err := executorPool.QueryRow(ctx, `SELECT event_sha256 FROM recording_campaign_expire_approval($1,$2,$3,$4,$5,$6)`, expireRequestID, approvalID, accountID, userID, expireSessionID, expireCredentialSHA).Scan(&terminalReplaySHA); err != nil || terminalReplaySHA != terminalSHA {
		t.Fatalf("typed expiration replay changed: first=%q replay=%q err=%v", terminalSHA, terminalReplaySHA, err)
	}
	if _, err := executorPool.Exec(ctx, `SELECT event_sha256 FROM recording_campaign_expire_approval($1,$2,$3,$4,$5,$6)`, uuid.New(), approvalID, accountID, userID, expireSessionID, expireCredentialSHA); err == nil {
		t.Fatal("expired approval accepted a second terminal event")
	}
	if _, err := migrator.Exec(ctx, `UPDATE streams SET source_url='https://source.example/changed.m3u8' WHERE id=$1; UPDATE streams SET source_url='https://source.example/live.m3u8' WHERE id=$1`, streamID); err != nil {
		t.Fatal(err)
	}
	var fenceEvents int
	if err := migrator.QueryRow(ctx, `SELECT count(*) FROM recording_campaign_admission_source_fence_events WHERE stream_id=$1`, streamID).Scan(&fenceEvents); err != nil || fenceEvents != 0 {
		t.Fatalf("terminal reservation still fenced source lineage: count=%d err=%v", fenceEvents, err)
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
		"recording_campaign_admission_reservation_terminal_events",
		"recording_campaign_reservation_terminal_validate",
		"recording_campaign_reservation_terminal_commit_seal",
		"recording_worker_targeted_probe_occupancy",
		"recording_campaign_worker_lifecycle_statement_fence",
		"recording_campaign_node_probe_guard",
		"recording_campaign_claim_head_probe_guard",
		"recording_campaign_node_token_probe_guard",
		"UPDATE OF state,node_id,do_droplet_id,region,size,build_sha,capacity",
		"UPDATE OF node_id,revoked_at,recording_claim_purpose,recording_claim_generation ON node_tokens",
		"campaign account additions require typed admission capacity and NAS recomputation",
		"The typed admission function recomputes and seals a new capacity/NAS",
		"active recording identity collides with active or protected campaign occupancy",
		"campaign track activation collides with active/protected/reserved occupancy",
		"recording_campaign_assert_track_activation_occupancy",
		"recording_campaign_track_state_fence",
		"recording_campaign_track_activation_occupancy",
		"use typed transition_recording_campaign_track authority",
		"REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON TABLE %I.recording_campaign_track_events",
		"REVOKE USAGE,SELECT,UPDATE ON SEQUENCE %I.recording_campaign_track_events_id_seq",
		"to_jsonb(e)::text",
		"submission_request_sha256",
		"authority_member_count<>1",
		"reservation.observation_id",
		"qualification_required_consecutive_windows",
		"reporting_required_consecutive_windows",
		"Admission-only; qualification requires 14 consecutive GOOD/GREAT",
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
	for source, minimum := range map[string]int{
		"../api/server.go":                         2, // stream patch + common helper
		"../api/server_recordings.go":              3, // create + schedule + status
		"../api/server_recording_source_repair.go": 1,
	} {
		rawSource, readErr := os.ReadFile(source)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if got := strings.Count(string(rawSource), "lockCampaignAdmissionFence("); got < minimum {
			t.Fatalf("supported recording writer %s has %d campaign pre-fences, want at least %d", source, got, minimum)
		}
	}
	recorderControl, readErr := os.ReadFile(filepath.Join("..", "..", "cmd", "stoaramactl", "recorder_control.go"))
	if readErr != nil {
		t.Fatal(readErr)
	}
	for _, required := range []string{"recording-worker-claim-v1", "recording-surrender-cloud-capacity-v1", "recording_worker_targeted_probe_occupancy"} {
		if !strings.Contains(string(recorderControl), required) {
			t.Fatalf("supported recorder drain omitted probe lifecycle fence %q", required)
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
