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

	"github.com/daydemir/stoarama/backend/internal/recordability"
)

func TestRecordingCampaignAdmissionMigrationFencesAndSealsActivation(t *testing.T) {
	url := os.Getenv("STOARAMA_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run campaign admission migration regression")
	}
	ctx := context.Background()
	c, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	schema := fmt.Sprintf("campaign_admission_%d", time.Now().UnixNano())
	if _, err := c.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	defer c.Exec(ctx, `DROP SCHEMA `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
	if _, err := c.Exec(ctx, `SET search_path TO `+pgx.Identifier{schema}.Sanitize()+`,public`); err != nil {
		t.Fatal(err)
	}
	_, err = c.Exec(ctx, `
		CREATE TABLE accounts(id BIGINT PRIMARY KEY,status TEXT NOT NULL);
		CREATE TABLE users(id BIGINT PRIMARY KEY,email TEXT NOT NULL,is_operator BOOLEAN NOT NULL);
		CREATE TABLE memberships(user_id BIGINT,org_id BIGINT,accepted_at TIMESTAMPTZ,role TEXT);
		CREATE TABLE account_sessions(id BIGINT PRIMARY KEY,account_id BIGINT,user_id BIGINT,current_org_id BIGINT,revoked_at TIMESTAMPTZ,expires_at TIMESTAMPTZ);
		CREATE TABLE streams(id BIGINT PRIMARY KEY,name TEXT NOT NULL,source_url TEXT NOT NULL,source_page_url TEXT NOT NULL,provider TEXT NOT NULL,external_id TEXT NOT NULL,tags TEXT[] NOT NULL,deleted_at TIMESTAMPTZ,local_timezone TEXT NOT NULL DEFAULT '',updated_at TIMESTAMPTZ NOT NULL);
		CREATE TABLE stream_source_revisions(id BIGINT PRIMARY KEY,stream_id BIGINT NOT NULL REFERENCES streams(id));
		CREATE TABLE nodes(id BIGINT PRIMARY KEY,account_id BIGINT NOT NULL,node_type TEXT NOT NULL,status TEXT NOT NULL);
		CREATE TABLE node_tokens(id BIGINT PRIMARY KEY,node_id BIGINT NOT NULL REFERENCES nodes(id),revoked_at TIMESTAMPTZ);
		CREATE TABLE recorder_droplets(id BIGINT PRIMARY KEY,name TEXT NOT NULL,node_id BIGINT REFERENCES nodes(id),do_droplet_id BIGINT,region TEXT NOT NULL,build_sha TEXT NOT NULL,state TEXT NOT NULL,last_seen_at TIMESTAMPTZ);
		CREATE TABLE storage_destinations(id BIGINT PRIMARY KEY,account_id BIGINT NOT NULL);
		CREATE TABLE recording_scene_frame_evidence(id BIGINT PRIMARY KEY,account_id BIGINT NOT NULL,stream_id BIGINT NOT NULL,scene_identity_sha256 TEXT NOT NULL,verified_at TIMESTAMPTZ NOT NULL,UNIQUE(id,account_id,stream_id,scene_identity_sha256));
		CREATE TABLE recordings(
		 id BIGINT PRIMARY KEY,account_id BIGINT NOT NULL,stream_id BIGINT REFERENCES streams(id),status TEXT NOT NULL,paused_at TIMESTAMPTZ,end_at TIMESTAMPTZ,
		 capture_via TEXT NOT NULL,target_fps INTEGER,mode TEXT NOT NULL,cron_expr TEXT,cron_timezone TEXT NOT NULL,clip_duration_sec INTEGER NOT NULL,
		 daily_window_start TIME,daily_window_end TIME,active_weekdays INTEGER[] NOT NULL,start_at TIMESTAMPTZ NOT NULL,storage_destination_id BIGINT NOT NULL,
		 delivery_storage_destination_id BIGINT,delivery TEXT NOT NULL,naming_profile TEXT NOT NULL,name TEXT NOT NULL,stream_url TEXT NOT NULL,source_kind TEXT NOT NULL,
		 folder_name TEXT NOT NULL,naming_metadata_jsonb JSONB NOT NULL,storage_retention_tier TEXT NOT NULL
		);
		CREATE TABLE recording_jobs(id BIGINT PRIMARY KEY,recording_id BIGINT NOT NULL REFERENCES recordings(id));
		CREATE TABLE recording_window_health(recording_id BIGINT,job_id BIGINT,window_end_at TIMESTAMPTZ);
	`)
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range []string{"0129_recording_campaign_tracks.sql", "0140_targeted_campaign_admission.sql"} {
		body, readErr := os.ReadFile(filepath.Join("..", "..", "..", "infra", "sql", "migrations", migration))
		if readErr != nil {
			t.Fatal(readErr)
		}
		if _, err := c.Exec(ctx, string(body)); err != nil {
			t.Fatalf("apply %s: %v", migration, err)
		}
	}
	end := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Microsecond)
	start := end.Add(-12 * time.Hour)
	_, err = c.Exec(ctx, `
		INSERT INTO accounts VALUES(47,'active');
		INSERT INTO users VALUES(7,'deniz@example.test',true);
		INSERT INTO memberships VALUES(7,47,transaction_timestamp(),'owner');
		INSERT INTO account_sessions VALUES(51,47,7,47,NULL,transaction_timestamp()+interval '1 day');
		INSERT INTO streams VALUES(17235,'FD scene','https://source.example/live.m3u8','https://source.example/page','publisher','fd-1',ARRAY['FD'],NULL,'Europe/Berlin',transaction_timestamp());
		INSERT INTO stream_source_revisions VALUES(81,17235);
		INSERT INTO nodes VALUES(91,47,'local_recorder','active');
		INSERT INTO node_tokens VALUES(92,91,NULL);
		INSERT INTO recorder_droplets VALUES(231,'worker-231',91,592454108,'nyc1',$1,'active',transaction_timestamp());
		INSERT INTO storage_destinations VALUES(12,47);
		INSERT INTO recording_scene_frame_evidence VALUES(501,47,17235,$4,transaction_timestamp());
		INSERT INTO recordings(id,account_id,stream_id,status,capture_via,target_fps,mode,cron_timezone,clip_duration_sec,daily_window_start,daily_window_end,active_weekdays,start_at,end_at,storage_destination_id,delivery,naming_profile,name,stream_url,source_kind,folder_name,naming_metadata_jsonb,storage_retention_tier)
		VALUES(381,47,17235,'completed','cloud',NULL,'continuous','Europe/Berlin',60,'06:00','18:00',ARRAY[1,2,3,4,5,6,7],$2,$3,12,'nas_pull','stoarama_v1','FD scene [17235]','https://source.example/live.m3u8','hls_live','recordings','{}','monthly')
	`, strings.Repeat("a", 40), start, end, strings.Repeat("b", 64))
	if err != nil {
		t.Fatal(err)
	}
	schedule := map[string]any{
		"target_account_id": 47, "stream_ids": []int64{17235}, "stream_timezones": []map[string]any{{"stream_id": 17235, "timezone": "Europe/Berlin"}},
		"naming_profile": "stoarama_v1", "mode": "continuous", "cron_expr": "", "clip_duration_sec": 60, "daily_window_start": "06:00", "daily_window_end": "18:00",
		"active_weekdays": []int{1, 2, 3, 4, 5, 6, 7}, "target_fps": nil, "start_at": start, "end_at": end, "storage_destination_id": 12,
		"delivery_storage_destination_id": 0, "delivery": "nas_pull", "dry_run": false, "required_relay_slots": 0, "campaign_admission_approval_id": "",
	}
	var sourceUpdated time.Time
	if err := c.QueryRow(ctx, `SELECT updated_at FROM streams WHERE id=17235`).Scan(&sourceUpdated); err != nil {
		t.Fatal(err)
	}
	sha := func(v string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(v))) }
	entries := []map[string]any{{"stream_id": 17235, "recording_id": 381, "source_revision_id": 81, "source_url_sha256": sha("https://source.example/live.m3u8"), "source_page_url_sha256": sha("https://source.example/page"), "source_updated_at_unix_micros": sourceUpdated.UTC().UnixMicro(), "provider": "publisher", "external_id": "fd-1", "normalized_label": "fdscene", "scene_frame_evidence_id": 501, "scene_identity_sha256": strings.Repeat("b", 64)}}
	scheduleJSON, _ := json.Marshal(schedule)
	entriesJSON, _ := json.Marshal(entries)
	var approval uuid.UUID
	approvalTx, err := c.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := approvalTx.Exec(ctx, `INSERT INTO recording_campaign_admission_tx_authorizations(transaction_id,action,account_id,actor_user_id,account_session_id) VALUES(txid_current(),'approve',47,7,51)`); err != nil {
		t.Fatal(err)
	}
	err = approvalTx.QueryRow(ctx, `
		WITH p AS (SELECT 47::bigint account_id,7::bigint actor_user_id,'deniz@example.test'::text actor_email,'deniz_fd_restore_20260814'::text authority_code,'FD'::text failure_domain_tag,$1::timestamptz deadline_at,$2::jsonb entries,$3::jsonb schedule_spec),
		h AS (SELECT *,encode(sha256(convert_to(schedule_spec::text,'UTF8')),'hex') schedule_sha256 FROM p)
		INSERT INTO recording_campaign_admission_approvals(request_id,account_id,actor_user_id,actor_email_snapshot,authority_code,failure_domain_tag,deadline_at,entries,schedule_spec,schedule_sha256,approval_sha256)
		SELECT $4,account_id,actor_user_id,actor_email,authority_code,failure_domain_tag,deadline_at,entries,schedule_spec,schedule_sha256,encode(sha256(convert_to(jsonb_build_object('account_id',account_id,'actor_user_id',actor_user_id,'actor_email',lower(actor_email),'authority_code',authority_code,'failure_domain_tag',failure_domain_tag,'deadline_epoch',extract(epoch from deadline_at),'entries',entries,'schedule_sha256',schedule_sha256)::text,'UTF8')),'hex') FROM h RETURNING id`, end, entriesJSON, scheduleJSON, uuid.New()).Scan(&approval)
	if err != nil {
		t.Fatal(err)
	}
	if err := approvalTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exec(ctx, `WITH p AS (SELECT 47::bigint account_id,7::bigint actor_user_id,'deniz@example.test'::text actor_email,'deniz_fd_restore_20260814'::text authority_code,'FD'::text failure_domain_tag,$1::timestamptz deadline_at,$2::jsonb entries,$3::jsonb schedule_spec),h AS (SELECT *,encode(sha256(convert_to(schedule_spec::text,'UTF8')),'hex') schedule_sha256 FROM p) INSERT INTO recording_campaign_admission_approvals(request_id,account_id,actor_user_id,actor_email_snapshot,authority_code,failure_domain_tag,deadline_at,entries,schedule_spec,schedule_sha256,approval_sha256) SELECT $4,account_id,actor_user_id,actor_email,authority_code,failure_domain_tag,deadline_at,entries,schedule_spec,schedule_sha256,encode(sha256(convert_to(jsonb_build_object('account_id',account_id,'actor_user_id',actor_user_id,'actor_email',lower(actor_email),'authority_code',authority_code,'failure_domain_tag',failure_domain_tag,'deadline_epoch',extract(epoch from deadline_at),'entries',entries,'schedule_sha256',schedule_sha256)::text,'UTF8')),'hex') FROM h`, end, entriesJSON, scheduleJSON, uuid.New()); err == nil || !strings.Contains(err.Error(), "typed transaction") {
		t.Fatalf("direct approval without typed session authorization was accepted: %v", err)
	}
	var durableEntries string
	if err := c.QueryRow(ctx, `SELECT entries::text FROM recording_campaign_admission_approvals WHERE id=$1`, approval).Scan(&durableEntries); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(durableEntries, "source.example") || strings.Contains(durableEntries, "source_url\"") || strings.Contains(durableEntries, "source_page_url\"") {
		t.Fatalf("durable approval leaked raw source URL: %s", durableEntries)
	}
	// The account row is the common occupancy lock for approval reservations,
	// generic recording activation, and campaign roster occupancy. Exercise both
	// commit orders with real concurrent PostgreSQL sessions: approval-first must
	// make the waiting activation reject, while activation-first must make the
	// waiting approval reject without leaving a reservation behind.
	seedRaceCandidate := func(streamID, recordingID, approvalRecordingID, revisionID, sceneID int64, status, externalID string) (time.Time, []byte, []byte) {
		t.Helper()
		name := fmt.Sprintf("Race scene %d", streamID)
		if _, err := c.Exec(ctx, `INSERT INTO streams(id,name,source_url,source_page_url,provider,external_id,tags,deleted_at,local_timezone,updated_at) VALUES($1,$2,$3,$4,'publisher',$5,ARRAY['FD'],NULL,'Europe/Berlin',transaction_timestamp())`, streamID, name, fmt.Sprintf("https://source.example/%d/live.m3u8", streamID), fmt.Sprintf("https://source.example/%d/page", streamID), externalID); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Exec(ctx, `INSERT INTO stream_source_revisions(id,stream_id) VALUES($1,$2)`, revisionID, streamID); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Exec(ctx, `INSERT INTO recording_scene_frame_evidence(id,account_id,stream_id,scene_identity_sha256,verified_at) VALUES($1,47,$2,$3,transaction_timestamp())`, sceneID, streamID, sha(fmt.Sprintf("scene-%d", streamID))); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Exec(ctx, `INSERT INTO recordings(id,account_id,stream_id,status,capture_via,target_fps,mode,cron_timezone,clip_duration_sec,daily_window_start,daily_window_end,active_weekdays,start_at,end_at,storage_destination_id,delivery,naming_profile,name,stream_url,source_kind,folder_name,naming_metadata_jsonb,storage_retention_tier) VALUES($1,47,$2,$3,'cloud',NULL,'continuous','Europe/Berlin',60,'06:00','18:00',ARRAY[1,2,3,4,5,6,7],$4,$5,12,'nas_pull','stoarama_v1',$6,$7,'hls_live','recordings','{}','monthly')`, recordingID, streamID, status, start, end, fmt.Sprintf("%s [%d]", name, streamID), fmt.Sprintf("https://source.example/%d/live.m3u8", streamID)); err != nil {
			t.Fatal(err)
		}
		var updated time.Time
		if err := c.QueryRow(ctx, `SELECT updated_at FROM streams WHERE id=$1`, streamID).Scan(&updated); err != nil {
			t.Fatal(err)
		}
		raceSchedule := map[string]any{
			"target_account_id": 47, "stream_ids": []int64{streamID}, "stream_timezones": []map[string]any{{"stream_id": streamID, "timezone": "Europe/Berlin"}},
			"naming_profile": "stoarama_v1", "mode": "continuous", "cron_expr": "", "clip_duration_sec": 60, "daily_window_start": "06:00", "daily_window_end": "18:00",
			"active_weekdays": []int{1, 2, 3, 4, 5, 6, 7}, "target_fps": nil, "start_at": start, "end_at": end, "storage_destination_id": 12,
			"delivery_storage_destination_id": 0, "delivery": "nas_pull", "dry_run": false, "required_relay_slots": 0, "campaign_admission_approval_id": "",
		}
		raceEntries := []map[string]any{{"stream_id": streamID, "recording_id": approvalRecordingID, "source_revision_id": revisionID, "source_url_sha256": sha(fmt.Sprintf("https://source.example/%d/live.m3u8", streamID)), "source_page_url_sha256": sha(fmt.Sprintf("https://source.example/%d/page", streamID)), "source_updated_at_unix_micros": updated.UTC().UnixMicro(), "provider": "publisher", "external_id": externalID, "normalized_label": fmt.Sprintf("racescene%d", streamID), "scene_frame_evidence_id": sceneID, "scene_identity_sha256": sha(fmt.Sprintf("scene-%d", streamID))}}
		rawSchedule, _ := json.Marshal(raceSchedule)
		rawEntries, _ := json.Marshal(raceEntries)
		return updated, rawEntries, rawSchedule
	}
	insertApproval := func(tx pgx.Tx, deadline time.Time, rawEntries, rawSchedule []byte) error {
		if _, err := tx.Exec(ctx, `INSERT INTO recording_campaign_admission_tx_authorizations(transaction_id,action,account_id,actor_user_id,account_session_id) VALUES(txid_current(),'approve',47,7,51)`); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `WITH p AS (SELECT 47::bigint account_id,7::bigint actor_user_id,'deniz@example.test'::text actor_email,'deniz_fd_restore_20260814'::text authority_code,'FD'::text failure_domain_tag,$1::timestamptz deadline_at,$2::jsonb entries,$3::jsonb schedule_spec),h AS (SELECT *,encode(sha256(convert_to(schedule_spec::text,'UTF8')),'hex') schedule_sha256 FROM p) INSERT INTO recording_campaign_admission_approvals(request_id,account_id,actor_user_id,actor_email_snapshot,authority_code,failure_domain_tag,deadline_at,entries,schedule_spec,schedule_sha256,approval_sha256) SELECT $4,account_id,actor_user_id,actor_email,authority_code,failure_domain_tag,deadline_at,entries,schedule_spec,schedule_sha256,encode(sha256(convert_to(jsonb_build_object('account_id',account_id,'actor_user_id',actor_user_id,'actor_email',lower(actor_email),'authority_code',authority_code,'failure_domain_tag',failure_domain_tag,'deadline_epoch',extract(epoch from deadline_at),'entries',entries,'schedule_sha256',schedule_sha256)::text,'UTF8')),'hex') FROM h`, deadline, rawEntries, rawSchedule, uuid.New())
		return err
	}
	concurrent, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer concurrent.Close(context.Background())
	if _, err := concurrent.Exec(ctx, `SET search_path TO `+pgx.Identifier{schema}.Sanitize()+`,public`); err != nil {
		t.Fatal(err)
	}
	observer, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close(context.Background())
	waitForBlock := func(blockedPID, blockerPID uint32) {
		t.Helper()
		waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			var blocked bool
			if err := observer.QueryRow(waitCtx, `SELECT $2::int=ANY(pg_blocking_pids($1::int))`, blockedPID, blockerPID).Scan(&blocked); err != nil {
				t.Fatalf("observe campaign occupancy lock: %v", err)
			}
			if blocked {
				return
			}
			select {
			case <-waitCtx.Done():
				t.Fatal("campaign occupancy operation did not wait on the common account lock")
			case <-ticker.C:
			}
		}
	}
	_, firstRaceEntries, firstRaceSchedule := seedRaceCandidate(18001, 701, 701, 901, 801, "completed", "race-approval-first")
	approvalFirst, err := c.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := insertApproval(approvalFirst, end, firstRaceEntries, firstRaceSchedule); err != nil {
		t.Fatal(err)
	}
	activationResult := make(chan error, 1)
	go func() {
		_, activateErr := concurrent.Exec(ctx, `UPDATE recordings SET status='active' WHERE id=701`)
		activationResult <- activateErr
	}()
	waitForBlock(concurrent.PgConn().PID(), c.PgConn().PID())
	if err := approvalFirst.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-activationResult; err == nil || !strings.Contains(err.Error(), "typed campaign admission") {
		t.Fatalf("approval-first race did not reject generic activation: %v", err)
	}

	_, activeFirstEntries, activeFirstSchedule := seedRaceCandidate(18002, 702, 0, 902, 802, "completed", "race-active-first")
	activeFirst, err := c.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := activeFirst.Exec(ctx, `UPDATE recordings SET status='active' WHERE id=702`); err != nil {
		t.Fatal(err)
	}
	approvalResult := make(chan error, 1)
	go func() {
		tx, beginErr := concurrent.Begin(ctx)
		if beginErr != nil {
			approvalResult <- beginErr
			return
		}
		defer tx.Rollback(context.Background())
		insertErr := insertApproval(tx, end, activeFirstEntries, activeFirstSchedule)
		if insertErr == nil {
			insertErr = tx.Commit(ctx)
		}
		approvalResult <- insertErr
	}()
	waitForBlock(concurrent.PgConn().PID(), c.PgConn().PID())
	if err := activeFirst.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-approvalResult; err == nil || !strings.Contains(err.Error(), "collides with active/protected occupancy") {
		t.Fatalf("activation-first race did not reject conflicting approval: %v", err)
	}
	var strayReservations int
	if err := c.QueryRow(ctx, `SELECT count(*) FROM recording_campaign_admission_reservations WHERE stream_id=18002`).Scan(&strayReservations); err != nil {
		t.Fatal(err)
	}
	if strayReservations != 0 {
		t.Fatalf("activation-first race left %d conflicting reservations", strayReservations)
	}
	_, expiringEntries, expiringSchedule := seedRaceCandidate(18003, 703, 703, 903, 803, "completed", "race-expiry")
	expiryStart := time.Now().UTC().Add(time.Second)
	expiryEnd := expiryStart.Add(2 * time.Second)
	var expiringSpec map[string]any
	if err := json.Unmarshal(expiringSchedule, &expiringSpec); err != nil {
		t.Fatal(err)
	}
	expiringSpec["start_at"], expiringSpec["end_at"] = expiryStart, expiryEnd
	expiringSchedule, _ = json.Marshal(expiringSpec)
	expiring, err := c.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := insertApproval(expiring, expiryEnd, expiringEntries, expiringSchedule); err != nil {
		t.Fatal(err)
	}
	if err := expiring.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if wait := time.Until(expiryEnd.Add(100 * time.Millisecond)); wait > 0 {
		time.Sleep(wait)
	}
	successorStart := time.Now().UTC().Add(time.Minute)
	successorEnd := successorStart.Add(time.Hour)
	expiringSpec["start_at"], expiringSpec["end_at"] = successorStart, successorEnd
	successorSchedule, _ := json.Marshal(expiringSpec)
	successor, err := c.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := insertApproval(successor, successorEnd, expiringEntries, successorSchedule); err != nil {
		t.Fatalf("expired reservation blocked exact successor approval: %v", err)
	}
	if err := successor.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var successorReservations int
	if err := c.QueryRow(ctx, `SELECT count(*) FROM recording_campaign_admission_reservations WHERE stream_id=18003`).Scan(&successorReservations); err != nil {
		t.Fatal(err)
	}
	if successorReservations != 2 {
		t.Fatalf("expired reservation successor rows=%d want append-only history of 2", successorReservations)
	}
	if _, err := c.Exec(ctx, `UPDATE recordings SET status='active' WHERE id=381`); err == nil || !strings.Contains(err.Error(), "typed campaign admission") {
		t.Fatalf("generic completed activation was not fenced: %v", err)
	}
	if _, err := c.Exec(ctx, `CREATE TEMP TABLE recording_campaign_admission_reservations(approval_id UUID,account_id BIGINT,recording_id BIGINT,stream_id BIGINT,source_url_sha256 TEXT,reserved_at TIMESTAMPTZ)`); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exec(ctx, `UPDATE recordings SET status='active' WHERE id=381`); err == nil || !strings.Contains(err.Error(), "typed campaign admission") {
		t.Fatalf("pg_temp shadow bypassed exact-schema activation fence: %v", err)
	}
	if _, err := c.Exec(ctx, `DROP TABLE pg_temp.recording_campaign_admission_reservations`); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exec(ctx, `INSERT INTO recording_targeted_probe_attempts(request_id,approval_id,account_id,stream_id,attempt_no,node_id,recorder_droplet_id,do_droplet_id,region,probe_build_sha,source_revision_id,source_url_sha256,source_page_url_sha256,source_updated_at,challenge,expires_at) SELECT $2,$1,47,17235,1,91,231,592454108,'nyc1',$3,source_revision_id,source_url_sha256,source_page_url_sha256,source_updated_at,$4,transaction_timestamp()+interval '15 minutes' FROM recording_campaign_admission_reservations WHERE approval_id=$1 AND stream_id=17235`, approval, uuid.New(), strings.Repeat("a", 40), sha("forged")); err == nil || !strings.Contains(err.Error(), "typed node") {
		t.Fatalf("direct attempt without typed node authorization was accepted: %v", err)
	}
	abaTx, err := c.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := abaTx.Exec(ctx, `UPDATE streams SET source_url='https://source.example/other.m3u8',updated_at=transaction_timestamp() WHERE id=17235`); err != nil {
		t.Fatal(err)
	}
	if _, err := abaTx.Exec(ctx, `UPDATE streams SET source_url='https://source.example/live.m3u8',updated_at=$1 WHERE id=17235`, sourceUpdated); err != nil {
		t.Fatal(err)
	}
	var fenceEvents int
	if err := abaTx.QueryRow(ctx, `SELECT count(*) FROM recording_campaign_admission_source_fence_events WHERE stream_id=17235`).Scan(&fenceEvents); err != nil {
		t.Fatal(err)
	}
	if fenceEvents != 2 {
		t.Fatalf("A-B-A source mutation events=%d want 2", fenceEvents)
	}
	if _, err := abaTx.Exec(ctx, `INSERT INTO recording_campaign_admission_tx_authorizations(transaction_id,action,approval_id,account_id,node_id,node_token_id) VALUES(txid_current(),'attempt',$1,47,91,92)`, approval); err != nil {
		t.Fatal(err)
	}
	if _, err := abaTx.Exec(ctx, `INSERT INTO recording_targeted_probe_attempts(request_id,approval_id,account_id,stream_id,attempt_no,node_id,recorder_droplet_id,do_droplet_id,region,probe_build_sha,source_revision_id,source_url_sha256,source_page_url_sha256,source_updated_at,challenge,expires_at) SELECT $2,$1,47,17235,1,91,231,592454108,'nyc1',$3,source_revision_id,source_url_sha256,source_page_url_sha256,source_updated_at,$4,transaction_timestamp()+interval '15 minutes' FROM recording_campaign_admission_reservations WHERE approval_id=$1 AND stream_id=17235`, approval, uuid.New(), strings.Repeat("a", 40), sha("aba")); err == nil || !strings.Contains(err.Error(), "fresh server-issued") {
		t.Fatalf("A-B-A source mutation was not fenced before targeted probing: %v", err)
	}
	if err := abaTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	createAttempt := func(attempt int) (uuid.UUID, string) {
		tx, err := c.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, `INSERT INTO recording_campaign_admission_tx_authorizations(transaction_id,action,approval_id,account_id,node_id,node_token_id) VALUES(txid_current(),'attempt',$1,47,91,92)`, approval); err != nil {
			t.Fatal(err)
		}
		var id uuid.UUID
		challenge := sha(fmt.Sprintf("attempt-%d", attempt))
		err = tx.QueryRow(ctx, `INSERT INTO recording_targeted_probe_attempts(request_id,approval_id,account_id,stream_id,attempt_no,node_id,recorder_droplet_id,do_droplet_id,region,probe_build_sha,source_revision_id,source_url_sha256,source_page_url_sha256,source_updated_at,challenge,expires_at) SELECT $2,$1,47,17235,$3,91,231,592454108,'nyc1',$4,source_revision_id,source_url_sha256,source_page_url_sha256,source_updated_at,$5,transaction_timestamp()+interval '15 minutes' FROM recording_campaign_admission_reservations WHERE approval_id=$1 AND stream_id=17235 RETURNING id`, approval, uuid.New(), attempt, strings.Repeat("a", 40), challenge).Scan(&id)
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		return id, challenge
	}
	insertEvidence := func(attemptID uuid.UUID, challenge string) uuid.UUID {
		tx, err := c.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, `INSERT INTO recording_campaign_admission_tx_authorizations(transaction_id,action,approval_id,account_id,node_id,node_token_id) VALUES(txid_current(),'evidence',$1,47,91,92)`, approval); err != nil {
			t.Fatal(err)
		}
		fps := 30.0
		evidence := recordability.TargetedEvidence{Result: recordability.ResultOK, Detail: "valid_ratio=1.000 segments=2 native_signature_stable=true frame=true", ValidRatio: 1, DurationMs: 120000, SegmentCount: 2, FrameSHA256: strings.Repeat("c", 64), MediaSHA256: strings.Repeat("d", 64), VideoCodec: "h264", AudioCodec: "aac", AudioPresent: true, VideoWidth: 1920, VideoHeight: 1080, ActualFPS: &fps}
		evidence.NativeSignatureSHA256 = recordability.TargetedNativeSignatureSHA256(evidence)
		evidence.ChallengeProofSHA256 = recordability.TargetedChallengeProofSHA256(challenge, evidence)
		var id uuid.UUID
		err = tx.QueryRow(ctx, `INSERT INTO recording_targeted_probe_evidence(attempt_id,approval_id,account_id,stream_id,result,valid_ratio,duration_ms,segment_count,frame_sha256,media_sha256,native_signature_sha256,challenge_proof_sha256,video_codec,audio_codec,audio_present,video_width,video_height,actual_fps,detail,evidence_sha256) VALUES($1,$2,47,17235,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18) RETURNING id`, attemptID, approval, evidence.Result, evidence.ValidRatio, evidence.DurationMs, evidence.SegmentCount, evidence.FrameSHA256, evidence.MediaSHA256, evidence.NativeSignatureSHA256, evidence.ChallengeProofSHA256, evidence.VideoCodec, evidence.AudioCodec, evidence.AudioPresent, evidence.VideoWidth, evidence.VideoHeight, *evidence.ActualFPS, evidence.Detail, strings.Repeat("0", 64)).Scan(&id)
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		return id
	}
	firstAttempt, firstChallenge := createAttempt(1)
	first := insertEvidence(firstAttempt, firstChallenge)
	secondAttempt, secondChallenge := createAttempt(2)
	second := insertEvidence(secondAttempt, secondChallenge)
	tx, err := c.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `INSERT INTO recording_campaign_admission_tx_authorizations(transaction_id,action,approval_id,account_id,actor_user_id,account_session_id) VALUES(txid_current(),'admit',$1,47,7,51)`, approval); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `UPDATE recordings SET status='active',paused_at=NULL WHERE id=381`); err != nil {
		t.Fatal(err)
	}
	var track, roster int64
	if err = tx.QueryRow(ctx, `INSERT INTO recording_campaign_tracks(account_id,campaign_key,label,deadline_at,target_count,grade_floor,created_by_user_id) VALUES(47,$1,'test',$2,1,'GOOD',7) RETURNING id`, "targeted-admission-"+approval.String(), end).Scan(&track); err != nil {
		t.Fatal(err)
	}
	if err = tx.QueryRow(ctx, `INSERT INTO recording_campaign_roster_entries(track_id,recording_id,stream_id,scene_identity_sha256,role,rank,status,reason_codes,effective_at,decision_at,evidence_observed_at,evidence_sha256,updated_by_user_id) VALUES($1,381,17235,$2,'primary',1,'probation',ARRAY['test'],transaction_timestamp(),transaction_timestamp(),transaction_timestamp(),$3,7) RETURNING id`, track, strings.Repeat("b", 64), strings.Repeat("d", 64)).Scan(&roster); err != nil {
		t.Fatal(err)
	}
	var scheduleSHA string
	if err = tx.QueryRow(ctx, `SELECT schedule_sha256 FROM recording_campaign_admission_approvals WHERE id=$1`, approval).Scan(&scheduleSHA); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO recording_campaign_admission_results(approval_id,first_probe_evidence_id,second_probe_evidence_id,account_id,track_id,roster_entry_id,stream_id,recording_id,actor_user_id,schedule_sha256,recording_config_sha256) VALUES($1,$2,$3,47,$4,$5,17235,381,7,$6,$7)`, approval, first, second, track, roster, scheduleSHA, strings.Repeat("0", 64)); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `SELECT transition_recording_campaign_track($1,'active',ARRAY['test'],7,transaction_timestamp())`, track); err != nil {
		t.Fatal(err)
	}
	responseJSON := []byte(fmt.Sprintf(`{"items":[{"stream_id":17235,"recording_id":381,"action":"updated","timezone":"Europe/Berlin"}],"created":0,"updated":1,"dry_run":false,"relay_streams":0,"online_relay_slots":0,"required_relay_slots":0,"campaign_track_id":%d,"campaign_admission_approval_id":%q}`, track, approval.String()))
	if _, err = tx.Exec(ctx, `INSERT INTO recording_campaign_admission_commits(approval_id,account_id,actor_user_id,track_id,schedule_sha256,response_json,response_sha256) VALUES($1,47,7,$2,$3,$4::jsonb,encode(sha256(convert_to($4::jsonb::text,'UTF8')),'hex'))`, approval, track, scheduleSHA, responseJSON); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	for name, statement := range map[string]string{
		"recording config":     `UPDATE recordings SET folder_name='forged' WHERE id=381`,
		"premature completion": `UPDATE recordings SET status='completed' WHERE id=381`,
		"paused lifecycle":     `UPDATE recordings SET status='paused',paused_at=transaction_timestamp() WHERE id=381`,
		"roster binding":       fmt.Sprintf(`UPDATE recording_campaign_roster_entries SET status='removed' WHERE id=%d`, roster),
		"track protection":     fmt.Sprintf(`SELECT transition_recording_campaign_track(%d,'complete',ARRAY['hostile'],7,transaction_timestamp())`, track),
		"source ABA":           `UPDATE streams SET source_url='https://source.example/other.m3u8',updated_at=transaction_timestamp() WHERE id=17235; UPDATE streams SET source_url='https://source.example/live.m3u8' WHERE id=17235`,
		"source revision":      `INSERT INTO stream_source_revisions VALUES(82,17235)`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := c.Exec(ctx, statement); err == nil {
				t.Fatalf("hostile post-admission mutation was not rejected: %v", err)
			}
		})
	}
}
