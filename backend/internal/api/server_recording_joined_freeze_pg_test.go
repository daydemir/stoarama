package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/db"
	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/daydemir/stoarama/backend/internal/recordingnaming"
	"github.com/daydemir/stoarama/backend/internal/secretbox"
)

var joinedMigrationNames = []string{
	"0137_recording_joined_outputs.sql",
	"0138_joined_historical_qualification_authority.sql",
	"0139_joined_historical_completed_recordings.sql",
	"0140_joined_tier1_resumable_freeze.sql",
	"0141_joined_tier1_checkpointed_dry_run.sql",
}

func testJoinedServerBeforeMigration(t *testing.T) (*Server, *pgxpool.Pool, func(), func()) {
	t.Helper()
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run joined freeze regressions")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("joined_freeze_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.ConnConfig.RuntimeParams["timezone"] = "UTC"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE schema_migrations(version TEXT PRIMARY KEY,applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		t.Fatal(err)
	}
	for _, migration := range joinedMigrationNames {
		if _, err := pool.Exec(ctx, `INSERT INTO schema_migrations(version) VALUES($1)`, migration); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.MigrateUp(ctx, pool, findMigrationsDir(t)); err != nil {
		t.Fatal(err)
	}
	sentinel := fmt.Sprintf("%s:%d", schema, time.Now().UnixNano())
	if _, err := pool.Exec(ctx, `CREATE TABLE joined_disposable_test_sentinel(value TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO joined_disposable_test_sentinel VALUES($1)`, sentinel); err != nil {
		t.Fatal(err)
	}
	s := &Server{pool: pool}
	applyMigration := func() {
		t.Helper()
		for _, migration := range joinedMigrationNames {
			if _, err := pool.Exec(ctx, `DELETE FROM schema_migrations WHERE version=$1`, migration); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.MigrateUp(ctx, pool, findMigrationsDir(t)); err != nil {
			t.Fatal(err)
		}
	}
	cleanup := func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
		admin.Close()
	}
	return s, pool, applyMigration, cleanup
}

type joinedHistoricalTier1Fixture struct {
	s               *Server
	pool            *pgxpool.Pool
	cleanup         func()
	userID          int64
	accountID       int64
	storageID       int64
	apiKeyID        int64
	connectionID    int64
	runID           int64
	firstJobID      int64
	clipID          int64
	zeroClipID      int64
	clipStart       time.Time
	sessionToken    string
	req             joinedTier1FreezeRequest
	plan            joinedTier1FreezePlan
	call            func(joinedTier1FreezeRequest) (*httptest.ResponseRecorder, joinedTier1FreezePlan)
	callWithContext func(context.Context, joinedTier1FreezeRequest) (*httptest.ResponseRecorder, joinedTier1FreezePlan)
	callHistorical  func(joinedHistoricalQualificationRequest) (*httptest.ResponseRecorder, joinedHistoricalQualificationPlan, int64)
	historicalReq   joinedHistoricalQualificationRequest
}

func finishJoinedTier1Fixture(t *testing.T, fixture joinedHistoricalTier1Fixture, req joinedTier1FreezeRequest) {
	t.Helper()
	for call := 1; call <= len(joinedrecording.Tier1RecordingIDs)+2; call++ {
		var state string
		err := fixture.pool.QueryRow(context.Background(), `SELECT state FROM recording_joined_batches WHERE batch_id=$1`, req.BatchID).Scan(&state)
		if err == nil && state != "snapshotting" {
			return
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			t.Fatal(err)
		}
		if response, _ := fixture.call(req); response.Code != http.StatusOK {
			t.Fatalf("finish resumable freeze call=%d status=%d body=%s", call, response.Code, response.Body.String())
		}
	}
	t.Fatal("resumable freeze did not reach building")
}

func newJoinedHistoricalTier1Fixture(t *testing.T, email string) joinedHistoricalTier1Fixture {
	return newJoinedHistoricalTier1FixtureWithCheckpoint(t, email, true)
}

func newJoinedHistoricalTier1FixtureWithoutCheckpoint(t *testing.T, email string) joinedHistoricalTier1Fixture {
	return newJoinedHistoricalTier1FixtureWithCheckpoint(t, email, false)
}

func newJoinedHistoricalTier1FixtureWithCheckpoint(t *testing.T, email string, seedCheckpoint bool) joinedHistoricalTier1Fixture {
	t.Helper()
	s, pool, applyMigration, cleanup := testJoinedServerBeforeMigration(t)
	applyMigration()
	ctx := context.Background()
	cipher, err := secretbox.NewFromBase64Key("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	if err != nil {
		t.Fatal(err)
	}
	s.secrets = cipher
	sourceSecret, err := cipher.Encrypt([]byte("joined-source-storage-secret"))
	if err != nil {
		t.Fatal(err)
	}
	userID, accountID := seedUserOrg(t, pool, email, true)
	var storageID int64
	if err := pool.QueryRow(ctx, `INSERT INTO storage_destinations(account_id,name,provider,endpoint,region,bucket,
		access_key_id,secret_access_key_enc,status,managed) VALUES($1,'joined-source','r2_managed',$2,'auto','clips',
		'key',$3,'verified',true) RETURNING id`, accountID, joinedTestSourceEndpoint, sourceSecret).Scan(&storageID); err != nil {
		t.Fatal(err)
	}
	metadata := recordingnaming.Metadata{PlazaID: "1", Continent: "Europe", Country: "Italy", City: "Bevagna", PlazaName: "Piazza"}
	metadataJSON, _ := json.Marshal(metadata)
	for _, recordingID := range joinedrecording.Tier1RecordingIDs {
		recordingStatus := "active"
		if recordingID == joinedrecording.Tier1RecordingIDs[0] {
			recordingStatus = "completed"
		}
		var streamID int64
		if err := pool.QueryRow(ctx, `INSERT INTO streams(provider,external_id,name,slug,source_url,source_page_url,
			capture_type,source_family,execution_class,capture_family,expected_fps) VALUES('direct',$1,$2,$1,$3,'','hls',
			'video_manifest','video_live','continuous_video',30) RETURNING id`, fmt.Sprintf("joined-%d", recordingID),
			fmt.Sprintf("stream-%d", recordingID), fmt.Sprintf("https://example.test/%d.m3u8", recordingID)).Scan(&streamID); err != nil {
			t.Fatal(err)
		}
		folder, err := recordingnaming.BuildFolderName(recordingnaming.ProfilePlazaHourlyV1, recordingID, metadata, "")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,source_kind,
			cron_expr,cron_timezone,clip_duration_sec,status,start_at,stream_id,mode,daily_window_start,daily_window_end,
			active_weekdays,delivery,naming_profile,folder_name,naming_metadata_jsonb) VALUES($1,$2,$3,$4,$5,'hls_live',
			'0 8 * * *','UTC',60,$6,'2026-07-01', $7,'continuous','08:00','20:00',127,'nas_pull',
			'plaza_hourly_v1',$8,$9)`, recordingID, accountID, storageID, fmt.Sprintf("recording-%d", recordingID),
			fmt.Sprintf("https://example.test/%d.m3u8", recordingID), recordingStatus, streamID, folder, metadataJSON); err != nil {
			t.Fatal(err)
		}
	}
	var apiKeyID, connectionID int64
	if err := pool.QueryRow(ctx, `INSERT INTO account_api_keys(account_id,key_prefix,secret_hash,label,scopes)
		VALUES($1,'sir_freeze','freeze-key','NAS',ARRAY['stoarama.pull']) RETURNING id`, accountID).Scan(&apiKeyID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO connections(account_id,kind,label,api_key_id,joined_protocol_version)
		VALUES($1,'nas_pull','NAS',$2,1) RETURNING id`, accountID, apiKeyID).Scan(&connectionID); err != nil {
		t.Fatal(err)
	}
	jobMap := make([]joinedHistoricalQualificationJobs, len(joinedrecording.Tier1RecordingIDs))
	firstDate := time.Date(2026, 7, 29, 8, 0, 0, 0, time.UTC)
	var firstJobID int64
	for recordingOrdinal, recordingID := range joinedrecording.Tier1RecordingIDs {
		jobMap[recordingOrdinal] = joinedHistoricalQualificationJobs{RecordingID: recordingID, JobIDs: make([]int64, 14)}
		for day := 0; day < 14; day++ {
			start := firstDate.AddDate(0, 0, day)
			scheduled := start
			if day == 1 {
				scheduled = start.Add(time.Minute)
			}
			status, completed := "done", start.Add(12*time.Hour+time.Minute)
			localDate := start.Format("2006-01-02")
			if (recordingID == 348 && localDate == "2026-07-29") ||
				((recordingID == 408 || recordingID == 406 || recordingID == 409) && localDate == "2026-08-11") {
				status, completed = "error", start.Add(time.Hour)
			}
			var jobID int64
			if err := pool.QueryRow(ctx, `INSERT INTO recording_jobs(recording_id,fire_at,scheduled_for,clip_duration_sec,
				status,idempotency_key,kind,window_end_at,completed_at) VALUES($1,$2,$3,60,$4,$5,'continuous_window',$6,$7)
				RETURNING id`, recordingID, start, scheduled, status, fmt.Sprintf("joined-historical:%d:%d", recordingID, day),
				start.Add(12*time.Hour), completed).Scan(&jobID); err != nil {
				t.Fatal(err)
			}
			jobMap[recordingOrdinal].JobIDs[day] = jobID
			if recordingOrdinal == 0 && day == 0 {
				firstJobID = jobID
			}
		}
	}
	s.cfg.JoinedRecordingControlPlaneEnabled = true
	s.cfg.JoinedRecordingProtocolVersion = 1
	s.cfg.JoinedRecordingWorkScope = config.JoinedWorkScopeCanary
	s.cfg.JoinedWorkerBootstrapToken = "joined-bootstrap-credential-32bytes"
	s.cfg.JoinedWorkerSigningKey = "joined-signing-credential-32-bytes"
	s.cfg.JoinedRecordingBatchID = joinedrecording.Tier1BatchID
	s.cfg.JoinedRecordingCanaryHourIDs = joinedCanaryScopeForTest(joinedrecording.Tier1BatchID,
		fmt.Sprintf("%s__recording-%d__date-2026-08-01__hour-01__generation-1",
			joinedrecording.Tier1BatchID, joinedrecording.Tier1RecordingIDs[0]))
	token := "joined-freeze-admin-session"
	insertSession(t, pool, accountID, userID, token)
	callHistorical := func(req joinedHistoricalQualificationRequest) (*httptest.ResponseRecorder, joinedHistoricalQualificationPlan, int64) {
		body, _ := json.Marshal(req)
		httpReq := httptest.NewRequest(http.MethodPost,
			"/api/v1/recording/joined/qualification/import-tier1-historical", bytes.NewReader(body))
		httpReq.AddCookie(&http.Cookie{Name: accountSessionCookie, Value: token})
		recorder := httptest.NewRecorder()
		s.requireAdminAuth(http.HandlerFunc(s.handleAdminJoinedHistoricalQualification)).ServeHTTP(recorder, httpReq)
		var response struct {
			Plan  joinedHistoricalQualificationPlan `json:"plan"`
			RunID int64                             `json:"run_id"`
		}
		_ = json.Unmarshal(recorder.Body.Bytes(), &response)
		return recorder, response.Plan, response.RunID
	}
	historicalRequest := joinedHistoricalQualificationRequest{ProtocolVersion: joinedrecording.JoinedProtocolVersion,
		ConnectionID: connectionID, BatchID: joinedrecording.Tier1BatchID, Generation: 1, RecordingJobs: jobMap}
	dryHistorical, historicalPlan, _ := callHistorical(historicalRequest)
	if dryHistorical.Code != http.StatusOK || !lowerHexSHA256(historicalPlan.RequestSHA256) ||
		len(historicalPlan.Members) != 33 || len(historicalPlan.Members[0].Qualification.Days) != 14 ||
		len(historicalPlan.Members[0].Qualification.Days[1].ReasonCodes) != 1 ||
		historicalPlan.Members[0].Qualification.Days[1].ReasonCodes[0] != "scheduled_for_drift" {
		t.Fatalf("historical authority dry-run status=%d body=%s", dryHistorical.Code, dryHistorical.Body.String())
	}
	badApply := historicalRequest
	badApply.Apply, badApply.ExpectedRequestSHA256 = true, strings.Repeat("0", 64)
	if response, _, _ := callHistorical(badApply); response.Code != http.StatusConflict {
		t.Fatalf("historical authority accepted wrong approval hash: status=%d body=%s", response.Code, response.Body.String())
	}
	var runsBefore int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM recording_qualification_runs`).Scan(&runsBefore); err != nil || runsBefore != 0 {
		t.Fatalf("failed historical apply mutated runs=%d err=%v", runsBefore, err)
	}
	historicalRequest.Apply, historicalRequest.ExpectedRequestSHA256 = true, historicalPlan.RequestSHA256
	appliedHistorical, appliedPlan, runID := callHistorical(historicalRequest)
	if appliedHistorical.Code != http.StatusOK || runID <= 0 || appliedPlan.RequestSHA256 != historicalPlan.RequestSHA256 {
		t.Fatalf("historical authority apply status=%d body=%s", appliedHistorical.Code, appliedHistorical.Body.String())
	}
	cutoff, _ := time.Parse(time.RFC3339Nano, joinedrecording.Tier1FrozenAt)
	var runFrozen time.Time
	var members, windows, nullScenes, errorJobs int
	if err := pool.QueryRow(ctx, `SELECT q.frozen_at,
		(SELECT count(*) FROM recording_qualification_members m WHERE m.run_id=q.id),
		(SELECT count(*) FROM recording_qualification_windows w WHERE w.run_id=q.id),
		(SELECT count(*) FROM recording_qualification_members m WHERE m.run_id=q.id
		  AND m.scene_identity_sha256 IS NULL AND m.scene_frame_evidence_id IS NULL),
		(SELECT count(*) FROM recording_jobs j WHERE j.id=ANY(ARRAY(
		  SELECT jobs.job_id::bigint FROM jsonb_array_elements(q.definition_jsonb->'recording_jobs') AS entries(item)
		  CROSS JOIN LATERAL jsonb_array_elements_text(entries.item->'job_ids') AS jobs(job_id))) AND j.status='error')
		FROM recording_qualification_runs q WHERE q.id=$1 AND q.status='active'
		AND q.definition_version=$2`, runID, joinedrecording.Tier1HistoricalQualificationVersion).
		Scan(&runFrozen, &members, &windows, &nullScenes, &errorJobs); err != nil ||
		!runFrozen.After(cutoff) || members != 33 || windows != 462 || nullScenes != 33 || errorJobs != 4 {
		t.Fatalf("historical authority persisted frozen=%v members=%d windows=%d null_scenes=%d errors=%d err=%v",
			runFrozen, members, windows, nullScenes, errorJobs, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_qualification_members SET recording_name='forbidden' WHERE run_id=$1`, runID); err == nil {
		t.Fatal("ordinary mutation changed an active historical qualification member")
	}
	if tag, err := pool.Exec(ctx, `UPDATE recording_jobs SET scheduled_for=scheduled_for+interval '1 minute' WHERE id=$1`, firstJobID); err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("raw historical job update must remain trigger-free: tag=%v err=%v", tag, err)
	}
	if replay, replayPlan, replayRunID := callHistorical(historicalRequest); replay.Code != http.StatusOK ||
		replayRunID != runID || replayPlan.RequestSHA256 != historicalPlan.RequestSHA256 {
		t.Fatalf("immutable historical authority replay after raw update status=%d body=%s", replay.Code, replay.Body.String())
	}
	var clipID int64
	clipStart := firstDate
	if err := pool.QueryRow(ctx, `INSERT INTO recording_clips(recording_id,recording_job_id,storage_destination_id,
		endpoint,bucket,object_key,display_path,mime_type,container,size_bytes,etag,sha256,duration_ms,video_codec,
		audio_present,fire_at,clip_start_at,clip_end_at,created_at,released_at) VALUES($1,$2,$3,$4,'clips','raw/one.mp4',
		'raw/one.mp4','video/mp4','mp4',10,'etag-one',$5,60000,'h264',false,$6,$6,$7,$6,$7) RETURNING id`,
		joinedrecording.Tier1RecordingIDs[0], firstJobID, storageID, joinedTestSourceEndpoint, strings.Repeat("a", 64),
		clipStart, clipStart.Add(time.Minute)).Scan(&clipID); err != nil {
		t.Fatal(err)
	}
	var zeroClipID int64
	if err := pool.QueryRow(ctx, `INSERT INTO recording_clips(recording_id,recording_job_id,storage_destination_id,
		endpoint,bucket,object_key,display_path,mime_type,container,size_bytes,etag,sha256,duration_ms,video_codec,
		audio_present,fire_at,clip_start_at,clip_end_at,created_at,released_at) VALUES($1,$2,$3,$4,'clips','raw/zero.mp4',
		'raw/zero.mp4','video/mp4','mp4',0,'etag-zero',$5,60000,'h264',false,$6,$6,$7,$6,$7) RETURNING id`,
		joinedrecording.Tier1RecordingIDs[0], firstJobID, storageID, joinedTestSourceEndpoint, strings.Repeat("0", 64),
		clipStart.Add(time.Minute), clipStart.Add(2*time.Minute)).Scan(&zeroClipID); err != nil {
		t.Fatal(err)
	}
	callWithContext := func(callCtx context.Context, req joinedTier1FreezeRequest) (*httptest.ResponseRecorder, joinedTier1FreezePlan) {
		body, _ := json.Marshal(req)
		httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/admin/recording/joined/freeze-tier1", bytes.NewReader(body)).WithContext(callCtx)
		httpReq.AddCookie(&http.Cookie{Name: accountSessionCookie, Value: token})
		rec := httptest.NewRecorder()
		s.requireAdminAuth(http.HandlerFunc(s.handleAdminJoinedFreezeTier1)).ServeHTTP(rec, httpReq)
		var response struct {
			Plan joinedTier1FreezePlan `json:"plan"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &response)
		return rec, response.Plan
	}
	call := func(req joinedTier1FreezeRequest) (*httptest.ResponseRecorder, joinedTier1FreezePlan) {
		return callWithContext(ctx, req)
	}
	req := validJoinedTier1FreezeRequest(t)
	req.ConnectionID, req.QualificationRunID = connectionID, runID
	s.cfg.JoinedRecordingBatchID = req.BatchID
	s.cfg.JoinedRecordingCanaryHourIDs = joinedCanaryScopeForTest(req.BatchID, fmt.Sprintf("%s__recording-%d__date-2026-08-01__hour-01__generation-1",
		req.BatchID, joinedrecording.Tier1RecordingIDs[0]))
	dry, plan := call(req)
	if dry.Code != http.StatusOK || !lowerHexSHA256(plan.RequestSHA256) || !lowerHexSHA256(plan.FrozenDenominatorSHA256) ||
		!lowerHexSHA256(plan.FreezeExclusionsSHA256) || plan.ProvisionalSourceClips != 1 || plan.ProvisionalSourceBytes != 10 ||
		plan.ProvisionalExclusions != 1 ||
		len(plan.Recordings) != 33 || len(plan.Recordings[0].Qualification.Days) != 14 {
		t.Fatalf("dry-run status=%d body=%s", dry.Code, dry.Body.String())
	}
	firstImportedDay := plan.Recordings[0].Qualification.Days[0]
	if firstImportedDay.ScheduledFor == nil || !firstImportedDay.ScheduledFor.Equal(firstImportedDay.WindowStart) ||
		len(firstImportedDay.ReasonCodes) != 0 {
		t.Fatalf("post-import raw job mutation changed immutable qualification: %+v", firstImportedDay)
	}
	if seedCheckpoint {
		progress, err := s.startJoinedTier1DryRun(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		for ordinal := 1; ordinal <= len(joinedrecording.Tier1RecordingIDs); ordinal++ {
			progress, err = s.stepJoinedTier1DryRun(ctx, joinedTier1DryRunStepRequest{RunID: progress.RunID, PriorityOrdinal: ordinal})
			if err != nil {
				t.Fatalf("seed checkpoint step %d: %v", ordinal, err)
			}
		}
		if progress.RequestSHA256 == nil || *progress.RequestSHA256 != plan.RequestSHA256 {
			t.Fatalf("seed checkpoint sha=%v want=%s", progress.RequestSHA256, plan.RequestSHA256)
		}
	}
	return joinedHistoricalTier1Fixture{s: s, pool: pool, cleanup: cleanup, userID: userID, accountID: accountID,
		storageID: storageID, apiKeyID: apiKeyID, connectionID: connectionID, runID: runID, firstJobID: firstJobID,
		clipID: clipID, zeroClipID: zeroClipID, clipStart: clipStart, sessionToken: token, req: req, plan: plan, call: call, callWithContext: callWithContext,
		callHistorical: callHistorical, historicalReq: historicalRequest}
}

func TestJoinedHistoricalQualificationRecordingStatuses(t *testing.T) {
	fixture := newJoinedHistoricalTier1Fixture(t, "joined-historical-status@example.test")
	defer fixture.cleanup()
	for _, recordingStatus := range []string{"paused", "canceled"} {
		t.Run(recordingStatus, func(t *testing.T) {
			if _, err := fixture.pool.Exec(context.Background(), `UPDATE recordings SET status=$1,
				paused_at=CASE WHEN $1='paused' THEN now() ELSE NULL END WHERE id=$2`,
				recordingStatus, joinedrecording.Tier1RecordingIDs[0]); err != nil {
				t.Fatal(err)
			}
			req := fixture.historicalReq
			req.Apply = false
			req.ExpectedRequestSHA256 = ""
			response, _, _ := fixture.callHistorical(req)
			if response.Code != http.StatusConflict {
				t.Fatalf("historical qualification accepted recording status %q: status=%d body=%s",
					recordingStatus, response.Code, response.Body.String())
			}
		})
	}
}

func TestJoinedTier1HistoricalApplyUsesExactFrozenDenominator(t *testing.T) {
	// This test deliberately mutates a source before checkpoint creation to
	// prove the dry-run evidence changes. Retention fencing is exercised after
	// the checkpoint starts by the dedicated checkpointed-dry-run test.
	fixture := newJoinedHistoricalTier1FixtureWithoutCheckpoint(t, "joined-freeze@example.test")
	defer fixture.cleanup()
	ctx := context.Background()
	pool := fixture.pool
	accountID := fixture.accountID
	storageID, connectionID, runID := fixture.storageID, fixture.connectionID, fixture.runID
	firstJobID, clipID, zeroClipID, clipStart := fixture.firstJobID, fixture.clipID, fixture.zeroClipID, fixture.clipStart
	req, plan, call, callWithContext := fixture.req, fixture.plan, fixture.call, fixture.callWithContext
	var secondStorageID int64
	if err := pool.QueryRow(ctx, `INSERT INTO storage_destinations(account_id,name,provider,endpoint,region,bucket,
		access_key_id,secret_access_key_enc,status,managed) VALUES($1,'joined-source-2','r2',$2,'auto','clips',
		'key',$3,'verified',false) RETURNING id`, accountID, joinedTestSourceEndpoint, []byte{1}).Scan(&secondStorageID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_clips SET storage_destination_id=$1 WHERE id=$2`, secondStorageID, clipID); err != nil {
		t.Fatal(err)
	}
	_, changed := call(req)
	if changed.FrozenDenominatorSHA256 == plan.FrozenDenominatorSHA256 || changed.RequestSHA256 == plan.RequestSHA256 {
		t.Fatal("storage destination substitution did not change frozen evidence")
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_clips SET storage_destination_id=$1 WHERE id=$2`, storageID, clipID); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := fixture.s.startJoinedTier1DryRun(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	for ordinal := 1; ordinal <= len(joinedrecording.Tier1RecordingIDs); ordinal++ {
		checkpoint, err = fixture.s.stepJoinedTier1DryRun(ctx, joinedTier1DryRunStepRequest{
			RunID: checkpoint.RunID, PriorityOrdinal: ordinal,
		})
		if err != nil {
			t.Fatalf("checkpoint step %d: %v", ordinal, err)
		}
	}
	if checkpoint.RequestSHA256 == nil || *checkpoint.RequestSHA256 != plan.RequestSHA256 {
		t.Fatalf("checkpoint hash=%v want=%s", checkpoint.RequestSHA256, plan.RequestSHA256)
	}
	protocolTx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := protocolTx.Exec(ctx, `UPDATE connections SET joined_protocol_version=0 WHERE id=$1`, connectionID); err != nil {
		_ = protocolTx.Rollback(ctx)
		t.Fatal(err)
	}
	protocolReq := req
	protocolReq.Apply, protocolReq.ExpectedRequestSHA256 = true, plan.RequestSHA256
	protocolHandlerCtx, cancelProtocolHandler := context.WithCancel(ctx)
	defer cancelProtocolHandler()
	protocolResult := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec, _ := callWithContext(protocolHandlerCtx, protocolReq)
		protocolResult <- rec
	}()
	abortProtocolRace := func() *httptest.ResponseRecorder {
		_ = protocolTx.Rollback(ctx)
		cancelProtocolHandler()
		select {
		case result := <-protocolResult:
			return result
		case <-time.After(5 * time.Second):
			return nil
		}
	}
	var protocolPID int32
	if err := protocolTx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&protocolPID); err != nil {
		result := abortProtocolRace()
		t.Fatalf("load protocol-revocation backend: %v (handler=%v)", err, result)
	}
	protocolWaitCtx, cancelProtocolWait := context.WithTimeout(ctx, 5*time.Second)
	defer cancelProtocolWait()
	for {
		select {
		case early := <-protocolResult:
			_ = protocolTx.Rollback(ctx)
			t.Fatalf("protocol-race apply returned before revocation committed: status=%d body=%s", early.Code, early.Body.String())
		default:
		}
		var blocked bool
		if err := pool.QueryRow(protocolWaitCtx, fmt.Sprintf(`SELECT EXISTS(SELECT 1 FROM pg_stat_activity
			WHERE %d=ANY(pg_blocking_pids(pid)))`, protocolPID)).Scan(&blocked); err != nil {
			result := abortProtocolRace()
			t.Fatalf("observe protocol-revocation lock: %v (handler=%v)", err, result)
		}
		if blocked {
			break
		}
		select {
		case <-protocolWaitCtx.Done():
			result := abortProtocolRace()
			if result == nil {
				t.Fatal("apply did not linearize behind protocol revocation and did not stop after cancellation")
			}
			t.Fatalf("apply did not linearize behind protocol revocation: status=%d body=%s", result.Code, result.Body.String())
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err := protocolTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if revoked := <-protocolResult; revoked.Code != http.StatusConflict {
		t.Fatalf("protocol-revoked apply status=%d body=%s", revoked.Code, revoked.Body.String())
	}
	if _, err := pool.Exec(ctx, `UPDATE connections SET joined_protocol_version=1 WHERE id=$1`, connectionID); err != nil {
		t.Fatal(err)
	}
	var raceClipID int64
	if err := pool.QueryRow(ctx, `INSERT INTO recording_clips(recording_id,recording_job_id,storage_destination_id,
		endpoint,bucket,object_key,display_path,mime_type,container,size_bytes,etag,sha256,duration_ms,video_codec,
		audio_present,fire_at,clip_start_at,clip_end_at,created_at) VALUES($1,$2,$3,$4,'clips','raw/race.mp4',
		'raw/race.mp4','video/mp4','mp4',11,'etag-race',$5,60000,'h264',false,$6,$6,$7,$6) RETURNING id`,
		joinedrecording.Tier1RecordingIDs[0], firstJobID, storageID, joinedTestSourceEndpoint, strings.Repeat("b", 64),
		clipStart.Add(time.Minute), clipStart.Add(2*time.Minute)).Scan(&raceClipID); err != nil {
		t.Fatal(err)
	}
	rr, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rr.Exec(ctx, `UPDATE recording_clips SET purged_at=now() WHERE id=$1`, raceClipID); err == nil {
		_ = rr.Rollback(ctx)
		t.Fatal("repeatable-read purge bypassed the retention fence")
	}
	_ = rr.Rollback(ctx)
	_, beforePurge := call(req)
	purgeTx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := purgeTx.Exec(ctx, `UPDATE recording_clips SET purged_at=now() WHERE id=$1`, raceClipID); err != nil {
		_ = purgeTx.Rollback(ctx)
		t.Fatal(err)
	}
	conflictingReq := req
	conflictingReq.Apply, conflictingReq.ExpectedRequestSHA256 = true, beforePurge.RequestSHA256
	conflictingResult := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec, _ := call(conflictingReq)
		conflictingResult <- rec
	}()
	waitJoinedDatabaseCondition(t, pool, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity
		WHERE pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE '%pg_advisory_xact_lock(137,1)%')`)
	if err := purgeTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if conflict := <-conflictingResult; conflict.Code != http.StatusConflict {
		t.Fatalf("purge-first apply status=%d body=%s", conflict.Code, conflict.Body.String())
	}
	var prematureBatches int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM recording_joined_batches WHERE batch_id=$1`, req.BatchID).Scan(&prematureBatches); err != nil || prematureBatches != 0 {
		t.Fatalf("purge-first apply created batches=%d err=%v", prematureBatches, err)
	}
	_, plan = call(req)
	req.Apply, req.ExpectedRequestSHA256 = true, plan.RequestSHA256
	blocker, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := blocker.Exec(ctx, `SELECT id FROM recording_qualification_runs WHERE id=$1 FOR UPDATE`, runID); err != nil {
		_ = blocker.Rollback(ctx)
		t.Fatal(err)
	}
	applyResult := make(chan struct {
		recorder *httptest.ResponseRecorder
		plan     joinedTier1FreezePlan
	}, 1)
	go func() {
		recorder, resultPlan := call(req)
		applyResult <- struct {
			recorder *httptest.ResponseRecorder
			plan     joinedTier1FreezePlan
		}{recorder, resultPlan}
	}()
	waitJoinedDatabaseCondition(t, pool, `SELECT EXISTS(SELECT 1 FROM pg_locks
		WHERE locktype='advisory' AND classid=137 AND objid=1 AND granted)`)
	purgeResult := make(chan error, 1)
	go func() {
		_, purgeErr := pool.Exec(ctx, `UPDATE recording_clips SET purged_at=now() WHERE id=$1`, clipID)
		purgeResult <- purgeErr
	}()
	waitJoinedDatabaseCondition(t, pool, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity
		WHERE pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE 'UPDATE recording_clips SET purged_at=now()%')`)
	if err := blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	appliedResult := <-applyResult
	applied, appliedPlan := appliedResult.recorder, appliedResult.plan
	if applied.Code != http.StatusOK || appliedPlan.RequestSHA256 != plan.RequestSHA256 {
		t.Fatalf("apply status=%d body=%s", applied.Code, applied.Body.String())
	}
	if replay, _ := call(req); replay.Code != http.StatusOK {
		t.Fatalf("exact apply replay status=%d body=%s", replay.Code, replay.Body.String())
	}
	if _, err := pool.Exec(ctx, `UPDATE connections SET joined_protocol_version=0 WHERE id=$1`, connectionID); err != nil {
		t.Fatal(err)
	}
	if replay, _ := call(req); replay.Code != http.StatusConflict {
		t.Fatalf("protocol-zero apply replay status=%d body=%s", replay.Code, replay.Body.String())
	}
	if _, err := pool.Exec(ctx, `UPDATE connections SET joined_protocol_version=1 WHERE id=$1`, connectionID); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*joinedTier1FreezeRequest){
		"connection":        func(changed *joinedTier1FreezeRequest) { changed.ConnectionID++ },
		"qualification run": func(changed *joinedTier1FreezeRequest) { changed.QualificationRunID++ },
		"source endpoint": func(changed *joinedTier1FreezeRequest) {
			changed.SourceEndpoint = "https://abcdef0123456789abcdef0123456789.r2.cloudflarestorage.com"
		},
	} {
		t.Run("existing batch rejects changed "+name, func(t *testing.T) {
			changed := req
			mutate(&changed)
			response, _ := call(changed)
			if response.Code != http.StatusConflict {
				t.Fatalf("changed %s status=%d body=%s", name, response.Code, response.Body.String())
			}
		})
	}
	finishJoinedTier1Fixture(t, fixture, req)
	if err := <-purgeResult; err == nil {
		t.Fatal("apply-first purge removed a frozen source")
	}
	var state string
	var recordings, days, snapshots int
	if err := pool.QueryRow(ctx, `SELECT b.state,(SELECT count(*) FROM recording_joined_batch_recordings WHERE batch_record_id=b.id),
		(SELECT count(*) FROM recording_joined_stream_days WHERE batch_record_id=b.id),
		(SELECT count(*) FROM recording_joined_source_snapshots WHERE batch_record_id=b.id)
		FROM recording_joined_batches b WHERE b.batch_id=$1`, req.BatchID).Scan(&state, &recordings, &days, &snapshots); err != nil ||
		state != "building" || recordings != 33 || days != 462 || snapshots != 1 {
		t.Fatalf("applied state=%s recordings=%d days=%d snapshots=%d err=%v", state, recordings, days, snapshots, err)
	}
	var zeroSnapshots, zeroExclusions int
	var zeroReason string
	if err := pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM recording_joined_source_snapshots WHERE clip_id=$1),
		(SELECT count(*) FROM recording_joined_freeze_exclusions WHERE clip_id=$1),
		COALESCE((SELECT reason_code FROM recording_joined_freeze_exclusions WHERE clip_id=$1),'')`, zeroClipID).
		Scan(&zeroSnapshots, &zeroExclusions, &zeroReason); err != nil || zeroSnapshots != 0 ||
		zeroExclusions != 1 || zeroReason != "nonpositive_source_size" {
		t.Fatalf("zero-size evidence snapshots=%d exclusions=%d reason=%q err=%v",
			zeroSnapshots, zeroExclusions, zeroReason, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_clips SET purged_at=now() WHERE id=$1`, clipID); err == nil {
		t.Fatal("retention-protected source was purged")
	}
	var batchRecordID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM recording_joined_batches WHERE batch_id=$1`, req.BatchID).Scan(&batchRecordID); err != nil {
		t.Fatal(err)
	}
	_, mismatchedRecordingErr := pool.Exec(ctx, `WITH altered AS (
		SELECT b.*,convert_to(jsonb_set(jsonb_set(jsonb_set(convert_from(b.freeze_request_bytes,'UTF8')::jsonb,
		  '{batch_id}',to_jsonb($2::text)),'{generation}','2'::jsonb),
		  '{recordings,0,frozen_recording,recording_id}','999'::jsonb)::text,'UTF8') bytes
		FROM recording_joined_batches b WHERE b.id=$1)
		INSERT INTO recording_joined_batches(account_id,connection_id,batch_id,generation,source_endpoint,
		  qualification_run_id,qualification_cohort_sha256,qualification_windows_sha256,
		  selected_qualification_windows_sha256,qualification_jobs_sha256,qualification_frozen_at,
		  ordered_recording_ids_sha256,selection_basis,policy_version,eligibility_cutoff,media_tool,media_tool_sha256,
		  freeze_request_bytes,freeze_request_sha256,frozen_denominator_sha256,freeze_exclusions_sha256,
		  expected_recordings,expected_stream_days,expected_scheduled_hours,expected_source_clips,expected_source_bytes,
		  expected_freeze_exclusions)
		SELECT account_id,connection_id,$2,2,source_endpoint,qualification_run_id,qualification_cohort_sha256,
		  qualification_windows_sha256,selected_qualification_windows_sha256,qualification_jobs_sha256,
		  qualification_frozen_at,ordered_recording_ids_sha256,selection_basis,policy_version,eligibility_cutoff,
		  media_tool,media_tool_sha256,bytes,encode(sha256(bytes),'hex'),frozen_denominator_sha256,
		  freeze_exclusions_sha256,expected_recordings,expected_stream_days,expected_scheduled_hours,
		  expected_source_clips,expected_source_bytes,expected_freeze_exclusions FROM altered`, batchRecordID,
		"tier1-historical-generation-2")
	if mismatchedRecordingErr == nil || !strings.Contains(mismatchedRecordingErr.Error(), "owned snapshotting state") {
		t.Fatalf("mismatched canonical recording ordinal error=%v", mismatchedRecordingErr)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_clips SET id=id+1000000 WHERE id=$1`, clipID); err == nil {
		t.Fatal("retention-protected source changed its clip identity")
	}
	planTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := planTx.Exec(ctx, `SET LOCAL enable_seqscan=off`); err != nil {
		_ = planTx.Rollback(ctx)
		t.Fatal(err)
	}
	rows, err := planTx.Query(ctx, `EXPLAIN (COSTS OFF) SELECT 1
		FROM recording_joined_source_snapshots WHERE clip_id=$1`, clipID)
	if err != nil {
		_ = planTx.Rollback(ctx)
		t.Fatal(err)
	}
	var retentionPlan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			rows.Close()
			_ = planTx.Rollback(ctx)
			t.Fatal(err)
		}
		retentionPlan.WriteString(line)
		retentionPlan.WriteByte('\n')
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		_ = planTx.Rollback(ctx)
		t.Fatal(err)
	}
	if !strings.Contains(retentionPlan.String(), "recording_joined_source_snapshots_clip_idx") {
		_ = planTx.Rollback(ctx)
		t.Fatalf("retention lookup did not use clip index:\n%s", retentionPlan.String())
	}
	if err := planTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	t.Log("JOINED_HISTORICAL_APPLY_EXECUTED")
}

func TestJoinedTier1CheckpointedDryRunBuildsCanonicalPlanAndStaysHidden(t *testing.T) {
	fixture := newJoinedHistoricalTier1FixtureWithoutCheckpoint(t, "joined-freeze-checkpointed-dry-run@example.test")
	defer fixture.cleanup()
	ctx := context.Background()
	req := fixture.req
	req.Apply, req.ExpectedRequestSHA256 = false, ""
	empty := []byte(`{}`)
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO recording_joined_dry_runs(account_id,connection_id,batch_id,generation,
		qualification_run_id,input_bytes,input_sha256,skeleton_bytes,skeleton_sha256,state,completed_recordings,
		final_plan_bytes,final_plan_sha256,ready_at) VALUES($1,$2,$3,1,$4,$5,$6,$5,$6,'ready',33,$5,$6,clock_timestamp())`,
		fixture.accountID, fixture.connectionID, req.BatchID+"-direct", fixture.runID, empty, sha256Bytes(empty)); err == nil {
		t.Fatal("direct ready dry-run insert succeeded")
	}
	abandonedReq := req
	abandonedReq.Generation = 2
	abandonedReq.BatchID = strings.TrimSuffix(req.BatchID, "-generation-1") + "-generation-2"
	abandoned, err := fixture.s.startJoinedTier1DryRun(ctx, abandonedReq)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE recording_joined_dry_runs SET state='invalidated',invalidated_at=clock_timestamp()
		WHERE id=$1`, abandoned.RunID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.s.stepJoinedTier1DryRun(ctx, joinedTier1DryRunStepRequest{RunID: abandoned.RunID, PriorityOrdinal: 1}); err == nil {
		t.Fatal("invalidated dry-run resumed")
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE recording_clips SET purged_at=clock_timestamp() WHERE id=$1`, fixture.clipID); err == nil {
		t.Fatal("invalidated dry-run released retention authority")
	}
	progress, err := fixture.s.startJoinedTier1DryRun(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if progress.State != "building" || progress.CompletedRecordings != 0 || progress.NextPriorityOrdinal == nil || *progress.NextPriorityOrdinal != 1 {
		t.Fatalf("start progress=%+v", progress)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE connections SET joined_protocol_version=0 WHERE id=$1`, fixture.connectionID); err != nil {
		t.Fatal(err)
	}
	reconciled, err := fixture.s.startJoinedTier1DryRun(ctx, req)
	if err != nil || reconciled.RunID != progress.RunID {
		t.Fatalf("lost-start reconciliation progress=%+v err=%v", reconciled, err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE connections SET joined_protocol_version=1 WHERE id=$1`, fixture.connectionID); err != nil {
		t.Fatal(err)
	}
	statusRequest := httptest.NewRequest(http.MethodGet, "/api/v1/admin/recording/joined/freeze-tier1/dry-run/status?batch_id="+
		url.QueryEscape(req.BatchID)+"&generation=1", nil)
	statusResponse := httptest.NewRecorder()
	fixture.s.handleAdminJoinedFreezeTier1DryRunStatus(statusResponse, statusRequest)
	if statusResponse.Code != http.StatusOK || !bytes.Contains(statusResponse.Body.Bytes(), []byte(progress.RunID)) {
		t.Fatalf("status by key code=%d body=%s", statusResponse.Code, statusResponse.Body.String())
	}
	var scopes, batches int
	if err := fixture.pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM recording_joined_dry_run_scopes WHERE dry_run_id=$1),
		(SELECT count(*) FROM recording_joined_batches WHERE batch_id=$2)`, progress.RunID, req.BatchID).Scan(&scopes, &batches); err != nil {
		t.Fatal(err)
	}
	if scopes != 462 || batches != 0 {
		t.Fatalf("start scopes=%d batches=%d, want 462/0", scopes, batches)
	}
	var mismatchedWatermarks int
	if err := fixture.pool.QueryRow(ctx, `SELECT count(*) FROM recording_joined_dry_run_scopes s
		JOIN recording_joined_dry_runs r ON r.id=s.dry_run_id
		CROSS JOIN LATERAL (SELECT convert_from(r.skeleton_bytes,'UTF8')::jsonb->'recordings'->(s.priority_ordinal-1)
		 ->'snapshot_days'->(s.date_ordinal-1) expected) x
		WHERE s.dry_run_id=$1 AND (x.expected->>'high_water_clip_id')::bigint IS DISTINCT FROM s.high_water_clip_id`, progress.RunID).
		Scan(&mismatchedWatermarks); err != nil || mismatchedWatermarks != 0 {
		t.Fatalf("watermark authority mismatches=%d err=%v", mismatchedWatermarks, err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE recording_clips SET purged_at=clock_timestamp() WHERE id=$1`, fixture.clipID); err == nil {
		t.Fatal("checkpointed dry-run scope allowed raw purge")
	}
	for ordinal := 1; ordinal <= len(joinedrecording.Tier1RecordingIDs); ordinal++ {
		progress, err = fixture.s.stepJoinedTier1DryRun(ctx, joinedTier1DryRunStepRequest{
			RunID: progress.RunID, PriorityOrdinal: ordinal,
		})
		if err != nil {
			t.Fatalf("step %d: %v", ordinal, err)
		}
	}
	if progress.State != "ready" || progress.RequestSHA256 == nil || *progress.RequestSHA256 != fixture.plan.RequestSHA256 {
		got := ""
		if progress.RequestSHA256 != nil {
			got = *progress.RequestSHA256
		}
		var finalBytes []byte
		var finalPlan joinedTier1FreezePlan
		if err := fixture.pool.QueryRow(ctx, `SELECT final_plan_bytes FROM recording_joined_dry_runs WHERE id=$1`, progress.RunID).Scan(&finalBytes); err == nil {
			_ = json.Unmarshal(finalBytes, &finalPlan)
		}
		t.Fatalf("ready state=%s completed=%d sha=%s want_sha=%s denominator=%s/%s exclusions=%s/%s rec-exclusions=%s/%s counts=%d,%d,%d/%d,%d,%d",
			progress.State, progress.CompletedRecordings, got, fixture.plan.RequestSHA256,
			finalPlan.FrozenDenominatorSHA256, fixture.plan.FrozenDenominatorSHA256,
			finalPlan.FreezeExclusionsSHA256, fixture.plan.FreezeExclusionsSHA256,
			finalPlan.Recordings[0].ExpectedExclusionsSHA256, fixture.plan.Recordings[0].ExpectedExclusionsSHA256,
			finalPlan.ProvisionalSourceClips, finalPlan.ProvisionalSourceBytes, finalPlan.ProvisionalExclusions,
			fixture.plan.ProvisionalSourceClips, fixture.plan.ProvisionalSourceBytes, fixture.plan.ProvisionalExclusions)
	}
	var skeletonBytes, finalBytes []byte
	if err := fixture.pool.QueryRow(ctx, `SELECT skeleton_bytes,final_plan_bytes FROM recording_joined_dry_runs WHERE id=$1`, progress.RunID).
		Scan(&skeletonBytes, &finalBytes); err != nil {
		t.Fatal(err)
	}
	var tamperSkeleton, tamperFinal joinedTier1FreezePlan
	if err := json.Unmarshal(skeletonBytes, &tamperSkeleton); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(finalBytes, &tamperFinal); err != nil {
		t.Fatal(err)
	}
	tamperReq := req
	tamperReq.Generation = 3
	tamperReq.BatchID = strings.TrimSuffix(req.BatchID, "-generation-1") + "-generation-3"
	tamperSkeleton.Generation, tamperSkeleton.BatchID = tamperReq.Generation, tamperReq.BatchID
	tamperFinal.Generation, tamperFinal.BatchID = tamperReq.Generation, tamperReq.BatchID
	tamperFinal.PolicyVersion += "-tampered"
	tamperInputBytes, _ := json.Marshal(tamperReq)
	tamperSkeletonBytes, _ := json.Marshal(tamperSkeleton)
	_, tamperFinalBytes, err := sealJoinedTier1FreezePlan(tamperFinal)
	if err != nil {
		t.Fatal(err)
	}
	var tamperRunID string
	if err := fixture.pool.QueryRow(ctx, `INSERT INTO recording_joined_dry_runs(account_id,connection_id,batch_id,generation,
		qualification_run_id,input_bytes,input_sha256,skeleton_bytes,skeleton_sha256)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id::text`, fixture.accountID, fixture.connectionID,
		tamperReq.BatchID, tamperReq.Generation, fixture.runID, tamperInputBytes, sha256Bytes(tamperInputBytes),
		tamperSkeletonBytes, sha256Bytes(tamperSkeletonBytes)).Scan(&tamperRunID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO recording_joined_dry_run_scopes SELECT $1::uuid,recording_id,
		priority_ordinal,local_date,date_ordinal,recording_job_id,high_water_clip_id,clock_timestamp()
		FROM recording_joined_dry_run_scopes WHERE dry_run_id=$2`, tamperRunID, progress.RunID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO recording_joined_dry_run_recordings SELECT $1::uuid,priority_ordinal,
		recording_id,evidence_bytes,evidence_sha256,source_clips,source_bytes,exclusions,exclusions_sha256,clock_timestamp()
		FROM recording_joined_dry_run_recordings WHERE dry_run_id=$2`, tamperRunID, progress.RunID); err != nil {
		t.Fatal(err)
	}
	for completed := 1; completed <= 32; completed++ {
		if _, err := fixture.pool.Exec(ctx, `UPDATE recording_joined_dry_runs SET completed_recordings=$2 WHERE id=$1`, tamperRunID, completed); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE recording_joined_dry_runs SET state='ready',completed_recordings=33,
		final_plan_bytes=$2,final_plan_sha256=$3,ready_at=clock_timestamp() WHERE id=$1`, tamperRunID, tamperFinalBytes,
		sha256Bytes(tamperFinalBytes)); err == nil {
		t.Fatal("top-level final plan authority tamper reached ready")
	}
	// Replay is read-only and retains the same canonical result.
	replayed, err := fixture.s.stepJoinedTier1DryRun(ctx, joinedTier1DryRunStepRequest{RunID: progress.RunID, PriorityOrdinal: 33})
	if err != nil || replayed.RequestSHA256 == nil || *replayed.RequestSHA256 != *progress.RequestSHA256 {
		t.Fatalf("ready replay=%+v err=%v", replayed, err)
	}
	apply := req
	apply.Apply, apply.ExpectedRequestSHA256 = true, *progress.RequestSHA256
	response, _ := fixture.call(apply)
	if response.Code != http.StatusOK {
		t.Fatalf("checkpointed apply status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestJoinedTier1FreezeChunkedSnapshotsPreserveCanonicalPlan(t *testing.T) {
	fixture := newJoinedHistoricalTier1Fixture(t, "joined-freeze-chunk-parity@example.test")
	defer fixture.cleanup()

	var chunks int
	fixture.s.joinedFreezeChunkHook = func(ctx context.Context, priority int) error {
		chunks++
		if priority != chunks {
			t.Fatalf("chunk priority=%d want=%d", priority, chunks)
		}
		return nil
	}
	req := fixture.req
	req.Apply = true
	req.ExpectedRequestSHA256 = fixture.plan.RequestSHA256
	response, applied := fixture.call(req)
	if response.Code != http.StatusOK {
		t.Fatalf("chunked apply status=%d body=%s", response.Code, response.Body.String())
	}
	if applied.RequestSHA256 != fixture.plan.RequestSHA256 ||
		applied.FrozenDenominatorSHA256 != fixture.plan.FrozenDenominatorSHA256 ||
		applied.FreezeExclusionsSHA256 != fixture.plan.FreezeExclusionsSHA256 {
		t.Fatalf("chunked apply changed canonical plan: applied=%+v dry=%+v", applied, fixture.plan)
	}
	for call := 2; call <= len(joinedrecording.Tier1RecordingIDs)+1; call++ {
		response, applied = fixture.call(req)
		if response.Code != http.StatusOK || applied.RequestSHA256 != fixture.plan.RequestSHA256 {
			t.Fatalf("resumable apply call=%d status=%d body=%s", call, response.Code, response.Body.String())
		}
	}
	if chunks != 33 {
		t.Fatalf("chunks=%d want=33", chunks)
	}
	var state string
	var recordings, days, snapshots int
	if err := fixture.pool.QueryRow(context.Background(), `SELECT b.state,
		(SELECT count(*) FROM recording_joined_batch_recordings WHERE batch_record_id=b.id),
		(SELECT count(*) FROM recording_joined_stream_days WHERE batch_record_id=b.id),
		(SELECT count(*) FROM recording_joined_source_snapshots WHERE batch_record_id=b.id)
		FROM recording_joined_batches b WHERE b.batch_id=$1`, req.BatchID).
		Scan(&state, &recordings, &days, &snapshots); err != nil || state != "building" ||
		recordings != 33 || days != 462 || snapshots != 1 {
		t.Fatalf("committed state=%s recordings=%d days=%d snapshots=%d err=%v",
			state, recordings, days, snapshots, err)
	}
}

func TestJoinedTier1FreezeChunkCancellationPreservesEarlierReceipts(t *testing.T) {
	fixture := newJoinedHistoricalTier1Fixture(t, "joined-freeze-chunk-cancel@example.test")
	defer fixture.cleanup()
	req := fixture.req
	req.Apply = true
	req.ExpectedRequestSHA256 = fixture.plan.RequestSHA256
	chunks := 0
	fixture.s.joinedFreezeChunkHook = func(context.Context, int) error { chunks++; return nil }
	for call := 1; call <= 16; call++ {
		if response, _ := fixture.call(req); response.Code != http.StatusOK {
			t.Fatalf("apply call=%d status=%d body=%s", call, response.Code, response.Body.String())
		}
	}
	statusRequest := httptest.NewRequest(http.MethodGet,
		"/api/v1/recording/joined/batches/status?batch_id="+req.BatchID, nil)
	statusRequest.AddCookie(&http.Cookie{Name: accountSessionCookie, Value: fixture.sessionToken})
	statusResponse := httptest.NewRecorder()
	fixture.s.router().ServeHTTP(statusResponse, statusRequest)
	if statusResponse.Code != http.StatusNotFound {
		t.Fatalf("partial snapshot leaked through status: status=%d body=%s", statusResponse.Code, statusResponse.Body.String())
	}
	callCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fixture.s.joinedFreezeChunkHook = func(ctx context.Context, priority int) error {
		chunks++
		if priority == 17 {
			cancel()
		}
		return ctx.Err()
	}
	response, _ := fixture.callWithContext(callCtx, req)
	if response.Code != http.StatusConflict || chunks != 17 {
		t.Fatalf("canceled chunk status=%d chunks=%d body=%s", response.Code, chunks, response.Body.String())
	}
	var batches, recordings, scopes, receipts, days, snapshots, exclusions int
	if err := fixture.pool.QueryRow(context.Background(), `SELECT
		(SELECT count(*) FROM recording_joined_batches WHERE batch_id=$1),
		(SELECT count(*) FROM recording_joined_batch_recordings WHERE batch_id=$1),
		(SELECT count(*) FROM recording_joined_snapshot_scopes s JOIN recording_joined_batches b ON b.id=s.batch_record_id WHERE b.batch_id=$1),
		(SELECT count(*) FROM recording_joined_snapshot_chunks c JOIN recording_joined_batches b ON b.id=c.batch_record_id WHERE b.batch_id=$1),
		(SELECT count(*) FROM recording_joined_stream_days WHERE batch_id=$1),
		(SELECT count(*) FROM recording_joined_source_snapshots s JOIN recording_joined_batches b ON b.id=s.batch_record_id WHERE b.batch_id=$1),
		(SELECT count(*) FROM recording_joined_freeze_exclusions e JOIN recording_joined_batches b ON b.id=e.batch_record_id WHERE b.batch_id=$1)`, req.BatchID).
		Scan(&batches, &recordings, &scopes, &receipts, &days, &snapshots, &exclusions); err != nil {
		t.Fatal(err)
	}
	if batches != 1 || recordings != 33 || scopes != 462 || receipts != 16 || days != 224 || snapshots != 1 || exclusions != 1 {
		t.Fatalf("canceled progress batches=%d recordings=%d scopes=%d receipts=%d days=%d snapshots=%d exclusions=%d",
			batches, recordings, scopes, receipts, days, snapshots, exclusions)
	}
	fixture.s.joinedFreezeChunkHook = nil
	for call := 17; call <= 34; call++ {
		if response, _ := fixture.call(req); response.Code != http.StatusOK {
			t.Fatalf("resume call=%d status=%d body=%s", call, response.Code, response.Body.String())
		}
	}
	var state string
	if err := fixture.pool.QueryRow(context.Background(), `SELECT state FROM recording_joined_batches WHERE batch_id=$1`, req.BatchID).Scan(&state); err != nil || state != "building" {
		t.Fatalf("resumed state=%q err=%v", state, err)
	}
}

func TestJoinedTier1FreezeWatermarkExcludesLateClipAndReleaseIsInformational(t *testing.T) {
	fixture := newJoinedHistoricalTier1Fixture(t, "joined-freeze-watermark@example.test")
	defer fixture.cleanup()
	req := fixture.req
	req.Apply = true
	req.ExpectedRequestSHA256 = fixture.plan.RequestSHA256
	if response, _ := fixture.call(req); response.Code != http.StatusOK {
		t.Fatalf("initial snapshot status=%d body=%s", response.Code, response.Body.String())
	}
	var capturedRelease time.Time
	if err := fixture.pool.QueryRow(context.Background(), `SELECT released_at FROM recording_joined_source_snapshots WHERE clip_id=$1`, fixture.clipID).Scan(&capturedRelease); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(context.Background(), `UPDATE recording_clips SET size_bytes=size_bytes+1 WHERE id=$1`, fixture.clipID); err == nil {
		t.Fatal("denominator-defining update changed a scoped clip")
	}
	if _, err := fixture.pool.Exec(context.Background(), `UPDATE recording_clips SET released_at=released_at+interval '1 hour' WHERE id=$1`, fixture.clipID); err != nil {
		t.Fatalf("released_at bookkeeping was fenced: %v", err)
	}
	var recordingID, jobID int64
	var start time.Time
	if err := fixture.pool.QueryRow(context.Background(), `SELECT scope.recording_id,scope.recording_job_id,
		scope.scheduled_start_at FROM recording_joined_snapshot_scopes scope
		JOIN recording_joined_batch_recordings br ON br.id=scope.batch_recording_id
		WHERE scope.priority_ordinal=2 AND scope.date_ordinal=1`).Scan(&recordingID, &jobID, &start); err != nil {
		t.Fatal(err)
	}
	var lateClipID int64
	if err := fixture.pool.QueryRow(context.Background(), `INSERT INTO recording_clips(recording_id,recording_job_id,
		storage_destination_id,endpoint,bucket,object_key,display_path,mime_type,container,size_bytes,etag,sha256,duration_ms,
		video_codec,audio_present,fire_at,clip_start_at,clip_end_at,created_at) VALUES($1,$2,$3,$4,'clips','raw/late-watermark.mp4',
		'raw/late-watermark.mp4','video/mp4','mp4',12,'etag-late-watermark',$5,60000,'h264',false,$6,$6,$7,$6)
		RETURNING id`, recordingID, jobID, fixture.storageID, joinedTestSourceEndpoint, strings.Repeat("c", 64), start,
		start.Add(time.Minute)).Scan(&lateClipID); err != nil {
		t.Fatal(err)
	}
	finishJoinedTier1Fixture(t, fixture, req)
	var lateEvidence int
	var storedRelease, rawRelease time.Time
	if err := fixture.pool.QueryRow(context.Background(), `SELECT
		(SELECT count(*) FROM recording_joined_source_snapshots WHERE clip_id=$1)+
		(SELECT count(*) FROM recording_joined_freeze_exclusions WHERE clip_id=$1),
		(SELECT released_at FROM recording_joined_source_snapshots WHERE clip_id=$2),
		(SELECT released_at FROM recording_clips WHERE id=$2)`, lateClipID, fixture.clipID).
		Scan(&lateEvidence, &storedRelease, &rawRelease); err != nil {
		t.Fatal(err)
	}
	if lateEvidence != 0 || !storedRelease.Equal(capturedRelease) || rawRelease.Equal(storedRelease) {
		t.Fatalf("late evidence=%d stored_release=%v captured=%v raw_release=%v", lateEvidence, storedRelease, capturedRelease, rawRelease)
	}
}

func TestJoinedTier1FreezeLinearizesTerminalInsertAboveWatermark(t *testing.T) {
	fixture := newJoinedHistoricalTier1Fixture(t, "joined-freeze-insert-fence@example.test")
	defer fixture.cleanup()
	req := fixture.req
	req.Apply = true
	req.ExpectedRequestSHA256 = fixture.plan.RequestSHA256
	started, release := make(chan struct{}), make(chan struct{})
	fixture.s.joinedFreezeChunkHook = func(context.Context, int) error { close(started); <-release; return nil }
	applyResult := make(chan *httptest.ResponseRecorder, 1)
	go func() { response, _ := fixture.call(req); applyResult <- response }()
	<-started
	recording := fixture.plan.Recordings[1]
	day := recording.Qualification.Days[0]
	lateResult := make(chan struct {
		id  int64
		err error
	}, 1)
	go func() {
		var id int64
		err := fixture.pool.QueryRow(context.Background(), `INSERT INTO recording_clips(recording_id,recording_job_id,
			storage_destination_id,endpoint,bucket,object_key,display_path,mime_type,container,size_bytes,etag,sha256,duration_ms,
			video_codec,audio_present,fire_at,clip_start_at,clip_end_at,created_at) VALUES($1,$2,$3,$4,'clips','raw/concurrent-late.mp4',
			'raw/concurrent-late.mp4','video/mp4','mp4',12,'etag-concurrent-late',$5,60000,'h264',false,$6,$6,$7,$6)
			RETURNING id`, recording.Frozen.RecordingID, day.JobID, fixture.storageID, joinedTestSourceEndpoint,
			strings.Repeat("d", 64), day.WindowStart, day.WindowStart.Add(time.Minute)).Scan(&id)
		lateResult <- struct {
			id  int64
			err error
		}{id, err}
	}()
	waitJoinedDatabaseCondition(t, fixture.pool, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity
		WHERE pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE 'INSERT INTO recording_clips%concurrent-late%')`)
	select {
	case result := <-lateResult:
		close(release)
		t.Fatalf("terminal insert crossed active freeze fence: id=%d err=%v", result.id, result.err)
	default:
	}
	close(release)
	if response := <-applyResult; response.Code != http.StatusOK {
		t.Fatalf("apply status=%d body=%s", response.Code, response.Body.String())
	}
	late := <-lateResult
	if late.err != nil || late.id <= recording.SnapshotDays[0].HighWaterClipID {
		t.Fatalf("late insert id=%d watermark=%d err=%v", late.id, recording.SnapshotDays[0].HighWaterClipID, late.err)
	}
	fixture.s.joinedFreezeChunkHook = nil
	finishJoinedTier1Fixture(t, fixture, req)
	var evidence int
	if err := fixture.pool.QueryRow(context.Background(), `SELECT
		(SELECT count(*) FROM recording_joined_source_snapshots WHERE clip_id=$1)+
		(SELECT count(*) FROM recording_joined_freeze_exclusions WHERE clip_id=$1)`, late.id).Scan(&evidence); err != nil || evidence != 0 {
		t.Fatalf("late evidence=%d err=%v", evidence, err)
	}
}

func TestJoinedTier1FreezeConcurrentCallsAdvanceDistinctOrderedChunks(t *testing.T) {
	fixture := newJoinedHistoricalTier1Fixture(t, "joined-freeze-concurrent@example.test")
	defer fixture.cleanup()
	req := fixture.req
	req.Apply = true
	req.ExpectedRequestSHA256 = fixture.plan.RequestSHA256
	if response, _ := fixture.call(req); response.Code != http.StatusOK {
		t.Fatalf("initial status=%d body=%s", response.Code, response.Body.String())
	}
	started, release := make(chan struct{}), make(chan struct{})
	priorities := make(chan int, 2)
	fixture.s.joinedFreezeChunkHook = func(_ context.Context, priority int) error {
		priorities <- priority
		if priority == 2 {
			close(started)
			<-release
		}
		return nil
	}
	results := make(chan *httptest.ResponseRecorder, 2)
	go func() { response, _ := fixture.call(req); results <- response }()
	<-started
	go func() { response, _ := fixture.call(req); results <- response }()
	waitJoinedDatabaseCondition(t, fixture.pool, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity
		WHERE pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE '%pg_advisory_xact_lock(137,1)%')`)
	select {
	case response := <-results:
		close(release)
		t.Fatalf("concurrent call escaped serialization: %d %s", response.Code, response.Body.String())
	default:
	}
	close(release)
	for i := 0; i < 2; i++ {
		if response := <-results; response.Code != http.StatusOK {
			t.Fatalf("concurrent status=%d body=%s", response.Code, response.Body.String())
		}
	}
	first, second := <-priorities, <-priorities
	if first != 2 || second != 3 {
		t.Fatalf("priorities=(%d,%d) want (2,3)", first, second)
	}
	var receipts, days int
	if err := fixture.pool.QueryRow(context.Background(), `SELECT
		(SELECT count(*) FROM recording_joined_snapshot_chunks),(SELECT count(*) FROM recording_joined_stream_days)`).Scan(&receipts, &days); err != nil || receipts != 3 || days != 42 {
		t.Fatalf("receipts=%d days=%d err=%v", receipts, days, err)
	}
}

func TestJoinedTier1ResumableMigrationRejectsExistingV1Batch(t *testing.T) {
	fixture := newJoinedHistoricalTier1Fixture(t, "joined-freeze-migration-gate@example.test")
	defer fixture.cleanup()
	req := fixture.req
	req.Apply = true
	req.ExpectedRequestSHA256 = fixture.plan.RequestSHA256
	finishJoinedTier1Fixture(t, fixture, req)
	if _, err := fixture.pool.Exec(context.Background(), `DELETE FROM schema_migrations WHERE version='0140_joined_tier1_resumable_freeze.sql'`); err != nil {
		t.Fatal(err)
	}
	err := db.MigrateUp(context.Background(), fixture.pool, findMigrationsDir(t))
	if err == nil || !strings.Contains(err.Error(), "cannot install denominator v2 while a v1 joined batch exists") {
		t.Fatalf("migration accepted existing joined batch: %v", err)
	}
}

func TestJoinedTier1ChunkReceiptRejectsEqualCountByteSourceTamper(t *testing.T) {
	fixture := newJoinedHistoricalTier1Fixture(t, "joined-freeze-receipt-tamper@example.test")
	defer fixture.cleanup()
	req := fixture.req
	req.Apply = true
	req.ExpectedRequestSHA256 = fixture.plan.RequestSHA256
	if response, _ := fixture.call(req); response.Code != http.StatusOK {
		t.Fatalf("initial status=%d body=%s", response.Code, response.Body.String())
	}
	tx, err := fixture.pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	statements := []string{
		`CREATE TEMP TABLE saved_joined_receipt ON COMMIT DROP AS SELECT * FROM recording_joined_snapshot_chunks WHERE priority_ordinal=1`,
		`ALTER TABLE recording_joined_snapshot_chunks DISABLE TRIGGER USER`,
		`DELETE FROM recording_joined_snapshot_chunks WHERE priority_ordinal=1`,
		`ALTER TABLE recording_joined_snapshot_chunks ENABLE TRIGGER USER`,
		`ALTER TABLE recording_joined_source_snapshots DISABLE TRIGGER USER`,
		`UPDATE recording_joined_source_snapshots SET sha256=repeat('e',64) WHERE clip_id=` + fmt.Sprint(fixture.clipID),
		`ALTER TABLE recording_joined_source_snapshots ENABLE TRIGGER USER`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(context.Background(), statement); err != nil {
			t.Fatalf("prepare tamper: %v", err)
		}
	}
	_, err = tx.Exec(context.Background(), `INSERT INTO recording_joined_snapshot_chunks(batch_record_id,batch_recording_id,
		priority_ordinal,recording_id,expected_source_clips,expected_source_bytes,expected_exclusions,
		expected_exclusions_sha256,actual_source_clips,actual_source_bytes,actual_exclusions,
		actual_exclusions_sha256,receipt_sha256,completed_at)
		SELECT batch_record_id,batch_recording_id,priority_ordinal,recording_id,expected_source_clips,
		expected_source_bytes,expected_exclusions,expected_exclusions_sha256,actual_source_clips,actual_source_bytes,
		actual_exclusions,actual_exclusions_sha256,receipt_sha256,completed_at FROM saved_joined_receipt`)
	if err == nil || !strings.Contains(err.Error(), "joined snapshot chunk receipt differs") {
		t.Fatalf("receipt accepted equal-count/equal-byte source tamper: %v", err)
	}
}

func TestJoinedHistoricalActivationLocksRawFacts(t *testing.T) {
	fixture := newJoinedHistoricalTier1Fixture(t, "joined-historical-lock@example.test")
	defer fixture.cleanup()
	ctx := context.Background()
	var evidenceMismatches int
	if err := fixture.pool.QueryRow(ctx, `SELECT count(*) FROM recording_qualification_runs q
		CROSS JOIN LATERAL jsonb_array_elements(q.definition_jsonb->'canonical_plan'->'members') member
		WHERE q.id=$1 AND recording_historical_qualification_evidence_sha256(member->'qualification')
			IS DISTINCT FROM member->'qualification'->>'evidence_sha256'`, fixture.runID).Scan(&evidenceMismatches); err != nil || evidenceMismatches != 0 {
		t.Fatalf("database qualification wire hash parity mismatches=%d err=%v", evidenceMismatches, err)
	}
	escapingFixture := "<>&\u2028\u2029\n\t\"\\"
	escapingJSON, _ := json.Marshal(escapingFixture)
	var databaseEscaping string
	if err := fixture.pool.QueryRow(ctx, `SELECT recording_historical_go_string_json($1)`, escapingFixture).
		Scan(&databaseEscaping); err != nil || databaseEscaping != string(escapingJSON) {
		t.Fatalf("database Go string escaping parity got=%q want=%q err=%v", databaseEscaping, escapingJSON, err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE recording_jobs j SET scheduled_for=
		(q.definition_jsonb->'canonical_plan'->'members'->0->'qualification'->'days'->0->>'scheduled_for')::timestamptz
		FROM recording_qualification_runs q WHERE q.id=$1 AND j.id=$2`, fixture.runID, fixture.firstJobID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE recording_qualification_runs SET status='canceled' WHERE id=$1`, fixture.runID); err != nil {
		t.Fatal(err)
	}
	var candidateID int64
	if err := fixture.pool.QueryRow(ctx, `INSERT INTO recording_qualification_runs(account_id,definition_version,
		definition_jsonb,target_recording_count,target_window_count,required_good_or_great,max_acceptable,window_sequence_start_at)
		SELECT account_id,definition_version,definition_jsonb,target_recording_count,target_window_count,
		required_good_or_great,max_acceptable,window_sequence_start_at FROM recording_qualification_runs WHERE id=$1 RETURNING id`,
		fixture.runID).Scan(&candidateID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO recording_qualification_members(run_id,account_id,recording_id,
		ordinal,stream_id,recording_name,stream_name,scene_identity_sha256,scene_frame_evidence_id,cron_timezone,
		daily_window_start,daily_window_end,active_weekdays,schedule_start_at,schedule_end_at,window_generator_version)
		SELECT $2,account_id,recording_id,ordinal,stream_id,recording_name,stream_name,scene_identity_sha256,
		scene_frame_evidence_id,cron_timezone,daily_window_start,daily_window_end,active_weekdays,schedule_start_at,
		schedule_end_at,window_generator_version FROM recording_qualification_members WHERE run_id=$1`,
		fixture.runID, candidateID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO recording_qualification_windows(run_id,recording_id,ordinal,
		local_open_at,local_end_at,open_utc_offset_seconds,end_utc_offset_seconds,window_start_at,window_end_at,expected_seconds)
		SELECT $2,recording_id,ordinal,local_open_at,local_end_at,open_utc_offset_seconds,end_utc_offset_seconds,
		window_start_at,window_end_at,expected_seconds FROM recording_qualification_windows WHERE run_id=$1`,
		fixture.runID, candidateID); err != nil {
		t.Fatal(err)
	}
	mutation, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mutation.Exec(ctx, `UPDATE recording_jobs SET scheduled_for=scheduled_for+interval '2 minutes' WHERE id=$1`,
		fixture.firstJobID); err != nil {
		_ = mutation.Rollback(ctx)
		t.Fatal(err)
	}
	activation := make(chan error, 1)
	go func() {
		_, activateErr := fixture.pool.Exec(ctx, `UPDATE recording_qualification_runs SET status='active' WHERE id=$1`, candidateID)
		activation <- activateErr
	}()
	waitJoinedDatabaseCondition(t, fixture.pool, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity
		WHERE pid<>pg_backend_pid() AND wait_event_type='Lock'
		AND query LIKE 'UPDATE recording_qualification_runs SET status=''active''%')`)
	if err := mutation.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-activation; err == nil {
		t.Fatal("historical activation accepted raw facts changed while waiting for its share lock")
	}
	var status string
	if err := fixture.pool.QueryRow(ctx, `SELECT status FROM recording_qualification_runs WHERE id=$1`, candidateID).Scan(&status); err != nil || status != "building" {
		t.Fatalf("failed historical activation status=%s err=%v", status, err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE recording_jobs j SET scheduled_for=
		(q.definition_jsonb->'canonical_plan'->'members'->0->'qualification'->'days'->0->>'scheduled_for')::timestamptz
		FROM recording_qualification_runs q WHERE q.id=$1 AND j.id=$2`, fixture.runID, fixture.firstJobID); err != nil {
		t.Fatal(err)
	}
	var forgedID int64
	if err := fixture.pool.QueryRow(ctx, `WITH source AS (
		SELECT *,jsonb_build_object('qualification_jobs_sha256',definition_jsonb->>'qualification_jobs_sha256') AS minimal
		FROM recording_qualification_runs WHERE id=$1), forged AS (
		SELECT *,encode(sha256(convert_to(minimal::text,'UTF8')),'hex') AS forged_sha FROM source)
		INSERT INTO recording_qualification_runs(account_id,definition_version,definition_jsonb,target_recording_count,
		target_window_count,required_good_or_great,max_acceptable,window_sequence_start_at)
		SELECT account_id,definition_version,definition_jsonb||jsonb_build_object(
			'request_canonical',minimal::text,'request_sha256',forged_sha,
			'canonical_plan',minimal||jsonb_build_object('request_sha256',forged_sha)),
			target_recording_count,target_window_count,required_good_or_great,max_acceptable,window_sequence_start_at
		FROM forged RETURNING id`, fixture.runID).Scan(&forgedID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO recording_qualification_members(run_id,account_id,recording_id,
		ordinal,stream_id,recording_name,stream_name,scene_identity_sha256,scene_frame_evidence_id,cron_timezone,
		daily_window_start,daily_window_end,active_weekdays,schedule_start_at,schedule_end_at,window_generator_version)
		SELECT $2,account_id,recording_id,ordinal,stream_id,recording_name,stream_name,scene_identity_sha256,
		scene_frame_evidence_id,cron_timezone,daily_window_start,daily_window_end,active_weekdays,schedule_start_at,
		schedule_end_at,window_generator_version FROM recording_qualification_members WHERE run_id=$1;
		`, fixture.runID, forgedID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO recording_qualification_windows(run_id,recording_id,ordinal,local_open_at,local_end_at,
		open_utc_offset_seconds,end_utc_offset_seconds,window_start_at,window_end_at,expected_seconds)
		SELECT $2,recording_id,ordinal,local_open_at,local_end_at,open_utc_offset_seconds,end_utc_offset_seconds,
		window_start_at,window_end_at,expected_seconds FROM recording_qualification_windows WHERE run_id=$1`,
		fixture.runID, forgedID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE recording_qualification_runs SET status='active' WHERE id=$1`, forgedID); err == nil {
		t.Fatal("self-hashed historical authority without exact member/day facts activated")
	}
	var definitionJSON, planJSON []byte
	if err := fixture.pool.QueryRow(ctx, `SELECT definition_jsonb,definition_jsonb->'canonical_plan'
		FROM recording_qualification_runs WHERE id=$1`, fixture.runID).Scan(&definitionJSON, &planJSON); err != nil {
		t.Fatal(err)
	}
	var definition map[string]any
	var evidenceForgedPlan joinedHistoricalQualificationPlan
	if err := json.Unmarshal(definitionJSON, &definition); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(planJSON, &evidenceForgedPlan); err != nil {
		t.Fatal(err)
	}
	evidenceForgedPlan.Members[0].Qualification.EvidenceSHA = strings.Repeat("f", 64)
	evidenceForgedPlan.RequestSHA256 = sha256Hex(joinedHistoricalQualificationApprovalBytes(evidenceForgedPlan))
	definition["canonical_plan"] = evidenceForgedPlan
	definition["request_sha256"] = evidenceForgedPlan.RequestSHA256
	definition["request_canonical"] = string(joinedHistoricalQualificationApprovalBytes(evidenceForgedPlan))
	var evidenceForgedID int64
	if err := fixture.pool.QueryRow(ctx, `INSERT INTO recording_qualification_runs(account_id,definition_version,
		definition_jsonb,target_recording_count,target_window_count,required_good_or_great,max_acceptable,window_sequence_start_at)
		SELECT account_id,definition_version,$2,target_recording_count,target_window_count,
		required_good_or_great,max_acceptable,window_sequence_start_at FROM recording_qualification_runs WHERE id=$1 RETURNING id`,
		fixture.runID, definition).Scan(&evidenceForgedID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO recording_qualification_members(run_id,account_id,recording_id,
		ordinal,stream_id,recording_name,stream_name,scene_identity_sha256,scene_frame_evidence_id,cron_timezone,
		daily_window_start,daily_window_end,active_weekdays,schedule_start_at,schedule_end_at,window_generator_version)
		SELECT $2,account_id,recording_id,ordinal,stream_id,recording_name,stream_name,scene_identity_sha256,
		scene_frame_evidence_id,cron_timezone,daily_window_start,daily_window_end,active_weekdays,schedule_start_at,
		schedule_end_at,window_generator_version FROM recording_qualification_members WHERE run_id=$1`,
		fixture.runID, evidenceForgedID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO recording_qualification_windows(run_id,recording_id,ordinal,
		local_open_at,local_end_at,open_utc_offset_seconds,end_utc_offset_seconds,window_start_at,window_end_at,expected_seconds)
		SELECT $2,recording_id,ordinal,local_open_at,local_end_at,open_utc_offset_seconds,end_utc_offset_seconds,
		window_start_at,window_end_at,expected_seconds FROM recording_qualification_windows WHERE run_id=$1`,
		fixture.runID, evidenceForgedID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE recording_qualification_runs SET status='active' WHERE id=$1`,
		evidenceForgedID); err == nil {
		t.Fatal("self-hashed historical authority with forged qualification evidence SHA activated")
	}
	decodeCanonicalDefinition := func() (map[string]any, joinedHistoricalQualificationPlan) {
		t.Helper()
		var decoded map[string]any
		decoder := json.NewDecoder(bytes.NewReader(definitionJSON))
		decoder.UseNumber()
		if err := decoder.Decode(&decoded); err != nil {
			t.Fatal(err)
		}
		var plan joinedHistoricalQualificationPlan
		if err := json.Unmarshal(planJSON, &plan); err != nil {
			t.Fatal(err)
		}
		return decoded, plan
	}
	_, basePlan := decodeCanonicalDefinition()
	baseCanonical := joinedHistoricalQualificationApprovalBytes(basePlan)
	replaceCanonical := func(canonical []byte, old, replacement string) []byte {
		t.Helper()
		if !bytes.Contains(canonical, []byte(old)) {
			t.Fatalf("canonical fixture lacks target %q", old)
		}
		return bytes.Replace(canonical, []byte(old), []byte(replacement), 1)
	}
	rehashDefinition := func(definition map[string]any, plan map[string]any, canonical []byte) {
		t.Helper()
		requestSHA := sha256Hex(canonical)
		plan["request_sha256"] = requestSHA
		definition["canonical_plan"] = plan
		definition["request_canonical"] = string(canonical)
		definition["request_sha256"] = requestSHA
	}
	for name, suffix := range map[string]string{"zero offset": "+00:00", "redundant fraction": ".000000Z"} {
		t.Run("rejects non-Go timestamp "+name, func(t *testing.T) {
			timestampDefinition, _ := decodeCanonicalDefinition()
			timestampPlan := timestampDefinition["canonical_plan"].(map[string]any)
			firstDay := timestampPlan["members"].([]any)[0].(map[string]any)["qualification"].(map[string]any)["days"].([]any)[0].(map[string]any)
			scheduled, ok := firstDay["scheduled_for"].(string)
			if !ok || !strings.HasSuffix(scheduled, "Z") {
				t.Fatal("valid historical definition lacks canonical timestamp fixture")
			}
			mutated := strings.TrimSuffix(scheduled, "Z") + suffix
			firstDay["scheduled_for"] = mutated
			scheduledJSON, _ := json.Marshal(scheduled)
			mutatedJSON, _ := json.Marshal(mutated)
			oldToken := `"scheduled_for":` + string(scheduledJSON)
			newToken := `"scheduled_for":` + string(mutatedJSON)
			canonical := replaceCanonical(baseCanonical, oldToken, newToken)
			rehashDefinition(timestampDefinition, timestampPlan, canonical)
			timestampID := cloneJoinedHistoricalQualificationRun(t, fixture, timestampDefinition)
			if _, err := fixture.pool.Exec(ctx, `UPDATE recording_qualification_runs SET status='active' WHERE id=$1`, timestampID); err == nil {
				t.Fatal("self-hashed historical authority with non-Go timestamp spelling activated")
			}
		})
	}
	for name, mutation := range map[string]struct {
		mutatePlan         func(map[string]any)
		oldToken, newToken string
	}{
		"outer plan": {
			mutatePlan: func(plan map[string]any) { plan["schema_version"] = json.Number("1.0") },
			oldToken:   `"schema_version":1`, newToken: `"schema_version":1.0`,
		},
		"member": {
			mutatePlan: func(plan map[string]any) {
				member := plan["members"].([]any)[0].(map[string]any)
				member["stream_id"] = json.Number(member["stream_id"].(json.Number).String() + ".0")
			},
			oldToken: fmt.Sprintf(`"stream_id":%d`, basePlan.Members[0].StreamID),
			newToken: fmt.Sprintf(`"stream_id":%d.0`, basePlan.Members[0].StreamID),
		},
		"day": {
			mutatePlan: func(plan map[string]any) {
				qualification := plan["members"].([]any)[0].(map[string]any)["qualification"].(map[string]any)
				qualification["days"].([]any)[0].(map[string]any)["qualification_window_ordinal"] = json.Number("1.0")
			},
			oldToken: `"qualification_window_ordinal":1`, newToken: `"qualification_window_ordinal":1.0`,
		},
	} {
		t.Run("rejects non-Go integer spelling in "+name, func(t *testing.T) {
			numericDefinition, _ := decodeCanonicalDefinition()
			numericPlan := numericDefinition["canonical_plan"].(map[string]any)
			mutation.mutatePlan(numericPlan)
			canonical := replaceCanonical(baseCanonical, mutation.oldToken, mutation.newToken)
			rehashDefinition(numericDefinition, numericPlan, canonical)
			numericID := cloneJoinedHistoricalQualificationRun(t, fixture, numericDefinition)
			if _, err := fixture.pool.Exec(ctx, `UPDATE recording_qualification_runs SET status='active' WHERE id=$1`, numericID); err == nil {
				t.Fatal("self-hashed historical authority with non-Go integer spelling activated")
			}
		})
	}
	for name, mutate := range map[string]func(string) string{
		"whitespace": func(canonical string) string { return " " + canonical },
		"integer spelling": func(canonical string) string {
			return strings.Replace(canonical, `"schema_version":1`, `"schema_version":1.0`, 1)
		},
	} {
		t.Run("rejects non-Go request canonical "+name, func(t *testing.T) {
			requestDefinition, requestPlan := decodeCanonicalDefinition()
			mutated := mutate(string(joinedHistoricalQualificationApprovalBytes(requestPlan)))
			requestSHA := sha256Hex([]byte(mutated))
			requestPlan.RequestSHA256 = requestSHA
			requestDefinition["canonical_plan"] = requestPlan
			requestDefinition["request_canonical"] = mutated
			requestDefinition["request_sha256"] = requestSHA
			requestID := cloneJoinedHistoricalQualificationRun(t, fixture, requestDefinition)
			if _, err := fixture.pool.Exec(ctx, `UPDATE recording_qualification_runs SET status='active' WHERE id=$1`, requestID); err == nil {
				t.Fatal("self-hashed historical authority with non-Go request canonical bytes activated")
			}
		})
	}
	for name, mutate := range map[string]func(string) string{
		"whitespace": func(canonical string) string { return canonical + " " },
		"integer spelling": func(canonical string) string {
			needle := fmt.Sprintf(`"recording_id":%d`, joinedrecording.Tier1RecordingIDs[0])
			return strings.Replace(canonical, needle, needle+".0", 1)
		},
	} {
		t.Run("rejects non-Go qualification jobs canonical "+name, func(t *testing.T) {
			jobsDefinition, jobsPlan := decodeCanonicalDefinition()
			canonical := jobsDefinition["qualification_jobs_canonical"].(string)
			mutated := mutate(canonical)
			jobsSHA := sha256Hex([]byte(mutated))
			jobsPlan.QualificationJobsSHA256 = jobsSHA
			requestCanonical := joinedHistoricalQualificationApprovalBytes(jobsPlan)
			requestSHA := sha256Hex(requestCanonical)
			jobsPlan.RequestSHA256 = requestSHA
			jobsDefinition["qualification_jobs_canonical"] = mutated
			jobsDefinition["qualification_jobs_sha256"] = jobsSHA
			jobsDefinition["canonical_plan"] = jobsPlan
			jobsDefinition["request_canonical"] = string(requestCanonical)
			jobsDefinition["request_sha256"] = requestSHA
			jobsID := cloneJoinedHistoricalQualificationRun(t, fixture, jobsDefinition)
			if _, err := fixture.pool.Exec(ctx, `UPDATE recording_qualification_runs SET status='active' WHERE id=$1`, jobsID); err == nil {
				t.Fatal("self-hashed historical authority with non-Go qualification jobs canonical bytes activated")
			}
		})
	}
}

func cloneJoinedHistoricalQualificationRun(t *testing.T, fixture joinedHistoricalTier1Fixture, definition any) int64 {
	t.Helper()
	ctx := context.Background()
	var runID int64
	if err := fixture.pool.QueryRow(ctx, `INSERT INTO recording_qualification_runs(account_id,definition_version,
		definition_jsonb,target_recording_count,target_window_count,required_good_or_great,max_acceptable,window_sequence_start_at)
		SELECT account_id,definition_version,$2,target_recording_count,target_window_count,
		required_good_or_great,max_acceptable,window_sequence_start_at FROM recording_qualification_runs WHERE id=$1 RETURNING id`,
		fixture.runID, definition).Scan(&runID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO recording_qualification_members(run_id,account_id,recording_id,
		ordinal,stream_id,recording_name,stream_name,scene_identity_sha256,scene_frame_evidence_id,cron_timezone,
		daily_window_start,daily_window_end,active_weekdays,schedule_start_at,schedule_end_at,window_generator_version)
		SELECT $2,account_id,recording_id,ordinal,stream_id,recording_name,stream_name,scene_identity_sha256,
		scene_frame_evidence_id,cron_timezone,daily_window_start,daily_window_end,active_weekdays,schedule_start_at,
		schedule_end_at,window_generator_version FROM recording_qualification_members WHERE run_id=$1`,
		fixture.runID, runID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO recording_qualification_windows(run_id,recording_id,ordinal,
		local_open_at,local_end_at,open_utc_offset_seconds,end_utc_offset_seconds,window_start_at,window_end_at,expected_seconds)
		SELECT $2,recording_id,ordinal,local_open_at,local_end_at,open_utc_offset_seconds,end_utc_offset_seconds,
		window_start_at,window_end_at,expected_seconds FROM recording_qualification_windows WHERE run_id=$1`,
		fixture.runID, runID); err != nil {
		t.Fatal(err)
	}
	return runID
}

func waitJoinedDatabaseCondition(t *testing.T, pool *pgxpool.Pool, query string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		var ready bool
		if err := pool.QueryRow(ctx, query).Scan(&ready); err != nil {
			t.Fatal(err)
		}
		if ready {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("database condition did not become true")
		case <-time.After(10 * time.Millisecond):
		}
	}
}
