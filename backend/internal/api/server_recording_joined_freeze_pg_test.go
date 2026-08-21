package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/db"
	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/daydemir/stoarama/backend/internal/recordingnaming"
	"github.com/daydemir/stoarama/backend/internal/secretbox"
)

const joinedMigrationName = "0137_recording_joined_outputs.sql"
const joinedReplicaRoleSetting = "session_replication_role"

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
	if _, err := pool.Exec(ctx, `INSERT INTO schema_migrations(version) VALUES($1)`, joinedMigrationName); err != nil {
		t.Fatal(err)
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
		if _, err := pool.Exec(ctx, `DELETE FROM schema_migrations WHERE version=$1`, joinedMigrationName); err != nil {
			t.Fatal(err)
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
	t.Cleanup(func() { assertJoinedReplicaRoleTestOnly(t) })
	return s, pool, applyMigration, cleanup
}

func seedJoinedHistoricalQualification(t *testing.T, pool *pgxpool.Pool, accountID int64, recordingIDs []int64) int64 {
	t.Helper()
	ctx := context.Background()
	createdAt := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	sequenceStart := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	var runID int64
	if err := pool.QueryRow(ctx, `INSERT INTO recording_qualification_runs(account_id,definition_version,definition_jsonb,
		target_recording_count,window_sequence_start_at,created_at) VALUES($1,$2,'{"scope":"timeline_and_certification"}',
		$3,$4,$5) RETURNING id`, accountID, recordingQualificationDefinition, len(recordingIDs), sequenceStart, createdAt).Scan(&runID); err != nil {
		t.Fatal(err)
	}
	for ordinal, recordingID := range recordingIDs {
		var streamID, evidenceID int64
		if err := pool.QueryRow(ctx, `SELECT r.stream_id,e.id FROM recordings r JOIN recording_scene_frame_evidence e
			ON e.account_id=r.account_id AND e.stream_id=r.stream_id WHERE r.id=$1 AND r.account_id=$2`, recordingID, accountID).
			Scan(&streamID, &evidenceID); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO recording_qualification_members(run_id,account_id,recording_id,ordinal,
			stream_id,recording_name,stream_name,scene_identity_sha256,scene_frame_evidence_id,cron_timezone,
			daily_window_start,daily_window_end,active_weekdays,schedule_start_at,window_generator_version)
			SELECT $1,$2,r.id,$3,r.stream_id,r.name,s.name,e.scene_identity_sha256,e.id,r.cron_timezone,
			r.daily_window_start,r.daily_window_end,r.active_weekdays,r.start_at,'recsched-next-full-v1'
			FROM recordings r JOIN streams s ON s.id=r.stream_id JOIN recording_scene_frame_evidence e ON e.id=$4
			WHERE r.id=$5 AND r.account_id=$2`, runID, accountID, ordinal+1, evidenceID, recordingID); err != nil {
			t.Fatal(err)
		}
		for day := 0; day < 14; day++ {
			start := sequenceStart.AddDate(0, 0, day).Add(8 * time.Hour)
			if _, err := pool.Exec(ctx, `INSERT INTO recording_qualification_windows(run_id,recording_id,ordinal,
				local_open_at,local_end_at,open_utc_offset_seconds,end_utc_offset_seconds,window_start_at,window_end_at,
				expected_seconds) VALUES($1,$2,$3,$4::timestamp,$5::timestamp,0,0,$4,$5,43200)`, runID,
				recordingID, day+1, start, start.Add(12*time.Hour)); err != nil {
				t.Fatal(err)
			}
		}
		_ = streamID
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_qualification_runs SET status='active',frozen_at=now()
		WHERE id=$1 AND status='building'`, runID); err != nil {
		t.Fatal(err)
	}
	historicalFrozenAt := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)
	correctJoinedHistoricalFrozenAt(t, pool, runID, accountID, createdAt, historicalFrozenAt)
	if _, err := pool.Exec(ctx, `UPDATE recording_qualification_members SET recording_name='forbidden' WHERE run_id=$1`, runID); err == nil {
		t.Fatal("ordinary mutation changed an active qualification member")
	}
	return runID
}

func correctJoinedHistoricalFrozenAt(t *testing.T, pool *pgxpool.Pool, runID, accountID int64, createdAt, historicalFrozenAt time.Time) {
	t.Helper()
	cfg := pool.Config()
	ip := net.ParseIP(cfg.ConnConfig.Host)
	if (cfg.ConnConfig.Host != "localhost" && (ip == nil || !ip.IsLoopback())) || !createdAt.Before(historicalFrozenAt) {
		t.Fatal("historical fixture requires a disposable loopback database and ordered timestamps")
	}
	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	discard := true
	defer func() {
		if discard {
			_ = conn.Conn().PgConn().Close(context.Background())
		} else {
			conn.Release()
		}
	}()
	var schema, sentinel, role string
	if err := conn.QueryRow(ctx, `SELECT current_schema(),value,current_setting($1) FROM joined_disposable_test_sentinel`,
		joinedReplicaRoleSetting).Scan(&schema, &sentinel, &role); err != nil || !strings.HasPrefix(schema, "joined_freeze_") || !strings.HasPrefix(sentinel, schema+":") || role != "origin" {
		t.Fatalf("disposable fixture sentinel failed schema=%q role=%q err=%v", schema, role, err)
	}
	var originalFrozen time.Time
	var cohortSHA, windowsSHA, beforeRun, beforeMembers, beforeWindows string
	var runCount, memberCount, windowCount int
	if err := conn.QueryRow(ctx, `SELECT frozen_at,cohort_sha256,windows_sha256,
		encode(sha256(convert_to((to_jsonb(q)-'frozen_at')::text,'UTF8')),'hex'),
		(SELECT encode(sha256(convert_to(jsonb_agg(to_jsonb(m) ORDER BY ordinal)::text,'UTF8')),'hex') FROM recording_qualification_members m WHERE m.run_id=q.id),
		(SELECT encode(sha256(convert_to(jsonb_agg(to_jsonb(w) ORDER BY recording_id,ordinal)::text,'UTF8')),'hex') FROM recording_qualification_windows w WHERE w.run_id=q.id),
		(SELECT count(*) FROM recording_qualification_runs),(SELECT count(*) FROM recording_qualification_members),
		(SELECT count(*) FROM recording_qualification_windows)
		FROM recording_qualification_runs q WHERE q.id=$1 AND q.account_id=$2 AND q.status='active' AND q.created_at=$3`,
		runID, accountID, createdAt).Scan(&originalFrozen, &cohortSHA, &windowsSHA, &beforeRun, &beforeMembers,
		&beforeWindows, &runCount, &memberCount, &windowCount); err != nil {
		t.Fatal(err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var firstWindowStart time.Time
	if err := conn.QueryRow(ctx, `SELECT min(window_start_at) FROM recording_qualification_windows WHERE run_id=$1`, runID).Scan(&firstWindowStart); err != nil || historicalFrozenAt.After(firstWindowStart) {
		t.Fatalf("historical frozen_at is after first window: %v %v", historicalFrozenAt, err)
	}
	setting := joinedReplicaRoleSetting
	if _, err := tx.Exec(ctx, `SET LOCAL `+setting+`=replica`); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	tag, err := tx.Exec(ctx, `UPDATE recording_qualification_runs SET frozen_at=$1 WHERE id=$2 AND account_id=$3
		AND status='active' AND created_at=$4 AND frozen_at=$5 AND cohort_sha256=$6 AND windows_sha256=$7`,
		historicalFrozenAt, runID, accountID, createdAt, originalFrozen, cohortSHA, windowsSHA)
	if err != nil || tag.RowsAffected() != 1 {
		_ = tx.Rollback(ctx)
		t.Fatalf("correct historical frozen_at rows=%d err=%v", tag.RowsAffected(), err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL `+setting+`=origin`); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.QueryRow(ctx, `SELECT current_setting($1)`, setting).Scan(&role); err != nil || role != "origin" {
		_ = tx.Rollback(ctx)
		t.Fatalf("fixture role was not restored in transaction: %q %v", role, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT current_setting($1)`, setting).Scan(&role); err != nil || role != "origin" {
		t.Fatalf("fixture role was not restored after commit: %q %v", role, err)
	}
	var afterRun, afterMembers, afterWindows string
	var afterRunCount, afterMemberCount, afterWindowCount int
	if err := conn.QueryRow(ctx, `SELECT encode(sha256(convert_to((to_jsonb(q)-'frozen_at')::text,'UTF8')),'hex'),
		(SELECT encode(sha256(convert_to(jsonb_agg(to_jsonb(m) ORDER BY ordinal)::text,'UTF8')),'hex') FROM recording_qualification_members m WHERE m.run_id=q.id),
		(SELECT encode(sha256(convert_to(jsonb_agg(to_jsonb(w) ORDER BY recording_id,ordinal)::text,'UTF8')),'hex') FROM recording_qualification_windows w WHERE w.run_id=q.id),
		(SELECT count(*) FROM recording_qualification_runs),(SELECT count(*) FROM recording_qualification_members),
		(SELECT count(*) FROM recording_qualification_windows)
		FROM recording_qualification_runs q WHERE q.id=$1 AND q.account_id=$2 AND q.status='active' AND q.created_at=$3
		AND q.frozen_at=$4 AND q.cohort_sha256=$5 AND q.windows_sha256=$6`, runID, accountID, createdAt,
		historicalFrozenAt, cohortSHA, windowsSHA).Scan(&afterRun, &afterMembers, &afterWindows, &afterRunCount,
		&afterMemberCount, &afterWindowCount); err != nil {
		t.Fatal(err)
	}
	if beforeRun != afterRun || beforeMembers != afterMembers || beforeWindows != afterWindows ||
		runCount != afterRunCount || memberCount != afterMemberCount || windowCount != afterWindowCount {
		t.Fatal("historical fixture changed evidence other than frozen_at")
	}
	discard = false
}

func assertJoinedReplicaRoleTestOnly(t *testing.T) {
	t.Helper()
	needle := joinedReplicaRoleSetting
	root := filepath.Clean(filepath.Join("..", "..", ".."))
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || strings.HasSuffix(path, "_test.go") ||
			(!strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, ".sql")) {
			return err
		}
		body, readErr := os.ReadFile(path)
		if readErr == nil && bytes.Contains(body, []byte(needle)) {
			return fmt.Errorf("replica role setting escaped test-only helper: %s", path)
		}
		return readErr
	})
	if err != nil {
		t.Fatal(err)
	}
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
	clipStart       time.Time
	sessionToken    string
	req             joinedTier1FreezeRequest
	plan            joinedTier1FreezePlan
	call            func(joinedTier1FreezeRequest) (*httptest.ResponseRecorder, joinedTier1FreezePlan)
	callWithContext func(context.Context, joinedTier1FreezeRequest) (*httptest.ResponseRecorder, joinedTier1FreezePlan)
}

func newJoinedHistoricalTier1Fixture(t *testing.T, email string) joinedHistoricalTier1Fixture {
	t.Helper()
	s, pool, applyMigration, cleanup := testJoinedServerBeforeMigration(t)
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
		access_key_id,secret_access_key_enc,status,managed) VALUES($1,'joined-source','r2',$2,'auto','clips',
		'key',$3,'verified',true) RETURNING id`, accountID, joinedTestSourceEndpoint, sourceSecret).Scan(&storageID); err != nil {
		t.Fatal(err)
	}
	allIDs := append([]int64(nil), joinedrecording.Tier1RecordingIDs...)
	for i := 0; i < 17; i++ {
		allIDs = append(allIDs, int64(10001+i))
	}
	metadata := recordingnaming.Metadata{PlazaID: "1", Continent: "Europe", Country: "Italy", City: "Bevagna", PlazaName: "Piazza"}
	metadataJSON, _ := json.Marshal(metadata)
	for ordinal, recordingID := range allIDs {
		var streamID, mediaID, frameID int64
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
			'0 8 * * *','UTC',60,'active','2026-07-01', $6,'continuous','08:00','20:00',127,'nas_pull',
			'plaza_hourly_v1',$7,$8)`, recordingID, accountID, storageID, fmt.Sprintf("recording-%d", recordingID),
			fmt.Sprintf("https://example.test/%d.m3u8", recordingID), streamID, folder, metadataJSON); err != nil {
			t.Fatal(err)
		}
		sha := fmt.Sprintf("%064x", ordinal+1)
		if err := pool.QueryRow(ctx, `INSERT INTO media_objects(storage_provider,bucket,object_key,mime_type,size_bytes,sha256)
			VALUES('r2','frames',$1,'image/jpeg',1,$2) RETURNING id`, fmt.Sprintf("frame-%d", recordingID), sha).Scan(&mediaID); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(ctx, `INSERT INTO frames(stream_id,captured_at,raw_media_object_id,capture_status,source_kind)
			VALUES($1,now()-interval '1 hour',$2,'success','live') RETURNING id`, streamID, mediaID).Scan(&frameID); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO recording_scene_frame_evidence(account_id,stream_id,frame_id,media_object_id,
			captured_at,frame_sha256,scene_identity_sha256,verification_method,verified_by_user_id,verified_at)
			SELECT $1,$2,$3,$4,f.captured_at,$5,$6,'operator_visual',$7,now() FROM frames f WHERE f.id=$3`, accountID, streamID,
			frameID, mediaID, sha, fmt.Sprintf("%064x", ordinal+1000), userID); err != nil {
			t.Fatal(err)
		}
	}
	runID := seedJoinedHistoricalQualification(t, pool, accountID, allIDs)
	var apiKeyID, connectionID int64
	if err := pool.QueryRow(ctx, `INSERT INTO account_api_keys(account_id,key_prefix,secret_hash,label,scopes)
		VALUES($1,'sir_freeze','freeze-key','NAS',ARRAY['stoarama.pull']) RETURNING id`, accountID).Scan(&apiKeyID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO connections(account_id,kind,label,api_key_id)
		VALUES($1,'nas_pull','NAS',$2) RETURNING id`, accountID, apiKeyID).Scan(&connectionID); err != nil {
		t.Fatal(err)
	}
	var firstJobID int64
	for _, recordingID := range joinedrecording.Tier1RecordingIDs {
		for day := 0; day < 14; day++ {
			start := time.Date(2026, 8, 1+day, 8, 0, 0, 0, time.UTC)
			var jobID int64
			if err := pool.QueryRow(ctx, `INSERT INTO recording_jobs(recording_id,fire_at,scheduled_for,clip_duration_sec,
				status,idempotency_key,kind,window_end_at,completed_at) VALUES($1,$2,$2,60,'done',$3,'continuous_window',$4,$5)
				RETURNING id`, recordingID, start, fmt.Sprintf("joined-freeze:%d:%d", recordingID, day),
				start.Add(12*time.Hour), start.Add(12*time.Hour+time.Minute)).Scan(&jobID); err != nil {
				t.Fatal(err)
			}
			if firstJobID == 0 {
				firstJobID = jobID
			}
		}
	}
	var clipID int64
	clipStart := time.Date(2026, 8, 1, 8, 0, 0, 0, time.UTC)
	if err := pool.QueryRow(ctx, `INSERT INTO recording_clips(recording_id,recording_job_id,storage_destination_id,
		endpoint,bucket,object_key,display_path,mime_type,container,size_bytes,etag,sha256,duration_ms,video_codec,
		audio_present,fire_at,clip_start_at,clip_end_at,created_at,released_at) VALUES($1,$2,$3,$4,'clips','raw/one.mp4',
		'raw/one.mp4','video/mp4','mp4',10,'etag-one',$5,60000,'h264',false,$6,$6,$7,$6,$7) RETURNING id`,
		joinedrecording.Tier1RecordingIDs[0], firstJobID, storageID, joinedTestSourceEndpoint, strings.Repeat("a", 64),
		clipStart, clipStart.Add(time.Minute)).Scan(&clipID); err != nil {
		t.Fatal(err)
	}
	var lateCompletions int
	cutoff, _ := time.Parse(time.RFC3339Nano, joinedrecording.Tier1FrozenAt)
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM recording_jobs WHERE recording_id=ANY($1::bigint[])
		AND completed_at>$2`, joinedrecording.Tier1RecordingIDs, cutoff).Scan(&lateCompletions); err != nil || lateCompletions != 0 {
		t.Fatalf("late completions=%d err=%v", lateCompletions, err)
	}
	applyMigration()
	if _, err := pool.Exec(ctx, `UPDATE connections SET joined_protocol_version=1 WHERE id=$1`, connectionID); err != nil {
		t.Fatal(err)
	}
	s.cfg.JoinedRecordingControlPlaneEnabled = true
	s.cfg.JoinedRecordingProtocolVersion = 1
	s.cfg.JoinedWorkerBootstrapToken = "joined-bootstrap-credential-32bytes"
	s.cfg.JoinedWorkerSigningKey = "joined-signing-credential-32-bytes"
	token := "joined-freeze-admin-session"
	insertSession(t, pool, accountID, userID, token)
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
		!lowerHexSHA256(plan.FreezeExclusionsSHA256) || plan.ProvisionalSourceClips != 1 || plan.ProvisionalSourceBytes != 10 {
		t.Fatalf("dry-run status=%d body=%s", dry.Code, dry.Body.String())
	}
	return joinedHistoricalTier1Fixture{s: s, pool: pool, cleanup: cleanup, userID: userID, accountID: accountID,
		storageID: storageID, apiKeyID: apiKeyID, connectionID: connectionID, runID: runID, firstJobID: firstJobID,
		clipID: clipID, clipStart: clipStart, sessionToken: token, req: req, plan: plan, call: call, callWithContext: callWithContext}
}

func TestJoinedTier1HistoricalApplyUsesExactFrozenDenominator(t *testing.T) {
	fixture := newJoinedHistoricalTier1Fixture(t, "joined-freeze@example.test")
	defer fixture.cleanup()
	ctx := context.Background()
	pool := fixture.pool
	accountID := fixture.accountID
	storageID, connectionID, runID := fixture.storageID, fixture.connectionID, fixture.runID
	firstJobID, clipID, clipStart := fixture.firstJobID, fixture.clipID, fixture.clipStart
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
