package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func pf(v float64) *float64 { return &v }
func pi(v int) *int         { return &v }
func pi64(v int64) *int64   { return &v }

func TestClassifyQualificationTimeline(t *testing.T) {
	base := qualificationWindowMetrics{CoveragePct: pf(99), LargestGap: pf(120), GapsOver30s: pi(1), GapsOver5m: pi(0), OverlapCount: pi(0), MetricVersion: pi(2), ExpectedSeconds: 43200, MeasuredExpected: pi64(43200), JobCount: 1, HealthCount: 1}
	if got, _ := classifyQualificationTimeline(base); got != "GREAT_CANDIDATE" {
		t.Fatalf("great boundary=%s", got)
	}
	good := base
	good.CoveragePct = pf(95)
	good.LargestGap = pf(900)
	good.GapsOver30s = pi(6)
	good.GapsOver5m = pi(1)
	if got, _ := classifyQualificationTimeline(good); got != "GOOD_CANDIDATE" {
		t.Fatalf("good boundary=%s", got)
	}
	acceptable := good
	acceptable.CoveragePct = pf(90)
	acceptable.LargestGap = pf(1800)
	acceptable.GapsOver30s = pi(9)
	acceptable.GapsOver5m = pi(2)
	if got, _ := classifyQualificationTimeline(acceptable); got != "ACCEPTABLE_CANDIDATE" {
		t.Fatalf("acceptable boundary=%s", got)
	}
	failed := acceptable
	failed.CoveragePct = pf(89.999)
	if got, _ := classifyQualificationTimeline(failed); got != "FAILED" {
		t.Fatalf("failed=%s", got)
	}
	unknown := base
	unknown.MetricVersion = pi(1)
	if got, _ := classifyQualificationTimeline(unknown); got != "UNKNOWN" {
		t.Fatalf("old metric=%s", got)
	}
	inconsistent := base
	inconsistent.LargestGap = pf(301)
	inconsistent.GapsOver5m = pi(0)
	if got, _ := classifyQualificationTimeline(inconsistent); got != "UNKNOWN" {
		t.Fatalf("inconsistent=%s", got)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*qualificationWindowMetrics)
		want   string
	}{
		{"job count", func(m *qualificationWindowMetrics) { m.JobCount = 0 }, "UNKNOWN"},
		{"overlap", func(m *qualificationWindowMetrics) { m.OverlapCount = pi(1) }, "FAILED"},
		{"expected duration", func(m *qualificationWindowMetrics) { m.MeasuredExpected = pi64(43199) }, "UNKNOWN"},
		{"late clip", func(m *qualificationWindowMetrics) { m.LateClip = true }, "UNKNOWN"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := base
			tc.mutate(&m)
			if got, _ := classifyQualificationTimeline(m); got != tc.want {
				t.Fatalf("got=%s want=%s", got, tc.want)
			}
		})
	}
}

func TestQualificationBuildFreezesAndIsIdempotent(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()
	userID, accountID := seedUserOrg(t, pool, "qualification-freeze@example.com", false)
	ctx := context.Background()
	_, err := pool.Exec(ctx, `
	 INSERT INTO storage_destinations(account_id,name,provider,endpoint,region,bucket,access_key_id,secret_access_key_enc,status,managed)
	 VALUES($1,'qual','s3_compatible','https://s3.example.test','auto','qual','key',decode('00','hex'),'verified',true)`, accountID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `
	 WITH ss AS (INSERT INTO streams(provider,external_id,name,slug,source_url,source_page_url,capture_type,source_family,execution_class,capture_family,expected_fps)
	   SELECT 'direct','q'||n,'stream-'||n,'qualification-'||n,'https://example.test/'||n||'.m3u8','','hls','video_manifest','video_live','continuous_video',30 FROM generate_series(1,50)n RETURNING id),
	 rr AS (INSERT INTO recordings(account_id,storage_destination_id,name,stream_url,source_kind,cron_expr,cron_timezone,clip_duration_sec,status,start_at,stream_id,mode,daily_window_start,daily_window_end,active_weekdays)
	   SELECT $1,(SELECT id FROM storage_destinations WHERE account_id=$1 AND name='qual'),'recording-'||row_number() over(),'https://example.test/live.m3u8','hls_live','0 8 * * *','UTC',60,'active','2026-08-01',id,'continuous','08:00','20:00',127 FROM ss RETURNING stream_id),
	 mo AS (INSERT INTO media_objects(storage_provider,bucket,object_key,mime_type,size_bytes,sha256)
	   SELECT 'r2','qual','frame-'||stream_id,'image/jpeg',1,lpad(to_hex(stream_id),64,'0') FROM rr RETURNING id,object_key,sha256),
	 ff AS (INSERT INTO frames(stream_id,captured_at,raw_media_object_id,capture_status,source_kind)
	   SELECT rr.stream_id,now()-interval '1 hour',mo.id,'success','live' FROM rr JOIN mo ON mo.object_key='frame-'||rr.stream_id RETURNING id,stream_id,raw_media_object_id,captured_at)
	 INSERT INTO recording_scene_frame_evidence(account_id,stream_id,frame_id,media_object_id,captured_at,frame_sha256,scene_identity_sha256,verification_method,verified_by_user_id)
	 SELECT $1,ff.stream_id,ff.id,ff.raw_media_object_id,ff.captured_at,mo.sha256,lpad(to_hex(ff.stream_id+100000),64,'0'),'operator_visual',$2 FROM ff JOIN mo ON mo.id=ff.raw_media_object_id`, accountID, userID)
	if err != nil {
		t.Fatal(err)
	}
	var ids []int64
	rows, err := pool.Query(ctx, `SELECT id FROM recordings WHERE account_id=$1 ORDER BY id`, accountID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	call := func(apply bool, expected string) *httptest.ResponseRecorder {
		raw, _ := json.Marshal(qualificationBuildRequest{RecordingIDs: ids, SequenceStart: time.Date(2026, 8, 13, 0, 0, 0, 0, time.UTC), Apply: apply, ExpectedPlanSHA256: expected})
		req := withPrincipal(httptest.NewRequest(http.MethodPost, "/api/v1/account/recordings/qualification/build", bytes.NewReader(raw)), accountPrincipal{AccountID: accountID, UserID: userID, MemberRole: "owner"}, "")
		rec := httptest.NewRecorder()
		s.handleAccountRecordingQualificationBuild(rec, req)
		return rec
	}
	dry := call(false, "")
	if dry.Code != http.StatusOK {
		t.Fatalf("dry status=%d body=%s", dry.Code, dry.Body.String())
	}
	var planned struct {
		Plan qualificationPlan `json:"plan"`
	}
	if err := json.Unmarshal(dry.Body.Bytes(), &planned); err != nil {
		t.Fatal(err)
	}
	mutation, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mutation.Exec(ctx, `UPDATE recordings SET status='paused',paused_at=now() WHERE id=$1`, ids[0]); err != nil {
		_ = mutation.Rollback(ctx)
		t.Fatal(err)
	}
	concurrent := make(chan *httptest.ResponseRecorder, 1)
	go func() { concurrent <- call(true, planned.Plan.PlanSHA256) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting bool
		err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE '/* qualification_plan */%')`).Scan(&waiting)
		if err != nil {
			_ = mutation.Rollback(ctx)
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			_ = mutation.Rollback(ctx)
			t.Fatal("qualification freeze did not reach the transaction-backed locked plan query")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := mutation.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	changed := <-concurrent
	if changed.Code != http.StatusConflict {
		t.Fatalf("concurrent mutation status=%d body=%s", changed.Code, changed.Body.String())
	}
	if _, err := pool.Exec(ctx, `UPDATE recordings SET status='active',paused_at=NULL WHERE id=$1`, ids[0]); err != nil {
		t.Fatal(err)
	}
	frozen := call(true, planned.Plan.PlanSHA256)
	if frozen.Code != http.StatusCreated {
		t.Fatalf("freeze status=%d body=%s", frozen.Code, frozen.Body.String())
	}
	var activeRunID, jobID, otherJobID int64
	var windowStart, windowEnd time.Time
	var expectedSeconds int64
	if err := pool.QueryRow(ctx, `
		SELECT r.id,w.window_start_at,w.window_end_at,w.expected_seconds
		FROM recording_qualification_runs r
		JOIN recording_qualification_windows w ON w.run_id=r.id
		WHERE r.account_id=$1 AND r.status='active' AND w.recording_id=$2
		ORDER BY w.ordinal LIMIT 1
	`, accountID, ids[0]).Scan(&activeRunID, &windowStart, &windowEnd, &expectedSeconds); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO recording_jobs(recording_id,fire_at,scheduled_for,clip_duration_sec,status,idempotency_key,kind,window_end_at)
		VALUES($1,$2,$2,60,'done','qualification-health-job','continuous_window',$3) RETURNING id
	`, ids[0], windowStart, windowEnd).Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO recording_window_health(recording_id,job_id,window_start_at,window_end_at,expected_seconds,covered_seconds,coverage_pct,largest_gap_seconds,gap_count,gap_over_30s_count,gap_over_5m_count,overlap_count,overlap_seconds,longest_run_seconds,layout_change_count,clip_count,metric_version,calculated_at)
		VALUES($1,$2,$3,$4,$5::bigint,$5::double precision,99.8,10,1,0,0,0,0,$5::double precision,0,10,2,$4::timestamptz+interval '1 minute')
	`, ids[0], jobID, windowStart, windowEnd, expectedSeconds); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO recording_jobs(recording_id,fire_at,scheduled_for,clip_duration_sec,status,idempotency_key,kind)
		VALUES($1,$2,$2,60,'done','qualification-other-job','clip') RETURNING id
	`, ids[0], windowStart).Scan(&otherJobID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO recording_clips(recording_id,recording_job_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,fire_at,clip_start_at,clip_end_at,created_at)
		VALUES($1,$2,(SELECT storage_destination_id FROM recordings WHERE id=$1),'https://s3.example.test','qual','qualification-other-job-late',1,$3,$3,$3::timestamptz+interval '1 minute',$4::timestamptz+interval '2 minutes')
	`, ids[0], otherJobID, windowStart, windowEnd); err != nil {
		t.Fatal(err)
	}
	reportReq := withPrincipal(httptest.NewRequest(http.MethodGet, "/api/v1/account/recordings/qualification", nil), accountPrincipal{AccountID: accountID, UserID: userID, MemberRole: "owner"}, "")
	reportRecorder := httptest.NewRecorder()
	s.handleAccountRecordingQualification(reportRecorder, reportReq)
	if reportRecorder.Code != http.StatusOK {
		t.Fatalf("report status=%d body=%s", reportRecorder.Code, reportRecorder.Body.String())
	}
	var report recordingQualificationResponse
	if err := json.Unmarshal(reportRecorder.Body.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	var reported *recordingQualificationWindow
	for i := range report.Members {
		if report.Members[i].RecordingID != ids[0] {
			continue
		}
		for j := range report.Members[i].Windows {
			if report.Members[i].Windows[j].WindowStartAt.Equal(windowStart) {
				reported = &report.Members[i].Windows[j]
			}
		}
	}
	if reported == nil || reported.TimelineGrade != "GREAT_CANDIDATE" {
		t.Fatalf("other-job late clip contaminated qualification: %+v", reported)
	}
	if _, err := pool.Exec(ctx, `UPDATE recordings SET status='paused',paused_at=now() WHERE id=$1`, ids[0]); err != nil {
		t.Fatal(err)
	}
	idempotent := call(true, planned.Plan.PlanSHA256)
	if idempotent.Code != http.StatusOK || !strings.Contains(idempotent.Body.String(), "idempotent") {
		t.Fatalf("idempotent status=%d body=%s", idempotent.Code, idempotent.Body.String())
	}
	differentIDs := append([]int64(nil), ids...)
	differentIDs[0] = 999999999
	raw, _ := json.Marshal(qualificationBuildRequest{RecordingIDs: differentIDs, SequenceStart: time.Date(2026, 8, 13, 0, 0, 0, 0, time.UTC), Apply: true, ExpectedPlanSHA256: planned.Plan.PlanSHA256})
	req := withPrincipal(httptest.NewRequest(http.MethodPost, "/api/v1/account/recordings/qualification/build", bytes.NewReader(raw)), accountPrincipal{AccountID: accountID, UserID: userID, MemberRole: "owner"}, "")
	different := httptest.NewRecorder()
	s.handleAccountRecordingQualificationBuild(different, req)
	if different.Code != http.StatusConflict {
		t.Fatalf("different request status=%d body=%s", different.Code, different.Body.String())
	}
	var status string
	var frozenAt *time.Time
	if err := pool.QueryRow(ctx, `SELECT status,frozen_at FROM recording_qualification_runs WHERE account_id=$1`, accountID).Scan(&status, &frozenAt); err != nil || status != "active" || frozenAt == nil {
		t.Fatalf("status=%s frozen=%v err=%v", status, frozenAt, err)
	}
	stale := call(true, strings.Repeat("0", 64))
	if stale.Code != http.StatusConflict {
		t.Fatalf("stale status=%d body=%s", stale.Code, stale.Body.String())
	}
	// Migration 0138 relaxes the physical NULL constraint only for the exact
	// historical authority. Prospective activation must still fail closed.
	if _, err := pool.Exec(ctx, `UPDATE recordings SET status='active',paused_at=NULL WHERE account_id=$1`, accountID); err != nil {
		t.Fatal(err)
	}
	var nullSceneRunID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM recording_qualification_runs WHERE account_id=$1 AND status='active'`, accountID).
		Scan(&activeRunID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_qualification_runs SET status='canceled' WHERE id=$1`, activeRunID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO recording_qualification_runs(account_id,definition_version,definition_jsonb,
		target_recording_count,target_window_count,required_good_or_great,max_acceptable,window_sequence_start_at)
		SELECT account_id,definition_version,definition_jsonb,target_recording_count,target_window_count,
		required_good_or_great,max_acceptable,window_sequence_start_at FROM recording_qualification_runs WHERE id=$1 RETURNING id`,
		activeRunID).Scan(&nullSceneRunID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_qualification_members(run_id,account_id,recording_id,ordinal,
		stream_id,recording_name,stream_name,scene_identity_sha256,scene_frame_evidence_id,cron_timezone,
		daily_window_start,daily_window_end,active_weekdays,schedule_start_at,schedule_end_at,window_generator_version)
		SELECT $2,account_id,recording_id,ordinal,stream_id,recording_name,stream_name,
		CASE WHEN ordinal=1 THEN NULL ELSE scene_identity_sha256 END,
		CASE WHEN ordinal=1 THEN NULL ELSE scene_frame_evidence_id END,cron_timezone,daily_window_start,daily_window_end,
		active_weekdays,schedule_start_at,schedule_end_at,window_generator_version
		FROM recording_qualification_members WHERE run_id=$1`, activeRunID, nullSceneRunID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_qualification_windows(run_id,recording_id,ordinal,local_open_at,
		local_end_at,open_utc_offset_seconds,end_utc_offset_seconds,window_start_at,window_end_at,expected_seconds)
		SELECT $2,recording_id,ordinal,local_open_at,local_end_at,open_utc_offset_seconds,end_utc_offset_seconds,
		window_start_at,window_end_at,expected_seconds FROM recording_qualification_windows WHERE run_id=$1`,
		activeRunID, nullSceneRunID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_qualification_runs SET status='active' WHERE id=$1`, nullSceneRunID); err == nil ||
		!strings.Contains(err.Error(), "qualification evidence or window set is invalid") {
		t.Fatalf("prospective missing-scene activation err=%v", err)
	}
}

func TestQualificationBuildFailsClosedBelowFiftyBeforeDatabase(t *testing.T) {
	ids := make([]int64, 49)
	for i := range ids {
		ids[i] = int64(i + 1)
	}
	_, err := (&Server{}).buildQualificationPlan(context.Background(), 1, qualificationBuildRequest{RecordingIDs: ids, SequenceStart: time.Now()})
	if err == nil || !strings.Contains(err.Error(), "at least 50") {
		t.Fatalf("err=%v", err)
	}
}

func TestQualificationBuildRejectsDuplicateIDsBeforeDatabase(t *testing.T) {
	ids := make([]int64, 50)
	for i := range ids {
		ids[i] = int64(i + 1)
	}
	ids[49] = 1
	_, err := (&Server{}).buildQualificationPlan(context.Background(), 1, qualificationBuildRequest{RecordingIDs: ids, SequenceStart: time.Now()})
	if err == nil || !strings.Contains(err.Error(), "unique") {
		t.Fatalf("err=%v", err)
	}
}

func TestSceneAttestRequiresMemberSession(t *testing.T) {
	for _, principal := range []accountPrincipal{{AccountID: 1, UserID: 0}, {}} {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/account/recordings/qualification/scene-attest", bytes.NewBufferString(`{"recording_id":1,"frame_id":2,"scene_identity":"place"}`))
		if principal.AccountID != 0 {
			req = withPrincipal(req, principal, "")
		}
		rec := httptest.NewRecorder()
		(&Server{}).handleAccountRecordingSceneAttest(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
	}
}

func TestQualificationBuildRequiresMemberSession(t *testing.T) {
	for _, principal := range []accountPrincipal{{AccountID: 1, UserID: 0}, {}} {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/account/recordings/qualification/build", bytes.NewBufferString(`{"recording_ids":[],"sequence_start_at":"2026-08-13T00:00:00Z","apply":false}`))
		if principal.AccountID != 0 {
			req = withPrincipal(req, principal, "")
		}
		rec := httptest.NewRecorder()
		(&Server{}).handleAccountRecordingQualificationBuild(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
	}
}

func TestNormalizeSceneIdentityStableAndBounded(t *testing.T) {
	a, err := normalizeSceneIdentity("  Anguk   Station, Seoul ")
	if err != nil {
		t.Fatal(err)
	}
	b, err := normalizeSceneIdentity("anguk station, seoul")
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("%q != %q", a, b)
	}
	if _, err := normalizeSceneIdentity(strings.Repeat("x", 2)); err == nil {
		t.Fatal("short identity accepted")
	}
	if _, err := normalizeSceneIdentity(strings.Repeat("x", 3)); err != nil {
		t.Fatalf("minimum identity rejected: %v", err)
	}
	if _, err := normalizeSceneIdentity(strings.Repeat("x", 240)); err != nil {
		t.Fatalf("maximum identity rejected: %v", err)
	}
	if _, err := normalizeSceneIdentity(strings.Repeat("x", 241)); err == nil {
		t.Fatal("long identity accepted")
	}
}
