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
	frozen := call(true, planned.Plan.PlanSHA256)
	if frozen.Code != http.StatusCreated {
		t.Fatalf("freeze status=%d body=%s", frozen.Code, frozen.Body.String())
	}
	idempotent := call(true, planned.Plan.PlanSHA256)
	if idempotent.Code != http.StatusOK || !strings.Contains(idempotent.Body.String(), "idempotent") {
		t.Fatalf("idempotent status=%d body=%s", idempotent.Code, idempotent.Body.String())
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
