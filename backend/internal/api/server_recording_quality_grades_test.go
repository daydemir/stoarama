package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/qualitygrade"
)

func TestAccountRecordingQualityGradesTenantWallAndGrades(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()
	userID, accountID := seedUserOrg(t, pool, "grades@example.com", false)
	_, otherAccount := seedUserOrg(t, pool, "grades-other@example.com", false)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO storage_destinations(account_id,name,provider,endpoint,region,bucket,access_key_id,secret_access_key_enc,status,managed)
		VALUES($1,'grades','s3_compatible','https://s3.example.test','auto','grades','key',decode('00','hex'),'verified',true),
		      ($2,'other','s3_compatible','https://s3.example.test','auto','other','key',decode('00','hex'),'verified',true)`, accountID, otherAccount); err != nil {
		t.Fatal(err)
	}
	var streamID int64
	if err := pool.QueryRow(ctx, `INSERT INTO streams(provider,external_id,name,slug,source_url,capture_type,source_family,execution_class,capture_family,expected_fps)
		VALUES('direct','grades','grades','grades','https://example.test/live.m3u8','hls','video_manifest','video_live','continuous_video',30) RETURNING id`).Scan(&streamID); err != nil {
		t.Fatal(err)
	}
	open := time.Now().UTC().Truncate(24 * time.Hour).Add(-48*time.Hour + 8*time.Hour)
	insertRec := `INSERT INTO recordings(account_id,storage_destination_id,name,stream_url,source_kind,cron_expr,cron_timezone,clip_duration_sec,status,start_at,stream_id,mode,daily_window_start,daily_window_end,active_weekdays)
		VALUES($1,(SELECT id FROM storage_destinations WHERE account_id=$1 LIMIT 1),$2,'https://example.test/live.m3u8','hls_live','0 8 * * *','UTC',60,'active',$3,$4,'continuous','08:00','20:00',127) RETURNING id`
	var mine, other int64
	if err := pool.QueryRow(ctx, insertRec, accountID, "mine", open.Add(-24*time.Hour), streamID).Scan(&mine); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, insertRec, otherAccount, "other", open.Add(-24*time.Hour), streamID).Scan(&other); err != nil {
		t.Fatal(err)
	}
	for _, rec := range []int64{mine, other} {
		var jobID int64
		if err := pool.QueryRow(ctx, `INSERT INTO recording_jobs(recording_id,fire_at,scheduled_for,clip_duration_sec,status,idempotency_key,kind,window_end_at)
			VALUES($1,$2,$2,60,'done',$3,'continuous_window',$4) RETURNING id`, rec, open, "grades-"+time.Now().Format("150405.000000000"), open.Add(12*time.Hour)).Scan(&jobID); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO recording_window_health(recording_id,job_id,window_start_at,window_end_at,expected_seconds,covered_seconds,coverage_pct,largest_gap_seconds,gap_count,gap_over_30s_count,gap_over_5m_count,overlap_count,overlap_seconds,longest_run_seconds,layout_change_count,clip_count,metric_version,calculated_at)
			VALUES($1,$2,$3,$4::timestamptz,43200,31000,71.7,2700,20,20,6,0,0,20000,0,500,2,$4::timestamptz+interval '5 minutes')`, rec, jobID, open, open.Add(12*time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	req := withPrincipal(httptest.NewRequest(http.MethodGet, "/api/v1/account/recordings/quality-grades?days=7", nil), accountPrincipal{AccountID: accountID, UserID: userID, MemberRole: "owner"}, "")
	rr := httptest.NewRecorder()
	s.handleAccountRecordingQualityGrades(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var out recordingQualityGradesResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Days != 7 || len(out.Recordings) != 1 || out.Recordings[0].RecordingID != mine {
		t.Fatalf("response=%+v want only own recording %d", out, mine)
	}
	latest, ok := out.Recordings[0].Latest()
	if !ok || latest.Grade != qualitygrade.GradeE {
		t.Fatalf("latest=%+v want E", latest)
	}
	if good := out.Recordings[0].TierProgress(qualitygrade.TierGood); good.RunDays != 1 || good.ECount != 1 {
		t.Fatalf("good+ progress=%+v", good)
	}
}
