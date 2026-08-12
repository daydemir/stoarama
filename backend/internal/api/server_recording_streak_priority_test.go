package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestFinishStreakPriorityProtectsNearGoal(t *testing.T) {
	item := streakPriorityRecording{}
	for i := 0; i < 13; i++ {
		item.RecentWindows = append(item.RecentWindows, streakWindow{Grade: "GOOD_CANDIDATE"})
	}
	finishStreakPriority(&item, "active")
	if item.CurrentStreak != 13 || item.WindowsTo14 != 1 || item.Protection != "critical_13_of_14" || item.Action != "protect_no_nonessential_change" {
		t.Fatalf("item=%+v", item)
	}
}

func TestStreakPriorityPostgresExpectedWindowFailuresAndTenantWall(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()
	userID, accountID := seedUserOrg(t, pool, "streak@example.com", false)
	_, otherAccount := seedUserOrg(t, pool, "streak-other@example.com", false)
	ctx := context.Background()
	_, err := pool.Exec(ctx, `
		INSERT INTO storage_destinations(account_id,name,provider,endpoint,region,bucket,access_key_id,secret_access_key_enc,status,managed)
		VALUES($1,'streak','s3_compatible','https://s3.example.test','auto','streak','key',decode('00','hex'),'verified',true),
		      ($2,'other','s3_compatible','https://s3.example.test','auto','other','key',decode('00','hex'),'verified',true)`, accountID, otherAccount)
	if err != nil {
		t.Fatal(err)
	}
	var streamID int64
	if err := pool.QueryRow(ctx, `INSERT INTO streams(provider,external_id,name,slug,source_url,capture_type,source_family,execution_class,capture_family,expected_fps)
		VALUES('direct','streak','streak','streak','https://example.test/live.m3u8','hls','video_manifest','video_live','continuous_video',30) RETURNING id`).Scan(&streamID); err != nil {
		t.Fatal(err)
	}
	open := time.Now().UTC().Truncate(24 * time.Hour).Add(-24*time.Hour + 8*time.Hour)
	var recID, otherRecID int64
	insertRec := `INSERT INTO recordings(account_id,storage_destination_id,name,stream_url,source_kind,cron_expr,cron_timezone,clip_duration_sec,status,start_at,stream_id,mode,daily_window_start,daily_window_end,active_weekdays)
		VALUES($1,(SELECT id FROM storage_destinations WHERE account_id=$1 LIMIT 1),$2,'https://example.test/live.m3u8','hls_live','0 8 * * *','UTC',60,'active',$3,$4,'continuous','08:00','20:00',127) RETURNING id`
	if err := pool.QueryRow(ctx, insertRec, accountID, "mine", open.Add(-48*time.Hour), streamID).Scan(&recID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, insertRec, otherAccount, "other", open.Add(-48*time.Hour), streamID).Scan(&otherRecID); err != nil {
		t.Fatal(err)
	}
	var jobID int64
	if err := pool.QueryRow(ctx, `INSERT INTO recording_jobs(recording_id,fire_at,scheduled_for,clip_duration_sec,status,idempotency_key,kind,window_end_at)
		VALUES($1,$2,$2,60,'done',$3,'continuous_window',$4) RETURNING id`, recID, open, "streak-job", open.Add(12*time.Hour)).Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `INSERT INTO recording_window_health(recording_id,job_id,window_start_at,window_end_at,expected_seconds,covered_seconds,coverage_pct,largest_gap_seconds,gap_count,gap_over_30s_count,gap_over_5m_count,overlap_count,overlap_seconds,longest_run_seconds,layout_change_count,clip_count,metric_version,calculated_at)
		VALUES($1,$2,$3,$4::timestamptz,43200,43100,99.8,10,1,0,0,0,0,43100,0,10,2,$4::timestamptz+interval '1 minute')`, recID, jobID, open, open.Add(12*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	call := func() streakPriorityResponse {
		req := withPrincipal(httptest.NewRequest(http.MethodGet, "/api/v1/account/recordings/streak-priority", nil), accountPrincipal{AccountID: accountID, UserID: userID, MemberRole: "owner"}, "")
		rr := httptest.NewRecorder()
		s.handleAccountRecordingStreakPriority(rr, req)
		if rr.Code != 200 {
			t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
		}
		var out streakPriorityResponse
		if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	out := call()
	if len(out.Items) != 1 || out.Items[0].RecordingID != recID || out.Items[0].CurrentStreak != 1 {
		t.Fatalf("out=%+v", out)
	}
	// A duplicate scheduled job makes the same expected window UNKNOWN.
	_, err = pool.Exec(ctx, `INSERT INTO recording_jobs(recording_id,fire_at,scheduled_for,clip_duration_sec,status,idempotency_key,kind,window_end_at) VALUES($1,$2,$2,60,'done','streak-duplicate','continuous_window',$3)`, recID, open, open.Add(12*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	out = call()
	if out.Items[0].CurrentStreak != 0 || out.Items[0].RecentWindows[0].Grade != "UNKNOWN" {
		t.Fatalf("duplicate did not fail closed: %+v", out.Items[0])
	}
	if _, err := pool.Exec(ctx, `DELETE FROM recording_jobs WHERE id<>$1 AND recording_id=$2; UPDATE recording_window_health SET calculated_at=window_end_at-interval '1 second' WHERE recording_id=$2`, jobID, recID); err != nil {
		t.Fatal(err)
	}
	out = call()
	if out.Items[0].RecentWindows[0].Grade != "UNKNOWN" {
		t.Fatalf("stale health did not fail closed: %+v", out.Items[0])
	}
	if _, err := pool.Exec(ctx, `DELETE FROM recording_window_health WHERE recording_id=$1`, recID); err != nil {
		t.Fatal(err)
	}
	out = call()
	if out.Items[0].RecentWindows[0].Grade != "UNKNOWN" {
		t.Fatalf("missing health did not fail closed: %+v", out.Items[0])
	}
	// PostgreSQL timezone/calendar semantics used by the production query: DST
	// changes UTC offsets while preserving the exact local 08:00-20:00 window.
	var springHours, fallHours float64
	if err := pool.QueryRow(ctx, `SELECT extract(epoch FROM((date '2026-03-08'+time '20:00') AT TIME ZONE 'America/New_York'-(date '2026-03-08'+time '08:00') AT TIME ZONE 'America/New_York'))/3600,
	 extract(epoch FROM((date '2026-11-01'+time '20:00') AT TIME ZONE 'America/New_York'-(date '2026-11-01'+time '08:00') AT TIME ZONE 'America/New_York'))/3600`).Scan(&springHours, &fallHours); err != nil || springHours != 12 || fallHours != 12 {
		t.Fatalf("DST local windows spring=%v fall=%v err=%v", springHours, fallHours, err)
	}
	var mondayIncluded, sundayIncluded bool
	if err := pool.QueryRow(ctx, `SELECT (1::int & (1 << (extract(isodow FROM date '2026-08-10')::int-1)))<>0,(1::int & (1 << (extract(isodow FROM date '2026-08-09')::int-1)))<>0`).Scan(&mondayIncluded, &sundayIncluded); err != nil || !mondayIncluded || sundayIncluded {
		t.Fatalf("weekday mask monday=%t sunday=%t err=%v", mondayIncluded, sundayIncluded, err)
	}
	// Explain the bounded production-shaped calendar query under PostgreSQL.
	var plan string
	if err := pool.QueryRow(ctx, `EXPLAIN (FORMAT TEXT) `+streakPriorityFactsSQL, accountID).Scan(&plan); err != nil || plan == "" {
		t.Fatalf("explain=%q err=%v", plan, err)
	}
}

func TestFinishStreakPriorityAllowsOnlyOneAcceptable(t *testing.T) {
	item := streakPriorityRecording{RecentWindows: []streakWindow{{Grade: "GOOD_CANDIDATE"}, {Grade: "ACCEPTABLE_CANDIDATE"}, {Grade: "GOOD_CANDIDATE"}, {Grade: "ACCEPTABLE_CANDIDATE"}, {Grade: "GOOD_CANDIDATE"}}}
	finishStreakPriority(&item, "active")
	if item.CurrentStreak != 3 || item.AcceptableInRun != 1 {
		t.Fatalf("item=%+v", item)
	}
}

func TestFinishStreakPriorityFlagsEarlyAndRepeatedFailures(t *testing.T) {
	early := streakPriorityRecording{RecentWindows: []streakWindow{{Grade: "FAILED"}}}
	finishStreakPriority(&early, "active")
	if early.Lifecycle != "probation" || early.Action != "early_failure_review_replace_or_reprobe" {
		t.Fatalf("early=%+v", early)
	}
	repeated := streakPriorityRecording{RecentWindows: []streakWindow{{Grade: "FAILED"}, {Grade: "GOOD_CANDIDATE"}, {Grade: "UNKNOWN"}, {Grade: "GOOD_CANDIDATE"}}}
	finishStreakPriority(&repeated, "active")
	if repeated.Action != "repeated_failure_replace_or_source_repair" {
		t.Fatalf("repeated=%+v", repeated)
	}
}

func TestFinishStreakPriorityReplacementKeepsOwnZeroStreak(t *testing.T) {
	item := streakPriorityRecording{RecentWindows: []streakWindow{{Grade: "UNKNOWN"}}}
	finishStreakPriority(&item, "paused")
	if item.Lifecycle != "candidate" || item.CurrentStreak != 0 || item.WindowsTo14 != 14 {
		t.Fatalf("item=%+v", item)
	}
}
