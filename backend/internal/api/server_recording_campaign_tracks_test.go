package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCampaignCheckpointUsesPreopenAndSeparatesFirst3FromFreshness(t *testing.T) {
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	fire := now.Add(20 * time.Minute)
	pass := "pass"
	state := "leased"
	if got := campaignCheckpoint(now, &fire, &state, 0, nil, nil, nil, nil, nil, nil); got != "preopen_t_minus_30m_due" {
		t.Fatal(got)
	}
	if got := campaignCheckpoint(now, &fire, &state, 0, nil, nil, nil, nil, nil, &pass); got != "preopen_pass" {
		t.Fatal(got)
	}
	opened := now.Add(-time.Hour)
	first := now.Add(-55 * time.Minute)
	fresh := now.Add(-time.Minute)
	if got := campaignCheckpoint(now, &opened, &state, 3, &first, &first, &fresh, &fresh, &pass, &pass); got != "first_3_clips_observed_current_job" {
		t.Fatal(got)
	}
	stale := now.Add(-6 * time.Minute)
	if got := campaignCheckpoint(now, &opened, &state, 3, &first, &first, &stale, &fresh, &pass, &pass); got != "first_3_clips_due" {
		t.Fatal(got)
	}
}

func TestCampaignTracksTenantWallAndPreopenEvidence(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()
	user, account := seedUserOrg(t, pool, "campaign@example.com", false)
	_, other := seedUserOrg(t, pool, "campaign-other@example.com", false)
	ctx := context.Background()
	_, err := pool.Exec(ctx, `INSERT INTO storage_destinations(account_id,name,provider,endpoint,region,bucket,access_key_id,secret_access_key_enc,status,managed) VALUES($1,'mine','s3_compatible','https://s3.test','auto','mine','k',decode('00','hex'),'verified',true),($2,'other','s3_compatible','https://s3.test','auto','other','k',decode('00','hex'),'verified',true)`, account, other)
	if err != nil {
		t.Fatal(err)
	}
	var stream int64
	if err = pool.QueryRow(ctx, `INSERT INTO streams(provider,external_id,name,slug,source_url,capture_type,source_family,execution_class,capture_family,expected_fps) VALUES('direct','campaign','campaign','campaign','https://example.test/live.m3u8','hls','video_manifest','video_live','continuous_video',30) RETURNING id`).Scan(&stream); err != nil {
		t.Fatal(err)
	}
	insertRec := `INSERT INTO recordings(account_id,storage_destination_id,name,stream_url,source_kind,cron_expr,cron_timezone,clip_duration_sec,status,start_at,stream_id,mode,daily_window_start,daily_window_end,active_weekdays) VALUES($1,(SELECT id FROM storage_destinations WHERE account_id=$1 LIMIT 1),$2,'https://example.test/live.m3u8','hls_live','0 8 * * *','UTC',60,'active',now()-interval '1 day',$3,'continuous','08:00','20:00',127) RETURNING id`
	var mine, theirs int64
	if err = pool.QueryRow(ctx, insertRec, account, "mine", stream).Scan(&mine); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, insertRec, other, "theirs", stream).Scan(&theirs); err != nil {
		t.Fatal(err)
	}
	seed := func(acct, rec int64, key, scene string) {
		var track int64
		if err := pool.QueryRow(ctx, `INSERT INTO recording_campaign_tracks(account_id,campaign_key,label,deadline_at,target_count,grade_floor,created_by_user_id) VALUES($1,$2,$2,now()+interval '7 days',1,'GOOD',$3) RETURNING id`, acct, key, user).Scan(&track); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO recording_campaign_roster_entries(track_id,recording_id,stream_id,scene_identity_sha256,role,rank,status,reason_codes,effective_at,decision_at,evidence_observed_at,evidence_sha256,updated_by_user_id) VALUES($1,$2,$3,$4,'primary',1,'protect',ARRAY['fixture'],now(),now(),now(),repeat('b',64),$5)`, track, rec, stream, scene, user); err != nil {
			t.Fatal(err)
		}
	}
	seed(account, mine, "mine", strings.Repeat("a", 64))
	seed(other, theirs, "theirs", strings.Repeat("c", 64))
	req := withPrincipal(httptest.NewRequest(http.MethodGet, "/api/v1/account/recordings/campaign-tracks", nil), accountPrincipal{AccountID: account, UserID: user, MemberRole: "owner"}, "")
	rr := httptest.NewRecorder()
	s.handleAccountRecordingCampaignTracks(rr, req)
	if rr.Code != 200 || !strings.Contains(rr.Body.String(), `"recording_id":`+fmt.Sprint(mine)) || strings.Contains(rr.Body.String(), `"recording_id":`+fmt.Sprint(theirs)) {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
}
