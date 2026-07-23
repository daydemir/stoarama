package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/recordingnaming"
)

func TestUniqueBatchStreamIDs(t *testing.T) {
	ids, err := uniqueBatchStreamIDs([]int64{9, 2, 5})
	if err != nil || len(ids) != 3 || ids[0] != 2 || ids[2] != 9 {
		t.Fatalf("ids=%v err=%v", ids, err)
	}
	for _, bad := range [][]int64{{}, {1, 1}, {0}} {
		if _, err := uniqueBatchStreamIDs(bad); err == nil {
			t.Fatalf("accepted %v", bad)
		}
	}
	maximum := make([]int64, 200)
	for i := range maximum {
		maximum[i] = int64(i + 1)
	}
	if _, err := uniqueBatchStreamIDs(maximum); err != nil {
		t.Fatalf("rejected 200 streams: %v", err)
	}
	tooMany := make([]int64, 201)
	for i := range tooMany {
		tooMany[i] = int64(i + 1)
	}
	if _, err := uniqueBatchStreamIDs(tooMany); err == nil {
		t.Fatal("accepted 201 streams")
	}
}

func TestBatchCaptureVia(t *testing.T) {
	cases := []struct {
		name, sourceURL, provider, existing, want string
	}{
		{"new SDOT", "https://example.com/live.m3u8", "SDOT", "", "relay"},
		{"existing SDOT cloud", "https://example.com/live.m3u8", "sdot", "cloud", "relay"},
		{"existing relay", "https://example.com/live.m3u8", "OTHER", "relay", "relay"},
		{"new direct stream", "https://example.com/live.m3u8", "OTHER", "", "cloud"},
		{"YouTube", "https://youtube.com/watch?v=test", "OTHER", "", "relay"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := batchCaptureVia(tc.sourceURL, tc.provider, tc.existing); got != tc.want {
				t.Fatalf("capture via = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBatchScheduleMixedRecordingStates(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()

	userID, accountID := seedUserOrg(t, pool, "batch@example.com", false)
	principal := accountPrincipal{AccountID: accountID, UserID: userID, MemberRole: "owner"}
	var destID int64
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO storage_destinations (account_id, name, provider, endpoint, region, bucket, access_key_id, secret_access_key_enc, status, managed)
		VALUES ($1, 'batch', 's3_compatible', 'https://s3.example.com', 'auto', 'batch', 'key', decode('00','hex'), 'verified', true)
		RETURNING id
	`, accountID).Scan(&destID); err != nil {
		t.Fatal(err)
	}

	statuses := []string{"new", "active", "paused", "completed", "canceled", "missing"}
	streamIDs := make(map[string]int64, len(statuses))
	for _, status := range statuses {
		zone := "America/New_York"
		if status == "completed" || status == "missing" {
			zone = ""
		}
		var streamID int64
		if err := pool.QueryRow(context.Background(), `
			INSERT INTO streams (provider, external_id, name, slug, stream_url, capture_type, source_family, execution_class, local_timezone)
			VALUES ('test', $1, $1, $1, 'https://www.youtube.com/watch?v=' || $1, 'youtube_watch', 'watch_page', 'youtube_direct', $2)
			RETURNING id
		`, status, zone).Scan(&streamID); err != nil {
			t.Fatal(err)
		}
		streamIDs[status] = streamID
		if status == "new" || status == "missing" {
			continue
		}
		recordingZone := "America/New_York"
		if status == "completed" {
			recordingZone = "Asia/Tokyo"
		}
		if _, err := pool.Exec(context.Background(), `
			INSERT INTO recordings (account_id, storage_destination_id, name, stream_url, stream_id, source_kind, mode, cron_expr, cron_timezone, clip_duration_sec, status, start_at, capture_via)
			VALUES ($1, $2, $3, 'https://www.youtube.com/watch?v=' || $3, $4, 'auto', 'sampled', '0 * * * *', $5, 60, $3, now(), 'cloud')
		`, accountID, destID, status, streamIDs[status], recordingZone); err != nil {
			t.Fatal(err)
		}
	}

	ids := make([]int64, 0, len(statuses))
	for _, status := range statuses {
		ids = append(ids, streamIDs[status])
	}
	request := batchScheduleRequest{StreamIDs: ids, NamingProfile: recordingnaming.ProfileStoaramaV1.String(), Mode: "sampled", CronExpr: "30 * * * *", ClipDurationSec: 60, StorageDestinationID: destID, Delivery: "managed"}
	post := func() *httptest.ResponseRecorder {
		body, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		req := withPrincipal(httptest.NewRequest(http.MethodPost, "/api/v1/account/recordings/batch-schedule", bytes.NewReader(body)), principal, "")
		rec := httptest.NewRecorder()
		s.handleAccountRecordingsBatchSchedule(rec, req)
		return rec
	}

	if rec := post(); rec.Code != http.StatusBadRequest {
		t.Fatalf("missing timezone status=%d body=%s", rec.Code, rec.Body.String())
	}
	for _, status := range []string{"active", "paused", "completed"} {
		var got string
		if err := pool.QueryRow(context.Background(), `SELECT status FROM recordings WHERE account_id=$1 AND stream_id=$2`, accountID, streamIDs[status]).Scan(&got); err != nil || got != status {
			t.Fatalf("atomic rollback %s: status=%q err=%v", status, got, err)
		}
	}

	var keyID int64
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO account_api_keys (account_id, key_prefix, secret_hash, scopes)
		VALUES ($1, 'batch', 'batch-nas-secret', ARRAY['stoarama.pull']) RETURNING id
	`, accountID).Scan(&keyID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `INSERT INTO connections (account_id, kind, api_key_id) VALUES ($1, 'nas_pull', $2)`, accountID, keyID); err != nil {
		t.Fatal(err)
	}
	request.StreamTimezones = []streamTimezoneInput{{StreamID: streamIDs["missing"], Timezone: "Europe/London"}}
	request.Delivery = "nas_pull"
	rec := post()
	if rec.Code != http.StatusOK {
		t.Fatalf("schedule status=%d body=%s", rec.Code, rec.Body.String())
	}
	var response batchScheduleResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Created != 3 || response.Updated != 3 {
		t.Fatalf("created=%d updated=%d", response.Created, response.Updated)
	}
	for _, status := range []string{"active", "paused", "completed"} {
		var gotStatus, gotDelivery string
		var gotDestID int64
		if err := pool.QueryRow(context.Background(), `SELECT status, delivery, storage_destination_id FROM recordings WHERE account_id=$1 AND stream_id=$2 AND status <> 'canceled'`, accountID, streamIDs[status]).Scan(&gotStatus, &gotDelivery, &gotDestID); err != nil || gotStatus != "active" || gotDelivery != "nas_pull" || gotDestID != destID {
			t.Fatalf("rescheduled %s: status=%q delivery=%q dest=%d err=%v", status, gotStatus, gotDelivery, gotDestID, err)
		}
	}
	for _, item := range response.Items {
		var gotDelivery, gotCaptureVia string
		if err := pool.QueryRow(context.Background(), `SELECT delivery, capture_via FROM recordings WHERE id=$1`, item.RecordingID).Scan(&gotDelivery, &gotCaptureVia); err != nil || gotDelivery != "nas_pull" || gotCaptureVia != "relay" {
			t.Fatalf("recording %d delivery=%q capture_via=%q err=%v", item.RecordingID, gotDelivery, gotCaptureVia, err)
		}
	}
	for stream, want := range map[string]string{"completed": "Asia/Tokyo", "missing": "Europe/London"} {
		var got string
		if err := pool.QueryRow(context.Background(), `SELECT local_timezone FROM streams WHERE id=$1`, streamIDs[stream]).Scan(&got); err != nil || got != want {
			t.Fatalf("%s timezone=%q want %q err=%v", stream, got, want, err)
		}
	}
}

func TestBatchSchedulePersistsPlazaHourlyNamingAndDaytimeWindow(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()

	userID, accountID := seedUserOrg(t, pool, "batch-plaza@example.com", false)
	principal := accountPrincipal{AccountID: accountID, UserID: userID, MemberRole: "owner"}
	var destID int64
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO storage_destinations (account_id, name, provider, endpoint, region, bucket, access_key_id, secret_access_key_enc, status, managed)
		VALUES ($1, 'batch-plaza', 's3_compatible', 'https://s3.example.com', 'auto', 'batch-plaza', 'key', decode('00','hex'), 'verified', true)
		RETURNING id
	`, accountID).Scan(&destID); err != nil {
		t.Fatal(err)
	}
	var streamID int64
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO streams (
			provider, external_id, name, slug, stream_url, capture_type, source_family,
			execution_class, local_timezone, location_country, location_city, metadata_jsonb
		)
		VALUES (
			'test', 'batch-plaza', 'Market Square', 'batch-plaza',
			'https://www.youtube.com/watch?v=batch-plaza', 'youtube_watch', 'watch_page',
			'youtube_direct', 'America/Los_Angeles', 'United States', 'Seattle',
			'{"continent":"North America"}'::jsonb
		)
		RETURNING id
	`).Scan(&streamID); err != nil {
		t.Fatal(err)
	}

	request := batchScheduleRequest{
		StreamIDs:            []int64{streamID},
		NamingProfile:        recordingnaming.ProfilePlazaHourlyV1.String(),
		Mode:                 "continuous",
		ClipDurationSec:      60,
		DailyWindowStart:     "08:00",
		DailyWindowEnd:       "20:00",
		ActiveWeekdays:       []int{1, 2, 3, 4, 5, 6, 7},
		StorageDestinationID: destID,
		Delivery:             "managed",
	}
	post := func() batchScheduleResponse {
		body, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		req := withPrincipal(httptest.NewRequest(http.MethodPost, "/api/v1/account/recordings/batch-schedule", bytes.NewReader(body)), principal, "")
		rec := httptest.NewRecorder()
		s.handleAccountRecordingsBatchSchedule(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("schedule status=%d body=%s", rec.Code, rec.Body.String())
		}
		var response batchScheduleResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		return response
	}

	assertPlaza := func() {
		var profile, folder, dailyStart, dailyEnd, plazaID string
		if err := pool.QueryRow(context.Background(), `
			SELECT naming_profile, folder_name, daily_window_start::text, daily_window_end::text,
			       naming_metadata_jsonb->>'plaza_id'
			FROM recordings
			WHERE account_id=$1 AND stream_id=$2 AND status <> 'canceled'
		`, accountID, streamID).Scan(&profile, &folder, &dailyStart, &dailyEnd, &plazaID); err != nil {
			t.Fatal(err)
		}
		if profile != recordingnaming.ProfilePlazaHourlyV1.String() || folder != "01_North_America_United_States_Seattle_Market_Square" {
			t.Fatalf("profile=%q folder=%q", profile, folder)
		}
		if dailyStart != "08:00:00" || dailyEnd != "20:00:00" || plazaID != "1" {
			t.Fatalf("window=%s-%s plaza_id=%q", dailyStart, dailyEnd, plazaID)
		}
	}

	if response := post(); response.Created != 1 || response.Updated != 0 {
		t.Fatalf("create response=%+v", response)
	}
	assertPlaza()

	if _, err := pool.Exec(context.Background(), `
		UPDATE recordings
		SET naming_profile='stoarama_v1', folder_name='recordings',
		    naming_metadata_jsonb='{}'::jsonb, daily_window_start='09:00', daily_window_end='21:00'
		WHERE account_id=$1 AND stream_id=$2
	`, accountID, streamID); err != nil {
		t.Fatal(err)
	}
	if response := post(); response.Created != 0 || response.Updated != 1 {
		t.Fatalf("update response=%+v", response)
	}
	assertPlaza()

	var mappedPlazaID int64
	if err := pool.QueryRow(context.Background(), `
		SELECT plaza_id FROM account_stream_plaza_ids WHERE account_id=$1 AND stream_id=$2
	`, accountID, streamID).Scan(&mappedPlazaID); err != nil || mappedPlazaID != 1 {
		t.Fatalf("mapped plaza id=%d err=%v", mappedPlazaID, err)
	}
}
