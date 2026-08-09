package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestBatchScheduleRejectsReencodingTargetFPSBeforePersistence(t *testing.T) {
	targetFPS := 30
	request := batchScheduleRequest{
		StreamIDs:            []int64{1},
		NamingProfile:        recordingnaming.ProfileStoaramaV1.String(),
		Mode:                 "sampled",
		CronExpr:             "*/5 * * * *",
		ClipDurationSec:      60,
		TargetFPS:            &targetFPS,
		StorageDestinationID: 1,
		Delivery:             "managed",
	}
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	req := withPrincipal(
		httptest.NewRequest(http.MethodPost, "/api/v1/account/recordings/batch-schedule", bytes.NewReader(body)),
		accountPrincipal{AccountID: 1, UserID: 1, MemberRole: "owner"},
		"",
	)
	rec := httptest.NewRecorder()
	(&Server{}).handleAccountRecordingsBatchSchedule(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "without re-encoding") {
		t.Fatalf("status=%d body=%s, want native-only rejection", rec.Code, rec.Body.String())
	}
}

func TestBatchScheduleDryRunIsReadOnly(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()

	userID, accountID := seedUserOrg(t, pool, "batch-dry-run@example.com", false)
	principal := accountPrincipal{AccountID: accountID, UserID: userID, MemberRole: "owner"}
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO nodes (account_id, display_name, node_type, status, last_heartbeat_at, relay_max_streams)
		VALUES ($1, 'dry-run-relay', 'relay', 'active', now(), 3)
	`, accountID); err != nil {
		t.Fatal(err)
	}
	var destID, streamID int64
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO storage_destinations (account_id, name, provider, endpoint, region, bucket, access_key_id, secret_access_key_enc, status, managed)
		VALUES ($1, 'dry-run', 's3_compatible', 'https://s3.example.com', 'auto', 'dry-run', 'key', decode('00','hex'), 'verified', true)
		RETURNING id
	`, accountID).Scan(&destID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO streams (provider, external_id, name, slug, source_url, capture_type, source_family, execution_class, capture_family, expected_fps)
		VALUES ('test', 'dry-run', 'Dry Run', 'dry-run', 'https://example.com/live.m3u8', 'hls', 'direct_stream', 'video_live', 'continuous_video', 30)
		RETURNING id
	`).Scan(&streamID); err != nil {
		t.Fatal(err)
	}
	request := batchScheduleRequest{
		StreamIDs:            []int64{streamID},
		StreamTimezones:      []streamTimezoneInput{{StreamID: streamID, Timezone: "Europe/Warsaw"}},
		NamingProfile:        recordingnaming.ProfilePlazaHourlyV1.String(),
		Mode:                 "continuous",
		ClipDurationSec:      60,
		DailyWindowStart:     "08:00",
		DailyWindowEnd:       "20:00",
		StorageDestinationID: destID,
		Delivery:             "managed",
		DryRun:               true,
	}
	_, otherAccountID := seedUserOrg(t, pool, "batch-other@example.com", false)
	request.TargetAccountID = otherAccountID
	forbiddenBody, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	forbiddenReq := withPrincipal(httptest.NewRequest(http.MethodPost, "/api/v1/account/recordings/batch-schedule", bytes.NewReader(forbiddenBody)), principal, "")
	forbiddenRec := httptest.NewRecorder()
	s.handleAccountRecordingsBatchSchedule(forbiddenRec, forbiddenReq)
	if forbiddenRec.Code != http.StatusForbidden {
		t.Fatalf("non-operator target status=%d body=%s", forbiddenRec.Code, forbiddenRec.Body.String())
	}
	request.TargetAccountID = accountID
	selfBody, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	selfReq := withPrincipal(httptest.NewRequest(http.MethodPost, "/api/v1/account/recordings/batch-schedule", bytes.NewReader(selfBody)), principal, "")
	selfRec := httptest.NewRecorder()
	s.handleAccountRecordingsBatchSchedule(selfRec, selfReq)
	if selfRec.Code != http.StatusOK {
		t.Fatalf("self target status=%d body=%s", selfRec.Code, selfRec.Body.String())
	}

	operatorUserID, operatorAccountID := seedUserOrg(t, pool, "batch-operator@example.com", true)
	operator := accountPrincipal{AccountID: operatorAccountID, UserID: operatorUserID, Role: accountRoleAdmin, MemberRole: "owner"}
	request.TargetAccountID = accountID
	operatorBody, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	operatorReq := withPrincipal(httptest.NewRequest(http.MethodPost, "/api/v1/account/recordings/batch-schedule", bytes.NewReader(operatorBody)), operator, "")
	operatorRec := httptest.NewRecorder()
	s.handleAccountRecordingsBatchSchedule(operatorRec, operatorReq)
	if operatorRec.Code != http.StatusOK {
		t.Fatalf("operator target status=%d body=%s", operatorRec.Code, operatorRec.Body.String())
	}

	request.TargetAccountID = 0
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	req := withPrincipal(httptest.NewRequest(http.MethodPost, "/api/v1/account/recordings/batch-schedule", bytes.NewReader(body)), principal, "")
	rec := httptest.NewRecorder()
	s.handleAccountRecordingsBatchSchedule(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("dry run status=%d body=%s", rec.Code, rec.Body.String())
	}
	var response batchScheduleResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if !response.DryRun || response.Created != 1 || response.Updated != 0 || response.Items[0].Action != "created" {
		t.Fatalf("response=%+v", response)
	}
	if response.RelayStreams != 0 || response.RequiredRelaySlots != 0 || response.OnlineRelaySlots != 3 {
		t.Fatalf("relay capacity response=%+v", response)
	}
	if response.Items[0].RecordingID != 0 {
		t.Fatalf("dry run returned nonexistent recording id %d", response.Items[0].RecordingID)
	}
	var recordings int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM recordings WHERE account_id=$1 AND stream_id=$2`, accountID, streamID).Scan(&recordings); err != nil {
		t.Fatal(err)
	}
	if recordings != 0 {
		t.Fatalf("dry run created %d recordings", recordings)
	}
	var timezone string
	if err := pool.QueryRow(context.Background(), `SELECT local_timezone FROM streams WHERE id=$1`, streamID).Scan(&timezone); err != nil {
		t.Fatal(err)
	}
	if timezone != "" {
		t.Fatalf("dry run persisted timezone %q", timezone)
	}
}

func TestBatchScheduleRejectsInsufficientRelayCapacity(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()

	userID, accountID := seedUserOrg(t, pool, "batch-no-relay-capacity@example.com", false)
	principal := accountPrincipal{AccountID: accountID, UserID: userID, MemberRole: "owner"}
	var destinationID, streamID int64
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO storage_destinations (account_id, name, provider, endpoint, region, bucket, access_key_id, secret_access_key_enc, status, managed)
		VALUES ($1, 'capacity-check', 's3_compatible', 'https://s3.example.com', 'auto', 'capacity-check', 'key', decode('00','hex'), 'verified', true)
		RETURNING id
	`, accountID).Scan(&destinationID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO streams (provider, external_id, name, slug, source_url, capture_type, source_family, execution_class, capture_family, expected_fps, local_timezone)
		VALUES ('SDOT', 'no-capacity', 'No Capacity', 'no-capacity', 'https://example.com/live.m3u8', 'hls', 'direct_stream', 'video_live', 'continuous_video', 30, 'America/Los_Angeles')
		RETURNING id
	`).Scan(&streamID); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(batchScheduleRequest{
		StreamIDs:            []int64{streamID},
		NamingProfile:        recordingnaming.ProfileStoaramaV1.String(),
		Mode:                 "continuous",
		ClipDurationSec:      60,
		DailyWindowStart:     "08:00",
		DailyWindowEnd:       "20:00",
		StorageDestinationID: destinationID,
		Delivery:             "managed",
	})
	if err != nil {
		t.Fatal(err)
	}
	req := withPrincipal(httptest.NewRequest(http.MethodPost, "/api/v1/account/recordings/batch-schedule", bytes.NewReader(body)), principal, "")
	rec := httptest.NewRecorder()
	s.handleAccountRecordingsBatchSchedule(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if !bytes.Contains(rec.Body.Bytes(), []byte("campaign requires 1 relay slots, but only 0 are available")) {
		t.Fatalf("unexpected body: %s", rec.Body.String())
	}
	var recordings int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM recordings WHERE account_id=$1 AND stream_id=$2`, accountID, streamID).Scan(&recordings); err != nil {
		t.Fatal(err)
	}
	if recordings != 0 {
		t.Fatalf("capacity rejection created %d recordings", recordings)
	}
}

func TestAvailableRelayCapacitySubtractsOnlyOutsideLeases(t *testing.T) {
	_, pool, cleanup := testIdentityServer(t)
	defer cleanup()

	_, accountID := seedUserOrg(t, pool, "relay-capacity@example.com", false)
	ctx := context.Background()
	var destinationID, nodeID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO storage_destinations (account_id, name, provider, endpoint, region, bucket, access_key_id, secret_access_key_enc, status, managed)
		VALUES ($1, 'capacity', 's3_compatible', 'https://s3.example.com', 'auto', 'capacity', 'key', decode('00','hex'), 'verified', true)
		RETURNING id
	`, accountID).Scan(&destinationID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO nodes (account_id, display_name, node_type, status, last_heartbeat_at, relay_max_streams)
		VALUES ($1, 'capacity-relay', 'relay', 'active', now(), 4)
		RETURNING id
	`, accountID).Scan(&nodeID); err != nil {
		t.Fatal(err)
	}
	streamIDs := make([]int64, 2)
	recordingIDs := make([]int64, 2)
	for i := range streamIDs {
		if err := pool.QueryRow(ctx, `
			INSERT INTO streams (provider, external_id, name, slug, source_url, capture_type)
			VALUES ('test', $1, $1, $1, 'https://example.com/'||$1||'.m3u8', 'hls')
			RETURNING id
		`, fmt.Sprintf("capacity-%d", i)).Scan(&streamIDs[i]); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(ctx, `
			INSERT INTO recordings (account_id, storage_destination_id, name, stream_url, stream_id, status, start_at, capture_via)
			VALUES ($1, $2, $3, 'https://example.com/live.m3u8', $4, 'active', now(), 'relay')
			RETURNING id
		`, accountID, destinationID, fmt.Sprintf("capacity-%d", i), streamIDs[i]).Scan(&recordingIDs[i]); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO recording_jobs (
				recording_id, fire_at, scheduled_for, clip_duration_sec, status,
				lease_owner, lease_expires_at, attempt_count, idempotency_key
			) VALUES ($1, now(), now(), 60, 'leased', $2, now()+interval '5 minutes', 1, $3)
		`, recordingIDs[i], fmt.Sprintf("node:%d", nodeID), fmt.Sprintf("capacity-%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	available, err := availableRelayCapacity(ctx, pool, accountID, []int64{streamIDs[0]})
	if err != nil {
		t.Fatal(err)
	}
	if available != 3 {
		t.Fatalf("available relay slots=%d want 3", available)
	}
}

func TestAvailableRelayCapacityCountsOfflineGroupLeases(t *testing.T) {
	_, pool, cleanup := testIdentityServer(t)
	defer cleanup()

	_, accountID := seedUserOrg(t, pool, "relay-group-capacity@example.com", false)
	ctx := context.Background()
	var groupID, onlineNodeID, offlineNodeID, destinationID, streamID, recordingID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO relay_groups (account_id, name, max_streams)
		VALUES ($1, 'shared uplink', 8)
		RETURNING id
	`, accountID).Scan(&groupID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO nodes (account_id, display_name, node_type, status, last_heartbeat_at, relay_max_streams, relay_group_id)
		VALUES ($1, 'online-group-relay', 'relay', 'active', now(), 4, $2)
		RETURNING id
	`, accountID, groupID).Scan(&onlineNodeID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO nodes (account_id, display_name, node_type, status, last_heartbeat_at, relay_max_streams, relay_group_id)
		VALUES ($1, 'offline-group-relay', 'relay', 'active', now()-interval '10 minutes', 4, $2)
		RETURNING id
	`, accountID, groupID).Scan(&offlineNodeID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO storage_destinations (account_id, name, provider, endpoint, region, bucket, access_key_id, secret_access_key_enc, status, managed)
		VALUES ($1, 'capacity', 's3_compatible', 'https://s3.example.com', 'auto', 'capacity', 'key', decode('00','hex'), 'verified', true)
		RETURNING id
	`, accountID).Scan(&destinationID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO streams (provider, external_id, name, slug, source_url, capture_type)
		VALUES ('test', 'offline-group', 'offline-group', 'offline-group', 'https://example.com/offline-group.m3u8', 'hls')
		RETURNING id
	`).Scan(&streamID); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if err := pool.QueryRow(ctx, `
			INSERT INTO recordings (account_id, storage_destination_id, name, stream_url, stream_id, status, start_at, capture_via)
			VALUES ($1, $2, $3, 'https://example.com/live.m3u8', $4, 'active', now(), 'relay')
			RETURNING id
		`, accountID, destinationID, fmt.Sprintf("offline-group-%d", i), streamID).Scan(&recordingID); err != nil {
			t.Fatal(err)
		}
		nodeID := offlineNodeID
		if i < 2 {
			nodeID = onlineNodeID
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO recording_jobs (
				recording_id, fire_at, scheduled_for, clip_duration_sec, status,
				lease_owner, lease_expires_at, attempt_count, idempotency_key
			) VALUES ($1, now(), now(), 60, 'leased', $2, now()+interval '5 minutes', 1, $3)
		`, recordingID, fmt.Sprintf("node:%d", nodeID), fmt.Sprintf("offline-group-%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	available, err := availableRelayCapacity(ctx, pool, accountID, []int64{})
	if err != nil {
		t.Fatal(err)
	}
	if available != 2 {
		t.Fatalf("available relay slots=%d want 2", available)
	}
}

func TestBatchScheduleRefreshesExistingRecordingSource(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()

	userID, accountID := seedUserOrg(t, pool, "batch-source-refresh@example.com", false)
	principal := accountPrincipal{AccountID: accountID, UserID: userID, MemberRole: "owner"}
	var destinationID, streamID, recordingID int64
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO storage_destinations (account_id, name, provider, endpoint, region, bucket, access_key_id, secret_access_key_enc, status, managed)
		VALUES ($1, 'source-refresh', 's3_compatible', 'https://s3.example.com', 'auto', 'source-refresh', 'key', decode('00','hex'), 'verified', true)
		RETURNING id
	`, accountID).Scan(&destinationID); err != nil {
		t.Fatal(err)
	}
	const oldURL = "https://old.example.com/live/stream.flv"
	const newURL = "https://new.example.com/live.m3u8"
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO streams (provider, external_id, name, slug, source_url, capture_type, source_family, execution_class, capture_family, expected_fps, local_timezone)
		VALUES ('test', 'source-refresh', 'Source Refresh', 'source-refresh', $1, 'hls', 'direct_stream', 'video_live', 'continuous_video', 30, 'UTC')
		RETURNING id
	`, oldURL).Scan(&streamID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO recordings (account_id, storage_destination_id, name, stream_url, stream_id, source_kind, mode, cron_expr, cron_timezone, clip_duration_sec, status, start_at, capture_via)
		VALUES ($1, $2, 'Source Refresh', $3, $4, 'ffmpeg_direct', 'sampled', '0 * * * *', 'UTC', 60, 'active', now(), 'cloud')
		RETURNING id
	`, accountID, destinationID, oldURL, streamID).Scan(&recordingID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE streams SET source_url=$2 WHERE id=$1`, streamID, newURL); err != nil {
		t.Fatal(err)
	}

	request := batchScheduleRequest{
		StreamIDs:            []int64{streamID},
		NamingProfile:        recordingnaming.ProfileStoaramaV1.String(),
		Mode:                 "sampled",
		CronExpr:             "30 * * * *",
		ClipDurationSec:      60,
		StorageDestinationID: destinationID,
		Delivery:             "managed",
	}
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

	var gotURL, gotKind string
	var gotRecordingID int64
	if err := pool.QueryRow(context.Background(), `
		SELECT id, stream_url, source_kind
		FROM recordings
		WHERE account_id=$1 AND stream_id=$2 AND status <> 'canceled'
	`, accountID, streamID).Scan(&gotRecordingID, &gotURL, &gotKind); err != nil {
		t.Fatal(err)
	}
	if gotRecordingID != recordingID || gotURL != newURL || gotKind != "hls_live" {
		t.Fatalf("recording id=%d url=%q kind=%q, want id=%d url=%q kind=hls_live", gotRecordingID, gotURL, gotKind, recordingID, newURL)
	}
}

func TestBatchScheduleMixedRecordingStates(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()

	userID, accountID := seedUserOrg(t, pool, "batch@example.com", false)
	principal := accountPrincipal{AccountID: accountID, UserID: userID, MemberRole: "owner"}
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO nodes (account_id, display_name, node_type, status, last_heartbeat_at, relay_max_streams)
		VALUES ($1, 'batch-relay', 'relay', 'active', now(), 6)
	`, accountID); err != nil {
		t.Fatal(err)
	}
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
			INSERT INTO streams (provider, external_id, name, slug, source_url, capture_type, source_family, execution_class, capture_family, expected_fps, local_timezone)
			VALUES ('test', $1, $1, $1, 'https://www.youtube.com/watch?v=' || $1, 'youtube_watch', 'watch_page', 'youtube_direct', 'continuous_video', 30, $2)
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
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO nodes (account_id, display_name, node_type, status, last_heartbeat_at, relay_max_streams)
		VALUES ($1, 'batch-plaza-relay', 'relay', 'active', now(), 1)
	`, accountID); err != nil {
		t.Fatal(err)
	}
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
			provider, external_id, name, slug, source_url, capture_type, source_family,
			execution_class, capture_family, expected_fps, local_timezone,
			location_country, location_city, metadata_jsonb
		)
		VALUES (
			'test', 'batch-plaza', 'Market Square', 'batch-plaza',
			'https://www.youtube.com/watch?v=batch-plaza', 'youtube_watch', 'watch_page',
			'youtube_direct', 'continuous_video', 30, 'America/Los_Angeles', 'United States', 'Seattle',
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
