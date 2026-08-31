package api

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
)

func TestRecordingListBaselineDoesNotWaitForMetricTables(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO accounts(id,email,name,role,status) VALUES(47,'metrics@example.test','Metrics','admin','active');
		INSERT INTO storage_destinations(id,account_id,name,endpoint,region,bucket,access_key_id,secret_access_key_enc,status)
		VALUES(1,47,'Metrics storage','https://example.test','auto','clips','access',''::bytea,'verified');
		INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,status,start_at,mode,cron_expr,cron_timezone,clip_duration_sec,
			naming_profile,folder_name,naming_metadata_jsonb)
		VALUES(700,47,1,'Metric-independent list','https://example.test/live.m3u8','active',now()-interval '1 day','sampled','* * * * *','UTC',60,
			'plaza_hourly_v1','08_Europe_Poland_Swidnik_Plac_Konstytucji',
			'{"plaza_id":"08","continent":"Europe","country":"Poland","city":"Swidnik","plaza_name":"Plac Konstytucji"}'::jsonb);
	`); err != nil {
		t.Fatal(err)
	}
	s.cfg = config.Config{SharedRecordingsAccountID: 47, SharedRecordingsSlug: "mit-scl", SharedRecordingsPublic: true}

	locker, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Release()
	tx, err := locker.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `LOCK TABLE recording_clips, recording_joined_sources, recording_window_health IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}

	requests := []struct {
		name       string
		handler    http.HandlerFunc
		path       string
		rowsKey    string
		wantRecent bool
		setup      func(*http.Request) *http.Request
	}{
		{
			name:       "authenticated",
			handler:    s.handleAccountRecordingsList,
			path:       "/api/v1/account/recordings",
			rowsKey:    "items",
			wantRecent: true,
			setup: func(req *http.Request) *http.Request {
				return withPrincipal(req, accountPrincipal{AccountID: 47}, "")
			},
		},
		{
			name:    "public",
			handler: s.handleSharedRecordingsList,
			path:    "/api/v1/shared/mit-scl/recordings",
			rowsKey: "recordings",
			setup:   func(req *http.Request) *http.Request { return req },
		},
	}
	for _, tc := range requests {
		t.Run(tc.name, func(t *testing.T) {
			requestCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			req := tc.setup(httptest.NewRequest(http.MethodGet, tc.path, nil).WithContext(requestCtx))
			rec := httptest.NewRecorder()
			started := time.Now()
			tc.handler(rec, req)
			if elapsed := time.Since(started); elapsed >= time.Second {
				t.Fatalf("baseline list waited for metric tables: %s", elapsed)
			}
			if rec.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
			var payload map[string]json.RawMessage
			if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
				t.Fatal(err)
			}
			var items []map[string]json.RawMessage
			if err := json.Unmarshal(payload[tc.rowsKey], &items); err != nil {
				t.Fatalf("decode %s: %v", tc.rowsKey, err)
			}
			if len(items) != 1 {
				t.Fatalf("%s rows=%d want 1", tc.rowsKey, len(items))
			}
			want := map[string]string{
				"capture_health_bins": "[]",
				"timeline_health":     "null",
				"source_duration_ms":  "0",
				"joined_ready_ms":     "0",
				"joined_percent":      "null",
				"captured_clip_count": "0",
				"expected_clip_count": "0",
				"capture_health":      `"unavailable"`,
			}
			for field, expected := range want {
				if got, ok := items[0][field]; !ok || string(got) != expected {
					t.Fatalf("baseline compatibility field %s=%s present=%t want %s", field, got, ok, expected)
				}
			}
			if tc.wantRecent {
				// Authenticated lists retain the compatibility field, but its exact
				// clip scan belongs to enrichment rather than the baseline response.
				if got := string(items[0]["recent_clip_count"]); got != "0" {
					t.Fatalf("baseline recent_clip_count=%s want placeholder 0", got)
				}
			}
			var naming struct {
				Profile    string `json:"profile"`
				FolderName string `json:"folder_name"`
			}
			if err := json.Unmarshal(items[0]["naming"], &naming); err != nil {
				t.Fatalf("decode baseline naming: %v body=%s", err, items[0]["naming"])
			}
			if naming.Profile != "plaza_hourly_v1" || naming.FolderName != "08_Europe_Poland_Swidnik_Plac_Konstytucji" {
				t.Fatalf("baseline naming=%+v", naming)
			}
		})
	}
}

func TestRecordingDetailAndCSVRetainExactClipMetrics(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO accounts(id,email,name,role,status) VALUES(47,'exact-metrics@example.test','Exact metrics','admin','active');
		INSERT INTO storage_destinations(id,account_id,name,endpoint,region,bucket,access_key_id,secret_access_key_enc,status)
		VALUES(1,47,'Exact storage','https://example.test','auto','clips','access',''::bytea,'verified');
		INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,status,start_at,mode,cron_expr,cron_timezone,clip_duration_sec)
		VALUES(700,47,1,'Exact metric detail','https://example.test/live.m3u8','active',now()-interval '5 minutes','sampled','* * * * *','UTC',60);
		INSERT INTO recording_clips(recording_id,size_bytes,clip_start_at,clip_end_at)
		VALUES(700,1,now()-interval '1 minute',now());
	`); err != nil {
		t.Fatal(err)
	}
	s.cfg = config.Config{SharedRecordingsAccountID: 47, SharedRecordingsSlug: "mit-scl", SharedRecordingsPublic: true}

	assertExact := func(t *testing.T, body []byte) {
		t.Helper()
		var item struct {
			Recent   int64                       `json:"recent_clip_count"`
			Captured int64                       `json:"captured_clip_count"`
			Expected int64                       `json:"expected_clip_count"`
			Health   recordingCaptureHealthState `json:"capture_health"`
		}
		if err := json.Unmarshal(body, &item); err != nil {
			t.Fatal(err)
		}
		if item.Recent != 1 || item.Captured != 1 || item.Expected <= 1 || item.Health != recordingCaptureHealthCritical {
			t.Fatalf("exact clip metrics changed: %+v", item)
		}
	}

	for _, test := range []struct {
		name    string
		handler http.HandlerFunc
		req     *http.Request
	}{
		{
			name:    "authenticated detail",
			handler: s.handleAccountRecordingGet,
			req:     withPrincipal(httptest.NewRequest(http.MethodGet, "/api/v1/account/recordings/700", nil), accountPrincipal{AccountID: 47}, "700"),
		},
		{
			name:    "public detail",
			handler: s.handleSharedRecordingGet,
			req:     withPrincipal(httptest.NewRequest(http.MethodGet, "/api/v1/shared/mit-scl/recordings/700", nil), accountPrincipal{}, "700"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			test.handler(response, test.req)
			if response.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			assertExact(t, response.Body.Bytes())
		})
	}

	csvResponse := httptest.NewRecorder()
	csvRequest := withPrincipal(httptest.NewRequest(http.MethodGet, "/api/v1/account/recordings.csv", nil), accountPrincipal{AccountID: 47}, "")
	s.handleAccountRecordingsCSV(csvResponse, csvRequest)
	if csvResponse.Code != http.StatusOK {
		t.Fatalf("CSV status=%d body=%s", csvResponse.Code, csvResponse.Body.String())
	}
	records, err := csv.NewReader(strings.NewReader(csvResponse.Body.String())).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("CSV records=%d want 2", len(records))
	}
	columns := make(map[string]string, len(records[0]))
	for i, name := range records[0] {
		columns[name] = records[1][i]
	}
	if columns["recent_clip_count_24h"] != "1" || columns["captured_clip_count"] != "1" || columns["expected_clip_count"] == "0" || columns["capture_health"] != string(recordingCaptureHealthCritical) {
		t.Fatalf("CSV exact clip metrics changed: %+v", columns)
	}
}

func TestRecordingMetricEndpointTimeoutsCoverScopeLookup(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()
	ctx := context.Background()
	locker, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Release()
	tx, err := locker.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `LOCK TABLE recordings IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name    string
		path    string
		handler http.HandlerFunc
	}{
		{name: "enrichment", path: "/api/v1/account/recordings/enrichment", handler: s.handleAccountRecordingListEnrichment},
		{name: "joined progress", path: "/api/v1/account/recordings/joined-progress", handler: s.handleAccountRecordingJoinedProgress},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requestCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			req := withPrincipal(httptest.NewRequest(http.MethodGet, tc.path, nil).WithContext(requestCtx), accountPrincipal{AccountID: 47}, "")
			rec := httptest.NewRecorder()
			started := time.Now()
			tc.handler(rec, req)
			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
			if rec.Header().Get("Retry-After") != "10" {
				t.Fatalf("retry-after=%q", rec.Header().Get("Retry-After"))
			}
			if elapsed := time.Since(started); elapsed > time.Second {
				t.Fatalf("scope lookup exceeded request deadline: %s", elapsed)
			}
		})
	}
}

func TestRecordingDetailTimelineEnrichmentDoesNotWaitForClipMetrics(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO accounts(id,email,name,role,status) VALUES(47,'detail-metrics@example.test','Detail metrics','admin','active');
		INSERT INTO storage_destinations(id,account_id,name,endpoint,region,bucket,access_key_id,secret_access_key_enc,status)
		VALUES(1,47,'Detail storage','https://example.test','auto','clips','access',''::bytea,'verified');
		INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,status,start_at,mode,cron_expr,cron_timezone,clip_duration_sec)
		VALUES(700,47,1,'High-volume detail','https://example.test/live.m3u8','completed',now()-interval '14 days','sampled','* * * * *','UTC',60);
	`); err != nil {
		t.Fatal(err)
	}
	s.cfg = config.Config{SharedRecordingsAccountID: 47, SharedRecordingsSlug: "mit-scl", SharedRecordingsPublic: true}

	locker, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Release()
	tx, err := locker.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `LOCK TABLE recording_clips, recording_joined_sources IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name    string
		path    string
		shared  bool
		handler http.HandlerFunc
	}{
		{name: "public", path: "/api/v1/shared/mit-scl/recordings/enrichment?recording_id=700&timeline_only=1", shared: true, handler: s.handleSharedRecordingListEnrichment},
		{name: "authenticated", path: "/api/v1/account/recordings/enrichment?recording_id=700&timeline_only=1", handler: s.handleAccountRecordingListEnrichment},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requestCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			req := httptest.NewRequest(http.MethodGet, tc.path, nil).WithContext(requestCtx)
			if !tc.shared {
				req = withPrincipal(req, accountPrincipal{AccountID: 47}, "")
			}
			response := httptest.NewRecorder()
			started := time.Now()
			tc.handler(response, req)
			if response.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			if elapsed := time.Since(started); elapsed >= time.Second {
				t.Fatalf("timeline-only detail enrichment waited for locked clip metrics: %s", elapsed)
			}
			var payload struct {
				Items []recordingListEnrichmentItem `json:"items"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
				t.Fatal(err)
			}
			if len(payload.Items) != 1 || payload.Items[0].RecordingID != 700 || payload.Items[0].TimelineHealth == nil || len(payload.Items[0].CaptureHealthBins) != 0 {
				t.Fatalf("unexpected timeline-only payload: %+v", payload.Items)
			}
			var rawPayload struct {
				Items []map[string]json.RawMessage `json:"items"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &rawPayload); err != nil {
				t.Fatal(err)
			}
			for _, field := range []string{"captured_clip_count", "expected_clip_count", "capture_health"} {
				if _, ok := rawPayload.Items[0][field]; ok {
					t.Fatalf("timeline-only detail enrichment must omit %s", field)
				}
			}
		})
	}

	requestCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/shared/mit-scl/recordings/enrichment?recording_id=700", nil).WithContext(requestCtx)
	response := httptest.NewRecorder()
	s.handleSharedRecordingListEnrichment(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("full enrichment status=%d body=%s; timeline-only cache leaked across metric shapes", response.Code, response.Body.String())
	}
}

func TestRecordingListEnrichmentScopesAVisibleBatchWithoutScanningTheWholeList(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO accounts(id,email,name,role,status)
		VALUES(47,'batch-metrics@example.test','Batch metrics','admin','active');
		INSERT INTO accounts(id,email,name,role,status)
		VALUES(99,'foreign-batch@example.test','Foreign batch','admin','active');
		INSERT INTO storage_destinations(id,account_id,name,endpoint,region,bucket,access_key_id,secret_access_key_enc,status)
		VALUES(1,47,'Batch storage','https://example.test','auto','clips','access',''::bytea,'verified');
		INSERT INTO storage_destinations(id,account_id,name,endpoint,region,bucket,access_key_id,secret_access_key_enc,status)
		VALUES(2,99,'Foreign batch storage','https://example.test','auto','foreign-clips','access',''::bytea,'verified');
		INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,status,start_at,mode,cron_expr,cron_timezone,clip_duration_sec)
		SELECT 700+g,47,1,'Batch recording '||g,'https://example.test/'||g||'.m3u8','completed',
		       now()-interval '1 day','sampled',
		       CASE WHEN g < 12 THEN '* * * * *' ELSE 'invalid cron outside requested batch' END,
		       'UTC',60
		FROM generate_series(0,103) AS g;
		UPDATE recordings SET status='canceled',cron_expr='* * * * *' WHERE id=803;
		UPDATE recordings SET status='active' WHERE id=700;
		INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,status,start_at,mode,cron_expr,cron_timezone,clip_duration_sec)
		VALUES(900,99,2,'Foreign batch recording','https://example.test/foreign.m3u8','completed',now()-interval '1 day','sampled','* * * * *','UTC',60);
		INSERT INTO recording_clips(recording_id,size_bytes,clip_start_at,clip_end_at)
		VALUES(700,1,now()-interval '1 minute',now());
	`); err != nil {
		t.Fatal(err)
	}
	s.cfg = config.Config{SharedRecordingsAccountID: 47, SharedRecordingsSlug: "mit-scl", SharedRecordingsPublic: true}

	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/shared/mit-scl/recordings/enrichment?recording_ids=700,701,702,703,704,705,706,707,708,709,710,711", nil)
	rec := httptest.NewRecorder()
	s.handleSharedRecordingListEnrichment(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var payload struct {
		Items []recordingListEnrichmentItem `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Items) != 12 {
		t.Fatalf("items=%d want 12", len(payload.Items))
	}
	for index, item := range payload.Items {
		wantID := int64(700 + index)
		if item.RecordingID != wantID {
			t.Fatalf("item[%d].recording_id=%d want %d", index, item.RecordingID, wantID)
		}
	}
	var enriched struct {
		Items []struct {
			RecordingID       int64                        `json:"recording_id"`
			CapturedClipCount *int64                       `json:"captured_clip_count"`
			ExpectedClipCount *int64                       `json:"expected_clip_count"`
			CaptureHealth     *recordingCaptureHealthState `json:"capture_health"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &enriched); err != nil {
		t.Fatal(err)
	}
	first := enriched.Items[0]
	if first.RecordingID != 700 || first.CapturedClipCount == nil || *first.CapturedClipCount != 1 || first.ExpectedClipCount == nil || *first.ExpectedClipCount <= 1 || first.CaptureHealth == nil || *first.CaptureHealth != recordingCaptureHealthCritical {
		t.Fatalf("recording list enrichment did not backfill capture metrics: %+v", first)
	}

	for _, test := range []struct {
		name       string
		path       string
		shared     bool
		wantStatus int
		wantItems  int
	}{
		{
			name:       "public rejects canceled recording",
			path:       "/api/v1/shared/mit-scl/recordings/enrichment?recording_ids=700,803",
			shared:     true,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "account rejects canceled recording",
			path:       "/api/v1/account/recordings/enrichment?recording_ids=700,803",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "public rejects foreign recording",
			path:       "/api/v1/shared/mit-scl/recordings/enrichment?recording_ids=700,900",
			shared:     true,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "account rejects foreign recording",
			path:       "/api/v1/account/recordings/enrichment?recording_ids=700,900",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "public timeline rejects canceled recording",
			path:       "/api/v1/shared/mit-scl/recordings/enrichment?recording_id=803&timeline_only=1",
			shared:     true,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "account timeline rejects canceled recording",
			path:       "/api/v1/account/recordings/enrichment?recording_id=803&timeline_only=1",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "public timeline rejects foreign recording",
			path:       "/api/v1/shared/mit-scl/recordings/enrichment?recording_id=900&timeline_only=1",
			shared:     true,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "account timeline rejects foreign recording",
			path:       "/api/v1/account/recordings/enrichment?recording_id=900&timeline_only=1",
			wantStatus: http.StatusNotFound,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, test.path, nil)
			response := httptest.NewRecorder()
			if test.shared {
				s.handleSharedRecordingListEnrichment(response, req)
			} else {
				s.handleAccountRecordingListEnrichment(response, withPrincipal(req, accountPrincipal{AccountID: 47}, ""))
			}
			if response.Code != test.wantStatus {
				t.Fatalf("status=%d want=%d body=%s", response.Code, test.wantStatus, response.Body.String())
			}
			if test.wantItems == 0 {
				return
			}
			var got struct {
				Items []recordingListEnrichmentItem `json:"items"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if len(got.Items) != test.wantItems {
				t.Fatalf("items=%d want=%d", len(got.Items), test.wantItems)
			}
		})
	}
}
