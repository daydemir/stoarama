package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
		INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,status,start_at,mode,cron_expr,cron_timezone,clip_duration_sec)
		VALUES(700,47,1,'Metric-independent list','https://example.test/live.m3u8','completed',now()-interval '1 day','sampled','* * * * *','UTC',60);
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
	if _, err := tx.Exec(ctx, `LOCK TABLE recording_joined_sources, recording_window_health IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}

	requests := []struct {
		name    string
		handler http.HandlerFunc
		path    string
		rowsKey string
		setup   func(*http.Request) *http.Request
	}{
		{
			name:    "authenticated",
			handler: s.handleAccountRecordingsList,
			path:    "/api/v1/account/recordings",
			rowsKey: "items",
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
			}
			for field, expected := range want {
				if got, ok := items[0][field]; !ok || string(got) != expected {
					t.Fatalf("baseline compatibility field %s=%s present=%t want %s", field, got, ok, expected)
				}
			}
		})
	}
}
