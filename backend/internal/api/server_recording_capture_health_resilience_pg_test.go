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

func TestRecordingCaptureHealthBaseDoesNotWaitForJoinedMetrics(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO accounts(id,email,name,role,status)
		VALUES(47,'capture-resilience@example.test','Capture resilience','admin','active');
		INSERT INTO storage_destinations(id,account_id,name,endpoint,region,bucket,access_key_id,secret_access_key_enc,status)
		VALUES(1,47,'Capture storage','https://example.test','auto','clips','access',''::bytea,'verified');
		INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,status,start_at,end_at,mode,cron_expr,cron_timezone,clip_duration_sec)
		VALUES(700,47,1,'Capture health','https://example.test/live.m3u8','completed',now()-interval '1 day',now(),'sampled','* * * * *','UTC',60);
		INSERT INTO recording_clips(recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,fire_at,clip_start_at,clip_end_at)
		VALUES(700,1,'https://example.test','clips','capture-health.mp4',1,now()-interval '30 minutes',now()-interval '30 minutes',now()-interval '29 minutes');
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
	if _, err := tx.Exec(ctx, `LOCK TABLE recording_joined_sources IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name    string
		shared  bool
		path    string
		handler http.HandlerFunc
	}{
		{name: "public", shared: true, path: "/api/v1/shared/mit-scl/recordings/700/capture-health?metrics=base", handler: s.handleSharedRecordingCaptureHealth},
		{name: "authenticated", path: "/api/v1/account/recordings/700/capture-health?metrics=base", handler: s.handleAccountRecordingCaptureHealth},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requestCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			req := httptest.NewRequest(http.MethodGet, tc.path, nil).WithContext(requestCtx)
			if tc.shared {
				req = withPrincipal(req, accountPrincipal{}, "700")
			} else {
				req = withPrincipal(req, accountPrincipal{AccountID: 47}, "700")
			}
			response := httptest.NewRecorder()
			started := time.Now()
			tc.handler(response, req)
			if response.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			if elapsed := time.Since(started); elapsed >= time.Second {
				t.Fatalf("base capture health waited for locked joined table: %s", elapsed)
			}
			var page recordingCaptureHealthPage
			if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
				t.Fatal(err)
			}
			if !page.CaptureIncluded || page.JoinedIncluded {
				t.Fatalf("base response metric flags capture=%t joined=%t", page.CaptureIncluded, page.JoinedIncluded)
			}
			captured := int64(0)
			for _, bin := range page.Bins {
				captured += bin.Captured
				if bin.SourceDurationMS != 0 || bin.JoinedReadyMS != 0 {
					t.Fatalf("base response leaked joined values: %+v", bin)
				}
			}
			if captured != 1 {
				t.Fatalf("captured clips=%d want=1", captured)
			}
		})
	}

	for _, metrics := range []string{"joined", "full"} {
		t.Run(metrics+" remains bounded", func(t *testing.T) {
			requestCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			req := withPrincipal(httptest.NewRequest(http.MethodGet, "/api/v1/account/recordings/700/capture-health?metrics="+metrics, nil).WithContext(requestCtx), accountPrincipal{AccountID: 47}, "700")
			response := httptest.NewRecorder()
			s.handleAccountRecordingCaptureHealth(response, req)
			if response.Code != http.StatusServiceUnavailable {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			if response.Header().Get("Retry-After") != "10" {
				t.Fatalf("retry-after=%q", response.Header().Get("Retry-After"))
			}
		})
	}
}

func TestRecordingCaptureHealthBaseRetainsAdmissionUnderJoinedSaturation(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO accounts(id,email,name,role,status)
		VALUES(47,'capture-admission@example.test','Capture admission','admin','active');
		INSERT INTO storage_destinations(id,account_id,name,endpoint,region,bucket,access_key_id,secret_access_key_enc,status)
		VALUES(1,47,'Capture admission storage','https://example.test','auto','clips','access',''::bytea,'verified');
		INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,status,start_at,end_at,mode,cron_expr,cron_timezone,clip_duration_sec)
		VALUES(700,47,1,'Capture admission','https://example.test/live.m3u8','completed',now()-interval '1 day',now(),'sampled','* * * * *','UTC',60);
		INSERT INTO recording_clips(recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,fire_at,clip_start_at,clip_end_at)
		VALUES(700,1,'https://example.test','clips','capture-admission.mp4',1,now()-interval '30 minutes',now()-interval '30 minutes',now()-interval '29 minutes');
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
	locked := true
	defer func() {
		if locked {
			_ = tx.Rollback(context.Background())
		}
	}()
	if _, err := tx.Exec(ctx, `LOCK TABLE recording_joined_sources IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}

	joinedDone := make(chan int, 2)
	for _, shared := range []bool{false, true} {
		shared := shared
		go func() {
			requestCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			path := "/api/v1/account/recordings/700/capture-health?metrics=joined"
			handler := s.handleAccountRecordingCaptureHealth
			principal := accountPrincipal{AccountID: 47}
			if shared {
				path = "/api/v1/shared/mit-scl/recordings/700/capture-health?metrics=joined"
				handler = s.handleSharedRecordingCaptureHealth
				principal = accountPrincipal{}
			}
			req := withPrincipal(httptest.NewRequest(http.MethodGet, path, nil).WithContext(requestCtx), principal, "700")
			response := httptest.NewRecorder()
			handler(response, req)
			joinedDone <- response.Code
		}()
	}

	flightDeadline := time.Now().Add(time.Second)
	for {
		s.recordingHealthPageCache.Mu.Lock()
		flights := len(s.recordingHealthPageCache.Flights)
		s.recordingHealthPageCache.Mu.Unlock()
		if flights == 2 {
			break
		}
		if time.Now().After(flightDeadline) {
			t.Fatal("two distinct joined metric flights did not start")
		}
		time.Sleep(5 * time.Millisecond)
	}
	admissionDeadline := time.Now().Add(time.Second)
	for len(s.recordingMetricHeavyWorkSlots()) != 1 || len(s.recordingMetricWorkSlots()) != 1 {
		if time.Now().After(admissionDeadline) {
			t.Fatalf("joined admission heavy=%d total=%d, want one heavy loader occupying one of two total slots", len(s.recordingMetricHeavyWorkSlots()), len(s.recordingMetricWorkSlots()))
		}
		time.Sleep(5 * time.Millisecond)
	}

	baseCtx, cancelBase := context.WithTimeout(context.Background(), time.Second)
	defer cancelBase()
	baseReq := withPrincipal(httptest.NewRequest(http.MethodGet, "/api/v1/account/recordings/700/capture-health?metrics=base", nil).WithContext(baseCtx), accountPrincipal{AccountID: 47}, "700")
	baseResponse := httptest.NewRecorder()
	started := time.Now()
	s.handleAccountRecordingCaptureHealth(baseResponse, baseReq)
	if baseResponse.Code != http.StatusOK {
		t.Fatalf("base status=%d body=%s", baseResponse.Code, baseResponse.Body.String())
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("base capture health lost reserved admission: %s", elapsed)
	}

	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	locked = false
	for range 2 {
		select {
		case status := <-joinedDone:
			if status != http.StatusOK {
				t.Fatalf("joined request status after releasing lock=%d", status)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("joined request did not finish after releasing lock")
		}
	}
}
