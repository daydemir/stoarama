package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
)

func TestRecordingCanarySpecRefusesProductionAndAllowsIdleRelay(t *testing.T) {
	pool, cleanup := testRecordingLeasePool(t)
	defer cleanup()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO accounts (id) VALUES (42);
		INSERT INTO nodes (id, account_id, node_type, status, last_heartbeat_at, relay_max_streams)
		VALUES (7, 42, 'relay', 'active', now(), 2);
		INSERT INTO streams (id, provider, source_page_url)
		VALUES (9, 'TEST', 'https://example.test/camera');
		INSERT INTO recordings
			(id, stream_id, account_id, storage_destination_id, name, stream_url, status, start_at, capture_via)
		VALUES (11, 9, 42, 7, 'canary', 'https://example.test/live.m3u8', 'active', now()-interval '1 hour', 'relay'),
		       (12, 9, 42, 7, 'other', 'https://example.test/other.m3u8', 'active', now()-interval '1 hour', 'relay');
		INSERT INTO recording_jobs
			(id, recording_id, fire_at, scheduled_for, clip_duration_sec, status,
			 lease_owner, lease_expires_at, attempt_count, idempotency_key, kind, window_end_at)
		VALUES (12, 11, now(), now(), 60, 'leased', 'node:7', now()+interval '3 minutes', 1, 'canary-active', 'continuous_window', now()+interval '1 hour')
	`); err != nil {
		t.Fatal(err)
	}

	s := &Server{pool: pool}
	request := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/node/recordings/11/canary-spec", nil)
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("id", "11")
		ctx := context.WithValue(req.Context(), chi.RouteCtxKey, rctx)
		ctx = context.WithValue(ctx, nodePrincipalContextKey, nodePrincipal{
			NodeID: 7, AccountID: 42, NodeType: nodeTypeRelay,
		})
		req = req.WithContext(ctx)
		rec := httptest.NewRecorder()
		s.handleNodeRecordingCanaryStart(rec, req)
		return rec
	}

	if rec := request(); rec.Code != http.StatusConflict {
		t.Fatalf("active production status=%d body=%s", rec.Code, rec.Body.String())
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_jobs SET recording_id=12 WHERE id=12`); err != nil {
		t.Fatal(err)
	}
	if rec := request(); rec.Code != http.StatusConflict {
		t.Fatalf("other group production status=%d body=%s", rec.Code, rec.Body.String())
	}
	if _, err := pool.Exec(ctx, `
		UPDATE recording_jobs SET status='done', completed_at=now(), lease_expires_at=NULL WHERE id=12
		;
		INSERT INTO recording_jobs
			(id, recording_id, fire_at, scheduled_for, clip_duration_sec, status,
			 attempt_count, idempotency_key, kind, window_end_at)
		VALUES (14, 11, now()+interval '2 minutes', now()+interval '2 minutes', 60, 'pending', 0, 'canary-imminent', 'continuous_window', now()+interval '1 hour')
	`); err != nil {
		t.Fatal(err)
	}
	if rec := request(); rec.Code != http.StatusConflict {
		t.Fatalf("imminent production status=%d body=%s", rec.Code, rec.Body.String())
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_jobs SET scheduled_for=now()+interval '10 minutes', fire_at=now()+interval '10 minutes' WHERE id=14`); err != nil {
		t.Fatal(err)
	}
	rec := request()
	if rec.Code != http.StatusOK {
		t.Fatalf("idle relay status=%d body=%s", rec.Code, rec.Body.String())
	}
	var spec recordingCanarySpec
	if err := json.Unmarshal(rec.Body.Bytes(), &spec); err != nil {
		t.Fatal(err)
	}
	if spec.RecordingID != 11 || spec.NodeID != 7 || spec.StreamID != 9 {
		t.Fatalf("unexpected spec: %+v", spec)
	}
	if spec.ReservationID == "" || time.Until(spec.SafeUntil) < 2*time.Minute {
		t.Fatalf("safe_until=%s is too short", spec.SafeUntil)
	}
	// Production always outranks a diagnostic canary. Leasing must invalidate
	// the reservation rather than holding real capture work behind it.
	if _, err := pool.Exec(ctx, `
		INSERT INTO recording_jobs
			(id, recording_id, fire_at, scheduled_for, clip_duration_sec, status, attempt_count, idempotency_key, kind, window_end_at)
		VALUES (13, 11, now(), now(), 60, 'pending', 0, 'canary-reserved', 'continuous_window', now()+interval '1 hour')
	`); err != nil {
		t.Fatal(err)
	}
	job, err := s.leaseRelayRecordingJob(ctx, nodePrincipal{NodeID: 7, AccountID: 42, NodeType: nodeTypeRelay}, true, recordingCaptureTimeoutMarginSec+recordingUploadMarginSec, false)
	if err != nil {
		t.Fatalf("production did not preempt canary: %v", err)
	}
	if job.JobID != 13 {
		t.Fatalf("leased job=%d want 13", job.JobID)
	}
	var reservations int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM recording_canary_reservations WHERE expires_at>now()`).Scan(&reservations); err != nil {
		t.Fatal(err)
	}
	if reservations != 0 {
		t.Fatalf("production lease left %d active canary reservation(s)", reservations)
	}
	checkReq := httptest.NewRequest(http.MethodPost, "/", nil)
	checkRoute := chi.NewRouteContext()
	checkRoute.URLParams.Add("id", "11")
	checkRoute.URLParams.Add("reservationId", spec.ReservationID)
	checkReq = checkReq.WithContext(context.WithValue(context.WithValue(checkReq.Context(), chi.RouteCtxKey, checkRoute), nodePrincipalContextKey, nodePrincipal{
		NodeID: 7, AccountID: 42, NodeType: nodeTypeRelay,
	}))
	checkRec := httptest.NewRecorder()
	s.handleNodeRecordingCanaryCheck(checkRec, checkReq)
	if checkRec.Code != http.StatusConflict {
		t.Fatalf("preempted reservation check status=%d body=%s", checkRec.Code, checkRec.Body.String())
	}
}

func TestRecordingCanaryRefusesBusyHeartbeatAndSecondGroupReservation(t *testing.T) {
	pool, cleanup := testRecordingLeasePool(t)
	defer cleanup()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO accounts (id) VALUES (42);
		INSERT INTO relay_groups (id, account_id, max_streams) VALUES (3, 42, 4), (4, 42, 4);
		INSERT INTO nodes (id, account_id, node_type, status, last_heartbeat_at, relay_max_streams, relay_group_id, capabilities_jsonb)
		VALUES (7, 42, 'relay', 'active', now(), 2, 3, '{"active_jobs":1}'),
		       (8, 42, 'relay', 'active', now(), 2, 3, '{}'),
		       (9, 42, 'relay', 'active', now(), 2, 4, '{}');
		INSERT INTO recordings
			(id, account_id, storage_destination_id, name, stream_url, status, start_at, capture_via, preferred_relay_group_id)
		VALUES (11, 42, 7, 'first', 'https://example.test/first.m3u8', 'active', now()-interval '1 hour', 'relay', 3),
		       (12, 42, 7, 'second', 'https://example.test/second.m3u8', 'active', now()-interval '1 hour', 'relay', 3),
		       (13, 42, 7, 'other-group', 'https://example.test/third.m3u8', 'active', now()-interval '1 hour', 'relay', 4)
	`); err != nil {
		t.Fatal(err)
	}

	s := &Server{pool: pool}
	start := func(nodeID, recordingID int64) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("id", fmt.Sprint(recordingID))
		req = req.WithContext(context.WithValue(context.WithValue(req.Context(), chi.RouteCtxKey, rctx), nodePrincipalContextKey, nodePrincipal{
			NodeID: nodeID, AccountID: 42, NodeType: nodeTypeRelay,
		}))
		rec := httptest.NewRecorder()
		s.handleNodeRecordingCanaryStart(rec, req)
		return rec
	}

	if rec := start(8, 11); rec.Code != http.StatusConflict {
		t.Fatalf("busy group heartbeat status=%d body=%s", rec.Code, rec.Body.String())
	}
	if _, err := pool.Exec(ctx, `UPDATE nodes SET capabilities_jsonb='{"active_jobs":"malformed"}'::jsonb WHERE id=7`); err != nil {
		t.Fatal(err)
	}
	if rec := start(8, 11); rec.Code != http.StatusConflict {
		t.Fatalf("malformed active_jobs must fail closed: status=%d body=%s", rec.Code, rec.Body.String())
	}
	if _, err := pool.Exec(ctx, `UPDATE nodes SET capabilities_jsonb='{}'::jsonb WHERE id=7`); err != nil {
		t.Fatal(err)
	}
	if rec := start(8, 11); rec.Code != http.StatusOK {
		t.Fatalf("first group canary status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := start(7, 12); rec.Code != http.StatusConflict {
		t.Fatalf("second same-group canary status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := start(9, 13); rec.Code != http.StatusOK {
		t.Fatalf("different-group canary status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestRecordingCanarySpecTenantAndNodeTypeWalls(t *testing.T) {
	pool, cleanup := testRecordingLeasePool(t)
	defer cleanup()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO accounts (id) VALUES (42), (43);
		INSERT INTO nodes (id, account_id, node_type, status, last_heartbeat_at, relay_max_streams)
		VALUES (7, 42, 'relay', 'active', now(), 2),
		       (8, 43, 'relay', 'active', now(), 2);
		INSERT INTO recordings
			(id, account_id, storage_destination_id, name, stream_url, status, start_at, capture_via)
		VALUES (11, 42, 7, 'canary', 'https://example.test/live.m3u8', 'active', now()-interval '1 hour', 'relay')
	`); err != nil {
		t.Fatal(err)
	}

	s := &Server{pool: pool}
	for _, tc := range []struct {
		principal nodePrincipal
		want      int
	}{
		{principal: nodePrincipal{NodeID: 8, AccountID: 43, NodeType: nodeTypeRelay}, want: http.StatusNotFound},
		{principal: nodePrincipal{NodeID: 7, AccountID: 42, NodeType: nodeTypeLocalRecorder}, want: http.StatusForbidden},
	} {
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("id", "11")
		req = req.WithContext(context.WithValue(context.WithValue(req.Context(), chi.RouteCtxKey, rctx), nodePrincipalContextKey, tc.principal))
		rec := httptest.NewRecorder()
		s.handleNodeRecordingCanaryStart(rec, req)
		if rec.Code != tc.want {
			t.Fatalf("principal=%+v status=%d want=%d body=%s", tc.principal, rec.Code, tc.want, rec.Body.String())
		}
	}
}

func TestRecordingCanaryCheckAndFinishAreOwnerBound(t *testing.T) {
	pool, cleanup := testRecordingLeasePool(t)
	defer cleanup()
	ctx := context.Background()
	var firstID, secondID string
	if _, err := pool.Exec(ctx, `
		INSERT INTO accounts (id) VALUES (42);
		INSERT INTO nodes (id, account_id, node_type, status, last_heartbeat_at, relay_max_streams)
		VALUES (7, 42, 'relay', 'active', now(), 2), (8, 42, 'relay', 'active', now(), 2);
		INSERT INTO recordings
			(id, account_id, storage_destination_id, name, stream_url, status, start_at, capture_via)
		VALUES (11, 42, 7, 'canary', 'https://example.test/live.m3u8', 'active', now()-interval '1 hour', 'relay')
	`); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO recording_canary_reservations (recording_id, node_id, expires_at)
		VALUES (11, 7, now()+interval '3 minutes') RETURNING id::text
	`).Scan(&firstID); err != nil {
		t.Fatal(err)
	}
	s := &Server{pool: pool}
	call := func(handler http.HandlerFunc, nodeID int64, reservationID string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("id", "11")
		rctx.URLParams.Add("reservationId", reservationID)
		req = req.WithContext(context.WithValue(context.WithValue(req.Context(), chi.RouteCtxKey, rctx), nodePrincipalContextKey, nodePrincipal{
			NodeID: nodeID, AccountID: 42, NodeType: nodeTypeRelay,
		}))
		rec := httptest.NewRecorder()
		handler(rec, req)
		return rec
	}
	if rec := call(s.handleNodeRecordingCanaryCheck, 8, firstID); rec.Code != http.StatusConflict {
		t.Fatalf("other-node check status=%d body=%s", rec.Code, rec.Body.String())
	}
	if _, err := pool.Exec(ctx, `DELETE FROM recording_canary_reservations WHERE id=$1`, firstID); err != nil {
		t.Fatal(err)
	}
	if rec := call(s.handleNodeRecordingCanaryCheck, 7, firstID); rec.Code != http.StatusConflict {
		t.Fatalf("deleted check status=%d body=%s", rec.Code, rec.Body.String())
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO recording_canary_reservations (recording_id, node_id, expires_at)
		VALUES (11, 7, now()+interval '3 minutes') RETURNING id::text
	`).Scan(&secondID); err != nil {
		t.Fatal(err)
	}
	if rec := call(s.handleNodeRecordingCanaryFinish, 7, secondID); rec.Code != http.StatusOK {
		t.Fatalf("finish status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := call(s.handleNodeRecordingCanaryCheck, 7, secondID); rec.Code != http.StatusConflict {
		t.Fatalf("post-finish check status=%d body=%s", rec.Code, rec.Body.String())
	}
}
