package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRelayGroupLimits(t *testing.T) {
	for _, max := range []int{relayGroupMinMaxStreams, relayGroupDefaultMaxStreams, relayGroupMaxMaxStreams} {
		if err := validateRelayGroupMaxStreams(max); err != nil {
			t.Fatalf("max %d rejected: %v", max, err)
		}
	}
	for _, max := range []int{relayGroupMinMaxStreams - 1, relayGroupMaxMaxStreams + 1} {
		if err := validateRelayGroupMaxStreams(max); err == nil {
			t.Fatalf("max %d accepted", max)
		}
	}
}

func TestRelayGroupChangeAllowed(t *testing.T) {
	group1, group2 := int64(1), int64(2)
	for _, tc := range []struct {
		name            string
		current, target *int64
		liveLeases      int
		want            bool
	}{
		{name: "idle move", current: &group1, target: &group2, want: true},
		{name: "busy first assignment", target: &group1, liveLeases: 1, want: true},
		{name: "busy no-op", current: &group1, target: &group1, liveLeases: 1, want: true},
		{name: "busy move", current: &group1, target: &group2, liveLeases: 1},
		{name: "busy unassign", current: &group1, liveLeases: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := relayGroupChangeAllowed(tc.current, tc.target, tc.liveLeases); got != tc.want {
				t.Fatalf("got %t want %t", got, tc.want)
			}
		})
	}
}

func TestRelayLeaseSQLIncludesTenantScopedGroupCap(t *testing.T) {
	for _, want := range []string{
		"n.relay_group_id IS NULL",
		"j.relay_fairness_started_at <= now()-interval '12 seconds'",
		"peer_group.id<>n.relay_group_id",
		"n.relay_group_id=rec.preferred_relay_group_id",
		"preferred_group.id=rec.preferred_relay_group_id",
		"peer_group_node.last_heartbeat_at>=now()-interval '120 seconds'",
		"peer_group_jobs.lease_expires_at>now()",
		"peer.relay_group_id=n.relay_group_id",
		"peer.last_heartbeat_at >= now()-interval '120 seconds'",
		"pj.lease_owner='node:'||peer.id::text",
		"gn.account_id=n.account_id",
		"gn.relay_group_id=n.relay_group_id",
		"g.account_id=n.account_id",
	} {
		if !strings.Contains(relayLeaseSQL, want) {
			t.Fatalf("relay lease SQL missing %q", want)
		}
	}
}

func TestRecordingHeartbeatCannotReviveExpiredLease(t *testing.T) {
	if !strings.Contains(recordingJobHeartbeatSQL, "j.lease_expires_at > now()") {
		t.Fatal("recording heartbeat must reject expired leases")
	}
}

func TestRelayGroupLeaseCapConcurrent(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run DB-backed relay group test")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("relay_group_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`) }()
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, `
		CREATE TABLE accounts (id BIGINT PRIMARY KEY);
		CREATE TABLE relay_groups (id BIGINT PRIMARY KEY, account_id BIGINT NOT NULL, max_streams INT NOT NULL);
		CREATE TABLE nodes (id BIGINT PRIMARY KEY, account_id BIGINT NOT NULL, node_type TEXT NOT NULL, status TEXT NOT NULL, last_heartbeat_at TIMESTAMPTZ, relay_max_streams INT NOT NULL, relay_group_id BIGINT);
		CREATE TABLE streams (id BIGINT PRIMARY KEY, provider TEXT, source_page_url TEXT);
		CREATE TABLE storage_destinations (id BIGINT PRIMARY KEY);
		CREATE TABLE account_billing (account_id BIGINT PRIMARY KEY, has_payment_method BOOLEAN NOT NULL);
		CREATE TABLE recordings (id BIGINT PRIMARY KEY, account_id BIGINT NOT NULL, status TEXT NOT NULL, start_at TIMESTAMPTZ NOT NULL, end_at TIMESTAMPTZ, capture_via TEXT NOT NULL, stream_url TEXT NOT NULL, stream_id BIGINT, storage_destination_id BIGINT NOT NULL, target_fps INT, preferred_relay_group_id BIGINT);
		CREATE TABLE recording_jobs (id BIGINT PRIMARY KEY, recording_id BIGINT NOT NULL, status TEXT NOT NULL, scheduled_for TIMESTAMPTZ NOT NULL, kind TEXT NOT NULL, fire_at TIMESTAMPTZ NOT NULL, clip_duration_sec INT NOT NULL, lease_owner TEXT, lease_expires_at TIMESTAMPTZ, lease_token UUID, attempt_count INT NOT NULL DEFAULT 0, updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), window_end_at TIMESTAMPTZ, handoff_owner TEXT, handoff_until TIMESTAMPTZ, relay_fairness_started_at TIMESTAMPTZ);
		INSERT INTO accounts VALUES (47);
		INSERT INTO relay_groups VALUES (1, 47, 1);
		INSERT INTO nodes VALUES (1, 47, 'relay', 'active', now(), 6, 1), (2, 47, 'relay', 'active', now(), 6, 1);
		INSERT INTO storage_destinations VALUES (1);
		INSERT INTO recordings VALUES
		  (1, 47, 'active', now()-interval '1 hour', NULL, 'relay', 'https://example.com/1.m3u8', NULL, 1, NULL),
		  (2, 47, 'active', now()-interval '1 hour', NULL, 'relay', 'https://example.com/2.m3u8', NULL, 1, NULL);
		INSERT INTO recording_jobs (id,recording_id,status,scheduled_for,kind,fire_at,clip_duration_sec,lease_owner,lease_expires_at,attempt_count,updated_at,window_end_at) VALUES
		  (1, 1, 'pending', now()-interval '1 second', 'clip', now(), 60, NULL, NULL, 0, now(), NULL),
		  (2, 2, 'pending', now()-interval '1 second', 'clip', now(), 60, NULL, NULL, 0, now(), NULL);
	`); err != nil {
		t.Fatal(err)
	}

	s := &Server{pool: pool}
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, nodeID := range []int64{1, 2} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.leaseRelayRecordingJob(ctx, nodePrincipal{NodeID: nodeID, AccountID: 47, NodeType: nodeTypeRelay}, true, recordingCaptureTimeoutMarginSec+recordingUploadMarginSec, false)
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	leased, empty := 0, 0
	for err := range results {
		switch {
		case err == nil:
			leased++
		case errors.Is(err, pgx.ErrNoRows):
			empty++
		default:
			t.Fatalf("lease error: %v", err)
		}
	}
	if leased != 1 || empty != 1 {
		t.Fatalf("leased=%d empty=%d, want 1/1", leased, empty)
	}
	var expiredJobID int64
	var expiredOwner string
	if err := pool.QueryRow(ctx, `SELECT id, lease_owner FROM recording_jobs WHERE status='leased' LIMIT 1`).Scan(&expiredJobID, &expiredOwner); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_jobs SET lease_expires_at=now()-interval '1 second' WHERE id=$1`, expiredJobID); err != nil {
		t.Fatal(err)
	}
	var renewedAt time.Time
	if err := pool.QueryRow(ctx, recordingJobHeartbeatSQL, expiredJobID, expiredOwner, recordingCaptureTimeoutMarginSec+recordingUploadMarginSec, nil).Scan(&renewedAt); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("expired heartbeat err=%v, want pgx.ErrNoRows", err)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO recordings VALUES
		  (7,47,'active',now()-interval '1 hour',NULL,'relay','https://example.com/7.m3u8',NULL,1,NULL),
		  (8,47,'active',now()-interval '1 hour',NULL,'relay','https://example.com/8.m3u8',NULL,1,NULL);
		INSERT INTO recording_jobs
		  (id,recording_id,status,scheduled_for,kind,fire_at,clip_duration_sec,lease_owner,lease_expires_at,attempt_count,updated_at,window_end_at,handoff_owner,handoff_until)
		VALUES
		  (7,7,'pending',now()-interval '2 minutes','continuous_window',now(),60,NULL,NULL,0,now(),now()+interval '1 hour','node:1',now()+interval '5 minutes'),
		  (8,8,'pending',now()-interval '1 minute','continuous_window',now(),60,NULL,NULL,0,now(),now()+interval '1 hour',NULL,NULL);
	`); err != nil {
		t.Fatal(err)
	}
	handoffLease, err := s.leaseRelayRecordingJob(ctx, nodePrincipal{NodeID: 1, AccountID: 47, NodeType: nodeTypeRelay}, true, recordingCaptureTimeoutMarginSec+recordingUploadMarginSec, false)
	if err != nil {
		t.Fatalf("lease behind handed-off oldest job: %v", err)
	}
	if handoffLease.JobID != 8 {
		t.Fatalf("leased job=%d want later eligible job 8", handoffLease.JobID)
	}
	var handedOffFairness *time.Time
	if err := pool.QueryRow(ctx, `SELECT relay_fairness_started_at FROM recording_jobs WHERE id=7`).Scan(&handedOffFairness); err != nil {
		t.Fatal(err)
	}
	if handedOffFairness != nil {
		t.Fatal("handed-off job incorrectly started its fairness timer")
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_jobs SET status='complete',lease_expires_at=NULL WHERE id IN (7,8)`); err != nil {
		t.Fatal(err)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO recordings VALUES
		  (9,47,'active',now()-interval '1 hour',NULL,'relay','https://example.com/9.m3u8',NULL,1,NULL);
		INSERT INTO recording_jobs
		  (id,recording_id,status,scheduled_for,kind,fire_at,clip_duration_sec,lease_owner,lease_expires_at,attempt_count,updated_at,window_end_at)
		VALUES
		  (9,9,'pending',now()-interval '1 minute','continuous_window',now(),60,NULL,NULL,0,now(),now()+interval '1 hour');
		INSERT INTO account_billing VALUES (47,false);
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.leaseRelayRecordingJob(ctx, nodePrincipal{NodeID: 1, AccountID: 47, NodeType: nodeTypeRelay}, false, recordingCaptureTimeoutMarginSec+recordingUploadMarginSec, false); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("lease without payment err=%v, want pgx.ErrNoRows", err)
	}
	var unpaidFairness *time.Time
	if err := pool.QueryRow(ctx, `SELECT relay_fairness_started_at FROM recording_jobs WHERE id=9`).Scan(&unpaidFairness); err != nil {
		t.Fatal(err)
	}
	if unpaidFairness != nil {
		t.Fatal("unpaid job incorrectly started its fairness timer")
	}
	if _, err := pool.Exec(ctx, `UPDATE account_billing SET has_payment_method=true WHERE account_id=47`); err != nil {
		t.Fatal(err)
	}
	paidLease, err := s.leaseRelayRecordingJob(ctx, nodePrincipal{NodeID: 1, AccountID: 47, NodeType: nodeTypeRelay}, false, recordingCaptureTimeoutMarginSec+recordingUploadMarginSec, false)
	if err != nil {
		t.Fatalf("lease after payment became available: %v", err)
	}
	if paidLease.JobID != 9 {
		t.Fatalf("leased job=%d want newly eligible job 9", paidLease.JobID)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_jobs SET status='complete',lease_expires_at=NULL WHERE id=9`); err != nil {
		t.Fatal(err)
	}

	if _, err := pool.Exec(ctx, `
		UPDATE relay_groups SET max_streams=10 WHERE id=1;
		INSERT INTO recordings VALUES
		  (10, 47, 'active', now()-interval '1 hour', NULL, 'relay', 'https://example.com/10.m3u8', NULL, 1, NULL),
		  (11, 47, 'active', now()-interval '1 hour', NULL, 'relay', 'https://example.com/11.m3u8', NULL, 1, NULL),
		  (12, 47, 'active', now()-interval '1 hour', NULL, 'relay', 'https://example.com/12.m3u8', NULL, 1, NULL);
		INSERT INTO recording_jobs (id,recording_id,status,scheduled_for,kind,fire_at,clip_duration_sec,lease_owner,lease_expires_at,attempt_count,updated_at,window_end_at) VALUES
		  (10, 10, 'pending', now()-interval '1 second', 'continuous_window', now(), 60, NULL, NULL, 0, now(), now()+interval '1 hour'),
		  (11, 11, 'pending', now()-interval '1 second', 'continuous_window', now(), 60, NULL, NULL, 0, now(), now()+interval '1 hour'),
		  (12, 12, 'pending', now()-interval '1 second', 'continuous_window', now(), 60, NULL, NULL, 0, now(), now()+interval '1 hour');
	`); err != nil {
		t.Fatal(err)
	}
	lease := func(nodeID int64) error {
		_, err := s.leaseRelayRecordingJob(ctx, nodePrincipal{NodeID: nodeID, AccountID: 47, NodeType: nodeTypeRelay}, true, recordingCaptureTimeoutMarginSec+recordingUploadMarginSec, false)
		return err
	}
	if err := lease(1); err != nil {
		t.Fatalf("first balanced lease: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_jobs SET scheduled_for=now() WHERE id IN (11,12)`); err != nil {
		t.Fatal(err)
	}
	if err := lease(1); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("busier node lease err=%v, want pgx.ErrNoRows", err)
	}
	if err := lease(2); err != nil {
		t.Fatalf("least-loaded peer lease: %v", err)
	}
	var node1Leases, node2Leases int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FILTER(WHERE lease_owner='node:1'),count(*) FILTER(WHERE lease_owner='node:2')
		FROM recording_jobs WHERE status='leased' AND lease_expires_at>now()
	`).Scan(&node1Leases, &node2Leases); err != nil {
		t.Fatal(err)
	}
	if node1Leases != 1 || node2Leases != 1 {
		t.Fatalf("balanced leases node1=%d node2=%d, want 1/1", node1Leases, node2Leases)
	}
	if err := lease(1); err != nil {
		t.Fatalf("tie permits either healthy peer: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO recordings VALUES
		  (13,47,'active',now()-interval '1 hour',NULL,'relay','https://example.com/13.m3u8',NULL,1,NULL);
		INSERT INTO recording_jobs (id,recording_id,status,scheduled_for,kind,fire_at,clip_duration_sec,lease_owner,lease_expires_at,attempt_count,updated_at,window_end_at) VALUES
		  (13,13,'pending',now()-interval '1 hour','continuous_window',now(),60,NULL,NULL,0,now(),now()+interval '1 hour');
	`); err != nil {
		t.Fatal(err)
	}
	if err := lease(1); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("nonpolling peer must receive bounded first opportunity: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_jobs SET relay_fairness_started_at=now()-interval '13 seconds' WHERE id=13`); err != nil {
		t.Fatal(err)
	}
	if err := lease(1); err != nil {
		t.Fatalf("overdue fairness fallback lease: %v", err)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_groups VALUES (10,47,10);
		INSERT INTO nodes VALUES (15,47,'relay','active',now(),6,10);
		INSERT INTO recordings VALUES
		  (20,47,'active',now()-interval '1 hour',NULL,'relay','https://example.com/20.m3u8',NULL,1,NULL),
		  (21,47,'active',now()-interval '1 hour',NULL,'relay','https://example.com/21.m3u8',NULL,1,NULL),
		  (22,47,'active',now()-interval '1 hour',NULL,'relay','https://example.com/22.m3u8',NULL,1,NULL),
		  (23,47,'active',now()-interval '1 hour',NULL,'relay','https://example.com/23.m3u8',NULL,1,NULL);
		INSERT INTO recording_jobs (id,recording_id,status,scheduled_for,kind,fire_at,clip_duration_sec,lease_owner,lease_expires_at,attempt_count,updated_at,window_end_at) VALUES
		  (20,20,'pending',now()-interval '1 hour','continuous_window',now(),60,NULL,NULL,0,now(),now()+interval '1 hour'),
		  (21,21,'pending',now()-interval '1 hour','continuous_window',now(),60,NULL,NULL,0,now(),now()+interval '1 hour'),
		  (22,22,'pending',now()-interval '1 hour','continuous_window',now(),60,NULL,NULL,0,now(),now()+interval '1 hour'),
		  (23,23,'pending',now()-interval '1 hour','continuous_window',now(),60,NULL,NULL,0,now(),now()+interval '1 hour');
	`); err != nil {
		t.Fatal(err)
	}
	if err := lease(1); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("busier internet group lease err=%v, want pgx.ErrNoRows", err)
	}
	if err := lease(15); err != nil {
		t.Fatalf("least-loaded internet group lease: %v", err)
	}
	for range 3 {
		if err := lease(15); err != nil {
			t.Fatalf("least-loaded internet group catch-up lease: %v", err)
		}
	}
	var group1Leases, group10Leases int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FILTER(WHERE n.relay_group_id=1),count(*) FILTER(WHERE n.relay_group_id=10)
		FROM recording_jobs j JOIN nodes n ON j.lease_owner='node:'||n.id::text
		WHERE j.status='leased' AND j.lease_expires_at>now()
	`).Scan(&group1Leases, &group10Leases); err != nil {
		t.Fatal(err)
	}
	if group1Leases != group10Leases {
		t.Fatalf("overdue batch group loads=%d/%d want equal", group1Leases, group10Leases)
	}

	// A soft preference gives the selected independent uplink the first opportunity
	// even when ordinary least-load balancing is tied. It must never become a pin:
	// an unavailable preferred group falls back immediately, and a heartbeat-fresh
	// group that does not poll delays capture only for the bounded fairness turn.
	if _, err := pool.Exec(ctx, `
		INSERT INTO recordings (id,account_id,status,start_at,capture_via,stream_url,storage_destination_id,preferred_relay_group_id)
		VALUES (24,47,'active',now()-interval '1 hour','relay','https://example.com/24.m3u8',1,10);
		INSERT INTO recording_jobs (id,recording_id,status,scheduled_for,kind,fire_at,clip_duration_sec,attempt_count,updated_at,window_end_at)
		VALUES (24,24,'pending',now(),'continuous_window',now(),60,0,now(),now()+interval '1 hour');
	`); err != nil {
		t.Fatal(err)
	}
	if err := lease(1); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("nonpreferred group took preferred job: %v", err)
	}
	if err := lease(15); err != nil {
		t.Fatalf("preferred group did not take job: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE recording_jobs SET status='complete',lease_owner=NULL,lease_expires_at=NULL WHERE id=24;
		UPDATE nodes SET last_heartbeat_at=now()-interval '3 minutes' WHERE id=15;
		INSERT INTO recordings (id,account_id,status,start_at,capture_via,stream_url,storage_destination_id,preferred_relay_group_id)
		VALUES (25,47,'active',now()-interval '1 hour','relay','https://example.com/25.m3u8',1,10);
		INSERT INTO recording_jobs (id,recording_id,status,scheduled_for,kind,fire_at,clip_duration_sec,attempt_count,updated_at,window_end_at)
		VALUES (25,25,'pending',now(),'continuous_window',now(),60,0,now(),now()+interval '1 hour');
	`); err != nil {
		t.Fatal(err)
	}
	if err := lease(1); err != nil {
		t.Fatalf("offline preferred group blocked fallback: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE recording_jobs SET status='complete',lease_owner=NULL,lease_expires_at=NULL WHERE id=25;
		UPDATE nodes SET last_heartbeat_at=now() WHERE id=15;
		INSERT INTO recordings (id,account_id,status,start_at,capture_via,stream_url,storage_destination_id,preferred_relay_group_id)
		VALUES (26,47,'active',now()-interval '1 hour','relay','https://example.com/26.m3u8',1,10);
		INSERT INTO recording_jobs (id,recording_id,status,scheduled_for,kind,fire_at,clip_duration_sec,attempt_count,updated_at,window_end_at)
		VALUES (26,26,'pending',now(),'continuous_window',now(),60,0,now(),now()+interval '1 hour');
	`); err != nil {
		t.Fatal(err)
	}
	if err := lease(1); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("healthy preferred group did not receive first opportunity: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_jobs SET relay_fairness_started_at=now()-interval '13 seconds' WHERE id=26`); err != nil {
		t.Fatal(err)
	}
	if err := lease(1); err != nil {
		t.Fatalf("nonpolling preferred group blocked bounded fallback: %v", err)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO nodes VALUES (3, 47, 'relay', 'active', now(), 1, NULL), (4, 47, 'relay', 'active', now(), 1, NULL);
		INSERT INTO recordings VALUES
		  (3, 47, 'active', now()-interval '1 hour', NULL, 'relay', 'https://example.com/3.m3u8', NULL, 1, NULL),
		  (4, 47, 'active', now()-interval '1 hour', NULL, 'relay', 'https://example.com/4.m3u8', NULL, 1, NULL);
		INSERT INTO recording_jobs (id,recording_id,status,scheduled_for,kind,fire_at,clip_duration_sec,lease_owner,lease_expires_at,attempt_count,updated_at,window_end_at) VALUES
		  (3, 3, 'pending', now()-interval '1 second', 'clip', now(), 60, NULL, NULL, 0, now(), NULL),
		  (4, 4, 'pending', now()-interval '1 second', 'clip', now(), 60, NULL, NULL, 0, now(), NULL);
	`); err != nil {
		t.Fatal(err)
	}
	results = make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.leaseRelayRecordingJob(ctx, nodePrincipal{NodeID: 3, AccountID: 47, NodeType: nodeTypeRelay}, true, recordingCaptureTimeoutMarginSec+recordingUploadMarginSec, false)
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	leased, empty = 0, 0
	for err := range results {
		if err == nil {
			leased++
		} else if errors.Is(err, pgx.ErrNoRows) {
			empty++
		} else {
			t.Fatalf("ungrouped lease error: %v", err)
		}
	}
	if leased != 1 || empty != 1 {
		t.Fatalf("ungrouped leased=%d empty=%d, want 1/1", leased, empty)
	}

	if _, err := pool.Exec(ctx, `INSERT INTO relay_groups VALUES (2, 48, 1)`); err != nil {
		t.Fatal(err)
	}
	routeCtx := chi.NewRouteContext()
	routeCtx.URLParams.Add("id", "4")
	routeCtx.URLParams.Add("group_id", "2")
	req := httptest.NewRequest(http.MethodPut, "/api/v1/account/nodes/4/relay-group/2", nil)
	req = req.WithContext(context.WithValue(context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: 47}), chi.RouteCtxKey, routeCtx))
	rec := httptest.NewRecorder()
	s.handleAccountNodeRelayGroupPut(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("cross-account assignment status=%d body=%s", rec.Code, rec.Body.String())
	}

	routingRequest := func(body string) *httptest.ResponseRecorder {
		routeCtx := chi.NewRouteContext()
		routeCtx.URLParams.Add("id", "24")
		req := httptest.NewRequest(http.MethodPatch, "/api/v1/account/recordings/24/relay-routing", strings.NewReader(body))
		req = req.WithContext(context.WithValue(context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: 47}), chi.RouteCtxKey, routeCtx))
		rec := httptest.NewRecorder()
		s.handleAccountRecordingRelayRouting(rec, req)
		return rec
	}
	if rec := routingRequest(`{"preferred_relay_group_id":2}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("cross-account preferred group status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := routingRequest(`{"preferred_relay_group_id":10}`); rec.Code != http.StatusOK {
		t.Fatalf("set preferred group status=%d body=%s", rec.Code, rec.Body.String())
	}
	var preferredGroupID *int64
	if err := pool.QueryRow(ctx, `SELECT preferred_relay_group_id FROM recordings WHERE id=24`).Scan(&preferredGroupID); err != nil {
		t.Fatal(err)
	}
	if preferredGroupID == nil || *preferredGroupID != 10 {
		t.Fatalf("preferred group=%v want 10", preferredGroupID)
	}
	if rec := routingRequest(`{"preferred_relay_group_id":null}`); rec.Code != http.StatusOK {
		t.Fatalf("clear preferred group status=%d body=%s", rec.Code, rec.Body.String())
	}
	if err := pool.QueryRow(ctx, `SELECT preferred_relay_group_id FROM recordings WHERE id=24`).Scan(&preferredGroupID); err != nil {
		t.Fatal(err)
	}
	if preferredGroupID != nil {
		t.Fatalf("preferred group not cleared: %v", *preferredGroupID)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO nodes VALUES
		  (5, 47, 'relay', 'active', now(), 1, 1),
		  (6, 47, 'relay', 'active', now(), 1, 1);
		INSERT INTO recordings VALUES
		  (6, 47, 'active', now()-interval '1 hour', NULL, 'relay', 'https://example.com/6.m3u8', NULL, 1, NULL);
		INSERT INTO recording_jobs (id,recording_id,status,scheduled_for,kind,fire_at,clip_duration_sec,lease_owner,lease_expires_at,attempt_count,updated_at,window_end_at) VALUES
		  (5, 5, 'leased', now(), 'continuous_window', now(), 60, 'node:5', now()+interval '500 milliseconds', 1, now(), NULL),
		  (6, 6, 'pending', now()-interval '1 second', 'clip', now(), 60, NULL, NULL, 0, now(), NULL);
	`); err != nil {
		t.Fatal(err)
	}
	jobLock, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = jobLock.Rollback(ctx) }()
	var lockedJobID int64
	if err := jobLock.QueryRow(ctx, `SELECT id FROM recording_jobs WHERE id=5 FOR UPDATE`).Scan(&lockedJobID); err != nil {
		t.Fatal(err)
	}
	heartbeatDone := make(chan error, 1)
	go func() {
		_, err := s.heartbeatRecordingJob(ctx, nodePrincipal{NodeID: 5, AccountID: 47, NodeType: nodeTypeRelay}, 5, "node:5", nil)
		heartbeatDone <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		probe, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var nodeID int64
		err = probe.QueryRow(ctx, `SELECT id FROM nodes WHERE id=5 FOR UPDATE NOWAIT`).Scan(&nodeID)
		_ = probe.Rollback(ctx)
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "55P03" {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("heartbeat did not lock relay node")
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(600 * time.Millisecond)
	leaseDone := make(chan error, 1)
	go func() {
		_, err := s.leaseRelayRecordingJob(ctx, nodePrincipal{NodeID: 6, AccountID: 47, NodeType: nodeTypeRelay}, true, recordingCaptureTimeoutMarginSec+recordingUploadMarginSec, false)
		leaseDone <- err
	}()
	select {
	case <-leaseDone:
		t.Fatal("second grouped lease did not wait for in-flight heartbeat")
	case <-time.After(100 * time.Millisecond):
	}
	transitionDone := make(chan bool, 1)
	go func() {
		tx, err := pool.Begin(ctx)
		if err != nil {
			transitionDone <- true
			return
		}
		defer func() { _ = tx.Rollback(ctx) }()
		groupID, liveLeases, err := lockRelayNode(ctx, tx, 5, 47)
		transitionDone <- err != nil || relayGroupChangeAllowed(groupID, nil, liveLeases)
	}()
	select {
	case <-transitionDone:
		t.Fatal("membership transition did not wait for in-flight heartbeat")
	case <-time.After(100 * time.Millisecond):
	}
	if err := jobLock.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-heartbeatDone; err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if err := <-leaseDone; !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("second grouped lease err=%v, want pgx.ErrNoRows", err)
	}
	if allowed := <-transitionDone; allowed {
		t.Fatal("busy grouped node became removable after in-flight heartbeat")
	}
}
