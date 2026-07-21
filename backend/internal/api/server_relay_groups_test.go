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
		CREATE TABLE relay_groups (id BIGINT PRIMARY KEY, account_id BIGINT NOT NULL, max_streams INT NOT NULL);
		CREATE TABLE nodes (id BIGINT PRIMARY KEY, account_id BIGINT NOT NULL, node_type TEXT NOT NULL, status TEXT NOT NULL, last_heartbeat_at TIMESTAMPTZ, relay_max_streams INT NOT NULL, relay_group_id BIGINT);
		CREATE TABLE streams (id BIGINT PRIMARY KEY, provider TEXT, source_page_url TEXT);
		CREATE TABLE storage_destinations (id BIGINT PRIMARY KEY);
		CREATE TABLE account_billing (account_id BIGINT PRIMARY KEY, has_payment_method BOOLEAN NOT NULL);
		CREATE TABLE recordings (id BIGINT PRIMARY KEY, account_id BIGINT NOT NULL, status TEXT NOT NULL, start_at TIMESTAMPTZ NOT NULL, end_at TIMESTAMPTZ, capture_via TEXT NOT NULL, stream_url TEXT NOT NULL, stream_id BIGINT, storage_destination_id BIGINT NOT NULL, target_fps INT);
		CREATE TABLE recording_jobs (id BIGINT PRIMARY KEY, recording_id BIGINT NOT NULL, status TEXT NOT NULL, scheduled_for TIMESTAMPTZ NOT NULL, kind TEXT NOT NULL, fire_at TIMESTAMPTZ NOT NULL, clip_duration_sec INT NOT NULL, lease_owner TEXT, lease_expires_at TIMESTAMPTZ, attempt_count INT NOT NULL DEFAULT 0, updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), window_end_at TIMESTAMPTZ);
		INSERT INTO relay_groups VALUES (1, 47, 1);
		INSERT INTO nodes VALUES (1, 47, 'relay', 'active', now(), 6, 1), (2, 47, 'relay', 'active', now(), 6, 1);
		INSERT INTO storage_destinations VALUES (1);
		INSERT INTO recordings VALUES
		  (1, 47, 'active', now()-interval '1 hour', NULL, 'relay', 'https://example.com/1.m3u8', NULL, 1, NULL),
		  (2, 47, 'active', now()-interval '1 hour', NULL, 'relay', 'https://example.com/2.m3u8', NULL, 1, NULL);
		INSERT INTO recording_jobs VALUES
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
			_, err := s.leaseRelayRecordingJob(ctx, nodePrincipal{NodeID: nodeID, AccountID: 47, NodeType: nodeTypeRelay}, true, recordingCaptureTimeoutMarginSec+recordingUploadMarginSec)
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
	if err := pool.QueryRow(ctx, recordingJobHeartbeatSQL, expiredJobID, expiredOwner, recordingCaptureTimeoutMarginSec+recordingUploadMarginSec).Scan(&renewedAt); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("expired heartbeat err=%v, want pgx.ErrNoRows", err)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO nodes VALUES (3, 47, 'relay', 'active', now(), 1, NULL), (4, 47, 'relay', 'active', now(), 1, NULL);
		INSERT INTO recordings VALUES
		  (3, 47, 'active', now()-interval '1 hour', NULL, 'relay', 'https://example.com/3.m3u8', NULL, 1, NULL),
		  (4, 47, 'active', now()-interval '1 hour', NULL, 'relay', 'https://example.com/4.m3u8', NULL, 1, NULL);
		INSERT INTO recording_jobs VALUES
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
			_, err := s.leaseRelayRecordingJob(ctx, nodePrincipal{NodeID: 3, AccountID: 47, NodeType: nodeTypeRelay}, true, recordingCaptureTimeoutMarginSec+recordingUploadMarginSec)
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

	if _, err := pool.Exec(ctx, `
		INSERT INTO nodes VALUES
		  (5, 47, 'relay', 'active', now(), 1, 1),
		  (6, 47, 'relay', 'active', now(), 1, 1);
		INSERT INTO recordings VALUES
		  (6, 47, 'active', now()-interval '1 hour', NULL, 'relay', 'https://example.com/6.m3u8', NULL, 1, NULL);
		INSERT INTO recording_jobs VALUES
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
		_, err := s.heartbeatRecordingJob(ctx, nodePrincipal{NodeID: 5, AccountID: 47, NodeType: nodeTypeRelay}, 5, "node:5")
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
		_, err := s.leaseRelayRecordingJob(ctx, nodePrincipal{NodeID: 6, AccountID: 47, NodeType: nodeTypeRelay}, true, recordingCaptureTimeoutMarginSec+recordingUploadMarginSec)
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
