package api

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func leaseAs(t *testing.T, s *Server, nodeID int64) (recordingLeaseResponse, error) {
	t.Helper()
	return s.leaseRelayRecordingJob(context.Background(), nodePrincipal{NodeID: nodeID, AccountID: 42, NodeType: nodeTypeRelay}, true, recordingCaptureTimeoutMarginSec+recordingUploadMarginSec, false)
}

func resetWindow(t *testing.T, pool *pgxpool.Pool, fairnessAge string) {
	t.Helper()
	fairness := "NULL"
	if fairnessAge != "" {
		fairness = "now()-interval '" + fairnessAge + "'"
	}
	if _, err := pool.Exec(context.Background(), `
		UPDATE recording_jobs SET status='pending', lease_owner=NULL, lease_expires_at=NULL,
		       lease_token=NULL, handoff_owner=NULL, handoff_until=NULL,
		       relay_fairness_started_at=`+fairness+`
		WHERE id=10`); err != nil {
		t.Fatal(err)
	}
}

// Recording 1's previous window was completed by node 2. Node 2 already carries
// another stream, so plain least-loaded balancing would hand today's window to
// idle node 1 and move the stream.
func seedStickyWindow(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO accounts (id) VALUES (42);
		INSERT INTO relay_groups (id, account_id, max_streams) VALUES (1, 42, 10);
		INSERT INTO nodes (id, account_id, node_type, status, last_heartbeat_at, relay_max_streams, relay_group_id)
		VALUES (1, 42, 'relay', 'active', now(), 4, 1),
		       (2, 42, 'relay', 'active', now(), 4, 1);
		INSERT INTO recordings (id, account_id, storage_destination_id, name, stream_url, status, start_at, capture_via)
		VALUES (1, 42, 7, 'sticky', 'https://example.test/1.m3u8', 'active', now()-interval '3 days', 'relay'),
		       (2, 42, 7, 'other', 'https://example.test/2.m3u8', 'active', now()-interval '3 days', 'relay');
		INSERT INTO recording_jobs
			(id, recording_id, fire_at, scheduled_for, clip_duration_sec, status, lease_owner, lease_expires_at, idempotency_key, kind, window_end_at)
		VALUES
			(9, 1, now()-interval '1 day', now()-interval '1 day', 60, 'done', 'node:2', NULL, 'yesterday', 'continuous_window', now()-interval '12 hours'),
			(10, 1, now(), now()-interval '1 second', 60, 'pending', NULL, NULL, 'today', 'continuous_window', now()+interval '12 hours'),
			(20, 2, now(), now()-interval '1 hour', 60, 'leased', 'node:2', now()+interval '3 minutes', 'other', 'continuous_window', now()+interval '12 hours');
	`); err != nil {
		t.Fatal(err)
	}
}

func TestRelayLeaseKeepsWindowOnPreviousNode(t *testing.T) {
	pool, cleanup := testRecordingLeasePool(t)
	defer cleanup()
	seedStickyWindow(t, pool)
	s := &Server{pool: pool}

	if _, err := leaseAs(t, s, 1); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("idle peer took a sticky window during the fairness turn: err=%v", err)
	}
	job, err := leaseAs(t, s, 2)
	if err != nil || job.JobID != 10 {
		t.Fatalf("affinity node lease=%+v err=%v want job 10", job, err)
	}

	// Bounded: once the turn ages out, any eligible node may take the window.
	resetWindow(t, pool, "13 seconds")
	if job, err := leaseAs(t, s, 1); err != nil || job.JobID != 10 {
		t.Fatalf("peer after fairness turn lease=%+v err=%v want job 10", job, err)
	}

	// An offline affinity node never holds a window hostage.
	resetWindow(t, pool, "")
	if _, err := pool.Exec(context.Background(), `UPDATE nodes SET last_heartbeat_at=now()-interval '10 minutes' WHERE id=2`); err != nil {
		t.Fatal(err)
	}
	if job, err := leaseAs(t, s, 1); err != nil || job.JobID != 10 {
		t.Fatalf("peer with offline affinity node lease=%+v err=%v want job 10", job, err)
	}
}

func TestRelayLeaseAffinityYieldsWhenStickyNodeFailedWindow(t *testing.T) {
	pool, cleanup := testRecordingLeasePool(t)
	defer cleanup()
	seedStickyWindow(t, pool)
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO recording_job_node_failures (recording_job_id, node_id, failure_count, last_failed_at)
		VALUES (10, 2, 1, now())`); err != nil {
		t.Fatal(err)
	}
	s := &Server{pool: pool}
	if job, err := leaseAs(t, s, 1); err != nil || job.JobID != 10 {
		t.Fatalf("healthy peer lease=%+v err=%v; a node that failed the window must not keep its affinity", job, err)
	}
}

func TestRelayLeaseFailedNodeYieldsToPeers(t *testing.T) {
	pool, cleanup := testRecordingLeasePool(t)
	defer cleanup()
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO accounts (id) VALUES (42);
		INSERT INTO nodes (id, account_id, node_type, status, last_heartbeat_at, relay_max_streams)
		VALUES (1, 42, 'relay', 'active', now(), 4), (2, 42, 'relay', 'active', now(), 4);
		INSERT INTO recordings (id, account_id, storage_destination_id, name, stream_url, status, start_at, capture_via)
		VALUES (1, 42, 7, 'tls', 'https://example.test/tls.m3u8', 'active', now()-interval '1 hour', 'relay');
		INSERT INTO recording_jobs
			(id, recording_id, fire_at, scheduled_for, clip_duration_sec, status, lease_owner, lease_expires_at, attempt_count, idempotency_key, kind, window_end_at)
		VALUES (10, 1, now(), now(), 60, 'leased', 'node:1', now()+interval '3 minutes', 1, 'tls', 'continuous_window', now()+interval '1 hour');
	`); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	var handoffUntil any
	// Self-update is a node condition, not a capture failure: nothing recorded.
	if err := pool.QueryRow(ctx, recordingJobSurrenderSQL, 10, "node:1", "self_update", nil, int64(1), false).Scan(&handoffUntil); err != nil {
		t.Fatal(err)
	}
	var failures int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM recording_job_node_failures`).Scan(&failures); err != nil || failures != 0 {
		t.Fatalf("self_update recorded failures=%d err=%v", failures, err)
	}
	for i := 0; i < 2; i++ {
		if _, err := pool.Exec(ctx, `UPDATE recording_jobs SET status='leased', lease_owner='node:1', lease_expires_at=now()+interval '3 minutes' WHERE id=10`); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(ctx, recordingJobSurrenderSQL, 10, "node:1", "no progress", nil, int64(1), true).Scan(&handoffUntil); err != nil {
			t.Fatal(err)
		}
	}
	if err := pool.QueryRow(ctx, `SELECT failure_count FROM recording_job_node_failures WHERE recording_job_id=10 AND node_id=1`).Scan(&failures); err != nil || failures != 2 {
		t.Fatalf("no-progress failures=%d err=%v want 2", failures, err)
	}

	s := &Server{pool: pool}
	// With the 5-minute handoff cleared, the failed node still yields to node 2.
	resetWindow(t, pool, "")
	if _, err := leaseAs(t, s, 1); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("failed node re-leased its window ahead of a peer: err=%v", err)
	}
	if job, err := leaseAs(t, s, 2); err != nil || job.JobID != 10 {
		t.Fatalf("peer lease=%+v err=%v want job 10", job, err)
	}
	// Never stranded: two failures cost 12s+60s, after which the node may retry.
	resetWindow(t, pool, "40 seconds")
	if _, err := leaseAs(t, s, 1); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("failed node skipped its yield: err=%v", err)
	}
	resetWindow(t, pool, "73 seconds")
	if job, err := leaseAs(t, s, 1); err != nil || job.JobID != 10 {
		t.Fatalf("failed node after yield lease=%+v err=%v want job 10", job, err)
	}
	// With no unfailed peer able to take it, the failed node retries at once
	// rather than burning the rest of a closing window.
	resetWindow(t, pool, "")
	if _, err := pool.Exec(ctx, `UPDATE nodes SET last_heartbeat_at=now()-interval '10 minutes' WHERE id=2`); err != nil {
		t.Fatal(err)
	}
	if job, err := leaseAs(t, s, 1); err != nil || job.JobID != 10 {
		t.Fatalf("failed node without an unfailed peer lease=%+v err=%v want job 10", job, err)
	}
}
