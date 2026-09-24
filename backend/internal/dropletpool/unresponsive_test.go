package dropletpool

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestWorkerLastLiveAtUsesFreshestEvidence(t *testing.T) {
	created := time.Date(2026, 9, 11, 13, 0, 0, 0, time.UTC)
	seen := created.Add(time.Hour)
	node := created.Add(2 * time.Hour)
	if got := WorkerLastLiveAt(Droplet{CreatedAt: created}); !got.Equal(created) {
		t.Fatalf("no heartbeat => created_at, got %s", got)
	}
	if got := WorkerLastLiveAt(Droplet{CreatedAt: created, LastSeenAt: &seen, NodeHeartbeatAt: &node}); !got.Equal(node) {
		t.Fatalf("fresher node heartbeat must win, got %s", got)
	}
	if got := WorkerLastLiveAt(Droplet{CreatedAt: created, LastSeenAt: &node, NodeHeartbeatAt: &seen}); !got.Equal(node) {
		t.Fatalf("fresher droplet heartbeat must win, got %s", got)
	}
}

func TestUnresponsiveDropletsSelectsOnlyStaleActiveRows(t *testing.T) {
	now := time.Date(2026, 9, 24, 7, 0, 0, 0, time.UTC)
	stale := now.Add(-16 * time.Minute)
	fresh := now.Add(-time.Minute)
	rows := []Droplet{
		{ID: 1, State: "active", CreatedAt: stale, LastSeenAt: &stale},                          // stale
		{ID: 2, State: "active", CreatedAt: stale, LastSeenAt: &fresh},                          // alive
		{ID: 3, State: "active", CreatedAt: stale, LastSeenAt: &stale, NodeHeartbeatAt: &fresh}, // alive by node
		{ID: 4, State: "provisioning", CreatedAt: stale},                                        // reconcile owns
		{ID: 5, State: "draining", CreatedAt: stale, LastSeenAt: &stale},                        // drain owns
		{ID: 6, State: "active", CreatedAt: stale},                                              // never reported
	}
	got := UnresponsiveDroplets(rows, now, 15*time.Minute)
	if len(got) != 2 || got[0].ID != 1 || got[1].ID != 6 {
		t.Fatalf("unresponsive=%+v want ids 1 and 6", got)
	}
	if UnresponsiveDroplets(rows, now, 0) != nil {
		t.Fatal("non-positive threshold must disable the check")
	}
}

func TestReapUnresponsiveRetiresOnlyIdleStaleSharedWorkers(t *testing.T) {
	tests := []struct {
		name        string
		role        string
		lastSeenAgo time.Duration
		nodeBeatAgo time.Duration
		leased      bool
		deleteError bool
		wantState   string
		wantDeleted int
		wantRevoked bool
	}{
		{name: "stale idle retires", role: sharedPoolRole, lastSeenAgo: 12 * 24 * time.Hour, nodeBeatAgo: 12 * 24 * time.Hour, wantState: "destroyed", wantDeleted: 1, wantRevoked: true},
		{name: "stale leased retained", role: sharedPoolRole, lastSeenAgo: time.Hour, nodeBeatAgo: time.Hour, leased: true, wantState: "active"},
		{name: "node heartbeat keeps alive", role: sharedPoolRole, lastSeenAgo: time.Hour, nodeBeatAgo: time.Minute, wantState: "active"},
		{name: "fresh worker untouched", role: sharedPoolRole, lastSeenAgo: time.Minute, nodeBeatAgo: time.Minute, wantState: "active"},
		{name: "delete failure resumable", role: sharedPoolRole, lastSeenAgo: time.Hour, nodeBeatAgo: time.Hour, deleteError: true, wantState: "destroying", wantDeleted: 1},
		{name: "dedicated canary untouched", role: dedicatedCanaryPoolRole, lastSeenAgo: time.Hour, nodeBeatAgo: time.Hour, wantState: "active"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pool, cleanup := testDropletPoolDB(t)
			defer cleanup()
			ctx := context.Background()
			now := time.Now().UTC()
			rowID, nodeID := insertActiveWorkerFixture(t, pool, now, tc.role, now.Add(-tc.lastSeenAgo), now.Add(-tc.nodeBeatAgo), tc.leased)
			provider := &fakeDOClient{fleet: []DODroplet{{ID: 8001, Name: "stoarama-rec-unresponsive", Status: "active", CreatedAt: now.Add(-24 * time.Hour)}}}
			if tc.deleteError {
				provider.deleteErr = errors.New("provider delete unavailable")
			}
			controller := NewController(pool, provider, Config{StaleHeartbeatTimeout: 15 * time.Minute, HeartbeatSec: 15})
			controller.reapUnresponsive(ctx, now)

			var state string
			if err := pool.QueryRow(ctx, `SELECT state FROM recorder_droplets WHERE id=$1`, rowID).Scan(&state); err != nil {
				t.Fatal(err)
			}
			if state != tc.wantState {
				t.Fatalf("state=%q want %q", state, tc.wantState)
			}
			if len(provider.deleted) != tc.wantDeleted {
				t.Fatalf("provider deletes=%v want %d", provider.deleted, tc.wantDeleted)
			}
			var revoked bool
			if err := pool.QueryRow(ctx, `SELECT revoked_at IS NOT NULL FROM node_tokens WHERE node_id=$1`, nodeID).Scan(&revoked); err != nil {
				t.Fatal(err)
			}
			if revoked != tc.wantRevoked {
				t.Fatalf("token revoked=%t want %t", revoked, tc.wantRevoked)
			}
			if tc.deleteError {
				// The next reconcile resumes the durable destroying state.
				provider.deleteErr = nil
				if err := controller.reconcile(ctx, now); err != nil {
					t.Fatal(err)
				}
				if err := pool.QueryRow(ctx, `SELECT state FROM recorder_droplets WHERE id=$1`, rowID).Scan(&state); err != nil {
					t.Fatal(err)
				}
				if state != "destroyed" {
					t.Fatalf("resumed state=%q want destroyed", state)
				}
			}
		})
	}
}

func insertActiveWorkerFixture(t *testing.T, pool *pgxpool.Pool, now time.Time, role string, lastSeen, nodeBeat time.Time, leased bool) (int64, int64) {
	t.Helper()
	ctx := context.Background()
	accountID := insertForecastAccount(t, pool)
	destID := insertForecastDestination(t, pool, accountID)
	var recordingID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO recordings (
		  account_id, storage_destination_id, name, stream_url, source_kind,
		  mode, cron_expr, cron_timezone, clip_duration_sec, status, next_fire_at,
		  start_at, capture_via
		)
		VALUES ($1, $2, 'unresponsive', 'https://example.com/live.m3u8', 'hls_live',
		        'sampled', '* * * * *', 'UTC', 30, 'active', $3, $3, 'cloud')
		RETURNING id
	`, accountID, destID, now).Scan(&recordingID); err != nil {
		t.Fatalf("insert recording: %v", err)
	}
	var nodeID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO nodes (account_id, node_type, display_name, status, last_heartbeat_at)
		VALUES ($1, 'local_recorder', 'stoarama-rec-unresponsive', 'active', $2) RETURNING id
	`, accountID, nodeBeat).Scan(&nodeID); err != nil {
		t.Fatalf("insert node: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO node_tokens (node_id, key_prefix, secret_hash) VALUES ($1, 'stale', 'hash')`, nodeID); err != nil {
		t.Fatalf("insert token: %v", err)
	}
	var rowID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO recorder_droplets
		  (name, node_id, do_droplet_id, region, size, capacity, state, pool_role, last_seen_at, created_at)
		VALUES ('stoarama-rec-unresponsive', $1, 8001, 'nyc3', 's-1vcpu-1gb', 1, 'active', $2, $3, $4)
		RETURNING id
	`, nodeID, role, lastSeen, now.Add(-24*time.Hour)).Scan(&rowID); err != nil {
		t.Fatalf("insert droplet: %v", err)
	}
	if leased {
		if _, err := pool.Exec(ctx, `
			INSERT INTO recording_jobs
			  (recording_id, fire_at, scheduled_for, clip_duration_sec, status, lease_owner, lease_expires_at, idempotency_key)
			VALUES ($1, $2, $2, 30, 'leased', 'stoarama-rec-unresponsive', $3, $4)
		`, recordingID, now, now.Add(time.Hour), fmt.Sprintf("unresponsive:%d", rowID)); err != nil {
			t.Fatalf("insert lease: %v", err)
		}
	}
	return rowID, nodeID
}

func TestBeginDestroyIfIdleAndSilentRechecksHeartbeatInsideTransaction(t *testing.T) {
	pool, cleanup := testDropletPoolDB(t)
	defer cleanup()
	ctx := context.Background()
	now := time.Now().UTC()
	// The controller's snapshot saw a stale worker, but it heartbeated since.
	rowID, _ := insertActiveWorkerFixture(t, pool, now, sharedPoolRole, now.Add(-time.Hour), now.Add(-time.Hour), false)
	if _, err := pool.Exec(ctx, `UPDATE recorder_droplets SET last_seen_at=now() WHERE id=$1`, rowID); err != nil {
		t.Fatal(err)
	}
	retired, err := NewStore(pool).BeginDestroyIfIdleAndSilent(ctx, rowID, now.Add(-15*time.Minute))
	if err != nil || retired {
		t.Fatalf("recovered worker retired=%t err=%v", retired, err)
	}
	var state string
	if err := pool.QueryRow(ctx, `SELECT state FROM recorder_droplets WHERE id=$1`, rowID).Scan(&state); err != nil || state != "active" {
		t.Fatalf("state=%q err=%v want active", state, err)
	}
}
