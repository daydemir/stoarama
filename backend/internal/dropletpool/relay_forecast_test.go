package dropletpool

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/db"
)

func TestForecastDemandExcludesRelayRecordings(t *testing.T) {
	pool, cleanup := testDropletPoolDB(t)
	defer cleanup()

	ctx := context.Background()
	accountID := insertForecastAccount(t, pool)
	destID := insertForecastDestination(t, pool, accountID)
	insertForecastRecording(t, pool, accountID, destID, "cloud")
	insertForecastRecording(t, pool, accountID, destID, "relay")

	now := mustTime(t, "2026-06-24T12:00:30Z")
	forecast, err := ForecastDemand(ctx, pool, false, now, 30*time.Minute)
	if err != nil {
		t.Fatalf("ForecastDemand: %v", err)
	}
	if forecast.PeakConcurrent != 1 {
		t.Fatalf("peak=%d want 1; relay recordings must not consume droplet capacity", forecast.PeakConcurrent)
	}
}

func TestValidateLiveBindings(t *testing.T) {
	pool, cleanup := testDropletPoolDB(t)
	defer cleanup()

	ctx := context.Background()
	accountID := insertForecastAccount(t, pool)
	var nodeID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO nodes (account_id, node_type, display_name, status)
		VALUES ($1, 'local_recorder', 'recorder-a', 'active') RETURNING id
	`, accountID).Scan(&nodeID); err != nil {
		t.Fatalf("insert node: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO node_tokens (node_id, key_prefix, secret_hash)
		VALUES ($1, 'prefix', 'hash')
	`, nodeID); err != nil {
		t.Fatalf("insert node token: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO recorder_droplets (name, node_id, region, size, capacity, state)
		VALUES ('recorder-a', $1, 'nyc3', 's-1vcpu-1gb', 1, 'active')
	`, nodeID); err != nil {
		t.Fatalf("insert droplet: %v", err)
	}

	store := NewStore(pool)
	if err := store.ValidateLiveBindings(ctx); err != nil {
		t.Fatalf("valid binding rejected: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE node_tokens SET revoked_at=now() WHERE node_id=$1`, nodeID); err != nil {
		t.Fatalf("revoke node token: %v", err)
	}
	if err := store.ValidateLiveBindings(ctx); err == nil {
		t.Fatal("revoked live binding accepted")
	}
	if _, err := pool.Exec(ctx, `UPDATE recorder_droplets SET state='destroyed' WHERE node_id=$1`, nodeID); err != nil {
		t.Fatalf("destroy droplet: %v", err)
	}
	if err := store.ValidateLiveBindings(ctx); err != nil {
		t.Fatalf("destroyed binding blocked startup: %v", err)
	}
}

func TestReconcileOverdueProvisioningLeaseSafety(t *testing.T) {
	tests := []struct {
		name         string
		provider     bool
		leased       bool
		fresh        bool
		initialState string
		wantState    string
		wantDeleted  int
		wantRevoked  bool
	}{
		{name: "present leased fresh promotes", provider: true, leased: true, fresh: true, wantState: "active"},
		{name: "missing leased stays provisioning", leased: true, fresh: true, wantState: "provisioning"},
		{name: "present leased stale stays provisioning", provider: true, leased: true, wantState: "provisioning"},
		{name: "present idle retires", provider: true, fresh: true, wantState: "failed", wantDeleted: 1, wantRevoked: true},
		{name: "missing idle retires", fresh: true, wantState: "destroyed", wantRevoked: true},
		{name: "present destroying resumes", provider: true, fresh: true, initialState: "destroying", wantState: "destroyed", wantDeleted: 1, wantRevoked: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pool, cleanup := testDropletPoolDB(t)
			defer cleanup()

			ctx := context.Background()
			now := time.Now().UTC()
			rowID, nodeID := insertProvisioningReconcileFixture(t, pool, now, tc.leased, tc.fresh)
			if tc.initialState != "" {
				if _, err := pool.Exec(ctx, `UPDATE recorder_droplets SET state=$2 WHERE id=$1`, rowID, tc.initialState); err != nil {
					t.Fatalf("set initial state: %v", err)
				}
			}
			provider := &fakeDOClient{}
			if tc.provider {
				provider.fleet = []DODroplet{{ID: 7001, Name: "stoarama-rec-lease-safety", Status: "active", CreatedAt: now.Add(-time.Hour)}}
			}
			controller := NewController(pool, provider, Config{ProvisionTimeout: 10 * time.Minute, HeartbeatSec: 30})
			if err := controller.reconcile(ctx, now); err != nil {
				t.Fatalf("reconcile: %v", err)
			}

			var state string
			if err := pool.QueryRow(ctx, `SELECT state FROM recorder_droplets WHERE id=$1`, rowID).Scan(&state); err != nil {
				t.Fatalf("read droplet state: %v", err)
			}
			if state != tc.wantState {
				t.Fatalf("state=%q want %q", state, tc.wantState)
			}
			if len(provider.deleted) != tc.wantDeleted {
				t.Fatalf("provider deletes=%v want count %d", provider.deleted, tc.wantDeleted)
			}
			var revoked bool
			if err := pool.QueryRow(ctx, `SELECT revoked_at IS NOT NULL FROM node_tokens WHERE node_id=$1`, nodeID).Scan(&revoked); err != nil {
				t.Fatalf("read token state: %v", err)
			}
			if revoked != tc.wantRevoked {
				t.Fatalf("token revoked=%t want %t", revoked, tc.wantRevoked)
			}
		})
	}
}

func insertProvisioningReconcileFixture(t *testing.T, pool *pgxpool.Pool, now time.Time, leased, fresh bool) (int64, int64) {
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
		VALUES ($1, $2, 'lease-safety', 'https://example.com/live.m3u8', 'hls',
		        'sampled', '* * * * *', 'UTC', 30, 'active', $3, $3, 'cloud')
		RETURNING id
	`, accountID, destID, now).Scan(&recordingID); err != nil {
		t.Fatalf("insert recording: %v", err)
	}
	var nodeID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO nodes (account_id, node_type, display_name, status)
		VALUES ($1, 'local_recorder', 'stoarama-rec-lease-safety', 'active') RETURNING id
	`, accountID).Scan(&nodeID); err != nil {
		t.Fatalf("insert node: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO node_tokens (node_id, key_prefix, secret_hash) VALUES ($1, 'lease', 'hash')`, nodeID); err != nil {
		t.Fatalf("insert token: %v", err)
	}
	lastSeen := now.Add(-time.Hour)
	if fresh {
		lastSeen = now.Add(-time.Minute)
	}
	var rowID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO recorder_droplets
		  (name, node_id, do_droplet_id, region, size, capacity, state, last_seen_at, created_at)
		VALUES ('stoarama-rec-lease-safety', $1, 7001, 'nyc3', 's-1vcpu-1gb', 1, 'provisioning', $2, $3)
		RETURNING id
	`, nodeID, lastSeen, now.Add(-time.Hour)).Scan(&rowID); err != nil {
		t.Fatalf("insert droplet: %v", err)
	}
	if leased {
		if _, err := pool.Exec(ctx, `
			INSERT INTO recording_jobs
			  (recording_id, fire_at, scheduled_for, clip_duration_sec, status, lease_owner, lease_expires_at, idempotency_key)
			VALUES ($1, $2, $2, 30, 'leased', 'stoarama-rec-lease-safety', $3, $4)
		`, recordingID, now, now.Add(time.Hour), fmt.Sprintf("lease-safety:%d", rowID)); err != nil {
			t.Fatalf("insert lease: %v", err)
		}
	}
	return rowID, nodeID
}

func testDropletPoolDB(t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()

	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run DB-backed dropletpool tests")
	}

	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open admin pool: %v", err)
	}

	schema := fmt.Sprintf("dropletpool_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schema)); err != nil {
		admin.Close()
		t.Fatalf("create schema: %v", err)
	}

	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		_, _ = admin.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
		admin.Close()
		t.Fatalf("parse db url: %v", err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		_, _ = admin.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
		admin.Close()
		t.Fatalf("open test pool: %v", err)
	}

	if err := db.MigrateUp(ctx, pool, findDropletPoolMigrationsDir(t)); err != nil {
		pool.Close()
		_, _ = admin.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
		admin.Close()
		t.Fatalf("apply migrations: %v", err)
	}

	cleanup := func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
		admin.Close()
	}
	return pool, cleanup
}

func findDropletPoolMigrationsDir(t *testing.T) string {
	t.Helper()
	candidates := []string{
		"../../../infra/sql/migrations",
		"../../infra/sql/migrations",
		"infra/sql/migrations",
	}
	for _, c := range candidates {
		if st, err := os.Stat(c); err == nil && st.IsDir() {
			abs, err := filepath.Abs(c)
			if err == nil {
				return abs
			}
			return c
		}
	}
	t.Fatalf("cannot locate infra/sql/migrations")
	return ""
}

func insertForecastAccount(t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO accounts (email, name, role, status)
		VALUES ('forecast@example.com', 'Forecast', 'member', 'active')
		RETURNING id
	`).Scan(&id); err != nil {
		t.Fatalf("insert account: %v", err)
	}
	return id
}

func insertForecastDestination(t *testing.T, pool *pgxpool.Pool, accountID int64) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO storage_destinations (
		  account_id, name, provider, endpoint, region, bucket,
		  access_key_id, secret_access_key_enc, status
		)
		VALUES (
		  $1, 'dest', 's3_compatible', 'https://s3.example.com', 'auto', 'bucket',
		  'key', decode('00', 'hex'), 'verified'
		)
		RETURNING id
	`, accountID).Scan(&id); err != nil {
		t.Fatalf("insert destination: %v", err)
	}
	return id
}

func insertForecastRecording(t *testing.T, pool *pgxpool.Pool, accountID, destID int64, captureVia string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO recordings (
		  account_id, storage_destination_id, name, stream_url, source_kind,
		  mode, cron_expr, cron_timezone, clip_duration_sec, status, next_fire_at,
		  start_at, capture_via
		)
		VALUES (
		  $1, $2, $3, 'https://example.com/live.m3u8', 'hls',
		  'sampled', '* * * * *', 'UTC', 30, 'active', now(),
		  '2026-06-24T00:00:00Z', $4
		)
	`, accountID, destID, "recording-"+captureVia, captureVia); err != nil {
		t.Fatalf("insert %s recording: %v", captureVia, err)
	}
}
