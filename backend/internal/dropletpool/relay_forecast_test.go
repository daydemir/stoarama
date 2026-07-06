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
