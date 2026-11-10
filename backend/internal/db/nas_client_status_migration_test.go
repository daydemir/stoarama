package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Joined-lifecycle fixtures deliberately omit the NAS cleanup backlog. Status
// DDL must work there and must never scan/index that large table while holding
// the connections ALTER lock. The existing pending index supports leasing.
func TestNASClientStatusMigrationIsIndependentOfCleanupBacklog(t *testing.T) {
	url := os.Getenv("STOARAMA_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("nas_status_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE") }()
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `CREATE TABLE connections(id bigint PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	name := "0175_nas_cleanup_client_status.sql"
	body, err := os.ReadFile(filepath.Join("../../../infra/sql/migrations", name))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), body, 0600); err != nil {
		t.Fatal(err)
	}
	if err := MigrateUp(ctx, pool, dir); err != nil {
		t.Fatal(err)
	}
	if err := MigrateUp(ctx, pool, dir); err != nil {
		t.Fatalf("repeat migration: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO connections(id,nas_cleanup_status,nas_cleanup_release_generation) VALUES(1,'{}',1); INSERT INTO nas_client_logs(connection_id,client_version,boot_id,lines) VALUES(1,'test','boot','[]')`); err != nil {
		t.Fatal(err)
	}
	var absent bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('nas_cleanup_items') IS NULL`).Scan(&absent); err != nil || !absent {
		t.Fatalf("cleanup backlog unexpectedly required: %v", err)
	}
}
