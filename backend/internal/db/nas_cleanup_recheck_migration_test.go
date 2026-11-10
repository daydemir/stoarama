package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestNASCleanupRecheckMigrationMetadataOnlyAndLockTimeout(t *testing.T) {
	dsn := os.Getenv("STOARAMA_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("recheck_migration_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE") }()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err = pool.Exec(ctx, `CREATE TABLE nas_cleanup_items(id bigint,attempt integer); INSERT INTO nas_cleanup_items VALUES(1,9)`); err != nil {
		t.Fatal(err)
	}
	var before, after uint32
	if err = pool.QueryRow(ctx, `SELECT pg_relation_filenode('nas_cleanup_items'::regclass)`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	name := "0176_nas_cleanup_recheck.sql"
	body, err := os.ReadFile(filepath.Join("../../../infra/sql/migrations", name))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, name), body, 0600); err != nil {
		t.Fatal(err)
	}
	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(ctx)
	if _, err = blocker.Exec(ctx, `SELECT * FROM nas_cleanup_items FOR SHARE`); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	err = MigrateUp(ctx, pool, dir)
	if err == nil || !strings.Contains(err.Error(), "lock timeout") {
		t.Fatalf("migration did not respect lock_timeout: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("migration blocked for %s", elapsed)
	}
	if err = blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err = MigrateUp(ctx, pool, dir); err != nil {
		t.Fatalf("retry after lock release: %v", err)
	}
	if err = MigrateUp(ctx, pool, dir); err != nil {
		t.Fatalf("repeat: %v", err)
	}
	if err = pool.QueryRow(ctx, `SELECT pg_relation_filenode('nas_cleanup_items'::regclass)`).Scan(&after); err != nil || before != after {
		t.Fatalf("table was rewritten: %d -> %d (%v)", before, after, err)
	}
	var attempt, base int
	if err = pool.QueryRow(ctx, `SELECT attempt,recheck_attempt_base FROM nas_cleanup_items WHERE id=1`).Scan(&attempt, &base); err != nil || attempt != 9 || base != 0 {
		t.Fatalf("existing counters changed: %d/%d %v", attempt, base, err)
	}
}
