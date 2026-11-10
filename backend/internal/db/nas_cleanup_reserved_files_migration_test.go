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

func TestNASCleanupReservedFilesMigrationLocksAndNoRewrite(t *testing.T) {
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
	schema := fmt.Sprintf("cleanup_reserved_migration_%d", time.Now().UnixNano())
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
	if _, err = pool.Exec(ctx, `CREATE TABLE nas_cleanup_plans(id bigint,account_id bigint,connection_id bigint,label text,mode text,manifest jsonb);
 CREATE TABLE nas_cleanup_items(id bigint,plan_id bigint,ordinal integer,delete_ever_issued boolean);
 INSERT INTO nas_cleanup_plans VALUES(1,47,13,'legacy','retired','[{"kind":"raw","source_id":1}]');
 INSERT INTO nas_cleanup_items VALUES(1,1,1,true)`); err != nil {
		t.Fatal(err)
	}
	var before, after uint32
	if err = pool.QueryRow(ctx, `SELECT pg_relation_filenode('nas_cleanup_items'::regclass)`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	name := "0177_nas_cleanup_reserved_files.sql"
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
	if _, err = blocker.Exec(ctx, `LOCK nas_cleanup_items IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	err = MigrateUp(ctx, pool, dir)
	if err == nil || !strings.Contains(err.Error(), "lock timeout") {
		t.Fatalf("expected lock_timeout: %v", err)
	}
	if time.Since(start) > 10*time.Second {
		t.Fatal("migration exceeded lock timeout bound")
	}
	if err = blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	// Read/update-compatible locks must not block this view-only migration.
	reader, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Rollback(ctx)
	if _, err = reader.Exec(ctx, `SELECT * FROM nas_cleanup_items FOR SHARE`); err != nil {
		t.Fatal(err)
	}
	if err = MigrateUp(ctx, pool, dir); err != nil {
		t.Fatal(err)
	}
	if err = MigrateUp(ctx, pool, dir); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT pg_relation_filenode('nas_cleanup_items'::regclass)`).Scan(&after); err != nil || before != after {
		t.Fatalf("table rewritten: %d -> %d %v", before, after, err)
	}
	var count int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM nas_cleanup_reserved_files WHERE source_id=1`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("issued reservation lost: %d %v", count, err)
	}
}
