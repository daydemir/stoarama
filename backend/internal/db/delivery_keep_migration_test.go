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

func TestDeliveryKeepMigrationsUpgradeCleanupBatchV4(t *testing.T) {
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
	schema := fmt.Sprintf("delivery_upgrade_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
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
	dir := t.TempDir()
	source := "../../../infra/sql/migrations"
	entries, err := os.ReadDir(source)
	if err != nil {
		t.Fatal(err)
	}
	copyMigration := func(name string) {
		t.Helper()
		body, err := os.ReadFile(filepath.Join(source, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), body, 0600); err != nil {
			t.Fatal(err)
		}
	}
	// First migrate exactly main's files, including the already-deployed 0188.
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".sql" && entry.Name() < "0189" {
			copyMigration(entry.Name())
		}
	}
	if err := MigrateUp(ctx, pool, dir); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"0189_delivery_keep_holds.sql", "0190_delivery_r2_retirement_guards.sql", "0191_nas_cleanup_delivery_lookup.sql", "0192_delivery_only_r2_apply.sql", "0193_nas_r2_copy.sql"} {
		copyMigration(name)
	}
	for pass := 0; pass < 2; pass++ {
		if err := MigrateUp(ctx, pool, dir); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"0188_nas_cleanup_leased_index.sql", "0189_delivery_keep_holds.sql", "0190_delivery_r2_retirement_guards.sql", "0191_nas_cleanup_delivery_lookup.sql", "0192_delivery_only_r2_apply.sql", "0193_nas_r2_copy.sql"} {
		if applied, err := isApplied(ctx, pool, name); err != nil || !applied {
			t.Fatalf("migration %s skipped: %v", name, err)
		}
	}
	var ready bool
	if err := pool.QueryRow(ctx, `SELECT r2_retirement_delivery_indexes_ready()`).Scan(&ready); err != nil || ready {
		t.Fatalf("migration must not build presence indexes: %t %v", ready, err)
	}
	for n, class := range []string{"", "owner_accepted_outside_window", "owner_r2_delivery_only"} {
		if _, err := pool.Exec(ctx, `INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256,retirement_class)
   VALUES('raw',$1,'stoarama',$2,1,repeat('a',64),repeat('b',64),$3)`, 9999990+n, fmt.Sprintf("managed/class-test-%d", n), class); err != nil {
			t.Fatalf("class %s: %v", class, err)
		}
	}
	if _, err := pool.Exec(ctx, `INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256,retirement_class)
   VALUES('raw',9999999,'stoarama','managed/unknown-class',1,repeat('a',64),repeat('b',64),'unknown')`); err == nil {
		t.Fatal("unknown class admitted")
	}

	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM delivery_keep_holds").Scan(&count); err != nil || count != 32825 {
		t.Fatalf("delivery seed: count=%d err=%v", count, err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM delivery_keep_output_dependencies").Scan(&count); err != nil || count != 59801 {
		t.Fatalf("delivery dependencies: count=%d err=%v", count, err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256) SELECT 'raw',1,bucket,object_key,size_bytes,repeat('a',64),repeat('b',64) FROM delivery_keep_holds LIMIT 1`); err == nil || !strings.Contains(err.Error(), "DELIVERY KEEP HOLD") {
		t.Fatalf("delivery R2 hold bypassed: %v", err)
	}
}
