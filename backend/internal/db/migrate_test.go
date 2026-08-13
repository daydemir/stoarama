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

// Reserved migration numbers can merge in either PR order. A lower-numbered
// migration arriving after a higher-numbered migration is already in the
// filename ledger must still run exactly once; the ledger is not a numeric
// high-water mark.
func TestMigrateUpAppliesLowerReservedMigrationAfterHigherHistory(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run the reserved migration ordering regression")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("migration_reserved_order_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams = map[string]string{"search_path": schema}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	dir := t.TempDir()
	higher := "0137_reserved_higher_test.sql"
	lower := "0136_reserved_lower_test.sql"
	if err = os.WriteFile(filepath.Join(dir, higher), []byte("CREATE TABLE reserved_higher(id integer PRIMARY KEY);\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err = MigrateUp(ctx, pool, dir); err != nil {
		t.Fatalf("apply higher reserved migration: %v", err)
	}
	if err = os.WriteFile(filepath.Join(dir, lower), []byte("CREATE TABLE reserved_lower(id integer PRIMARY KEY);\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err = MigrateUp(ctx, pool, dir); err != nil {
		t.Fatalf("apply later lower reserved migration: %v", err)
	}
	if err = MigrateUp(ctx, pool, dir); err != nil {
		t.Fatalf("replay reserved migrations: %v", err)
	}
	var versions []string
	rows, err := pool.Query(ctx, `SELECT version FROM schema_migrations ORDER BY version`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var version string
		if err = rows.Scan(&version); err != nil {
			t.Fatal(err)
		}
		versions = append(versions, version)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(versions, ","); got != lower+","+higher {
		t.Fatalf("migration ledger=%q", got)
	}
	for _, table := range []string{"reserved_lower", "reserved_higher"} {
		var present *string
		if err = pool.QueryRow(ctx, `SELECT to_regclass($1)::text`, table).Scan(&present); err != nil || present == nil {
			t.Fatalf("table %s missing: present=%v err=%v", table, present, err)
		}
	}
}

// A migration that cannot take its lock must abort quickly instead of queueing
// behind a live transaction. Without SET LOCAL lock_timeout this blocks forever
// and every query arriving after the ALTER stacks up behind it.
func TestMigrateUpAbortsWhenLockIsHeld(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run the migration lock-timeout regression")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS migrate_lock_probe (id int)`); err != nil {
		t.Fatalf("create probe table: %v", err)
	}
	defer func() { _, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS migrate_lock_probe`) }()

	// Hold ACCESS EXCLUSIVE so the migration's ALTER cannot proceed.
	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin blocker: %v", err)
	}
	defer func() { _ = blocker.Rollback(ctx) }()
	if _, err := blocker.Exec(ctx, `LOCK TABLE migrate_lock_probe IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatalf("lock probe table: %v", err)
	}

	dir := t.TempDir()
	name := "9999_migrate_lock_probe_test.sql"
	if err := os.WriteFile(filepath.Join(dir, name), []byte("ALTER TABLE migrate_lock_probe ADD COLUMN blocked int;\n"), 0o600); err != nil {
		t.Fatalf("write migration: %v", err)
	}
	defer func() { _, _ = pool.Exec(ctx, `DELETE FROM schema_migrations WHERE version = $1`, name) }()

	start := time.Now()
	err = MigrateUp(ctx, pool, dir)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected the blocked migration to fail, got nil")
	}
	if !strings.Contains(err.Error(), "lock timeout") && !strings.Contains(err.Error(), "canceling statement") {
		t.Fatalf("expected a lock timeout error, got: %v", err)
	}
	if elapsed > 10*time.Second {
		t.Fatalf("migration took %s; lock_timeout did not apply", elapsed)
	}
}
