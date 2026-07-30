package db

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

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
	if elapsed > 30*time.Second {
		t.Fatalf("migration took %s; lock_timeout did not apply", elapsed)
	}
}
