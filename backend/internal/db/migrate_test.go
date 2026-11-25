package db

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// testPool skips when no test database is configured and returns a pool the
// caller must close.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run the migration lock regression")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// setMigrationLock overrides the retry policy for one test and restores it.
func setMigrationLock(t *testing.T, policy migrationLockPolicy) {
	t.Helper()
	previous := migrationLock
	migrationLock = policy
	t.Cleanup(func() { migrationLock = previous })
}

func writeMigration(t *testing.T, name, body string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
		t.Fatalf("write migration: %v", err)
	}
	return dir
}

// A migration that cannot take its lock must abort each attempt quickly instead
// of queueing behind a live transaction, and the bounded retry must give up fast
// once the attempts are spent. Without SET LOCAL lock_timeout this blocks forever
// and every query arriving after the ALTER stacks up behind it.
func TestMigrateUpAbortsWhenLockIsHeld(t *testing.T) {
	pool := testPool(t)
	setMigrationLock(t, migrationLockPolicy{timeout: 500 * time.Millisecond, attempts: 3, backoff: 100 * time.Millisecond})
	ctx := context.Background()

	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS migrate_lock_probe (id int)`); err != nil {
		t.Fatalf("create probe table: %v", err)
	}
	defer func() { _, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS migrate_lock_probe`) }()

	// Hold ACCESS EXCLUSIVE for the whole test so no attempt can proceed.
	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin blocker: %v", err)
	}
	defer func() { _ = blocker.Rollback(ctx) }()
	if _, err := blocker.Exec(ctx, `LOCK TABLE migrate_lock_probe IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatalf("lock probe table: %v", err)
	}

	name := "9999_migrate_lock_probe_test.sql"
	dir := writeMigration(t, name, "ALTER TABLE migrate_lock_probe ADD COLUMN blocked int;\n")
	defer func() { _, _ = pool.Exec(ctx, `DELETE FROM schema_migrations WHERE version = $1`, name) }()

	start := time.Now()
	err = MigrateUp(ctx, pool, dir)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected the blocked migration to fail, got nil")
	}
	if !strings.Contains(err.Error(), "lock busy after 3 attempts") {
		t.Fatalf("expected the bound to be reported, got: %v", err)
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
		t.Fatalf("expected the lock timeout to be preserved, got: %v", err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("migration took %s; lock_timeout or the attempt bound did not apply", elapsed)
	}
}

// A lock held only briefly must not fail the deploy: the retry outlives a live
// reader that releases its conflicting lock.
func TestMigrateUpRetriesUntilLockFrees(t *testing.T) {
	pool := testPool(t)
	setMigrationLock(t, migrationLockPolicy{timeout: 500 * time.Millisecond, attempts: 10, backoff: 200 * time.Millisecond})
	ctx := context.Background()

	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS migrate_retry_probe (id int)`); err != nil {
		t.Fatalf("create probe table: %v", err)
	}
	defer func() { _, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS migrate_retry_probe`) }()

	// A slow reader holds ACCESS SHARE, so the migration's ALTER cannot take
	// ACCESS EXCLUSIVE until the reader goes away.
	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin blocker: %v", err)
	}
	if _, err := blocker.Exec(ctx, `SELECT count(*) FROM migrate_retry_probe`); err != nil {
		t.Fatalf("read probe table: %v", err)
	}
	released := make(chan struct{})
	go func() {
		defer close(released)
		time.Sleep(1500 * time.Millisecond)
		_ = blocker.Rollback(context.Background())
	}()
	defer func() {
		<-released
		_ = blocker.Rollback(ctx)
	}()

	name := "9998_migrate_retry_probe_test.sql"
	dir := writeMigration(t, name, "ALTER TABLE migrate_retry_probe ADD COLUMN released int;\n")
	defer func() { _, _ = pool.Exec(ctx, `DELETE FROM schema_migrations WHERE version = $1`, name) }()

	if err := MigrateUp(ctx, pool, dir); err != nil {
		t.Fatalf("expected the retried migration to succeed, got: %v", err)
	}

	applied, err := isApplied(ctx, pool, name)
	if err != nil {
		t.Fatalf("check applied: %v", err)
	}
	if !applied {
		t.Fatal("expected the migration to be recorded in schema_migrations")
	}
	var columnExists bool
	err = pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_name = 'migrate_retry_probe' AND column_name = 'released'
		)`).Scan(&columnExists)
	if err != nil {
		t.Fatalf("check column: %v", err)
	}
	if !columnExists {
		t.Fatal("expected the migrated column to exist")
	}
}

// Only lock timeouts are retried; a real migration error must fail on the first
// attempt instead of waiting out the backoff.
func TestMigrateUpDoesNotRetryOtherErrors(t *testing.T) {
	pool := testPool(t)
	setMigrationLock(t, migrationLockPolicy{timeout: 5 * time.Second, attempts: 3, backoff: 2 * time.Second})
	ctx := context.Background()

	name := "9997_migrate_syntax_probe_test.sql"
	dir := writeMigration(t, name, "ALTER TABLE migrate_syntax_probe ADD COLUMN;\n")
	defer func() { _, _ = pool.Exec(ctx, `DELETE FROM schema_migrations WHERE version = $1`, name) }()

	start := time.Now()
	err := MigrateUp(ctx, pool, dir)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected the broken migration to fail, got nil")
	}
	if !strings.Contains(err.Error(), "apply migration "+name) {
		t.Fatalf("expected the apply error, got: %v", err)
	}
	if strings.Contains(err.Error(), "lock busy") {
		t.Fatalf("expected no retry for a non-lock error, got: %v", err)
	}
	if elapsed > time.Second {
		t.Fatalf("migration took %s; a non-lock error must not wait out the backoff", elapsed)
	}

	applied, err := isApplied(ctx, pool, name)
	if err != nil {
		t.Fatalf("check applied: %v", err)
	}
	if applied {
		t.Fatal("a failed migration must not be recorded in schema_migrations")
	}
}

func TestMigrateUpRefusesEmptyMigration(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	for _, body := range []string{"", " \n\t\n"} {
		name := "9996_migrate_empty_probe_test.sql"
		dir := writeMigration(t, name, body)
		err := MigrateUp(ctx, pool, dir)
		if err == nil || !strings.Contains(err.Error(), "is empty") {
			t.Fatalf("body %q: want empty-migration refusal, got %v", body, err)
		}
		if applied, err := isApplied(ctx, pool, name); err != nil || applied {
			t.Fatalf("body %q: an empty migration must never be recorded (applied=%v err=%v)", body, applied, err)
		}
	}
}
