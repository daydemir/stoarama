package db

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type migrationLockPolicy struct {
	timeout  time.Duration
	attempts int
	backoff  time.Duration
}

// Each attempt waits at most migrationLock.timeout for its ACCESS EXCLUSIVE
// lock, so a migration never queues production behind it longer than it does
// today. Live 15-30s readers on hot tables (e.g. the collated feed joining
// connections) hold conflicting locks, so one failed attempt is retried a
// bounded number of times: 24 x (5s wait + 5s backoff) is about 4 minutes
// total before the deploy fails.
var migrationLock = migrationLockPolicy{timeout: 5 * time.Second, attempts: 24, backoff: 5 * time.Second}

func MigrateUp(ctx context.Context, pool *pgxpool.Pool, migrationDir string) error {
	if migrationDir == "" {
		migrationDir = resolveMigrationDir()
	}
	if err := ensureMigrationsTable(ctx, pool); err != nil {
		return err
	}
	entries, err := os.ReadDir(migrationDir)
	if err != nil {
		return fmt.Errorf("read migrations dir %s: %w", migrationDir, err)
	}
	files := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(e.Name(), ".sql") {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)
	for _, name := range files {
		applied, err := isApplied(ctx, pool, name)
		if err != nil {
			return err
		}
		if applied {
			continue
		}
		path := filepath.Join(migrationDir, name)
		body, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", path, err)
		}
		if strings.TrimSpace(string(body)) == "" {
			return fmt.Errorf("migration %s is empty; refusing to record it as applied", path)
		}
		if err := applyWithLockRetry(ctx, pool, name, string(body)); err != nil {
			return err
		}
	}
	return nil
}

// applyWithLockRetry runs the migration, retrying only when the transaction
// could not take its lock within migrationLock.timeout.
func applyWithLockRetry(ctx context.Context, pool *pgxpool.Pool, name, body string) error {
	for attempt := 1; ; attempt++ {
		err := applyMigration(ctx, pool, name, body)
		if err == nil || !isLockTimeout(err) {
			return err
		}
		if attempt >= migrationLock.attempts {
			return fmt.Errorf("apply migration %s: lock busy after %d attempts: %w", name, attempt, err)
		}
		log.Printf("migrate: %s lock busy (attempt %d/%d); retrying in %s", name, attempt, migrationLock.attempts, migrationLock.backoff)
		select {
		case <-ctx.Done():
			return fmt.Errorf("retry migration %s: %w", name, ctx.Err())
		case <-time.After(migrationLock.backoff):
		}
		// Another migrator may have taken the same lock and committed.
		applied, err := isApplied(ctx, pool, name)
		if err != nil || applied {
			return err
		}
	}
}

// applyMigration applies one file in its own transaction.
func applyMigration(ctx context.Context, pool *pgxpool.Pool, name, body string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration tx: %w", err)
	}
	// An ALTER that waits on ACCESS EXCLUSIVE queues every later query behind
	// it, so a slow lock takes prod down. Fail this attempt quickly instead.
	// SET LOCAL reverts with the tx.
	// ponytail: lock acquisition only; a migration that takes its lock fast and
	// then rewrites a huge table still holds it. Add statement_timeout if a
	// rewrite ever bites, but that would also kill legitimate backfills.
	lockTimeout := fmt.Sprintf(`SET LOCAL lock_timeout = '%dms'`, migrationLock.timeout.Milliseconds())
	if _, err := tx.Exec(ctx, lockTimeout); err != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("set lock_timeout for %s: %w", name, err)
	}
	if _, err := tx.Exec(ctx, body); err != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("apply migration %s: %w", name, err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, name); err != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("record migration %s: %w", name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration %s: %w", name, err)
	}
	return nil
}

// isLockTimeout reports whether err is a PostgreSQL lock_timeout cancellation.
func isLockTimeout(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "55P03"
}

func ensureMigrationsTable(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version text PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now()
		)
	`)
	if err != nil {
		return fmt.Errorf("ensure schema_migrations: %w", err)
	}
	return nil
}

func isApplied(ctx context.Context, pool *pgxpool.Pool, version string) (bool, error) {
	var exists bool
	err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)`, version).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check migration %s: %w", version, err)
	}
	return exists, nil
}

func resolveMigrationDir() string {
	candidates := []string{
		"infra/sql/migrations",
		"../infra/sql/migrations",
		"../../infra/sql/migrations",
	}
	for _, c := range candidates {
		if st, err := os.Stat(c); err == nil && st.IsDir() {
			return c
		}
	}
	return "infra/sql/migrations"
}
