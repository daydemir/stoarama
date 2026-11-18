package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/db"
)

func TestR2OwnerFenceTimingMigration(t *testing.T) {
	const migration = "0210_r2_owner_fence_timing.sql"
	for _, bucket := range []string{"stoarama", "other", ""} {
		t.Run("bucket="+bucket, func(t *testing.T) {
			ctx := context.Background()
			pool := joinedPurgeTestPool(t, "owner_fence_migration")
			dir := t.TempDir()
			source := "../../../infra/sql/migrations"
			entries, err := os.ReadDir(source)
			if err != nil {
				t.Fatal(err)
			}
			copyMigration := func(name string) {
				t.Helper()
				data, err := os.ReadFile(filepath.Join(source, name))
				if err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			for _, e := range entries {
				if strings.HasSuffix(e.Name(), ".sql") && e.Name() < migration {
					copyMigration(e.Name())
				}
			}
			if err = db.MigrateUp(ctx, pool, dir); err != nil {
				t.Fatal(err)
			}
			var before, after string
			var deadlineBefore time.Time
			if bucket != "" {
				if _, err = pool.Exec(ctx, `INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256) VALUES($1,$2)`, bucket, r2tSHA); err != nil {
					t.Fatal(err)
				}
				if err = pool.QueryRow(ctx, `SELECT (to_jsonb(f)-'drain_not_before')::text,drain_not_before FROM r2_retirement_writer_fences f`).Scan(&before, &deadlineBefore); err != nil {
					t.Fatal(err)
				}
			}
			copyMigration(migration)
			if bucket == "stoarama" {
				for _, key := range []int64{7343226011187, 7343226011173} {
					blocker, err := pool.Begin(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if _, err = blocker.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, key); err != nil {
						t.Fatal(err)
					}
					err = db.MigrateUp(ctx, pool, dir)
					blocker.Rollback(ctx)
					if err == nil {
						t.Fatal("active wire/apply lock allowed timing change", key)
					}
					var unchanged bool
					if err = pool.QueryRow(ctx, `SELECT drain_not_before=$1 AND (to_jsonb(f)-'drain_not_before')::text=$2 AND EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='r2_retirement_writer_fences'::regclass AND tgname='r2_writer_fence_transition' AND tgenabled='O') FROM r2_retirement_writer_fences f`, deadlineBefore, before).Scan(&unchanged); err != nil || !unchanged {
						t.Fatal("failed migration changed fence", unchanged, err)
					}
				}
			}
			if err = db.MigrateUp(ctx, pool, dir); err != nil {
				t.Fatal(err)
			}
			var enabled string
			if err = pool.QueryRow(ctx, `SELECT tgenabled::text FROM pg_trigger WHERE tgrelid='r2_retirement_writer_fences'::regclass AND tgname='r2_writer_fence_transition'`).Scan(&enabled); err != nil || enabled != "O" {
				t.Fatal("fence trigger not enabled", enabled, err)
			}
			if bucket == "" {
				var count int
				if err = pool.QueryRow(ctx, `SELECT count(*) FROM r2_retirement_writer_fences`).Scan(&count); err != nil || count != 0 {
					t.Fatal("migration created a fence", count, err)
				}
				return
			}
			var deadline, applied time.Time
			if err = pool.QueryRow(ctx, `SELECT (to_jsonb(f)-'drain_not_before')::text,drain_not_before,applied_at FROM r2_retirement_writer_fences f CROSS JOIN schema_migrations WHERE version=$1`, migration).Scan(&after, &deadline, &applied); err != nil || before != after {
				t.Fatal("migration changed fence identity", before, after, err)
			}
			if bucket == "stoarama" && !deadline.Equal(applied) || bucket != "stoarama" && !deadline.Equal(deadlineBefore) {
				t.Fatal("unexpected fence deadline", deadline, applied)
			}
			if _, err = pool.Exec(ctx, `UPDATE r2_retirement_writer_fences SET drain_not_before=now()`); err == nil || !strings.Contains(err.Error(), "immutable") {
				t.Fatal("subsequent fence UPDATE was not rejected", err)
			}
		})
	}
}
