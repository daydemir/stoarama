package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestR2FenceIndexMigration(t *testing.T) {
	pool := joinedPurgeTestPool(t, "r2_fence_index")
	ctx := context.Background()
	body, err := os.ReadFile(filepath.Join(joinedPurgeMigrationsDir(), "0185_nas_delivery_retirement_fence.sql"))
	if err != nil {
		t.Fatal(err)
	}
	exec := func(sql string) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	exec(`CREATE TABLE connections(id bigint PRIMARY KEY);
CREATE TABLE recording_clips(display_path text, padding text)`)
	for _, tc := range []struct {
		name, precreate string
		valid           bool
	}{
		{"fresh", "", true},
		{"concurrent exact", `CREATE INDEX CONCURRENTLY nas_delivery_raw_writer_path ON recording_clips(display_path) WHERE display_path<>''`, true},
		{"wrong key", `CREATE INDEX nas_delivery_raw_writer_path ON recording_clips(padding) WHERE display_path<>''`, false},
		{"wrong predicate", `CREATE INDEX nas_delivery_raw_writer_path ON recording_clips(display_path)`, false},
		{"wrong method", `CREATE INDEX nas_delivery_raw_writer_path ON recording_clips USING hash(display_path) WHERE display_path<>''`, false},
		{"wrong opclass", `CREATE INDEX nas_delivery_raw_writer_path ON recording_clips(display_path text_pattern_ops) WHERE display_path<>''`, false},
		{"wrong collation", `CREATE INDEX nas_delivery_raw_writer_path ON recording_clips(display_path COLLATE "C") WHERE display_path<>''`, false},
		{"wrong order", `CREATE INDEX nas_delivery_raw_writer_path ON recording_clips(display_path DESC) WHERE display_path<>''`, false},
		{"included column", `CREATE INDEX nas_delivery_raw_writer_path ON recording_clips(display_path) INCLUDE(padding) WHERE display_path<>''`, false},
		{"unique", `CREATE UNIQUE INDEX nas_delivery_raw_writer_path ON recording_clips(display_path) WHERE display_path<>''`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.precreate != "" {
				// The operator's concurrent build must work outside the migration.
				exec(tc.precreate)
				defer exec(`DROP INDEX nas_delivery_raw_writer_path`)
			}
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			_, err = tx.Exec(ctx, string(body))
			if tc.valid {
				if err != nil {
					t.Fatalf("valid migration: %v", err)
				}
				if _, err := tx.Exec(ctx, `SELECT 1`); err != nil {
					t.Fatalf("migration closed runner transaction: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "not the valid exact delivery writer index") {
				t.Fatalf("wrong precreated index accepted: %v", err)
			}
		})
	}
	// A failed concurrent build leaves a named, invalid index. Its name alone
	// must not let the migration pass.
	exec(`INSERT INTO recording_clips(display_path) VALUES('duplicate'),('duplicate')`)
	if _, err := pool.Exec(ctx, `CREATE UNIQUE INDEX CONCURRENTLY nas_delivery_raw_writer_path ON recording_clips(display_path) WHERE display_path<>''`); err == nil {
		t.Fatal("expected duplicate build failure")
	}
	if _, err := pool.Exec(ctx, string(body)); err == nil || !strings.Contains(err.Error(), "not the valid exact delivery writer index") {
		t.Fatalf("invalid precreated index accepted: %v", err)
	}
	exec(`DROP INDEX nas_delivery_raw_writer_path; TRUNCATE recording_clips;
ALTER TABLE recording_clips ALTER COLUMN padding SET STORAGE PLAIN;
INSERT INTO recording_clips SELECT n::text,repeat(md5(n::text),128) FROM generate_series(1,18000) n`)
	var large bool
	if err := pool.QueryRow(ctx, `SELECT pg_relation_size('recording_clips')>134217728`).Scan(&large); err != nil || !large {
		t.Fatal("large-table fixture did not cross guard", err)
	}
	if _, err := pool.Exec(ctx, string(body)); err == nil || !strings.Contains(err.Error(), "precreate nas_delivery_raw_writer_path") {
		t.Fatalf("large unindexed table did not refuse transactional build: %v", err)
	}
	exec(`CREATE INDEX CONCURRENTLY nas_delivery_raw_writer_path ON recording_clips(display_path) WHERE display_path<>''`)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, string(body)); err != nil {
		t.Fatalf("large table with exact concurrent precreate: %v", err)
	}
}
