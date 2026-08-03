package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestMaintainTransientLedgersDeletesOnlyExpiredBookkeeping(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run transient-ledger retention regression")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("transient_retention_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`) }()

	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	old := now.Add(-transientLedgerRetention - time.Hour)
	recent := now.Add(-time.Hour)
	for _, ddl := range []string{
		`CREATE TABLE upload_intents (
		  id TEXT PRIMARY KEY, status TEXT NOT NULL, expires_at TIMESTAMPTZ NOT NULL,
		  created_at TIMESTAMPTZ NOT NULL
		)`,
		`CREATE TABLE api_idempotency (
		  endpoint TEXT NOT NULL, idempotency_key TEXT NOT NULL,
		  created_at TIMESTAMPTZ NOT NULL,
		  PRIMARY KEY(endpoint,idempotency_key)
		)`,
		`CREATE TABLE capture_segments (id BIGINT PRIMARY KEY)`,
		`CREATE TABLE recording_clips (id BIGINT PRIMARY KEY)`,
		`CREATE TABLE media_objects (id BIGINT PRIMARY KEY)`,
		`CREATE TABLE recording_upload_intents (id BIGINT PRIMARY KEY)`,
	} {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	for _, table := range []string{"capture_segments", "recording_clips", "media_objects", "recording_upload_intents"} {
		if _, err := pool.Exec(ctx, `INSERT INTO `+table+` VALUES (1)`); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO upload_intents VALUES
		  ('old-consumed','consumed',$1,$1),
		  ('old-pending-expired','pending',$1,$1),
		  ('old-pending-live','pending',$2,$1),
		  ('recent-consumed','consumed',$2,$3)
	`, old, now.Add(time.Hour), recent); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO api_idempotency VALUES
		  ('endpoint','old',$1), ('endpoint','recent',$2)
	`, old, recent); err != nil {
		t.Fatal(err)
	}

	got, err := maintainTransientLedgers(ctx, pool, now)
	if err != nil {
		t.Fatal(err)
	}
	if got.UploadIntentsDeleted != 2 || got.IdempotencyKeysDeleted != 1 {
		t.Fatalf("maintenance=%+v, want upload=2 idempotency=1", got)
	}
	for id, want := range map[string]bool{
		"old-consumed":        false,
		"old-pending-expired": false,
		"old-pending-live":    true,
		"recent-consumed":     true,
	} {
		var exists bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM upload_intents WHERE id=$1)`, id).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists != want {
			t.Fatalf("upload_intents id=%q exists=%t want %t", id, exists, want)
		}
	}
	for key, want := range map[string]bool{"old": false, "recent": true} {
		var exists bool
		if err := pool.QueryRow(ctx, `
			SELECT EXISTS(
				SELECT 1 FROM api_idempotency
				WHERE endpoint='endpoint' AND idempotency_key=$1
			)
		`, key).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists != want {
			t.Fatalf("api_idempotency endpoint=%q key=%q exists=%t want %t", "endpoint", key, exists, want)
		}
	}
	for table, want := range map[string]int{
		"upload_intents": 2, "api_idempotency": 1, "capture_segments": 1,
		"recording_clips": 1, "media_objects": 1, "recording_upload_intents": 1,
	} {
		var count int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM `+table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != want {
			t.Fatalf("%s rows=%d want %d", table, count, want)
		}
	}
}
