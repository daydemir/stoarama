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

func TestRecordingCanaryPreopenMigrationRunsThroughMigrateUp(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("canary_preopen_migration_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
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
	for _, stmt := range []string{
		`CREATE TABLE accounts(id BIGINT PRIMARY KEY)`,
		`CREATE TABLE nodes(id BIGINT PRIMARY KEY)`,
		`CREATE TABLE recordings(id BIGINT PRIMARY KEY)`,
		`CREATE TABLE recording_canary_reservations(id UUID PRIMARY KEY,recording_id BIGINT,node_id BIGINT,expires_at TIMESTAMPTZ,created_at TIMESTAMPTZ NOT NULL DEFAULT now())`,
	} {
		if _, err = pool.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	dir := t.TempDir()
	raw, err := os.ReadFile("../../../infra/sql/migrations/0134_recording_canary_preopen_evidence.sql")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "0134_recording_canary_preopen_evidence.sql"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err = MigrateUp(ctx, pool, dir); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO accounts VALUES(3); INSERT INTO nodes VALUES(2); INSERT INTO recordings VALUES(1); INSERT INTO recording_canary_reservations(id,recording_id,node_id,expires_at,window_start_at,preopen_stage) VALUES(gen_random_uuid(),1,2,now()+interval '3 minutes',now()+interval '90 minutes','early')`); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO recording_canary_preopen_evidence(reservation_id,recording_id,node_id,account_id,window_start_at,stage,duration_ms,size_bytes,media_sha256,video_codec,relay_version,source_revision,probe_ok,decode_ok,native_copy,uploaded,reservation_created_at) SELECT id,1,2,3,window_start_at,'early',15000,1,$1,'h264','v1','r1',true,true,true,false,created_at FROM recording_canary_reservations LIMIT 1`, strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_canary_preopen_evidence SET size_bytes=2`); err == nil {
		t.Fatal("append-only evidence updated")
	}
	if _, err = pool.Exec(ctx, `DELETE FROM recordings WHERE id=1`); err != nil {
		t.Fatalf("logical evidence unexpectedly blocked parent cleanup: %v", err)
	}
	var evidence int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM recording_canary_preopen_evidence WHERE recording_id=1`).Scan(&evidence); err != nil || evidence != 1 {
		t.Fatalf("durable evidence count=%d err=%v", evidence, err)
	}
}
