package db_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestAuthoritativeFrameSourceMigrationPreservesSurveyAndAddsIdentityFence(t *testing.T) {
	url := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if url == "" {
		t.Skip("DATABASE_URL is required")
	}
	ctx := context.Background()
	c, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	schema := fmt.Sprintf("authoritative_frame_migration_%d", time.Now().UnixNano())
	if _, err = c.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer c.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
	if _, err = c.Exec(ctx, "SET search_path TO "+schema); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, `CREATE TABLE frames(id bigserial primary key,stream_id bigint not null,captured_at timestamptz not null,source_kind text not null CONSTRAINT legacy_source_kind CHECK(source_kind IN ('live','snapshot_url','survey')))`); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "infra", "sql", "migrations", "0130_authoritative_frame_source.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, string(raw)); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, `INSERT INTO frames(stream_id,captured_at,source_kind) VALUES(1,now(),'survey'),(1,now()+interval '1 second','authoritative_frame_refresh')`); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, `INSERT INTO frames(stream_id,captured_at,source_kind) SELECT stream_id,captured_at,source_kind FROM frames WHERE source_kind='authoritative_frame_refresh'`); err == nil {
		t.Fatal("duplicate authoritative identity accepted")
	}
}
