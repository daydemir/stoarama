package db

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestRecordingClipFrameProvenanceMigration(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	c, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	schema := fmt.Sprintf("clip_frame_migration_%d", time.Now().UnixNano())
	if _, err = c.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer c.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
	if _, err = c.Exec(ctx, "SET search_path="+schema); err != nil {
		t.Fatal(err)
	}
	for _, ddl := range []string{
		`CREATE TABLE recording_clips(id bigint primary key)`,
		`CREATE TABLE frames(id bigserial primary key,stream_id bigint,captured_at timestamptz,raw_media_object_id bigint,source_kind text)`,
		`INSERT INTO recording_clips VALUES(1),(2)`,
		`INSERT INTO frames(stream_id,captured_at,raw_media_object_id,source_kind) VALUES(9,now(),3,'authoritative_frame_refresh')`,
	} {
		if _, err = c.Exec(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := os.ReadFile("../../../infra/sql/migrations/0131_recording_clip_frame_provenance.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, string(raw)); err != nil {
		t.Fatal(err)
	}
	sha := strings.Repeat("a", 64)
	if _, err = c.Exec(ctx, `INSERT INTO frames(stream_id,captured_at,source_kind,source_recording_clip_id,source_recording_clip_sha256,source_recording_clip_etag) VALUES(9,now(),'authoritative_frame_refresh',1,$1,'etag')`, sha); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, `UPDATE frames SET source_recording_clip_etag='other' WHERE source_recording_clip_id=1`); err == nil {
		t.Fatal("provenance mutation succeeded")
	}
	if _, err = c.Exec(ctx, `INSERT INTO frames(stream_id,captured_at,source_kind,source_recording_clip_id,source_recording_clip_sha256) VALUES(9,now(),'authoritative_frame_refresh',2,$1)`, sha); err == nil {
		t.Fatal("partial provenance succeeded")
	}
	if _, err = c.Exec(ctx, `INSERT INTO frames(stream_id,captured_at,source_kind,source_recording_clip_id,source_recording_clip_sha256,source_recording_clip_etag) VALUES(9,now(),'authoritative_frame_refresh',1,$1,'etag')`, sha); err == nil {
		t.Fatal("duplicate source clip succeeded")
	}
}
