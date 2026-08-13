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

func TestRecordingPresentationV2MigrationRunsThroughMigrateUp(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("presentation_v2_migration_%d", time.Now().UnixNano())
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
	if err = MigrateUp(ctx, pool, filepath.Join("..", "..", "..", "infra", "sql", "migrations")); err != nil {
		t.Fatalf("MigrateUp with presentation v2 C1: %v", err)
	}
	for _, table := range []string{
		"recording_presentation_v2_admissions",
		"recording_presentation_v2_attempts",
		"recording_presentation_v2_probe_tasks",
		"recording_presentation_v2_authored_facts",
		"recording_presentation_v2_fact_axes",
		"recording_presentation_v2_packet_edges",
		"recording_presentation_v2_raw_extents",
		"recording_presentation_v2_video_frame_edges",
		"recording_presentation_v2_audio_block_edges",
		"recording_presentation_v2_release_authorizations",
		"recording_presentation_v2_release_acknowledgements",
	} {
		var present *string
		if err = pool.QueryRow(ctx, `SELECT to_regclass($1)::text`, table).Scan(&present); err != nil || present == nil {
			t.Fatalf("table %s missing: present=%v err=%v", table, present, err)
		}
	}
}
