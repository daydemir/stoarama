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

func TestMaterializeRecordingWindowHealthPersistsExactTimelineAndSummary(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run DB-backed health summary regression")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("health_summary_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`) }()
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `
		CREATE TABLE recordings (id BIGINT PRIMARY KEY);
		CREATE TABLE recording_jobs (id BIGINT PRIMARY KEY,recording_id BIGINT NOT NULL,kind TEXT NOT NULL,fire_at TIMESTAMPTZ NOT NULL,window_end_at TIMESTAMPTZ NOT NULL);
		CREATE TABLE recording_clips (
		  id BIGINT PRIMARY KEY,recording_id BIGINT NOT NULL,clip_start_at TIMESTAMPTZ NOT NULL,clip_end_at TIMESTAMPTZ NOT NULL,
		  video_codec TEXT,audio_codec TEXT,audio_present BOOLEAN,video_width INTEGER,video_height INTEGER,created_at TIMESTAMPTZ NOT NULL DEFAULT now());
		CREATE TABLE recording_window_health (
		  recording_id BIGINT NOT NULL,job_id BIGINT NOT NULL,window_start_at TIMESTAMPTZ NOT NULL,window_end_at TIMESTAMPTZ NOT NULL,
		  expected_seconds BIGINT NOT NULL,covered_seconds DOUBLE PRECISION NOT NULL,coverage_pct DOUBLE PRECISION NOT NULL,
		  largest_gap_seconds DOUBLE PRECISION NOT NULL,gap_count INTEGER NOT NULL,overlap_count INTEGER NOT NULL,
		  overlap_seconds DOUBLE PRECISION NOT NULL,longest_run_seconds DOUBLE PRECISION NOT NULL,layout_change_count INTEGER NOT NULL,
		  clip_count INTEGER NOT NULL,calculated_at TIMESTAMPTZ NOT NULL,PRIMARY KEY(recording_id,job_id));
		CREATE TABLE recording_health_summaries (
		  recording_id BIGINT PRIMARY KEY,recent_expected_seconds BIGINT NOT NULL,recent_covered_seconds DOUBLE PRECISION NOT NULL,recent_coverage_pct DOUBLE PRECISION,
		  recent_largest_gap_seconds DOUBLE PRECISION NOT NULL,recent_gap_count BIGINT NOT NULL,recent_overlap_count BIGINT NOT NULL,recent_overlap_seconds DOUBLE PRECISION NOT NULL,
		  recent_layout_change_count BIGINT NOT NULL,recent_window_count INTEGER NOT NULL,lifetime_expected_seconds BIGINT NOT NULL,lifetime_covered_seconds DOUBLE PRECISION NOT NULL,
		  lifetime_coverage_pct DOUBLE PRECISION,lifetime_largest_gap_seconds DOUBLE PRECISION NOT NULL,lifetime_gap_count BIGINT NOT NULL,lifetime_overlap_count BIGINT NOT NULL,
		  lifetime_overlap_seconds DOUBLE PRECISION NOT NULL,lifetime_layout_change_count BIGINT NOT NULL,lifetime_window_count INTEGER NOT NULL,lifetime_complete BOOLEAN NOT NULL,calculated_at TIMESTAMPTZ NOT NULL);
	`); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	start := now.Add(-2 * time.Hour)
	if _, err := pool.Exec(ctx, `
		INSERT INTO recordings VALUES (402);
		INSERT INTO recording_jobs VALUES (1,402,'continuous_window',$1,$2);
		INSERT INTO recording_clips VALUES
		  (1,402,$1,$1::timestamptz+interval '20 minutes','h264','aac',true,1280,720),
		  (2,402,$1::timestamptz+interval '30 minutes',$2,'h264','',false,1280,720)
	`, start, start.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := materializeRecordingWindowHealth(ctx, pool, now); err != nil {
		t.Fatal(err)
	}
	var coverage, largestGap float64
	var gaps, overlaps, layouts, clips int
	if err := pool.QueryRow(ctx, `SELECT coverage_pct,largest_gap_seconds,gap_count,overlap_count,layout_change_count,clip_count FROM recording_window_health WHERE recording_id=402`).Scan(&coverage, &largestGap, &gaps, &overlaps, &layouts, &clips); err != nil {
		t.Fatal(err)
	}
	if coverage < 83.32 || coverage > 83.34 || largestGap != 600 || gaps != 1 || overlaps != 0 || layouts != 1 || clips != 2 {
		t.Fatalf("window coverage=%f gap=%f gaps=%d overlaps=%d layouts=%d clips=%d", coverage, largestGap, gaps, overlaps, layouts, clips)
	}
	var recentCoverage, lifetimeCoverage float64
	var recentWindows, lifetimeWindows int
	if err := pool.QueryRow(ctx, `SELECT recent_coverage_pct,lifetime_coverage_pct,recent_window_count,lifetime_window_count FROM recording_health_summaries WHERE recording_id=402`).Scan(&recentCoverage, &lifetimeCoverage, &recentWindows, &lifetimeWindows); err != nil {
		t.Fatal(err)
	}
	if recentCoverage != coverage || lifetimeCoverage != coverage || recentWindows != 1 || lifetimeWindows != 1 {
		t.Fatalf("summary recent=%f lifetime=%f windows=%d/%d", recentCoverage, lifetimeCoverage, recentWindows, lifetimeWindows)
	}
}
