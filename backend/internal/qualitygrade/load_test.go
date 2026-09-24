package qualitygrade

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestLoadGradesCompletedWindowsPerLocalDay(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run DB-backed grade loader test")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("quality_grade_%d", time.Now().UnixNano())
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

	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, `
		CREATE TABLE recordings (id BIGINT PRIMARY KEY, account_id BIGINT NOT NULL, name TEXT NOT NULL, status TEXT NOT NULL, mode TEXT NOT NULL, cron_timezone TEXT NOT NULL);
		CREATE TABLE recording_jobs (id BIGINT PRIMARY KEY, recording_id BIGINT NOT NULL, fire_at TIMESTAMPTZ NOT NULL, kind TEXT NOT NULL, window_end_at TIMESTAMPTZ);
		CREATE TABLE recording_window_health (recording_id BIGINT NOT NULL, job_id BIGINT NOT NULL, coverage_pct DOUBLE PRECISION NOT NULL, largest_gap_seconds DOUBLE PRECISION NOT NULL,
		  overlap_count INT NOT NULL, clip_count INT NOT NULL, calculated_at TIMESTAMPTZ NOT NULL, gap_over_30s_count INT, gap_over_5m_count INT, metric_version INT);
		INSERT INTO recordings VALUES
		  (402, 47, 'Seoul', 'active', 'continuous', 'Asia/Seoul'),
		  (500, 47, 'paused', 'paused', 'continuous', 'UTC'),
		  (600, 48, 'other account', 'active', 'continuous', 'UTC');
		-- Seoul windows open 08:00 KST (23:00Z the previous UTC day).
		INSERT INTO recording_jobs VALUES
		  (1, 402, '2026-09-20 23:00Z', 'continuous_window', '2026-09-21 11:00Z'),
		  (2, 402, '2026-09-21 23:00Z', 'continuous_window', '2026-09-22 11:00Z'),
		  (3, 402, '2026-09-22 23:00Z', 'continuous_window', '2026-09-23 11:00Z'),
		  (4, 402, '2026-09-23 23:00Z', 'continuous_window', '2026-09-24 11:00Z'),
		  (5, 402, '2026-09-24 11:30Z', 'clip', NULL),
		  (6, 500, '2026-09-22 08:00Z', 'continuous_window', '2026-09-22 20:00Z'),
		  (7, 600, '2026-09-22 08:00Z', 'continuous_window', '2026-09-22 20:00Z');
		INSERT INTO recording_window_health VALUES
		  (402, 1, 99.6, 88, 0, 720, '2026-09-21 11:30Z', 0, 0, 2),
		  (402, 2, 72.1, 2763, 0, 520, '2026-09-22 11:30Z', 30, 8, 2),
		  (402, 3, 99.9, 20, 0, 720, '2026-09-23 10:30Z', 0, 0, 2),
		  (600, 7, 99.9, 20, 0, 720, '2026-09-22 20:30Z', 0, 0, 2);
	`); err != nil {
		t.Fatal(err)
	}
	recs, err := Load(ctx, pool, 47, now, 14)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || recs[0].RecordingID != 402 {
		t.Fatalf("recordings=%+v want only active account-47 recording 402", recs)
	}
	got := []string{}
	for _, w := range recs[0].Windows {
		got = append(got, w.LocalDate.Format("01-02")+":"+string(w.Grade))
	}
	// Job 3's row predates its close (outdated => unknown); job 4 closed one
	// hour ago and is not yet measured, so it is not graded yet.
	if want := "09-21:A 09-22:E 09-23:unknown"; strings.Join(got, " ") != want {
		t.Fatalf("windows=%s want %s", strings.Join(got, " "), want)
	}
	good := recs[0].TierProgress(TierGood)
	if good.RunDays != 0 || good.DaysToGo != 14 {
		t.Fatalf("good+ after unknown = %+v", good)
	}
	all, err := Load(ctx, pool, 0, now, 14)
	if err != nil || len(all) != 2 {
		t.Fatalf("all accounts=%d err=%v want 2", len(all), err)
	}
}
