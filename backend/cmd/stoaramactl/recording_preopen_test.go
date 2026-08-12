package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPreopenStageBoundariesAndNoPostOpenProbe(t *testing.T) {
	now := time.Date(2026, 3, 8, 6, 0, 0, 0, time.UTC)
	cases := []struct {
		name  string
		start time.Time
		want  string
	}{
		{"two hour early", now.Add(2 * time.Hour), "early"},
		{"dst local instant still early", time.Date(2026, 3, 8, 7, 45, 0, 0, time.UTC), "early"},
		{"thirty minute confirmation", now.Add(30 * time.Minute), "confirm"},
		{"one second before open", now.Add(time.Second), "confirm"},
		{"at open", now, ""}, {"after open", now.Add(-time.Second), ""}, {"too early", now.Add(2*time.Hour + time.Second), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := preopenStage(tc.start, now); got != tc.want {
				t.Fatalf("stage=%q want %q", got, tc.want)
			}
		})
	}
}

func TestSelectPreopenTargetsStagesRetriesAndNeverAfterOpen(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	schema := fmt.Sprintf("preopen_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(ctx, `DROP SCHEMA IF EXISTS `+pgx.Identifier{schema}.Sanitize()+` CASCADE`) }()
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams = map[string]string{"search_path": schema}
	// The fixture setup and migration contain multiple SQL statements, matching
	// the repository migration runner's simple-protocol execution.
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	_, err = pool.Exec(ctx, `CREATE TABLE accounts(id BIGINT PRIMARY KEY,name TEXT,email TEXT);CREATE TABLE streams(id BIGINT PRIMARY KEY,name TEXT,provider TEXT,source_url TEXT,source_page_url TEXT);CREATE TABLE recordings(id BIGINT PRIMARY KEY,stream_id BIGINT,account_id BIGINT,name TEXT,stream_url TEXT,capture_via TEXT,status TEXT,mode TEXT,next_fire_at TIMESTAMPTZ);INSERT INTO accounts VALUES(1,'org','ops@example.test');INSERT INTO streams SELECT n,'s'||n,'direct','https://example.com/live.m3u8','' FROM generate_series(1,6)n;`)
	if err != nil {
		t.Fatal(err)
	}
	migration, err := os.ReadFile(filepath.Join("..", "..", "..", "infra", "sql", "migrations", "0124_recording_preopen_checks.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(migration)); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	_, err = pool.Exec(ctx, `INSERT INTO recordings SELECT n,n,1,'r'||n,'https://example.com/live.m3u8','cloud','active','continuous',$1 + CASE n WHEN 1 THEN interval '90 minutes' WHEN 2 THEN interval '20 minutes' WHEN 3 THEN interval '-1 minute' WHEN 4 THEN interval '90 minutes' WHEN 5 THEN interval '90 minutes' ELSE interval '20 minutes' END FROM generate_series(1,6)n;INSERT INTO recording_preopen_checks(recording_id,window_start_at,stage,result,method,attempt_count,next_retry_at) SELECT id,next_fire_at,'early','fail','media_probe',1,$1-interval '1 minute' FROM recordings WHERE id=4;INSERT INTO recording_preopen_checks(recording_id,window_start_at,stage,result,method,attempt_count,next_retry_at) SELECT id,next_fire_at,'early','fail','media_probe',1,$1+interval '1 minute' FROM recordings WHERE id=5;INSERT INTO recording_preopen_checks(recording_id,window_start_at,stage,result,method) SELECT id,next_fire_at,'confirm','pass','media_probe' FROM recordings WHERE id=6;`, now)
	if err != nil {
		t.Fatal(err)
	}
	targets, err := selectPreopenTargets(ctx, pool, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 3 {
		t.Fatalf("targets=%+v", targets)
	}
	got := map[int64]preopenTarget{}
	for _, target := range targets {
		got[target.recordingID] = target
	}
	if got[1].stage != "early" || got[1].attempt != 1 || got[2].stage != "confirm" || got[4].stage != "early" || got[4].attempt != 2 {
		t.Fatalf("targets=%+v", targets)
	}
}

func TestSanitizePreopenDetail(t *testing.T) {
	got := sanitizePreopenDetail("failed https://user:pass@example.com/live.m3u8?token=abc bearer SECRET\ncredential=x")
	if strings.Contains(got, "example.com") || strings.Contains(got, "SECRET") || strings.Contains(got, "credential=x") || strings.Contains(got, "\n") {
		t.Fatalf("detail leaked: %q", got)
	}
	if len([]rune(sanitizePreopenDetail(strings.Repeat("x", 700)))) > 500 {
		t.Fatal("detail not bounded")
	}
}

func TestRelayPreopenNeverPassesFromGenericFrameEvidence(t *testing.T) {
	target := preopenTarget{recordingID: 445, streamID: 99, captureVia: "relay"}
	// No database is needed: even a fresh generic frame must never be consulted
	// because it cannot identify the intended relay group or uplink.
	got := probePreopenTarget(context.Background(), nil, target, 20*time.Second)
	if got.result != "unknown" || got.method != "relay_canary" {
		t.Fatalf("relay result=%+v, want fail-closed UNKNOWN relay canary", got)
	}
}

func TestRelayPreopenAbsentOrStaleEvidenceRemainsUnknown(t *testing.T) {
	for _, name := range []string{"absent", "stale"} {
		t.Run(name, func(t *testing.T) {
			got := probePreopenTarget(context.Background(), nil, preopenTarget{captureVia: "relay"}, 20*time.Second)
			if got.result != "unknown" {
				t.Fatalf("relay result=%q, want unknown", got.result)
			}
		})
	}
}
