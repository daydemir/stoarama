package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/config"
)

// Exercise the actual `nas-stitch backlog` command against a disposable schema:
// read-only transaction, extended statement_timeout, JSON on stdout, and no
// writes at all.
func TestNASStitchBacklogCommandIsReadOnlyJSON(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if dsn == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("stitch_backlog_cmd_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	// A minimal window fixture: one approved scope on a stream outside the
	// pinned keep28 policy, two hours of clips with every one present on the NAS.
	base := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	setup := []string{
		`CREATE TABLE streams(id bigint PRIMARY KEY,local_timezone text)`,
		`CREATE TABLE nas_cleanup_frozen_streams(stream_id bigint)`,
		`CREATE TABLE recordings(id bigint PRIMARY KEY,account_id bigint,stream_id bigint,cron_timezone text)`,
		`CREATE TABLE recording_clips(id bigint PRIMARY KEY,recording_id bigint,object_key text,sha256 text,size_bytes bigint,display_path text,clip_start_at timestamptz,clip_end_at timestamptz,bucket text)`,
		`CREATE TABLE nas_inventory_files(connection_id bigint,clip_id bigint,recording_id bigint,relative_path text,size_bytes bigint,sha256 text,state text,client_updated_at timestamptz)`,
		`INSERT INTO streams VALUES(999,'UTC')`,
		`INSERT INTO recordings VALUES(99,47,999,'UTC')`,
		`INSERT INTO recording_clips VALUES(1,99,'k/1',repeat('a',64),3,'s/1.mp4','2026-09-20T00:00Z','2026-09-20T00:05Z','b')`,
		`INSERT INTO recording_clips VALUES(2,99,'k/2',repeat('b',64),3,'s/2.mp4','2026-09-20T01:00Z','2026-09-20T01:05Z','b')`,
		`INSERT INTO nas_inventory_files VALUES(13,1,99,'s/1.mp4',3,repeat('a',64),'present',now())`,
		`INSERT INTO nas_inventory_files VALUES(13,2,99,'s/2.mp4',3,repeat('b',64),'present',now())`,
		`CREATE TABLE nas_stitch_scopes (
 id bigserial PRIMARY KEY, stream_id bigint NOT NULL, start_at timestamptz NOT NULL, end_at timestamptz NOT NULL,
 reason text NOT NULL CHECK(length(reason) BETWEEN 1 AND 500), approved_until timestamptz NOT NULL,
 deletion_approved boolean NOT NULL DEFAULT false, paused boolean NOT NULL DEFAULT false,
 created_at timestamptz NOT NULL DEFAULT now(), CHECK(end_at>start_at AND end_at<=start_at+interval '29 days'))`,
		`CREATE TABLE nas_stitch_tasks (
 id bigserial PRIMARY KEY, scope_id bigint NOT NULL, recording_id bigint NOT NULL, hour_at timestamptz NOT NULL,
 plan jsonb NOT NULL, plan_sha256 text NOT NULL CHECK(plan_sha256 ~ '^[a-f0-9]{64}$'), state text NOT NULL DEFAULT 'pending',
 attempt integer NOT NULL DEFAULT 0, lease_token text, lease_expires_at timestamptz, result jsonb,
 cleanup_plan_id bigint, updated_at timestamptz NOT NULL DEFAULT now(), UNIQUE(recording_id,hour_at))`,
		`CREATE TABLE nas_stitch_outputs(task_id bigint NOT NULL,relative_path text UNIQUE,sha256 text,size_bytes bigint,proof jsonb,PRIMARY KEY(task_id,sha256))`,
		`INSERT INTO nas_stitch_scopes(stream_id,start_at,end_at,reason,approved_until)
 VALUES(999,'2026-09-19T00:00Z','2026-09-21T00:00Z','backlog command fixture',now()+interval '1 hour')`,
		`INSERT INTO nas_stitch_tasks(scope_id,recording_id,hour_at,plan,plan_sha256,state)
 VALUES(1,99,'` + base.Format("2006-01-02T15:04:05Z") + `','{}',repeat('c',64),'pending')`,
	}
	for _, stmt := range setup {
		if _, err = pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt[:40], err)
		}
	}
	// Prove the DSN actually points at this schema for the command.
	var countsBefore string
	if err = pool.QueryRow(ctx, `SELECT concat((SELECT count(*) FROM nas_stitch_tasks),'/',(SELECT count(*) FROM nas_stitch_scopes))`).Scan(&countsBefore); err != nil {
		t.Fatal(err)
	}

	// The command opens its own pool from cfg.DatabaseURL with search_path unset,
	// so scope the search path through the connection string's options.
	scoped := dsn
	if strings.Contains(dsn, "?") {
		scoped += "&options=-c%20search_path%3D" + schema
	} else {
		scoped += "?options=-c%20search_path%3D" + schema
	}
	out := captureStdout(t, func() {
		runNASStitchBacklog(context.Background(), config.Config{DatabaseURL: scoped}, nil)
	})
	var report api.NASStitchBacklogReport
	if err = json.Unmarshal(out, &report); err != nil {
		t.Fatalf("stdout is not a JSON report: %v\n%s", err, out)
	}
	if report.Policy != "v85r" || report.Source != "scope" || len(report.Streams) != 1 {
		t.Fatalf("report shape: %+v", report)
	}
	s := report.Streams[0]
	if s.StreamID != 999 || s.RecordingHours != 2 || s.ReadyHours != 2 || s.MissingClipsHours != 0 {
		t.Fatalf("counts: %+v", s)
	}
	if s.TaskHoursByState["pending"] != 1 || s.RemainingReadyHours != 1 || s.RemainingReadyInputBytes != 3 {
		t.Fatalf("task state/remaining: %+v", s)
	}
	if report.Total.RecordingHours != 2 || report.Total.ReadyHours != 2 {
		t.Fatalf("totals: %+v", report.Total)
	}
	// Read-only command: nothing was written.
	var countsAfter string
	if err = pool.QueryRow(ctx, `SELECT concat((SELECT count(*) FROM nas_stitch_tasks),'/',(SELECT count(*) FROM nas_stitch_scopes))`).Scan(&countsAfter); err != nil {
		t.Fatal(err)
	}
	if countsAfter != countsBefore {
		t.Fatalf("command wrote rows: %s -> %s", countsBefore, countsAfter)
	}
	// Unknown arguments and unknown flags are rejected, not silently ignored.
	for _, args := range [][]string{{"unexpected"}, {"--nope"}} {
		if err := parseNASStitchBacklogArgs(args); err == nil {
			t.Fatalf("stray arguments accepted: %v", args)
		}
	}
	if err := parseNASStitchBacklogArgs(nil); err != nil {
		t.Fatalf("no arguments refused: %v", err)
	}
	if err := parseNASStitchBacklogArgs([]string{}); err != nil {
		t.Fatalf("empty arguments refused: %v", err)
	}
}
