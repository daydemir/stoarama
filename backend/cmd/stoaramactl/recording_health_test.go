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

func TestShouldNotifyHealthIncident(t *testing.T) {
	runStart := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name          string
		newlyInserted bool
		lastAlertedAt time.Time
		want          bool
	}{
		{"newly inserted always notifies", true, runStart.Add(-48 * time.Hour), true},
		{"restamped this cycle notifies", false, runStart.Add(2 * time.Second), true},
		{"restamped exactly at runStart notifies", false, runStart, true},
		{"stale last_alerted stays quiet", false, runStart.Add(-3 * time.Hour), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldNotifyHealthIncident(tc.newlyInserted, tc.lastAlertedAt, runStart); got != tc.want {
				t.Fatalf("shouldNotifyHealthIncident=%v want %v", got, tc.want)
			}
		})
	}
}

func sampleIncidents() []healthIncident {
	return []healthIncident{
		{
			RecordingID: 101, StreamID: 11, OrgName: "MIT", OrgEmail: "ops@mit.edu",
			RecName: "Kresge Plaza", StreamURL: "https://cam/1.m3u8",
			Signal: signalContinuousSilentDeath, Severity: "CRITICAL",
			SinceText: "window opened 2026-07-04T13:00:00Z, last clip never",
			Diag:      "last_error=ffmpeg exited",
		},
		{
			RecordingID: 202, StreamID: 22, OrgName: "Lab", OrgEmail: "lab@stoarama.com",
			RecName: "Corner", StreamURL: "https://cam/2.m3u8",
			Signal: signalJobRetriesExhausted, Severity: "HIGH",
			SinceText: "2026-07-04T13:20:00Z",
			Diag:      "job_id=999 kind=clip attempts=3/3 error=timeout",
		},
	}
}

func TestComposeHealthEmailSubject(t *testing.T) {
	one := sampleIncidents()[:1]
	if got := composeHealthEmailSubject(one); got != "[Stoarama] Recording 101 unhealthy: "+healthSignalLabels[signalContinuousSilentDeath] {
		t.Fatalf("single subject unexpected: %q", got)
	}
	if got := composeHealthEmailSubject(sampleIncidents()); got != "[Stoarama] 2 recording health alert(s)" {
		t.Fatalf("multi subject unexpected: %q", got)
	}
}

func TestComposeHealthEmailBody(t *testing.T) {
	body := composeHealthEmailBody("https://stoarama.test/", sampleIncidents())
	for _, want := range []string{
		"2 recording health issue(s)",
		"Recording #101 \"Kresge Plaza\"",
		"MIT <ops@mit.edu>",
		"https://cam/1.m3u8",
		"Stoarama: https://stoarama.test/streams/11",
		"Recording: https://stoarama.test/recordings/101",
		healthSignalLabels[signalContinuousSilentDeath] + " [CRITICAL]",
		"last_error=ffmpeg exited",
		"Recording #202 \"Corner\"",
		"job_id=999 kind=clip attempts=3/3 error=timeout",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("body missing %q\n---\n%s", want, body)
		}
	}
}

func TestDiagTextDropsBlanks(t *testing.T) {
	got := diagText("job_id", "5", "error", "  ", "kind", "clip")
	if got != "job_id=5 kind=clip" {
		t.Fatalf("diagText=%q", got)
	}
}

// TestDetectClipTimestampDriftFindsWorstClipNotNewest pins the two decisions that
// make this signal useful rather than decorative: it fires only past the drift
// limit, and it judges the WORST clip of the last hour rather than the newest one.
//
// The second matters because a drifted chain re-anchors whenever its ffmpeg
// restarts. Zanzibar's most recent clips were healthy at the very moment an hour
// of its footage was being filed under the wrong day, so a newest-clip check would
// have reported it fine and the drift would have kept running unseen.
func TestDetectClipTimestampDriftFindsWorstClipNotNewest(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run DB-backed clip drift regression")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("clip_drift_%d", time.Now().UnixNano())
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

	if _, err := pool.Exec(ctx, `
		CREATE TABLE accounts (id BIGINT PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL);
		CREATE TABLE recordings (
			id BIGINT PRIMARY KEY, stream_id BIGINT, account_id BIGINT NOT NULL,
			name TEXT NOT NULL, stream_url TEXT NOT NULL,
			status TEXT NOT NULL, mode TEXT NOT NULL);
		CREATE TABLE recording_clips (
			recording_id BIGINT NOT NULL,
			clip_start_at TIMESTAMPTZ NOT NULL,
			created_at TIMESTAMPTZ NOT NULL);
		INSERT INTO accounts VALUES (1, 'MIT SCL', 'scl@example.edu');
		INSERT INTO recordings VALUES
		  (10, 100, 1, 'healthy',    'https://e.test/a', 'active', 'continuous'),
		  (11, 101, 1, 'marginal',   'https://e.test/b', 'active', 'continuous'),
		  (12, 102, 1, 'reanchored', 'https://e.test/c', 'active', 'continuous'),
		  (13, 103, 1, 'stale',      'https://e.test/d', 'active', 'continuous'),
		  (14, 104, 1, 'exactly90',  'https://e.test/e', 'active', 'continuous');
	`); err != nil {
		t.Fatal(err)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO recording_clips (recording_id, clip_start_at, created_at) VALUES
		  -- healthy: stamped BEFORE ingest, which is the normal ordering
		  (10, now() - interval '5 minutes' - interval '60 seconds', now() - interval '5 minutes'),
		  -- marginal: leads by 30s, under the 90s limit, so end-of-window truncated
		  -- clips do not page anyone
		  (11, now() - interval '5 minutes' + interval '30 seconds', now() - interval '5 minutes'),
		  -- reanchored: an hour of drifted clips followed by a healthy newest clip,
		  -- exactly the shape that hid Zanzibar
		  (12, now() - interval '30 minutes' + interval '4 hours',   now() - interval '30 minutes'),
		  (12, now() - interval '1 minute'  - interval '60 seconds', now() - interval '1 minute'),
		  -- stale: badly drifted but outside the one-hour window, so it is history,
		  -- not a live incident
		  (13, now() - interval '5 hours' + interval '4 hours', now() - interval '5 hours'),
		  -- exactly at the limit: the comparison is strictly greater-than, so the
		  -- threshold itself is still healthy and must not page anyone
		  (14, now() - interval '5 minutes' + interval '90 seconds', now() - interval '5 minutes');
	`); err != nil {
		t.Fatal(err)
	}

	got := detectClipTimestampDrift(ctx, pool)
	if len(got) != 1 {
		ids := []int64{}
		for _, i := range got {
			ids = append(ids, i.RecordingID)
		}
		t.Fatalf("incidents=%v (recordings %v), want exactly recording 12", len(got), ids)
	}
	if got[0].RecordingID != 12 {
		t.Fatalf("flagged recording %d, want 12 (the re-anchored one)", got[0].RecordingID)
	}
	if got[0].Signal != signalClipTimestampDrift || got[0].Severity != "CRITICAL" {
		t.Fatalf("signal=%q severity=%q", got[0].Signal, got[0].Severity)
	}
	if !strings.Contains(got[0].Diag, "ahead_of_real_time") {
		t.Fatalf("diag missing lead: %q", got[0].Diag)
	}
}
