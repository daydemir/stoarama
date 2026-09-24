package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/qualitygrade"
)

func TestResolveChurnRate(t *testing.T) {
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	started := now.Add(-30 * time.Minute).Format(time.RFC3339Nano)
	if rate, breach := resolveChurnRate(40, started, now); !breach || rate != 80 {
		t.Fatalf("40 resolves/30m rate=%v breach=%t want 80/h breach", rate, breach)
	}
	if _, breach := resolveChurnRate(8, now.Add(-10*time.Minute).Format(time.RFC3339Nano), now); breach {
		t.Fatal("young lease must not breach")
	}
	if _, breach := resolveChurnRate(9, started, now); breach {
		t.Fatal("below the absolute minimum must not breach")
	}
	if _, breach := resolveChurnRate(12, now.Add(-2*time.Hour).Format(time.RFC3339Nano), now); breach {
		t.Fatal("6/h is healthy")
	}
	if _, breach := resolveChurnRate(1000, "not-a-time", now); breach {
		t.Fatal("malformed started_at must be tolerated, not alerted")
	}
}

func gradedRecording(id int64, end time.Time, grades ...qualitygrade.Grade) qualitygrade.Recording {
	r := qualitygrade.Recording{RecordingID: id, Name: fmt.Sprintf("rec-%d", id)}
	days := []qualitygrade.Day{}
	first := end.Truncate(24*time.Hour).AddDate(0, 0, -(len(grades) - 1))
	for k, g := range grades {
		date := first.AddDate(0, 0, k)
		cov := 99.0
		r.Windows = append(r.Windows, qualitygrade.Window{JobID: int64(k + 1), LocalDate: date, WindowEndAt: date.Add(end.Sub(end.Truncate(24 * time.Hour))), Grade: g, CoveragePct: &cov})
		days = append(days, qualitygrade.Day{Date: date, Grade: g})
	}
	r.Tiers = qualitygrade.Evaluate(qualitygrade.FillMissing(days))
	return r
}

func TestPoorWindowGradeIncidentCarriesTierImpact(t *testing.T) {
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	closed := time.Date(2026, 9, 22, 11, 0, 0, 0, time.UTC)
	recs := []qualitygrade.Recording{
		gradedRecording(402, closed, qualitygrade.GradeA, qualitygrade.GradeB, qualitygrade.GradeE),
		gradedRecording(403, closed, qualitygrade.GradeA, qualitygrade.GradeA),
		gradedRecording(404, closed.Add(-72*time.Hour), qualitygrade.GradeF),
	}
	poor := poorWindowGradeRecordings(recs, now)
	if len(poor) != 1 || poor[0].RecordingID != 402 {
		t.Fatalf("poor=%+v want only fresh E recording 402", poor)
	}
	inc := windowGradeIncident(healthIncident{RecordingID: 402}, poor[0])
	if inc.Signal != signalWindowGradePoor || inc.Severity != "HIGH" {
		t.Fatalf("incident=%+v", inc)
	}
	for _, want := range []string{"grade=E", "last14=ABE", "good+=run=3/14 E=1 F=0", "great+=run=0/14"} {
		if !strings.Contains(inc.Diag, want) {
			t.Fatalf("diag %q missing %q", inc.Diag, want)
		}
	}
	if healthSignalLabels[signalWindowGradePoor] == "" || healthSignalLabels[signalRelayResolveRate] == "" {
		t.Fatal("new signals must be labeled for the operator email")
	}
}

func TestComposeStaleRecorderDropletEmailAndDigest(t *testing.T) {
	now := time.Date(2026, 9, 24, 7, 0, 0, 0, time.UTC)
	stale := []staleRecorderDroplet{{ID: 1132, Name: "stoarama-rec-1789132863170762155-0", State: "active", PoolRole: "shared", LastLiveAt: now.Add(-12 * 24 * time.Hour)}}
	subject, body := composeStaleRecorderDropletEmail(stale, now)
	if !strings.Contains(subject, "1 recorder droplet") || !strings.Contains(body, "droplet #1132") || !strings.Contains(body, "silent=288h0m0s") {
		t.Fatalf("subject=%q body=%q", subject, body)
	}
	if stale[0].alertKey() != "recorder_droplet_stale:1132" {
		t.Fatalf("alert key=%q", stale[0].alertKey())
	}
	closed := now.Add(-20 * time.Hour)
	var b strings.Builder
	composeDigestStability(&b, digestStability{
		Recordings: []qualitygrade.Recording{
			gradedRecording(402, closed, qualitygrade.GradeA, qualitygrade.GradeE),
			gradedRecording(403, closed, qualitygrade.GradeA),
		},
		Stale: stale,
	}, now)
	digest := b.String()
	for _, want := range []string{"A=1 B=0 C=0 D=0 E=1 F=0 unknown=0", "#402 rec-402", "last14 AE", "UNRESPONSIVE droplet #1132"} {
		if !strings.Contains(digest, want) {
			t.Fatalf("digest missing %q:\n%s", want, digest)
		}
	}
}

func testStabilityMonitorPool(t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run DB-backed stability monitor tests")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("stability_monitor_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
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
	return pool, func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
	}
}

func TestOpsAlertEpisodeLifecycle(t *testing.T) {
	pool, cleanup := testStabilityMonitorPool(t)
	defer cleanup()
	ctx := context.Background()
	migration, err := os.ReadFile("../../../infra/sql/migrations/0153_recording_stability_guards.sql")
	if err != nil {
		t.Fatal(err)
	}
	ddl := string(migration)
	// Only the ops table is needed here; the failures table references tables
	// this narrow schema does not create.
	ddl = ddl[strings.Index(ddl, "CREATE TABLE ops_alert_episodes"):]
	if _, err := pool.Exec(ctx, ddl); err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 9, 24, 7, 0, 0, 0, time.UTC)
	key := "recorder_droplet_stale:1132"
	due, err := recordOpsAlertEpisodes(ctx, pool, signalDropletStale, []string{key}, t0)
	if err != nil || len(due) != 1 {
		t.Fatalf("new episode due=%v err=%v", due, err)
	}
	if err := markOpsAlertsDelivered(ctx, pool, due, t0); err != nil {
		t.Fatal(err)
	}
	if due, err = recordOpsAlertEpisodes(ctx, pool, signalDropletStale, []string{key}, t0.Add(5*time.Minute)); err != nil || len(due) != 0 {
		t.Fatalf("open delivered episode must be deduped: due=%v err=%v", due, err)
	}
	if due, err = recordOpsAlertEpisodes(ctx, pool, signalDropletStale, []string{key}, t0.Add(25*time.Hour)); err != nil || len(due) != 1 {
		t.Fatalf("daily reminder due=%v err=%v", due, err)
	}
	if _, err = recordOpsAlertEpisodes(ctx, pool, signalDropletStale, []string{}, t0.Add(26*time.Hour)); err != nil {
		t.Fatal(err)
	}
	var resolved bool
	if err := pool.QueryRow(ctx, `SELECT resolved_at IS NOT NULL FROM ops_alert_episodes WHERE alert_key=$1`, key).Scan(&resolved); err != nil || !resolved {
		t.Fatalf("absent subject must resolve: resolved=%t err=%v", resolved, err)
	}
	if due, err = recordOpsAlertEpisodes(ctx, pool, signalDropletStale, []string{key}, t0.Add(27*time.Hour)); err != nil || len(due) != 1 {
		t.Fatalf("reopened episode due=%v err=%v", due, err)
	}
}

func TestLoadStaleRecorderDroplets(t *testing.T) {
	pool, cleanup := testStabilityMonitorPool(t)
	defer cleanup()
	ctx := context.Background()
	now := time.Now().UTC()
	if _, err := pool.Exec(ctx, `
		CREATE TABLE nodes (id BIGINT PRIMARY KEY, last_heartbeat_at TIMESTAMPTZ);
		CREATE TABLE recorder_droplets (id BIGINT PRIMARY KEY, name TEXT NOT NULL, node_id BIGINT, state TEXT NOT NULL, pool_role TEXT NOT NULL,
		  last_seen_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL);
		CREATE TABLE recording_jobs (id BIGINT PRIMARY KEY, lease_owner TEXT, status TEXT NOT NULL, lease_expires_at TIMESTAMPTZ)`); err != nil {
		t.Fatal(err)
	}
	old, fresh := now.Add(-12*24*time.Hour), now.Add(-time.Minute)
	for _, stmt := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO nodes VALUES (1, $1), (2, $2), (3, NULL), (4, NULL)`, []any{old, fresh}},
		{`INSERT INTO recorder_droplets VALUES
		  (1132, 'dead', 1, 'active', 'shared', $1, $1),
		  (366, 'alive-by-node', 2, 'active', 'shared', $1, $1),
		  (2379, 'new', 3, 'provisioning', 'shared', NULL, $2),
		  (9, 'gone', 4, 'destroyed', 'shared', NULL, $1)`, []any{old, fresh}},
		{`INSERT INTO recording_jobs VALUES (1, 'dead', 'leased', $1::timestamptz + interval '1 hour')`, []any{fresh}},
	} {
		if _, err := pool.Exec(ctx, stmt.sql, stmt.args...); err != nil {
			t.Fatal(err)
		}
	}
	stale, err := loadStaleRecorderDroplets(ctx, pool, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(stale) != 1 || stale[0].ID != 1132 || stale[0].LiveLeases != 1 {
		t.Fatalf("stale=%+v want only droplet 1132 with its live lease", stale)
	}
}
