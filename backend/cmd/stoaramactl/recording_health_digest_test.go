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

func TestHealthDigestBucket(t *testing.T) {
	got := healthDigestBucket(time.Date(2026, 8, 12, 15, 59, 0, 0, time.FixedZone("west", -4*60*60)))
	want := time.Date(2026, 8, 12, 16, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("bucket = %s, want %s", got, want)
	}
}

func TestClaimHealthDigestDeliveryLifecycle(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run DB-backed digest claim regression")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("health_digest_%d", time.Now().UnixNano())
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
	if _, err := pool.Exec(ctx, `CREATE TABLE recording_health_digest_deliveries(bucket_start_at TIMESTAMPTZ NOT NULL,recipient TEXT NOT NULL,attempted_at TIMESTAMPTZ,delivered_at TIMESTAMPTZ,PRIMARY KEY(bucket_start_at,recipient))`); err != nil {
		t.Fatal(err)
	}
	bucket := time.Date(2026, 8, 12, 16, 0, 0, 0, time.UTC)
	claimed, err := claimHealthDigestDelivery(ctx, pool, bucket, "ops@example.com")
	if err != nil || !claimed {
		t.Fatalf("first claim=%t err=%v", claimed, err)
	}
	claimed, err = claimHealthDigestDelivery(ctx, pool, bucket, "ops@example.com")
	if err != nil || claimed {
		t.Fatalf("immediate duplicate=%t err=%v", claimed, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_health_digest_deliveries SET attempted_at=now()-interval '11 minutes'`); err != nil {
		t.Fatal(err)
	}
	claimed, err = claimHealthDigestDelivery(ctx, pool, bucket, "ops@example.com")
	if err != nil || !claimed {
		t.Fatalf("expired attempt=%t err=%v", claimed, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_health_digest_deliveries SET delivered_at=now(),attempted_at=now()-interval '11 minutes'`); err != nil {
		t.Fatal(err)
	}
	claimed, err = claimHealthDigestDelivery(ctx, pool, bucket, "ops@example.com")
	if err != nil || claimed {
		t.Fatalf("delivered claim=%t err=%v", claimed, err)
	}
}

func TestComposeHealthDigestSeparatesCurrentAndHistorical(t *testing.T) {
	now := time.Date(2026, 8, 12, 16, 0, 0, 0, time.UTC)
	items := []digestRecording{
		{ID: 1, Name: "healthy now", Scheduled: true, Bucket: "stable", CurrentCause: "current capture progressing", HistoryBucket: "failing", HistoryNote: "latest completed 50%"},
		{ID: 2, Name: "off window", Scheduled: false, Bucket: "unknown", CurrentCause: "off-window / not currently assessed", HistoryBucket: "stable", HistoryNote: "latest completed 99.9%"},
		{ID: 3, Name: "live failure", Scheduled: true, Bucket: "failing", CurrentCause: "no fresh ingest", HistoryBucket: "degraded", HistoryNote: "latest completed 96%"},
	}
	body := composeHealthDigest("https://stoarama.com", now, items, digestNAS{})
	for _, want := range []string{
		"Active fleet: 3 total; 2 currently scheduled/live; 1 off-window/not assessed.",
		"Current operational health: 1 stable, 0 degraded, 1 failing, 0 unknown (of 2 live).",
		"Latest completed-window quality (historical, not current status): 1 stable, 1 degraded, 1 failing, 0 unknown.",
		"CURRENT FAILING (1)",
		"#3 live failure",
		"CURRENT STABLE (1)",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("body missing %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "#1 healthy now — latest completed 50%") {
		t.Fatalf("historical failure was rendered as a current failure:\n%s", body)
	}
}

func TestHealthDigestIdempotencyKey(t *testing.T) {
	bucket := time.Date(2026, 8, 12, 16, 0, 0, 0, time.UTC)
	a := healthDigestIdempotencyKey(bucket, " Deniz@Example.com ")
	b := healthDigestIdempotencyKey(bucket, "deniz@example.com")
	if a != b {
		t.Fatalf("normalized recipient keys differ: %q != %q", a, b)
	}
	if a == healthDigestIdempotencyKey(bucket.Add(8*time.Hour), "deniz@example.com") {
		t.Fatal("adjacent buckets reused idempotency key")
	}
	if strings.Contains(a, "deniz") || strings.Contains(a, "example") {
		t.Fatalf("idempotency key exposed recipient: %q", a)
	}
}

func TestDigestOperationalSignalsExcludeCompletedWindowOnlyAlerts(t *testing.T) {
	live := map[string]bool{}
	for _, signal := range liveRecordingHealthSignals() {
		live[signal] = true
	}
	for _, historical := range []string{signalContinuousCoverageLow, signalContinuousOverlap, signalContinuousLongGap, signalContinuousFragmented, signalContinuousLayoutChange} {
		if live[historical] {
			t.Fatalf("completed-window signal %q would contaminate current digest", historical)
		}
	}
	for _, current := range []string{signalContinuousSilentDeath, signalJobRetriesExhausted, signalStuckLease, signalClipTimestampDrift} {
		if !live[current] {
			t.Fatalf("current signal %q missing from digest registry", current)
		}
	}
}

func TestComposeHealthDigestNASCurrentAndRecoveredAreDistinct(t *testing.T) {
	now := time.Date(2026, 8, 12, 16, 0, 0, 0, time.UTC)
	reported := now.Add(-time.Minute)
	recovered := now.Add(-time.Hour)
	free, total := int64(2e12), int64(8e12)
	body := composeHealthDigest("", now, nil, digestNAS{
		Label: "NAS", Phase: "idle", Free: &free, Total: &total,
		ReportedAt: &reported, Blocked: true, CapacityTransitionState: "unknown", CapacityTransitionAt: &recovered,
	})
	if !strings.Contains(body, "Current: state=healthy") || !strings.Contains(body, "Latest historical storage-capacity transition: unknown") || !strings.Contains(body, "not the current derived state") {
		t.Fatalf("NAS current/recovered wording missing:\n%s", body)
	}
}
