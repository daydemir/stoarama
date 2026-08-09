package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/email"
)

func TestNASStorageCapacityThresholdsAndFreshness(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	total := int64(10_000)
	fresh := now.Add(-time.Minute)
	stale := now.Add(-nasStorageTelemetryFreshness - time.Second)
	boundary := now.Add(-nasStorageTelemetryFreshness)
	value := func(v int64) *int64 { return &v }
	tests := []struct {
		name        string
		total, free *int64
		reported    *time.Time
		want        nasStorageCapacityState
	}{
		{name: "missing telemetry", want: nasStorageUnknown},
		{name: "stale telemetry", total: &total, free: value(8_000), reported: &stale, want: nasStorageUnknown},
		{name: "exact freshness boundary", total: &total, free: value(8_000), reported: &boundary, want: nasStorageUnknown},
		{name: "negative free", total: &total, free: value(-1), reported: &fresh, want: nasStorageUnknown},
		{name: "free exceeds total", total: &total, free: value(10_001), reported: &fresh, want: nasStorageUnknown},
		{name: "healthy above ten percent", total: &total, free: value(1_001), reported: &fresh, want: nasStorageHealthy},
		{name: "warning at ten percent", total: &total, free: value(1_000), reported: &fresh, want: nasStorageWarning},
		{name: "warning above five percent", total: &total, free: value(501), reported: &fresh, want: nasStorageWarning},
		{name: "critical at five percent", total: &total, free: value(500), reported: &fresh, want: nasStorageCritical},
		{name: "critical at zero", total: &total, free: value(0), reported: &fresh, want: nasStorageCritical},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := nasStorageStateAt(tc.total, tc.free, tc.reported, now); got != tc.want {
				t.Fatalf("state=%s want %s", got, tc.want)
			}
		})
	}
}

func TestNASStorageCapacityMessageForbidsDeletion(t *testing.T) {
	total, free := int64(1000), int64(40)
	reported := time.Date(2026, 8, 9, 11, 59, 0, 0, time.UTC)
	state := nasStorageCapacityTransition{
		ConnectionID: 13, ConnectionLabel: "NAS", OrgName: "MIT SCL", OrgEmail: "scl@example.edu",
		State: nasStorageCritical, ChangedAt: reported.Add(time.Minute), TotalBytes: &total, FreeBytes: &free, StorageReportedAt: &reported,
	}
	body := nasStorageCapacityBody("https://stoarama.com/", state)
	for _, want := range []string{"NAS storage is critical", "Free: 4.00%", "No clips were deleted", "Add capacity", "org-settings#connections"} {
		if !strings.Contains(body, want) {
			t.Errorf("body missing %q: %s", want, body)
		}
	}
}

func TestRecordNASStorageCapacityQueuesInitialRiskTransitionsAndRecovery(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run DB-backed NAS capacity regression")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("nas_capacity_%d", time.Now().UnixNano())
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
		CREATE TYPE nas_storage_capacity_state AS ENUM ('healthy','warning','critical','unknown');
		CREATE TABLE accounts (id BIGINT PRIMARY KEY,name TEXT NOT NULL,email TEXT NOT NULL);
		CREATE TABLE users (email TEXT PRIMARY KEY,is_operator BOOLEAN NOT NULL);
		CREATE TABLE connections (id BIGINT PRIMARY KEY,account_id BIGINT NOT NULL,kind TEXT NOT NULL,label TEXT NOT NULL,nas_storage_total_bytes BIGINT,nas_storage_free_bytes BIGINT,nas_storage_reported_at TIMESTAMPTZ);
		CREATE TABLE nas_storage_capacity_alert_states (connection_id BIGINT PRIMARY KEY,observed_state nas_storage_capacity_state NOT NULL,observed_at TIMESTAMPTZ NOT NULL);
		CREATE TABLE nas_storage_capacity_alert_events (id BIGSERIAL PRIMARY KEY,connection_id BIGINT NOT NULL,state nas_storage_capacity_state NOT NULL,observed_at TIMESTAMPTZ NOT NULL,total_bytes BIGINT,free_bytes BIGINT,storage_reported_at TIMESTAMPTZ,notified_at TIMESTAMPTZ);
		CREATE TABLE nas_storage_capacity_alert_deliveries (event_id BIGINT NOT NULL,recipient TEXT NOT NULL,delivered_at TIMESTAMPTZ,PRIMARY KEY(event_id,recipient));
		INSERT INTO accounts VALUES (47,'MIT SCL','scl@example.edu'),(48,'Other','other@example.edu');
		INSERT INTO users VALUES ('a@example.com',true),('b@example.com',true);
	`); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, `
		INSERT INTO connections VALUES
		  (13,47,'nas_pull','NAS',10000,2000,$1),
		  (14,48,'nas_pull','Other NAS',10000,100,$1)
	`, now); err != nil {
		t.Fatal(err)
	}
	states, err := currentNASStorageCapacity(ctx, pool, now)
	if err != nil || len(states) != 1 || states[0].ConnectionID != 13 || states[0].State != nasStorageHealthy {
		t.Fatalf("scoped states=%v err=%v", states, err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO connections VALUES (15,47,'nas_pull','Partial',10000,NULL,$1),(16,47,'nas_pull','Invalid',10000,10001,$1)`, now); err != nil {
		t.Fatal(err)
	}
	states, err = currentNASStorageCapacity(ctx, pool, now)
	if err != nil || len(states) != 3 {
		t.Fatalf("invalid telemetry states=%v err=%v", states, err)
	}
	for _, state := range states[1:] {
		if state.State != nasStorageUnknown || state.TotalBytes != nil || state.FreeBytes != nil {
			t.Fatalf("invalid telemetry not normalized: %+v", state)
		}
	}
	if _, err := pool.Exec(ctx, `DELETE FROM connections WHERE id IN (15,16)`); err != nil {
		t.Fatal(err)
	}
	if pending, err := recordNASStorageCapacity(ctx, pool, now); err != nil || len(pending) != 0 {
		t.Fatalf("healthy baseline pending=%v err=%v", pending, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE connections SET nas_storage_free_bytes=900,nas_storage_reported_at=$2 WHERE id=$1`, int64(13), now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	pending, err := recordNASStorageCapacity(ctx, pool, now.Add(time.Second))
	if err != nil || len(pending) != 1 || pending[0].State != nasStorageWarning || pending[0].FreeBytes == nil || *pending[0].FreeBytes != 900 {
		t.Fatalf("warning pending=%v err=%v", pending, err)
	}
	fake := &fakeNASStorageSender{failOnce: map[string]bool{"b@example.com": true}}
	if err := deliverNASStorageCapacityEmailWithSender(ctx, pool, config.Config{}, fake, pending); err == nil {
		t.Fatal("expected one recipient delivery failure")
	}
	if got := strings.Join(fake.sent, ","); got != "a@example.com,b@example.com" {
		t.Fatalf("first delivery attempts=%q", got)
	}
	if err := markNASStorageCapacityNotified(ctx, pool, pending); err == nil {
		t.Fatal("partially delivered event must not finalize")
	}
	fake.sent = nil
	if err := deliverNASStorageCapacityEmailWithSender(ctx, pool, config.Config{}, fake, pending); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(fake.sent, ","); got != "b@example.com" {
		t.Fatalf("retry attempts=%q want only remaining recipient", got)
	}
	if err := markNASStorageCapacityNotified(ctx, pool, pending); err != nil {
		t.Fatal(err)
	}
	if pending, err := recordNASStorageCapacity(ctx, pool, now.Add(2*time.Second)); err != nil || len(pending) != 0 {
		t.Fatalf("stable warning duplicated/lost pending=%v err=%v", pending, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE connections SET nas_storage_free_bytes=400,nas_storage_reported_at=$2 WHERE id=$1`, int64(13), now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	pending, err = recordNASStorageCapacity(ctx, pool, now.Add(3*time.Second))
	if err != nil || len(pending) != 1 || pending[0].State != nasStorageCritical {
		t.Fatalf("critical pending=%v err=%v", pending, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE connections SET nas_storage_free_bytes=2000,nas_storage_reported_at=$2 WHERE id=$1`, int64(13), now.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	pending, err = recordNASStorageCapacity(ctx, pool, now.Add(4*time.Second))
	if err != nil || len(pending) != 2 || pending[1].State != nasStorageHealthy {
		t.Fatalf("recovery pending=%v err=%v", pending, err)
	}
	var eventCount, deliveryCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM nas_storage_capacity_alert_events`).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM nas_storage_capacity_alert_deliveries`).Scan(&deliveryCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != 3 || deliveryCount != 6 {
		t.Fatalf("events=%d deliveries=%d want 3/6", eventCount, deliveryCount)
	}
}

type fakeNASStorageSender struct {
	failOnce map[string]bool
	sent     []string
}

func (f *fakeNASStorageSender) Send(_ context.Context, msg email.Message) (email.DeliveryReceipt, error) {
	f.sent = append(f.sent, msg.To)
	if f.failOnce[msg.To] {
		f.failOnce[msg.To] = false
		return email.DeliveryReceipt{}, fmt.Errorf("forced failure")
	}
	return email.DeliveryReceipt{Provider: "fake", Status: "accepted"}, nil
}
