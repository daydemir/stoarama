package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestCollatedHoldClipsAreNeverMeteredAsStorage pins the billing side of
// collated-only NAS delivery: a held clip stays unreleased in managed staging
// past the 24h nas_pull grace, yet neither the Stripe storage facts nor the
// display snapshot may count it. A raw nas_pull clip past the grace still counts.
func TestCollatedHoldClipsAreNeverMeteredAsStorage(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("collated_hold_meter_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE") }()
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
	if _, err := pool.Exec(ctx, `
		CREATE TABLE accounts(id BIGINT PRIMARY KEY);
		INSERT INTO accounts(id) VALUES(47);
		CREATE TABLE account_billing(account_id BIGINT PRIMARY KEY,stripe_customer_id TEXT,stripe_subscription_id TEXT,last_metered_period_end DATE,updated_at TIMESTAMPTZ DEFAULT now());
		INSERT INTO account_billing(account_id,stripe_customer_id,stripe_subscription_id) VALUES(47,'cus_47','sub_47');
		CREATE TABLE recordings(id BIGINT PRIMARY KEY,account_id BIGINT,storage_retention_tier TEXT,delivery TEXT);
		CREATE TABLE storage_destinations(id BIGINT PRIMARY KEY,managed BOOLEAN);
		CREATE TABLE recording_clips(id BIGINT PRIMARY KEY,recording_id BIGINT,storage_destination_id BIGINT,created_at TIMESTAMPTZ,clip_start_at TIMESTAMPTZ,clip_end_at TIMESTAMPTZ,released_at TIMESTAMPTZ,purged_at TIMESTAMPTZ,size_bytes BIGINT);
		CREATE TABLE account_storage_snapshots(account_id BIGINT,snapshot_date DATE,bytes_stored BIGINT,stream_hours_stored DOUBLE PRECISION,PRIMARY KEY(account_id,snapshot_date));
		CREATE TABLE recording_billing_hours(account_id BIGINT,rec_hour TIMESTAMPTZ);
	`); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"0103_billing_meter_report_ledger.sql", "0107_billing_period_ledger.sql", "0158_nas_collated_only_hold.sql"} {
		body, err := os.ReadFile("../../../infra/sql/migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(body)); err != nil {
			t.Fatalf("apply %s: %v", name, err)
		}
	}
	created := time.Now().UTC().Add(-72 * time.Hour).Truncate(time.Hour)
	if _, err := pool.Exec(ctx, `
		INSERT INTO storage_destinations VALUES(1,true);
		INSERT INTO recordings(id,account_id,storage_retention_tier,delivery) VALUES(1,47,'monthly','nas_pull'),(2,47,'monthly','nas_pull');
		UPDATE recordings SET nas_delivery_mode='collated_only' WHERE id=2;
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO recording_clips(id,recording_id,storage_destination_id,created_at,clip_start_at,clip_end_at,size_bytes)
		VALUES(1,1,1,$1,$1,$1::timestamptz+interval '1 hour',10),(2,2,1,$1,$1,$1::timestamptz+interval '2 hours',20)`, created); err != nil {
		t.Fatal(err)
	}
	var rawMode, heldMode string
	if err := pool.QueryRow(ctx, `SELECT max(mode) FILTER(WHERE clip_id=1),max(mode) FILTER(WHERE clip_id=2) FROM clip_storage_billing_contracts`).Scan(&rawMode, &heldMode); err != nil {
		t.Fatal(err)
	}
	if rawMode != "nas_pull_monthly" || heldMode != "nas_collated_hold" {
		t.Fatalf("frozen modes raw=%s held=%s", rawMode, heldMode)
	}
	// Switching back to raw does not rewrite the frozen contract of a held clip.
	if _, err := pool.Exec(ctx, `UPDATE recordings SET nas_delivery_mode='raw' WHERE id=2`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recordings SET nas_delivery_mode='bogus' WHERE id=2`); err == nil {
		t.Fatal("invalid nas_delivery_mode accepted")
	}
	if _, err := pool.Exec(ctx, `INSERT INTO clip_storage_billing_contracts(clip_id,mode,authoritative) VALUES(99,'bogus',true)`); err == nil {
		t.Fatal("invalid contract mode accepted")
	}
	day := created.Add(24 * time.Hour).Truncate(24 * time.Hour).Add(24 * time.Hour)
	if err := reconstructStorageDailyFacts(ctx, pool, 47, day, day.Add(24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	var factHours float64
	if err := pool.QueryRow(ctx, `SELECT stream_hours_stored FROM billing_storage_daily_facts WHERE account_id=47 AND usage_date=$1::date`, day).Scan(&factHours); err != nil {
		t.Fatal(err)
	}
	if math.Abs(factHours-1) > 1e-9 {
		t.Fatalf("billing facts counted %.3f stream-hours, want only the raw clip's 1", factHours)
	}
	if err := snapshotManagedStorage(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var snapshotHours float64
	var snapshotBytes int64
	if err := pool.QueryRow(ctx, `SELECT stream_hours_stored,bytes_stored FROM account_storage_snapshots WHERE account_id=47`).Scan(&snapshotHours, &snapshotBytes); err != nil {
		t.Fatal(err)
	}
	if math.Abs(snapshotHours-1) > 1e-9 || snapshotBytes != 10 {
		t.Fatalf("snapshot hours=%.3f bytes=%d, want only the raw clip", snapshotHours, snapshotBytes)
	}
}
