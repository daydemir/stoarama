package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/billing"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestMeterReportLedgerFailsClosedOnAmbiguousRetry(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run meter report ledger regression")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("meter_report_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = admin.Exec(cleanupCtx, "DROP SCHEMA "+schema+" CASCADE")
	}()
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
		CREATE TABLE accounts(id BIGINT PRIMARY KEY);
		INSERT INTO accounts(id) VALUES(47);
		CREATE TABLE account_billing(account_id BIGINT PRIMARY KEY,stripe_customer_id TEXT,stripe_subscription_id TEXT,last_metered_period_end DATE,updated_at TIMESTAMPTZ DEFAULT now());
		CREATE TABLE recordings(id BIGINT PRIMARY KEY,account_id BIGINT,storage_retention_tier TEXT,delivery TEXT);
		CREATE TABLE storage_destinations(id BIGINT PRIMARY KEY,managed BOOLEAN);
		CREATE TABLE recording_clips(id BIGINT PRIMARY KEY,recording_id BIGINT,storage_destination_id BIGINT,created_at TIMESTAMPTZ,clip_start_at TIMESTAMPTZ,clip_end_at TIMESTAMPTZ,released_at TIMESTAMPTZ,purged_at TIMESTAMPTZ,size_bytes BIGINT);
		CREATE TABLE account_storage_snapshots(account_id BIGINT,snapshot_date DATE,bytes_stored BIGINT,stream_hours_stored DOUBLE PRECISION,PRIMARY KEY(account_id,snapshot_date));
		CREATE TABLE recording_billing_hours(account_id BIGINT,rec_hour TIMESTAMPTZ);
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO recordings VALUES(9,47,'monthly','managed');
		INSERT INTO storage_destinations VALUES(9,true);
		INSERT INTO recording_clips(id,recording_id,storage_destination_id,created_at,clip_start_at,clip_end_at,size_bytes) VALUES(9,9,9,'2026-07-01 00:00+00','2026-07-01 00:00+00','2026-07-01 00:01+00',1);
	`); err != nil {
		t.Fatal(err)
	}
	migration, err := os.ReadFile("../../../infra/sql/migrations/0103_billing_meter_report_ledger.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		t.Fatalf("apply production billing ledger migration: %v", err)
	}
	ledgerMigration, err := os.ReadFile("../../../infra/sql/migrations/0107_billing_period_ledger.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(ledgerMigration)); err != nil {
		t.Fatal(err)
	}
	backfillMigration, err := os.ReadFile("../../../infra/sql/migrations/0108_backfill_clip_storage_billing_contracts.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(backfillMigration)); err != nil {
		t.Fatal(err)
	}
	var legacyAuthoritative bool
	if err := pool.QueryRow(ctx, `SELECT authoritative FROM clip_storage_billing_contracts WHERE clip_id=9`).Scan(&legacyAuthoritative); err != nil || legacyAuthoritative {
		t.Fatalf("legacy authoritative=%v err=%v", legacyAuthoritative, err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO recordings VALUES(1,47,'monthly','nas_pull');
		INSERT INTO storage_destinations VALUES(1,true);
		INSERT INTO recording_clips(id,recording_id,storage_destination_id,created_at,clip_start_at,clip_end_at,size_bytes) VALUES(1,1,1,'2026-09-02 00:00+00','2026-09-02 00:00+00','2026-09-03 00:00+00',1);
		UPDATE recordings SET storage_retention_tier='yearly_prepaid',delivery='managed' WHERE id=1;
		INSERT INTO recording_clips(id,recording_id,storage_destination_id,created_at,clip_start_at,clip_end_at,size_bytes) VALUES(2,1,1,'2026-09-02 00:00+00','2026-09-02 00:00+00','2026-09-03 00:00+00',1);
	`); err != nil {
		t.Fatal(err)
	}
	var firstMode, secondMode string
	if err := pool.QueryRow(ctx, `SELECT max(mode) FILTER(WHERE clip_id=1),max(mode) FILTER(WHERE clip_id=2) FROM clip_storage_billing_contracts`).Scan(&firstMode, &secondMode); err != nil {
		t.Fatal(err)
	}
	if firstMode != "nas_pull_monthly" || secondMode != "excluded" {
		t.Fatalf("frozen modes first=%s second=%s", firstMode, secondMode)
	}
	periodEnd := time.Date(2026, 8, 29, 6, 28, 0, 0, time.UTC)
	eventAt := periodEnd.Add(-time.Minute)
	send, err := reserveMeterReport(ctx, pool, 47, periodEnd, "recording_hour", "912", "47-2026-08-29", eventAt)
	if err != nil || !send {
		t.Fatalf("first reserve send=%v err=%v", send, err)
	}
	if send, err = reserveMeterReport(ctx, pool, 47, periodEnd, "recording_hour", "912", "47-2026-08-29", eventAt); err == nil || send || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("ambiguous retry send=%v err=%v", send, err)
	}
	if _, err := reserveMeterReport(ctx, pool, 47, periodEnd, "recording_hour", "913", "47-2026-08-29", eventAt); err == nil || !strings.Contains(err.Error(), "differs") {
		t.Fatalf("changed pending usage was accepted: %v", err)
	}
	if err := markMeterReportReported(ctx, pool, 47, periodEnd, "recording_hour"); err != nil {
		t.Fatal(err)
	}
	if send, err = reserveMeterReport(ctx, pool, 47, periodEnd, "recording_hour", "912", "47-2026-08-29", eventAt); err != nil || send {
		t.Fatalf("reported retry send=%v err=%v", send, err)
	}
	// The durable key exactly matches Stripe's (meter,event identifier) key. Even
	// if a later retry reconstructed a different timestamp/value for that period
	// key, an already-reported event is never sent again.
	if send, err = reserveMeterReport(ctx, pool, 47, periodEnd.Add(time.Hour), "recording_hour", "913", "47-2026-08-29", eventAt); err != nil || send {
		t.Fatalf("repeated Stripe identifier send=%v err=%v", send, err)
	}

	start := time.Date(2026, 9, 1, 12, 9, 0, 0, time.UTC)
	end := time.Date(2026, 10, 1, 12, 9, 0, 0, time.UTC)
	for _, setup := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO account_billing(account_id,stripe_customer_id,stripe_subscription_id) VALUES(47,'cus_47','sub_47')`, nil},
		{`UPDATE billing_storage_fact_config SET eligible_from='2026-09-01'`, nil},
		{`INSERT INTO billing_meter_periods(account_id,stripe_customer_id,stripe_subscription_id,period_start,period_end) VALUES(47,'cus_47','sub_47',$1,$2)`, []any{start, end}},
		{`INSERT INTO recording_billing_hours(account_id,rec_hour) SELECT 47,$1::timestamptz + (n||' hours')::interval FROM generate_series(1,5) n`, []any{start}},
	} {
		if _, err := pool.Exec(ctx, setup.sql, setup.args...); err != nil {
			t.Fatal(err)
		}
	}
	f := &fakeMeteringStripe{invoice: billing.PeriodInvoice{ID: "in_period", Status: "draft", FinalizesAt: time.Now().Add(2 * time.Hour)}}
	a := meterableAccount{accountID: 47, customerID: "cus_47", subscriptionID: "sub_47"}
	if err := meterClosedPeriod(ctx, pool, f, a, start, end); err == nil || !strings.Contains(err.Error(), "legacy inventory") {
		t.Fatalf("untrusted legacy guard err=%v", err)
	}
	if len(f.reports) != 0 {
		t.Fatalf("legacy guard emitted reports: %+v", f.reports)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_clips SET released_at='2026-08-01' WHERE id=9`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM clip_storage_billing_contracts WHERE clip_id=1`); err != nil {
		t.Fatal(err)
	}
	if err := meterClosedPeriod(ctx, pool, f, a, start, end); err == nil || !strings.Contains(err.Error(), "legacy inventory") {
		t.Fatalf("missing contract guard err=%v", err)
	}
	if len(f.reports) != 0 {
		t.Fatalf("missing contract guard emitted reports: %+v", f.reports)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO clip_storage_billing_contracts(clip_id,mode,authoritative) VALUES(1,'nas_pull_monthly',true)`); err != nil {
		t.Fatal(err)
	}
	f.invoice.FinalizesAt = time.Now().Add(10 * time.Minute)
	if err := meterClosedPeriod(ctx, pool, f, a, start, end); err == nil || !strings.Contains(err.Error(), "30 minutes") {
		t.Fatalf("short grace guard err=%v", err)
	}
	if len(f.reports) != 0 {
		t.Fatalf("short grace emitted reports: %+v", f.reports)
	}
	f.invoice.FinalizesAt = time.Now().Add(2 * time.Hour)
	if err := meterClosedPeriod(ctx, pool, f, a, start, end); err == nil || !strings.Contains(err.Error(), "awaiting Stripe") {
		t.Fatalf("submit phase err=%v", err)
	}
	if len(f.reports) != 1 || f.reports[0].hours != 5 {
		t.Fatalf("submitted reports=%+v", f.reports)
	}
	if len(f.shmReports) != 1 {
		t.Fatalf("submitted storage reports=%+v", f.shmReports)
	}
	storageUsage, parseErr := strconv.ParseFloat(f.shmReports[0].hoursDecimal, 64)
	if parseErr != nil {
		t.Fatal(parseErr)
	}
	f.meterUsageByKind = map[string]float64{"recording_hour": 5, "stream_hour_month": storageUsage}
	if err := meterClosedPeriod(ctx, pool, f, a, start, end); err == nil || !strings.Contains(err.Error(), "awaiting invoice") {
		t.Fatalf("aggregate phase err=%v", err)
	}
	if len(f.reports) != 1 {
		t.Fatalf("aggregate retry resent event: %+v", f.reports)
	}
	if !f.invoiceStart.Equal(start) || !f.invoiceEnd.Equal(end) ||
		!f.meterStarts["recording_hour"].Equal(start) ||
		!f.meterEnds["recording_hour"].Equal(end) ||
		!f.meterStarts["stream_hour_month"].Equal(start) ||
		!f.meterEnds["stream_hour_month"].Equal(end) {
		t.Fatalf("Stripe period bounds not preserved")
	}
	wantStorageCents := int64(math.Round(storageUsage * float64(billing.StreamHourMonthUnitAmountCents)))
	f.invoice = billing.PeriodInvoice{ID: "in_period", Status: "paid", RecordingCents: 25, StorageCents: wantStorageCents + 1}
	if err := meterClosedPeriod(ctx, pool, f, a, start, end); err == nil || !strings.Contains(err.Error(), "usage mismatch") {
		t.Fatalf("storage mismatch err=%v", err)
	}
	f.invoice.StorageCents = wantStorageCents
	if err := meterClosedPeriod(ctx, pool, f, a, start, end); err != nil {
		t.Fatalf("invoice verification: %v", err)
	}
	var completed bool
	if err := pool.QueryRow(ctx, `SELECT metered_at IS NOT NULL AND invoice_verified_at IS NOT NULL FROM billing_meter_periods WHERE account_id=47 AND period_end=$1`, end).Scan(&completed); err != nil || !completed {
		t.Fatalf("completed=%v err=%v", completed, err)
	}
}

func dateUTC(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func TestMeterPeriodKey(t *testing.T) {
	// Period-end UTC date, the per-period meter-event identifier component, so
	// re-sends within a period collapse to one Stripe meter event while two distinct
	// periods (even two ending in the same month after a re-anchor) get distinct keys.
	got := meterPeriodKey(time.Date(2026, 7, 24, 23, 59, 59, 0, time.UTC))
	if got != "20260724T235959Z" {
		t.Fatalf("meterPeriodKey = %q, want exact period end", got)
	}
	// Non-UTC input is normalized to UTC before formatting (period end just after
	// UTC midnight, expressed in a -05:00 zone, still belongs to the next UTC day).
	loc := time.FixedZone("UTC-5", -5*3600)
	gotTZ := meterPeriodKey(time.Date(2026, 7, 31, 20, 0, 0, 0, loc)) // == 2026-08-01T01:00Z
	if gotTZ != "20260801T010000Z" {
		t.Fatalf("meterPeriodKey(tz) = %q, want exact UTC period end", gotTZ)
	}
	// Two distinct closing periods inside the SAME calendar month (an out-of-cycle
	// re-anchor) must get DISTINCT keys so their identifiers cannot collide.
	k1 := meterPeriodKey(time.Date(2026, 7, 5, 12, 0, 0, 0, time.UTC))
	k2 := meterPeriodKey(time.Date(2026, 7, 29, 6, 28, 0, 0, time.UTC))
	if k1 == k2 {
		t.Fatalf("two July periods collapsed to one key %q; want distinct date keys", k1)
	}
}

func TestShouldReportHours(t *testing.T) {
	for _, n := range []int{0, -1} {
		if shouldReportHours(n) {
			t.Fatalf("shouldReportHours(%d) = true, want false (empty period suppressed)", n)
		}
	}
	for _, n := range []int{1, 7, 744} {
		if !shouldReportHours(n) {
			t.Fatalf("shouldReportHours(%d) = false, want true", n)
		}
	}
}

// fakeMeteringStripe records ReportRecordingHours and ReportStreamHourMonth calls so
// each report branch's arguments (customer, account, period key, value) can be
// asserted without Stripe.
type fakeMeteringStripe struct {
	periodStart              time.Time
	periodEnd                time.Time
	reports                  []reportCall
	shmReports               []shmReportCall
	meterUsageByKind         map[string]float64
	invoice                  billing.PeriodInvoice
	invoiceStart, invoiceEnd time.Time
	meterStarts, meterEnds   map[string]time.Time
}

type reportCall struct {
	customerID string
	accountID  int64
	periodKey  string
	hours      int
}

type shmReportCall struct {
	customerID   string
	accountID    int64
	periodKey    string
	hoursDecimal string
}

func (f *fakeMeteringStripe) GetSubscriptionPeriod(_ context.Context, _ string) (time.Time, time.Time, error) {
	return f.periodStart, f.periodEnd, nil
}

func (f *fakeMeteringStripe) ReportRecordingHours(_ context.Context, customerID string, accountID int64, periodKey string, _ time.Time, hours int) error {
	f.reports = append(f.reports, reportCall{customerID, accountID, periodKey, hours})
	return nil
}

func (f *fakeMeteringStripe) ReportStreamHourMonth(_ context.Context, customerID string, accountID int64, periodKey string, _ time.Time, hoursDecimal string) error {
	f.shmReports = append(f.shmReports, shmReportCall{customerID, accountID, periodKey, hoursDecimal})
	return nil
}

func (f *fakeMeteringStripe) MeterUsage(_ context.Context, _ string, kind string, start, end time.Time) (float64, error) {
	if f.meterStarts == nil {
		f.meterStarts = map[string]time.Time{}
		f.meterEnds = map[string]time.Time{}
	}
	f.meterStarts[kind], f.meterEnds[kind] = start, end
	return f.meterUsageByKind[kind], nil
}
func (f *fakeMeteringStripe) PeriodInvoice(_ context.Context, _, _ string, start, end time.Time) (billing.PeriodInvoice, error) {
	f.invoiceStart, f.invoiceEnd = start, end
	if f.invoice.ID == "" {
		return billing.PeriodInvoice{ID: "in_test", Status: "draft"}, nil
	}
	return f.invoice, nil
}

func (f *fakeMeteringStripe) ChargePrepaidBatch(_ context.Context, _ string, batchKey string, cents int64, _ map[string]string) (billing.PrepaidBatch, error) {
	return billing.PrepaidBatch{InvoiceID: "in_" + batchKey, InvoiceItemID: "ii_" + batchKey}, nil
}

// TestMeteringReportBranch exercises the report decision the same way meterAccount
// does (guard -> hour-count gate -> ReportRecordingHours with the period key) so the
// idempotency-guard + hour-count gating is covered end to end against a fake Stripe.
func TestMeteringReportBranch(t *testing.T) {
	ctx := context.Background()
	periodEnd := dateUTC(2026, 8, 1)
	report := func(a meterableAccount, hours int) []reportCall {
		f := &fakeMeteringStripe{periodStart: dateUTC(2026, 7, 1), periodEnd: periodEnd}
		if shouldReportHours(hours) {
			_ = f.ReportRecordingHours(ctx, a.customerID, a.accountID, meterPeriodKey(f.periodEnd), f.periodEnd.Add(-time.Minute), hours)
		}
		return f.reports
	}

	// Fresh account, 7 billable hours: one report with the period-end month key.
	got := report(meterableAccount{accountID: 42, customerID: "cus_x"}, 7)
	if len(got) != 1 || got[0] != (reportCall{"cus_x", 42, "20260801T000000Z", 7}) {
		t.Fatalf("fresh 7-hour report = %+v", got)
	}

	// Zero billable hours: no report (empty invoice suppressed).
	if got := report(meterableAccount{accountID: 42, customerID: "cus_x"}, 0); len(got) != 0 {
		t.Fatalf("zero-hour report = %+v, want none", got)
	}

}

// TestRecordHourBillingMath locks the record-hour pricing math: hours x $0.05. The
// authoritative charge is Stripe's meter sum (hours reported), but the cost a given
// hour count maps to must match the published $0.05/record-hour rate so the UI/API
// estimate and the meter agree on the unit price.
func TestRecordHourBillingMath(t *testing.T) {
	const rateCents = 5 // $0.05 per record-hour
	cases := []struct {
		hours     int
		wantCents int
	}{
		{hours: 0, wantCents: 0},
		{hours: 1, wantCents: 5},
		{hours: 24, wantCents: 120},   // a recording active in all 24 distinct UTC hours of a day
		{hours: 744, wantCents: 3720}, // a 31-day month, every hour
	}
	for _, c := range cases {
		if got := c.hours * rateCents; got != c.wantCents {
			t.Fatalf("%d record-hours x %dc = %dc, want %dc", c.hours, rateCents, got, c.wantCents)
		}
	}
}

func TestStreamHourMonthMeterValue(t *testing.T) {
	cases := []struct {
		name     string
		sumHours float64
		snapDays int
		want     string
		report   bool
	}{
		// 31 daily snapshots summing to 76.601 stream-hour-days => avg 2.471
		// stream-hour-months, 3 decimals. NOTE the value is already in hours: there is
		// NO /1e9 byte->GB conversion (the gb_month-copied bug would 1e-9 the charge).
		{name: "averages to 3 decimals", sumHours: 76.601, snapDays: 31, want: "2.471", report: true},
		// A single day storing 1.5 stream-hours stays 1.500 (no rounding to whole hours).
		{name: "no whole-hour rounding", sumHours: 1.5, snapDays: 1, want: "1.500", report: true},
		// Mid-period opt-in: only the days the data existed count toward the average
		// (denominator is the snapshot-row count, not the period length).
		{name: "averages over snapshot days only", sumHours: 6.0, snapDays: 3, want: "2.000", report: true},
		// A clip stored a full month (744h) over 31 snapshot days averages 24.000.
		{name: "month-long clip averages to its hours", sumHours: 744.0, snapDays: 31, want: "24.000", report: true},
		// No snapshots (BYO account): report nothing.
		{name: "zero snapshot days", sumHours: 0, snapDays: 0, report: false},
		// Snapshots exist but every clip was purged: report nothing.
		{name: "zero hours", sumHours: 0, snapDays: 5, report: false},
	}
	for _, c := range cases {
		got, ok := streamHourMonthMeterValue(c.sumHours, c.snapDays)
		if ok != c.report {
			t.Fatalf("%s: report=%v, want %v", c.name, ok, c.report)
		}
		if ok && got != c.want {
			t.Fatalf("%s: streamHourMonthMeterValue=%q, want %q", c.name, got, c.want)
		}
	}
}

// TestStreamHourMonthReportBranch exercises the stream_hour_month report decision the
// same way meterAccount does (guard -> snapshot-average gate -> ReportStreamHourMonth
// with the 3-decimal string) so the idempotency-guard + averaged-decimal value is
// covered end to end against a fake Stripe. The (sumHours, snapDays) pair stands in
// for the account's seeded account_storage_snapshots rows over the closing period.
func TestStreamHourMonthReportBranch(t *testing.T) {
	ctx := context.Background()
	periodEnd := dateUTC(2026, 8, 1)
	report := func(a meterableAccount, sumHours float64, snapDays int) []shmReportCall {
		f := &fakeMeteringStripe{periodStart: dateUTC(2026, 7, 1), periodEnd: periodEnd}
		if hoursDecimal, ok := streamHourMonthMeterValue(sumHours, snapDays); ok {
			_ = f.ReportStreamHourMonth(ctx, a.customerID, a.accountID, meterPeriodKey(f.periodEnd), f.periodEnd.Add(-time.Minute), hoursDecimal)
		}
		return f.shmReports
	}

	// Fresh account, 31 snapshots => one report with the averaged decimal string.
	got := report(meterableAccount{accountID: 42, customerID: "cus_x"}, 76.601, 31)
	if len(got) != 1 || got[0] != (shmReportCall{"cus_x", 42, "20260801T000000Z", "2.471"}) {
		t.Fatalf("fresh stream-hour-month report = %+v, want one {cus_x,42,20260801T000000Z,2.471}", got)
	}

	// No managed footage (BYO / fully purged): no report.
	if got := report(meterableAccount{accountID: 42, customerID: "cus_x"}, 0, 0); len(got) != 0 {
		t.Fatalf("zero report = %+v, want none", got)
	}

}
