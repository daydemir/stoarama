package main

import (
	"context"
	"testing"
	"time"
)

func dateUTC(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func TestPeriodAlreadyMetered(t *testing.T) {
	// Period end on the period-close boundary; the cursor stores the last metered
	// period end as a DATE, so comparison is calendar-date only (time of day in the
	// period end is irrelevant).
	periodEnd := time.Date(2026, 7, 24, 15, 30, 0, 0, time.UTC)

	// Never metered: must meter.
	if periodAlreadyMetered(periodEnd, nil) {
		t.Fatalf("nil cursor: want meter (false), got skip (true)")
	}

	cases := []struct {
		name string
		last time.Time
		skip bool
	}{
		{name: "cursor before period end -> meter", last: dateUTC(2026, 6, 24), skip: false},
		{name: "cursor equals period end date -> skip", last: dateUTC(2026, 7, 24), skip: true},
		{name: "cursor after period end -> skip", last: dateUTC(2026, 8, 24), skip: true},
		// Same calendar day, earlier wall-clock cursor: still the same date -> skip.
		{name: "cursor same day midnight -> skip", last: dateUTC(2026, 7, 24), skip: true},
	}
	for _, c := range cases {
		last := c.last
		if got := periodAlreadyMetered(periodEnd, &last); got != c.skip {
			t.Fatalf("%s: periodAlreadyMetered=%v, want %v", c.name, got, c.skip)
		}
	}
}

func TestMeterPeriodKey(t *testing.T) {
	// Year-month of the period end, used as the per-period meter-event identifier
	// component so re-sends within a period collapse to a single Stripe meter event.
	got := meterPeriodKey(time.Date(2026, 7, 24, 23, 59, 59, 0, time.UTC))
	if got != "2026-07" {
		t.Fatalf("meterPeriodKey = %q, want 2026-07", got)
	}
	// Non-UTC input is normalized to UTC before formatting (period end just after
	// UTC midnight, expressed in a -05:00 zone, still belongs to the UTC month).
	loc := time.FixedZone("UTC-5", -5*3600)
	gotTZ := meterPeriodKey(time.Date(2026, 7, 31, 20, 0, 0, 0, loc)) // == 2026-08-01T01:00Z
	if gotTZ != "2026-08" {
		t.Fatalf("meterPeriodKey(tz) = %q, want 2026-08", gotTZ)
	}
}

func TestShouldReportDays(t *testing.T) {
	for _, n := range []int{0, -1} {
		if shouldReportDays(n) {
			t.Fatalf("shouldReportDays(%d) = true, want false (empty period suppressed)", n)
		}
	}
	for _, n := range []int{1, 7, 31} {
		if !shouldReportDays(n) {
			t.Fatalf("shouldReportDays(%d) = false, want true", n)
		}
	}
}

// fakeMeteringStripe records ReportRecordingDays and ReportGBMonth calls so each
// report branch's arguments (customer, account, period key, value) can be asserted
// without Stripe.
type fakeMeteringStripe struct {
	periodStart time.Time
	periodEnd   time.Time
	reports     []reportCall
	gbReports   []gbReportCall
}

type reportCall struct {
	customerID string
	accountID  int64
	periodKey  string
	days       int
}

type gbReportCall struct {
	customerID string
	accountID  int64
	periodKey  string
	gbDecimal  string
}

func (f *fakeMeteringStripe) GetSubscriptionPeriod(_ context.Context, _ string) (time.Time, time.Time, error) {
	return f.periodStart, f.periodEnd, nil
}

func (f *fakeMeteringStripe) ReportRecordingDays(_ context.Context, customerID string, accountID int64, periodKey string, days int) error {
	f.reports = append(f.reports, reportCall{customerID, accountID, periodKey, days})
	return nil
}

func (f *fakeMeteringStripe) ReportGBMonth(_ context.Context, customerID string, accountID int64, periodKey, gbDecimal string) error {
	f.gbReports = append(f.gbReports, gbReportCall{customerID, accountID, periodKey, gbDecimal})
	return nil
}

// TestMeteringReportBranch exercises the report decision the same way meterAccount
// does (guard -> day-count gate -> ReportRecordingDays with the period key) so the
// idempotency-guard + day-count gating is covered end to end against a fake Stripe.
func TestMeteringReportBranch(t *testing.T) {
	ctx := context.Background()
	periodEnd := dateUTC(2026, 8, 1)
	report := func(a meterableAccount, days int) []reportCall {
		f := &fakeMeteringStripe{periodStart: dateUTC(2026, 7, 1), periodEnd: periodEnd}
		if periodAlreadyMetered(f.periodEnd, a.lastMeteredPeriodEnd) {
			return nil // guard skip: never reports
		}
		if shouldReportDays(days) {
			_ = f.ReportRecordingDays(ctx, a.customerID, a.accountID, meterPeriodKey(f.periodEnd), days)
		}
		return f.reports
	}

	// Fresh account, 7 billable days: one report with the period-end month key.
	got := report(meterableAccount{accountID: 42, customerID: "cus_x"}, 7)
	if len(got) != 1 || got[0] != (reportCall{"cus_x", 42, "2026-08", 7}) {
		t.Fatalf("fresh 7-day report = %+v, want one {cus_x,42,2026-08,7}", got)
	}

	// Zero billable days: no report (empty invoice suppressed).
	if got := report(meterableAccount{accountID: 42, customerID: "cus_x"}, 0); len(got) != 0 {
		t.Fatalf("zero-day report = %+v, want none", got)
	}

	// Already metered this period (cursor == period end): guard skips before report.
	last := periodEnd
	if got := report(meterableAccount{accountID: 42, customerID: "cus_x", lastMeteredPeriodEnd: &last}, 7); len(got) != 0 {
		t.Fatalf("already-metered report = %+v, want none (idempotent skip)", got)
	}
}

func TestGBMonthMeterValue(t *testing.T) {
	cases := []struct {
		name     string
		sumBytes int64
		snapDays int
		want     string
		report   bool
	}{
		// 31 daily snapshots summing to 76.6 GB-days => avg 2.471 GB, 3 decimals.
		{name: "averages to 3 decimals", sumBytes: 76_601_000_000, snapDays: 31, want: "2.471", report: true},
		// Decimals are NOT rounded to whole GB: 1.5 GB over a single day stays 1.500.
		{name: "no whole-GB rounding", sumBytes: 1_500_000_000, snapDays: 1, want: "1.500", report: true},
		// Mid-period opt-in: only the days the data existed count toward the average
		// (denominator is the snapshot-row count, not the period length).
		{name: "averages over snapshot days only", sumBytes: 6_000_000_000, snapDays: 3, want: "2.000", report: true},
		// No snapshots (BYO account): report nothing.
		{name: "zero snapshot days", sumBytes: 0, snapDays: 0, report: false},
		// Snapshots exist but every byte was purged: report nothing.
		{name: "zero bytes", sumBytes: 0, snapDays: 5, report: false},
	}
	for _, c := range cases {
		got, ok := gbMonthMeterValue(c.sumBytes, c.snapDays)
		if ok != c.report {
			t.Fatalf("%s: report=%v, want %v", c.name, ok, c.report)
		}
		if ok && got != c.want {
			t.Fatalf("%s: gbMonthMeterValue=%q, want %q", c.name, got, c.want)
		}
	}
}

// TestGBMonthReportBranch exercises the gb_month report decision the same way
// meterAccount does (guard -> snapshot-average gate -> ReportGBMonth with the
// 3-decimal string) so the idempotency-guard + averaged-decimal value is covered
// end to end against a fake Stripe. The (sumBytes, snapDays) pair stands in for the
// account's seeded account_storage_snapshots rows over the closing period.
func TestGBMonthReportBranch(t *testing.T) {
	ctx := context.Background()
	periodEnd := dateUTC(2026, 8, 1)
	report := func(a meterableAccount, sumBytes int64, snapDays int) []gbReportCall {
		f := &fakeMeteringStripe{periodStart: dateUTC(2026, 7, 1), periodEnd: periodEnd}
		if periodAlreadyMetered(f.periodEnd, a.lastMeteredPeriodEnd) {
			return nil // guard skip: never reports
		}
		if gbDecimal, ok := gbMonthMeterValue(sumBytes, snapDays); ok {
			_ = f.ReportGBMonth(ctx, a.customerID, a.accountID, meterPeriodKey(f.periodEnd), gbDecimal)
		}
		return f.gbReports
	}

	// Fresh account, 31 snapshots => one report with the averaged decimal string.
	got := report(meterableAccount{accountID: 42, customerID: "cus_x"}, 76_601_000_000, 31)
	if len(got) != 1 || got[0] != (gbReportCall{"cus_x", 42, "2026-08", "2.471"}) {
		t.Fatalf("fresh gb report = %+v, want one {cus_x,42,2026-08,2.471}", got)
	}

	// No managed bytes (BYO / fully purged): no report.
	if got := report(meterableAccount{accountID: 42, customerID: "cus_x"}, 0, 0); len(got) != 0 {
		t.Fatalf("zero-gb report = %+v, want none", got)
	}

	// Already metered this period (cursor == period end): guard skips before report,
	// so a re-run is a no-op for the gb_month meter too (idempotency).
	last := periodEnd
	if got := report(meterableAccount{accountID: 42, customerID: "cus_x", lastMeteredPeriodEnd: &last}, 76_601_000_000, 31); len(got) != 0 {
		t.Fatalf("already-metered gb report = %+v, want none (idempotent skip)", got)
	}
}
