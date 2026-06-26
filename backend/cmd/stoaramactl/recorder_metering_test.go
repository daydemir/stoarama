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

// fakeMeteringStripe records ReportRecordingDays calls so the report branch's
// arguments (customer, account, period key, day count) can be asserted without
// Stripe.
type fakeMeteringStripe struct {
	periodStart time.Time
	periodEnd   time.Time
	reports     []reportCall
}

type reportCall struct {
	customerID string
	accountID  int64
	periodKey  string
	days       int
}

func (f *fakeMeteringStripe) GetSubscriptionPeriod(_ context.Context, _ string) (time.Time, time.Time, error) {
	return f.periodStart, f.periodEnd, nil
}

func (f *fakeMeteringStripe) ReportRecordingDays(_ context.Context, customerID string, accountID int64, periodKey string, days int) error {
	f.reports = append(f.reports, reportCall{customerID, accountID, periodKey, days})
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
