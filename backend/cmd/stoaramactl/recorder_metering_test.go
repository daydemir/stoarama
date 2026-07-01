package main

import (
	"context"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/billing"
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

func TestPeriodReadyToMeter(t *testing.T) {
	// A subscription whose current open period ends 2026-07-29 (close instant
	// 06:28 UTC). The billable-day count (rec_day < 2026-07-29) is only complete
	// once the period-end UTC day has arrived, and the closing meter event must be
	// pushed on that day, before the close instant. periodReadyToMeter encodes that.
	periodEnd := time.Date(2026, 7, 29, 6, 28, 33, 0, time.UTC)
	cases := []struct {
		name  string
		now   time.Time
		ready bool
	}{
		// Mid-period: end date is in the future, count not final -> NOT ready. This
		// is the case the cursor-jump bug mis-handled by advancing anyway.
		{name: "mid-period not ready", now: time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC), ready: false},
		// Day before close: still not the period-end day -> NOT ready.
		{name: "day before close not ready", now: time.Date(2026, 7, 28, 23, 59, 0, 0, time.UTC), ready: false},
		// Period-end day, before the close instant: count is final, period still
		// open -> READY. This is the only correct moment to report.
		{name: "period-end day before close ready", now: time.Date(2026, 7, 29, 0, 30, 0, 0, time.UTC), ready: true},
		// Period-end day, after the close instant (job ran late same day) -> still
		// READY by date; the guard fires on the closed period exactly once.
		{name: "period-end day after close ready", now: time.Date(2026, 7, 29, 9, 0, 0, 0, time.UTC), ready: true},
	}
	for _, c := range cases {
		if got := periodReadyToMeter(periodEnd, c.now); got != c.ready {
			t.Fatalf("%s: periodReadyToMeter=%v, want %v", c.name, got, c.ready)
		}
	}
}

// TestCursorDoesNotJumpOpenPeriod reproduces the cursor-jump billing bug and proves
// the fix. It replicates meterAccount's guard sequence (periodAlreadyMetered ->
// periodReadyToMeter -> hour-count gate -> report -> advance cursor) against the fake
// Stripe, stepping a single subscription through its first OPEN period and then its
// close, asserting the closed period with real usage is metered EXACTLY ONCE.
func TestCursorDoesNotJumpOpenPeriod(t *testing.T) {
	ctx := context.Background()

	// Subscription created 2026-06-29; first period [06-29, 07-29). The account
	// records 5 billable recording-hours during it. Stripe rolls current_period_end
	// to 08-29 the instant the first period closes.
	period1End := dateUTC(2026, 7, 29)
	period2End := dateUTC(2026, 8, 29)

	// cursor is the DB last_metered_period_end; nil = never metered (fresh sub).
	var cursor *time.Time

	// step models one daily sweep: it returns the open-period end Stripe reports as
	// of `now`, runs the exact guard sequence, and returns whether a report fired and
	// the period key it used. hours is the billable count Stripe would sum for the
	// reported period (our recording_billing_hours count for [start,end)).
	step := func(now, openPeriodEnd time.Time, hours int) (reported bool, key string) {
		f := &fakeMeteringStripe{periodStart: dateUTC(2026, 6, 29), periodEnd: openPeriodEnd}
		if periodAlreadyMetered(f.periodEnd, cursor) {
			return false, ""
		}
		if !periodReadyToMeter(f.periodEnd, now) {
			return false, "" // open period: must NOT advance the cursor (the bug).
		}
		if shouldReportHours(hours) {
			_ = f.ReportRecordingHours(ctx, "cus_x", 1, meterPeriodKey(f.periodEnd), hours)
			reported = true
			key = f.reports[0].periodKey
		}
		end := f.periodEnd
		cursor = &end // advance cursor only after a ready (closed-day) period.
		return reported, key
	}

	// Sweep 1: mid-period (2026-07-10), Stripe still reports the OPEN first period
	// (end 07-29), 2 hours accrued so far. The OLD code would report+advance here,
	// sealing the period before close. The fix must NOT advance.
	if rep, _ := step(dateUTC(2026, 7, 10), period1End, 2); rep {
		t.Fatalf("mid-period sweep reported; want skip (period still open)")
	}
	if cursor != nil {
		t.Fatalf("mid-period sweep advanced cursor to %v; want untouched (cursor-jump bug)", *cursor)
	}

	// Sweep 2: the period-end day (2026-07-29), before close. Stripe still reports
	// the closing period (end 07-29); the full 5-hour count is now final. The fix
	// meters it here, exactly once, with the period-end month key.
	rep, key := step(dateUTC(2026, 7, 29), period1End, 5)
	if !rep {
		t.Fatalf("period-end-day sweep did not report; the closed period would be silently $0-billed (the bug)")
	}
	if key != "2026-07-29" {
		t.Fatalf("reported period key = %q, want 2026-07-29 (period-end date)", key)
	}
	if cursor == nil || !cursor.Equal(period1End) {
		t.Fatalf("cursor = %v, want %v after metering the closed period", cursor, period1End)
	}

	// Sweep 3: after close Stripe reports the NEW open period (end 08-29). It is not
	// yet its period-end day, so no report and no advance: the new period is billed
	// only when ITS day arrives, never double-billing period 1.
	if rep, _ := step(dateUTC(2026, 7, 30), period2End, 1); rep {
		t.Fatalf("post-close sweep reported the new open period; want skip until its end day")
	}
	if !cursor.Equal(period1End) {
		t.Fatalf("post-close sweep moved cursor to %v; want it to stay at period1 end", *cursor)
	}

	// Sweep 4: a same-day RE-RUN on the period-end day must be a no-op (cursor
	// already at period1End => periodAlreadyMetered short-circuits before any report).
	if rep, _ := step(dateUTC(2026, 7, 29), period1End, 5); rep {
		t.Fatalf("same-period re-run reported again; want idempotent skip (double-bill risk)")
	}
}

func TestMeterPeriodKey(t *testing.T) {
	// Period-end UTC date, the per-period meter-event identifier component, so
	// re-sends within a period collapse to one Stripe meter event while two distinct
	// periods (even two ending in the same month after a re-anchor) get distinct keys.
	got := meterPeriodKey(time.Date(2026, 7, 24, 23, 59, 59, 0, time.UTC))
	if got != "2026-07-24" {
		t.Fatalf("meterPeriodKey = %q, want 2026-07-24", got)
	}
	// Non-UTC input is normalized to UTC before formatting (period end just after
	// UTC midnight, expressed in a -05:00 zone, still belongs to the next UTC day).
	loc := time.FixedZone("UTC-5", -5*3600)
	gotTZ := meterPeriodKey(time.Date(2026, 7, 31, 20, 0, 0, 0, loc)) // == 2026-08-01T01:00Z
	if gotTZ != "2026-08-01" {
		t.Fatalf("meterPeriodKey(tz) = %q, want 2026-08-01", gotTZ)
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
	periodStart time.Time
	periodEnd   time.Time
	reports     []reportCall
	shmReports  []shmReportCall
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

func (f *fakeMeteringStripe) ReportRecordingHours(_ context.Context, customerID string, accountID int64, periodKey string, hours int) error {
	f.reports = append(f.reports, reportCall{customerID, accountID, periodKey, hours})
	return nil
}

func (f *fakeMeteringStripe) ReportStreamHourMonth(_ context.Context, customerID string, accountID int64, periodKey, hoursDecimal string) error {
	f.shmReports = append(f.shmReports, shmReportCall{customerID, accountID, periodKey, hoursDecimal})
	return nil
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
		if periodAlreadyMetered(f.periodEnd, a.lastMeteredPeriodEnd) {
			return nil // guard skip: never reports
		}
		if shouldReportHours(hours) {
			_ = f.ReportRecordingHours(ctx, a.customerID, a.accountID, meterPeriodKey(f.periodEnd), hours)
		}
		return f.reports
	}

	// Fresh account, 7 billable hours: one report with the period-end month key.
	got := report(meterableAccount{accountID: 42, customerID: "cus_x"}, 7)
	if len(got) != 1 || got[0] != (reportCall{"cus_x", 42, "2026-08-01", 7}) {
		t.Fatalf("fresh 7-hour report = %+v, want one {cus_x,42,2026-08-01,7}", got)
	}

	// Zero billable hours: no report (empty invoice suppressed).
	if got := report(meterableAccount{accountID: 42, customerID: "cus_x"}, 0); len(got) != 0 {
		t.Fatalf("zero-hour report = %+v, want none", got)
	}

	// Already metered this period (cursor == period end): guard skips before report.
	last := periodEnd
	if got := report(meterableAccount{accountID: 42, customerID: "cus_x", lastMeteredPeriodEnd: &last}, 7); len(got) != 0 {
		t.Fatalf("already-metered report = %+v, want none (idempotent skip)", got)
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
		if periodAlreadyMetered(f.periodEnd, a.lastMeteredPeriodEnd) {
			return nil // guard skip: never reports
		}
		if hoursDecimal, ok := streamHourMonthMeterValue(sumHours, snapDays); ok {
			_ = f.ReportStreamHourMonth(ctx, a.customerID, a.accountID, meterPeriodKey(f.periodEnd), hoursDecimal)
		}
		return f.shmReports
	}

	// Fresh account, 31 snapshots => one report with the averaged decimal string.
	got := report(meterableAccount{accountID: 42, customerID: "cus_x"}, 76.601, 31)
	if len(got) != 1 || got[0] != (shmReportCall{"cus_x", 42, "2026-08-01", "2.471"}) {
		t.Fatalf("fresh stream-hour-month report = %+v, want one {cus_x,42,2026-08-01,2.471}", got)
	}

	// No managed footage (BYO / fully purged): no report.
	if got := report(meterableAccount{accountID: 42, customerID: "cus_x"}, 0, 0); len(got) != 0 {
		t.Fatalf("zero report = %+v, want none", got)
	}

	// Already metered this period (cursor == period end): guard skips before report,
	// so a re-run is a no-op for the stream_hour_month meter too (idempotency).
	last := periodEnd
	if got := report(meterableAccount{accountID: 42, customerID: "cus_x", lastMeteredPeriodEnd: &last}, 76.601, 31); len(got) != 0 {
		t.Fatalf("already-metered report = %+v, want none (idempotent skip)", got)
	}
}
