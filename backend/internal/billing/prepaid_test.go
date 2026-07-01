package billing

import (
	"testing"
	"time"

	stripe "github.com/stripe/stripe-go/v82"
)

// TestPrepaidBatchCents locks the cents math round(stream_hours * 12 * $0.05) and the
// 0-stream-hours skip (never charge $0 for no footage).
func TestPrepaidBatchCents(t *testing.T) {
	cases := []struct {
		name        string
		streamHours float64
		wantCents   int64
	}{
		{name: "zero hours -> 0 (skip)", streamHours: 0, wantCents: 0},
		{name: "negative hours -> 0 (skip)", streamHours: -3, wantCents: 0},
		// 1 stream-hour * 12 * 5c = 60c.
		{name: "1 hour -> 60c", streamHours: 1, wantCents: 60},
		// 10 stream-hours * 12 * 5c = 600c ($6.00).
		{name: "10 hours -> 600c", streamHours: 10, wantCents: 600},
		// 0.5 * 12 * 5 = 30c.
		{name: "half hour -> 30c", streamHours: 0.5, wantCents: 30},
		// Rounding: 0.001 * 12 * 5 = 0.06c -> rounds to 0 (skip; a few seconds of footage).
		{name: "tiny footage rounds to 0", streamHours: 0.001, wantCents: 0},
		// Rounding: 0.01 * 12 * 5 = 0.6c -> rounds to 1c.
		{name: "0.01 hour -> 1c", streamHours: 0.01, wantCents: 1},
		// 2.5 * 12 * 5 = 150c.
		{name: "2.5 hours -> 150c", streamHours: 2.5, wantCents: 150},
	}
	for _, tc := range cases {
		if got := PrepaidBatchCents(tc.streamHours); got != tc.wantCents {
			t.Fatalf("%s: PrepaidBatchCents(%v)=%d, want %d", tc.name, tc.streamHours, got, tc.wantCents)
		}
	}
}

// TestPrepaidBatchCentsHalfMetered pins the effective yearly rate at exactly half the
// metered $0.10/hr-mo: a full year of storage for 1 stream-hour = 12 * 5c = 60c,
// versus 12 * 10c = 120c metered.
func TestPrepaidBatchCentsHalfMetered(t *testing.T) {
	if PrepaidStreamHourMonthRateCents*2 != 10 {
		t.Fatalf("prepaid rate %d is not half the metered 10c", PrepaidStreamHourMonthRateCents)
	}
	if PrepaidCreditMonths != 12 {
		t.Fatalf("prepaid covers %d months, want 12", PrepaidCreditMonths)
	}
}

// TestPrepaidCreditGrantScopeStorageOnly is the load-bearing test: the credit grant
// must scope to the storage price ONLY and NEVER to the recording-hour price, or the
// prepaid credit would silently pay for recording-hours too.
func TestPrepaidCreditGrantScopeStorageOnly(t *testing.T) {
	const storagePriceID = "price_stream_hour_month_storage"
	const recordingHourPriceID = "price_recording_hour"

	params := prepaidCreditGrantParams("cus_test", 600, time.Now().AddDate(0, 12, 0), "prepay:acct-1:2026-07", storagePriceID, map[string]string{"kind": "yearly_prepaid_storage"})

	if params.ApplicabilityConfig == nil || params.ApplicabilityConfig.Scope == nil {
		t.Fatalf("credit grant has no applicability scope")
	}
	prices := params.ApplicabilityConfig.Scope.Prices
	if len(prices) != 1 {
		t.Fatalf("credit grant scope has %d prices, want exactly 1 (storage only)", len(prices))
	}
	if prices[0] == nil || prices[0].ID == nil {
		t.Fatalf("credit grant scope price id is nil")
	}
	if *prices[0].ID != storagePriceID {
		t.Fatalf("credit grant scoped to %q, want the storage price %q", *prices[0].ID, storagePriceID)
	}
	if *prices[0].ID == recordingHourPriceID {
		t.Fatalf("credit grant scoped to the recording-hour price; it must NEVER be")
	}

	// Category must be paid (the customer paid via the prepay invoice).
	if params.Category == nil || *params.Category != string(stripe.BillingCreditGrantCategoryPaid) {
		t.Fatalf("credit grant category is not 'paid'")
	}
	// Amount must be USD monetary with the exact cents.
	if params.Amount == nil || params.Amount.Monetary == nil || params.Amount.Monetary.Value == nil || *params.Amount.Monetary.Value != 600 {
		t.Fatalf("credit grant amount is not 600 cents monetary")
	}
}
