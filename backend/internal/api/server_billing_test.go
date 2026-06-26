package api

import (
	"testing"
	"time"

	stripe "github.com/stripe/stripe-go/v82"
)

func TestRecorderLineItemMatchesPrice(t *testing.T) {
	periodEnd := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	sub := &stripe.Subscription{
		Items: &stripe.SubscriptionItemList{
			Data: []*stripe.SubscriptionItem{
				{ID: "si_other", Quantity: 9, Price: &stripe.Price{ID: "price_other"}, CurrentPeriodEnd: 111},
				{ID: "si_rec", Quantity: 3, Price: &stripe.Price{ID: "price_rec"}, CurrentPeriodEnd: periodEnd.Unix()},
			},
		},
	}
	itemID, qty, gotEnd := recorderLineItem(sub, "price_rec")
	if itemID != "si_rec" {
		t.Fatalf("item id = %q, want si_rec", itemID)
	}
	if qty != 3 {
		t.Fatalf("quantity = %d, want 3", qty)
	}
	if !gotEnd.Equal(periodEnd) {
		t.Fatalf("period end = %v, want %v", gotEnd, periodEnd)
	}
}

func TestRecorderLineItemFallsBackToFirstItem(t *testing.T) {
	// Price id does not match any line; the handler must still surface an item so
	// quantity/period are recorded (e.g. the price was renamed in Stripe).
	sub := &stripe.Subscription{
		Items: &stripe.SubscriptionItemList{
			Data: []*stripe.SubscriptionItem{
				{ID: "si_only", Quantity: 2, Price: &stripe.Price{ID: "price_renamed"}, CurrentPeriodEnd: 222},
			},
		},
	}
	itemID, qty, _ := recorderLineItem(sub, "price_rec")
	if itemID != "si_only" || qty != 2 {
		t.Fatalf("fallback line = (%q,%d), want (si_only,2)", itemID, qty)
	}
}

func TestRecorderLineItemEmptySubscription(t *testing.T) {
	if id, qty, end := recorderLineItem(nil, "price_rec"); id != "" || qty != 0 || !end.IsZero() {
		t.Fatalf("nil sub = (%q,%d,%v), want empty", id, qty, end)
	}
	empty := &stripe.Subscription{Items: &stripe.SubscriptionItemList{Data: nil}}
	if id, qty, end := recorderLineItem(empty, "price_rec"); id != "" || qty != 0 || !end.IsZero() {
		t.Fatalf("empty items = (%q,%d,%v), want empty", id, qty, end)
	}
}

func TestValidSubscriptionStatus(t *testing.T) {
	valid := []string{"none", "incomplete", "trialing", "active", "past_due", "canceled", "unpaid", "incomplete_expired"}
	for _, s := range valid {
		if !validSubscriptionStatus(s) {
			t.Fatalf("validSubscriptionStatus(%q) = false, want true", s)
		}
	}
	for _, s := range []string{"paused", "", "ACTIVE", "garbage"} {
		if validSubscriptionStatus(s) {
			t.Fatalf("validSubscriptionStatus(%q) = true, want false", s)
		}
	}
}

func TestSubscriptionStatusGrantsAccess(t *testing.T) {
	grant := []string{"active", "trialing", "past_due"}
	for _, s := range grant {
		if !subscriptionStatusGrantsAccess(s) {
			t.Fatalf("subscriptionStatusGrantsAccess(%q) = false, want true", s)
		}
	}
	deny := []string{"none", "incomplete", "canceled", "unpaid", "incomplete_expired", ""}
	for _, s := range deny {
		if subscriptionStatusGrantsAccess(s) {
			t.Fatalf("subscriptionStatusGrantsAccess(%q) = true, want false", s)
		}
	}
}

func TestQuantitySyncCancels(t *testing.T) {
	// Stripe rejects quantity 0 on a licensed item, so a drop to 0 active
	// recordings must cancel the subscription instead of pushing quantity 0.
	cancels := []int64{0, -1, -5}
	for _, q := range cancels {
		if !quantitySyncCancels(q) {
			t.Fatalf("quantitySyncCancels(%d) = false, want true (cancel on zero/empty)", q)
		}
	}
	keeps := []int64{1, 2, 50}
	for _, q := range keeps {
		if quantitySyncCancels(q) {
			t.Fatalf("quantitySyncCancels(%d) = true, want false (push quantity)", q)
		}
	}
}

func TestClientRefAccountID(t *testing.T) {
	cases := map[string]int64{
		"42":    42,
		"  7  ": 7,
		"0":     0,
		"-1":    0,
		"":      0,
		"abc":   0,
		"12.5":  0,
	}
	for in, want := range cases {
		if got := clientRefAccountID(in); got != want {
			t.Fatalf("clientRefAccountID(%q) = %d, want %d", in, got, want)
		}
	}
}
