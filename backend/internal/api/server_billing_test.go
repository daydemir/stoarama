package api

import (
	"testing"
	"time"

	stripe "github.com/stripe/stripe-go/v82"
)

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

func TestCustomerIDOf(t *testing.T) {
	if got := customerIDOf(nil); got != "" {
		t.Fatalf("customerIDOf(nil) = %q, want empty", got)
	}
	if got := customerIDOf(&stripe.Customer{ID: "  cus_123 "}); got != "cus_123" {
		t.Fatalf("customerIDOf trimmed = %q, want cus_123", got)
	}
}

func TestCheckoutDefaultPaymentMethodID(t *testing.T) {
	// No subscription expanded onto the session: nothing to read.
	if got := checkoutDefaultPaymentMethodID(&stripe.CheckoutSession{}); got != "" {
		t.Fatalf("no subscription = %q, want empty", got)
	}
	// Subscription present but no default payment method.
	noPM := &stripe.CheckoutSession{Subscription: &stripe.Subscription{}}
	if got := checkoutDefaultPaymentMethodID(noPM); got != "" {
		t.Fatalf("no default pm = %q, want empty", got)
	}
	// Subscription with a default payment method (trimmed).
	withPM := &stripe.CheckoutSession{Subscription: &stripe.Subscription{
		DefaultPaymentMethod: &stripe.PaymentMethod{ID: " pm_abc "},
	}}
	if got := checkoutDefaultPaymentMethodID(withPM); got != "pm_abc" {
		t.Fatalf("default pm = %q, want pm_abc", got)
	}
}

func TestEventCreatedArg(t *testing.T) {
	// No created timestamp -> nil arg so the out-of-order guard treats it as "now".
	if got := eventCreatedArg(stripe.Event{}); got != nil {
		t.Fatalf("zero created = %v, want nil", got)
	}
	created := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	got := eventCreatedArg(stripe.Event{Created: created.Unix()})
	gotTime, ok := got.(time.Time)
	if !ok || !gotTime.Equal(created) {
		t.Fatalf("created arg = %v, want %v", got, created)
	}
}
