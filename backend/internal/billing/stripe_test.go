package billing

import (
	"errors"
	"fmt"
	"testing"

	stripe "github.com/stripe/stripe-go/v82"
)

// TestIsDuplicateMeterEvent locks in the no-double-bill guarantee: a re-sent meter
// event whose identifier was already used (a same-day retry, or usage a prior
// out-of-cycle invoice already consumed) is rejected by Stripe with a generic
// invalid_request_error whose message contains the identifier-collision phrase, and
// the metering job must treat that as a no-op (so the cursor still advances) rather
// than an error (which would retry forever and never re-bill the consumed usage).
func TestIsDuplicateMeterEvent(t *testing.T) {
	// The exact Stripe message observed for a duplicate identifier (verified live in
	// test mode against the recording_hour meter).
	dup := errors.New("An event already exists with identifier 1-2026-07.")
	if !isDuplicateMeterEvent(dup) {
		t.Fatalf("duplicate-identifier error not recognized: %v", dup)
	}
	// Wrapped the way the SDK/our code may surround it.
	if !isDuplicateMeterEvent(fmt.Errorf("report recording hours: %w", dup)) {
		t.Fatalf("wrapped duplicate-identifier error not recognized")
	}

	// Structured path: stripe.Error with Code=resource_already_exists is also a duplicate.
	stripeAlreadyExists := &stripe.Error{Code: stripe.ErrorCodeResourceAlreadyExists}
	if !isDuplicateMeterEvent(stripeAlreadyExists) {
		t.Fatalf("stripe.Error resource_already_exists not recognized as duplicate")
	}
	// Wrapped structured error.
	if !isDuplicateMeterEvent(fmt.Errorf("report recording hours: %w", stripeAlreadyExists)) {
		t.Fatalf("wrapped stripe.Error resource_already_exists not recognized as duplicate")
	}

	// Unrelated Stripe/transport errors must NOT be swallowed; they have to surface
	// so the job retries instead of silently advancing the cursor over real usage.
	for _, e := range []error{
		nil,
		errors.New("No such customer: 'cus_x'"),
		errors.New("rate limit exceeded"),
		errors.New("connection reset by peer"),
	} {
		if isDuplicateMeterEvent(e) {
			t.Fatalf("non-duplicate error wrongly treated as duplicate: %v", e)
		}
	}
}
