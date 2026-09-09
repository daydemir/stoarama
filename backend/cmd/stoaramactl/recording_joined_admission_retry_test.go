package main

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestJoinedAdmissionRetryDistinguishesBootstrapAndAmbiguousClaim(t *testing.T) {
	t.Parallel()
	now := time.Now()
	tests := []struct {
		name string
		err  error
		want time.Duration
		ok   bool
	}{
		{"bootstrap", &joinedAPIResponseError{path: "/api/v1/recording/joined/token", status: http.StatusServiceUnavailable}, 2 * time.Second, true},
		{"status", &joinedAPITransportError{path: "/api/v1/recording/joined/leases/status", cause: syscall.ECONNRESET}, 2 * time.Second, true},
		{"publication claim", &joinedAPIResponseError{path: "/api/v1/recording/joined/publication/claim", status: http.StatusTooManyRequests}, joinedClaimLeaseTTL + joinedClaimLeaseMargin, true},
		{"preflight claim", &joinedAPITransportError{path: "/api/v1/recording/joined/claim", cause: context.DeadlineExceeded}, joinedClaimLeaseTTL + joinedClaimLeaseMargin, true},
		{"auth", &joinedAPIResponseError{path: "/api/v1/recording/joined/token", status: http.StatusUnauthorized}, 0, false},
		{"schema", errors.New("invalid bootstrap response"), 0, false},
		{"deterministic transport", &joinedAPITransportError{path: "/api/v1/recording/joined/token", cause: errors.New("TLS certificate differs")}, 0, false},
		{"unknown endpoint", &joinedAPIResponseError{path: "/api/v1/recording/joined/finalize", status: http.StatusServiceUnavailable}, 0, false},
		{"mixed ambiguity", errors.Join(&joinedAPIResponseError{path: "/api/v1/recording/joined/token", status: http.StatusServiceUnavailable}, errors.New("identity differs")), 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := joinedAdmissionRetryState{}
			got, ok := state.next(tc.err, 2*time.Second, now)
			if got != tc.want || ok != tc.ok {
				t.Fatalf("delay=%s ok=%v want %s/%v", got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestJoinedAdmissionRetryIsAttemptAndTimeBounded(t *testing.T) {
	t.Parallel()
	err := &joinedAPITransportError{path: "/api/v1/recording/joined/claim", cause: syscall.ECONNRESET}
	now := time.Now()
	state := joinedAdmissionRetryState{}
	for attempt := 1; attempt <= joinedAdmissionRetryMaxAttempts; attempt++ {
		if delay, ok := state.next(err, 2*time.Second, now.Add(time.Duration(attempt-1)*(joinedClaimLeaseTTL+joinedClaimLeaseMargin))); !ok || delay != joinedClaimLeaseTTL+joinedClaimLeaseMargin {
			t.Fatalf("attempt %d delay=%s ok=%v", attempt, delay, ok)
		}
	}
	if delay, ok := state.next(err, 2*time.Second, now); ok || delay != 0 {
		t.Fatalf("ninth retry delay=%s ok=%v", delay, ok)
	}

	state.reset()
	if _, ok := state.next(err, 2*time.Second, now); !ok {
		t.Fatal("first retry after reset was rejected")
	}
	if delay, ok := state.next(err, 2*time.Second, now.Add(joinedAdmissionRetryWindow)); ok || delay != 0 {
		t.Fatalf("retry beyond time window delay=%s ok=%v", delay, ok)
	}
}

func TestJoinedWorkerLoopRecoversBriefBootstrapOutage(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Int32
	err := runJoinedWorkerLoop(ctx, time.Millisecond, func(context.Context, context.Context) (bool, error) {
		if calls.Add(1) == 1 {
			return false, &joinedAPIResponseError{path: "/api/v1/recording/joined/token", status: http.StatusServiceUnavailable}
		}
		cancel()
		return false, nil
	})
	if err != nil || calls.Load() != 2 {
		t.Fatalf("worker err=%v calls=%d", err, calls.Load())
	}
}
