package joinedrecording

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHeartbeatRefreshKeepsStableLeaseAndScratch(t *testing.T) {
	now := time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC)
	leaseID := strings.Repeat("L", 43)
	initial := OperationCredentials{LeaseID: leaseID, OperationToken: strings.Repeat("a", 32), ExpiresAt: now.Add(5 * time.Minute)}
	ticks := make(chan time.Time, 1)
	refreshed := OperationCredentials{LeaseID: leaseID, OperationToken: strings.Repeat("b", 32), ExpiresAt: now.Add(6 * time.Minute)}
	err := runWithHeartbeat(context.Background(), initial, time.Minute, func(_ context.Context, token string) (OperationCredentials, error) {
		if token != initial.OperationToken {
			t.Fatalf("heartbeat used unexpected token")
		}
		return refreshed, nil
	}, func(_ context.Context, current func() OperationCredentials) error {
		ticks <- now.Add(time.Minute)
		for current().OperationToken != refreshed.OperationToken {
			runtime.Gosched()
		}
		if current().LeaseID != leaseID || current().ExpiresAt != refreshed.ExpiresAt {
			t.Fatal("heartbeat changed lease or ignored greatest expiry")
		}
		return nil
	}, func() time.Time { return now }, ticks)
	if err != nil {
		t.Fatal(err)
	}
	want := "/tmp/joined/" + leaseID
	if got := leaseScratchDir("/tmp/joined", leaseID); got != want {
		t.Fatalf("scratch=%s want %s", got, want)
	}
}

func TestHeartbeatRetainsGreatestSameLeaseExpiry(t *testing.T) {
	now := time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC)
	leaseID := strings.Repeat("L", 43)
	initial := OperationCredentials{LeaseID: leaseID, OperationToken: strings.Repeat("a", 32), ExpiresAt: now.Add(5 * time.Minute)}
	ticks := make(chan time.Time, 2)
	secondCalled := make(chan struct{})
	var calls atomic.Int32
	err := runWithHeartbeat(context.Background(), initial, time.Minute, func(_ context.Context, token string) (OperationCredentials, error) {
		if calls.Add(1) == 1 {
			return OperationCredentials{LeaseID: leaseID, OperationToken: strings.Repeat("b", 32), ExpiresAt: now.Add(4 * time.Minute)}, nil
		}
		if token != initial.OperationToken {
			t.Fatalf("lower-expiry token was used on the next heartbeat")
		}
		close(secondCalled)
		return initial, nil
	}, func(_ context.Context, current func() OperationCredentials) error {
		ticks <- now.Add(time.Minute)
		ticks <- now.Add(2 * time.Minute)
		<-secondCalled
		if current().OperationToken != initial.OperationToken {
			t.Fatal("lower-expiry response replaced current credentials")
		}
		return nil
	}, func() time.Time { return now }, ticks)
	if err != nil {
		t.Fatal(err)
	}
}

func TestHeartbeatLostResponseCanRetryWithPriorToken(t *testing.T) {
	now := time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC)
	leaseID := strings.Repeat("L", 43)
	initial := OperationCredentials{LeaseID: leaseID, OperationToken: strings.Repeat("a", 32), ExpiresAt: now.Add(5 * time.Minute)}
	refreshed := OperationCredentials{LeaseID: leaseID, OperationToken: strings.Repeat("b", 32), ExpiresAt: now.Add(6 * time.Minute)}
	ticks := make(chan time.Time, 2)
	first := make(chan struct{})
	var calls atomic.Int32
	err := runWithHeartbeat(context.Background(), initial, time.Minute, func(_ context.Context, token string) (OperationCredentials, error) {
		if token != initial.OperationToken {
			t.Fatalf("retry used uncommitted token")
		}
		if calls.Add(1) == 1 {
			close(first)
			return OperationCredentials{}, errors.New("lost heartbeat response")
		}
		return refreshed, nil
	}, func(_ context.Context, current func() OperationCredentials) error {
		ticks <- now.Add(time.Minute)
		<-first
		if current().OperationToken != initial.OperationToken {
			t.Fatal("lost response replaced current credentials")
		}
		ticks <- now.Add(2 * time.Minute)
		for current().OperationToken != refreshed.OperationToken {
			runtime.Gosched()
		}
		return nil
	}, func() time.Time { return now }, ticks)
	if err != nil || calls.Load() != 2 {
		t.Fatalf("calls=%d err=%v", calls.Load(), err)
	}
}

func TestHeartbeatRejectsExpiredAndForeignRenewalsBeforeWorkCanFinish(t *testing.T) {
	now := time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC)
	leaseID := strings.Repeat("L", 43)
	initial := OperationCredentials{LeaseID: leaseID, OperationToken: strings.Repeat("a", 32), ExpiresAt: now.Add(5 * time.Minute)}
	for name, next := range map[string]OperationCredentials{
		"expired":       {LeaseID: leaseID, OperationToken: strings.Repeat("b", 32), ExpiresAt: now},
		"foreign lease": {LeaseID: strings.Repeat("F", 43), OperationToken: strings.Repeat("b", 32), ExpiresAt: now.Add(6 * time.Minute)},
	} {
		t.Run(name, func(t *testing.T) {
			ticks := make(chan time.Time, 1)
			var finished atomic.Bool
			err := runWithHeartbeat(context.Background(), initial, time.Minute, func(context.Context, string) (OperationCredentials, error) {
				return next, nil
			}, func(ctx context.Context, _ func() OperationCredentials) error {
				ticks <- now.Add(time.Minute)
				<-ctx.Done()
				finished.Store(true)
				return ctx.Err()
			}, func() time.Time { return now }, ticks)
			if err == nil || !finished.Load() {
				t.Fatalf("invalid renewal did not cancel work: %v", err)
			}
		})
	}
}

func TestHeartbeatCancellationPreventsFinalize(t *testing.T) {
	now := time.Now().UTC()
	leaseID := strings.Repeat("L", 43)
	initial := OperationCredentials{LeaseID: leaseID, OperationToken: strings.Repeat("a", 32), ExpiresAt: now.Add(5 * time.Minute)}
	ctx, cancel := context.WithCancel(context.Background())
	var finalized atomic.Bool
	err := runWithHeartbeat(ctx, initial, time.Minute, func(context.Context, string) (OperationCredentials, error) {
		return initial, nil
	}, func(workCtx context.Context, _ func() OperationCredentials) error {
		cancel()
		<-workCtx.Done()
		if workCtx.Err() == nil {
			finalized.Store(true)
		}
		return workCtx.Err()
	}, time.Now, make(chan time.Time))
	if !errors.Is(err, context.Canceled) || finalized.Load() {
		t.Fatalf("cancel err=%v finalized=%v", err, finalized.Load())
	}
}

func TestOperationRefreshRejectsExpiredLowerAndForeignCredentials(t *testing.T) {
	now := time.Now().UTC()
	leaseID := strings.Repeat("L", 43)
	claim := WorkerClaim{LeaseID: leaseID, LeaseExpires: now.Add(5 * time.Minute)}
	for name, credentials := range map[string]OperationCredentials{
		"expired": {LeaseID: leaseID, OperationToken: strings.Repeat("a", 32), ExpiresAt: now.Add(-time.Second)},
		"lower":   {LeaseID: leaseID, OperationToken: strings.Repeat("a", 32), ExpiresAt: now.Add(4 * time.Minute)},
		"foreign": {LeaseID: strings.Repeat("F", 43), OperationToken: strings.Repeat("a", 32), ExpiresAt: now.Add(6 * time.Minute)},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := claim.WithOperation(credentials); err == nil {
				t.Fatal("invalid operation refresh accepted")
			}
		})
	}
	if validLeaseID(strings.Repeat("L", 42)) || validLeaseID(strings.Repeat("L", 44)) || !validLeaseID(leaseID) {
		t.Fatal("lease identity length is not exact")
	}
}
