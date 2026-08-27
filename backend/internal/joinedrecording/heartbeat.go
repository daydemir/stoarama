package joinedrecording

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const DefaultHeartbeatInterval = 60 * time.Second

type OperationCredentials struct {
	LeaseID        string    `json:"lease_id"`
	OperationToken string    `json:"operation_token"`
	ExpiresAt      time.Time `json:"expires_at"`
}

type HeartbeatOperation func(context.Context, string) (OperationCredentials, error)
type renewableRunner func(context.Context, OperationCredentials, HeartbeatOperation, func(context.Context, func() OperationCredentials) error) error

func defaultRenewableRunner(ctx context.Context, initial OperationCredentials, heartbeat HeartbeatOperation, work func(context.Context, func() OperationCredentials) error) error {
	return RunWithHeartbeat(ctx, initial, DefaultHeartbeatInterval, heartbeat, work)
}

// RunWithHeartbeat keeps one stable lease alive while work uses refreshed
// operation tokens. The lease ID is correlation only and never authorizes a
// request. Work must call current immediately before each fenced API call.
func RunWithHeartbeat(ctx context.Context, initial OperationCredentials, interval time.Duration, heartbeat HeartbeatOperation, work func(context.Context, func() OperationCredentials) error) error {
	if interval <= 0 {
		return fmt.Errorf("invalid renewable joined operation")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	return runWithHeartbeat(ctx, initial, interval, heartbeat, work, time.Now, ticker.C)
}

func runWithHeartbeat(ctx context.Context, initial OperationCredentials, interval time.Duration, heartbeat HeartbeatOperation, work func(context.Context, func() OperationCredentials) error, now func() time.Time, ticks <-chan time.Time) error {
	if now == nil || ticks == nil || !validLeaseID(initial.LeaseID) || !validOperationToken(initial.OperationToken) || !initial.ExpiresAt.After(now()) || interval <= 0 || heartbeat == nil || work == nil {
		return fmt.Errorf("invalid renewable joined operation")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var mu sync.RWMutex
	current := initial
	get := func() OperationCredentials {
		mu.RLock()
		defer mu.RUnlock()
		return current
	}
	heartbeatErr := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticks:
				prior := get()
				next, err := heartbeat(ctx, prior.OperationToken)
				if err != nil {
					if now().Add(interval).Before(prior.ExpiresAt) {
						continue
					}
					select {
					case heartbeatErr <- fmt.Errorf("joined heartbeat failed before lease expiry: %w", err):
					default:
					}
					cancel()
					return
				}
				if next.LeaseID != prior.LeaseID || !validOperationToken(next.OperationToken) || !next.ExpiresAt.After(now()) {
					select {
					case heartbeatErr <- fmt.Errorf("joined heartbeat moved lease identity"):
					default:
					}
					cancel()
					return
				}
				if next.ExpiresAt.Before(prior.ExpiresAt) {
					continue
				}
				mu.Lock()
				current = next
				mu.Unlock()
			}
		}
	}()
	workErr := work(ctx, get)
	cancel()
	<-done
	select {
	case err := <-heartbeatErr:
		if workErr != nil && !errors.Is(workErr, context.Canceled) {
			return errors.Join(workErr, err)
		}
		return err
	default:
		return workErr
	}
}

func (c WorkerClaim) WithOperation(credentials OperationCredentials) (WorkerClaim, error) {
	if credentials.LeaseID != c.LeaseID || !validOperationToken(credentials.OperationToken) || !credentials.ExpiresAt.After(time.Now()) || credentials.ExpiresAt.Before(c.LeaseExpires) {
		return WorkerClaim{}, fmt.Errorf("renewed hour operation differs")
	}
	c.OperationToken, c.LeaseExpires = credentials.OperationToken, credentials.ExpiresAt
	return c, nil
}
