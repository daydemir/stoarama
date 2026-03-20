package capture

import (
	"context"
	"fmt"
	"time"
)

// EnqueueWithTimeout sends value into ch and fails fast with an explicit error
// when the sink is blocked for longer than timeout.
func EnqueueWithTimeout[T any](ctx context.Context, ch chan<- T, value T, timeout time.Duration, sinkName string) error {
	if timeout <= 0 {
		return fmt.Errorf("invalid enqueue timeout for %s: %s", sinkName, timeout)
	}

	select {
	case ch <- value:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case ch <- value:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("frame sink backpressure: %s blocked for %s", sinkName, timeout)
	}
}
