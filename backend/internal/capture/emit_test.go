package capture

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestEnqueueWithTimeoutSuccess(t *testing.T) {
	t.Parallel()

	ch := make(chan int, 1)
	if err := EnqueueWithTimeout(context.Background(), ch, 7, 50*time.Millisecond, "test.sink"); err != nil {
		t.Fatalf("enqueue success error: %v", err)
	}
	select {
	case got := <-ch:
		if got != 7 {
			t.Fatalf("enqueued value=%d want=7", got)
		}
	default:
		t.Fatalf("expected value in channel")
	}
}

func TestEnqueueWithTimeoutContextCanceled(t *testing.T) {
	t.Parallel()

	ch := make(chan int)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := EnqueueWithTimeout(ctx, ch, 1, 20*time.Millisecond, "test.sink")
	if err == nil {
		t.Fatalf("expected context error")
	}
	if !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("error=%v does not include context canceled", err)
	}
}

func TestEnqueueWithTimeoutBackpressure(t *testing.T) {
	t.Parallel()

	ch := make(chan int, 1)
	ch <- 1

	err := EnqueueWithTimeout(context.Background(), ch, 2, 20*time.Millisecond, "test.sink")
	if err == nil {
		t.Fatalf("expected backpressure error")
	}
	if !strings.Contains(err.Error(), "frame sink backpressure") {
		t.Fatalf("unexpected error: %v", err)
	}
}
