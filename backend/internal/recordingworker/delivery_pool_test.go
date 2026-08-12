package recordingworker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/recordingapi"
)

// TestSegmentDeliveryPoolKeepsSeveralUploadsInFlight is the regression this whole
// change exists for: one continuous job used to deliver segments strictly one at a
// time, so its sustained throughput was capped at a single reserve+PUT+ingest round
// trip. The pool must have all UploadWorkers uploads in flight simultaneously for
// one job.
func TestSegmentDeliveryPoolKeepsSeveralUploadsInFlight(t *testing.T) {
	const workers = 4
	var mu sync.Mutex
	inFlight, maxInFlight := 0, 0
	release := make(chan struct{})

	pool := startSegmentDeliveryPool(workers, func() {}, func(capture.Segment) error {
		mu.Lock()
		inFlight++
		if inFlight > maxInFlight {
			maxInFlight = inFlight
		}
		mu.Unlock()
		<-release
		mu.Lock()
		inFlight--
		mu.Unlock()
		return nil
	})

	for i := range workers {
		if err := pool.Submit(capture.Segment{Path: fmt.Sprintf("seg-%d.mp4", i)}); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}
	waitFor(t, "all uploads in flight", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return inFlight == workers
	})
	close(release)

	result := pool.close()
	if result.err != nil {
		t.Fatalf("delivery error: %v", result.err)
	}
	mu.Lock()
	peak := maxInFlight
	mu.Unlock()
	if peak != workers {
		t.Fatalf("peak concurrent uploads=%d want %d (serial delivery is the bug)", peak, workers)
	}
	if result.submitted != workers || result.pending != 0 || !result.ingested {
		t.Fatalf("result=%+v want submitted=%d pending=0 ingested=true", result, workers)
	}
}

func TestSegmentDeliveryPoolQueueObserverCannotPublishAfterLaterMutation(t *testing.T) {
	observerEntered := make(chan struct{})
	releaseObserver := make(chan struct{})
	delivered := make(chan struct{})
	var mu sync.Mutex
	var depths []int
	pool := startSegmentDeliveryPool(1, func() {}, func(capture.Segment) error { <-delivered; return nil }, func(depth int) {
		mu.Lock()
		first := len(depths) == 0
		mu.Unlock()
		if first {
			close(observerEntered)
			<-releaseObserver
		}
		mu.Lock()
		depths = append(depths, depth)
		mu.Unlock()
	})
	submittedOne := make(chan error, 1)
	go func() { submittedOne <- pool.Submit(capture.Segment{Path: "one"}) }()
	<-observerEntered
	// This forces the original inversion: enqueue(1) has mutated the queue but its
	// publication is blocked while a worker and another submit try to mutate it.
	// Both must wait behind the same queue lock, so depth 1 cannot publish stale
	// after a later depth 0/1 mutation.
	submitted := make(chan error, 1)
	go func() { submitted <- pool.Submit(capture.Segment{Path: "two"}) }()
	select {
	case <-submitted:
		t.Fatal("later mutation overtook blocked depth publication")
	case <-time.After(25 * time.Millisecond):
	}
	close(releaseObserver)
	if err := <-submittedOne; err != nil {
		t.Fatal(err)
	}
	if err := <-submitted; err != nil {
		t.Fatal(err)
	}
	close(delivered)
	result := pool.close()
	if result.err != nil {
		t.Fatal(result.err)
	}
	mu.Lock()
	got := append([]int(nil), depths...)
	mu.Unlock()
	if len(got) != 4 {
		t.Fatalf("depth events=%v", got)
	}
	if got[0] != 1 || got[len(got)-1] != 0 {
		t.Fatalf("depth events=%v: stale publication escaped mutation order", got)
	}
}

// TestSegmentDeliveryPoolCloseDrainsOutstandingUploads pins the window-close
// contract: close() joins every accepted segment, so making delivery concurrent
// does not simply move clip loss to the window boundary.
func TestSegmentDeliveryPoolCloseDrainsOutstandingUploads(t *testing.T) {
	const workers = 3
	const segments = 9
	var delivered atomic.Int64
	started := make(chan struct{}, segments)

	pool := startSegmentDeliveryPool(workers, func() {}, func(capture.Segment) error {
		started <- struct{}{}
		time.Sleep(10 * time.Millisecond)
		delivered.Add(1)
		return nil
	})

	for i := range segments {
		if err := pool.Submit(capture.Segment{Path: fmt.Sprintf("seg-%d.mp4", i)}); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}
	// At least one upload is still running when the window closes.
	<-started

	result := pool.close()
	if got := delivered.Load(); got != segments {
		t.Fatalf("delivered=%d want %d: window close dropped outstanding uploads", got, segments)
	}
	if result.err != nil || result.pending != 0 || result.submitted != segments || !result.ingested {
		t.Fatalf("result=%+v want no error, pending=0, submitted=%d", result, segments)
	}
}

// TestSegmentDeliveryPoolSubmitDoesNotBlockDuringOutage proves a stalled object
// store cannot pin the capture sweep past window cancellation. Only descriptors
// are queued here; the independent disk monitor bounds their media files.
func TestSegmentDeliveryPoolSubmitDoesNotBlockDuringOutage(t *testing.T) {
	const workers = 2
	const segments = 10_000
	release := make(chan struct{})
	var accepted atomic.Int64

	pool := startSegmentDeliveryPool(workers, func() {}, func(capture.Segment) error {
		<-release
		return nil
	})

	for range segments {
		if err := pool.Submit(capture.Segment{Path: "seg.mp4"}); err != nil {
			t.Fatalf("submit during outage: %v", err)
		}
		accepted.Add(1)
	}
	if got := accepted.Load(); got != segments {
		t.Fatalf("accepted=%d want %d: storage outage blocked capture", got, segments)
	}
	close(release)
	result := pool.close()
	if result.err != nil || result.pending != 0 || result.submitted != segments {
		t.Fatalf("result=%+v want all queued descriptors drained", result)
	}
}

// TestSegmentDeliveryPoolSurfacesFailureAndAbortsAttempt locks the error
// semantics: a worker goroutine must not swallow a delivery failure. It aborts the
// capture attempt, is returned by the next Submit (so CaptureContinuous still
// surfaces it to the supervisor's reconnect/backoff path), and is reported by
// close() with the failed segment still counted as unacknowledged.
func TestSegmentDeliveryPoolSurfacesFailureAndAbortsAttempt(t *testing.T) {
	ctx, abort := context.WithCancel(context.Background())
	defer abort()
	boom := fmt.Errorf("%w: upload segment: connection reset", errSegmentDelivery)

	pool := startSegmentDeliveryPool(1, abort, func(capture.Segment) error { return boom })
	if err := pool.Submit(capture.Segment{Path: "seg-0.mp4"}); err != nil {
		t.Fatalf("first submit: %v", err)
	}
	// fail() records the error before it aborts, so once the attempt context is
	// canceled the next Submit is guaranteed to report the failure.
	waitFor(t, "attempt aborted", func() bool { return ctx.Err() != nil })

	submitErr := pool.Submit(capture.Segment{Path: "seg-1.mp4"})
	if !errors.Is(submitErr, errSegmentDelivery) {
		t.Fatalf("submit error=%v want %v", submitErr, errSegmentDelivery)
	}

	result := pool.close()
	if !errors.Is(result.err, errSegmentDelivery) {
		t.Fatalf("result error=%v want %v", result.err, errSegmentDelivery)
	}
	if result.pending == 0 || result.ingested {
		t.Fatalf("result=%+v want an unacknowledged segment and no ingest", result)
	}
}

func TestSegmentDeliveryPoolTerminalFailureDrainsQueuedDescriptors(t *testing.T) {
	const segments = 10_000
	ctx, abort := context.WithCancel(context.Background())
	defer abort()
	started := make(chan struct{})
	releaseFailure := make(chan struct{})
	var deliveryCalls atomic.Int64

	pool := startSegmentDeliveryPool(1, abort, func(capture.Segment) error {
		if deliveryCalls.Add(1) == 1 {
			close(started)
			<-releaseFailure
			return fmt.Errorf("%w: object store unavailable", errSegmentDeliveryExhausted)
		}
		return nil
	})
	if err := pool.Submit(capture.Segment{Path: "seg-0.mp4"}); err != nil {
		t.Fatal(err)
	}
	<-started
	for i := 1; i < segments; i++ {
		if err := pool.Submit(capture.Segment{Path: fmt.Sprintf("seg-%d.mp4", i)}); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}
	close(releaseFailure)
	waitFor(t, "terminal failure abort", func() bool { return ctx.Err() != nil })

	result := pool.close()
	if !errors.Is(result.err, errSegmentDeliveryExhausted) {
		t.Fatalf("result error=%v want delivery exhaustion", result.err)
	}
	if got := deliveryCalls.Load(); got != 1 {
		t.Fatalf("delivery calls=%d want 1: queued descriptors delivered after terminal failure", got)
	}
	if result.submitted != segments || result.pending != segments || result.ingested {
		t.Fatalf("result=%+v want every unacknowledged descriptor preserved as pending", result)
	}
}

// TestJoinSegmentDeliveryError checks that an async delivery failure reaches the
// supervisor's classifier exactly once, whether or not CaptureContinuous already
// carried it out through Submit.
func TestJoinSegmentDeliveryError(t *testing.T) {
	delivery := fmt.Errorf("%w: ingest segment: boom", errSegmentDeliveryExhausted)
	carried := fmt.Errorf("continuous segment delivery failed: %w", delivery)

	if got := joinSegmentDeliveryError(nil, nil); got != nil {
		t.Fatalf("clean attempt error=%v want nil", got)
	}
	if got := joinSegmentDeliveryError(nil, delivery); !errors.Is(got, errSegmentDeliveryExhausted) {
		t.Fatalf("late delivery failure lost: %v", got)
	}
	if got := joinSegmentDeliveryError(carried, delivery); got.Error() != carried.Error() {
		t.Fatalf("already-carried failure duplicated: %v", got)
	}
	captureErr := errors.New("continuous ffmpeg exited: signal: interrupt")
	got := joinSegmentDeliveryError(captureErr, delivery)
	if !errors.Is(got, errSegmentDeliveryExhausted) || !errors.Is(got, captureErr) {
		t.Fatalf("joined error=%v want both causes", got)
	}
}

func TestContinuousProgressIsConcurrentAndMonotonic(t *testing.T) {
	base := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	progress := newContinuousProgress(base)
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			progress.mark(base.Add(time.Duration(i) * time.Second))
			_ = progress.last()
		}()
	}
	wg.Wait()
	if got := progress.last(); !got.Equal(base.Add(15 * time.Second)) {
		t.Fatalf("last progress=%s want %s", got, base.Add(15*time.Second))
	}
	progress.mark(base.Add(-time.Hour))
	if got := progress.last(); !got.Equal(base.Add(15 * time.Second)) {
		t.Fatalf("progress moved backwards: %s", got)
	}
}

func TestClampContinuousSegmentTimelineAcrossReconnects(t *testing.T) {
	priorEnd := time.Date(2026, 8, 3, 13, 10, 0, 0, time.UTC)

	// A replayed/DVR-backed attempt continues at the accepted media end.
	replayed := capture.Segment{
		StartAt:    priorEnd.Add(-2 * time.Minute),
		EndAt:      priorEnd.Add(-time.Minute),
		DurationMs: 60_000,
	}
	got := clampContinuousSegmentTimeline(replayed, priorEnd)
	if !got.StartAt.Equal(priorEnd) || !got.EndAt.Equal(priorEnd.Add(time.Minute)) {
		t.Fatalf("replayed segment=%+v want start=%s end=%s", got, priorEnd, priorEnd.Add(time.Minute))
	}

	// Actual reconnect downtime remains visible rather than being collapsed.
	afterGap := capture.Segment{
		StartAt:    priorEnd.Add(37 * time.Second),
		EndAt:      priorEnd.Add(97 * time.Second),
		DurationMs: 60_000,
	}
	got = clampContinuousSegmentTimeline(afterGap, priorEnd)
	if !got.StartAt.Equal(afterGap.StartAt) || !got.EndAt.Equal(afterGap.EndAt) {
		t.Fatalf("real reconnect gap changed: got=%+v want=%+v", got, afterGap)
	}

	unknownDuration := capture.Segment{
		StartAt: priorEnd.Add(-time.Minute),
		EndAt:   priorEnd.Add(-time.Minute),
	}
	got = clampContinuousSegmentTimeline(unknownDuration, priorEnd)
	if !got.StartAt.Equal(priorEnd) || !got.EndAt.Equal(priorEnd.Add(time.Millisecond)) {
		t.Fatalf("unknown-duration segment=%+v want distinct 1ms timeline key", got)
	}
}

func TestContinuousTimelineDelay(t *testing.T) {
	now := time.Date(2026, 8, 6, 20, 0, 0, 0, time.UTC)
	if got := continuousTimelineDelay(now.Add(4*time.Second), now); got != 0 {
		t.Fatalf("ordinary lead delay=%s want 0", got)
	}
	if got := continuousTimelineDelay(now.Add(2*time.Minute), now); got != 115*time.Second {
		t.Fatalf("future timeline delay=%s want 1m55s", got)
	}
	if got := continuousTimelineDelay(time.Time{}, now); got != 0 {
		t.Fatalf("zero end delay=%s want 0", got)
	}
}

func TestWaitForContinuousTimelineCancels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	now := time.Date(2026, 8, 6, 20, 0, 0, 0, time.UTC)
	started := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- waitForContinuousTimeline(ctx, now.Add(time.Hour), func() time.Time {
			close(started)
			return now
		})
	}()
	<-started
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("wait error=%v want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeline wait did not return promptly after cancellation")
	}
}

func TestUploadWorkersDefaultsToBoundedConcurrency(t *testing.T) {
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: "https://api.test", NodeToken: "test"})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := NewWorker(Config{Client: client})
	if err != nil {
		t.Fatal(err)
	}
	if worker.cfg.UploadWorkers != defaultUploadWorkers {
		t.Fatalf("upload workers=%d want %d", worker.cfg.UploadWorkers, defaultUploadWorkers)
	}
	worker, err = NewWorker(Config{Client: client, UploadWorkers: 2})
	if err != nil {
		t.Fatal(err)
	}
	if worker.cfg.UploadWorkers != 2 {
		t.Fatalf("configured upload workers=%d want 2", worker.cfg.UploadWorkers)
	}
}

func waitFor(t *testing.T, what string, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}
