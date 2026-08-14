package recordingworker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
)

// continuousProgress is a continuous job's "last successfully ingested segment"
// clock (the value the no-progress surrender and the bounded reconnect delay read).
// Segment delivery no longer runs on the job goroutine, so this can no longer be a
// plain variable owned by it: every read and write goes through the mutex.
type continuousProgress struct {
	mu sync.Mutex
	at time.Time
}

func newContinuousProgress(at time.Time) *continuousProgress {
	return &continuousProgress{at: at}
}

func (p *continuousProgress) mark(at time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if at.After(p.at) {
		p.at = at
	}
}

func (p *continuousProgress) last() time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.at
}

// segmentDeliveryResult is one capture attempt's delivery outcome, read by the
// job goroutine only after close() has joined every worker.
type segmentDeliveryResult struct {
	// err is the FIRST delivery error any worker saw (nil when every accepted
	// segment was ingested). It carries the same sentinels the inline delivery
	// path produced, so the supervisor classifies it exactly as before.
	err error
	// submitted counts the segments CaptureContinuous handed over this attempt.
	submitted int
	// pending counts accepted segments that were never acknowledged, i.e. the
	// "unacknowledged segment" condition the window-close and temp-dir cleanup
	// decisions key on.
	pending int
	// ingested reports whether at least one unique segment was ingested this
	// attempt (the supervisor's healthy-connection signal that resets reconnect
	// backoff). Acknowledged replays do not set it.
	ingested bool
}

// segmentDeliveryPool delivers the finalized segments of ONE continuous capture
// attempt with bounded concurrency.
//
// Delivery used to be strictly serial per job: capture's sweep loop called
// onSegment, which blocked on reserve -> PUT -> ingest before the next segment
// could even be finalized. That capped a single job's sustained throughput at
// roughly one segment per reserve+upload+ingest round trip, so a stream whose
// bitrate exceeded that ceiling built a backlog that never drained.
//
// The pool keeps the ordered half of delivery unchanged: CaptureContinuous still
// calls Submit synchronously, in order, from the same sweep loop, so the media
// timeline chaining in capture.deliverContinuousSegment (which stamps StartAt and
// EndAt and advances nextStart before the segment is handed over) still runs on one
// goroutine and cannot be reordered. Only the reserve -> PUT -> ingest flow moves
// off that goroutine, onto a fixed set of workers.
//
// Finalized media stays in the attempt directory until delivery is acknowledged.
// Submit queues only small descriptors, but pauses at the exact producer seal
// limit. The server ledger and local spool therefore share one hard outstanding
// bound; the disk monitor remains a second host-wide safety fence while close
// drains every descriptor already accepted.
type segmentDeliveryPool struct {
	wg      sync.WaitGroup
	deliver func(capture.Segment) error
	// abort cancels the capture attempt on the first delivery failure, so a dead
	// uplink stops ffmpeg as promptly as the inline path did instead of waiting
	// for the next finalized segment to observe the error.
	abort context.CancelFunc

	mu           sync.Mutex
	ready        *sync.Cond
	queue        []capture.Segment
	err          error
	submitted    int
	inFlight     int
	maxInFlight  int
	ingested     bool
	closed       bool
	onQueueDepth func(int)
}

// startSegmentDeliveryPool starts worker goroutines draining the on-disk spool's
// descriptor queue. deliver performs one segment's reserve -> PUT -> ingest and
// must be safe to call from several goroutines at once.
func startSegmentDeliveryPool(workers int, abort context.CancelFunc, deliver func(capture.Segment) error, queueObserver ...func(int)) *segmentDeliveryPool {
	if workers <= 0 {
		workers = 1
	}
	sealedIntentLimit := workers
	if sealedIntentLimit > 8 {
		sealedIntentLimit = 8
	}
	p := &segmentDeliveryPool{
		deliver:     deliver,
		abort:       abort,
		maxInFlight: sealedIntentLimit,
	}
	if len(queueObserver) > 0 {
		p.onQueueDepth = queueObserver[0]
	}
	p.ready = sync.NewCond(&p.mu)
	p.wg.Add(workers)
	for range workers {
		go p.run()
	}
	return p
}

func (p *segmentDeliveryPool) run() {
	defer p.wg.Done()
	for {
		seg, ok := p.take()
		if !ok {
			return
		}
		if p.failed() {
			// A previous delivery already failed this attempt. Drain descriptors
			// without uploading; their media remains on disk and they stay counted
			// as pending for the supervisor's cleanup/retry decision.
			continue
		}
		if err := p.deliver(seg); errors.Is(err, errSegmentReplayAcknowledged) {
			p.acknowledge(false)
			continue
		} else if err != nil {
			p.fail(err)
			continue
		}
		p.acknowledge(true)
	}
}

func (p *segmentDeliveryPool) take() (capture.Segment, bool) {
	p.mu.Lock()
	for len(p.queue) == 0 && !p.closed {
		p.ready.Wait()
	}
	if len(p.queue) == 0 {
		p.mu.Unlock()
		return capture.Segment{}, false
	}
	seg := p.queue[0]
	p.queue[0] = capture.Segment{}
	p.queue = p.queue[1:]
	depth := len(p.queue)
	if p.onQueueDepth != nil {
		p.onQueueDepth(depth)
	}
	p.mu.Unlock()
	return seg, true
}

// Submit is the onSegment callback handed to CaptureContinuous. It returns the
// first delivery error seen by any worker, so an upload failure still surfaces
// through CaptureContinuous and still drives the supervisor's reconnect/backoff
// path rather than being swallowed by a worker goroutine.
//
// It is called only from the capture goroutine and queues no media bytes. The
// hard outstanding bound matches the capture producer's server-side sealed
// intent limit. When all delivery slots are occupied, backpressure pauses the
// capture callback until an upload is acknowledged or the attempt fails.
func (p *segmentDeliveryPool) Submit(seg capture.Segment) error {
	p.mu.Lock()
	for p.err == nil && !p.closed && p.inFlight >= p.maxInFlight {
		p.ready.Wait()
	}
	if p.err != nil {
		p.mu.Unlock()
		return p.err
	}
	if p.closed {
		p.mu.Unlock()
		return context.Canceled
	}
	p.submitted++
	p.inFlight++
	seg.DeliveryQueuedAt = time.Now().UTC()
	p.queue = append(p.queue, seg)
	depth := len(p.queue)
	p.ready.Signal()
	if p.onQueueDepth != nil {
		p.onQueueDepth(depth)
	}
	p.mu.Unlock()
	return nil
}

func (p *segmentDeliveryPool) failed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err != nil
}

func (p *segmentDeliveryPool) fail(err error) {
	p.mu.Lock()
	if p.err == nil {
		p.err = err
	}
	p.ready.Broadcast()
	p.mu.Unlock()
	if p.abort != nil {
		p.abort()
	}
}

func (p *segmentDeliveryPool) acknowledge(uniqueIngest bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inFlight--
	p.ready.Broadcast()
	if uniqueIngest {
		p.ingested = true
	}
}

// waitIdle pauses a source-mutation stop barrier until every descriptor that
// was accepted before SIGSTOP has reached a terminal delivery outcome. It does
// not close the pool: the final, namespace-isolated FFmpeg leaf is submitted
// only after the stopped process has been interrupted, continued, and reaped.
func (p *segmentDeliveryPool) waitIdle(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		p.mu.Lock()
		for p.inFlight > 0 && p.err == nil && !p.closed {
			p.ready.Wait()
		}
		p.mu.Unlock()
		close(done)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.err
	}
}

// close stops accepting segments, waits for every outstanding upload to finish,
// and reports the attempt's delivery outcome. The continuous job MUST call it
// after CaptureContinuous returns and BEFORE it decides the attempt's fate
// (temp-dir cleanup, reconnect versus complete), so window close joins the
// in-flight uploads instead of moving clip loss to the boundary.
func (p *segmentDeliveryPool) close() segmentDeliveryResult {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		p.ready.Broadcast()
	}
	p.mu.Unlock()
	p.wg.Wait()
	p.mu.Lock()
	defer p.mu.Unlock()
	return segmentDeliveryResult{
		err:       p.err,
		submitted: p.submitted,
		pending:   p.inFlight,
		ingested:  p.ingested,
	}
}
