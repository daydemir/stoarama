package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

// Every read the inverse job makes is safe to repeat: a LIST page, a multipart
// inventory page, the pre-DELETE identity HEAD and the confirming HEAD. So one
// bounded retry policy covers all of them, and one limiter, owned by the job's
// single store, is shared by every worker and every read type. A 429 (or any
// transient server status, which includes 503) fences every new read: no reader
// leaves until the cooldown ends, so the job never re-creates the herd that
// caused it. The DELETE is not a read and is never retried.
const (
	// inverseReadAttempts is the capped number of tries for one read.
	inverseReadAttempts = 4
	// inverseReadMaxBackoff bounds the jittered wait between tries.
	inverseReadMaxBackoff = 2 * time.Second
	// inverseReadMaxRetryAfter bounds a provider's Retry-After. A provider
	// asking for longer stops that read cleanly: the retry is never sent early.
	inverseReadMaxRetryAfter = 30 * time.Second
	// inverseReadPageTimeout bounds one LIST page attempt when the command has no
	// deadline of its own. It caps a page, never the whole bucket listing.
	inverseReadPageTimeout = 60 * time.Second
	// inverseReadHerdJitter staggers readers released by one shared cooldown, so
	// they do not retry in the same millisecond.
	inverseReadHerdJitter = 250 * time.Millisecond
)

// inverseReadPolicy is one read's own bounds. A zero attempt timeout or total
// timeout means the caller's context supplies it, which is how the pre-DELETE
// HEAD keeps the wire's existing 30-second (or custody) budget.
type inverseReadPolicy struct {
	attempts       int
	attemptTimeout time.Duration
	maxBackoff     time.Duration
	timeout        time.Duration
	maxRetryAfter  time.Duration
}

// inverseConfirmReadPolicy keeps the confirming HEAD's reviewed #577 bounds: 4
// attempts of at most 4s, full-jitter backoff of at most 2s, and its own 22s
// deadline inside the caller's network and custody bounds.
var inverseConfirmReadPolicy = inverseReadPolicy{
	attempts:       inverseConfirmAttempts,
	attemptTimeout: inverseConfirmAttemptTimeout,
	maxBackoff:     inverseConfirmMaxBackoff,
	timeout:        inverseConfirmTimeout,
	maxRetryAfter:  inverseReadMaxRetryAfter,
}

// inverseListReadPolicy retries one LIST page, keeping its ContinuationToken, so
// a 429 never restarts the bucket listing from the first page. Each attempt is
// finite even with no command deadline; the listing itself is not.
var inverseListReadPolicy = inverseReadPolicy{
	attempts:       inverseReadAttempts,
	attemptTimeout: inverseReadPageTimeout,
	maxBackoff:     inverseReadMaxBackoff,
	maxRetryAfter:  inverseReadMaxRetryAfter,
}

// inverseHeadReadPolicy retries the pre-DELETE HEAD under the wire's own
// existing deadline: it never adds a bound. It is the only read that runs while
// the key's shared protection locks are held. That tradeoff is deliberate: the
// HEAD is the exact identity/LastModified authority for the DELETE, so it must
// stay inside the wire transaction; it holds only shared locks, for at most the
// network budget that already bounded it, and never extends it.
var inverseHeadReadPolicy = inverseReadPolicy{
	attempts:      inverseReadAttempts,
	maxBackoff:    inverseReadMaxBackoff,
	maxRetryAfter: inverseReadMaxRetryAfter,
}

// inverseReadGate is the job-wide read fence. One store holds it, so every
// worker and every read type waits for the same instant; extending the cooldown
// wakes everyone to recompute it.
type inverseReadGate struct {
	mu      sync.Mutex
	until   time.Time
	closed  bool
	changed chan struct{}
}

func newInverseReadGate() *inverseReadGate {
	return &inverseReadGate{changed: make(chan struct{})}
}

// errInverseReadFenced reports a read refused because the job fenced its reads
// for longer than any retry this process could usefully wait. Nothing was sent.
var errInverseReadFenced = errors.New("inverse reads fenced: provider asked for a cooldown past the retry cap")

// close latches the gate shut. A provider asking for a cooldown past the retry
// cap is not a 30-second problem: releasing a worker that is deep in a LIST or a
// long read at the cap would walk it straight back into the throttle, so the job
// stops issuing reads instead. A new process builds a new gate.
func (g *inverseReadGate) close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return
	}
	g.closed = true
	close(g.changed)
	g.changed = make(chan struct{})
}

// pause holds every new read until d from now, extending an existing cooldown
// but never shortening it.
func (g *inverseReadGate) pause(d time.Duration) {
	if d <= 0 {
		return
	}
	until := time.Now().Add(d)
	g.mu.Lock()
	defer g.mu.Unlock()
	if !until.After(g.until) {
		return
	}
	g.until = until
	close(g.changed)
	g.changed = make(chan struct{})
}

// untilNow returns the current cooldown, whether the gate is latched shut, and
// the channel that closes when either changes.
func (g *inverseReadGate) untilNow() (time.Time, bool, chan struct{}) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.until, g.closed, g.changed
}

// remaining reports how long every new read must still wait, and whether the
// gate is latched shut.
func (g *inverseReadGate) remaining() (time.Duration, bool) {
	until, closed, _ := g.untilNow()
	return time.Until(until), closed
}

// wait blocks until the shared cooldown has passed, or the context ends, or the
// gate latches shut. Every reader calls it before every attempt, so a 429 seen by
// one key stops the rest. A reader that waited on a cooldown leaves staggered, and
// every wake re-reads the gate: a sibling that extends the cooldown during this
// wait extends this wait too, whatever a ready timer suggested.
func (g *inverseReadGate) wait(ctx context.Context) error {
	waited, staggered := false, false
	for {
		remaining, closed := g.remaining()
		if closed {
			return errInverseReadFenced
		}
		if remaining <= 0 {
			if !waited || staggered {
				return nil
			}
			// Readers released by one cooldown must not all retry on the same
			// millisecond: each takes its own stagger, then rechecks.
			staggered = true
			if nap := rand.N(inverseReadHerdJitter); nap > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(nap):
				}
			}
			continue
		}
		waited, staggered = true, false
		// Nap in short slices and wake on any extension, then loop and read the
		// gate again rather than trusting this timer.
		_, _, changed := g.untilNow()
		nap := min(remaining, inverseReadHerdJitter) + rand.N(inverseReadHerdJitter)
		timer := time.NewTimer(nap)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-changed:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// readGate returns the store's shared limiter, creating it once. Real stores get
// one from their constructor; this keeps a zero store safe for tests.
func (s *inverseSDKStore) readGate() *inverseReadGate {
	s.readsMu.Lock()
	defer s.readsMu.Unlock()
	if s.reads == nil {
		s.reads = newInverseReadGate()
	}
	return s.reads
}

// read runs one safe read under the shared limiter. It retries only what the
// confirming HEAD already classifies as transient, caps the tries, honors a
// provider's Retry-After (delta seconds or an HTTP date, and R2's
// millisecond X-Amz-Retry-After), and never retries earlier than that: a floor
// past what is left of this read's budget stops this read cleanly, and a floor
// past the retry cap fences the whole job. A clean stop is an ordinary error, so
// the job takes its existing abort path.
func (s *inverseSDKStore) read(ctx context.Context, p inverseReadPolicy, what string, op func(context.Context) error) error {
	gate := s.readGate()
	if p.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.timeout)
		defer cancel()
	}
	fail := func(tries int, err error) error {
		return fmt.Errorf("%s after %d attempts: %w", what, tries, err)
	}
	for tries := 1; ; tries++ {
		if err := gate.wait(ctx); err != nil {
			return fail(tries, err)
		}
		// Every attempt carries its own deadline, so no attempt outlives the
		// policy even when the command has no deadline of its own.
		attemptCtx, cancel := ctx, context.CancelFunc(func() {})
		if p.attemptTimeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, p.attemptTimeout)
		}
		err := op(attemptCtx)
		ownTimeout := attemptCtx.Err() != nil && ctx.Err() == nil
		cancel()
		if err == nil {
			return nil
		}
		terminal := tries == p.attempts
		if !terminal && inverseConfirmRetryable.IsErrorRetryable(err) != aws.TrueTernary && !ownTimeout {
			return fail(tries, err)
		}
		wait, fence, latch, stop := inverseReadWait(ctx, p, err)
		// A throttle fences the job even when this reader stops here, so a
		// terminal 429 or a long floor cannot leave the next read unfenced.
		switch {
		case latch:
			gate.close()
		case fence:
			gate.pause(wait)
		}
		if stop != nil {
			return fail(tries, stop)
		}
		if terminal {
			return fail(tries, err)
		}
		if !fence {
			// Any other transient read failure waits only for its own key.
			select {
			case <-ctx.Done():
				return fail(tries, err)
			case <-time.After(wait):
			}
		}
	}
}

// inverseReadWait decides how long a failed read waits, whether that wait fences
// the job, and why it must stop instead. A Retry-After is clamped to no more than
// 24h so a hostile header cannot overflow a Duration. A floor past the retry cap
// does not degrade to the cap — a worker deep in a LIST would be released into
// the same throttle — it latches the gate shut, so the job issues no further
// reads. A floor past what is left of this read's budget still fences the job for
// the provider's real ask and stops only this read.
func inverseReadWait(ctx context.Context, p inverseReadPolicy, err error) (time.Duration, bool, bool, error) {
	throttle := inverseReadStatus(err) == http.StatusTooManyRequests
	// A throttle always fences the job for a real interval, even without a
	// header and even when a reader stops: half to full backoff.
	wait := rand.N(p.maxBackoff)
	if throttle {
		wait = p.maxBackoff/2 + rand.N(p.maxBackoff/2)
	}
	retryAfter, ok := inverseReadRetryAfter(err)
	if !ok {
		return wait, throttle, false, nil
	}
	switch {
	case retryAfter > p.maxRetryAfter:
		// Latch, never release at the cap: the provider's floor is longer than
		// any worker can usefully wait here.
		return 0, true, true, fmt.Errorf("%w: provider asked for %v, past the %v retry cap; reads fenced", err, retryAfter, p.maxRetryAfter)
	case retryAfter <= 0:
		return wait, true, false, nil
	}
	if deadline, ok := ctx.Deadline(); ok && retryAfter >= time.Until(deadline) {
		return retryAfter, true, false, fmt.Errorf("%w: provider asked for %v, past the %v left in this read's budget", err, retryAfter, time.Until(deadline).Round(time.Millisecond))
	}
	return retryAfter, true, false, nil
}

// inverseReadStatus reports an HTTP status carried by a read error, or 0.
func inverseReadStatus(err error) int {
	var responseErr *awshttp.ResponseError
	if errors.As(err, &responseErr) {
		return responseErr.HTTPStatusCode()
	}
	var statusErr interface{ HTTPStatusCode() int }
	if errors.As(err, &statusErr) {
		return statusErr.HTTPStatusCode()
	}
	return 0
}

// inverseReadRetryAfter reads the provider's own floor for the next try.
// Retry-After is delta seconds or an HTTP date; R2 answers a throttled request
// with X-Amz-Retry-After in milliseconds. Every value is clamped so the parse
// itself can never overflow a Duration.
func inverseReadRetryAfter(err error) (time.Duration, bool) {
	var responseErr *awshttp.ResponseError
	if !errors.As(err, &responseErr) || responseErr.Response == nil || responseErr.Response.Response == nil {
		return 0, false
	}
	header := responseErr.Response.Header
	const maxSeconds = int64(24 * 60 * 60)
	if raw := strings.TrimSpace(header.Get("Retry-After")); raw != "" {
		if seconds, parseErr := strconv.ParseInt(raw, 10, 64); parseErr == nil {
			if seconds <= 0 {
				return 0, true
			}
			return time.Duration(min(seconds, maxSeconds)) * time.Second, true
		}
		if at, parseErr := http.ParseTime(raw); parseErr == nil {
			wait := time.Until(at)
			if wait <= 0 {
				return 0, true
			}
			return min(wait, 24*time.Hour), true
		}
	}
	if raw := strings.TrimSpace(header.Get("X-Amz-Retry-After")); raw != "" {
		if millis, parseErr := strconv.ParseInt(raw, 10, 64); parseErr == nil {
			if millis <= 0 {
				return 0, true
			}
			// Saturate before multiplying: millis can be near int64 max.
			if millis > int64(math.MaxInt64/time.Millisecond) {
				return 24 * time.Hour, true
			}
			return min(time.Duration(millis)*time.Millisecond, 24*time.Hour), true
		}
	}
	return 0, false
}

// readOptions binds the job's own transport and a nop retryer, so each attempt
// is exactly one request and the executor's caps are the real caps.
func (s *inverseSDKStore) readOptions() r2.ReadOptions {
	return r2.ReadOptions{HTTPClient: s.readClient, Retryer: aws.NopRetryer{}}
}

// newInverseReadClient is the read transport for the job's own LIST pages,
// multipart inventory and identity HEAD. It is the confirming HEAD's client with
// the same guarantees: one request per attempt on its own fresh HTTP/1.1
// connection, keep-alives off, HTTP/2 refused, and every redirect refused, so a
// read is exactly the physical attempts the policy counts and a published
// cooldown is never followed by a stray extra request.
func newInverseReadClient(attempt time.Duration) *http.Client {
	transport := awshttp.NewBuildableClient().WithTransportOptions(func(t *http.Transport) {
		t.ForceAttemptHTTP2 = false
		t.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
		t.Protocols = new(http.Protocols)
		t.Protocols.SetHTTP1(true)
		t.TLSClientConfig = t.TLSClientConfig.Clone()
		t.TLSClientConfig.NextProtos = []string{"http/1.1"}
		t.DisableKeepAlives = true
	}).GetTransport()
	return &http.Client{Transport: transport, Timeout: attempt, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

// Head reads one key's exact provider identity through the bounded read path,
// reusing r2.Client's normalization. The pre-DELETE guard and the restart probe
// both use it; the DELETE's own single-attempt authority is untouched.
func (s *inverseSDKStore) Head(ctx context.Context, key string) (r2.ObjectHead, error) {
	var head r2.ObjectHead
	err := s.read(ctx, inverseHeadReadPolicy, "head "+key, func(ctx context.Context) error {
		out, err := s.Client.HeadWithOptions(ctx, key, s.readOptions())
		head = out
		return err
	})
	return head, err
}

// ListPrefix lists the bucket page by page. A page that meets a transient status
// is retried with the same ContinuationToken, so the listing continues from where
// it stopped instead of relisting what it already classified.
func (s *inverseSDKStore) ListPrefix(ctx context.Context, prefix string, limit int, fn func(r2.ObjectInfo) error) error {
	return s.listPrefix(ctx, inverseListReadPolicy, prefix, limit, fn)
}

// listPrefix runs the shared LIST loop with one page's bounded read policy, so
// tests can run it at a smaller bound and production keeps the reviewed one.
func (s *inverseSDKStore) listPrefix(ctx context.Context, p inverseReadPolicy, prefix string, limit int, fn func(r2.ObjectInfo) error) error {
	page := 0
	return s.Client.ListPrefixWithPage(ctx, prefix, limit, func(ctx context.Context, in *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
		page++
		var out *s3.ListObjectsV2Output
		err := s.read(ctx, p, fmt.Sprintf("list %q page %d", prefix, page), func(ctx context.Context) error {
			got, err := s.Client.ListObjectsPage(ctx, in, s.readOptions())
			out = got
			return err
		})
		return out, err
	}, fn)
}

// RequireNoMultipartOutsideStitchStaging inventories multipart uploads page by
// page with the same exact eligibility and continuation validation as r2.Client,
// under the job's read policy and limiter.
func (s *inverseSDKStore) RequireNoMultipartOutsideStitchStaging(ctx context.Context, known func(context.Context, string, string) (bool, error)) error {
	page := 0
	return s.Client.RequireNoMultipartOutsideStitchStagingPages(ctx, func(ctx context.Context, in *s3.ListMultipartUploadsInput) (*s3.ListMultipartUploadsOutput, error) {
		page++
		var out *s3.ListMultipartUploadsOutput
		err := s.read(ctx, inverseListReadPolicy, fmt.Sprintf("multipart inventory page %d", page), func(ctx context.Context) error {
			got, err := s.Client.ListMultipartUploadsPage(ctx, in, s.readOptions())
			out = got
			return err
		})
		return out, err
	}, known)
}
