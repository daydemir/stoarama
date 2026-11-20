package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

// readResponseError carries a provider's status and headers, as the SDK's HTTP
// response error does.
func readResponseError(header http.Header) error {
	response := &http.Response{StatusCode: http.StatusTooManyRequests, Header: header, Body: io.NopCloser(strings.NewReader(""))}
	return &awshttp.ResponseError{ResponseError: &smithyhttp.ResponseError{Response: &smithyhttp.Response{Response: response}, Err: errors.New("throttled")}}
}

// readScriptServer answers each read from a script keyed by request kind, so a
// test can count exact network attempts per page or per key.
type readScriptServer struct {
	mu       sync.Mutex
	requests []string
	answer   func(kind, marker string, attempt int) (status int, header http.Header)
}

func (s *readScriptServer) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.requests)
}

func (s *readScriptServer) attempts(kind, marker string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, r := range s.requests {
		if r == kind+" "+marker {
			n++
		}
	}
	return n
}

func newReadScriptStore(t *testing.T, script *readScriptServer) *inverseSDKStore {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		kind, marker := "", ""
		switch {
		case req.Method == http.MethodGet && req.URL.Query().Has("uploads"):
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprint(w, `<ListMultipartUploadsResult><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>`)
			return
		case req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/stoarama"):
			kind, marker = "list", req.URL.Query().Get("continuation-token")
			if marker == "" {
				marker = "page1"
			}
		case req.Method == http.MethodHead:
			kind, marker = "head", strings.TrimPrefix(req.URL.Path, "/stoarama/")
		default:
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		script.mu.Lock()
		script.requests = append(script.requests, kind+" "+marker)
		attempt := 0
		for _, r := range script.requests {
			if r == kind+" "+marker {
				attempt++
			}
		}
		answer := script.answer
		script.mu.Unlock()
		status, header := http.StatusOK, http.Header{}
		if answer != nil {
			status, header = answer(kind, marker, attempt)
		}
		for k, v := range header {
			w.Header()[k] = v
		}
		if status != http.StatusOK {
			w.WriteHeader(status)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		if kind == "list" {
			if marker == "page1" {
				fmt.Fprint(w, `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>page2</NextContinuationToken>`+
					`<Contents><Key>managed/a.mp4</Key><ETag>"e1"</ETag><Size>10</Size><LastModified>2026-01-01T00:00:00.000Z</LastModified></Contents></ListBucketResult>`)
				return
			}
			fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated>`+
				`<Contents><Key>managed/b.mp4</Key><ETag>"e2"</ETag><Size>20</Size><LastModified>2026-01-01T00:00:00.000Z</LastModified></Contents></ListBucketResult>`)
			return
		}
		w.Header().Set("ETag", `"etag"`)
		w.Header().Set("Content-Length", "100")
		w.Header().Set("Last-Modified", "Thu, 01 Jan 2026 00:00:00 GMT")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	store, err := newInverseSDKStore(context.Background(), r2.Config{Bucket: "stoarama", Endpoint: server.URL, AccessKey: "test", SecretKey: "test", Region: "auto"})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

// A throttled LIST page is retried with its own ContinuationToken: the listing
// continues where it stopped, never from the first page.
func TestR2InverseListRetriesOnePageKeepingItsToken(t *testing.T) {
	script := &readScriptServer{answer: func(kind, marker string, attempt int) (int, http.Header) {
		if attempt == 1 {
			switch marker {
			case "page1":
				return http.StatusTooManyRequests, http.Header{"Retry-After": []string{"0"}}
			case "page2":
				return http.StatusServiceUnavailable, nil
			}
		}
		return http.StatusOK, nil
	}}
	store := newReadScriptStore(t, script)
	var keys []string
	if err := store.ListPrefix(context.Background(), "", 0, func(o r2.ObjectInfo) error {
		keys = append(keys, o.Key)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if strings.Join(keys, ",") != "managed/a.mp4,managed/b.mp4" {
		t.Fatalf("keys %v: a retried page relisted or skipped", keys)
	}
	if script.attempts("list", "page1") != 2 || script.attempts("list", "page2") != 2 {
		t.Fatalf("page attempts %d/%d, want one retry per page", script.attempts("list", "page1"), script.attempts("list", "page2"))
	}
}

// The partition is a real entry point: a 429 on the first page and a 503 on the
// second still produce the whole bucket once, each page retried in place.
func TestR2InversePartitionSurvivesThrottledPages(t *testing.T) {
	script := &readScriptServer{answer: func(kind, marker string, attempt int) (int, http.Header) {
		if attempt == 1 {
			if marker == "page1" {
				return http.StatusTooManyRequests, http.Header{"Retry-After": []string{"0"}}
			}
			return http.StatusServiceUnavailable, nil
		}
		return http.StatusOK, nil
	}}
	store := newReadScriptStore(t, script)
	var out bytes.Buffer
	sum, err := partitionInverse(context.Background(), store, inverseTestProtection(), "policy", &out)
	if err != nil {
		t.Fatal(err)
	}
	if sum.Total.Objects != 2 || sum.Total.Bytes != 30 || strings.Count(out.String(), "\n") != 2 {
		t.Fatalf("partition %+v over %d entries", sum.Total, strings.Count(out.String(), "\n"))
	}
	if script.attempts("list", "page1") != 2 || script.attempts("list", "page2") != 2 {
		t.Fatalf("page attempts %d/%d", script.attempts("list", "page1"), script.attempts("list", "page2"))
	}
}

// The identity HEAD before a DELETE retries a 429 and a 503 through the same
// bounded read path, and reports the provider's identity once it answers.
func TestR2InverseHeadRetriesThrottleAndUnavailable(t *testing.T) {
	script := &readScriptServer{answer: func(kind, marker string, attempt int) (int, http.Header) {
		switch attempt {
		case 1:
			return http.StatusTooManyRequests, http.Header{"Retry-After": []string{"0"}}
		case 2:
			return http.StatusServiceUnavailable, nil
		}
		return http.StatusOK, nil
	}}
	store := newReadScriptStore(t, script)
	head, err := store.Head(context.Background(), "managed/a.mp4")
	if err != nil {
		t.Fatal(err)
	}
	if head.ETag != "etag" || head.SizeBytes != 100 || script.attempts("head", "managed/a.mp4") != 3 {
		t.Fatalf("head %+v after %d attempts", head, script.attempts("head", "managed/a.mp4"))
	}
}

// A read that is throttled past its capped tries stops cleanly after exactly
// that many network attempts: nothing beyond the tries is ever sent.
func TestR2InverseReadStopsAfterCappedThrottledTries(t *testing.T) {
	script := &readScriptServer{answer: func(kind, marker string, attempt int) (int, http.Header) {
		return http.StatusTooManyRequests, http.Header{"Retry-After": []string{"0"}}
	}}
	store := newReadScriptStore(t, script)
	err := store.ListPrefix(context.Background(), "", 0, func(r2.ObjectInfo) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "after 4 attempts") {
		t.Fatalf("throttled forever: %v", err)
	}
	if got := script.count(); got != inverseReadAttempts {
		t.Fatalf("%d network attempts, want exactly %d", got, inverseReadAttempts)
	}
}

// Retry-After is delta seconds or an HTTP date; R2's throttle header is
// milliseconds. A value past the read's cap stops it instead of retrying early.
func TestR2InverseRetryAfterParsingAndCaps(t *testing.T) {
	for name, tc := range map[string]struct {
		header http.Header
		want   time.Duration
		ok     bool
	}{
		"delta seconds": {http.Header{"Retry-After": []string{"3"}}, 3 * time.Second, true},
		"http date":     {http.Header{"Retry-After": []string{time.Now().Add(4 * time.Second).UTC().Format(http.TimeFormat)}}, 4 * time.Second, true},
		"amz millis":    {http.Header{"X-Amz-Retry-After": []string{"1500"}}, 1500 * time.Millisecond, true},
		"invalid":       {http.Header{"Retry-After": []string{"later"}}, 0, false},
		"absent":        {http.Header{}, 0, false},
	} {
		t.Run(name, func(t *testing.T) {
			err := readResponseError(tc.header)
			got, ok := inverseReadRetryAfter(err)
			if ok != tc.ok {
				t.Fatalf("parsed %v, want ok=%v", got, tc.ok)
			}
			if ok && name != "http date" && got != tc.want {
				t.Fatalf("parsed %v, want %v", got, tc.want)
			}
			if ok && (got <= 0 || got > tc.want+time.Second) {
				t.Fatalf("parsed %v, want about %v", got, tc.want)
			}
		})
	}
}

// A Retry-After past the cap, and one past what is left of the read's budget,
// both stop cleanly on the first try: the retry is never sent early.
func TestR2InverseRetryAfterBeyondBudgetStopsCleanly(t *testing.T) {
	for name, header := range map[string]http.Header{
		"past the cap":   {"Retry-After": []string{strconv.Itoa(int(inverseReadMaxRetryAfter/time.Second) + 60)}},
		"past the bound": {"Retry-After": []string{"5"}},
	} {
		t.Run(name, func(t *testing.T) {
			script := &readScriptServer{answer: func(kind, marker string, attempt int) (int, http.Header) {
				return http.StatusTooManyRequests, header
			}}
			store := newReadScriptStore(t, script)
			policy := inverseHeadReadPolicy
			if name == "past the bound" {
				policy.timeout = 2 * time.Second
			}
			started := time.Now()
			err := store.read(context.Background(), policy, "head managed/a.mp4", func(ctx context.Context) error {
				_, err := store.Client.HeadWithOptions(ctx, "managed/a.mp4", store.readOptions())
				return err
			})
			if err == nil || !strings.Contains(err.Error(), "after 1 attempts") || script.count() != 1 {
				t.Fatalf("stopped read: %v after %d attempts", err, script.count())
			}
			if elapsed := time.Since(started); elapsed > 2*time.Second {
				t.Fatalf("waited %v for a retry it must never send", elapsed)
			}
		})
	}
}

// One worker's 429 sets a cooldown every other reader honors, so they leave
// together instead of each re-discovering the throttle.
func TestR2InverseReadGateBacksOffEveryReader(t *testing.T) {
	gate := newInverseReadGate()
	const readers = 8
	var released sync.WaitGroup
	at := make([]time.Time, readers)
	released.Add(readers)
	start := make(chan struct{})
	for i := range readers {
		go func() {
			defer released.Done()
			<-start
			if err := gate.wait(context.Background()); err != nil {
				t.Error(err)
				return
			}
			at[i] = time.Now()
		}()
	}
	begin := time.Now()
	gate.pause(300 * time.Millisecond)
	close(start)
	released.Wait()
	earliest, latest := at[0], at[0]
	for _, t0 := range at {
		if t0.Before(earliest) {
			earliest = t0
		}
		if t0.After(latest) {
			latest = t0
		}
	}
	if waited := earliest.Sub(begin); waited < 250*time.Millisecond {
		t.Fatalf("a reader left after %v, inside the 300ms cooldown", waited)
	}
	// Every reader obeys the shared cooldown, and their release is staggered
	// inside one jitter window rather than all waking on the same millisecond.
	if latest.Sub(earliest) > 2*inverseReadHerdJitter {
		t.Fatalf("readers spread over %v, beyond one stagger window", latest.Sub(earliest))
	}
}

// A cooldown set by one read type holds the others: an identity HEAD that a
// throttle just backed off keeps a LIST page, run by another worker, waiting for
// the same instant instead of each key discovering the outage itself.
func TestR2InverseCooldownIsSharedAcrossReadTypes(t *testing.T) {
	store := newReadScriptStore(t, &readScriptServer{})
	throttle := readResponseError(http.Header{"Retry-After": []string{"1"}})
	exhausted := make(chan struct{})
	go func() {
		defer close(exhausted)
		// One identity HEAD meets a 429 that asks for a second, sees the
		// throttle once, then succeeds.
		attempts := atomic.Int64{}
		_ = store.read(context.Background(), inverseHeadReadPolicy, "head a", func(ctx context.Context) error {
			if attempts.Add(1) == 1 {
				return throttle
			}
			return nil
		})
	}()
	time.Sleep(100 * time.Millisecond)
	// A different read type, from another worker, leaves only after the gate.
	begin := time.Now()
	var at time.Time
	if err := store.read(context.Background(), inverseListReadPolicy, "list", func(ctx context.Context) error {
		at = time.Now()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	<-exhausted
	if at.Sub(begin) < 700*time.Millisecond {
		t.Fatalf("the LIST page left after %v, inside the shared cooldown", at.Sub(begin))
	}
}

// A cooldown extension that lands while a reader sleeps still holds it: a stale
// timer is not proof the cooldown ended. Eight readers wait on one cooldown; a
// ninth keeps extending it, and every reader must still be inside the final
// cooldown when it finally leaves.
func TestR2InverseReadGateRechecksExtensionOnTimerWake(t *testing.T) {
	gate := newInverseReadGate()
	const readers = 8
	var wg sync.WaitGroup
	var mu sync.Mutex
	left := []time.Time{}
	// The cooldown exists before any reader starts, so every one of them waits.
	gate.pause(80 * time.Millisecond)
	wg.Add(readers)
	for range readers {
		go func() {
			defer wg.Done()
			if err := gate.wait(context.Background()); err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			left = append(left, time.Now())
			mu.Unlock()
		}()
	}
	// Extend the cooldown repeatedly while the readers are asleep, the way a
	// sibling's second 429 would. Each extension lands before the last expires.
	for i := 0; i < 6; i++ {
		time.Sleep(60 * time.Millisecond)
		gate.pause(80 * time.Millisecond)
	}
	wg.Wait()
	final, _ := gate.untilNow()
	mu.Lock()
	defer mu.Unlock()
	if len(left) != readers {
		t.Fatalf("%d readers left, want %d", len(left), readers)
	}
	for _, at := range left {
		if at.Before(final) {
			t.Fatalf("a reader left %v before the final cooldown ended: a stale timer woke it",
				final.Sub(at))
		}
	}
}

// Readers released together leave staggered, not in one herd on the same
// millisecond.
func TestR2InverseReadGateStaggersReleasedReaders(t *testing.T) {
	gate := newInverseReadGate()
	const readers = 6
	var wg sync.WaitGroup
	var mu sync.Mutex
	left := []time.Time{}
	wg.Add(readers)
	for range readers {
		go func() {
			defer wg.Done()
			if err := gate.wait(context.Background()); err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			left = append(left, time.Now())
			mu.Unlock()
		}()
	}
	gate.pause(100 * time.Millisecond)
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	earliest, latest := left[0], left[0]
	for _, at := range left {
		if at.Before(earliest) {
			earliest = at
		}
		if at.After(latest) {
			latest = at
		}
	}
	if latest.Sub(earliest) == 0 {
		t.Fatal("every released reader left on the same instant")
	}
}

// A terminal 429 still fences the job: the reader that hit it stops, but the
// next new read waits out a real cooldown instead of walking into the same wall.
func TestR2InverseTerminalThrottleStillFencesNewReads(t *testing.T) {
	store := newReadScriptStore(t, &readScriptServer{})
	throttle := readResponseError(http.Header{})
	failing := func(context.Context) error { return throttle }
	// The last permitted try is a 429: the read stops, yet the job is fenced.
	if err := store.read(context.Background(), inverseReadPolicy{attempts: 1, maxBackoff: 400 * time.Millisecond, maxRetryAfter: inverseReadMaxRetryAfter}, "head a", failing); err == nil {
		t.Fatal("a throttled read succeeded")
	}
	if remaining := store.readGate().remaining(); remaining < 100*time.Millisecond {
		t.Fatalf("a terminal 429 left only %v of cooldown: the next read is unfenced", remaining)
	}
	// The fence is real: the next new read waits it out and never reaches the
	// provider before it ends.
	begin := time.Now()
	var at time.Time
	if err := store.read(context.Background(), inverseListReadPolicy, "list", func(context.Context) error {
		at = time.Now()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if at.Sub(begin) < 50*time.Millisecond {
		t.Fatalf("the next read left after %v, inside the terminal throttle's fence", at.Sub(begin))
	}
}

// A Retry-After past the cap also fences the job for the cap, not for nothing,
// even though the reader itself stops.
func TestR2InverseOverCapRetryAfterStillFences(t *testing.T) {
	store := newReadScriptStore(t, &readScriptServer{})
	policy := inverseReadPolicy{attempts: 1, maxBackoff: inverseReadMaxBackoff, maxRetryAfter: time.Second}
	err := store.read(context.Background(), policy, "head a", func(context.Context) error {
		return readResponseError(http.Header{"Retry-After": []string{"3600"}})
	})
	if err == nil || !strings.Contains(err.Error(), "retry cap") {
		t.Fatalf("over-cap Retry-After: %v", err)
	}
	if remaining := store.readGate().remaining(); remaining < 500*time.Millisecond {
		t.Fatalf("over-cap Retry-After left %v of cooldown", remaining)
	}
}

// A header value near int64 max is clamped, never multiplied into an overflowed
// Duration.
func TestR2InverseRetryAfterCannotOverflow(t *testing.T) {
	for name, header := range map[string]http.Header{
		"huge seconds": {"Retry-After": []string{"9223372036854775807"}},
		"huge millis":  {"X-Amz-Retry-After": []string{"9223372036854775807"}},
	} {
		t.Run(name, func(t *testing.T) {
			got, ok := inverseReadRetryAfter(readResponseError(header))
			if !ok || got <= 0 || got > 24*time.Hour {
				t.Fatalf("parsed %v (ok=%v), want a clamped positive wait", got, ok)
			}
		})
	}
}

// A LIST page attempt is finite even when the command has no deadline; the
// bucket listing itself is not bounded by that.
func TestR2InverseListPageAttemptIsFiniteWithoutParentDeadline(t *testing.T) {
	if inverseListReadPolicy.attemptTimeout <= 0 {
		t.Fatal("a LIST page attempt inherits an unbounded parent")
	}
	if inverseListReadPolicy.timeout != 0 {
		t.Fatalf("the LIST policy bounds the whole listing (%v), not one page", inverseListReadPolicy.timeout)
	}
	store := newReadScriptStore(t, &readScriptServer{})
	var seen int
	ctx := context.Background()
	deadlinePolicy := inverseListReadPolicy
	deadlinePolicy.attemptTimeout = 2 * time.Second
	if err := store.listPrefix(ctx, deadlinePolicy, "", 0, func(r2.ObjectInfo) error {
		seen++
		return nil
	}); err != nil || seen != 2 {
		t.Fatalf("bounded listing read %d keys: %v", seen, err)
	}
	// The parent context had no deadline before or after; only the page did.
	if _, ok := ctx.Deadline(); ok {
		t.Fatal("a LIST page bound leaked onto the command context")
	}
}

// The multipart inventory is a read too: one throttled page is retried in place
// with the same markers, and the existing eligibility checks still decide.
func TestR2InverseMultipartInventoryRetriesOnePage(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		n := calls.Add(1)
		w.Header().Set("Content-Type", "application/xml")
		page := req.URL.Query().Get("key-marker")
		if n == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		if page == "" {
			fmt.Fprintf(w, `<ListMultipartUploadsResult><Upload><Key>stitch-staging/a/one.mp4</Key><UploadId>a</UploadId></Upload><IsTruncated>true</IsTruncated><NextKeyMarker>stitch-staging/a/one.mp4</NextKeyMarker><NextUploadIdMarker>a</NextUploadIdMarker></ListMultipartUploadsResult>`)
			return
		}
		fmt.Fprint(w, `<ListMultipartUploadsResult><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>`)
	}))
	defer server.Close()
	client, err := newInverseSDKStore(context.Background(), r2.Config{Bucket: "bucket", Endpoint: server.URL, AccessKey: "test", SecretKey: "test", Region: "auto"})
	if err != nil {
		t.Fatal(err)
	}
	var seen atomic.Int64
	if err := client.RequireNoMultipartOutsideStitchStaging(context.Background(), func(_ context.Context, key, upload string) (bool, error) {
		seen.Add(1)
		return key == "stitch-staging/a/one.mp4" && upload == "a", nil
	}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 || seen.Load() != 1 {
		t.Fatalf("%d inventory requests, %d identity checks: want a retried first page then page two", calls.Load(), seen.Load())
	}
}

// A cooldown never keeps a cancelled job waiting: the context ends the wait.
func TestR2InverseReadGateRespectsContext(t *testing.T) {
	gate := newInverseReadGate()
	gate.pause(time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	if err := gate.wait(ctx); err == nil {
		t.Fatal("an hour-long cooldown outlived a cancelled context")
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("context wait took %v", elapsed)
	}
}

// A prolonged cooldown also ends a read that is already waiting, rather than
// letting it send its next attempt.
func TestR2InverseReadStopsInCooldownWhenContextEnds(t *testing.T) {
	script := &readScriptServer{answer: func(kind, marker string, attempt int) (int, http.Header) {
		return http.StatusTooManyRequests, http.Header{"Retry-After": []string{"20"}}
	}}
	store := newReadScriptStore(t, script)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := store.Head(ctx, "managed/a.mp4")
	if err == nil || script.count() != 1 {
		t.Fatalf("read sent %d attempts during a cooldown: %v", script.count(), err)
	}
}

// freshReadFence models a new command: runR2InverseJob builds one store, and so
// one fence, per process. A test that simulates a restart after a throttled read
// starts a new fence rather than inheriting the stopped run's cooldown.
func freshReadFence(store *inverseSDKStore) {
	store.readsMu.Lock()
	defer store.readsMu.Unlock()
	store.reads = newInverseReadGate()
}
