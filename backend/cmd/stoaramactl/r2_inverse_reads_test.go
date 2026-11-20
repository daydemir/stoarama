package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/db"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

// readResponseError carries a provider's 429 and headers, as the SDK's HTTP
// response error does.
func readResponseError(header http.Header) error {
	return readStatusError(http.StatusTooManyRequests, header)
}

// readStatusError carries any provider status and headers.
func readStatusError(status int, header http.Header) error {
	response := &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(""))}
	return &awshttp.ResponseError{ResponseError: &smithyhttp.ResponseError{Response: &smithyhttp.Response{Response: response}, Err: errors.New("provider status")}}
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
	final, _, _ := gate.untilNow()
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
	if remaining, closed := store.readGate().remaining(); closed || remaining < 100*time.Millisecond {
		t.Fatalf("a terminal 429 left %v of cooldown (closed=%v): the next read is unfenced", remaining, closed)
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

// A Retry-After past the cap latches the job's reads shut instead of releasing
// workers at the cap, where a long read would just meet the same throttle again.
func TestR2InverseOverCapRetryAfterLatchesReadsShut(t *testing.T) {
	store := newReadScriptStore(t, &readScriptServer{})
	policy := inverseReadPolicy{attempts: 4, maxBackoff: inverseReadMaxBackoff, maxRetryAfter: time.Second}
	var sends atomic.Int64
	err := store.read(context.Background(), policy, "head a", func(context.Context) error {
		sends.Add(1)
		return readResponseError(http.Header{"Retry-After": []string{"3600"}})
	})
	if err == nil || !strings.Contains(err.Error(), "reads fenced") {
		t.Fatalf("over-cap Retry-After: %v", err)
	}
	if _, closed := store.readGate().remaining(); !closed {
		t.Fatal("a floor past the cap left the job's reads open")
	}
	// The latch is the point: no sibling read is released at the cap, and the
	// reader itself stops after exactly the one attempt that saw the floor.
	if err := store.read(context.Background(), inverseHeadReadPolicy, "head b", func(context.Context) error { return nil }); err == nil || !strings.Contains(err.Error(), "fenced") {
		t.Fatalf("a read after the latch: %v", err)
	}
	if sends.Load() != 1 {
		t.Fatalf("%d provider sends, want exactly 1", sends.Load())
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
	const key = "stitch-staging/a/one.mp4"
	var calls, continuationAttempts atomic.Int64
	var mu sync.Mutex
	markers := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/xml")
		keyMarker := req.URL.Query().Get("key-marker")
		uploadMarker := req.URL.Query().Get("upload-id-marker")
		mu.Lock()
		markers = append(markers, keyMarker+"|"+uploadMarker)
		mu.Unlock()
		if keyMarker == "" {
			// The first page advertises a populated continuation pair.
			fmt.Fprintf(w, `<ListMultipartUploadsResult><Upload><Key>%s</Key><UploadId>a</UploadId></Upload><IsTruncated>true</IsTruncated><NextKeyMarker>%s</NextKeyMarker><NextUploadIdMarker>a</NextUploadIdMarker></ListMultipartUploadsResult>`, key, key)
			return
		}
		// The populated continuation page itself is throttled once, so the
		// retried page must carry the very same non-empty pair.
		if continuationAttempts.Add(1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		if keyMarker != key || uploadMarker != "a" {
			t.Errorf("continuation markers %q/%q, want %q/a", keyMarker, uploadMarker, key)
		}
		fmt.Fprint(w, `<ListMultipartUploadsResult><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>`)
	}))
	defer server.Close()
	client, err := newInverseSDKStore(context.Background(), r2.Config{Bucket: "bucket", Endpoint: server.URL, AccessKey: "test", SecretKey: "test", Region: "auto"})
	if err != nil {
		t.Fatal(err)
	}
	var seen atomic.Int64
	if err := client.RequireNoMultipartOutsideStitchStaging(context.Background(), func(_ context.Context, k, upload string) (bool, error) {
		seen.Add(1)
		return k == key && upload == "a", nil
	}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 || seen.Load() != 1 {
		t.Fatalf("%d inventory requests, %d identity checks: want the first page, a throttled continuation, then its retry", calls.Load(), seen.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	// The throttled continuation and its retry carry the identical populated
	// key + upload marker pair.
	want := []string{"|", key + "|a", key + "|a"}
	if !slices.Equal(markers, want) {
		t.Fatalf("markers %v, want %v: the continuation pair was not preserved through the retry", markers, want)
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
	store := newReadScriptStore(t, &readScriptServer{})
	// A sibling publishes a real cooldown as its first answer. Its own cap is
	// above the floor, so this is a cooldown, not a latch.
	sibling := make(chan struct{})
	go func() {
		defer close(sibling)
		_ = store.read(context.Background(), inverseReadPolicy{attempts: 1, maxBackoff: inverseReadMaxBackoff, maxRetryAfter: inverseReadMaxRetryAfter}, "head sibling", func(context.Context) error {
			return readResponseError(http.Header{"Retry-After": []string{"20"}})
		})
	}()
	<-sibling
	if got := store.readGate().publishedCooldown(); got < 19*time.Second {
		t.Fatalf("sibling published %v, want about 20s", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := store.read(ctx, inverseListReadPolicy, "list", func(context.Context) error {
		t.Error("the SDK ran inside a cooldown")
		return nil
	})
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cooldown wait ended with %v, want the context's own error", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("cooldown wait took %v", elapsed)
	}
	// Nothing was handed to the SDK: the count is zero operations invoked.
	if !strings.Contains(err.Error(), "after 0 attempts") {
		t.Fatalf("a gate failure reported a phantom attempt: %v", err)
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

// A 503 with no Retry-After still retries under the bounded jitter, one request
// per attempt, and finds the object once the provider recovers.
func TestR2Inverse503WithoutRetryAfterRetriesBounded(t *testing.T) {
	script := &readScriptServer{answer: func(kind, marker string, attempt int) (int, http.Header) {
		if attempt == 1 {
			return http.StatusServiceUnavailable, nil
		}
		return http.StatusOK, nil
	}}
	store := newReadScriptStore(t, script)
	started := time.Now()
	head, err := store.Head(context.Background(), "managed/a.mp4")
	if err != nil || head.ETag != "etag" {
		t.Fatalf("503 then 200: %+v %v", head, err)
	}
	if n := script.attempts("head", "managed/a.mp4"); n != 2 {
		t.Fatalf("%d HEADs, want one retry", n)
	}
	if elapsed := time.Since(started); elapsed > inverseReadMaxBackoff+2*inverseReadHerdJitter {
		t.Fatalf("503 retry waited %v, past the bounded jitter", elapsed)
	}
}

// A 429 with a small Retry-After still uses bounded jitter for its own retry and
// publishes a shared cooldown that holds another reader for its real floor.
func TestR2InverseSmallRetryAfterKeepsBoundedJitterAndCooldown(t *testing.T) {
	script := &readScriptServer{answer: func(kind, marker string, attempt int) (int, http.Header) {
		if attempt == 1 {
			return http.StatusTooManyRequests, http.Header{"Retry-After": []string{"1"}}
		}
		return http.StatusOK, nil
	}}
	store := newReadScriptStore(t, script)
	// Another worker's read starts while the HEAD's floor is in force.
	sibling := make(chan time.Duration, 1)
	go func() {
		time.Sleep(80 * time.Millisecond)
		begin := time.Now()
		store.read(context.Background(), inverseListReadPolicy, "sibling", func(context.Context) error { return nil })
		sibling <- time.Since(begin)
	}()
	started := time.Now()
	if _, err := store.Head(context.Background(), "managed/a.mp4"); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(started)
	if elapsed < 750*time.Millisecond {
		t.Fatalf("a one-second floor retried after %v: the floor was not honored", elapsed)
	}
	if elapsed > inverseReadMaxRetryAfter {
		t.Fatalf("a one-second floor waited %v", elapsed)
	}
	if waited := <-sibling; waited < 400*time.Millisecond {
		t.Fatalf("a sibling read left after %v, inside the shared cooldown", waited)
	}
}

// A bare 503 with no Retry-After publishes the shared minimum cooldown, so a
// distinct sibling read type is held by it. The sibling is launched as soon as
// the first observed 503 has published that floor, while the early case's
// reader is still provably in flight: its second try waits on an explicit
// channel, so the read cannot reach a terminal try before the sibling is in.
func TestR2InverseBare503PublishesSharedCooldown(t *testing.T) {
	// awaitCooldown blocks until a floor is published, so the sibling is never
	// launched against an empty gate.
	awaitCooldown := func(t *testing.T, store *inverseSDKStore) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for store.readGate().publishedCooldown() <= 0 {
			if time.Now().After(deadline) {
				t.Fatal("the first 503 published no shared cooldown")
			}
			time.Sleep(2 * time.Millisecond)
		}
	}
	// siblings are held by the published floor and report how long they waited.
	launchSibling := func(t *testing.T, store *inverseSDKStore) <-chan time.Duration {
		t.Helper()
		waited := make(chan time.Duration, 1)
		go func() {
			begin := time.Now()
			_ = store.read(context.Background(), inverseListReadPolicy, "sibling list", func(context.Context) error { return nil })
			waited <- time.Since(begin)
		}()
		return waited
	}

	t.Run("early", func(t *testing.T) {
		store := newReadScriptStore(t, &readScriptServer{})
		// The second try waits on this channel, so the reader is still on a
		// non-terminal try when the sibling starts.
		release := make(chan struct{})
		policy := inverseReadPolicy{attempts: 4, maxBackoff: 600 * time.Millisecond, maxRetryAfter: inverseReadMaxRetryAfter}
		sends := atomic.Int64{}
		finished := make(chan error, 1)
		go func() {
			finished <- store.read(context.Background(), policy, "head a", func(context.Context) error {
				if sends.Add(1) == 1 {
					return readStatusError(http.StatusServiceUnavailable, http.Header{})
				}
				<-release
				return readStatusError(http.StatusServiceUnavailable, http.Header{})
			})
		}()
		awaitCooldown(t, store)
		// The reader has one try left before terminal and is blocked here.
		if sends.Load() != 1 {
			t.Fatalf("%d sends before the sibling launched, want the reader still on try 1", sends.Load())
		}
		waited := launchSibling(t, store)
		select {
		case d := <-waited:
			if d < 100*time.Millisecond {
				t.Fatalf("an early bare 503 did not hold a sibling: it left after %v", d)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("sibling read never finished")
		}
		close(release)
		if err := <-finished; err == nil {
			t.Fatal("a bare 503 read succeeded")
		}
	})

	t.Run("terminal", func(t *testing.T) {
		store := newReadScriptStore(t, &readScriptServer{})
		policy := inverseReadPolicy{attempts: 1, maxBackoff: 600 * time.Millisecond, maxRetryAfter: inverseReadMaxRetryAfter}
		sends := atomic.Int64{}
		finished := make(chan error, 1)
		go func() {
			finished <- store.read(context.Background(), policy, "head a", func(context.Context) error {
				sends.Add(1)
				return readStatusError(http.StatusServiceUnavailable, http.Header{})
			})
		}()
		awaitCooldown(t, store)
		// The read is terminal at this single try; the floor it published still
		// holds a sibling.
		waited := launchSibling(t, store)
		select {
		case d := <-waited:
			if d < 100*time.Millisecond {
				t.Fatalf("a terminal bare 503 did not hold a sibling: it left after %v", d)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("sibling read never finished")
		}
		if err := <-finished; err == nil {
			t.Fatal("a bare 503 read succeeded")
		}
		if sends.Load() != 1 {
			t.Fatalf("%d terminal sends, want exactly 1", sends.Load())
		}
	})
}

// An overflowing positive decimal is a real, very long floor, not an absent
// hint: both seconds and R2 milliseconds saturate at the parse clamp. Values
// beyond int64 and beyond uint64 both count. A malformed or signed value keeps
// its "no hint" meaning, and an HTTP date still parses.
func TestR2InverseRetryAfterOverflowSaturates(t *testing.T) {
	beyondInt64 := "9223372036854775808"   // math.MaxInt64 + 1
	beyondUint64 := "18446744073709551616" // math.MaxUint64 + 1
	for name, tc := range map[string]struct {
		header http.Header
		want   time.Duration
	}{
		"seconds beyond int64":  {http.Header{"Retry-After": []string{beyondInt64}}, 24 * time.Hour},
		"seconds beyond uint64": {http.Header{"Retry-After": []string{beyondUint64}}, 24 * time.Hour},
		"millis beyond int64":   {http.Header{"X-Amz-Retry-After": []string{beyondInt64}}, 24 * time.Hour},
		"millis beyond uint64":  {http.Header{"X-Amz-Retry-After": []string{beyondUint64}}, 24 * time.Hour},
		"huge seconds":          {http.Header{"Retry-After": []string{"9223372036854775807"}}, 24 * time.Hour},
		"huge millis":           {http.Header{"X-Amz-Retry-After": []string{"9223372036854775807"}}, 24 * time.Hour},
	} {
		t.Run(name, func(t *testing.T) {
			got, ok := inverseReadRetryAfter(readResponseError(tc.header))
			if !ok || got != tc.want {
				t.Fatalf("parsed %v (ok=%v), want %v as a saturated floor", got, ok, tc.want)
			}
		})
	}
	// Malformed and signed input keeps "no hint".
	for _, raw := range []string{"-5", "later", "1.5", " 5x", "+7"} {
		if _, ok := inverseReadRetryAfter(readResponseError(http.Header{"Retry-After": []string{raw}})); ok {
			t.Fatalf("%q was read as a numeric floor", raw)
		}
	}
	// An HTTP date still parses.
	at := time.Now().Add(3 * time.Second).UTC().Format(http.TimeFormat)
	got, ok := inverseReadRetryAfter(readResponseError(http.Header{"Retry-After": []string{at}}))
	if !ok || got <= 0 || got > 4*time.Second {
		t.Fatalf("HTTP date parsed %v (ok=%v)", got, ok)
	}
}

// When both headers are present the longest valid floor wins, so a short
// standard hint never undercuts R2's own millisecond ask.
func TestR2InverseRetryAfterChoosesLongestFloor(t *testing.T) {
	got, ok := inverseReadRetryAfter(readResponseError(http.Header{
		"Retry-After":       []string{"1"},
		"X-Amz-Retry-After": []string{"2500"},
	}))
	if !ok || got != 2500*time.Millisecond {
		t.Fatalf("coexisting headers chose %v (ok=%v), want the longer 2500ms", got, ok)
	}
	// The reverse direction also holds.
	got, ok = inverseReadRetryAfter(readResponseError(http.Header{
		"Retry-After":       []string{"7"},
		"X-Amz-Retry-After": []string{"10"},
	}))
	if !ok || got != 7*time.Second {
		t.Fatalf("coexisting headers chose %v (ok=%v), want the longer 7s", got, ok)
	}
	// An invalid longer field does not hide a valid one.
	got, ok = inverseReadRetryAfter(readResponseError(http.Header{
		"Retry-After":       []string{"4"},
		"X-Amz-Retry-After": []string{"nonsense"},
	}))
	if !ok || got != 4*time.Second {
		t.Fatalf("invalid R2 hint hid the standard one: %v (ok=%v)", got, ok)
	}
}

// A definitive confirming 200 must never become a retry, even if a short
// attempt deadline expires at the same moment: the classification reads the
// returned error, not the context.
func TestR2InverseDefinitiveAnswerIsNeverRetried(t *testing.T) {
	store := newReadScriptStore(t, &readScriptServer{})
	// A short attempt deadline, so the attempt really expires before the
	// definitive answer is returned; the parent stays healthy throughout.
	policy := inverseReadPolicy{attempts: 4, attemptTimeout: 100 * time.Millisecond, maxBackoff: 10 * time.Millisecond, maxRetryAfter: inverseReadMaxRetryAfter}
	parent, cancelParent := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelParent()
	callbacks := atomic.Int64{}
	parentLive := atomic.Bool{}
	err := store.read(parent, policy, "confirming head managed/a.mp4", func(attemptCtx context.Context) error {
		callbacks.Add(1)
		// Wait for this attempt's own deadline to fire.
		<-attemptCtx.Done()
		// The parent must still be independently live: this is the attempt's
		// expiry, never the job ending.
		parentLive.Store(parent.Err() == nil)
		// The provider answered: the object is still there. A definitive answer
		// returned after its attempt deadline must not become a retry.
		return inverseAbsence(nil)
	})
	if err == nil || !strings.Contains(err.Error(), "object remains after DELETE") {
		t.Fatalf("definitive answer lost: %v", err)
	}
	if !parentLive.Load() {
		t.Fatal("the parent context was not live; the case did not test an attempt-local deadline")
	}
	if callbacks.Load() != 1 {
		t.Fatalf("%d callback invocation(s) for a definitive answer, want exactly 1", callbacks.Load())
	}
}

// Gate failures before the SDK call report operations actually invoked, which is
// zero for an already latched gate, and never a phantom attempt.
func TestR2InverseGateFailureReportsInvokedOperations(t *testing.T) {
	store := newReadScriptStore(t, &readScriptServer{})
	store.readGate().close()
	err := store.read(context.Background(), inverseHeadReadPolicy, "head a", func(context.Context) error {
		t.Error("the SDK ran behind a latched gate")
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "after 0 attempts") {
		t.Fatalf("latched gate reported: %v", err)
	}
}

// The real standalone dispatch reads through the bounded inverse store, under
// its own archived bucket and endpoint rather than the ordinary ones: a 429 on a
// continuation page retries in place with the same token, and every request
// carries the archived target.
func TestR2InversePartitionDispatchUsesBoundedReader(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if dsn == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	pool := joinedPurgeTestPool(t, "inverse_partition_dispatch")
	if err := db.MigrateUp(ctx, pool, joinedPurgeMigrationsDir()); err != nil {
		t.Fatal(err)
	}
	// The archived target differs from every ordinary bucket/endpoint setting,
	// so a request to the wrong one is unmistakable.
	const archivedBucket = "archived-inverse-bucket"
	var mu sync.Mutex
	type request struct{ path, token, method string }
	requests := []request{}
	var listCalls, continuationAttempts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		lister := req.Method == http.MethodGet && req.URL.Query().Has("list-type")
		mu.Lock()
		requests = append(requests, request{path: req.URL.Path, token: req.URL.Query().Get("continuation-token"), method: req.Method})
		mu.Unlock()
		if !lister {
			// The multipart inventory is not part of a partition read; refuse it
			// so a wrong path is visible rather than silently tolerated.
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		listCalls.Add(1)
		token := req.URL.Query().Get("continuation-token")
		pageTwo := 0
		if token == "page-two" {
			pageTwo = int(continuationAttempts.Add(1))
		}
		switch {
		case token == "":
			fmt.Fprint(w, `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>page-two</NextContinuationToken>`+
				`<Contents><Key>managed/a.mp4</Key><ETag>"e1"</ETag><Size>10</Size><LastModified>2026-01-01T00:00:00.000Z</LastModified></Contents></ListBucketResult>`)
		case token == "page-two" && pageTwo <= 3:
			// Three throttled tries at the continuation page outlast the SDK's own
			// three-attempt default, so only the inverse bounded read (four tries)
			// can finish this partition. Every retry must carry the same token.
			w.WriteHeader(http.StatusTooManyRequests)
		case token == "page-two":
			fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated>`+
				`<Contents><Key>managed/b.mp4</Key><ETag>"e2"</ETag><Size>20</Size><LastModified>2026-01-01T00:00:00.000Z</LastModified></Contents></ListBucketResult>`)
		default:
			t.Errorf("unexpected continuation token %q", token)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	picksPath := filepath.Join(t.TempDir(), "picks.json")
	picksData, _ := json.Marshal(map[string]any{"choices": map[string]any{}})
	if err := os.WriteFile(picksPath, picksData, 0600); err != nil {
		t.Fatal(err)
	}
	// The command opens its own pool from the connection string.
	scoped := dsn
	if strings.Contains(dsn, "?") {
		scoped += "&options=-c%20search_path%3D" + pool.Config().ConnConfig.RuntimeParams["search_path"]
	} else {
		scoped += "?options=-c%20search_path%3D" + pool.Config().ConnConfig.RuntimeParams["search_path"]
	}
	out := filepath.Join(t.TempDir(), "report")
	cfg := config.Config{
		DatabaseURL:       scoped,
		R2AccountID:       "archived-account",
		R2AccessKeyID:     "archived-key",
		R2SecretAccessKey: "archived-secret",
		R2Bucket:          archivedBucket,
		R2Region:          "auto",
		R2Endpoint:        server.URL,
	}
	sum, err := writeInversePartition(ctx, cfg, picksPath, out)
	if err != nil {
		t.Fatal(err)
	}
	if sum.Bucket != archivedBucket || sum.EndpointSHA256 == "" {
		t.Fatalf("partition summary names %q, not the archived bucket", sum.Bucket)
	}
	if sum.Total.Objects != 2 || sum.Total.Bytes != 30 {
		t.Fatalf("partition %+v", sum.Total)
	}
	mu.Lock()
	defer mu.Unlock()
	var tokens []string
	for _, r := range requests {
		if r.path != "/"+archivedBucket {
			t.Fatalf("request %s %s, want the archived bucket %q", r.method, r.path, archivedBucket)
		}
		if r.method == http.MethodGet {
			tokens = append(tokens, r.token)
		}
	}
	// The first page, then three throttled tries at the continuation page and the
	// fourth try that succeeds, all four carrying the identical token.
	want := []string{"", "page-two", "page-two", "page-two", "page-two"}
	if !slices.Equal(tokens, want) {
		t.Fatalf("continuation-token sequence %v, want %v", tokens, want)
	}
	if continuationAttempts.Load() != 4 {
		t.Fatalf("%d continuation attempts, want exactly 4", continuationAttempts.Load())
	}
}

// The dispatch itself passes a real inverse store, not a raw r2.Client.
func TestR2InversePartitionDispatchStoreType(t *testing.T) {
	store, err := newInverseSDKStore(context.Background(), r2.Config{Bucket: "stoarama", Endpoint: "http://127.0.0.1:1", AccessKey: "test", SecretKey: "test", Region: "auto"})
	if err != nil {
		t.Fatal(err)
	}
	var lister inverseLister = store
	if lister.Bucket() != "stoarama" || lister.EndpointSHA256() == "" {
		t.Fatalf("bounded store does not carry the archived bucket/endpoint: %q %q", lister.Bucket(), lister.EndpointSHA256())
	}
	if _, ok := lister.(interface {
		ListPrefix(context.Context, string, int, func(r2.ObjectInfo) error) error
	}); !ok {
		t.Fatal("bounded store is not a lister")
	}
}
