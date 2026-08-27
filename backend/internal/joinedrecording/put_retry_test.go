package joinedrecording

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

func hashBytes(body []byte) string { sum := sha256.Sum256(body); return hex.EncodeToString(sum[:]) }

func distinctCreateCapability(t *testing.T, capability ObjectCreateCapability, attempt int) ObjectCreateCapability {
	t.Helper()
	parsed, err := url.Parse(capability.Request.URL)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("retry-attempt", strconv.Itoa(attempt))
	parsed.RawQuery = query.Encode()
	capability.Request.URL = parsed.String()
	capability.Request.RawQuery = parsed.RawQuery
	return capability
}

type scriptedPutClient struct {
	*memoryCapabilityClient
	statuses           []int
	putCalls           int
	bodies             [][]byte
	commitOnFirstError bool
	afterPut           func(int)
}

type trackedReadCloser struct {
	*bytes.Reader
	closed *int
}

func (r *trackedReadCloser) Close() error { *r.closed++; return nil }

type ambiguousPartClient struct {
	*memoryCapabilityClient
	targetKey  string
	targetPuts int
	targetGets int
}

func (*ambiguousPartClient) joinedRedirectSafe() {}

func (c *ambiguousPartClient) Do(req *http.Request) (*http.Response, error) {
	key := req.URL.Query().Get("key")
	if key == c.targetKey && req.Method == http.MethodPut {
		c.targetPuts++
		if c.targetPuts == 1 {
			body, err := io.ReadAll(req.Body)
			_ = req.Body.Close()
			if err != nil {
				return nil, err
			}
			c.objects[key] = append([]byte(nil), body...)
			return nil, errors.New("ambiguous edge 502")
		}
	}
	if key == c.targetKey && req.Method == http.MethodGet {
		c.targetGets++
	}
	return c.memoryCapabilityClient.Do(req)
}

func (c *scriptedPutClient) Do(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodPut {
		return c.memoryCapabilityClient.Do(req)
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	c.bodies = append(c.bodies, body)
	c.putCalls++
	if c.afterPut != nil {
		c.afterPut(c.putCalls)
	}
	status := c.statuses[c.putCalls-1]
	if status == 0 {
		if c.commitOnFirstError && c.putCalls == 1 {
			c.objects[req.URL.EscapedPath()] = append([]byte(nil), body...)
		}
		return nil, errors.New("temporary transport failure")
	}
	if status == http.StatusPreconditionFailed {
		return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(nil))}, nil
	}
	if status >= 200 && status < 300 {
		c.objects[req.URL.EscapedPath()] = append([]byte(nil), body...)
	}
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(nil))}, nil
}

func TestCreateOnlyPutRetriesAmbiguousWriteAndKeepsExactBody(t *testing.T) {
	body := []byte("immutable joined artifact")
	sha := hashBytes(body)
	capability := createCapability(7, "recordings", "joined/test.mp4", "video/mp4", body)
	client := &scriptedPutClient{memoryCapabilityClient: &memoryCapabilityClient{objects: map[string][]byte{}}, statuses: []int{0, http.StatusPreconditionFailed}, commitOnFirstError: true}
	waits := 0
	resolves := 0
	opens := 0
	closes := 0
	policy := putRetryPolicy{wait: func(context.Context, time.Duration) error { waits++; return nil }, now: time.Now}
	observation, err := putCreateOnlyWithRetry(context.Background(), client, testSourceAuthority, "recordings", 7, "joined/test.mp4", "video/mp4", int64(len(body)), sha, func() time.Time { return time.Now().Add(time.Minute) }, "lease-12345678", func(context.Context) (ObjectCreateCapability, error) {
		resolves++
		return distinctCreateCapability(t, capability, resolves), nil
	}, func() (io.ReadCloser, error) {
		opens++
		return &trackedReadCloser{Reader: bytes.NewReader(body), closed: &closes}, nil
	}, policy)
	if err != nil {
		t.Fatal(err)
	}
	if observation.Created || client.putCalls != 2 || waits != 1 || resolves != 2 || opens != 2 || closes != 2 {
		t.Fatalf("observation=%+v puts=%d waits=%d resolves=%d opens=%d closes=%d", observation, client.putCalls, waits, resolves, opens, closes)
	}
	for i, got := range client.bodies {
		if !bytes.Equal(got, body) {
			t.Fatalf("attempt %d body changed: %q", i+1, got)
		}
	}
}

func TestCreateOnlyPutCancellationStopsBeforeAnotherAttempt(t *testing.T) {
	body := []byte("artifact")
	capability := createCapability(7, "recordings", "joined/test.mp4", "video/mp4", body)
	client := &scriptedPutClient{memoryCapabilityClient: &memoryCapabilityClient{objects: map[string][]byte{}}, statuses: []int{503}}
	ctx, cancel := context.WithCancel(context.Background())
	resolves, opens := 0, 0
	policy := putRetryPolicy{wait: func(context.Context, time.Duration) error { cancel(); return context.Canceled }, now: time.Now}
	_, err := putCreateOnlyWithRetry(ctx, client, testSourceAuthority, "recordings", 7, "joined/test.mp4", "video/mp4", int64(len(body)), hashBytes(body), func() time.Time { return time.Now().Add(time.Minute) }, "lease-12345678", func(context.Context) (ObjectCreateCapability, error) { resolves++; return capability, nil }, func() (io.ReadCloser, error) { opens++; return io.NopCloser(bytes.NewReader(body)), nil }, policy)
	if !errors.Is(err, context.Canceled) || client.putCalls != 1 || resolves != 1 || opens != 1 {
		t.Fatalf("err=%v puts=%d resolves=%d opens=%d", err, client.putCalls, resolves, opens)
	}
}

func TestCreateOnlyPutRetryMatrix(t *testing.T) {
	tests := []struct {
		name      string
		statuses  []int
		wantCalls int
		wantErr   bool
	}{
		{"408 then success", []int{408, 200}, 2, false},
		{"425 then success", []int{425, 200}, 2, false},
		{"429 then success", []int{429, 200}, 2, false},
		{"five hundred then success", []int{500, 200}, 2, false},
		{"four hundred does not retry", []int{400}, 1, true},
		{"five failures exhaust", []int{503, 503, 503, 503, 503}, 5, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := []byte("artifact")
			capability := createCapability(7, "recordings", "joined/test.mp4", "video/mp4", body)
			client := &scriptedPutClient{memoryCapabilityClient: &memoryCapabilityClient{objects: map[string][]byte{}}, statuses: tt.statuses}
			policy := putRetryPolicy{wait: func(context.Context, time.Duration) error { return nil }, now: time.Now}
			_, err := putCreateOnlyWithRetry(context.Background(), client, testSourceAuthority, "recordings", 7, "joined/test.mp4", "video/mp4", int64(len(body)), hashBytes(body), func() time.Time { return time.Now().Add(time.Minute) }, "lease-12345678", func(context.Context) (ObjectCreateCapability, error) { return capability, nil }, func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }, policy)
			if (err != nil) != tt.wantErr || client.putCalls != tt.wantCalls {
				t.Fatalf("err=%v calls=%d", err, client.putCalls)
			}
		})
	}
}

func TestCreateOnlyPutRetriesOnlyTransientCapabilityResolution(t *testing.T) {
	tests := []struct {
		name      string
		failures  []*StorageCapabilityError
		wantCalls int
		wantErr   bool
	}{
		{"transport then success", []*StorageCapabilityError{{Operation: "create_capability", Reason: "transport", Cause: context.DeadlineExceeded}}, 2, false},
		{"caller canceled transport", []*StorageCapabilityError{{Operation: "create_capability", Reason: "transport", Cause: context.Canceled}}, 1, true},
		{"408 then success", []*StorageCapabilityError{{Operation: "create_capability", Reason: "status", StatusCode: 408}}, 2, false},
		{"425 then success", []*StorageCapabilityError{{Operation: "create_capability", Reason: "status", StatusCode: 425}}, 2, false},
		{"429 then success", []*StorageCapabilityError{{Operation: "create_capability", Reason: "status", StatusCode: 429}}, 2, false},
		{"503 then success", []*StorageCapabilityError{{Operation: "create_capability", Reason: "status", StatusCode: 503}}, 2, false},
		{"deterministic capability", []*StorageCapabilityError{{Operation: "create_capability", Reason: "capability"}}, 1, true},
		{"deterministic 400", []*StorageCapabilityError{{Operation: "create_capability", Reason: "status", StatusCode: 400}}, 1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := []byte("artifact")
			capability := createCapability(7, "recordings", "joined/test.mp4", "video/mp4", body)
			client := &scriptedPutClient{memoryCapabilityClient: &memoryCapabilityClient{objects: map[string][]byte{}}, statuses: []int{200}}
			resolves, opens, waits := 0, 0, 0
			_, err := putCreateOnlyWithRetry(context.Background(), client, testSourceAuthority, "recordings", 7,
				"joined/test.mp4", "video/mp4", int64(len(body)), hashBytes(body), func() time.Time { return time.Now().Add(time.Minute) },
				"lease-12345678", func(context.Context) (ObjectCreateCapability, error) {
					resolves++
					if resolves <= len(tt.failures) {
						failure := *tt.failures[resolves-1]
						failure.ArtifactID = 7
						return ObjectCreateCapability{}, &failure
					}
					return distinctCreateCapability(t, capability, resolves), nil
				}, func() (io.ReadCloser, error) {
					opens++
					return io.NopCloser(bytes.NewReader(body)), nil
				}, putRetryPolicy{wait: func(context.Context, time.Duration) error { waits++; return nil }, now: time.Now})
			if (err != nil) != tt.wantErr || resolves != tt.wantCalls || waits != tt.wantCalls-1 ||
				opens != boolInt(!tt.wantErr) || client.putCalls != boolInt(!tt.wantErr) {
				t.Fatalf("err=%v resolves=%d waits=%d opens=%d puts=%d", err, resolves, waits, opens, client.putCalls)
			}
		})
	}
}

func TestCreateOnlyPutCapabilityResolutionExhaustionPreservesAttempts(t *testing.T) {
	body := []byte("artifact")
	resolves, waits, opens := 0, 0, 0
	_, err := putCreateOnlyWithRetry(context.Background(), &memoryCapabilityClient{objects: map[string][]byte{}},
		testSourceAuthority, "recordings", 646, "joined/test.mp4", "video/mp4", int64(len(body)), hashBytes(body),
		func() time.Time { return time.Now().Add(time.Minute) }, "lease-12345678", func(context.Context) (ObjectCreateCapability, error) {
			resolves++
			return ObjectCreateCapability{}, &StorageCapabilityError{Operation: "create_capability", Reason: "transport",
				ArtifactID: 646, Cause: context.DeadlineExceeded}
		}, func() (io.ReadCloser, error) {
			opens++
			return io.NopCloser(bytes.NewReader(body)), nil
		}, putRetryPolicy{wait: func(context.Context, time.Duration) error { waits++; return nil }, now: time.Now})
	var diagnostic *StorageCapabilityError
	if !errors.As(err, &diagnostic) || diagnostic.Attempts != len(putRetryDelays)+1 || diagnostic.ArtifactID != 646 ||
		resolves != len(putRetryDelays)+1 || waits != len(putRetryDelays) || opens != 0 {
		t.Fatalf("diagnostic=%+v resolves=%d waits=%d opens=%d err=%v", diagnostic, resolves, waits, opens, err)
	}
}

func TestCapabilityResolutionStopsWhenCallerContextEnds(t *testing.T) {
	for _, operation := range []string{"create", "read"} {
		t.Run(operation, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			resolves, waits := 0, 0
			if operation == "create" {
				_, err := putCreateOnlyWithRetry(ctx, &memoryCapabilityClient{objects: map[string][]byte{}}, testSourceAuthority,
					"recordings", 646, "joined/test.mp4", "video/mp4", 1, strings.Repeat("a", 64),
					func() time.Time { return time.Now().Add(time.Minute) }, "lease-12345678",
					func(context.Context) (ObjectCreateCapability, error) {
						resolves++
						cancel()
						return ObjectCreateCapability{}, &StorageCapabilityError{Operation: "create_capability", Reason: "transport",
							ArtifactID: 646, Cause: context.DeadlineExceeded}
					}, func() (io.ReadCloser, error) { t.Fatal("unexpected body open"); return nil, nil },
					putRetryPolicy{wait: func(context.Context, time.Duration) error { waits++; return nil }, now: time.Now})
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("create error=%v", err)
				}
			} else {
				_, err := resolveReadCapabilityWithRetry(ctx, 646, func() time.Time { return time.Now().Add(time.Minute) },
					"lease-12345678", func(context.Context) (ObjectReadCapability, error) {
						resolves++
						cancel()
						return ObjectReadCapability{}, &StorageCapabilityError{Operation: "reread_capability", Reason: "transport",
							ArtifactID: 646, Cause: context.DeadlineExceeded}
					}, putRetryPolicy{wait: func(context.Context, time.Duration) error { waits++; return nil }, now: time.Now})
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("read error=%v", err)
				}
			}
			if resolves != 1 || waits != 0 {
				t.Fatalf("resolves=%d waits=%d", resolves, waits)
			}
		})
	}
}

func TestReadCapabilityResolutionRetriesTransientFailure(t *testing.T) {
	capability := ObjectReadCapability{ArtifactID: 646}
	resolves, waits := 0, 0
	got, err := resolveReadCapabilityWithRetry(context.Background(), 646, func() time.Time { return time.Now().Add(time.Minute) },
		"lease-12345678", func(context.Context) (ObjectReadCapability, error) {
			resolves++
			if resolves == 1 {
				return ObjectReadCapability{}, &StorageCapabilityError{Operation: "reread_capability", Reason: "transport",
					ArtifactID: 646, Cause: context.DeadlineExceeded}
			}
			return capability, nil
		}, putRetryPolicy{wait: func(context.Context, time.Duration) error { waits++; return nil }, now: time.Now})
	if err != nil || got.ArtifactID != 646 || resolves != 2 || waits != 1 {
		t.Fatalf("got=%+v err=%v resolves=%d waits=%d", got, err, resolves, waits)
	}
}

func TestReadCapabilityResolutionDoesNotRetryDeterministicFailure(t *testing.T) {
	resolves := 0
	_, err := resolveReadCapabilityWithRetry(context.Background(), 646, func() time.Time { return time.Now().Add(time.Minute) },
		"lease-12345678", func(context.Context) (ObjectReadCapability, error) {
			resolves++
			return ObjectReadCapability{}, &StorageCapabilityError{Operation: "reread_capability", Reason: "status",
				StatusCode: http.StatusConflict, ArtifactID: 646}
		}, putRetryPolicy{wait: func(context.Context, time.Duration) error { t.Fatal("unexpected retry wait"); return nil }, now: time.Now})
	var diagnostic *StorageCapabilityError
	if !errors.As(err, &diagnostic) || diagnostic.StatusCode != http.StatusConflict || diagnostic.Attempts != 1 || resolves != 1 {
		t.Fatalf("diagnostic=%+v resolves=%d err=%v", diagnostic, resolves, err)
	}
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func TestCreateOnlyPutDoesNotWaitPastLease(t *testing.T) {
	body := []byte("artifact")
	capability := createCapability(7, "recordings", "joined/test.mp4", "video/mp4", body)
	client := &scriptedPutClient{memoryCapabilityClient: &memoryCapabilityClient{objects: map[string][]byte{}}, statuses: []int{503}}
	waits := 0
	now := time.Now()
	policy := putRetryPolicy{wait: func(context.Context, time.Duration) error { waits++; return nil }, now: func() time.Time { return now }}
	_, err := putCreateOnlyWithRetry(context.Background(), client, testSourceAuthority, "recordings", 7, "joined/test.mp4", "video/mp4", int64(len(body)), hashBytes(body), func() time.Time { return now.Add(100 * time.Millisecond) }, "lease-12345678", func(context.Context) (ObjectCreateCapability, error) { return capability, nil }, func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }, policy)
	if err == nil || client.putCalls != 1 || waits != 0 {
		t.Fatalf("err=%v calls=%d waits=%d", err, client.putCalls, waits)
	}
}

func TestCreateOnlyPutUsesRenewedLeaseDeadlineAfterLongAttempt(t *testing.T) {
	body := []byte("artifact")
	capability := createCapability(7, "recordings", "joined/test.mp4", "video/mp4", body)
	fakeNow := time.Now()
	leaseExpiry := fakeNow.Add(time.Second)
	client := &scriptedPutClient{memoryCapabilityClient: &memoryCapabilityClient{objects: map[string][]byte{}}, statuses: []int{503, 200}}
	client.afterPut = func(attempt int) {
		if attempt == 1 {
			fakeNow = fakeNow.Add(2 * time.Second)
			leaseExpiry = fakeNow.Add(time.Minute)
		}
	}
	policy := putRetryPolicy{wait: func(context.Context, time.Duration) error { return nil }, now: func() time.Time { return fakeNow }}
	_, err := putCreateOnlyWithRetry(context.Background(), client, testSourceAuthority, "recordings", 7, "joined/test.mp4", "video/mp4", int64(len(body)), hashBytes(body), func() time.Time { return leaseExpiry }, "lease-12345678", func(context.Context) (ObjectCreateCapability, error) { return capability, nil }, func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }, policy)
	if err != nil || client.putCalls != 2 {
		t.Fatalf("err=%v puts=%d", err, client.putCalls)
	}
}

func TestHourPublicationReconcilesAmbiguousPartWriteBeforeContinuing(t *testing.T) {
	claim, scratch, _, _ := thirtyPartPublicationFixture(t)
	client := &ambiguousPartClient{memoryCapabilityClient: &memoryCapabilityClient{objects: map[string][]byte{}}, targetKey: claim.Plan.Outputs[28].ObjectKey}
	finalized := false
	published, err := publishClaimedHour(context.Background(), client, claim, scratch, testCreateResolver(), testReadResolver(client.memoryCapabilityClient), func(context.Context, WorkerClaim, PublishedHour) error { finalized = true; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if !finalized || client.targetPuts != 2 || client.targetGets != 1 || len(published.Outputs) != 30 || published.Outputs[28].Created {
		t.Fatalf("finalized=%v puts=%d gets=%d outputs=%d created=%v", finalized, client.targetPuts, client.targetGets, len(published.Outputs), published.Outputs[28].Created)
	}
}
