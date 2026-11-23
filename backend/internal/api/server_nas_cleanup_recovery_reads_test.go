package api

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
)

func recoveryReadServer(t *testing.T, wrap func(http.Handler) http.Handler) (*Server, *fakeCleanupBucket, nasCleanupItem) {
	t.Helper()
	body := []byte("exact recovery bytes")
	bucket := &fakeCleanupBucket{objects: map[string][]byte{"object": body}}
	var handler http.Handler = bucket
	if wrap != nil {
		handler = wrap(handler)
	}
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	client, err := r2.New(context.Background(), r2.Config{UngatedWriter: true, AccessKey: "key", SecretKey: "secret", Region: "auto", Bucket: "bucket", Endpoint: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(body)
	return &Server{r2: client}, bucket, nasCleanupItem{ObjectKey: "object", SizeBytes: int64(len(body)), SHA256: hex.EncodeToString(sum[:])}
}

func TestNASCleanupRecoveryReadWeights(t *testing.T) {
	const chunk int64 = 64 << 20
	for _, c := range []struct{ size, want int64 }{{0, 1}, {1, 1}, {chunk, 1}, {chunk + 1, 2}, {16 * chunk, 16}, {16*chunk + 1, 16}, {math.MaxInt64, 16}} {
		if got := cleanupRecoveryReadWeight(c.size); got != c.want {
			t.Fatalf("size %d: weight %d want %d", c.size, got, c.want)
		}
	}
}

func TestNASCleanupRecoveryReadsBoundAcrossRequestsAndReleaseOnCancellation(t *testing.T) {
	// Reserve14of16 units to exercise two real HTTP readers and a queued third.
	// Weight boundary tests above prove how large objects consume these units.
	if err := nasCleanupRecoveryReads.Acquire(context.Background(), 14); err != nil {
		t.Fatal(err)
	}
	defer nasCleanupRecoveryReads.Release(14)
	started := make(chan struct{}, 3)
	release := make(chan struct{})
	var active, peak atomic.Int32
	s, _, item := recoveryReadServer(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet {
				n := active.Add(1)
				defer active.Add(-1)
				for old := peak.Load(); n > old; old = peak.Load() {
					if peak.CompareAndSwap(old, n) {
						break
					}
				}
				started <- struct{}{}
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	results := make(chan string, 3)
	var wg sync.WaitGroup
	for n := 0; n < 3; n++ {
		wg.Add(1)
		go func() { defer wg.Done(); status, _ := s.cleanupRecoveryFullOnce(ctx, item); results <- status }()
	}
	for n := 0; n < 2; n++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("read did not start")
		}
	}
	select {
	case <-started:
		t.Fatal("third request escaped shared bound")
	case <-time.After(25 * time.Millisecond):
	}
	cancel()
	close(release)
	wg.Wait()
	if peak.Load() != 2 {
		t.Fatalf("peak readers %d", peak.Load())
	}
	for n := 0; n < 3; n++ {
		if status := <-results; status != "r2_present_unverified" && status != "r2_sha_verified" {
			t.Fatalf("cancelled recovery %s", status)
		}
	}
	// Every reader and cancelled waiter must have released its exact units.
	verify, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	if err := nasCleanupRecoveryReads.Acquire(verify, 2); err != nil {
		t.Fatal("read slots leaked:", err)
	}
	nasCleanupRecoveryReads.Release(2)
}

func TestNASCleanupRecoveryReadGateBypassesConditionalHEADAndMD5(t *testing.T) {
	s, bucket, item := recoveryReadServer(t, nil)
	status, evidence := s.cleanupRecoveryFullOnce(context.Background(), item)
	if status != "r2_sha_verified" {
		t.Fatal(status)
	}
	if err := nasCleanupRecoveryReads.Acquire(context.Background(), 16); err != nil {
		t.Fatal(err)
	}
	defer nasCleanupRecoveryReads.Release(16)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if got := s.cleanupRecoveryBound(ctx, item, *evidence); got != "r2_etag_bound" {
		t.Fatal(got)
	}
	sum := md5.Sum(bucket.objects["object"])
	digest := hex.EncodeToString(sum[:])
	if got, _, ok := s.cleanupRecoveryMD5(ctx, item, digest, digest); !ok || got != "r2_md5_etag_matched" {
		t.Fatalf("MD5 %s %t", got, ok)
	}
	// A cancelled full-read waiter yields no verdict and performs no GET.
	before, _ := bucket.counts()
	short, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	if got, _ := s.cleanupRecoveryFullOnce(short, item); got != "r2_present_unverified" {
		t.Fatal(got)
	}
	after, _ := bucket.counts()
	if after != before {
		t.Fatal("full GET started without capacity")
	}
}

func TestNASCleanupRecoveryLongerReadCompletesWithinRealWriteDeadline(t *testing.T) {
	s, bucket, item := recoveryReadServer(t, nil)
	bucket.getDelay = 300 * time.Millisecond
	old := nasCleanupFullReadBudget
	defer func() { nasCleanupFullReadBudget = old }()
	// Scale production25/35/40/60-second bounds by1/100 for an actual HTTP server.
	for _, c := range []struct {
		read time.Duration
		want string
	}{{250 * time.Millisecond, "r2_present_unverified"}, {350 * time.Millisecond, "r2_sha_verified"}} {
		nasCleanupFullReadBudget = c.read
		srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), 400*time.Millisecond)
			defer cancel()
			got, _ := s.cleanupRecoveryFullOnce(ctx, item)
			fmt.Fprint(w, got)
		}))
		srv.Config.WriteTimeout = 600 * time.Millisecond
		srv.Start()
		response, err := http.Get(srv.URL)
		if err != nil {
			srv.Close()
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		srv.Close()
		if err != nil || string(body) != c.want {
			t.Fatalf("read budget %s: %q %v", c.read, body, err)
		}
	}
	if nasCleanupRecoveryBudget != 40*time.Second || nasCleanupExecutionBudget != 10*time.Second {
		t.Fatal("prefetch/execution deadline changed")
	}
}

func TestNASCleanupRecoveryReadsRetainProgressAcrossContendedRequests(t *testing.T) {
	f := newCleanupFixture(t, 24)
	_, _, ids := f.admit("read-pressure", syntheticFiles(100, 4, 4))
	if tasks := f.leaseV2(4, 0, 0, ""); len(tasks) != 4 {
		t.Fatalf("dry-run leases %d", len(tasks))
	}
	f.bucket.getDelay = 200 * time.Millisecond
	old := nasCleanupRecoveryBudget
	nasCleanupRecoveryBudget = 350 * time.Millisecond
	defer func() { nasCleanupRecoveryBudget = old }()
	if err := nasCleanupRecoveryReads.Acquire(context.Background(), 14); err != nil {
		t.Fatal(err)
	}
	defer nasCleanupRecoveryReads.Release(14)
	maps := make(chan map[int64]cleanupPrefetchedRecovery, 2)
	for n := 0; n < 2; n++ {
		part := map[int64]string{ids[n*2]: "", ids[n*2+1]: ""}
		go func() { maps <- f.s.cleanupPrefetchRecovery(f.ctx, 13, "", part) }()
	}
	verified := map[int64]bool{}
	for n := 0; n < 2; n++ {
		for id, result := range <-maps {
			if result.recovery == "r2_sha_verified" {
				verified[id] = true
			}
		}
	}
	if len(verified) != 2 {
		t.Fatalf("first contended requests retained%dverified results;want2", len(verified))
	}
	remaining := map[int64]string{}
	for _, id := range ids {
		if !verified[id] {
			remaining[id] = ""
		}
	}
	for id, result := range f.s.cleanupPrefetchRecovery(f.ctx, 13, "", remaining) {
		if result.recovery == "r2_sha_verified" {
			verified[id] = true
		}
	}
	if len(verified) != 4 {
		t.Fatalf("retry did not make progress: %d/4", len(verified))
	}
}
