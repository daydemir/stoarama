package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
)

// stageWireFixture is a loopback provider for the stage wire. It answers one
// HEAD with a chosen generation and one DELETE with a chosen outcome, and
// counts every request so a retry or a second DELETE is visible.
type stageWireFixture struct {
	heads, deletes atomic.Int32
	deleteStatus   int
	holdDelete     chan struct{}
	// loseDelete closes the connection after reading the request, so the
	// transport must report an ambiguous, never-retried outcome.
	loseDelete atomic.Bool
	srv        *httptest.Server
}

func newStageWireFixture(t *testing.T) *stageWireFixture {
	t.Helper()
	f := &stageWireFixture{deleteStatus: http.StatusNoContent}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			f.heads.Add(1)
			w.Header().Set("ETag", `"stage-etag"`)
			w.Header().Set("Content-Length", "100")
			w.Header().Set("X-Amz-Meta-Sha256", strings.Repeat("a", 64))
			w.Header().Set("Last-Modified", time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC).Format(http.TimeFormat))
		case http.MethodDelete:
			f.deletes.Add(1)
			if f.holdDelete != nil {
				select {
				case <-f.holdDelete:
				case <-r.Context().Done():
					return
				}
			}
			if f.loseDelete.Load() {
				if hj, ok := w.(http.Hijacker); ok {
					if conn, _, err := hj.Hijack(); err == nil {
						conn.Close()
						return
					}
				}
				return
			}
			w.WriteHeader(f.deleteStatus)
		default:
			t.Errorf("stage wire sent unexpected %s", r.Method)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *stageWireFixture) wire(t *testing.T) *durhamStageWire {
	t.Helper()
	store, err := newInverseSDKStore(context.Background(), r2ConfigFor(t, f.srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	return &durhamStageWire{store: store}
}

func (f *stageWireFixture) target() r2.StitchStageTarget {
	return r2.StitchStageTarget{Bucket: "stoarama",
		EndpointSHA256: sha256HexOf(f.srv.URL),
		Key:            "stitch-staging/00000000-0000-0000-0000-000000000123/" + strings.Repeat("a", 64) + ".mp4",
		ETag:           "stage-etag",
		SHA256:         strings.Repeat("a", 64),
		SizeBytes:      100,
		LastModified:   time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)}
}

func sha256HexOf(v string) string {
	sum := sha256.Sum256([]byte(strings.TrimRight(v, "/")))
	return hex.EncodeToString(sum[:])
}

func r2ConfigFor(t *testing.T, endpoint string) r2.Config {
	t.Helper()
	return r2.Config{Bucket: "stoarama", Endpoint: endpoint, Region: "auto",
		AccessKey: "test", SecretKey: "test"}
}

func TestDurhamStageWireSendsExactlyOneDelete(t *testing.T) {
	f := newStageWireFixture(t)
	w := f.wire(t)
	ctx := context.Background()
	target := f.target()
	deadline := time.Now().Add(10 * time.Second)
	head, err := w.ExactHead(ctx, target, deadline)
	if err != nil {
		t.Fatal(err)
	}
	if head.ETag != "stage-etag" || head.SizeBytes != 100 || head.SHA256Metadata != target.SHA256 ||
		!head.LastModified.Equal(target.LastModified) {
		t.Fatalf("exact HEAD identity differs: %+v", head)
	}
	var rechecks atomic.Int32
	recheck := func(context.Context) error { rechecks.Add(1); return nil }
	if err := w.DeleteOnce(ctx, target, deadline, recheck); err != nil {
		t.Fatal(err)
	}
	// The shared transport checks at admission and again at the first byte,
	// exactly as it does for the RAW lanes.
	if got := f.deletes.Load(); got != 1 || rechecks.Load() != 2 {
		t.Fatalf("stage wire sent %d DELETEs after %d rechecks", got, rechecks.Load())
	}
}

func TestDurhamStageWireRefusesTheFirstByteWhenRecheckFails(t *testing.T) {
	f := newStageWireFixture(t)
	w := f.wire(t)
	denied := errors.New("stage generation changed")
	err := w.DeleteOnce(context.Background(), f.target(), time.Now().Add(10*time.Second),
		func(context.Context) error { return denied })
	if !errors.Is(err, denied) {
		t.Fatalf("stage wire ignored the pre-byte recheck: %v", err)
	}
	if got := f.deletes.Load(); got != 0 {
		t.Fatalf("stage wire sent %d DELETEs without its recheck", got)
	}
}

func TestDurhamStageWireRefusesTheFirstByteAfterTheDeadline(t *testing.T) {
	f := newStageWireFixture(t)
	w := f.wire(t)
	err := w.DeleteOnce(context.Background(), f.target(), time.Now().Add(-time.Millisecond),
		func(context.Context) error { return nil })
	if err == nil {
		t.Fatal("stage wire sent a DELETE after its deadline")
	}
	if got := f.deletes.Load(); got != 0 {
		t.Fatalf("stage wire sent %d DELETEs past its deadline", got)
	}
}

func TestDurhamStageWireReportsALostDeleteAsAmbiguousWithoutRetry(t *testing.T) {
	f := newStageWireFixture(t)
	f.loseDelete.Store(true)
	w := f.wire(t)
	err := w.DeleteOnce(context.Background(), f.target(), time.Now().Add(10*time.Second),
		func(context.Context) error { return nil })
	if !errors.Is(err, errInverseWireAmbiguous) {
		t.Fatalf("lost stage DELETE was not ambiguous: %v", err)
	}
	if got := f.deletes.Load(); got != 1 {
		t.Fatalf("lost stage DELETE was retried: %d requests", got)
	}
}

func TestDurhamStageWireNeverRetriesAProviderError(t *testing.T) {
	f := newStageWireFixture(t)
	f.deleteStatus = http.StatusServiceUnavailable
	w := f.wire(t)
	err := w.DeleteOnce(context.Background(), f.target(), time.Now().Add(10*time.Second),
		func(context.Context) error { return nil })
	if err == nil || errors.Is(err, errInverseWireAmbiguous) {
		t.Fatalf("answered stage DELETE was classified as ambiguous: %v", err)
	}
	if got := f.deletes.Load(); got != 1 {
		t.Fatalf("answered stage DELETE was retried: %d requests", got)
	}
}

// The DELETE's key is the authority's own exact key, never a second argument
// that could disagree with it. A target the wire refuses still sends nothing,
// and a hand-built authority whose target names a non-stage key cannot smuggle
// that key onto the wire: the transport's path guard and the request path both
// come from the same authority value.
func TestDurhamStageTransportRefusesAnotherKey(t *testing.T) {
	f := newStageWireFixture(t)
	store, err := newInverseSDKStore(context.Background(), r2ConfigFor(t, f.srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	target := f.target()
	target.EndpointSHA256 = store.EndpointSHA256()
	target.Key = "managed/acct-47/raw.mp4"
	target.SHA256 = strings.Repeat("a", 64)
	if err := (&durhamStageWire{store: store}).DeleteOnce(context.Background(), target,
		time.Now().Add(10*time.Second), func(context.Context) error { return nil }); err == nil {
		t.Fatal("stage wire accepted a target outside its staging key")
	}
	if got := f.deletes.Load(); got != 0 {
		t.Fatalf("stage wire reached the provider for a non-stage key: %d", got)
	}
}

// A nil authority carries no key to read and no one-shot state to consume, so
// it is refused before anything is dereferenced rather than reaching the SDK
// with an empty key.
func TestDurhamStageTransportRefusesANilAuthority(t *testing.T) {
	f := newStageWireFixture(t)
	store, err := newInverseSDKStore(context.Background(), r2ConfigFor(t, f.srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	err = store.deleteOneWire(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "wire authority") {
		t.Fatalf("nil authority: %v", err)
	}
	if got := f.deletes.Load(); got != 0 {
		t.Fatalf("nil authority reached the provider: %d", got)
	}
}
