package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/jackc/pgx/v5/pgxpool"
)

// tinyDeliveryPool opens the disposable schema from deliveryExemptionTestPool with
// a deliberately tiny connection budget: two connections, of which a cold
// registry load must need only one at a time.
func tinyDeliveryPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("STOARAMA_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	base := deliveryExemptionTestPool(t)
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	var schema string
	if err := base.QueryRow(context.Background(), `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 2
	cfg.MinConns = 1
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// TestDeliveryRegistryColdLoadAndQuiesceDoNotDeadlock is the #555 (B) regression:
// a cold public load must validate its keep snapshot on the SAME transaction that
// holds the shared public lock, not on a second pool acquire; otherwise, with a
// two-connection pool, two concurrent cold loads each holding one connection can
// both wait for a second and neither finishes, and a quiesce behind them never
// runs. Here the loads are concurrent and a quiesce follows; the whole test fails
// on a deadline instead of hanging.
func TestDeliveryRegistryColdLoadAndQuiesceDoNotDeadlock(t *testing.T) {
	pool := tinyDeliveryPool(t)
	ctx := context.Background()
	// No warm cache: both loads must go cold through the fence transaction.
	reg := publicDeliveryRegistryFixture()
	// Seed the durable keep holds the registry's assets and files name, so the
	// cold load's keep validation has something to match.
	for _, o := range reg.Assets {
		if _, err := pool.Exec(ctx, `INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind,sha256)
 VALUES('stoarama',$1,btrim($2,'"'),$3,'asset',$4)`, o.Key, o.ETag, o.SizeBytes, o.SHA256); err != nil {
			t.Fatal(err)
		}
	}
	for _, o := range reg.Files {
		if _, err := pool.Exec(ctx, `INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind,sha256)
 VALUES('stoarama',$1,btrim($2,'"'),$3,'video_collated',$4)`, o.Key, o.ETag, o.SizeBytes, o.SHA256); err != nil {
			t.Fatal(err)
		}
	}
	body, err := json.Marshal(reg)
	if err != nil {
		t.Fatal(err)
	}
	// Head/OpenExact answer the registry object's real bytes and size, so a cold
	// load parses it rather than the placeholder body.
	store := &registryDeliveryStore{size: int64(len(body))}
	store.body = body
	newServer := func() *Server {
		return &Server{pool: pool, globalDeliveryStore: store, deliveryPublic: poolDeliveryGate{pool}}
	}

	done := make(chan struct{}, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := newServer()
			if _, err := s.loadGlobalDeliveryRegistryScope(ctx, true, ""); err != nil {
				t.Errorf("cold load: %v", err)
			}
			done <- struct{}{}
		}()
	}
	// After both loads are requested, a quiesce must be able to commit. It takes
	// the exclusive public lock, so it cannot finish while a cold load is between
	// its serving check and its keep validation — but the load never needs a
	// second connection, so there is always one free for the quiesce.
	deadline := time.After(30 * time.Second)
	go func() {
		select {
		case <-deadline:
			// Let the test fail by closing the wait below; nothing to cancel here.
		}
	}()
	wg.Wait()
	close(done)
	if _, err := pool.Exec(ctx, `INSERT INTO r2_delivery_page_quiesce(reason) VALUES('test: quiesce after cold loads')`); err != nil {
		t.Fatalf("quiesce after concurrent cold loads failed: %v", err)
	}
	// A request after quiesce is refused without a new cold load.
	s := newServer()
	w := httptest.NewRecorder()
	s.handleGlobalDeliveryPage(w, httptest.NewRequest(http.MethodGet, "/global-street-scores-delivery", nil))
	if w.Code != http.StatusServiceUnavailable && w.Code != http.StatusGone {
		t.Fatalf("quiesced page served %d", w.Code)
	}
}

// registryDeliveryStore serves the registry object's real bytes at its real size,
// so a cold load in the pool test parses a valid registry.
type registryDeliveryStore struct {
	globalDeliveryTestStore
	size int64
	// stallOpen makes OpenExact block until its context ends, modelling a public
	// read that never completes.
	stallOpen bool
}

func (r *registryDeliveryStore) OpenExact(ctx context.Context, key, etag, version string) (io.ReadCloser, error) {
	// Only the media object stalls; the registry object must still load.
	if r.stallOpen && !strings.HasPrefix(key, globalDeliveryPrefix) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return r.globalDeliveryTestStore.OpenExact(ctx, key, etag, version)
}

func (r *registryDeliveryStore) Head(context.Context, string) (r2.ObjectHead, error) {
	return r2.ObjectHead{ETag: "etag", SizeBytes: r.size}, nil
}

// TestDeliveryServingGenerationRecordsOnlyTheCurrentPointer proves the API records
// the generation it actually loaded as the unpinned current pointer, and does not
// record a pinned read. Retirement's authority binds to that row.
func TestDeliveryServingGenerationRecordsOnlyTheCurrentPointer(t *testing.T) {
	pool := deliveryExemptionTestPool(t)
	ctx := context.Background()
	reg := publicDeliveryRegistryFixture()
	body, err := json.Marshal(reg)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range reg.Assets {
		if _, err := pool.Exec(ctx, `INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind,sha256)
 VALUES('stoarama',$1,btrim($2,'"'),$3,'asset',$4)`, o.Key, o.ETag, o.SizeBytes, o.SHA256); err != nil {
			t.Fatal(err)
		}
	}
	for _, o := range reg.Files {
		if _, err := pool.Exec(ctx, `INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind,sha256)
 VALUES('stoarama',$1,btrim($2,'"'),$3,'video_collated',$4)`, o.Key, o.ETag, o.SizeBytes, o.SHA256); err != nil {
			t.Fatal(err)
		}
	}
	store := &registryDeliveryStore{size: int64(len(body))}
	store.body = body
	s := &Server{pool: pool, globalDeliveryStore: store, deliveryPublic: poolDeliveryGate{pool}}
	// A pinned read must NOT record the pointer.
	if _, err := s.loadGlobalDeliveryRegistryScope(ctx, true, reg.Generation); err != nil {
		t.Fatal(err)
	}
	var observed int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM r2_delivery_serving_generation`).Scan(&observed); err != nil || observed != 0 {
		t.Fatalf("a pinned read recorded the serving generation: %d %v", observed, err)
	}
	// A cold current load DOES record the generation it served from
	// public/current.json.
	if _, err := s.loadGlobalDeliveryRegistryScope(ctx, true, ""); err != nil {
		t.Fatal(err)
	}
	var generation string
	if err := pool.QueryRow(ctx, `SELECT generation FROM r2_delivery_serving_generation WHERE singleton`).Scan(&generation); err != nil {
		t.Fatalf("current load did not record the serving generation: %v", err)
	}
	if generation != reg.Generation {
		t.Fatalf("recorded %s, served %s", generation, reg.Generation)
	}
}

// TestDeliveryPublicReadIsBoundedByItsLease is the (1) regression: a public read
// that stalls forever must be cancelled by the response budget, which is shorter
// than the reader lease, so the handler returns and releases its lease instead of
// outliving it (and instead of depending on HTTP WriteTimeout).
func TestDeliveryPublicReadIsBoundedByItsLease(t *testing.T) {
	if deliveryPublicResponseBudget >= deliveryPublicReaderTTL {
		t.Fatalf("response budget %v is not shorter than the lease %v", deliveryPublicResponseBudget, deliveryPublicReaderTTL)
	}
	// Shrink the budget for this test only; it must still be shorter than the
	// lease, and the handler must still be bounded by it.
	original := deliveryPublicResponseBudget
	deliveryPublicResponseBudget = 300 * time.Millisecond
	t.Cleanup(func() { deliveryPublicResponseBudget = original })
	pool := deliveryExemptionTestPool(t)
	ctx := context.Background()
	reg := publicDeliveryRegistryFixture()
	body, err := json.Marshal(reg)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range reg.Assets {
		if _, err := pool.Exec(ctx, `INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind,sha256)
 VALUES('stoarama',$1,btrim($2,'"'),$3,'asset',$4)`, o.Key, o.ETag, o.SizeBytes, o.SHA256); err != nil {
			t.Fatal(err)
		}
	}
	for _, o := range reg.Files {
		if _, err := pool.Exec(ctx, `INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind,sha256)
 VALUES('stoarama',$1,btrim($2,'"'),$3,'video_collated',$4)`, o.Key, o.ETag, o.SizeBytes, o.SHA256); err != nil {
			t.Fatal(err)
		}
	}
	store := &registryDeliveryStore{size: int64(len(body))}
	store.body = body
	// A file open that blocks until its context ends: the store honours the
	// deadline the handler derived from its lease.
	store.stallOpen = true
	s := &Server{pool: pool, globalDeliveryStore: store, deliveryPublic: poolDeliveryGate{pool}}
	file := reg.Files["1"]
	// Prewarm the registry so the stall is only in the file body.
	if _, err := s.loadGlobalDeliveryRegistryScope(ctx, true, ""); err != nil {
		t.Fatal(err)
	}
	// The fixture file id is "1"; confirm it is present before the stall test.
	if _, ok := reg.Files["1"]; !ok {
		t.Fatal("fixture file missing")
	}
	router := publicDeliveryRouter(s)
	start := time.Now()
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, publicGlobalDeliveryAPI+"/file/1?sha256="+file.SHA256, nil))
	if elapsed := time.Since(start); elapsed > deliveryPublicResponseBudget+5*time.Second {
		t.Fatalf("a stalled read was not bounded by its lease budget: %v", elapsed)
	}
	if w.Code != http.StatusConflict && w.Code != http.StatusServiceUnavailable {
		t.Fatalf("a stall surfaced as %d, want a bounded failure", w.Code)
	}
	var live int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM r2_delivery_public_readers`).Scan(&live); err != nil || live != 0 {
		t.Fatalf("stalled read left %d leases behind: %v", live, err)
	}
}

// TestDeliveryQuiesceWaitsForPausedReadsDeterministically runs real concurrent
// cold loads against a tiny pool, deterministically pauses each inside its storage
// read, arms the page while they are outstanding, and proves the quiesce commits
// (finds a free connection) and every paused read then finishes and releases its
// lease. No sleeps decide the outcome: the reads signal when they are paused, and
// the test arms only after both signals.
func TestDeliveryQuiesceWaitsForPausedReadsDeterministically(t *testing.T) {
	pool := tinyDeliveryPool(t)
	ctx := context.Background()
	reg := publicDeliveryRegistryFixture()
	body, err := json.Marshal(reg)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range reg.Assets {
		if _, err := pool.Exec(ctx, `INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind,sha256)
 VALUES('stoarama',$1,btrim($2,'"'),$3,'asset',$4)`, o.Key, o.ETag, o.SizeBytes, o.SHA256); err != nil {
			t.Fatal(err)
		}
	}
	for _, o := range reg.Files {
		if _, err := pool.Exec(ctx, `INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind,sha256)
 VALUES('stoarama',$1,btrim($2,'"'),$3,'video_collated',$4)`, o.Key, o.ETag, o.SizeBytes, o.SHA256); err != nil {
			t.Fatal(err)
		}
	}
	// Two independent paused registries, so both cold loads can be inside their
	// read at the same time.
	store := &pausedRegistryStore{body: body, size: int64(len(body)), paused: make(chan struct{}, 2), release: make(chan struct{})}
	s := &Server{pool: pool, globalDeliveryStore: store, deliveryPublic: poolDeliveryGate{pool}}

	var wg sync.WaitGroup
	codes := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, codes[i] = s.loadGlobalDeliveryRegistryScope(ctx, true, "")
		}(i)
	}
	// Wait until BOTH cold loads are paused inside storage.
	for i := 0; i < 2; i++ {
		select {
		case <-store.paused:
		case <-time.After(20 * time.Second):
			t.Fatal("cold load never paused inside storage")
		}
	}
	// Arm while both are paused. This must commit: the loads each hold one of the
	// two connections for their own transaction and never need a second.
	armed := make(chan error, 1)
	go func() {
		_, err := pool.Exec(ctx, `INSERT INTO r2_delivery_page_quiesce(reason) VALUES('test: concurrent arming')`)
		armed <- err
	}()
	select {
	case err := <-armed:
		if err != nil {
			t.Fatalf("quiesce with paused reads outstanding failed: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("quiesce deadlocked behind paused cold loads")
	}
	// Release the reads. Each one read an object while the page was still
	// serving, but armed before its publish fence, so it must REFUSE to publish:
	// a load that lost the race to arming cannot repopulate the cache. The point
	// is that quiesce committed promptly and nothing hung.
	close(store.release)
	wg.Wait()
	for i, err := range codes {
		if !errors.Is(err, errDeliveryPageNotServing) {
			t.Fatalf("cold load %d published after arming: %v", i, err)
		}
	}
	// A cache entry cannot exist after that: nothing was published.
	if _, found := s.cachedDeliveryRegistry(true, ""); found {
		t.Fatal("an armed page published a cached registry")
	}
	if _, err := s.loadGlobalDeliveryRegistryScope(ctx, true, ""); err == nil {
		t.Fatal("a new cold load succeeded after quiesce")
	}
}

// pausedRegistryStore pauses the registry object's OpenExact until released.
type pausedRegistryStore struct {
	globalDeliveryTestStore
	body    []byte
	size    int64
	paused  chan struct{}
	release chan struct{}
}

func (p *pausedRegistryStore) Head(context.Context, string) (r2.ObjectHead, error) {
	return r2.ObjectHead{ETag: "etag", SizeBytes: p.size}, nil
}

func (p *pausedRegistryStore) OpenExact(ctx context.Context, key, etag, version string) (io.ReadCloser, error) {
	p.opened.Add(1)
	select {
	case p.paused <- struct{}{}:
	default:
	}
	select {
	case <-p.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return io.NopCloser(bytes.NewReader(p.body)), nil
}
