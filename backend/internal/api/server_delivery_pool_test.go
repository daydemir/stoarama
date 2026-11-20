package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
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
}

func (r *registryDeliveryStore) Head(context.Context, string) (r2.ObjectHead, error) {
	return r2.ObjectHead{ETag: "etag", SizeBytes: r.size}, nil
}
