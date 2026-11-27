package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func testAssetKey(name string) globalDeliveryAssetKey { return globalDeliveryAssetKey{sha256: name} }

func TestGlobalDeliveryAssetCacheSharesOneFill(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var c globalDeliveryAssetCache
		var fills atomic.Int32
		release := make(chan struct{})
		fill := func(err error) func() (globalDeliveryAssetBody, error) {
			return func() (globalDeliveryAssetBody, error) {
				fills.Add(1)
				<-release
				return globalDeliveryAssetBody{data: []byte("body"), length: 4}, err
			}
		}
		run := func(key string, err error) []error {
			errs := make([]error, 16)
			var wg sync.WaitGroup
			for i := range errs {
				wg.Go(func() {
					body, done, got := c.get(context.Background(), testAssetKey(key), fill(err))
					defer done()
					if got == nil && string(body.data) != "body" {
						t.Errorf("waiter %d got %q", i, body.data)
					}
					errs[i] = got
				})
			}
			synctest.Wait() // every waiter is parked on the one fill
			if n := fills.Load(); n != 1 {
				t.Fatalf("%d concurrent misses ran %d fills", len(errs), n)
			}
			release <- struct{}{}
			wg.Wait()
			fills.Store(0)
			return errs
		}
		for _, err := range run("good", nil) {
			if err != nil {
				t.Fatal(err)
			}
		}
		// A failed fill reaches every waiter and is not kept.
		for _, err := range run("bad", errGlobalDeliveryAssetUnverified) {
			if err != errGlobalDeliveryAssetUnverified {
				t.Fatal("shared failure", err)
			}
		}
		if _, kept := c.entries[testAssetKey("bad")]; kept || c.entries[testAssetKey("good")] == nil || c.entries[testAssetKey("good")].refs != 0 {
			t.Fatal("failed fill cached, verified fill dropped, or a reference leaked")
		}
		// A waiter whose request ends leaves without failing the fill.
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		wg.Go(func() {
			if _, _, err := c.get(ctx, testAssetKey("slow"), fill(nil)); err != context.Canceled {
				t.Error("departed waiter", err)
			}
		})
		synctest.Wait()
		cancel()
		wg.Wait()
		release <- struct{}{}
		synctest.Wait()
		if e := c.entries[testAssetKey("slow")]; e == nil || e.lru == nil || e.refs != 0 {
			t.Fatal("fill abandoned with its requester")
		}
	})
}

func TestGlobalDeliveryAssetCachePanickingFillFailsClosed(t *testing.T) {
	var c globalDeliveryAssetCache
	_, _, err := c.get(context.Background(), testAssetKey("p"), func() (globalDeliveryAssetBody, error) { panic("boom") })
	if err != errGlobalDeliveryAssetFailed || c.entries[testAssetKey("p")] != nil || c.pending != 0 {
		t.Fatal("panicking fill", err)
	}
	body, done, err := c.get(context.Background(), testAssetKey("p"), func() (globalDeliveryAssetBody, error) {
		return globalDeliveryAssetBody{data: []byte("ok"), length: 2}, nil
	})
	defer done()
	if err != nil || string(body.data) != "ok" {
		t.Fatal("panicked fill was kept", err)
	}
}

const testAssetSize = 3 << 20

func testAssetFill(fills map[string]int, key string) func() (globalDeliveryAssetBody, error) {
	return func() (globalDeliveryAssetBody, error) {
		fills[key]++
		return globalDeliveryAssetBody{data: make([]byte, testAssetSize), length: testAssetSize}, nil
	}
}

func TestGlobalDeliveryAssetCacheByteBoundEvictsLRU(t *testing.T) {
	var c globalDeliveryAssetCache
	fills := map[string]int{}
	get := func(key string) {
		_, done, err := c.get(context.Background(), testAssetKey(key), testAssetFill(fills, key))
		if err != nil {
			t.Fatal(err)
		}
		done()
		if c.bytes > globalDeliveryAssetCacheBytes {
			t.Fatalf("cache holds %d B past its %d B bound", c.bytes, globalDeliveryAssetCacheBytes)
		}
	}
	fit := int(globalDeliveryAssetCacheBytes / (testAssetSize + globalDeliveryAssetEntryOverhead))
	for i := range fit {
		get(fmt.Sprint(i))
	}
	get("0") // most recently used: survives the next evictions
	get("new-1")
	get("new-2")
	if fills["0"] != 1 || c.entries[testAssetKey("0")] == nil || c.entries[testAssetKey("1")] != nil || c.entries[testAssetKey("2")] != nil || c.entries[testAssetKey("3")] == nil {
		t.Fatalf("eviction is not least recently used: fills=%v", fills)
	}
	get("1")
	if fills["1"] != 2 || c.order.Len() != len(c.entries) {
		t.Fatal("evicted entry not refilled")
	}
}

// A body a response still writes is never evicted, so slow clients cannot
// push the cache's memory past its bound: past it, misses and inflating
// responses go to the slot path.
func TestGlobalDeliveryAssetCacheHeldBodiesStayWithinBound(t *testing.T) {
	var c globalDeliveryAssetCache
	fills := map[string]int{}
	fit := int(globalDeliveryAssetCacheBytes / (testAssetSize + globalDeliveryAssetEntryOverhead))
	var held []func()
	for i := range fit {
		body, done, err := c.get(context.Background(), testAssetKey(fmt.Sprint(i)), testAssetFill(fills, fmt.Sprint(i)))
		if err != nil || body.uncached {
			t.Fatal(i, err)
		}
		held = append(held, done)
	}
	body, done, err := c.get(context.Background(), testAssetKey("late"), testAssetFill(fills, "late"))
	done()
	if err != nil || !body.uncached || c.entries[testAssetKey("late")] != nil || len(c.entries) != fit {
		t.Fatal("a body was admitted past the bound by evicting one still being written")
	}
	if c.bytes > globalDeliveryAssetCacheBytes {
		t.Fatalf("cache charges %d B past its bound", c.bytes)
	}
	uncharge, ok := c.charge(globalDeliveryInflateBytes)
	for ; ok; uncharge, ok = c.charge(globalDeliveryInflateBytes) {
		defer uncharge()
	}
	if c.bytes > globalDeliveryAssetCacheBytes || c.bytes+globalDeliveryInflateBytes <= globalDeliveryAssetCacheBytes {
		t.Fatalf("inflate charges stop at %d B of a %d B bound", c.bytes, globalDeliveryAssetCacheBytes)
	}
	held[0]()
	body, done, err = c.get(context.Background(), testAssetKey("late"), testAssetFill(fills, "late"))
	defer done()
	if err != nil || body.uncached || c.entries[testAssetKey("0")] != nil {
		t.Fatal("released body not evicted for a new one")
	}
	for _, release := range held[1:] {
		release()
	}
}

func TestGlobalDeliveryAssetCacheBoundsPendingFills(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var c globalDeliveryAssetCache
		release := make(chan struct{})
		var wg sync.WaitGroup
		for i := range globalDeliveryAssetPendingFills {
			wg.Go(func() {
				_, done, _ := c.get(context.Background(), testAssetKey(fmt.Sprint(i)), func() (globalDeliveryAssetBody, error) {
					<-release
					return globalDeliveryAssetBody{}, nil
				})
				done()
			})
		}
		synctest.Wait()
		body, _, err := c.get(context.Background(), testAssetKey("next"), func() (globalDeliveryAssetBody, error) {
			t.Error("fill started past the pending bound")
			return globalDeliveryAssetBody{}, nil
		})
		if err != nil || !body.uncached || c.entries[testAssetKey("next")] != nil {
			t.Fatal("miss past the pending bound not sent to the slot path")
		}
		close(release)
		wg.Wait()
	})
}

func TestGlobalDeliveryAssetGzipCapsEntries(t *testing.T) {
	random := make([]byte, globalDeliveryAssetEntryBytes)
	_, _ = rand.Read(random)
	growth := benchJSON(8_300_000)
	if body := gzipGlobalDeliveryAsset(func(w io.Writer) { _, _ = w.Write(random) }); !body.uncached || body.data != nil {
		t.Fatal("incompressible body over the entry cap cached")
	}
	body := gzipGlobalDeliveryAsset(func(w io.Writer) { _, _ = w.Write(growth) })
	if body.uncached || !body.gzipped || body.length != int64(len(growth)) || len(body.data) != cap(body.data) || len(body.data) > globalDeliveryAssetEntryBytes {
		t.Fatalf("growth.json: uncached=%v %d B (cap %d)", body.uncached, len(body.data), cap(body.data))
	}
	zr, err := gzip.NewReader(bytes.NewReader(body.data))
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := io.ReadAll(zr); !bytes.Equal(got, growth) {
		t.Fatal("gzipped body differs")
	}
}

// globalDeliveryCacheTestAssets registers each JSON body as a public asset of
// reg and stores it in store.
func globalDeliveryCacheTestAssets(reg *globalDeliveryRegistry, store globalDeliveryMapStore, bodies map[string][]byte) {
	for name, body := range bodies {
		sum := sha256.Sum256(body)
		key := globalDeliveryPrefix + "assets/" + hex.EncodeToString(sum[:])
		store[key] = body
		contentType := "application/json"
		if !bytes.HasPrefix(body, []byte("{")) {
			contentType = "image/jpeg"
		}
		reg.Assets[name] = globalDeliveryObject{Key: key, ETag: "etag", SHA256: hex.EncodeToString(sum[:]), SizeBytes: int64(len(body)), ContentType: contentType, Verified: true}
	}
}

func globalDeliveryCacheTestRequest(name, generation string, gzipped bool) *http.Request {
	r := httptest.NewRequest(http.MethodGet, publicGlobalDeliveryAPI+"/metadata?generation="+generation+"&path="+url.QueryEscape(name), nil)
	if gzipped {
		r.Header.Set("Accept-Encoding", "gzip")
	}
	return r
}

func TestPublicGlobalDeliveryCacheHitsBypassAssetSlots(t *testing.T) {
	reg := globalDeliveryTestRegistry()
	reg.Generation, reg.PublicDelivery, reg.SelectionScope = strings.Repeat("e", 32), true, "available"
	store := globalDeliveryMapStore{}
	catalog, growth := benchJSON(90<<10), benchJSON(1<<20)
	globalDeliveryCacheTestAssets(reg, store, map[string][]byte{"/public/catalog.json": catalog, "/public/growth.json": growth})
	s := publicDeliveryTestServer(t, store, reg)
	serve := func(ctx context.Context, name string, gzipped bool) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		s.handleGlobalDeliveryAsset(w, globalDeliveryCacheTestRequest(name, reg.Generation, gzipped).WithContext(ctx))
		return w
	}
	if w := serve(context.Background(), "/public/catalog.json", true); w.Code != http.StatusOK {
		t.Fatal("warm", w.Code)
	}

	for range cap(globalDeliveryAssetSlots) {
		globalDeliveryAssetSlots <- struct{}{}
	}
	held := true
	releaseSlots := func() {
		if held {
			for range cap(globalDeliveryAssetSlots) {
				<-globalDeliveryAssetSlots
			}
			held = false
		}
	}
	defer releaseSlots()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Go(func() {
			gzipped := i%2 == 0
			w := serve(ctx, "/public/catalog.json", gzipped)
			body, sent := w.Body.Bytes(), strconv.Itoa(w.Body.Len())
			if gzipped {
				if w.Header().Get("Content-Encoding") != "gzip" {
					t.Error("hit not gzipped")
					return
				}
				zr, err := gzip.NewReader(w.Body)
				if err != nil {
					t.Error(err)
					return
				}
				body, _ = io.ReadAll(zr)
			}
			if w.Code != http.StatusOK || !bytes.Equal(body, catalog) || w.Header().Get("Content-Length") != sent {
				t.Errorf("hit with every slot held: status %d, %d B, Content-Length %s", w.Code, len(body), w.Header().Get("Content-Length"))
			}
		})
	}
	wg.Wait()
	if ctx.Err() != nil {
		t.Fatal("cache hits waited on the asset slots")
	}
	// A miss still waits for a slot.
	missCtx, missCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer missCancel()
	if w := serve(missCtx, "/public/growth.json", true); w.Body.Len() != 0 || w.Header().Get("Content-Type") != "" {
		t.Fatal("miss served while every slot was held")
	}
	releaseSlots()
	w := serve(context.Background(), "/public/growth.json", false)
	if w.Code != http.StatusOK || !bytes.Equal(w.Body.Bytes(), growth) {
		t.Fatal("miss after slots freed", w.Code)
	}
}

func TestGlobalDeliveryAssetOverEntryCapStreamsEachRequest(t *testing.T) {
	large := make([]byte, globalDeliveryAssetEntryBytes+1)
	_, _ = rand.Read(large)
	sum := sha256.Sum256(large)
	reg := globalDeliveryTestRegistry()
	o := reg.Assets["/fixture.txt"]
	o.SHA256, o.SizeBytes = hex.EncodeToString(sum[:]), int64(len(large))
	reg.Assets["/fixture.txt"] = o
	m := &globalDeliveryTestStore{body: large}
	s := &Server{globalDeliveryStore: m, globalDeliveryRegistry: reg, globalDeliveryRegistryAt: time.Now()}
	for i := 1; i <= 2; i++ {
		w := httptest.NewRecorder()
		s.handleGlobalDeliveryAsset(w, globalDeliveryTestRequest("GET", "/?path=/fixture.txt", "", &accountPrincipal{AccountID: 47}))
		// The first read fills an uncached marker; later requests skip the fill.
		if w.Code != http.StatusOK || !bytes.Equal(w.Body.Bytes(), large) || m.opened != i+1 {
			t.Fatalf("request %d: status %d, %d B, %d reads", i, w.Code, w.Body.Len(), m.opened)
		}
	}
	if s.globalDeliveryAssets.bytes > 1024 {
		t.Fatal("over-cap body counted in the cache", s.globalDeliveryAssets.bytes)
	}
}

func TestPublicGlobalDeliveryManifestEdgeBodies(t *testing.T) {
	generation := strings.Repeat("f", 32)
	serve := func(t *testing.T, body []byte, contentType string) (*globalDeliveryTestStore, func() *httptest.ResponseRecorder) {
		sum := sha256.Sum256(body)
		reg := &globalDeliveryRegistry{Generation: generation, Assets: map[string]globalDeliveryObject{
			memoryTestManifestPath: {Key: "k", ETag: "etag", ContentType: contentType, SHA256: hex.EncodeToString(sum[:]), SizeBytes: int64(len(body))},
		}}
		store := &globalDeliveryTestStore{body: body}
		s := publicDeliveryTestServer(t, store, reg)
		return store, func() *httptest.ResponseRecorder {
			w := httptest.NewRecorder()
			s.handleGlobalDeliveryAsset(w, globalDeliveryCacheTestRequest(memoryTestManifestPath, generation, true))
			return w
		}
	}
	t.Run("null manifest fails closed", func(t *testing.T) {
		store, get := serve(t, []byte("null"), "application/json")
		for i := 1; i <= 2; i++ {
			if w := get(); w.Code != http.StatusConflict || store.opened != i {
				t.Fatalf("request %d: status %d after %d reads", i, w.Code, store.opened)
			}
		}
	})
	t.Run("non-JSON manifest is stamped every time", func(t *testing.T) {
		store, get := serve(t, []byte(`{"outputs":[{"id":1}]}`), "application/octet-stream")
		for i := 1; i <= 2; i++ {
			w := get()
			if w.Code != http.StatusOK || store.opened != i || w.Header().Get("Content-Encoding") != "" ||
				!strings.Contains(w.Body.String(), `"delivery_generation":"`+generation+`"`) || !strings.Contains(w.Body.String(), `"generation":"`+generation+`"`) {
				t.Fatalf("request %d: status %d after %d reads: %s", i, w.Code, store.opened, w.Body.String())
			}
		}
	})
}
