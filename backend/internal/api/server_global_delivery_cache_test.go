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
					body, got := c.get(context.Background(), key, fill(err))
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
		if _, kept := c.entries["bad"]; kept || c.entries["good"] == nil {
			t.Fatal("failed fill cached or verified fill dropped")
		}
		// A waiter whose request ends leaves without failing the fill.
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		wg.Go(func() {
			if _, err := c.get(ctx, "slow", fill(nil)); err != context.Canceled {
				t.Error("departed waiter", err)
			}
		})
		synctest.Wait()
		cancel()
		wg.Wait()
		release <- struct{}{}
		synctest.Wait()
		if c.entries["slow"] == nil || c.entries["slow"].lru == nil {
			t.Fatal("fill abandoned with its requester")
		}
	})
}

func TestGlobalDeliveryAssetCacheByteBoundEvictsLRU(t *testing.T) {
	var c globalDeliveryAssetCache
	const size = 3 << 20
	fills := map[string]int{}
	get := func(key string) {
		_, err := c.get(context.Background(), key, func() (globalDeliveryAssetBody, error) {
			fills[key]++
			return globalDeliveryAssetBody{data: make([]byte, size), length: size}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if c.bytes > globalDeliveryAssetCacheBytes {
			t.Fatalf("cache holds %d B past its %d B bound", c.bytes, globalDeliveryAssetCacheBytes)
		}
	}
	fit := int(globalDeliveryAssetCacheBytes / (size + 1024))
	for i := range fit {
		get(fmt.Sprint(i))
	}
	get("0") // most recently used: survives the next evictions
	get("new-1")
	get("new-2")
	if fills["0"] != 1 || c.entries["0"] == nil || c.entries["1"] != nil || c.entries["2"] != nil || c.entries["3"] == nil {
		t.Fatalf("eviction is not least recently used: fills=%v", fills)
	}
	get("1")
	if fills["1"] != 2 || c.order.Len() != len(c.entries) {
		t.Fatal("evicted entry not refilled")
	}
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
