package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func globalDeliveryTestRegistry() *globalDeliveryRegistry {
	sha := sha256.Sum256([]byte("hello"))
	ids := make([]int64, 49)
	for i := range ids {
		ids[i] = int64(i + 1)
	}
	o := globalDeliveryObject{Key: globalDeliveryPrefix + "assets/fixture.txt", ETag: "etag", SHA256: hex.EncodeToString(sha[:]), SizeBytes: 5, ContentType: "text/plain", Verified: true}
	f := o
	f.Key = "original/cohort/video.mp4"
	f.Path = "1_Test/May/Monday/1_Test_2026_May_W1_Monday_hour_01_part_01.mp4"
	f.RecordingID = 1
	f.ContentType = "video/mp4"
	return &globalDeliveryRegistry{SchemaVersion: 1, AccountID: 47, RecordingIDs: ids, Assets: map[string]globalDeliveryObject{"/fixture.txt": o}, Files: map[string]globalDeliveryObject{"1": f}}
}

type globalDeliveryTestStore struct {
	body              []byte
	opened, presigned int
}

func (m *globalDeliveryTestStore) Head(context.Context, string) (r2.ObjectHead, error) {
	return r2.ObjectHead{ETag: "etag", SizeBytes: 5}, nil
}
func (m *globalDeliveryTestStore) HeadExact(context.Context, string, string, string) (r2.ObjectHead, error) {
	return r2.ObjectHead{ETag: "etag", SizeBytes: 5}, nil
}
func (m *globalDeliveryTestStore) Get(context.Context, string) ([]byte, error) { return m.body, nil }
func (m *globalDeliveryTestStore) OpenExact(context.Context, string, string, string) (io.ReadCloser, error) {
	m.opened++
	return io.NopCloser(bytes.NewReader(m.body)), nil
}
func (m *globalDeliveryTestStore) OpenExactRange(_ context.Context, _ string, _ string, _ string, a, b int64) (io.ReadCloser, error) {
	m.opened++
	return io.NopCloser(bytes.NewReader(m.body[a : b+1])), nil
}
func (m *globalDeliveryTestStore) PresignGetExactDownloadRequest(context.Context, string, string, string, string, time.Duration) (r2.PresignedRequest, error) {
	m.presigned++
	return r2.PresignedRequest{URL: "https://test.r2.cloudflarestorage.com/bucket/a%20b.mp4?X-Amz-Expires=300", Method: "GET", Headers: http.Header{"If-Match": {`"etag"`}, "Host": {"test.r2.cloudflarestorage.com"}}}, nil
}
func globalDeliveryTestRequest(method, target, id string, p *accountPrincipal) *http.Request {
	r := httptest.NewRequest(method, target, nil)
	ctx := r.Context()
	if p != nil {
		ctx = context.WithValue(ctx, accountPrincipalContextKey, *p)
	}
	route := chi.NewRouteContext()
	route.URLParams.Add("fileID", id)
	ctx = context.WithValue(ctx, chi.RouteCtxKey, route)
	return r.WithContext(ctx)
}
func TestGlobalDeliveryAuthorizationPrecedesStorage(t *testing.T) {
	pullID := int64(1)
	cases := []struct {
		name   string
		p      *accountPrincipal
		status int
	}{{"anonymous", nil, 401}, {"other org", &accountPrincipal{AccountID: 48}, 403}, {"pull key", &accountPrincipal{AccountID: 47, APIKeyID: &pullID, KeyScopes: []string{accountScopePull}}, 403}}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := &Server{}
			w := httptest.NewRecorder()
			s.handleGlobalDeliveryAsset(w, globalDeliveryTestRequest("GET", "/?path=/fixture.txt", "", c.p))
			if w.Code != c.status {
				t.Fatalf("status %d", w.Code)
			}
		})
	}
}
func TestGlobalDeliveryRegistryRejectsCohortAndTraversal(t *testing.T) {
	if err := validateGlobalDeliveryRegistry(globalDeliveryTestRegistry()); err != nil {
		t.Fatal(err)
	}
	cases := []func(*globalDeliveryRegistry){func(x *globalDeliveryRegistry) { x.AccountID = 48 }, func(x *globalDeliveryRegistry) { x.RecordingIDs = x.RecordingIDs[:48] }, func(x *globalDeliveryRegistry) { x.RecordingIDs[1] = x.RecordingIDs[0] }, func(x *globalDeliveryRegistry) {
		o := x.Assets["/fixture.txt"]
		o.Key = "other/private.json"
		x.Assets["/fixture.txt"] = o
	}, func(x *globalDeliveryRegistry) { o := x.Files["1"]; o.RecordingID = 99; x.Files["1"] = o }, func(x *globalDeliveryRegistry) { o := x.Files["1"]; o.Verified = false; x.Files["1"] = o }, func(x *globalDeliveryRegistry) { o := x.Files["1"]; o.Path = "../video.mp4"; x.Files["1"] = o }}
	for i, mutate := range cases {
		x := globalDeliveryTestRegistry()
		mutate(x)
		if validateGlobalDeliveryRegistry(x) == nil {
			t.Fatalf("case %d allowed", i)
		}
	}
}

func TestGlobalDeliveryRegistryAcceptsGrowingPublishedMetadata(t *testing.T) {
	for _, size := range []int64{10_674_456, 64 << 20, (64 << 20) + 1} {
		x := globalDeliveryTestRegistry()
		o := x.Assets["/fixture.txt"]
		o.Key = globalDeliveryPrefix + "assets/growth.json"
		o.SizeBytes = size
		x.Assets["/public/growth.json"] = o
		err := validateGlobalDeliveryRegistry(x)
		if size <= 64<<20 && err != nil {
			t.Fatalf("published growth asset with %d bytes rejected: %v", size, err)
		}
		if size > 64<<20 && err == nil {
			t.Fatalf("oversized growth asset with %d bytes accepted", size)
		}
	}
}
func TestGlobalDeliveryAssetSHAGuardAndExactLookup(t *testing.T) {
	p := &accountPrincipal{AccountID: 47}
	m := &globalDeliveryTestStore{body: []byte("hello")}
	s := &Server{globalDeliveryStore: m, globalDeliveryRegistry: globalDeliveryTestRegistry(), globalDeliveryRegistryAt: time.Now()}
	w := httptest.NewRecorder()
	s.handleGlobalDeliveryAsset(w, globalDeliveryTestRequest("GET", "/?path=/fixture.txt", "", p))
	if w.Code != 200 || w.Body.String() != "hello" {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	m.body = []byte("wrong")
	w = httptest.NewRecorder()
	s.handleGlobalDeliveryAsset(w, globalDeliveryTestRequest("GET", "/?path=/fixture.txt", "", p))
	if w.Code != 409 {
		t.Fatalf("changed payload %d", w.Code)
	}
	before := m.opened
	w = httptest.NewRecorder()
	s.handleGlobalDeliveryAsset(w, globalDeliveryTestRequest("GET", "/?path=../secrets.env", "", p))
	if w.Code != 404 || m.opened != before {
		t.Fatal("unregistered path reached storage")
	}
}
func TestGlobalDeliveryTicketExpectedSHAAndScopedCapability(t *testing.T) {
	p := &accountPrincipal{AccountID: 47}
	m := &globalDeliveryTestStore{body: []byte("hello")}
	reg := globalDeliveryTestRegistry()
	s := &Server{globalDeliveryStore: m, globalDeliveryRegistry: reg, globalDeliveryRegistryAt: time.Now()}
	for _, claim := range []string{"", strings.Repeat("a", 64)} {
		w := httptest.NewRecorder()
		s.handleGlobalDeliveryTicket(w, globalDeliveryTestRequest("GET", "/?sha256="+claim, "1", p))
		if w.Code != 400 && w.Code != 409 {
			t.Fatalf("invalid claim %d", w.Code)
		}
	}
	if m.presigned != 0 {
		t.Fatal("bad claim minted access")
	}
	w := httptest.NewRecorder()
	s.handleGlobalDeliveryTicket(w, globalDeliveryTestRequest("GET", "/?sha256="+reg.Files["1"].SHA256, "1", p))
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	var out map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out["transport"] != "r2-direct" || out["storage_host"] != "test.r2.cloudflarestorage.com" || out["ttl_seconds"] != float64(300) {
		t.Fatal(out)
	}
	headers := out["required_headers"].(map[string]any)
	if headers["If-Match"] != `"etag"` || headers["Authorization"] != nil || len(headers) != 1 {
		t.Fatal(headers)
	}
}
func TestGlobalDeliveryRangeAndHEADPreserveGeneration(t *testing.T) {
	p := &accountPrincipal{AccountID: 47}
	m := &globalDeliveryTestStore{body: []byte("hello")}
	s := &Server{globalDeliveryStore: m, globalDeliveryRegistry: globalDeliveryTestRegistry(), globalDeliveryRegistryAt: time.Now()}
	r := globalDeliveryTestRequest("GET", "/", "1", p)
	r.Header.Set("Range", "bytes=1-3")
	w := httptest.NewRecorder()
	s.handleGlobalDeliveryFile(w, r)
	if w.Code != 206 || w.Body.String() != "ell" || w.Header().Get("Content-Range") != "bytes 1-3/5" {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	before := m.opened
	w = httptest.NewRecorder()
	s.handleGlobalDeliveryFile(w, globalDeliveryTestRequest("HEAD", "/", "1", p))
	if w.Code != 200 || w.Body.Len() != 0 || m.opened != before {
		t.Fatal("HEAD opened payload")
	}
}

func TestGlobalDeliveryApprovedCohortExpansion(t *testing.T) {
	reg := globalDeliveryTestRegistry()
	for id := int64(50); id <= 60; id++ {
		reg.RecordingIDs = append(reg.RecordingIDs, id)
	}
	if err := validateGlobalDeliveryRegistry(reg); err != nil {
		t.Fatal(err)
	}
	reg.RecordingIDs = append(reg.RecordingIDs, 61)
	if err := validateGlobalDeliveryRegistry(reg); err == nil {
		t.Fatal("outside approved cohort size accepted")
	}
}

func TestPublicGlobalDeliveryScopedAnonymousAndPresentation(t *testing.T) {
	reg := globalDeliveryTestRegistry()
	reg.Generation, reg.PublicDelivery, reg.SelectionScope = strings.Repeat("a", 32), true, "available"
	reg.Assets["/public/catalog.json"] = reg.Assets["/fixture.txt"]
	delete(reg.Assets, "/fixture.txt")
	m := &globalDeliveryTestStore{body: []byte("hello")}
	s := publicDeliveryTestServer(t, m, reg)
	gen := "&generation=" + reg.Generation
	w := httptest.NewRecorder()
	s.handleGlobalDeliveryAsset(w, globalDeliveryTestRequest("GET", publicGlobalDeliveryAPI+"/metadata?path=/public/catalog.json"+gen, "", nil))
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	w = httptest.NewRecorder()
	s.handleGlobalDeliveryAsset(w, globalDeliveryTestRequest("GET", publicGlobalDeliveryAPI+"/metadata?path=/private/audit.json"+gen, "", nil))
	if w.Code != 404 {
		t.Fatal("private asset exposed", w.Code)
	}
	w = httptest.NewRecorder()
	s.handleGlobalDeliveryTicket(w, globalDeliveryTestRequest("GET", publicGlobalDeliveryAPI+"/ticket/2?sha256="+reg.Files["1"].SHA256+gen, "2", nil))
	if w.Code != 404 {
		t.Fatal("unselected output exposed", w.Code)
	}
	w = httptest.NewRecorder()
	s.handleGlobalDeliveryTicket(w, globalDeliveryTestRequest("GET", publicGlobalDeliveryAPI+"/ticket/1?sha256="+reg.Files["1"].SHA256+"&path=other.mp4"+gen, "1", nil))
	if w.Code != 409 || m.presigned != 0 {
		t.Fatal("arbitrary presentation accepted", w.Code)
	}
	w = httptest.NewRecorder()
	s.handleGlobalDeliveryTicket(w, globalDeliveryTestRequest("GET", publicGlobalDeliveryAPI+"/ticket/1?sha256="+reg.Files["1"].SHA256+gen, "1", nil))
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	w = httptest.NewRecorder()
	s.handleGlobalDeliveryAsset(w, globalDeliveryTestRequest("GET", globalDeliveryAPI+"/metadata?path=/public/catalog.json", "", nil))
	if w.Code != 401 {
		t.Fatal("private auth changed", w.Code)
	}
	for _, name := range []string{"/api/coverage-delivery/manifest?stream=1&scope=best", "/private/audit.json", "/public/../secret", "/api/coverage-delivery/manifest?stream=999&scope=available"} {
		if publicGlobalDeliveryAsset(name, reg.RecordingIDs) {
			t.Fatal("unsafe public asset", name)
		}
	}
}

func TestPublicGlobalDeliveryEncodedFileIDThroughRouter(t *testing.T) {
	reg := globalDeliveryTestRegistry()
	reg.Generation, reg.PublicDelivery, reg.SelectionScope = strings.Repeat("a", 32), true, "available"
	file := reg.Files["1"]
	delete(reg.Files, "1")
	reg.Files["sha256:"+file.SHA256] = file
	store := &globalDeliveryTestStore{body: []byte("hello")}
	s := publicDeliveryTestServer(t, store, reg)
	router := chi.NewRouter()
	router.Get(publicGlobalDeliveryAPI+"/ticket/{fileID}", s.handleGlobalDeliveryTicket)
	for _, tc := range []struct {
		id     string
		status int
	}{
		{"sha256:" + file.SHA256, http.StatusOK},
		{"sha256%3A" + file.SHA256, http.StatusOK},
		{"sha256%253A" + file.SHA256, http.StatusNotFound},
		{"sha256%2F" + file.SHA256, http.StatusNotFound},
	} {
		t.Run(tc.id, func(t *testing.T) {
			w := httptest.NewRecorder()
			router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, publicGlobalDeliveryAPI+"/ticket/"+tc.id+"?sha256="+file.SHA256+"&generation="+reg.Generation, nil))
			if w.Code != tc.status {
				t.Fatalf("ticket status = %d, want %d", w.Code, tc.status)
			}
		})
	}
}

func TestPublicGlobalDeliveryRevalidatesAssetRepresentations(t *testing.T) {
	const assetPath = "/api/coverage-delivery/manifest?stream=1&scope=available"
	body := []byte(`{"outputs":[{"id":"1","path":"unchanged.mp4"}]}`)
	sum := sha256.Sum256(body)
	generation := strings.Repeat("a", 32)
	reg := &globalDeliveryRegistry{Generation: generation, Assets: map[string]globalDeliveryObject{
		assetPath:             {ContentType: "application/json", SHA256: hex.EncodeToString(sum[:]), SizeBytes: int64(len(body))},
		"/download-script.py": {SHA256: strings.Repeat("c", 64)},
	}}
	store := &globalDeliveryTestStore{body: body}
	s := publicDeliveryTestServer(t, store, reg)
	target := publicGlobalDeliveryAPI + "/metadata?path=" + url.QueryEscape(assetPath) + "&generation="
	request := func(encoding, etag string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, target+reg.Generation, nil)
		r.Header.Set("Accept-Encoding", encoding)
		r.Header.Set("If-None-Match", etag)
		w := httptest.NewRecorder()
		s.handleGlobalDeliveryAsset(w, r)
		return w
	}
	compressed := request("br, gzip", "")
	etag := compressed.Header().Get("ETag")
	if compressed.Code != http.StatusOK || compressed.Header().Get("Content-Encoding") != "gzip" ||
		compressed.Header().Get("Vary") != "Accept-Encoding" || compressed.Header().Get("Cache-Control") != "private, no-cache" ||
		!strings.HasPrefix(etag, "W/") {
		t.Fatalf("gzip response: status %d, headers %v", compressed.Code, compressed.Header())
	}
	zr, err := gzip.NewReader(compressed.Body)
	if err != nil {
		t.Fatal(err)
	}
	defer zr.Close()
	decoded, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	plain := request("gzip;q=0", "")
	if plain.Header().Get("Content-Encoding") != "" || plain.Header().Get("ETag") != etag || !bytes.Equal(decoded, plain.Body.Bytes()) ||
		!bytes.Contains(decoded, []byte(`"delivery_generation":"`+generation+`"`)) ||
		!bytes.Contains(decoded, []byte(`"download_script_sha256":"`+reg.Assets["/download-script.py"].SHA256+`"`)) ||
		!bytes.Contains(decoded, []byte(`"path":"unchanged.mp4"`)) {
		t.Fatalf("identity and gzip must carry the same stamped manifest: %q", decoded)
	}
	w := request("gzip", `"unrelated", `+etag)
	if w.Code != http.StatusNotModified || w.Body.Len() != 0 || store.opened != 2 {
		t.Fatalf("revalidation opened storage or wrote a body: status=%d opens=%d", w.Code, store.opened)
	}
	// An unchanged R2 asset must be stamped again after publication moves.
	reg.Generation = strings.Repeat("b", 32)
	if _, err := s.pool.Exec(context.Background(), `UPDATE gss_public_generation_pin SET generation=$1`, reg.Generation); err != nil {
		t.Fatal(err)
	}
	w = request("identity", etag)
	if w.Code != http.StatusOK || etag == w.Header().Get("ETag") || store.opened != 3 {
		t.Fatalf("new generation reused old manifest: status=%d opens=%d", w.Code, store.opened)
	}
}

// publicDeliveryTestServer serves reg as the pinned public generation, its
// holds taken as confirmed for the rest of the test.
func publicDeliveryTestServer(t *testing.T, store globalDeliveryObjectStore, reg *globalDeliveryRegistry) *Server {
	t.Helper()
	pool := cleanupTestPool(t)
	if _, err := pool.Exec(context.Background(), `UPDATE gss_public_generation_pin SET generation=$1`, reg.Generation); err != nil {
		t.Fatal(err)
	}
	s := &Server{globalDeliveryStore: store, pool: pool}
	s.publicDelivery.Store(&publicDeliveryEntry{registry: reg, admittedAt: time.Now().Add(time.Hour)})
	return s
}

// deliveryRegistryTestStore counts registry reads and HEADs, and can hold a
// HEAD of one key until released, like a stalled R2 request.
type deliveryRegistryTestStore struct {
	globalDeliveryMapStore
	mu                   sync.Mutex
	registryOpens, heads int
	stallKey             string
	stalled              chan struct{}
	release              chan struct{}
}

func (s *deliveryRegistryTestStore) Head(ctx context.Context, key string) (r2.ObjectHead, error) {
	s.mu.Lock()
	s.heads++
	s.mu.Unlock()
	if key == s.stallKey {
		close(s.stalled)
		<-s.release
	}
	head, err := s.globalDeliveryMapStore.Head(ctx, key)
	sum := sha256.Sum256(s.globalDeliveryMapStore[key])
	head.ETag = hex.EncodeToString(sum[:])
	return head, err
}

func (s *deliveryRegistryTestStore) OpenExact(ctx context.Context, key, etag, version string) (io.ReadCloser, error) {
	s.mu.Lock()
	s.registryOpens++
	s.mu.Unlock()
	return s.globalDeliveryMapStore.OpenExact(ctx, key, etag, version)
}

func (s *deliveryRegistryTestStore) counts() (opens, heads int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.registryOpens, s.heads
}

// publicDeliveryPoolFixture publishes a held public generation, pinned, in
// generations/ and returns a server on a maxConns pool of db.
func publicDeliveryPoolFixture(t *testing.T, ctx context.Context, db *pgxpool.Pool, maxConns int32) (*Server, *deliveryRegistryTestStore, *globalDeliveryRegistry, []byte) {
	t.Helper()
	cfg := db.Config()
	cfg.MaxConns = maxConns
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	reg := globalDeliveryTestRegistry()
	reg.Generation, reg.PublicDelivery, reg.SelectionScope = strings.Repeat("a", 32), true, "available"
	asset := reg.Assets["/fixture.txt"]
	reg.Assets = map[string]globalDeliveryObject{"/public/catalog.json": asset}
	for _, object := range []globalDeliveryObject{asset, reg.Files["1"]} {
		if _, err := db.Exec(ctx, `INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind) VALUES('stoarama',$1,$2,$3,'live_file')`, object.Key, object.ETag, object.SizeBytes); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := json.Marshal(reg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `UPDATE gss_public_generation_pin SET generation=$1`, reg.Generation); err != nil {
		t.Fatal(err)
	}
	store := &deliveryRegistryTestStore{globalDeliveryMapStore: globalDeliveryMapStore{publicDeliveryGenerationKey(reg.Generation): raw}}
	return &Server{globalDeliveryStore: store, pool: pool}, store, reg, raw
}

func publicDeliveryGenerationKey(generation string) string {
	return globalDeliveryPrefix + "public/generations/" + generation + ".json"
}

func TestPublicGlobalDeliveryColdRegistryUsesFenceConnection(t *testing.T) {
	db := cleanupTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s, store, reg, raw := publicDeliveryPoolFixture(t, ctx, db, 1)
	first, err := s.loadGlobalDeliveryRegistryScope(ctx, true, "")
	if err != nil {
		t.Fatalf("cold load must complete with a single database connection: %v", err)
	}
	next, err := s.loadGlobalDeliveryRegistryScope(ctx, true, reg.Generation)
	if opens, heads := store.counts(); err != nil || next != first || opens != 1 || heads != 1 {
		t.Fatalf("unchanged pin must reuse the parsed registry without R2: opens=%d heads=%d err=%v", opens, heads, err)
	}

	// A moved pin whose holds are unavailable is refused and evicts the entry.
	newGeneration := strings.Repeat("b", 32)
	store.globalDeliveryMapStore[publicDeliveryGenerationKey(newGeneration)] = bytes.Replace(raw, []byte(reg.Generation), []byte(newGeneration), 1)
	if _, err := db.Exec(ctx, `UPDATE gss_public_generation_pin SET generation='`+newGeneration+`';ALTER TABLE delivery_keep_holds RENAME TO fixture_holds`); err != nil {
		t.Fatal(err)
	}
	// The former pin is still the cached entry; naming it is outdated.
	if _, err := s.loadGlobalDeliveryRegistryScope(ctx, true, reg.Generation); !errors.Is(err, errGlobalDeliveryOutdatedGeneration) {
		t.Fatalf("cached former pin must answer 410: %v", err)
	}
	if _, err := s.loadGlobalDeliveryRegistryScope(ctx, true, ""); err == nil || s.publicDelivery.Load() != nil {
		t.Fatalf("moved pin skipped the unavailable hold check: %v", err)
	}
	if _, err := db.Exec(ctx, `ALTER TABLE fixture_holds RENAME TO delivery_keep_holds`); err != nil {
		t.Fatal(err)
	}
	next, err = s.loadGlobalDeliveryRegistryScope(ctx, true, "")
	if err != nil || next.Generation != newGeneration {
		t.Fatalf("moved pin must reload: %v", err)
	}
	// Evicted former pin: still 410, and nothing is read from R2.
	opens, _ := store.counts()
	if _, err := s.loadGlobalDeliveryRegistryScope(ctx, true, reg.Generation); !errors.Is(err, errGlobalDeliveryOutdatedGeneration) {
		t.Fatalf("evicted former pin must answer 410: %v", err)
	}
	if after, _ := store.counts(); after != opens {
		t.Fatal("outdated generation was loaded from R2")
	}
}

// Cold loads must not hold a connection while they queue for the registry
// mutex or wait on R2: with one of two connections held elsewhere, 64
// concurrent first requests all complete.
func TestPublicGlobalDeliveryColdRegistryConcurrentSmallPool(t *testing.T) {
	db := cleanupTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, store, _, _ := publicDeliveryPoolFixture(t, ctx, db, 2)
	held, err := s.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()
	const readers = 64
	start := make(chan struct{})
	errs := make(chan error, readers)
	for range readers {
		go func() {
			<-start
			_, err := s.loadGlobalDeliveryRegistryScope(ctx, true, "")
			errs <- err
		}()
	}
	began := time.Now()
	close(start)
	for range readers {
		if err := <-errs; err != nil {
			t.Fatalf("cold public registry load starved with one of two connections held after %s: %v", time.Since(began), err)
		}
	}
	if opens, _ := store.counts(); opens != 1 {
		t.Fatalf("%d concurrent cold requests read the registry %d times", readers, opens)
	}
	if elapsed := time.Since(began); elapsed > 5*time.Second {
		t.Fatalf("%d cold public registry loads took %s", readers, elapsed)
	}
}

// Cache hits take neither the registry mutex nor R2: while the account
// registry load holds the mutex, stalled on R2, 64 concurrent hits on a
// two-connection pool (one held elsewhere) complete without an R2 HEAD. With
// both connections free and the cleanup fence held exclusively, two hits wait
// on the shared fence lock at the same time: they overlap, not serialize.
func TestPublicGlobalDeliveryCacheHitsRunConcurrently(t *testing.T) {
	db := cleanupTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, store, reg, raw := publicDeliveryPoolFixture(t, ctx, db, 2)
	if _, err := s.loadGlobalDeliveryRegistryScope(ctx, true, ""); err != nil {
		t.Fatal(err)
	}
	held, err := s.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	hit := func() error {
		got, err := s.loadGlobalDeliveryRegistryScope(ctx, true, "")
		if err == nil && got.Generation != reg.Generation {
			err = fmt.Errorf("served %s", got.Generation)
		}
		return err
	}

	latest := globalDeliveryPrefix + "latest.json"
	store.globalDeliveryMapStore[latest] = raw
	store.stallKey, store.stalled, store.release = latest, make(chan struct{}), make(chan struct{})
	loaded := make(chan error, 1)
	go func() {
		_, err := s.loadGlobalDeliveryRegistryScope(ctx, false, "")
		loaded <- err
	}()
	<-store.stalled
	released := false
	defer func() {
		if !released {
			close(store.release)
		}
	}()
	_, headsBefore := store.counts()
	const readers = 64
	done := make(chan error, readers)
	for range readers {
		go func() { done <- hit() }()
	}
	for range readers {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("hit: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("cache hits waited behind the stalled load")
		}
	}
	if _, headsAfter := store.counts(); headsAfter != headsBefore {
		t.Fatalf("cache hits sent %d R2 HEADs", headsAfter-headsBefore)
	}

	held.Release()
	fence, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer fence.Rollback(ctx)
	if _, err := fence.Exec(ctx, `SELECT pg_advisory_xact_lock(7343226011101)`); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		go func() { done <- hit() }()
	}
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		var waiting int
		if err := db.QueryRow(ctx, `SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND NOT granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND (classid::bigint<<32 | objid::bigint)=7343226011101`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of 2 warm hits reached the shared fence lock together", waiting)
		}
	}
	if err := fence.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := <-done; err != nil {
			t.Fatalf("hit after fence release: %v", err)
		}
	}
	released = true
	close(store.release)
	if err := <-loaded; err != nil {
		t.Fatalf("stalled load: %v", err)
	}
}

// A load that read pin A must not admit A when the pin moves to B while it
// waits on R2.
func TestPublicGlobalDeliveryPinMoveDuringLoadAdmitsNothing(t *testing.T) {
	db := cleanupTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, store, reg, _ := publicDeliveryPoolFixture(t, ctx, db, 2)
	store.stallKey, store.stalled, store.release = publicDeliveryGenerationKey(reg.Generation), make(chan struct{}), make(chan struct{})
	type result struct {
		reg *globalDeliveryRegistry
		err error
	}
	loaded := make(chan result, 1)
	go func() {
		got, err := s.loadGlobalDeliveryRegistryScope(ctx, true, "")
		loaded <- result{got, err}
	}()
	<-store.stalled
	if _, err := db.Exec(ctx, `UPDATE gss_public_generation_pin SET generation=$1`, strings.Repeat("b", 32)); err != nil {
		t.Fatal(err)
	}
	close(store.release)
	got := <-loaded
	if got.err == nil || got.reg != nil || s.publicDelivery.Load() != nil {
		t.Fatalf("former pin was served or admitted after the pin moved: %v", got.err)
	}
}

// A request that read pin A and then queued on the mutex re-reads the pin and
// never returns A once the pin has moved.
func TestPublicGlobalDeliveryQueuedRequestRereadsPin(t *testing.T) {
	db := cleanupTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, store, reg, raw := publicDeliveryPoolFixture(t, ctx, db, 2)
	a, err := s.loadGlobalDeliveryRegistryScope(ctx, true, "")
	if err != nil {
		t.Fatal(err)
	}
	// Due for its hold recheck, so the request queues on the mutex.
	s.publicDelivery.Store(&publicDeliveryEntry{registry: a, admittedAt: time.Now().Add(-publicDeliveryHoldRecheck)})
	b := strings.Repeat("b", 32)
	store.globalDeliveryMapStore[publicDeliveryGenerationKey(b)] = bytes.Replace(raw, []byte(reg.Generation), []byte(b), 1)
	s.globalDeliveryMu.Lock()
	acquired := s.pool.Stat().AcquireCount()
	got := make(chan *globalDeliveryRegistry, 1)
	go func() {
		r, err := s.loadGlobalDeliveryRegistryScope(ctx, true, "")
		if err != nil {
			t.Error(err)
		}
		got <- r
	}()
	// Wait until the request has read pin A and released its connection.
	for s.pool.Stat().AcquireCount() == acquired || s.pool.Stat().AcquiredConns() != 0 {
		time.Sleep(time.Millisecond)
	}
	if _, err := db.Exec(ctx, `UPDATE gss_public_generation_pin SET generation=$1`, b); err != nil {
		t.Fatal(err)
	}
	s.globalDeliveryMu.Unlock()
	if r := <-got; r == nil || r.Generation != b {
		t.Fatalf("queued request served the former pin")
	}
}

// Holds are confirmed again once an entry is publicDeliveryHoldRecheck old: a
// failed hold check then fails closed and evicts the entry.
func TestPublicGlobalDeliveryHoldRecheckFailsClosed(t *testing.T) {
	db := cleanupTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, _, _, _ := publicDeliveryPoolFixture(t, ctx, db, 2)
	first, err := s.loadGlobalDeliveryRegistryScope(ctx, true, "")
	if err != nil {
		t.Fatal(err)
	}
	// Holds cannot be deleted outside a reviewed release; an unavailable hold
	// table fails the check the same way.
	if _, err := db.Exec(ctx, `ALTER TABLE delivery_keep_holds RENAME TO fixture_holds`); err != nil {
		t.Fatal(err)
	}
	if got, err := s.loadGlobalDeliveryRegistryScope(ctx, true, ""); err != nil || got != first {
		t.Fatalf("entry inside its recheck window: %v", err)
	}
	s.publicDelivery.Store(&publicDeliveryEntry{registry: first, admittedAt: time.Now().Add(-publicDeliveryHoldRecheck - time.Second)})
	if _, err := s.loadGlobalDeliveryRegistryScope(ctx, true, ""); err == nil || s.publicDelivery.Load() != nil {
		t.Fatalf("failed hold check kept serving past the recheck: %v", err)
	}
}
