package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
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
	reg.PublicDelivery = true
	reg.SelectionScope = "available"
	reg.Assets["/public/catalog.json"] = reg.Assets["/fixture.txt"]
	delete(reg.Assets, "/fixture.txt")
	m := &globalDeliveryTestStore{body: []byte("hello")}
	s := &Server{globalDeliveryStore: m, publicDeliveryRegistries: []*globalDeliveryRegistry{reg}, publicDeliveryCurrentAt: time.Now()}
	w := httptest.NewRecorder()
	s.handleGlobalDeliveryAsset(w, globalDeliveryTestRequest("GET", publicGlobalDeliveryAPI+"/metadata?path=/public/catalog.json", "", nil))
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	w = httptest.NewRecorder()
	s.handleGlobalDeliveryAsset(w, globalDeliveryTestRequest("GET", publicGlobalDeliveryAPI+"/metadata?path=/private/audit.json", "", nil))
	if w.Code != 404 {
		t.Fatal("private asset exposed", w.Code)
	}
	w = httptest.NewRecorder()
	s.handleGlobalDeliveryTicket(w, globalDeliveryTestRequest("GET", publicGlobalDeliveryAPI+"/ticket/2?sha256="+reg.Files["1"].SHA256, "2", nil))
	if w.Code != 404 {
		t.Fatal("unselected output exposed", w.Code)
	}
	w = httptest.NewRecorder()
	s.handleGlobalDeliveryTicket(w, globalDeliveryTestRequest("GET", publicGlobalDeliveryAPI+"/ticket/1?sha256="+reg.Files["1"].SHA256+"&path=other.mp4", "1", nil))
	if w.Code != 409 || m.presigned != 0 {
		t.Fatal("arbitrary presentation accepted", w.Code)
	}
	w = httptest.NewRecorder()
	s.handleGlobalDeliveryTicket(w, globalDeliveryTestRequest("GET", publicGlobalDeliveryAPI+"/ticket/1?sha256="+reg.Files["1"].SHA256, "1", nil))
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
	reg.PublicDelivery = true
	reg.SelectionScope = "available"
	file := reg.Files["1"]
	delete(reg.Files, "1")
	reg.Files["sha256:"+file.SHA256] = file
	store := &globalDeliveryTestStore{body: []byte("hello")}
	s := &Server{globalDeliveryStore: store, publicDeliveryRegistries: []*globalDeliveryRegistry{reg}, publicDeliveryCurrentAt: time.Now()}
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
			router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, publicGlobalDeliveryAPI+"/ticket/"+tc.id+"?sha256="+file.SHA256, nil))
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
	s := &Server{
		globalDeliveryStore: store, publicDeliveryRegistries: []*globalDeliveryRegistry{reg},
		publicDeliveryCurrent: generation, publicDeliveryCurrentAt: time.Now(),
	}
	target := publicGlobalDeliveryAPI + "/metadata?path=" + url.QueryEscape(assetPath)
	request := func(encoding, etag string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, target, nil)
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
	s.publicDeliveryCurrent = reg.Generation
	w = request("identity", etag)
	if w.Code != http.StatusOK || etag == w.Header().Get("ETag") || store.opened != 3 {
		t.Fatalf("new generation reused old manifest: status=%d opens=%d", w.Code, store.opened)
	}
}

type deliveryCurrentTestStore struct {
	globalDeliveryMapStore
	registryOpens int
}

func (s *deliveryCurrentTestStore) Head(ctx context.Context, key string) (r2.ObjectHead, error) {
	head, err := s.globalDeliveryMapStore.Head(ctx, key)
	sum := sha256.Sum256(s.globalDeliveryMapStore[key])
	head.ETag = hex.EncodeToString(sum[:])
	return head, err
}

func (s *deliveryCurrentTestStore) OpenExact(ctx context.Context, key, etag, version string) (io.ReadCloser, error) {
	s.registryOpens++
	return s.globalDeliveryMapStore.OpenExact(ctx, key, etag, version)
}

func TestPublicGlobalDeliveryColdRegistryUsesFenceConnection(t *testing.T) {
	db := cleanupTestPool(t)
	cfg := db.Config()
	cfg.MaxConns = 1
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

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
	key := globalDeliveryPrefix + "public/current.json"
	store := &deliveryCurrentTestStore{globalDeliveryMapStore: globalDeliveryMapStore{key: raw}}
	s := &Server{globalDeliveryStore: store, pool: pool}
	first, err := s.loadGlobalDeliveryRegistryScope(ctx, true, "")
	if err != nil {
		t.Fatalf("cold load must complete with a single database connection: %v", err)
	}

	s.publicDeliveryCurrentAt = time.Time{}
	next, err := s.loadGlobalDeliveryRegistryScope(ctx, true, "")
	if err != nil || next != first || store.registryOpens != 1 {
		t.Fatalf("unchanged current should reuse parsed registry: opens=%d err=%v", store.registryOpens, err)
	}

	if _, err := db.Exec(ctx, `ALTER TABLE delivery_keep_holds RENAME TO fixture_holds`); err != nil {
		t.Fatal(err)
	}
	s.publicDeliveryCurrentAt = time.Time{}
	if _, err := s.loadGlobalDeliveryRegistryScope(ctx, true, ""); err == nil || store.registryOpens != 1 {
		t.Fatalf("unchanged current skipped the unavailable hold check: opens=%d err=%v", store.registryOpens, err)
	}

	// A same-size publication with a different ETag still reloads.
	if _, err := db.Exec(ctx, `ALTER TABLE fixture_holds RENAME TO delivery_keep_holds`); err != nil {
		t.Fatal(err)
	}
	newGeneration := strings.Repeat("b", 32)
	store.globalDeliveryMapStore[key] = bytes.Replace(raw, []byte(reg.Generation), []byte(newGeneration), 1)
	next, err = s.loadGlobalDeliveryRegistryScope(ctx, true, "")
	if err != nil || next.Generation != newGeneration || store.registryOpens != 2 {
		t.Fatalf("changed current must reload: opens=%d err=%v", store.registryOpens, err)
	}
}

func TestPublicGlobalDeliveryColdRegistryConcurrentSmallPool(t *testing.T) {
	db := cleanupTestPool(t)
	cfg := db.Config()
	cfg.MaxConns = 2
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	held, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()

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
	store := &deliveryCurrentTestStore{globalDeliveryMapStore: globalDeliveryMapStore{globalDeliveryPrefix + "public/current.json": raw}}
	s := &Server{globalDeliveryStore: store, pool: pool}

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
	if elapsed := time.Since(began); elapsed > 5*time.Second {
		t.Fatalf("%d cold public registry loads took %s", readers, elapsed)
	}
}
