package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
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

// deliveryPublicFixture stands in for the live public delivery gate in unit
// tests. Production reads the committed serving state from the pool on every
// request, records one lease per response and records capabilities.
//
// It reproduces the SQL contract, so the unit tests exercise the same
// shared/exclusive semantics: retired and quiesced both stop new readers,
// admission and capability records are refused while either is set, and it
// exposes the reader set so a test can prove retirement observed it.
type deliveryPublicFixture struct {
	retired, quiesced bool
	minted            int
	// readers is the live lease set; a test may pin one to hold retirement.
	readers map[string]bool
	// hold, when set, makes AdmitPublicReader block until it is closed, so a
	// test can prove a retirement attempt waits for an admitted reader instead
	// of racing it.
	hold chan struct{}
}

func newDeliveryPublicFixture() *deliveryPublicFixture {
	return &deliveryPublicFixture{readers: map[string]bool{}}
}

func (f *deliveryPublicFixture) DeliveryServing(context.Context) (bool, bool, error) {
	return f.retired, !f.retired && !f.quiesced, nil
}

func (f *deliveryPublicFixture) AdmitPublicReader(_ context.Context, id string) error {
	if f.hold != nil {
		<-f.hold
	}
	if f.retired || f.quiesced {
		return errors.New("the public delivery page is not serving")
	}
	f.readers[id] = true
	return nil
}

func (f *deliveryPublicFixture) ReleasePublicReader(_ context.Context, id string) error {
	delete(f.readers, id)
	return nil
}

func (f *deliveryPublicFixture) RecordPublicDeliveryTicket(context.Context, globalDeliveryObject, string, time.Duration) (bool, error) {
	if f.retired || f.quiesced {
		return false, nil
	}
	f.minted++
	return true, nil
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
	s := &Server{globalDeliveryStore: m, publicGlobalDeliveryRegistry: reg, publicGlobalDeliveryRegistryAt: time.Now(),
		deliveryPublic: newDeliveryPublicFixture()}
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
	s := &Server{globalDeliveryStore: store, publicGlobalDeliveryRegistry: reg, publicGlobalDeliveryRegistryAt: time.Now(),
		deliveryPublic: newDeliveryPublicFixture()}
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

// publicDeliveryRouter mounts exactly the public delivery routes server.go mounts,
// so a retirement assertion covers every one of them.
func publicDeliveryRouter(s *Server) http.Handler {
	router := chi.NewRouter()
	router.Get("/global-street-scores-delivery", s.handleGlobalDeliveryPage)
	router.Get("/global-street-scores-delivery/files", s.handleGlobalDeliveryPage)
	router.Get("/global-street-scores-delivery/static/*", s.handleGlobalDeliveryStatic)
	router.Get(publicGlobalDeliveryAPI+"/asset", s.handleGlobalDeliveryAsset)
	router.Get(publicGlobalDeliveryAPI+"/metadata", s.handleGlobalDeliveryAsset)
	router.Get(publicGlobalDeliveryAPI+"/ticket/{fileID}", s.handleGlobalDeliveryTicket)
	router.Get(publicGlobalDeliveryAPI+"/file/{fileID}", s.handleGlobalDeliveryFile)
	router.Head(publicGlobalDeliveryAPI+"/file/{fileID}", s.handleGlobalDeliveryFile)
	return router
}

func publicDeliveryRegistryFixture() *globalDeliveryRegistry {
	reg := globalDeliveryTestRegistry()
	reg.PublicDelivery = true
	reg.SelectionScope = "available"
	reg.Generation = strings.Repeat("a", 32)
	reg.Assets["/public/catalog.json"] = reg.Assets["/fixture.txt"]
	delete(reg.Assets, "/fixture.txt")
	return reg
}

// TestPublicDeliveryRetirementRefusesEveryRoute is the P1 regression: with both
// the current and the pinned registry warm in cache, taking the page down must
// make the page, every metadata asset, download tickets and file responses
// refuse, for the current and for every pinned generation.
func TestPublicDeliveryRetirementRefusesEveryRoute(t *testing.T) {
	reg := publicDeliveryRegistryFixture()
	store := &globalDeliveryTestStore{body: []byte("hello")}
	retirement := newDeliveryPublicFixture()
	s := &Server{globalDeliveryStore: store, publicGlobalDeliveryRegistry: reg, publicGlobalDeliveryRegistryAt: time.Now(),
		publicPinnedDeliveryRegistry: reg, deliveryPublic: retirement}
	router := publicDeliveryRouter(s)
	file := reg.Files["1"]

	requests := []struct{ name, method, target string }{
		{"page", "GET", "/global-street-scores-delivery"},
		{"file_list_page", "GET", "/global-street-scores-delivery/files"},
		{"static", "GET", "/global-street-scores-delivery/static/app.js"},
		{"metadata", "GET", publicGlobalDeliveryAPI + "/metadata?path=/public/catalog.json"},
		{"metadata_pinned", "GET", publicGlobalDeliveryAPI + "/metadata?path=/public/catalog.json&generation=" + reg.Generation},
		{"ticket", "GET", publicGlobalDeliveryAPI + "/ticket/1?sha256=" + file.SHA256},
		{"ticket_pinned", "GET", publicGlobalDeliveryAPI + "/ticket/1?sha256=" + file.SHA256 + "&generation=" + reg.Generation},
		{"file", "GET", publicGlobalDeliveryAPI + "/file/1?sha256=" + file.SHA256},
		{"file_pinned", "GET", publicGlobalDeliveryAPI + "/file/1?sha256=" + file.SHA256 + "&generation=" + reg.Generation},
		{"file_head", "HEAD", publicGlobalDeliveryAPI + "/file/1?sha256=" + file.SHA256},
	}
	// While serving, every route works, so the refusal below is retirement's.
	for _, r := range requests {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(r.method, r.target, nil))
		if w.Code >= 400 {
			t.Fatalf("serving page: %s refused with %d: %s", r.name, w.Code, w.Body.String())
		}
	}
	// Prewarm BOTH caches: the current registry and the pinned generation.
	if _, err := s.loadGlobalDeliveryRegistryScope(context.Background(), true, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := s.loadGlobalDeliveryRegistryScope(context.Background(), true, reg.Generation); err != nil {
		t.Fatal(err)
	}
	before, presignedBefore := store.opened, store.presigned
	retirement.retired = true
	for _, r := range requests {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(r.method, r.target, nil))
		if w.Code != http.StatusGone {
			t.Fatalf("retired page: %s served %d: %s", r.name, w.Code, w.Body.String())
		}
	}
	// A retired page never reaches storage, and mints no capability.
	if store.opened != before {
		t.Fatalf("retired page opened %d storage objects", store.opened-before)
	}
	if store.presigned != presignedBefore {
		t.Fatalf("retired page minted %d capabilities", store.presigned-presignedBefore)
	}
	// Republication is refused while retired.
	broadcast := DeliveryKeepSnapshot{Objects: map[string]DeliveryKeepIdentity{"dataset": {Key: "dataset", ETag: "e", Size: 1, Kind: "asset"}}}
	if err := RequireDeliveryKeep(context.Background(), retiredKeepQuerier{}, broadcast); err == nil || !strings.Contains(err.Error(), "retired") {
		t.Fatalf("retired page accepted republication: %v", err)
	}
	// An armed page refuses republication too, and says so as arming, not
	// retirement: the page has not gone down yet.
	if err := RequireDeliveryKeep(context.Background(), quiescedKeepQuerier{}, broadcast); err == nil || !strings.Contains(err.Error(), "quiesced") {
		t.Fatalf("quiesced page accepted republication: %v", err)
	}
}

// retiredKeepQuerier answers the publication gate the way a retired page does.
type retiredKeepQuerier struct{}

func (retiredKeepQuerier) QueryRow(context.Context, string, ...any) pgx.Row {
	return deliveryKeepRow{retired: true, serving: false}
}

// quiescedKeepQuerier answers it the way an armed but not yet retired page does:
// publication is refused, and the refusal is not retirement.
type quiescedKeepQuerier struct{}

func (quiescedKeepQuerier) QueryRow(context.Context, string, ...any) pgx.Row {
	return deliveryKeepRow{retired: false, serving: false}
}

// deliveryKeepRow answers the two-bool serving-state row (retired, serving).
type deliveryKeepRow struct{ retired, serving bool }

func (r deliveryKeepRow) Scan(dest ...any) error {
	if len(dest) != 2 {
		return errors.New("unexpected scan arity")
	}
	for i, d := range dest {
		v, ok := d.(*bool)
		if !ok {
			return errors.New("unexpected scan destination")
		}
		if i == 0 {
			*v = r.retired
		} else {
			*v = r.serving
		}
	}
	return nil
}

// TestPublicDeliveryRetirementFailsClosedWithoutAReader: a serving path that
// cannot prove the page is up must not serve it.
func TestPublicDeliveryRetirementFailsClosedWithoutAReader(t *testing.T) {
	s := &Server{globalDeliveryStore: &globalDeliveryTestStore{body: []byte("hello")}}
	w := httptest.NewRecorder()
	s.handleGlobalDeliveryPage(w, httptest.NewRequest("GET", "/global-street-scores-delivery", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("unreadable retirement state served the page: %d", w.Code)
	}
}
