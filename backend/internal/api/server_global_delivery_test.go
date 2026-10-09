package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/go-chi/chi/v5"
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
