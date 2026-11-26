package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
)

// Production shape on 2026-11-26: a public generation registers ~55k files
// across 60 streams, and the largest stream manifest lists ~4,000 outputs of
// ~1.2 KB each (generation 549402679c9d… measured 1,452 outputs = 1.67 MB).
const (
	memoryTestFiles   = 55_000
	memoryTestOutputs = 4_000
)

type globalDeliveryMapStore map[string][]byte

func (m globalDeliveryMapStore) Head(_ context.Context, key string) (r2.ObjectHead, error) {
	return r2.ObjectHead{ETag: "etag", SizeBytes: int64(len(m[key]))}, nil
}
func (m globalDeliveryMapStore) HeadExact(_ context.Context, key, _, _ string) (r2.ObjectHead, error) {
	return m.Head(context.Background(), key)
}
func (m globalDeliveryMapStore) Get(_ context.Context, key string) ([]byte, error) {
	return m[key], nil
}
func (m globalDeliveryMapStore) OpenExact(_ context.Context, key, _, _ string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(m[key])), nil
}
func (m globalDeliveryMapStore) OpenExactRange(_ context.Context, key, _, _ string, a, b int64) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(m[key][a : b+1])), nil
}
func (m globalDeliveryMapStore) PresignGetExactDownloadRequest(context.Context, string, string, string, string, time.Duration) (r2.PresignedRequest, error) {
	return r2.PresignedRequest{}, nil
}

func memoryTestSHA(i int) string {
	sum := sha256.Sum256([]byte(fmt.Sprint(i)))
	return hex.EncodeToString(sum[:])
}

func memoryTestManifest(outputs int) []byte {
	type proof struct {
		ID           int    `json:"id"`
		RecordingID  int    `json:"recording_id"`
		ObjectKey    string `json:"object_key"`
		SHA256       string `json:"sha256"`
		SizeBytes    int    `json:"size_bytes"`
		VerifiedAt   string `json:"historical_r2_verified_at"`
		Available    bool   `json:"current_r2_available"`
		ETag         string `json:"current_etag"`
		SizeMismatch bool   `json:"size_mismatch"`
	}
	list := make([]map[string]any, outputs)
	for i := range list {
		sha := memoryTestSHA(i)
		list[i] = map[string]any{"recording_id": 1, "local_date": "2026-09-22", "delivery_hour": 1, "collation_hour_id": 646, "id": i, "size_bytes": 2734018, "sha256": sha,
			"source_clip_ids": []int{1556541}, "part": 1, "parts": 9, "start_at": "2026-09-22T15:00:19+00:00", "end_at": "2026-09-22T15:00:38.999+00:00",
			"current_r2_proof": proof{i, 1, "joined/nightly-collation-v2/objects/" + sha + ".mp4", sha, 2734018, "2026-09-24T22:26:38.365917+00:00", true, `"dec80a5acbf4fecfc3f8469cb01ad3bc"`, false},
			"available":        true, "collections": []string{"expanded60", "available"}, "scope": "expanded60", "timezone": "America/Los_Angeles", "file_url": fmt.Sprintf("/api/v1/global-street-scores-delivery/file/%d", i),
			"path": "1_North_America_United_States_Seattle_7th_Ave_and_Bell_St/September/Tuesday/1_7th_Ave_and_Bell_St_2026_September_W4_Tuesday_hour_01_part_01.mp4", "ticket_presentation_path": true}
	}
	raw, _ := json.Marshal(map[string]any{"schema": 2, "collection": "verified-assembled", "recording_id": 1, "period": []string{"2026-09-22", "2026-10-05"}, "outputs": list})
	return raw
}

// memoryTestDelivery stores, the way R2 holds them, one public registry of
// memoryTestFiles files per generation, all listing one memoryTestOutputs-output
// manifest for stream 1.
func memoryTestDelivery(t testing.TB, generations ...string) (globalDeliveryMapStore, []byte) {
	manifest := memoryTestManifest(memoryTestOutputs)
	sum := sha256.Sum256(manifest)
	manifestKey := globalDeliveryPrefix + "assets/" + hex.EncodeToString(sum[:]) + "/manifest"
	store := globalDeliveryMapStore{manifestKey: manifest}
	ids := make([]int64, 60)
	for i := range ids {
		ids[i] = int64(i + 1)
	}
	for _, generation := range generations {
		reg := globalDeliveryRegistry{Generation: generation, PublicDelivery: true, SelectionScope: "available", SchemaVersion: 1, AccountID: 47, RecordingIDs: ids,
			Assets: map[string]globalDeliveryObject{memoryTestManifestPath: {Key: manifestKey, ETag: "etag", SHA256: hex.EncodeToString(sum[:]), SizeBytes: int64(len(manifest)), ContentType: "application/json", Verified: true}},
			Files:  make(map[string]globalDeliveryObject, memoryTestFiles)}
		for i := range memoryTestFiles {
			sha := memoryTestSHA(i)
			reg.Files[fmt.Sprint(100000+i)] = globalDeliveryObject{Key: "joined/goodplus-20260911-generation-1/objects/" + sha + ".mp4", ETag: `"01d85dc101b2a0ad7792a4515f15651f"`, SHA256: sha, SizeBytes: 1355531, RecordingID: ids[i%60], Verified: true,
				Path: "444_Europe_Czech_republic_Most_Public_square_089_Market_square/August/Tuesday/444_Public_square_089_Market_square_2026_August_W3_Tuesday_hour_02_part_01.mp4"}
		}
		raw, err := json.Marshal(reg)
		if err != nil {
			t.Fatal(err)
		}
		var gz bytes.Buffer
		zw := gzip.NewWriter(&gz)
		_, _ = zw.Write(raw)
		_ = zw.Close()
		store[globalDeliveryPrefix+"public/generations/"+generation+".json"] = gz.Bytes()
		t.Logf("registry %s: %d files, %.1f MB JSON, %.1f MB gzip; manifest: %d outputs, %.1f MB", generation[:4], memoryTestFiles, float64(len(raw))/1e6, float64(gz.Len())/1e6, memoryTestOutputs, float64(len(manifest))/1e6)
	}
	return store, manifest
}

const memoryTestManifestPath = "/api/coverage-delivery/manifest?stream=1&scope=available"

func heapAfterGC() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}
func totalAlloc() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.TotalAlloc
}

// TestGlobalDeliveryMemoryBudget measures the two allocations behind the
// 2026-11-26 OOM at production scale. Before the fix: a registry load allocated
// 118.8 MB and one manifest request 58.9 MB (13x its 4.4 MB body, decoded into
// map[string]any), so 8 concurrent manifest requests peaked at +334 MB heap
// and 16 at +629 MB on a 512 MiB instance.
func TestGlobalDeliveryMemoryBudget(t *testing.T) {
	gen := "549402679c9d5f158cece1c4f77bad16"
	store, manifest := memoryTestDelivery(t, gen)
	s := &Server{globalDeliveryStore: store}
	ctx := context.Background()

	alloc := totalAlloc()
	if _, err := s.loadGlobalDeliveryRegistryScope(ctx, true, gen); err != nil {
		t.Fatal(err)
	}
	loadAlloc := totalAlloc() - alloc
	held := heapAfterGC()
	s.publicDeliveryRegistries = nil
	retained := held - heapAfterGC()
	if _, err := s.loadGlobalDeliveryRegistryScope(ctx, true, gen); err != nil {
		t.Fatal(err)
	}
	t.Logf("registry load: %.1f MB allocated, %.1f MB retained per cached generation", float64(loadAlloc)/1e6, float64(retained)/1e6)
	if loadAlloc > 100e6 || retained > 40e6 {
		t.Fatalf("55k-file registry load allocates %d B, retains %d B", loadAlloc, retained)
	}

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, publicGlobalDeliveryAPI+"/metadata?generation="+gen+"&path="+url.QueryEscape(memoryTestManifestPath), nil)
	req.Header.Set("Accept-Encoding", "gzip")
	alloc = totalAlloc()
	s.handleGlobalDeliveryAsset(w, req)
	manifestAlloc := totalAlloc() - alloc - uint64(w.Body.Cap())
	if w.Code != http.StatusOK {
		t.Fatalf("manifest %d %s", w.Code, w.Body.String())
	}
	t.Logf("manifest request: %.1f MB allocated besides the response body", float64(manifestAlloc)/1e6)
	if manifestAlloc > 6*uint64(len(manifest)) {
		t.Fatalf("%d-byte manifest request allocates %d B", len(manifest), manifestAlloc)
	}

	// Same document the former decode-everything rewrite served.
	var want, got map[string]any
	if err := json.Unmarshal(manifest, &want); err != nil {
		t.Fatal(err)
	}
	want["generation"] = gen
	for _, o := range want["outputs"].([]any) {
		o.(map[string]any)["delivery_generation"] = gen
	}
	compressed, err := gzip.NewReader(w.Body)
	if err != nil {
		t.Fatal(err)
	}
	defer compressed.Close()
	if err := json.NewDecoder(compressed).Decode(&got); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("rewritten manifest differs: %v", err)
	}
}

func TestGlobalDeliveryManifestRewriteEdgeShapes(t *testing.T) {
	sha := strings.Repeat("ab", 32)
	for _, c := range []struct{ in, script, want string }{
		{`{"outputs":[{},{"id":1},7],"generation":"old","schema":2}`, sha, `{"outputs":[{"delivery_generation":"g"},{"id":1,"delivery_generation":"g"},7],"download_script_sha256":"` + sha + `","generation":"g","schema":2}`},
		{`{"outputs":{"id":1},"download_script_sha256":"kept"}`, "", `{"outputs":{"id":1},"download_script_sha256":"kept","generation":"g"}`},
		{`{"schema":2}`, "", `{"generation":"g","schema":2}`},
	} {
		w := httptest.NewRecorder()
		writeGlobalDeliveryManifest(w, httptest.NewRequest(http.MethodGet, "/", nil), []byte(c.in), "g", c.script, "application/json", "")
		if w.Body.String() != c.want {
			t.Fatalf("%s:\n got %s\nwant %s", c.in, w.Body.String(), c.want)
		}
	}
	w := httptest.NewRecorder()
	writeGlobalDeliveryManifest(w, httptest.NewRequest(http.MethodGet, "/", nil), []byte(`{"outputs":[`), "g", "", "application/json", "")
	if w.Code != http.StatusConflict {
		t.Fatal("invalid manifest served", w.Code)
	}
}

// Parsed generations stay bounded however many distinct ?generation= ids
// arrive: the most recently used two, current first while it is in use.
func TestPublicDeliveryRegistryCacheIsBoundedLRU(t *testing.T) {
	gens := []string{strings.Repeat("a", 32), strings.Repeat("b", 32), strings.Repeat("c", 32)}
	held := func(s *Server) (out string) {
		for _, r := range s.publicDeliveryRegistries {
			out += r.Generation[:1]
		}
		return out
	}
	s := &Server{}
	for _, g := range gens {
		s.cachePublicDeliveryRegistry(&globalDeliveryRegistry{Generation: g})
	}
	if held(s) != "cb" {
		t.Fatalf("cache %q", held(s))
	}
	s.publicDeliveryCurrent, s.publicDeliveryCurrentAt = gens[1], time.Now()
	if reg, err := s.loadGlobalDeliveryRegistryScope(context.Background(), true, ""); err != nil || reg.Generation != gens[1] || held(s) != "bc" {
		t.Fatalf("current not served from cache: %v %q", err, held(s))
	}
	s.cachePublicDeliveryRegistry(&globalDeliveryRegistry{Generation: gens[1]})
	if held(s) != "bc" {
		t.Fatalf("reload of a cached generation duplicated it: %q", held(s))
	}
}

func TestGlobalDeliveryRegistryDecodeRejectsCorruptGzip(t *testing.T) {
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	_, _ = zw.Write([]byte(`{"schema_version":1}`))
	_ = zw.Close()
	good := gz.Bytes()
	if decoded, err := decodeGlobalDeliveryRegistry(good); err != nil || string(decoded) != `{"schema_version":1}` {
		t.Fatalf("valid gzip: %v %q", err, decoded)
	}
	badCRC := bytes.Clone(good)
	badCRC[len(badCRC)-8] ^= 0xff
	shortSize := bytes.Clone(good)
	shortSize[len(shortSize)-4]--
	for name, raw := range map[string][]byte{"crc": badCRC, "size": shortSize} {
		if _, err := decodeGlobalDeliveryRegistry(raw); err == nil {
			t.Fatalf("corrupt %s accepted", name)
		}
	}
}
