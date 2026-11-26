package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/util"
	webassets "github.com/daydemir/stoarama/backend/web"
	"github.com/go-chi/chi/v5"
)

const globalDeliveryPrefix = "datasets/global-street-scores-delivery/47/"
const publicGlobalDeliveryAPI = "/api/v1/global-street-scores-delivery"
const globalDeliveryAPI = "/api/v1/account/global-street-scores-delivery"

// errGlobalDeliveryOutdatedGeneration refuses a copied command whose generation
// the NAS cleanup fence does not serve. Pasted commands print only the HTTP
// status, so it answers 410 Gone rather than a retryable 503.
var errGlobalDeliveryOutdatedGeneration = errors.New("This download link is outdated; copy a fresh command from the page.")

type globalDeliveryObject struct {
	Key         string `json:"key"`
	ETag        string `json:"etag"`
	VersionID   string `json:"version_id,omitempty"`
	SHA256      string `json:"sha256"`
	SizeBytes   int64  `json:"size_bytes"`
	ContentType string `json:"content_type"`
	Path        string `json:"path,omitempty"`
	RecordingID int64  `json:"recording_id,omitempty"`
	Verified    bool   `json:"verified"`
}
type globalDeliveryRegistry struct {
	Generation     string                          `json:"generation,omitempty"`
	PublicDelivery bool                            `json:"public_delivery,omitempty"`
	SelectionScope string                          `json:"selection_scope,omitempty"`
	SchemaVersion  int                             `json:"schema_version"`
	AccountID      int64                           `json:"account_id"`
	RecordingIDs   []int64                         `json:"recording_ids"`
	Assets         map[string]globalDeliveryObject `json:"assets"`
	Files          map[string]globalDeliveryObject `json:"files"`
}
type globalDeliveryObjectStore interface {
	Head(context.Context, string) (r2.ObjectHead, error)
	HeadExact(context.Context, string, string, string) (r2.ObjectHead, error)
	Get(context.Context, string) ([]byte, error)
	OpenExact(context.Context, string, string, string) (io.ReadCloser, error)
	OpenExactRange(context.Context, string, string, string, int64, int64) (io.ReadCloser, error)
	PresignGetExactDownloadRequest(context.Context, string, string, string, string, time.Duration) (r2.PresignedRequest, error)
}

func (s *Server) globalDeliveryObjectStore() globalDeliveryObjectStore {
	if s.globalDeliveryStore != nil {
		return s.globalDeliveryStore
	}
	if s.r2 == nil {
		return nil
	}
	return s.r2
}
func authorizeGlobalDelivery(w http.ResponseWriter, r *http.Request) bool {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "Sign in to your Stoarama organization to view this dataset.")
		return false
	}
	if principal.AccountID != 47 || isPullScopedPrincipal(principal) {
		util.WriteError(w, http.StatusForbidden, "This dataset is restricted to its existing organization.")
		return false
	}
	return true
}
func validateGlobalDeliveryRegistry(x *globalDeliveryRegistry) error {
	if x.SchemaVersion != 1 || x.AccountID != 47 || (len(x.RecordingIDs) < 49 || len(x.RecordingIDs) > 60) {
		return errors.New("dataset cohort identity differs")
	}
	ids := map[int64]bool{}
	for _, id := range x.RecordingIDs {
		if id <= 0 || ids[id] {
			return errors.New("duplicate or invalid recording")
		}
		ids[id] = true
	}
	valid := func(o globalDeliveryObject) bool {
		_, err := hex.DecodeString(o.SHA256)
		return o.Key != "" && len(o.SHA256) == 64 && err == nil && o.SizeBytes > 0 && o.ETag != "" && o.Verified
	}
	assetPrefix := globalDeliveryPrefix + "assets/"
	for name, o := range x.Assets {
		if !strings.HasPrefix(name, "/") || strings.Contains(name, "..") || !strings.HasPrefix(o.Key, assetPrefix) || !valid(o) || o.SizeBytes > 64<<20 {
			return errors.New("invalid dataset asset")
		}
	}
	for id, o := range x.Files {
		if id == "" || strings.ContainsAny(id, "/\\\r\n") || !ids[o.RecordingID] || !valid(o) || !strings.HasSuffix(o.Path, ".mp4") || strings.Contains(o.Path, "..") || strings.HasPrefix(o.Path, "/") {
			return errors.New("invalid dataset output")
		}
	}
	return nil
}
func (s *Server) loadGlobalDeliveryRegistry(ctx context.Context) (*globalDeliveryRegistry, error) {
	return s.loadGlobalDeliveryRegistryScope(ctx, false, "")
}
func (s *Server) loadGlobalDeliveryRegistryScope(ctx context.Context, public bool, generation string) (*globalDeliveryRegistry, error) {
	// While cleanup permits can be outstanding, serve only the operator-pinned
	// public generation (gss_public_generation_pin, 0268). A direct registry
	// publication cannot introduce a new protected source midway through a
	// separately approved deletion batch; a missing pin fails closed.
	if public && s.pool != nil {
		fence, err := s.pool.Begin(ctx)
		if err != nil {
			return nil, errors.New("public cleanup fence unavailable")
		}
		defer fence.Rollback(ctx)
		if _, err = fence.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(7343226011101)`); err != nil {
			return nil, errors.New("public cleanup fence unavailable")
		}
		var fenced bool
		var pinned *string
		err = fence.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nas_cleanup_plans p WHERE p.mode='enabled' AND p.approved_until>now()) OR EXISTS(SELECT 1 FROM nas_cleanup_items i WHERE i.delete_ever_issued AND i.updated_at>now()-interval '2 minutes'),(SELECT generation FROM gss_public_generation_pin)`).Scan(&fenced, &pinned)
		if err != nil {
			return nil, errors.New("public cleanup fence unavailable")
		}
		if fenced {
			if pinned == nil {
				return nil, errors.New("public generation pin missing during NAS cleanup")
			}
			if generation != "" && generation != *pinned {
				return nil, errGlobalDeliveryOutdatedGeneration
			}
			generation = *pinned
		}
	}
	s.globalDeliveryMu.Lock()
	defer s.globalDeliveryMu.Unlock()
	if public {
		fresh := generation == "" && time.Since(s.publicDeliveryCurrentAt) < 15*time.Second
		want := generation
		if fresh {
			want = s.publicDeliveryCurrent
		}
		for i, cached := range s.publicDeliveryRegistries {
			if (generation != "" || fresh) && cached.Generation == want {
				s.publicDeliveryRegistries[0], s.publicDeliveryRegistries[i] = cached, s.publicDeliveryRegistries[0]
				return cached, nil
			}
		}
	}
	if !public && s.globalDeliveryRegistry != nil && time.Since(s.globalDeliveryRegistryAt) < 15*time.Second {
		return s.globalDeliveryRegistry, nil
	}
	store := s.globalDeliveryObjectStore()
	if store == nil {
		return nil, errors.New("dataset storage unavailable")
	}
	key := globalDeliveryPrefix + "latest.json"
	if public {
		key = globalDeliveryPrefix + "public/current.json"
		if generation != "" {
			decoded, e := hex.DecodeString(generation)
			if e != nil || len(decoded) != 16 {
				return nil, errors.New("invalid published generation")
			}
			key = globalDeliveryPrefix + "public/generations/" + generation + ".json"
		}
	}
	head, err := store.Head(ctx, key)
	if err != nil || head.SizeBytes <= 0 || head.SizeBytes > 64<<20 {
		return nil, errors.New("dataset registry unavailable")
	}
	body, err := store.OpenExact(ctx, key, head.ETag, head.VersionID)
	if err != nil {
		return nil, errors.New("dataset registry changed")
	}
	defer body.Close()
	bytes, err := readGlobalDeliveryExact(body, head.SizeBytes)
	if err != nil {
		return nil, errors.New("dataset registry length differs")
	}
	bytes, err = decodeGlobalDeliveryRegistry(bytes)
	if err != nil {
		return nil, err
	}
	var result globalDeliveryRegistry
	if err = json.Unmarshal(bytes, &result); err != nil {
		return nil, errors.New("dataset registry unreadable")
	}
	if err = validateGlobalDeliveryRegistry(&result); err != nil {
		return nil, err
	}
	if public {
		if !result.PublicDelivery || result.SelectionScope != "available" || (generation != "" && result.Generation != generation) {
			return nil, errors.New("dataset is not published")
		}
		for name := range result.Assets {
			if !publicGlobalDeliveryAsset(name, result.RecordingIDs) {
				return nil, errors.New("private asset in public dataset")
			}
		}
		keep := DeliveryKeepSnapshot{Objects: map[string]DeliveryKeepIdentity{}}
		for _, o := range result.Assets {
			if err := keep.add(o, "asset"); err != nil {
				return nil, err
			}
		}
		for _, o := range result.Files {
			if err := keep.add(o, "live_file"); err != nil {
				return nil, err
			}
		}
		if s.pool != nil {
			if err := RequireDeliveryKeep(ctx, s.pool, keep); err != nil {
				return nil, err
			}
		}
		if generation == "" {
			s.publicDeliveryCurrent, s.publicDeliveryCurrentAt = result.Generation, time.Now()
		}
		s.cachePublicDeliveryRegistry(&result)
	} else {
		s.globalDeliveryRegistry = &result
		s.globalDeliveryRegistryAt = time.Now()
	}
	return &result, nil
}

// globalDeliveryAssetSlots bounds asset bodies held in memory at once: each is
// verified whole before it is sent, so a burst of large manifest or growth.json
// requests cannot add up past the instance's memory.
var globalDeliveryAssetSlots = make(chan struct{}, 4)

// publicDeliveryRegistryCacheSize bounds parsed public generations held in
// memory: the served one plus one other (the cleanup pin, or the generation an
// older tab or a running download command still names). A 55k-file registry
// retains ~22 MB once parsed and allocates ~84 MB while loading.
const publicDeliveryRegistryCacheSize = 2

// cachePublicDeliveryRegistry puts reg first, replacing a same-generation entry
// and evicting the least recently used one past the bound.
func (s *Server) cachePublicDeliveryRegistry(reg *globalDeliveryRegistry) {
	kept := []*globalDeliveryRegistry{reg}
	for _, cached := range s.publicDeliveryRegistries {
		if cached.Generation != reg.Generation && len(kept) < publicDeliveryRegistryCacheSize {
			kept = append(kept, cached)
		}
	}
	s.publicDeliveryRegistries = kept
}

// readGlobalDeliveryExact reads exactly size bytes into one allocation and fails
// unless the body then ends cleanly (for gzip, that read also checks the CRC).
func readGlobalDeliveryExact(body io.Reader, size int64) ([]byte, error) {
	data := make([]byte, size)
	if _, err := io.ReadFull(body, data); err != nil {
		return nil, err
	}
	if n, err := body.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		return nil, errors.New("object length or checksum differs")
	}
	return data, nil
}

// writeGlobalDeliveryManifest writes a verified manifest stamped with its
// serving generation. Outputs, the bulk of a manifest, are rewritten one at a
// time straight to w, so a request holds ~2x the manifest bytes instead of a
// fully decoded tree (a 4,000-output manifest decoded into map[string]any
// allocated ~59 MB per request).
// scriptSHA is empty when the generation has no download script.
func writeGlobalDeliveryManifest(w http.ResponseWriter, data []byte, generation, scriptSHA, contentType string) {
	var manifest map[string]json.RawMessage
	if json.Unmarshal(data, &manifest) != nil {
		util.WriteError(w, http.StatusConflict, "Manifest payload invalid.")
		return
	}
	stamped, _ := json.Marshal(generation)
	manifest["generation"] = stamped
	if scriptSHA != "" {
		manifest["download_script_sha256"], _ = json.Marshal(scriptSHA)
	}
	outputs, hasOutputs := manifest["outputs"]
	delete(manifest, "outputs")
	rest, _ := json.Marshal(manifest) // never "{}": it holds generation
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if !hasOutputs {
		_, _ = w.Write(rest)
		return
	}
	_, _ = io.WriteString(w, `{"outputs":`)
	list := json.NewDecoder(bytes.NewReader(outputs))
	if open, _ := list.Token(); open != json.Delim('[') {
		_, _ = w.Write(outputs)
	} else {
		stamp := append([]byte(`"delivery_generation":`), stamped...)
		_, _ = io.WriteString(w, "[")
		for i := 0; list.More(); i++ {
			var output json.RawMessage
			if list.Decode(&output) != nil {
				return
			}
			if i > 0 {
				_, _ = io.WriteString(w, ",")
			}
			if output[0] != '{' {
				_, _ = w.Write(output)
				continue
			}
			body := bytes.TrimSpace(output[1 : len(output)-1])
			_, _ = io.WriteString(w, "{")
			if len(body) > 0 {
				_, _ = w.Write(body)
				_, _ = io.WriteString(w, ",")
			}
			_, _ = w.Write(stamp)
			_, _ = io.WriteString(w, "}")
		}
		_, _ = io.WriteString(w, "]")
	}
	_, _ = io.WriteString(w, ",")
	_, _ = w.Write(rest[1:])
}

func writeGlobalDeliveryRegistryError(w http.ResponseWriter, err error, unavailable string) {
	if errors.Is(err, errGlobalDeliveryOutdatedGeneration) {
		util.WriteError(w, http.StatusGone, err.Error())
		return
	}
	util.WriteError(w, http.StatusServiceUnavailable, unavailable)
}
func (s *Server) handleGlobalDeliveryPage(w http.ResponseWriter, r *http.Request) {
	pageName := "global-delivery/index.html"
	if r.URL.Path == "/global-street-scores-delivery/files" {
		pageName = "global-delivery/files.html"
	}
	body, err := webassets.ReadStatic(pageName)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeHTML(w, body)
}
func (s *Server) handleGlobalDeliveryStatic(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "*")
	if name == "" || strings.Contains(name, "..") || strings.HasPrefix(name, "/") {
		http.NotFound(w, r)
		return
	}
	data, err := webassets.ReadStatic("global-delivery/" + name)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	kind := mime.TypeByExtension(path.Ext(name))
	if kind == "" {
		kind = "application/octet-stream"
	}
	w.Header().Set("Content-Type", kind)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "public, max-age=300")
	_, _ = w.Write(data)
}
func (s *Server) handleGlobalDeliveryAsset(w http.ResponseWriter, r *http.Request) {
	public := strings.HasPrefix(r.URL.Path, publicGlobalDeliveryAPI+"/")
	if !public && !authorizeGlobalDelivery(w, r) {
		return
	}
	registry, err := s.loadGlobalDeliveryRegistryScope(r.Context(), public, r.URL.Query().Get("generation"))
	if err != nil {
		writeGlobalDeliveryRegistryError(w, err, "Verified dataset metadata is temporarily unavailable. Reload to retry.")
		return
	}
	name := r.URL.Query().Get("path")
	object, ok := registry.Assets[name]
	if !ok {
		http.NotFound(w, r)
		return
	}
	// Keep only these past this point so a waiting or streaming request never
	// pins an evicted registry in memory.
	generation, scriptSHA := registry.Generation, registry.Assets["/download-script.py"].SHA256
	select {
	case globalDeliveryAssetSlots <- struct{}{}:
		defer func() { <-globalDeliveryAssetSlots }()
	case <-r.Context().Done():
		return
	}
	body, err := s.globalDeliveryObjectStore().OpenExact(r.Context(), object.Key, object.ETag, object.VersionID)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "Dataset asset identity changed. Reload to retry.")
		return
	}
	defer body.Close()
	data, err := readGlobalDeliveryExact(body, object.SizeBytes)
	sum := sha256.Sum256(data)
	if err != nil || hex.EncodeToString(sum[:]) != object.SHA256 {
		util.WriteError(w, http.StatusConflict, "Dataset asset verification failed.")
		return
	}
	if public && (strings.HasPrefix(name, "/api/coverage-delivery/manifest?") || (strings.HasPrefix(name, "/public/manifest/") && strings.HasSuffix(name, ".json"))) {
		writeGlobalDeliveryManifest(w, data, generation, scriptSHA, object.ContentType)
		return
	}
	w.Header().Set("Content-Type", object.ContentType)
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	_, _ = w.Write(data)
}
func globalDeliveryFileID(r *http.Request) string {
	id := chi.URLParam(r, "fileID")
	// Chi matches RawPath when present; otherwise URL.Path is already decoded.
	if r.URL.RawPath == "" {
		return id
	}
	id, err := url.PathUnescape(id)
	if err != nil {
		return ""
	}
	return id
}
func (s *Server) globalDeliveryFile(w http.ResponseWriter, r *http.Request) (globalDeliveryObject, bool) {
	public := strings.HasPrefix(r.URL.Path, publicGlobalDeliveryAPI+"/")
	if !public && !authorizeGlobalDelivery(w, r) {
		return globalDeliveryObject{}, false
	}
	registry, err := s.loadGlobalDeliveryRegistryScope(r.Context(), public, r.URL.Query().Get("generation"))
	if err != nil {
		writeGlobalDeliveryRegistryError(w, err, "Verified file registry unavailable.")
		return globalDeliveryObject{}, false
	}
	object, ok := registry.Files[globalDeliveryFileID(r)]
	if !ok {
		http.NotFound(w, r)
		return globalDeliveryObject{}, false
	}
	if expected := r.URL.Query().Get("sha256"); expected != "" && expected != object.SHA256 {
		util.WriteError(w, http.StatusConflict, "Manifest file identity differs.")
		return globalDeliveryObject{}, false
	}
	if presentation := r.URL.Query().Get("path"); presentation != "" && presentation != object.Path {
		util.WriteError(w, http.StatusConflict, "Download presentation path differs.")
		return globalDeliveryObject{}, false
	}
	if public && strings.HasPrefix(r.URL.Path, publicGlobalDeliveryAPI+"/ticket/") {
		return object, true
	}
	head, err := s.globalDeliveryObjectStore().HeadExact(r.Context(), object.Key, object.ETag, object.VersionID)
	if err != nil || head.SizeBytes != object.SizeBytes {
		util.WriteError(w, http.StatusConflict, "The recorded R2 file is unavailable or changed.")
		return globalDeliveryObject{}, false
	}
	return object, true
}
func (s *Server) handleGlobalDeliveryTicket(w http.ResponseWriter, r *http.Request) {
	object, ok := s.globalDeliveryFile(w, r)
	if !ok {
		return
	}
	if r.URL.Query().Get("sha256") != object.SHA256 {
		util.WriteError(w, http.StatusBadRequest, "Expected SHA-256 is required.")
		return
	}
	capability, err := s.globalDeliveryObjectStore().PresignGetExactDownloadRequest(r.Context(), object.Key, object.ETag, object.VersionID, path.Base(object.Path), 5*time.Minute)
	if err != nil {
		util.WriteError(w, http.StatusServiceUnavailable, "Download capability unavailable.")
		return
	}
	storageURL, parseErr := url.Parse(capability.URL)
	if parseErr != nil || storageURL.Scheme != "https" || storageURL.Hostname() == "" {
		util.WriteError(w, http.StatusServiceUnavailable, "Invalid storage capability.")
		return
	}
	pathSHA := sha256.Sum256([]byte(storageURL.EscapedPath()))
	headers := map[string]string{}
	for k, v := range capability.Headers {
		if strings.EqualFold(k, "If-Match") && len(v) > 0 {
			headers["If-Match"] = v[0]
		}
	}
	w.Header().Set("Cache-Control", "private, no-store")
	util.WriteJSON(w, http.StatusOK, map[string]any{"transport": "r2-direct", "url": capability.URL, "storage_host": storageURL.Hostname(), "object_path_sha256": hex.EncodeToString(pathSHA[:]), "expires_at": time.Now().Add(5 * time.Minute).Unix(), "required_headers": headers, "etag": `"` + strings.Trim(object.ETag, `"`) + `"`, "sha256": object.SHA256, "size_bytes": object.SizeBytes, "filename": path.Base(object.Path), "ttl_seconds": 300})
}
func (s *Server) handleGlobalDeliveryFile(w http.ResponseWriter, r *http.Request) {
	object, ok := s.globalDeliveryFile(w, r)
	if !ok {
		return
	}
	store := s.globalDeliveryObjectStore()
	size := object.SizeBytes
	etag := `"` + strings.Trim(object.ETag, `"`) + `"`
	w.Header().Set("ETag", etag)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", "video/mp4")
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Disposition", mime.FormatMediaType("inline", map[string]string{"filename": path.Base(object.Path)}))
	if r.URL.Query().Get("download") == "1" {
		w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": path.Base(object.Path)}))
	}
	if r.Method == http.MethodHead {
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		w.WriteHeader(http.StatusOK)
		return
	}
	rangeHeader := r.Header.Get("Range")
	if ifRange := r.Header.Get("If-Range"); ifRange != "" && ifRange != etag {
		rangeHeader = ""
	}
	part, err := parseJoinedByteRange(rangeHeader, size)
	if err != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", size))
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}
	var body io.ReadCloser
	if part == nil {
		body, err = store.OpenExact(r.Context(), object.Key, object.ETag, object.VersionID)
	} else {
		body, err = store.OpenExactRange(r.Context(), object.Key, object.ETag, object.VersionID, part.start, part.end)
	}
	if err != nil {
		util.WriteError(w, http.StatusConflict, "R2 file changed while opening.")
		return
	}
	defer body.Close()
	status := http.StatusOK
	responseBytes := size
	if part != nil {
		status = http.StatusPartialContent
		responseBytes = part.end - part.start + 1
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", part.start, part.end, size))
	}
	w.Header().Set("Content-Length", strconv.FormatInt(responseBytes, 10))
	w.WriteHeader(status)
	_, _ = io.CopyN(w, body, responseBytes)
}

func publicGlobalDeliveryAsset(name string, ids []int64) bool {
	if strings.HasPrefix(name, "/public/") && !strings.Contains(name, "..") && !strings.ContainsAny(name, "?\\") {
		return true
	}
	if name == "/download-script.py" {
		return true
	}
	for _, id := range ids {
		if name == fmt.Sprintf("/api/coverage-delivery/manifest?stream=%d&scope=available", id) {
			return true
		}
	}
	return false
}

func decodeGlobalDeliveryRegistry(raw []byte) ([]byte, error) {
	if len(raw) < 2 || raw[0] != 0x1f || raw[1] != 0x8b {
		return raw, nil
	}
	reader, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return nil, errors.New("dataset registry compression invalid")
	}
	defer reader.Close()
	// The gzip trailer records the decoded size; reading into one exact buffer
	// avoids io.ReadAll's doubling copies of a ~30 MB registry.
	size := binary.LittleEndian.Uint32(raw[len(raw)-4:])
	if size > 64<<20 {
		return nil, errors.New("dataset registry decompression failed")
	}
	decoded, err := readGlobalDeliveryExact(reader, int64(size))
	if err != nil {
		return nil, errors.New("dataset registry decompression failed")
	}
	return decoded, nil
}
