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
	"github.com/jackc/pgx/v5"
)

const globalDeliveryPrefix = "datasets/global-street-scores-delivery/47/"
const publicGlobalDeliveryAPI = "/api/v1/global-street-scores-delivery"
const globalDeliveryAPI = "/api/v1/account/global-street-scores-delivery"

// errGlobalDeliveryOutdatedGeneration refuses a page or copied command naming a
// generation other than the pinned one. Pasted commands print only the HTTP
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
	if public {
		return s.loadPublicDeliveryRegistry(ctx, generation)
	}
	s.globalDeliveryMu.Lock()
	defer s.globalDeliveryMu.Unlock()
	if s.globalDeliveryRegistry != nil && time.Since(s.globalDeliveryRegistryAt) < 15*time.Second {
		return s.globalDeliveryRegistry, nil
	}
	store := s.globalDeliveryObjectStore()
	if store == nil {
		return nil, errors.New("dataset storage unavailable")
	}
	key := globalDeliveryPrefix + "latest.json"
	head, err := store.Head(ctx, key)
	if err != nil || head.SizeBytes <= 0 || head.SizeBytes > 64<<20 {
		return nil, errors.New("dataset registry unavailable")
	}
	result, err := readGlobalDeliveryRegistry(ctx, store, key, head)
	if err != nil {
		return nil, err
	}
	s.globalDeliveryRegistry, s.globalDeliveryRegistryAt = result, time.Now()
	return result, nil
}

// publicDeliveryHoldRecheck bounds how long a loaded public generation is
// served before its files and assets are checked against delivery_keep_holds
// again.
const publicDeliveryHoldRecheck = 15 * time.Second

// publicDeliveryEntry is the served public generation and when its holds were
// last confirmed. It is immutable and replaced whole under globalDeliveryMu.
// A 55k-file registry retains ~22 MB once parsed and allocates ~84 MB while
// loading.
type publicDeliveryEntry struct {
	registry   *globalDeliveryRegistry
	admittedAt time.Time
}

func (e *publicDeliveryEntry) serves(pin string) bool {
	return e != nil && e.registry.Generation == pin && time.Since(e.admittedAt) < publicDeliveryHoldRecheck
}

// loadPublicDeliveryRegistry serves the pinned public generation
// (gss_public_generation_pin). A request naming any other generation gets 410,
// fenced by NAS cleanup or not: only the pin's files and assets are sure to be
// kept, so an older registry could point at files the prune lane removes.
//
// The pin alone decides, so a request needs no R2 HEAD of public/current.json.
// The one publisher, gss_gen.py swap (Jelly ~/stoarama-gss-freeze; also its
// rollback), copies public/generations/<id>.json byte for byte to current.json,
// verifies the readback, then moves the pin and verifies it; the republish
// cycle's post-swap gates read the live API and swap back on failure. Serving
// generations/<pin>.json is the same registry, switching at the pin write.
//
// A hit reads the pin in a short fence transaction on its own connection,
// releases it, and reads the immutable entry without a lock. Only a miss, a
// moved pin or an entry due for its hold recheck takes globalDeliveryMu, then
// re-reads the pin (it may have moved while the request queued). It loads from
// R2 holding no connection, then re-reads the pin and runs RequireDeliveryKeep
// in one fence transaction; any failure evicts the entry and fails closed. No
// request holds a connection while it waits on the mutex or R2. A missing pin
// or database fails closed.
func (s *Server) loadPublicDeliveryRegistry(ctx context.Context, generation string) (*globalDeliveryRegistry, error) {
	if s.pool == nil {
		return nil, errors.New("public cleanup fence unavailable")
	}
	if decoded, err := hex.DecodeString(generation); generation != "" && (err != nil || len(decoded) != 16) {
		return nil, errors.New("invalid published generation")
	}
	pin, err := s.publicDeliveryPin(ctx, generation)
	if err != nil {
		return nil, err
	}
	if entry := s.publicDelivery.Load(); entry.serves(pin) {
		return entry.registry, nil
	}
	s.globalDeliveryMu.Lock()
	defer s.globalDeliveryMu.Unlock()
	if pin, err = s.publicDeliveryPin(ctx, generation); err != nil {
		return nil, err
	}
	entry := s.publicDelivery.Load()
	if entry.serves(pin) {
		return entry.registry, nil
	}
	registry, err := s.admitPublicDeliveryRegistry(ctx, entry, pin, generation)
	if err != nil {
		s.publicDelivery.Store(nil)
		return nil, err
	}
	return registry, nil
}

// admitPublicDeliveryRegistry reuses entry's registry when it is the pin, or
// loads the pin from R2, then confirms the pin and its holds and stores it.
func (s *Server) admitPublicDeliveryRegistry(ctx context.Context, entry *publicDeliveryEntry, pin, generation string) (*globalDeliveryRegistry, error) {
	var registry *globalDeliveryRegistry
	if entry != nil && entry.registry.Generation == pin {
		registry = entry.registry
	} else {
		var err error
		if registry, err = s.readPublicDeliveryRegistry(ctx, pin); err != nil {
			return nil, err
		}
	}
	keep := DeliveryKeepSnapshot{Objects: map[string]DeliveryKeepIdentity{}}
	for _, o := range registry.Assets {
		if err := keep.add(o, "asset"); err != nil {
			return nil, err
		}
	}
	for _, o := range registry.Files {
		if err := keep.add(o, "live_file"); err != nil {
			return nil, err
		}
	}
	checked := time.Now()
	if err := s.publicDeliveryFence(ctx, func(tx pgx.Tx) error {
		again, err := readPublicDeliveryPin(ctx, tx, generation)
		if err != nil {
			return err
		}
		if again != pin {
			return errors.New("public generation moved while loading")
		}
		return RequireDeliveryKeep(ctx, tx, keep)
	}); err != nil {
		return nil, err
	}
	s.publicDelivery.Store(&publicDeliveryEntry{registry: registry, admittedAt: checked})
	return registry, nil
}

func (s *Server) readPublicDeliveryRegistry(ctx context.Context, generation string) (*globalDeliveryRegistry, error) {
	store := s.globalDeliveryObjectStore()
	if store == nil {
		return nil, errors.New("dataset storage unavailable")
	}
	key := globalDeliveryPrefix + "public/generations/" + generation + ".json"
	head, err := store.Head(ctx, key)
	if err != nil || head.SizeBytes <= 0 || head.SizeBytes > 64<<20 {
		return nil, errors.New("dataset registry unavailable")
	}
	registry, err := readGlobalDeliveryRegistry(ctx, store, key, head)
	if err != nil {
		return nil, err
	}
	if !registry.PublicDelivery || registry.SelectionScope != "available" || registry.Generation != generation {
		return nil, errors.New("dataset is not published")
	}
	for name := range registry.Assets {
		if !publicGlobalDeliveryAsset(name, registry.RecordingIDs) {
			return nil, errors.New("private asset in public dataset")
		}
	}
	return registry, nil
}

// publicDeliveryPin reads the pin on a connection released before it returns.
func (s *Server) publicDeliveryPin(ctx context.Context, generation string) (pin string, err error) {
	err = s.publicDeliveryFence(ctx, func(tx pgx.Tx) error {
		pin, err = readPublicDeliveryPin(ctx, tx, generation)
		return err
	})
	return pin, err
}

// publicDeliveryFence runs fn in a transaction on its own connection,
// released before it returns.
func (s *Server) publicDeliveryFence(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return errors.New("public cleanup fence unavailable")
	}
	defer tx.Rollback(ctx)
	return fn(tx)
}

// readPublicDeliveryPin takes the shared NAS cleanup fence lock, so it orders
// after any cleanup step that holds it exclusively, then reads the pin. A
// request naming another generation (non-empty) gets 410.
func readPublicDeliveryPin(ctx context.Context, tx pgx.Tx, generation string) (string, error) {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(7343226011101)`); err != nil {
		return "", errors.New("public cleanup fence unavailable")
	}
	var pinned *string
	if err := tx.QueryRow(ctx, `SELECT (SELECT generation FROM gss_public_generation_pin)`).Scan(&pinned); err != nil {
		return "", errors.New("public cleanup fence unavailable")
	}
	if pinned == nil {
		return "", errors.New("public generation pin missing")
	}
	if generation != "" && generation != *pinned {
		return "", errGlobalDeliveryOutdatedGeneration
	}
	return *pinned, nil
}

func readGlobalDeliveryRegistry(ctx context.Context, store globalDeliveryObjectStore, key string, head r2.ObjectHead) (*globalDeliveryRegistry, error) {
	body, err := store.OpenExact(ctx, key, head.ETag, head.VersionID)
	if err != nil {
		return nil, errors.New("dataset registry changed")
	}
	defer body.Close()
	raw, err := readGlobalDeliveryExact(body, head.SizeBytes)
	if err != nil {
		return nil, errors.New("dataset registry length differs")
	}
	raw, err = decodeGlobalDeliveryRegistry(raw)
	if err != nil {
		return nil, err
	}
	var result globalDeliveryRegistry
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, errors.New("dataset registry unreadable")
	}
	if err := validateGlobalDeliveryRegistry(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// globalDeliveryAssetSlots bounds asset bodies held in memory at once: each is
// verified whole before it is sent, so a burst of large manifest or growth.json
// requests cannot add up past the instance's memory.
var globalDeliveryAssetSlots = make(chan struct{}, 4)

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
func writeGlobalDeliveryManifest(w http.ResponseWriter, r *http.Request, data []byte, generation, scriptSHA, contentType, etag string) {
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
	writer, closeStream := startGlobalDeliveryAsset(w, r, contentType, etag)
	defer closeStream()
	if !hasOutputs {
		_, _ = writer.Write(rest)
		return
	}
	_, _ = io.WriteString(writer, `{"outputs":`)
	list := json.NewDecoder(bytes.NewReader(outputs))
	if open, _ := list.Token(); open != json.Delim('[') {
		_, _ = writer.Write(outputs)
	} else {
		stamp := append([]byte(`"delivery_generation":`), stamped...)
		_, _ = io.WriteString(writer, "[")
		for i := 0; list.More(); i++ {
			var output json.RawMessage
			if list.Decode(&output) != nil {
				return
			}
			if i > 0 {
				_, _ = io.WriteString(writer, ",")
			}
			if output[0] != '{' {
				_, _ = writer.Write(output)
				continue
			}
			body := bytes.TrimSpace(output[1 : len(output)-1])
			_, _ = io.WriteString(writer, "{")
			if len(body) > 0 {
				_, _ = writer.Write(body)
				_, _ = io.WriteString(writer, ",")
			}
			_, _ = writer.Write(stamp)
			_, _ = io.WriteString(writer, "}")
		}
		_, _ = io.WriteString(writer, "]")
	}
	_, _ = io.WriteString(writer, ",")
	_, _ = writer.Write(rest[1:])
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

// startGlobalDeliveryAsset is called after verification. Keep the original
// representation for clients that request it, including the download tool.
func startGlobalDeliveryAsset(w http.ResponseWriter, r *http.Request, contentType, etag string) (io.Writer, func()) {
	h := w.Header()
	h.Set("Content-Type", contentType)
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Cache-Control", "private, no-store")
	if etag != "" {
		h.Set("Cache-Control", "private, no-cache")
		h.Set("ETag", etag)
		h.Set("Vary", "Accept-Encoding")
	}
	if kind, _, _ := mime.ParseMediaType(contentType); etag != "" && kind == "application/json" {
		for _, value := range strings.Split(r.Header.Get("Accept-Encoding"), ",") {
			coding, params, err := mime.ParseMediaType(value)
			if err != nil || coding != "gzip" {
				continue
			}
			if q, ok := params["q"]; ok {
				weight, err := strconv.ParseFloat(q, 64)
				if err != nil || !(weight > 0 && weight <= 1) {
					break
				}
			}
			h.Set("Content-Encoding", "gzip")
			zw, _ := gzip.NewWriterLevel(w, gzip.BestSpeed)
			return zw, func() { _ = zw.Close() }
		}
	}
	return w, func() {}
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
	manifest := public && (strings.HasPrefix(name, "/api/coverage-delivery/manifest?") || (strings.HasPrefix(name, "/public/manifest/") && strings.HasSuffix(name, ".json")))
	var etag string
	if public {
		identity := object.ContentType + ":" + object.SHA256
		if manifest {
			// The served bytes also include the generation and tool digest.
			identity += ":manifest-v1:" + generation + ":" + scriptSHA
		}
		etag = fmt.Sprintf(`W/"%x"`, sha256.Sum256([]byte(identity)))
		for _, value := range strings.Split(r.Header.Get("If-None-Match"), ",") {
			if strings.TrimPrefix(strings.TrimSpace(value), "W/") == etag[2:] {
				h := w.Header()
				h.Set("ETag", etag)
				h.Set("Cache-Control", "private, no-cache")
				h.Set("Vary", "Accept-Encoding")
				w.WriteHeader(http.StatusNotModified)
				return
			}
		}
	}
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
	if manifest {
		writeGlobalDeliveryManifest(w, r, data, generation, scriptSHA, object.ContentType, etag)
		return
	}
	writer, closeStream := startGlobalDeliveryAsset(w, r, object.ContentType, etag)
	defer closeStream()
	_, _ = writer.Write(data)
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
