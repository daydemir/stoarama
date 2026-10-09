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
	for name, o := range x.Assets {
		if !strings.HasPrefix(name, "/") || strings.Contains(name, "..") || !strings.HasPrefix(o.Key, globalDeliveryPrefix+"assets/") || !valid(o) || o.SizeBytes > 10<<20 {
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
	s.globalDeliveryMu.Lock()
	defer s.globalDeliveryMu.Unlock()
	if public && generation != "" && s.publicPinnedDeliveryRegistry != nil && s.publicPinnedDeliveryRegistry.Generation == generation {
		return s.publicPinnedDeliveryRegistry, nil
	}
	if public && generation == "" && s.publicGlobalDeliveryRegistry != nil && time.Since(s.publicGlobalDeliveryRegistryAt) < 15*time.Second {
		return s.publicGlobalDeliveryRegistry, nil
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
	bytes, err := io.ReadAll(io.LimitReader(body, head.SizeBytes+1))
	if err != nil || int64(len(bytes)) != head.SizeBytes {
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
		if generation != "" {
			s.publicPinnedDeliveryRegistry = &result
		} else {
			s.publicGlobalDeliveryRegistry = &result
			s.publicGlobalDeliveryRegistryAt = time.Now()
		}
	} else {
		s.globalDeliveryRegistry = &result
		s.globalDeliveryRegistryAt = time.Now()
	}
	return &result, nil
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
		util.WriteError(w, http.StatusServiceUnavailable, "Verified dataset metadata is temporarily unavailable. Reload to retry.")
		return
	}
	name := r.URL.Query().Get("path")
	object, ok := registry.Assets[name]
	if !ok {
		http.NotFound(w, r)
		return
	}
	body, err := s.globalDeliveryObjectStore().OpenExact(r.Context(), object.Key, object.ETag, object.VersionID)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "Dataset asset identity changed. Reload to retry.")
		return
	}
	defer body.Close()
	data, err := io.ReadAll(io.LimitReader(body, object.SizeBytes+1))
	sum := sha256.Sum256(data)
	if err != nil || int64(len(data)) != object.SizeBytes || hex.EncodeToString(sum[:]) != object.SHA256 {
		util.WriteError(w, http.StatusConflict, "Dataset asset verification failed.")
		return
	}
	if public && (strings.HasPrefix(name, "/api/coverage-delivery/manifest?") || (strings.HasPrefix(name, "/public/manifest/") && strings.HasSuffix(name, ".json"))) {
		var manifest map[string]any
		if json.Unmarshal(data, &manifest) != nil {
			util.WriteError(w, http.StatusConflict, "Manifest payload invalid.")
			return
		}
		manifest["generation"] = registry.Generation
		if outputs, ok := manifest["outputs"].([]any); ok {
			for _, entry := range outputs {
				if output, ok := entry.(map[string]any); ok {
					output["delivery_generation"] = registry.Generation
				}
			}
		}
		data, err = json.Marshal(manifest)
		if err != nil {
			util.WriteError(w, http.StatusConflict, "Manifest generation unavailable.")
			return
		}
	}
	w.Header().Set("Content-Type", object.ContentType)
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	_, _ = w.Write(data)
}
func globalDeliveryFileID(r *http.Request) string { return chi.URLParam(r, "fileID") }
func (s *Server) globalDeliveryFile(w http.ResponseWriter, r *http.Request) (globalDeliveryObject, bool) {
	public := strings.HasPrefix(r.URL.Path, publicGlobalDeliveryAPI+"/")
	if !public && !authorizeGlobalDelivery(w, r) {
		return globalDeliveryObject{}, false
	}
	registry, err := s.loadGlobalDeliveryRegistryScope(r.Context(), public, r.URL.Query().Get("generation"))
	if err != nil {
		util.WriteError(w, http.StatusServiceUnavailable, "Verified file registry unavailable.")
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
	decoded, err := io.ReadAll(io.LimitReader(reader, (64<<20)+1))
	if err != nil || len(decoded) > 64<<20 {
		return nil, errors.New("dataset registry decompression failed")
	}
	return decoded, nil
}
