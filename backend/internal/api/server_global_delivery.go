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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// errDeliveryPageRetired is the one answer every public delivery route gives once
// the page is retired: page, metadata, ticket and file, for the current and for
// every pinned generation.
var errDeliveryPageRetired = errors.New("the public delivery page is retired")

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

// deliveryRetirementReader answers the one question the public serving path
// asks: is the old delivery page retired right now? The pool implementation reads
// r2_delivery_page_retired() live on every call; there is no cache.
type deliveryRetirementReader interface {
	DeliveryPageRetired(context.Context) (bool, error)
}

type poolDeliveryRetirement struct{ q deliveryKeepQuerier }

func (p poolDeliveryRetirement) DeliveryPageRetired(ctx context.Context) (bool, error) {
	var retired bool
	if err := p.q.QueryRow(ctx, `SELECT r2_delivery_page_retired()`).Scan(&retired); err != nil {
		return false, errors.New("delivery retirement state unavailable")
	}
	return retired, nil
}

// deliveryTicketLedger records a public download capability under the same lock
// retirement takes, so a capability can never outlive the page that issued it.
type deliveryTicketLedger interface {
	RecordPublicDeliveryTicket(context.Context, globalDeliveryObject, string, time.Duration) (bool, error)
}

type poolDeliveryTickets struct{ pool *pgxpool.Pool }

func (p poolDeliveryTickets) RecordPublicDeliveryTicket(ctx context.Context, object globalDeliveryObject, pathSHA string, ttl time.Duration) (bool, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return false, errors.New("delivery ticket ledger unavailable")
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared($1)`, deliveryTicketLock); err != nil {
		return false, err
	}
	var id int64
	err = tx.QueryRow(ctx, `INSERT INTO r2_delivery_download_tickets(bucket,object_key,etag,size_bytes,object_path_sha256,expires_at)
 SELECT $1,$2,$3,$4,$5,clock_timestamp()+$6::interval
 WHERE NOT r2_delivery_page_retired() RETURNING id`,
		"stoarama", object.Key, strings.Trim(object.ETag, `"`), object.SizeBytes, pathSHA, ttl.String()).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

// deliveryPageRetired reads the committed serving state live, on every call.
// There is no cache and no bypass: retirement is a serving fact, not a claim.
func (s *Server) deliveryPageRetired(ctx context.Context) (bool, error) {
	if s.deliveryRetirement != nil {
		return s.deliveryRetirement.DeliveryPageRetired(ctx)
	}
	if s.pool == nil {
		return false, errors.New("delivery retirement state unavailable")
	}
	return poolDeliveryRetirement{s.pool}.DeliveryPageRetired(ctx)
}

// refuseRetiredPublicDelivery refuses a public delivery request while the page is
// retired, before any HTML, registry, ticket or file byte is produced. A reader
// that cannot answer refuses too: a serving path that cannot prove the page is up
// must not serve it.
func (s *Server) refuseRetiredPublicDelivery(w http.ResponseWriter, r *http.Request) bool {
	retired, err := s.deliveryPageRetired(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusServiceUnavailable, "Delivery availability is unavailable. Reload to retry.")
		return true
	}
	if retired {
		util.WriteError(w, http.StatusGone, "This dataset has been retired. See the published file list for NAS paths.")
		return true
	}
	return false
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
	// A retired page serves nothing at all: no registry, for any generation. This
	// check runs before the cleanup fence, before the cached current and pinned
	// registries, and before any object read, so retirement cannot be bypassed by
	// a warm cache or by pinning the generation that was serving.
	if public {
		retired, err := s.deliveryPageRetired(ctx)
		if err != nil {
			return nil, err
		}
		if retired {
			return nil, errDeliveryPageRetired
		}
	}
	// While cleanup permits can be outstanding, serve the immutable reviewed
	// public generation. A direct registry publication cannot introduce a new
	// protected source midway through a separately approved deletion batch.
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
		err = fence.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nas_cleanup_plans p WHERE p.mode='enabled' AND p.approved_until>now()) OR EXISTS(SELECT 1 FROM nas_cleanup_items i WHERE i.delete_ever_issued AND i.updated_at>now()-interval '2 minutes')`).Scan(&fenced)
		if err != nil {
			return nil, errors.New("public cleanup fence unavailable")
		}
		if fenced {
			const reviewed = "c4049f6dc0f940be8b7eb8b9bef6f42f"
			if generation != "" && generation != reviewed {
				return nil, errors.New("public publication deferred during NAS cleanup")
			}
			generation = reviewed
		}
	}
	s.globalDeliveryMu.Lock()
	defer s.globalDeliveryMu.Unlock()
	if public && generation != "" && s.publicGlobalDeliveryRegistry != nil && s.publicGlobalDeliveryRegistry.Generation == generation {
		return s.publicGlobalDeliveryRegistry, nil
	}
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
	if s.refuseRetiredPublicDelivery(w, r) {
		return
	}
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
	if s.refuseRetiredPublicDelivery(w, r) {
		return
	}
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
		if errors.Is(err, errDeliveryPageRetired) {
			util.WriteError(w, http.StatusGone, "This dataset has been retired. See the published file list for NAS paths.")
			return
		}
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
		if script, ok := registry.Assets["/download-script.py"]; ok {
			manifest["download_script_sha256"] = script.SHA256
		}
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
		if errors.Is(err, errDeliveryPageRetired) {
			util.WriteError(w, http.StatusGone, "This dataset has been retired. See the published file list for NAS paths.")
			return globalDeliveryObject{}, false
		}
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

// deliveryTicketLock serializes minting a public download capability against page
// retirement. A ticket minted under the shared lock commits before retirement can
// check its drain; retirement under the exclusive lock makes every later mint see
// the retired row and refuse. Neither side can leave a live capability unrecorded.
const deliveryTicketLock int64 = 7343226011104

// deliveryTicketTTL is the capability lifetime the page has always handed out.
const deliveryTicketTTL = 5 * time.Minute

// deliveryTicketLedger opens the transaction that records a public download
// capability. The pool is the ledger in production; tests provide a stub.
func (s *Server) recordPublicTickets(ctx context.Context, object globalDeliveryObject, pathSHA string) (bool, error) {
	if s.deliveryTickets != nil {
		return s.deliveryTickets.RecordPublicDeliveryTicket(ctx, object, pathSHA, deliveryTicketTTL)
	}
	if s.pool == nil {
		return false, errors.New("delivery ticket ledger unavailable")
	}
	return poolDeliveryTickets{s.pool}.RecordPublicDeliveryTicket(ctx, object, pathSHA, deliveryTicketTTL)
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
	capability, err := s.globalDeliveryObjectStore().PresignGetExactDownloadRequest(r.Context(), object.Key, object.ETag, object.VersionID, path.Base(object.Path), deliveryTicketTTL)
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
	// A public capability is recorded before it is handed out, so a retirement
	// that follows is guaranteed to see and drain it.
	if strings.HasPrefix(r.URL.Path, publicGlobalDeliveryAPI+"/") {
		minted, err := s.recordPublicTickets(r.Context(), object, hex.EncodeToString(pathSHA[:]))
		if err != nil {
			util.WriteError(w, http.StatusServiceUnavailable, "Download capability unavailable.")
			return
		}
		if !minted {
			util.WriteError(w, http.StatusGone, "This dataset has been retired. See the published file list for NAS paths.")
			return
		}
	}
	headers := map[string]string{}
	for k, v := range capability.Headers {
		if strings.EqualFold(k, "If-Match") && len(v) > 0 {
			headers["If-Match"] = v[0]
		}
	}
	w.Header().Set("Cache-Control", "private, no-store")
	util.WriteJSON(w, http.StatusOK, map[string]any{"transport": "r2-direct", "url": capability.URL, "storage_host": storageURL.Hostname(), "object_path_sha256": hex.EncodeToString(pathSHA[:]), "expires_at": time.Now().Add(deliveryTicketTTL).Unix(), "required_headers": headers, "etag": `"` + strings.Trim(object.ETag, `"`) + `"`, "sha256": object.SHA256, "size_bytes": object.SizeBytes, "filename": path.Base(object.Path), "ttl_seconds": int(deliveryTicketTTL.Seconds())})
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
