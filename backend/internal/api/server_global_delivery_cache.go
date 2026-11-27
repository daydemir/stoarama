package api

import (
	"bytes"
	"compress/gzip"
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/daydemir/stoarama/backend/internal/util"
)

// globalDeliveryAssetCacheBytes bounds the memory the asset cache owns: every
// resident body, including those responses are still writing (a referenced
// body is never evicted), plus each identity response's inflate buffers. It is
// part of the instance's memory budget (512 MiB, GOMEMLIMIT 400 MiB) beside
// the parsed registry (~22 MB) and the bodies read under
// globalDeliveryAssetSlots; TestGlobalDeliveryMemoryBudget measures all three.
// A production generation's public assets gzip to ~12 MB (2026-11-26:
// growth.json 1.3 MB, 60 stream manifests ~9 MB). Past the bound, requests
// take the slot path.
const globalDeliveryAssetCacheBytes = 48 << 20

// globalDeliveryAssetEntryBytes caps one cached body. A body that encodes
// larger is read from R2 under a slot on every request.
const globalDeliveryAssetEntryBytes = 4 << 20

// globalDeliveryAssetEntryOverhead is charged per entry besides its body, so
// bodyless entries (uncached markers) are bounded too.
const globalDeliveryAssetEntryOverhead = 512

// globalDeliveryInflateBytes is charged while an identity response inflates a
// gzipped body: a gzip reader (~42 KB measured) and a 32 KB copy buffer.
const globalDeliveryInflateBytes = 128 << 10

// globalDeliveryAssetPendingFills bounds distinct fills waiting on or holding
// a slot; further misses take the slot path within their own request.
const globalDeliveryAssetPendingFills = 32

// globalDeliveryAssetFillTimeout bounds a fill's slot wait and R2 read. A fill
// outlives the request that started it, so it ends with the API's 60 s
// WriteTimeout.
const globalDeliveryAssetFillTimeout = time.Minute

var (
	errGlobalDeliveryAssetChanged    = errors.New("Dataset asset identity changed. Reload to retry.")
	errGlobalDeliveryAssetUnverified = errors.New("Dataset asset verification failed.")
	errGlobalDeliveryManifestInvalid = errors.New("Manifest payload invalid.")
	errGlobalDeliveryAssetBusy       = errors.New("Dataset asset temporarily unavailable. Reload to retry.")
	errGlobalDeliveryAssetFailed     = errors.New("Dataset asset could not be prepared. Reload to retry.")
	errGlobalDeliveryAssetLarge      = errors.New("dataset asset exceeds the cache entry cap")
)

func writeGlobalDeliveryAssetError(w http.ResponseWriter, err error) {
	status := http.StatusConflict
	switch {
	case errors.Is(err, errGlobalDeliveryAssetBusy):
		status = http.StatusServiceUnavailable
	case errors.Is(err, errGlobalDeliveryAssetFailed):
		status = http.StatusInternalServerError
	}
	util.WriteError(w, status, err.Error())
}

// globalDeliveryServedAsset is what one asset request serves, copied out of
// the registry so a waiting or streaming request never pins an evicted one.
type globalDeliveryServedAsset struct {
	object                globalDeliveryObject
	manifest              bool
	generation, scriptSHA string
}

// identity is the string the public ETag hashes; it names the same bytes as
// key.
func (a globalDeliveryServedAsset) identity() string {
	identity := a.object.ContentType + ":" + a.object.SHA256
	if a.manifest {
		identity += ":manifest-v1:" + a.generation + ":" + a.scriptSHA
	}
	return identity
}

// globalDeliveryAssetKey names served bytes: the content-addressed object and,
// for a manifest, the generation and tool digest stamped into it. An entry
// keyed by it is never stale.
type globalDeliveryAssetKey struct {
	contentType, sha256   string
	size                  int64
	manifest              bool
	generation, scriptSHA string // set only for a manifest
}

func (a globalDeliveryServedAsset) key() globalDeliveryAssetKey {
	k := globalDeliveryAssetKey{contentType: a.object.ContentType, sha256: a.object.SHA256, size: a.object.SizeBytes, manifest: a.manifest}
	if a.manifest {
		k.generation, k.scriptSHA = a.generation, a.scriptSHA
	}
	return k
}

// globalDeliveryAssetBody is one verified served body: gzip for JSON, as
// stored otherwise. uncached marks a body served on the slot path instead.
type globalDeliveryAssetBody struct {
	data     []byte
	gzipped  bool
	length   int64 // identity length
	uncached bool
}

// serve writes the body; an identity response to a gzipped body must hold a
// globalDeliveryInflateBytes charge.
func (b globalDeliveryAssetBody) serve(w http.ResponseWriter, r *http.Request, contentType, etag string) {
	setGlobalDeliveryAssetHeaders(w, contentType, etag)
	h := w.Header()
	if b.gzipped && acceptsGlobalDeliveryGzip(r, contentType, etag) {
		h.Set("Content-Encoding", "gzip")
		h.Set("Content-Length", strconv.Itoa(len(b.data)))
		_, _ = w.Write(b.data)
		return
	}
	h.Set("Content-Length", strconv.FormatInt(b.length, 10))
	if !b.gzipped {
		_, _ = w.Write(b.data)
		return
	}
	zr, _ := gzip.NewReader(bytes.NewReader(b.data)) // written by gzipGlobalDeliveryAsset
	_, _ = io.Copy(w, zr)
}

type globalDeliveryAssetEntry struct {
	key  globalDeliveryAssetKey
	done chan struct{} // closed once body or err is set
	body globalDeliveryAssetBody
	err  error
	refs int           // waiters and responses holding body
	lru  *list.Element // set while resident
}

func (e *globalDeliveryAssetEntry) cost() int64 {
	return int64(len(e.body.data)) + globalDeliveryAssetEntryOverhead
}

// globalDeliveryAssetCache holds verified bodies by served identity. Bodies
// no response references are evicted least recently used first to stay within
// globalDeliveryAssetCacheBytes; a body that does not fit is served on the
// slot path. Concurrent misses for one key share one fill; a failed fill is
// not kept. The zero value is ready to use.
type globalDeliveryAssetCache struct {
	mu      sync.Mutex
	entries map[globalDeliveryAssetKey]*globalDeliveryAssetEntry
	order   list.List // resident entries, front most recently used
	bytes   int64     // resident entries and inflate charges
	pending int       // fills not yet done
}

// get returns key's body, starting fill on a miss, and a release to call once
// the response is written. A body served from the cache stays resident until
// released. The fill runs detached from ctx so a departed requester does not
// fail the others sharing it.
func (c *globalDeliveryAssetCache) get(ctx context.Context, key globalDeliveryAssetKey, fill func() (globalDeliveryAssetBody, error)) (globalDeliveryAssetBody, func(), error) {
	c.mu.Lock()
	e, ok := c.entries[key]
	switch {
	case ok:
		if e.lru != nil {
			c.order.MoveToFront(e.lru)
		}
	case c.pending >= globalDeliveryAssetPendingFills:
		c.mu.Unlock()
		return globalDeliveryAssetBody{uncached: true}, func() {}, nil
	default:
		if c.entries == nil {
			c.entries = map[globalDeliveryAssetKey]*globalDeliveryAssetEntry{}
		}
		e = &globalDeliveryAssetEntry{key: key, done: make(chan struct{})}
		c.entries[key] = e
		c.pending++
		go c.fill(e, fill)
	}
	e.refs++
	c.mu.Unlock()
	select {
	case <-e.done:
	case <-ctx.Done():
		c.release(e)
		return globalDeliveryAssetBody{}, func() {}, ctx.Err()
	}
	if e.err != nil || e.body.uncached {
		c.release(e)
		return e.body, func() {}, e.err
	}
	return e.body, func() { c.release(e) }, nil
}

func (c *globalDeliveryAssetCache) release(e *globalDeliveryAssetEntry) {
	c.mu.Lock()
	e.refs--
	c.mu.Unlock()
}

// fill runs one fill. A panic fails it like any error, so it is never kept.
func (c *globalDeliveryAssetCache) fill(e *globalDeliveryAssetEntry, fill func() (globalDeliveryAssetBody, error)) {
	body, err := func() (body globalDeliveryAssetBody, err error) {
		defer func() {
			if p := recover(); p != nil {
				log.Printf("global delivery asset fill panicked sha256=%s: %v", e.key.sha256, p)
				body, err = globalDeliveryAssetBody{}, errGlobalDeliveryAssetFailed
			}
		}()
		return fill()
	}()
	c.mu.Lock()
	c.pending--
	e.body, e.err = body, err
	if err == nil && c.reserve(e.cost()) {
		e.lru = c.order.PushFront(e)
	} else {
		// Not kept: a failure is retried, and a body past the bound is served
		// on the slot path until room frees.
		delete(c.entries, e.key)
		if err == nil {
			e.body = globalDeliveryAssetBody{uncached: true}
		}
	}
	c.mu.Unlock()
	close(e.done)
}

// reserve charges n bytes, first evicting unreferenced entries least recently
// used first; it reports false, charging nothing, if they cannot make room.
// The caller holds mu.
func (c *globalDeliveryAssetCache) reserve(n int64) bool {
	for el := c.order.Back(); el != nil && c.bytes+n > globalDeliveryAssetCacheBytes; {
		prev := el.Prev()
		if old := el.Value.(*globalDeliveryAssetEntry); old.refs == 0 {
			c.order.Remove(el)
			old.lru = nil
			delete(c.entries, old.key)
			c.bytes -= old.cost()
		}
		el = prev
	}
	if c.bytes+n > globalDeliveryAssetCacheBytes {
		return false
	}
	c.bytes += n
	return true
}

// charge reserves n bytes outside any entry and returns its release, or false.
func (c *globalDeliveryAssetCache) charge(n int64) (func(), bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.reserve(n) {
		return nil, false
	}
	return func() {
		c.mu.Lock()
		c.bytes -= n
		c.mu.Unlock()
	}, true
}

func withGlobalDeliveryAssetSlot(ctx context.Context, fn func() error) error {
	select {
	case globalDeliveryAssetSlots <- struct{}{}:
		defer func() { <-globalDeliveryAssetSlots }()
	case <-ctx.Done():
		return errGlobalDeliveryAssetBusy
	}
	return fn()
}

// readGlobalDeliveryAsset reads the object whole and verifies its sha256. The
// caller holds an asset slot.
func (s *Server) readGlobalDeliveryAsset(ctx context.Context, o globalDeliveryObject) ([]byte, error) {
	body, err := s.globalDeliveryObjectStore().OpenExact(ctx, o.Key, o.ETag, o.VersionID)
	if err != nil {
		return nil, errGlobalDeliveryAssetChanged
	}
	defer body.Close()
	data, err := readGlobalDeliveryExact(body, o.SizeBytes)
	sum := sha256.Sum256(data)
	if err != nil || hex.EncodeToString(sum[:]) != o.SHA256 {
		return nil, errGlobalDeliveryAssetUnverified
	}
	return data, nil
}

// body returns a writer of the served body of verified data.
func (a globalDeliveryServedAsset) body(data []byte) (func(io.Writer), error) {
	if a.manifest {
		return globalDeliveryManifestBody(data, a.generation, a.scriptSHA)
	}
	return func(w io.Writer) { _, _ = w.Write(data) }, nil
}

// fillGlobalDeliveryAsset reads, verifies and encodes one body under a slot:
// JSON gzipped, anything else as stored.
func (s *Server) fillGlobalDeliveryAsset(a globalDeliveryServedAsset) (globalDeliveryAssetBody, error) {
	if a.manifest && !globalDeliveryJSON(a.object.ContentType) {
		// Stamped per request on the slot path; only JSON is cached stamped.
		return globalDeliveryAssetBody{uncached: true}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), globalDeliveryAssetFillTimeout)
	defer cancel()
	var body globalDeliveryAssetBody
	err := withGlobalDeliveryAssetSlot(ctx, func() error {
		data, err := s.readGlobalDeliveryAsset(ctx, a.object)
		switch {
		case err != nil:
			return err
		case globalDeliveryJSON(a.object.ContentType):
			write, err := a.body(data)
			if err != nil {
				return err
			}
			body = gzipGlobalDeliveryAsset(write)
		case len(data) > globalDeliveryAssetEntryBytes:
			body = globalDeliveryAssetBody{uncached: true}
		default:
			body = globalDeliveryAssetBody{data: data, length: int64(len(data))}
		}
		return nil
	})
	return body, err
}

// gzipGlobalDeliveryAsset gzips a body into at most
// globalDeliveryAssetEntryBytes; past that it stops and marks it uncached.
func gzipGlobalDeliveryAsset(write func(io.Writer)) globalDeliveryAssetBody {
	buf := &cappedBuffer{}
	zw, _ := gzip.NewWriterLevel(buf, gzip.BestSpeed)
	counted := &countingWriter{w: zw}
	write(counted)
	if zw.Close() != nil || buf.full {
		return globalDeliveryAssetBody{uncached: true}
	}
	// Copy to an exact allocation so cost() counts what the cache retains.
	data := make([]byte, buf.Len())
	copy(data, buf.Bytes())
	return globalDeliveryAssetBody{data: data, gzipped: true, length: counted.n}
}

// streamGlobalDeliveryAsset serves a body too large to cache, read under a
// slot for this request.
func (s *Server) streamGlobalDeliveryAsset(w http.ResponseWriter, r *http.Request, a globalDeliveryServedAsset, etag string) {
	err := withGlobalDeliveryAssetSlot(r.Context(), func() error {
		data, err := s.readGlobalDeliveryAsset(r.Context(), a.object)
		if err != nil {
			return err
		}
		write, err := a.body(data)
		if err != nil {
			return err
		}
		writer, closeStream := startGlobalDeliveryAsset(w, r, a.object.ContentType, etag)
		defer closeStream()
		write(writer)
		return nil
	})
	if err != nil && r.Context().Err() == nil {
		writeGlobalDeliveryAssetError(w, err)
	}
}

type cappedBuffer struct {
	bytes.Buffer
	full bool
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	if b.full || b.Len()+len(p) > globalDeliveryAssetEntryBytes {
		b.full = true
		return 0, errGlobalDeliveryAssetLarge
	}
	return b.Buffer.Write(p)
}
