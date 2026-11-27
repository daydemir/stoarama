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
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/daydemir/stoarama/backend/internal/util"
)

// globalDeliveryAssetCacheBytes bounds the verified asset bodies kept between
// requests. It is part of the instance's memory budget (512 MiB, GOMEMLIMIT
// 400 MiB) beside the parsed registry (~22 MB) and the bodies read under
// globalDeliveryAssetSlots; TestGlobalDeliveryMemoryBudget measures all three.
// A production generation's public assets gzip to ~12 MB (2026-11-26:
// growth.json 1.3 MB, 60 stream manifests ~9 MB).
const globalDeliveryAssetCacheBytes = 48 << 20

// globalDeliveryAssetEntryBytes caps one cached body. A body that encodes
// larger is read from R2 under a slot on every request.
const globalDeliveryAssetEntryBytes = 4 << 20

// globalDeliveryAssetEntryOverhead is charged per entry besides its body, so
// bodyless entries (uncached markers) are bounded too.
const globalDeliveryAssetEntryOverhead = 512

// globalDeliveryAssetFillTimeout bounds a fill's slot wait and R2 read. A fill
// outlives the request that started it, so it ends with the API's 60 s
// WriteTimeout.
const globalDeliveryAssetFillTimeout = time.Minute

var (
	errGlobalDeliveryAssetChanged    = errors.New("Dataset asset identity changed. Reload to retry.")
	errGlobalDeliveryAssetUnverified = errors.New("Dataset asset verification failed.")
	errGlobalDeliveryManifestInvalid = errors.New("Manifest payload invalid.")
	errGlobalDeliveryAssetBusy       = errors.New("Dataset asset temporarily unavailable. Reload to retry.")
	errGlobalDeliveryAssetLarge      = errors.New("dataset asset exceeds the cache entry cap")
)

func writeGlobalDeliveryAssetError(w http.ResponseWriter, err error) {
	status := http.StatusConflict
	if errors.Is(err, errGlobalDeliveryAssetBusy) {
		status = http.StatusServiceUnavailable
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

// identity names the served bytes: the content-addressed object and, for a
// manifest, the generation and tool digest stamped into it. An entry keyed by
// it is never stale.
func (a globalDeliveryServedAsset) identity() string {
	identity := a.object.ContentType + ":" + a.object.SHA256
	if a.manifest {
		identity += ":manifest-v1:" + a.generation + ":" + a.scriptSHA
	}
	return identity
}

// globalDeliveryAssetBody is one verified served body: gzip for JSON, as
// stored otherwise. uncached marks a body over globalDeliveryAssetEntryBytes.
type globalDeliveryAssetBody struct {
	data     []byte
	gzipped  bool
	length   int64 // identity length
	uncached bool
}

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
	key  string
	done chan struct{} // closed once body or err is set
	body globalDeliveryAssetBody
	err  error
	lru  *list.Element // set while resident
}

func (e *globalDeliveryAssetEntry) cost() int64 {
	return int64(len(e.key)+len(e.body.data)) + globalDeliveryAssetEntryOverhead
}

// globalDeliveryAssetCache holds verified bodies by served identity, least
// recently used evicted past globalDeliveryAssetCacheBytes. Concurrent misses
// for one identity share one fill; a failed fill is not kept. The zero value is
// ready to use.
type globalDeliveryAssetCache struct {
	mu      sync.Mutex
	entries map[string]*globalDeliveryAssetEntry
	order   list.List // front is most recently used
	bytes   int64
}

// get returns key's body, starting fill on a miss. The fill runs detached from
// ctx so a departed requester does not fail the others sharing it.
func (c *globalDeliveryAssetCache) get(ctx context.Context, key string, fill func() (globalDeliveryAssetBody, error)) (globalDeliveryAssetBody, error) {
	c.mu.Lock()
	e, ok := c.entries[key]
	switch {
	case !ok:
		if c.entries == nil {
			c.entries = map[string]*globalDeliveryAssetEntry{}
		}
		e = &globalDeliveryAssetEntry{key: key, done: make(chan struct{})}
		c.entries[key] = e
		go c.fill(e, fill)
	case e.lru != nil:
		c.order.MoveToFront(e.lru)
	}
	c.mu.Unlock()
	select {
	case <-e.done:
		return e.body, e.err
	case <-ctx.Done():
		return globalDeliveryAssetBody{}, ctx.Err()
	}
}

func (c *globalDeliveryAssetCache) fill(e *globalDeliveryAssetEntry, fill func() (globalDeliveryAssetBody, error)) {
	body, err := fill()
	c.mu.Lock()
	e.body, e.err = body, err
	if err != nil {
		delete(c.entries, e.key)
	} else {
		e.lru = c.order.PushFront(e)
		c.bytes += e.cost()
		for c.bytes > globalDeliveryAssetCacheBytes {
			old := c.order.Remove(c.order.Back()).(*globalDeliveryAssetEntry)
			delete(c.entries, old.key)
			c.bytes -= old.cost()
		}
	}
	c.mu.Unlock()
	close(e.done)
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
