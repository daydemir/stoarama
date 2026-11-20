package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// errDeliveryPageRetired is the one answer every public delivery route gives once
// the page is retired: page, metadata, ticket and file, for the current and for
// every pinned generation.
var errDeliveryPageRetired = errors.New("the public delivery page is retired")

// errDeliveryPageNotServing means retirement is being armed: the quiesce state is
// committed, so no NEW public request may start, but the page is not retired yet
// and no exemption class has opened. The API answers 503, not 410.
var errDeliveryPageNotServing = errors.New("the public delivery page is not serving")

const globalDeliveryPrefix = "datasets/global-street-scores-delivery/47/"
const publicGlobalDeliveryAPI = "/api/v1/global-street-scores-delivery"
const globalDeliveryAPI = "/api/v1/account/global-street-scores-delivery"

// deliveryPublicReaderTTL bounds one admitted public response's lease. It is the
// hard ceiling on how long a crashed or abandoned response can delay retirement:
// the lease is deleted as soon as the handler returns, so the normal case is
// milliseconds, and this bound is only reached by a process that died mid-write.
const deliveryPublicReaderTTL = 2 * time.Minute

// deliveryPublicResponseBudget is the deadline every public storage read runs
// under, and it is STRICTLY shorter than the lease. It is a var only so a test
// can shrink it; production never assigns it. A stalled HEAD, registry
// fetch, asset read, file open/range or stalled body is therefore cancelled by
// this context and the handler returns (releasing its lease) BEFORE the durable
// lease could expire, so a stalled read can never outlive the very lease that
// retirement drains against. HTTP WriteTimeout is not used for this: it does not
// bound a read that never completes, and it is a server-wide setting rather than
// this response's admission.
var deliveryPublicResponseBudget = 90 * time.Second

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

// deliveryPublicGate is the ONE shared/exclusive admission the public delivery
// origin uses. The exclusive side is retirement (guard_r2_delivery_page_retirement
// in 0225); the shared side is every public read, every ticket and publication.
// The pool implementation is the product path; tests provide a stub. There is no
// cache: retirement is a serving fact read live, and a reader that cannot prove
// the page is serving is refused.
type deliveryPublicGate interface {
	// DeliveryServing reports the live serving state: (retired, serving).
	DeliveryServing(context.Context) (bool, bool, error)
	// AdmitPublicReader records one lease before the first body byte.
	AdmitPublicReader(context.Context, string) error
	// ReleasePublicReader drops this response's lease after its last write.
	ReleasePublicReader(context.Context, string) error
	// RecordPublicDeliveryTicket records a capability, or reports the page is
	// no longer serving, in the same statement that checks it.
	RecordPublicDeliveryTicket(context.Context, globalDeliveryObject, string, time.Duration) (bool, error)
}

type poolDeliveryGate struct{ pool *pgxpool.Pool }

// DeliveryServing reads both live facts in one statement. r2_delivery_public_serving()
// is the SQL function the retirement trigger also requires, so the API and the
// arm gate can never disagree.
func (p poolDeliveryGate) DeliveryServing(ctx context.Context) (bool, bool, error) {
	var retired, serving bool
	if err := p.pool.QueryRow(ctx, `SELECT r2_delivery_page_retired(),r2_delivery_public_serving()`).Scan(&retired, &serving); err != nil {
		return false, false, errors.New("delivery serving state unavailable")
	}
	return retired, serving, nil
}

// AdmitPublicReader commits one lease row before the handler reads any registry,
// metadata, ticket or file. 0225's admission trigger takes the shared public lock
// and refuses unless the page is serving at that instant, so a reader admitted
// after quiesce is impossible and an admitted reader is visible to retirement.
func (p poolDeliveryGate) AdmitPublicReader(ctx context.Context, readerID string) error {
	_, err := p.pool.Exec(ctx, `INSERT INTO r2_delivery_public_readers(reader_id,expires_at)
 VALUES($1,clock_timestamp()+$2::interval)`, readerID, deliveryPublicReaderTTL.String())
	if err != nil {
		return err
	}
	return nil
}

// ReleasePublicReader drops the lease. It uses a background context on purpose:
// the request context is already cancelled by the time a streamed response ends,
// and a failed release must not leave the page retired forever — the lease's own
// expiry bounds that anyway.
func (p poolDeliveryGate) ReleasePublicReader(ctx context.Context, readerID string) error {
	_, err := p.pool.Exec(ctx, `DELETE FROM r2_delivery_public_readers WHERE reader_id=$1`, readerID)
	return err
}

// deliveryPublicLock is r2_delivery_public_lock() in 0225, the ONE advisory key
// shared by public reader admission, capability minting and registry publication
// and taken exclusively by quiesce and retirement. It is a Go constant as well
// because the API must take it without depending on the function existing in a
// fixture schema, exactly as the neighbouring cleanup fence does. The two values
// are asserted equal by TestDeliveryPublicLockMatchesSQL.
const deliveryPublicLock int64 = 7343226011104

// deliveryTicketTTL is the capability lifetime the page has always handed out.
const deliveryTicketTTL = 5 * time.Minute

// RecordPublicDeliveryTicket writes the capability row in one statement. 0225's
// admission trigger takes the shared public lock and requires the page to be
// serving, so the row and the capability it describes cannot be separated from
// retirement: either the row commits before retirement's exclusive lock, and
// retirement drains it, or retirement wins and this statement raises instead.
//
// It records only the capability's own expiry. It deliberately records NO
// transfer bound: a provider does not cut an already-started transfer at presign
// expiry, and a bound the server invented would be a guess, not evidence. The
// authoritative end of any such transfer is the operator's cutover authority,
// which retirement requires separately.
func (p poolDeliveryGate) RecordPublicDeliveryTicket(ctx context.Context, object globalDeliveryObject, pathSHA string, ttl time.Duration) (bool, error) {
	var id int64
	err := p.pool.QueryRow(ctx, `INSERT INTO r2_delivery_download_tickets(bucket,object_key,etag,size_bytes,object_path_sha256,expires_at)
 SELECT $1,$2,$3,$4,$5,clock_timestamp()+$6::interval
 WHERE r2_delivery_public_serving() RETURNING id`,
		"stoarama", object.Key, strings.Trim(object.ETag, `"`), object.SizeBytes, pathSHA,
		ttl.String()).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		// 55000 is the admission trigger refusing: the page stopped serving
		// between the check and the insert. That is a refusal, not an outage.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "55000" {
			return false, nil
		}
		return false, errors.New("delivery ticket ledger unavailable")
	}
	return true, nil
}

// deliveryGate resolves the gate: the test stub when set, otherwise the pool.
func (s *Server) deliveryGate() (deliveryPublicGate, error) {
	if s.deliveryPublic != nil {
		return s.deliveryPublic, nil
	}
	if s.pool == nil {
		return nil, errors.New("delivery serving state unavailable")
	}
	return poolDeliveryGate{s.pool}, nil
}

// deliveryPageRetired reads the committed serving state live, on every call.
// There is no cache and no bypass: retirement is a serving fact, not a claim.
func (s *Server) deliveryPageRetired(ctx context.Context) (bool, error) {
	gate, err := s.deliveryGate()
	if err != nil {
		return false, err
	}
	retired, _, err := gate.DeliveryServing(ctx)
	return retired, err
}

// admitPublicDelivery is the ONE public admission for every delivery route. It
// reads the live serving state, refuses with 410 when retired and 503 while
// retirement is arming, then records a lease that retirement must drain. The
// returned release must run after the response's last write; callers defer it.
// A caller that cannot prove the page is serving refuses: fail closed.
// admitPublicDelivery admits one public reader and returns the SAME bounded
// context every subsequent public gate and storage call must use.
//
// The bound starts BEFORE the admission SQL, not after it. A slow admission
// acknowledgment can otherwise consume the lease window and then let a further
// upstream read run past the durable lease. Starting it here means the whole
// response — admission, gate reads and every public storage read — is bounded by
// one deadline strictly shorter than the lease, and an admission that consumed
// the budget is refused before any storage is opened.
func (s *Server) admitPublicDelivery(w http.ResponseWriter, r *http.Request) (ctx context.Context, release func(), ok bool) {
	ctx, cancel := leaseBoundContext(r.Context())
	gate, err := s.deliveryGate()
	if err != nil {
		cancel()
		util.WriteError(w, http.StatusServiceUnavailable, "Delivery availability is unavailable. Reload to retry.")
		return nil, nil, false
	}
	retired, serving, err := gate.DeliveryServing(ctx)
	if err != nil {
		cancel()
		util.WriteError(w, http.StatusServiceUnavailable, "Delivery availability is unavailable. Reload to retry.")
		return nil, nil, false
	}
	if retired {
		cancel()
		util.WriteError(w, http.StatusGone, "This dataset has been retired. See the published file list for NAS paths.")
		return nil, nil, false
	}
	if !serving {
		cancel()
		util.WriteError(w, http.StatusServiceUnavailable, "This dataset is being taken offline. Reload in a few minutes.")
		return nil, nil, false
	}
	id, err := newDeliveryReaderID()
	if err != nil {
		cancel()
		util.WriteError(w, http.StatusServiceUnavailable, "Delivery availability is unavailable. Reload to retry.")
		return nil, nil, false
	}
	if err = gate.AdmitPublicReader(ctx, id); err != nil {
		cancel()
		// A 55000 is retirement arming in this instant: refuse as not serving.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "55000" {
			util.WriteError(w, http.StatusServiceUnavailable, "This dataset is being taken offline. Reload in a few minutes.")
			return nil, nil, false
		}
		util.WriteError(w, http.StatusServiceUnavailable, "Delivery availability is unavailable. Reload to retry.")
		return nil, nil, false
	}
	// Refuse an acknowledgment that consumed the response budget: opening storage
	// after the deadline would run under an expired lease.
	if ctx.Err() != nil {
		cancel()
		util.WriteError(w, http.StatusServiceUnavailable, "This dataset is being taken offline. Reload in a few minutes.")
		return nil, nil, false
	}
	return ctx, func() {
		// Push every byte this handler wrote onto the socket BEFORE dropping the
		// lease, so the lease spans the response through its last body write and
		// not merely through the handler's return. net/http's own final flush
		// after return writes only the terminating chunk, no body bytes.
		if f, isFlusher := w.(http.Flusher); isFlusher {
			f.Flush()
		}
		// The request context is done by now; the lease is still ours to drop.
		cancel()
		rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer rcancel()
		_ = gate.ReleasePublicReader(rctx, id)
	}, true
}

// leaseBoundContext derives the context every public storage read runs under. It
// ends at the response budget, strictly shorter than the reader lease, so a
// stalled read is cancelled and the handler returns (and releases its lease)
// before the lease can expire. Every storage call on a public route takes this
// context; none relies on HTTP WriteTimeout, which bounds neither a read that
// never completes nor this response's admission.
func leaseBoundContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, deliveryPublicResponseBudget)
}

// newDeliveryReaderID is a 128-bit random lease id, the table's only key.
func newDeliveryReaderID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
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
	// A retired page serves nothing at all: no registry, for any generation. A
	// page being quiesced admits no NEW reader either. This check runs before the
	// cleanup fence, before the cached current and pinned registries, and before
	// any object read, so retirement cannot be bypassed by a warm cache or by
	// pinning the generation that was serving.
	if public {
		gate, err := s.deliveryGate()
		if err != nil {
			return nil, err
		}
		retired, serving, err := gate.DeliveryServing(ctx)
		if err != nil {
			return nil, err
		}
		if retired {
			return nil, errDeliveryPageRetired
		}
		if !serving {
			return nil, errDeliveryPageNotServing
		}
	}
	// A private load may serve a warm entry without touching the pool: it is not
	// the public origin and has no cleanup fence to honour.
	if !public {
		if hit, found := s.cachedDeliveryRegistry(public, generation); found {
			return hit, nil
		}
	}
	// Two SHORT fence transactions bracket the object read. Neither is held across
	// a network call: the first resolves the reviewed generation and a warm hit,
	// the read runs with NO connection and NO lock held, and the second rechecks
	// the serving state, validates the keep snapshot on its own transaction and
	// records the observation. This is what lets a quiesce commit promptly while a
	// cold load is outstanding, and it keeps the registry mutex off every read.
	if public && s.pool != nil {
		f, err := s.beginDeliveryFence(ctx)
		if err != nil {
			return nil, err
		}
		// Resolve the reviewed generation while the fence is held, then release
		// the connection before any storage read.
		var fenced bool
		if fenced, err = f.fenced(ctx); err != nil {
			f.rollback()
			return nil, err
		}
		if fenced {
			const reviewed = "c4049f6dc0f940be8b7eb8b9bef6f42f"
			if generation != "" && generation != reviewed {
				f.rollback()
				return nil, errors.New("public publication deferred during NAS cleanup")
			}
			generation = reviewed
		}
		// A warm entry for the RESOLVED generation is served from memory; nothing
		// is published or observed on that path, so release the fence and return.
		if hit, found := s.cachedDeliveryRegistry(true, generation); found {
			f.rollback()
			return hit, nil
		}
		if err = f.release(ctx); err != nil {
			return nil, err
		}
	} else if s.pool == nil {
		// No pool (unit stubs): there is no cleanup fence to honour, so a warm
		// entry is served directly. Production always has a pool and always
		// resolves the fence above.
		if hit, found := s.cachedDeliveryRegistry(public, generation); found {
			return hit, nil
		}
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
	// The object read runs with NO fence transaction and NO registry mutex held,
	// so a quiesce behind it never waits on storage I/O this request owns.
	head, err := store.Head(ctx, key)
	if err != nil || head.SizeBytes <= 0 || head.SizeBytes > 64<<20 {
		return nil, errors.New("dataset registry unavailable")
	}
	body, err := store.OpenExact(ctx, key, head.ETag, head.VersionID)
	if err != nil {
		return nil, errors.New("dataset registry changed")
	}
	bodyBytes, err := func() ([]byte, error) {
		defer body.Close()
		raw, err := io.ReadAll(io.LimitReader(body, head.SizeBytes+1))
		if err != nil || int64(len(raw)) != head.SizeBytes {
			return nil, errors.New("dataset registry length differs")
		}
		return decodeGlobalDeliveryRegistry(raw)
	}()
	if err != nil {
		return nil, err
	}
	var result globalDeliveryRegistry
	if err = json.Unmarshal(bodyBytes, &result); err != nil {
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
			// FINAL publish fence: one short transaction that decides the allowed
			// generation, revalidates the keep snapshot and records the
			// observation, and the cache mutation happens INSIDE it so the commit
			// is the publication point. A cleanup batch that started during the
			// read is therefore seen here (fenced() is rechecked), and a failed
			// commit can never leave a published cache entry behind.
			f, err := s.beginDeliveryFence(ctx)
			if err != nil {
				return nil, err
			}
			fenced, err := f.fenced(ctx)
			if err != nil {
				f.rollback()
				return nil, err
			}
			if fenced {
				const reviewed = "c4049f6dc0f940be8b7eb8b9bef6f42f"
				if generation != reviewed {
					f.rollback()
					return nil, errors.New("public publication deferred during NAS cleanup")
				}
				// The resolved generation was substituted BEFORE the read; if the
				// batch started during the read, the object actually read must
				// already be the reviewed generation or the publication is refused.
				if result.Generation != reviewed {
					f.rollback()
					return nil, errors.New("public publication deferred during NAS cleanup")
				}
			}
			if err = RequireDeliveryKeep(ctx, f.tx, keep); err != nil {
				f.rollback()
				return nil, err
			}
			if generation == "" {
				if _, err = f.tx.Exec(ctx, `INSERT INTO r2_delivery_serving_generation(singleton,generation)
 VALUES(true,$1) ON CONFLICT(singleton) DO UPDATE SET generation=excluded.generation,observed_at=clock_timestamp()`, result.Generation); err != nil {
					f.rollback()
					return nil, errors.New("delivery serving generation record unavailable")
				}
			}
			// Publish the cache entry while the fence still holds the public lock,
			// so arming and any other cold put serialize with this exact mutation.
			s.publishDeliveryRegistry(public, generation, &result)
			if err = f.commit(ctx); err != nil {
				// Undo the just-published entry: a failed commit must never leave
				// a cache entry that the database did not accept. The entry is
				// keyed by the registry's OWN generation, which is what publish
				// stored for the current case.
				s.unpublishDeliveryRegistry(public, generation, result.Generation)
				return nil, err
			}
			return &result, nil
		}
	}
	// Publish under the mutex, which is taken only for these few assignments.
	s.publishDeliveryRegistry(public, generation, &result)
	return &result, nil
}

// deliveryFence is ONE short transaction that holds the shared public lock, the
// cleanup fence and the serving recheck together, and is released before any
// object read. It is a pointer so a caller can drop it early; every exit path
// rolls back a still-open transaction so no connection or lock leaks.
type deliveryFence struct {
	pool *pgxpool.Pool
	tx   pgx.Tx
}

// beginDeliveryFence opens the fence transaction and takes the shared public lock
// and the serving recheck under it.
func (s *Server) beginDeliveryFence(ctx context.Context) (*deliveryFence, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, errors.New("public cleanup fence unavailable")
	}
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared($1)`, deliveryPublicLock); err != nil {
		tx.Rollback(context.Background())
		return nil, errors.New("public publication fence unavailable")
	}
	var retired, serving bool
	if err = tx.QueryRow(ctx, `SELECT r2_delivery_page_retired(),r2_delivery_public_serving()`).Scan(&retired, &serving); err != nil {
		tx.Rollback(context.Background())
		return nil, errors.New("public delivery serving state unavailable")
	}
	if retired {
		tx.Rollback(context.Background())
		return nil, errDeliveryPageRetired
	}
	if !serving {
		tx.Rollback(context.Background())
		return nil, errDeliveryPageNotServing
	}
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(7343226011101)`); err != nil {
		tx.Rollback(context.Background())
		return nil, errors.New("public cleanup fence unavailable")
	}
	return &deliveryFence{pool: s.pool, tx: tx}, nil
}

// fenced reports whether a separately approved cleanup batch is outstanding.
func (f *deliveryFence) fenced(ctx context.Context) (bool, error) {
	var fenced bool
	if err := f.tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nas_cleanup_plans p WHERE p.mode='enabled' AND p.approved_until>now()) OR EXISTS(SELECT 1 FROM nas_cleanup_items i WHERE i.delete_ever_issued AND i.updated_at>now()-interval '2 minutes')`).Scan(&fenced); err != nil {
		return false, errors.New("public cleanup fence unavailable")
	}
	return fenced, nil
}

// release drops the fence transaction without publishing anything, freeing its
// connection and locks before a storage read.
func (f *deliveryFence) release(ctx context.Context) error {
	err := f.tx.Rollback(context.Background())
	f.tx = nil
	if err != nil {
		return errors.New("public publication fence unavailable")
	}
	return nil
}

// rollback drops a still-open fence; it is safe on nil and after release.
func (f *deliveryFence) rollback() {
	if f == nil || f.tx == nil {
		return
	}
	_ = f.tx.Rollback(context.Background())
	f.tx = nil
}

func (f *deliveryFence) commit(ctx context.Context) error {
	err := f.tx.Commit(ctx)
	f.tx = nil
	if err != nil {
		return errors.New("public publication fence unavailable")
	}
	return nil
}

// cachedDeliveryRegistry returns a warm cache entry, holding the mutex only for
// the pointer reads. A 15s current entry and every pinned generation are exact
// and immutable, so they need no pool round trip.
func (s *Server) cachedDeliveryRegistry(public bool, generation string) (*globalDeliveryRegistry, bool) {
	s.globalDeliveryMu.Lock()
	defer s.globalDeliveryMu.Unlock()
	if public && generation != "" {
		if r := s.publicGlobalDeliveryRegistry; r != nil && r.Generation == generation {
			return r, true
		}
		if r := s.publicPinnedDeliveryRegistry; r != nil && r.Generation == generation {
			return r, true
		}
		return nil, false
	}
	if public && generation == "" {
		if r := s.publicGlobalDeliveryRegistry; r != nil && time.Since(s.publicGlobalDeliveryRegistryAt) < 15*time.Second {
			return r, true
		}
		return nil, false
	}
	if !public {
		if r := s.globalDeliveryRegistry; r != nil && time.Since(s.globalDeliveryRegistryAt) < 15*time.Second {
			return r, true
		}
	}
	return nil, false
}

// unpublishDeliveryRegistry removes a cache entry that was published inside a
// fence whose commit then failed, so a failed commit never leaves a servable
// entry behind. It only clears the entry for this exact generation.
func (s *Server) unpublishDeliveryRegistry(public bool, requestedGeneration, storedGeneration string) {
	s.globalDeliveryMu.Lock()
	defer s.globalDeliveryMu.Unlock()
	if !public {
		s.globalDeliveryRegistry = nil
		return
	}
	if requestedGeneration != "" {
		if r := s.publicPinnedDeliveryRegistry; r != nil && r.Generation == requestedGeneration {
			s.publicPinnedDeliveryRegistry = nil
		}
		return
	}
	if r := s.publicGlobalDeliveryRegistry; r != nil && r.Generation == storedGeneration {
		s.publicGlobalDeliveryRegistry = nil
	}
}

// publishDeliveryRegistry stores a freshly loaded registry under the mutex.
func (s *Server) publishDeliveryRegistry(public bool, generation string, result *globalDeliveryRegistry) {
	s.globalDeliveryMu.Lock()
	defer s.globalDeliveryMu.Unlock()
	if !public {
		s.globalDeliveryRegistry = result
		s.globalDeliveryRegistryAt = time.Now()
		return
	}
	if generation != "" {
		s.publicPinnedDeliveryRegistry = result
		return
	}
	s.publicGlobalDeliveryRegistry = result
	s.publicGlobalDeliveryRegistryAt = time.Now()
}

func (s *Server) handleGlobalDeliveryPage(w http.ResponseWriter, r *http.Request) {
	// Every public delivery response admits exactly one reader lease and holds it
	// until its last socket write, so retirement cannot commit mid-response.
	_, release, ok := s.admitPublicDelivery(w, r)
	if !ok {
		return
	}
	defer release()
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
	_, release, ok := s.admitPublicDelivery(w, r)
	if !ok {
		return
	}
	defer release()
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
	// A private account response is not the public origin: it takes no lease and
	// reads no serving state, so the owner's own tooling still works after
	// retirement. The public route admits and holds one lease for its whole body.
	ctx := r.Context()
	var release func()
	if public {
		var ok bool
		// The bound started before admission and is reused for every public
		// storage read below; no second, later-starting deadline is derived.
		if ctx, release, ok = s.admitPublicDelivery(w, r); !ok {
			return
		}
		defer release()
	}
	registry, err := s.loadGlobalDeliveryRegistryScope(ctx, public, r.URL.Query().Get("generation"))
	if err != nil {
		if errors.Is(err, errDeliveryPageRetired) {
			util.WriteError(w, http.StatusGone, "This dataset has been retired. See the published file list for NAS paths.")
			return
		}
		if errors.Is(err, errDeliveryPageNotServing) {
			util.WriteError(w, http.StatusServiceUnavailable, "This dataset is being taken offline. Reload in a few minutes.")
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
	body, err := s.globalDeliveryObjectStore().OpenExact(ctx, object.Key, object.ETag, object.VersionID)
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
func (s *Server) globalDeliveryFile(ctx context.Context, w http.ResponseWriter, r *http.Request) (globalDeliveryObject, bool) {
	public := strings.HasPrefix(r.URL.Path, publicGlobalDeliveryAPI+"/")
	if !public && !authorizeGlobalDelivery(w, r) {
		return globalDeliveryObject{}, false
	}
	// The caller owns the reader lease and holds it across the response body. A
	// ticket lookup is metadata only, but it still runs inside that lease so a
	// retirement cannot arm between admission and the capability row.
	registry, err := s.loadGlobalDeliveryRegistryScope(ctx, public, r.URL.Query().Get("generation"))
	if err != nil {
		if errors.Is(err, errDeliveryPageRetired) {
			util.WriteError(w, http.StatusGone, "This dataset has been retired. See the published file list for NAS paths.")
			return globalDeliveryObject{}, false
		}
		if errors.Is(err, errDeliveryPageNotServing) {
			util.WriteError(w, http.StatusServiceUnavailable, "This dataset is being taken offline. Reload in a few minutes.")
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
	head, err := s.globalDeliveryObjectStore().HeadExact(ctx, object.Key, object.ETag, object.VersionID)
	if err != nil || head.SizeBytes != object.SizeBytes {
		util.WriteError(w, http.StatusConflict, "The recorded R2 file is unavailable or changed.")
		return globalDeliveryObject{}, false
	}
	return object, true
}

// recordPublicTickets records a public capability against the live serving gate.
// 0225's ticket trigger takes the shared public lock and requires the page to be
// serving, so the row and the capability cannot be separated from retirement.
func (s *Server) recordPublicTickets(ctx context.Context, object globalDeliveryObject, pathSHA string) (bool, error) {
	gate, err := s.deliveryGate()
	if err != nil {
		return false, err
	}
	return gate.RecordPublicDeliveryTicket(ctx, object, pathSHA, deliveryTicketTTL)
}

func (s *Server) handleGlobalDeliveryTicket(w http.ResponseWriter, r *http.Request) {
	public := strings.HasPrefix(r.URL.Path, publicGlobalDeliveryAPI+"/")
	ctx := r.Context()
	var release func()
	if public {
		var ok bool
		if ctx, release, ok = s.admitPublicDelivery(w, r); !ok {
			return
		}
		defer release()
	}
	object, ok := s.globalDeliveryFile(ctx, w, r)
	if !ok {
		return
	}
	if r.URL.Query().Get("sha256") != object.SHA256 {
		util.WriteError(w, http.StatusBadRequest, "Expected SHA-256 is required.")
		return
	}
	capability, err := s.globalDeliveryObjectStore().PresignGetExactDownloadRequest(ctx, object.Key, object.ETag, object.VersionID, path.Base(object.Path), deliveryTicketTTL)
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
	if public {
		minted, err := s.recordPublicTickets(ctx, object, hex.EncodeToString(pathSHA[:]))
		if err != nil {
			util.WriteError(w, http.StatusServiceUnavailable, "Download capability unavailable.")
			return
		}
		if !minted {
			util.WriteError(w, http.StatusServiceUnavailable, "This dataset is being taken offline. Reload in a few minutes.")
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
	public := strings.HasPrefix(r.URL.Path, publicGlobalDeliveryAPI+"/")
	ctx := r.Context()
	var release func()
	if public {
		var ok bool
		// One bound for admission plus the whole streamed body, through its last
		// socket write, so a stalled body cannot outlive the durable lease.
		if ctx, release, ok = s.admitPublicDelivery(w, r); !ok {
			return
		}
		defer release()
	}
	object, ok := s.globalDeliveryFile(ctx, w, r)
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
		body, err = store.OpenExact(ctx, object.Key, object.ETag, object.VersionID)
	} else {
		body, err = store.OpenExactRange(ctx, object.Key, object.ETag, object.VersionID, part.start, part.end)
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
