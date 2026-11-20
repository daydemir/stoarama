package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/jackc/pgx/v5"
)

type inverseDeleteBinding struct {
	Bucket, EndpointSHA256, Key, ETag                      string
	ManifestSHA256, PolicySHA256, PicksSHA256, DrainSHA256 string
	SourceID, StreamID, SizeBytes                          int64
	LastModified                                           time.Time
}

type inverseLastModifiedError struct{}

func (inverseLastModifiedError) Error() string {
	return "listing/HEAD LastModified missing, changed, future or outside the pre-fence cutoff"
}

// Only the concrete executor verifier below can issue this private capability,
// for either lane. Copies share consumption state, and the DB independently
// forbids reminting. ctx bounds this key's network work; on the keep-28 lane it
// also ends at custodyEnd, the local instant NAS custody expires, and custody
// is the proof the intent carries.
type inverseDeleteAuthority struct {
	binding    inverseDeleteBinding
	guard      *inverseWireGuard
	used       *atomic.Bool
	gate       *inverseSendGate
	ctx        context.Context
	cancel     context.CancelFunc
	custody    keep28Custody
	custodyEnd time.Time
}

// inverseSendGate closes when a batch hits a hard failure. A DELETE whose first
// request byte is not yet written never sends after that; closing waits for a
// first write already under way.
type inverseSendGate struct {
	mu     sync.RWMutex
	closed bool
}

func (g *inverseSendGate) close() {
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()
}

func (g *inverseSendGate) isClosed() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.closed
}

// first runs a DELETE's first write unless the gate has closed.
func (g *inverseSendGate) first(write func() (int, error)) (int, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.closed {
		return 0, errInverseAdmissionClosed
	}
	return write()
}

func (a *inverseDeleteAuthority) requireWire(ctx context.Context, bucket, endpoint string) error {
	if a == nil || a.used == nil || !a.used.CompareAndSwap(false, true) {
		return errors.New("inverse authority absent or consumed")
	}
	return a.check(ctx, bucket, endpoint)
}
func (a *inverseDeleteAuthority) check(ctx context.Context, bucket, endpoint string) error {
	if a.guard == nil || bucket != a.binding.Bucket || endpoint != a.binding.EndpointSHA256 {
		return errors.New("inverse wire binding differs")
	}
	if err := a.guard.check(ctx, a.binding); err != nil {
		return err
	}
	lane := a.guard.a.opts.Lane
	var issued bool
	err := a.guard.tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM `+lane.attemptTable()+` WHERE bucket=$1 AND object_key=$2)`, bucket, a.binding.Key).Scan(&issued)
	if err != nil {
		return err
	}
	if !issued {
		return errors.New("durable wire issuance missing")
	}
	if lane == inverseNASKeep28 {
		return a.checkCustody(ctx)
	}
	return nil
}

// protectionEnvelope is the ONE wire protection envelope shared by the RAW and
// collated paths: the metadata barrier, ordered shared protection locks, the
// exact COPY key latch, the KE-epoch check and a locked live reload, then fresh
// controller evidence immediately before the DELETE. load is the only per-lane
// part (typed RAW or typed collated); everything that guards the DELETE is
// common, so the collated path cannot drift into a weaker copy. The reservation
// commits before wire, so this envelope is the only thing standing between a
// stale protection change and the DELETE.
func (a *inverseApplier) protectionEnvelope(ctx context.Context, tx pgx.Tx, e inverseEntry, load func(context.Context, pgx.Tx, inverseEntry) error) error {
	if err := inverseProtectionBarrier(ctx, tx); err != nil {
		return err
	}
	if err := inverseProtectionLocks(ctx, tx, true); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `SELECT lock_nas_r2_copy_key($1,$2)`, a.store.Bucket(), e.Key); err != nil {
		return err
	}
	if err := load(ctx, tx, e); err != nil {
		return err
	}
	// KE folder membership, picks and pinned windows are cached per batch. A
	// catalog identity change must not survive into wire authority.
	var epoch int64
	if err := tx.QueryRow(ctx, `SELECT epoch FROM r2_inverse_ke_epoch FOR SHARE`).Scan(&epoch); err != nil {
		return err
	}
	if epoch != a.keEpoch {
		return errors.New("KE catalog identity changed; refresh partition before further wire attempts")
	}
	// Fresh controller evidence and live writer categories immediately before the
	// DELETE, not just at batch start.
	return a.checkEvidence(ctx, tx)
}

func (a *inverseApplier) prepareWireTarget(ctx context.Context, tx pgx.Tx, e inverseEntry) (inverseRawTarget, error) {
	// The step's own bound replaces the role's statement_timeout, whose 57014
	// would end a lock wait publication also takes; only a cancel then stops it.
	if _, err := tx.Exec(ctx, `SET LOCAL statement_timeout = 0`); err != nil {
		return inverseRawTarget{}, err
	}
	var target inverseRawTarget
	err := a.protectionEnvelope(ctx, tx, e, func(ctx context.Context, tx pgx.Tx, e inverseEntry) error {
		t, err := a.loadTarget(ctx, tx, e)
		if err != nil {
			return err
		}
		if err = a.checkTarget(ctx, tx, t, e, true); err != nil {
			return err
		}
		target = t
		return nil
	})
	return target, err
}
func (a *inverseApplier) checkWireHead(ctx context.Context, tx pgx.Tx, e inverseEntry, target inverseRawTarget) error {
	head, err := a.store.Head(ctx, e.Key)
	if err != nil {
		return err
	}
	var now time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return err
	}
	item := r2RetireItem{r2RetireCandidate: r2RetireCandidate{SizeBytes: e.SizeBytes, SHA256: target.SHA, ETag: e.ETag}}
	if err = r2RetireValidateGuardIdentity(head, item, target.ETag); err != nil {
		return err
	}
	// HEAD's Last-Modified is an HTTP date in whole seconds; listings carry
	// milliseconds. R2 floors the listing time, so compare at second precision.
	// This still detects every rewrite after the listing: the listing time is
	// before the fixed cutoff, and a later write's second is after it. A rewrite
	// before the listing is already in the listing.
	if !head.LastModified.Equal(e.LastModified.Truncate(time.Second)) || !inverseLastModifiedAllowed(e.LastModified, a.summary.CreatedAt, now) {
		return inverseLastModifiedError{}
	}
	if err = a.checkMultipart(ctx, tx); err != nil {
		return err
	}
	return nil
}

// authorize issues one key's single-use DELETE authority on the applier's lane.
// Keep-28 adds its NAS custody proof and bounds the authority by its expiry.
// Preparing the wire transaction waits on protection locks under its own
// inverseDBTimeout; the key's network budget starts once it holds them.
func (a *inverseApplier) authorize(ctx context.Context, tx pgx.Tx, e inverseEntry, gate *inverseSendGate) (*inverseDeleteAuthority, error) {
	db, cancelDB := context.WithTimeout(ctx, inverseDBTimeout)
	defer cancelDB()
	target, err := a.prepareWireTarget(db, tx, e)
	if err != nil {
		return nil, err
	}
	// Keep-28 also ends at custody end, an absolute instant fixed when sampled.
	var custody keep28Custody
	var custodyEnd time.Time
	if a.opts.Lane == inverseNASKeep28 {
		if custody, custodyEnd, err = a.proveKeep28(db, tx, e, target); err != nil {
			return nil, err
		}
	}
	deadline := time.Now().Add(inverseNetworkTimeout)
	if a.opts.Lane == inverseNASKeep28 && custodyEnd.Before(deadline) {
		deadline = custodyEnd
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	fail := func(err error) (*inverseDeleteAuthority, error) { cancel(); return nil, err }
	if err = a.checkWireHead(ctx, tx, e, target); err != nil {
		return fail(err)
	}
	guard := &inverseWireGuard{a, tx, target, e}
	binding := guard.binding()
	if err = guard.check(ctx, binding); err != nil {
		return fail(err)
	}
	// Independent commit precedes capability issuance. Even another verifier or
	// process cannot remint authority after any ambiguous/crashed wire attempt.
	tag, err := a.pool.Exec(ctx, `INSERT INTO `+a.opts.Lane.attemptTable()+`(bucket,object_key) VALUES($1,$2) ON CONFLICT DO NOTHING`, binding.Bucket, binding.Key)
	if err != nil {
		return fail(err)
	}
	if tag.RowsAffected() != 1 {
		return fail(errors.New("inverse wire authority was already issued; reconcile only"))
	}
	return &inverseDeleteAuthority{binding: binding, guard: guard, used: &atomic.Bool{}, gate: gate, ctx: ctx, cancel: cancel, custody: custody, custodyEnd: custodyEnd}, nil
}

type inverseSDKStore struct {
	*r2.Client
	sdk       *s3.Client
	transport http.RoundTripper
	// readClient carries the job's LIST pages, multipart inventory and identity
	// HEADs through the bounded read path; confirm carries the confirming HEAD's
	// fresh per-attempt connections. Both refuse redirects and send one request
	// per attempt.
	readClient *http.Client
	confirm    *http.Client
	// reads is the one rate limiter every worker and every read type shares.
	reads   *inverseReadGate
	readsMu sync.Mutex
}

// The confirming HEAD after an answered DELETE is a read, so it alone retries:
// at most 4 attempts of at most 4s each, with full-jitter backoff of at most
// 2s between them, under its own 22s deadline inside the caller's network and
// custody bounds. Only timeouts, connection errors, 429, every 5xx and
// throttling retry, and a provider's retry-after never stretches the backoff.
// Each attempt is one request on its own fresh connection, so neither the
// transport nor the SDK can turn one attempt into two. Only a 404 for the exact
// key confirms: a redirect is refused, and a 200 fails at once.
const (
	inverseConfirmAttempts       = 4
	inverseConfirmAttemptTimeout = 4 * time.Second
	inverseConfirmMaxBackoff     = 2 * time.Second
	inverseConfirmTimeout        = inverseConfirmAttempts*inverseConfirmAttemptTimeout + (inverseConfirmAttempts-1)*inverseConfirmMaxBackoff
)

// inverseConfirmRetryable is the SDK's default retry classification widened to
// every 5xx, plus a bare 429, which carries no throttling error code on a HEAD.
// A Cloudflare-fronted provider answers a transient failure as 520-599, and the
// SDK's default set covers only 500/502/503/504.
var inverseConfirmRetryable = retry.IsErrorRetryables(append(
	slices.Clone(retry.DefaultRetryables),
	retry.RetryableHTTPStatusCode{Codes: inverseTransientHTTPStatus()},
))

// inverseTransientHTTPStatus is every server-side status, 500 through 599, plus
// 429: a confirming HEAD may retry those, and nothing else.
func inverseTransientHTTPStatus() map[int]struct{} {
	codes := map[int]struct{}{http.StatusTooManyRequests: {}}
	for status := 500; status <= 599; status++ {
		codes[status] = struct{}{}
	}
	return codes
}

// The confirming HEAD sends exactly one request per attempt, so its client
// carries the attempt's own deadline on a fresh HTTP/1.1 connection: no
// keep-alive means no pooled connection can be reused, and a reused connection
// closing under a request is the only case Go's HTTP transport replays.
func newInverseConfirmClient(attempt time.Duration) *http.Client {
	transport := awshttp.NewBuildableClient().WithTransportOptions(func(t *http.Transport) {
		t.ForceAttemptHTTP2 = false
		t.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
		t.Protocols = new(http.Protocols)
		t.Protocols.SetHTTP1(true)
		t.TLSClientConfig = t.TLSClientConfig.Clone()
		t.TLSClientConfig.NextProtos = []string{"http/1.1"}
		t.DisableKeepAlives = true
	}).GetTransport()
	return &http.Client{Transport: transport, Timeout: attempt, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

// newInverseDeleteHTTPTransport carries every DELETE over its own fresh HTTP/1.1
// connection. HTTP/2 is neither offered nor negotiated: Go's HTTP/2 client
// resends a refused stream inside one RoundTrip, below the one-DELETE guard. A
// fresh connection also never meets a provider closing an idle one under it.
func newInverseDeleteHTTPTransport() *http.Transport {
	return awshttp.NewBuildableClient().WithTransportOptions(func(t *http.Transport) {
		t.ForceAttemptHTTP2 = false
		t.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
		t.Protocols = new(http.Protocols)
		t.Protocols.SetHTTP1(true)
		t.TLSClientConfig = t.TLSClientConfig.Clone()
		t.TLSClientConfig.NextProtos = []string{"http/1.1"}
		t.DisableKeepAlives = true
	}).GetTransport()
}

func newInverseSDKStore(ctx context.Context, cfg r2.Config) (*inverseSDKStore, error) {
	reader, err := r2.New(ctx, cfg)
	if err != nil {
		return nil, err
	}
	region := cfg.Region
	if region == "" {
		region = "auto"
	}
	transport := newInverseDeleteHTTPTransport()
	sdk := s3.NewFromConfig(aws.Config{Region: region, Credentials: credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")}, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		o.Retryer = aws.NopRetryer{}
	})
	return &inverseSDKStore{Client: reader, sdk: sdk, transport: transport, readClient: newInverseReadClient(inverseReadPageTimeout), confirm: newInverseConfirmClient(inverseConfirmAttemptTimeout), reads: newInverseReadGate()}, nil
}

// confirmAbsent HEADs the key after its answered DELETE: nil only on 404. It is
// a read, so it shares the job's bounded read policy and its rate limiter; it
// keeps its reviewed 22s bound, 4 attempts of 4s and fresh connection per
// attempt.
func (s *inverseSDKStore) confirmAbsent(ctx context.Context, key string) error {
	return s.read(ctx, inverseConfirmReadPolicy, "confirming head "+key, func(ctx context.Context) error {
		_, err := s.sdk.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.Bucket()), Key: aws.String(key)}, func(o *s3.Options) {
			o.HTTPClient = s.confirm
			o.Retryer = aws.NopRetryer{}
		})
		return inverseAbsence(err)
	})
}
func (s *inverseSDKStore) deleteOnce(ctx context.Context, authority *inverseDeleteAuthority) error {
	if err := authority.requireWire(ctx, s.Bucket(), s.EndpointSHA256()); err != nil {
		return err
	}
	template, ok := s.transport.(*http.Transport)
	if !ok {
		return errors.New("inverse DELETE needs the HTTP/1.1 transport template")
	}
	wire := &inverseWire{gate: authority.gate}
	transport := &inverseDeleteTransport{base: wire.transport(template), authority: authority, bucket: s.Bucket(), endpoint: s.EndpointSHA256(), wire: wire}
	_, err := s.sdk.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.Bucket()), Key: aws.String(authority.binding.Key)}, func(o *s3.Options) {
		o.Retryer = aws.NopRetryer{}
		o.HTTPClient = &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	})
	if err != nil && wire.lostAfterClose() {
		return fmt.Errorf("%w: %w", errInverseWireAmbiguous, err)
	}
	return err
}

// errInverseWireAmbiguous marks a DELETE whose request its own connection
// accepted in full, with no write error, and which then received not one
// response byte: the connection dropped while awaiting the answer, or the
// answer timed out. The request may or may not have reached the provider, so
// only that key is in doubt: it stays held and is never retransmitted. A veto,
// a failed dial, handshake or write, and any response byte at all (403, 503, a
// cut status line) are never this error and stop the job.
var errInverseWireAmbiguous = errors.New("DELETE sent but no response; result unknown")

// errInverseWireDeadline refuses a first request byte at or after the DELETE's
// deadline. Nothing was written; it is never a lost response.
var errInverseWireDeadline = errors.New("DELETE deadline passed before its first request byte")

// inverseWire records what one DELETE's own connection did. deadline, when set,
// is the instant from which no first request byte may leave; for keep-28 it is
// no later than custody expiry. answered is set by the connection read itself;
// reading is held across every read so classification can wait one out.
type inverseWire struct {
	gate                         *inverseSendGate
	deadline                     time.Time
	wrote, writeFailed, answered atomic.Bool
	mu                           sync.Mutex
	conn                         net.Conn
	reading                      sync.RWMutex
}

func (w *inverseWire) lost() bool {
	return w.wrote.Load() && !w.writeFailed.Load() && !w.answered.Load()
}

// lostAfterClose classifies a failed DELETE only after its connection is closed
// and any read under way has returned: a response byte the socket delivered
// always counts, even when cancellation ended the round trip first.
func (w *inverseWire) lostAfterClose() bool {
	w.mu.Lock()
	conn := w.conn
	w.mu.Unlock()
	if conn != nil {
		conn.Close()
	}
	w.reading.Lock()
	defer w.reading.Unlock()
	return w.lost()
}

// track records the DELETE's connection and routes its bytes through w.
func (w *inverseWire) track(conn net.Conn) net.Conn {
	w.mu.Lock()
	w.conn = conn
	w.mu.Unlock()
	return &inverseWireConn{Conn: conn, wire: w}
}

// beforeFirst runs under the send gate's read lock. It refuses the first
// request byte at or after the deadline and makes the socket refuse it too,
// should the writer stall past that instant.
func (w *inverseWire) beforeFirst(conn net.Conn) error {
	if w.deadline.IsZero() {
		return nil
	}
	if !time.Now().Before(w.deadline) {
		return errInverseWireDeadline
	}
	return conn.SetWriteDeadline(w.deadline)
}

// transport clones the template so this DELETE dials its own connection and
// every byte the HTTP layer writes passes through w. TLS is set up here so the
// recorder sees request bytes only, never handshake or close alerts.
func (w *inverseWire) transport(template *http.Transport) *http.Transport {
	t := template.Clone()
	t.Proxy = nil
	dial := t.DialContext
	t.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dial(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return w.track(conn), nil
	}
	config, handshake := t.TLSClientConfig, t.TLSHandshakeTimeout
	t.DialTLSContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		raw, err := dial(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		c := config.Clone()
		if c.ServerName == "" {
			if c.ServerName, _, err = net.SplitHostPort(addr); err != nil {
				raw.Close()
				return nil, err
			}
		}
		conn := tls.Client(raw, c)
		bounded, cancel := context.WithTimeout(ctx, handshake)
		defer cancel()
		if err = conn.HandshakeContext(bounded); err != nil {
			raw.Close()
			return nil, err
		}
		return w.track(conn), nil
	}
	return t
}

type inverseWireConn struct {
	net.Conn
	wire *inverseWire
}

func (c *inverseWireConn) Write(p []byte) (int, error) {
	// An empty write carries no request byte: it neither passes the gate nor
	// counts as a request the connection accepted.
	if len(p) == 0 {
		return c.Conn.Write(p)
	}
	write := func() (int, error) { return c.Conn.Write(p) }
	var n int
	var err error
	if c.wire.wrote.Load() || c.wire.writeFailed.Load() {
		n, err = write()
	} else {
		n, err = c.wire.gate.first(func() (int, error) {
			if err := c.wire.beforeFirst(c.Conn); err != nil {
				return 0, err
			}
			return write()
		})
	}
	if err != nil || n < len(p) {
		c.wire.writeFailed.Store(true)
	} else {
		c.wire.wrote.Store(true)
	}
	return n, err
}

// Read marks the DELETE answered on its first response byte, before the HTTP
// layer sees it.
func (c *inverseWireConn) Read(p []byte) (int, error) {
	c.wire.reading.RLock()
	defer c.wire.reading.RUnlock()
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.wire.answered.Store(true)
	}
	return n, err
}

// inverseWireAuthority is what one DELETE's transport may check: the live
// binding check and the send gate. Both the RAW/keep-28 authority and the
// collated authority implement it, so every lane sends over the ONE audited
// transport rather than a weaker copy.
type inverseWireAuthority interface {
	check(context.Context, string, string) error
	sendGate() *inverseSendGate
	wireKey() string
	bound(context.Context) (context.Context, context.CancelFunc, error)
}

func (a *inverseDeleteAuthority) sendGate() *inverseSendGate { return a.gate }
func (a *inverseDeleteAuthority) wireKey() string            { return a.binding.Key }

// inverseNonrawAuthority denies the deadline boundary outright: the collated
// lane's parent context already carries the key's network deadline, and a
// nonraw key has no NAS custody to end at.
func (a *inverseNonrawAuthority) sendGate() *inverseSendGate { return a.gate }
func (a *inverseNonrawAuthority) wireKey() string            { return a.binding.Key }
func (a *inverseNonrawAuthority) bound(ctx context.Context) (context.Context, context.CancelFunc, error) {
	return ctx, func() {}, nil
}

type inverseDeleteTransport struct {
	base             http.RoundTripper
	authority        inverseWireAuthority
	bucket, endpoint string
	attempted        atomic.Bool
	wire             *inverseWire
}

func (t *inverseDeleteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodDelete || req.URL.Path != "/"+t.bucket+"/"+t.authority.wireKey() || req.URL.RawQuery != "x-id=DeleteObject" || !t.attempted.CompareAndSwap(false, true) {
		return nil, errors.New("inverse transport permits one exact DELETE")
	}
	if err := t.authority.check(req.Context(), t.bucket, t.endpoint); err != nil {
		return nil, err
	}
	bounded, cancel, err := t.authority.bound(req.Context())
	if err != nil {
		return nil, err
	}
	// Set before the round trip dials: the first write happens after it.
	t.wire.deadline, _ = bounded.Deadline()
	req = req.Clone(bounded)
	req.Header.Del("Idempotency-Key")
	req.Header.Del("X-Idempotency-Key")
	response, err := t.base.RoundTrip(req)
	if err != nil {
		cancel()
		return nil, err
	}
	response.Body = &inverseResponseBody{ReadCloser: response.Body, cancel: cancel}
	return response, nil
}

// inverseResponseBody holds the bounded request context open until the SDK has
// read the answer.
type inverseResponseBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *inverseResponseBody) Close() error { defer b.cancel(); return b.ReadCloser.Close() }
