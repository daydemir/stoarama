package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
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

func (a *inverseApplier) prepareWireTarget(ctx context.Context, tx pgx.Tx, e inverseEntry) (inverseRawTarget, error) {
	// The step's own bound replaces the role's statement_timeout, whose 57014
	// would end a lock wait publication also takes; only a cancel then stops it.
	if _, err := tx.Exec(ctx, `SET LOCAL statement_timeout = 0`); err != nil {
		return inverseRawTarget{}, err
	}
	if err := inverseProtectionBarrier(ctx, tx); err != nil {
		return inverseRawTarget{}, err
	}
	if err := inverseProtectionLocks(ctx, tx, true); err != nil {
		return inverseRawTarget{}, err
	}
	// The wire transaction holds the wide cleanup locks shared from here on, so
	// every later table or row wait fails at once; admission awaits it outside.
	if err := boundInverseLocks(ctx, tx); err != nil {
		return inverseRawTarget{}, err
	}
	if err := lockInverseCopyKey(ctx, tx, a.store.Bucket(), e.Key); err != nil {
		return inverseRawTarget{}, err
	}
	target, err := a.loadTarget(ctx, tx, e)
	if err != nil {
		return inverseRawTarget{}, err
	}
	if err = a.checkTarget(ctx, tx, target, e, true); err != nil {
		return inverseRawTarget{}, err
	}
	return target, nil
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
// admitCtx is a database-only bound: it covers preparing the wire transaction,
// the keep-28 custody proof, every pre-issuance check and the attempt insert,
// and ends with admission. wireCtx is the key's own network deadline and
// outlives admission, so the returned authority is never bound by, or
// cancelled with, the admission attempt. Keep-28 adds its NAS custody proof and
// bounds both by its expiry.
func (a *inverseApplier) authorize(admitCtx, wireCtx context.Context, tx pgx.Tx, e inverseEntry, gate *inverseSendGate) (*inverseDeleteAuthority, error) {
	// The key's own network budget starts here, before its locks are prepared.
	// Every pre-issuance step holds the wide cleanup locks, so preflight that
	// outlived the wire deadline could never issue: bound each one by the
	// earliest of admission, the parent's deadline and the network timeout.
	// Keep-28 tightens the same bound to custody end once it is proven.
	deadline := time.Now().Add(inverseNetworkTimeout)
	if parent, ok := wireCtx.Deadline(); ok && parent.Before(deadline) {
		deadline = parent
	}
	preIssue, stopPreIssue := context.WithDeadline(admitCtx, deadline)
	defer stopPreIssue()
	target, err := a.prepareWireTarget(preIssue, tx, e)
	if err != nil {
		return nil, err
	}
	// Keep-28 also ends at custody end, an absolute instant fixed when sampled.
	var custody keep28Custody
	var custodyEnd time.Time
	if a.opts.Lane == inverseNASKeep28 {
		if custody, custodyEnd, err = a.proveKeep28(preIssue, tx, e, target); err != nil {
			return nil, err
		}
		tightened, stop := context.WithDeadline(preIssue, custodyEnd)
		defer stop()
		preIssue = tightened
		if custodyEnd.Before(deadline) {
			deadline = custodyEnd
		}
	}
	// The returned authority carries wireCtx, which outlives admission and is
	// never cancelled with it; an earlier parent bound still wins.
	ctx, cancel := context.WithDeadline(wireCtx, deadline)
	fail := func(err error) (*inverseDeleteAuthority, error) { cancel(); return nil, err }
	// Every check before issuance and the attempt insert itself are admission
	// work under the pre-issuance bound: none of them can outlive the wire
	// deadline while the wire transaction holds the wide cleanup locks.
	if err = a.checkWireHead(preIssue, tx, e, target); err != nil {
		return fail(err)
	}
	guard := &inverseWireGuard{a, tx, target, e}
	binding := guard.binding()
	if err = guard.check(preIssue, binding); err != nil {
		return fail(err)
	}
	// Nothing is minted once the wire deadline has passed: authority that can
	// never send must not exist, so this fails before the attempt insert.
	if err = preIssue.Err(); err != nil {
		return fail(fmt.Errorf("pre-issuance ended before wire authority could be issued: %w", err))
	}
	// Independent commit precedes capability issuance. Even another verifier or
	// process cannot remint authority after any ambiguous/crashed wire attempt.
	issued, err := a.issueAttempt(preIssue, binding)
	if err != nil {
		return fail(err)
	}
	if issued != 1 {
		return fail(errors.New("inverse wire authority was already issued; reconcile only"))
	}
	return &inverseDeleteAuthority{binding: binding, guard: guard, used: &atomic.Bool{}, gate: gate, ctx: ctx, cancel: cancel, custody: custody, custodyEnd: custodyEnd}, nil
}

// issueAttempt records the wire attempt in its own transaction, so it commits
// independently of the wire transaction. That transaction also takes the lock
// bound: a busy attempt table is contention here, never a wait of the wire
// transaction that still holds the wide cleanup locks.
func (a *inverseApplier) issueAttempt(ctx context.Context, binding inverseDeleteBinding) (int64, error) {
	tx, err := a.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.Background())
	if err = boundInverseLocks(ctx, tx); err != nil {
		return 0, err
	}
	tag, err := tx.Exec(ctx, `INSERT INTO `+a.opts.Lane.attemptTable()+`(bucket,object_key) VALUES($1,$2) ON CONFLICT DO NOTHING`, binding.Bucket, binding.Key)
	if err != nil {
		return 0, err
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

type inverseSDKStore struct {
	*r2.Client
	sdk       *s3.Client
	transport http.RoundTripper
	// confirm sends each confirming HEAD attempt under its own timeout and
	// never follows a redirect.
	confirm *http.Client
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
	return &inverseSDKStore{reader, sdk, transport, newInverseConfirmClient(inverseConfirmAttemptTimeout)}, nil
}

func (s *inverseSDKStore) confirmAbsent(ctx context.Context, key string) error {
	ctx, cancel := context.WithTimeout(ctx, inverseConfirmTimeout)
	defer cancel()
	for attempt := 1; ; attempt++ {
		_, err := s.sdk.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.Bucket()), Key: aws.String(key)}, func(o *s3.Options) {
			o.HTTPClient = s.confirm
			o.Retryer = aws.NopRetryer{}
		})
		err = inverseAbsence(err)
		if err == nil {
			return nil
		}
		if attempt == inverseConfirmAttempts || inverseConfirmRetryable.IsErrorRetryable(err) != aws.TrueTernary {
			return fmt.Errorf("confirming head %s after %d attempts: %w", key, attempt, err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("confirming head %s after %d attempts: %w", key, attempt, err)
		case <-time.After(rand.N(inverseConfirmMaxBackoff)):
		}
	}
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

type inverseDeleteTransport struct {
	base             http.RoundTripper
	authority        *inverseDeleteAuthority
	bucket, endpoint string
	attempted        atomic.Bool
	wire             *inverseWire
}

func (t *inverseDeleteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodDelete || req.URL.Path != "/"+t.bucket+"/"+t.authority.binding.Key || req.URL.RawQuery != "x-id=DeleteObject" || !t.attempted.CompareAndSwap(false, true) {
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
