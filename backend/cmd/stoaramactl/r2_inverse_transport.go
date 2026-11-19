package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
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

// Only the concrete executor verifier below can issue this private capability.
// Copies share consumption state, and the DB independently forbids reminting.
type inverseDeleteAuthority struct {
	binding inverseDeleteBinding
	guard   *inverseWireGuard
	used    *atomic.Bool
	gate    *inverseSendGate
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
	if a.guard == nil || a.guard.a.opts.Lane != inverseOutsideKeep28 || bucket != a.binding.Bucket || endpoint != a.binding.EndpointSHA256 {
		return errors.New("inverse wire binding differs")
	}
	if err := a.guard.check(ctx, a.binding); err != nil {
		return err
	}
	var issued bool
	err := a.guard.tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM r2_inverse_attempts WHERE bucket=$1 AND object_key=$2)`, bucket, a.binding.Key).Scan(&issued)
	if err != nil {
		return err
	}
	if !issued {
		return errors.New("durable wire issuance missing")
	}
	return nil
}

// protectionEnvelope is the ONE wire protection envelope shared by the RAW and
// collated paths: the metadata barrier, ordered shared protection locks, the
// exact COPY key latch, a locked live reload and a fresh KE-epoch plus controller
// evidence check. load is the only per-lane part (typed RAW or typed collated);
// everything that guards the DELETE is common, so the collated path cannot drift
// into a weaker copy. The reservation commits before wire, so this envelope is
// the only thing standing between a stale protection change and the DELETE.
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
func (a *inverseApplier) authorize(ctx context.Context, tx pgx.Tx, e inverseEntry, gate *inverseSendGate) (*inverseDeleteAuthority, error) {
	if a.opts.Lane != inverseOutsideKeep28 {
		return nil, errors.New("B1 authority cannot authorize B2")
	}
	target, err := a.prepareWireTarget(ctx, tx, e)
	if err != nil {
		return nil, err
	}
	if err = a.checkWireHead(ctx, tx, e, target); err != nil {
		return nil, err
	}
	guard := &inverseWireGuard{a, tx, target, e}
	binding := guard.binding()
	if err = guard.check(ctx, binding); err != nil {
		return nil, err
	}
	// Independent commit precedes capability issuance. Even another verifier or
	// process cannot remint authority after any ambiguous/crashed wire attempt.
	tag, err := a.pool.Exec(ctx, `INSERT INTO r2_inverse_attempts(bucket,object_key) VALUES($1,$2) ON CONFLICT DO NOTHING`, binding.Bucket, binding.Key)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() != 1 {
		return nil, errors.New("inverse wire authority was already issued; reconcile only")
	}
	return &inverseDeleteAuthority{binding: binding, guard: guard, used: &atomic.Bool{}, gate: gate}, nil
}

type inverseSDKStore struct {
	*r2.Client
	sdk       *s3.Client
	transport http.RoundTripper
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
	return &inverseSDKStore{reader, sdk, transport}, nil
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
	if err != nil && wire.lost() {
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

// inverseWire records what one DELETE's own connection did.
type inverseWire struct {
	gate                         *inverseSendGate
	wrote, writeFailed, answered atomic.Bool
}

func (w *inverseWire) lost() bool {
	return w.wrote.Load() && !w.writeFailed.Load() && !w.answered.Load()
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
		return &inverseWireConn{Conn: conn, wire: w}, nil
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
		return &inverseWireConn{Conn: conn, wire: w}, nil
	}
	return t
}

type inverseWireConn struct {
	net.Conn
	wire *inverseWire
}

func (c *inverseWireConn) Write(p []byte) (int, error) {
	write := func() (int, error) { return c.Conn.Write(p) }
	var n int
	var err error
	if c.wire.wrote.Load() || c.wire.writeFailed.Load() {
		n, err = write()
	} else {
		n, err = c.wire.gate.first(write)
	}
	if err != nil || n < len(p) {
		c.wire.writeFailed.Store(true)
	} else {
		c.wire.wrote.Store(true)
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
	req.Header.Del("Idempotency-Key")
	req.Header.Del("X-Idempotency-Key")
	trace := &httptrace.ClientTrace{GotFirstResponseByte: func() { t.wire.answered.Store(true) }}
	response, err := t.base.RoundTrip(req.WithContext(httptrace.WithClientTrace(req.Context(), trace)))
	if response != nil {
		t.wire.answered.Store(true)
	}
	return response, err
}
