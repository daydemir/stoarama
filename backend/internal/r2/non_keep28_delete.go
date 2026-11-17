package r2

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// NonKeep28DeleteBinding names one committed raw, outside-keep28 intent.
type NonKeep28DeleteBinding struct {
	Bucket, EndpointSHA256, Key, ETag                      string
	ManifestSHA256, PolicySHA256, PicksSHA256, DrainSHA256 string
	SourceID, StreamID, SizeBytes                          int64
	LastModified                                           time.Time
}

// NonKeep28DeleteGuard checks the binding against live database protections and
// pinned evidence. It issues authority only after a successful check and is
// checked again immediately before the wire.
type NonKeep28DeleteGuard interface {
	CheckNonKeep28Delete(context.Context, NonKeep28DeleteBinding) error
}

// NonKeep28DeleteAuthority is opaque and single-use, including after an
// ambiguous response. Copies share consumption state; a zero value grants none.
type NonKeep28DeleteAuthority struct {
	binding NonKeep28DeleteBinding
	guard   NonKeep28DeleteGuard
	used    *atomic.Bool
}

func AuthorizeNonKeep28Delete(ctx context.Context, b NonKeep28DeleteBinding, guard NonKeep28DeleteGuard) (*NonKeep28DeleteAuthority, error) {
	if guard == nil || b.Bucket == "" || !strings.HasPrefix(b.Key, "managed/") || strings.TrimSpace(b.Key) != b.Key || b.ETag == "" || b.SourceID <= 0 || b.StreamID <= 0 || b.SizeBytes <= 0 || b.LastModified.IsZero() {
		return nil, errors.New("non-keep28 authority requires one exact raw intent")
	}
	for _, digest := range []string{b.EndpointSHA256, b.ManifestSHA256, b.PolicySHA256, b.PicksSHA256, b.DrainSHA256} {
		if len(digest) != 64 || strings.Trim(digest, "0123456789abcdef") != "" {
			return nil, errors.New("invalid non-keep28 evidence binding")
		}
	}
	if err := guard.CheckNonKeep28Delete(ctx, b); err != nil {
		return nil, err
	}
	return &NonKeep28DeleteAuthority{binding: b, guard: guard, used: &atomic.Bool{}}, nil
}

func (a *NonKeep28DeleteAuthority) Binding() NonKeep28DeleteBinding { return a.binding }

// RequireWire consumes authority before transport setup. Failure burns it too;
// callers must reconcile the durable intent rather than mint a retry.
func (a *NonKeep28DeleteAuthority) RequireWire(ctx context.Context, bucket, endpoint string) error {
	if a == nil || a.used == nil || !a.used.CompareAndSwap(false, true) {
		return errors.New("non-keep28 authority absent or already consumed")
	}
	return a.check(ctx, bucket, endpoint)
}
func (a *NonKeep28DeleteAuthority) check(ctx context.Context, bucket, endpoint string) error {
	if a.guard == nil || bucket != a.binding.Bucket || endpoint != a.binding.EndpointSHA256 {
		return errors.New("non-keep28 wire binding differs")
	}
	return a.guard.CheckNonKeep28Delete(ctx, a.binding)
}

func (c *Client) DeleteNonKeep28Once(ctx context.Context, authority *NonKeep28DeleteAuthority) error {
	if err := authority.RequireWire(ctx, c.bucket, c.EndpointSHA256()); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	key := authority.binding.Key
	transport := &nonKeep28Transport{base: c.deleteTransport, authority: authority, bucket: c.bucket, endpoint: c.EndpointSHA256(), key: key}
	_, err := c.s3.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(c.bucket), Key: aws.String(key)}, func(o *s3.Options) {
		o.Retryer = aws.NopRetryer{}
		o.HTTPClient = &http.Client{Transport: transport, CheckRedirect: refuseRetirementRedirect}
	})
	return err
}

type nonKeep28Transport struct {
	base                  http.RoundTripper
	authority             *NonKeep28DeleteAuthority
	bucket, endpoint, key string
	attempted             atomic.Bool
}

func (t *nonKeep28Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodDelete || req.URL.Path != "/"+t.bucket+"/"+t.key || req.URL.RawQuery != "x-id=DeleteObject" || !t.attempted.CompareAndSwap(false, true) {
		return nil, errors.New("non-keep28 transport permits one exact DELETE")
	}
	if err := t.authority.check(req.Context(), t.bucket, t.endpoint); err != nil {
		return nil, err
	}
	// DELETE is not retried by net/http without an idempotency header. Explicitly
	// remove SDK invocation-id headers if they could opt into transport replay.
	req.Header.Del("Idempotency-Key")
	req.Header.Del("X-Idempotency-Key")
	return t.base.RoundTrip(req)
}
