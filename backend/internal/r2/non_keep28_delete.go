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

// NonKeep28DeleteGuard is the separate raw, outside-keep28 lane. Implementations
// must bind the exact key, immutable listing, committed intent, policy, holds,
// mature writer fence and controller drain evidence. It is checked at the wire;
// a stitch proof cannot be substituted for these facts.
type NonKeep28DeleteGuard interface {
	CheckNonKeep28Delete(context.Context, string, string, string) error
}

func (c *Client) DeleteNonKeep28Once(ctx context.Context, key string, guard NonKeep28DeleteGuard) error {
	if guard == nil || key == "" || strings.TrimSpace(key) != key {
		return errors.New("non-keep28 DELETE requires exact key and live authority")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := guard.CheckNonKeep28Delete(ctx, c.bucket, c.EndpointSHA256(), key); err != nil {
		return err
	}
	transport := &nonKeep28Transport{base: c.deleteTransport, guard: guard, bucket: c.bucket, endpoint: c.EndpointSHA256(), key: key}
	_, err := c.s3.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(c.bucket), Key: aws.String(key)}, func(o *s3.Options) {
		o.Retryer = aws.NopRetryer{}
		o.HTTPClient = &http.Client{Transport: transport, CheckRedirect: refuseRetirementRedirect}
	})
	return err
}

type nonKeep28Transport struct {
	base                  http.RoundTripper
	guard                 NonKeep28DeleteGuard
	bucket, endpoint, key string
	attempted             atomic.Bool
}

func (t *nonKeep28Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodDelete || req.URL.Path != "/"+t.bucket+"/"+t.key || req.URL.RawQuery != "x-id=DeleteObject" || !t.attempted.CompareAndSwap(false, true) {
		return nil, errors.New("non-keep28 transport permits one exact DELETE")
	}
	if err := t.guard.CheckNonKeep28Delete(req.Context(), t.bucket, t.endpoint, t.key); err != nil {
		return nil, err
	}
	// DELETE is not retried by net/http without an idempotency header. Explicitly
	// remove SDK invocation-id headers if they could opt into transport replay.
	req.Header.Del("Idempotency-Key")
	req.Header.Del("X-Idempotency-Key")
	return t.base.RoundTrip(req)
}
