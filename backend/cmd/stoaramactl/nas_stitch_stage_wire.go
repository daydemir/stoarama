package main

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

// durhamStageWire is the audited single-attempt transport for one Durham stage
// object. It shares the RAW lanes' HTTP/1.1 one-DELETE transport, their
// first-byte deadline guard and their strict confirming-HEAD ladder, and adds
// only the per-stage binding those lanes do not need. It never writes a RAW or
// keep-28 key and never retries a DELETE.
type durhamStageWire struct {
	store *inverseSDKStore
}

// EndpointSHA256 is this wire's own credential-backed provider endpoint. The
// retirement compares it with its own client before any HEAD or DELETE, and
// StitchStageTarget carries the same value, so a bucket name alone can never
// pair a read at one endpoint with a delete at another.
func (w *durhamStageWire) EndpointSHA256() string { return w.store.EndpointSHA256() }

// durhamStageAuthority is one stage object's single-use wire authority. The
// retirement prepares it after the exact HEAD; before the first request byte it
// consumes its one shot and re-runs the retirement's own revalidation, which
// still holds the retirement transaction's locks.
type durhamStageAuthority struct {
	store   *inverseSDKStore
	target  r2.StitchStageTarget
	recheck func(context.Context) error
	used    *atomic.Bool
	gate    *inverseSendGate
}

func (a *durhamStageAuthority) wireKey() string { return a.target.Key }

func (a *durhamStageAuthority) requireWire(ctx context.Context, bucket, endpoint string) error {
	if a == nil || a.used == nil || !a.used.CompareAndSwap(false, true) {
		return errors.New("Durham stage wire authority absent or consumed")
	}
	return a.check(ctx, bucket, endpoint)
}

// check revalidates the exact stage generation and its still-live drain and
// fence. It runs at the transport, after the one-shot intent committed and
// before the first request byte, so a changed row stops the wire unsent.
func (a *durhamStageAuthority) check(ctx context.Context, bucket, endpoint string) error {
	// The transport's own bucket and endpoint are the ones this credential will
	// actually sign for. Both must equal the committed stage row, so a config
	// endpoint that differs from the certified one stops the DELETE rather than
	// sending it to a different provider.
	if bucket != a.target.Bucket || endpoint != a.target.EndpointSHA256 {
		return errors.New("Durham stage wire binding differs")
	}
	return a.recheck(ctx)
}

// bound hands the wire its deadline. The stage path has no NAS custody, so the
// caller's own bounded context is the whole bound.
func (a *durhamStageAuthority) bound(ctx context.Context) (context.Context, context.CancelFunc, error) {
	return ctx, func() {}, nil
}

func (a *durhamStageAuthority) sendGate() *inverseSendGate { return a.gate }

func cleanStitchWireETag(v string) string {
	for len(v) >= 2 && v[0] == '"' && v[len(v)-1] == '"' {
		v = v[1 : len(v)-1]
	}
	return v
}

// ExactHead sends one direct, non-retrying, no-redirect HEAD of the exact
// generation under the wire deadline: the same read policy as the RAW lanes'
// confirming HEAD, one request on its own fresh connection.
func (w *durhamStageWire) ExactHead(ctx context.Context, t r2.StitchStageTarget, deadline time.Time) (r2.ObjectHead, error) {
	if err := w.boundTarget(t); err != nil {
		return r2.ObjectHead{}, err
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	out, err := w.store.sdk.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(t.Bucket), Key: aws.String(t.Key),
	}, func(o *s3.Options) {
		o.HTTPClient = newInverseConfirmClient(max(time.Until(deadline), time.Millisecond))
		o.Retryer = aws.NopRetryer{}
	})
	if err != nil {
		return r2.ObjectHead{}, err
	}
	return r2.ObjectHead{
		LastModified:   aws.ToTime(out.LastModified),
		ETag:           cleanStitchWireETag(aws.ToString(out.ETag)),
		SizeBytes:      aws.ToInt64(out.ContentLength),
		VersionID:      aws.ToString(out.VersionId),
		SHA256Metadata: out.Metadata["sha256"],
	}, nil
}

// DeleteOnce sends exactly one DELETE through the shared audited transport.
func (w *durhamStageWire) DeleteOnce(ctx context.Context, t r2.StitchStageTarget, deadline time.Time, recheck func(context.Context) error) error {
	// The transport trusts its authority's key, so the stage lane refuses any
	// key that is not exactly one stage generation before it can reach the wire.
	if err := w.boundTarget(t); err != nil {
		return err
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	authority := &durhamStageAuthority{
		store: w.store, target: t, recheck: recheck, used: &atomic.Bool{}, gate: &inverseSendGate{},
	}
	return w.store.deleteOneWire(ctx, authority)
}

// boundTarget refuses a target that is not this exact stage generation at this
// exact bucket and endpoint. The wire never reads or deletes across endpoints.
func (w *durhamStageWire) boundTarget(t r2.StitchStageTarget) error {
	// The transport trusts its authority's key, so the stage lane refuses any
	// key that is not exactly one stage generation before it can reach the wire.
	if !r2.ValidStitchStageKey(t.Key, t.SHA256) {
		return errors.New("Durham stage wire refused a key outside its stage namespace")
	}
	if t.Bucket != w.store.Bucket() || t.EndpointSHA256 != w.store.EndpointSHA256() {
		return errors.New("Durham stage wire refused a target at a different bucket or endpoint")
	}
	return nil
}

// ConfirmAbsent is deliberately absent: the stage package owns the stricter
// absence readback (exact bucket/prefix echo, empty multipart inventory and the
// stage-specific 404 header check), so this wire exposes no second, weaker
// confirming ladder that a caller could reach by mistake.
