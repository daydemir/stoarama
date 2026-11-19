package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
)

// Closed table selection: no operator-supplied identifier enters SQL. B1 stays
// in its existing namespace; ordinary reservations and byte charges are shared.
func (a *inverseApplier) intentTable() string {
	if a.opts.Lane == inverseNASKeep28 {
		return "r2_keep28_intents"
	}
	return "r2_inverse_intents"
}
func (a *inverseApplier) eventTable() string {
	if a.opts.Lane == inverseNASKeep28 {
		return "r2_keep28_events"
	}
	return "r2_inverse_events"
}
func (a *inverseApplier) attemptTable() string {
	if a.opts.Lane == inverseNASKeep28 {
		return "r2_keep28_attempts"
	}
	return "r2_inverse_attempts"
}
func (a *inverseApplier) retirementReason() string {
	if a.opts.Lane == inverseNASKeep28 {
		return "keep28_exact_nas"
	}
	return "inverse_non_keep28"
}

type keep28Custody struct {
	ConnectionID, SourceID, SizeBytes         int64
	Path, SHA, State, SidecarPath, SidecarSHA string
	SidecarSize                               int64
	VerifiedAt, ReceivedAt, Expiry            time.Time
}

func (a *inverseApplier) lockKeep28Custody(ctx context.Context, tx pgx.Tx, t inverseRawTarget) (keep28Custody, error) {
	c := keep28Custody{ConnectionID: 13, SourceID: t.ID, SizeBytes: t.Size, Path: t.Path, SHA: t.SHA}
	item := r2RetireItem{r2RetireCandidate: r2RetireCandidate{Kind: "raw", SourceID: t.ID, SizeBytes: t.Size, SHA256: t.SHA, RelativePath: t.Path}}
	expiry, err := r2RetireLockNASIdentity(ctx, tx, item, c.ConnectionID)
	if err != nil {
		return c, err
	}
	// The row is already FOR SHARE locked; capture the entire proof in a locked
	// read too. A changed sidecar/timestamp cannot hide behind an unchanged expiry.
	err = tx.QueryRow(ctx, `SELECT state,verified_at,server_received_at,sidecar_relative_path,sidecar_size_bytes,sidecar_sha256 FROM nas_inventory_files WHERE connection_id=$1 AND clip_id=$2 AND relative_path=$3 AND size_bytes=$4 AND sha256=$5 FOR SHARE`, c.ConnectionID, c.SourceID, c.Path, c.SizeBytes, c.SHA).Scan(&c.State, &c.VerifiedAt, &c.ReceivedAt, &c.SidecarPath, &c.SidecarSize, &c.SidecarSHA)
	c.VerifiedAt = c.VerifiedAt.UTC()
	c.ReceivedAt = c.ReceivedAt.UTC()
	c.Expiry = expiry.UTC()
	return c, err
}

// Selection trails fresh custody in verified_at order. A report is never wire
// authority; every identity and proof is subsequently reloaded under locks.
func (a *inverseApplier) selectKeep28(ctx context.Context, entries map[string]inverseEntry) ([]inverseEntry, error) {
	rows, err := a.pool.Query(ctx, `SELECT c.object_key`+r2RetireRawExactSQL+`
 FROM recording_clips c JOIN nas_inventory_files n ON n.connection_id=$2 AND n.clip_id=c.id
 JOIN recordings r ON r.id=c.recording_id
 WHERE c.bucket=$1 AND c.purged_at IS NULL AND r.account_id=47
 AND n.state='present' AND n.verified_at>=clock_timestamp()-interval '24 hours'
 ORDER BY n.verified_at,c.id`, a.store.Bucket(), int64(13))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := []inverseEntry{}
	var bytes int64
	for rows.Next() {
		var key, path string
		var exact bool
		if err = rows.Scan(&key, &exact, &path); err != nil {
			return nil, err
		}
		e, ok := entries[key]
		if !ok || !exact || e.SizeBytes <= 0 {
			continue
		}
		if e.SizeBytes > a.opts.MaxBytes-bytes {
			continue
		}
		jobs = append(jobs, e)
		bytes += e.SizeBytes
		if len(jobs) >= a.opts.MaxItems {
			break
		}
	}
	return jobs, rows.Err()
}

type keep28DBClock struct{ utc, observed time.Time }

func keep28Clock(ctx context.Context, q r2RetireQuerier) (keep28DBClock, error) {
	c := keep28DBClock{observed: time.Now()}
	err := q.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&c.utc)
	return c, err
}
func (c keep28DBClock) remaining(expiry time.Time) time.Duration {
	return expiry.Sub(c.utc) - time.Since(c.observed)
}

type keep28DeleteStore interface {
	inverseReader
	deleteKeep28Once(context.Context, *keep28DeleteAuthority) error
}

// B2 cannot be passed to B1 transport. Full proof and exact intent are checked
// by this concrete verifier, with one permanent issuance across process loss.
type keep28DeleteAuthority struct {
	guard   *inverseWireGuard
	binding inverseDeleteBinding
	custody keep28Custody
	clock   keep28DBClock
	used    *atomic.Bool
	ctx     context.Context
	cancel  context.CancelFunc
}

func (a *inverseApplier) authorizeKeep28(ctx context.Context, tx pgx.Tx, e inverseEntry) (*keep28DeleteAuthority, error) {
	if a.opts.Lane != inverseNASKeep28 {
		return nil, errors.New("B2 authority requires keep28 lane")
	}
	target, err := a.prepareWireTarget(ctx, tx, e)
	if err != nil {
		return nil, err
	}
	custody, err := a.lockKeep28Custody(ctx, tx, target)
	if err != nil {
		return nil, err
	}
	var data []byte
	if err = tx.QueryRow(ctx, `SELECT custody FROM r2_keep28_intents WHERE bucket=$1 AND object_key=$2`, a.store.Bucket(), e.Key).Scan(&data); err != nil {
		return nil, err
	}
	var intended keep28Custody
	if err = json.Unmarshal(data, &intended); err != nil {
		return nil, err
	}
	if custody != intended {
		return nil, errors.New("custody proof changed after durable intent")
	}
	clock, err := keep28Clock(ctx, tx)
	if err != nil {
		return nil, err
	}
	if clock.remaining(custody.Expiry) <= 0 {
		return nil, errors.New("NAS custody expired before wire")
	}
	bounded, cancel := context.WithTimeout(ctx, min(inverseNetworkTimeout, clock.remaining(custody.Expiry)))
	fail := func(err error) (*keep28DeleteAuthority, error) { cancel(); return nil, err }
	if err = a.checkWireHead(bounded, tx, e, target); err != nil {
		return fail(err)
	}
	guard := &inverseWireGuard{a, tx, target, e}
	binding := guard.binding()
	if err = guard.check(bounded, binding); err != nil {
		return fail(err)
	}
	tag, err := a.pool.Exec(bounded, `INSERT INTO r2_keep28_attempts(bucket,object_key) VALUES($1,$2) ON CONFLICT DO NOTHING`, binding.Bucket, binding.Key)
	if err != nil {
		return fail(err)
	}
	if tag.RowsAffected() != 1 {
		return fail(errors.New("B2 wire authority already issued; reconcile only"))
	}
	return &keep28DeleteAuthority{guard: guard, binding: binding, custody: custody, clock: clock, used: &atomic.Bool{}, ctx: bounded, cancel: cancel}, nil
}
func (a *keep28DeleteAuthority) check(ctx context.Context, bucket, endpoint string) error {
	if a == nil || a.guard == nil || a.guard.a.opts.Lane != inverseNASKeep28 || bucket != a.binding.Bucket || endpoint != a.binding.EndpointSHA256 {
		return errors.New("B2 authority binding differs")
	}
	if a.clock.remaining(a.custody.Expiry) <= 0 {
		return errors.New("NAS custody expired during handoff")
	}
	if err := a.guard.check(ctx, a.binding); err != nil {
		return err
	}
	c, err := a.guard.a.lockKeep28Custody(ctx, a.guard.tx, a.guard.t)
	if err != nil {
		return err
	}
	if c != a.custody {
		return errors.New("locked custody proof changed")
	}
	var issued bool
	if err = a.guard.tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM r2_keep28_attempts WHERE bucket=$1 AND object_key=$2)`, bucket, a.binding.Key).Scan(&issued); err != nil {
		return err
	}
	if !issued {
		return errors.New("durable B2 issuance missing")
	}
	clock, err := keep28Clock(ctx, a.guard.tx)
	if err != nil {
		return err
	}
	if min(a.clock.remaining(a.custody.Expiry), clock.remaining(a.custody.Expiry)) <= 0 {
		return errors.New("NAS custody expired at transport")
	}
	return nil
}
func (s *inverseSDKStore) deleteKeep28Once(ctx context.Context, a *keep28DeleteAuthority) error {
	if a == nil || a.used == nil || !a.used.CompareAndSwap(false, true) {
		return errors.New("B2 authority absent or consumed")
	}
	if err := a.check(ctx, s.Bucket(), s.EndpointSHA256()); err != nil {
		return err
	}
	transport := &keep28DeleteTransport{base: s.transport, authority: a, bucket: s.Bucket(), endpoint: s.EndpointSHA256()}
	_, err := s.sdk.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.Bucket()), Key: aws.String(a.binding.Key)}, func(o *s3.Options) {
		o.Retryer = aws.NopRetryer{}
		o.HTTPClient = &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	})
	return err
}

type keep28DeleteTransport struct {
	base             http.RoundTripper
	authority        *keep28DeleteAuthority
	bucket, endpoint string
	attempted        atomic.Bool
}

func (t *keep28DeleteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodDelete || req.URL.Path != "/"+t.bucket+"/"+t.authority.binding.Key || req.URL.RawQuery != "x-id=DeleteObject" || !t.attempted.CompareAndSwap(false, true) {
		return nil, errors.New("B2 transport permits one exact DELETE")
	}
	if err := t.authority.check(req.Context(), t.bucket, t.endpoint); err != nil {
		return nil, err
	}
	bounded, cancel, err := t.authority.bound(req.Context())
	if err != nil {
		return nil, err
	}
	req = req.Clone(bounded)
	req.Header.Del("Idempotency-Key")
	req.Header.Del("X-Idempotency-Key")
	response, err := t.base.RoundTrip(req)
	if err != nil {
		cancel()
		return response, err
	}
	if response.Body == nil {
		cancel()
	} else {
		response.Body = &keep28ResponseBody{ReadCloser: response.Body, cancel: cancel}
	}
	return response, nil
}

// Sample before every network handoff. A newer DB clock can shorten, never
// extend, the lifetime measured from the original monotonic sample.
func (a *keep28DeleteAuthority) bound(ctx context.Context) (context.Context, context.CancelFunc, error) {
	clock, err := keep28Clock(ctx, a.guard.tx)
	if err != nil {
		return nil, nil, err
	}
	remaining := min(a.clock.remaining(a.custody.Expiry), clock.remaining(a.custody.Expiry))
	if remaining <= 0 {
		return nil, nil, errors.New("NAS custody expired at network boundary")
	}
	bounded, cancel := context.WithTimeout(ctx, min(inverseNetworkTimeout, remaining))
	return bounded, cancel, nil
}

type keep28ResponseBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *keep28ResponseBody) Close() error { defer b.cancel(); return b.ReadCloser.Close() }
