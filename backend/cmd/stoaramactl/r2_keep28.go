package main

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// Closed table selection: no operator-supplied identifier enters SQL. B1 stays
// in its existing namespace; ordinary reservations and byte charges are shared.
func (l inverseLane) intentTable() string {
	if l == inverseNASKeep28 {
		return "r2_keep28_intents"
	}
	return "r2_inverse_intents"
}
func (l inverseLane) eventTable() string {
	if l == inverseNASKeep28 {
		return "r2_keep28_events"
	}
	return "r2_inverse_events"
}
func (l inverseLane) attemptTable() string {
	if l == inverseNASKeep28 {
		return "r2_keep28_attempts"
	}
	return "r2_inverse_attempts"
}
func (l inverseLane) retirementReason() string {
	if l == inverseNASKeep28 {
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

// keep28AdmissionCustody is the custody a key needs left to be selected: one
// network timeout plus the margin, the same rule reserve enforces under lock.
// A shorter key would only be skipped, so it takes no item or byte allowance.
const keep28AdmissionCustody = keep28CustodyMargin + inverseNetworkTimeout

// Selection trails fresh custody in verified_at order. A report is never wire
// authority; every identity and proof is subsequently reloaded under locks.
func (a *inverseApplier) selectKeep28(ctx context.Context, entries map[string]inverseEntry) ([]inverseEntry, error) {
	rows, err := a.pool.Query(ctx, `SELECT c.object_key`+r2RetireRawExactSQL+`
 FROM recording_clips c JOIN nas_inventory_files n ON n.connection_id=$2 AND n.clip_id=c.id
 JOIN recordings r ON r.id=c.recording_id
 WHERE c.bucket=$1 AND c.purged_at IS NULL AND r.account_id=47
 AND n.state='present'
 AND LEAST(n.verified_at,n.server_received_at)>clock_timestamp()-interval '24 hours'+make_interval(secs=>$3)
 ORDER BY n.verified_at,c.id`, a.store.Bucket(), int64(13), keep28AdmissionCustody.Seconds())
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

// keep28CustodyMargin ends local authority this long before the sampled DB
// expiry. Custody lasts up to 24 hours, so 2s costs nothing; it absorbs a
// PostgreSQL clock step or host stall after the last sample, up to its size.
const keep28CustodyMargin = 2 * time.Second

// deadline is the local monotonic instant, keep28CustodyMargin before this
// sample says expiry arrives. It is fixed when sampled, so however late a timer
// is made from it, it never moves. observed precedes the DB read, so the
// instant errs early.
func (c keep28DBClock) deadline(expiry time.Time) time.Time {
	return c.observed.Add(expiry.Sub(c.utc) - keep28CustodyMargin)
}

// proveKeep28 relocks the NAS custody behind a keep-28 intent and requires it
// to equal the proof persisted with that intent. It returns the local instant
// custody ends, which must still be ahead.
func (a *inverseApplier) proveKeep28(ctx context.Context, tx pgx.Tx, e inverseEntry, target inverseRawTarget) (keep28Custody, time.Time, error) {
	custody, err := a.lockKeep28Custody(ctx, tx, target)
	if err != nil {
		return custody, time.Time{}, err
	}
	var data []byte
	if err = tx.QueryRow(ctx, `SELECT custody FROM r2_keep28_intents WHERE bucket=$1 AND object_key=$2`, a.store.Bucket(), e.Key).Scan(&data); err != nil {
		return custody, time.Time{}, err
	}
	var intended keep28Custody
	if err = json.Unmarshal(data, &intended); err != nil {
		return custody, time.Time{}, err
	}
	if custody != intended {
		return custody, time.Time{}, errors.New("custody proof changed after durable intent")
	}
	clock, err := keep28Clock(ctx, tx)
	if err != nil {
		return custody, time.Time{}, err
	}
	end := clock.deadline(custody.Expiry)
	if !time.Now().Before(end) {
		return custody, end, errors.New("NAS custody expired before wire")
	}
	return custody, end, nil
}

// checkCustody runs at the transport, under the network transaction: the locked
// custody row must still equal the intent's proof, with lifetime left.
func (a *inverseDeleteAuthority) checkCustody(ctx context.Context) error {
	c, err := a.guard.a.lockKeep28Custody(ctx, a.guard.tx, a.guard.t)
	if err != nil {
		return err
	}
	if c != a.custody {
		return errors.New("locked custody proof changed")
	}
	_, err = a.custodyDeadline(ctx)
	return err
}

// custodyDeadline samples the DB clock at every custody check and network
// handoff and keeps custodyEnd as the running minimum across every sample, so
// each later handoff (first byte, socket deadline, confirming HEAD) ends no
// later than the tightest one. A newer sample can bring it forward, never back.
func (a *inverseDeleteAuthority) custodyDeadline(ctx context.Context) (time.Time, error) {
	clock, err := keep28Clock(ctx, a.guard.tx)
	if err != nil {
		return time.Time{}, err
	}
	if sampled := clock.deadline(a.custody.Expiry); sampled.Before(a.custodyEnd) {
		a.custodyEnd = sampled
	}
	if !time.Now().Before(a.custodyEnd) {
		return a.custodyEnd, errors.New("NAS custody expired at network boundary")
	}
	return a.custodyEnd, nil
}

// bound ends network work at custody end; the parent context keeps the key's
// network deadline. RAW carries no custody and is bounded by the parent alone.
func (a *inverseDeleteAuthority) bound(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if a.guard.a.opts.Lane != inverseNASKeep28 {
		return ctx, func() {}, nil
	}
	end, err := a.custodyDeadline(ctx)
	if err != nil {
		return nil, nil, err
	}
	bounded, cancel := context.WithDeadline(ctx, end)
	return bounded, cancel, nil
}
