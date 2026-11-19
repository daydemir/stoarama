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

// proveKeep28 relocks the NAS custody behind a keep-28 intent and requires it
// to equal the proof persisted with that intent, with lifetime left.
func (a *inverseApplier) proveKeep28(ctx context.Context, tx pgx.Tx, e inverseEntry, target inverseRawTarget) (keep28Custody, keep28DBClock, error) {
	custody, err := a.lockKeep28Custody(ctx, tx, target)
	if err != nil {
		return custody, keep28DBClock{}, err
	}
	var data []byte
	if err = tx.QueryRow(ctx, `SELECT custody FROM r2_keep28_intents WHERE bucket=$1 AND object_key=$2`, a.store.Bucket(), e.Key).Scan(&data); err != nil {
		return custody, keep28DBClock{}, err
	}
	var intended keep28Custody
	if err = json.Unmarshal(data, &intended); err != nil {
		return custody, keep28DBClock{}, err
	}
	if custody != intended {
		return custody, keep28DBClock{}, errors.New("custody proof changed after durable intent")
	}
	clock, err := keep28Clock(ctx, tx)
	if err != nil {
		return custody, clock, err
	}
	if clock.remaining(custody.Expiry) <= 0 {
		return custody, clock, errors.New("NAS custody expired before wire")
	}
	return custody, clock, nil
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
	_, err = a.custodyRemaining(ctx)
	return err
}

// custodyRemaining samples the DB clock before every network handoff. A newer
// DB clock can shorten, never extend, the lifetime measured from the original
// monotonic sample.
func (a *inverseDeleteAuthority) custodyRemaining(ctx context.Context) (time.Duration, error) {
	clock, err := keep28Clock(ctx, a.guard.tx)
	if err != nil {
		return 0, err
	}
	remaining := min(a.clock.remaining(a.custody.Expiry), clock.remaining(a.custody.Expiry))
	if remaining <= 0 {
		return 0, errors.New("NAS custody expired at network boundary")
	}
	return remaining, nil
}

// bound limits network work to what is left of keep-28 custody. RAW carries no
// custody; its work is bounded by the key's network deadline alone.
func (a *inverseDeleteAuthority) bound(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if a.guard.a.opts.Lane != inverseNASKeep28 {
		return ctx, func() {}, nil
	}
	remaining, err := a.custodyRemaining(ctx)
	if err != nil {
		return nil, nil, err
	}
	bounded, cancel := context.WithTimeout(ctx, min(inverseNetworkTimeout, remaining))
	return bounded, cancel, nil
}
