package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Closed table selection: no operator-supplied identifier enters SQL. B1 stays
// in its existing namespace; ordinary reservations and byte charges are shared.
func (l inverseLane) intentTable() string {
	switch l {
	case inverseNASKeep28:
		return "r2_keep28_intents"
	case inverseNonrawCollated:
		return "r2_nonraw_intents"
	}
	return "r2_inverse_intents"
}
func (l inverseLane) eventTable() string {
	switch l {
	case inverseNASKeep28:
		return "r2_keep28_events"
	case inverseNonrawCollated:
		return "r2_nonraw_events"
	}
	return "r2_inverse_events"
}
func (l inverseLane) attemptTable() string {
	switch l {
	case inverseNASKeep28:
		return "r2_keep28_attempts"
	case inverseNonrawCollated:
		return "r2_nonraw_attempts"
	}
	return "r2_inverse_attempts"
}
func (l inverseLane) retirementReason() string {
	switch l {
	case inverseNASKeep28:
		return "keep28_exact_nas"
	case inverseNonrawCollated:
		return inverseNonrawReason
	}
	return "inverse_non_keep28"
}

type keep28Custody struct {
	ConnectionID, SourceID, SizeBytes         int64
	Path, SHA, State, SidecarPath, SidecarSHA string
	SidecarSize                               int64
	VerifiedAt, ReceivedAt, Expiry            time.Time
	VerifyItemID                              int64  `json:",omitempty"`
	VerifyPlanID                              int64  `json:",omitempty"`
	VerifyPlanSHA                             string `json:",omitempty"`
}

// keep28Identity is the source and custody identity selection observed for a
// key. Reserve requires the locked custody to carry the very same one.
type keep28Identity struct {
	SourceID, SizeBytes, SidecarSize int64
	Path, SHA, SidecarSHA            string
	VerifyItemID                     int64 `json:",omitempty"`
}

func (c keep28Custody) identity() keep28Identity {
	return keep28Identity{SourceID: c.SourceID, SizeBytes: c.SizeBytes, SidecarSize: c.SidecarSize, Path: c.Path, SHA: c.SHA, SidecarSHA: c.SidecarSHA, VerifyItemID: c.VerifyItemID}
}

type keep28SelectedPlan struct {
	ID  int64
	SHA string
}

func (p *keep28SelectedPlan) flags(fs *flag.FlagSet) {
	fs.Int64Var(&p.ID, "selected-verify-plan", 0, "immutable NAS selected verifier plan ID (keep-28 only)")
	fs.StringVar(&p.SHA, "selected-verify-plan-sha256", "", "approved NAS selected verifier plan digest")
}

func (p keep28SelectedPlan) validFor(l inverseLane) bool {
	return p == (keep28SelectedPlan{}) || l == inverseNASKeep28 && p.ID > 0 && r2RetireLowerHex64(p.SHA)
}

func (a *inverseApplier) lockSelectedKeep28Custody(ctx context.Context, tx pgx.Tx, t inverseRawTarget, key string) (keep28Custody, error) {
	p := a.opts.SelectedPlan
	c := keep28Custody{
		ConnectionID: 13, SourceID: t.ID, SizeBytes: t.Size, Path: t.Path, SHA: t.SHA, State: "present",
		VerifyPlanID: p.ID, VerifyPlanSHA: p.SHA,
	}
	var locked bool
	if err := tx.QueryRow(ctx, `SELECT try_lock_r2_keep28_selected_source($1)`, t.ID).Scan(&locked); err != nil {
		return c, err
	}
	if !locked {
		return c, inverseBusy{}
	}
	var fresh bool
	err := tx.QueryRow(ctx, `SELECT v.verify_item_id,v.completed_at,v.completed_at+interval '24 hours',
 v.completed_at>=clock_timestamp()-interval '24 hours'
 FROM r2_keep28_selected_custody v JOIN nas_selected_verify_items i ON i.id=v.verify_item_id
 WHERE v.verify_plan_id=$1 AND v.plan_sha256=$2 AND v.bucket=$3 AND v.object_key=$4
 AND v.source_id=$5 AND v.relative_path=$6 AND v.size_bytes=$7 AND v.sha256=$8
 FOR SHARE OF i NOWAIT`, p.ID, p.SHA, a.store.Bucket(), key, t.ID, t.Path, t.Size, t.SHA).
		Scan(&c.VerifyItemID, &c.VerifiedAt, &c.Expiry, &fresh)
	if err != nil {
		return c, err
	}
	c.VerifiedAt, c.Expiry = c.VerifiedAt.UTC(), c.Expiry.UTC()
	c.ReceivedAt = c.VerifiedAt
	if !fresh {
		return c, errR2RetireNASProofAged
	}
	return c, nil
}

func (a *inverseApplier) lockKeep28Proof(ctx context.Context, tx pgx.Tx, t inverseRawTarget, key string) (keep28Custody, error) {
	if a.opts.SelectedPlan.ID != 0 {
		return a.lockSelectedKeep28Custody(ctx, tx, t, key)
	}
	return a.lockKeep28Custody(ctx, tx, t)
}

// lockKeep28Custody locks and reads the exact custody proof. An aged proof is
// still read in full and returned with errR2RetireNASProofAged, so callers can
// compare it before treating it as aged.
func (a *inverseApplier) lockKeep28Custody(ctx context.Context, tx pgx.Tx, t inverseRawTarget) (keep28Custody, error) {
	c := keep28Custody{ConnectionID: 13, SourceID: t.ID, SizeBytes: t.Size, Path: t.Path, SHA: t.SHA}
	item := r2RetireItem{r2RetireCandidate: r2RetireCandidate{Kind: "raw", SourceID: t.ID, SizeBytes: t.Size, SHA256: t.SHA, RelativePath: t.Path}}
	// Admission holds the protection locks here, so it never waits on custody.
	custody := inverseWait{`SELECT 1 FROM nas_inventory_files WHERE connection_id=$1 AND clip_id=$2 FOR SHARE`, []any{c.ConnectionID, c.SourceID}}
	expiry, fresh, err := r2RetireLockNASProof(ctx, tx, item, c.ConnectionID, r2RetireShareNoWait)
	if err != nil {
		return c, inverseRowBusy(fmt.Errorf("exact present NAS identity no longer lockable: %w", err), custody)
	}
	// The row is already FOR SHARE locked; capture the entire proof in a locked
	// read too. A changed sidecar/timestamp cannot hide behind an unchanged expiry.
	err = tx.QueryRow(ctx, `SELECT state,verified_at,server_received_at,sidecar_relative_path,sidecar_size_bytes,sidecar_sha256 FROM nas_inventory_files WHERE connection_id=$1 AND clip_id=$2 AND relative_path=$3 AND size_bytes=$4 AND sha256=$5 FOR SHARE NOWAIT`, c.ConnectionID, c.SourceID, c.Path, c.SizeBytes, c.SHA).Scan(&c.State, &c.VerifiedAt, &c.ReceivedAt, &c.SidecarPath, &c.SidecarSize, &c.SidecarSHA)
	c.VerifiedAt = c.VerifiedAt.UTC()
	c.ReceivedAt = c.ReceivedAt.UTC()
	c.Expiry = expiry.UTC()
	// The row is share-locked; the sidecar read above cannot hide a change.
	if err == nil && !fresh {
		return c, fmt.Errorf("exact present NAS identity no longer lockable: %w", errR2RetireNASProofAged)
	}
	return c, inverseRowBusy(err, custody)
}

// keep28AdmissionCustody is the custody a key needs left to be selected: one
// network timeout plus the margin, the same rule reserve enforces under lock.
// A shorter key would only be skipped, so it takes no item or byte allowance.
const keep28AdmissionCustody = keep28CustodyMargin + inverseNetworkTimeout

// Selection trails fresh custody in verified_at order. The join admits only
// exact, complete evidence, the predicates the custody lock enforces, so a
// legacy row without sidecar evidence is simply ineligible. A report is never
// wire authority; every identity and proof is subsequently reloaded under locks,
// and reserve requires the identity observed here. Committed stitch input holds
// are left out so they never fill a batch.
func (a *inverseApplier) selectKeep28(ctx context.Context, entries map[string]inverseEntry) ([]inverseEntry, error) {
	query := `SELECT c.object_key,c.id,c.size_bytes,c.display_path,lower(c.sha256),n.sidecar_size_bytes,n.sidecar_sha256,0::bigint
 FROM recording_clips c JOIN nas_inventory_files n ON n.connection_id=$2 AND n.clip_id=c.id
 JOIN recordings r ON r.id=c.recording_id
 WHERE c.bucket=$1 AND c.purged_at IS NULL AND r.account_id=47
 AND n.state='present' AND n.relative_path=c.display_path AND n.sha256=lower(c.sha256) AND n.size_bytes=c.size_bytes
 AND n.verified_at<=clock_timestamp()+interval '5 minutes'
 AND n.sidecar_relative_path=c.display_path||'.stoarama.json' AND n.sidecar_size_bytes>0 AND n.sidecar_sha256 ~ '^[a-f0-9]{64}$'
 AND LEAST(n.verified_at,n.server_received_at)>clock_timestamp()-interval '24 hours'+make_interval(secs=>$3)
 AND NOT EXISTS(SELECT 1 FROM nas_stitch_r2_input_holds h WHERE h.bucket=c.bucket AND h.object_key=c.object_key AND h.released_at IS NULL)
 ORDER BY n.verified_at,c.id`
	args := []any{a.store.Bucket(), int64(13), keep28AdmissionCustody.Seconds()}
	if p := a.opts.SelectedPlan; p.ID != 0 {
		query = `SELECT v.object_key,v.source_id,v.size_bytes,v.relative_path,v.sha256,0::bigint,''::text,v.verify_item_id
 FROM r2_keep28_selected_custody v WHERE v.bucket=$1 AND v.verify_plan_id=$2 AND v.plan_sha256=$4
 AND v.completed_at>clock_timestamp()-interval '24 hours'+make_interval(secs=>$3)
 AND NOT r2_keep28_copy_blocks(v.bucket,v.object_key,v.verify_item_id)
 AND NOT EXISTS(SELECT 1 FROM nas_stitch_r2_input_holds h WHERE h.bucket=v.bucket AND h.object_key=v.object_key AND h.released_at IS NULL)
 ORDER BY v.completed_at,v.source_id`
		args[1] = p.ID
		args = append(args, p.SHA)
	}
	rows, err := a.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := []inverseEntry{}
	var bytes int64
	for rows.Next() {
		var key string
		var id keep28Identity
		if err = rows.Scan(&key, &id.SourceID, &id.SizeBytes, &id.Path, &id.SHA, &id.SidecarSize, &id.SidecarSHA, &id.VerifyItemID); err != nil {
			return nil, err
		}
		e, ok := entries[key]
		e.Selected = id
		if !ok || e.SizeBytes <= 0 {
			continue
		}
		if a.opts.skipped.has(e.Key) {
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
	custody, err := a.lockKeep28Proof(ctx, tx, target, e.Key)
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
	c, err := a.guard.a.lockKeep28Proof(ctx, a.guard.tx, a.guard.t, a.binding.Key)
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
