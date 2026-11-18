package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/jackc/pgx/v5"
)

// Reviewed non-raw (collated) retirement, owner scope 2026-11-18. A delivery
// hold is an ownership veto derived from catalog/dependency rows; it is never a
// source kind and never a table. Collated outputs live in their own immutable
// namespace keyed by recording_collation_outputs.id so no collated id can enter
// the RAW ledger (0201 UNIQUE(source_id) plus raw-only guards).
const (
	inverseNonrawRetirementClass = "owner_delivery_scope_20261118_v1"
	inverseNonrawReason          = "owner_delivery_scope_outside_pinned_keep28"
)

// The content-addressed key must bind the catalog hash column exactly; the 0158
// regexp alone does not. A key prefix is never mapping authority.
var inverseCollatedKeyRE = regexp.MustCompile(`^joined/([a-z0-9][a-z0-9-]{0,62})/objects/([0-9a-f]{64})\.mp4$`)

// Mirrors the reviewed 0174 collated allowlist. Anything else is held BEFORE
// wire so a DELETE can never succeed and then fail the receipt CHECK.
func inverseCollatedPrefixAllowed(key string) bool {
	return strings.HasPrefix(key, "joined/goodplus-") ||
		strings.HasPrefix(key, "joined/nightly-collation-v2/") ||
		strings.HasPrefix(key, "joined/best14-20261021/")
}

type inverseCollatedSource struct {
	ID       int64      `json:"id"`
	StreamID int64      `json:"stream_id"`
	Path     string     `json:"path"`
	Start    *time.Time `json:"start"`
	End      *time.Time `json:"end"`
	Timezone string     `json:"tz"`
}

// inverseCollatedAssessment is one output's reviewed classification. It is a
// planning fact only; every batch reloads it under lock and re-derives it.
type inverseCollatedAssessment struct {
	ID, StreamID int64
	Key, Size    string
	SizeBytes    int64
	SHA, Path    string
	VerifiedAt   time.Time
	Shared       bool
	Held         bool
	Sources      []inverseCollatedSource
	Reason       string
	Keep28       bool
	Choice       string
}

// $1 is the bucket. The jsonb source aggregate is built in the same statement so
// a partition pass and a locked reload share one definition.
const inverseNonrawColumns = `
 o.id,o.object_key,coalesce(r.stream_id,0),o.size_bytes,lower(o.sha256),o.nas_relative_path,o.r2_verified_at,
 (SELECT count(*) FROM recording_collation_outputs d WHERE d.object_key=o.object_key),
 cardinality(o.source_clip_ids),
 (SELECT count(*) FROM (SELECT DISTINCT unnest(o.source_clip_ids)) u),
 (SELECT count(*) FROM recording_clips c JOIN recordings rr ON rr.id=c.recording_id
   WHERE c.id=ANY(o.source_clip_ids) AND c.purged_at IS NULL AND c.bucket=$1),
 (SELECT coalesce(jsonb_agg(jsonb_build_object('id',c.id,'stream_id',coalesce(rr.stream_id,0),
    'path',coalesce(c.display_path,''),'start',c.clip_start_at,'end',c.clip_end_at,
    'tz',coalesce(nullif(s.local_timezone,''),rr.cron_timezone,'')) ORDER BY c.id),'[]'::jsonb)
   FROM recording_collation_outputs oo CROSS JOIN LATERAL unnest(oo.source_clip_ids) sid
   JOIN recording_clips c ON c.id=sid JOIN recordings rr ON rr.id=c.recording_id
   LEFT JOIN streams s ON s.id=rr.stream_id WHERE oo.id=o.id),
 EXISTS(SELECT 1 FROM nas_cleanup_incident_roster i WHERE i.bucket=$1 AND i.object_key=o.object_key)
  OR EXISTS(SELECT 1 FROM nas_cleanup_reserved_files f WHERE f.kind='raw' AND f.source_id=ANY(o.source_clip_ids))
  OR EXISTS(SELECT 1 FROM nas_cleanup_source_holds h JOIN nas_cleanup_plans p ON p.id=h.plan_id
    WHERE h.clip_id=ANY(o.source_clip_ids) AND (p.mode<>'retired' OR EXISTS(SELECT 1 FROM nas_cleanup_items it
     WHERE it.plan_id=p.id AND (it.delete_ever_issued OR it.state IN ('delete_issued','unverified','media_deleted_sidecar_failed')))))
  OR EXISTS(SELECT 1 FROM nas_cleanup_incident_sources n WHERE n.clip_id=ANY(o.source_clip_ids))
  OR EXISTS(SELECT 1 FROM nas_restore_requests n JOIN recording_clips c ON c.id=n.clip_id
    WHERE c.id=ANY(o.source_clip_ids) AND n.state IN ('pending','leased'))
  OR EXISTS(SELECT 1 FROM recording_joined_source_snapshots n WHERE n.clip_id=ANY(o.source_clip_ids))
  OR EXISTS(SELECT 1 FROM r2_object_retirements x WHERE x.bucket=$1 AND x.object_key=o.object_key)
  OR EXISTS(SELECT 1 FROM nas_r2_copy_items ci WHERE (ci.state<>'verified' OR ci.blocked_reason<>'')
    AND ci.item->>'bucket'=$1 AND ci.item->>'object_key'=o.object_key)
  OR EXISTS(SELECT 1 FROM r2_nonraw_intents ni WHERE ni.bucket=$1 AND ni.object_key=o.object_key AND ni.source_id<>o.id)
  OR EXISTS(SELECT 1 FROM r2_inverse_intents ri WHERE ri.bucket=$1 AND ri.object_key=o.object_key)
  OR EXISTS(SELECT 1 FROM r2_retirement_reservations rv WHERE rv.bucket=$1 AND rv.object_key=o.object_key
    AND NOT (rv.kind='collated' AND rv.source_id=o.id))`

const inverseNonrawBatchSQL = `SELECT` + inverseNonrawColumns + `
FROM recording_collation_outputs o
JOIN recording_collation_hours h ON h.id=o.collation_hour_id
JOIN recordings r ON r.id=h.recording_id
LEFT JOIN streams s ON s.id=r.stream_id`

const inverseNonrawTargetSQL = inverseNonrawBatchSQL + ` WHERE o.object_key=$2 FOR SHARE OF o`

// classifyCollated is the partition-side answer for a key with a collated
// catalog identity. Default (unreviewed) behavior stays exactly as before.
func (p inverseProtection) classifyCollated(o r2.ObjectInfo, identityUncertain bool) (inverseClass, string) {
	if strings.HasPrefix(o.Key, inverseToolsPrefix) {
		return inverseTools, "pinned_stitch_tools"
	}
	// Default behavior is byte-for-byte what it was: collated stays deferred.
	if !p.ReviewedCollated {
		return inverseHeld, "collated_deferred"
	}
	// Under the reviewed scope, respect the shared unknown veto before any
	// eligibility. An identity the RAW path already judged uncertain must never
	// gain eligibility merely by being collated.
	if identityUncertain || p.Unknown[o.Key] {
		return inverseUnknown, "uncertain_policy_or_identity"
	}
	x, ok := p.Collated[o.Key]
	if !ok {
		return inverseUnknown, "no_collated_catalog_output"
	}
	if x.StreamID > 0 && p.Picks.Choices[x.StreamID].Choice == "all" {
		return inverseKE, "keep_everything_stream"
	}
	if x.Reason != "" {
		return inverseHeld, x.Reason
	}
	if x.Keep28 {
		// Neither owner ACKs nor copied RAW sources prove a current hashed
		// inventory of this output, so keep28 collated footage stays held.
		return inverseHeld, "keep28_nonraw_no_fresh_nas_inventory"
	}
	// A delivery hold on mapped footage is an ownership veto, not a protection.
	return inverseCollated, inverseNonrawReason
}

func scanCollated(rows pgx.Rows) (inverseCollatedAssessment, error) {
	var x inverseCollatedAssessment
	var sourceJSON string
	var shared, card, distinct, resolved int64
	if err := rows.Scan(&x.ID, &x.Key, &x.StreamID, &x.SizeBytes, &x.SHA, &x.Path, &x.VerifiedAt,
		&shared, &card, &distinct, &resolved, &sourceJSON, &x.Held); err != nil {
		return x, err
	}
	x.Shared = shared != 1
	if err := json.Unmarshal([]byte(sourceJSON), &x.Sources); err != nil {
		return x, err
	}
	if card == 0 || distinct != card || resolved != card || int64(len(x.Sources)) != card {
		x.Reason = "collated_sources_empty_duplicate_or_unresolved"
	}
	return x, nil
}

// assessCollated applies KE and pinned keep28 to ONE output using fresh picks and
// windows. Any KE source, KE path alias, unresolved source or live hold protects
// the whole output; ANY source overlapping a pinned window makes it keep28.
func assessCollated(p inverseProtection, x inverseCollatedAssessment) inverseCollatedAssessment {
	if x.Reason != "" {
		return x
	}
	if strings.HasPrefix(x.Key, inverseToolsPrefix) {
		x.Reason = "pinned_stitch_tools"
		return x
	}
	if !inverseCollatedPrefixAllowed(x.Key) {
		x.Reason = "unsupported_collated_prefix"
		return x
	}
	if x.Shared {
		x.Reason = "shared_collated_object_key"
		return x
	}
	if x.VerifiedAt.IsZero() {
		x.Reason = "collated_output_unverified"
		return x
	}
	if m := inverseCollatedKeyRE.FindStringSubmatch(x.Key); m == nil || m[2] != strings.ToLower(x.SHA) {
		x.Reason = "content_addressed_key_mismatch"
		return x
	}
	if x.Held {
		x.Reason = "live_hold"
		return x
	}
	for _, component := range strings.Split(x.Path, "/") {
		if p.KEFolders[component] {
			x.Reason = "keep_everything_path"
			return x
		}
	}
	for _, s := range x.Sources {
		for _, component := range strings.Split(s.Path, "/") {
			if p.KEFolders[component] {
				x.Reason = "keep_everything_catalog_path"
				return x
			}
		}
		if p.Picks.Choices[s.StreamID].Choice == "all" {
			x.Reason = "keep_everything_stream"
			return x
		}
		// The SAME uncertainty predicate the RAW path uses. An absent window, a
		// nil/invalid span, a missing or mismatched timezone or an undecided
		// stream is uncertainty, and uncertainty HOLDS: it must never fall through
		// as "not overlapping" and become eligible.
		if p.sourceIdentityUncertain(s) {
			x.Reason = "collated_source_uncertain"
			return x
		}
		w := p.Windows[s.StreamID]
		if s.Start.Before(w.End) && s.End.After(w.Start) {
			// ANY source in the pinned window protects the WHOLE output. Owner ACKs
			// and copied RAW sources are not a current hashed inventory of it.
			x.Keep28 = true
		}
	}
	return x
}

// loadInverseCollatedTargets assesses every catalog output once per batch so the
// partition can report each object individually. Reports never authorize wire.
func loadInverseCollatedTargets(ctx context.Context, q r2RetireQuerier, bucket string, p inverseProtection) (map[string]inverseCollatedAssessment, error) {
	rows, err := q.Query(ctx, inverseNonrawBatchSQL, bucket)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]inverseCollatedAssessment{}
	for rows.Next() {
		x, err := scanCollated(rows)
		if err != nil {
			return nil, err
		}
		out[x.Key] = assessCollated(p, x)
	}
	return out, rows.Err()
}

// loadNonrawTarget reloads the exact output under lock. A partition assessment is
// never authority; identity, key/hash binding, source resolution, KE and keep28
// are all re-derived here and again at the wire.
func (a *inverseApplier) loadNonrawTarget(ctx context.Context, tx pgx.Tx, e inverseEntry) (inverseCollatedAssessment, error) {
	var t inverseCollatedAssessment
	rows, err := tx.Query(ctx, inverseNonrawTargetSQL, a.store.Bucket(), e.Key)
	if err != nil {
		return t, err
	}
	n := 0
	for rows.Next() {
		n++
		if t, err = scanCollated(rows); err != nil {
			rows.Close()
			return t, err
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return t, err
	}
	t = assessCollated(a.protection, t)
	if n != 1 || t.ID <= 0 || t.SizeBytes <= 0 || t.SizeBytes != e.SizeBytes || t.Reason != "" || t.Keep28 {
		return t, fmt.Errorf("%w: collated identity absent, protected, unresolved or changed (%s)", errInverseNonrawIneligible, t.Reason)
	}
	t.Choice = a.protection.Picks.Choices[t.StreamID].Choice
	if t.Choice == "all" {
		return t, fmt.Errorf("%w: keep-everything stream", errInverseNonrawIneligible)
	}
	if class, reason := a.protection.classify(e.ObjectInfo); class != inverseCollated {
		return t, fmt.Errorf("%w: reclassified %s (%s)", errInverseNonrawIneligible, class, reason)
	}
	return t, nil
}

var errInverseNonrawIneligible = errors.New("collated target ineligible")

func (a *inverseApplier) reserveNonraw(ctx context.Context, e inverseEntry) (inverseCollatedAssessment, error) {
	t := inverseCollatedAssessment{}
	tx, err := a.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return t, err
	}
	defer tx.Rollback(context.Background())
	if err = a.checkEvidence(ctx, tx); err != nil {
		return t, err
	}
	if err = inverseProtectionBarrier(ctx, tx); err != nil {
		return t, err
	}
	if err = inverseProtectionLocks(ctx, tx, false); err != nil {
		return t, err
	}
	if _, err = tx.Exec(ctx, `SELECT lock_nas_r2_copy_key($1,$2)`, a.store.Bucket(), e.Key); err != nil {
		return t, err
	}
	if t, err = a.loadNonrawTarget(ctx, tx, e); err != nil {
		return t, err
	}
	ids := make([]int64, 0, len(t.Sources))
	for _, s := range t.Sources {
		ids = append(ids, s.ID)
	}
	var charged int64
	if err = tx.QueryRow(ctx, `SELECT coalesce(sum(charged_bytes),0) FROM r2_inverse_budget_minutes WHERE bucket=$1 AND minute>=date_trunc('minute',clock_timestamp()-interval '24 hours')`, a.store.Bucket()).Scan(&charged); err != nil {
		return t, err
	}
	if a.opts.Budget24h > 0 && e.SizeBytes > a.opts.Budget24h-charged {
		return t, errors.New("rolling 24h inverse budget exhausted")
	}
	// Typed immutable intent BEFORE the shared reservation, in ONE transaction,
	// with no network access before commit.
	if _, err = tx.Exec(ctx, `INSERT INTO r2_nonraw_intents(bucket,object_key,endpoint_sha256,kind,stream_id,source_id,source_ids,size_bytes,sha256,nas_relative_path,etag,last_modified,verification_at,manifest_sha256,policy_sha256,picks_sha256,drain_evidence_sha256,retirement_class,choice)
 VALUES($1,$2,$3,'collated',$4,$5,$6::bigint[],$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`,
		a.store.Bucket(), e.Key, a.store.EndpointSHA256(), t.StreamID, t.ID, ids,
		e.SizeBytes, t.SHA, t.Path, e.ETag, e.LastModified, t.VerifiedAt,
		a.opts.ManifestSHA, a.summary.PolicySHA256, a.summary.PicksSHA256, a.opts.DrainSHA,
		inverseNonrawRetirementClass, t.Choice); err != nil {
		return t, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256,retirement_class)
 VALUES('collated',$1,$2,$3,$4,$5,$6,$7)`, t.ID, a.store.Bucket(), e.Key, e.SizeBytes, t.SHA, a.opts.ManifestSHA, inverseNonrawRetirementClass); err != nil {
		return t, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO r2_inverse_budget_minutes(bucket,minute,charged_bytes) VALUES($1,date_trunc('minute',clock_timestamp()),$2) ON CONFLICT(bucket,minute) DO UPDATE SET charged_bytes=r2_inverse_budget_minutes.charged_bytes+excluded.charged_bytes`, a.store.Bucket(), e.SizeBytes); err != nil {
		return t, err
	}
	return t, tx.Commit(ctx)
}

// proveNonrawReceipt exercises the exact final receipt inside a rolled-back
// savepoint, so a DELETE can never succeed and then fail a DB CHECK.
func (a *inverseApplier) proveNonrawReceipt(ctx context.Context, tx pgx.Tx, t inverseCollatedAssessment, e inverseEntry) error {
	if _, err := tx.Exec(ctx, `SAVEPOINT nonraw_receipt_proof`); err != nil {
		return err
	}
	_, proofErr := tx.Exec(ctx, `INSERT INTO r2_object_retirements(kind,source_id,stream_id,bucket,object_key,size_bytes,sha256,reason,choice,manifest_sha256,policy_sha256,already_absent)
 VALUES('collated',$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,false)`,
		t.ID, t.StreamID, a.store.Bucket(), e.Key, e.SizeBytes, t.SHA, inverseNonrawReason, t.Choice, a.opts.ManifestSHA, a.summary.PolicySHA256)
	if _, err := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT nonraw_receipt_proof`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `RELEASE SAVEPOINT nonraw_receipt_proof`); err != nil {
		return err
	}
	if proofErr != nil {
		return fmt.Errorf("pre-delete receipt constraint proof failed: %w", proofErr)
	}
	return nil
}

func (a *inverseApplier) publishNonrawAbsent(ctx context.Context, e inverseEntry, manifest string, reconciled bool) error {
	tx, err := a.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if err = inverseProtectionLocks(ctx, tx, false); err != nil {
		return err
	}
	var sourceID int64
	if err = tx.QueryRow(ctx, `SELECT o.id FROM r2_nonraw_intents i
 JOIN recording_collation_outputs o ON o.id=i.source_id
 WHERE i.bucket=$1 AND i.object_key=$2 AND i.manifest_sha256=$3 AND i.endpoint_sha256=$4
 AND o.object_key=i.object_key AND o.size_bytes=i.size_bytes AND lower(o.sha256)=i.sha256 FOR SHARE OF o`,
		a.store.Bucket(), e.Key, manifest, a.store.EndpointSHA256()).Scan(&sourceID); err != nil {
		return fmt.Errorf("nonraw absence publication requires exact catalog output: %w", err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO r2_object_retirements(kind,source_id,stream_id,bucket,object_key,size_bytes,sha256,reason,choice,manifest_sha256,policy_sha256,already_absent)
 SELECT 'collated',o.id,i.stream_id,i.bucket,i.object_key,i.size_bytes,i.sha256,$4,i.choice,i.manifest_sha256,i.policy_sha256,$5
 FROM r2_nonraw_intents i JOIN recording_collation_outputs o ON o.id=i.source_id
 WHERE i.bucket=$1 AND i.object_key=$2 AND i.manifest_sha256=$3 ON CONFLICT DO NOTHING`,
		a.store.Bucket(), e.Key, manifest, inverseNonrawReason, reconciled); err != nil {
		return err
	}
	var receipt bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM r2_object_retirements r JOIN r2_nonraw_intents i
 ON r.bucket=i.bucket AND r.object_key=i.object_key WHERE i.bucket=$1 AND i.object_key=$2
 AND r.kind='collated' AND r.reason=$3 AND r.source_id=$4 AND r.manifest_sha256=$5)`,
		a.store.Bucket(), e.Key, inverseNonrawReason, sourceID, manifest).Scan(&receipt); err != nil {
		return err
	}
	if !receipt {
		return errors.New("nonraw absence publication requires exact receipt")
	}
	if _, err = tx.Exec(ctx, `INSERT INTO r2_nonraw_events(bucket,object_key,event) VALUES($1,$2,'absent') ON CONFLICT DO NOTHING`, a.store.Bucket(), e.Key); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	return a.record(e, inverseAbsent, manifest)
}

func (a *inverseApplier) deleteNonraw(ctx context.Context, e inverseEntry) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, inverseNetworkTimeout)
	defer cancel()
	if _, err := a.reserveNonraw(ctx, e); err != nil {
		return false, err
	}
	if err := a.record(e, inverseIntent, a.opts.ManifestSHA); err != nil {
		return false, err
	}
	tx, err := a.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, err
	}
	defer tx.Rollback(context.Background())
	authority, err := a.authorizeNonraw(ctx, tx, e)
	if err != nil {
		return false, err
	}
	if err = a.store.(inverseNonrawStore).deleteNonrawOnce(ctx, authority); err != nil {
		return false, fmt.Errorf("single DELETE result ambiguous; intent retained, never retransmit: %w", err)
	}
	if _, err = a.store.Head(ctx, e.Key); !r2.IsNotFound(err) {
		if err == nil {
			err = errors.New("object remains after DELETE")
		}
		return false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, a.publishNonrawAbsent(ctx, e, a.opts.ManifestSHA, false)
}

// authorizeNonraw issues the single-use capability only after every live check,
// and only once across restarts and processes.
// prepareNonrawWire uses the SAME protection envelope as the RAW wire path via
// protectionEnvelope; only the typed collated reload differs. Everything that
// guards the DELETE is shared, so this path cannot become a weaker copy.
func (a *inverseApplier) prepareNonrawWire(ctx context.Context, tx pgx.Tx, e inverseEntry) (inverseCollatedAssessment, error) {
	var t inverseCollatedAssessment
	err := a.protectionEnvelope(ctx, tx, e, func(ctx context.Context, tx pgx.Tx, e inverseEntry) error {
		loaded, err := a.loadNonrawTarget(ctx, tx, e)
		if err != nil {
			return err
		}
		t = loaded
		return nil
	})
	return t, err
}

func (a *inverseApplier) authorizeNonraw(ctx context.Context, tx pgx.Tx, e inverseEntry) (*inverseNonrawAuthority, error) {
	if a.opts.Lane != inverseNonrawCollated {
		return nil, errors.New("nonraw authority requires the collated lane")
	}
	t, err := a.prepareNonrawWire(ctx, tx, e)
	if err != nil {
		return nil, err
	}
	var issued bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM r2_nonraw_attempts WHERE bucket=$1 AND object_key=$2)`, a.store.Bucket(), e.Key).Scan(&issued); err != nil {
		return nil, err
	}
	if issued {
		return nil, errors.New("nonraw wire authority already issued; reconcile only")
	}
	if err = a.proveNonrawReceipt(ctx, tx, t, e); err != nil {
		return nil, err
	}
	binding := inverseDeleteBinding{Bucket: a.store.Bucket(), EndpointSHA256: a.store.EndpointSHA256(), Key: e.Key, ETag: e.ETag, SourceID: t.ID, StreamID: t.StreamID, SizeBytes: e.SizeBytes, LastModified: e.LastModified, ManifestSHA256: a.opts.ManifestSHA, PolicySHA256: a.summary.PolicySHA256, PicksSHA256: a.summary.PicksSHA256, DrainSHA256: a.opts.DrainSHA}
	key := inverseWireGuardKey(a, tx, t, e)
	if err = key.check(ctx, binding); err != nil {
		return nil, err
	}
	head, err := a.store.Head(ctx, e.Key)
	if err != nil {
		return nil, err
	}
	var now time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return nil, err
	}
	item := r2RetireItem{r2RetireCandidate: r2RetireCandidate{SizeBytes: e.SizeBytes, SHA256: t.SHA, ETag: e.ETag}}
	if err = r2RetireValidateGuardIdentity(head, item, e.ETag); err != nil {
		return nil, err
	}
	// Pre-fence cutoff on the listing AND the final HEAD. No seven-day age is
	// invented from the owner's pinned scope.
	if !head.LastModified.Equal(e.LastModified) || !inverseLastModifiedAllowed(e.LastModified, a.summary.CreatedAt, now) {
		return nil, inverseLastModifiedError{}
	}
	if err = a.store.RequireNoMultipartUploads(ctx); err != nil {
		return nil, err
	}
	tag, err := a.pool.Exec(ctx, `INSERT INTO r2_nonraw_attempts(bucket,object_key) VALUES($1,$2) ON CONFLICT DO NOTHING`, binding.Bucket, binding.Key)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() != 1 {
		return nil, errors.New("nonraw wire authority was already issued; reconcile only")
	}
	return &inverseNonrawAuthority{binding: binding, applier: a, tx: tx, entry: e, used: &atomic.Bool{}}, nil
}

// inverseWireGuardKey is the collated durable-intent guard. It reuses the RAW
// guard's shape and binding, restricted to the collated namespace.
func inverseWireGuardKey(a *inverseApplier, tx pgx.Tx, t inverseCollatedAssessment, e inverseEntry) *inverseNonrawGuard {
	return &inverseNonrawGuard{a: a, tx: tx, t: t, e: e}
}

type inverseNonrawGuard struct {
	a  *inverseApplier
	tx pgx.Tx
	t  inverseCollatedAssessment
	e  inverseEntry
}

func (g *inverseNonrawGuard) check(ctx context.Context, binding inverseDeleteBinding) error {
	if g.a.opts.Lane != inverseNonrawCollated || binding.SourceID != g.t.ID || binding.Key != g.e.Key {
		return errors.New("nonraw wire authority scope differs")
	}
	var durable bool
	err := g.tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM r2_nonraw_intents i WHERE i.bucket=$1 AND i.object_key=$2
 AND i.source_id=$3 AND i.manifest_sha256=$4 AND i.drain_evidence_sha256=$5 AND i.endpoint_sha256=$6
 AND i.size_bytes=$7 AND i.etag=$8 AND i.stream_id=$9 AND i.picks_sha256=$10 AND i.policy_sha256=$11
 AND i.last_modified=$12 AND i.retirement_class=$13)`,
		binding.Bucket, binding.Key, g.t.ID, g.a.opts.ManifestSHA, g.a.opts.DrainSHA, binding.EndpointSHA256,
		g.e.SizeBytes, g.e.ETag, g.t.StreamID, binding.PicksSHA256, binding.PolicySHA256,
		g.e.LastModified, inverseNonrawRetirementClass).Scan(&durable)
	if err != nil {
		return err
	}
	if !durable {
		return errors.New("nonraw wire intent missing")
	}
	return nil
}

type inverseNonrawStore interface {
	inverseReader
	deleteNonrawOnce(context.Context, *inverseNonrawAuthority) error
}

type inverseNonrawAuthority struct {
	binding inverseDeleteBinding
	applier *inverseApplier
	tx      pgx.Tx
	entry   inverseEntry
	used    *atomic.Bool
}

func (a *inverseNonrawAuthority) check(ctx context.Context, bucket, endpoint string) error {
	if a == nil || a.applier == nil || a.applier.opts.Lane != inverseNonrawCollated ||
		bucket != a.binding.Bucket || endpoint != a.binding.EndpointSHA256 {
		return errors.New("nonraw wire binding differs")
	}
	var durable, issued bool
	if err := a.tx.QueryRow(ctx, `SELECT
 EXISTS(SELECT 1 FROM r2_nonraw_intents i WHERE i.bucket=$1 AND i.object_key=$2 AND i.source_id=$3
   AND i.manifest_sha256=$4 AND i.drain_evidence_sha256=$5 AND i.endpoint_sha256=$6 AND i.size_bytes=$7
   AND i.etag=$8 AND i.stream_id=$9 AND i.picks_sha256=$10 AND i.policy_sha256=$11 AND i.last_modified=$12
   AND i.retirement_class=$13),
 EXISTS(SELECT 1 FROM r2_nonraw_attempts t WHERE t.bucket=$1 AND t.object_key=$2)`,
		a.binding.Bucket, a.binding.Key, a.binding.SourceID, a.binding.ManifestSHA256, a.binding.DrainSHA256,
		a.binding.EndpointSHA256, a.binding.SizeBytes, a.binding.ETag, a.binding.StreamID,
		a.binding.PicksSHA256, a.binding.PolicySHA256, a.binding.LastModified, inverseNonrawRetirementClass).Scan(&durable, &issued); err != nil {
		return err
	}
	if !durable {
		return errors.New("nonraw wire intent missing")
	}
	if !issued {
		return errors.New("durable nonraw issuance missing")
	}
	// Re-derive eligibility from live state under the same locks, then re-check
	// the KE epoch and fresh evidence: the reservation committed earlier, so a
	// protection or catalog change must be able to veto at the wire boundary.
	if _, err := a.applier.prepareNonrawWire(ctx, a.tx, a.entry); err != nil {
		return err
	}
	return nil
}

func (s *inverseSDKStore) deleteNonrawOnce(ctx context.Context, a *inverseNonrawAuthority) error {
	if a == nil || a.used == nil || !a.used.CompareAndSwap(false, true) {
		return errors.New("nonraw authority absent or consumed")
	}
	if err := a.check(ctx, s.Bucket(), s.EndpointSHA256()); err != nil {
		return err
	}
	transport := &inverseNonrawTransport{base: s.transport, authority: a, bucket: s.Bucket(), endpoint: s.EndpointSHA256()}
	_, err := s.sdk.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.Bucket()), Key: aws.String(a.binding.Key)}, func(o *s3.Options) {
		o.Retryer = aws.NopRetryer{}
		o.HTTPClient = &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	})
	return err
}

type inverseNonrawTransport struct {
	base             http.RoundTripper
	authority        *inverseNonrawAuthority
	bucket, endpoint string
	attempted        atomic.Bool
}

func (t *inverseNonrawTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodDelete || req.URL.Path != "/"+t.bucket+"/"+t.authority.binding.Key || req.URL.RawQuery != "x-id=DeleteObject" || !t.attempted.CompareAndSwap(false, true) {
		return nil, errors.New("nonraw transport permits one exact DELETE")
	}
	if err := t.authority.check(req.Context(), t.bucket, t.endpoint); err != nil {
		return nil, err
	}
	req.Header.Del("Idempotency-Key")
	req.Header.Del("X-Idempotency-Key")
	return t.base.RoundTrip(req)
}
