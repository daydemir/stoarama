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

// inverseConfidentRepairPrefix holds the delivery page's confident repairs:
// lossless joins of original delivery parts, with no catalog row (0225).
const inverseConfidentRepairPrefix = "experimental/stitching-confident-repairs/"

// inverseNonrawKind is the ledger kind a nonraw key retires under. A collated
// output is keyed by its catalog id; a confident repair, which has no catalog
// row, by its owner exemption id.
type inverseNonrawKind string

const (
	inverseNonrawKindCollated inverseNonrawKind = "collated"
	inverseNonrawKindRepair   inverseNonrawKind = "delivery_repair"
)

// inverseNonrawKindOf picks the loader for a key from its family. The loader,
// not the prefix, is the mapping authority: it still needs an exact catalog
// output or an exact open exemption.
func inverseNonrawKindOf(key string) (inverseNonrawKind, bool) {
	switch {
	case inverseCollatedPrefixAllowed(key):
		return inverseNonrawKindCollated, true
	case strings.HasPrefix(key, inverseConfidentRepairPrefix):
		return inverseNonrawKindRepair, true
	}
	return "", false
}

type inverseCollatedSource struct {
	ID       int64      `json:"id"`
	StreamID int64      `json:"stream_id"`
	Key      string     `json:"key"`
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
	// Start/End/Timezone are the output's OWN live interval and zone. They decide
	// its pinned-window class and must be reloaded, not assumed from the batch.
	Start, End *time.Time
	Timezone   string
	Shared     bool
	// RawIdentities counts recording_clips rows sharing this object key. A key
	// must belong to exactly one namespace: a collated lane must never retire a
	// RAW clip's key, whose RAW holds and purged_at it would not satisfy.
	RawIdentities int64
	Held          bool
	Sources       []inverseCollatedSource
	Reason        string
	Keep28        bool
	Choice        string
	Kind          inverseNonrawKind
	// Exemption is the key's open owner exemption class, empty when none, and
	// ExemptETag the exact page ETag that exemption binds.
	Exemption  deliveryExemptionClass
	ExemptETag string
}

// keep28Held reports whether the pinned window still protects the output. Only
// an open in_window exemption, whose gate needs NAS custody, lifts it.
func (x inverseCollatedAssessment) keep28Held() bool {
	return x.Keep28 && x.Exemption != deliveryExemptInWindow
}

// reason is the partition and receipt reason the output retires under.
func (x inverseCollatedAssessment) reason() string {
	if x.Exemption != "" {
		return x.Exemption.receiptReason()
	}
	return inverseNonrawReason
}

// $1 is the bucket. The jsonb source aggregate is built in the same statement so
// a partition pass and a locked reload share one definition.
const inverseNonrawColumns = `
 o.id,o.object_key,coalesce(r.stream_id,0),o.size_bytes,lower(o.sha256),o.nas_relative_path,o.r2_verified_at,
 o.start_at,o.end_at,coalesce(nullif(s.local_timezone,''),r.cron_timezone,''),
 (SELECT count(*) FROM recording_clips rc WHERE rc.bucket=$1 AND rc.object_key=o.object_key),
 (SELECT count(*) FROM recording_collation_outputs d WHERE d.object_key=o.object_key),
 cardinality(o.source_clip_ids),
 (SELECT count(*) FROM (SELECT DISTINCT unnest(o.source_clip_ids)) u),
 (SELECT count(*) FROM recording_clips c JOIN recordings rr ON rr.id=c.recording_id
   WHERE c.id=ANY(o.source_clip_ids) AND c.purged_at IS NULL AND c.bucket=$1),
 (SELECT coalesce(jsonb_agg(jsonb_build_object('id',c.id,'stream_id',coalesce(rr.stream_id,0),
    'key',c.object_key,'path',coalesce(c.display_path,''),'start',c.clip_start_at,'end',c.clip_end_at,
    'tz',coalesce(nullif(s.local_timezone,''),rr.cron_timezone,'')) ORDER BY c.id),'[]'::jsonb)
   FROM recording_collation_outputs oo CROSS JOIN LATERAL unnest(oo.source_clip_ids) sid
   JOIN recording_clips c ON c.id=sid JOIN recordings rr ON rr.id=c.recording_id
   LEFT JOIN streams s ON s.id=rr.stream_id WHERE oo.id=o.id),
 r2_nonraw_key_held($1,o.object_key,'collated',o.id)
  OR EXISTS(SELECT 1 FROM nas_cleanup_reserved_files f WHERE f.kind='raw' AND f.source_id=ANY(o.source_clip_ids))
  OR EXISTS(SELECT 1 FROM nas_cleanup_source_holds h JOIN nas_cleanup_plans p ON p.id=h.plan_id
    WHERE h.clip_id=ANY(o.source_clip_ids) AND (p.mode<>'retired' OR EXISTS(SELECT 1 FROM nas_cleanup_items it
     WHERE it.plan_id=p.id AND (it.delete_ever_issued OR it.state IN ('delete_issued','unverified','media_deleted_sidecar_failed')))))
  OR EXISTS(SELECT 1 FROM nas_cleanup_incident_sources n WHERE n.clip_id=ANY(o.source_clip_ids))
  OR EXISTS(SELECT 1 FROM nas_restore_requests n JOIN recording_clips c ON c.id=n.clip_id
    WHERE c.id=ANY(o.source_clip_ids) AND n.state IN ('pending','leased'))
  OR EXISTS(SELECT 1 FROM recording_joined_source_snapshots n WHERE n.clip_id=ANY(o.source_clip_ids))
  OR EXISTS(SELECT 1 FROM recording_clips c JOIN recording_joined_snapshot_scopes js
    ON js.recording_id=c.recording_id AND js.recording_job_id=c.recording_job_id AND c.id<=js.high_water_clip_id
    JOIN recording_joined_batches jb ON jb.id=js.batch_record_id AND jb.state='snapshotting'
    WHERE c.id=ANY(o.source_clip_ids))
  OR EXISTS(SELECT 1 FROM recording_clips c JOIN recording_joined_dry_run_scopes ds
    ON ds.recording_id=c.recording_id AND ds.recording_job_id=c.recording_job_id AND c.id<=ds.high_water_clip_id
    WHERE c.id=ANY(o.source_clip_ids)),
 coalesce(x.class::text,''),coalesce(x.etag,'')`

const inverseNonrawBatchSQL = `SELECT` + inverseNonrawColumns + `
FROM recording_collation_outputs o
JOIN recording_collation_hours h ON h.id=o.collation_hour_id
JOIN recordings r ON r.id=h.recording_id
LEFT JOIN streams s ON s.id=r.stream_id
LEFT JOIN r2_delivery_exemptions x ON x.bucket=$1 AND x.object_key=o.object_key AND r2_delivery_exempt(x.bucket,x.object_key)`

const inverseNonrawTargetSQL = inverseNonrawBatchSQL + ` WHERE o.object_key=$2 FOR SHARE OF o`

// A confident repair's identity is its open exemption and the exact page row it
// binds; it has no catalog output and no RAW sources. Every input is re-read
// under lock at reserve and at wire, and the page row is share-locked so its
// identity cannot change before the DELETE.
const inverseRepairBatchSQL = `SELECT x.id,h.object_key,coalesce(r.stream_id,0),h.size_bytes,h.sha256,h.relative_path,h.imported_at,
 (SELECT count(*) FROM recording_clips rc WHERE rc.bucket=$1 AND rc.object_key=h.object_key),
 (SELECT count(*) FROM recording_collation_outputs d WHERE d.object_key=h.object_key),
 r2_nonraw_key_held($1,h.object_key,'delivery_repair',x.id),x.class::text,x.etag
FROM r2_delivery_exemptions x JOIN delivery_keep_holds h ON h.bucket=x.bucket AND h.object_key=x.object_key
LEFT JOIN recordings r ON r.id=h.recording_id
WHERE x.bucket=$1 AND x.class='confident_repair' AND r2_delivery_exempt(x.bucket,x.object_key)`

const inverseRepairTargetSQL = inverseRepairBatchSQL + ` AND x.object_key=$2 FOR SHARE OF x,h`

// inverseNonrawLoader is the only per-kind part of the nonraw lane: how a key's
// identity is loaded and assessed, and how its absence is bound back to it.
type inverseNonrawLoader struct {
	batchSQL, targetSQL string
	scan                func(pgx.Rows) (inverseCollatedAssessment, error)
	assess              func(inverseProtection, inverseCollatedAssessment) inverseCollatedAssessment
	// absentSQL selects the intent's source id only while the intent still names
	// the exact identity it reserved ($1 bucket, $2 key, $3 manifest, $4 endpoint).
	absentSQL string
}

var inverseNonrawLoaders = map[inverseNonrawKind]inverseNonrawLoader{
	inverseNonrawKindCollated: {inverseNonrawBatchSQL, inverseNonrawTargetSQL, scanCollated, assessCollated, `SELECT o.id FROM r2_nonraw_intents i
 JOIN recording_collation_outputs o ON o.id=i.source_id
 WHERE i.kind='collated' AND i.bucket=$1 AND i.object_key=$2 AND i.manifest_sha256=$3 AND i.endpoint_sha256=$4
 AND o.object_key=i.object_key AND o.size_bytes=i.size_bytes AND lower(o.sha256)=i.sha256 FOR SHARE OF o`},
	inverseNonrawKindRepair: {inverseRepairBatchSQL, inverseRepairTargetSQL, scanRepair, assessRepair, `SELECT x.id FROM r2_nonraw_intents i
 JOIN r2_delivery_exemptions x ON x.id=i.source_id
 WHERE i.kind='delivery_repair' AND i.bucket=$1 AND i.object_key=$2 AND i.manifest_sha256=$3 AND i.endpoint_sha256=$4
 AND x.bucket=i.bucket AND x.object_key=i.object_key AND x.size_bytes=i.size_bytes AND x.etag=btrim(i.etag,'"')`},
}

// classifyCollated is the partition-side answer for a collated key. The caller
// reaches here only from the collated branch of classify, so a RAW identity can
// never be read as a collated one. Default (unreviewed) behavior is unchanged.
func (p inverseProtection) classifyCollated(o r2.ObjectInfo, x inverseCollatedAssessment, mapped bool) (inverseClass, string) {
	if strings.HasPrefix(o.Key, inverseToolsPrefix) {
		return inverseTools, "pinned_stitch_tools"
	}
	if !p.ReviewedCollated || !mapped {
		return inverseHeld, "collated_deferred"
	}
	if x.StreamID > 0 && p.Picks.Choices[x.StreamID].Choice == "all" {
		return inverseKE, "keep_everything_stream"
	}
	// A KE folder alias on the output's own stored path protects it. These are
	// catalog facts, so they are read here rather than trusted from the batch,
	// and assessRepair applies the same rule to a confident repair's stored path
	// so a seed can never approve a row execution would refuse.
	if p.keepEverythingPathComponent(x.Path) {
		return inverseKE, "keep_everything_path"
	}
	if x.Reason != "" {
		return inverseHeld, x.Reason
	}
	// Eligibility needs a positively mapped, content-addressed identity. An
	// assessment without a bound catalog SHA is not mapping authority.
	if x.ID <= 0 || !r2RetireLowerHex64(x.SHA) {
		return inverseUnknown, "no_collated_catalog_output"
	}
	if m := inverseCollatedKeyRE.FindStringSubmatch(o.Key); x.Kind == inverseNonrawKindCollated && (m == nil || m[2] != strings.ToLower(x.SHA)) {
		return inverseHeld, "content_addressed_key_mismatch"
	}
	if x.keep28Held() {
		// Neither owner ACKs nor copied RAW sources prove a current hashed
		// inventory of this output, so keep28 collated footage stays held.
		return inverseHeld, "keep28_nonraw_no_fresh_nas_inventory"
	}
	// A delivery hold on mapped footage is an ownership veto, not a protection.
	return inverseCollated, x.reason()
}

func scanCollated(rows pgx.Rows) (inverseCollatedAssessment, error) {
	var x inverseCollatedAssessment
	var sourceJSON string
	var shared, card, distinct, resolved, rawIdentities int64
	if err := rows.Scan(&x.ID, &x.Key, &x.StreamID, &x.SizeBytes, &x.SHA, &x.Path, &x.VerifiedAt,
		&x.Start, &x.End, &x.Timezone, &rawIdentities,
		&shared, &card, &distinct, &resolved, &sourceJSON, &x.Held, &x.Exemption, &x.ExemptETag); err != nil {
		return x, err
	}
	x.Kind = inverseNonrawKindCollated
	x.Shared = shared != 1
	x.RawIdentities = rawIdentities
	if err := json.Unmarshal([]byte(sourceJSON), &x.Sources); err != nil {
		return x, err
	}
	if card == 0 || distinct != card || resolved != card || int64(len(x.Sources)) != card {
		x.Reason = "collated_sources_empty_duplicate_or_unresolved"
	}
	return x, nil
}

func scanRepair(rows pgx.Rows) (inverseCollatedAssessment, error) {
	x := inverseCollatedAssessment{Kind: inverseNonrawKindRepair}
	var outputs int64
	err := rows.Scan(&x.ID, &x.Key, &x.StreamID, &x.SizeBytes, &x.SHA, &x.Path, &x.VerifiedAt,
		&x.RawIdentities, &outputs, &x.Held, &x.Exemption, &x.ExemptETag)
	x.Shared = outputs != 0
	return x, err
}

// assessRepair holds a confident repair unless it is a lone page key with a
// complete identity on a decided stream. It has no interval: the owner approved
// every confident repair once the page is down, so no window applies.
//
// A KE folder component in the repair's stored path (or its R2 key) protects it
// exactly as it does for a collated output. Execution re-checks this in
// classifyCollated before the wire, so the seed must apply the same rule here or
// it would approve a roster row the executor then refuses.
func assessRepair(p inverseProtection, x inverseCollatedAssessment) inverseCollatedAssessment {
	switch choice := p.Picks.Choices[x.StreamID].Choice; {
	case !strings.HasPrefix(x.Key, inverseConfidentRepairPrefix):
		x.Reason = "unsupported_repair_prefix"
	case x.Shared:
		x.Reason = "repair_key_also_collated_output"
	case x.RawIdentities != 0:
		x.Reason = "key_also_raw_identity"
	case x.Held:
		x.Reason = "live_hold"
	case x.StreamID <= 0 || !r2RetireLowerHex64(x.SHA) || x.Path == "":
		x.Reason = "repair_identity_incomplete"
	case choice != "all" && choice != "none" && choice != "st" && choice != "keep":
		x.Reason = "repair_stream_undecided"
	case p.keepEverythingPathComponent(x.Path):
		x.Reason = "keep_everything_path"
	case p.keepEverythingPathComponent(x.Key):
		x.Reason = "keep_everything_path"
	}
	return x
}

// keepEverythingPathComponent reports whether any complete folder component of
// path names a KE stream folder. It matches whole components, never a partial
// name, so the same path rule the RAW and collated classifiers use is shared.
func (p inverseProtection) keepEverythingPathComponent(path string) bool {
	for _, component := range strings.Split(path, "/") {
		if p.KEFolders[component] {
			return true
		}
	}
	return false
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
	if x.RawIdentities != 0 {
		x.Reason = "key_also_raw_identity"
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
	// The output's OWN interval is a live catalog fact. It must positively
	// establish a pinned-window class just like a source, so an interval moved
	// into keep28 after the batch can never authorize a DELETE.
	own := inverseIdentity{StreamID: x.StreamID, Path: x.Path, Start: x.Start, End: x.End, Timezone: x.Timezone, Kind: "collated"}
	if p.identityUncertain(own) {
		x.Reason = "collated_output_interval_uncertain"
		return x
	}
	if x.StreamID > 0 && p.Picks.Choices[x.StreamID].Choice == "all" {
		x.Reason = "keep_everything_stream"
		return x
	}
	if w := p.Windows[x.StreamID]; x.Start.Before(w.End) && x.End.After(w.Start) {
		x.Keep28 = true
	}
	for _, component := range strings.Split(x.Path, "/") {
		if p.KEFolders[component] {
			x.Reason = "keep_everything_path"
			return x
		}
	}
	for _, s := range x.Sources {
		// The RAW classifier protects a KE folder in either the R2 key or the
		// catalog path; a source must be protected the same way.
		for _, component := range strings.Split(s.Key, "/") {
			if p.KEFolders[component] {
				x.Reason = "keep_everything_path"
				return x
			}
		}
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
	out := map[string]inverseCollatedAssessment{}
	for _, kind := range []inverseNonrawKind{inverseNonrawKindCollated, inverseNonrawKindRepair} {
		loader := inverseNonrawLoaders[kind]
		if err := func() error {
			rows, err := q.Query(ctx, loader.batchSQL, bucket)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				x, err := loader.scan(rows)
				if err != nil {
					return err
				}
				if _, dup := out[x.Key]; dup && kind == inverseNonrawKindRepair {
					return fmt.Errorf("confident repair %s is also a collated output key", x.Key)
				}
				out[x.Key] = loader.assess(p, x)
			}
			return rows.Err()
		}(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// loadNonrawTarget reloads the exact output under lock. A partition assessment is
// never authority; identity, key/hash binding, source resolution, KE and keep28
// are all re-derived here and again at the wire.
func (a *inverseApplier) loadNonrawTarget(ctx context.Context, tx pgx.Tx, e inverseEntry) (inverseCollatedAssessment, error) {
	var t inverseCollatedAssessment
	kind, ok := inverseNonrawKindOf(e.Key)
	if !ok {
		return t, fmt.Errorf("%w: key is in no nonraw family", errInverseNonrawIneligible)
	}
	loader := inverseNonrawLoaders[kind]
	rows, err := tx.Query(ctx, loader.targetSQL, a.store.Bucket(), e.Key)
	if err != nil {
		return t, err
	}
	n := 0
	for rows.Next() {
		n++
		if t, err = loader.scan(rows); err != nil {
			rows.Close()
			return t, err
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return t, err
	}
	t = loader.assess(a.protection, t)
	if n != 1 || t.ID <= 0 || t.SizeBytes <= 0 || t.SizeBytes != e.SizeBytes || t.Reason != "" || t.keep28Held() {
		return t, fmt.Errorf("%w: %s identity absent, protected, unresolved or changed (%s)", errInverseNonrawIneligible, kind, t.Reason)
	}
	// An exemption binds the exact page object; the listed object must be it.
	if t.Exemption != "" && strings.Trim(e.ETag, `"`) != t.ExemptETag {
		return t, fmt.Errorf("%w: listed ETag differs from the exempt page identity", errInverseNonrawIneligible)
	}
	t.Choice = a.protection.Picks.Choices[t.StreamID].Choice
	if t.Choice == "all" {
		return t, fmt.Errorf("%w: keep-everything stream", errInverseNonrawIneligible)
	}
	// For an exempt delivery page key, the PAGE RECORDING's own stream must be
	// decided too, not only the catalog output's: a page row can name a
	// keep-everything or undecided stream while the output names a decided one.
	if t.Exemption != "" {
		pageStream, found, err := a.pageRecordingStream(ctx, tx, e.Key)
		if err != nil {
			return t, err
		}
		if !found || !deliveryPageStreamDecided(a.protection.Picks, a.protection.Windows, pageStream) {
			return t, fmt.Errorf("%w: page recording stream %d has no positive non-keep-everything decision", errInverseNonrawIneligible, pageStream)
		}
	}
	if class, reason := a.protection.classify(e.ObjectInfo); class != inverseCollated {
		return t, fmt.Errorf("%w: reclassified %s (%s)", errInverseNonrawIneligible, class, reason)
	}
	return t, nil
}

// pageRecordingStream reads the live stream the delivery page recording row
// names for an exact page key. found is false when the key has no page row.
func (a *inverseApplier) pageRecordingStream(ctx context.Context, q r2RetireQuerier, key string) (stream int64, found bool, err error) {
	err = q.QueryRow(ctx, `SELECT coalesce(rec.stream_id,0) FROM delivery_keep_holds h
 LEFT JOIN recordings rec ON rec.id=h.recording_id WHERE h.bucket=$1 AND h.object_key=$2`, a.store.Bucket(), key).Scan(&stream)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	return stream, err == nil, err
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
	// Hold the RAW source closure under the shared RAW row lock for the rest of
	// this transaction, so a purge or cleanup source-hold cannot slip in between
	// the eligibility read and the committed reservation.
	if err = lockInverseRawSourceClosure(ctx, tx, ids); err != nil {
		return t, err
	}
	// A purge or hold may have committed while the locks were awaited. Reassess
	// in a fresh statement under the locks before anything durable is written.
	locked, err := a.loadNonrawTarget(ctx, tx, e)
	if err != nil {
		return locked, err
	}
	if locked.ID != t.ID || len(locked.Sources) != len(ids) {
		return locked, fmt.Errorf("%w: source closure changed while locking", errInverseNonrawIneligible)
	}
	for n, s := range locked.Sources {
		if s.ID != ids[n] {
			return locked, fmt.Errorf("%w: source closure changed while locking", errInverseNonrawIneligible)
		}
	}
	t = locked
	// The classifier reads cached KE folders, picks and windows. Checking the
	// epoch BEFORE the permanent intent is written means a stale cached
	// classification can never leave append-only state behind.
	var epoch int64
	if err = tx.QueryRow(ctx, `SELECT epoch FROM r2_inverse_ke_epoch FOR SHARE`).Scan(&epoch); err != nil {
		return t, err
	}
	if epoch != a.keEpoch {
		return t, errors.New("KE catalog identity changed; refresh partition before further wire attempts")
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
 VALUES($1,$2,$3,$19,$4,$5,$6::bigint[],$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`,
		a.store.Bucket(), e.Key, a.store.EndpointSHA256(), t.StreamID, t.ID, ids,
		e.SizeBytes, t.SHA, t.Path, e.ETag, e.LastModified, t.VerifiedAt,
		a.opts.ManifestSHA, a.summary.PolicySHA256, a.summary.PicksSHA256, a.opts.DrainSHA,
		inverseNonrawRetirementClass, t.Choice, string(t.Kind)); err != nil {
		return t, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256,retirement_class)
 VALUES($8,$1,$2,$3,$4,$5,$6,$7)`, t.ID, a.store.Bucket(), e.Key, e.SizeBytes, t.SHA, a.opts.ManifestSHA, inverseNonrawRetirementClass, string(t.Kind)); err != nil {
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
 VALUES($11,$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,false)`,
		t.ID, t.StreamID, a.store.Bucket(), e.Key, e.SizeBytes, t.SHA, t.reason(), t.Choice, a.opts.ManifestSHA, a.summary.PolicySHA256, string(t.Kind))
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

func (a *inverseApplier) publishNonrawAbsent(ctx context.Context, e inverseEntry, t inverseCollatedAssessment, manifest string) error {
	tx, err := a.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if err = inverseProtectionLocks(ctx, tx, false); err != nil {
		return err
	}
	var sourceID int64
	if err = tx.QueryRow(ctx, inverseNonrawLoaders[t.Kind].absentSQL,
		a.store.Bucket(), e.Key, manifest, a.store.EndpointSHA256()).Scan(&sourceID); err != nil {
		return fmt.Errorf("nonraw absence publication requires the exact %s identity: %w", t.Kind, err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO r2_object_retirements(kind,source_id,stream_id,bucket,object_key,size_bytes,sha256,reason,choice,manifest_sha256,policy_sha256,already_absent)
 SELECT i.kind,i.source_id,i.stream_id,i.bucket,i.object_key,i.size_bytes,i.sha256,$4,i.choice,i.manifest_sha256,i.policy_sha256,false
 FROM r2_nonraw_intents i WHERE i.bucket=$1 AND i.object_key=$2 AND i.manifest_sha256=$3 ON CONFLICT DO NOTHING`,
		a.store.Bucket(), e.Key, manifest, t.reason()); err != nil {
		return err
	}
	var receipt bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM r2_object_retirements r JOIN r2_nonraw_intents i
 ON r.bucket=i.bucket AND r.object_key=i.object_key WHERE i.bucket=$1 AND i.object_key=$2
 AND r.kind=i.kind AND r.kind=$6 AND r.reason=$3 AND r.source_id=$4 AND r.manifest_sha256=$5)`,
		a.store.Bucket(), e.Key, t.reason(), sourceID, manifest, string(t.Kind)).Scan(&receipt); err != nil {
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

// deleteNonraw runs one collated key under the same admission and send gates as
// delete.
func (a *inverseApplier) deleteNonraw(ctx, admit context.Context, send *inverseSendGate, e inverseEntry) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, inverseNetworkTimeout)
	defer cancel()
	if admit.Err() != nil {
		return false, errInverseAdmissionClosed
	}
	reserving, stopReserving := context.WithCancel(ctx)
	defer context.AfterFunc(admit, stopReserving)()
	t, err := a.reserveNonraw(reserving, e)
	stopReserving()
	if err != nil {
		if admit.Err() != nil && errors.Is(err, context.Canceled) {
			return false, errInverseAdmissionClosed
		}
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
	authority, err := a.authorizeNonraw(ctx, tx, e, send)
	if err != nil {
		return false, err
	}
	if send.isClosed() {
		return false, errInverseAdmissionClosed
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
	if err = a.publishNonrawAbsent(ctx, e, t, a.opts.ManifestSHA); err != nil {
		return false, err
	}
	return true, nil
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
		// One lock path: the same RAW row locks the single-key RAW wire takes,
		// over the whole source closure, held by this transaction across the final
		// check and the DELETE. A purge or cleanup source hold therefore cannot
		// commit after authorization.
		ids := make([]int64, 0, len(loaded.Sources))
		for _, src := range loaded.Sources {
			ids = append(ids, src.ID)
		}
		if err = lockInverseRawSourceClosure(ctx, tx, ids); err != nil {
			return err
		}
		t = loaded
		return nil
	})
	return t, err
}

func (a *inverseApplier) authorizeNonraw(ctx context.Context, tx pgx.Tx, e inverseEntry, gate *inverseSendGate) (*inverseNonrawAuthority, error) {
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
	// A collated output has NO catalog ETag column, so a dedicated contract is
	// used instead of passing a stand-in into the RAW catalog-ETag slot.
	if err = r2RetireValidateCollatedGuardIdentity(head, e.ETag, e.SizeBytes, t.SHA); err != nil {
		return nil, err
	}
	// Pre-fence cutoff on the listing AND the final HEAD. No seven-day age is
	// invented from the owner's pinned scope.
	// HEAD's Last-Modified is whole seconds; R2 floors the listing time.
	if !head.LastModified.Equal(e.LastModified.Truncate(time.Second)) || !inverseLastModifiedAllowed(e.LastModified, a.summary.CreatedAt, now) {
		return nil, inverseLastModifiedError{}
	}
	if err = a.checkMultipart(ctx, tx); err != nil {
		return nil, err
	}
	tag, err := a.pool.Exec(ctx, `INSERT INTO r2_nonraw_attempts(bucket,object_key) VALUES($1,$2) ON CONFLICT DO NOTHING`, binding.Bucket, binding.Key)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() != 1 {
		return nil, errors.New("nonraw wire authority was already issued; reconcile only")
	}
	return &inverseNonrawAuthority{binding: binding, applier: a, tx: tx, entry: e, used: &atomic.Bool{}, gate: gate}, nil
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
	gate    *inverseSendGate
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
	return s.deleteOnceWith(ctx, a)
}

// deleteOnceWith sends one DELETE through the ONE audited transport every lane
// shares: a fresh HTTP/1.1 connection, the batch send gate on the first request
// byte, the first-write deadline, no redirects, and a lost-response
// classification only after the connection is closed and any read has returned.
func (s *inverseSDKStore) deleteOnceWith(ctx context.Context, authority inverseWireAuthority) error {
	if err := authority.check(ctx, s.Bucket(), s.EndpointSHA256()); err != nil {
		return err
	}
	template, ok := s.transport.(*http.Transport)
	if !ok {
		return errors.New("inverse DELETE needs the HTTP/1.1 transport template")
	}
	wire := &inverseWire{gate: authority.sendGate()}
	transport := &inverseDeleteTransport{base: wire.transport(template), authority: authority, bucket: s.Bucket(), endpoint: s.EndpointSHA256(), wire: wire}
	_, err := s.sdk.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.Bucket()), Key: aws.String(authority.wireKey())}, func(o *s3.Options) {
		o.Retryer = aws.NopRetryer{}
		o.HTTPClient = &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	})
	if err != nil && wire.lostAfterClose() {
		return fmt.Errorf("%w: %w", errInverseWireAmbiguous, err)
	}
	return err
}
