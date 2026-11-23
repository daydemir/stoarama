package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Matches r2-retire's read-only catalog budget.
const r2InversePartitionStatementTimeout = "1800s"

type inverseClass string

const (
	inverseKE        inverseClass = "keep_everything"
	inverseKeep28    inverseClass = "keep28"
	inverseTools     inverseClass = "stitch_tools"
	inverseHeld      inverseClass = "held"
	inverseUnknown   inverseClass = "unknown"
	inverseDeletable inverseClass = "deletable"
	inverseDelivery  inverseClass = "delivery"
	// Collated outputs outside pinned keep28 with exactly one catalog output and
	// fully resolved sources. Eligible only for the reviewed collated lane.
	inverseCollated inverseClass = "collated"
)

var inverseClasses = []inverseClass{inverseKE, inverseKeep28, inverseTools, inverseHeld, inverseUnknown, inverseDeletable, inverseDelivery, inverseCollated}

type inverseIdentity struct {
	StreamID   int64
	Path       string
	Start, End *time.Time
	Timezone   string
	Kind       string
}

// inversePageIdentity is the exact delivery page identity an exemption binds.
type inversePageIdentity struct {
	ETag      string
	SizeBytes int64
}

type inverseProtection struct {
	Identities     map[string][]inverseIdentity
	Held, Delivery map[string]bool
	// PageKeep is the exact key set of delivery_keep_holds: every link on the
	// public delivery page. No owner scope waives it; only an owner exemption
	// whose class gate is open (Exempt) does.
	PageKeep map[string]bool
	Exempt   map[string]deliveryExemptionClass
	// ExemptIdentity is each open exemption's bound page identity. A listing
	// whose ETag or size differs from it is never a candidate: the exemption
	// covers one exact object, not the key.
	ExemptIdentity map[string]inversePageIdentity
	// Retiring holds keys with a retirement reservation or receipt: retirement
	// progress, this job's own included, not an external protection input.
	Retiring map[string]bool
	// OwnerKeepPrefixes are durable owner keep holds on whole key prefixes. A
	// key under one is kept whatever its catalog status.
	OwnerKeepPrefixes []string
	// OwnerKeepStreams protects the pinned Keep Everything streams and streams
	// named by owner RAW prefixes, including footage whose key has no stream ID.
	OwnerKeepStreams map[int64]bool
	// JoinedReleased holds clips whose every joined snapshot belongs to a batch
	// the owner released to the RAW lane. Only their outside-window part leaves.
	JoinedReleased map[string]bool
	KEFolders      map[string]bool
	Windows        map[int64]api.Keep28Window
	Picks          r2RetirePicks
	// ReviewedCollated selects the explicit non-raw partition scope. It is opt-in
	// so default partition/apply output is unchanged for B1/B2.
	ReviewedCollated bool
	Collated         map[string]inverseCollatedAssessment
	// KEEpoch versions catalog identity: it advances on every change to the
	// raw and collated identities KE path discovery reads.
	KEEpoch int64
}

// holdsSHA256 digests every external classification input outside the catalog
// rows and the picks and policy the summary pins: the KE epoch, which versions
// identity changes to those rows, and the exact owner keep, KE folder, hold,
// delivery and joined release sets. It leaves out Retiring, so a job's own
// reservations and receipts never invalidate its restart partition; the live
// gate refuses every reserved or retired key anyway. Equal digests mean those
// inputs are unchanged.
func (p inverseProtection) holdsSHA256() (string, error) {
	// PageKeep, Exempt and ExemptIdentity are included because an owner
	// exemption, and the exact page identity it binds, changes the class of an
	// already listed key without touching the catalog rows or the delivery
	// closure: a restart must not reuse a partition classified under a different
	// exemption state. ReviewedCollated is included so a RAW-scope partition can
	// never be reused by the reviewed collated scope, or the reverse.
	data, err := json.Marshal([]any{p.KEEpoch, p.OwnerKeepPrefixes, p.OwnerKeepStreams, p.KEFolders, p.Held, p.Delivery,
		p.JoinedReleased, p.PageKeep, p.Exempt, p.ExemptIdentity, p.ReviewedCollated})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

type inverseEntry struct {
	r2.ObjectInfo
	Class  inverseClass `json:"class"`
	Reason string       `json:"reason"`
	// Selected is the identity keep-28 selection observed; never serialized.
	Selected keep28Identity `json:"-"`
}
type inverseSummary struct {
	Version              int                           `json:"version"`
	Bucket               string                        `json:"bucket"`
	EndpointSHA256       string                        `json:"endpoint_sha256"`
	CreatedAt            time.Time                     `json:"created_at"`
	PolicySHA256         string                        `json:"policy_sha256"`
	PicksSHA256          string                        `json:"picks_sha256"`
	Total                r2RetireStat                  `json:"total"`
	LastModifiedExcluded r2RetireStat                  `json:"last_modified_excluded"`
	Classes              map[inverseClass]r2RetireStat `json:"classes"`
	ReadOnly             bool                          `json:"read_only"`
	// ProtectionSHA256 is the classification's holdsSHA256, so a restart can
	// prove those inputs unchanged before it reuses this partition.
	ProtectionSHA256 string `json:"protection_sha256"`
}

// matches requires a partition of this bucket endpoint, policy and picks.
func (s inverseSummary) matches(store inverseLister, policy, picks string) error {
	if s.Version != 1 || s.Bucket != store.Bucket() || s.EndpointSHA256 != store.EndpointSHA256() || s.PolicySHA256 != policy || s.PicksSHA256 != picks || !s.ReadOnly {
		return errors.New("partition metadata differs")
	}
	return nil
}

// fresh requires a partition at most 24h old by the database clock now.
func (s inverseSummary) fresh(now time.Time) error {
	if now.Sub(s.CreatedAt) > 24*time.Hour || s.CreatedAt.After(now) {
		return errors.New("partition is stale or future-dated")
	}
	return nil
}

// A path alias of any KE stream remains protected even when its catalog row
// is missing. Match complete folder components, never a partial name.
// inverseToolsPrefix is the retired WORK custody tree. It is not footage.
const inverseToolsPrefix = "experimental/v85-retired-WORK-custody/"

func (p inverseProtection) classify(o r2.ObjectInfo) (inverseClass, string) {
	// Hard cut FIRST: no eligibility, delivery or hold branch can ever label a
	// tool-prefix key as footage, so all 666 stay protected regardless of what
	// the catalog or an owner scope says about them.
	if strings.HasPrefix(o.Key, inverseToolsPrefix) {
		return inverseTools, "pinned_stitch_tools"
	}
	// An owner keep prefix wins over every other answer.
	for _, prefix := range p.OwnerKeepPrefixes {
		if strings.HasPrefix(o.Key, prefix) {
			return inverseKE, "owner_keep_prefix"
		}
	}
	// Deniz, 2026-11-12: keep up only the links on the public delivery page.
	// Those exact keys stay delivery in every scope, reviewed or not, until the
	// owner exempts the key and its class gate opens (0225). The exemption binds
	// one exact identity: a listing whose ETag or size drifted from it is held.
	if p.PageKeep[o.Key] {
		if _, exempt := p.Exempt[o.Key]; !exempt {
			return inverseDelivery, "delivery_page_keep"
		}
		if id, bound := p.ExemptIdentity[o.Key]; bound && (!inverseETagMatches(o.ETag, id.ETag) || o.SizeBytes != id.SizeBytes) {
			return inverseDelivery, "delivery_exemption_identity_drift"
		}
	}
	if p.Held[o.Key] {
		return inverseHeld, "live_hold"
	}
	if p.Retiring[o.Key] {
		return inverseHeld, "retirement_recorded"
	}
	identities := p.Identities[o.Key]
	// Collated outputs and RAW clips are disjoint key namespaces. A collated key is
	// classified from the reviewed output map alone, never from a RAW identity list
	// and never with a RAW loader.
	collatedIdentity := false
	for _, id := range identities {
		if id.Kind == "collated" {
			collatedIdentity = true
		}
	}
	x, mapped := p.Collated[o.Key]
	nonraw := mapped || collatedIdentity
	// The derived delivery closure holds every RAW key in every scope. Only the
	// reviewed nonraw lane judges its own collated outputs and repairs.
	if p.Delivery[o.Key] && !(p.ReviewedCollated && nonraw) {
		return inverseDelivery, "delivery_hold_pending_owner"
	}
	for _, component := range strings.Split(o.Key, "/") {
		if p.KEFolders[component] {
			return inverseKE, "keep_everything_path"
		}
	}
	if nonraw {
		if !p.ReviewedCollated {
			return inverseHeld, "collated_deferred"
		}
		return p.classifyCollated(o, x, mapped)
	}
	for _, id := range identities {
		if p.OwnerKeepStreams[id.StreamID] {
			return inverseKE, "owner_keep_stream"
		}
		for _, component := range strings.Split(id.Path, "/") {
			if p.KEFolders[component] {
				return inverseKE, "keep_everything_catalog_path"
			}
		}
		if p.Picks.Choices[id.StreamID].Choice == "all" {
			return inverseKE, "keep_everything_stream"
		}
	}
	if len(identities) == 0 {
		return inverseUnknown, "no_catalog_identity"
	}
	// Every alias must positively establish non-keep28 identity. A single
	// overlapping alias protects the key; an uncertain alias holds it. Collated
	// keys carry one collated row plus one row per RAW source; the sources are
	// assessed by the collated path, and the output's own row is judged here so
	// the shared unknown veto is never bypassed.
	unknown := false
	for _, id := range identities {
		if p.identityUncertain(id) {
			unknown = true
			continue
		}
		w := p.Windows[id.StreamID]
		if id.Start.Before(w.End) && id.End.After(w.Start) {
			if p.JoinedReleased[o.Key] {
				return inverseHeld, "joined_source_snapshot"
			}
			return inverseKeep28, "pinned_window_overlap"
		}
	}
	if len(identities) > 1 {
		unknown = true
	}
	if unknown {
		return inverseUnknown, "uncertain_policy_or_identity"
	}
	if r2RetireCheckUngatedPrefix(o.Key) != nil {
		return inverseUnknown, "ungated_writer_prefix"
	}
	return inverseDeletable, "outside_pinned_keep28"
}

// identityUncertain is the single uncertainty predicate shared by the RAW and
// collated paths: absent pinned window, nil/invalid span, missing or mismatched
// timezone, empty path or an undecided stream policy all make an identity
// uncertain, and uncertainty always holds instead of authorizing deletion.
// sourceIdentityUncertain is the collated-source view of the same predicate, so a
// collated source and a RAW clip are judged by one rule and cannot drift.
func (p inverseProtection) sourceIdentityUncertain(s inverseCollatedSource) bool {
	if p.OwnerKeepStreams[s.StreamID] {
		return true
	}
	return p.identityUncertain(inverseIdentity{StreamID: s.StreamID, Path: s.Path, Start: s.Start, End: s.End, Timezone: s.Timezone, Kind: "source"})
}

func (p inverseProtection) identityUncertain(id inverseIdentity) bool {
	w, ok := p.Windows[id.StreamID]
	if !ok || id.Start == nil || id.End == nil || !r2RetireValidSpan(*id.Start, *id.End) ||
		id.Timezone == "" || id.Timezone != w.Timezone || id.Path == "" {
		return true
	}
	return r2RetireEffectiveChoice(p.Picks.Choices[id.StreamID], true, w) == ""
}

// inverseClassifierInputs declares, once, every catalog fact the classifier reads
// to decide eligibility, and the trigger that versions it. The KE epoch is how a
// cached batch approval notices that one of these changed, so this list and the
// epoch triggers in 0212 must agree: a new classifier input added without a
// matching entry here (and in the migration) would silently escape the freeze.
// TestR2InverseEpochCoversClassifierInputs asserts this struct against pg_trigger.
type inverseClassifierInput struct {
	Table   string
	Columns []string
	// HoldCovered lists inputs that a classifier reads but that are enforced by
	// the locked per-key reload instead of the batch epoch. purged_at is the only
	// such input: the pre-delete purge probe writes it inside the wire
	// transaction that already holds the epoch row, so versioning it there would
	// self-deadlock. Every identity query requires purged_at IS NULL, so a purge
	// HOLDs the key. ROOT's rule is satisfied by either branch.
	HoldCovered []string
}

// inverseClassifierQueries are the identity queries whose column reads the frozen
// input set must cover, together with the alias→table mapping they use. A test
// parses these and fails if a referenced column is not declared as an epoch- or
// hold-covered input, so a column added to a classifier identity query later
// cannot silently escape the freeze. Live hold queries are deliberately excluded:
// they are per-key vetoes re-evaluated under lock at every wire boundary, not
// batch-frozen inputs.
var inverseClassifierQueries = []struct {
	Name    string
	SQL     string
	Aliases map[string]string
}{
	{Name: "inverseCatalogSQL", SQL: inverseCatalogSQL, Aliases: map[string]string{
		"c": "recording_clips", "r": "recordings", "s": "streams",
		"o": "recording_collation_outputs", "h": "recording_collation_hours",
	}},
	{Name: "inverseNonrawColumns", SQL: inverseNonrawColumns, Aliases: map[string]string{
		"o": "recording_collation_outputs", "r": "recordings", "s": "streams",
		"oo": "recording_collation_outputs", "rr": "recordings", "rc": "recording_clips",
	}},
	// The per-key locked reload reads the remaining target identity inputs
	// (etag, sha256, purged_at) and is scanned for the same reason.
	{Name: "inverseRawTargetSQL", SQL: inverseRawTargetSQL, Aliases: map[string]string{
		"c": "recording_clips", "r": "recordings", "s": "streams",
	}},
}

var inverseClassifierInputs = []inverseClassifierInput{
	{"recording_clips", []string{"bucket", "recording_id", "object_key", "display_path", "etag", "size_bytes", "sha256", "clip_start_at", "clip_end_at"}, []string{"purged_at"}},
	{Table: "recording_collation_outputs", Columns: []string{"object_key", "nas_relative_path", "collation_hour_id", "source_clip_ids", "sha256", "size_bytes", "start_at", "end_at", "r2_verified_at"}},
	{Table: "recording_collation_hours", Columns: []string{"recording_id"}},
	// account_id is a B2 keep28 account scope, not a collated classifier input; it
	// is enforced by the keep28 scope query and its own epoch/fence coverage.
	{Table: "recordings", Columns: []string{"stream_id", "cron_timezone"}},
	{Table: "streams", Columns: []string{"local_timezone"}},
}

const inverseCatalogSQL = `
SELECT c.object_key,coalesce(r.stream_id,0),coalesce(c.display_path,''),c.clip_start_at,c.clip_end_at,
 coalesce(nullif(s.local_timezone,''),r.cron_timezone,''),'raw'
FROM recording_clips c JOIN recordings r ON r.id=c.recording_id LEFT JOIN streams s ON s.id=r.stream_id WHERE c.bucket=$1
UNION ALL
SELECT o.object_key,coalesce(r.stream_id,0),coalesce(o.nas_relative_path,''),o.start_at,o.end_at,
 coalesce(nullif(s.local_timezone,''),r.cron_timezone,''),'collated'
FROM recording_collation_outputs o JOIN recording_collation_hours h ON h.id=o.collation_hour_id
JOIN recordings r ON r.id=h.recording_id LEFT JOIN streams s ON s.id=r.stream_id
UNION ALL
SELECT o.object_key,coalesce(r.stream_id,0),coalesce(c.display_path,''),c.clip_start_at,c.clip_end_at,
 coalesce(nullif(s.local_timezone,''),r.cron_timezone,''),'source'
FROM recording_collation_outputs o LEFT JOIN LATERAL unnest(o.source_clip_ids) source_id ON true
LEFT JOIN recording_clips c ON c.id=source_id LEFT JOIN recordings r ON r.id=c.recording_id
LEFT JOIN streams s ON s.id=r.stream_id`

// Use the actual hold tables, without interpreting a NAS inventory row as a
// hold release. Retired plans that ever issued a delete retain source holds.
const inverseHeldSQL = `
WITH held_sources AS (
 SELECT kind,source_id FROM nas_cleanup_reserved_files
 UNION SELECT kind,source_id FROM nas_cleanup_incident_files
 UNION SELECT 'raw',clip_id FROM nas_cleanup_incident_sources
 UNION SELECT 'raw',h.clip_id FROM nas_cleanup_source_holds h JOIN nas_cleanup_plans p ON p.id=h.plan_id
 WHERE p.mode<>'retired' OR EXISTS(SELECT 1 FROM nas_cleanup_items i WHERE i.plan_id=p.id AND (i.delete_ever_issued OR i.state IN ('delete_issued','unverified','media_deleted_sidecar_failed')))
), held_keys AS (
 SELECT bucket,object_key FROM nas_cleanup_incident_roster
 UNION SELECT c.bucket,c.object_key FROM held_sources h JOIN recording_clips c ON h.kind='raw' AND c.id=h.source_id
 UNION SELECT $1,o.object_key FROM held_sources h JOIN recording_collation_outputs o ON h.kind='collated' AND o.id=h.source_id
 UNION SELECT $1,o.object_key FROM recording_collation_outputs o WHERE EXISTS(SELECT 1 FROM nas_cleanup_incident_sources i WHERE i.clip_id=ANY(o.source_clip_ids))
 UNION SELECT c.bucket,c.object_key FROM recording_clips c JOIN nas_restore_requests r ON r.clip_id=c.id WHERE r.state IN ('pending','leased')
 UNION SELECT c.bucket,c.object_key FROM recording_clips c JOIN recording_joined_source_snapshots s ON s.clip_id=c.id
 WHERE NOT r2_joined_source_released(c.id)
 UNION SELECT c.bucket,c.object_key FROM recording_clips c JOIN recording_joined_snapshot_scopes s
 ON s.recording_id=c.recording_id AND s.recording_job_id=c.recording_job_id AND c.id<=s.high_water_clip_id
 JOIN recording_joined_batches b ON b.id=s.batch_record_id AND b.state='snapshotting'
 UNION SELECT c.bucket,c.object_key FROM recording_clips c JOIN recording_joined_dry_run_scopes s
 ON s.recording_id=c.recording_id AND s.recording_job_id=c.recording_job_id AND c.id<=s.high_water_clip_id
 WHERE NOT r2_joined_source_released(c.id)
 UNION SELECT item->>'bucket',item->>'object_key' FROM nas_r2_copy_items WHERE state<>'verified' OR blocked_reason<>''
 UNION SELECT bucket,object_key FROM nas_stitch_r2_input_holds WHERE released_at IS NULL
) SELECT object_key FROM held_keys WHERE bucket=$1`
const inversePageKeepSQL = `SELECT object_key FROM delivery_keep_holds WHERE bucket=$1`
const inverseExemptSQL = `SELECT x.object_key,x.class::text,h.etag,h.size_bytes FROM r2_delivery_exemptions x
 JOIN delivery_keep_holds h ON h.bucket=x.bucket AND h.object_key=x.object_key
 WHERE x.bucket=$1 AND r2_delivery_exempt(x.bucket,x.object_key)`
const inverseRetiringSQL = `SELECT object_key FROM r2_retirement_reservations WHERE bucket=$1
UNION SELECT object_key FROM r2_object_retirements WHERE bucket=$1`
const inverseJoinedReleasedSQL = `SELECT DISTINCT c.object_key FROM recording_joined_source_snapshots s JOIN recording_clips c ON c.id=s.clip_id
WHERE c.bucket=$1 AND r2_joined_source_released(c.id)`
const inverseDeliverySQL = `
WITH delivery_outputs AS (
 SELECT o.object_key,o.source_clip_ids FROM recording_collation_outputs o JOIN recording_collation_hours h ON h.id=o.collation_hour_id
 WHERE EXISTS(SELECT 1 FROM delivery_keep_output_dependencies d WHERE d.output_id=o.id AND d.recording_id=h.recording_id)
 OR EXISTS(SELECT 1 FROM delivery_nas_source_holds d WHERE d.clip_id=ANY(o.source_clip_ids))
 OR EXISTS(SELECT 1 FROM delivery_keep_holds d WHERE d.object_key=o.object_key)
 OR EXISTS(SELECT 1 FROM delivery_keep_holds d WHERE d.relative_path<>'' AND d.relative_path IN(o.nas_relative_path,regexp_replace(o.nas_relative_path,'^joined/','')))
 OR EXISTS(SELECT 1 FROM delivery_keep_holds d WHERE d.sha256<>'' AND d.sha256=o.sha256 AND d.size_bytes=o.size_bytes)
)
SELECT object_key FROM delivery_keep_holds WHERE bucket=$1
UNION SELECT c.object_key FROM recording_clips c JOIN delivery_nas_source_holds d ON d.clip_id=c.id WHERE c.bucket=$1
UNION SELECT c.object_key FROM recording_clips c JOIN delivery_keep_holds d ON d.bucket=c.bucket AND d.object_key=c.object_key WHERE c.bucket=$1
UNION SELECT c.object_key FROM recording_clips c JOIN delivery_keep_holds d ON d.relative_path<>'' AND d.relative_path IN(c.display_path,regexp_replace(c.display_path,'^joined/','')) WHERE c.bucket=$1
UNION SELECT c.object_key FROM recording_clips c JOIN delivery_keep_holds d ON d.sha256<>'' AND d.sha256=lower(c.sha256) AND d.size_bytes=c.size_bytes WHERE c.bucket=$1
UNION SELECT object_key FROM delivery_outputs
UNION SELECT c.object_key FROM delivery_outputs o CROSS JOIN LATERAL unnest(o.source_clip_ids) source_id
 JOIN recording_clips c ON c.id=source_id WHERE c.bucket=$1`

// loadInverseProtection loads the full catalog identities on top of
// loadInverseHolds.
func loadInverseProtection(ctx context.Context, q r2RetireQuerier, bucket string, picks r2RetirePicks, windows map[int64]api.Keep28Window) (inverseProtection, error) {
	return loadInverseProtectionMode(ctx, q, bucket, picks, windows, false)
}

// loadInverseProtectionMode adds the reviewed collated assessment when the
// reviewed non-raw scope is explicitly selected, on top of the shared hold and
// exemption inputs loadInverseHolds reads. Default callers keep the exact RAW
// classification, where collated keys are deferred and every page key is a
// delivery hold.
func loadInverseProtectionMode(ctx context.Context, q r2RetireQuerier, bucket string, picks r2RetirePicks, windows map[int64]api.Keep28Window, reviewed bool) (inverseProtection, error) {
	p, err := loadInverseHolds(ctx, q, bucket, picks, windows)
	if err != nil {
		return p, err
	}
	p.ReviewedCollated = reviewed
	rows, err := q.Query(ctx, inverseCatalogSQL, bucket)
	if err != nil {
		return p, err
	}
	for rows.Next() {
		var key string
		var id inverseIdentity
		if err = rows.Scan(&key, &id.StreamID, &id.Path, &id.Start, &id.End, &id.Timezone, &id.Kind); err != nil {
			rows.Close()
			return p, err
		}
		if key == "" {
			continue
		}
		p.Identities[key] = append(p.Identities[key], id)

	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return p, err
	}
	if reviewed {
		p.Collated, err = loadInverseCollatedTargets(ctx, q, bucket, p)
		if err != nil {
			return p, err
		}
	}
	return p, nil
}

// loadInverseHolds loads every classification input except the catalog rows,
// which the KE epoch versions. It is cheap next to the catalog and listing.
func loadInverseHolds(ctx context.Context, q r2RetireQuerier, bucket string, picks r2RetirePicks, windows map[int64]api.Keep28Window) (inverseProtection, error) {
	p := inverseProtection{Identities: map[string][]inverseIdentity{}, Held: map[string]bool{}, Delivery: map[string]bool{},
		PageKeep: map[string]bool{}, Exempt: map[string]deliveryExemptionClass{}, ExemptIdentity: map[string]inversePageIdentity{},
		Retiring: map[string]bool{}, JoinedReleased: map[string]bool{}, Windows: windows, Picks: picks}
	err := q.QueryRow(ctx, `SELECT epoch FROM r2_inverse_ke_epoch`).Scan(&p.KEEpoch)
	if err != nil {
		return p, err
	}
	if p.KEFolders, err = loadInverseKEFolders(ctx, q, bucket, picks); err != nil {
		return p, err
	}
	if p.OwnerKeepPrefixes, err = loadInverseOwnerKeepPrefixes(ctx, q, bucket); err != nil {
		return p, err
	}
	if p.OwnerKeepStreams, err = loadInverseOwnerKeepStreams(ctx, q, bucket, p.OwnerKeepPrefixes); err != nil {
		return p, err
	}
	if p.Exempt, p.ExemptIdentity, err = loadInverseExempt(ctx, q, bucket); err != nil {
		return p, err
	}
	if err = loadInversePageKeep(ctx, q, bucket, p.PageKeep); err != nil {
		return p, err
	}
	for _, set := range []struct {
		sql  string
		keys map[string]bool
	}{{inverseHeldSQL, p.Held}, {inverseDeliverySQL, p.Delivery}, {inverseRetiringSQL, p.Retiring}, {inverseJoinedReleasedSQL, p.JoinedReleased}} {
		rows, err := q.Query(ctx, set.sql, bucket)
		if err != nil {
			return p, err
		}
		for rows.Next() {
			var key string
			if err = rows.Scan(&key); err != nil {
				rows.Close()
				return p, err
			}
			if key == "" {
				rows.Close()
				return p, errors.New("hold has an empty object key")
			}
			set.keys[key] = true
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return p, err
		}
	}
	return p, nil
}

// loadInverseExempt reads the page keys whose owner exemption is open now, with
// the exact page identity each exemption binds.
func loadInverseExempt(ctx context.Context, q r2RetireQuerier, bucket string) (map[string]deliveryExemptionClass, map[string]inversePageIdentity, error) {
	rows, err := q.Query(ctx, inverseExemptSQL, bucket)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	out, identities := map[string]deliveryExemptionClass{}, map[string]inversePageIdentity{}
	for rows.Next() {
		var key, class, etag string
		var size int64
		if err = rows.Scan(&key, &class, &etag, &size); err != nil {
			return nil, nil, err
		}
		out[key] = deliveryExemptionClass(class)
		// The bound identity is the page row's exact stored ETag. It is never
		// normalized: a stored quoted or padded value is a different identity
		// from every canonical listing, so the key stays held rather than being
		// trimmed into a match.
		identities[key] = inversePageIdentity{ETag: etag, SizeBytes: size}
	}
	return out, identities, rows.Err()
}

// loadInversePageKeep reads the exact key set of delivery_keep_holds: every link
// on the public delivery page.
func loadInversePageKeep(ctx context.Context, q r2RetireQuerier, bucket string, into map[string]bool) error {
	rows, err := q.Query(ctx, inversePageKeepSQL, bucket)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		if err = rows.Scan(&key); err != nil {
			return err
		}
		if key == "" {
			return errors.New("delivery page hold has an empty object key")
		}
		into[key] = true
	}
	return rows.Err()
}

type inverseLister interface {
	Bucket() string
	EndpointSHA256() string
	ListPrefix(context.Context, string, int, func(r2.ObjectInfo) error) error
}

func partitionInverse(ctx context.Context, store inverseLister, p inverseProtection, policy string, out io.Writer) (inverseSummary, error) {
	sum := inverseSummary{Version: 1, Bucket: store.Bucket(), EndpointSHA256: store.EndpointSHA256(), CreatedAt: time.Now().UTC(), PolicySHA256: policy, PicksSHA256: p.Picks.SHA256, ReadOnly: true, Classes: map[inverseClass]r2RetireStat{}}
	for _, class := range inverseClasses {
		sum.Classes[class] = r2RetireStat{}
	}
	var err error
	if sum.ProtectionSHA256, err = p.holdsSHA256(); err != nil {
		return sum, err
	}
	encoder := json.NewEncoder(out)
	// R2 can return a key after its descendants (X.json.sig before X.json).
	// Track identity independently of order, without retaining every key string.
	// A 128-bit SHA-256 prefix collision also fails closed, never drops an entry.
	seen := map[[16]byte]struct{}{}
	previous := ""
	err = store.ListPrefix(ctx, "", 0, func(o r2.ObjectInfo) error {
		digest := sha256.Sum256([]byte(o.Key))
		identity := [16]byte(digest[:16])
		_, duplicate := seen[identity]
		predicate := ""
		switch {
		case o.Key == "":
			predicate = "empty_key"
		case duplicate:
			predicate = "duplicate_key"
		case o.SizeBytes < 0:
			predicate = "negative_size"
		}
		if predicate != "" {
			return fmt.Errorf("invalid full-bucket listing: predicate=%s previous=%q offending=%q size=%d", predicate, previous, o.Key, o.SizeBytes)
		}
		seen[identity] = struct{}{}
		previous = o.Key
		class, reason := p.classify(o)
		if err := encoder.Encode(inverseEntry{ObjectInfo: o, Class: class, Reason: reason}); err != nil {
			return err
		}
		stat := sum.Classes[class]
		stat.add(o.SizeBytes)
		sum.Classes[class] = stat
		sum.Total.add(o.SizeBytes)
		if !inverseLastModifiedAllowed(o.LastModified, sum.CreatedAt, sum.CreatedAt) {
			sum.LastModifiedExcluded.add(o.SizeBytes)
		}
		return nil
	})
	if err != nil {
		return sum, err
	}
	var check r2RetireStat
	for _, stat := range sum.Classes {
		check.Objects += stat.Objects
		check.Bytes += stat.Bytes
	}
	if check != sum.Total {
		return sum, errors.New("partition does not reconcile")
	}
	return sum, nil
}

// beginInversePartitionTx opens the read-only catalog snapshot the partition
// classifies against, with a budget that fits the full protection load instead
// of the DB role's default statement_timeout.
func beginInversePartitionTx(ctx context.Context, pool *pgxpool.Pool) (pgx.Tx, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, "SET LOCAL statement_timeout = '"+r2InversePartitionStatementTimeout+"'"); err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	}
	return tx, nil
}

func runR2Inverse(ctx context.Context, cfg config.Config, args []string) {
	if len(args) > 0 && args[0] == "refresh" {
		runKeep28RefreshControl(ctx, cfg, args[1:])
		return
	}
	if len(args) > 0 && args[0] == "apply-keep28" {
		runR2InverseLane(ctx, cfg, args[1:], inverseNASKeep28)
		return
	}
	if len(args) > 0 && args[0] == "apply" {
		runR2InverseApply(ctx, cfg, args[1:])
		return
	}
	if len(args) > 0 && args[0] == "apply-collated" {
		runR2InverseLane(ctx, cfg, args[1:], inverseNonrawCollated)
		return
	}
	if len(args) > 0 && args[0] == "job" {
		runR2InverseJob(ctx, cfg, args[1:], inverseOutsideKeep28)
		return
	}
	if len(args) > 0 && args[0] == "job-keep28" {
		runR2InverseJob(ctx, cfg, args[1:], inverseNASKeep28)
		return
	}
	if len(args) > 0 && args[0] == "job-collated" {
		runR2InverseJob(ctx, cfg, args[1:], inverseNonrawCollated)
		return
	}
	if len(args) > 0 && args[0] == "exempt" {
		runR2InverseExempt(ctx, cfg, args[1:])
		return
	}
	if len(args) > 0 && args[0] == "validate" {
		runR2InverseValidate(ctx, cfg, args[1:])
		return
	}
	if len(args) > 0 && args[0] == "approve" {
		runR2InverseApprove(ctx, cfg, args[1:])
		return
	}
	const usage = "usage: stoaramactl r2-inverse partition --picks FILE --out DIR [--owner-footage-scope] (read-only full bucket partition)"
	if len(args) == 0 || args[0] != "partition" {
		log.Fatal(usage)
	}
	fs := flag.NewFlagSet("r2-inverse partition", flag.ExitOnError)
	picksPath := fs.String("picks", "", "owner picks")
	out := fs.String("out", "", "new report directory")
	ownerFootage := fs.Bool("owner-footage-scope", false, "reviewed non-raw scope: classify mapped collated footage separately from collated_deferred")
	if err := fs.Parse(args[1:]); err != nil || fs.NArg() != 0 || *picksPath == "" || *out == "" {
		log.Fatal(usage)
	}
	sum, err := writeInversePartition(ctx, cfg, *picksPath, *out, *ownerFootage)
	if err != nil {
		log.Fatal(err)
	}
	data, _ := json.MarshalIndent(sum, "", "  ")
	fmt.Println(string(data))
}

// writeInversePartition writes a read-only full-bucket partition and its
// summary into a new directory, through the job's bounded read path so a
// throttled page retries in place here exactly as it does for a job. reviewed
// selects the explicit non-raw scope; the RAW lanes and the default partition
// keep the unchanged classification.
func writeInversePartition(ctx context.Context, cfg config.Config, picksPath, out string, ownerFootage bool) (inverseSummary, error) {
	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	p, policy, err := inversePartitionProtection(ctx, cfg, pool, picksPath, ownerFootage)
	if err != nil {
		return inverseSummary{}, err
	}
	// Same archived bucket, endpoint and environment config the plain client
	// used; only the read transport and retry policy differ.
	store, err := newInverseSDKStore(ctx, archiveR2Config(cfg, pool))
	if err != nil {
		return inverseSummary{}, err
	}
	return partitionInverseDir(ctx, store, p, policy, out)
}

// inversePartitionProtection reads the picks file and the policy, then loads the
// protection inputs a full-bucket partition classifies against, in one
// read-only catalog snapshot. reviewed selects the explicit non-raw scope; the
// RAW lanes and the default partition keep the unchanged classification.
func inversePartitionProtection(ctx context.Context, cfg config.Config, pool *pgxpool.Pool, picksPath string, reviewed bool) (inverseProtection, string, error) {
	picks, err := loadR2RetirePicks(picksPath)
	if err != nil {
		return inverseProtection{}, "", err
	}
	policy, windows, err := api.Keep28Policy()
	if err != nil {
		return inverseProtection{}, "", err
	}
	// Enforced read-only at PostgreSQL, independently of CLI control flow.
	tx, err := beginInversePartitionTx(ctx, pool)
	if err != nil {
		return inverseProtection{}, "", err
	}
	defer tx.Rollback(ctx)
	protection, err := loadInverseProtectionMode(ctx, tx, cfg.R2Bucket, picks, windows, reviewed)
	if err != nil {
		return inverseProtection{}, "", err
	}
	if err = tx.Commit(ctx); err != nil {
		return inverseProtection{}, "", err
	}
	return protection, policy, nil
}

// partitionInverseDir lists the whole bucket into a new directory, classified
// against p, and writes its partition.jsonl and summary.json.
func partitionInverseDir(ctx context.Context, store inverseLister, p inverseProtection, policy, out string) (inverseSummary, error) {
	if err := os.Mkdir(out, 0700); err != nil {
		return inverseSummary{}, err
	}
	f, err := os.OpenFile(filepath.Join(out, "partition.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return inverseSummary{}, err
	}
	sum, err := partitionInverse(ctx, store, p, policy, f)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return inverseSummary{}, err
	}
	data, err := json.MarshalIndent(sum, "", "  ")
	if err == nil {
		err = os.WriteFile(filepath.Join(out, "summary.json"), append(data, '\n'), 0600)
	}
	return sum, err
}

// inverseFreshPartition writes a fresh partition and requires the picks its
// listing was classified with to be the independently approved ones: the
// summary records the digest actually read, so a swapped picks file cannot ride
// along on the approved digest of the file the partition replaced.
func inverseFreshPartition(ctx context.Context, store inverseLister, p inverseProtection, policy, approvedPicks, out string) (inverseSummary, error) {
	sum, err := partitionInverseDir(ctx, store, p, policy, out)
	if err != nil {
		return sum, err
	}
	if sum.PicksSHA256 != approvedPicks {
		return sum, errors.New("partition read owner picks other than the approved digest")
	}
	return sum, nil
}

// Both full partition and bounded apply use the same KE path normalization and
// discovery, including raw source paths behind content-addressed outputs.
func loadInverseKEFolders(ctx context.Context, q r2RetireQuerier, bucket string, picks r2RetirePicks) (map[string]bool, error) {
	folders := map[string]bool{}
	var streams []int64
	for id, pick := range picks.Choices {
		if pick.Choice == "all" {
			streams = append(streams, id)
		}
	}
	if len(streams) == 0 {
		return folders, nil
	}
	rows, err := q.Query(ctx, `SELECT DISTINCT folder FROM (
 SELECT split_part(regexp_replace(c.display_path,'^joined/',''),'/',1) folder FROM recording_clips c JOIN recordings r ON r.id=c.recording_id WHERE c.bucket=$1 AND r.stream_id=ANY($2::bigint[])
 UNION SELECT split_part(regexp_replace(o.nas_relative_path,'^joined/',''),'/',1) FROM recording_collation_outputs o JOIN recording_collation_hours h ON h.id=o.collation_hour_id JOIN recordings r ON r.id=h.recording_id WHERE r.stream_id=ANY($2::bigint[])
 UNION SELECT split_part(regexp_replace(c.display_path,'^joined/',''),'/',1) FROM recording_collation_outputs o CROSS JOIN LATERAL unnest(o.source_clip_ids) source_id JOIN recording_clips c ON c.id=source_id JOIN recordings r ON r.id=c.recording_id WHERE r.stream_id=ANY($2::bigint[])) folders WHERE folder<>''`, bucket, streams)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var folder string
		if err = rows.Scan(&folder); err != nil {
			return nil, err
		}
		folders[folder] = true
	}
	return folders, rows.Err()
}

func loadInverseOwnerKeepPrefixes(ctx context.Context, q r2RetireQuerier, bucket string) ([]string, error) {
	rows, err := q.Query(ctx, `SELECT prefix FROM r2_owner_keep_prefixes WHERE bucket=$1 ORDER BY prefix`, bucket)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var prefixes []string
	for rows.Next() {
		var prefix string
		if err = rows.Scan(&prefix); err != nil {
			return nil, err
		}
		prefixes = append(prefixes, prefix)
	}
	return prefixes, rows.Err()
}

// ownerKeepPrefixStreamRE matches the one prefix shape a stream hold can take:
// the RAW key namespace for exactly one stream. Any other prefix stays a pure
// key-prefix hold, so this never widens a hold beyond what its text names.
var ownerKeepPrefixStreamRE = regexp.MustCompile(`^raw/stream/([1-9][0-9]*)/$`)

func ownerKeepStreamIDs(prefixes []string) map[int64]bool {
	streams := make(map[int64]bool)
	for _, prefix := range prefixes {
		if id, ok := ownerKeepStreamID(prefix); ok {
			streams[id] = true
		}
	}
	return streams
}

func loadInverseOwnerKeepStreams(ctx context.Context, q r2RetireQuerier, bucket string, prefixes []string) (map[int64]bool, error) {
	streams := ownerKeepStreamIDs(prefixes)
	if bucket != "stoarama" {
		return streams, nil
	}
	rows, err := q.Query(ctx, `SELECT stream_id FROM nas_media_keep20_windows WHERE keep_everything`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		streams[id] = true
	}
	return streams, rows.Err()
}

// ownerKeepStreamID is the stream a RAW owner-keep prefix names, if any.
func ownerKeepStreamID(prefix string) (int64, bool) {
	m := ownerKeepPrefixStreamRE.FindStringSubmatch(prefix)
	if m == nil {
		return 0, false
	}
	id, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil || id <= 0 {
		return 0, false
	}
	return id, true
}
