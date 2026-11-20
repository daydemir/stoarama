package main

import (
	"bufio"
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
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type inverseReader interface {
	inverseLister
	Head(context.Context, string) (r2.ObjectHead, error)
	EndpointSHA256() string
	RequireNoMultipartOutsideStitchStaging(context.Context, func(context.Context, string, string) (bool, error)) error
}
type inverseDeleteStore interface {
	inverseReader
	deleteOnce(context.Context, *inverseDeleteAuthority) error
}
type inverseDrainEvidence struct {
	Proof                  inverseDrainProof           `json:"proof"`
	Bucket                 string                      `json:"bucket"`
	EndpointSHA256         string                      `json:"endpoint_sha256"`
	FenceActivatedAt       time.Time                   `json:"fence_activated_at"`
	DrainNotBefore         time.Time                   `json:"drain_not_before"`
	ObservedAt             time.Time                   `json:"observed_at"`
	DeployedWriterCoverage string                      `json:"deployed_writer_coverage_evidence"`
	AcceptedPUTCompletion  string                      `json:"accepted_put_completion_evidence"`
	FencePolicy            *inverseFencePolicyEvidence `json:"fence_policy,omitempty"`
}
type inverseLane uint8

const (
	inverseOutsideKeep28 inverseLane = iota
	inverseNASKeep28
)

type inverseApplyOptions struct {
	Lane                                                                               inverseLane
	Manifest, ManifestSHA, Summary, SummarySHA, Picks, Ledger, DrainEvidence, DrainSHA string
	MaxItems                                                                           int
	MaxBytes, Budget24h                                                                int64
	Workers                                                                            int
}
type inverseApplySummary struct {
	// Selected counts the new keys this batch chose to work on.
	Selected      int64 `json:"selected"`
	Deleted       int64 `json:"deleted"`
	AmbiguousHeld int64 `json:"ambiguous_held"`
	// StitchSkipped counts keys a Durham stitch hold owned before reserve.
	StitchSkipped int64 `json:"stitch_skipped"`
	// HeldAbsent lists held keys, issued before this run, whose objects are gone.
	// They stay held and unretired; this only reports them.
	HeldAbsent       []string     `json:"held_absent"`
	WireAmbiguous    []string     `json:"wire_ambiguous"`
	LastModifiedHeld r2RetireStat `json:"last_modified_held"`
	// CustodyShort counts keep-28 keys skipped before any write because their
	// custody could end before a DELETE finishes; fresh custody retries them.
	CustodyShort     r2RetireStat `json:"custody_short"`
	Bytes            int64        `json:"bytes"`
	ElapsedSeconds   float64      `json:"elapsed_seconds"`
	ObjectsPerSecond float64      `json:"objects_per_second"`
}
type inverseRawTarget struct {
	ID, StreamID    int64
	Size            int64
	SHA, ETag, Path string
	Identity        inverseIdentity
}
type inverseApplier struct {
	keEpoch    int64
	pool       *pgxpool.Pool
	store      inverseReader
	opts       inverseApplyOptions
	summary    inverseSummary
	protection inverseProtection
	ledger     *inverseLedger
}

func (a *inverseApplier) checkMultipart(ctx context.Context, q r2RetireQuerier) error {
	return a.store.RequireNoMultipartOutsideStitchStaging(ctx, func(ctx context.Context, key, uploadID string) (bool, error) {
		var known bool
		err := q.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nas_stitch_r2_stage_objects
 WHERE bucket=$1 AND endpoint_sha256=$2 AND object_key=$3 AND upload_id=$4
   AND state IN ('uploading','completing'))`, a.store.Bucket(), a.store.EndpointSHA256(), key, uploadID).Scan(&known)
		return known, err
	})
}

func inverseFileSHA(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func inverseReadPinned(path, digest string, dst any) error {
	if !r2RetireLowerHex64(digest) {
		return errors.New("invalid evidence SHA256")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != digest {
		return errors.New("evidence digest changed")
	}
	return json.Unmarshal(data, dst)
}

// inverseFenceWait waits for the writer fence that admission found busy.
func (a *inverseApplier) inverseFenceWait() inverseWait {
	return inverseWait{sql: `SELECT 1 FROM r2_retirement_writer_fences WHERE bucket=$1 AND endpoint_sha256=$2 FOR SHARE`,
		args: []any{a.store.Bucket(), a.store.EndpointSHA256()}}
}

func (a *inverseApplier) checkEvidence(ctx context.Context, q r2RetireQuerier) error {
	return a.checkEvidenceLock(ctx, q, `FOR SHARE`)
}

// checkEvidenceAdmission reads the same fence under the wide protection locks,
// so it only tries the row: admission awaits a busy fence outside them.
func (a *inverseApplier) checkEvidenceAdmission(ctx context.Context, tx pgx.Tx) error {
	return inverseRowBusy(a.checkEvidenceLock(ctx, tx, `FOR SHARE NOWAIT`), a.inverseFenceWait())
}

func (a *inverseApplier) checkEvidenceLock(ctx context.Context, q r2RetireQuerier, lock string) error {
	picks, err := loadR2RetirePicks(a.opts.Picks)
	if err != nil {
		return err
	}
	if picks.SHA256 != a.summary.PicksSHA256 {
		return errors.New("owner picks changed")
	}
	var e inverseDrainEvidence
	if err = inverseReadPinned(a.opts.DrainEvidence, a.opts.DrainSHA, &e); err != nil {
		return err
	}
	var activated, deadline, now time.Time
	var blocked, recorderReplay bool
	err = q.QueryRow(ctx, `SELECT activated_at,drain_not_before,clock_timestamp(),r2_retirement_writes_blocked($1,$2),
 EXISTS(SELECT 1 FROM recordings WHERE status='active') OR EXISTS(SELECT 1 FROM recording_jobs WHERE status IN ('pending','leased'))
 FROM r2_retirement_writer_fences WHERE bucket=$1 AND endpoint_sha256=$2 `+lock, a.store.Bucket(), a.store.EndpointSHA256()).Scan(&activated, &deadline, &now, &blocked, &recorderReplay)
	if err != nil {
		return fmt.Errorf("exact 0198 writer fence required: %w", err)
	}
	if recorderReplay {
		return errors.New("R2 recorder replay is active or queued; batch cannot run")
	}
	if !blocked || !now.After(deadline) || e.Bucket != a.store.Bucket() || e.EndpointSHA256 != a.store.EndpointSHA256() || !e.FenceActivatedAt.Equal(activated) || !e.DrainNotBefore.Equal(deadline) || !e.ObservedAt.After(deadline) || e.ObservedAt.After(now) {
		return errors.New("writer drain or controller evidence missing, stale or outside exact fence")
	}
	if !activated.Truncate(time.Second).Equal(inverseRetirementCutoff()) {
		return errors.New("live fence activation differs from the owner-approved object cutoff")
	}
	if err = e.validateProof(a.opts); err != nil {
		return err
	}
	if now.Sub(a.summary.CreatedAt) > 24*time.Hour || a.summary.CreatedAt.After(now) {
		return errors.New("partition is stale or future-dated")
	}
	return nil
}
func inverseProtectionBarrier(ctx context.Context, tx pgx.Tx) error {
	// First lock: a waiting B1 transaction owns nothing a legacy mutator needs.
	_, err := tx.Exec(ctx, `SELECT epoch FROM r2_inverse_ke_epoch FOR SHARE`)
	return err
}

// inverseWait is one lock that admission found busy after the barrier.
type inverseWait struct {
	sql  string
	args []any
}

// inverseBusy reports locks other sessions own, found while admission held
// the wide protection locks. The caller rolls back, awaits each lock in its
// own transaction holding nothing else, then retries admission.
type inverseBusy []inverseWait

func (inverseBusy) Error() string { return "R2 retirement admission lock busy" }

// await waits for each busy lock in turn; begin opens every wait transaction
// exactly as the caller's admission does.
func (b inverseBusy) await(ctx context.Context, begin func(context.Context) (pgx.Tx, error)) error {
	for _, w := range b {
		tx, err := begin(ctx)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, w.sql, w.args...)
		tx.Rollback(context.Background())
		if err != nil {
			return err
		}
	}
	return nil
}

// Admission contention is bounded: a lock owned by a stuck session must fail
// the key or the batch clearly instead of retrying forever.
var (
	// A busy lock is normally free after one outside wait. The cap only stops a
	// pathological live-lock between many workers; it is not a fairness bound.
	inverseAdmissionRetries  = 200
	inverseAdmissionDeadline = 2 * time.Minute
)

// errInverseAdmissionBusy reports admission that never cleared its busy locks.
var errInverseAdmissionBusy = errors.New("R2 retirement admission stayed busy past its retry bound")

// admitInverse retries admit until it reports no busy lock, at most
// inverseAdmissionRetries times and never past inverseAdmissionDeadline.
// admit must have rolled its transaction back before it returns inverseBusy.
func admitInverse[T any](ctx context.Context, begin func(context.Context) (pgx.Tx, error), admit func() (T, error)) (T, error) {
	ctx, cancel := context.WithTimeout(ctx, inverseAdmissionDeadline)
	defer cancel()
	busyFailure := func(attempts int, busy inverseBusy, cause error) error {
		if cause == nil {
			return fmt.Errorf("%w: %d waits: %v", errInverseAdmissionBusy, attempts, busy)
		}
		return fmt.Errorf("%w: %d waits: %v: %w", errInverseAdmissionBusy, attempts, busy, cause)
	}
	for attempts := 0; ; attempts++ {
		v, err := admit()
		var busy inverseBusy
		if !errors.As(err, &busy) {
			return v, err
		}
		if attempts >= inverseAdmissionRetries {
			return v, busyFailure(attempts, busy, nil)
		}
		if err = busy.await(ctx, begin); err != nil {
			if ctx.Err() != nil {
				return v, busyFailure(attempts+1, busy, ctx.Err())
			}
			return v, busyFailure(attempts+1, busy, err)
		}
	}
}

func inverseBegin(pool *pgxpool.Pool) func(context.Context) (pgx.Tx, error) {
	return func(ctx context.Context) (pgx.Tx, error) {
		return pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	}
}

// admitWire opens the wire transaction and authorizes in it, awaiting busy
// locks outside it between attempts. The caller owns the returned transaction.
func admitWire[T any](ctx context.Context, pool *pgxpool.Pool, authorize func(pgx.Tx) (T, error)) (pgx.Tx, T, error) {
	var tx pgx.Tx
	v, err := admitInverse(ctx, inverseBegin(pool), func() (T, error) {
		var zero T
		var err error
		if tx, err = pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}); err != nil {
			return zero, err
		}
		v, err := authorize(tx)
		if errors.As(err, new(inverseBusy)) {
			tx.Rollback(context.Background())
			tx = nil
		}
		return v, err
	})
	return tx, v, err
}

// Lock order is 7343226011103, then 137,1, then the final fence
// 7343226011101. 1103 serializes executors and is queued on first, while this
// transaction still holds nothing; a joined freeze takes 137,1 exclusively and
// may then need 1103, so holding 137,1 while blocking on 1103 would close a
// cycle. 137,1 and 1101 are therefore only tried: contention returns
// inverseBusy so admission rolls everything back, awaits that lock outside,
// then retries.
func inverseProtectionLocks(ctx context.Context, tx pgx.Tx, shared bool) error {
	mode := ""
	if shared {
		mode = "_shared"
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock`+mode+`(7343226011103)`); err != nil {
		return err
	}
	tries := []struct {
		try  string
		wait inverseWait
	}{
		{`SELECT pg_try_advisory_xact_lock_shared(137,1)`, inverseWait{sql: `SELECT pg_advisory_xact_lock_shared(137,1)`}},
		{`SELECT pg_try_advisory_xact_lock` + mode + `(7343226011101)`, inverseWait{sql: `SELECT pg_advisory_xact_lock` + mode + `(7343226011101)`}},
	}
	for _, t := range tries {
		var ok bool
		if err := tx.QueryRow(ctx, t.try).Scan(&ok); err != nil {
			return err
		}
		if !ok {
			return inverseBusy{t.wait}
		}
	}
	return nil
}

// lockInverseCopyKey takes the key's copy lock without waiting.
func lockInverseCopyKey(ctx context.Context, tx pgx.Tx, bucket, key string) error {
	var ok bool
	if err := tx.QueryRow(ctx, `SELECT lock_nas_r2_copy_keys(ARRAY[$1::text],ARRAY[$2::text],false)`, bucket, key).Scan(&ok); err != nil {
		return err
	}
	if !ok {
		return inverseBusy{{`SELECT lock_nas_r2_copy_key($1,$2)`, []any{bucket, key}}}
	}
	return nil
}

// inverseRowBusy turns a NOWAIT miss into a wait for that row.
func inverseRowBusy(err error, wait inverseWait) error {
	var pg *pgconn.PgError
	if errors.As(err, &pg) && pg.Code == "55P03" {
		return inverseBusy{wait}
	}
	return err
}

// The parent recording needs no row lock: 0201's identity trigger and the
// epoch barrier refuse identity changes, and the locked clip blocks deletion.
const inverseRawTargetSQL = `SELECT c.id,coalesce(r.stream_id,0),c.size_bytes,lower(c.sha256),coalesce(c.etag,''),c.display_path,c.clip_start_at,c.clip_end_at,
 coalesce(nullif(s.local_timezone,''),r.cron_timezone,'') FROM recording_clips c JOIN recordings r ON r.id=c.recording_id
 LEFT JOIN streams s ON s.id=r.stream_id WHERE c.bucket=$1 AND c.object_key=$2 AND c.purged_at IS NULL AND NOT EXISTS(SELECT 1 FROM recording_collation_outputs o WHERE o.object_key=c.object_key) FOR UPDATE OF c NOWAIT`

var errInverseRawIneligible = errors.New("raw target ineligible")

// errInverseStitchConflict reports a key that a Durham stitch hold owns or is
// admitting, found before reserve wrote anything durable. The batch skips it.
var errInverseStitchConflict = errors.New("R2 key held by Durham stitching; skipped")

// 0223's ledger trigger raises exactly these for a held or admitting key.
var inverseStitchLedgerOutcomes = []pgconn.PgError{
	{Code: "55000", Message: "active R2 stitch input hold"},
	{Code: "55P03", Message: "stitch input hold admission is busy"},
}

const inverseStitchHeldSQL = `EXISTS(SELECT 1 FROM nas_stitch_r2_input_holds WHERE bucket=$1 AND object_key=$2 AND released_at IS NULL)`

func inverseStitchLedgerError(err error) error {
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		for _, outcome := range inverseStitchLedgerOutcomes {
			if pg.Code == outcome.Code && pg.Message == outcome.Message {
				return fmt.Errorf("%w: %s", errInverseStitchConflict, pg.Message)
			}
		}
	}
	return err
}

// keyContention rolls back a reserve that found its copy key or source row
// busy, then names the owner: a stitch hold or hold admission is skipped, any
// other owner is awaited.
func (a *inverseApplier) keyContention(ctx context.Context, tx pgx.Tx, key string, err error) error {
	var busy inverseBusy
	if !errors.As(err, &busy) {
		return err
	}
	if err = tx.Rollback(ctx); err != nil {
		return err
	}
	var held, admitting bool
	err = a.pool.QueryRow(ctx, `SELECT `+inverseStitchHeldSQL+`,NOT pg_try_advisory_xact_lock_shared(nas_stitch_r2_hold_lock($1,$2))`,
		a.store.Bucket(), key).Scan(&held, &admitting)
	switch {
	case err != nil:
		return err
	case held:
		return fmt.Errorf("%w: committed hold", errInverseStitchConflict)
	case admitting:
		return fmt.Errorf("%w: hold admission in progress", errInverseStitchConflict)
	}
	return busy
}

func (a *inverseApplier) loadTarget(ctx context.Context, tx pgx.Tx, e inverseEntry) (inverseRawTarget, error) {
	return loadInverseRawTarget(ctx, tx, a.store.Bucket(), a.protection, a.opts.Lane, e)
}
func loadInverseRawTarget(ctx context.Context, tx pgx.Tx, bucket string, protection inverseProtection, lane inverseLane, e inverseEntry) (inverseRawTarget, error) {
	t := inverseRawTarget{}
	clip := inverseWait{`SELECT 1 FROM recording_clips WHERE bucket=$1 AND object_key=$2 FOR UPDATE`, []any{bucket, e.Key}}
	rows, err := tx.Query(ctx, inverseRawTargetSQL, bucket, e.Key)
	if err != nil {
		return t, inverseRowBusy(err, clip)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		n++
		if err = rows.Scan(&t.ID, &t.StreamID, &t.Size, &t.SHA, &t.ETag, &t.Path, &t.Identity.Start, &t.Identity.End, &t.Identity.Timezone); err != nil {
			return t, err
		}
	}
	if err = rows.Err(); err != nil {
		return t, inverseRowBusy(err, clip)
	}
	if n != 1 || t.ID <= 0 || t.Size <= 0 || t.Size != e.SizeBytes || !r2RetireLowerHex64(t.SHA) || strings.Trim(t.ETag, `"`) == "" || strings.Trim(t.ETag, `"`) != strings.Trim(e.ETag, `"`) || !strings.HasPrefix(e.Key, "managed/") {
		return t, fmt.Errorf("%w: raw catalog identity absent, ambiguous or changed", errInverseRawIneligible)
	}
	t.Identity.StreamID = t.StreamID
	t.Identity.Path = t.Path
	t.Identity.Kind = "raw"
	// Use a copy: an old snapshot never becomes wire authority for current identity.
	p := protection
	p.Identities = map[string][]inverseIdentity{e.Key: {t.Identity}}
	p.Held = map[string]bool{}
	p.Delivery = map[string]bool{}
	want := inverseDeletable
	if lane == inverseNASKeep28 {
		want = inverseKeep28
	}
	if class, _ := p.classify(e.ObjectInfo); class != want {
		return t, fmt.Errorf("%w: target is no longer in the requested raw lane", errInverseRawIneligible)
	}
	if lane == inverseNASKeep28 {
		var scoped bool
		if err = tx.QueryRow(ctx, `SELECT r.account_id=47 AND EXISTS(SELECT 1 FROM connections WHERE id=13 AND account_id=47 AND kind='nas_pull') FROM recording_clips c JOIN recordings r ON r.id=c.recording_id WHERE c.id=$1`, t.ID).Scan(&scoped); err != nil {
			return t, err
		}
		choice := protection.Picks.Choices[t.StreamID].Choice
		if !scoped || (choice != "none" && choice != "st" && choice != "keep") || r2RetireCheckUngatedPrefix(e.Key) != nil {
			return t, fmt.Errorf("%w: keep28 scope or owner choice is unknown", errInverseRawIneligible)
		}
	}
	return t, nil
}

const inverseRawHeldSQL = `SELECT
 nas_cleanup_file_reserved('raw',$1)
 OR EXISTS(SELECT 1 FROM nas_cleanup_source_holds h JOIN nas_cleanup_plans p ON p.id=h.plan_id WHERE h.clip_id=$1 AND (p.mode<>'retired' OR EXISTS(SELECT 1 FROM nas_cleanup_items i WHERE i.plan_id=p.id AND (i.delete_ever_issued OR i.state IN ('delete_issued','unverified','media_deleted_sidecar_failed')))))
 OR EXISTS(SELECT 1 FROM nas_cleanup_incident_sources WHERE clip_id=$1)
 OR EXISTS(SELECT 1 FROM nas_cleanup_incident_roster WHERE bucket=$2 AND object_key=$3)
 OR EXISTS(SELECT 1 FROM r2_retirement_reservations WHERE bucket=$2 AND object_key=$3 AND NOT (kind='raw' AND source_id=$1 AND manifest_sha256=$4))
 OR EXISTS(SELECT 1 FROM r2_object_retirements WHERE bucket=$2 AND object_key=$3)
 OR EXISTS(SELECT 1 FROM nas_stitch_r2_input_holds WHERE bucket=$2 AND object_key=$3 AND released_at IS NULL)
 OR EXISTS(SELECT 1 FROM nas_r2_copy_items WHERE (state<>'verified' OR blocked_reason<>'') AND item->>'bucket'=$2 AND item->>'object_key'=$3)
 OR r2_inverse_delivery_held($1,$2,$3,$5,$6,$7)
 OR r2_owner_keep_held($2,$3)
 OR EXISTS(SELECT 1 FROM nas_restore_requests WHERE clip_id=$1 AND state IN ('pending','leased'))
 OR (EXISTS(SELECT 1 FROM recording_joined_source_snapshots WHERE clip_id=$1) AND NOT ($8 AND r2_joined_source_released($1)))
 OR EXISTS(SELECT 1 FROM recording_joined_snapshot_scopes s JOIN recording_joined_batches b ON b.id=s.batch_record_id AND b.state='snapshotting' JOIN recording_clips c ON c.id=$1 WHERE s.recording_id=c.recording_id AND s.recording_job_id=c.recording_job_id AND c.id<=s.high_water_clip_id)
 OR (EXISTS(SELECT 1 FROM recording_joined_dry_run_scopes s JOIN recording_clips c ON c.id=$1 WHERE s.recording_id=c.recording_id AND s.recording_job_id=c.recording_job_id AND c.id<=s.high_water_clip_id) AND NOT ($8 AND r2_joined_source_released($1)))`

// probeInversePurge exercises every installed purge trigger before wire
// deletion; all effects are rolled back. The source row is already locked.
func probeInversePurge(ctx context.Context, tx pgx.Tx, id int64) error {
	if _, err := tx.Exec(ctx, `SAVEPOINT inverse_purge_probe`); err != nil {
		return err
	}
	_, probeErr := tx.Exec(ctx, `UPDATE recording_clips SET purged_at=clock_timestamp() WHERE id=$1`, id)
	_, rollbackErr := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT inverse_purge_probe`)
	if probeErr != nil {
		return fmt.Errorf("pre-delete purge veto: %w", probeErr)
	}
	if rollbackErr != nil {
		return rollbackErr
	}
	_, err := tx.Exec(ctx, `RELEASE SAVEPOINT inverse_purge_probe`)
	return err
}

func (a *inverseApplier) checkTarget(ctx context.Context, tx pgx.Tx, t inverseRawTarget, e inverseEntry, purgeProbe bool) error {
	if err := a.checkEvidenceAdmission(ctx, tx); err != nil {
		return err
	}
	var epoch int64
	if err := tx.QueryRow(ctx, `SELECT epoch FROM r2_inverse_ke_epoch FOR SHARE`).Scan(&epoch); err != nil {
		return err
	}
	if epoch != a.keEpoch {
		return errors.New("KE catalog identity changed; refresh partition before further wire attempts")
	}
	var held bool
	// A released joined batch frees its snapshots for the RAW lane only; the
	// keep-28 lane keeps every snapshot as a hold.
	if err := tx.QueryRow(ctx, inverseRawHeldSQL, t.ID, a.store.Bucket(), e.Key, a.opts.ManifestSHA, t.Path, t.SHA, t.Size, a.opts.Lane == inverseOutsideKeep28).Scan(&held); err != nil {
		return err
	}
	if held {
		return errors.New("live key protection veto")
	}
	if purgeProbe {
		return probeInversePurge(ctx, tx, t.ID)
	}
	return nil
}
func (a *inverseApplier) reserve(ctx context.Context, e inverseEntry) (inverseRawTarget, error) {
	return admitInverse(ctx, inverseBegin(a.pool), func() (inverseRawTarget, error) { return a.reserveOnce(ctx, e) })
}
func (a *inverseApplier) reserveOnce(ctx context.Context, e inverseEntry) (inverseRawTarget, error) {
	t := inverseRawTarget{}
	tx, err := a.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return t, err
	}
	defer tx.Rollback(context.Background())
	if err = inverseProtectionBarrier(ctx, tx); err != nil {
		return t, err
	}
	if err = inverseProtectionLocks(ctx, tx, false); err != nil {
		return t, err
	}
	if err = lockInverseCopyKey(ctx, tx, a.store.Bucket(), e.Key); err != nil {
		return t, a.keyContention(ctx, tx, e.Key, err)
	}
	if t, err = a.loadTarget(ctx, tx, e); err != nil {
		return t, a.keyContention(ctx, tx, e.Key, err)
	}
	var held bool
	if err = tx.QueryRow(ctx, `SELECT `+inverseStitchHeldSQL, a.store.Bucket(), e.Key).Scan(&held); err != nil {
		return t, err
	}
	if held {
		return t, fmt.Errorf("%w: committed hold", errInverseStitchConflict)
	}
	// The purge probe runs after this reservation's intent exists, below: a
	// released joined source admits its purge only for the RAW lane's intent.
	if err = a.checkTarget(ctx, tx, t, e, false); err != nil {
		return t, err
	}
	var custody *keep28Custody
	if a.opts.Lane == inverseNASKeep28 {
		c, err := a.lockKeep28Custody(ctx, tx, t)
		aged := errors.Is(err, errR2RetireNASProofAged)
		if err != nil && !aged {
			return t, err
		}
		// The identity selection observed (source, catalog path, size, media
		// and sidecar digests) must still be the locked one, whatever the
		// proof's age: any change is a hard veto, never a skip.
		if c.identity() != e.Selected {
			return t, errors.New("keep-28 source or custody identity changed since selection")
		}
		if aged {
			// Exact custody that aged out while the key waited is no failure.
			return t, errKeep28CustodyShort
		}
		// Admit only a key that can finish inside custody: one network timeout
		// must fit before the margin-early custody end. Nothing is written yet.
		clock, err := keep28Clock(ctx, tx)
		if err != nil {
			return t, err
		}
		if clock.deadline(c.Expiry).Before(time.Now().Add(inverseNetworkTimeout)) {
			return t, errKeep28CustodyShort
		}
		custody = &c
	}
	var charged int64
	err = tx.QueryRow(ctx, `SELECT coalesce(sum(charged_bytes),0) FROM r2_inverse_budget_minutes WHERE bucket=$1 AND minute>=date_trunc('minute',clock_timestamp()-interval '24 hours')`, a.store.Bucket()).Scan(&charged)
	if err != nil {
		return t, err
	}
	if a.opts.Budget24h > 0 && e.SizeBytes > a.opts.Budget24h-charged {
		return t, errors.New("rolling 24h inverse budget exhausted")
	}
	// Independent committed transaction. Never use an outer network transaction's
	// connection or assume its rollback can discard this source reservation.
	_, err = tx.Exec(ctx, `INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256) VALUES('raw',$1,$2,$3,$4,$5,$6)`, t.ID, a.store.Bucket(), e.Key, e.SizeBytes, t.SHA, a.opts.ManifestSHA)
	if err != nil {
		return t, inverseStitchLedgerError(err)
	}
	intentSQL := `INSERT INTO ` + a.opts.Lane.intentTable() + `(bucket,object_key,source_id,size_bytes,etag,last_modified,manifest_sha256,policy_sha256,picks_sha256,drain_evidence_sha256,choice,endpoint_sha256,stream_id`
	intentArgs := []any{a.store.Bucket(), e.Key, t.ID, e.SizeBytes, e.ETag, e.LastModified, a.opts.ManifestSHA, a.summary.PolicySHA256, a.summary.PicksSHA256, a.opts.DrainSHA, a.protection.Picks.Choices[t.StreamID].Choice, a.store.EndpointSHA256(), t.StreamID}
	if custody != nil {
		data, marshalErr := json.Marshal(custody)
		if marshalErr != nil {
			return t, marshalErr
		}
		intentSQL += `,custody`
		intentArgs = append(intentArgs, data)
	}
	intentSQL += `) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13`
	if custody != nil {
		intentSQL += `,$14`
	}
	intentSQL += `)`
	_, err = tx.Exec(ctx, intentSQL, intentArgs...)
	if err != nil {
		return t, inverseStitchLedgerError(err)
	}
	if err = probeInversePurge(ctx, tx, t.ID); err != nil {
		return t, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO r2_inverse_budget_minutes(bucket,minute,charged_bytes) VALUES($1,date_trunc('minute',clock_timestamp()),$2) ON CONFLICT(bucket,minute) DO UPDATE SET charged_bytes=r2_inverse_budget_minutes.charged_bytes+excluded.charged_bytes`, a.store.Bucket(), e.SizeBytes)
	if err != nil {
		return t, err
	}
	return t, tx.Commit(ctx)
}
func (a *inverseApplier) record(e inverseEntry, event inverseEvent, manifest string) error {
	return a.ledger.append(inverseLedgerRecord{Event: event, Bucket: a.store.Bucket(), EndpointSHA256: a.store.EndpointSHA256(), Key: e.Key, ManifestSHA256: manifest, At: time.Now().UTC(), SizeBytes: e.SizeBytes})
}

type inverseWireGuard struct {
	a  *inverseApplier
	tx pgx.Tx
	t  inverseRawTarget
	e  inverseEntry
}

func (g *inverseWireGuard) binding() inverseDeleteBinding {
	return inverseDeleteBinding{Bucket: g.a.store.Bucket(), EndpointSHA256: g.a.store.EndpointSHA256(), Key: g.e.Key, ETag: g.e.ETag, SourceID: g.t.ID, StreamID: g.t.StreamID, SizeBytes: g.e.SizeBytes, LastModified: g.e.LastModified, ManifestSHA256: g.a.opts.ManifestSHA, PolicySHA256: g.a.summary.PolicySHA256, PicksSHA256: g.a.summary.PicksSHA256, DrainSHA256: g.a.opts.DrainSHA}
}
func (g *inverseWireGuard) check(ctx context.Context, binding inverseDeleteBinding) error {
	if binding != g.binding() {
		return errors.New("wire authority scope differs")
	}
	if err := g.a.checkTarget(ctx, g.tx, g.t, g.e, false); err != nil {
		return err
	}
	var durable bool
	err := g.tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM `+g.a.opts.Lane.intentTable()+` WHERE bucket=$1 AND object_key=$2 AND source_id=$3 AND manifest_sha256=$4 AND drain_evidence_sha256=$5 AND endpoint_sha256=$6 AND size_bytes=$7 AND etag=$8 AND stream_id=$9 AND picks_sha256=$10 AND policy_sha256=$11 AND last_modified=$12)`, binding.Bucket, binding.Key, g.t.ID, g.a.opts.ManifestSHA, g.a.opts.DrainSHA, binding.EndpointSHA256, g.e.SizeBytes, g.e.ETag, g.t.StreamID, binding.PicksSHA256, binding.PolicySHA256, binding.LastModified).Scan(&durable)
	if err != nil {
		return err
	}
	if !durable {
		return errors.New("wire intent missing")
	}
	return nil
}

const inverseNetworkTimeout = 30 * time.Second

// errKeep28CustodyShort skips a keep-28 key whose custody ends within one
// network timeout plus keep28CustodyMargin. It writes nothing durable and is
// not a batch failure: the key is retried once its custody is refreshed.
var errKeep28CustodyShort = errors.New("NAS custody too short to finish a DELETE; skipped before any write")

// errInverseAdmissionClosed reports a key that wrote nothing because a sibling
// already stopped the batch. It is never a failure of its own.
var errInverseAdmissionClosed = errors.New("batch stopped before this key was reserved")

// delete runs one key. admit closes on any sibling failure: a key not yet
// reserved, or still waiting on its reservation locks, writes nothing and stops.
// send closes on a hard failure only: a reserved key then stops before its
// first DELETE and stays held. A DELETE already sent finishes confirmation.
func (a *inverseApplier) delete(ctx, admit context.Context, send *inverseSendGate, e inverseEntry) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, inverseNetworkTimeout)
	defer cancel()
	if admit.Err() != nil {
		return false, errInverseAdmissionClosed
	}
	reserving, stopReserving := context.WithCancel(ctx)
	defer context.AfterFunc(admit, stopReserving)()
	_, err := a.reserve(reserving, e)
	stopReserving()
	if err != nil {
		// Only a wait that closed admission cancelled is not this key's own
		// failure; any gate failure is reported even if admission also closed.
		if admit.Err() != nil && errors.Is(err, context.Canceled) {
			return false, errInverseAdmissionClosed
		}
		return false, err
	}
	if err = a.record(e, inverseIntent, a.opts.ManifestSHA); err != nil {
		return false, err
	}
	var tx pgx.Tx
	defer func() {
		if tx != nil {
			tx.Rollback(context.Background())
		}
	}()
	// authorize runs under the wide protection locks, so every lock it waits on
	// after them is a NOWAIT try: admission awaits a busy one outside, holding
	// nothing else, then revalidates from a fresh transaction.
	var authority *inverseDeleteAuthority
	var authErr error
	tx, authority, authErr = admitWire(ctx, a.pool, func(tx pgx.Tx) (*inverseDeleteAuthority, error) {
		return a.authorize(ctx, tx, e, send)
	})
	if authErr != nil {
		return false, authErr
	}
	defer authority.cancel()
	if send.isClosed() {
		return false, errInverseAdmissionClosed
	}
	if err = a.store.(inverseDeleteStore).deleteOnce(authority.ctx, authority); err != nil {
		if errors.Is(err, errInverseWireAmbiguous) {
			return false, fmt.Errorf("single DELETE result ambiguous; intent retained, never retransmit: %w", err)
		}
		return false, fmt.Errorf("single DELETE failed; intent retained, never retransmit: %w", err)
	}

	// Confirmation ends at the running custody minimum, sampled again here.
	ctx, stop, err := authority.bound(authority.ctx)
	if err != nil {
		return false, err
	}
	defer stop()

	if _, err = a.store.Head(ctx, e.Key); !r2.IsNotFound(err) {
		if err == nil {
			err = errors.New("object remains after DELETE")
		}
		return false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return false, err
	}
	if err = a.publishAbsent(ctx, e, a.opts.ManifestSHA); err != nil {
		return false, err
	}
	return true, nil
}
func (a *inverseApplier) publishAbsent(ctx context.Context, e inverseEntry, manifest string) error {
	_, err := admitInverse(ctx, inverseBegin(a.pool), func() (struct{}, error) { return struct{}{}, a.publishAbsentOnce(ctx, e, manifest) })
	return err
}
func (a *inverseApplier) publishAbsentOnce(ctx context.Context, e inverseEntry, manifest string) error {
	tx, err := a.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if a.opts.Lane == inverseNASKeep28 {
		if err = inverseProtectionBarrier(ctx, tx); err != nil {
			return err
		}
	}
	if err = inverseProtectionLocks(ctx, tx, false); err != nil {
		return err
	}
	// A receipt and source publication must resolve the exact durable identity.
	// Never turn a missing/rebound source into a successful absence event.
	var sourceID int64
	exactSource := ""
	if a.opts.Lane == inverseNASKeep28 {
		exactSource = ` AND lower(c.sha256)=i.custody->>'SHA' AND c.display_path=i.custody->>'Path' AND EXISTS(SELECT 1 FROM recordings r WHERE r.id=c.recording_id AND r.stream_id=i.stream_id)`
	}
	err = tx.QueryRow(ctx, `SELECT c.id FROM `+a.opts.Lane.intentTable()+` i JOIN recording_clips c ON c.id=i.source_id
 WHERE i.bucket=$1 AND i.object_key=$2 AND i.manifest_sha256=$3 AND i.endpoint_sha256=$4
 AND c.bucket=i.bucket AND c.object_key=i.object_key AND c.size_bytes=i.size_bytes AND btrim(c.etag,'"')=btrim(i.etag,'"')
 `+exactSource+` FOR UPDATE OF c NOWAIT`, a.store.Bucket(), e.Key, manifest, a.store.EndpointSHA256()).Scan(&sourceID)
	if err != nil {
		return inverseRowBusy(fmt.Errorf("absence publication requires exact source: %w", err),
			inverseWait{`SELECT 1 FROM recording_clips WHERE bucket=$1 AND object_key=$2 FOR UPDATE`, []any{a.store.Bucket(), e.Key}})
	}
	_, err = tx.Exec(ctx, `INSERT INTO r2_object_retirements(kind,source_id,stream_id,bucket,object_key,size_bytes,sha256,reason,choice,manifest_sha256,policy_sha256,already_absent)
 SELECT 'raw',c.id,i.stream_id,c.bucket,c.object_key,c.size_bytes,lower(c.sha256),'`+a.opts.Lane.retirementReason()+`',i.choice,$3,i.policy_sha256,$4
 FROM recording_clips c JOIN recordings r ON r.id=c.recording_id JOIN `+a.opts.Lane.intentTable()+` i ON i.source_id=c.id
 WHERE i.bucket=$1 AND i.object_key=$2 AND i.manifest_sha256=$3 ON CONFLICT DO NOTHING`, a.store.Bucket(), e.Key, manifest, false)
	if err != nil {
		return err
	}
	var receipt bool
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM r2_object_retirements r JOIN `+a.opts.Lane.intentTable()+` i ON r.bucket=i.bucket AND r.object_key=i.object_key JOIN recording_clips c ON c.id=i.source_id
 WHERE i.bucket=$1 AND i.object_key=$2 AND r.kind='raw' AND r.reason='`+a.opts.Lane.retirementReason()+`' AND r.source_id=$3 AND r.stream_id=i.stream_id AND r.size_bytes=i.size_bytes AND r.sha256=lower(c.sha256) AND r.choice=i.choice AND r.manifest_sha256=$4 AND r.policy_sha256=i.policy_sha256)`, a.store.Bucket(), e.Key, sourceID, manifest).Scan(&receipt)
	if err != nil {
		return err
	}
	if !receipt {
		return errors.New("absence publication requires exact receipt")
	}
	_, err = tx.Exec(ctx, `UPDATE recording_clips c SET purged_at=clock_timestamp() FROM `+a.opts.Lane.intentTable()+` i WHERE i.bucket=$1 AND i.object_key=$2 AND i.source_id=c.id AND c.purged_at IS NULL`, a.store.Bucket(), e.Key)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO `+a.opts.Lane.eventTable()+`(bucket,object_key,event) VALUES($1,$2,'absent') ON CONFLICT DO NOTHING`, a.store.Bucket(), e.Key)
	if err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	return a.record(e, inverseAbsent, manifest)
}

func runR2InverseApply(ctx context.Context, cfg config.Config, args []string) {
	runR2InverseLane(ctx, cfg, args, inverseOutsideKeep28)
}
func runR2InverseLane(ctx context.Context, cfg config.Config, args []string, lane inverseLane) {
	fs := flag.NewFlagSet("r2-inverse apply", flag.ExitOnError)
	o := inverseApplyOptions{Lane: lane}
	fs.StringVar(&o.Manifest, "manifest", "", "partition.jsonl")
	fs.StringVar(&o.ManifestSHA, "expected-sha256", "", "partition SHA256")
	fs.StringVar(&o.Summary, "summary", "", "summary.json")
	fs.StringVar(&o.SummarySHA, "expected-summary-sha256", "", "summary SHA256")
	fs.StringVar(&o.Picks, "picks", "", "owner picks")
	fs.StringVar(&o.Ledger, "ledger", "", "private append-only key ledger")
	fs.StringVar(&o.DrainEvidence, "drain-evidence", "", "controller-reviewed accepted-PUT drain evidence")
	fs.StringVar(&o.DrainSHA, "expected-drain-evidence-sha256", "", "drain evidence SHA256")
	fs.IntVar(&o.Workers, "workers", 16, "concurrent keys (1..64, at least two DB connections remain reserved)")
	fs.IntVar(&o.MaxItems, "max-items", 100, "maximum new objects")
	fs.Int64Var(&o.MaxBytes, "max-bytes", 0, "maximum new intent bytes this run")
	fs.Int64Var(&o.Budget24h, "budget-24h-bytes", 0, "maximum charged bytes in previous24h (0 = unlimited; per-run bounds still apply)")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || o.MaxItems < 1 || o.Workers < 1 || o.Workers > 64 || o.MaxBytes <= 0 || o.Budget24h < 0 || o.Ledger == "" {
		log.Fatal("invalid bounded r2-inverse apply options")
	}
	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	store, err := newInverseSDKStore(ctx, archiveR2Config(cfg, pool))
	if err != nil {
		log.Fatal(err)
	}
	result, err := applyInverse(ctx, pool, store, o)
	data, _ := json.Marshal(result)
	fmt.Println(string(data))
	if err != nil {
		log.Fatal(err)
	}
}

// inverseWorkersFit keeps one session connection and one independent issuance
// connection available even when every worker owns a transaction or waits for
// admission.
func inverseWorkersFit(pool *pgxpool.Pool, workers int) error {
	if workers > int(pool.Config().MaxConns)-2 {
		return errors.New("workers exceed database capacity with session and issuance connections reserved")
	}
	return nil
}

func applyInverse(ctx context.Context, pool *pgxpool.Pool, store inverseReader, o inverseApplyOptions) (inverseApplySummary, error) {
	started := time.Now()
	result := inverseApplySummary{}
	if o.Lane != inverseOutsideKeep28 && o.Lane != inverseNASKeep28 {
		return result, errors.New("unknown inverse lane")
	}
	if _, ok := store.(inverseDeleteStore); !ok {
		return result, errors.New("inverse DELETE transport required")
	}
	if o.Workers < 1 || o.Workers > 64 || o.MaxItems < 1 || o.MaxBytes <= 0 || o.Budget24h < 0 {
		return result, errors.New("invalid bounded apply options")
	}
	if err := inverseWorkersFit(pool, o.Workers); err != nil {
		return result, err
	}
	var summary inverseSummary
	if err := inverseReadPinned(o.Summary, o.SummarySHA, &summary); err != nil {
		return result, err
	}
	sha, err := inverseFileSHA(o.Manifest)
	if err != nil {
		return result, err
	}
	if sha != o.ManifestSHA {
		return result, errors.New("partition digest differs")
	}
	policy, windows, err := api.Keep28Policy()
	if err != nil {
		return result, err
	}
	picks, err := loadR2RetirePicks(o.Picks)
	if err != nil {
		return result, err
	}
	if summary.Version != 1 || summary.Bucket != store.Bucket() || summary.EndpointSHA256 != store.EndpointSHA256() || summary.PolicySHA256 != policy || summary.PicksSHA256 != picks.SHA256 || !summary.ReadOnly {
		return result, errors.New("partition metadata differs")
	}
	lock, err := pool.Acquire(ctx)
	if err != nil {
		return result, err
	}
	defer lock.Release()
	var locked bool
	if err = lock.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, r2RetireAdvisoryLockKey).Scan(&locked); err != nil || !locked {
		return result, errors.New("another retirement apply is active")
	}
	defer lock.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, r2RetireAdvisoryLockKey)
	var ledger *inverseLedger
	if o.Lane == inverseNASKeep28 {
		ledger, err = openKeep28Ledger(o.Ledger)
	} else {
		ledger, err = openInverseLedger(o.Ledger)
	}
	if err != nil {
		return result, err
	}
	defer ledger.close()
	var keEpoch int64
	protection, err := admitInverse(ctx, inverseBegin(pool), func() (inverseProtection, error) {
		initTx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
		if err != nil {
			return inverseProtection{}, err
		}
		defer initTx.Rollback(context.Background())
		if err = inverseProtectionBarrier(ctx, initTx); err != nil {
			return inverseProtection{}, err
		}
		if err = inverseProtectionLocks(ctx, initTx, true); err != nil {
			return inverseProtection{}, err
		}
		if err = initTx.QueryRow(ctx, `SELECT epoch FROM r2_inverse_ke_epoch FOR SHARE`).Scan(&keEpoch); err != nil {
			return inverseProtection{}, err
		}
		protection, err := loadInverseApplyProtection(ctx, initTx, store.Bucket(), picks, windows)
		if err != nil {
			return protection, err
		}
		return protection, initTx.Commit(ctx)
	})
	if err != nil {
		return result, err
	}
	a := &inverseApplier{pool: pool, store: store, opts: o, summary: summary, protection: protection, ledger: ledger, keEpoch: keEpoch}
	if err = a.checkEvidence(ctx, pool); err != nil {
		return result, err
	}
	probeCtx, probeCancel := context.WithTimeout(ctx, inverseNetworkTimeout)
	err = a.checkMultipart(probeCtx, pool)
	probeCancel()
	if err != nil {
		return result, err
	}
	// An existing intent is NEVER resubmitted, regardless of manifest or ledger.
	// Only confirmed absence after our own issued DELETE can advance it to a
	// receipt on restart. An intent that never issued a DELETE is never retired:
	// if its object is gone, someone else removed it, so stop before any new work.
	existing := map[string]bool{}
	rows, err := pool.Query(ctx, `SELECT i.object_key,i.size_bytes,i.etag,i.last_modified,i.manifest_sha256,i.endpoint_sha256,EXISTS(SELECT 1 FROM `+a.opts.Lane.eventTable()+` e WHERE e.bucket=i.bucket AND e.object_key=i.object_key AND e.event='absent'),EXISTS(SELECT 1 FROM `+a.opts.Lane.attemptTable()+` t WHERE t.bucket=i.bucket AND t.object_key=i.object_key) FROM `+a.opts.Lane.intentTable()+` i WHERE i.bucket=$1`, store.Bucket())
	if err != nil {
		return result, err
	}
	type pending struct {
		e        inverseEntry
		manifest string
		issued   bool
	}
	var pendingItems []pending
	for rows.Next() {
		var key, etag, manifest, endpoint string
		var size int64
		var modified time.Time
		var done, issued bool
		if err = rows.Scan(&key, &size, &etag, &modified, &manifest, &endpoint, &done, &issued); err != nil {
			rows.Close()
			return result, err
		}
		if endpoint != store.EndpointSHA256() {
			rows.Close()
			return result, errors.New("durable intent belongs to a different provider endpoint")
		}
		existing[key] = true
		if done {
			e := inverseEntry{ObjectInfo: r2.ObjectInfo{Key: key, SizeBytes: size}}
			if err = a.record(e, inverseIntent, manifest); err != nil {
				rows.Close()
				return result, err
			}
			if err = a.record(e, inverseAbsent, manifest); err != nil {
				rows.Close()
				return result, err
			}
		}
		if !done {
			pendingItems = append(pendingItems, pending{inverseEntry{ObjectInfo: r2.ObjectInfo{Key: key, SizeBytes: size, ETag: etag, LastModified: modified}}, manifest, issued})
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return result, err
	}
	for _, p := range pendingItems {
		if err = a.record(p.e, inverseIntent, p.manifest); err != nil {
			return result, err
		}
		// An unresolved intent is never retransmitted and never retired here: an
		// attempt row cannot prove this executor's DELETE removed the object. A
		// key that never reached the wire must still exist; if it is gone,
		// something else deleted it. A present object must still be the one the
		// intent named. Either difference needs review.
		probeCtx, probeCancel := context.WithTimeout(ctx, inverseNetworkTimeout)
		head, err := store.Head(probeCtx, p.e.Key)
		probeCancel()
		switch {
		case r2.IsNotFound(err) && !p.issued:
			return result, fmt.Errorf("intent %s never issued a DELETE but its object is absent; held for review", p.e.Key)
		case r2.IsNotFound(err):
			result.HeldAbsent = append(result.HeldAbsent, p.e.Key)
		case err != nil:
			return result, err
		case strings.Trim(head.ETag, `"`) != strings.Trim(p.e.ETag, `"`) || head.SizeBytes != p.e.SizeBytes || !head.LastModified.Equal(p.e.LastModified.Truncate(time.Second)):
			return result, fmt.Errorf("held intent %s now names a different object; held for review", p.e.Key)
		}
		result.AmbiguousHeld++
	}
	f, err := os.Open(o.Manifest)
	if err != nil {
		return result, err
	}
	defer f.Close()
	manifestHash := sha256.New()
	scanner := bufio.NewScanner(io.TeeReader(f, manifestHash))
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	var jobs []inverseEntry
	candidates := map[string]inverseEntry{}
	var planned int64
	for scanner.Scan() {
		var e inverseEntry
		if err = json.Unmarshal(scanner.Bytes(), &e); err != nil {
			return result, err
		}
		want := inverseDeletable
		if o.Lane == inverseNASKeep28 {
			want = inverseKeep28
		}
		if e.Class != want || existing[e.Key] {
			continue
		}
		// A nonpositive size can never reserve; selecting it would abort the batch.
		if e.SizeBytes <= 0 {
			continue
		}
		if !inverseLastModifiedAllowed(e.LastModified, summary.CreatedAt, time.Now().UTC()) {
			result.LastModifiedHeld.add(e.SizeBytes)
			continue
		}
		if o.Lane == inverseNASKeep28 {
			if _, dup := candidates[e.Key]; dup {
				return result, errors.New("duplicate keep28 partition key")
			}
			candidates[e.Key] = e
			continue
		}
		if len(jobs) >= o.MaxItems || e.SizeBytes > o.MaxBytes-planned {
			continue
		}
		if !strings.HasPrefix(e.Key, "managed/") || r2RetireCheckUngatedPrefix(e.Key) != nil {
			continue
		}
		jobs = append(jobs, e)
		planned += e.SizeBytes
	}
	if err = scanner.Err(); err != nil {
		return result, err
	}
	if hex.EncodeToString(manifestHash.Sum(nil)) != o.ManifestSHA {
		return result, errors.New("partition changed while reading")
	}
	if o.Lane == inverseNASKeep28 {
		jobs, err = a.selectKeep28(ctx, candidates)
		if err != nil {
			return result, err
		}
	}
	// Any failure closes admission to new reservations. A hard failure also
	// closes the send gate, so no reserved key sends its first DELETE after it.
	// Deletes already sent finish under their own deadline; cancelling them
	// would strand DELETEs the provider may already have taken.
	stop, halt := context.WithCancel(ctx)
	defer halt()
	send := &inverseSendGate{}
	queue := make(chan inverseEntry)
	var wg sync.WaitGroup
	var first error
	var mu sync.Mutex
	var deleted, bytes atomic.Int64
	var wireAmbiguous []string
	var modifiedHeld, modifiedBytes, stitchSkipped atomic.Int64
	var custodyShort r2RetireStat
	for range o.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for e := range queue {
				ok, err := a.delete(ctx, stop, send, e)
				if ok {
					deleted.Add(1)
					bytes.Add(e.SizeBytes)
				}
				if errors.Is(err, errInverseAdmissionClosed) {
					return
				}
				// Only selection or reserve report these, before anything durable.
				if errors.Is(err, errKeep28CustodyShort) {
					mu.Lock()
					custodyShort.add(e.SizeBytes)
					mu.Unlock()
					continue
				}
				if errors.Is(err, errInverseStitchConflict) {
					stitchSkipped.Add(1)
					continue
				}
				if err != nil {
					var modifiedErr inverseLastModifiedError
					if errors.As(err, &modifiedErr) {
						modifiedHeld.Add(1)
						modifiedBytes.Add(e.SizeBytes)
					}
					wire := errors.Is(err, errInverseWireAmbiguous)
					// Report any other failure over a lost response, so callers
					// see wire ambiguity only when it is the sole failure.
					mu.Lock()
					if wire {
						wireAmbiguous = append(wireAmbiguous, e.Key)
					}
					if first == nil || !wire && errors.Is(first, errInverseWireAmbiguous) {
						first = err
					}
					mu.Unlock()
					if !wire {
						send.close()
					}
					halt()
					return
				}
			}
		}()
	}
send:
	for _, e := range jobs {
		select {
		case queue <- e:
		case <-stop.Done():
			break send
		}
	}
	close(queue)
	wg.Wait()
	result.Selected = int64(len(jobs))
	result.Deleted = deleted.Load()
	result.StitchSkipped = stitchSkipped.Load()
	sort.Strings(wireAmbiguous)
	result.WireAmbiguous = wireAmbiguous
	result.Bytes = bytes.Load()
	result.LastModifiedHeld.Objects += modifiedHeld.Load()
	result.LastModifiedHeld.Bytes += modifiedBytes.Load()
	result.CustodyShort = custodyShort
	result.ElapsedSeconds = time.Since(started).Seconds()
	result.ObjectsPerSecond = float64(result.Deleted) / result.ElapsedSeconds
	return result, first
}

// Apply loads only the small KE path set, not the full4M-row catalog. Every raw
// target is reloaded and checked under locks. Full catalog maps belong only to
// the read-only partition; repeated batches must not rescan them.
func loadInverseApplyProtection(ctx context.Context, q r2RetireQuerier, bucket string, picks r2RetirePicks, windows map[int64]api.Keep28Window) (inverseProtection, error) {
	p := inverseProtection{Identities: map[string][]inverseIdentity{}, Held: map[string]bool{}, Delivery: map[string]bool{}, KEFolders: map[string]bool{}, Picks: picks, Windows: windows}
	var err error
	if p.KEFolders, err = loadInverseKEFolders(ctx, q, bucket, picks); err != nil {
		return p, err
	}
	p.OwnerKeepPrefixes, err = loadInverseOwnerKeepPrefixes(ctx, q, bucket)
	return p, err
}
