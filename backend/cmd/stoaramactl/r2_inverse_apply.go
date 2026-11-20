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
	// confirmAbsent HEADs a key after its answered DELETE; nil only on 404.
	confirmAbsent(context.Context, string) error
}

// inverseAbsence reads a confirming HEAD's outcome: only a 404 confirms the
// DELETE, and an object that answers is still there.
func inverseAbsence(err error) error {
	switch {
	case r2.IsNotFound(err):
		return nil
	case err == nil:
		return errors.New("object remains after DELETE")
	}
	return err
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
	// skipped remembers keys a later batch must not reselect. A key skipped for a
	// live stitch hold wrote nothing durable, so only this run can tell the next
	// batch, in the same job, that it was already passed over. Batches of one job
	// share it; a fresh apply starts empty.
	skipped *inverseSkipSet
}

// inverseSkipSet is the keys this job already passed over, shared by its
// batches and by concurrent workers within a batch.
type inverseSkipSet struct {
	mu   sync.Mutex
	keys map[string]struct{}
}

func (s *inverseSkipSet) has(key string) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.keys[key]
	return ok
}

func (s *inverseSkipSet) add(key string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.keys == nil {
		s.keys = map[string]struct{}{}
	}
	s.keys[key] = struct{}{}
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
	HeldAbsent    []string `json:"held_absent"`
	WireAmbiguous []string `json:"wire_ambiguous"`
	// Unpublished lists keys whose DELETE was confirmed by a 404 but whose
	// absence could not be published. Like WireAmbiguous they stay held.
	Unpublished      []string     `json:"unpublished"`
	LastModifiedHeld r2RetireStat `json:"last_modified_held"`
	// CustodyShort counts keep-28 keys skipped before any write because their
	// custody could end before a DELETE finishes; fresh custody retries them.
	CustodyShort     r2RetireStat `json:"custody_short"`
	Bytes            int64        `json:"bytes"`
	ElapsedSeconds   float64      `json:"elapsed_seconds"`
	ObjectsPerSecond float64      `json:"objects_per_second"`
	// partitionRead reports that this batch reconciled its held intents and
	// read its whole partition, so its held and last_modified_held snapshots
	// are measurements.
	partitionRead bool
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

// inverseFenceTableWait waits for the fence table's own lock. SELECT ... FOR
// SHARE needs ROW SHARE on its table, so a session holding the table EXCLUSIVE
// makes even a NOWAIT row read queue for that table lock.
func (a *inverseApplier) inverseFenceTableWait() inverseWait {
	return inverseWait{sql: `LOCK TABLE r2_retirement_writer_fences IN ROW SHARE MODE`}
}

func (a *inverseApplier) checkEvidence(ctx context.Context, q r2RetireQuerier) error {
	return a.checkEvidenceLock(ctx, q, `FOR SHARE`)
}

// checkEvidenceAdmission reads the same fence under the wide protection locks,
// so it only tries it: both the table lock the SELECT needs and the row lock
// are NOWAIT, and admission awaits a busy one outside them.
func (a *inverseApplier) checkEvidenceAdmission(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, `LOCK TABLE r2_retirement_writer_fences IN ROW SHARE MODE NOWAIT`); err != nil {
		return inverseRowBusy(err, a.inverseFenceTableWait())
	}
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
	return a.summary.fresh(now)
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

// inverseContentionBackoff is the pause between attempts when a lock bound
// refused a statement without naming the lock. The attempt itself names nothing
// in that case, so admission frees the wide locks and waits outside them.
const inverseContentionBackoff = 20 * time.Millisecond

// await waits for each busy lock in turn; begin opens every wait transaction
// exactly as the caller's admission does. A busy lock that names no wait backs
// off outside every lock the attempt held instead.
func (b inverseBusy) await(ctx context.Context, begin func(context.Context) (pgx.Tx, error)) error {
	if len(b) == 0 {
		timer := time.NewTimer(inverseContentionBackoff)
		defer timer.Stop()
		select {
		case <-timer.C:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
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
// inverseAdmissionRetries attempts and never past inverseAdmissionDeadline.
// admit receives the bounded admission context and must use it for its own
// transaction and every lock it takes, so pool acquisition, the epoch barrier
// and each lock acquisition are all inside the deadline. admit must have rolled
// its transaction back before it returns inverseBusy.
func admitInverse[T any](ctx context.Context, begin func(context.Context) (pgx.Tx, error), admit func(context.Context) (T, error)) (T, error) {
	ctx, cancel := context.WithTimeout(ctx, inverseAdmissionDeadline)
	defer cancel()
	busyFailure := func(attempts int, busy inverseBusy, cause error) error {
		if cause == nil {
			return fmt.Errorf("%w: %d waits: %v", errInverseAdmissionBusy, attempts, busy)
		}
		return fmt.Errorf("%w: %d waits: %v: %w", errInverseAdmissionBusy, attempts, busy, cause)
	}
	var last inverseBusy
	var value T
	for attempts := 1; ; attempts++ {
		var err error
		value, err = admit(ctx)
		var busy inverseBusy
		if !errors.As(err, &busy) {
			// A lock acquired inside the attempt is bounded by the admission
			// context, so its expiry is the same named busy failure. A cancelled
			// admission keeps its own error: a sibling's stop is not contention.
			if err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return value, busyFailure(attempts, last, err)
			}
			return value, err
		}
		last = busy
		if attempts >= inverseAdmissionRetries {
			return value, busyFailure(attempts, busy, nil)
		}
		if err = busy.await(ctx, begin); err != nil {
			if ctx.Err() != nil {
				return value, busyFailure(attempts, busy, ctx.Err())
			}
			return value, busyFailure(attempts, busy, err)
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
func admitWire[T any](ctx context.Context, pool *pgxpool.Pool, authorize func(context.Context, pgx.Tx) (T, error)) (pgx.Tx, T, error) {
	var tx pgx.Tx
	v, err := admitInverse(ctx, inverseBegin(pool), func(ctx context.Context) (T, error) {
		var zero T
		var err error
		if tx, err = pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}); err != nil {
			return zero, err
		}
		v, err := authorize(ctx, tx)
		// A lock the attempt lost after the wide cleanup locks is contention:
		// roll the wire transaction back so admission waits outside them.
		err = inverseContention(err)
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
	if inverseLockBusy(err) {
		return inverseBusy{wait}
	}
	return err
}

// inverseLockTimeout bounds every lock wait once a transaction holds the wide
// cleanup locks 7343226011103/7343226011101. NOWAIT covers a row but not the
// table lock the statement still needs, so only a transaction-wide bound stops
// a table or row wait from queueing while the wide locks are held. With it, any
// such wait ends as SQLSTATE 55P03 instead.
const inverseLockTimeout = 100 * time.Millisecond

// boundInverseLocks applies the transaction-wide lock bound. It runs right
// after the wide cleanup locks, so no later statement in the transaction can
// wait on a table or row lock.
func boundInverseLocks(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, `SELECT set_config('lock_timeout',$1,true)`, inverseLockTimeout.String())
	return err
}

// inverseLockBusy reports a lock wait PostgreSQL refused: a lock_timeout expiry
// or a NOWAIT miss. Both mean another session owns the lock right now.
func inverseLockBusy(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "55P03"
}

// inverseLockContention reports a lock wait our own transaction bound refused.
// Only PostgreSQL's own refusals count: a trigger may raise 55P03 with its own
// message, and those exact outcomes (a busy stitch hold admission) keep the
// meaning they already had, so they are never retried as plain contention.
func inverseLockContention(err error) bool {
	var pg *pgconn.PgError
	if !errors.As(err, &pg) || pg.Code != "55P03" {
		return false
	}
	return strings.HasPrefix(pg.Message, "canceling statement due to lock timeout") ||
		strings.HasPrefix(pg.Message, "could not obtain lock")
}

// inverseContention turns a bound lock wait into contention, so admission takes
// its existing path: roll back, wait outside, retry inside the deadline. It
// names no wait because the refused statement does not say which lock it wanted;
// admission backs off outside the wide locks instead.
func inverseContention(err error) error {
	var busy inverseBusy
	if !inverseLockContention(err) || errors.As(err, &busy) {
		return err
	}
	return inverseBusy{}
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

// inverseLiveHeldKeys reports every key a live stitch hold currently owns for
// the batch's bucket. Selection reads it once, before any item allowance is
// spent, so a held key is never reselected batch after batch. A hold admitted
// after this read is still caught at reserve, which skips that one key.
func (a *inverseApplier) inverseLiveHeldKeys(ctx context.Context) (map[string]bool, error) {
	held := map[string]bool{}
	rows, err := a.pool.Query(ctx, `SELECT object_key FROM nas_stitch_r2_input_holds WHERE bucket=$1 AND released_at IS NULL`, a.store.Bucket())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		if err = rows.Scan(&key); err != nil {
			return nil, err
		}
		held[key] = true
	}
	return held, rows.Err()
}

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
	return admitInverse(ctx, inverseBegin(a.pool), func(ctx context.Context) (inverseRawTarget, error) {
		t, err := a.reserveOnce(ctx, e)
		// Any lock the attempt lost after the wide cleanup locks is contention:
		// the attempt rolled back, so admission waits outside them and retries.
		return t, inverseContention(err)
	})
}
func (a *inverseApplier) reserveOnce(ctx context.Context, e inverseEntry) (inverseRawTarget, error) {
	t := inverseRawTarget{}
	tx, err := a.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return t, err
	}
	defer tx.Rollback(context.Background())
	// The step's own bound replaces the role's statement_timeout, whose 57014
	// would end a lock wait publication also takes; only a cancel then stops it.
	if _, err = tx.Exec(ctx, `SET LOCAL statement_timeout = 0`); err != nil {
		return t, err
	}
	if err = inverseProtectionBarrier(ctx, tx); err != nil {
		return t, err
	}
	if err = inverseProtectionLocks(ctx, tx, false); err != nil {
		return t, err
	}
	// From here the transaction holds both wide cleanup locks, so every later
	// table or row wait must fail at once instead of queueing behind them.
	if err = boundInverseLocks(ctx, tx); err != nil {
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
		return t, a.keyContention(ctx, tx, e.Key, inverseContention(err))
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

// One key's network calls and database steps have separate bounds, so lock
// waits never spend the network budget. The network calls (identity HEAD, the
// single DELETE, the confirming HEAD and its retries) share
// inverseNetworkTimeout from when the wire transaction holds its locks;
// keep-28 also ends them at custody end. Each database step (reservation, wire
// connection, wire preparation, the wire commit, each publication attempt) gets
// a fresh inverseDBTimeout: it may queue behind sibling protection locks.
const (
	inverseNetworkTimeout = 30 * time.Second
	inverseDBTimeout      = 2 * time.Minute
	// inversePublishAttempts bounds publication of a confirmed absence.
	inversePublishAttempts = 3
)

// inverseStep names the step of one key that failed, so a stop says where.
type inverseStep string

const (
	inverseStepReserve inverseStep = "reserve"
	inverseStepWire    inverseStep = "wire"
	inverseStepHead    inverseStep = "head"
	inverseStepCommit  inverseStep = "commit"
	inverseStepPublish inverseStep = "publish"
)

type inverseStepError struct {
	step inverseStep
	key  string
	err  error
}

func (e *inverseStepError) Error() string { return fmt.Sprintf("%s %s: %v", e.step, e.key, e.err) }
func (e *inverseStepError) Unwrap() error { return e.err }

// errInverseUnpublished marks a key whose DELETE a 404 confirmed but whose
// absence a database timeout or lock contention kept from being published.
// Like a lost response it holds only that key, issued and unresolved, and is
// never resent.
var errInverseUnpublished = errors.New("DELETE confirmed absent but absence unpublished; key held, never retransmit")

// inverseKeyHeld reports a failure that holds only its own key.
func inverseKeyHeld(err error) bool {
	return errors.Is(err, errInverseWireAmbiguous) || errors.Is(err, errInverseUnpublished)
}

// SQLSTATEs of lock contention a database step can retry.
const (
	pgLockNotAvailable     = "55P03"
	pgDeadlockDetected     = "40P01"
	pgSerializationFailure = "40001"
)

// inverseDBTransient reports a database step that ran out of its own bound
// (waiting for a pool connection, a lock or a statement), or lost to lock
// contention. A step whose parent already ended never got its own bound: a
// parent cancel or deadline stops the job and holds nothing, because the 404
// that confirmed the DELETE is not this step's failure. An explicit cancel
// (57014: publication disables the role's statement_timeout, so only a cancel
// raises it), a refusal or an inconsistent publication is never transient.
func inverseDBTransient(parent, step context.Context, err error) bool {
	if parent.Err() != nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == pgLockNotAvailable || pgErr.Code == pgDeadlockDetected || pgErr.Code == pgSerializationFailure
	}
	return errors.Is(step.Err(), context.DeadlineExceeded) && errors.Is(err, context.DeadlineExceeded)
}

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
// Every failure names its step and key.
func (a *inverseApplier) delete(ctx, admit context.Context, send *inverseSendGate, e inverseEntry) (bool, error) {
	if admit.Err() != nil {
		return false, errInverseAdmissionClosed
	}
	fail := func(step inverseStep, err error) (bool, error) {
		return false, &inverseStepError{step, e.Key, err}
	}
	db, cancel := context.WithTimeout(ctx, inverseDBTimeout)
	defer cancel()
	reserving, stopReserving := context.WithCancel(db)
	defer context.AfterFunc(admit, stopReserving)()
	_, err := a.reserve(reserving, e)
	stopReserving()
	if err != nil {
		// Only a wait that closed admission cancelled is not this key's own
		// failure; any gate failure is reported even if admission also closed.
		if admit.Err() != nil && errors.Is(err, context.Canceled) {
			return false, errInverseAdmissionClosed
		}
		return fail(inverseStepReserve, err)
	}
	if err = a.record(e, inverseIntent, a.opts.ManifestSHA); err != nil {
		return fail(inverseStepReserve, err)
	}
	var tx pgx.Tx
	defer func() {
		if tx != nil {
			tx.Rollback(context.Background())
		}
	}()
	// authorize runs under the wide protection locks, so every lock it waits on
	// after them is a NOWAIT try or a 100ms transaction-local lock_timeout:
	// admission awaits a busy one outside, holding nothing else, then
	// revalidates from a fresh transaction. Its admission work keeps the
	// DB-only deadline through the attempt insert; the returned authority
	// carries the key's own 30s network bound and, on keep-28, custody end.
	var authority *inverseDeleteAuthority
	var authErr error
	tx, authority, authErr = admitWire(ctx, a.pool, func(admitCtx context.Context, tx pgx.Tx) (*inverseDeleteAuthority, error) {
		return a.authorize(admitCtx, ctx, tx, e, send)
	})
	if authErr != nil {
		return fail(inverseStepWire, authErr)
	}
	defer authority.cancel()
	if send.isClosed() {
		return false, errInverseAdmissionClosed
	}
	if err = a.store.(inverseDeleteStore).deleteOnce(authority.ctx, authority); err != nil {
		if errors.Is(err, errInverseWireAmbiguous) {
			return fail(inverseStepWire, fmt.Errorf("single DELETE result ambiguous; intent retained, never retransmit: %w", err))
		}
		return fail(inverseStepWire, fmt.Errorf("single DELETE failed; intent retained, never retransmit: %w", err))
	}

	// Confirmation ends at the running custody minimum, sampled again here.
	confirming, stop, err := authority.bound(authority.ctx)
	if err != nil {
		return fail(inverseStepHead, err)
	}
	err = a.store.(inverseDeleteStore).confirmAbsent(confirming, e.Key)
	stop()
	if err != nil {
		return fail(inverseStepHead, err)
	}
	// The key is gone; what follows is bookkeeping. The wire transaction holds
	// only locks, and publication must wait for it to release them.
	committing, cancel := context.WithTimeout(ctx, inverseDBTimeout)
	defer cancel()
	if err = tx.Commit(committing); err != nil {
		return fail(inverseStepCommit, inverseHoldUnpublished(ctx, committing, err))
	}
	if err = a.publishConfirmed(ctx, e); err != nil {
		return fail(inverseStepPublish, err)
	}
	return true, nil
}

// inverseHoldUnpublished turns a transient database failure after a confirmed
// DELETE into errInverseUnpublished; any other failure stays a hard one. A
// parent that already ended stops the job instead: nothing about the confirmed
// absence is in doubt, only this run stopped.
func inverseHoldUnpublished(parent, step context.Context, err error) error {
	if inverseDBTransient(parent, step, err) {
		return fmt.Errorf("%w: %w", errInverseUnpublished, err)
	}
	return err
}

// publishConfirmed publishes a confirmed absence, retrying a database step
// that ran out of its own inverseDBTimeout or lost to lock contention. Every
// such failure happens before its COMMIT is sent; a failed COMMIT has an
// unknown outcome and stops the job instead.
func (a *inverseApplier) publishConfirmed(ctx context.Context, e inverseEntry) error {
	for attempt := 1; ; attempt++ {
		db, cancel := context.WithTimeout(ctx, inverseDBTimeout)
		err := a.publishAbsent(db, e, a.opts.ManifestSHA)
		held := inverseHoldUnpublished(ctx, db, err)
		cancel()
		if !errors.Is(held, errInverseUnpublished) || attempt == inversePublishAttempts {
			return held
		}
		select {
		case <-ctx.Done():
			// The parent ended mid-retry: this is a stopped job, not a held key.
			return err
		case <-time.After(time.Duration(attempt) * time.Second):
		}
	}
}
func (a *inverseApplier) publishAbsent(ctx context.Context, e inverseEntry, manifest string) error {
	_, err := admitInverse(ctx, inverseBegin(a.pool), func(ctx context.Context) (struct{}, error) {
		return struct{}{}, inverseContention(a.publishAbsentOnce(ctx, e, manifest))
	})
	return err
}
func (a *inverseApplier) publishAbsentOnce(ctx context.Context, e inverseEntry, manifest string) error {
	tx, err := a.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	// The step's own bound replaces the role's statement_timeout, whose 57014
	// would be indistinguishable from an explicit cancel.
	if _, err = tx.Exec(ctx, `SET LOCAL statement_timeout = 0`); err != nil {
		return err
	}
	if a.opts.Lane == inverseNASKeep28 {
		if err = inverseProtectionBarrier(ctx, tx); err != nil {
			return err
		}
	}
	if err = inverseProtectionLocks(ctx, tx, false); err != nil {
		return err
	}
	// Publication holds the wide cleanup locks from here on, so the clip row,
	// the purge update and the receipt insert can never wait behind a session
	// that needs those same locks.
	if err = boundInverseLocks(ctx, tx); err != nil {
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
		// Sent but unacknowledged: it may yet take effect, so this failure is
		// never classified as transient, retried or held.
		return fmt.Errorf("absence publication COMMIT outcome unknown: %v", err)
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
	// A direct apply is one batch: a skip still has to stay out of its own retry
	// loop, and a job's batches pass the shared set instead.
	if o.skipped == nil {
		o.skipped = &inverseSkipSet{}
	}
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
	if err = summary.matches(store, policy, picks.SHA256); err != nil {
		return result, err
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
	protection, err := admitInverse(ctx, inverseBegin(pool), func(ctx context.Context) (inverseProtection, error) {
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
	// A key a live stitch hold owns, or that this job already skipped, is
	// dropped before MaxItems or the byte cap applies: otherwise it would spend
	// the batch's allowance every batch and starve the keys behind it.
	heldKeys, err := a.inverseLiveHeldKeys(ctx)
	if err != nil {
		return result, err
	}
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
			if heldKeys[e.Key] || o.skipped.has(e.Key) {
				continue
			}
			candidates[e.Key] = e
			continue
		}
		if heldKeys[e.Key] || o.skipped.has(e.Key) {
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
	result.partitionRead = true
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
	var wireAmbiguous, unpublished []string
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
					// This key wrote nothing; remember it so no later batch of
					// this job spends its whole allowance reselecting it.
					o.skipped.add(e.Key)
					stitchSkipped.Add(1)
					continue
				}
				if err != nil {
					log.Printf("r2-inverse key failed: %v", err)
					var modifiedErr inverseLastModifiedError
					if errors.As(err, &modifiedErr) {
						modifiedHeld.Add(1)
						modifiedBytes.Add(e.SizeBytes)
					}
					held := inverseKeyHeld(err)
					// Report any other failure over a held key, so callers see
					// a held key only when holding is the sole failure.
					mu.Lock()
					switch {
					case errors.Is(err, errInverseWireAmbiguous):
						wireAmbiguous = append(wireAmbiguous, e.Key)
					case errors.Is(err, errInverseUnpublished):
						unpublished = append(unpublished, e.Key)
					}
					if first == nil || !held && inverseKeyHeld(first) {
						first = err
					}
					mu.Unlock()
					if !held {
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
	sort.Strings(unpublished)
	result.Unpublished = unpublished
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
