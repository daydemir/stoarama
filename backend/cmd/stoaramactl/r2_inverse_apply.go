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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type inverseDeleteStore interface {
	inverseLister
	Head(context.Context, string) (r2.ObjectHead, error)
	EndpointSHA256() string
	RequireNoMultipartUploads(context.Context) error
	DeleteNonKeep28Once(context.Context, string, r2.NonKeep28DeleteGuard) error
}
type inverseDrainEvidence struct {
	Bucket                 string    `json:"bucket"`
	EndpointSHA256         string    `json:"endpoint_sha256"`
	FenceActivatedAt       time.Time `json:"fence_activated_at"`
	DrainNotBefore         time.Time `json:"drain_not_before"`
	ObservedAt             time.Time `json:"observed_at"`
	ExpiresAt              time.Time `json:"expires_at"`
	DeployedWriterCoverage string    `json:"deployed_writer_coverage_evidence"`
	AcceptedPUTCompletion  string    `json:"accepted_put_completion_evidence"`
}
type inverseApplyOptions struct {
	Manifest, ManifestSHA, Summary, SummarySHA, Picks, Ledger, DrainEvidence, DrainSHA string
	MaxItems                                                                           int
	MaxBytes, Budget24h                                                                int64
	Workers                                                                            int
}
type inverseApplySummary struct {
	Deleted          int64   `json:"deleted"`
	Reconciled       int64   `json:"reconciled"`
	AmbiguousHeld    int64   `json:"ambiguous_held"`
	Bytes            int64   `json:"bytes"`
	ElapsedSeconds   float64 `json:"elapsed_seconds"`
	ObjectsPerSecond float64 `json:"objects_per_second"`
}
type inverseRawTarget struct {
	ID, StreamID    int64
	Size            int64
	SHA, ETag, Path string
	Identity        inverseIdentity
}
type inverseApplier struct {
	pool       *pgxpool.Pool
	store      inverseDeleteStore
	opts       inverseApplyOptions
	summary    inverseSummary
	protection inverseProtection
	ledger     *inverseLedger
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
func (a *inverseApplier) checkEvidence(ctx context.Context, q r2RetireQuerier) error {
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
	var blocked bool
	err = q.QueryRow(ctx, `SELECT activated_at,drain_not_before,clock_timestamp(),r2_retirement_writes_blocked($1,$2)
 FROM r2_retirement_writer_fences WHERE bucket=$1 AND endpoint_sha256=$2 FOR SHARE`, a.store.Bucket(), a.store.EndpointSHA256()).Scan(&activated, &deadline, &now, &blocked)
	if err != nil {
		return fmt.Errorf("exact 0198 writer fence required: %w", err)
	}
	if !blocked || !now.After(deadline) || e.Bucket != a.store.Bucket() || e.EndpointSHA256 != a.store.EndpointSHA256() || !e.FenceActivatedAt.Equal(activated) || !e.DrainNotBefore.Equal(deadline) || e.ObservedAt.Before(deadline) || e.ObservedAt.After(now) || !e.ExpiresAt.After(now) || e.ExpiresAt.After(e.ObservedAt.Add(24*time.Hour)) || strings.TrimSpace(e.DeployedWriterCoverage) == "" || strings.TrimSpace(e.AcceptedPUTCompletion) == "" {
		return errors.New("writer drain or controller evidence missing, stale or outside exact fence")
	}
	if now.Sub(a.summary.CreatedAt) > 24*time.Hour || a.summary.CreatedAt.After(now) {
		return errors.New("partition is stale or future-dated")
	}
	return nil
}
func inverseProtectionLocks(ctx context.Context, tx pgx.Tx, shared bool) error {
	sql := `SELECT pg_advisory_xact_lock($1)`
	if shared {
		sql = `SELECT pg_advisory_xact_lock_shared($1)`
	}
	for _, key := range []int64{7343226011103, 7343226011101} {
		if _, err := tx.Exec(ctx, sql, key); err != nil {
			return err
		}
	}
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(137,1)`)
	return err
}

const inverseRawTargetSQL = `SELECT c.id,coalesce(r.stream_id,0),c.size_bytes,lower(c.sha256),coalesce(c.etag,''),c.display_path,c.clip_start_at,c.clip_end_at,
 coalesce(nullif(s.local_timezone,''),r.cron_timezone,'') FROM recording_clips c JOIN recordings r ON r.id=c.recording_id
 LEFT JOIN streams s ON s.id=r.stream_id WHERE c.bucket=$1 AND c.object_key=$2 AND c.purged_at IS NULL FOR UPDATE OF c FOR SHARE OF r`

func (a *inverseApplier) loadTarget(ctx context.Context, tx pgx.Tx, e inverseEntry) (inverseRawTarget, error) {
	t := inverseRawTarget{}
	rows, err := tx.Query(ctx, inverseRawTargetSQL, a.store.Bucket(), e.Key)
	if err != nil {
		return t, err
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
		return t, err
	}
	if n != 1 || t.ID <= 0 || t.Size <= 0 || t.Size != e.SizeBytes || !r2RetireLowerHex64(t.SHA) || strings.Trim(t.ETag, `"`) == "" || strings.Trim(t.ETag, `"`) != strings.Trim(e.ETag, `"`) || !strings.HasPrefix(e.Key, "managed/") {
		return t, errors.New("raw catalog identity absent, ambiguous or changed")
	}
	t.Identity.StreamID = t.StreamID
	t.Identity.Path = t.Path
	t.Identity.Kind = "raw"
	// Use a copy: an old snapshot never becomes wire authority for current identity.
	p := a.protection
	p.Identities = map[string][]inverseIdentity{e.Key: {t.Identity}}
	p.Held = map[string]bool{}
	p.Delivery = map[string]bool{}
	if class, _ := p.classify(e.ObjectInfo); class != inverseDeletable {
		return t, errors.New("target is no longer raw non-keep28")
	}
	return t, nil
}

const inverseRawHeldSQL = `SELECT
 EXISTS(SELECT 1 FROM nas_cleanup_reserved_files WHERE kind='raw' AND source_id=$1)
 OR EXISTS(SELECT 1 FROM nas_cleanup_source_holds h JOIN nas_cleanup_plans p ON p.id=h.plan_id WHERE h.clip_id=$1 AND (p.mode<>'retired' OR EXISTS(SELECT 1 FROM nas_cleanup_items i WHERE i.plan_id=p.id AND i.delete_ever_issued)))
 OR EXISTS(SELECT 1 FROM nas_cleanup_incident_sources WHERE clip_id=$1)
 OR EXISTS(SELECT 1 FROM r2_retirement_reservations WHERE bucket=$2 AND object_key=$3 AND NOT (kind='raw' AND source_id=$1 AND manifest_sha256=$4))
 OR EXISTS(SELECT 1 FROM r2_object_retirements WHERE bucket=$2 AND object_key=$3)
 OR EXISTS(SELECT 1 FROM nas_r2_copy_items WHERE (state<>'verified' OR blocked_reason<>'') AND item->>'bucket'=$2 AND item->>'object_key'=$3)
 OR EXISTS(SELECT 1 FROM delivery_keep_holds WHERE bucket=$2 AND (object_key=$3 OR (relative_path<>'' AND relative_path=$5)))
 OR EXISTS(SELECT 1 FROM delivery_nas_source_holds WHERE clip_id=$1)
 OR EXISTS(SELECT 1 FROM nas_restore_requests WHERE clip_id=$1 AND state IN ('pending','leased'))
 OR EXISTS(SELECT 1 FROM recording_joined_source_snapshots WHERE clip_id=$1)
 OR EXISTS(SELECT 1 FROM recording_joined_snapshot_scopes s JOIN recording_joined_batches b ON b.id=s.batch_record_id AND b.state='snapshotting' JOIN recording_clips c ON c.id=$1 WHERE s.recording_id=c.recording_id AND s.recording_job_id=c.recording_job_id AND c.id<=s.high_water_clip_id)
 OR EXISTS(SELECT 1 FROM recording_joined_dry_run_scopes s JOIN recording_clips c ON c.id=$1 WHERE s.recording_id=c.recording_id AND s.recording_job_id=c.recording_job_id AND c.id<=s.high_water_clip_id)`

func (a *inverseApplier) checkTarget(ctx context.Context, tx pgx.Tx, t inverseRawTarget, e inverseEntry, purgeProbe bool) error {
	if err := a.checkEvidence(ctx, tx); err != nil {
		return err
	}
	var held bool
	if err := tx.QueryRow(ctx, inverseRawHeldSQL, t.ID, a.store.Bucket(), e.Key, a.opts.ManifestSHA, t.Path).Scan(&held); err != nil {
		return err
	}
	if held {
		return errors.New("live key protection veto")
	}
	if purgeProbe {
		// Exercise every installed purge trigger before wire deletion; all effects
		// are rolled back. The source row was locked before this savepoint.
		if _, err := tx.Exec(ctx, `SAVEPOINT inverse_purge_probe`); err != nil {
			return err
		}
		_, probeErr := tx.Exec(ctx, `UPDATE recording_clips SET purged_at=clock_timestamp() WHERE id=$1`, t.ID)
		_, rollbackErr := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT inverse_purge_probe`)
		if probeErr != nil {
			return fmt.Errorf("pre-delete purge veto: %w", probeErr)
		}
		if rollbackErr != nil {
			return rollbackErr
		}
		if _, err := tx.Exec(ctx, `RELEASE SAVEPOINT inverse_purge_probe`); err != nil {
			return err
		}
	}
	return nil
}
func (a *inverseApplier) reserve(ctx context.Context, e inverseEntry) (inverseRawTarget, error) {
	t := inverseRawTarget{}
	tx, err := a.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return t, err
	}
	defer tx.Rollback(context.Background())
	if err = inverseProtectionLocks(ctx, tx, false); err != nil {
		return t, err
	}
	t, err = a.loadTarget(ctx, tx, e)
	if err != nil {
		return t, err
	}
	if err = a.checkTarget(ctx, tx, t, e, true); err != nil {
		return t, err
	}
	var charged int64
	err = tx.QueryRow(ctx, `SELECT coalesce(sum(charged_bytes),0) FROM r2_inverse_budget_minutes WHERE bucket=$1 AND minute>=date_trunc('minute',clock_timestamp()-interval '24 hours')`, a.store.Bucket()).Scan(&charged)
	if err != nil {
		return t, err
	}
	if e.SizeBytes > a.opts.Budget24h-charged {
		return t, errors.New("rolling 24h inverse budget exhausted")
	}
	// Independent committed transaction. Never use an outer network transaction's
	// connection or assume its rollback can discard this source reservation.
	_, err = tx.Exec(ctx, `INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256) VALUES('raw',$1,$2,$3,$4,$5,$6)`, t.ID, a.store.Bucket(), e.Key, e.SizeBytes, t.SHA, a.opts.ManifestSHA)
	if err != nil {
		return t, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO r2_inverse_intents(bucket,object_key,source_id,size_bytes,etag,last_modified,manifest_sha256,policy_sha256,picks_sha256,drain_evidence_sha256,choice) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, a.store.Bucket(), e.Key, t.ID, e.SizeBytes, e.ETag, e.LastModified, a.opts.ManifestSHA, a.summary.PolicySHA256, a.summary.PicksSHA256, a.opts.DrainSHA, a.protection.Picks.Choices[t.StreamID].Choice)
	if err != nil {
		return t, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO r2_inverse_budget_minutes(bucket,minute,charged_bytes) VALUES($1,date_trunc('minute',clock_timestamp()),$2) ON CONFLICT(bucket,minute) DO UPDATE SET charged_bytes=r2_inverse_budget_minutes.charged_bytes+excluded.charged_bytes`, a.store.Bucket(), e.SizeBytes)
	if err != nil {
		return t, err
	}
	return t, tx.Commit(ctx)
}
func (a *inverseApplier) record(e inverseEntry, event inverseEvent, manifest string) error {
	return a.ledger.append(inverseLedgerRecord{Event: event, Bucket: a.store.Bucket(), Key: e.Key, ManifestSHA256: manifest, At: time.Now().UTC(), SizeBytes: e.SizeBytes})
}

type inverseWireGuard struct {
	a  *inverseApplier
	tx pgx.Tx
	t  inverseRawTarget
	e  inverseEntry
}

func (g *inverseWireGuard) CheckNonKeep28Delete(ctx context.Context, bucket, endpoint, key string) error {
	if bucket != g.a.store.Bucket() || endpoint != g.a.store.EndpointSHA256() || key != g.e.Key {
		return errors.New("wire authority scope differs")
	}
	if err := g.a.checkTarget(ctx, g.tx, g.t, g.e, false); err != nil {
		return err
	}
	var durable bool
	err := g.tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM r2_inverse_intents WHERE bucket=$1 AND object_key=$2 AND source_id=$3 AND manifest_sha256=$4 AND drain_evidence_sha256=$5)`, bucket, key, g.t.ID, g.a.opts.ManifestSHA, g.a.opts.DrainSHA).Scan(&durable)
	if err != nil {
		return err
	}
	if !durable {
		return errors.New("wire intent missing")
	}
	return nil
}
func (a *inverseApplier) delete(ctx context.Context, e inverseEntry) (bool, error) {
	t, err := a.reserve(ctx, e)
	if err != nil {
		return false, err
	}
	if err = a.record(e, inverseIntent, a.opts.ManifestSHA); err != nil {
		return false, err
	}
	tx, err := a.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, err
	}
	defer tx.Rollback(context.Background())
	if err = inverseProtectionLocks(ctx, tx, true); err != nil {
		return false, err
	}
	t, err = a.loadTarget(ctx, tx, e)
	if err != nil {
		return false, err
	}
	if err = a.checkTarget(ctx, tx, t, e, true); err != nil {
		return false, err
	}
	head, err := a.store.Head(ctx, e.Key)
	if err != nil {
		return false, err
	}
	item := r2RetireItem{r2RetireCandidate: r2RetireCandidate{SizeBytes: e.SizeBytes, SHA256: t.SHA, ETag: e.ETag}}
	if err = r2RetireValidateGuardHead(head, item, t.ETag, 7*24*time.Hour, time.Now().UTC()); err != nil {
		return false, err
	}
	if e.LastModified.IsZero() || !head.LastModified.Equal(e.LastModified) || !e.LastModified.Before(time.Now().Add(-7*24*time.Hour)) {
		return false, errors.New("listing LastModified missing, changed or too young")
	}
	if err = a.store.RequireNoMultipartUploads(ctx); err != nil {
		return false, err
	}
	if err = a.store.DeleteNonKeep28Once(ctx, e.Key, &inverseWireGuard{a, tx, t, e}); err != nil {
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
	return true, a.publishAbsent(ctx, e, a.opts.ManifestSHA)
}
func (a *inverseApplier) publishAbsent(ctx context.Context, e inverseEntry, manifest string) error {
	tx, err := a.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if err = inverseProtectionLocks(ctx, tx, false); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO r2_object_retirements(kind,source_id,stream_id,bucket,object_key,size_bytes,sha256,reason,choice,manifest_sha256,policy_sha256,already_absent)
 SELECT 'raw',c.id,r.stream_id,c.bucket,c.object_key,c.size_bytes,lower(c.sha256),'inverse_non_keep28',i.choice,$3,i.policy_sha256,true
 FROM recording_clips c JOIN recordings r ON r.id=c.recording_id JOIN r2_inverse_intents i ON i.source_id=c.id
 WHERE i.bucket=$1 AND i.object_key=$2 AND i.manifest_sha256=$3 ON CONFLICT DO NOTHING`, a.store.Bucket(), e.Key, manifest)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE recording_clips c SET purged_at=clock_timestamp() FROM r2_inverse_intents i WHERE i.bucket=$1 AND i.object_key=$2 AND i.source_id=c.id AND c.purged_at IS NULL`, a.store.Bucket(), e.Key)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO r2_inverse_events(bucket,object_key,event) VALUES($1,$2,'absent') ON CONFLICT DO NOTHING`, a.store.Bucket(), e.Key)
	if err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	return a.record(e, inverseAbsent, manifest)
}

func runR2InverseApply(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("r2-inverse apply", flag.ExitOnError)
	o := inverseApplyOptions{}
	fs.StringVar(&o.Manifest, "manifest", "", "partition.jsonl")
	fs.StringVar(&o.ManifestSHA, "expected-sha256", "", "partition SHA256")
	fs.StringVar(&o.Summary, "summary", "", "summary.json")
	fs.StringVar(&o.SummarySHA, "expected-summary-sha256", "", "summary SHA256")
	fs.StringVar(&o.Picks, "picks", "", "owner picks")
	fs.StringVar(&o.Ledger, "ledger", "", "private append-only key ledger")
	fs.StringVar(&o.DrainEvidence, "drain-evidence", "", "controller-reviewed accepted-PUT drain evidence")
	fs.StringVar(&o.DrainSHA, "expected-drain-evidence-sha256", "", "drain evidence SHA256")
	fs.IntVar(&o.Workers, "workers", 16, "concurrent independent keys (1..64)")
	fs.IntVar(&o.MaxItems, "max-items", 100, "maximum new objects")
	fs.Int64Var(&o.MaxBytes, "max-bytes", 0, "maximum new intent bytes this run")
	fs.Int64Var(&o.Budget24h, "budget-24h-bytes", 0, "maximum charged bytes in previous24h")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || o.MaxItems < 1 || o.Workers < 1 || o.Workers > 64 || o.MaxBytes <= 0 || o.Budget24h <= 0 || o.Ledger == "" {
		log.Fatal("invalid bounded r2-inverse apply options")
	}
	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	store := mustArchiveR2Client(ctx, cfg, pool)
	result, err := applyInverse(ctx, pool, store, o)
	data, _ := json.Marshal(result)
	fmt.Println(string(data))
	if err != nil {
		log.Fatal(err)
	}
}

func applyInverse(ctx context.Context, pool *pgxpool.Pool, store inverseDeleteStore, o inverseApplyOptions) (inverseApplySummary, error) {
	started := time.Now()
	result := inverseApplySummary{}
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
	if summary.Version != 1 || summary.Bucket != store.Bucket() || summary.PolicySHA256 != policy || summary.PicksSHA256 != picks.SHA256 || !summary.ReadOnly {
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
	ledger, err := openInverseLedger(o.Ledger)
	if err != nil {
		return result, err
	}
	defer ledger.close()
	protection, err := loadInverseProtection(ctx, pool, store.Bucket(), picks, windows)
	if err != nil {
		return result, err
	}
	a := &inverseApplier{pool: pool, store: store, opts: o, summary: summary, protection: protection, ledger: ledger}
	if err = a.checkEvidence(ctx, pool); err != nil {
		return result, err
	}
	if err = store.RequireNoMultipartUploads(ctx); err != nil {
		return result, err
	}
	// An existing intent is NEVER resubmitted, regardless of manifest or ledger.
	// Only confirmed absence can advance it to a receipt on restart.
	existing := map[string]bool{}
	rows, err := pool.Query(ctx, `SELECT i.object_key,i.size_bytes,i.manifest_sha256,EXISTS(SELECT 1 FROM r2_inverse_events e WHERE e.bucket=i.bucket AND e.object_key=i.object_key AND e.event='absent') FROM r2_inverse_intents i WHERE i.bucket=$1`, store.Bucket())
	if err != nil {
		return result, err
	}
	type pending struct {
		e        inverseEntry
		manifest string
	}
	var pendingItems []pending
	for rows.Next() {
		var key, manifest string
		var size int64
		var done bool
		if err = rows.Scan(&key, &size, &manifest, &done); err != nil {
			rows.Close()
			return result, err
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
			pendingItems = append(pendingItems, pending{inverseEntry{ObjectInfo: r2.ObjectInfo{Key: key, SizeBytes: size}}, manifest})
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
		_, err = store.Head(ctx, p.e.Key)
		if r2.IsNotFound(err) {
			if err = a.publishAbsent(ctx, p.e, p.manifest); err != nil {
				return result, err
			}
			result.Reconciled++
		} else if err != nil {
			return result, err
		} else {
			result.AmbiguousHeld++
		}
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
	var planned int64
	for scanner.Scan() {
		var e inverseEntry
		if err = json.Unmarshal(scanner.Bytes(), &e); err != nil {
			return result, err
		}
		if e.Class != inverseDeletable || existing[e.Key] {
			continue
		}
		if len(jobs) >= o.MaxItems || e.SizeBytes > o.MaxBytes-planned {
			continue
		}
		class, _ := protection.classify(e.ObjectInfo)
		if class != inverseDeletable {
			continue
		}
		ids := protection.Identities[e.Key]
		if len(ids) != 1 || ids[0].Kind != "raw" {
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
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	queue := make(chan inverseEntry)
	var wg sync.WaitGroup
	var first error
	var mu sync.Mutex
	var deleted, bytes atomic.Int64
	for range o.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for e := range queue {
				if ctx.Err() != nil {
					return
				}
				ok, err := a.delete(ctx, e)
				if ok {
					deleted.Add(1)
					bytes.Add(e.SizeBytes)
				}
				if err != nil {
					mu.Lock()
					if first == nil {
						first = err
						cancel()
					}
					mu.Unlock()
					return
				}
			}
		}()
	}
send:
	for _, e := range jobs {
		select {
		case queue <- e:
		case <-ctx.Done():
			break send
		}
	}
	close(queue)
	wg.Wait()
	result.Deleted = deleted.Load()
	result.Bytes = bytes.Load()
	result.ElapsedSeconds = time.Since(started).Seconds()
	result.ObjectsPerSecond = float64(result.Deleted) / result.ElapsedSeconds
	return result, first
}
