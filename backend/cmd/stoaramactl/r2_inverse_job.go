package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"sort"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

// r2-inverse job (RAW), job-keep28 and job-collated run one inverse lane next to
// the database and bucket, as a provider one-off job. The executor serializes
// reserves and spends one round trip per statement, so it must not run across a
// slow home link. The job takes a fresh read-only partition and stores it as the
// bucket's reusable one, or with --reuse-partition restores that stored partition
// while it is fresh and its protection inputs are unchanged. It then runs bounded
// apply batches. Each batch gets fresh owner-fence evidence and an exact
// readback of the lane's own tables. A DELETE that lost its response holds
// only its own key, permanently, and so does a confirmed DELETE whose absence
// could not be published. Any other failure stops the job. Only reads and
// bookkeeping retry: the confirming HEAD and publication of a confirmed absence.
func runR2InverseJob(ctx context.Context, cfg config.Config, args []string, lane inverseLane) {
	f, err := parseInverseJobFlags(args, lane)
	if err != nil {
		log.Fatal(err)
	}
	// The partition and every apply check derive from this file, so it must be
	// the owner-approved picks, pinned independently of the file itself.
	if digest, err := inverseFileSHA(f.picks); err != nil || digest != f.picksSHA {
		log.Fatal("owner picks differ from the approved digest")
	}
	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	// Refuse an oversized worker pool before any partition or evidence exists.
	if err = inverseWorkersFit(pool, f.workers); err != nil {
		log.Fatal(err)
	}
	if err = os.Mkdir(f.work, 0700); err != nil {
		log.Fatal(err)
	}
	store, err := newInverseSDKStore(ctx, archiveR2Config(cfg, pool))
	if err != nil {
		log.Fatal(err)
	}
	partition := filepath.Join(f.work, "partition")
	// The job always runs under owner-fence evidence, so every lane partitions in
	// the reviewed owner scope: RAW delivery keys and mapped collated footage. The
	// collated lane additionally requires collated mapping, which the stored
	// partition's own scope digest proves before it can be reused.
	reviewed := true
	sum, digest, err := inverseJobPartition(ctx, cfg, pool, store, f, partition, reviewed)
	if err != nil {
		log.Fatal(err)
	}
	printInverseJob(map[string]any{"step": "partition", "partition_sha256": digest, "reused": f.reuse != "", "summary": sum})
	// Deniz, 2026-11-12: keep up only the links on the public delivery page.
	// Before any delete, no candidate this job can select may still be one of
	// them; only an exempt key whose class gate is open now is not.
	candidates, pageKeep, overlap, err := inverseJobPageKeepOverlap(ctx, pool, store.Bucket(), filepath.Join(partition, "partition.jsonl"), lane)
	if err != nil {
		log.Fatal(err)
	}
	printInverseJob(map[string]any{"step": "page_keep_check", "candidates": candidates, "page_keep_holds": pageKeep, "overlap": overlap})
	if overlap != 0 {
		log.Fatal("partition candidates include public delivery page keys")
	}
	o, err := inverseJobApplyOptions(f, lane, partition, digest)
	if err != nil {
		log.Fatal(err)
	}
	if err = runInverseJobBatches(ctx, pool, store, o, f.owner, f.work, f.batches); err != nil {
		log.Fatal(err)
	}
}

// inverseJobApplyOptions binds the job's apply steps to the digest the
// partition step returned, never one re-derived from disk: the existing
// apply-time hash check then rejects a partition file changed in between.
// Batches of one job also share the skip set, so a key skipped for a live
// stitch hold is not reselected by every later batch.
func inverseJobApplyOptions(f inverseJobFlags, lane inverseLane, partition, digest string) (inverseApplyOptions, error) {
	o := inverseApplyOptions{Lane: lane, Manifest: filepath.Join(partition, "partition.jsonl"), Summary: filepath.Join(partition, "summary.json"), Picks: f.picks, Ledger: filepath.Join(f.work, "ledger.jsonl"), Workers: f.workers, MaxItems: f.items, MaxBytes: f.maxBytes, Budget24h: f.budget24h, ManifestSHA: digest, skipped: &inverseSkipSet{}}
	var err error
	if o.SummarySHA, err = inverseFileSHA(o.Summary); err != nil {
		return o, err
	}
	return o, nil
}

// inverseJobStalledBatches caps consecutive batches that held keys and
// published nothing, so a persistent outage stops the job instead of spinning.
const inverseJobStalledBatches = 3

// runInverseJobBatches runs up to batches apply batches until one selects
// nothing. A batch whose only failures held keys continues the job, even with
// nothing published, up to inverseJobStalledBatches in a row.
func runInverseJobBatches(ctx context.Context, pool *pgxpool.Pool, store inverseDeleteStore, o inverseApplyOptions, owner, work string, batches int) error {
	total := inverseApplySummary{}
	stalled := 0
	for n := 1; n <= batches; n++ {
		done, result, err := runInverseJobBatch(ctx, pool, store, o, owner, filepath.Join(work, fmt.Sprintf("evidence-%03d.json", n)))
		total.addBatch(result)
		printInverseJob(map[string]any{"step": "batch", "batch": n, "result": result, "total": total, "error": errorText(err)})
		if err != nil {
			return err
		}
		if done {
			printInverseJob(map[string]any{"step": "done", "total": total})
			return nil
		}
		stalled++
		if result.Deleted > 0 || len(result.WireAmbiguous)+len(result.Unpublished) == 0 {
			stalled = 0
		}
		if stalled == inverseJobStalledBatches {
			return fmt.Errorf("%d consecutive batches held keys and published nothing", stalled)
		}
	}
	printInverseJob(map[string]any{"step": "batch_limit", "total": total})
	return nil
}

// addBatch folds one batch into the job total. Selected, deleted, byte and
// custody_short counts are per batch and sum. Held keys and last_modified_held
// are snapshots of the latest batch that read its partition: every batch
// rereads the same held intents and the same partition, so summing would count
// them again, and a batch that failed before reading keeps the last measurement.
func (t *inverseApplySummary) addBatch(r inverseApplySummary) {
	t.Selected += r.Selected
	t.Deleted += r.Deleted
	t.Bytes += r.Bytes
	t.StitchSkipped += r.StitchSkipped
	t.CustodyShort.Objects += r.CustodyShort.Objects
	t.CustodyShort.Bytes += r.CustodyShort.Bytes
	t.WireAmbiguous = append(t.WireAmbiguous, r.WireAmbiguous...)
	t.Unpublished = append(t.Unpublished, r.Unpublished...)
	if r.partitionRead {
		t.AmbiguousHeld, t.HeldAbsent, t.LastModifiedHeld = r.AmbiguousHeld, r.HeldAbsent, r.LastModifiedHeld
	}
}

// inverseJobPartition stores a fresh partition, or restores the stored one
// f.reuse names, and returns its summary and digest. Either way it requires the
// picks its listing was classified with to be the approved digest, and the
// partition to have been classified in this lane's scope.
func inverseJobPartition(ctx context.Context, cfg config.Config, pool *pgxpool.Pool, store inverseLister, f inverseJobFlags, dir string, reviewed bool) (inverseSummary, string, error) {
	if f.reuse != "" {
		sum, err := reuseInversePartition(ctx, pool, store, f.picks, f.picksSHA, f.reuse, dir, reviewed)
		return sum, f.reuse, err
	}
	p, policy, err := inversePartitionProtection(ctx, cfg, pool, f.picks, reviewed)
	if err != nil {
		return inverseSummary{}, "", err
	}
	sum, err := inverseFreshPartition(ctx, store, p, policy, f.picksSHA, dir)
	if err != nil {
		return sum, "", err
	}
	digest, err := storeInversePartition(ctx, pool, dir)
	return sum, digest, err
}

type inverseJobFlags struct {
	picks, picksSHA, owner, work, reuse string
	workers, items, batches             int
	maxBytes, budget24h                 int64
}

const inverseJobUsage = "usage: stoaramactl r2-inverse job|job-keep28 --picks FILE --expected-picks-sha256 HEX --owner-decision FILE --work DIR [--reuse-partition HEX --workers N --max-items N --max-bytes N --budget-24h-bytes N --batches N]; job-keep28 requires --max-bytes and --budget-24h-bytes"

// parseInverseJobFlags reads job flags; --help prints them with their lane's
// defaults and exits 0. RAW keeps its large batches and no rolling budget.
// Keep-28 defaults to the 100-object canary batch and runs only with an
// explicit per-batch byte cap and rolling 24h budget (0 = unlimited).
func parseInverseJobFlags(args []string, lane inverseLane) (inverseJobFlags, error) {
	f := inverseJobFlags{}
	name, items, maxBytes := "r2-inverse job", 5000, int64(1<<40)
	if lane == inverseNASKeep28 {
		name, items, maxBytes = "r2-inverse job-keep28", 100, 0
	}
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	fs.StringVar(&f.picks, "picks", "", "owner picks")
	fs.StringVar(&f.picksSHA, "expected-picks-sha256", "", "independently approved owner picks digest")
	fs.StringVar(&f.owner, "owner-decision", "", "owner decision record pinned by the executor")
	fs.StringVar(&f.work, "work", "", "new working directory")
	fs.StringVar(&f.reuse, "reuse-partition", "", "partition_sha256 of the stored partition to reuse instead of relisting the bucket")
	fs.IntVar(&f.workers, "workers", 16, "concurrent keys per batch (1..64, at least two DB connections remain reserved)")
	fs.IntVar(&f.items, "max-items", items, "maximum new objects per batch")
	fs.Int64Var(&f.maxBytes, "max-bytes", maxBytes, "maximum new intent bytes per batch (required for job-keep28)")
	fs.Int64Var(&f.budget24h, "budget-24h-bytes", 0, "maximum charged bytes in previous 24h, 0 = unlimited (required for job-keep28)")
	fs.IntVar(&f.batches, "batches", 1, "maximum batches")
	if err := fs.Parse(args); err != nil {
		return f, err
	}
	budgetSet := false
	fs.Visit(func(fl *flag.Flag) { budgetSet = budgetSet || fl.Name == "budget-24h-bytes" })
	if fs.NArg() != 0 || f.picks == "" || !r2RetireLowerHex64(f.picksSHA) || f.owner == "" || f.work == "" || (f.reuse != "" && !r2RetireLowerHex64(f.reuse)) || f.workers < 1 || f.workers > 64 || f.items < 1 || f.maxBytes <= 0 || f.budget24h < 0 || f.batches < 1 || (lane == inverseNASKeep28 && !budgetSet) {
		return f, errors.New(inverseJobUsage)
	}
	return f, nil
}

// runInverseJobBatch runs one bounded apply batch. It reports done when a clean
// batch selected nothing. Unresolved intents from earlier batches stay held;
// the executor's startup stops if one that never reached the wire is gone. A
// batch whose only failures held keys (lost DELETE responses, unpublished
// confirmed absences) returns no error once its readback proves exactly those
// keys are unresolved; runInverseJobBatches caps how many such batches in a
// row may publish nothing.
func runInverseJobBatch(ctx context.Context, pool *pgxpool.Pool, store inverseDeleteStore, o inverseApplyOptions, owner, evidencePath string) (bool, inverseApplySummary, error) {
	var err error
	if o.DrainSHA, err = writeInverseOwnerEvidence(ctx, pool, store, o, owner, evidencePath); err != nil {
		return false, inverseApplySummary{}, err
	}
	o.DrainEvidence = evidencePath
	result, applyErr := applyInverse(ctx, pool, store, o)
	// Read this batch's durable outcomes on every path, including a failed one.
	r := inverseBatchReadback{Unresolved: []string{}}
	if err = pool.QueryRow(ctx, `SELECT count(*),count(v.object_key),coalesce(sum(i.size_bytes) FILTER (WHERE v.object_key IS NOT NULL),0),count(a.object_key),
 coalesce(array_agg(i.object_key ORDER BY i.object_key) FILTER (WHERE v.object_key IS NULL),'{}')
 FROM `+o.Lane.intentTable()+` i
 LEFT JOIN `+o.Lane.eventTable()+` v ON v.bucket=i.bucket AND v.object_key=i.object_key AND v.event='absent'
 LEFT JOIN `+o.Lane.attemptTable()+` a ON a.bucket=i.bucket AND a.object_key=i.object_key
 WHERE i.bucket=$1 AND i.drain_evidence_sha256=$2`, store.Bucket(), o.DrainSHA).Scan(&r.Intents, &r.Absent, &r.AbsentBytes, &r.Attempts, &r.Unresolved); err != nil {
		return false, result, errors.Join(applyErr, err)
	}
	// Compare key sets in one byte order; the database orders by its collation.
	sort.Strings(r.Unresolved)
	printInverseJob(map[string]any{"step": "readback", "evidence_sha256": o.DrainSHA, "intents": r.Intents, "absent": r.Absent, "absent_bytes": r.AbsentBytes, "attempts": r.Attempts, "unresolved": r.Unresolved})
	done, err := r.settle(result, applyErr)
	return done, result, err
}

// inverseBatchReadback is one batch's durable outcome, read from the lane's
// tables by the batch's own evidence digest.
type inverseBatchReadback struct {
	Intents, Absent, AbsentBytes, Attempts int64
	Unresolved                             []string
}

// settle accepts a batch only when the database proves exactly what the
// executor reports: every absence and byte, an attempt per intent, and the very
// keys it holds as lost responses or unpublished absences, not just as many.
func (r inverseBatchReadback) settle(result inverseApplySummary, applyErr error) (bool, error) {
	held := inverseKeyHeld(applyErr)
	if applyErr != nil && !held {
		return false, applyErr
	}
	unresolved := slices.Sorted(slices.Values(slices.Concat(result.WireAmbiguous, result.Unpublished)))
	if r.Absent != result.Deleted || r.AbsentBytes != result.Bytes || r.Attempts != r.Intents || !slices.Equal(r.Unresolved, unresolved) {
		return false, fmt.Errorf("database batch evidence differs: intents %d absent %d bytes %d attempts %d unresolved %v", r.Intents, r.Absent, r.AbsentBytes, r.Attempts, r.Unresolved)
	}
	if held {
		printInverseJob(map[string]any{"step": "held", "held": r.Unresolved, "error": applyErr.Error()})
		return false, nil
	}
	// Done only when nothing new was eligible: a batch whose keys were all
	// skipped for short custody leaves others for the next batch.
	return result.Selected == 0, nil
}

// writeInverseOwnerEvidence writes owner-fence evidence observed now from the
// exact live fence row, bound to this partition, and returns its digest.
func writeInverseOwnerEvidence(ctx context.Context, pool *pgxpool.Pool, store inverseReader, o inverseApplyOptions, owner, path string) (string, error) {
	e := inverseDrainEvidence{Proof: inverseOwnerFenceProof, Bucket: store.Bucket(), EndpointSHA256: store.EndpointSHA256(), FencePolicy: &inverseFencePolicyEvidence{
		PartitionSHA256: o.ManifestSHA, SummarySHA256: o.SummarySHA, LastModifiedBefore: inverseRetirementCutoff(),
		OwnerDecision: inverseEvidenceReference{Path: owner, SHA256: inverseOwnerDecisionSHA256}, ResidualAcceptance: inverseApprovedFenceResidual,
	}}
	if err := pool.QueryRow(ctx, `SELECT activated_at,drain_not_before,clock_timestamp() FROM r2_retirement_writer_fences WHERE bucket=$1 AND endpoint_sha256=$2`, store.Bucket(), store.EndpointSHA256()).Scan(&e.FenceActivatedAt, &e.DrainNotBefore, &e.ObservedAt); err != nil {
		return "", fmt.Errorf("exact writer fence required: %w", err)
	}
	if err := e.validateProof(o); err != nil {
		return "", err
	}
	data, err := json.Marshal(e)
	if err != nil {
		return "", err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return "", err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return "", err
	}
	return inverseFileSHA(path)
}

func printInverseJob(v map[string]any) {
	data, err := json.Marshal(v)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(data))
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// inverseJobPageKeepOverlap counts the lane's partition candidates, the exact
// protected public delivery page keys, and the candidates that are such keys.
func inverseJobPageKeepOverlap(ctx context.Context, pool *pgxpool.Pool, bucket, manifest string, lane inverseLane) (int, int64, int64, error) {
	f, err := os.Open(manifest)
	if err != nil {
		return 0, 0, 0, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	var keys []string
	for scanner.Scan() {
		var e inverseEntry
		if err = json.Unmarshal(scanner.Bytes(), &e); err != nil {
			return 0, 0, 0, err
		}
		if e.Class == lane.class() {
			keys = append(keys, e.Key)
		}
	}
	if err = scanner.Err(); err != nil {
		return 0, 0, 0, err
	}
	var held, overlap int64
	// An exempt key whose class gate is open is no longer a protected page key.
	err = pool.QueryRow(ctx, `SELECT count(*),count(*) FILTER (WHERE object_key=ANY($2::text[])) FROM delivery_keep_holds WHERE bucket=$1 AND NOT r2_delivery_exempt(bucket,object_key)`, bucket, keys).Scan(&held, &overlap)
	return len(keys), held, overlap, err
}
