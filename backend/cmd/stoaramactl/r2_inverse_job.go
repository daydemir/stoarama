package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"slices"
	"sort"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

// r2-inverse job (RAW) and job-keep28 run one inverse lane next to the database
// and bucket, as a provider one-off job. The executor serializes reserves and
// spends one round trip per statement, so it must not run across a slow home
// link. The job takes a fresh read-only partition, then runs bounded apply
// batches. Each batch gets fresh owner-fence evidence and an exact readback of
// the lane's own tables. A DELETE that lost its response holds only its own
// key, permanently. Any other failure stops the job; nothing is retried.
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
	partition := filepath.Join(f.work, "partition")
	sum, err := writeInversePartition(ctx, cfg, f.picks, partition)
	if err != nil {
		log.Fatal(err)
	}
	// Bind the approved digest to the bytes the partition actually read; every
	// apply re-reads the picks file and must match this summary digest.
	if sum.PicksSHA256 != f.picksSHA {
		log.Fatal("partition read owner picks other than the approved digest")
	}
	printInverseJob(map[string]any{"step": "partition", "summary": sum})
	store, err := newInverseSDKStore(ctx, archiveR2Config(cfg, pool))
	if err != nil {
		log.Fatal(err)
	}
	o := inverseApplyOptions{Lane: lane, Manifest: filepath.Join(partition, "partition.jsonl"), Summary: filepath.Join(partition, "summary.json"), Picks: f.picks, Ledger: filepath.Join(f.work, "ledger.jsonl"), Workers: f.workers, MaxItems: f.items, MaxBytes: f.maxBytes, Budget24h: f.budget24h}
	if o.ManifestSHA, err = inverseFileSHA(o.Manifest); err != nil {
		log.Fatal(err)
	}
	if o.SummarySHA, err = inverseFileSHA(o.Summary); err != nil {
		log.Fatal(err)
	}
	total := inverseApplySummary{}
	for n := 1; n <= f.batches; n++ {
		done, result, err := runInverseJobBatch(ctx, pool, store, o, f.owner, filepath.Join(f.work, fmt.Sprintf("evidence-%03d.json", n)))
		total.Deleted += result.Deleted
		total.Bytes += result.Bytes
		total.WireAmbiguous = append(total.WireAmbiguous, result.WireAmbiguous...)
		total.AmbiguousHeld, total.HeldAbsent = result.AmbiguousHeld, result.HeldAbsent
		printInverseJob(map[string]any{"step": "batch", "batch": n, "result": result, "total": total, "error": errorText(err)})
		if err != nil {
			log.Fatal(err)
		}
		if done {
			printInverseJob(map[string]any{"step": "done", "total": total})
			return
		}
	}
	printInverseJob(map[string]any{"step": "batch_limit", "total": total})
}

type inverseJobFlags struct {
	picks, picksSHA, owner, work string
	workers, items, batches      int
	maxBytes, budget24h          int64
}

const inverseJobUsage = "usage: stoaramactl r2-inverse job|job-keep28 --picks FILE --expected-picks-sha256 HEX --owner-decision FILE --work DIR [--workers N --max-items N --max-bytes N --budget-24h-bytes N --batches N]; job-keep28 requires --max-bytes and --budget-24h-bytes"

// parseInverseJobFlags reads job flags. RAW keeps its large batches and no
// rolling budget. Keep-28 defaults to the 100-object canary batch and runs only
// with an explicit per-batch byte cap and rolling 24h budget (0 = unlimited).
func parseInverseJobFlags(args []string, lane inverseLane) (inverseJobFlags, error) {
	f := inverseJobFlags{}
	items, maxBytes, budget := 5000, int64(1<<40), int64(0)
	if lane == inverseNASKeep28 {
		items, maxBytes, budget = 100, 0, -1
	}
	fs := flag.NewFlagSet("r2-inverse job", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&f.picks, "picks", "", "owner picks")
	fs.StringVar(&f.picksSHA, "expected-picks-sha256", "", "independently approved owner picks digest")
	fs.StringVar(&f.owner, "owner-decision", "", "owner decision record pinned by the executor")
	fs.StringVar(&f.work, "work", "", "new working directory")
	fs.IntVar(&f.workers, "workers", 16, "concurrent keys per batch (1..64, at least two DB connections remain reserved)")
	fs.IntVar(&f.items, "max-items", items, "maximum new objects per batch")
	fs.Int64Var(&f.maxBytes, "max-bytes", maxBytes, "maximum new intent bytes per batch")
	fs.Int64Var(&f.budget24h, "budget-24h-bytes", budget, "maximum charged bytes in previous 24h (0 = unlimited)")
	fs.IntVar(&f.batches, "batches", 1, "maximum batches")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || f.picks == "" || !r2RetireLowerHex64(f.picksSHA) || f.owner == "" || f.work == "" || f.workers < 1 || f.workers > 64 || f.items < 1 || f.maxBytes <= 0 || f.budget24h < 0 || f.batches < 1 {
		return f, errors.New(inverseJobUsage)
	}
	return f, nil
}

// runInverseJobBatch runs one bounded apply batch. It reports done when a clean
// batch selected nothing. Unresolved intents from earlier batches stay held;
// the executor's startup stops if one that never reached the wire is gone. A
// batch whose only failures are lost DELETE responses returns no error once its
// readback proves exactly those keys are unresolved, as long as it also deleted
// something; a batch that deletes nothing stops the job.
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
// keys it holds as lost responses, not just as many of them.
func (r inverseBatchReadback) settle(result inverseApplySummary, applyErr error) (bool, error) {
	wire := errors.Is(applyErr, errInverseWireAmbiguous)
	if applyErr != nil && !wire {
		return false, applyErr
	}
	if r.Absent != result.Deleted || r.AbsentBytes != result.Bytes || r.Attempts != r.Intents || !slices.Equal(r.Unresolved, result.WireAmbiguous) {
		return false, fmt.Errorf("database batch evidence differs: intents %d absent %d bytes %d attempts %d unresolved %v", r.Intents, r.Absent, r.AbsentBytes, r.Attempts, r.Unresolved)
	}
	if wire {
		if result.Deleted == 0 {
			return false, applyErr
		}
		printInverseJob(map[string]any{"step": "wire_ambiguous", "held": r.Unresolved, "error": applyErr.Error()})
		return false, nil
	}
	return result.Deleted == 0, nil
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
