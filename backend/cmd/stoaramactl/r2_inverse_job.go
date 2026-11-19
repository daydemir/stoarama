package main

import (
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

// r2-inverse job runs the RAW inverse lane next to the database and bucket, as
// a provider one-off job. The executor serializes reserves and spends one round
// trip per statement, so it must not run across a slow home link. The job takes
// a fresh read-only partition, then runs bounded apply batches. Each batch gets
// fresh owner-fence evidence and an exact database readback. A DELETE that lost
// its response holds only its own key, permanently. Any other failure stops the
// job; nothing is retried.
func runR2InverseJob(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("r2-inverse job", flag.ExitOnError)
	picks := fs.String("picks", "", "owner picks")
	picksSHA := fs.String("expected-picks-sha256", "", "independently approved owner picks digest")
	owner := fs.String("owner-decision", "", "owner decision record pinned by the executor")
	work := fs.String("work", "", "new working directory")
	workers := fs.Int("workers", 16, "concurrent keys per batch")
	items := fs.Int("max-items", 5000, "maximum new objects per batch")
	maxBytes := fs.Int64("max-bytes", 1<<40, "maximum new intent bytes per batch")
	batches := fs.Int("batches", 1, "maximum batches")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || *picks == "" || !r2RetireLowerHex64(*picksSHA) || *owner == "" || *work == "" || *items < 1 || *maxBytes <= 0 || *batches < 1 {
		log.Fatal("usage: stoaramactl r2-inverse job --picks FILE --expected-picks-sha256 HEX --owner-decision FILE --work DIR [--workers N --max-items N --max-bytes N --batches N]")
	}
	// The partition and every apply check derive from this file, so it must be
	// the owner-approved picks, pinned independently of the file itself.
	if digest, err := inverseFileSHA(*picks); err != nil || digest != *picksSHA {
		log.Fatal("owner picks differ from the approved digest")
	}
	if err := os.Mkdir(*work, 0700); err != nil {
		log.Fatal(err)
	}
	partition := filepath.Join(*work, "partition")
	sum, err := writeInversePartition(ctx, cfg, *picks, partition)
	if err != nil {
		log.Fatal(err)
	}
	// Bind the approved digest to the bytes the partition actually read; every
	// apply re-reads the picks file and must match this summary digest.
	if sum.PicksSHA256 != *picksSHA {
		log.Fatal("partition read owner picks other than the approved digest")
	}
	printInverseJob(map[string]any{"step": "partition", "summary": sum})
	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	store, err := newInverseSDKStore(ctx, archiveR2Config(cfg, pool))
	if err != nil {
		log.Fatal(err)
	}
	o := inverseApplyOptions{Lane: inverseOutsideKeep28, Manifest: filepath.Join(partition, "partition.jsonl"), Summary: filepath.Join(partition, "summary.json"), Picks: *picks, Ledger: filepath.Join(*work, "ledger.jsonl"), Workers: *workers, MaxItems: *items, MaxBytes: *maxBytes}
	if o.ManifestSHA, err = inverseFileSHA(o.Manifest); err != nil {
		log.Fatal(err)
	}
	if o.SummarySHA, err = inverseFileSHA(o.Summary); err != nil {
		log.Fatal(err)
	}
	total := inverseApplySummary{}
	for n := 1; n <= *batches; n++ {
		done, result, err := runInverseJobBatch(ctx, pool, store, o, *owner, filepath.Join(*work, fmt.Sprintf("evidence-%03d.json", n)))
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
	var n, absent, absentBytes, attempts int64
	unresolved := []string{}
	if err = pool.QueryRow(ctx, `SELECT count(*),count(v.object_key),coalesce(sum(i.size_bytes) FILTER (WHERE v.object_key IS NOT NULL),0),count(a.object_key),
 coalesce(array_agg(i.object_key ORDER BY i.object_key) FILTER (WHERE v.object_key IS NULL),'{}')
 FROM r2_inverse_intents i
 LEFT JOIN r2_inverse_events v ON v.bucket=i.bucket AND v.object_key=i.object_key AND v.event='absent'
 LEFT JOIN r2_inverse_attempts a ON a.bucket=i.bucket AND a.object_key=i.object_key
 WHERE i.bucket=$1 AND i.drain_evidence_sha256=$2`, store.Bucket(), o.DrainSHA).Scan(&n, &absent, &absentBytes, &attempts, &unresolved); err != nil {
		return false, result, errors.Join(applyErr, err)
	}
	// Compare key sets in one byte order; the database orders by its collation.
	sort.Strings(unresolved)
	printInverseJob(map[string]any{"step": "readback", "evidence_sha256": o.DrainSHA, "intents": n, "absent": absent, "absent_bytes": absentBytes, "attempts": attempts, "unresolved": unresolved})
	wire := errors.Is(applyErr, errInverseWireAmbiguous)
	if applyErr != nil && !wire {
		return false, result, applyErr
	}
	if absent != result.Deleted || absentBytes != result.Bytes || attempts != n || !slices.Equal(unresolved, result.WireAmbiguous) {
		return false, result, fmt.Errorf("database batch evidence differs: intents %d absent %d bytes %d attempts %d unresolved %v", n, absent, absentBytes, attempts, unresolved)
	}
	if wire {
		if result.Deleted == 0 {
			return false, result, applyErr
		}
		printInverseJob(map[string]any{"step": "wire_ambiguous", "held": unresolved, "error": applyErr.Error()})
		return false, result, nil
	}
	return result.Deleted == 0, result, nil
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
