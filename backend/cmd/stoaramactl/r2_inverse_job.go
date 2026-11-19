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
	laneName := fs.String("lane", "raw", "raw (outside keep28, uncollated) or collated (mapped collated footage)")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || *picks == "" || !r2RetireLowerHex64(*picksSHA) || *owner == "" || *work == "" || *items < 1 || *maxBytes <= 0 || *batches < 1 {
		log.Fatal("usage: stoaramactl r2-inverse job --picks FILE --expected-picks-sha256 HEX --owner-decision FILE --work DIR [--lane raw|collated --workers N --max-items N --max-bytes N --batches N]")
	}
	lanes := map[string]inverseLane{"raw": inverseOutsideKeep28, "collated": inverseNonrawCollated}
	lane, ok := lanes[*laneName]
	if !ok {
		log.Fatal("--lane must be raw or collated")
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
	// The job always runs under owner-fence evidence, so both lanes partition in
	// the reviewed owner scope: RAW delivery keys and mapped collated footage.
	sum, err := writeInversePartition(ctx, cfg, *picks, partition, true)
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
	// Deniz, 2026-11-12: keep up only the links on the public delivery page.
	// Before any delete, no candidate this job can select may be one of them.
	candidates, pageKeep, overlap, err := inverseJobPageKeepOverlap(ctx, pool, store.Bucket(), filepath.Join(partition, "partition.jsonl"), lane)
	if err != nil {
		log.Fatal(err)
	}
	printInverseJob(map[string]any{"step": "page_keep_check", "candidates": candidates, "page_keep_holds": pageKeep, "overlap": overlap})
	if overlap != 0 {
		log.Fatal("partition candidates include public delivery page keys")
	}
	o := inverseApplyOptions{Lane: lane, Manifest: filepath.Join(partition, "partition.jsonl"), Summary: filepath.Join(partition, "summary.json"), Picks: *picks, Ledger: filepath.Join(*work, "ledger.jsonl"), Workers: *workers, MaxItems: *items, MaxBytes: *maxBytes}
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
	tables := &inverseApplier{opts: o}
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
 FROM `+tables.intentTable()+` i
 LEFT JOIN `+tables.eventTable()+` v ON v.bucket=i.bucket AND v.object_key=i.object_key AND v.event='absent'
 LEFT JOIN `+tables.attemptTable()+` a ON a.bucket=i.bucket AND a.object_key=i.object_key
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

// inverseJobPageKeepOverlap counts the lane's partition candidates, the exact
// public delivery page keys on record, and the candidates that are page keys.
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
	err = pool.QueryRow(ctx, `SELECT count(*),count(*) FILTER (WHERE object_key=ANY($2::text[])) FROM delivery_keep_holds WHERE bucket=$1`, bucket, keys).Scan(&held, &overlap)
	return len(keys), held, overlap, err
}
