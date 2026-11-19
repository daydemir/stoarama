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

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/jackc/pgx/v5/pgxpool"
)

// r2-inverse job runs the RAW inverse lane next to the database and bucket, as
// a provider one-off job. The executor serializes reserves and spends one round
// trip per statement, so it must not run across a slow home link. The job takes
// a fresh read-only partition, then runs bounded apply batches. Each batch gets
// fresh owner-fence evidence and an exact database readback. The first failure
// stops the job; nothing is retried.
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
// batch selected nothing.
func runInverseJobBatch(ctx context.Context, pool *pgxpool.Pool, store inverseDeleteStore, o inverseApplyOptions, owner, evidencePath string) (bool, inverseApplySummary, error) {
	pending, err := requireInversePendingIntentsPresent(ctx, pool, store)
	if err != nil {
		return false, inverseApplySummary{}, err
	}
	if o.DrainSHA, err = writeInverseOwnerEvidence(ctx, pool, store, o, owner, evidencePath); err != nil {
		return false, inverseApplySummary{}, err
	}
	o.DrainEvidence = evidencePath
	result, applyErr := applyInverse(ctx, pool, store, o)
	// Read this batch's durable outcomes on every path, including a failed one.
	var n, bytes, absent, attempts int64
	var unresolved []string
	if err = pool.QueryRow(ctx, `SELECT count(*),coalesce(sum(i.size_bytes),0),count(v.object_key),count(a.object_key),
 coalesce(array_agg(i.object_key ORDER BY i.object_key) FILTER (WHERE v.object_key IS NULL),'{}')
 FROM r2_inverse_intents i
 LEFT JOIN r2_inverse_events v ON v.bucket=i.bucket AND v.object_key=i.object_key AND v.event='absent'
 LEFT JOIN r2_inverse_attempts a ON a.bucket=i.bucket AND a.object_key=i.object_key
 WHERE i.bucket=$1 AND i.drain_evidence_sha256=$2`, store.Bucket(), o.DrainSHA).Scan(&n, &bytes, &absent, &attempts, &unresolved); err != nil {
		return false, result, errors.Join(applyErr, err)
	}
	printInverseJob(map[string]any{"step": "readback", "evidence_sha256": o.DrainSHA, "intents": n, "bytes": bytes, "absent": absent, "attempts": attempts, "unresolved": unresolved})
	if applyErr != nil {
		return false, result, applyErr
	}
	if result.Reconciled != 0 || result.AmbiguousHeld != pending {
		return false, result, fmt.Errorf("held intents changed: reconciled %d, ambiguous %d, expected %d", result.Reconciled, result.AmbiguousHeld, pending)
	}
	if n != result.Deleted || bytes != result.Bytes || absent != n || attempts != n {
		return false, result, fmt.Errorf("database batch evidence differs: intents %d bytes %d absent %d attempts %d", n, bytes, absent, attempts)
	}
	return result.Deleted == 0, result, nil
}

// An unresolved intent is never retransmitted; at startup the executor records
// absence for any that HEAD as missing. Require every one to still exist so a
// held key cannot be retired by an outside deletion.
func requireInversePendingIntentsPresent(ctx context.Context, pool *pgxpool.Pool, store inverseReader) (int64, error) {
	rows, err := pool.Query(ctx, `SELECT i.object_key FROM r2_inverse_intents i WHERE i.bucket=$1 AND NOT EXISTS(SELECT 1 FROM r2_inverse_events v WHERE v.bucket=i.bucket AND v.object_key=i.object_key AND v.event='absent') ORDER BY 1`, store.Bucket())
	if err != nil {
		return 0, err
	}
	var keys []string
	for rows.Next() {
		var key string
		if err = rows.Scan(&key); err != nil {
			rows.Close()
			return 0, err
		}
		keys = append(keys, key)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return 0, err
	}
	for _, key := range keys {
		probe, cancel := context.WithTimeout(ctx, inverseNetworkTimeout)
		_, err = store.Head(probe, key)
		cancel()
		if r2.IsNotFound(err) {
			return 0, fmt.Errorf("held intent %s is absent at the provider; review before any batch", key)
		}
		if err != nil {
			return 0, err
		}
	}
	return int64(len(keys)), nil
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
