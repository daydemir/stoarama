package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"golang.org/x/sync/errgroup"
)

const deleteBatch = 1000 // S3/R2 DeleteObjects limit

type outcome string

const (
	outcomeDeleted          outcome = "deleted"
	outcomeFailed           outcome = "failed"
	outcomeSkippedProtected outcome = "skipped_protected"
	outcomeSkippedChanged   outcome = "skipped_changed"
)

type ledgerRow struct {
	Key     string  `json:"key"`
	Size    int64   `json:"size"`
	Outcome outcome `json:"outcome"`
	Detail  string  `json:"detail,omitempty"`
	At      string  `json:"at"`
}

func runDelete(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("delete", flag.ExitOnError)
	var files ruleFiles
	files.register(fs)
	manifest := fs.String("manifest", "", "delete-manifest.jsonl from plan")
	summaryPath := fs.String("summary", "", "summary.json the reviewed plan wrote beside the manifest")
	freshPath := fs.String("fresh-listing", "", "listing taken right before this run; a key whose ETag, size or mtime changed since the plan is skipped")
	expected := fs.String("expected-sha256", "", "reviewed SHA-256 of the manifest")
	ledger := fs.String("ledger", "", "new ledger JSONL path")
	workers := fs.Int("workers", 16, "parallel DeleteObjects requests")
	limit := fs.Int("limit", 0, "delete at most this many keys (0: all), for a canary")
	fs.Parse(args)
	if *manifest == "" || *expected == "" || *ledger == "" || *summaryPath == "" || *freshPath == "" {
		return errors.New("--manifest, --summary, --fresh-listing, --expected-sha256 and --ledger are required")
	}
	run, err := verifyReviewedInputs(*manifest, *summaryPath, *expected, files)
	if err != nil {
		return err
	}
	st, err := storeFromEnv()
	if err != nil {
		return err
	}
	r, err := files.load()
	if err != nil {
		return err
	}
	if run.FreshListingSHA256, err = fileSHA256(*freshPath); err != nil {
		return err
	}
	if r.Fresh, err = loadFresh(*freshPath, *manifest); err != nil {
		return err
	}
	// Coverage outputs are judged against the same fresh listing, never the
	// plan-time snapshot.
	if r.Outputs, err = scanOutputSizes(*freshPath, r.OutputKeys); err != nil {
		return err
	}
	return deleteManifest(ctx, st, r, run, *manifest, *ledger, *workers, *limit)
}

// runHeader is the ledger's first line: exactly what this run was proven against.
type runHeader struct {
	Type           string            `json:"type"`
	ManifestSHA256 string            `json:"manifest_sha256"`
	Inputs         map[string]string `json:"inputs_sha256"`
	RecentFrom     string            `json:"recent_from"`
	Release        string            `json:"release"`
	// FreshListingSHA256 pins the listing each key's identity was re-checked against.
	FreshListingSHA256 string `json:"fresh_listing_sha256"`
	Limit              int    `json:"limit"`
	StartedAt          string `json:"started_at"`
}

// verifyReviewedInputs refuses unless the manifest and every rule file the
// re-check reads are byte-identical to the reviewed plan's, and recent-from
// is the one that plan used.
func verifyReviewedInputs(manifest, summaryPath, expected string, files ruleFiles) (runHeader, error) {
	run := runHeader{Type: "run"}
	raw, err := os.ReadFile(summaryPath)
	if err != nil {
		return run, err
	}
	var sum summary
	if err := json.Unmarshal(raw, &sum); err != nil {
		return run, err
	}
	if run.ManifestSHA256, err = fileSHA256(manifest); err != nil {
		return run, err
	}
	if run.ManifestSHA256 != expected || sum.ManifestSHA256 != expected {
		return run, fmt.Errorf("manifest SHA-256 %s, summary %s, reviewed %s must all match", run.ManifestSHA256, sum.ManifestSHA256, expected)
	}
	if files.recentFrom != sum.RecentFrom {
		return run, fmt.Errorf("--recent-from %q is not the plan's %q", files.recentFrom, sum.RecentFrom)
	}
	if run.Inputs, err = files.digests(); err != nil {
		return run, err
	}
	if files.release != sum.Release {
		return run, fmt.Errorf("--release %q is not the plan's %q", files.release, sum.Release)
	}
	reviewed := map[string]string{}
	for name, digest := range sum.Inputs {
		if name != "snapshot" {
			reviewed[name] = digest
		}
	}
	if len(run.Inputs) != len(reviewed) {
		return run, fmt.Errorf("rule inputs %v are not the reviewed set %v", sortedKeysOf(run.Inputs), sortedKeysOf(reviewed))
	}
	for name, got := range run.Inputs {
		if got != reviewed[name] {
			return run, fmt.Errorf("%s SHA-256 %s is not the reviewed %s", name, got, reviewed[name])
		}
	}
	run.RecentFrom, run.Release = sum.RecentFrom, sum.Release
	return run, nil
}

// deleteManifest re-decides every key against freshly loaded rules right
// before the wire; a key that no longer decides to the same delete class is
// skipped and recorded, never deleted.
func deleteManifest(ctx context.Context, st store, r *rules, run runHeader, manifest, ledgerPath string, workers, limit int) error {
	if st.bucket != r.Bucket {
		return fmt.Errorf("R2_BUCKET %q is not the protect file's bucket %q", st.bucket, r.Bucket)
	}
	lf, err := os.OpenFile(ledgerPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer lf.Close()
	var mu sync.Mutex
	lw := bufio.NewWriter(lf)
	enc := json.NewEncoder(lw)
	run.Limit, run.StartedAt = limit, time.Now().UTC().Format(time.RFC3339Nano)
	if err := enc.Encode(run); err != nil {
		return err
	}
	var deleted, failed, skipped int64
	record := func(rows []ledgerRow) error {
		mu.Lock()
		defer mu.Unlock()
		for _, row := range rows {
			row.At = time.Now().UTC().Format(time.RFC3339Nano)
			switch row.Outcome {
			case outcomeDeleted:
				deleted++
			case outcomeFailed:
				failed++
			case outcomeSkippedProtected, outcomeSkippedChanged:
				skipped++
			}
			if err := enc.Encode(row); err != nil {
				return err
			}
		}
		return lw.Flush()
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)
	var batch []manifestRow
	flush := func() {
		b := batch
		batch = nil
		g.Go(func() error { return deleteBatchOnce(gctx, st, b, record) })
	}
	n := 0
	err = eachLine(manifest, func(line string) error {
		var row manifestRow
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return err
		}
		if limit > 0 && n >= limit {
			return nil
		}
		n++
		planned := object{Key: row.Key, Size: row.Size, ETag: row.ETag, MTime: row.MTime}
		if now, ok := r.Fresh[row.Key]; !ok || now != planned {
			detail := "absent_from_fresh_listing"
			if ok {
				detail = fmt.Sprintf("now size=%d etag=%s mtime=%d", now.Size, now.ETag, now.MTime)
			}
			return record([]ledgerRow{{Key: row.Key, Size: row.Size, Outcome: outcomeSkippedChanged, Detail: detail}})
		}
		if c, reason := r.decide(planned); c != row.Class || !c.deletes() {
			return record([]ledgerRow{{Key: row.Key, Size: row.Size, Outcome: outcomeSkippedProtected, Detail: string(c) + ":" + reason}})
		}
		batch = append(batch, row)
		if len(batch) == deleteBatch {
			flush()
		}
		return nil
	})
	if err == nil && len(batch) > 0 {
		flush()
	}
	if werr := g.Wait(); err == nil {
		err = werr
	}
	fmt.Fprintf(os.Stderr, "deleted %d, failed %d, skipped (protected or changed) %d\n", deleted, failed, skipped)
	if err == nil && failed > 0 {
		err = fmt.Errorf("%d keys failed; see the ledger", failed)
	}
	return err
}

func deleteBatchOnce(ctx context.Context, st store, rows []manifestRow, record func([]ledgerRow) error) error {
	ids := make([]types.ObjectIdentifier, len(rows))
	sizes := make(map[string]int64, len(rows))
	for i, row := range rows {
		ids[i] = types.ObjectIdentifier{Key: aws.String(row.Key)}
		sizes[row.Key] = row.Size
	}
	out, err := st.s3.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(st.bucket), Delete: &types.Delete{Objects: ids, Quiet: aws.Bool(false)}})
	if err != nil {
		failedRows := make([]ledgerRow, len(rows))
		for i, row := range rows {
			failedRows[i] = ledgerRow{Key: row.Key, Size: row.Size, Outcome: outcomeFailed, Detail: err.Error()}
		}
		return record(failedRows)
	}
	result := make([]ledgerRow, 0, len(rows))
	for _, d := range out.Deleted {
		k := aws.ToString(d.Key)
		result = append(result, ledgerRow{Key: k, Size: sizes[k], Outcome: outcomeDeleted})
		delete(sizes, k)
	}
	for _, e := range out.Errors {
		k := aws.ToString(e.Key)
		result = append(result, ledgerRow{Key: k, Size: sizes[k], Outcome: outcomeFailed, Detail: aws.ToString(e.Code) + ": " + aws.ToString(e.Message)})
		delete(sizes, k)
	}
	for k, size := range sizes {
		result = append(result, ledgerRow{Key: k, Size: size, Outcome: outcomeFailed, Detail: "absent from DeleteObjects response"})
	}
	return record(result)
}

func sortedKeysOf(m map[string]string) []string {
	keys := map[string]bool{}
	for k := range m {
		keys[k] = true
	}
	return sortedKeys(keys)
}

// loadFresh reads the fresh listing rows for the manifest's keys only.
func loadFresh(listing, manifest string) (map[string]object, error) {
	want := map[string]bool{}
	if err := eachLine(manifest, func(line string) error {
		var row manifestRow
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return err
		}
		want[row.Key] = true
		return nil
	}); err != nil {
		return nil, err
	}
	fresh := make(map[string]object, len(want))
	err := eachSnapshot(listing, func(o object) error {
		if want[o.Key] {
			fresh[o.Key] = o
		}
		return nil
	})
	return fresh, err
}
