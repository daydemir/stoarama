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
	expected := fs.String("expected-sha256", "", "reviewed SHA-256 of the manifest")
	ledger := fs.String("ledger", "", "new ledger JSONL path")
	workers := fs.Int("workers", 16, "parallel DeleteObjects requests")
	limit := fs.Int("limit", 0, "delete at most this many keys (0: all), for a canary")
	fs.Parse(args)
	if *manifest == "" || *expected == "" || *ledger == "" {
		return errors.New("--manifest, --expected-sha256 and --ledger are required")
	}
	got, err := fileSHA256(*manifest)
	if err != nil {
		return err
	}
	if got != *expected {
		return fmt.Errorf("manifest SHA-256 %s is not the reviewed %s", got, *expected)
	}
	st, err := storeFromEnv()
	if err != nil {
		return err
	}
	r, err := files.load()
	if err != nil {
		return err
	}
	return deleteManifest(ctx, st, r, *manifest, *ledger, *workers, *limit)
}

// deleteManifest re-decides every key against freshly loaded rules right
// before the wire; a key that no longer decides to the same delete class is
// skipped and recorded, never deleted.
func deleteManifest(ctx context.Context, st store, r *rules, manifest, ledgerPath string, workers, limit int) error {
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
			case outcomeSkippedProtected:
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
		if c, reason := r.decide(object{Key: row.Key, Size: row.Size, ETag: row.ETag, MTime: row.MTime}); c != row.Class || !c.deletes() {
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
	fmt.Fprintf(os.Stderr, "deleted %d, failed %d, skipped as protected %d\n", deleted, failed, skipped)
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
