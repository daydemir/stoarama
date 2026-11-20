package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/jackc/pgx/v5/pgxpool"
)

// storeInversePartition keeps a sealed partition directory as its bucket
// endpoint's one reusable partition in r2_inverse_partitions, replacing the
// previous one, and returns its digest.
func storeInversePartition(ctx context.Context, pool *pgxpool.Pool, dir string) (string, error) {
	summary, err := os.ReadFile(filepath.Join(dir, "summary.json"))
	if err != nil {
		return "", err
	}
	var sum inverseSummary
	if err = json.Unmarshal(summary, &sum); err != nil {
		return "", err
	}
	f, err := os.Open(filepath.Join(dir, "partition.jsonl"))
	if err != nil {
		return "", err
	}
	defer f.Close()
	digest := sha256.New()
	var packed bytes.Buffer
	z := gzip.NewWriter(&packed)
	if _, err = io.Copy(z, io.TeeReader(f, digest)); err != nil {
		return "", err
	}
	if err = z.Close(); err != nil {
		return "", err
	}
	sha := hex.EncodeToString(digest.Sum(nil))
	_, err = pool.Exec(ctx, `INSERT INTO r2_inverse_partitions(bucket,endpoint_sha256,partition_sha256,summary,partition_gzip) VALUES($1,$2,$3,$4,$5)
 ON CONFLICT(bucket,endpoint_sha256) DO UPDATE SET partition_sha256=excluded.partition_sha256,summary=excluded.summary,partition_gzip=excluded.partition_gzip`,
		sum.Bucket, sum.EndpointSHA256, sha, summary, packed.Bytes())
	return sha, err
}

// reuseInversePartition restores the stored partition with this exact digest
// into a new directory, for a restart. It requires the same bucket, endpoint
// and policy, picks equal to the independently approved digest, the 24h
// freshness every batch also requires, and unchanged external classification
// inputs: the stored holds digest must equal one read now. The partition only
// proposes candidates; it never authorizes a DELETE.
func reuseInversePartition(ctx context.Context, pool *pgxpool.Pool, store inverseLister, picksPath, approvedPicks, digest, dir string, reviewed bool) (inverseSummary, error) {
	var sum inverseSummary
	var summary, packed []byte
	if err := pool.QueryRow(ctx, `SELECT summary,partition_gzip FROM r2_inverse_partitions WHERE bucket=$1 AND endpoint_sha256=$2 AND partition_sha256=$3`,
		store.Bucket(), store.EndpointSHA256(), digest).Scan(&summary, &packed); err != nil {
		return sum, fmt.Errorf("no stored partition %s for this bucket endpoint: %w", digest, err)
	}
	if err := writeInverseReusedPartition(dir, summary, packed, digest); err != nil {
		return sum, err
	}
	if err := json.Unmarshal(summary, &sum); err != nil {
		return sum, err
	}
	picks, err := loadR2RetirePicks(picksPath)
	if err != nil {
		return sum, err
	}
	policy, windows, err := api.Keep28Policy()
	if err != nil {
		return sum, err
	}
	if picks.SHA256 != approvedPicks {
		return sum, errors.New("owner picks differ from the approved digest")
	}
	if err = sum.matches(store, policy, approvedPicks); err != nil {
		return sum, err
	}
	tx, err := beginInversePartitionTx(ctx, pool)
	if err != nil {
		return sum, err
	}
	defer tx.Rollback(context.Background())
	var now time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return sum, err
	}
	if err = sum.fresh(now); err != nil {
		return sum, err
	}
	p, err := loadInverseHolds(ctx, tx, store.Bucket(), picks, windows)
	if err != nil {
		return sum, err
	}
	// A stored partition is reused only in the scope it was classified in: the
	// page-exemption and collated-mapping inputs are part of the holds digest, so
	// a RAW-scope partition can never carry a reviewed-scope decision, or the
	// reverse.
	p.ReviewedCollated = reviewed
	holds, err := p.holdsSHA256()
	if err != nil {
		return sum, err
	}
	if holds != sum.ProtectionSHA256 {
		return sum, errors.New("protection inputs changed since the partition; take a fresh partition")
	}
	return sum, nil
}

// writeInverseReusedPartition writes a stored partition's files into a new
// directory and requires the restored partition to have the requested digest.
func writeInverseReusedPartition(dir string, summary, packed []byte, digest string) error {
	if err := os.Mkdir(dir, 0700); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "summary.json"), summary, 0600); err != nil {
		return err
	}
	z, err := gzip.NewReader(bytes.NewReader(packed))
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, "partition.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	h := sha256.New()
	_, err = io.Copy(io.MultiWriter(f, h), z)
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if hex.EncodeToString(h.Sum(nil)) != digest {
		return errors.New("stored partition differs from its digest")
	}
	return nil
}
