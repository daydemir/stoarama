package main

import (
	"bufio"
	"compress/gzip"
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
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

// Manifest batches bound operator review/import size; they never limit transfer rate.
const nasCopyBatchBytes int64 = 100_000_000_000

// Each item costs ~6 database round trips; from Jelly that is ~0.5 s, so
// queue throughput is latency-bound and scales with workers.
const nasCopyQueueWorkers = 16

// A lease holding a row an item touches refuses its insert with 55P03 after
// the 2 s lock_timeout. Retry only that item; failing the manifest made every
// rerun revalidate the items already committed.
const nasCopyQueueLockAttempts = 8

type nasCopyHeader struct {
	Version      int    `json:"version"`
	PolicySHA256 string `json:"policy_sha256"`
	SourceSHA256 string `json:"source_sha256"`
	Items        int64  `json:"items"`
	Bytes        int64  `json:"bytes"`
}
type nasCopyStat struct {
	Objects int64 `json:"objects"`
	Bytes   int64 `json:"bytes"`
}
type nasCopySummary struct {
	SourceSHA256    string                           `json:"source_sha256"`
	PolicySHA256    string                           `json:"policy_sha256"`
	Raw             nasCopyStat                      `json:"raw"`
	CollatedBlocked nasCopyStat                      `json:"collated_blocked"`
	Streams         map[int64]map[string]nasCopyStat `json:"streams"`
	Batches         map[string]string                `json:"batches_sha256"`
}

// Input is the immutable retirement copy classification, not an executable
// retirement plan. Both kinds are accounted for; only raw batches can queue.
func planNASR2Copy(input, out, expected string) (nasCopySummary, error) {
	sum := nasCopySummary{Streams: map[int64]map[string]nasCopyStat{}, Batches: map[string]string{}}
	policy, _, err := api.Keep28Policy()
	if err != nil {
		return sum, err
	}
	sum.PolicySHA256 = policy
	f, err := os.Open(input)
	if err != nil {
		return sum, err
	}
	defer f.Close()
	digest := sha256.New()
	if _, err = io.Copy(digest, f); err != nil {
		return sum, err
	}
	sum.SourceSHA256 = hex.EncodeToString(digest.Sum(nil))
	if len(expected) != 64 || sum.SourceSHA256 != expected {
		return sum, errors.New("source SHA256 differs")
	}
	if _, err = f.Seek(0, 0); err != nil {
		return sum, err
	}
	var reader io.Reader = f
	if strings.HasSuffix(input, ".gz") {
		z, e := gzip.NewReader(f)
		if e != nil {
			return sum, e
		}
		defer z.Close()
		reader = z
	}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 65536), 2<<20)
	if !scanner.Scan() {
		return sum, errors.New("missing source header")
	}
	var envelope struct {
		Header struct {
			Version      int    `json:"version"`
			Phase        string `json:"phase"`
			PolicySHA256 string `json:"policy_sha256"`
			Items        int64  `json:"items"`
			Bytes        int64  `json:"bytes"`
			ConnectionID int64  `json:"connection_id"`
			AccountID    int64  `json:"account_id"`
		} `json:"header"`
	}
	if json.Unmarshal(scanner.Bytes(), &envelope) != nil || envelope.Header.Version != 2 || envelope.Header.Phase != "copy" || envelope.Header.PolicySHA256 != policy || envelope.Header.ConnectionID != 13 || envelope.Header.AccountID != 47 {
		return sum, errors.New("invalid copy classification header")
	}
	// Validate the whole input before creating an output directory. This is
	// operator-side metadata; the NAS never loads these manifests.
	seen := map[string]bool{}
	paths := map[string]string{}
	var count, bytes int64
	batches := [][]api.NASR2CopyItem{}
	batch := []api.NASR2CopyItem{}
	var batchBytes int64
	collated := []api.NASR2CopyItem{}
	for scanner.Scan() {
		var i api.NASR2CopyItem
		var reason struct {
			Reason string `json:"reason"`
		}
		if json.Unmarshal(scanner.Bytes(), &i) != nil || json.Unmarshal(scanner.Bytes(), &reason) != nil || reason.Reason != "copy_to_nas" {
			return sum, errors.New("invalid copy classification row")
		}
		if err = api.ValidateNASR2CopyItem(i); err != nil {
			return sum, err
		}
		key := fmt.Sprintf("%s/%d", i.Kind, i.SourceID)
		path := string(i.Kind) + "/" + i.RelativePath
		if seen[key] || paths[path] != "" {
			return sum, errors.New("duplicate copy source or destination; reconcile before planning")
		}
		seen[key] = true
		paths[path] = key
		count++
		bytes += i.SizeBytes
		if sum.Streams[i.StreamID] == nil {
			sum.Streams[i.StreamID] = map[string]nasCopyStat{}
		}
		st := sum.Streams[i.StreamID][string(i.Kind)]
		st.Objects++
		st.Bytes += i.SizeBytes
		sum.Streams[i.StreamID][string(i.Kind)] = st
		if i.Kind == "collated" {
			sum.CollatedBlocked.Objects++
			sum.CollatedBlocked.Bytes += i.SizeBytes
			collated = append(collated, i)
			continue
		}
		sum.Raw.Objects++
		sum.Raw.Bytes += i.SizeBytes
		if len(batch) >= 5000 || batchBytes+i.SizeBytes > nasCopyBatchBytes {
			batches = append(batches, batch)
			batch = nil
			batchBytes = 0
		}
		batch = append(batch, i)
		batchBytes += i.SizeBytes
	}
	if err = scanner.Err(); err != nil {
		return sum, err
	}
	if count != envelope.Header.Items || bytes != envelope.Header.Bytes {
		return sum, errors.New("source classification totals differ")
	}
	if len(batch) > 0 {
		batches = append(batches, batch)
	}
	if err = os.Mkdir(out, 0700); err != nil {
		return sum, err
	}
	// A failed write preserves its directory for inspection; no input is changed.
	write := func(name string, items []api.NASR2CopyItem) error {
		var n int64
		for _, i := range items {
			n += i.SizeBytes
		}
		h := nasCopyHeader{1, policy, sum.SourceSHA256, int64(len(items)), n}
		dest, e := os.OpenFile(filepath.Join(out, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if e != nil {
			return e
		}
		hash := sha256.New()
		enc := json.NewEncoder(io.MultiWriter(dest, hash))
		e = enc.Encode(map[string]any{"header": h})
		for _, i := range items {
			if e == nil {
				e = enc.Encode(i)
			}
		}
		if e == nil {
			e = dest.Sync()
		}
		ce := dest.Close()
		if e != nil {
			return e
		}
		if ce != nil {
			return ce
		}
		sum.Batches[name] = hex.EncodeToString(hash.Sum(nil))
		return nil
	}
	for n, items := range batches {
		if err = write(fmt.Sprintf("raw-%04d.jsonl", n), items); err != nil {
			return sum, err
		}
	}
	if err = write("collated-blocked.jsonl", collated); err != nil {
		return sum, err
	}
	data, err := json.MarshalIndent(sum, "", "  ")
	if err != nil {
		return sum, err
	}
	err = os.WriteFile(filepath.Join(out, "summary.json"), append(data, '\n'), 0600)
	return sum, err
}

func readNASCopyBatch(filename, expected string) (nasCopyHeader, []api.NASR2CopyItem, error) {
	var h nasCopyHeader
	f, err := os.Open(filename)
	if err != nil {
		return h, nil, err
	}
	defer f.Close()
	hash := sha256.New()
	scanner := bufio.NewScanner(io.TeeReader(f, hash))
	scanner.Buffer(make([]byte, 65536), 2<<20)
	if !scanner.Scan() {
		return h, nil, errors.New("missing manifest header")
	}
	var e struct {
		Header nasCopyHeader `json:"header"`
	}
	if json.Unmarshal(scanner.Bytes(), &e) != nil {
		return h, nil, errors.New("invalid manifest")
	}
	h = e.Header
	policy, _, err := api.Keep28Policy()
	if err != nil {
		return h, nil, err
	}
	if h.Version != 1 || h.PolicySHA256 != policy || len(h.SourceSHA256) != 64 || h.Items <= 0 || h.Items > 5000 || h.Bytes <= 0 || h.Bytes > nasCopyBatchBytes {
		return h, nil, errors.New("invalid manifest totals or policy")
	}
	items := []api.NASR2CopyItem{}
	seen := map[int64]bool{}
	paths := map[string]bool{}
	var n int64
	for scanner.Scan() {
		var i api.NASR2CopyItem
		if json.Unmarshal(scanner.Bytes(), &i) != nil {
			return h, nil, errors.New("invalid manifest item")
		}
		if err = api.ValidateNASR2CopyItem(i); err != nil {
			return h, nil, err
		}
		if i.Kind != "raw" || seen[i.SourceID] || paths[i.RelativePath] || len(items) >= 5000 {
			return h, nil, errors.New("blocked kind or duplicate manifest item")
		}
		seen[i.SourceID] = true
		paths[i.RelativePath] = true
		items = append(items, i)
		n += i.SizeBytes
	}
	if err = scanner.Err(); err != nil {
		return h, nil, err
	}
	if int64(len(items)) != h.Items || n != h.Bytes || len(expected) != 64 || hex.EncodeToString(hash.Sum(nil)) != expected {
		return h, nil, errors.New("manifest SHA256 or totals differ")
	}
	return h, items, nil
}

func nasR2CopyPoolConfig(databaseURL string, apply bool) (*pgxpool.Config, error) {
	pc, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	pc.MaxConns = nasCopyQueueWorkers
	pc.ConnConfig.RuntimeParams["statement_timeout"] = "10s"
	pc.ConnConfig.RuntimeParams["work_mem"] = "16MB"
	pc.ConnConfig.RuntimeParams["application_name"] = "stoarama-nas-r2-copy"
	if !apply {
		pc.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	}
	return pc, nil
}

type nasCopyDB interface {
	Begin(context.Context) (pgx.Tx, error)
}

func queueNASR2CopyItem(ctx context.Context, pool nasCopyDB, h nasCopyHeader, manifestSHA string, i api.NASR2CopyItem, apply bool) error {
	if err := ctx.Err(); err != nil {
		return finishDBError(finishImportBegin, 0, err)
	}
	tx, err := api.BeginNASR2CopyTx(ctx, pool)
	if err != nil {
		return finishDBError(finishImportBegin, 0, err)
	}
	defer func() {
		// Cancellation must not skip rollback. A broken connection still has
		// a hard cleanup deadline and is discarded by pgx on rollback failure.
		cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	if _, err = api.LoadNASR2CopySource(ctx, tx, i); err != nil {
		var read *api.NASR2CopySourceReadError
		if errors.As(err, &read) {
			return finishDBError(finishImportSource, 0, err)
		}
		return err
	}
	if !apply {
		return tx.Rollback(ctx)
	}
	data, _ := json.Marshal(i)
	var old []byte
	err = tx.QueryRow(ctx, `INSERT INTO nas_r2_copy_items(connection_id,kind,source_id,manifest_sha256,policy_sha256,item) VALUES(13,$1,$2,$3,$4,$5) ON CONFLICT(connection_id,kind,source_id) DO UPDATE SET source_id=excluded.source_id RETURNING item`, i.Kind, i.SourceID, manifestSHA, h.PolicySHA256, data).Scan(&old)
	if err != nil {
		return finishDBError(finishImportInsert, 0, err)
	}
	var existing api.NASR2CopyItem
	if json.Unmarshal(old, &existing) != nil || existing != i {
		return errors.New("queued source identity differs; preserved")
	}
	return finishDBError(finishImportCommit, 0, tx.Commit(ctx))
}

func queueNASR2CopyItemRetryingLocks(ctx context.Context, pool nasCopyDB, h nasCopyHeader, manifestSHA string, i api.NASR2CopyItem, apply bool) error {
	for attempt := 1; ; attempt++ {
		err := queueNASR2CopyItem(ctx, pool, h, manifestSHA, i, apply)
		var pg *pgconn.PgError
		if !errors.As(err, &pg) || pg.Code != "55P03" || attempt == nasCopyQueueLockAttempts {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(time.Duration(attempt) * 250 * time.Millisecond):
		}
	}
}

func queueNASR2Copy(ctx context.Context, pool nasCopyDB, h nasCopyHeader, manifestSHA string, items []api.NASR2CopyItem, apply bool) error {
	group, work := errgroup.WithContext(ctx)
	var next atomic.Uint32
	// Only four goroutines exist regardless of manifest size. Any failure can
	// leave an arbitrary valid subset committed; exact-manifest restarts are
	// idempotent. Wait joins all started work before the CLI reports failure.
	for range nasCopyQueueWorkers {
		group.Go(func() error {
			for {
				n := int(next.Add(1) - 1)
				if n >= len(items) {
					return nil
				}
				if err := queueNASR2CopyItemRetryingLocks(work, pool, h, manifestSHA, items[n], apply); err != nil {
					return fmt.Errorf("copy source %d: %w", items[n].SourceID, err)
				}
			}
		})
	}
	return group.Wait()
}

func runNASR2Copy(ctx context.Context, cfg config.Config, args []string) {
	if len(args) == 0 {
		log.Fatal("nas-r2-copy plan|queue|status|activate-keep20")
	}
	fs := flag.NewFlagSet("nas-r2-copy", flag.ExitOnError)
	input := fs.String("input", "", "source copy.jsonl[.gz] or batch")
	sha := fs.String("sha256", "", "required exact input file SHA256")
	out := fs.String("out", "", "new output directory")
	jsonOutput := fs.Bool("json", false, "machine-readable status")
	apply := fs.Bool("apply", false, "queue validated raw work; default read-only")
	maintenanceReason := fs.String("maintenance-reason", "", "activate-keep20 only: release the matching maintenance fence during activation")
	_ = fs.Parse(args[1:])
	if fs.NArg() != 0 {
		log.Fatal("unexpected arguments")
	}
	if args[0] == "plan" {
		sum, err := planNASR2Copy(*input, *out, *sha)
		if err != nil {
			log.Fatal(err)
		}
		_ = json.NewEncoder(os.Stdout).Encode(sum)
		return
	}
	pc, err := nasR2CopyPoolConfig(cfg.DatabaseURL, *apply)
	if err != nil {
		log.Fatal("invalid database configuration")
	}
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		log.Fatal("copy database unavailable")
	}
	defer pool.Close()
	if args[0] == "activate-keep20" {
		if !*apply || !r2RetireLowerHex64(*sha) {
			log.Fatal("activate-keep20 requires --apply and the reviewed policy --sha256")
		}
		var activated time.Time
		err := pool.QueryRow(ctx, `SELECT activate_nas_media_keep20($1,$2)`, *sha, *maintenanceReason).Scan(&activated)
		if err != nil {
			log.Fatal("20-day NAS work activation refused: ", err)
		}
		fmt.Printf("nas_keep20_active_at=%s policy_sha256=%s\n", activated.UTC().Format(time.RFC3339Nano), *sha)
		return
	}
	if args[0] == "status" {
		if *jsonOutput {
			status, err := loadNASCopyStatus(ctx, pool)
			if err != nil {
				log.Fatal(err)
			}
			if err := json.NewEncoder(os.Stdout).Encode(status); err != nil {
				log.Fatal(err)
			}
			return
		}
		fmt.Printf("enabled_by=NAS_R2_COPY_ENABLED floor_bytes=%d daily_limit=none rate_limit=none workers=%d range_bytes=%d\n", api.NASR2CopyFloor, api.NASR2CopyWorkers, api.NASR2CopyRangeBytes)
		rows, err := pool.Query(ctx, `SELECT kind,state,count(*),coalesce(sum((item->>'size_bytes')::bigint),0) FROM nas_r2_copy_items WHERE connection_id=13 GROUP BY kind,state ORDER BY kind,state`)
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var k, s string
			var n, b int64
			if err = rows.Scan(&k, &s, &n, &b); err != nil {
				log.Fatal(err)
			}
			fmt.Printf("kind=%s state=%s objects=%d bytes=%d\n", k, s, n, b)
		}
		if rows.Err() != nil {
			log.Fatal(rows.Err())
		}
		return
	}
	if args[0] != "queue" {
		log.Fatal("nas-r2-copy plan|queue|status|activate-keep20")
	}
	h, items, err := readNASCopyBatch(*input, *sha)
	if err != nil {
		log.Fatal(err)
	}
	if err = queueNASR2Copy(ctx, pool, h, *sha, items, *apply); err != nil {
		pool.Close() // log.Fatal skips defers; also finish pgx's connection cleanup.
		log.Fatal(err)
	}
	fmt.Printf("validated=%d queued=%t switch_unchanged=true deletes=0\n", len(items), *apply)
}
