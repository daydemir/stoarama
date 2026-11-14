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

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

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
		if len(batch) >= 5000 || batchBytes+i.SizeBytes > api.NASR2CopyDailyBytes {
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
	if h.Version != 1 || h.PolicySHA256 != policy || len(h.SourceSHA256) != 64 || h.Items <= 0 || h.Items > 5000 || h.Bytes <= 0 || h.Bytes > api.NASR2CopyDailyBytes {
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

func runNASR2Copy(ctx context.Context, cfg config.Config, args []string) {
	if len(args) == 0 {
		log.Fatal("nas-r2-copy plan|queue|status")
	}
	fs := flag.NewFlagSet("nas-r2-copy", flag.ExitOnError)
	input := fs.String("input", "", "source copy.jsonl[.gz] or batch")
	sha := fs.String("sha256", "", "required exact input file SHA256")
	out := fs.String("out", "", "new output directory")
	apply := fs.Bool("apply", false, "queue validated raw work; default read-only")
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
	pc, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		log.Fatal("invalid database configuration")
	}
	pc.MaxConns = 1
	pc.ConnConfig.RuntimeParams["statement_timeout"] = "10s"
	pc.ConnConfig.RuntimeParams["work_mem"] = "16MB"
	pc.ConnConfig.RuntimeParams["application_name"] = "stoarama-nas-r2-copy"
	if !*apply {
		pc.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	}
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		log.Fatal("copy database unavailable")
	}
	defer pool.Close()
	if args[0] == "status" {
		fmt.Printf("enabled_by=NAS_R2_COPY_ENABLED floor_bytes=%d daily_bytes=%d download_bytes_per_sec=%d\n", api.NASR2CopyFloor, api.NASR2CopyDailyBytes, api.NASR2CopyRate)
		var today int64
		if err = pool.QueryRow(ctx, `SELECT coalesce((SELECT charged_bytes FROM nas_r2_copy_budgets WHERE connection_id=13 AND day=(now() AT TIME ZONE 'UTC')::date),0)`).Scan(&today); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("charged_today_bytes=%d remaining_today_bytes=%d\n", today, api.NASR2CopyDailyBytes-today)
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
		log.Fatal("nas-r2-copy plan|queue|status")
	}
	h, items, err := readNASCopyBatch(*input, *sha)
	if err != nil {
		log.Fatal(err)
	}
	var queued int
	// Per-item transactions are bounded, idempotent and safely restartable.
	for _, i := range items {
		tx, e := api.BeginNASR2CopyTx(ctx, pool)
		if e != nil {
			log.Fatal(e)
		}
		_, e = api.LoadNASR2CopySource(ctx, tx, i)
		if e == nil && *apply {
			data, _ := json.Marshal(i)
			var old []byte
			e = tx.QueryRow(ctx, `INSERT INTO nas_r2_copy_items(connection_id,kind,source_id,manifest_sha256,policy_sha256,item) VALUES(13,$1,$2,$3,$4,$5) ON CONFLICT(connection_id,kind,source_id) DO UPDATE SET source_id=excluded.source_id RETURNING item`, i.Kind, i.SourceID, *sha, h.PolicySHA256, data).Scan(&old)
			var existing api.NASR2CopyItem
			if e == nil && (json.Unmarshal(old, &existing) != nil || existing != i) {
				e = errors.New("queued source identity differs; preserved")
			}
		}
		if e != nil {
			tx.Rollback(ctx)
			log.Fatal(e)
		}
		if *apply {
			e = tx.Commit(ctx)
		} else {
			e = tx.Rollback(ctx)
		}
		if e != nil {
			log.Fatal(e)
		}
		queued++
	}
	fmt.Printf("validated=%d queued=%t switch_unchanged=true deletes=0\n", queued, *apply)
}
