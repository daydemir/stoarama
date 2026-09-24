package main

import (
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
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

// nas-verified-purge deletes raw 1-minute clips from R2 once the NAS holds an
// exact copy. It is the raw-clip counterpart of joined-source-purge and never
// touches anything the joined pipeline may still need:
//
//   - the NAS connection has a present inventory row for the clip at its
//     display path with the recorded size and sha256, and no other clip row or
//     unmatched file claims that path (the same proof NAS release uses);
//   - the clip ended, and was ingested, more than --grace ago (default 7 days),
//     and it is not in the last 7 days of a recording that is still active;
//   - it has no joined source snapshot (those follow 0155 via
//     joined-source-purge), is not covered by a snapshotting or dry-run scope,
//     and does not overlap any joined snapshot scope window or any window of a
//     non-canceled qualification run (the good+ windows stay in R2 until their
//     joined hour is sealed and purged by joined-source-purge);
//   - no NAS restore of it is queued, running, or verified in the last 30 days.
//
// Work is batched: parallel HEADs, then one read-committed transaction per
// batch re-checks every rule under FOR UPDATE, sets purged_at (the retention
// trigger still refuses anything joined-protected), deletes the objects in one
// multi-delete, and commits. A crash between the delete and the commit leaves
// absent objects behind unpurged rows, which the next run completes.

const (
	nasPurgeAppName     = "stoarama-nas-verified-purge"
	nasPurgeUSDPerGB    = 0.015
	nasPurgeMaxBatch    = 1000
	nasPurgeMaxWorkers  = 32
	nasPurgeShaTimeout  = 10 * time.Minute
	nasPurgeFinishLimit = 2 * time.Minute
)

type nasPurgeOptions struct {
	connectionID int64
	recordingID  int64
	from, to     time.Time
	afterClipID  int64
	grace        time.Duration
	nasMaxAge    time.Duration
	apply        bool
	checkR2      bool
	limit        int64
	maxBytes     int64
	pageSize     int
	batchSize    int
	workers      int
	rate         float64
	shaEvery     int
	maxErrors    int
	logPath      string
	logSkips     bool
	asJSON       bool
}

func parseNASPurgeArgs(args []string) (nasPurgeOptions, error) {
	fs := flag.NewFlagSet("nas-verified-purge run", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	opts := nasPurgeOptions{}
	fs.Int64Var(&opts.connectionID, "connection-id", 0, "NAS pull connection (default: the only one)")
	fs.Int64Var(&opts.recordingID, "recording-id", 0, "limit to one recording")
	from := fs.String("from", "", "only clips starting at or after (RFC3339 or YYYY-MM-DD, UTC)")
	to := fs.String("to", "", "only clips starting before (RFC3339 or YYYY-MM-DD, UTC)")
	fs.Int64Var(&opts.afterClipID, "after-clip-id", 0, "resume the scan after this clip id")
	fs.DurationVar(&opts.grace, "grace", 7*24*time.Hour, "minimum clip age (end and ingest)")
	fs.DurationVar(&opts.nasMaxAge, "nas-max-age", 0, "also require the NAS copy to be verified within this age (0 = any age)")
	fs.BoolVar(&opts.apply, "apply", false, "delete objects and set purged_at (default is a dry run)")
	fs.BoolVar(&opts.checkR2, "check-r2", false, "dry run only: also HEAD the source objects")
	fs.Int64Var(&opts.limit, "limit", 0, "stop after this many eligible clips (0 = no limit)")
	fs.Int64Var(&opts.maxBytes, "max-bytes", 0, "stop after this many eligible bytes (0 = no limit)")
	fs.IntVar(&opts.pageSize, "page-size", 1000, "candidate rows per database page")
	fs.IntVar(&opts.batchSize, "batch-size", 200, "clips per purge transaction and multi-delete")
	fs.IntVar(&opts.workers, "workers", 16, "concurrent HEAD requests")
	fs.Float64Var(&opts.rate, "rate", 50, "maximum clips purged per second")
	fs.IntVar(&opts.shaEvery, "sha-every", 500, "fully sha256-verify every Nth R2 source object (0 = never)")
	fs.IntVar(&opts.maxErrors, "max-errors", 50, "abort after this many errors")
	fs.StringVar(&opts.logPath, "log", "", "JSONL log path (default ~/.stoarama/tmp/nas-verified-purge-YYYYMMDD.log)")
	fs.BoolVar(&opts.logSkips, "log-skips", false, "also log every skipped clip")
	fs.BoolVar(&opts.asJSON, "json", false, "print the summary as JSON")
	if err := fs.Parse(args); err != nil {
		return opts, err
	}
	if len(fs.Args()) != 0 {
		return opts, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	var err error
	if *from != "" {
		if opts.from, err = parseNASRestoreTime(*from); err != nil {
			return opts, err
		}
	}
	if *to != "" {
		if opts.to, err = parseNASRestoreTime(*to); err != nil {
			return opts, err
		}
	}
	switch {
	case opts.connectionID < 0 || opts.recordingID < 0 || opts.afterClipID < 0:
		return opts, errors.New("ids must be positive")
	case !opts.from.IsZero() && !opts.to.IsZero() && !opts.to.After(opts.from):
		return opts, errors.New("--to must be after --from")
	case opts.grace < 24*time.Hour:
		return opts, errors.New("--grace must be at least 24h")
	case opts.nasMaxAge < 0 || opts.limit < 0 || opts.maxBytes < 0:
		return opts, errors.New("--nas-max-age, --limit and --max-bytes must be >= 0")
	case opts.pageSize < 1 || opts.pageSize > 5000:
		return opts, errors.New("--page-size must be 1..5000")
	case opts.batchSize < 1 || opts.batchSize > nasPurgeMaxBatch:
		return opts, fmt.Errorf("--batch-size must be 1..%d", nasPurgeMaxBatch)
	case opts.workers < 1 || opts.workers > nasPurgeMaxWorkers:
		return opts, fmt.Errorf("--workers must be 1..%d", nasPurgeMaxWorkers)
	case opts.rate <= 0 || opts.rate > 500:
		return opts, errors.New("--rate must be in (0, 500]")
	case opts.shaEvery < 0 || opts.maxErrors < 1:
		return opts, errors.New("--sha-every must be >= 0 and --max-errors >= 1")
	case opts.apply && opts.checkR2:
		return opts, errors.New("--check-r2 is implied by --apply")
	}
	if opts.logPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return opts, fmt.Errorf("resolve home for default log: %w", err)
		}
		opts.logPath = filepath.Join(home, ".stoarama", "tmp", "nas-verified-purge-"+time.Now().UTC().Format("20060102")+".log")
	}
	return opts, nil
}

// nasPurgeFactsSQL evaluates every rule for clip c (recording rec, destination
// sd) as named booleans. $1 connection, $2 bucket, $3 grace seconds, $4 NAS max
// age seconds. The candidate scan classifies with them; the locked re-check
// requires all of them.
const nasPurgeFactsSQL = `
  (c.size_bytes>0 AND lower(COALESCE(c.sha256,'')) ~ '^[0-9a-f]{64}$' AND COALESCE(c.object_key,'')<>'' AND COALESCE(c.display_path,'')<>'') AS identity_ok,
  (COALESCE(sd.managed,false) AND COALESCE(c.bucket,'')=$2 AND COALESCE(sd.bucket,'')=$2) AS managed_ok,
  EXISTS(SELECT 1 FROM nas_inventory_files n
    WHERE n.connection_id=$1 AND n.clip_id=c.id AND n.state='present'
      AND n.sha256=lower(c.sha256) AND n.size_bytes=c.size_bytes AND n.relative_path=c.display_path
      AND ($4::bigint=0 OR n.verified_at>=now()-make_interval(secs => $4::bigint))
      AND NOT EXISTS(SELECT 1 FROM nas_inventory_files other WHERE other.connection_id=n.connection_id
        AND other.relative_path=n.relative_path AND other.clip_id<>n.clip_id AND other.state IN ('present','mismatch'))
      AND NOT EXISTS(SELECT 1 FROM nas_inventory_unmatched_files u WHERE u.connection_id=n.connection_id
        AND u.relative_path=n.relative_path AND u.state='present')) AS nas_ok,
  (c.clip_end_at<=now()-make_interval(secs => $3::bigint) AND c.created_at<=now()-make_interval(secs => $3::bigint)) AS aged,
  NOT (rec.status='active' AND c.clip_end_at>now()-interval '7 days') AS schedule_ok,
  NOT EXISTS(SELECT 1 FROM recording_joined_source_snapshots s WHERE s.clip_id=c.id) AS no_snapshot,
  (NOT EXISTS(SELECT 1 FROM recording_joined_snapshot_scopes scope
      JOIN recording_joined_batches b ON b.id=scope.batch_record_id AND b.state='snapshotting'
      WHERE scope.recording_id=c.recording_id AND scope.recording_job_id=c.recording_job_id AND c.id<=scope.high_water_clip_id)
   AND NOT EXISTS(SELECT 1 FROM recording_joined_dry_run_scopes d
      WHERE d.recording_id=c.recording_id AND d.recording_job_id=c.recording_job_id AND c.id<=d.high_water_clip_id)) AS no_scope,
  NOT EXISTS(SELECT 1 FROM recording_joined_snapshot_scopes scope
      WHERE scope.recording_id=c.recording_id AND scope.scheduled_start_at<c.clip_end_at AND scope.scheduled_end_at>c.clip_start_at) AS no_joined_window,
  NOT EXISTS(SELECT 1 FROM recording_qualification_windows w
      JOIN recording_qualification_runs run ON run.id=w.run_id AND run.status<>'canceled'
      WHERE w.recording_id=c.recording_id AND w.window_start_at<c.clip_end_at AND w.window_end_at>c.clip_start_at) AS no_qualification_window,
  NOT EXISTS(SELECT 1 FROM nas_restore_requests q WHERE q.clip_id=c.id
      AND (q.state IN ('pending','leased') OR (q.target='original' AND q.state='verified' AND q.completed_at>now()-interval '30 days'))) AS no_restore`

var nasPurgeFactNames = []string{
	"identity_ok", "managed_ok", "nas_ok", "aged", "schedule_ok", "no_snapshot", "no_scope",
	"no_joined_window", "no_qualification_window", "no_restore",
}

// nasPurgeFactReasons maps a failing fact to its skip reason, in check order.
var nasPurgeFactReasons = map[string]string{
	"identity_ok": "clip_identity_incomplete", "managed_ok": "not_managed_storage", "no_snapshot": "joined_snapshot",
	"no_scope": "joined_scope", "no_joined_window": "joined_window", "no_qualification_window": "qualification_window",
	"aged": "within_grace", "schedule_ok": "active_recording_recent", "no_restore": "restore_recent", "nas_ok": "nas_unverified",
}

var nasPurgeCandidatesSQL = `
SELECT c.id, c.recording_id, c.size_bytes, lower(COALESCE(c.sha256,'')), COALESCE(c.object_key,''), ` + nasPurgeFactsSQL + `
FROM recording_clips c
JOIN recordings rec ON rec.id=c.recording_id AND rec.delivery='nas_pull'
JOIN connections conn ON conn.id=$1 AND conn.account_id=rec.account_id AND conn.kind='nas_pull'
LEFT JOIN storage_destinations sd ON sd.id=c.storage_destination_id
WHERE c.purged_at IS NULL AND c.id>$5
  AND ($6::bigint=0 OR c.recording_id=$6)
  AND ($7::timestamptz IS NULL OR c.clip_start_at>=$7) AND ($8::timestamptz IS NULL OR c.clip_start_at<$8)
ORDER BY c.id
LIMIT $9`

var nasPurgeLockSQL = `
SELECT c.id, c.size_bytes, lower(COALESCE(c.sha256,'')), COALESCE(c.object_key,'')
FROM (SELECT c.id, c.size_bytes, c.sha256, c.object_key, ` + nasPurgeFactsSQL + `
      FROM recording_clips c
      JOIN recordings rec ON rec.id=c.recording_id AND rec.delivery='nas_pull'
      JOIN connections conn ON conn.id=$1 AND conn.account_id=rec.account_id AND conn.kind='nas_pull'
      LEFT JOIN storage_destinations sd ON sd.id=c.storage_destination_id
      WHERE c.id=ANY($5) AND c.purged_at IS NULL) f
JOIN recording_clips c ON c.id=f.id
WHERE f.` + strings.Join(nasPurgeFactNames, " AND f.") + `
FOR UPDATE OF c`

type nasPurgeCandidate struct {
	ClipID      int64
	RecordingID int64
	SizeBytes   int64
	SHA256      string
	ObjectKey   string
	Facts       map[string]bool
}

func (c nasPurgeCandidate) skipReason() string {
	for _, name := range []string{"identity_ok", "managed_ok", "no_snapshot", "no_scope", "no_joined_window",
		"no_qualification_window", "aged", "schedule_ok", "no_restore", "nas_ok"} {
		if !c.Facts[name] {
			return nasPurgeFactReasons[name]
		}
	}
	return ""
}

type nasPurgeSummary struct {
	Mode            string           `json:"mode"`
	ConnectionID    int64            `json:"connection_id"`
	Scanned         int64            `json:"scanned"`
	Eligible        int64            `json:"eligible"`
	EligibleBytes   int64            `json:"eligible_bytes"`
	Purged          int64            `json:"purged"`
	PurgedBytes     int64            `json:"purged_bytes"`
	SourceAbsent    int64            `json:"source_already_absent"`
	ShaChecks       int64            `json:"sha_checks"`
	Skipped         map[string]int64 `json:"skipped"`
	Errors          int64            `json:"errors"`
	LastClipID      int64            `json:"last_clip_id"`
	LimitReached    bool             `json:"limit_reached"`
	Interrupted     bool             `json:"interrupted"`
	ElapsedSec      float64          `json:"elapsed_sec"`
	LogPath         string           `json:"log_path"`
	MonthlySavedUSD float64          `json:"monthly_saved_usd"`
}

type nasPurgeLogLine struct {
	At          time.Time `json:"at"`
	Mode        string    `json:"mode"`
	ClipID      int64     `json:"clip_id"`
	RecordingID int64     `json:"recording_id"`
	SizeBytes   int64     `json:"size_bytes"`
	Action      string    `json:"action"`
	Reason      string    `json:"reason,omitempty"`
	ObjectKey   string    `json:"object_key,omitempty"`
}

type nasPurgeStore interface {
	Bucket() string
	Head(ctx context.Context, key string) (r2.ObjectHead, error)
	OpenExact(ctx context.Context, key, etag, versionID string) (io.ReadCloser, error)
	DeleteObjects(ctx context.Context, keys []string) error
}

var _ nasPurgeStore = (*r2.Client)(nil)

type nasPurgeRunner struct {
	pool    *pgxpool.Pool
	store   nasPurgeStore
	opts    nasPurgeOptions
	summary *nasPurgeSummary
	logMu   sync.Mutex
	logw    io.Writer
	headN   int64
}

func (r *nasPurgeRunner) log(c nasPurgeCandidate, action, reason string) {
	if r.logw == nil || (action == "skip" && !r.opts.logSkips) {
		return
	}
	line := nasPurgeLogLine{At: time.Now().UTC(), Mode: r.summary.Mode, ClipID: c.ClipID, RecordingID: c.RecordingID,
		SizeBytes: c.SizeBytes, Action: action, Reason: reason}
	if action == "purged" || action == "error" {
		line.ObjectKey = c.ObjectKey
	}
	b, _ := json.Marshal(line)
	r.logMu.Lock()
	_, _ = r.logw.Write(append(b, '\n'))
	r.logMu.Unlock()
}

func (r *nasPurgeRunner) skip(c nasPurgeCandidate, reason string) {
	r.summary.Skipped[reason]++
	r.log(c, "skip", reason)
}

func (r *nasPurgeRunner) fail(c nasPurgeCandidate, reason string, err error) error {
	r.summary.Errors++
	r.log(c, "error", reason+": "+err.Error())
	if r.summary.Errors >= int64(r.opts.maxErrors) {
		return fmt.Errorf("aborting after %d errors (last: %s: %v)", r.summary.Errors, reason, err)
	}
	return nil
}

func loadNASPurgeCandidates(ctx context.Context, pool *pgxpool.Pool, bucket string, opts nasPurgeOptions, after int64) ([]nasPurgeCandidate, error) {
	var from, to any
	if !opts.from.IsZero() {
		from = opts.from
	}
	if !opts.to.IsZero() {
		to = opts.to
	}
	rows, err := pool.Query(ctx, nasPurgeCandidatesSQL, opts.connectionID, bucket, int64(opts.grace/time.Second),
		int64(opts.nasMaxAge/time.Second), after, opts.recordingID, from, to, opts.pageSize)
	if err != nil {
		return nil, fmt.Errorf("load purge candidates: %w", err)
	}
	defer rows.Close()
	var out []nasPurgeCandidate
	for rows.Next() {
		c := nasPurgeCandidate{Facts: map[string]bool{}}
		facts := make([]bool, len(nasPurgeFactNames))
		dest := []any{&c.ClipID, &c.RecordingID, &c.SizeBytes, &c.SHA256, &c.ObjectKey}
		for i := range facts {
			dest = append(dest, &facts[i])
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("scan purge candidate: %w", err)
		}
		for i, name := range nasPurgeFactNames {
			c.Facts[name] = facts[i]
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func resolveNASPurgeConnection(ctx context.Context, pool *pgxpool.Pool, id int64) (int64, error) {
	if id > 0 {
		var ok bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM connections WHERE id=$1 AND kind='nas_pull')`, id).Scan(&ok); err != nil {
			return 0, err
		}
		if !ok {
			return 0, fmt.Errorf("connection %d is not a NAS pull connection", id)
		}
		return id, nil
	}
	var ids []int64
	rows, err := pool.Query(ctx, `SELECT id FROM connections WHERE kind='nas_pull' ORDER BY id`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return 0, err
		}
		ids = append(ids, v)
	}
	if len(ids) != 1 {
		return 0, fmt.Errorf("found %d NAS pull connections; pass --connection-id", len(ids))
	}
	return ids[0], nil
}

func runNASVerifiedPurgePass(ctx context.Context, pool *pgxpool.Pool, store nasPurgeStore, opts nasPurgeOptions, logw io.Writer) (summary nasPurgeSummary, err error) {
	started := time.Now()
	summary = nasPurgeSummary{Mode: "dry_run", ConnectionID: opts.connectionID, Skipped: map[string]int64{}, LogPath: opts.logPath, LastClipID: opts.afterClipID}
	if opts.apply {
		summary.Mode = "apply"
	}
	defer func() {
		bytes := summary.EligibleBytes
		if opts.apply {
			bytes = summary.PurgedBytes
		}
		summary.MonthlySavedUSD = float64(bytes) / 1e9 * nasPurgeUSDPerGB
		summary.ElapsedSec = time.Since(started).Seconds()
	}()
	r := &nasPurgeRunner{pool: pool, store: store, opts: opts, summary: &summary, logw: logw}
	var pending []nasPurgeCandidate
	var pendingBytes int64
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		batchStart := time.Now()
		err := r.processBatch(ctx, pending)
		n := len(pending)
		pending, pendingBytes = nil, 0
		if err != nil {
			return err
		}
		if min := time.Duration(float64(n) / opts.rate * float64(time.Second)); time.Since(batchStart) < min {
			select {
			case <-ctx.Done():
			case <-time.After(min - time.Since(batchStart)):
			}
		}
		return nil
	}
	after := opts.afterClipID
	for {
		if ctx.Err() != nil {
			summary.Interrupted = true
			return summary, nil
		}
		page, err := loadNASPurgeCandidates(ctx, pool, store.Bucket(), opts, after)
		if err != nil {
			if ctx.Err() != nil {
				summary.Interrupted = true
				return summary, nil
			}
			return summary, err
		}
		if len(page) == 0 {
			if err := flush(); err != nil {
				return summary, err
			}
			return summary, nil
		}
		for _, c := range page {
			after = c.ClipID
			summary.Scanned++
			if reason := c.skipReason(); reason != "" {
				r.skip(c, reason)
				continue
			}
			if (opts.limit > 0 && summary.Eligible >= opts.limit) || (opts.maxBytes > 0 && summary.EligibleBytes+c.SizeBytes > opts.maxBytes) {
				summary.LimitReached = true
				if err := flush(); err != nil {
					return summary, err
				}
				return summary, nil
			}
			summary.Eligible++
			summary.EligibleBytes += c.SizeBytes
			if !opts.apply && !opts.checkR2 {
				r.log(c, "eligible", "")
				continue
			}
			pending = append(pending, c)
			pendingBytes += c.SizeBytes
			if len(pending) >= opts.batchSize {
				if err := flush(); err != nil {
					return summary, err
				}
				if ctx.Err() != nil {
					summary.Interrupted = true
					return summary, nil
				}
			}
		}
		// Only advance the resume point past a page once all of it is settled.
		if err := flush(); err != nil {
			return summary, err
		}
		if ctx.Err() != nil {
			// The last batch may be unsettled: keep the previous resume point.
			summary.Interrupted = true
			return summary, nil
		}
		summary.LastClipID = after
	}
}

type nasPurgeHead struct {
	absent bool
	reason string
	err    error
}

// headBatch HEADs every candidate (and fully hashes every Nth) concurrently.
func (r *nasPurgeRunner) headBatch(ctx context.Context, batch []nasPurgeCandidate) []nasPurgeHead {
	out := make([]nasPurgeHead, len(batch))
	sem := make(chan struct{}, r.opts.workers)
	var wg sync.WaitGroup
	for i := range batch {
		r.headN++
		fullHash := r.opts.shaEvery > 0 && r.headN%int64(r.opts.shaEvery) == 1%int64(r.opts.shaEvery)
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, fullHash bool) {
			defer wg.Done()
			defer func() { <-sem }()
			c := batch[i]
			head, err := r.store.Head(ctx, c.ObjectKey)
			switch {
			case err != nil && r2.IsNotFound(err):
				out[i].absent = true
				return
			case err != nil:
				out[i].err = err
				return
			case head.SizeBytes != c.SizeBytes:
				out[i].reason = "source_size_mismatch"
				return
			}
			if !fullHash {
				return
			}
			hctx, cancel := context.WithTimeout(ctx, nasPurgeShaTimeout)
			defer cancel()
			body, err := r.store.OpenExact(hctx, c.ObjectKey, head.ETag, head.VersionID)
			if err != nil {
				out[i].err = err
				return
			}
			defer body.Close()
			h := sha256.New()
			if _, err := io.Copy(h, body); err != nil {
				out[i].err = err
				return
			}
			out[i].reason = "sha_checked"
			if hex.EncodeToString(h.Sum(nil)) != c.SHA256 {
				out[i].reason = "source_sha_mismatch"
			}
		}(i, fullHash)
	}
	wg.Wait()
	return out
}

func (r *nasPurgeRunner) processBatch(ctx context.Context, batch []nasPurgeCandidate) error {
	heads := r.headBatch(ctx, batch)
	var ready []nasPurgeCandidate
	absent := map[int64]bool{}
	for i, c := range batch {
		h := heads[i]
		switch {
		case h.err != nil:
			if ctx.Err() != nil {
				return nil
			}
			if err := r.fail(c, "source_head_failed", h.err); err != nil {
				return err
			}
			continue
		case h.reason == "sha_checked":
			r.summary.ShaChecks++
		case h.reason == "source_sha_mismatch":
			r.summary.ShaChecks++
			r.skip(c, h.reason)
			r.log(c, "error", h.reason)
			continue
		case h.reason != "":
			r.skip(c, h.reason)
			continue
		}
		if h.absent {
			absent[c.ClipID] = true
		}
		ready = append(ready, c)
	}
	if len(ready) == 0 {
		return nil
	}
	if !r.opts.apply {
		for _, c := range ready {
			if absent[c.ClipID] {
				r.summary.SourceAbsent++
				r.log(c, "eligible", "source_already_absent")
			} else {
				r.log(c, "eligible", "")
			}
		}
		return nil
	}
	purged, skipped, err := r.purgeBatch(ctx, ready)
	if err != nil && isRetentionProtected(err) && len(ready) > 1 {
		// Isolate the protected clip(s): the rest of the batch is still valid.
		for _, c := range ready {
			if ctx.Err() != nil {
				return nil
			}
			one, oneSkipped, oneErr := r.purgeBatch(ctx, []nasPurgeCandidate{c})
			if oneErr != nil && isRetentionProtected(oneErr) {
				r.skip(c, "trigger_refused")
				continue
			}
			if oneErr != nil {
				if err := r.fail(c, "purge_failed", oneErr); err != nil {
					return err
				}
				continue
			}
			r.record(one, oneSkipped, absent)
		}
		return nil
	}
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		for _, c := range ready {
			if ferr := r.fail(c, "purge_failed", err); ferr != nil {
				return ferr
			}
		}
		return nil
	}
	r.record(purged, skipped, absent)
	return nil
}

func (r *nasPurgeRunner) record(purged []nasPurgeCandidate, skipped map[int64]nasPurgeCandidate, absent map[int64]bool) {
	for _, c := range skipped {
		r.skip(c, "changed_before_lock")
	}
	for _, c := range purged {
		r.summary.Purged++
		r.summary.PurgedBytes += c.SizeBytes
		reason := ""
		if absent[c.ClipID] {
			r.summary.SourceAbsent++
			reason = "source_already_absent"
		}
		r.log(c, "purged", reason)
	}
}

func isRetentionProtected(err error) bool {
	return err != nil && strings.Contains(err.Error(), "retention protected")
}

// purgeBatch is the single write path: lock and re-check every rule, set
// purged_at, delete the objects, commit. Rows whose identity or eligibility
// changed since the scan are returned in skipped and left alone.
func (r *nasPurgeRunner) purgeBatch(ctx context.Context, batch []nasPurgeCandidate) ([]nasPurgeCandidate, map[int64]nasPurgeCandidate, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	ids := make([]int64, len(batch))
	byID := map[int64]nasPurgeCandidate{}
	for i, c := range batch {
		ids[i] = c.ClipID
		byID[c.ClipID] = c
	}
	rows, err := tx.Query(ctx, nasPurgeLockSQL, r.opts.connectionID, r.store.Bucket(), int64(r.opts.grace/time.Second),
		int64(r.opts.nasMaxAge/time.Second), ids)
	if err != nil {
		return nil, nil, err
	}
	var lockedIDs []int64
	var purged []nasPurgeCandidate
	for rows.Next() {
		var id, size int64
		var sha, key string
		if err := rows.Scan(&id, &size, &sha, &key); err != nil {
			rows.Close()
			return nil, nil, err
		}
		c := byID[id]
		if c.SizeBytes != size || c.SHA256 != sha || c.ObjectKey != key {
			continue
		}
		lockedIDs = append(lockedIDs, id)
		purged = append(purged, c)
		delete(byID, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	if len(lockedIDs) == 0 {
		return nil, byID, nil
	}
	tag, err := tx.Exec(ctx, `UPDATE recording_clips SET purged_at=now() WHERE id=ANY($1) AND purged_at IS NULL`, lockedIDs)
	if err != nil {
		return nil, nil, err
	}
	if tag.RowsAffected() != int64(len(lockedIDs)) {
		return nil, nil, fmt.Errorf("purged %d rows, locked %d", tag.RowsAffected(), len(lockedIDs))
	}
	// Past this point the delete and the commit must finish together.
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), nasPurgeFinishLimit)
	defer cancel()
	keys := make([]string, len(purged))
	for i, c := range purged {
		keys[i] = c.ObjectKey
	}
	if err := r.store.DeleteObjects(finishCtx, keys); err != nil {
		return nil, nil, fmt.Errorf("delete source objects: %w", err)
	}
	if err := tx.Commit(finishCtx); err != nil {
		return nil, nil, fmt.Errorf("commit after delete (a rerun completes it): %w", err)
	}
	return purged, byID, nil
}

func runNASVerifiedPurge(ctx context.Context, cfg config.Config, args []string) {
	const usage = `usage: stoaramactl nas-verified-purge run [--apply --connection-id N --recording-id N --from TIME --to TIME --after-clip-id N
    --grace 168h --nas-max-age 0 --limit N --max-bytes N --page-size 1000 --batch-size 200 --workers 16 --rate 50
    --sha-every 500 --max-errors 50 --log PATH --log-skips --check-r2 --json]`
	if len(args) < 1 || args[0] != "run" {
		log.Fatal(usage)
	}
	opts, err := parseNASPurgeArgs(args[1:])
	if err != nil {
		log.Fatalf("%v\n%s", err, usage)
	}
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		log.Fatal("invalid database configuration")
	}
	poolCfg.MaxConns = 3
	poolCfg.MinConns = 0
	poolCfg.ConnConfig.RuntimeParams["application_name"] = nasPurgeAppName
	poolCfg.ConnConfig.RuntimeParams["statement_timeout"] = "120s"
	poolCfg.ConnConfig.RuntimeParams["lock_timeout"] = "5s"
	poolCfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "300s"
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		log.Fatalf("connect database: %v", err)
	}
	defer pool.Close()
	if opts.connectionID, err = resolveNASPurgeConnection(ctx, pool, opts.connectionID); err != nil {
		log.Fatal(err)
	}
	store := mustArchiveR2Client(ctx, cfg)
	logFile, err := openJoinedPurgeLog(opts.logPath)
	if err != nil {
		log.Fatal(err)
	}
	defer logFile.Close()
	summary, err := runNASVerifiedPurgePass(ctx, pool, store, opts, logFile)
	if opts.asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(summary)
	} else {
		fmt.Printf("mode=%s connection=%d scanned=%d eligible=%d eligible_bytes=%d purged=%d purged_bytes=%d source_already_absent=%d sha_checks=%d errors=%d last_clip_id=%d limit_reached=%t interrupted=%t elapsed_sec=%.0f monthly_saved_usd=%.2f\n",
			summary.Mode, summary.ConnectionID, summary.Scanned, summary.Eligible, summary.EligibleBytes, summary.Purged, summary.PurgedBytes,
			summary.SourceAbsent, summary.ShaChecks, summary.Errors, summary.LastClipID, summary.LimitReached, summary.Interrupted, summary.ElapsedSec, summary.MonthlySavedUSD)
		printCounts("skipped", summary.Skipped)
		fmt.Printf("log=%s\n", summary.LogPath)
	}
	if err != nil {
		log.Fatalf("nas-verified-purge: %v", err)
	}
}
