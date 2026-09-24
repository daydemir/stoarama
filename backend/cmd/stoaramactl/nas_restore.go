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
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

// nas-restore queues NAS -> R2 restores that the NAS pull client executes (see
// internal/api/server_nas_restore.go). An 'original' restore writes the clip's
// own object key and is only allowed for purged clips; --test-prefix writes a
// disposable copy under nas-restore-test/<label>/ that the API deletes once it
// is verified, which measures restore throughput without touching live keys.

const nasRestoreUsage = `usage:
  stoaramactl nas-restore request (--clip-ids 1,2,3 | --hour-record-id N | --recording-id N --from TIME --to TIME)
      [--test-prefix --label LABEL --max-bytes N --limit N --max-attempts 5 --dry-run --json]
  stoaramactl nas-restore status [--label LABEL --json]
  stoaramactl nas-restore cancel --label LABEL`

var nasRestoreLabelRE = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,79}$`)

type nasRestoreRequestOptions struct {
	clipIDs      []int64
	hourRecordID int64
	recordingID  int64
	from, to     time.Time
	test         bool
	label        string
	maxBytes     int64
	limit        int
	maxAttempts  int
	dryRun       bool
	asJSON       bool
}

func parseNASRestoreTime(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t.UTC(), nil
	}
	t, err := time.Parse("2006-01-02", raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("time %q must be RFC3339 or YYYY-MM-DD (UTC)", raw)
	}
	return t.UTC(), nil
}

func parseNASRestoreRequestArgs(args []string, now time.Time) (nasRestoreRequestOptions, error) {
	fs := flag.NewFlagSet("nas-restore request", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	opts := nasRestoreRequestOptions{}
	clipIDs := fs.String("clip-ids", "", "comma separated clip ids")
	fs.Int64Var(&opts.hourRecordID, "hour-record-id", 0, "every source clip of one joined hour")
	fs.Int64Var(&opts.recordingID, "recording-id", 0, "clips of one recording between --from and --to (clip_start_at)")
	from := fs.String("from", "", "inclusive start (RFC3339 or YYYY-MM-DD, UTC)")
	to := fs.String("to", "", "exclusive end (RFC3339 or YYYY-MM-DD, UTC)")
	fs.BoolVar(&opts.test, "test-prefix", false, "upload to nas-restore-test/<label>/ instead of the original key")
	fs.StringVar(&opts.label, "label", "", "request label (default restore-<utc timestamp>)")
	fs.Int64Var(&opts.maxBytes, "max-bytes", 0, "stop queueing once this many bytes are selected (0 = no cap)")
	fs.IntVar(&opts.limit, "limit", 0, "stop after this many clips (0 = no cap)")
	fs.IntVar(&opts.maxAttempts, "max-attempts", 5, "upload attempts per clip before the request fails")
	fs.BoolVar(&opts.dryRun, "dry-run", false, "classify only; queue nothing")
	fs.BoolVar(&opts.asJSON, "json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return opts, err
	}
	if len(fs.Args()) != 0 {
		return opts, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	if strings.TrimSpace(*clipIDs) != "" {
		for _, raw := range strings.Split(*clipIDs, ",") {
			id, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
			if err != nil || id <= 0 {
				return opts, fmt.Errorf("invalid clip id %q", raw)
			}
			opts.clipIDs = append(opts.clipIDs, id)
		}
	}
	selectors := 0
	if len(opts.clipIDs) > 0 {
		selectors++
	}
	if opts.hourRecordID != 0 {
		selectors++
	}
	if opts.recordingID != 0 {
		selectors++
		if *from == "" || *to == "" {
			return opts, errors.New("--recording-id requires --from and --to")
		}
		var err error
		if opts.from, err = parseNASRestoreTime(*from); err != nil {
			return opts, err
		}
		if opts.to, err = parseNASRestoreTime(*to); err != nil {
			return opts, err
		}
		if !opts.to.After(opts.from) {
			return opts, errors.New("--to must be after --from")
		}
	} else if *from != "" || *to != "" {
		return opts, errors.New("--from/--to require --recording-id")
	}
	switch {
	case selectors != 1:
		return opts, errors.New("exactly one of --clip-ids, --hour-record-id or --recording-id is required")
	case opts.hourRecordID < 0 || opts.recordingID < 0:
		return opts, errors.New("ids must be positive")
	case opts.maxBytes < 0 || opts.limit < 0:
		return opts, errors.New("--max-bytes and --limit must be >= 0")
	case opts.maxAttempts < 1 || opts.maxAttempts > 20:
		return opts, errors.New("--max-attempts must be 1..20")
	}
	if opts.label == "" {
		opts.label = "restore-" + now.UTC().Format("20060102t150405")
	}
	if !nasRestoreLabelRE.MatchString(opts.label) {
		return opts, errors.New("--label must match " + nasRestoreLabelRE.String())
	}
	return opts, nil
}

type nasRestoreCandidate struct {
	ClipID        int64
	RecordingID   int64
	ObjectKey     string
	DisplayPath   string
	ContentType   string
	SizeBytes     int64
	SHA256        string
	Purged        bool
	Bucket        string
	Managed       bool
	ConnectionID  int64
	NASVerified   bool
	NASConnection int
}

// classifyNASRestoreCandidate returns "" when the clip may be queued.
func classifyNASRestoreCandidate(c nasRestoreCandidate, bucket string, test bool) string {
	switch {
	case c.NASConnection == 0:
		return "no_nas_connection"
	case c.NASConnection > 1:
		return "ambiguous_nas_connection"
	case c.SizeBytes <= 0 || len(c.SHA256) != 64 || c.DisplayPath == "" || c.ObjectKey == "":
		return "clip_identity_incomplete"
	case c.SizeBytes > r2.MaxConditionalPutBytes:
		return "too_large"
	case !c.NASVerified:
		return "nas_unverified"
	case !test && (!c.Managed || c.Bucket != bucket):
		return "not_managed_storage"
	case !test && !c.Purged:
		return "not_purged"
	}
	return ""
}

func nasRestoreTestKey(label string, clipID int64, displayPath string) string {
	return fmt.Sprintf("nas-restore-test/%s/%d/%s", label, clipID, path.Base(displayPath))
}

const nasRestoreCandidatesSQL = `
SELECT c.id, c.recording_id, COALESCE(c.object_key,''), COALESCE(c.display_path,''),
       COALESCE(NULLIF(c.mime_type,''),'video/mp4'), c.size_bytes, lower(COALESCE(c.sha256,'')),
       c.purged_at IS NOT NULL, COALESCE(c.bucket,''), COALESCE(sd.managed,false),
       COALESCE(conn.id,0),
       EXISTS(SELECT 1 FROM nas_inventory_files n
              WHERE n.connection_id=conn.id AND n.clip_id=c.id AND n.state='present'
                AND n.sha256=lower(c.sha256) AND n.size_bytes=c.size_bytes AND n.relative_path=c.display_path),
       (SELECT count(*) FROM connections k WHERE k.account_id=rec.account_id AND k.kind='nas_pull')
FROM recording_clips c
JOIN recordings rec ON rec.id=c.recording_id
LEFT JOIN storage_destinations sd ON sd.id=c.storage_destination_id
LEFT JOIN LATERAL (SELECT k.id FROM connections k WHERE k.account_id=rec.account_id AND k.kind='nas_pull' ORDER BY k.id LIMIT 1) conn ON true
WHERE ($1::bigint[] IS NULL OR c.id=ANY($1))
  AND ($2::bigint=0 OR c.id IN (SELECT s.clip_id FROM recording_joined_sources s WHERE s.hour_record_id=$2))
  AND ($3::bigint=0 OR (c.recording_id=$3 AND c.clip_start_at>=$4 AND c.clip_start_at<$5))
ORDER BY c.id`

type nasRestoreRequestSummary struct {
	Label         string           `json:"label"`
	Target        string           `json:"target"`
	DryRun        bool             `json:"dry_run"`
	Selected      int              `json:"selected"`
	Queued        int              `json:"queued"`
	QueuedBytes   int64            `json:"queued_bytes"`
	AlreadyQueued int              `json:"already_queued"`
	Skipped       map[string]int64 `json:"skipped"`
	CapReached    bool             `json:"cap_reached"`
}

func loadNASRestoreCandidates(ctx context.Context, q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, opts nasRestoreRequestOptions) ([]nasRestoreCandidate, error) {
	var ids any
	if len(opts.clipIDs) > 0 {
		ids = opts.clipIDs
	}
	rows, err := q.Query(ctx, nasRestoreCandidatesSQL, ids, opts.hourRecordID, opts.recordingID, opts.from, opts.to)
	if err != nil {
		return nil, fmt.Errorf("load restore candidates: %w", err)
	}
	defer rows.Close()
	var out []nasRestoreCandidate
	for rows.Next() {
		var c nasRestoreCandidate
		if err := rows.Scan(&c.ClipID, &c.RecordingID, &c.ObjectKey, &c.DisplayPath, &c.ContentType, &c.SizeBytes, &c.SHA256,
			&c.Purged, &c.Bucket, &c.Managed, &c.ConnectionID, &c.NASVerified, &c.NASConnection); err != nil {
			return nil, fmt.Errorf("scan restore candidate: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func requestNASRestores(ctx context.Context, pool *pgxpool.Pool, bucket string, opts nasRestoreRequestOptions) (nasRestoreRequestSummary, error) {
	summary := nasRestoreRequestSummary{Label: opts.label, Target: "original", DryRun: opts.dryRun, Skipped: map[string]int64{}}
	if opts.test {
		summary.Target = "test"
	}
	if !opts.test && strings.TrimSpace(bucket) == "" {
		return summary, errors.New("R2_BUCKET is required for original restores")
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return summary, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	candidates, err := loadNASRestoreCandidates(ctx, tx, opts)
	if err != nil {
		return summary, err
	}
	summary.Selected = len(candidates)
	for _, c := range candidates {
		if reason := classifyNASRestoreCandidate(c, bucket, opts.test); reason != "" {
			summary.Skipped[reason]++
			continue
		}
		if (opts.limit > 0 && summary.Queued >= opts.limit) || (opts.maxBytes > 0 && summary.QueuedBytes+c.SizeBytes > opts.maxBytes) {
			summary.CapReached = true
			break
		}
		key := c.ObjectKey
		if opts.test {
			key = nasRestoreTestKey(opts.label, c.ClipID, c.DisplayPath)
		}
		if opts.dryRun {
			summary.Queued++
			summary.QueuedBytes += c.SizeBytes
			continue
		}
		tag, err := tx.Exec(ctx, `
			INSERT INTO nas_restore_requests(connection_id,clip_id,recording_id,target,object_key,source_object_key,relative_path,
			  content_type,size_bytes,sha256,label,max_attempts)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
			ON CONFLICT (object_key) WHERE state IN ('pending','leased') DO NOTHING`,
			c.ConnectionID, c.ClipID, c.RecordingID, summary.Target, key, c.ObjectKey, c.DisplayPath,
			c.ContentType, c.SizeBytes, c.SHA256, opts.label, opts.maxAttempts)
		if err != nil {
			return summary, fmt.Errorf("queue clip %d: %w", c.ClipID, err)
		}
		if tag.RowsAffected() == 0 {
			summary.AlreadyQueued++
			continue
		}
		summary.Queued++
		summary.QueuedBytes += c.SizeBytes
	}
	if opts.dryRun {
		return summary, nil
	}
	return summary, tx.Commit(ctx)
}

type nasRestoreStatus struct {
	Label           string           `json:"label"`
	Target          string           `json:"target"`
	States          map[string]int64 `json:"states"`
	TotalBytes      int64            `json:"total_bytes"`
	VerifiedBytes   int64            `json:"verified_bytes"`
	ClipsRestored   int64            `json:"clips_restored"`
	TestObjectsLeft int64            `json:"test_objects_undeleted"`
	FirstLeasedAt   *time.Time       `json:"first_leased_at"`
	LastCompletedAt *time.Time       `json:"last_completed_at"`
	WallMbps        *float64         `json:"wall_mbps"`
	MeanUploadMbps  *float64         `json:"mean_upload_mbps"`
	Errors          map[string]int64 `json:"errors"`
}

func loadNASRestoreStatus(ctx context.Context, pool *pgxpool.Pool, label string) ([]nasRestoreStatus, error) {
	rows, err := pool.Query(ctx, `
		WITH labels AS (
		  SELECT label, max(id) AS last_id FROM nas_restore_requests WHERE ($1='' OR label=$1)
		  GROUP BY label ORDER BY max(id) DESC LIMIT 20)
		SELECT q.label, min(q.target), q.state, count(*), COALESCE(sum(q.size_bytes),0),
		       count(q.clip_restored_at), count(*) FILTER (WHERE q.target='test' AND q.object_deleted_at IS NULL AND q.state IN ('verified','failed','canceled')),
		       min(q.first_leased_at), max(q.completed_at),
		       COALESCE(sum(q.bytes_uploaded) FILTER (WHERE q.state='verified'),0),
		       COALESCE(sum(q.upload_duration_ms) FILTER (WHERE q.state='verified'),0)
		FROM nas_restore_requests q JOIN labels l ON l.label=q.label
		GROUP BY q.label, q.state, l.last_id ORDER BY l.last_id DESC, q.state`, label)
	if err != nil {
		return nil, fmt.Errorf("load restore status: %w", err)
	}
	defer rows.Close()
	byLabel := map[string]*nasRestoreStatus{}
	var order []string
	type sums struct{ bytes, ms int64 }
	upload := map[string]*sums{}
	for rows.Next() {
		var l, target, state string
		var count, bytes, restored, testLeft, uploadedBytes, uploadMS int64
		var first, last *time.Time
		if err := rows.Scan(&l, &target, &state, &count, &bytes, &restored, &testLeft, &first, &last, &uploadedBytes, &uploadMS); err != nil {
			return nil, err
		}
		st, ok := byLabel[l]
		if !ok {
			st = &nasRestoreStatus{Label: l, Target: target, States: map[string]int64{}, Errors: map[string]int64{}}
			byLabel[l] = st
			upload[l] = &sums{}
			order = append(order, l)
		}
		st.States[state] = count
		st.TotalBytes += bytes
		st.ClipsRestored += restored
		st.TestObjectsLeft += testLeft
		if state == "verified" {
			st.VerifiedBytes = bytes
		}
		if first != nil && (st.FirstLeasedAt == nil || first.Before(*st.FirstLeasedAt)) {
			st.FirstLeasedAt = first
		}
		if last != nil && (st.LastCompletedAt == nil || last.After(*st.LastCompletedAt)) {
			st.LastCompletedAt = last
		}
		upload[l].bytes += uploadedBytes
		upload[l].ms += uploadMS
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	out := make([]nasRestoreStatus, 0, len(order))
	for _, l := range order {
		st := byLabel[l]
		if st.FirstLeasedAt != nil && st.LastCompletedAt != nil && st.LastCompletedAt.After(*st.FirstLeasedAt) && st.VerifiedBytes > 0 {
			v := float64(st.VerifiedBytes) * 8 / st.LastCompletedAt.Sub(*st.FirstLeasedAt).Seconds() / 1e6
			st.WallMbps = &v
		}
		if u := upload[l]; u.ms > 0 {
			v := float64(u.bytes) * 8 / (float64(u.ms) / 1000) / 1e6
			st.MeanUploadMbps = &v
		}
		errRows, err := pool.Query(ctx, `SELECT outcome||': '||left(last_error,120), count(*) FROM nas_restore_requests
			WHERE label=$1 AND last_error<>'' GROUP BY 1 ORDER BY 2 DESC LIMIT 10`, l)
		if err != nil {
			return nil, err
		}
		for errRows.Next() {
			var msg string
			var n int64
			if err := errRows.Scan(&msg, &n); err != nil {
				errRows.Close()
				return nil, err
			}
			st.Errors[msg] = n
		}
		errRows.Close()
		out = append(out, *st)
	}
	return out, nil
}

func runNASRestore(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 {
		log.Fatal(nasRestoreUsage)
	}
	switch args[0] {
	case "request":
		opts, err := parseNASRestoreRequestArgs(args[1:], time.Now())
		if err != nil {
			log.Fatalf("%v\n%s", err, nasRestoreUsage)
		}
		pool := mustJoinedPurgePool(ctx, cfg)
		defer pool.Close()
		summary, err := requestNASRestores(ctx, pool, cfg.R2Bucket, opts)
		if err != nil {
			log.Fatalf("nas-restore request: %v", err)
		}
		if opts.asJSON {
			_ = json.NewEncoder(os.Stdout).Encode(summary)
			return
		}
		fmt.Printf("label=%s target=%s dry_run=%t selected=%d queued=%d queued_bytes=%d already_queued=%d cap_reached=%t\n",
			summary.Label, summary.Target, summary.DryRun, summary.Selected, summary.Queued, summary.QueuedBytes, summary.AlreadyQueued, summary.CapReached)
		printCounts("skipped", summary.Skipped)
	case "status":
		fs := flag.NewFlagSet("nas-restore status", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		label := fs.String("label", "", "one label (default: the 20 newest)")
		asJSON := fs.Bool("json", false, "print JSON")
		if err := fs.Parse(args[1:]); err != nil || len(fs.Args()) != 0 {
			log.Fatal(nasRestoreUsage)
		}
		pool := mustJoinedPurgePool(ctx, cfg)
		defer pool.Close()
		statuses, err := loadNASRestoreStatus(ctx, pool, *label)
		if err != nil {
			log.Fatal(err)
		}
		if *asJSON {
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			_ = enc.Encode(statuses)
			return
		}
		for _, st := range statuses {
			wall, mean := "n/a", "n/a"
			if st.WallMbps != nil {
				wall = fmt.Sprintf("%.1f", *st.WallMbps)
			}
			if st.MeanUploadMbps != nil {
				mean = fmt.Sprintf("%.1f", *st.MeanUploadMbps)
			}
			fmt.Printf("label=%s target=%s total_bytes=%d verified_bytes=%d clips_restored=%d test_objects_undeleted=%d wall_mbps=%s mean_upload_mbps=%s\n",
				st.Label, st.Target, st.TotalBytes, st.VerifiedBytes, st.ClipsRestored, st.TestObjectsLeft, wall, mean)
			printCounts("  state", st.States)
			printCounts("  error", st.Errors)
		}
	case "cancel":
		fs := flag.NewFlagSet("nas-restore cancel", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		label := fs.String("label", "", "label to cancel")
		if err := fs.Parse(args[1:]); err != nil || len(fs.Args()) != 0 || !nasRestoreLabelRE.MatchString(*label) {
			log.Fatal(nasRestoreUsage)
		}
		pool := mustJoinedPurgePool(ctx, cfg)
		defer pool.Close()
		tag, err := pool.Exec(ctx, `UPDATE nas_restore_requests SET state='canceled', completed_at=now(), outcome='canceled'
			WHERE label=$1 AND state='pending'`, *label)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("label=%s canceled_pending=%d (leased rows finish or expire)\n", *label, tag.RowsAffected())
	default:
		log.Fatal(nasRestoreUsage)
	}
}

func printCounts(prefix string, counts map[string]int64) {
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("%s %s=%d\n", prefix, k, counts[k])
	}
}
