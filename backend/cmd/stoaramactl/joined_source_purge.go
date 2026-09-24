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
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

// joined-source-purge is the retention primitive for collated recordings: once
// a joined hour is sealed, its manifest and every media part are published, and
// no operator hold covers it, the 1-minute source clips the hour INCLUDED are
// duplicates. For each such clip it verifies the joined media object in R2
// (exact HEAD, periodic full sha256) and the NAS copy (inventory row with the
// same path, size and sha256). Then, in one transaction, it marks the clip
// purged_at (the recording_clips trigger re-checks eligibility), deletes the
// source object, and commits. The recording_clips row and its thumbnail stay.
//
// Resumable by construction: purged clips are skipped on the next run, and a
// crash between the R2 delete and the commit leaves an absent object with
// purged_at NULL, which the next run completes (the delete is idempotent).

const (
	joinedSourcePurgeGUC        = "stoarama.joined_source_purge"
	joinedSourcePurgeGUCValue   = "verified"
	joinedSourcePurgeMaxPage    = 5000
	joinedSourcePurgeAppName    = "stoarama-joined-source-purge"
	joinedSourcePurgeShaTimeout = 10 * time.Minute
	joinedSourcePurgeUSDPerGB   = 0.015
	// joinedSourcePurgeFinishTimeout bounds the uncancellable delete+commit tail.
	joinedSourcePurgeFinishTimeout = 60 * time.Second
)

var joinedHoldReasonRE = regexp.MustCompile(`^[a-z][a-z0-9_]{0,79}$`)

type joinedSourcePurgeOptions struct {
	batchID      string
	hourRecordID int64
	apply        bool
	checkR2      bool
	limit        int
	pageSize     int
	rate         float64
	requireNAS   bool
	nasMaxAge    time.Duration
	shaEvery     int
	maxErrors    int
	logPath      string
	asJSON       bool
}

// purgeObjectStore is the R2 surface the purge needs. *r2.Client satisfies it.
type purgeObjectStore interface {
	Bucket() string
	Head(ctx context.Context, key string) (r2.ObjectHead, error)
	HeadExact(ctx context.Context, key, etag, versionID string) (r2.ObjectHead, error)
	OpenExact(ctx context.Context, key, etag, versionID string) (io.ReadCloser, error)
	DeleteObjects(ctx context.Context, keys []string) error
}

var _ purgeObjectStore = (*r2.Client)(nil)

type joinedPurgeCandidate struct {
	ClipID         int64
	RecordingID    int64
	RecordingJobID int64
	SizeBytes      int64
	SHA256         string
	ObjectKey      string
	ClipBucket     string
	Managed        bool
	DestBucket     string
	HourRecordID   int64
	SourceID       int64
	HourID         string
	Disposition    string
	HourFinal      bool
	HourHeld       bool
	MediaID        int64
	MediaKey       string
	MediaETag      string
	MediaVersion   string
	MediaSize      int64
	MediaSHA256    string
	MediaPublished bool
	StructEligible bool
	NASVerified    bool
}

type joinedPurgeLogLine struct {
	At           time.Time `json:"at"`
	Mode         string    `json:"mode"`
	ClipID       int64     `json:"clip_id"`
	RecordingID  int64     `json:"recording_id"`
	HourRecordID int64     `json:"hour_record_id"`
	SizeBytes    int64     `json:"size_bytes"`
	Action       string    `json:"action"`
	Reason       string    `json:"reason,omitempty"`
	ObjectKey    string    `json:"object_key,omitempty"`
}

type joinedPurgeSummary struct {
	Mode            string           `json:"mode"`
	BatchID         string           `json:"batch_id,omitempty"`
	HourRecordID    int64            `json:"hour_record_id,omitempty"`
	Scanned         int64            `json:"scanned"`
	Eligible        int64            `json:"eligible"`
	EligibleBytes   int64            `json:"eligible_bytes"`
	Purged          int64            `json:"purged"`
	PurgedBytes     int64            `json:"purged_bytes"`
	SourceAbsent    int64            `json:"source_already_absent"`
	Skipped         map[string]int64 `json:"skipped"`
	Errors          int64            `json:"errors"`
	MediaHeads      int64            `json:"media_heads"`
	MediaSHAChecks  int64            `json:"media_sha_checks"`
	LimitReached    bool             `json:"limit_reached"`
	Interrupted     bool             `json:"interrupted"`
	LogPath         string           `json:"log_path"`
	MonthlySavedUSD float64          `json:"monthly_saved_usd"`
}

const joinedSourcePurgeUsage = `usage:
  stoaramactl joined-source-purge run [--batch-id ID --hour-record-id N --apply --check-r2 --limit N --page-size 500 --rate 10 --require-nas=true --nas-max-age 0 --sha-every 50 --max-errors 20 --log PATH --json]
  stoaramactl joined-source-purge hold --hour-record-id N --reason CODE [--note TEXT]
  stoaramactl joined-source-purge holds [--json]`

func parseJoinedSourcePurgeRunArgs(args []string) (joinedSourcePurgeOptions, error) {
	fs := flag.NewFlagSet("joined-source-purge run", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	opts := joinedSourcePurgeOptions{}
	fs.StringVar(&opts.batchID, "batch-id", "", "limit to one joined batch id")
	fs.Int64Var(&opts.hourRecordID, "hour-record-id", 0, "limit to one joined hour record id")
	fs.BoolVar(&opts.apply, "apply", false, "delete source objects and set purged_at (default is a dry run)")
	fs.BoolVar(&opts.checkR2, "check-r2", false, "dry run only: also HEAD joined media and source objects")
	fs.IntVar(&opts.limit, "limit", 0, "stop after this many eligible clips (0 = no limit)")
	fs.IntVar(&opts.pageSize, "page-size", 500, "candidate rows per database page")
	fs.Float64Var(&opts.rate, "rate", 10, "maximum clips verified or purged per second")
	fs.BoolVar(&opts.requireNAS, "require-nas", true, "require a verified NAS inventory copy (path, size, sha256)")
	fs.DurationVar(&opts.nasMaxAge, "nas-max-age", 0, "also require the NAS copy to be verified within this age (0 = any age)")
	fs.IntVar(&opts.shaEvery, "sha-every", 50, "fully sha256-verify every Nth distinct joined media object (0 = never)")
	fs.IntVar(&opts.maxErrors, "max-errors", 20, "abort after this many per-clip errors")
	fs.StringVar(&opts.logPath, "log", "", "JSONL log path (default ~/.stoarama/tmp/joined-source-purge-YYYYMMDD.log)")
	fs.BoolVar(&opts.asJSON, "json", false, "print the summary as JSON")
	if err := fs.Parse(args); err != nil {
		return opts, err
	}
	if len(fs.Args()) != 0 {
		return opts, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	opts.batchID = strings.TrimSpace(opts.batchID)
	switch {
	case opts.hourRecordID < 0:
		return opts, errors.New("--hour-record-id must be positive")
	case opts.limit < 0:
		return opts, errors.New("--limit must be >= 0")
	case opts.pageSize < 1 || opts.pageSize > joinedSourcePurgeMaxPage:
		return opts, fmt.Errorf("--page-size must be 1..%d", joinedSourcePurgeMaxPage)
	case opts.rate <= 0 || opts.rate > 200:
		return opts, errors.New("--rate must be in (0, 200]")
	case opts.nasMaxAge < 0:
		return opts, errors.New("--nas-max-age must be >= 0")
	case opts.shaEvery < 0:
		return opts, errors.New("--sha-every must be >= 0")
	case opts.maxErrors < 1:
		return opts, errors.New("--max-errors must be >= 1")
	case opts.apply && opts.checkR2:
		return opts, errors.New("--check-r2 is implied by --apply")
	}
	if opts.logPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return opts, fmt.Errorf("resolve home for default log: %w", err)
		}
		opts.logPath = filepath.Join(home, ".stoarama", "tmp", "joined-source-purge-"+time.Now().UTC().Format("20060102")+".log")
	}
	return opts, nil
}

func runJoinedSourcePurge(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 {
		log.Fatal(joinedSourcePurgeUsage)
	}
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	switch args[0] {
	case "run":
		opts, err := parseJoinedSourcePurgeRunArgs(args[1:])
		if err != nil {
			log.Fatalf("%v\n%s", err, joinedSourcePurgeUsage)
		}
		pool := mustJoinedPurgePool(ctx, cfg)
		defer pool.Close()
		store := mustArchiveR2Client(ctx, cfg)
		logFile, err := openJoinedPurgeLog(opts.logPath)
		if err != nil {
			log.Fatal(err)
		}
		defer logFile.Close()
		summary, err := runJoinedSourcePurgePass(ctx, pool, store, opts, logFile)
		printJoinedPurgeSummary(summary, opts.asJSON)
		if err != nil {
			log.Fatalf("joined-source-purge: %v", err)
		}
	case "hold":
		fs := flag.NewFlagSet("joined-source-purge hold", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		hourID := fs.Int64("hour-record-id", 0, "joined hour record id")
		reason := fs.String("reason", "", "reason code")
		note := fs.String("note", "", "free-text note")
		if err := fs.Parse(args[1:]); err != nil || len(fs.Args()) != 0 || *hourID <= 0 || !joinedHoldReasonRE.MatchString(*reason) {
			log.Fatalf("invalid hold arguments\n%s", joinedSourcePurgeUsage)
		}
		pool := mustJoinedPurgePool(ctx, cfg)
		defer pool.Close()
		inserted, err := addJoinedRetentionHold(ctx, pool, *hourID, *reason, *note)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("hour_record_id=%d reason=%s inserted=%t\n", *hourID, *reason, inserted)
	case "holds":
		asJSON := len(args) > 1 && args[1] == "--json"
		pool := mustJoinedPurgePool(ctx, cfg)
		defer pool.Close()
		holds, err := listJoinedRetentionHolds(ctx, pool)
		if err != nil {
			log.Fatal(err)
		}
		if asJSON {
			_ = json.NewEncoder(os.Stdout).Encode(holds)
			return
		}
		for _, h := range holds {
			fmt.Printf("%d\t%s\t%s\t%s\t%s\n", h.HourRecordID, h.ReasonCode, h.HourID, h.CreatedAt.UTC().Format(time.RFC3339), h.Note)
		}
	default:
		log.Fatal(joinedSourcePurgeUsage)
	}
}

func mustJoinedPurgePool(ctx context.Context, cfg config.Config) *pgxpool.Pool {
	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		log.Fatal("invalid database configuration")
	}
	poolCfg.MaxConns = 2
	poolCfg.MinConns = 0
	poolCfg.ConnConfig.RuntimeParams["application_name"] = joinedSourcePurgeAppName
	poolCfg.ConnConfig.RuntimeParams["statement_timeout"] = "120s"
	poolCfg.ConnConfig.RuntimeParams["lock_timeout"] = "5s"
	poolCfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "300s"
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		log.Fatalf("connect database: %v", err)
	}
	return pool
}

func openJoinedPurgeLog(path string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create log directory: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open log: %w", err)
	}
	return f, nil
}

// joinedPurgeNASVerifiedSQL is the NAS rule shared by the candidate scan and the
// locked re-check: the batch's NAS connection holds a present inventory row for
// the clip at its display path with the same size and sha256.
const joinedPurgeNASVerifiedSQL = `EXISTS(SELECT 1 FROM nas_inventory_files n
         WHERE n.connection_id=b.connection_id AND n.clip_id=c.id AND n.state='present'
           AND n.sha256=lower(c.sha256) AND n.size_bytes=c.size_bytes AND n.relative_path=c.display_path
           AND (NAS_MAX_AGE::bigint=0 OR n.verified_at>=now()-make_interval(secs => NAS_MAX_AGE::bigint)))`

func joinedPurgeNASVerified(maxAgeParam string) string {
	return strings.ReplaceAll(joinedPurgeNASVerifiedSQL, "NAS_MAX_AGE", maxAgeParam)
}

// joinedPurgeCandidatesSQL lists the not-yet-purged sources of sealed joined
// hours in (hour_record_id, source_id) keyset order, with every fact the
// classifier needs. Structural eligibility comes from the same SQL function
// the recording_clips trigger enforces.
var joinedPurgeCandidatesSQL = `
SELECT c.id, c.recording_id, COALESCE(c.recording_job_id,0), c.size_bytes, lower(COALESCE(c.sha256,'')), COALESCE(c.object_key,''),
       COALESCE(c.bucket,''), sd.managed, COALESCE(sd.bucket,''),
       d.hour_record_id, d.source_id, h.hour_id, d.disposition,
       recording_joined_hour_retention_final(h.id),
       EXISTS(SELECT 1 FROM recording_joined_source_retention_holds hold WHERE hold.hour_record_id=h.id),
       COALESCE(media.id,0), COALESCE(media.object_key,''), COALESCE(media.etag,''), COALESCE(media.version_id,''),
       COALESCE(media.expected_size_bytes,0), COALESCE(media.expected_sha256,''),
       COALESCE(media.published_at IS NOT NULL AND media.artifact_kind='media', false),
       recording_joined_source_purge_eligible(c.id, c.recording_id, c.recording_job_id),
       ` + joinedPurgeNASVerified("$5") + `
FROM recording_joined_hour_dispositions d
JOIN recording_joined_hours h ON h.id=d.hour_record_id
JOIN recording_joined_batches b ON b.id=h.batch_record_id
JOIN recording_joined_sources src ON src.id=d.source_id AND src.hour_record_id=d.hour_record_id
JOIN recording_clips c ON c.id=src.clip_id
JOIN storage_destinations sd ON sd.id=c.storage_destination_id
LEFT JOIN recording_joined_artifacts media ON media.id=d.media_artifact_id
WHERE h.state='sealed' AND c.purged_at IS NULL
  AND ($1::text='' OR b.batch_id=$1) AND ($2::bigint=0 OR h.id=$2)
  AND (d.hour_record_id, d.source_id) > ($3::bigint, $4::bigint)
ORDER BY d.hour_record_id, d.source_id
LIMIT $6`

func loadJoinedPurgeCandidates(ctx context.Context, pool *pgxpool.Pool, opts joinedSourcePurgeOptions, afterHour, afterSource int64) ([]joinedPurgeCandidate, error) {
	rows, err := pool.Query(ctx, joinedPurgeCandidatesSQL, opts.batchID, opts.hourRecordID, afterHour, afterSource,
		int64(opts.nasMaxAge/time.Second), opts.pageSize)
	if err != nil {
		return nil, fmt.Errorf("load purge candidates: %w", err)
	}
	defer rows.Close()
	out := make([]joinedPurgeCandidate, 0, opts.pageSize)
	for rows.Next() {
		var c joinedPurgeCandidate
		if err := rows.Scan(&c.ClipID, &c.RecordingID, &c.RecordingJobID, &c.SizeBytes, &c.SHA256, &c.ObjectKey,
			&c.ClipBucket, &c.Managed, &c.DestBucket,
			&c.HourRecordID, &c.SourceID, &c.HourID, &c.Disposition, &c.HourFinal, &c.HourHeld,
			&c.MediaID, &c.MediaKey, &c.MediaETag, &c.MediaVersion, &c.MediaSize, &c.MediaSHA256, &c.MediaPublished,
			&c.StructEligible, &c.NASVerified); err != nil {
			return nil, fmt.Errorf("scan purge candidate: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// classifyJoinedPurgeCandidate returns "" when the clip passes every DB-side
// rule, else the first failing rule as a stable reason code.
func classifyJoinedPurgeCandidate(c joinedPurgeCandidate, joinedBucket string, requireNAS bool) string {
	switch {
	case c.Disposition != "included":
		return "not_included"
	case c.HourHeld:
		return "hour_held"
	case !c.HourFinal:
		return "hour_not_final"
	case c.MediaID == 0 || !c.MediaPublished || c.MediaKey == "" || c.MediaSize <= 0 || len(c.MediaSHA256) != 64:
		return "media_unpublished"
	case !c.StructEligible:
		return "structurally_ineligible"
	case !c.Managed || c.DestBucket != joinedBucket || c.ClipBucket != c.DestBucket || strings.TrimSpace(c.ObjectKey) == "":
		return "not_managed_storage"
	case c.SizeBytes <= 0 || len(c.SHA256) != 64:
		return "clip_identity_incomplete"
	case requireNAS && !c.NASVerified:
		return "nas_unverified"
	}
	return ""
}

type joinedPurgeRunner struct {
	pool     *pgxpool.Pool
	store    purgeObjectStore
	opts     joinedSourcePurgeOptions
	logw     io.Writer
	summary  *joinedPurgeSummary
	mediaOK  map[int64]string // artifact id -> "" when verified, else the skip reason
	distinct int
}

func runJoinedSourcePurgePass(ctx context.Context, pool *pgxpool.Pool, store purgeObjectStore, opts joinedSourcePurgeOptions, logw io.Writer) (summary joinedPurgeSummary, err error) {
	summary = joinedPurgeSummary{Mode: "dry_run", BatchID: opts.batchID, HourRecordID: opts.hourRecordID, Skipped: map[string]int64{}, LogPath: opts.logPath}
	if opts.apply {
		summary.Mode = "apply"
	}
	defer func() {
		bytes := summary.EligibleBytes
		if opts.apply {
			bytes = summary.PurgedBytes
		}
		summary.MonthlySavedUSD = float64(bytes) / 1e9 * joinedSourcePurgeUSDPerGB
	}()
	r := &joinedPurgeRunner{pool: pool, store: store, opts: opts, logw: logw, summary: &summary, mediaOK: map[int64]string{}}
	interval := time.Duration(float64(time.Second) / opts.rate)
	var next time.Time
	var afterHour, afterSource int64
	for {
		if ctx.Err() != nil {
			summary.Interrupted = true
			return summary, nil
		}
		page, err := loadJoinedPurgeCandidates(ctx, pool, opts, afterHour, afterSource)
		if err != nil {
			if ctx.Err() != nil {
				summary.Interrupted = true
				return summary, nil
			}
			return summary, err
		}
		if len(page) == 0 {
			return summary, nil
		}
		for _, c := range page {
			afterHour, afterSource = c.HourRecordID, c.SourceID
			summary.Scanned++
			if reason := classifyJoinedPurgeCandidate(c, store.Bucket(), opts.requireNAS); reason != "" {
				summary.Skipped[reason]++
				r.logLine(c, "skip", reason)
				continue
			}
			if opts.limit > 0 && summary.Eligible >= int64(opts.limit) {
				summary.LimitReached = true
				return summary, nil
			}
			if !opts.apply && !opts.checkR2 {
				summary.Eligible++
				summary.EligibleBytes += c.SizeBytes
				r.logLine(c, "eligible", "")
				continue
			}
			if wait := time.Until(next); wait > 0 {
				select {
				case <-ctx.Done():
					summary.Interrupted = true
					return summary, nil
				case <-time.After(wait):
				}
			}
			next = time.Now().Add(interval)
			if err := r.process(ctx, c); err != nil {
				return summary, err
			}
		}
	}
}

// process verifies one candidate in R2 and, with --apply, purges it. It returns
// an error only for conditions that must stop the whole run.
func (r *joinedPurgeRunner) process(ctx context.Context, c joinedPurgeCandidate) error {
	reason, err := r.verifyMedia(ctx, c)
	if err != nil {
		return err
	}
	if reason != "" {
		r.summary.Skipped[reason]++
		r.logLine(c, "skip", reason)
		return nil
	}
	sourceAbsent := false
	head, err := r.store.Head(ctx, c.ObjectKey)
	switch {
	case err != nil && r2.IsNotFound(err):
		sourceAbsent = true
	case err != nil:
		return r.clipError(c, "source_head_failed", err)
	case head.SizeBytes != c.SizeBytes:
		r.summary.Skipped["source_size_mismatch"]++
		r.logLine(c, "skip", "source_size_mismatch")
		return nil
	}
	absentReason := ""
	if sourceAbsent {
		absentReason = "source_already_absent"
	}
	r.summary.Eligible++
	r.summary.EligibleBytes += c.SizeBytes
	if !r.opts.apply {
		r.logLine(c, "eligible", absentReason)
		return nil
	}
	purged, skip, err := purgeJoinedSourceClip(ctx, r.pool, r.store, c, r.opts)
	if err != nil {
		if ctx.Err() != nil {
			r.logLine(c, "error", "interrupted: "+err.Error())
			return nil
		}
		return r.clipError(c, "purge_failed", err)
	}
	if !purged {
		r.summary.Skipped[skip]++
		r.logLine(c, "skip", skip)
		return nil
	}
	r.summary.Purged++
	r.summary.PurgedBytes += c.SizeBytes
	if sourceAbsent {
		r.summary.SourceAbsent++
	}
	r.logLine(c, "purged", absentReason)
	return nil
}

func (r *joinedPurgeRunner) clipError(c joinedPurgeCandidate, reason string, err error) error {
	r.summary.Errors++
	r.logLine(c, "error", reason+": "+err.Error())
	if r.summary.Errors >= int64(r.opts.maxErrors) {
		return fmt.Errorf("aborting after %d errors (last: %s: %v)", r.summary.Errors, reason, err)
	}
	return nil
}

// verifyMedia checks each joined media object once per run: an exact HEAD at
// the published identity with the expected size, plus a full sha256 for every
// Nth distinct object. A sha256 mismatch aborts the run: published joined output
// that differs from its manifest means the purge's premise is broken.
func (r *joinedPurgeRunner) verifyMedia(ctx context.Context, c joinedPurgeCandidate) (string, error) {
	if reason, seen := r.mediaOK[c.MediaID]; seen {
		return reason, nil
	}
	r.summary.MediaHeads++
	reason := ""
	head, err := r.store.HeadExact(ctx, c.MediaKey, c.MediaETag, c.MediaVersion)
	switch {
	case err != nil:
		reason = "media_head_failed"
	case head.SizeBytes != c.MediaSize:
		reason = "media_size_mismatch"
	}
	if reason == "" && r.opts.shaEvery > 0 && r.distinct%r.opts.shaEvery == 0 {
		r.summary.MediaSHAChecks++
		sum, err := sha256JoinedObject(ctx, r.store, c)
		if err != nil {
			reason = "media_sha_read_failed"
		} else if sum != c.MediaSHA256 {
			r.mediaOK[c.MediaID] = "media_sha_mismatch"
			return "", fmt.Errorf("joined media artifact %d (%s) sha256 %s != expected %s", c.MediaID, c.MediaKey, sum, c.MediaSHA256)
		}
	}
	r.distinct++
	r.mediaOK[c.MediaID] = reason
	return reason, nil
}

func sha256JoinedObject(ctx context.Context, store purgeObjectStore, c joinedPurgeCandidate) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, joinedSourcePurgeShaTimeout)
	defer cancel()
	body, err := store.OpenExact(ctx, c.MediaKey, c.MediaETag, c.MediaVersion)
	if err != nil {
		return "", err
	}
	defer body.Close()
	h := sha256.New()
	n, err := io.Copy(h, body)
	if err != nil {
		return "", err
	}
	if n != c.MediaSize {
		return "", fmt.Errorf("read %d bytes, expected %d", n, c.MediaSize)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// purgeJoinedSourceClip is the single write path. Inside one read-committed
// transaction it locks the clip, re-checks its identity and the NAS copy, sets
// purged_at (the retention trigger re-validates eligibility and requires the
// verified GUC), deletes the source object, and commits. Any failure before the
// delete rolls purged_at back; the delete is the last step before the commit.
func purgeJoinedSourceClip(ctx context.Context, pool *pgxpool.Pool, store purgeObjectStore, c joinedPurgeCandidate, opts joinedSourcePurgeOptions) (bool, string, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, "", err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `SELECT set_config($1,$2,true)`, joinedSourcePurgeGUC, joinedSourcePurgeGUCValue); err != nil {
		return false, "", err
	}
	var objectKey, bucket, sha string
	var size int64
	var purgedAt *time.Time
	err = tx.QueryRow(ctx, `SELECT COALESCE(object_key,''), COALESCE(bucket,''), lower(COALESCE(sha256,'')), size_bytes, purged_at
		FROM recording_clips WHERE id=$1 FOR UPDATE`, c.ClipID).Scan(&objectKey, &bucket, &sha, &size, &purgedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, "clip_missing", nil
	}
	if err != nil {
		return false, "", err
	}
	if purgedAt != nil {
		return false, "already_purged", nil
	}
	if objectKey != c.ObjectKey || bucket != c.ClipBucket || sha != c.SHA256 || size != c.SizeBytes {
		return false, "clip_identity_changed", nil
	}
	if opts.requireNAS {
		var nasOK bool
		if err := tx.QueryRow(ctx, `SELECT `+joinedPurgeNASVerified("$3")+`
			FROM recording_clips c
			JOIN recording_joined_hours h ON h.id=$2
			JOIN recording_joined_batches b ON b.id=h.batch_record_id
			WHERE c.id=$1`,
			c.ClipID, c.HourRecordID, int64(opts.nasMaxAge/time.Second)).Scan(&nasOK); err != nil {
			return false, "", err
		}
		if !nasOK {
			return false, "nas_unverified", nil
		}
	}
	tag, err := tx.Exec(ctx, `UPDATE recording_clips SET purged_at=now() WHERE id=$1 AND purged_at IS NULL`, c.ClipID)
	if err != nil {
		if strings.Contains(err.Error(), "retention protected") {
			return false, "trigger_refused", nil
		}
		return false, "", err
	}
	if tag.RowsAffected() != 1 {
		return false, "already_purged", nil
	}
	// Past this point the delete and the commit must finish together: an
	// interrupt between them would leave an absent object behind a live row.
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), joinedSourcePurgeFinishTimeout)
	defer cancel()
	if err := store.DeleteObjects(finishCtx, []string{c.ObjectKey}); err != nil {
		return false, "", fmt.Errorf("delete source object: %w", err)
	}
	if err := tx.Commit(finishCtx); err != nil {
		return false, "", fmt.Errorf("commit after delete (a rerun completes it): %w", err)
	}
	return true, "", nil
}

func (r *joinedPurgeRunner) logLine(c joinedPurgeCandidate, action, reason string) {
	if r.logw == nil {
		return
	}
	line := joinedPurgeLogLine{At: time.Now().UTC(), Mode: r.summary.Mode, ClipID: c.ClipID, RecordingID: c.RecordingID,
		HourRecordID: c.HourRecordID, SizeBytes: c.SizeBytes, Action: action, Reason: reason}
	if action == "purged" || action == "error" {
		line.ObjectKey = c.ObjectKey
	}
	b, _ := json.Marshal(line)
	_, _ = r.logw.Write(append(b, '\n'))
}

func printJoinedPurgeSummary(s joinedPurgeSummary, asJSON bool) {
	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(s)
		return
	}
	fmt.Printf("mode=%s scanned=%d eligible=%d eligible_bytes=%d purged=%d purged_bytes=%d source_already_absent=%d errors=%d media_heads=%d media_sha_checks=%d limit_reached=%t interrupted=%t monthly_saved_usd=%.2f\n",
		s.Mode, s.Scanned, s.Eligible, s.EligibleBytes, s.Purged, s.PurgedBytes, s.SourceAbsent, s.Errors, s.MediaHeads, s.MediaSHAChecks, s.LimitReached, s.Interrupted, s.MonthlySavedUSD)
	reasons := make([]string, 0, len(s.Skipped))
	for k := range s.Skipped {
		reasons = append(reasons, k)
	}
	sort.Strings(reasons)
	for _, k := range reasons {
		fmt.Printf("skipped %s=%d\n", k, s.Skipped[k])
	}
	fmt.Printf("log=%s\n", s.LogPath)
}

type joinedRetentionHold struct {
	HourRecordID int64     `json:"hour_record_id"`
	ReasonCode   string    `json:"reason_code"`
	HourID       string    `json:"hour_id"`
	Note         string    `json:"note"`
	CreatedAt    time.Time `json:"created_at"`
}

func addJoinedRetentionHold(ctx context.Context, pool *pgxpool.Pool, hourRecordID int64, reason, note string) (bool, error) {
	tag, err := pool.Exec(ctx, `INSERT INTO recording_joined_source_retention_holds(hour_record_id, reason_code, note)
		VALUES ($1,$2,$3) ON CONFLICT (hour_record_id) DO NOTHING`, hourRecordID, reason, strings.TrimSpace(note))
	if err != nil {
		return false, fmt.Errorf("add retention hold: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func listJoinedRetentionHolds(ctx context.Context, pool *pgxpool.Pool) ([]joinedRetentionHold, error) {
	rows, err := pool.Query(ctx, `SELECT hold.hour_record_id, hold.reason_code, h.hour_id, hold.note, hold.created_at
		FROM recording_joined_source_retention_holds hold JOIN recording_joined_hours h ON h.id=hold.hour_record_id
		ORDER BY hold.hour_record_id`)
	if err != nil {
		return nil, fmt.Errorf("list retention holds: %w", err)
	}
	defer rows.Close()
	out := []joinedRetentionHold{}
	for rows.Next() {
		var h joinedRetentionHold
		if err := rows.Scan(&h.HourRecordID, &h.ReasonCode, &h.HourID, &h.Note, &h.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}
