package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/daydemir/stoarama/backend/internal/collation"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/recordingnaming"
	"github.com/jackc/pgx/v5/pgxpool"
)

const collationV2Usage = `usage:
  stoaramactl collation-v2 plan --scope backfill|nightly --out worklist.jsonl [--broken-ids FILE] [--recordings 1,2] [--hour-ids FILE] [--date YYYY-MM-DD | --days N]
  stoaramactl collation-v2 run --worklist FILE|r2:KEY|r2prefix:PREFIX --scratch DIR --results FILE [--hour-workers N --cpu N --net N --publish-dir DIR --keep-scratch]
  stoaramactl collation-v2 register --worklist FILE|r2:KEY|r2prefix:PREFIX [--dry-run]
  stoaramactl collation-v2 put-worklist --worklist FILE --key r2-key`

// NightlyBatchID names the rolling generation-2 batch for closed local days.
const NightlyBatchID = "nightly-collation-v2"

func runCollationV2(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 {
		log.Fatal(collationV2Usage)
	}
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	fs := flag.NewFlagSet("collation-v2 "+args[0], flag.ExitOnError)
	switch args[0] {
	case "plan":
		scope := fs.String("scope", "", "backfill|nightly")
		out := fs.String("out", "", "worklist output path")
		broken := fs.String("broken-ids", "", "file of known-unplayable clip ids (one per line)")
		recordings := fs.String("recordings", "", "optional comma-separated recording ids")
		hourIDs := fs.String("hour-ids", "", "optional file of hour ids to keep")
		date := fs.String("date", "", "nightly: local date to plan (default: the last --days closed days in each recording's timezone)")
		days := fs.Int("days", 2, "nightly: how many closed local days to (re)plan; published hours are skipped by run")
		_ = fs.Parse(args[1:])
		if *out == "" || (*scope != "backfill" && *scope != "nightly") {
			log.Fatal(collationV2Usage)
		}
		pool := mustCollationPool(ctx, cfg)
		defer pool.Close()
		opts := collationPlanOptions{scope: *scope, date: *date, days: *days, now: time.Now()}
		var err error
		if opts.broken, err = readIDSet(*broken); err != nil {
			log.Fatal(err)
		}
		if opts.recordings, err = parseIDList(*recordings); err != nil {
			log.Fatal(err)
		}
		if opts.hourIDs, err = readLineSet(*hourIDs); err != nil {
			log.Fatal(err)
		}
		work, err := planCollation(ctx, pool, opts)
		if err != nil {
			log.Fatal(err)
		}
		if err := writeWorklist(*out, work); err != nil {
			log.Fatal(err)
		}
		clips, purged, knownBroken := 0, 0, 0
		for _, w := range work {
			clips += len(w.Clips)
			for _, c := range w.Clips {
				if c.Purged {
					purged++
				}
				if c.KnownBroken {
					knownBroken++
				}
			}
		}
		fmt.Printf("hours=%d clips=%d purged=%d known_broken=%d out=%s\n", len(work), clips, purged, knownBroken, *out)
	case "run":
		worklist := fs.String("worklist", "", "worklist path or r2:KEY")
		scratch := fs.String("scratch", "", "scratch directory")
		results := fs.String("results", "", "results JSONL (appended)")
		hourWorkers := fs.Int("hour-workers", max(runtime.NumCPU()/2, 2), "hours processed concurrently")
		cpu := fs.Int("cpu", runtime.NumCPU(), "concurrent ffmpeg/ffprobe processes")
		net := fs.Int("net", 32, "concurrent downloads")
		publishDir := fs.String("publish-dir", "", "publish outputs and manifests to this local dir instead of R2 (canary/dry run)")
		keepScratch := fs.Bool("keep-scratch", false, "keep downloaded sources per hour (canary review)")
		_ = fs.Parse(args[1:])
		if *worklist == "" || *scratch == "" || *results == "" || *hourWorkers < 1 || *cpu < 1 || *net < 1 {
			log.Fatal(collationV2Usage)
		}
		client := mustArchiveR2Client(ctx, cfg)
		store := collation.R2Store{Client: client}
		work, err := loadWorklist(ctx, store, *worklist)
		if err != nil {
			log.Fatal(err)
		}
		tools := collation.ToolsFromEnv()
		version, err := tools.Version(ctx)
		if err != nil {
			log.Fatal(err)
		}
		if err := os.MkdirAll(*scratch, 0o700); err != nil {
			log.Fatal(err)
		}
		rf, err := os.OpenFile(*results, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			log.Fatal(err)
		}
		defer rf.Close()
		env := collation.Env{Tools: tools, Policy: collation.DefaultSeamPolicy(), ScratchRoot: *scratch, Store: store, MediaTool: version,
			CPU: make(chan struct{}, *cpu), Net: make(chan struct{}, *net), Now: time.Now, KeepScratch: *keepScratch}
		var existence collation.ExistenceChecker = store
		if *publishDir != "" {
			local := collation.LocalPublishStore{R2Store: store, Dir: *publishDir}
			env.Store, existence = local, local
		}
		log.Printf("collation-v2 run: hours=%d hour_workers=%d cpu=%d net=%d publish_dir=%q tool=%q", len(work), *hourWorkers, *cpu, *net, *publishDir, version)
		if err := collation.RunWorklist(ctx, env, existence, work, *hourWorkers, rf); err != nil {
			log.Fatal(err)
		}
	case "register":
		worklist := fs.String("worklist", "", "worklist path or r2:KEY")
		dryRun := fs.Bool("dry-run", false, "validate manifests without writing")
		_ = fs.Parse(args[1:])
		if *worklist == "" {
			log.Fatal(collationV2Usage)
		}
		client := mustArchiveR2Client(ctx, cfg)
		store := collation.R2Store{Client: client}
		work, err := loadWorklist(ctx, store, *worklist)
		if err != nil {
			log.Fatal(err)
		}
		pool := mustCollationPool(ctx, cfg)
		defer pool.Close()
		summary, err := registerCollation(ctx, pool, store, work, *dryRun)
		out, _ := json.Marshal(summary)
		fmt.Println(string(out))
		if err != nil {
			log.Fatal(err)
		}
	case "put-worklist":
		worklist := fs.String("worklist", "", "local worklist path")
		key := fs.String("key", "", "R2 key")
		_ = fs.Parse(args[1:])
		if *worklist == "" || !strings.HasPrefix(*key, "collation-v2/worklists/") {
			log.Fatal(collationV2Usage)
		}
		body, err := os.ReadFile(*worklist)
		if err != nil {
			log.Fatal(err)
		}
		client := mustArchiveR2Client(ctx, cfg)
		if _, err := client.PutBytes(ctx, *key, "application/x-ndjson", body); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("put %s bytes=%d\n", *key, len(body))
	default:
		log.Fatal(collationV2Usage)
	}
}

func mustCollationPool(ctx context.Context, cfg config.Config) *pgxpool.Pool {
	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		log.Fatal("invalid database configuration")
	}
	poolCfg.MaxConns = 4
	poolCfg.ConnConfig.RuntimeParams["application_name"] = "stoarama-collation-v2"
	poolCfg.ConnConfig.RuntimeParams["statement_timeout"] = "300s"
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		log.Fatalf("connect database: %v", err)
	}
	return pool
}

func loadWorklist(ctx context.Context, store collation.R2Store, ref string) ([]collation.HourWork, error) {
	if prefix, ok := strings.CutPrefix(ref, "r2prefix:"); ok {
		if !strings.HasPrefix(prefix, "collation-v2/worklists/") {
			return nil, fmt.Errorf("worklist prefix must be under collation-v2/worklists/")
		}
		var keys []string
		if err := store.Client.ListPrefix(ctx, prefix, 0, func(o r2.ObjectInfo) error {
			if strings.HasSuffix(o.Key, ".jsonl") {
				keys = append(keys, o.Key)
			}
			return nil
		}); err != nil {
			return nil, err
		}
		seen := map[string]bool{}
		var all []collation.HourWork
		for _, key := range keys {
			work, err := loadWorklist(ctx, store, "r2:"+key)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", key, err)
			}
			for _, w := range work {
				if !seen[w.HourID] {
					seen[w.HourID] = true
					all = append(all, w)
				}
			}
		}
		return all, nil
	}
	if key, ok := strings.CutPrefix(ref, "r2:"); ok {
		body, err := store.Client.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		return collation.ReadWorklist(bytes.NewReader(body))
	}
	f, err := os.Open(ref)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return collation.ReadWorklist(f)
}

func writeWorklist(path string, work []collation.HourWork) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	for _, h := range work {
		if err := enc.Encode(h); err != nil {
			f.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func readLineSet(path string) (map[string]bool, error) {
	if path == "" {
		return nil, nil
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, line := range strings.Split(string(body), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			out[line] = true
		}
	}
	return out, nil
}

func readIDSet(path string) (map[int64]bool, error) {
	lines, err := readLineSet(path)
	if err != nil || lines == nil {
		return map[int64]bool{}, err
	}
	out := make(map[int64]bool, len(lines))
	for line := range lines {
		id, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("bad id %q in %s", line, path)
		}
		out[id] = true
	}
	return out, nil
}

func parseIDList(raw string) (map[int64]bool, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	out := map[int64]bool{}
	for _, part := range strings.Split(raw, ",") {
		id, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
		if err != nil || id <= 0 {
			return nil, fmt.Errorf("bad recording id %q", part)
		}
		out[id] = true
	}
	return out, nil
}

type collationPlanOptions struct {
	scope      string
	date       string
	days       int
	now        time.Time
	broken     map[int64]bool
	recordings map[int64]bool
	hourIDs    map[string]bool
}

// collationDay is one planned local day for one recording.
type collationDay struct {
	batchID     string
	recordingID int64
	localDate   string
}

type collationRecording struct {
	id            int64
	timezone      string
	namingProfile string
	folderName    string
	metadata      recordingnaming.Metadata
	windowStart   string
	windowEnd     string
	clipSeconds   float64
}

// planCollation freezes the hours to (re)collate. Backfill = the approved
// good+ qualification windows of both cohorts (33 via the tier-1 run, 9 via the
// September cohort's first dates); nightly = the last closed local day of every
// active continuous 08:00-20:00 recording.
func planCollation(ctx context.Context, pool *pgxpool.Pool, opts collationPlanOptions) ([]collation.HourWork, error) {
	var days []collationDay
	switch opts.scope {
	case "backfill":
		var runID int64
		if err := pool.QueryRow(ctx, `SELECT qualification_run_id FROM recording_joined_batches WHERE batch_id=$1`, joinedrecording.Tier1BatchID).Scan(&runID); err != nil {
			return nil, fmt.Errorf("tier-1 batch: %w", err)
		}
		rows, err := pool.Query(ctx, `SELECT w.recording_id, to_char((w.window_start_at AT TIME ZONE r.cron_timezone)::date,'YYYY-MM-DD')
			FROM recording_qualification_windows w JOIN recordings r ON r.id=w.recording_id WHERE w.run_id=$1 ORDER BY 1, 2`, runID)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			d := collationDay{batchID: joinedrecording.Tier1BatchID}
			if err := rows.Scan(&d.recordingID, &d.localDate); err != nil {
				rows.Close()
				return nil, err
			}
			days = append(days, d)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
		sep := joinedrecording.CohortForBatch(joinedrecording.SeptemberBatchID)
		for i, id := range sep.RecordingIDs {
			first, err := time.Parse("2006-01-02", sep.FirstDates[i])
			if err != nil {
				return nil, err
			}
			for k := 0; k < 14; k++ {
				days = append(days, collationDay{batchID: joinedrecording.SeptemberBatchID, recordingID: id, localDate: first.AddDate(0, 0, k).Format("2006-01-02")})
			}
		}
	case "nightly":
		rows, err := pool.Query(ctx, `SELECT id, cron_timezone FROM recordings WHERE status='active' AND mode='continuous'
			AND delivery='nas_pull' AND daily_window_start='08:00'::time AND daily_window_end='20:00'::time ORDER BY id`)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id int64
			var tz string
			if err := rows.Scan(&id, &tz); err != nil {
				rows.Close()
				return nil, err
			}
			loc, err := time.LoadLocation(tz)
			if err != nil {
				rows.Close()
				return nil, err
			}
			if opts.date != "" {
				days = append(days, collationDay{batchID: NightlyBatchID, recordingID: id, localDate: opts.date})
				continue
			}
			for k := max(opts.days, 1); k >= 1; k-- {
				days = append(days, collationDay{batchID: NightlyBatchID, recordingID: id, localDate: opts.now.In(loc).AddDate(0, 0, -k).Format("2006-01-02")})
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	byRecording := map[int64][]collationDay{}
	var order []int64
	for _, d := range days {
		if opts.recordings != nil && !opts.recordings[d.recordingID] {
			continue
		}
		if _, ok := byRecording[d.recordingID]; !ok {
			order = append(order, d.recordingID)
		}
		byRecording[d.recordingID] = append(byRecording[d.recordingID], d)
	}
	var work []collation.HourWork
	for _, recordingID := range order {
		rec, err := loadCollationRecording(ctx, pool, recordingID)
		if err != nil {
			return nil, err
		}
		loc, err := time.LoadLocation(rec.timezone)
		if err != nil {
			return nil, err
		}
		gen1, err := loadGen1Hours(ctx, pool, recordingID)
		if err != nil {
			return nil, err
		}
		for _, d := range byRecording[recordingID] {
			date, err := time.ParseInLocation("2006-01-02", d.localDate, loc)
			if err != nil {
				return nil, err
			}
			if opts.scope == "nightly" && (rec.windowStart != "08:00:00" || rec.windowEnd != "20:00:00") {
				continue
			}
			dayStart := time.Date(date.Year(), date.Month(), date.Day(), 8, 0, 0, 0, loc)
			// A day that has not closed would freeze a partial clip set into a
			// create-only manifest; never plan it.
			if !opts.now.After(time.Date(date.Year(), date.Month(), date.Day(), 20, 0, 0, 0, loc).Add(15 * time.Minute)) {
				if opts.scope == "nightly" {
					return nil, fmt.Errorf("recording %d day %s is not closed yet", recordingID, d.localDate)
				}
				log.Printf("collation-v2 plan: skipping open day recording=%d date=%s", recordingID, d.localDate)
				continue
			}
			clips, err := loadCollationClips(ctx, pool, recordingID, dayStart, dayStart.Add(12*time.Hour), opts.broken, rec.clipSeconds)
			if err != nil {
				return nil, err
			}
			for h := 1; h <= 12; h++ {
				hourID, err := collation.HourIDFor(d.batchID, recordingID, d.localDate, h)
				if err != nil {
					return nil, err
				}
				if opts.hourIDs != nil && !opts.hourIDs[hourID] {
					continue
				}
				start := time.Date(date.Year(), date.Month(), date.Day(), 7+h, 0, 0, 0, loc)
				end := time.Date(date.Year(), date.Month(), date.Day(), 8+h, 0, 0, 0, loc)
				w := collation.HourWork{BatchID: d.batchID, HourID: hourID, RecordingID: recordingID, Timezone: rec.timezone, LocalDate: d.localDate,
					DeliveryHour: h, ScheduledStart: start.UTC(), ScheduledEnd: end.UTC(), NamingProfile: rec.namingProfile, FolderName: rec.folderName,
					Metadata: rec.metadata, Clips: []collation.Clip{}}
				for _, c := range clips {
					if !c.StartUTC.Before(start) && c.StartUTC.Before(end) {
						w.Clips = append(w.Clips, c)
					}
				}
				if gen1ID, ok := gen1[collationGen1Key{d.batchID, d.localDate, h}]; ok {
					w.SupersedesHourRecordID = gen1ID.id
					w.SupersedesHourID = gen1ID.hourID
				}
				work = append(work, w)
			}
		}
	}
	return work, nil
}

type collationGen1Key struct {
	batchID   string
	localDate string
	hour      int
}

type collationGen1Hour struct {
	id     int64
	hourID string
}

// loadGen1Hours maps every generation-1 joined hour of a recording to its row.
func loadGen1Hours(ctx context.Context, pool *pgxpool.Pool, recordingID int64) (map[collationGen1Key]collationGen1Hour, error) {
	rows, err := pool.Query(ctx, `SELECT id, hour_id, batch_id, to_char(local_date,'YYYY-MM-DD'), delivery_hour FROM recording_joined_hours
		WHERE recording_id=$1 AND hour_id LIKE '%__generation-1'`, recordingID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[collationGen1Key]collationGen1Hour{}
	for rows.Next() {
		var h collationGen1Hour
		var k collationGen1Key
		if err := rows.Scan(&h.id, &h.hourID, &k.batchID, &k.localDate, &k.hour); err != nil {
			return nil, err
		}
		out[k] = h
	}
	return out, rows.Err()
}

func loadCollationRecording(ctx context.Context, pool *pgxpool.Pool, id int64) (collationRecording, error) {
	rec := collationRecording{id: id}
	var metaRaw []byte
	if err := pool.QueryRow(ctx, `SELECT cron_timezone, naming_profile, folder_name, COALESCE(naming_metadata_jsonb,'{}'::jsonb),
		daily_window_start::text, daily_window_end::text, clip_duration_sec::float8 FROM recordings WHERE id=$1`, id).
		Scan(&rec.timezone, &rec.namingProfile, &rec.folderName, &metaRaw, &rec.windowStart, &rec.windowEnd, &rec.clipSeconds); err != nil {
		return rec, fmt.Errorf("recording %d: %w", id, err)
	}
	if rec.namingProfile == string(recordingnaming.ProfilePlazaHourlyV1) {
		meta, err := recordingnaming.ParseMetadata(metaRaw)
		if err != nil {
			return rec, fmt.Errorf("recording %d naming metadata: %w", id, err)
		}
		rec.metadata = meta
		folder, err := recordingnaming.BuildFolderName(recordingnaming.ProfilePlazaHourlyV1, id, meta, rec.folderName)
		if err != nil {
			return rec, err
		}
		rec.folderName = folder
	} else if strings.TrimSpace(rec.folderName) == "" {
		rec.folderName = "recordings"
	}
	return rec, nil
}

func loadCollationClips(ctx context.Context, pool *pgxpool.Pool, recordingID int64, from, to time.Time, broken map[int64]bool, nominal float64) ([]collation.Clip, error) {
	rows, err := pool.Query(ctx, `SELECT id, recording_id, COALESCE(recording_job_id,0), COALESCE(capture_attempt_id::text,''),
		COALESCE(capture_lease_token::text,''), COALESCE(capture_sequence,0), clip_start_at, clip_end_at, bucket, object_key, etag,
		size_bytes, sha256, purged_at IS NOT NULL
		FROM recording_clips WHERE recording_id=$1 AND clip_start_at >= $2 AND clip_start_at < $3 ORDER BY clip_start_at, id`, recordingID, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []collation.Clip
	for rows.Next() {
		var c collation.Clip
		if err := rows.Scan(&c.ClipID, &c.RecordingID, &c.JobID, &c.CaptureAttemptID, &c.CaptureLeaseToken, &c.CaptureSequence,
			&c.StartUTC, &c.EndUTC, &c.Bucket, &c.ObjectKey, &c.ETag, &c.SizeBytes, &c.SHA256, &c.Purged); err != nil {
			return nil, err
		}
		c.StartUTC, c.EndUTC = c.StartUTC.UTC(), c.EndUTC.UTC()
		c.KnownBroken = broken[c.ClipID]
		c.NominalSeconds = nominal
		out = append(out, c)
	}
	return out, rows.Err()
}

type collationRegisterSummary struct {
	Hours      int            `json:"hours"`
	Registered int            `json:"registered"`
	Already    int            `json:"already_registered"`
	Missing    int            `json:"manifest_missing"`
	Outputs    int            `json:"outputs"`
	ByStatus   map[string]int `json:"by_status"`
}

// registerCollation records published generation-2 manifests. It never
// mutates or deletes generation-1 rows or objects; supersession is the
// generation-2 row's pointer to the generation-1 hour.
func registerCollation(ctx context.Context, pool *pgxpool.Pool, store collation.R2Store, work []collation.HourWork, dryRun bool) (collationRegisterSummary, error) {
	s := collationRegisterSummary{Hours: len(work), ByStatus: map[string]int{}}
	for _, w := range work {
		key := collation.ManifestKey(w.BatchID, w.HourID)
		var exists bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM recording_collation_hours WHERE hour_id=$1)`, w.HourID).Scan(&exists); err != nil {
			return s, err
		}
		if exists {
			s.Already++
			continue
		}
		body, err := store.Client.Get(ctx, key)
		if err != nil {
			ok, existsErr := store.Exists(ctx, key)
			if existsErr != nil {
				return s, fmt.Errorf("%s: %w", key, errors.Join(err, existsErr))
			}
			if !ok {
				s.Missing++
				continue
			}
			return s, err
		}
		var m collation.HourManifest
		if err := json.Unmarshal(body, &m); err != nil {
			return s, fmt.Errorf("%s: %w", key, err)
		}
		if err := m.Validate(); err != nil {
			return s, fmt.Errorf("%s: %w", key, err)
		}
		if m.HourID != w.HourID || m.SupersedesHourID != w.SupersedesHourID {
			return s, fmt.Errorf("%s: manifest differs from worklist", key)
		}
		for _, o := range m.Outputs {
			head, err := store.Client.Head(ctx, o.ObjectKey)
			if err != nil || head.SizeBytes != o.SizeBytes {
				return s, fmt.Errorf("%s part %d: R2 object missing or wrong size", key, o.Part)
			}
		}
		s.ByStatus[m.Status]++
		s.Outputs += len(m.Outputs)
		if dryRun {
			continue
		}
		if err := insertCollationHour(ctx, pool, w, m, key, body); err != nil {
			return s, fmt.Errorf("%s: %w", key, err)
		}
		s.Registered++
	}
	return s, nil
}

func insertCollationHour(ctx context.Context, pool *pgxpool.Pool, w collation.HourWork, m collation.HourManifest, key string, body []byte) error {
	joins, splits, included, quarantined := 0, 0, 0, 0
	for _, seam := range m.Seams {
		if seam.Decision == collation.DecisionJoin {
			joins++
		} else {
			splits++
		}
	}
	for _, c := range m.Clips {
		if c.Disposition == "included" {
			included++
		} else {
			quarantined++
		}
	}
	var supersedes *int64
	if w.SupersedesHourRecordID > 0 {
		supersedes = &w.SupersedesHourRecordID
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var hourRecordID int64
	if err := tx.QueryRow(ctx, `INSERT INTO recording_collation_hours(hour_id,batch_id,generation,recording_id,local_date,delivery_hour,status,
		supersedes_joined_hour_id,manifest_object_key,manifest_sha256,manifest,source_clip_count,included_clip_count,excluded_clip_count,join_count,split_count)
		VALUES($1,$2,$3,$4,$5::date,$6,$7,$8,$9,encode(sha256($10::bytea),'hex'),convert_from($10::bytea,'UTF8')::jsonb,$11,$12,$13,$14,$15) RETURNING id`,
		m.HourID, m.BatchID, m.Generation, m.RecordingID, m.LocalDate, m.DeliveryHour, m.Status, supersedes, key, body,
		len(m.Clips), included, quarantined, joins, splits).Scan(&hourRecordID); err != nil {
		return err
	}
	for _, o := range m.Outputs {
		if _, err := tx.Exec(ctx, `INSERT INTO recording_collation_outputs(collation_hour_id,part,parts,object_key,nas_relative_path,size_bytes,sha256,
			start_at,end_at,source_clip_ids,r2_verified_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
			hourRecordID, o.Part, o.Parts, o.ObjectKey, o.NASRelativePath, o.SizeBytes, o.SHA256, o.StartUTC, o.EndUTC, o.SourceClipIDs, o.R2VerifiedAt); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
