package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/config"
)

// nas-day-inventory queues read-only stream-day NAS inventories that the
// r2-copy-v8-inventory client runs (internal/api/server_nas_day_inventory.go),
// and reports what each found. A clip it matched by name and size, with the
// catalogued sha256, can then be restored with nas-restore request (target
// original, create-only, from the path the inventory read).

const nasDayInventoryUsage = `usage:
  stoaramactl nas-day-inventory scope --days-file TSV --reason TEXT [--recording-ids 1,2,3 --dry-run]
      approves those days in nas_restore_day_scope (delete a row there to withdraw it)
  stoaramactl nas-day-inventory request --days-file TSV --label LABEL [--recording-ids 1,2,3 --max-attempts 3 --dry-run --json]
      TSV has a header row with recording_id and local_date (YYYY-MM-DD) columns; other columns are ignored.
  stoaramactl nas-day-inventory status --label LABEL [--json]
  stoaramactl nas-day-inventory cancel --label LABEL   (also withdraws the label's restore scope)`

type nasDayInventoryDay struct {
	RecordingID int64
	LocalDate   string
}

// readNASDayInventoryDays reads the operator's day list, keeping only the
// given recordings when any are named; each must appear in the file.
func readNASDayInventoryDays(r io.Reader, only []int64) ([]nasDayInventoryDay, error) {
	reader := csv.NewReader(r)
	reader.Comma, reader.FieldsPerRecord = '\t', -1
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, errors.New("days file is empty")
	}
	recCol, dateCol := slices.Index(rows[0], "recording_id"), slices.Index(rows[0], "local_date")
	if recCol < 0 || dateCol < 0 {
		return nil, errors.New("days file header needs recording_id and local_date")
	}
	seen := map[nasDayInventoryDay]bool{}
	found := map[int64]bool{}
	var days []nasDayInventoryDay
	for i, row := range rows[1:] {
		if len(row) <= max(recCol, dateCol) {
			return nil, fmt.Errorf("days file line %d is short", i+2)
		}
		id, err := strconv.ParseInt(row[recCol], 10, 64)
		if err != nil || id <= 0 {
			return nil, fmt.Errorf("days file line %d: invalid recording_id %q", i+2, row[recCol])
		}
		if _, err := time.Parse(time.DateOnly, row[dateCol]); err != nil {
			return nil, fmt.Errorf("days file line %d: invalid local_date %q", i+2, row[dateCol])
		}
		day := nasDayInventoryDay{id, row[dateCol]}
		if seen[day] {
			return nil, fmt.Errorf("days file line %d repeats recording %d %s", i+2, id, day.LocalDate)
		}
		seen[day] = true
		if len(only) > 0 && !slices.Contains(only, id) {
			continue
		}
		found[id] = true
		days = append(days, day)
	}
	for _, id := range only {
		if !found[id] {
			return nil, fmt.Errorf("days file has no day for recording %d", id)
		}
	}
	if len(days) == 0 {
		return nil, errors.New("days file selects no day")
	}
	return days, nil
}

// scopeNASDayInventories approves every day or none; it returns how many rows
// were new.
func scopeNASDayInventories(ctx context.Context, pool *pgxpool.Pool, reason string, dryRun bool, days []nasDayInventoryDay) (int, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	approved := 0
	for _, day := range days {
		tag, err := tx.Exec(ctx, `INSERT INTO nas_restore_day_scope(recording_id,local_date,reason) VALUES($1,$2::date,$3) ON CONFLICT DO NOTHING`,
			day.RecordingID, day.LocalDate, reason)
		if err != nil {
			return 0, fmt.Errorf("approve recording %d %s: %w", day.RecordingID, day.LocalDate, err)
		}
		approved += int(tag.RowsAffected())
	}
	if dryRun {
		return approved, nil
	}
	return approved, tx.Commit(ctx)
}

type nasDayInventoryRequestSummary struct {
	Label         string `json:"label"`
	DryRun        bool   `json:"dry_run"`
	Queued        int    `json:"queued"`
	AlreadyQueued int    `json:"already_queued"`
}

// requestNASDayInventories queues every day or none: an unknown recording, a
// stream without a pinned window or an ambiguous NAS connection refuses all.
func requestNASDayInventories(ctx context.Context, pool *pgxpool.Pool, label string, maxAttempts int, dryRun bool, days []nasDayInventoryDay) (nasDayInventoryRequestSummary, error) {
	summary := nasDayInventoryRequestSummary{Label: label, DryRun: dryRun}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return summary, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	for _, day := range days {
		var stream *int64
		var conn, conns int64
		var windowed bool
		err := tx.QueryRow(ctx, `SELECT r.stream_id,
  COALESCE((SELECT min(k.id) FROM connections k WHERE k.account_id=r.account_id AND k.kind='nas_pull'),0),
  (SELECT count(*) FROM connections k WHERE k.account_id=r.account_id AND k.kind='nas_pull'),
  EXISTS(SELECT 1 FROM nas_media_keep20_windows w WHERE w.stream_id=r.stream_id)
 FROM recordings r WHERE r.id=$1`, day.RecordingID).Scan(&stream, &conn, &conns, &windowed)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			return summary, fmt.Errorf("recording %d does not exist", day.RecordingID)
		case err != nil:
			return summary, err
		case stream == nil || !windowed:
			return summary, fmt.Errorf("recording %d has no stream with a pinned window", day.RecordingID)
		case conns != 1:
			return summary, fmt.Errorf("recording %d has %d NAS connections, want 1", day.RecordingID, conns)
		}
		tag, err := tx.Exec(ctx, `INSERT INTO nas_day_inventory_units(connection_id,label,recording_id,local_date,max_attempts)
 VALUES($1,$2,$3,$4::date,$5) ON CONFLICT DO NOTHING`, conn, label, day.RecordingID, day.LocalDate, maxAttempts)
		if err != nil {
			return summary, fmt.Errorf("queue recording %d %s: %w", day.RecordingID, day.LocalDate, err)
		}
		if tag.RowsAffected() == 0 {
			summary.AlreadyQueued++
		} else {
			summary.Queued++
		}
	}
	if dryRun {
		return summary, nil
	}
	return summary, tx.Commit(ctx)
}

type nasDayInventoryUnitStatus struct {
	RecordingID  int64  `json:"recording_id"`
	LocalDate    string `json:"local_date"`
	State        string `json:"state"`
	Attempts     int    `json:"attempts"`
	LastError    string `json:"last_error,omitempty"`
	CatalogClips int64  `json:"catalog_clips"`
	FoundRaw     int64  `json:"found_raw"`
	FoundJoined  int64  `json:"found_joined"`
	Matched      int64  `json:"matched"`
	MatchedSHAOK int64  `json:"matched_sha_ok"`
	Unmatched    int64  `json:"unmatched_raw"`
	RawBytes     int64  `json:"raw_bytes"`
}

// catalog_clips counts the recording's clips starting on that local day.
const nasDayInventoryStatusSQL = `SELECT u.recording_id, u.local_date::text, u.state, u.attempts, u.last_error,
  (SELECT count(*) FROM recording_clips c WHERE c.recording_id=u.recording_id
    AND c.clip_start_at>=u.local_date::timestamp AT TIME ZONE w.timezone AND c.clip_start_at<(u.local_date+1)::timestamp AT TIME ZONE w.timezone),
  count(f.relative_path) FILTER (WHERE f.kind='raw'), count(f.relative_path) FILTER (WHERE f.kind='joined'),
  count(f.clip_id), count(f.clip_id) FILTER (WHERE f.sha256=lower(c.sha256)),
  COALESCE(sum(f.size_bytes) FILTER (WHERE f.kind='raw'),0)
 FROM nas_day_inventory_units u
 JOIN recordings r ON r.id=u.recording_id
 JOIN nas_media_keep20_windows w ON w.stream_id=r.stream_id
 LEFT JOIN nas_day_inventory_files f ON f.unit_id=u.id
 LEFT JOIN recording_clips c ON c.id=f.clip_id
 WHERE u.label=$1 GROUP BY u.id, w.timezone ORDER BY u.recording_id, u.local_date`

func loadNASDayInventoryStatus(ctx context.Context, pool *pgxpool.Pool, label string) ([]nasDayInventoryUnitStatus, error) {
	rows, err := pool.Query(ctx, nasDayInventoryStatusSQL, label)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (nasDayInventoryUnitStatus, error) {
		var u nasDayInventoryUnitStatus
		err := row.Scan(&u.RecordingID, &u.LocalDate, &u.State, &u.Attempts, &u.LastError, &u.CatalogClips,
			&u.FoundRaw, &u.FoundJoined, &u.Matched, &u.MatchedSHAOK, &u.RawBytes)
		u.Unmatched = u.FoundRaw - u.Matched
		return u, err
	})
}

func runNASDayInventory(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 {
		log.Fatal(nasDayInventoryUsage)
	}
	fs := flag.NewFlagSet("nas-day-inventory "+args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	label := fs.String("label", "", "inventory label")
	daysFile := fs.String("days-file", "", "TSV of recording_id and local_date")
	recordingIDs := fs.String("recording-ids", "", "only these recordings of the days file")
	maxAttempts := fs.Int("max-attempts", 3, "scan attempts per day before it fails")
	dryRun := fs.Bool("dry-run", false, "validate only; queue nothing")
	asJSON := fs.Bool("json", false, "print JSON")
	reason := fs.String("reason", "", "why these days are approved")
	// scope takes no label; every other subcommand needs one.
	labeled := args[0] != "scope"
	if err := fs.Parse(args[1:]); err != nil || len(fs.Args()) != 0 || labeled != nasRestoreLabelRE.MatchString(*label) {
		log.Fatal(nasDayInventoryUsage)
	}
	pool := mustJoinedPurgePool(ctx, cfg)
	defer pool.Close()
	switch args[0] {
	case "scope", "request":
		if *daysFile == "" || *maxAttempts < 1 || *maxAttempts > 10 || (args[0] == "scope") == (strings.TrimSpace(*reason) == "") {
			log.Fatal(nasDayInventoryUsage)
		}
		var only []int64
		for _, raw := range strings.FieldsFunc(*recordingIDs, func(r rune) bool { return r == ',' }) {
			id, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
			if err != nil || id <= 0 {
				log.Fatalf("invalid recording id %q", raw)
			}
			only = append(only, id)
		}
		file, err := os.Open(*daysFile)
		if err != nil {
			log.Fatal(err)
		}
		days, err := readNASDayInventoryDays(file, only)
		file.Close()
		if err != nil {
			log.Fatalf("nas-day-inventory request: %v", err)
		}
		if args[0] == "scope" {
			approved, err := scopeNASDayInventories(ctx, pool, *reason, *dryRun, days)
			if err != nil {
				log.Fatalf("nas-day-inventory scope: %v", err)
			}
			fmt.Printf("dry_run=%t approved=%d already_approved=%d\n", *dryRun, approved, len(days)-approved)
			return
		}
		summary, err := requestNASDayInventories(ctx, pool, *label, *maxAttempts, *dryRun, days)
		if err != nil {
			log.Fatalf("nas-day-inventory request: %v", err)
		}
		if *asJSON {
			_ = json.NewEncoder(os.Stdout).Encode(summary)
			return
		}
		fmt.Printf("label=%s dry_run=%t queued=%d already_queued=%d\n", summary.Label, summary.DryRun, summary.Queued, summary.AlreadyQueued)
	case "status":
		units, err := loadNASDayInventoryStatus(ctx, pool, *label)
		if err != nil {
			log.Fatal(err)
		}
		if *asJSON {
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			_ = enc.Encode(units)
			return
		}
		var total nasDayInventoryUnitStatus
		for _, u := range units {
			fmt.Printf("recording=%d local_date=%s state=%s attempts=%d catalog_clips=%d found_raw=%d found_joined=%d matched=%d matched_sha_ok=%d unmatched_raw=%d raw_bytes=%d %s\n",
				u.RecordingID, u.LocalDate, u.State, u.Attempts, u.CatalogClips, u.FoundRaw, u.FoundJoined, u.Matched, u.MatchedSHAOK, u.Unmatched, u.RawBytes, u.LastError)
			total.CatalogClips += u.CatalogClips
			total.FoundRaw += u.FoundRaw
			total.FoundJoined += u.FoundJoined
			total.Matched += u.Matched
			total.MatchedSHAOK += u.MatchedSHAOK
			total.Unmatched += u.Unmatched
			total.RawBytes += u.RawBytes
		}
		fmt.Printf("total units=%d catalog_clips=%d found_raw=%d found_joined=%d matched=%d matched_sha_ok=%d unmatched_raw=%d raw_bytes=%d\n",
			len(units), total.CatalogClips, total.FoundRaw, total.FoundJoined, total.Matched, total.MatchedSHAOK, total.Unmatched, total.RawBytes)
	case "cancel":
		tag, err := pool.Exec(ctx, `UPDATE nas_day_inventory_units SET state='canceled', completed_at=now(), lease_expires_at=NULL
 WHERE label=$1 AND state IN ('pending','leased','completed')`, *label)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("label=%s canceled=%d\n", *label, tag.RowsAffected())
	default:
		log.Fatal(nasDayInventoryUsage)
	}
}
