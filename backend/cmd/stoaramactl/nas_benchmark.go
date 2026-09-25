package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/config"
)

const nasBenchmarkUsage = `usage:
  stoaramactl nas-benchmark report --connection-id ID [--limit 5 --json]
  stoaramactl nas-benchmark request --connection-id ID [--recording-id ID --window-start RFC3339]`

type nasBenchmarkOptions struct {
	command      string
	connectionID int64
	limit        int
	asJSON       bool
	recordingID  int64
	windowStart  *time.Time
}

type nasBenchmarkRow struct {
	ID            int64           `json:"id"`
	State         string          `json:"state"`
	RequestedBy   string          `json:"requested_by"`
	RequestedAt   time.Time       `json:"requested_at"`
	ClaimCount    int             `json:"claim_count"`
	ClaimedAt     *time.Time      `json:"claimed_at"`
	ReportedAt    *time.Time      `json:"reported_at"`
	ClientVersion string          `json:"client_version"`
	Status        string          `json:"status"`
	Error         string          `json:"error"`
	RecordingID   *int64          `json:"recording_id"`
	WindowStartAt *time.Time      `json:"window_start_at"`
	PlanClips     int             `json:"plan_clips"`
	PlanBytes     int64           `json:"plan_bytes"`
	Host          json.RawMessage `json:"host,omitempty"`
	Result        json.RawMessage `json:"result,omitempty"`
}

type nasBenchmarkReport struct {
	ConnectionID   int64             `json:"connection_id"`
	Label          string            `json:"label"`
	Host           json.RawMessage   `json:"host,omitempty"`
	HostReportedAt *time.Time        `json:"host_reported_at"`
	Benchmarks     []nasBenchmarkRow `json:"benchmarks"`
}

func parseNASBenchmarkArgs(args []string) (nasBenchmarkOptions, error) {
	usage := fmt.Errorf("%s", nasBenchmarkUsage)
	if len(args) < 1 || (args[0] != "report" && args[0] != "request") {
		return nasBenchmarkOptions{}, usage
	}
	opts := nasBenchmarkOptions{command: args[0]}
	fs := flag.NewFlagSet("nas-benchmark "+args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.Int64Var(&opts.connectionID, "connection-id", 0, "NAS connection id")
	fs.IntVar(&opts.limit, "limit", 5, "benchmarks to show, newest first")
	fs.BoolVar(&opts.asJSON, "json", false, "print JSON")
	fs.Int64Var(&opts.recordingID, "recording-id", 0, "pin the benchmark to this recording")
	windowStart := fs.String("window-start", "", "pin the benchmark hour start (RFC3339)")
	if err := fs.Parse(args[1:]); err != nil {
		return nasBenchmarkOptions{}, err
	}
	if len(fs.Args()) != 0 {
		return nasBenchmarkOptions{}, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	if opts.connectionID <= 0 {
		return nasBenchmarkOptions{}, fmt.Errorf("--connection-id is required")
	}
	if opts.command == "report" {
		if opts.limit < 1 || opts.limit > 100 || opts.recordingID != 0 || *windowStart != "" {
			return nasBenchmarkOptions{}, fmt.Errorf("report takes --limit 1..100 and no pin flags")
		}
		return opts, nil
	}
	if (opts.recordingID != 0) != (*windowStart != "") || opts.recordingID < 0 {
		return nasBenchmarkOptions{}, fmt.Errorf("--recording-id and --window-start go together")
	}
	if *windowStart != "" {
		parsed, err := time.Parse(time.RFC3339, *windowStart)
		if err != nil {
			return nasBenchmarkOptions{}, fmt.Errorf("--window-start: %w", err)
		}
		parsed = parsed.UTC()
		opts.windowStart = &parsed
	}
	return opts, nil
}

func runNASBenchmark(ctx context.Context, cfg config.Config, args []string) {
	opts, err := parseNASBenchmarkArgs(args)
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	poolConfig, err := nasInventoryPoolConfig(cfg.DatabaseURL)
	if err != nil {
		log.Fatal("invalid database configuration")
	}
	poolConfig.ConnConfig.RuntimeParams["application_name"] = "stoarama-nas-benchmark"
	// The shared inventory-report pool is read-only; only the report is.
	poolConfig.ConnConfig.RuntimeParams["default_transaction_read_only"] = nasBenchmarkReadOnly(opts.command)
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		log.Fatalf("connect database: %v", err)
	}
	defer pool.Close()
	if opts.command == "request" {
		id, err := requestNASBenchmark(ctx, pool, opts)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("requested NAS benchmark %d for connection %d; the client claims it within ~30 minutes\n", id, opts.connectionID)
		return
	}
	report, err := loadNASBenchmarkReport(ctx, pool, opts)
	if err != nil {
		log.Fatal(err)
	}
	if err := writeNASBenchmarkReport(os.Stdout, report, opts.asJSON); err != nil {
		log.Fatal(err)
	}
}

func nasBenchmarkReadOnly(command string) string {
	if command == "request" {
		return "off"
	}
	return "on"
}

func requestNASBenchmark(ctx context.Context, pool *pgxpool.Pool, opts nasBenchmarkOptions) (int64, error) {
	var recordingID *int64
	if opts.recordingID > 0 {
		recordingID = &opts.recordingID
	}
	var id int64
	err := pool.QueryRow(ctx, `
		INSERT INTO nas_benchmarks (connection_id,requested_by,recording_id,window_start_at)
		SELECT id,'operator',$2,$3 FROM connections WHERE id=$1 AND kind='nas_pull'
		RETURNING id`, opts.connectionID, recordingID, opts.windowStart).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("request benchmark for connection %d: %w", opts.connectionID, err)
	}
	return id, nil
}

func loadNASBenchmarkReport(ctx context.Context, pool *pgxpool.Pool, opts nasBenchmarkOptions) (nasBenchmarkReport, error) {
	report := nasBenchmarkReport{ConnectionID: opts.connectionID, Benchmarks: []nasBenchmarkRow{}}
	var host []byte
	if err := pool.QueryRow(ctx, `SELECT label,nas_host,nas_host_reported_at FROM connections WHERE id=$1 AND kind='nas_pull'`,
		opts.connectionID).Scan(&report.Label, &host, &report.HostReportedAt); err != nil {
		return report, fmt.Errorf("load connection %d: %w", opts.connectionID, err)
	}
	report.Host = host
	rows, err := pool.Query(ctx, `
		SELECT id,state,requested_by,requested_at,claim_count,claimed_at,reported_at,client_version,status,error,
		       recording_id,window_start_at,
		       COALESCE(jsonb_array_length(plan->'clips'),0),
		       COALESCE((SELECT sum((c->>'size_bytes')::bigint) FROM jsonb_array_elements(plan->'clips') c),0)::bigint,
		       host,result
		FROM nas_benchmarks WHERE connection_id=$1 ORDER BY id DESC LIMIT $2`, opts.connectionID, opts.limit)
	if err != nil {
		return report, fmt.Errorf("load benchmarks: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var row nasBenchmarkRow
		var rowHost, result []byte
		if err := rows.Scan(&row.ID, &row.State, &row.RequestedBy, &row.RequestedAt, &row.ClaimCount, &row.ClaimedAt,
			&row.ReportedAt, &row.ClientVersion, &row.Status, &row.Error, &row.RecordingID, &row.WindowStartAt,
			&row.PlanClips, &row.PlanBytes, &rowHost, &result); err != nil {
			return report, err
		}
		row.Host, row.Result = rowHost, result
		report.Benchmarks = append(report.Benchmarks, row)
	}
	return report, rows.Err()
}

// nasBenchmarkStage is the per-stage shape the NAS client reports.
type nasBenchmarkStage struct {
	OK       *bool    `json:"ok"`
	WallS    *float64 `json:"wall_s"`
	CPUS     *float64 `json:"cpu_s"`
	MaxRSSKB *int64   `json:"max_rss_kb"`
	Error    string   `json:"error"`
	Skipped  string   `json:"skipped"`
	Seams    *int     `json:"seams"`
	Failures *int     `json:"failures"`
	Match    *bool    `json:"match"`
	OutBytes *int64   `json:"output_bytes"`
}

type nasBenchmarkResultSummary struct {
	MediaSeconds float64                      `json:"media_seconds"`
	InputBytes   int64                        `json:"input_bytes"`
	Threads      int                          `json:"threads"`
	Nice         int                          `json:"nice"`
	TotalWallS   float64                      `json:"total_wall_s"`
	DeadlineHit  bool                         `json:"deadline_hit"`
	TempRemoved  *bool                        `json:"temp_removed"`
	Tool         map[string]any               `json:"tool"`
	Stages       map[string]nasBenchmarkStage `json:"stages"`
}

var nasBenchmarkStageOrder = []string{"tool_install", "hash_inputs", "probe", "remux", "packet_chain", "seam_decode", "full_decode"}

func writeNASBenchmarkReport(out io.Writer, report nasBenchmarkReport, asJSON bool) error {
	if asJSON {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	fmt.Fprintf(out, "NAS benchmarks for connection %d (%s)\n", report.ConnectionID, report.Label)
	if len(report.Host) > 0 {
		at := "-"
		if report.HostReportedAt != nil {
			at = report.HostReportedAt.UTC().Format(time.RFC3339)
		}
		fmt.Fprintf(out, "Host (%s): %s\n", at, string(report.Host))
	} else {
		fmt.Fprintln(out, "Host: not reported yet")
	}
	if len(report.Benchmarks) == 0 {
		fmt.Fprintln(out, "No benchmarks recorded yet.")
		return nil
	}
	for _, row := range report.Benchmarks {
		fmt.Fprintf(out, "\n#%d %s by %s at %s claims=%d version=%s status=%s",
			row.ID, row.State, row.RequestedBy, row.RequestedAt.UTC().Format(time.RFC3339), row.ClaimCount,
			valueOrDash(row.ClientVersion), valueOrDash(row.Status))
		if row.Error != "" {
			fmt.Fprintf(out, " error=%q", row.Error)
		}
		fmt.Fprintln(out)
		if row.PlanClips > 0 {
			fmt.Fprintf(out, "  hour: %d clips, %.2f GB", row.PlanClips, float64(row.PlanBytes)/1e9)
			if row.RecordingID != nil && row.WindowStartAt != nil {
				fmt.Fprintf(out, " (pinned recording %d at %s)", *row.RecordingID, row.WindowStartAt.UTC().Format(time.RFC3339))
			}
			fmt.Fprintln(out)
		}
		if len(row.Result) == 0 {
			continue
		}
		var summary nasBenchmarkResultSummary
		if err := json.Unmarshal(row.Result, &summary); err != nil {
			fmt.Fprintf(out, "  result (unparsed): %s\n", string(row.Result))
			continue
		}
		mediaHours := summary.MediaSeconds / 3600
		fmt.Fprintf(out, "  media %.0fs  input %.2f GB  threads=%d nice=%d  total wall %.0fs  deadline_hit=%t",
			summary.MediaSeconds, float64(summary.InputBytes)/1e9, summary.Threads, summary.Nice, summary.TotalWallS, summary.DeadlineHit)
		if summary.TempRemoved != nil {
			fmt.Fprintf(out, "  temp_removed=%t", *summary.TempRemoved)
		}
		fmt.Fprintln(out)
		fmt.Fprintf(out, "  %-13s %-5s %9s %9s %13s %10s  %s\n", "STAGE", "OK", "WALL_S", "CPU_S", "CPU_S/MEDIA_H", "MAXRSS_MB", "NOTE")
		for _, name := range nasBenchmarkStageOrder {
			stage, ok := summary.Stages[name]
			if !ok {
				continue
			}
			okText, wall, cpu, perHour, rss := "-", "-", "-", "-", "-"
			if stage.OK != nil {
				okText = fmt.Sprint(*stage.OK)
			}
			if stage.WallS != nil {
				wall = fmt.Sprintf("%.1f", *stage.WallS)
			}
			if stage.CPUS != nil {
				cpu = fmt.Sprintf("%.1f", *stage.CPUS)
				if mediaHours > 0 && name != "tool_install" {
					perHour = fmt.Sprintf("%.1f", *stage.CPUS/mediaHours)
				}
			}
			if stage.MaxRSSKB != nil {
				rss = fmt.Sprintf("%.0f", float64(*stage.MaxRSSKB)/1024)
			}
			var notes []string
			if stage.Match != nil {
				notes = append(notes, fmt.Sprintf("match=%t", *stage.Match))
			}
			if stage.Seams != nil {
				notes = append(notes, fmt.Sprintf("seams=%d", *stage.Seams))
			}
			if stage.Failures != nil {
				notes = append(notes, fmt.Sprintf("failures=%d", *stage.Failures))
			}
			if stage.OutBytes != nil {
				notes = append(notes, fmt.Sprintf("out=%.2fGB", float64(*stage.OutBytes)/1e9))
			}
			if stage.Skipped != "" {
				notes = append(notes, "skipped="+stage.Skipped)
			}
			if stage.Error != "" {
				notes = append(notes, fmt.Sprintf("error=%q", stage.Error))
			}
			fmt.Fprintf(out, "  %-13s %-5s %9s %9s %13s %10s  %s\n", name, okText, wall, cpu, perHour, rss, strings.Join(notes, " "))
		}
	}
	return nil
}

func valueOrDash(value string) string {
	if value == "" {
		return "-"
	}
	return value
}
