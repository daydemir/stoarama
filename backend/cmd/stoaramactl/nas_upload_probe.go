package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/config"
)

type nasUploadProbeOptions struct {
	connectionID int64
	limit        int
	asJSON       bool
}

type nasUploadProbeRow struct {
	ID                   int64      `json:"id"`
	Status               string     `json:"status"`
	CreatedAt            time.Time  `json:"created_at"`
	ReportedAt           *time.Time `json:"reported_at"`
	SizeBytes            int64      `json:"size_bytes"`
	Streams              int        `json:"streams"`
	BytesUploaded        *int64     `json:"bytes_uploaded"`
	DurationMS           *int64     `json:"duration_ms"`
	Mbps                 *float64   `json:"mbps"`
	Error                string     `json:"error"`
	ClientVersion        string     `json:"client_version"`
	ClientPhaseStart     string     `json:"client_phase_start"`
	ClientPhaseEnd       string     `json:"client_phase_end"`
	JoinedTransferActive bool       `json:"joined_transfer_active"`
	BytesPulledDuring    int64      `json:"bytes_pulled_during"`
	ObjectsDeleted       bool       `json:"objects_deleted"`
	BytesDownloaded      *int64     `json:"bytes_downloaded"`
	DownloadDurationMS   *int64     `json:"download_duration_ms"`
	DownloadMbps         *float64   `json:"download_mbps"`
	DownloadError        string     `json:"download_error"`
}

type nasUploadProbeReport struct {
	ConnectionID int64    `json:"connection_id"`
	Label        string   `json:"label"`
	OKCount      int      `json:"ok_count"`
	MedianMbps   *float64 `json:"median_mbps"`
	MinMbps      *float64 `json:"min_mbps"`
	MaxMbps      *float64 `json:"max_mbps"`
	// Downlink: complete probe downloads only. TB/day is the median sustained.
	DownloadOKCount        int                 `json:"download_ok_count"`
	DownloadMedianMbps     *float64            `json:"download_median_mbps"`
	DownloadMedianTBPerDay *float64            `json:"download_median_tb_per_day"`
	Probes                 []nasUploadProbeRow `json:"probes"`
}

func parseNASUploadProbeArgs(args []string) (nasUploadProbeOptions, error) {
	usage := fmt.Errorf("usage: stoaramactl nas-upload-probe report --connection-id ID [--limit 20 --json]")
	if len(args) < 1 || args[0] != "report" {
		return nasUploadProbeOptions{}, usage
	}
	fs := flag.NewFlagSet("nas-upload-probe report", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	connectionID := fs.Int64("connection-id", 0, "NAS connection id")
	limit := fs.Int("limit", 20, "probes to show, newest first")
	asJSON := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args[1:]); err != nil {
		return nasUploadProbeOptions{}, err
	}
	if len(fs.Args()) != 0 {
		return nasUploadProbeOptions{}, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	if *connectionID <= 0 || *limit < 1 || *limit > 1000 {
		return nasUploadProbeOptions{}, fmt.Errorf("--connection-id is required and --limit must be 1..1000")
	}
	return nasUploadProbeOptions{connectionID: *connectionID, limit: *limit, asJSON: *asJSON}, nil
}

func runNASUploadProbe(ctx context.Context, cfg config.Config, args []string) {
	opts, err := parseNASUploadProbeArgs(args)
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	poolConfig, err := nasInventoryPoolConfig(cfg.DatabaseURL)
	if err != nil {
		log.Fatal("invalid database configuration")
	}
	poolConfig.ConnConfig.RuntimeParams["application_name"] = "stoarama-nas-upload-probe-report"
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		log.Fatalf("connect database: %v", err)
	}
	defer pool.Close()
	report, err := loadNASUploadProbeReport(ctx, pool, opts, time.Now())
	if err != nil {
		log.Fatal(err)
	}
	if err := writeNASUploadProbeReport(os.Stdout, report, opts.asJSON); err != nil {
		log.Fatal(err)
	}
}

func loadNASUploadProbeReport(ctx context.Context, pool *pgxpool.Pool, opts nasUploadProbeOptions, now time.Time) (nasUploadProbeReport, error) {
	report := nasUploadProbeReport{ConnectionID: opts.connectionID, Probes: []nasUploadProbeRow{}}
	if err := pool.QueryRow(ctx, `SELECT label FROM connections WHERE id=$1 AND kind='nas_pull'`, opts.connectionID).Scan(&report.Label); err != nil {
		return report, fmt.Errorf("load connection %d: %w", opts.connectionID, err)
	}
	rows, err := pool.Query(ctx, `
		SELECT id,created_at,expires_at,reported_at,size_bytes,streams,bytes_uploaded,duration_ms,error,client_version,
		       client_phase_start,client_phase_end,joined_transfer_active,bytes_pulled_during,objects_deleted_at IS NOT NULL,
		       bytes_downloaded,download_duration_ms,download_error
		FROM nas_upload_probes WHERE connection_id=$1 ORDER BY created_at DESC, id DESC LIMIT $2`,
		opts.connectionID, opts.limit)
	if err != nil {
		return report, fmt.Errorf("load upload probes: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var row nasUploadProbeRow
		var expiresAt time.Time
		if err := rows.Scan(&row.ID, &row.CreatedAt, &expiresAt, &row.ReportedAt, &row.SizeBytes, &row.Streams,
			&row.BytesUploaded, &row.DurationMS, &row.Error, &row.ClientVersion, &row.ClientPhaseStart,
			&row.ClientPhaseEnd, &row.JoinedTransferActive, &row.BytesPulledDuring, &row.ObjectsDeleted,
			&row.BytesDownloaded, &row.DownloadDurationMS, &row.DownloadError); err != nil {
			return report, err
		}
		row.Status = nasUploadProbeStatus(row, expiresAt, now)
		if row.BytesUploaded != nil && row.DurationMS != nil && *row.DurationMS > 0 {
			mbps := float64(*row.BytesUploaded) * 8 / (float64(*row.DurationMS) / 1000) / 1e6
			row.Mbps = &mbps
		}
		if row.BytesDownloaded != nil && row.DownloadDurationMS != nil && *row.DownloadDurationMS > 0 {
			mbps := float64(*row.BytesDownloaded) * 8 / (float64(*row.DownloadDurationMS) / 1000) / 1e6
			row.DownloadMbps = &mbps
		}
		report.Probes = append(report.Probes, row)
	}
	if err := rows.Err(); err != nil {
		return report, err
	}
	summarizeNASUploadProbes(&report)
	summarizeNASDownloadProbes(&report)
	return report, nil
}

func nasUploadProbeStatus(row nasUploadProbeRow, expiresAt, now time.Time) string {
	switch {
	case row.ReportedAt == nil && now.Before(expiresAt):
		return "pending"
	case row.ReportedAt == nil:
		return "abandoned"
	case row.Error != "":
		return "error"
	default:
		return "ok"
	}
}

// summarizeNASUploadProbes uses only complete uploads: a partial upload's
// throughput is real but is cut short by a timeout, so it would skew the
// median downward.
func summarizeNASUploadProbes(report *nasUploadProbeReport) {
	var values []float64
	for _, row := range report.Probes {
		if row.Status == "ok" && row.Mbps != nil {
			values = append(values, *row.Mbps)
		}
	}
	report.OKCount = len(values)
	if len(values) == 0 {
		return
	}
	sort.Float64s(values)
	median := values[len(values)/2]
	if len(values)%2 == 0 {
		median = (values[len(values)/2-1] + values[len(values)/2]) / 2
	}
	minimum, maximum := values[0], values[len(values)-1]
	report.MedianMbps, report.MinMbps, report.MaxMbps = &median, &minimum, &maximum
}

func summarizeNASDownloadProbes(report *nasUploadProbeReport) {
	var values []float64
	for _, row := range report.Probes {
		if row.DownloadMbps != nil && row.DownloadError == "" && row.BytesDownloaded != nil && *row.BytesDownloaded == row.SizeBytes {
			values = append(values, *row.DownloadMbps)
		}
	}
	report.DownloadOKCount = len(values)
	if len(values) == 0 {
		return
	}
	sort.Float64s(values)
	median := values[len(values)/2]
	if len(values)%2 == 0 {
		median = (values[len(values)/2-1] + values[len(values)/2]) / 2
	}
	tbPerDay := median * 1e6 / 8 * 86400 / 1e12
	report.DownloadMedianMbps, report.DownloadMedianTBPerDay = &median, &tbPerDay
}

func writeNASUploadProbeReport(out io.Writer, report nasUploadProbeReport, asJSON bool) error {
	if asJSON {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	fmt.Fprintf(out, "NAS upload probes for connection %d (%s)\n", report.ConnectionID, report.Label)
	if len(report.Probes) == 0 {
		fmt.Fprintln(out, "No upload probes recorded yet.")
		return nil
	}
	latest := report.Probes[0]
	fmt.Fprintf(out, "Latest: #%d %s at %s", latest.ID, latest.Status, latest.CreatedAt.UTC().Format(time.RFC3339))
	if latest.Mbps != nil {
		fmt.Fprintf(out, " %.1f Mbps", *latest.Mbps)
	}
	fmt.Fprintln(out)
	if report.MedianMbps != nil {
		fmt.Fprintf(out, "Complete uploads shown: %d  median %.1f Mbps  min %.1f  max %.1f\n",
			report.OKCount, *report.MedianMbps, *report.MinMbps, *report.MaxMbps)
	}
	if report.DownloadMedianMbps != nil {
		fmt.Fprintf(out, "Complete downloads shown: %d  median %.1f Mbps (%.2f TB/day sustained)\n",
			report.DownloadOKCount, *report.DownloadMedianMbps, *report.DownloadMedianTBPerDay)
	}
	fmt.Fprintf(out, "%-7s %-20s %-9s %9s %7s %9s %10s %-17s %-6s %s\n",
		"ID", "CREATED_UTC", "STATUS", "MIB", "STREAMS", "SECONDS", "MBPS", "PHASE", "JOINED", "ERROR")
	for _, row := range report.Probes {
		mib, seconds, mbps := "-", "-", "-"
		if row.BytesUploaded != nil {
			mib = fmt.Sprintf("%.1f", float64(*row.BytesUploaded)/(1<<20))
		}
		if row.DurationMS != nil {
			seconds = fmt.Sprintf("%.1f", float64(*row.DurationMS)/1000)
		}
		if row.Mbps != nil {
			mbps = fmt.Sprintf("%.1f", *row.Mbps)
		}
		phase := "-"
		if row.ClientPhaseStart != "" || row.ClientPhaseEnd != "" {
			phase = row.ClientPhaseStart + ">" + row.ClientPhaseEnd
		}
		fmt.Fprintf(out, "%-7d %-20s %-9s %9s %7d %9s %10s %-17s %-6t %s\n",
			row.ID, row.CreatedAt.UTC().Format(time.RFC3339), row.Status, mib, row.Streams, seconds, mbps,
			phase, row.JoinedTransferActive, row.Error)
	}
	return nil
}
