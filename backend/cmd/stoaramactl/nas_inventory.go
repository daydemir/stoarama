package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/config"
)

type nasInventoryOptions struct {
	connectionID int64
	limit        int
	asJSON       bool
}

type nasInventorySummary struct {
	ConnectionID    int64            `json:"connection_id"`
	Label           string           `json:"label"`
	Mode            string           `json:"mode"`
	Generation      string           `json:"generation"`
	ScanCompletedAt *time.Time       `json:"scan_completed_at"`
	Clips           int64            `json:"clips"`
	Bytes           int64            `json:"bytes"`
	Mismatches      int64            `json:"mismatches"`
	Unmatched       int64            `json:"unmatched"`
	ServerOnly      int64            `json:"server_only"`
	Items           []map[string]any `json:"items"`
}

func parseNASInventoryArgs(args []string) (nasInventoryOptions, error) {
	if len(args) < 1 || args[0] != "report" {
		return nasInventoryOptions{}, fmt.Errorf("usage: stoaramactl nas-inventory report --connection-id ID [--limit 50 --json]")
	}
	fs := flag.NewFlagSet("nas-inventory report", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	connectionID := fs.Int64("connection-id", 0, "NAS connection id")
	limit := fs.Int("limit", 50, "reconciliation rows to show")
	asJSON := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args[1:]); err != nil {
		return nasInventoryOptions{}, err
	}
	if *connectionID <= 0 || *limit < 1 || *limit > 500 {
		return nasInventoryOptions{}, fmt.Errorf("--connection-id is required and --limit must be 1..500")
	}
	return nasInventoryOptions{connectionID: *connectionID, limit: *limit, asJSON: *asJSON}, nil
}

func runNASInventory(ctx context.Context, cfg config.Config, args []string) {
	opts, err := parseNASInventoryArgs(args)
	if err != nil {
		log.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("connect database: %v", err)
	}
	defer pool.Close()
	var summary nasInventorySummary
	err = pool.QueryRow(ctx, `
		SELECT c.id,c.label,c.inventory_mode,c.inventory_generation,c.inventory_scan_completed_at,
		       c.inventory_clips,c.inventory_bytes,c.inventory_mismatches,c.inventory_unmatched,
		       (SELECT count(*) FROM recording_clips rc JOIN recordings r ON r.id=rc.recording_id
		        WHERE r.account_id=c.account_id AND r.delivery='nas_pull' AND rc.purged_at IS NULL
		          AND NOT EXISTS(SELECT 1 FROM nas_inventory_files i WHERE i.connection_id=c.id AND i.clip_id=rc.id AND i.state='present'))
		FROM connections c WHERE c.id=$1
	`, opts.connectionID).Scan(&summary.ConnectionID, &summary.Label, &summary.Mode, &summary.Generation, &summary.ScanCompletedAt, &summary.Clips, &summary.Bytes, &summary.Mismatches, &summary.Unmatched, &summary.ServerOnly)
	if err != nil {
		log.Fatalf("load inventory summary: %v", err)
	}
	rows, err := pool.Query(ctx, `
		SELECT i.clip_id,i.relative_path,
		       CASE WHEN c.id IS NULL THEN 'nas_only'
		            WHEN i.state='missing' THEN 'missing'
		            WHEN i.state='mismatch' OR c.size_bytes<>i.size_bytes OR lower(c.sha256)<>i.sha256 OR c.display_path<>i.relative_path THEN 'mismatch'
		            ELSE 'confirmed' END,
		       i.verified_at
		FROM nas_inventory_files i
		JOIN connections conn ON conn.id=i.connection_id
		LEFT JOIN (recording_clips c JOIN recordings rec ON rec.id=c.recording_id) ON c.id=i.clip_id AND rec.account_id=conn.account_id
		WHERE i.connection_id=$1 AND (
			c.id IS NULL OR i.state<>'present' OR c.size_bytes<>i.size_bytes OR
			lower(c.sha256)<>i.sha256 OR c.display_path<>i.relative_path)
		ORDER BY i.clip_id LIMIT $2
	`, opts.connectionID, opts.limit)
	if err != nil {
		log.Fatalf("load reconciliation rows: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var path, state string
		var verified *time.Time
		if err := rows.Scan(&id, &path, &state, &verified); err != nil {
			log.Fatal(err)
		}
		summary.Items = append(summary.Items, map[string]any{"clip_id": id, "relative_path": path, "state": state, "verified_at": verified})
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("iterate reconciliation rows: %v", err)
	}
	rows.Close()
	remaining := opts.limit - len(summary.Items)
	if remaining > 0 {
		unmatchedRows, err := pool.Query(ctx, `SELECT relative_path,state FROM nas_inventory_unmatched_files WHERE connection_id=$1 AND state='present' ORDER BY relative_path LIMIT $2`, opts.connectionID, remaining)
		if err != nil {
			log.Fatalf("load unmatched inventory rows: %v", err)
		}
		for unmatchedRows.Next() {
			var path, state string
			if err := unmatchedRows.Scan(&path, &state); err != nil {
				unmatchedRows.Close()
				log.Fatalf("scan unmatched inventory row: %v", err)
			}
			summary.Items = append(summary.Items, map[string]any{"clip_id": nil, "relative_path": path, "state": "nas_only:" + state, "verified_at": nil})
		}
		if err := unmatchedRows.Err(); err != nil {
			log.Fatalf("iterate unmatched inventory rows: %v", err)
		}
		unmatchedRows.Close()
	}
	if err := writeNASInventoryReport(os.Stdout, summary, opts.asJSON); err != nil {
		log.Fatal(err)
	}
}

func writeNASInventoryReport(out io.Writer, summary nasInventorySummary, asJSON bool) error {
	if asJSON {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(summary)
	}
	if _, err := fmt.Fprintf(out, "connection=%d label=%q mode=%s generation=%s scan_completed=%v clips=%d bytes=%d mismatches=%d unmatched=%d server_only=%d\n", summary.ConnectionID, summary.Label, summary.Mode, summary.Generation, summary.ScanCompletedAt, summary.Clips, summary.Bytes, summary.Mismatches, summary.Unmatched, summary.ServerOnly); err != nil {
		return err
	}
	for _, item := range summary.Items {
		if _, err := fmt.Fprintf(out, "clip=%v state=%v path=%v verified=%v\n", item["clip_id"], item["state"], item["relative_path"], item["verified_at"]); err != nil {
			return err
		}
	}
	return nil
}
