package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/config"
)

func runNASInventory(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 || args[0] != "report" {
		log.Fatal("usage: stoaramactl nas-inventory report --connection-id ID [--limit 50 --json]")
	}
	fs := flag.NewFlagSet("nas-inventory report", flag.ExitOnError)
	connectionID := fs.Int64("connection-id", 0, "NAS connection id")
	limit := fs.Int("limit", 50, "reconciliation rows to show")
	asJSON := fs.Bool("json", false, "print JSON")
	_ = fs.Parse(args[1:])
	if *connectionID <= 0 || *limit < 1 || *limit > 500 {
		log.Fatal("--connection-id is required and --limit must be 1..500")
	}
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("connect database: %v", err)
	}
	defer pool.Close()
	var summary struct {
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
	err = pool.QueryRow(ctx, `
		SELECT c.id,c.label,c.inventory_mode,c.inventory_generation,c.inventory_scan_completed_at,
		       c.inventory_clips,c.inventory_bytes,c.inventory_mismatches,c.inventory_unmatched,
		       (SELECT count(*) FROM recording_clips rc JOIN recordings r ON r.id=rc.recording_id
		        WHERE r.account_id=c.account_id AND r.delivery='nas_pull' AND rc.purged_at IS NULL
		          AND NOT EXISTS(SELECT 1 FROM nas_inventory_files i WHERE i.connection_id=c.id AND i.clip_id=rc.id AND i.state='present'))
		FROM connections c WHERE c.id=$1
	`, *connectionID).Scan(&summary.ConnectionID, &summary.Label, &summary.Mode, &summary.Generation, &summary.ScanCompletedAt, &summary.Clips, &summary.Bytes, &summary.Mismatches, &summary.Unmatched, &summary.ServerOnly)
	if err != nil {
		log.Fatalf("load inventory summary: %v", err)
	}
	rows, err := pool.Query(ctx, `SELECT clip_id,relative_path,state,verified_at FROM nas_inventory_files WHERE connection_id=$1 AND state<>'present' ORDER BY clip_id LIMIT $2`, *connectionID, *limit)
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
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(summary); err != nil {
			log.Fatal(err)
		}
		return
	}
	fmt.Printf("connection=%d label=%q mode=%s generation=%s scan_completed=%v clips=%d bytes=%d mismatches=%d unmatched=%d server_only=%d\n", summary.ConnectionID, summary.Label, summary.Mode, summary.Generation, summary.ScanCompletedAt, summary.Clips, summary.Bytes, summary.Mismatches, summary.Unmatched, summary.ServerOnly)
	for _, item := range summary.Items {
		fmt.Printf("clip=%v state=%v path=%v verified=%v\n", item["clip_id"], item["state"], item["relative_path"], item["verified_at"])
	}
}
