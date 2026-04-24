package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/config"
)

type youtubeStreamRepairRow struct {
	ID             int64
	Provider       string
	Slug           string
	SourceURL      string
	SourcePageURL  string
	SourceFamily   string
	CaptureType    string
	ExecutionClass string
}

type youtubeStreamRepairItem struct {
	ID                     int64    `json:"id"`
	Provider               string   `json:"provider"`
	Slug                   string   `json:"slug"`
	SourceURL              string   `json:"source_url"`
	SourcePageURL          string   `json:"source_page_url"`
	CurrentSourceFamily    string   `json:"current_source_family"`
	CurrentCaptureType     string   `json:"current_capture_type"`
	CurrentExecutionClass  string   `json:"current_execution_class"`
	ProposedSourceFamily   string   `json:"proposed_source_family"`
	ProposedCaptureType    string   `json:"proposed_capture_type"`
	ProposedExecutionClass string   `json:"proposed_execution_class"`
	WouldChange            bool     `json:"would_change"`
	Reasons                []string `json:"reasons,omitempty"`
}

func runStreamsRepairYouTube(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("streams repair-youtube", flag.ExitOnError)
	id := fs.Int64("id", 0, "single stream id")
	limit := fs.Int("limit", 0, "optional max rows to inspect")
	onlyChanged := fs.Bool("only-changed", false, "only print rows that would change")
	apply := fs.Bool("apply", false, "persist proposed updates")
	reportJSON := fs.String("report-json", "", "optional path to write JSON report")
	asJSON := fs.Bool("json", false, "print JSON")
	_ = fs.Parse(args)

	if *id < 0 {
		log.Fatalf("--id must be >= 0")
	}
	if *limit < 0 {
		log.Fatalf("--limit must be >= 0")
	}

	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()

	items, err := loadYouTubeRepairItems(ctx, pool, *id, *limit)
	if err != nil {
		log.Fatalf("load youtube repair items: %v", err)
	}

	filtered := make([]youtubeStreamRepairItem, 0, len(items))
	for _, item := range items {
		if *onlyChanged && !item.WouldChange {
			continue
		}
		filtered = append(filtered, item)
	}

	applied := 0
	if *apply {
		applied, err = applyYouTubeRepair(ctx, pool, filtered)
		if err != nil {
			log.Fatalf("apply youtube repair: %v", err)
		}
	}

	report := map[string]any{
		"total":                      len(items),
		"selected":                   len(filtered),
		"changed":                    countYouTubeRepairItems(items, func(it youtubeStreamRepairItem) bool { return it.WouldChange }),
		"applied":                    applied,
		"proposed_capture_types":     summarizeYouTubeRepairItems(items, func(it youtubeStreamRepairItem) string { return it.ProposedCaptureType }),
		"proposed_execution_classes": summarizeYouTubeRepairItems(items, func(it youtubeStreamRepairItem) string { return it.ProposedExecutionClass }),
		"proposed_source_families":   summarizeYouTubeRepairItems(items, func(it youtubeStreamRepairItem) string { return it.ProposedSourceFamily }),
		"items":                      filtered,
	}

	if path := strings.TrimSpace(*reportJSON); path != "" {
		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			log.Fatalf("marshal youtube repair report: %v", err)
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			log.Fatalf("write report %s: %v", path, err)
		}
	}
	if *asJSON {
		printJSON(report)
		return
	}

	fmt.Printf("youtube repair: total=%d selected=%d changed=%d applied=%d\n",
		report["total"], report["selected"], report["changed"], report["applied"])
	for _, item := range filtered {
		status := "keep"
		if item.WouldChange {
			status = "update"
		}
		fmt.Printf("stream_id=%d slug=%s status=%s capture=%s->%s execution=%s->%s source_family=%s->%s reasons=%s\n",
			item.ID,
			item.Slug,
			status,
			item.CurrentCaptureType,
			item.ProposedCaptureType,
			item.CurrentExecutionClass,
			item.ProposedExecutionClass,
			item.CurrentSourceFamily,
			item.ProposedSourceFamily,
			strings.Join(item.Reasons, ","),
		)
	}
}

func loadYouTubeRepairItems(ctx context.Context, pool *pgxpool.Pool, streamID int64, limit int) ([]youtubeStreamRepairItem, error) {
	where := []string{"1=1"}
	args := make([]any, 0, 2)
	if streamID > 0 {
		args = append(args, streamID)
		where = append(where, fmt.Sprintf("s.id=$%d", len(args)))
	}
	sql := fmt.Sprintf(`
		SELECT
			s.id,
			s.provider,
			s.slug,
			s.source_url,
			s.source_page_url,
			s.source_family,
			s.capture_type,
			s.execution_class
		FROM streams s
		WHERE %s
		ORDER BY s.id ASC
	`, strings.Join(where, " AND "))
	if limit > 0 {
		args = append(args, limit)
		sql += fmt.Sprintf(" LIMIT $%d", len(args))
	}
	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]youtubeStreamRepairItem, 0, 1024)
	for rows.Next() {
		var row youtubeStreamRepairRow
		if err := rows.Scan(
			&row.ID,
			&row.Provider,
			&row.Slug,
			&row.SourceURL,
			&row.SourcePageURL,
			&row.SourceFamily,
			&row.CaptureType,
			&row.ExecutionClass,
		); err != nil {
			return nil, err
		}
		item, ok := proposeYouTubeRepair(row)
		if !ok {
			continue
		}
		items = append(items, item)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return items, nil
}

func proposeYouTubeRepair(row youtubeStreamRepairRow) (youtubeStreamRepairItem, bool) {
	item := youtubeStreamRepairItem{
		ID:                    row.ID,
		Provider:              strings.TrimSpace(row.Provider),
		Slug:                  strings.TrimSpace(row.Slug),
		SourceURL:             strings.TrimSpace(row.SourceURL),
		SourcePageURL:         strings.TrimSpace(row.SourcePageURL),
		CurrentSourceFamily:   normalizeSourceFamily(row.SourceFamily),
		CurrentCaptureType:    normalizeCaptureType(row.CaptureType),
		CurrentExecutionClass: normalizeExecutionClass(row.ExecutionClass),
	}
	inferredCaptureType, inferredReason := capture.InferCaptureType(item.Provider, item.SourceURL, item.SourcePageURL)
	if inferredCaptureType != capture.CaptureTypeYouTubeWatch && item.CurrentCaptureType != capture.CaptureTypeYouTubeWatch {
		return youtubeStreamRepairItem{}, false
	}
	reasons := []string{"youtube_relay_hard_cut"}
	if inferredCaptureType == capture.CaptureTypeYouTubeWatch && inferredReason != "" {
		reasons = append(reasons, inferredReason)
	}
	if item.CurrentCaptureType == capture.CaptureTypeYouTubeWatch && item.CurrentExecutionClass == capture.ExecutionClassYouTubeRelay {
		reasons = append(reasons, "youtube_relay_hard_cut")
	}
	if item.CurrentCaptureType == capture.CaptureTypeHLS || item.CurrentExecutionClass == capture.ExecutionClassVideoLive {
		reasons = append(reasons, "resolved_runtime_youtube_misclassified")
	}
	item.ProposedSourceFamily = capture.SourceFamilyWatchPage
	item.ProposedCaptureType = capture.CaptureTypeYouTubeWatch
	item.ProposedExecutionClass = capture.ExecutionClassYouTubeDirect
	item.Reasons = uniqueStrings(reasons)
	item.WouldChange = item.CurrentSourceFamily != item.ProposedSourceFamily ||
		item.CurrentCaptureType != item.ProposedCaptureType ||
		item.CurrentExecutionClass != item.ProposedExecutionClass
	return item, true
}

func applyYouTubeRepair(ctx context.Context, pool *pgxpool.Pool, items []youtubeStreamRepairItem) (int, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows := make([][]any, 0, len(items))
	for _, item := range items {
		if !item.WouldChange {
			continue
		}
		rows = append(rows, []any{item.ID, item.ProposedSourceFamily, item.ProposedCaptureType, item.ProposedExecutionClass})
	}
	if len(rows) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return 0, err
		}
		return 0, nil
	}
	if _, err := tx.Exec(ctx, `
		CREATE TEMP TABLE youtube_stream_updates_tmp (
			id BIGINT PRIMARY KEY,
			source_family TEXT NOT NULL,
			capture_type TEXT NOT NULL,
			execution_class TEXT NOT NULL
		) ON COMMIT DROP
	`); err != nil {
		return 0, err
	}
	if _, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"youtube_stream_updates_tmp"},
		[]string{"id", "source_family", "capture_type", "execution_class"},
		pgx.CopyFromRows(rows),
	); err != nil {
		return 0, err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE streams s
		SET
			source_family=u.source_family,
			capture_type=u.capture_type,
			execution_class=u.execution_class,
			updated_at=now()
		FROM youtube_stream_updates_tmp u
		WHERE s.id=u.id
		  AND (
			s.source_family IS DISTINCT FROM u.source_family OR
			s.capture_type IS DISTINCT FROM u.capture_type OR
			s.execution_class IS DISTINCT FROM u.execution_class
		  )
	`)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func countYouTubeRepairItems(items []youtubeStreamRepairItem, keep func(youtubeStreamRepairItem) bool) int {
	total := 0
	for _, item := range items {
		if keep(item) {
			total++
		}
	}
	return total
}

func summarizeYouTubeRepairItems(items []youtubeStreamRepairItem, pick func(youtubeStreamRepairItem) string) map[string]int {
	out := map[string]int{}
	for _, item := range items {
		key := strings.TrimSpace(pick(item))
		if key == "" {
			key = "<empty>"
		}
		out[key]++
	}
	return out
}
