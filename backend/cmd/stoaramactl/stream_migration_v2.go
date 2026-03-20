package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/config"
)

type streamV2MigrationRow struct {
	ID                  int64
	Provider            string
	ExternalID          string
	Slug                string
	SourceURL           string
	SourcePageURL       string
	SourceFamily        string
	CaptureType         string
	ExecutionClass      string
	ResolvedCaptureType string
	ResolvedURL         string
}

type streamV2MigrationItem struct {
	ID                     int64    `json:"id"`
	Provider               string   `json:"provider"`
	ExternalID             string   `json:"external_id"`
	Slug                   string   `json:"slug"`
	SourceURL              string   `json:"source_url"`
	SourcePageURL          string   `json:"source_page_url"`
	CurrentSourceFamily    string   `json:"current_source_family"`
	CurrentCaptureType     string   `json:"current_capture_type"`
	CurrentExecutionClass  string   `json:"current_execution_class"`
	ResolvedCaptureType    string   `json:"resolved_capture_type,omitempty"`
	ResolvedURL            string   `json:"resolved_url,omitempty"`
	ProposedSourceFamily   string   `json:"proposed_source_family"`
	ProposedCaptureType    string   `json:"proposed_capture_type"`
	ProposedExecutionClass string   `json:"proposed_execution_class"`
	WouldChange            bool     `json:"would_change"`
	ReviewRequired         bool     `json:"review_required"`
	Reasons                []string `json:"reasons,omitempty"`
}

func runStreamsMigrateV2(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("streams migrate-v2", flag.ExitOnError)
	id := fs.Int64("id", 0, "single stream id")
	limit := fs.Int("limit", 0, "optional max rows to inspect")
	onlyChanged := fs.Bool("only-changed", false, "only print rows that would change")
	onlyReview := fs.Bool("only-review", false, "only print rows that require review")
	apply := fs.Bool("apply", false, "persist safe updates")
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

	items, err := loadStreamV2MigrationItems(ctx, pool, *id, *limit)
	if err != nil {
		log.Fatalf("load stream v2 migration items: %v", err)
	}

	filtered := make([]streamV2MigrationItem, 0, len(items))
	for _, item := range items {
		if *onlyChanged && !item.WouldChange {
			continue
		}
		if *onlyReview && !item.ReviewRequired {
			continue
		}
		filtered = append(filtered, item)
	}

	applied := 0
	if *apply {
		applied, err = applyStreamV2Migration(ctx, pool, filtered)
		if err != nil {
			log.Fatalf("apply stream v2 migration: %v", err)
		}
	}

	report := map[string]any{
		"total":                      len(items),
		"selected":                   len(filtered),
		"changed":                    countStreamV2Items(items, func(it streamV2MigrationItem) bool { return it.WouldChange }),
		"review_required":            countStreamV2Items(items, func(it streamV2MigrationItem) bool { return it.ReviewRequired }),
		"safe_to_apply":              countStreamV2Items(items, func(it streamV2MigrationItem) bool { return it.WouldChange && !it.ReviewRequired }),
		"applied":                    applied,
		"proposed_capture_types":     summarizeStreamV2Items(items, func(it streamV2MigrationItem) string { return it.ProposedCaptureType }),
		"proposed_execution_classes": summarizeStreamV2Items(items, func(it streamV2MigrationItem) string { return it.ProposedExecutionClass }),
		"proposed_source_families":   summarizeStreamV2Items(items, func(it streamV2MigrationItem) string { return it.ProposedSourceFamily }),
		"items":                      filtered,
	}

	if path := strings.TrimSpace(*reportJSON); path != "" {
		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			log.Fatalf("marshal migration report: %v", err)
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			log.Fatalf("write migration report %s: %v", path, err)
		}
	}

	if *asJSON {
		printJSON(report)
		return
	}

	fmt.Printf("stream v2 migration: total=%d selected=%d changed=%d review_required=%d safe_to_apply=%d applied=%d\n",
		report["total"], report["selected"], report["changed"], report["review_required"], report["safe_to_apply"], report["applied"])
	for _, item := range filtered {
		status := "keep"
		switch {
		case item.ReviewRequired:
			status = "review"
		case item.WouldChange:
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
	if countStreamV2Items(items, func(it streamV2MigrationItem) bool { return it.ReviewRequired }) > 0 {
		os.Exit(2)
	}
}

func loadStreamV2MigrationItems(ctx context.Context, pool *pgxpool.Pool, streamID int64, limit int) ([]streamV2MigrationItem, error) {
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
			s.external_id,
			s.slug,
			s.source_url,
			s.source_page_url,
			s.source_family,
			s.capture_type,
			s.execution_class,
			COALESCE(rt.resolved_capture_type, ''),
			COALESCE(rt.resolved_url, '')
		FROM streams s
		LEFT JOIN stream_capture_runtime rt ON rt.stream_id=s.id
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

	items := make([]streamV2MigrationItem, 0, 1024)
	for rows.Next() {
		var row streamV2MigrationRow
		if err := rows.Scan(
			&row.ID,
			&row.Provider,
			&row.ExternalID,
			&row.Slug,
			&row.SourceURL,
			&row.SourcePageURL,
			&row.SourceFamily,
			&row.CaptureType,
			&row.ExecutionClass,
			&row.ResolvedCaptureType,
			&row.ResolvedURL,
		); err != nil {
			return nil, err
		}
		items = append(items, proposeStreamV2Migration(row))
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return items, nil
}

func applyStreamV2Migration(ctx context.Context, pool *pgxpool.Pool, items []streamV2MigrationItem) (int, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows := make([][]any, 0, len(items))
	for _, item := range items {
		if !item.WouldChange || item.ReviewRequired {
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
		CREATE TEMP TABLE stream_v2_updates_tmp (
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
		pgx.Identifier{"stream_v2_updates_tmp"},
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
		FROM stream_v2_updates_tmp u
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

func proposeStreamV2Migration(row streamV2MigrationRow) streamV2MigrationItem {
	item := streamV2MigrationItem{
		ID:                    row.ID,
		Provider:              strings.TrimSpace(row.Provider),
		ExternalID:            strings.TrimSpace(row.ExternalID),
		Slug:                  strings.TrimSpace(row.Slug),
		SourceURL:             strings.TrimSpace(row.SourceURL),
		SourcePageURL:         strings.TrimSpace(row.SourcePageURL),
		CurrentSourceFamily:   normalizeSourceFamily(row.SourceFamily),
		CurrentCaptureType:    normalizeCaptureType(row.CaptureType),
		CurrentExecutionClass: normalizeExecutionClass(row.ExecutionClass),
		ResolvedCaptureType:   normalizeCaptureType(row.ResolvedCaptureType),
		ResolvedURL:           strings.TrimSpace(row.ResolvedURL),
	}
	reasons := make([]string, 0, 8)

	proposedCaptureType := item.CurrentCaptureType
	inferredCaptureType, inferredReason := capture.InferCaptureType(item.Provider, item.SourceURL, item.SourcePageURL)
	if inferredCaptureType == capture.CaptureTypeYouTubeWatch {
		proposedCaptureType = inferredCaptureType
		reasons = append(reasons, inferredReason)
	} else if item.ResolvedCaptureType != "" && item.ResolvedCaptureType != "unknown" {
		proposedCaptureType = item.ResolvedCaptureType
		reasons = append(reasons, "resolved_capture_type")
	} else if inferredCaptureType != "" {
		proposedCaptureType = inferredCaptureType
		reasons = append(reasons, inferredReason)
	}
	if proposedCaptureType == "" {
		proposedCaptureType = "unknown"
		reasons = append(reasons, "capture_type_unknown")
	}

	proposedExecutionClass, executionReason, reviewForExecution := deriveExecutionClass(proposedCaptureType, item.CurrentExecutionClass)
	if executionReason != "" {
		reasons = append(reasons, executionReason)
	}
	proposedSourceFamily, familyReason := deriveSourceFamily(proposedCaptureType, item.CurrentSourceFamily)
	if familyReason != "" {
		reasons = append(reasons, familyReason)
	}

	reviewRequired := reviewForExecution
	if proposedCaptureType == "unknown" || proposedExecutionClass == "" || proposedSourceFamily == "" {
		reviewRequired = true
	}
	if item.SourceURL == "" && item.SourcePageURL == "" {
		reviewRequired = true
		reasons = append(reasons, "missing_source_url")
	}
	if item.CurrentExecutionClass == "youtube_relay" && proposedCaptureType != "youtube_watch" {
		reviewRequired = true
		reasons = append(reasons, "relay_non_youtube_mismatch")
	}

	item.ProposedCaptureType = proposedCaptureType
	item.ProposedExecutionClass = proposedExecutionClass
	item.ProposedSourceFamily = proposedSourceFamily
	item.ReviewRequired = reviewRequired
	item.Reasons = uniqueStrings(reasons)
	item.WouldChange = item.CurrentSourceFamily != item.ProposedSourceFamily ||
		item.CurrentCaptureType != item.ProposedCaptureType ||
		item.CurrentExecutionClass != item.ProposedExecutionClass
	return item
}

func deriveExecutionClass(captureType, currentExecutionClass string) (string, string, bool) {
	switch captureType {
	case "youtube_watch":
		if currentExecutionClass == "youtube_relay" {
			return "youtube_relay", "keep_youtube_relay", false
		}
		return "youtube_relay", "youtube_relay_default", false
	case "still_image":
		return "image_poll", "image_poll_from_capture_type", false
	case "hls", "dash", "rtsp", "rtmp", "http_video":
		return "video_live", "video_live_from_capture_type", false
	case "webrtc":
		if currentExecutionClass != "" {
			return currentExecutionClass, "keep_current_execution_class", true
		}
		return "", "webrtc_requires_review", true
	case "unknown":
		if currentExecutionClass != "" {
			return currentExecutionClass, "keep_current_execution_class", true
		}
		return "", "unknown_requires_review", true
	default:
		if currentExecutionClass != "" {
			return currentExecutionClass, "keep_current_execution_class", true
		}
		return "", "execution_class_unknown", true
	}
}

func deriveSourceFamily(captureType, currentSourceFamily string) (string, string) {
	switch captureType {
	case "youtube_watch":
		return "watch_page", "watch_page_from_capture_type"
	case "hls", "dash":
		return "video_manifest", "video_manifest_from_capture_type"
	case "rtsp", "rtmp", "http_video":
		return "video_stream", "video_stream_from_capture_type"
	case "still_image":
		return "still_image", "still_image_from_capture_type"
	case "webrtc":
		return "embed_page", "embed_page_from_capture_type"
	case "unknown":
		if currentSourceFamily != "" {
			return currentSourceFamily, "keep_current_source_family"
		}
		return "", "source_family_unknown"
	default:
		if currentSourceFamily != "" {
			return currentSourceFamily, "keep_current_source_family"
		}
		return "", "source_family_unknown"
	}
}

func normalizeSourceFamily(v string) string {
	if normalized, ok := capture.NormalizeSourceFamily(v); ok {
		return normalized
	}
	return ""
}

func normalizeCaptureType(v string) string {
	if normalized, ok := capture.NormalizeCaptureType(v); ok {
		return normalized
	}
	return ""
}

func normalizeExecutionClass(v string) string {
	if normalized, ok := capture.NormalizeExecutionClass(v); ok {
		return normalized
	}
	return ""
}

func uniqueStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, v := range in {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func countStreamV2Items(items []streamV2MigrationItem, keep func(streamV2MigrationItem) bool) int {
	n := 0
	for _, item := range items {
		if keep(item) {
			n++
		}
	}
	return n
}

func summarizeStreamV2Items(items []streamV2MigrationItem, field func(streamV2MigrationItem) string) map[string]int {
	out := map[string]int{}
	for _, item := range items {
		key := strings.TrimSpace(field(item))
		if key == "" {
			key = "<empty>"
		}
		out[key]++
	}
	return out
}
