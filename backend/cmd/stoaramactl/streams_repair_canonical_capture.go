package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/config"
)

func runStreamsRepairCanonicalCapture(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("streams repair-canonical-capture", flag.ExitOnError)
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	apiToken := fs.String("api-token", cfg.APIToken, "service token")
	streamID := fs.Int64("id", 0, "single stream id")
	sourceURLLike := fs.String("source-url-like", "", "SQL ILIKE pattern for source_url")
	provider := fs.String("provider", "", "optional provider filter")
	limit := fs.Int("limit", 0, "optional max rows to inspect")
	onlyChanged := fs.Bool("only-changed", false, "only return rows that would change")
	onlyReview := fs.Bool("only-review", false, "only return rows that require review")
	legacyImportedOnly := fs.Bool("legacy-imported-only", true, "only inspect streams tagged as imported from legacy social-isolation")
	nonYouTubeOnly := fs.Bool("non-youtube-only", true, "skip youtube streams")
	apply := fs.Bool("apply", false, "persist safe repairs")
	asJSON := fs.Bool("json", false, "print JSON")
	_ = fs.Parse(args)

	payload := map[string]any{
		"stream_id":            *streamID,
		"source_url_like":      strings.TrimSpace(*sourceURLLike),
		"provider":             strings.TrimSpace(*provider),
		"limit":                *limit,
		"only_changed":         *onlyChanged,
		"only_review":          *onlyReview,
		"legacy_imported_only": *legacyImportedOnly,
		"non_youtube_only":     *nonYouTubeOnly,
		"apply":                *apply,
		"recording_actor":      "stoaramactl.streams_repair_canonical_capture",
		"recording_reason":     "repair canonical capture classification",
	}
	out := mustAPIRequest(ctx, http.MethodPost, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), "/api/v1/imports/streams/repair-canonical-capture", payload)
	if *asJSON {
		printJSON(out)
		return
	}

	items := make([]map[string]any, 0)
	if raw, ok := out["items"].([]any); ok {
		items = make([]map[string]any, 0, len(raw))
		for _, item := range raw {
			items = append(items, asMap(item))
		}
	}
	fmt.Printf("canonical capture repair: total=%d selected=%d changed=%d review_required=%d safe_to_apply=%d applied=%d\n",
		int64FromAny(out["total"]),
		int64FromAny(out["selected"]),
		int64FromAny(out["changed"]),
		int64FromAny(out["review_required"]),
		int64FromAny(out["safe_to_apply"]),
		int64FromAny(out["applied"]),
	)
	for _, item := range items {
		status := "keep"
		if boolFromAny(item["review_required"]) {
			status = "review"
		} else if boolFromAny(item["would_change"]) {
			status = "update"
		}
		fmt.Printf(
			"stream_id=%d slug=%s provider=%s status=%s capture=%s->%s execution=%s->%s source_family=%s->%s reasons=%s\n",
			int64FromAny(item["id"]),
			fmt.Sprint(item["slug"]),
			fmt.Sprint(item["provider"]),
			status,
			fmt.Sprint(item["current_capture_type"]),
			fmt.Sprint(item["proposed_capture_type"]),
			fmt.Sprint(item["current_execution_class"]),
			fmt.Sprint(item["proposed_execution_class"]),
			fmt.Sprint(item["current_source_family"]),
			fmt.Sprint(item["proposed_source_family"]),
			strings.Join(asStringSlice(item["reasons"]), ","),
		)
	}
}
