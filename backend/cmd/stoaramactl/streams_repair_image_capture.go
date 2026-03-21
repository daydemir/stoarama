package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/config"
)

func runStreamsRepairImageCapture(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("streams repair-image-capture", flag.ExitOnError)
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	apiToken := fs.String("api-token", cfg.APIToken, "service token")
	streamID := fs.Int64("id", 0, "single stream id")
	sourceURLLike := fs.String("source-url-like", "", "SQL ILIKE pattern for source_url")
	provider := fs.String("provider", "", "optional provider filter")
	limit := fs.Int("limit", 0, "optional max rows to inspect")
	onlyChanged := fs.Bool("only-changed", false, "only return rows that would change")
	apply := fs.Bool("apply", false, "persist proposed repairs")
	asJSON := fs.Bool("json", false, "print JSON")
	_ = fs.Parse(args)

	payload := map[string]any{
		"stream_id":        *streamID,
		"source_url_like":  strings.TrimSpace(*sourceURLLike),
		"provider":         strings.TrimSpace(*provider),
		"limit":            *limit,
		"only_changed":     *onlyChanged,
		"apply":            *apply,
		"recording_actor":  "stoaramactl.streams_repair_image_capture",
		"recording_reason": "repair image capture classification",
	}
	out := mustAPIRequest(ctx, http.MethodPost, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), "/api/v1/imports/streams/repair-image-capture", payload)
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
	fmt.Printf("image capture repair: total=%d selected=%d changed=%d applied=%d\n",
		int64FromAny(out["total"]),
		int64FromAny(out["selected"]),
		int64FromAny(out["changed"]),
		int64FromAny(out["applied"]),
	)
	for _, item := range items {
		fmt.Printf(
			"stream_id=%d slug=%s status=%s capture=%s->%s execution=%s->%s server=%v->%v source=%s\n",
			int64FromAny(item["id"]),
			fmt.Sprint(item["slug"]),
			map[bool]string{true: "update", false: "keep"}[boolFromAny(item["would_change"])],
			fmt.Sprint(item["current_capture_type"]),
			fmt.Sprint(item["proposed_capture_type"]),
			fmt.Sprint(item["current_execution_class"]),
			fmt.Sprint(item["proposed_execution_class"]),
			item["previous_server_id"],
			item["new_server_id"],
			fmt.Sprint(item["source_url"]),
		)
	}
}
