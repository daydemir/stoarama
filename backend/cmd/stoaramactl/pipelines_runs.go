package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
)

func printPipelinesUsage() {
	fmt.Print("stoaramactl pipelines <list|register|versions|runs|overview|stream-list|set> ...\n")
}

func runPipelineRegister(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("pipelines register", flag.ExitOnError)
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
	id := fs.String("id", "", "pipeline id")
	family := fs.String("family", "", "pipeline family")
	kind := fs.String("kind", "detector", "pipeline kind")
	specJSON := fs.String("spec-json", "{}", "pipeline spec JSON object")
	active := fs.Bool("active", true, "whether the pipeline is active")
	asJSON := fs.Bool("json", false, "print JSON")
	_ = fs.Parse(args)

	if strings.TrimSpace(*id) == "" {
		log.Fatalf("--id is required")
	}
	if strings.TrimSpace(*family) == "" {
		log.Fatalf("--family is required")
	}
	spec := parseJSONObjectFlag("--spec-json", *specJSON)
	payload := mustAPIRequest(ctx, http.MethodPost, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), "/api/v1/pipelines/sync", map[string]any{
		"pipelines": []map[string]any{{
			"id":              strings.TrimSpace(*id),
			"pipeline_family": strings.TrimSpace(*family),
			"kind":            strings.TrimSpace(*kind),
			"spec_json":       spec,
			"active":          *active,
		}},
	})
	if *asJSON {
		printJSON(payload)
		return
	}
	fmt.Printf("registered pipeline id=%s family=%s kind=%s active=%t\n", strings.TrimSpace(*id), strings.TrimSpace(*family), strings.TrimSpace(*kind), *active)
}

func runPipelineVersions(ctx context.Context, cfg config.Config, args []string) {
	if len(args) >= 1 && (args[0] == "-h" || args[0] == "--help") {
		fmt.Print("stoaramactl pipelines versions <sync|list> ...\n")
		return
	}
	if len(args) < 1 {
		fmt.Print("stoaramactl pipelines versions <sync|list> ...\n")
		return
	}
	switch args[0] {
	case "sync":
		fs := flag.NewFlagSet("pipelines versions sync", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		pipelineID := fs.String("pipeline-id", "", "pipeline id")
		versionID := fs.String("version-id", "", "version id")
		runnerKind := fs.String("runner-kind", "external", "runner kind")
		specJSON := fs.String("spec-json", "{}", "version spec JSON object")
		createdBy := fs.String("created-by", "stoaramactl", "created by label")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if strings.TrimSpace(*pipelineID) == "" {
			log.Fatalf("--pipeline-id is required")
		}
		if strings.TrimSpace(*versionID) == "" {
			log.Fatalf("--version-id is required")
		}
		spec := parseJSONObjectFlag("--spec-json", *specJSON)
		payload := mustAPIRequest(ctx, http.MethodPost, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), "/api/v1/pipeline-versions/sync", map[string]any{
			"versions": []map[string]any{{
				"pipeline_id": strings.TrimSpace(*pipelineID),
				"version_id":  strings.TrimSpace(*versionID),
				"runner_kind": strings.TrimSpace(*runnerKind),
				"spec_json":   spec,
				"created_by":  strings.TrimSpace(*createdBy),
			}},
		})
		if *asJSON {
			printJSON(payload)
			return
		}
		fmt.Printf("registered pipeline_version pipeline=%s version=%s runner_kind=%s\n", strings.TrimSpace(*pipelineID), strings.TrimSpace(*versionID), strings.TrimSpace(*runnerKind))
	case "list":
		fs := flag.NewFlagSet("pipelines versions list", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		pipelineID := fs.String("pipeline-id", "", "optional pipeline id")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		path := "/api/v1/pipeline-versions"
		if v := strings.TrimSpace(*pipelineID); v != "" {
			path += "?pipeline_id=" + url.QueryEscape(v)
		}
		payload := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), path)
		if *asJSON {
			printJSON(payload)
			return
		}
		items, _ := payload["items"].([]any)
		for _, raw := range items {
			it := asMap(raw)
			fmt.Printf("id=%v pipeline=%s version=%s runner_kind=%s created_by=%s created_at=%s spec=%s\n",
				it["id"], fmt.Sprint(it["pipeline_id"]), fmt.Sprint(it["version_id"]), fmt.Sprint(it["runner_kind"]),
				fmt.Sprint(it["created_by"]), fmt.Sprint(it["created_at"]), oneLineJSON(it["spec_json"]))
		}
	default:
		log.Fatalf("unknown pipelines versions subcommand: %s", args[0])
	}
}

func runPipelineRuns(ctx context.Context, cfg config.Config, args []string) {
	if len(args) >= 1 && (args[0] == "-h" || args[0] == "--help") {
		fmt.Print("stoaramactl pipelines runs <create|list|get|claim|complete|fail> ...\n")
		return
	}
	if len(args) < 1 {
		fmt.Print("stoaramactl pipelines runs <create|list|get|claim|complete|fail> ...\n")
		return
	}
	switch args[0] {
	case "create":
		fs := flag.NewFlagSet("pipelines runs create", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		pipelineID := fs.String("pipeline-id", "", "pipeline id")
		versionID := fs.String("version-id", "", "pipeline version id")
		label := fs.String("label", "", "run label")
		workerKind := fs.String("worker-kind", "external", "worker kind")
		frameIDsRaw := fs.String("frame-ids", "", "comma-separated frame ids")
		streamIDsRaw := fs.String("stream-ids", "", "comma-separated stream ids")
		tagsRaw := fs.String("tags", "", "comma-separated tags")
		latestOnly := fs.Bool("latest-only-per-stream", false, "use the latest frame per selected stream")
		limit := fs.Int("limit", 0, "optional target limit")
		metadataJSON := fs.String("metadata-json", "{}", "run metadata JSON object")
		createdBy := fs.String("created-by", "stoaramactl", "created by label")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if strings.TrimSpace(*pipelineID) == "" {
			log.Fatalf("--pipeline-id is required")
		}
		if strings.TrimSpace(*versionID) == "" {
			log.Fatalf("--version-id is required")
		}
		frameIDs, err := parseInt64CSV(*frameIDsRaw)
		if err != nil {
			log.Fatalf("parse --frame-ids: %v", err)
		}
		streamIDs, err := parseInt64CSV(*streamIDsRaw)
		if err != nil {
			log.Fatalf("parse --stream-ids: %v", err)
		}
		metadata := parseJSONObjectFlag("--metadata-json", *metadataJSON)
		payload := mustAPIRequest(ctx, http.MethodPost, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), "/api/v1/pipeline-runs", map[string]any{
			"pipeline_id":            strings.TrimSpace(*pipelineID),
			"version_id":             strings.TrimSpace(*versionID),
			"label":                  strings.TrimSpace(*label),
			"worker_kind":            strings.TrimSpace(*workerKind),
			"frame_ids":              frameIDs,
			"stream_ids":             streamIDs,
			"tags":                   parseStringCSVFlag(*tagsRaw),
			"latest_only_per_stream": *latestOnly,
			"limit":                  *limit,
			"metadata_json":          metadata,
			"created_by":             strings.TrimSpace(*createdBy),
		})
		if *asJSON {
			printJSON(payload)
			return
		}
		printPipelineRunItem(asMap(payload))
	case "list":
		fs := flag.NewFlagSet("pipelines runs list", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		pipelineID := fs.String("pipeline-id", "", "optional pipeline id")
		limit := fs.Int("limit", 200, "row limit")
		offset := fs.Int("offset", 0, "row offset")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		q := url.Values{}
		q.Set("limit", strconv.Itoa(*limit))
		q.Set("offset", strconv.Itoa(*offset))
		if v := strings.TrimSpace(*pipelineID); v != "" {
			q.Set("pipeline_id", v)
		}
		payload := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), "/api/v1/pipeline-runs?"+q.Encode())
		if *asJSON {
			printJSON(payload)
			return
		}
		items, _ := payload["items"].([]any)
		for _, raw := range items {
			printPipelineRunItem(asMap(raw))
		}
	case "get":
		fs := flag.NewFlagSet("pipelines runs get", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		id := fs.Int64("id", 0, "run id")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if *id <= 0 {
			log.Fatalf("--id is required")
		}
		payload := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), fmt.Sprintf("/api/v1/pipeline-runs/%d", *id))
		if *asJSON {
			printJSON(payload)
			return
		}
		printPipelineRunItem(payload)
	case "claim":
		fs := flag.NewFlagSet("pipelines runs claim", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		id := fs.Int64("id", 0, "run id")
		claimedBy := fs.String("claimed-by", "", "worker id")
		limit := fs.Int("limit", 100, "claim limit")
		leaseSec := fs.Int("lease-sec", 600, "lease duration seconds")
		forceRerun := fs.Bool("force-rerun", false, "allow rerunning already successful targets")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if *id <= 0 {
			log.Fatalf("--id is required")
		}
		if strings.TrimSpace(*claimedBy) == "" {
			log.Fatalf("--claimed-by is required")
		}
		payload := mustAPIRequest(ctx, http.MethodPost, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), fmt.Sprintf("/api/v1/pipeline-runs/%d/claims", *id), map[string]any{
			"claimed_by":  strings.TrimSpace(*claimedBy),
			"limit":       *limit,
			"lease_sec":   *leaseSec,
			"force_rerun": *forceRerun,
		})
		if *asJSON {
			printJSON(payload)
			return
		}
		items, _ := payload["items"].([]any)
		fmt.Printf("claimed=%d run_id=%d claimed_by=%s\n", len(items), *id, strings.TrimSpace(*claimedBy))
		for _, raw := range items {
			it := asMap(raw)
			fmt.Printf("claim_id=%v frame_id=%v stream_id=%v pipeline=%s pipeline_version_id=%v lease_expires_at=%s download_url=%s\n",
				it["claim_id"], it["frame_id"], it["stream_id"], fmt.Sprint(it["pipeline_id"]), it["pipeline_version_id"], fmt.Sprint(it["lease_expires_at"]), fmt.Sprint(it["download_url"]))
		}
	case "complete":
		fs := flag.NewFlagSet("pipelines runs complete", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		claimID := fs.Int64("claim-id", 0, "claim id")
		pipelineID := fs.String("pipeline-id", "", "pipeline id")
		pipelineRunID := fs.Int64("pipeline-run-id", 0, "pipeline run id")
		pipelineVersionID := fs.Int64("pipeline-version-id", 0, "pipeline version id")
		frameID := fs.Int64("frame-id", 0, "frame id")
		claimedBy := fs.String("claimed-by", "", "worker id")
		forceRerun := fs.Bool("force-rerun", false, "force rerun revision")
		revisionMode := fs.String("revision-mode", "", "optional revision mode, e.g. force_rerun")
		summaryJSON := fs.String("summary-json", "{}", "summary JSON object")
		rawOutputJSON := fs.String("raw-output-json", "{}", "raw output JSON object")
		runnerInfoJSON := fs.String("runner-info-json", "{}", "runner info JSON object")
		detectionsJSON := fs.String("detections-json", "[]", "detections JSON array")
		signalsJSON := fs.String("signals-json", "[]", "signals JSON array")
		startedAtRaw := fs.String("started-at", "", "optional RFC3339 timestamp")
		finishedAtRaw := fs.String("finished-at", "", "optional RFC3339 timestamp")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		payload := buildPipelineRunCompletionPayload(
			*claimID,
			strings.TrimSpace(*pipelineID),
			*pipelineRunID,
			*pipelineVersionID,
			*frameID,
			strings.TrimSpace(*claimedBy),
			*forceRerun,
			strings.TrimSpace(*revisionMode),
			*summaryJSON,
			*rawOutputJSON,
			*runnerInfoJSON,
			*detectionsJSON,
			*signalsJSON,
			*startedAtRaw,
			*finishedAtRaw,
		)
		resp := mustAPIRequest(ctx, http.MethodPost, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), "/api/v1/inference/commit", payload)
		if *asJSON {
			printJSON(resp)
			return
		}
		fmt.Printf("completed claim_id=%d result_id=%v revision=%v\n", *claimID, resp["result_id"], resp["revision"])
	case "fail":
		fs := flag.NewFlagSet("pipelines runs fail", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		claimID := fs.Int64("claim-id", 0, "claim id")
		pipelineID := fs.String("pipeline-id", "", "pipeline id")
		pipelineRunID := fs.Int64("pipeline-run-id", 0, "pipeline run id")
		pipelineVersionID := fs.Int64("pipeline-version-id", 0, "pipeline version id")
		frameID := fs.Int64("frame-id", 0, "frame id")
		claimedBy := fs.String("claimed-by", "", "worker id")
		errorText := fs.String("error-text", "", "failure reason")
		runnerInfoJSON := fs.String("runner-info-json", "{}", "runner info JSON object")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if *claimID <= 0 || *frameID <= 0 || strings.TrimSpace(*pipelineID) == "" || strings.TrimSpace(*claimedBy) == "" || strings.TrimSpace(*errorText) == "" {
			log.Fatalf("--claim-id, --frame-id, --pipeline-id, --claimed-by, and --error-text are required")
		}
		if *pipelineRunID <= 0 {
			log.Fatalf("--pipeline-run-id is required")
		}
		payload := map[string]any{
			"claim_id":         *claimID,
			"pipeline_id":      strings.TrimSpace(*pipelineID),
			"pipeline_run_id":  *pipelineRunID,
			"frame_id":         *frameID,
			"claimed_by":       strings.TrimSpace(*claimedBy),
			"error_text":       strings.TrimSpace(*errorText),
			"runner_info_json": parseJSONObjectFlag("--runner-info-json", *runnerInfoJSON),
		}
		if *pipelineVersionID > 0 {
			payload["pipeline_version_id"] = *pipelineVersionID
		}
		resp := mustAPIRequest(ctx, http.MethodPost, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), "/api/v1/inference/fail", payload)
		if *asJSON {
			printJSON(resp)
			return
		}
		fmt.Printf("failed claim_id=%d ok=%v\n", *claimID, resp["ok"])
	default:
		log.Fatalf("unknown pipelines runs subcommand: %s", args[0])
	}
}

func buildPipelineRunCompletionPayload(claimID int64, pipelineID string, pipelineRunID int64, pipelineVersionID int64, frameID int64, claimedBy string, forceRerun bool, revisionMode string, summaryJSON string, rawOutputJSON string, runnerInfoJSON string, detectionsJSON string, signalsJSON string, startedAtRaw string, finishedAtRaw string) map[string]any {
	if claimID <= 0 || frameID <= 0 || pipelineRunID <= 0 || strings.TrimSpace(pipelineID) == "" || strings.TrimSpace(claimedBy) == "" {
		log.Fatalf("--claim-id, --frame-id, --pipeline-run-id, --pipeline-id, and --claimed-by are required")
	}
	payload := map[string]any{
		"claim_id":         claimID,
		"pipeline_id":      strings.TrimSpace(pipelineID),
		"pipeline_run_id":  pipelineRunID,
		"frame_id":         frameID,
		"claimed_by":       strings.TrimSpace(claimedBy),
		"force_rerun":      forceRerun,
		"revision_mode":    strings.TrimSpace(revisionMode),
		"summary_json":     parseJSONObjectFlag("--summary-json", summaryJSON),
		"raw_output_json":  parseJSONObjectFlag("--raw-output-json", rawOutputJSON),
		"runner_info_json": parseJSONObjectFlag("--runner-info-json", runnerInfoJSON),
		"detections":       parseJSONArrayFlag("--detections-json", detectionsJSON),
		"signals":          parseJSONArrayFlag("--signals-json", signalsJSON),
	}
	if pipelineVersionID > 0 {
		payload["pipeline_version_id"] = pipelineVersionID
	}
	if startedAt := parseOptionalRFC3339("--started-at", startedAtRaw); startedAt != nil {
		payload["started_at"] = startedAt.Format(time.RFC3339)
	}
	if finishedAt := parseOptionalRFC3339("--finished-at", finishedAtRaw); finishedAt != nil {
		payload["finished_at"] = finishedAt.Format(time.RFC3339)
	}
	return payload
}

func parseJSONObjectFlag(flagName string, raw string) map[string]any {
	value := strings.TrimSpace(raw)
	if value == "" {
		return map[string]any{}
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(value), &out); err != nil {
		log.Fatalf("invalid %s: %v", flagName, err)
	}
	if out == nil {
		return map[string]any{}
	}
	return out
}

func parseJSONArrayFlag(flagName string, raw string) []any {
	value := strings.TrimSpace(raw)
	if value == "" {
		return nil
	}
	var out []any
	if err := json.Unmarshal([]byte(value), &out); err != nil {
		log.Fatalf("invalid %s: %v", flagName, err)
	}
	return out
}

func parseStringCSVFlag(raw string) []string {
	parts := strings.Split(raw, ",")
	seen := make(map[string]struct{}, len(parts))
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		v := strings.TrimSpace(part)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func parseOptionalRFC3339(flagName string, raw string) *time.Time {
	value := strings.TrimSpace(raw)
	if value == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		log.Fatalf("invalid %s: %v", flagName, err)
	}
	return &parsed
}

func printPipelineRunItem(it map[string]any) {
	fmt.Printf("run_id=%v pipeline=%s version=%s version_row_id=%v status=%s worker_kind=%s targets=%v completed=%v errors=%v leased=%v created_by=%s created_at=%s label=%q selector=%s metadata=%s\n",
		it["id"], fmt.Sprint(it["pipeline_id"]), fmt.Sprint(it["version_id"]), it["pipeline_version_id"],
		fmt.Sprint(it["status"]), fmt.Sprint(it["worker_kind"]), it["target_count"], it["completed_count"], it["error_count"], it["leased_count"],
		fmt.Sprint(it["created_by"]), fmt.Sprint(it["created_at"]), fmt.Sprint(it["label"]), oneLineJSON(it["selector_json"]), oneLineJSON(it["metadata_json"]))
}
