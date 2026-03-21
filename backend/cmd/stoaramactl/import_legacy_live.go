package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/model"
)

type legacyDashboardPage struct {
	Items  []legacyDashboardItem `json:"items"`
	Limit  int                   `json:"limit"`
	Offset int                   `json:"offset"`
	Total  int64                 `json:"total"`
}

type legacyDashboardItem struct {
	Stream          model.Stream `json:"stream"`
	LatestCaptured  *time.Time   `json:"latest_captured_at"`
	LatestFrameURL  string       `json:"latest_frame_url"`
	RecordingHealth string       `json:"recording_health"`
	LegacyOffset    int          `json:"-"`
}

type legacyImportResult struct {
	LegacyOffset        int       `json:"legacy_offset"`
	LegacyID            int64     `json:"legacy_id"`
	Provider            string    `json:"provider"`
	ExternalID          string    `json:"external_id"`
	Slug                string    `json:"slug"`
	Name                string    `json:"name"`
	RecordingHealth     string    `json:"recording_health"`
	ProbeOK             bool      `json:"probe_ok"`
	ProbeResolvedURL    string    `json:"probe_resolved_url,omitempty"`
	ProbeImage          bool      `json:"probe_is_image"`
	ProbeWidth          int       `json:"probe_width,omitempty"`
	ProbeHeight         int       `json:"probe_height,omitempty"`
	ProbeMIMEType       string    `json:"probe_mime_type,omitempty"`
	ProbeError          string    `json:"probe_error,omitempty"`
	Imported            bool      `json:"imported"`
	Created             bool      `json:"created"`
	ImportedStreamID    int64     `json:"imported_stream_id,omitempty"`
	ImportedSlug        string    `json:"imported_slug,omitempty"`
	LatestFrameImported bool      `json:"latest_frame_imported"`
	LatestFrameError    string    `json:"latest_frame_error,omitempty"`
	ImportError         string    `json:"import_error,omitempty"`
	StartedAt           time.Time `json:"started_at"`
	FinishedAt          time.Time `json:"finished_at"`
	DurationMs          int64     `json:"duration_ms"`
}

type legacyImportReport struct {
	LegacyAPIURL         string               `json:"legacy_api_url"`
	TargetAPIURL         string               `json:"target_api_url"`
	Offset               int                  `json:"offset"`
	Limit                int                  `json:"limit"`
	PageSize             int                  `json:"page_size"`
	Concurrency          int                  `json:"concurrency"`
	ProbeTimeout         string               `json:"probe_timeout"`
	Apply                bool                 `json:"apply"`
	CheckpointJSONL      string               `json:"checkpoint_jsonl,omitempty"`
	ImportLatestFrame    bool                 `json:"import_latest_frame"`
	GeneratedAt          time.Time            `json:"generated_at"`
	Processed            int                  `json:"processed"`
	ProbedOK             int                  `json:"probed_ok"`
	Imported             int                  `json:"imported"`
	Created              int                  `json:"created"`
	Updated              int                  `json:"updated"`
	ProbeFailed          int                  `json:"probe_failed"`
	ImportFailed         int                  `json:"import_failed"`
	LatestFramesImported int                  `json:"latest_frames_imported"`
	Results              []legacyImportResult `json:"results"`
}

func runImport(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 {
		printImportUsage()
		return
	}
	switch args[0] {
	case "legacy-live-streams":
		runImportLegacyLiveStreams(ctx, cfg, args[1:])
	default:
		log.Fatalf("unknown import subcommand: %s", args[0])
	}
}

func printImportUsage() {
	fmt.Print("stoaramactl import legacy-live-streams [--legacy-api-url URL --legacy-api-token TOKEN --target-api-url URL --service-token TOKEN --offset N --limit 200 --page-size 50 --concurrency 4 --probe-timeout-sec 45 --legacy-recording-state off|on --legacy-provider P --apply --import-latest-frame --checkpoint-jsonl file --resume=true --report-json out.json --json]\n")
}

func runImportLegacyLiveStreams(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("import legacy-live-streams", flag.ExitOnError)
	legacyAPIURL := fs.String("legacy-api-url", defaultLegacyBackendAPIURL(), "legacy backend API base URL")
	legacyAPIToken := fs.String("legacy-api-token", defaultLegacyAPIToken(), "legacy backend API token")
	targetAPIURL := fs.String("target-api-url", defaultBackendAPIURL(), "target Stoarama API base URL")
	serviceToken := fs.String("service-token", cfg.ServiceToken, "target Stoarama service token")
	offset := fs.Int("offset", -1, "legacy stream offset (-1 means infer from checkpoint when resuming)")
	limit := fs.Int("limit", 200, "maximum legacy streams to process")
	pageSize := fs.Int("page-size", 50, "legacy API page size")
	concurrency := fs.Int("concurrency", 4, "probe/import worker concurrency")
	probeTimeoutSec := fs.Int("probe-timeout-sec", 45, "per-stream probe timeout seconds")
	legacyRecordingState := fs.String("legacy-recording-state", "", "optional legacy recording state filter off|on")
	legacyProvider := fs.String("legacy-provider", "", "optional legacy provider filter")
	apply := fs.Bool("apply", false, "import live streams into Stoarama")
	importLatestFrame := fs.Bool("import-latest-frame", false, "import the legacy latest frame as a snapshot for successfully imported streams")
	checkpointJSONL := fs.String("checkpoint-jsonl", defaultLegacyImportCheckpointPath(), "append-only checkpoint jsonl path")
	resume := fs.Bool("resume", true, "resume from checkpoint and skip already-checked legacy stream ids")
	reportJSON := fs.String("report-json", "", "optional report JSON path")
	asJSON := fs.Bool("json", false, "print JSON report")
	_ = fs.Parse(args)

	if strings.TrimSpace(*legacyAPIURL) == "" {
		log.Fatalf("--legacy-api-url is required")
	}
	if strings.TrimSpace(*legacyAPIToken) == "" {
		log.Fatalf("--legacy-api-token is required")
	}
	if strings.TrimSpace(*targetAPIURL) == "" {
		log.Fatalf("--target-api-url is required")
	}
	if *apply && strings.TrimSpace(*serviceToken) == "" {
		log.Fatalf("--service-token is required with --apply")
	}
	if *limit <= 0 {
		log.Fatalf("--limit must be > 0")
	}
	if *pageSize <= 0 {
		log.Fatalf("--page-size must be > 0")
	}
	if *pageSize > *limit {
		*pageSize = *limit
	}
	if *concurrency <= 0 {
		log.Fatalf("--concurrency must be > 0")
	}
	if *probeTimeoutSec <= 0 {
		log.Fatalf("--probe-timeout-sec must be > 0")
	}

	seen := map[int64]legacyImportResult{}
	resumeOffset := 0
	if *resume && strings.TrimSpace(*checkpointJSONL) != "" {
		loaded, maxOffset, err := loadLegacyImportCheckpoint(strings.TrimSpace(*checkpointJSONL))
		if err != nil {
			log.Fatalf("load checkpoint: %v", err)
		}
		seen = loaded
		resumeOffset = maxOffset + 1
	}
	effectiveOffset := *offset
	if effectiveOffset < 0 {
		effectiveOffset = 0
		if *resume && len(seen) > 0 {
			effectiveOffset = resumeOffset
		}
	}

	items, err := fetchLegacyStreams(ctx, strings.TrimSpace(*legacyAPIURL), strings.TrimSpace(*legacyAPIToken), effectiveOffset, *limit, *pageSize, strings.TrimSpace(*legacyRecordingState), strings.TrimSpace(*legacyProvider), *importLatestFrame, seen)
	if err != nil {
		log.Fatalf("fetch legacy streams: %v", err)
	}

	results := make([]legacyImportResult, len(items))
	var wg sync.WaitGroup
	workCh := make(chan int)
	for worker := 0; worker < *concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range workCh {
				results[idx] = processLegacyImportItem(ctx, items[idx], strings.TrimSpace(*targetAPIURL), strings.TrimSpace(*serviceToken), time.Duration(*probeTimeoutSec)*time.Second, *apply, *importLatestFrame)
				if strings.TrimSpace(*checkpointJSONL) != "" {
					if err := appendLegacyImportCheckpoint(strings.TrimSpace(*checkpointJSONL), results[idx]); err != nil {
						log.Printf("append checkpoint legacy_id=%d: %v", results[idx].LegacyID, err)
					}
				}
			}
		}()
	}
	for idx := range items {
		workCh <- idx
	}
	close(workCh)
	wg.Wait()

	sort.Slice(results, func(i, j int) bool {
		return results[i].LegacyID < results[j].LegacyID
	})

	report := legacyImportReport{
		LegacyAPIURL:      strings.TrimSpace(*legacyAPIURL),
		TargetAPIURL:      strings.TrimSpace(*targetAPIURL),
		Offset:            effectiveOffset,
		Limit:             *limit,
		PageSize:          *pageSize,
		Concurrency:       *concurrency,
		ProbeTimeout:      (time.Duration(*probeTimeoutSec) * time.Second).String(),
		Apply:             *apply,
		CheckpointJSONL:   strings.TrimSpace(*checkpointJSONL),
		ImportLatestFrame: *importLatestFrame,
		GeneratedAt:       time.Now().UTC(),
		Processed:         len(results),
		Results:           results,
	}
	for _, result := range results {
		if result.ProbeOK {
			report.ProbedOK++
		} else {
			report.ProbeFailed++
		}
		if result.Imported {
			report.Imported++
			if result.Created {
				report.Created++
			} else {
				report.Updated++
			}
		}
		if result.ImportError != "" {
			report.ImportFailed++
		}
		if result.LatestFrameImported {
			report.LatestFramesImported++
		}
	}

	if path := strings.TrimSpace(*reportJSON); path != "" {
		if err := writeLegacyImportReport(path, report); err != nil {
			log.Fatalf("write report: %v", err)
		}
	}
	if *asJSON {
		printJSON(report)
		return
	}
	fmt.Printf("processed=%d probed_ok=%d probe_failed=%d imported=%d created=%d updated=%d import_failed=%d\n",
		report.Processed, report.ProbedOK, report.ProbeFailed, report.Imported, report.Created, report.Updated, report.ImportFailed)
	for _, result := range results {
		state := "dead"
		if result.ProbeOK {
			state = "live"
		}
		importState := "dry-run"
		if result.Imported {
			if result.Created {
				importState = "created"
			} else {
				importState = "updated"
			}
		} else if result.ImportError != "" {
			importState = "import-error"
		}
		fmt.Printf("legacy_id=%d provider=%s external_id=%s state=%s import=%s slug=%s error=%s\n",
			result.LegacyID, result.Provider, result.ExternalID, state, importState, result.Slug, firstNonEmptyString(result.ImportError, result.ProbeError))
	}
}

func fetchLegacyStreams(ctx context.Context, baseURL, token string, offset, limit, pageSize int, recordingState, provider string, includeImageURLs bool, seen map[int64]legacyImportResult) ([]legacyDashboardItem, error) {
	items := make([]legacyDashboardItem, 0, limit)
	scanOffset := offset
	for len(items) < limit {
		batchSize := pageSize
		if remaining := limit - len(items); remaining < batchSize {
			batchSize = remaining
		}
		q := url.Values{}
		q.Set("limit", fmt.Sprintf("%d", batchSize))
		q.Set("offset", fmt.Sprintf("%d", scanOffset))
		if includeImageURLs {
			q.Set("include_image_urls", "true")
		} else {
			q.Set("include_image_urls", "false")
		}
		if strings.TrimSpace(recordingState) != "" {
			q.Set("recording_state", strings.TrimSpace(recordingState))
		}
		if strings.TrimSpace(provider) != "" {
			q.Set("provider", strings.TrimSpace(provider))
		}
		var page legacyDashboardPage
		if err := getJSONWithToken(ctx, baseURL, token, "/api/v1/dashboard/streams?"+q.Encode(), &page); err != nil {
			return nil, err
		}
		if len(page.Items) == 0 {
			break
		}
		for idx, item := range page.Items {
			item.LegacyOffset = scanOffset + idx
			if _, ok := seen[item.Stream.ID]; ok {
				continue
			}
			items = append(items, item)
			if len(items) >= limit {
				break
			}
		}
		if len(page.Items) < batchSize || len(items) >= limit {
			break
		}
		scanOffset += len(page.Items)
	}
	return items, nil
}

func processLegacyImportItem(ctx context.Context, item legacyDashboardItem, targetAPIURL, serviceToken string, probeTimeout time.Duration, apply bool, importLatestFrame bool) (result legacyImportResult) {
	started := time.Now().UTC()
	result = legacyImportResult{
		LegacyOffset:    item.LegacyOffset,
		LegacyID:        item.Stream.ID,
		Provider:        item.Stream.Provider,
		ExternalID:      item.Stream.ExternalID,
		Slug:            item.Stream.Slug,
		Name:            item.Stream.Name,
		RecordingHealth: item.RecordingHealth,
		StartedAt:       started,
	}
	defer func() {
		result.FinishedAt = time.Now().UTC()
		result.DurationMs = result.FinishedAt.Sub(started).Milliseconds()
	}()

	probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	resolvedURL, isImage, err := capture.ResolveCaptureInput(probeCtx, item.Stream.Provider, item.Stream.SourceURL, item.Stream.SourcePageURL)
	if err != nil {
		result.ProbeError = err.Error()
		return result
	}
	frame, err := capture.CaptureFrame(probeCtx, resolvedURL)
	if err != nil {
		result.ProbeResolvedURL = resolvedURL
		result.ProbeImage = isImage
		result.ProbeError = err.Error()
		return result
	}
	result.ProbeOK = true
	result.ProbeResolvedURL = resolvedURL
	result.ProbeImage = isImage
	result.ProbeWidth = frame.Width
	result.ProbeHeight = frame.Height
	result.ProbeMIMEType = frame.MIMEType

	if !apply {
		return result
	}

	payload := map[string]any{
		"provider":              strings.TrimSpace(item.Stream.Provider),
		"external_id":           strings.TrimSpace(item.Stream.ExternalID),
		"name":                  strings.TrimSpace(item.Stream.Name),
		"slug":                  strings.TrimSpace(item.Stream.Slug),
		"source_url":            strings.TrimSpace(item.Stream.SourceURL),
		"source_page_url":       strings.TrimSpace(item.Stream.SourcePageURL),
		"source_family":         strings.TrimSpace(item.Stream.SourceFamily),
		"capture_type":          strings.TrimSpace(item.Stream.CaptureType),
		"execution_class":       strings.TrimSpace(item.Stream.ExecutionClass),
		"execution_config_json": item.Stream.ExecutionConfigJSON,
		"tags":                  append([]string{"imported:legacy-social-isolation"}, item.Stream.Tags...),
		"location_text":         strings.TrimSpace(item.Stream.LocationText),
		"location_country":      strings.TrimSpace(item.Stream.LocationCountry),
		"location_country_code": strings.TrimSpace(item.Stream.LocationCountryCode),
		"location_region":       strings.TrimSpace(item.Stream.LocationRegion),
		"location_city":         strings.TrimSpace(item.Stream.LocationCity),
		"location_locality":     strings.TrimSpace(item.Stream.LocationLocality),
		"location_source":       strings.TrimSpace(item.Stream.LocationSource),
		"metadata_json":         legacyImportMetadata(item, resolvedURL, frame),
	}
	var response struct {
		OK      bool         `json:"ok"`
		Created bool         `json:"created"`
		Stream  model.Stream `json:"stream"`
	}
	if err := postJSONWithToken(ctx, targetAPIURL, serviceToken, "/api/v1/imports/streams", payload, &response); err != nil {
		result.ImportError = err.Error()
		return result
	}
	result.Imported = true
	result.Created = response.Created
	result.ImportedStreamID = response.Stream.ID
	result.ImportedSlug = response.Stream.Slug
	if importLatestFrame && strings.TrimSpace(item.LatestFrameURL) != "" && result.ImportedStreamID > 0 {
		payload := map[string]any{
			"stream_id":    result.ImportedStreamID,
			"frame_url":    strings.TrimSpace(item.LatestFrameURL),
			"captured_at":  legacyCapturedAtString(item.LatestCaptured),
			"source_kind":  "snapshot_url",
			"source_label": "legacy-latest-frame",
		}
		var frameResp struct {
			OK        bool   `json:"ok"`
			Inserted  bool   `json:"inserted"`
			ObjectKey string `json:"object_key"`
		}
		if err := postJSONWithToken(ctx, targetAPIURL, serviceToken, "/api/v1/imports/frames", payload, &frameResp); err != nil {
			result.LatestFrameError = err.Error()
		} else {
			result.LatestFrameImported = frameResp.Inserted
		}
	}
	return result
}

func legacyImportMetadata(item legacyDashboardItem, resolvedURL string, frame capture.Frame) map[string]any {
	meta := map[string]any{}
	for k, v := range item.Stream.MetadataJSON {
		meta[k] = v
	}
	meta["imported_from"] = map[string]any{
		"system":           "social-isolation",
		"legacy_stream_id": item.Stream.ID,
		"imported_at":      time.Now().UTC().Format(time.RFC3339Nano),
		"recording_health": item.RecordingHealth,
	}
	meta["import_probe"] = map[string]any{
		"resolved_url": resolvedURL,
		"mime_type":    frame.MIMEType,
		"width":        frame.Width,
		"height":       frame.Height,
		"size_bytes":   frame.SizeBytes,
		"sha256":       frame.SHA256,
	}
	return meta
}

func writeLegacyImportReport(path string, report legacyImportReport) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func appendLegacyImportCheckpoint(path string, result legacyImportResult) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	b, err := json.Marshal(result)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		return err
	}
	return nil
}

func loadLegacyImportCheckpoint(path string) (map[int64]legacyImportResult, int, error) {
	out := map[int64]legacyImportResult{}
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return out, -1, nil
		}
		return nil, 0, err
	}
	maxOffset := -1
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var item legacyImportResult
		if err := json.Unmarshal([]byte(line), &item); err != nil {
			return nil, 0, fmt.Errorf("decode checkpoint line: %w", err)
		}
		out[item.LegacyID] = item
		if item.LegacyOffset > maxOffset {
			maxOffset = item.LegacyOffset
		}
	}
	return out, maxOffset, nil
}

func defaultLegacyImportCheckpointPath() string {
	return "local/reports/legacy-import-checkpoint.jsonl"
}

func legacyCapturedAtString(t *time.Time) string {
	if t == nil || t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func getJSONWithToken(ctx context.Context, baseURL, token, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(token))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errPayload map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&errPayload)
		return fmt.Errorf("GET %s: status=%d body=%v", path, resp.StatusCode, errPayload)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func postJSONWithToken(ctx context.Context, baseURL, token, path string, payload, out any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+path, strings.NewReader(string(b)))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(token))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errPayload map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&errPayload)
		return fmt.Errorf("POST %s: status=%d body=%v", path, resp.StatusCode, errPayload)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func defaultLegacyBackendAPIURL() string {
	for _, key := range []string{"LEGACY_BACKEND_API_URL", "BACKEND_API_URL"} {
		if v := strings.TrimSpace(os.Getenv(key)); v != "" {
			return v
		}
	}
	return ""
}

func defaultLegacyAPIToken() string {
	for _, key := range []string{"LEGACY_API_TOKEN", "API_TOKEN"} {
		if v := strings.TrimSpace(os.Getenv(key)); v != "" {
			return v
		}
	}
	return ""
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
