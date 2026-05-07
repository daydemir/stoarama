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
	"strings"
	"sync"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/captureapi"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/model"
	"github.com/daydemir/stoarama/backend/internal/utic"
)

const (
	uticProvider       = "UTIC"
	uticReviewActor    = "stoaramactl korea utic"
	uticRefreshKind    = "utic_refresh_frame"
	uticLocationSource = "utic_open_data"
)

type koreaUTICReport struct {
	StartedAt  time.Time     `json:"started_at"`
	FinishedAt time.Time     `json:"finished_at"`
	Endpoint   string        `json:"endpoint"`
	Count      int           `json:"count"`
	Cameras    []utic.Camera `json:"cameras,omitempty"`
}

type koreaUTICIngestItem struct {
	CameraID    string `json:"camera_id"`
	Name        string `json:"name"`
	Status      string `json:"status"`
	CandidateID int64  `json:"candidate_id,omitempty"`
	StreamID    int64  `json:"stream_id,omitempty"`
	Reason      string `json:"reason,omitempty"`
}

type koreaUTICIngestReport struct {
	StartedAt      time.Time             `json:"started_at"`
	FinishedAt     time.Time             `json:"finished_at"`
	Endpoint       string                `json:"endpoint"`
	DryRun         bool                  `json:"dry_run"`
	AutoImport     bool                  `json:"auto_import"`
	Discovered     int                   `json:"discovered"`
	ProcessedCount int                   `json:"processed_count"`
	ImportedCount  int                   `json:"imported_count"`
	UpsertedCount  int                   `json:"upserted_count"`
	Items          []koreaUTICIngestItem `json:"items"`
}

type koreaUTICFrameTarget struct {
	Stream model.Stream `json:"stream"`
}

type koreaUTICFrameResult struct {
	StreamID       int64     `json:"stream_id"`
	Slug           string    `json:"slug"`
	Provider       string    `json:"provider"`
	Status         string    `json:"status"`
	Reason         string    `json:"reason,omitempty"`
	CaptureType    string    `json:"capture_type"`
	ExecutionClass string    `json:"execution_class"`
	EffectiveMode  string    `json:"effective_mode"`
	ResolvedURL    string    `json:"resolved_url,omitempty"`
	CapturedAt     time.Time `json:"captured_at,omitempty"`
	Width          int       `json:"width,omitempty"`
	Height         int       `json:"height,omitempty"`
	SizeBytes      int64     `json:"size_bytes,omitempty"`
}

type koreaUTICFrameReport struct {
	StartedAt      time.Time              `json:"started_at"`
	FinishedAt     time.Time              `json:"finished_at"`
	TargetCount    int                    `json:"target_count"`
	ProcessedCount int                    `json:"processed_count"`
	SucceededCount int                    `json:"succeeded_count"`
	FailedCount    int                    `json:"failed_count"`
	DryRunCount    int                    `json:"dry_run_count"`
	DryRun         bool                   `json:"dry_run"`
	Items          []koreaUTICFrameResult `json:"items"`
}

func runKoreaUTIC(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 || args[0] == "-h" || args[0] == "--help" {
		fmt.Println("usage: stoaramactl korea utic <scrape|ingest|refresh-frames>")
		return
	}
	switch args[0] {
	case "scrape":
		runKoreaUTICScrape(ctx, args[1:])
	case "ingest":
		runKoreaUTICIngest(ctx, cfg, args[1:])
	case "refresh-frames":
		runKoreaUTICRefreshFrames(ctx, cfg, args[1:])
	default:
		log.Fatalf("unknown korea utic subcommand: %s", args[0])
	}
}

func runKoreaUTICScrape(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("korea utic scrape", flag.ExitOnError)
	endpoint := fs.String("api-url", utic.DefaultEndpoint, "UTIC CCTV API URL; {key} is replaced when present")
	serviceKey := fs.String("service-key", strings.TrimSpace(os.Getenv("UTIC_OPEN_DATA_KEY")), "UTIC open data service key")
	out := fs.String("out", "", "optional JSON report path")
	asJSON := fs.Bool("json", false, "print JSON")
	_ = fs.Parse(args)
	cameras := mustFetchUTICCameras(ctx, *endpoint, *serviceKey)
	report := koreaUTICReport{
		StartedAt:  time.Now().UTC(),
		FinishedAt: time.Now().UTC(),
		Endpoint:   strings.TrimSpace(*endpoint),
		Count:      len(cameras),
		Cameras:    cameras,
	}
	writeOptionalJSONReport(*out, report)
	if *asJSON {
		printJSON(report)
		return
	}
	fmt.Printf("utic scrape complete: cameras=%d endpoint=%s\n", report.Count, report.Endpoint)
}

func runKoreaUTICIngest(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("korea utic ingest", flag.ExitOnError)
	endpoint := fs.String("api-url", utic.DefaultEndpoint, "UTIC CCTV API URL; {key} is replaced when present")
	serviceKey := fs.String("service-key", strings.TrimSpace(os.Getenv("UTIC_OPEN_DATA_KEY")), "UTIC open data service key")
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	apiToken := fs.String("api-token", cfg.APIToken, "service token")
	autoImport := fs.Bool("auto-import", true, "auto-accept and import source candidates")
	dryRun := fs.Bool("dry-run", false, "discover and validate without API writes")
	limit := fs.Int("limit", 0, "maximum cameras to ingest (0=unlimited)")
	reportPath := fs.String("report-json", "", "optional JSON report path")
	asJSON := fs.Bool("json", false, "print JSON")
	_ = fs.Parse(args)
	if !*dryRun && strings.TrimSpace(*apiToken) == "" {
		log.Fatalf("--api-token is required")
	}
	if *limit < 0 {
		log.Fatalf("--limit must be >= 0")
	}
	cameras := mustFetchUTICCameras(ctx, *endpoint, *serviceKey)
	if *limit > 0 && len(cameras) > *limit {
		cameras = cameras[:*limit]
	}
	report := koreaUTICIngestReport{
		StartedAt:  time.Now().UTC(),
		Endpoint:   strings.TrimSpace(*endpoint),
		DryRun:     *dryRun,
		AutoImport: *autoImport,
		Discovered: len(cameras),
		Items:      make([]koreaUTICIngestItem, 0, len(cameras)),
	}
	for _, camera := range cameras {
		item := ingestUTICCamera(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), camera, *autoImport, *dryRun)
		report.ProcessedCount++
		if item.Status == "imported" {
			report.ImportedCount++
		}
		if item.Status == "upserted" {
			report.UpsertedCount++
		}
		report.Items = append(report.Items, item)
		if !*asJSON {
			fmt.Printf("camera_id=%s status=%s candidate_id=%d stream_id=%d name=%q\n", item.CameraID, item.Status, item.CandidateID, item.StreamID, item.Name)
		}
	}
	report.FinishedAt = time.Now().UTC()
	writeOptionalJSONReport(*reportPath, report)
	if *asJSON {
		printJSON(report)
		return
	}
	fmt.Printf("utic ingest complete: discovered=%d processed=%d upserted=%d imported=%d dry_run=%t\n", report.Discovered, report.ProcessedCount, report.UpsertedCount, report.ImportedCount, report.DryRun)
}

func runKoreaUTICRefreshFrames(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("korea utic refresh-frames", flag.ExitOnError)
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	apiToken := fs.String("api-token", cfg.APIToken, "service token")
	limit := fs.Int("limit", 0, "maximum streams to process (0=unlimited)")
	concurrency := fs.Int("concurrency", 4, "parallel stream workers")
	timeoutSec := fs.Int("timeout-sec", 90, "per-stream resolution/capture timeout seconds")
	allowFailures := fs.Bool("allow-failures", false, "exit zero when some streams fail")
	dryRun := fs.Bool("dry-run", false, "capture frames without ingesting them")
	reportPath := fs.String("report-json", "", "optional JSON report path")
	asJSON := fs.Bool("json", false, "print JSON")
	_ = fs.Parse(args)
	baseURL := strings.TrimSpace(*backendAPIURL)
	token := strings.TrimSpace(*apiToken)
	if baseURL == "" {
		log.Fatalf("--backend-api-url is required")
	}
	if token == "" {
		log.Fatalf("--api-token is required")
	}
	if *limit < 0 {
		log.Fatalf("--limit must be >= 0")
	}
	if *concurrency <= 0 {
		log.Fatalf("--concurrency must be > 0")
	}
	if *timeoutSec <= 0 {
		log.Fatalf("--timeout-sec must be > 0")
	}
	targets, err := loadKoreaUTICFrameTargets(ctx, baseURL, token, *limit)
	if err != nil {
		log.Fatalf("load UTIC frame targets: %v", err)
	}
	client, err := captureapi.NewClient(captureapi.ClientConfig{BaseURL: baseURL, APIToken: token})
	if err != nil {
		log.Fatalf("init capture api client: %v", err)
	}
	registry, err := capture.NewDefaultRegistry()
	if err != nil {
		log.Fatalf("init capture registry: %v", err)
	}
	report := processKoreaUTICFrameTargets(ctx, registry, client, targets, *concurrency, time.Duration(*timeoutSec)*time.Second, *dryRun, *asJSON)
	writeOptionalJSONReport(*reportPath, report)
	if *asJSON {
		printJSON(report)
	} else {
		fmt.Printf("utic refresh frames complete: targets=%d processed=%d success=%d failed=%d dry_run=%d dry_run_mode=%t\n", report.TargetCount, report.ProcessedCount, report.SucceededCount, report.FailedCount, report.DryRunCount, report.DryRun)
	}
	if report.FailedCount > 0 && !*allowFailures {
		os.Exit(1)
	}
}

func mustFetchUTICCameras(ctx context.Context, endpoint string, serviceKey string) []utic.Camera {
	cameras, err := utic.FetchCameras(ctx, utic.ClientConfig{
		Endpoint:   strings.TrimSpace(endpoint),
		ServiceKey: strings.TrimSpace(serviceKey),
	})
	if err != nil {
		log.Fatalf("fetch UTIC cameras: %v", err)
	}
	return cameras
}

func ingestUTICCamera(ctx context.Context, baseURL string, apiToken string, camera utic.Camera, autoImport bool, dryRun bool) koreaUTICIngestItem {
	item := koreaUTICIngestItem{CameraID: camera.ID, Name: camera.Name}
	payload := uticCandidatePayload(camera)
	if dryRun {
		item.Status = "dry_run"
		return item
	}
	candidate := mustAPIRequest(ctx, http.MethodPost, baseURL, apiToken, "/api/v1/source-candidates", payload)
	item.CandidateID = int64FromAny(candidate["id"])
	if !autoImport {
		item.Status = "upserted"
		return item
	}
	importPayload := map[string]any{
		"reviewer":             uticReviewActor,
		"review_reason":        "official UTIC open data CCTV listing",
		"review_metadata_json": map[string]any{"source": "utic_open_data"},
		"import":               uticImportPayload(camera),
	}
	imported := mustAPIRequest(ctx, http.MethodPost, baseURL, apiToken, fmt.Sprintf("/api/v1/source-candidates/%d/auto-import", item.CandidateID), importPayload)
	stream := asMap(imported["stream"])
	item.StreamID = int64FromAny(stream["id"])
	item.Status = "imported"
	return item
}

func uticCandidatePayload(camera utic.Camera) map[string]any {
	return map[string]any{
		"provider":        uticProvider,
		"external_id":     camera.ID,
		"source_family":   capture.SourceFamilyVideoManifest,
		"capture_type":    capture.CaptureTypeHLS,
		"source_url":      camera.StreamURL,
		"source_page_url": utic.SourcePageURL,
		"title":           camera.Name,
		"slug":            slugifyForCLI(uticProvider + "-" + camera.ID),
		"metadata_json":   uticMetadata(camera),
	}
}

func uticImportPayload(camera utic.Camera) map[string]any {
	return map[string]any{
		"provider":              uticProvider,
		"external_id":           camera.ID,
		"name":                  camera.Name,
		"slug":                  slugifyForCLI(uticProvider + "-" + camera.ID),
		"source_url":            camera.StreamURL,
		"source_page_url":       utic.SourcePageURL,
		"source_family":         capture.SourceFamilyVideoManifest,
		"capture_type":          capture.CaptureTypeHLS,
		"execution_class":       capture.ExecutionClassVideoLive,
		"tags":                  []string{"south-korea", "utic", "police"},
		"location_text":         camera.Name,
		"location_country":      "South Korea",
		"location_country_code": "KR",
		"location_city":         "Seoul",
		"location_source":       uticLocationSource,
		"metadata_json":         uticMetadata(camera),
	}
}

func uticMetadata(camera utic.Camera) map[string]any {
	meta := map[string]any{
		"source_family":      "utic",
		"upstream_owner":     "Korean National Police Agency / Road Traffic Authority UTIC",
		"official_source":    utic.SourcePageURL,
		"utic_id":            camera.ID,
		"utic_name":          camera.Name,
		"utic_raw":           camera.Raw,
		"discovery_provider": uticProvider,
	}
	if camera.Kind != "" {
		meta["utic_kind"] = camera.Kind
	}
	if camera.Format != "" {
		meta["utic_format"] = camera.Format
	}
	if camera.Lat != nil {
		meta["lat"] = *camera.Lat
	}
	if camera.Lon != nil {
		meta["lon"] = *camera.Lon
	}
	return meta
}

func loadKoreaUTICFrameTargets(ctx context.Context, baseURL string, apiToken string, limit int) ([]koreaUTICFrameTarget, error) {
	const pageSize = 500
	out := make([]koreaUTICFrameTarget, 0, 512)
	offset := 0
	for {
		remaining := pageSize
		if limit > 0 && limit-len(out) < remaining {
			remaining = limit - len(out)
		}
		if remaining <= 0 {
			break
		}
		q := url.Values{}
		q.Set("include_image_urls", "false")
		q.Set("sort_by", "id")
		q.Set("sort_dir", "asc")
		q.Set("korea_family", "utic")
		q.Set("limit", fmt.Sprint(remaining))
		q.Set("offset", fmt.Sprint(offset))
		payload := mustAPIGet(ctx, baseURL, apiToken, "/api/v1/dashboard/streams?"+q.Encode())
		items, _ := payload["items"].([]any)
		if len(items) == 0 {
			break
		}
		for _, raw := range items {
			streamMap := asMap(asMap(raw)["stream"])
			var stream model.Stream
			if err := decodeAnyJSON(streamMap, &stream); err != nil {
				return nil, fmt.Errorf("decode dashboard stream item: %w", err)
			}
			out = append(out, koreaUTICFrameTarget{Stream: stream})
			if limit > 0 && len(out) >= limit {
				return out, nil
			}
		}
		if len(items) < remaining {
			break
		}
		offset += len(items)
	}
	return out, nil
}

func processKoreaUTICFrameTargets(ctx context.Context, registry *capture.Registry, client *captureapi.Client, targets []koreaUTICFrameTarget, concurrency int, timeout time.Duration, dryRun bool, asJSON bool) koreaUTICFrameReport {
	report := koreaUTICFrameReport{
		StartedAt:   time.Now().UTC(),
		TargetCount: len(targets),
		DryRun:      dryRun,
		Items:       make([]koreaUTICFrameResult, 0, len(targets)),
	}
	workCh := make(chan koreaUTICFrameTarget)
	resCh := make(chan koreaUTICFrameResult, len(targets))
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for target := range workCh {
				resCh <- processKoreaUTICFrameTarget(ctx, registry, client, target, timeout, dryRun)
			}
		}()
	}
	go func() {
		defer close(workCh)
		for _, target := range targets {
			workCh <- target
		}
	}()
	go func() {
		wg.Wait()
		close(resCh)
	}()
	for res := range resCh {
		report.ProcessedCount++
		switch res.Status {
		case "success":
			report.SucceededCount++
		case "dry_run":
			report.DryRunCount++
		default:
			report.FailedCount++
		}
		report.Items = append(report.Items, res)
		if !asJSON {
			fmt.Printf("stream_id=%d slug=%s status=%s mode=%s reason=%s\n", res.StreamID, res.Slug, res.Status, res.EffectiveMode, res.Reason)
		}
	}
	report.FinishedAt = time.Now().UTC()
	return report
}

func processKoreaUTICFrameTarget(ctx context.Context, registry *capture.Registry, client *captureapi.Client, target koreaUTICFrameTarget, timeout time.Duration, dryRun bool) koreaUTICFrameResult {
	stream := target.Stream
	result := koreaUTICFrameResult{
		StreamID:       stream.ID,
		Slug:           strings.TrimSpace(stream.Slug),
		Provider:       strings.TrimSpace(stream.Provider),
		CaptureType:    stream.CaptureType,
		ExecutionClass: stream.ExecutionClass,
	}
	if stream.ID <= 0 {
		result.Status = "error"
		result.Reason = "invalid stream id"
		return result
	}
	mode := backfillMissingEffectiveMode(stream)
	result.EffectiveMode = string(mode)
	spec := capture.StreamSpec{
		ID:            stream.ID,
		Provider:      stream.Provider,
		StreamURL:     stream.SourceURL,
		SourcePageURL: stream.SourcePageURL,
		CaptureMode:   mode,
		CaptureConfig: stream.ExecutionConfigJSON,
		TargetFPS:     capture.SegmentTargetFPS,
	}
	effective := capture.EffectiveMode(spec)
	adapter, ok := registry.Get(effective)
	if !ok {
		result.Status = "error"
		result.Reason = fmt.Sprintf("adapter not found for mode %s", effective)
		return result
	}
	resolveCtx, cancelResolve := context.WithTimeout(ctx, timeout)
	resolved, err := adapter.Resolve(resolveCtx, spec)
	cancelResolve()
	if err != nil {
		result.Status = "error"
		result.Reason = fmt.Sprintf("resolve capture source: %v", err)
		return result
	}
	result.ResolvedURL = resolved.URL
	capCtx, cancelCap := context.WithTimeout(ctx, timeout)
	frame, err := capture.CaptureFrame(capCtx, resolved.URL)
	cancelCap()
	if err != nil {
		result.Status = "error"
		result.Reason = fmt.Sprintf("capture frame: %v", err)
		return result
	}
	result.CapturedAt = time.Now().UTC()
	result.Width = frame.Width
	result.Height = frame.Height
	result.SizeBytes = frame.SizeBytes
	if dryRun {
		result.Status = "dry_run"
		return result
	}
	ingestCtx, cancelIngest := context.WithTimeout(ctx, timeout)
	defer cancelIngest()
	if err := client.IngestSuccess(ingestCtx, captureapi.IngestSuccessRequest{
		StreamID:           stream.ID,
		CapturedAt:         result.CapturedAt,
		SourceKind:         uticRefreshKind,
		EffectiveMode:      effective,
		ResolvedURL:        resolved.URL,
		MIMEType:           frame.MIMEType,
		FrameBytes:         frame.Bytes,
		RecordingHeartbeat: false,
	}); err != nil {
		result.Status = "error"
		result.Reason = fmt.Sprintf("ingest capture success: %v", err)
		return result
	}
	result.Status = "success"
	return result
}

func writeOptionalJSONReport(path string, payload any) {
	if strings.TrimSpace(path) == "" {
		return
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		log.Fatalf("marshal report json: %v", err)
	}
	if err := os.WriteFile(strings.TrimSpace(path), append(raw, '\n'), 0o644); err != nil {
		log.Fatalf("write report json: %v", err)
	}
}
