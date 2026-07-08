package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/captureapi"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/model"
)

type captureBackfillMissingCandidate struct {
	Stream          model.Stream `json:"stream"`
	CapturesSuccess int64        `json:"captures_success"`
	LatestFrameURL  string       `json:"latest_frame_url,omitempty"`
	BackfillReason  string       `json:"backfill_reason,omitempty"`
}

type captureBackfillMissingResult struct {
	StreamID        int64     `json:"stream_id"`
	Slug            string    `json:"slug"`
	Provider        string    `json:"provider"`
	Status          string    `json:"status"`
	Reason          string    `json:"reason,omitempty"`
	CaptureType     string    `json:"capture_type"`
	ExecutionClass  string    `json:"execution_class"`
	EffectiveMode   string    `json:"effective_mode"`
	ResolvedURL     string    `json:"resolved_url,omitempty"`
	CapturedAt      time.Time `json:"captured_at,omitempty"`
	Width           int       `json:"width,omitempty"`
	Height          int       `json:"height,omitempty"`
	SizeBytes       int64     `json:"size_bytes,omitempty"`
	CapturesSuccess int64     `json:"captures_success"`
	LatestFrameURL  string    `json:"latest_frame_url,omitempty"`
	BackfillReason  string    `json:"backfill_reason,omitempty"`
}

type captureBackfillMissingReport struct {
	StartedAt      time.Time                      `json:"started_at"`
	FinishedAt     time.Time                      `json:"finished_at"`
	TargetCount    int                            `json:"target_count"`
	ProcessedCount int                            `json:"processed_count"`
	SucceededCount int                            `json:"succeeded_count"`
	FailedCount    int                            `json:"failed_count"`
	DryRunCount    int                            `json:"dry_run_count"`
	DryRun         bool                           `json:"dry_run"`
	Items          []captureBackfillMissingResult `json:"items"`
}

func runCaptureBackfillMissing(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("capture backfill-missing", flag.ExitOnError)
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	apiToken := fs.String("api-token", cfg.APIToken, "service token")
	limit := fs.Int("limit", 0, "maximum streams to process (0=unlimited)")
	concurrency := fs.Int("concurrency", 4, "parallel stream workers")
	timeoutSec := fs.Int("timeout-sec", 90, "per-stream resolution/capture timeout seconds")
	dryRun := fs.Bool("dry-run", false, "print actions without ingesting frames")
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

	client, err := captureapi.NewClient(captureapi.ClientConfig{BaseURL: baseURL, APIToken: token})
	if err != nil {
		log.Fatalf("init capture api client: %v", err)
	}
	registry, err := capture.NewDefaultRegistry()
	if err != nil {
		log.Fatalf("init capture registry: %v", err)
	}

	targets, err := loadCaptureBackfillMissingTargets(ctx, baseURL, token, *limit)
	if err != nil {
		log.Fatalf("load backfill targets: %v", err)
	}

	report := captureBackfillMissingReport{
		StartedAt:   time.Now().UTC(),
		TargetCount: len(targets),
		DryRun:      *dryRun,
		Items:       make([]captureBackfillMissingResult, 0, len(targets)),
	}

	workCh := make(chan captureBackfillMissingCandidate)
	resCh := make(chan captureBackfillMissingResult, len(targets))
	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for target := range workCh {
				resCh <- processCaptureBackfillMissingTarget(ctx, registry, client, target, time.Duration(*timeoutSec)*time.Second, *dryRun)
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
		if res.Status == "success" {
			report.SucceededCount++
		} else if res.Status == "dry_run" {
			report.DryRunCount++
		} else {
			report.FailedCount++
		}
		report.Items = append(report.Items, res)
		if !*asJSON {
			switch res.Status {
			case "success":
				fmt.Printf("stream_id=%d slug=%s status=success mode=%s resolved_url=%s\n", res.StreamID, res.Slug, res.EffectiveMode, res.ResolvedURL)
			case "dry_run":
				fmt.Printf("stream_id=%d slug=%s status=dry_run mode=%s resolved_url=%s\n", res.StreamID, res.Slug, res.EffectiveMode, res.ResolvedURL)
			default:
				fmt.Printf("stream_id=%d slug=%s status=%s reason=%s\n", res.StreamID, res.Slug, res.Status, res.Reason)
			}
		}
	}

	report.FinishedAt = time.Now().UTC()
	if *asJSON {
		printJSON(report)
		return
	}
	fmt.Printf(
		"capture backfill missing complete: targets=%d processed=%d success=%d failed=%d dry_run=%d dry_run_mode=%t\n",
		report.TargetCount, report.ProcessedCount, report.SucceededCount, report.FailedCount, report.DryRunCount, report.DryRun,
	)
}

func processCaptureBackfillMissingTarget(
	ctx context.Context,
	registry *capture.Registry,
	client *captureapi.Client,
	target captureBackfillMissingCandidate,
	timeout time.Duration,
	dryRun bool,
) captureBackfillMissingResult {
	stream := target.Stream
	result := captureBackfillMissingResult{
		StreamID:        stream.ID,
		Slug:            strings.TrimSpace(stream.Slug),
		Provider:        strings.TrimSpace(stream.Provider),
		CapturesSuccess: target.CapturesSuccess,
		LatestFrameURL:  strings.TrimSpace(target.LatestFrameURL),
		BackfillReason:  strings.TrimSpace(target.BackfillReason),
	}
	if stream.ID <= 0 {
		result.Status = "error"
		result.Reason = "invalid stream id"
		return result
	}

	mode := backfillMissingEffectiveMode(stream)
	result.CaptureType = stream.CaptureType
	result.ExecutionClass = stream.ExecutionClass
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
	defer cancelCap()
	var frame capture.Frame
	if resolved.IsImage {
		frame, err = capture.CaptureFrame(capCtx, resolved.URL)
	} else {
		frame, err = capture.CaptureSingleFrameWithHeaders(capCtx, resolved.URL, "", resolved.InputHeaders)
	}
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
		SourceKind:         "backfill_missing_frame",
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

func loadCaptureBackfillMissingTargets(ctx context.Context, baseURL, apiToken string, limit int) ([]captureBackfillMissingCandidate, error) {
	const pageSize = 500
	out := make([]captureBackfillMissingCandidate, 0, 512)
	offset := 0
	for {
		remaining := pageSize
		if limit > 0 && limit-len(out) < remaining {
			remaining = limit - len(out)
		}
		if remaining <= 0 {
			break
		}
		payload := mustAPIGet(ctx, baseURL, apiToken, fmt.Sprintf("/api/v1/dashboard/streams?include_image_urls=false&sort_by=id&sort_dir=asc&limit=%d&offset=%d", remaining, offset))
		items, _ := payload["items"].([]any)
		if len(items) == 0 {
			break
		}
		for _, raw := range items {
			item := asMap(raw)
			streamMap := asMap(item["stream"])
			var stream model.Stream
			if err := decodeAnyJSON(streamMap, &stream); err != nil {
				return nil, fmt.Errorf("decode dashboard stream item: %w", err)
			}
			capturesSuccess := int64FromAny(item["captures_success"])
			if capturesSuccess > 0 {
				continue
			}
			out = append(out, captureBackfillMissingCandidate{
				Stream:          stream,
				CapturesSuccess: capturesSuccess,
				BackfillReason:  "zero_success",
			})
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

func backfillMissingEffectiveMode(stream model.Stream) capture.Mode {
	mode := capture.LegacyModeForStream(stream.CaptureType, stream.ExecutionClass)
	if mode == capture.ModeYouTubeRelay {
		return capture.ModeYouTubeLive
	}
	return mode
}

func decodeAnyJSON(src any, dst any) error {
	raw, err := json.Marshal(src)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, dst)
}
