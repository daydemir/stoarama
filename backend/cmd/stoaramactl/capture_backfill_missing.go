package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"sort"
	"strconv"
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
	CapturedAt      time.Time `json:"captured_at,omitempty"`
	Width           int       `json:"width,omitempty"`
	Height          int       `json:"height,omitempty"`
	SizeBytes       int64     `json:"size_bytes,omitempty"`
	CapturesSuccess int64     `json:"captures_success"`
	BackfillReason  string    `json:"backfill_reason,omitempty"`
}

type streamIDFlags []int64

const maxFrameRefreshConcurrency = 4
const maxExplicitFrameRefreshStreams = 50

func (v *streamIDFlags) String() string { return fmt.Sprint([]int64(*v)) }
func (v *streamIDFlags) Set(raw string) error {
	id, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || id <= 0 {
		return fmt.Errorf("stream id must be a positive integer")
	}
	*v = append(*v, id)
	return nil
}

func validateCaptureBackfillOptions(limit, concurrency int, ids []int64) error {
	if limit < 0 {
		return fmt.Errorf("--limit must be >= 0")
	}
	if concurrency < 1 || concurrency > maxFrameRefreshConcurrency {
		return fmt.Errorf("--concurrency must be 1..%d", maxFrameRefreshConcurrency)
	}
	if len(ids) > maxExplicitFrameRefreshStreams {
		return fmt.Errorf("at most %d --stream-id values are allowed", maxExplicitFrameRefreshStreams)
	}
	if len(ids) > 0 && limit > 0 {
		return fmt.Errorf("--limit cannot be combined with --stream-id")
	}
	seen := make(map[int64]bool, len(ids))
	for _, id := range ids {
		if id <= 0 {
			return fmt.Errorf("stream ids must be positive")
		}
		if seen[id] {
			return fmt.Errorf("duplicate --stream-id %d", id)
		}
		seen[id] = true
	}
	return nil
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
	concurrency := fs.Int("concurrency", 1, "parallel stream workers")
	var streamIDs streamIDFlags
	fs.Var(&streamIDs, "stream-id", "explicit stream id to refresh (repeatable; includes streams with existing frames)")
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
	if err := validateCaptureBackfillOptions(*limit, *concurrency, streamIDs); err != nil {
		log.Fatal(err)
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

	targets, err := loadCaptureBackfillMissingTargets(ctx, baseURL, token, *limit, streamIDs)
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
				fmt.Printf("stream_id=%d slug=%s status=success mode=%s\n", res.StreamID, res.Slug, res.EffectiveMode)
			case "dry_run":
				fmt.Printf("stream_id=%d slug=%s status=dry_run mode=%s\n", res.StreamID, res.Slug, res.EffectiveMode)
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
		result.Reason = "resolve capture source failed"
		return result
	}
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
		result.Reason = "capture frame failed"
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
		result.Reason = "ingest capture success failed"
		return result
	}
	result.Status = "success"
	return result
}

func loadCaptureBackfillMissingTargets(ctx context.Context, baseURL, apiToken string, limit int, explicitIDs []int64) ([]captureBackfillMissingCandidate, error) {
	const pageSize = 500
	out := make([]captureBackfillMissingCandidate, 0, 512)
	explicitSelection := len(explicitIDs) > 0
	wanted := make(map[int64]bool, len(explicitIDs))
	for _, id := range explicitIDs {
		if wanted[id] {
			return nil, fmt.Errorf("duplicate --stream-id %d", id)
		}
		wanted[id] = true
	}
	offset := 0
	for {
		remaining := pageSize
		if len(wanted) == 0 && limit > 0 && limit-len(out) < remaining {
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
			explicit := wanted[stream.ID]
			if explicitSelection && !explicit {
				continue
			}
			if !explicit && capturesSuccess > 0 {
				continue
			}
			reason := "zero_success"
			if explicit {
				reason = "explicit_refresh"
			}
			out = append(out, captureBackfillMissingCandidate{
				Stream:          stream,
				CapturesSuccess: capturesSuccess,
				BackfillReason:  reason,
			})
			delete(wanted, stream.ID)
			if explicitSelection && len(wanted) == 0 {
				return out, nil
			}
			if limit > 0 && len(out) >= limit {
				return out, nil
			}
		}
		if len(items) < remaining || (len(explicitIDs) > 0 && len(wanted) == 0) {
			break
		}
		offset += len(items)
	}
	if len(wanted) > 0 {
		missing := make([]int64, 0, len(wanted))
		for id := range wanted {
			missing = append(missing, id)
		}
		sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
		return nil, fmt.Errorf("stream ids not found: %v", missing)
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
