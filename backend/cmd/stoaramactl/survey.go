package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/recordingapi"
	"github.com/daydemir/stoarama/backend/internal/survey"
)

// dailyGateHour returns a deterministic per-day UTC hour in [0,24) for the given
// date, so an hourly cron can run the full sweep at a different time each day.
func dailyGateHour(day time.Time) int {
	d := day.UTC()
	seed := int64(d.Year())*10000 + int64(d.Month())*100 + int64(d.Day())
	return rand.New(rand.NewSource(seed)).Intn(24)
}

func runSurvey(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 {
		log.Fatalf("usage: stoaramactl survey <run-once|coverage|delete-stream-captures> ...")
	}
	switch args[0] {
	case "run-once":
		runSurveyRunOnce(ctx, cfg, args[1:])
	case "relay-worker":
		runSurveyRelayWorker(ctx, cfg, args[1:])
	case "coverage":
		runSurveyCoverage(ctx, cfg, args[1:])
	case "delete-stream-captures":
		runSurveyDeleteStreamCaptures(ctx, cfg, args[1:])
	case "detect-image":
		runSurveyDetectImage(ctx, cfg, args[1:])
	case "download-model":
		runSurveyDownloadModel(ctx, cfg, args[1:])
	default:
		log.Fatalf("unknown survey subcommand: %s", args[0])
	}
}

func runSurveyRelayWorker(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("survey relay-worker", flag.ExitOnError)
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	nodeToken := fs.String("node-token", "", "relay node token")
	concurrency := fs.Int("concurrency", 1, "targets to lease per poll")
	pollSec := fs.Int("poll-sec", 30, "sleep seconds when no target is available")
	durationSec := fs.Int("duration", 0, "stop after this many seconds (0 = run until interrupted)")
	resolveTimeoutSec := fs.Int("resolve-timeout-sec", 60, "per-stream resolve timeout seconds")
	captureTimeoutSec := fs.Int("capture-timeout-sec", 60, "per-stream one-frame capture timeout seconds")
	detect := fs.Bool("detect", false, "run yolo11x detection locally and submit counts")
	detectSampleRate := fs.Float64("detect-sample-rate", cfg.SurveyDetectSampleRate, "probability [0,1] a captured frame is sampled for detection")
	_ = fs.Parse(args)
	if strings.TrimSpace(*nodeToken) == "" {
		log.Fatalf("--node-token is required")
	}
	if *concurrency <= 0 || *pollSec <= 0 || *resolveTimeoutSec <= 0 || *captureTimeoutSec <= 0 {
		log.Fatalf("--concurrency, --poll-sec, --resolve-timeout-sec, and --capture-timeout-sec must be > 0")
	}
	if *detect && (*detectSampleRate <= 0 || *detectSampleRate > 1) {
		log.Fatalf("--detect-sample-rate must be in (0,1] when --detect is set")
	}
	if *durationSec > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(*durationSec)*time.Second)
		defer cancel()
	}
	registry, err := capture.NewDefaultRegistry()
	if err != nil {
		log.Fatalf("init capture registry: %v", err)
	}
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: *backendAPIURL, NodeToken: *nodeToken})
	if err != nil {
		log.Fatalf("init relay survey client: %v", err)
	}
	var det survey.Detector
	if *detect {
		d, derr := newSurveyDetector(cfg)
		if derr != nil {
			log.Fatalf("init survey detector: %v", derr)
		}
		defer d.Close()
		det = surveyDetectorAdapter{d: d}
	}
	caps := map[string]any{
		"survey_enabled":        true,
		"survey_detect_enabled": *detect,
		"survey_max_active":     *concurrency,
	}
	if err := startSurveyRelayHeartbeat(ctx, client, caps, 30*time.Second); err != nil {
		log.Fatalf("survey relay heartbeat: %v", err)
	}
	poll := time.Duration(*pollSec) * time.Second
	for ctx.Err() == nil {
		lease, err := client.LeaseSurveyTargets(ctx, *concurrency)
		if err != nil {
			log.Printf("survey relay lease failed: %v", err)
			sleepContext(ctx, poll)
			continue
		}
		if len(lease.Targets) == 0 {
			sleepContext(ctx, poll)
			continue
		}
		for _, target := range lease.Targets {
			if err := runSurveyRelayTarget(ctx, client, registry, target, lease.Day, time.Duration(*resolveTimeoutSec)*time.Second, time.Duration(*captureTimeoutSec)*time.Second, det, *detectSampleRate); err != nil {
				log.Printf("survey relay target stream=%d failed: %v", target.ID, err)
			}
		}
	}
}

func startSurveyRelayHeartbeat(ctx context.Context, client *recordingapi.Client, caps map[string]any, every time.Duration) error {
	if err := client.NodeHeartbeat(ctx, caps); err != nil {
		return err
	}
	go func() {
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := client.NodeHeartbeat(ctx, caps); err != nil {
					log.Printf("survey relay heartbeat failed: %v", err)
				}
			}
		}
	}()
	return nil
}

func runSurveyRelayTarget(ctx context.Context, client *recordingapi.Client, registry *capture.Registry, target survey.Target, day string, resolveTimeout, captureTimeout time.Duration, det survey.Detector, sampleRate float64) error {
	frame, err := survey.CaptureFrame(ctx, registry, target, resolveTimeout, captureTimeout)
	if err != nil {
		_ = client.FailSurveyTarget(ctx, target, err)
		return err
	}
	var result *survey.DetectionResult
	if det != nil && rand.Float64() < sampleRate {
		counts, ms, derr := det.Detect(frame.Bytes)
		if derr != nil {
			_ = client.FailSurveyTarget(ctx, target, derr)
			return derr
		}
		result = &survey.DetectionResult{
			PipelineVersion: det.PipelineVersion(),
			ConfThreshold:   det.ConfThreshold(),
			Imgsz:           det.Imgsz(),
			Counts:          counts,
			DetectMs:        ms,
		}
	}
	if err := client.CompleteSurveyTarget(ctx, target, day, frame, result); err != nil {
		return err
	}
	return nil
}

func sleepContext(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

// runSurveyRunOnce sweeps ALL non-pruned streams a single time so a full survey
// of the whole catalog can be completed before relying on the daily cadence. It
// reuses the same capture+persist code as the embedded scheduler.
func runSurveyRunOnce(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("survey run-once", flag.ExitOnError)
	concurrency := fs.Int("concurrency", 4, "bounded capture concurrency")
	resolveTimeoutSec := fs.Int("resolve-timeout-sec", 60, "per-stream resolve timeout seconds")
	captureTimeoutSec := fs.Int("capture-timeout-sec", 60, "per-stream one-frame capture timeout seconds")
	limit := fs.Int("limit", 0, "max streams to survey this run (0 = all non-pruned); useful for verifying a small sample")
	streamIDs := fs.String("stream-ids", "", "comma-separated stream ids to survey (empty = all selected targets); useful for verifying specific streams")
	dailyGate := fs.Bool("daily-gate", false, "for an hourly cron: only run during one per-day deterministic random UTC hour, skip otherwise (gives a different capture time each day)")
	detect := fs.Bool("detect", false, "run inline yolo11x detection on a randomized per-stream sample of captured frames, writing survey_detections rows")
	detectSampleRate := fs.Float64("detect-sample-rate", cfg.SurveyDetectSampleRate, "probability [0,1] a captured frame is sampled for detection (only with --detect)")
	asJSON := fs.Bool("json", false, "print JSON")
	_ = fs.Parse(args)
	if *concurrency <= 0 || *resolveTimeoutSec <= 0 || *captureTimeoutSec <= 0 {
		log.Fatalf("--concurrency, --resolve-timeout-sec, --capture-timeout-sec must all be > 0")
	}
	if *detect && (*detectSampleRate <= 0 || *detectSampleRate > 1) {
		log.Fatalf("--detect-sample-rate must be in (0,1] when --detect is set")
	}
	if *dailyGate {
		now := time.Now().UTC()
		chosen := dailyGateHour(now)
		if now.Hour() != chosen {
			fmt.Printf("survey run-once: skipped (daily-gate chosen_hour=%d current_hour=%d)\n", chosen, now.Hour())
			return
		}
	}
	registry, err := capture.NewDefaultRegistry()
	if err != nil {
		log.Fatalf("init capture registry: %v", err)
	}
	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	r2c := mustArchiveR2Client(ctx, cfg)

	// Load the yolo11x ONNX detector ONCE and reuse it across the whole sweep.
	// Fail-fast: with --detect set, a missing model or onnxruntime library aborts
	// the run rather than silently capturing without detection.
	var det survey.Detector
	if *detect {
		d, derr := newSurveyDetector(cfg)
		if derr != nil {
			log.Fatalf("init survey detector: %v", derr)
		}
		defer d.Close()
		det = surveyDetectorAdapter{d: d}
		log.Printf("survey run-once: detection ON (pipeline=%s conf=%.2f imgsz=%d sample_rate=%.3f)",
			d.PipelineVersion(), d.ConfThreshold(), d.Imgsz(), *detectSampleRate)
	}

	targets, err := survey.SelectTargets(ctx, pool)
	if err != nil {
		log.Fatalf("select survey targets: %v", err)
	}
	if ids := parseStreamIDs(*streamIDs); len(ids) > 0 {
		filtered := targets[:0:0]
		for _, t := range targets {
			if ids[t.ID] {
				filtered = append(filtered, t)
			}
		}
		targets = filtered
	}
	if *limit > 0 && len(targets) > *limit {
		targets = targets[:*limit]
	}
	day := time.Now().UTC()
	sampleRate := 0.0
	if *detect {
		sampleRate = *detectSampleRate
	}
	res := survey.RunOnce(ctx, pool, r2c, registry, targets, day,
		*concurrency,
		time.Duration(*resolveTimeoutSec)*time.Second,
		time.Duration(*captureTimeoutSec)*time.Second,
		det, sampleRate,
		func(streamID int64, err error) {
			log.Printf("survey run-once: stream %d failed: %v", streamID, err)
		},
	)
	if *asJSON {
		printJSON(map[string]any{
			"day":     day.Format("2006-01-02"),
			"total":   res.Total,
			"success": res.Success,
			"skipped": res.Skipped,
			"failed":  res.Failed,
		})
		return
	}
	fmt.Printf("survey run-once day=%s total=%d success=%d skipped=%d failed=%d\n",
		day.Format("2006-01-02"), res.Total, res.Success, res.Skipped, res.Failed)
}

// parseStreamIDs parses a comma-separated list of stream ids into a set,
// skipping blanks and non-numeric entries. An empty/blank input yields nil so
// the caller surveys all selected targets.
func parseStreamIDs(raw string) map[int64]bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	ids := make(map[int64]bool)
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, err := strconv.ParseInt(part, 10, 64)
		if err != nil {
			log.Fatalf("invalid --stream-ids entry %q: %v", part, err)
		}
		ids[id] = true
	}
	return ids
}

// runSurveyCoverage reports how many non-pruned streams have at least one survey
// frame ever and at least one for today's UTC date, so we know when the full
// pass is complete.
func runSurveyCoverage(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("survey coverage", flag.ExitOnError)
	asJSON := fs.Bool("json", false, "print JSON")
	_ = fs.Parse(args)
	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	day := time.Now().UTC()
	c, err := survey.ComputeCoverage(ctx, pool, day)
	if err != nil {
		log.Fatalf("compute coverage: %v", err)
	}
	if *asJSON {
		printJSON(map[string]any{
			"day":               day.Format("2006-01-02"),
			"non_pruned_total":  c.NonPrunedTotal,
			"with_any_survey":   c.WithAnySurvey,
			"with_today_survey": c.WithTodaySurvey,
		})
		return
	}
	fmt.Printf("survey coverage day=%s non_pruned_total=%d with_any_survey=%d with_today_survey=%d\n",
		day.Format("2006-01-02"), c.NonPrunedTotal, c.WithAnySurvey, c.WithTodaySurvey)
}

// runSurveyDeleteStreamCaptures deletes a stream's survey objects from R2 and
// the orphaned media_objects + survey frame rows. A DB ON DELETE CASCADE removes
// frame rows but never deletes R2 bytes and orphans media_objects, so this is
// the explicit cleanup path.
func runSurveyDeleteStreamCaptures(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("survey delete-stream-captures", flag.ExitOnError)
	id := fs.Int64("id", 0, "stream id whose survey captures to delete")
	apply := fs.Bool("apply", false, "perform deletion (required)")
	_ = fs.Parse(args)
	if *id <= 0 {
		log.Fatalf("--id is required")
	}
	if !*apply {
		log.Fatalf("--apply is required for delete-stream-captures")
	}
	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	r2c := mustArchiveR2Client(ctx, cfg)
	deleted, err := survey.DeleteStreamCaptures(ctx, pool, r2c, *id)
	if err != nil {
		log.Fatalf("delete survey captures for stream %d: %v", *id, err)
	}
	fmt.Printf("deleted survey captures stream=%d r2_objects=%d\n", *id, deleted)
}
