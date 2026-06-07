package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/survey"
)

func runSurvey(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 {
		log.Fatalf("usage: stoaramactl survey <run-once|coverage|soft-prune|delete-stream-captures> ...")
	}
	switch args[0] {
	case "run-once":
		runSurveyRunOnce(ctx, cfg, args[1:])
	case "coverage":
		runSurveyCoverage(ctx, cfg, args[1:])
	case "soft-prune":
		runSurveySoftPrune(ctx, cfg, args[1:])
	case "delete-stream-captures":
		runSurveyDeleteStreamCaptures(ctx, cfg, args[1:])
	default:
		log.Fatalf("unknown survey subcommand: %s", args[0])
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
	asJSON := fs.Bool("json", false, "print JSON")
	_ = fs.Parse(args)
	if *concurrency <= 0 || *resolveTimeoutSec <= 0 || *captureTimeoutSec <= 0 {
		log.Fatalf("--concurrency, --resolve-timeout-sec, --capture-timeout-sec must all be > 0")
	}
	registry, err := capture.NewDefaultRegistry()
	if err != nil {
		log.Fatalf("init capture registry: %v", err)
	}
	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	r2c := mustArchiveR2Client(ctx, cfg)

	targets, err := survey.SelectTargets(ctx, pool)
	if err != nil {
		log.Fatalf("select survey targets: %v", err)
	}
	if *limit > 0 && len(targets) > *limit {
		targets = targets[:*limit]
	}
	day := time.Now().UTC()
	res := survey.RunOnce(ctx, pool, r2c, registry, targets, day,
		*concurrency,
		time.Duration(*resolveTimeoutSec)*time.Second,
		time.Duration(*captureTimeoutSec)*time.Second,
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

// runSurveySoftPrune disables a stream so it is hidden from the public catalog.
// Reversible: enabled=false, and excluded_flag=true when --exclude is set.
func runSurveySoftPrune(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("survey soft-prune", flag.ExitOnError)
	id := fs.Int64("id", 0, "stream id to soft-prune")
	exclude := fs.Bool("exclude", false, "also set excluded_flag=true")
	_ = fs.Parse(args)
	if *id <= 0 {
		log.Fatalf("--id is required")
	}
	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	updated, err := survey.SoftPrune(ctx, pool, *id, *exclude)
	if err != nil {
		log.Fatalf("soft-prune stream %d: %v", *id, err)
	}
	if !updated {
		log.Fatalf("stream %d not found", *id)
	}
	fmt.Printf("soft-pruned stream=%d exclude=%t\n", *id, *exclude)
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
