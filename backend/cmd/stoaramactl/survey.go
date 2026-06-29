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
	case "coverage":
		runSurveyCoverage(ctx, cfg, args[1:])
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
	streamIDs := fs.String("stream-ids", "", "comma-separated stream ids to survey (empty = all selected targets); useful for verifying specific streams")
	dailyGate := fs.Bool("daily-gate", false, "for an hourly cron: only run during one per-day deterministic random UTC hour, skip otherwise (gives a different capture time each day)")
	asJSON := fs.Bool("json", false, "print JSON")
	_ = fs.Parse(args)
	if *concurrency <= 0 || *resolveTimeoutSec <= 0 || *captureTimeoutSec <= 0 {
		log.Fatalf("--concurrency, --resolve-timeout-sec, --capture-timeout-sec must all be > 0")
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
