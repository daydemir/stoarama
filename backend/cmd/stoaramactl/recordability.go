package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/recordability"
)

func runRecordability(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 {
		log.Fatalf("usage: stoaramactl recordability <run-once> ...")
	}
	switch args[0] {
	case "run-once":
		runRecordabilityRunOnce(ctx, cfg, args[1:])
	default:
		log.Fatalf("unknown recordability subcommand: %s", args[0])
	}
}

// runRecordabilityRunOnce probes a small batch of untested/re-probeable streams
// from THIS host and records each verdict. It is EXECUTION-GATED behind
// STREAM_RECORDABILITY_PROBE_ENABLED (default off): with the flag off it refuses to
// probe, so no ffmpeg runs and nothing is spent. This host MUST be a DO egress path
// matching the recorder droplet pool (a Render host is invalid); the flag stays off
// until a confirmed DO-egress probe host exists.
func runRecordabilityRunOnce(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("recordability run-once", flag.ExitOnError)
	batch := fs.Int("batch", 1, "max streams to probe this run (decision #3: one or very few at a time)")
	windowSec := fs.Int("window-sec", 600, "real recording window seconds (~600 = 10min)")
	segmentSec := fs.Int("segment-sec", 60, "continuous segment seconds")
	probeHost := fs.String("probe-host", "", "audit label for the host running this probe (e.g. droplet id)")
	asJSON := fs.Bool("json", false, "print JSON")
	_ = fs.Parse(args)

	if !cfg.StreamRecordabilityProbeEnabled {
		log.Fatalf("recordability run-once: refusing to probe, STREAM_RECORDABILITY_PROBE_ENABLED is off (ship-dark). Enable it only on a confirmed DO-egress host.")
	}
	if *batch < 1 || *windowSec <= 0 || *segmentSec <= 0 {
		log.Fatalf("--batch must be >=1 and --window-sec/--segment-sec > 0")
	}

	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()

	targets, err := recordability.SelectTargets(ctx, pool, *batch)
	if err != nil {
		log.Fatalf("select recordability targets: %v", err)
	}
	res := recordability.RunOnce(ctx, pool, targets,
		time.Duration(*windowSec)*time.Second,
		time.Duration(*segmentSec)*time.Second,
		*probeHost,
		func(streamID int64, err error) {
			log.Printf("recordability run-once: stream %d failed: %v", streamID, err)
		},
	)
	if *asJSON {
		printJSON(map[string]any{
			"total":   res.Total,
			"ok":      res.OK,
			"blocked": res.Blocked,
			"other":   res.Other,
			"failed":  res.Failed,
		})
		return
	}
	fmt.Printf("recordability run-once total=%d ok=%d blocked=%d other=%d failed=%d\n",
		res.Total, res.OK, res.Blocked, res.Other, res.Failed)
}
