package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/recordability"
	"github.com/google/uuid"
)

func runRecordability(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 {
		log.Fatalf("usage: stoaramactl recordability <run-once> ...")
	}
	switch args[0] {
	case "run-once":
		runRecordabilityRunOnce(ctx, cfg, args[1:])
	case "run-targeted":
		runRecordabilityTargeted(ctx, cfg, args[1:])
	default:
		log.Fatalf("unknown recordability subcommand: %s", args[0])
	}
}

func runRecordabilityTargeted(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("recordability run-targeted", flag.ExitOnError)
	approvalRaw := fs.String("approval-id", "", "exact immutable campaign approval UUID")
	streamIDsRaw := fs.String("stream-ids", "", "exact comma-separated approved stream IDs")
	windowSec := fs.Int("window-sec", 600, "real recording window seconds")
	segmentSec := fs.Int("segment-sec", 60, "continuous segment seconds")
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	nodeToken := fs.String("node-token", strings.TrimSpace(os.Getenv("RECORDER_NODE_TOKEN")), "managed recorder node token")
	attempts := fs.Int("attempts", 2, "separate full probes per stream (admission requires 2)")
	asJSON := fs.Bool("json", false, "print JSON")
	_ = fs.Parse(args)
	if !cfg.StreamRecordabilityProbeEnabled {
		log.Fatal("recordability run-targeted: refusing to probe, STREAM_RECORDABILITY_PROBE_ENABLED is off")
	}
	approvalID, err := uuid.Parse(strings.TrimSpace(*approvalRaw))
	if err != nil {
		log.Fatalf("--approval-id must be a UUID: %v", err)
	}
	ids, err := parseInt64CSV(*streamIDsRaw)
	if err != nil || len(ids) == 0 {
		log.Fatalf("--stream-ids must contain unique positive integers: %v", err)
	}
	if *windowSec < 120 || *segmentSec <= 0 || *segmentSec > *windowSec/2 {
		log.Fatal("--window-sec must be >=120 and permit at least two --segment-sec segments")
	}
	if strings.TrimSpace(*backendAPIURL) == "" || strings.TrimSpace(*nodeToken) == "" || *attempts != 2 {
		log.Fatal("--backend-api-url and --node-token are required; --attempts must be exactly 2")
	}
	results := make([]map[string]any, 0, len(ids)*2)
	failed := false
	for _, streamID := range ids {
		for attempt := 1; attempt <= *attempts; attempt++ {
			requestID := uuid.New()
			var targetResponse struct {
				Targets []recordability.Target `json:"targets"`
			}
			if err := postJSONWithToken(ctx, *backendAPIURL, *nodeToken, "/api/v1/recording/campaign-admission/targets", map[string]any{"approval_id": approvalID, "stream_ids": []int64{streamID}, "request_id": requestID}, &targetResponse); err != nil {
				log.Fatalf("load exact approved target attempt: %v", err)
			}
			if len(targetResponse.Targets) != 1 || targetResponse.Targets[0].ID != streamID || targetResponse.Targets[0].AttemptID == "" || targetResponse.Targets[0].Challenge == "" {
				log.Fatal("server did not return the exact challenged approved target")
			}
			target := targetResponse.Targets[0]
			evidence := recordability.ProbeStreamTargeted(ctx, target, time.Duration(*windowSec)*time.Second, time.Duration(*segmentSec)*time.Second)
			var saved map[string]any
			if err := postJSONWithToken(ctx, *backendAPIURL, *nodeToken, "/api/v1/recording/campaign-admission/evidence", map[string]any{"approval_id": approvalID, "stream_id": target.ID, "attempt_id": target.AttemptID, "request_id": requestID, "evidence": evidence}, &saved); err != nil {
				log.Printf("recordability run-targeted: stream %d attempt %d: %v", target.ID, attempt, err)
				failed = true
				break
			}
			results = append(results, map[string]any{"stream_id": target.ID, "attempt": attempt, "result": evidence.Result, "valid_ratio": evidence.ValidRatio, "duration_ms": evidence.DurationMs, "segment_count": evidence.SegmentCount})
			if evidence.Result != recordability.ResultOK {
				failed = true
				break
			}
		}
	}
	if *asJSON {
		printJSON(map[string]any{"approval_id": approvalID, "results": results, "all_ok": !failed})
	} else {
		fmt.Printf("approval_id=%s total=%d all_ok=%t\n", approvalID, len(results), !failed)
	}
	if failed {
		log.Fatal("one or more targeted probes failed or were not persisted")
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
