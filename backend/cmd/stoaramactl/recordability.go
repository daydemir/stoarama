package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/recordability"
	"github.com/google/uuid"
)

func runRecordability(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 {
		log.Fatalf("usage: stoaramactl recordability <run-once|run-targeted|review-targeted> ...")
	}
	switch args[0] {
	case "run-once":
		runRecordabilityRunOnce(ctx, cfg, args[1:])
	case "run-targeted":
		runRecordabilityTargeted(ctx, cfg, args[1:])
	case "review-targeted":
		runRecordabilityTargetedReview(ctx, cfg, args[1:])
	default:
		log.Fatalf("unknown recordability subcommand: %s", args[0])
	}
}

func runRecordabilityTargetedReview(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("recordability review-targeted", flag.ExitOnError)
	approvalRaw := fs.String("approval-id", "", "exact immutable campaign approval UUID")
	evidenceRaw := fs.String("probe-evidence-ids", "", "exact comma-separated server-derived probe evidence UUIDs reviewed against the approved scene")
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	sessionCookiePath := fs.String("session-cookie-file", "", "Deniz operator session cookie file")
	asJSON := fs.Bool("json", false, "print JSON")
	_ = fs.Parse(args)
	_ = cfg
	approvalID, err := uuid.Parse(strings.TrimSpace(*approvalRaw))
	if err != nil {
		log.Fatalf("--approval-id must be a UUID: %v", err)
	}
	rawIDs := strings.Split(strings.TrimSpace(*evidenceRaw), ",")
	if len(rawIDs) == 0 || strings.TrimSpace(*evidenceRaw) == "" {
		log.Fatal("--probe-evidence-ids is required")
	}
	evidenceIDs := make([]uuid.UUID, 0, len(rawIDs))
	seen := map[uuid.UUID]bool{}
	for _, raw := range rawIDs {
		id, parseErr := uuid.Parse(strings.TrimSpace(raw))
		if parseErr != nil || seen[id] {
			log.Fatalf("--probe-evidence-ids must contain unique UUIDs: %v", parseErr)
		}
		seen[id] = true
		evidenceIDs = append(evidenceIDs, id)
	}
	cookie, err := readCampaignSessionCookie(*sessionCookiePath)
	if err != nil {
		log.Fatal(err)
	}
	results := make([]map[string]any, 0, len(evidenceIDs))
	for _, evidenceID := range evidenceIDs {
		results = append(results, postRecordingSessionJSON(ctx, *backendAPIURL, cookie, "/api/v1/account/recordings/campaign-admission/scene-reviews", map[string]any{"approval_id": approvalID, "probe_evidence_id": evidenceID, "request_id": uuid.New()}))
	}
	if *asJSON {
		printJSON(map[string]any{"approval_id": approvalID, "scene_reviews": results})
		return
	}
	fmt.Printf("approval_id=%s reviewed=%d\n", approvalID, len(results))
}

func runRecordabilityTargeted(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("recordability run-targeted", flag.ExitOnError)
	approvalRaw := fs.String("approval-id", "", "exact immutable campaign approval UUID")
	streamIDsRaw := fs.String("stream-ids", "", "exact comma-separated approved stream IDs")
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	sessionCookiePath := fs.String("session-cookie-file", "", "Deniz operator session cookie file")
	asJSON := fs.Bool("json", false, "print JSON")
	_ = fs.Parse(args)
	_ = cfg
	approvalID, err := uuid.Parse(strings.TrimSpace(*approvalRaw))
	if err != nil {
		log.Fatalf("--approval-id must be a UUID: %v", err)
	}
	ids, err := parseInt64CSV(*streamIDsRaw)
	if err != nil || len(ids) == 0 {
		log.Fatalf("--stream-ids must contain unique positive integers: %v", err)
	}
	if strings.TrimSpace(*backendAPIURL) == "" {
		log.Fatal("--backend-api-url is required")
	}
	cookie, err := readCampaignSessionCookie(*sessionCookiePath)
	if err != nil {
		log.Fatal(err)
	}
	results := make([]map[string]any, 0, len(ids))
	for _, streamID := range ids {
		requestID := uuid.New()
		result := postRecordingSessionJSON(ctx, *backendAPIURL, cookie, "/api/v1/account/recordings/campaign-admission/probe-orders", map[string]any{"approval_id": approvalID, "stream_id": streamID, "request_id": requestID})
		results = append(results, result)
	}
	if *asJSON {
		printJSON(map[string]any{"approval_id": approvalID, "orders": results})
	} else {
		fmt.Printf("approval_id=%s queued=%d desired_attempts_per_stream=2\n", approvalID, len(results))
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
