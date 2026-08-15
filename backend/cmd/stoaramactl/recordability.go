package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/recordability"
	"github.com/google/uuid"
)

func runRecordability(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 {
		log.Fatalf("usage: stoaramactl recordability <run-once|run-targeted|present-targeted|review-targeted|expire-targeted> ...")
	}
	switch args[0] {
	case "run-once":
		runRecordabilityRunOnce(ctx, cfg, args[1:])
	case "run-targeted":
		runRecordabilityTargeted(ctx, cfg, args[1:])
	case "present-targeted":
		runRecordabilityTargetedPresent(ctx, cfg, args[1:])
	case "review-targeted":
		runRecordabilityTargetedReview(ctx, cfg, args[1:])
	case "expire-targeted":
		runRecordabilityTargetedExpire(ctx, cfg, args[1:])
	default:
		log.Fatalf("unknown recordability subcommand: %s", args[0])
	}
}

func runRecordabilityTargetedExpire(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("recordability expire-targeted", flag.ExitOnError)
	approvalRaw := fs.String("approval-id", "", "exact expired immutable campaign approval UUID")
	requestRaw := fs.String("request-id", "", "stable idempotency UUID")
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	sessionCookiePath := fs.String("session-cookie-file", "", "Deniz operator session cookie file")
	_ = fs.Parse(args)
	_ = cfg
	approvalID, approvalErr := uuid.Parse(strings.TrimSpace(*approvalRaw))
	requestID, requestErr := uuid.Parse(strings.TrimSpace(*requestRaw))
	if approvalErr != nil || requestErr != nil {
		log.Fatal("--approval-id and --request-id must be UUIDs")
	}
	cookie, err := readCampaignSessionCookie(*sessionCookiePath)
	if err != nil {
		log.Fatal(err)
	}
	printJSON(postRecordingSessionJSON(ctx, *backendAPIURL, cookie, "/api/v1/account/recordings/campaign-admission/expire", map[string]any{"approval_id": approvalID, "request_id": requestID}))
}

func runRecordabilityTargetedReview(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("recordability review-targeted", flag.ExitOnError)
	approvalRaw := fs.String("approval-id", "", "exact immutable campaign approval UUID")
	evidenceRaw := fs.String("probe-evidence-ids", "", "exact comma-separated server-derived probe evidence UUIDs reviewed against the approved scene")
	presentationRaw := fs.String("presentation-receipt-ids", "", "exact comma-separated protected presentation receipt UUIDs, in evidence order")
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
	rawPresentationIDs := strings.Split(strings.TrimSpace(*presentationRaw), ",")
	if len(rawPresentationIDs) != len(evidenceIDs) || strings.TrimSpace(*presentationRaw) == "" {
		log.Fatal("--presentation-receipt-ids must contain one receipt for each probe evidence ID")
	}
	presentationIDs := make([]uuid.UUID, 0, len(rawPresentationIDs))
	for _, raw := range rawPresentationIDs {
		id, parseErr := uuid.Parse(strings.TrimSpace(raw))
		if parseErr != nil {
			log.Fatalf("--presentation-receipt-ids must contain UUIDs: %v", parseErr)
		}
		presentationIDs = append(presentationIDs, id)
	}
	cookie, err := readCampaignSessionCookie(*sessionCookiePath)
	if err != nil {
		log.Fatal(err)
	}
	results := make([]map[string]any, 0, len(evidenceIDs))
	for i, evidenceID := range evidenceIDs {
		results = append(results, postRecordingSessionJSON(ctx, *backendAPIURL, cookie, "/api/v1/account/recordings/campaign-admission/scene-reviews", map[string]any{"approval_id": approvalID, "probe_evidence_id": evidenceID, "presentation_id": presentationIDs[i], "request_id": uuid.New()}))
	}
	if *asJSON {
		printJSON(map[string]any{"approval_id": approvalID, "scene_reviews": results})
		return
	}
	fmt.Printf("approval_id=%s reviewed=%d\n", approvalID, len(results))
}

func runRecordabilityTargetedPresent(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("recordability present-targeted", flag.ExitOnError)
	evidenceRaw := fs.String("probe-evidence-ids", "", "exact comma-separated server-derived probe evidence UUIDs")
	outputDir := fs.String("output-dir", "", "existing private directory for protected review frames")
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	sessionCookiePath := fs.String("session-cookie-file", "", "Deniz operator session cookie file")
	_ = fs.Parse(args)
	_ = cfg
	if strings.TrimSpace(*outputDir) == "" {
		log.Fatal("--output-dir is required")
	}
	info, err := os.Stat(*outputDir)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		log.Fatal("--output-dir must be an existing private directory (mode 0700)")
	}
	cookie, err := readCampaignSessionCookie(*sessionCookiePath)
	if err != nil {
		log.Fatal(err)
	}
	var receipts []map[string]any
	seen := map[uuid.UUID]bool{}
	for _, raw := range strings.Split(strings.TrimSpace(*evidenceRaw), ",") {
		evidenceID, parseErr := uuid.Parse(strings.TrimSpace(raw))
		if parseErr != nil || seen[evidenceID] {
			log.Fatalf("--probe-evidence-ids must contain unique UUIDs: %v", parseErr)
		}
		seen[evidenceID] = true
		requestID := uuid.New()
		path := fmt.Sprintf("/api/v1/account/recordings/campaign-admission/scene-presentations/%s?request_id=%s", evidenceID, requestID)
		presentation := getRecordingSessionJSON(ctx, *backendAPIURL, cookie, path)
		encoded, ok := presentation["frame_base64"].(string)
		if !ok {
			log.Fatal("scene presentation omitted protected frame bytes")
		}
		frame, decodeErr := base64.StdEncoding.DecodeString(encoded)
		if decodeErr != nil || len(frame) == 0 || len(frame) > recordability.TargetedFrameMaxBytes {
			log.Fatal("scene presentation returned invalid frame bytes")
		}
		file := filepath.Join(*outputDir, evidenceID.String()+".jpg")
		if writeErr := os.WriteFile(file, frame, 0o600); writeErr != nil {
			log.Fatalf("write protected scene frame: %v", writeErr)
		}
		delete(presentation, "frame_base64")
		presentation["local_frame"] = file
		receipts = append(receipts, presentation)
	}
	printJSON(map[string]any{"presentations": receipts})
}

func getRecordingSessionJSON(ctx context.Context, baseURL, cookie, path string) map[string]any {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+path, nil)
	if err != nil {
		log.Fatalf("create request: %v", err)
	}
	req.Header.Set("Cookie", cookie)
	resp, err := (&http.Client{Timeout: 90 * time.Second}).Do(req)
	if err != nil {
		log.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, int64(recordability.TargetedFrameMaxBytes*4/3)+(1<<20)))
	if err != nil {
		log.Fatalf("read response: %v", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Fatalf("request failed status=%d body=%q", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		log.Fatalf("decode response status=%d: %v", resp.StatusCode, err)
	}
	return out
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
