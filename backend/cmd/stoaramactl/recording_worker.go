package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/recordingapi"
	"github.com/daydemir/stoarama/backend/internal/recordingworker"
)

// runRecordingWorker runs the recorder droplet's clip-capture loop. It
// authenticates with a per-droplet local_recorder node token (RECORDER_NODE_TOKEN)
// and never uses the shared service token. The lease duration is computed
// server-side per job, so there is no --lease-sec flag.
func runRecordingWorker(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 || args[0] != "run" {
		log.Fatalf("usage: stoaramactl recording-worker run [--backend-api-url URL --node-token TOKEN --worker-id ID --concurrency 1 --heartbeat-sec 15 --poll-sec 5]")
	}
	fs := flag.NewFlagSet("recording-worker run", flag.ExitOnError)
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	nodeToken := fs.String("node-token", strings.TrimSpace(os.Getenv("RECORDER_NODE_TOKEN")), "per-droplet local_recorder node token")
	workerID := fs.String("worker-id", defaultRecordingWorkerID(), "worker id (= droplet name = recorder_droplets.name)")
	concurrency := fs.Int("concurrency", cfg.RecordingWorkerConcurrency, "concurrent clip captures (must equal the droplet's baked capacity)")
	heartbeatSec := fs.Int("heartbeat-sec", cfg.RecordingWorkerHeartbeatSec, "lease heartbeat interval seconds")
	pollSec := fs.Int("poll-sec", cfg.RecordingWorkerPollSec, "job poll interval seconds")
	duration := fs.Duration("duration", 0, "optional run duration (e.g. 30m, 8h)")
	_ = fs.Parse(args[1:])

	if strings.TrimSpace(*backendAPIURL) == "" {
		log.Fatalf("--backend-api-url is required")
	}
	if strings.TrimSpace(*nodeToken) == "" {
		log.Fatalf("--node-token (or RECORDER_NODE_TOKEN) is required")
	}
	if strings.TrimSpace(*workerID) == "" {
		log.Fatalf("--worker-id is required")
	}
	if *concurrency <= 0 {
		log.Fatalf("--concurrency must be > 0")
	}
	if *heartbeatSec <= 0 {
		log.Fatalf("--heartbeat-sec must be > 0")
	}
	if *pollSec <= 0 {
		log.Fatalf("--poll-sec must be > 0")
	}

	client, err := recordingapi.NewClient(recordingapi.ClientConfig{
		BaseURL:   strings.TrimSpace(*backendAPIURL),
		NodeToken: strings.TrimSpace(*nodeToken),
	})
	if err != nil {
		log.Fatalf("init recording api client: %v", err)
	}
	worker, err := recordingworker.NewWorker(recordingworker.Config{
		Client:       client,
		WorkerID:     strings.TrimSpace(*workerID),
		Concurrency:  *concurrency,
		HeartbeatSec: *heartbeatSec,
		PollInterval: time.Duration(*pollSec) * time.Second,
	})
	if err != nil {
		log.Fatalf("init recording worker: %v", err)
	}

	runCtx := ctx
	cancel := func() {}
	if *duration > 0 {
		runCtx, cancel = context.WithTimeout(ctx, *duration)
	}
	defer cancel()

	if err := worker.Run(runCtx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		log.Fatalf("recording-worker run failed: %v", err)
	}
}

// defaultRecordingWorkerID prefers RECORDER_SERVER_ID (set via cloud-init so the
// worker need not fetch the metadata service), falling back to the hostname.
func defaultRecordingWorkerID() string {
	if v := strings.TrimSpace(os.Getenv("RECORDER_SERVER_ID")); v != "" {
		return v
	}
	if h, err := os.Hostname(); err == nil {
		host := strings.TrimSpace(h)
		if i := strings.IndexByte(host, '.'); i > 0 {
			host = host[:i]
		}
		if host != "" {
			return strings.ToLower(host)
		}
	}
	return "recording-worker"
}
