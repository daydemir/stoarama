package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"runtime/debug"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/dropletpool"
	"github.com/daydemir/stoarama/backend/internal/recordingapi"
	"github.com/daydemir/stoarama/backend/internal/recordingworker"
	"golang.org/x/sys/unix"
)

// recorderExecutableBuildSHA is set by the cloud build's -X linker flag. It is
// deliberately not sourced from worker environment or flags.
var recorderExecutableBuildSHA string

const (
	cloudRecorderMinLeaseFreeBytes  = 2 << 30
	cloudRecorderMinActiveFreeBytes = 512 << 20
	cloudRecorderNoProgressTimeout  = 5 * time.Minute
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
		BuildSHA:     derivedRecorderExecutableBuildSHA(),
		DropletRolloutFacts: func(observeCtx context.Context) (*recordingapi.DropletRolloutFacts, error) {
			return collectDropletRolloutFacts(observeCtx, derivedRecorderExecutableBuildSHA(), *concurrency, *heartbeatSec, *pollSec, *backendAPIURL)
		},
		UploadWorkers:                cfg.RelayUploadWorkers,
		ContinuousNoProgressTimeout:  cloudRecorderNoProgressTimeout,
		FrozenHLSQuiescenceAllowlist: cfg.RecordingFrozenHLSQuiescenceAllowlist,
		DiskFreeBytes: func() (uint64, error) {
			var stat unix.Statfs_t
			if err := unix.Statfs(os.TempDir(), &stat); err != nil {
				return 0, err
			}
			return stat.Bavail * uint64(stat.Bsize), nil
		},
		MinLeaseFreeBytes:  cloudRecorderMinLeaseFreeBytes,
		MinActiveFreeBytes: cloudRecorderMinActiveFreeBytes,
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

func derivedRecorderExecutableBuildSHA() string {
	if value := strings.ToLower(strings.TrimSpace(recorderExecutableBuildSHA)); len(value) == 40 {
		return value
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	var revision string
	modified := false
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			revision = strings.ToLower(strings.TrimSpace(setting.Value))
		case "vcs.modified":
			modified = setting.Value == "true"
		}
	}
	if modified || len(revision) != 40 {
		return ""
	}
	return revision
}

func collectDropletRolloutFacts(ctx context.Context, buildSHA string, capacity, heartbeatSec, pollSec int, backendAPIURL string) (*recordingapi.DropletRolloutFacts, error) {
	if len(buildSHA) != 40 {
		return nil, fmt.Errorf("running executable has no exact clean build identity")
	}
	script, err := os.ReadFile("/usr/local/sbin/stoarama-egress-firewall.sh")
	if err != nil {
		return nil, fmt.Errorf("read installed host firewall: %w", err)
	}
	v4, err := exec.CommandContext(ctx, "iptables", "-S").Output()
	if err != nil {
		return nil, fmt.Errorf("read active IPv4 firewall: %w", err)
	}
	v6, err := exec.CommandContext(ctx, "ip6tables", "-S").Output()
	if err != nil {
		return nil, fmt.Errorf("read active IPv6 firewall: %w", err)
	}
	policy, err := dropletpool.DeriveKernelFirewallPolicy(v4, v6)
	if err != nil {
		return nil, err
	}
	return &recordingapi.DropletRolloutFacts{
		ExecutableBuildSHA: buildSHA,
		Capacity:           capacity,
		HeartbeatSec:       heartbeatSec,
		PollSec:            pollSec,
		BackendAPIURL:      strings.TrimRight(strings.TrimSpace(backendAPIURL), "/"),
		HostFirewallScript: string(script),
		KernelPolicy:       policy,
	}, nil
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
