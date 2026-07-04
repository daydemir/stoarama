package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/daydemir/stoarama/backend/internal/recordingapi"
	"github.com/daydemir/stoarama/backend/internal/recordingworker"
)

// runRelay is the launchd/systemd service entrypoint. It runs the shared
// recordingworker loop with the relay-specific config (node:{id} lease owner,
// droplet heartbeat skipped, cookie-error classification on), points the shared
// capture/resolve.go at the bundled yt-dlp/ffmpeg, and runs the node heartbeat +
// cookie probe on its own goroutine.
func runRelay(ctx context.Context) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	bd, err := binDir()
	if err != nil {
		return err
	}
	ytdlp := filepath.Join(bd, "yt-dlp")

	// Force UTC for this process AND every ffmpeg child it spawns. The capture path
	// names segments with a strftime pattern that ffmpeg expands in the local zone,
	// and the ingest handler parses those names as clip_start_at; without this a relay
	// on a non-UTC machine would emit clips whose timestamps are offset by the local
	// UTC offset, landing them outside the job window. Setting TZ in the process env
	// (inherited by exec'd ffmpeg) plus resetting time.Local keeps both sides in UTC.
	os.Setenv("TZ", "UTC")
	time.Local = time.UTC

	// Point the shared capture path at the bundled binaries. The cookie source is set
	// by the probe (applyCookieEnv) once the startup probe decides whether the exported
	// cookie FILE resolves; both cookie env vars are cleared here so a stale value from
	// the environment can never leak in. --cookies-from-browser is never used in this
	// headless path (no Keychain grant).
	os.Setenv("YT_DLP_BIN", ytdlp)
	os.Unsetenv("YT_DLP_COOKIES_FROM_BROWSER")
	os.Unsetenv("YT_DLP_COOKIES_FILE")
	os.Setenv("FFMPEG_BIN", filepath.Join(bd, "ffmpeg"))
	prependPath(bd) // ffprobe is resolved from PATH by the capture path

	client, err := recordingapi.NewClient(recordingapi.ClientConfig{
		BaseURL:   cfg.APIURL,
		NodeToken: cfg.NodeToken,
	})
	if err != nil {
		return fmt.Errorf("init recording api client: %w", err)
	}

	var activeJobs atomic.Int64
	worker, err := recordingworker.NewWorker(recordingworker.Config{
		Client:                      client,
		WorkerID:                    fmt.Sprintf("node:%d", cfg.NodeID),
		Concurrency:                 cfg.Concurrency,
		HeartbeatSec:                15,
		PollInterval:                5 * time.Second,
		SkipDropletHeartbeat:        true,
		ClassifyYouTubeCookieErrors: true,
		ActiveJobs:                  &activeJobs,
	})
	if err != nil {
		return fmt.Errorf("init relay worker: %w", err)
	}

	// Startup probe (hard-timeout bounded) so the cookie env reflects reality before
	// the first job can be leased. The cookie mode (Chrome cookies vs cookie-less
	// resolve) is decided HERE, ONCE, from this initial probe and set via applyCookieEnv
	// before the worker starts. It is never mutated again for the process lifetime: the
	// heartbeat goroutine keeps re-probing and reporting yt_cookies_ok/yt_cookie_error,
	// but does NOT touch the cookie env, so there is no data race between a capture
	// reading YT_DLP_COOKIES_FROM_BROWSER and the probe writing it. A cookie-mode change
	// takes effect only across a process restart. See probe.applyCookieEnv for the
	// sanctioned-fallback rationale.
	pr := newProbe(ytdlp)
	pr.runOnce(ctx)
	pr.applyCookieEnv()
	log.Printf("stoarama-relay run node=%d concurrency=%d api=%s cookies_ok=%t cookie_class=%q",
		cfg.NodeID, cfg.Concurrency, cfg.APIURL, pr.ok(), pr.errorClass())

	go relayHeartbeatLoop(ctx, client, pr, &activeJobs, cfg)
	go selfUpdateLoop(ctx, cfg.APIURL)

	return worker.Run(ctx)
}

// prependPath puts dir at the front of PATH so the bundled ffprobe (and any other
// bundled tool resolved by name) is preferred over a system install.
func prependPath(dir string) {
	if cur := os.Getenv("PATH"); cur != "" {
		os.Setenv("PATH", dir+string(os.PathListSeparator)+cur)
		return
	}
	os.Setenv("PATH", dir)
}
