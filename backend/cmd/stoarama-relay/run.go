package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/daydemir/stoarama/backend/internal/recordingapi"
	"github.com/daydemir/stoarama/backend/internal/recordingworker"
)

// runRelay is the launchd/systemd service entrypoint. It runs the shared
// recordingworker loop with the relay-specific config (node:{id} lease owner,
// droplet heartbeat skipped, cookie-error classification on), points the shared
// capture/resolve.go at the installed yt-dlp/platform ffmpeg, and runs the node
// heartbeat and YouTube probe on separate goroutines.
func runRelay(ctx context.Context) error {
	startedAt := time.Now().UTC()
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

	// Point the shared capture path at the installed yt-dlp and platform ffmpeg.
	// YouTube resolves COOKIELESS by default (applyCookieEnv leaves the cookie env unset unless the
	// experimental with-cookies opt-in is on); both cookie env vars are cleared here so
	// a stale value from the environment (or a leftover ~/.stoarama/cookies.txt) can
	// never leak in. --cookies-from-browser is never used in this headless path.
	os.Setenv("YT_DLP_BIN", ytdlp)
	os.Unsetenv("YT_DLP_COOKIES_FROM_BROWSER")
	os.Unsetenv("YT_DLP_COOKIES_FILE")
	os.Setenv("FFMPEG_BIN", relayFFmpegBin(bd))
	prependPath(bd) // ffprobe is resolved from PATH by the capture path

	client, err := recordingapi.NewClient(recordingapi.ClientConfig{
		BaseURL:   cfg.APIURL,
		NodeToken: cfg.NodeToken,
	})
	if err != nil {
		return fmt.Errorf("init recording api client: %w", err)
	}

	var activeJobs atomic.Int64
	relayDiag := &recordingworker.RelayDiagnostics{}
	worker, err := recordingworker.NewWorker(recordingworker.Config{
		Client:                      client,
		WorkerID:                    fmt.Sprintf("node:%d", cfg.NodeID),
		Concurrency:                 cfg.Concurrency,
		HeartbeatSec:                15,
		PollInterval:                5 * time.Second,
		SkipDropletHeartbeat:        true,
		ClassifyYouTubeCookieErrors: true,
		ActiveJobs:                  &activeJobs,
		RelayDiagnostics:            relayDiag,
	})
	if err != nil {
		return fmt.Errorf("init relay worker: %w", err)
	}

	// Startup probe (hard-timeout bounded) so the resolve env reflects reality before
	// the first job can be leased. The mode (cookieless default vs experimental
	// with-cookies) is decided HERE, ONCE, and set via applyCookieEnv before the worker
	// starts. It is never mutated again for the process lifetime: later probes only
	// update heartbeat visibility and do not touch the resolve env. A mode change takes
	// effect only across a process restart.
	pr := newProbe(ytdlp)
	firstHeartbeat := make(chan struct{})
	go relayHeartbeatLoop(ctx, client, pr, &activeJobs, cfg, relayDiag, startedAt, firstHeartbeat)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-firstHeartbeat:
	}
	pr.runOnce(ctx)
	pr.applyCookieEnv()
	log.Printf("stoarama-relay run node=%d concurrency=%d api=%s youtube_ready=%t youtube_error=%q",
		cfg.NodeID, cfg.Concurrency, cfg.APIURL, pr.ok(), pr.errorClass())

	go pr.runLoop(ctx)
	go selfUpdateLoop(ctx, cfg.APIURL)

	return worker.Run(ctx)
}

func relayFFmpegBin(binDir string) string {
	bundled := filepath.Join(binDir, "ffmpeg")
	system := "/usr/bin/ffmpeg"
	if runtime.GOOS == "linux" && runtime.GOARCH == "arm64" && executable(system) {
		return system
	}
	return bundled
}

func executable(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode()&0111 != 0
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
