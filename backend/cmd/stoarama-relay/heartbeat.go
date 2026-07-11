package main

import (
	"context"
	"log"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/daydemir/stoarama/backend/internal/recordingapi"
)

const heartbeatInterval = 30 * time.Second

type relayDiagnostics interface {
	Snapshot() map[string]any
}

// relayHeartbeatLoop reports this relay's liveness and capabilities every 30s. It
// refreshes the cookieless YouTube probe when due (5 min while failing, 1 hour while
// healthy) and posts the capabilities to POST /api/v1/node/heartbeat, which sets
// last_heartbeat_at and merges the reported keys into nodes.capabilities_jsonb. It
// deliberately does NOT call applyCookieEnv: the resolve env is set once at startup
// (see runRelay) and stays stable for the process lifetime, so re-probing here only
// updates the reported youtube_ready/youtube_error visibility, never the live capture
// env (no race).
func relayHeartbeatLoop(ctx context.Context, client *recordingapi.Client, pr *probe, active *atomic.Int64, cfg relayConfig, diag relayDiagnostics) {
	bd, _ := binDir()
	bundledFFmpeg := filepath.Join(bd, "ffmpeg")
	ffmpegVer := ffmpegVersion(bundledFFmpeg)
	ffmpegProbe := ffmpegNetworkProbe(bundledFFmpeg)
	systemFFmpegVer := ffmpegVersion("/usr/bin/ffmpeg")
	systemFFmpegProbe := ffmpegNetworkProbe("/usr/bin/ffmpeg")

	send := func() {
		if pr.due() {
			pr.runOnce(ctx)
		}
		mode := "cookieless"
		if experimentalCookieMode() {
			mode = "with_cookies"
		}
		caps := map[string]any{
			"youtube_mode":           mode,
			"youtube_ready":          pr.ok(),
			"youtube_error":          pr.errorClass(),
			"active_jobs":            active.Load(),
			"relay_version":          version,
			"ytdlp_version":          pr.ytdlpVersion(),
			"ffmpeg_version":         ffmpegVer,
			"ffmpeg_network_probe":   ffmpegProbe,
			"system_ffmpeg_version":  systemFFmpegVer,
			"system_ffmpeg_probe":    systemFFmpegProbe,
			"max_concurrent_streams": cfg.Concurrency,
		}
		if diag != nil {
			caps["recording_job"] = diag.Snapshot()
		}
		hctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		err := client.NodeHeartbeat(hctx, caps)
		cancel()
		if err != nil && ctx.Err() == nil {
			log.Printf("relay heartbeat error: %v", err)
		}
	}

	send()
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			send()
		}
	}
}
