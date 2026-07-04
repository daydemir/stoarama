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

// relayHeartbeatLoop reports this relay's liveness and capabilities every 30s. It
// refreshes the cookie probe when due (5 min while failing, 1 hour while healthy) and
// posts the capabilities to POST /api/v1/node/heartbeat, which sets last_heartbeat_at
// and merges the reported keys into nodes.capabilities_jsonb. It deliberately does NOT
// call applyCookieEnv: the cookie env is set once at startup (see runRelay) and stays
// stable for the process lifetime, so re-probing here only updates the reported
// yt_cookies_ok/yt_cookie_error visibility, never the live capture env (no race).
func relayHeartbeatLoop(ctx context.Context, client *recordingapi.Client, pr *probe, active *atomic.Int64, cfg relayConfig) {
	bd, _ := binDir()
	ffmpegVer := ffmpegVersion(filepath.Join(bd, "ffmpeg"))

	send := func() {
		if pr.due() {
			pr.runOnce(ctx)
		}
		caps := map[string]any{
			"yt_cookies_ok":          pr.ok(),
			"yt_cookie_error":        pr.errorClass(),
			"chrome_present":         chromeCookieDBPresent(),
			"active_jobs":            active.Load(),
			"relay_version":          version,
			"ytdlp_version":          pr.ytdlpVersion(),
			"ffmpeg_version":         ffmpegVer,
			"max_concurrent_streams": cfg.Concurrency,
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
