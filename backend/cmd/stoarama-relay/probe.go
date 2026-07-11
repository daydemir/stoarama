package main

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
)

const (
	probeTimeout      = 45 * time.Second
	probeIntervalFail = 5 * time.Minute
	probeIntervalOK   = 1 * time.Hour
	// probeURL is a stable, unrestricted public YouTube video ("Me at the zoo").
	probeURL = "https://www.youtube.com/watch?v=jNQXAC9IVRw"
)

// probe runs the cookieless YouTube health check and holds its last classification.
// All state access is mutex-guarded because the heartbeat goroutine reads it while
// the same goroutine periodically refreshes it.
type probe struct {
	ytdlpBin string

	mu       sync.Mutex
	class    capture.YTDLPClass
	lastRun  time.Time
	ranOnce  bool
	ytdlpVer string
}

func newProbe(ytdlpBin string) *probe {
	return &probe{
		ytdlpBin: ytdlpBin,
		class:    capture.YTDLPClassOther,
		ytdlpVer: readYtdlpVersion(ytdlpBin),
	}
}

// runOnce resolves the public probe URL under a hard timeout, COOKIELESS by default.
// yt-dlp's android client resolves public YouTube from a residential IP with no
// cookies at all, which is the default relay mode: youtube_ready=true means yt-dlp
// itself resolves. A failure is classified honestly (resolver_outdated, network,
// ...); there is no cookie source in the default path, so it can never be a cookie
// error. When the experimental with-cookies path is opted in (see experimentalCookieMode)
// and a cookie file exists, it is added so the probe reflects that mode. The parent
// ctx cancels it early on shutdown.
func (p *probe) runOnce(ctx context.Context) {
	cctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	args := []string{"-g", "--no-warnings", "--no-playlist", probeURL}
	if experimentalCookieMode() {
		if cp, _ := cookiesFilePath(); cp != "" && fileExists(cp) {
			args = append([]string{"--cookies", cp}, args...)
		}
	}
	out, err := exec.CommandContext(cctx, p.ytdlpBin, args...).CombinedOutput()

	var class capture.YTDLPClass
	switch {
	case err == nil && hasHTTPURL(string(out)):
		class = capture.YTDLPClassOK
	case cctx.Err() == context.DeadlineExceeded:
		// Cookieless resolve, so a timeout is a slow resolve / network stall.
		class = capture.YTDLPClassOther
	default:
		class = capture.ClassifyYTDLPOutput(string(out))
	}

	p.set(class)
}

func (p *probe) set(class capture.YTDLPClass) {
	p.mu.Lock()
	p.class = class
	p.lastRun = time.Now()
	p.ranOnce = true
	p.mu.Unlock()
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func (p *probe) ok() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.class == capture.YTDLPClassOK
}

func (p *probe) errorClass() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.class == capture.YTDLPClassOK {
		return ""
	}
	return string(p.class)
}

// due reports whether the probe should run again: every probeIntervalFail while
// failing, every probeIntervalOK while healthy, and always before the first run.
func (p *probe) due() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.ranOnce {
		return true
	}
	interval := probeIntervalFail
	if p.class == capture.YTDLPClassOK {
		interval = probeIntervalOK
	}
	return time.Since(p.lastRun) >= interval
}

func (p *probe) ytdlpVersion() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ytdlpVer
}

// applyCookieEnv sets the cookie source the shared capture/resolve.go reads. In the
// DEFAULT cookieless mode it clears YT_DLP_COOKIES_FILE so yt-dlp resolves with no
// cookies (the android client resolves public lives fine from a residential IP). It
// points yt-dlp at the exported cookie FILE only under the experimental with-cookies
// opt-in, and only when the startup probe resolved with it. It never sets
// YT_DLP_COOKIES_FROM_BROWSER: the background agent has no Keychain grant. Called
// exactly ONCE at startup (runRelay); the mode never changes mid-flight.
func (p *probe) applyCookieEnv() {
	cookiePath, _ := cookiesFilePath()
	if experimentalCookieMode() && p.ok() && cookiePath != "" && fileExists(cookiePath) {
		os.Setenv("YT_DLP_COOKIES_FILE", cookiePath)
	} else {
		os.Unsetenv("YT_DLP_COOKIES_FILE")
	}
}

func hasHTTPURL(out string) bool {
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "http://") || strings.HasPrefix(line, "https://") {
			return true
		}
	}
	return false
}

func readYtdlpVersion(bin string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, bin, "--version").CombinedOutput()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func ffmpegVersion(bin string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, bin, "-version").CombinedOutput()
	if err != nil {
		return ""
	}
	first := strings.SplitN(strings.TrimSpace(string(out)), "\n", 2)[0]
	// "ffmpeg version N.N ..." -> "N.N"
	fields := strings.Fields(first)
	if len(fields) >= 3 && fields[0] == "ffmpeg" && fields[1] == "version" {
		return fields[2]
	}
	return strings.TrimSpace(first)
}

func ffmpegNetworkProbe(bin string) string {
	if ffmpegVersion(bin) == "" {
		return "unavailable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, _ := exec.CommandContext(ctx, bin, "-v", "error", "-i", "https://manifest.googlevideo.com/", "-f", "null", "-").CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		return "timeout"
	}
	return classifyFFmpegNetworkProbe(string(out))
}

func classifyFFmpegNetworkProbe(out string) string {
	lower := strings.ToLower(out)
	if strings.Contains(lower, "failed to resolve hostname") || strings.Contains(lower, "temporary failure in name resolution") {
		return "dns_failed"
	}
	return "host_reached"
}
