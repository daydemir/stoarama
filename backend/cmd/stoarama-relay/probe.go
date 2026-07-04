package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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

// probe runs the YouTube cookie health check and holds its last classification.
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

// runOnce runs the cookie probe under a hard timeout. It NEVER uses
// --cookies-from-browser: in the background launchd/systemd agent the macOS Chrome
// Safe Storage key is not granted to this process, so a browser read would extract
// zero cookies and (worse) hang on a Keychain prompt. Instead it reads the file the
// GUI-session `link-youtube` export wrote. When no cookie file exists the relay is
// simply not linked to YouTube; that is reported honestly as cookies_unavailable,
// not a failure (public lives still resolve cookie-less). The parent ctx cancels it
// early on shutdown.
func (p *probe) runOnce(ctx context.Context) {
	cookiePath, _ := cookiesFilePath()
	if cookiePath == "" || !fileExists(cookiePath) {
		p.set(capture.YTDLPClassCookiesUnavailable)
		return
	}

	cctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	args := []string{"-g", "--no-warnings", "--no-playlist", "--cookies", cookiePath, probeURL}
	out, err := exec.CommandContext(cctx, p.ytdlpBin, args...).CombinedOutput()

	var class capture.YTDLPClass
	switch {
	case err == nil && hasHTTPURL(string(out)):
		class = capture.YTDLPClassOK
	case cctx.Err() == context.DeadlineExceeded:
		// A --cookies FILE read never prompts Keychain, so a timeout here is a slow
		// resolve/network stall, not a cookie problem.
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

// applyCookieEnv sets the cookie source the shared capture/resolve.go reads: point
// yt-dlp at the exported cookie FILE only when the startup probe resolved with it;
// otherwise resolve without cookies (a residential IP resolves public lives fine)
// rather than failing. It never sets YT_DLP_COOKIES_FROM_BROWSER: the background
// agent has no Keychain grant, so a browser read is guaranteed to fail.
//
// SANCTIONED-EXCEPTION (pending owner confirmation, DECISIONS 2026-07-04 §P2.3): the
// cookie-less fallback deliberately relaxes Deniz's no-fallbacks rule, justified
// because most relays never link YouTube cookies and public lives resolve fine
// cookie-less from residential IPs. The fallback fires ONLY when the probe classifies
// cookies as unusable, and it is visible via the yt_cookies_ok/yt_cookie_error
// heartbeat capabilities. This is called exactly ONCE at startup (runRelay); the mode
// never changes mid-flight, only across a restart, so there is no os.Setenv race with
// an in-flight capture.
func (p *probe) applyCookieEnv() {
	cookiePath, _ := cookiesFilePath()
	if p.ok() && cookiePath != "" {
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

// chromeCookieDBPath returns the platform path to Chrome's default cookie SQLite
// DB, or "" on an unsupported OS. Only stat'd, never opened, so no Keychain prompt.
func chromeCookieDBPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	switch runtime.GOOS {
	case "darwin":
		return filepath.Join(home, "Library", "Application Support", "Google", "Chrome", "Default", "Cookies")
	case "linux":
		return filepath.Join(home, ".config", "google-chrome", "Default", "Cookies")
	default:
		return ""
	}
}

func chromeCookieDBPresent() bool {
	p := chromeCookieDBPath()
	if p == "" {
		return false
	}
	_, err := os.Stat(p)
	return err == nil
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
