package capture

import (
	"context"
	"fmt"
	"html"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/netguard"
)

func ResolveCaptureInput(ctx context.Context, provider, streamURL, sourcePageURL string) (resolvedURL string, isImage bool, err error) {
	resolvedURL, isImage, _, err = ResolveCaptureInputWithHeaders(ctx, provider, streamURL, sourcePageURL)
	return resolvedURL, isImage, err
}

// ResolveCaptureInputWithHeaders converts provider/page URLs into a direct
// capture input URL plus any HTTP headers ffmpeg needs to open it.
func ResolveCaptureInputWithHeaders(ctx context.Context, provider, streamURL, sourcePageURL string) (resolvedURL string, isImage bool, inputHeaders string, err error) {
	provider = strings.ToUpper(strings.TrimSpace(provider))
	streamURL = strings.TrimSpace(streamURL)
	sourcePageURL = strings.TrimSpace(sourcePageURL)

	if streamURL == "" {
		if sourcePageURL == "" {
			return "", false, "", fmt.Errorf("stream has no capture URL")
		}
		streamURL = sourcePageURL
	}

	if isSkylineStream(provider, streamURL, sourcePageURL) && sourcePageURL != "" {
		u, err := resolveSkylineManifestURL(ctx, sourcePageURL, 20*time.Second)
		if err != nil {
			return "", false, "", err
		}
		if u == "" {
			return "", false, "", fmt.Errorf("skyline source page did not contain a playable manifest")
		}
		return u, false, "", nil
	}

	if shouldResolveEarthCamPage(provider, streamURL, sourcePageURL) {
		u, err := resolveEarthCamManifestURL(ctx, sourcePageURL, 20*time.Second)
		if err != nil {
			return "", false, "", err
		}
		if u == "" {
			return "", false, "", fmt.Errorf("earthcam source page did not contain a playable manifest")
		}
		return u, false, earthCamInputHeaders(sourcePageURL), nil
	}

	if provider == "KBS" && strings.Contains(streamURL, "!hls") {
		if u, ok, err := resolveIndirectURL(ctx, streamURL, 20*time.Second); err != nil {
			return "", false, "", err
		} else if ok {
			return u, false, "", nil
		}
	}

	if isYouTubeURL(streamURL) {
		u, err := resolveYouTubeStreamURL(ctx, streamURL)
		if err != nil {
			return "", false, "", err
		}
		return u, false, "", nil
	}

	if looksLikeImageURL(streamURL) {
		return streamURL, true, "", nil
	}

	if strings.Contains(streamURL, "!hls") {
		if u, ok, err := resolveIndirectURL(ctx, streamURL, 20*time.Second); err != nil {
			return "", false, "", err
		} else if ok {
			return u, false, "", nil
		}
	}

	// Fail closed: an indirect marker (e.g. "!hls") that survived resolution is
	// not a playable URL. Handing it to ffmpeg yields "Invalid data found"
	// (exit 183), so reject it here exactly as the survey path's
	// hlsLiveAdapter.Resolve does, rather than silently passing the raw marker.
	if hasIndirectMarker(streamURL) {
		return "", false, "", fmt.Errorf("indirect stream reference did not resolve to a playable URL: %s", streamURL)
	}

	return streamURL, false, "", nil
}

func isSkylineStream(provider, streamURL, sourcePageURL string) bool {
	if strings.EqualFold(strings.TrimSpace(provider), "SKYLINEWEBCAMS") {
		return true
	}
	for _, raw := range []string{streamURL, sourcePageURL} {
		u, err := url.Parse(strings.TrimSpace(raw))
		if err != nil {
			continue
		}
		host := strings.ToLower(strings.TrimSpace(u.Hostname()))
		if host == "skylinewebcams.com" || strings.HasSuffix(host, ".skylinewebcams.com") {
			return true
		}
	}
	return false
}

func isEarthCamStream(provider, streamURL, sourcePageURL string) bool {
	if strings.EqualFold(strings.TrimSpace(provider), "EARTHCAM") {
		return true
	}
	for _, raw := range []string{streamURL, sourcePageURL} {
		u, err := url.Parse(strings.TrimSpace(raw))
		if err != nil {
			continue
		}
		host := strings.ToLower(strings.TrimSpace(u.Hostname()))
		if host == "earthcam.com" || strings.HasSuffix(host, ".earthcam.com") || host == "myearthcam.com" || strings.HasSuffix(host, ".myearthcam.com") {
			return true
		}
	}
	return false
}

func shouldResolveEarthCamPage(provider, streamURL, sourcePageURL string) bool {
	return strings.TrimSpace(sourcePageURL) != "" &&
		isEarthCamStream(provider, streamURL, sourcePageURL) &&
		!isYouTubeURL(streamURL) &&
		!isYouTubeURL(sourcePageURL)
}

func resolveSkylineManifestURL(ctx context.Context, pageURL string, timeout time.Duration) (string, error) {
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	if _, err := resolveValidateURL(pageURL); err != nil {
		return "", fmt.Errorf("skyline page rejected: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pageURL, nil)
	if err != nil {
		return "", fmt.Errorf("build skyline request: %w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; stoarama-capture/1.0)")
	req.Header.Set("Accept", "text/html,application/xhtml+xml")
	client := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
				Control:   resolveDialControl,
			}).DialContext,
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return fmt.Errorf("too many redirects resolving skyline page")
			}
			if _, err := resolveValidateURL(req.URL.String()); err != nil {
				return fmt.Errorf("redirect target rejected: %w", err)
			}
			return nil
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("skyline request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("skyline request status=%d", resp.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
	if err != nil {
		return "", fmt.Errorf("read skyline page: %w", err)
	}
	u := skylineManifestFromHTML(string(b))
	if u == "" {
		return "", fmt.Errorf("skyline page did not contain player source")
	}
	return u, nil
}

const earthCamUserAgent = "Mozilla/5.0 (compatible; stoarama-capture/1.0)"

var skylinePlayerSourceRE = regexp.MustCompile(`(?i)\bsource\s*:\s*["']([^"']+?\.m3u8[^"']*)["']`)
var earthCamStreamRE = regexp.MustCompile(`(?i)"stream"\s*:\s*"((?:https?:)?\\?/\\?/[^"]+?\.m3u8[^"]*)"`)

func skylineManifestFromHTML(pageHTML string) string {
	m := skylinePlayerSourceRE.FindStringSubmatch(pageHTML)
	if len(m) < 2 {
		return ""
	}
	raw := strings.TrimSpace(html.UnescapeString(m[1]))
	if raw == "" {
		return ""
	}
	if u, err := url.Parse(raw); err == nil && u.IsAbs() {
		if strings.EqualFold(u.Hostname(), "hd-auth.skylinewebcams.com") {
			return u.String()
		}
		if strings.EqualFold(u.Hostname(), "www.skylinewebcams.com") {
			u.Scheme = "https"
			u.Host = "hd-auth.skylinewebcams.com"
			u.Path = "/live.m3u8"
			return u.String()
		}
		return u.String()
	}
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	base := &url.URL{Scheme: "https", Host: "hd-auth.skylinewebcams.com", Path: "/live.m3u8"}
	base.RawQuery = u.RawQuery
	return base.String()
}

func resolveEarthCamManifestURL(ctx context.Context, pageURL string, timeout time.Duration) (string, error) {
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	if _, err := resolveValidateURL(pageURL); err != nil {
		return "", fmt.Errorf("earthcam page rejected: %w", err)
	}
	client := resolveHTTPClient(timeout)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pageURL, nil)
	if err != nil {
		return "", fmt.Errorf("build earthcam request: %w", err)
	}
	req.Header.Set("User-Agent", earthCamUserAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml")
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("earthcam request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("earthcam request status=%d", resp.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
	if err != nil {
		return "", fmt.Errorf("read earthcam page: %w", err)
	}
	for _, manifestURL := range earthCamManifestCandidatesFromHTML(string(b)) {
		if _, err := resolveValidateURL(manifestURL); err != nil {
			continue
		}
		if earthCamManifestPlayable(ctx, client, pageURL, manifestURL) {
			return manifestURL, nil
		}
	}
	return "", fmt.Errorf("earthcam page did not contain a playable manifest")
}

func earthCamManifestCandidatesFromHTML(pageHTML string) []string {
	matches := earthCamStreamRE.FindAllStringSubmatch(pageHTML, -1)
	out := make([]string, 0, len(matches))
	seen := map[string]struct{}{}
	for _, m := range matches {
		if len(m) < 2 {
			continue
		}
		raw := strings.TrimSpace(html.UnescapeString(m[1]))
		raw = strings.ReplaceAll(raw, `\/`, `/`)
		if strings.HasPrefix(raw, "//") {
			raw = "https:" + raw
		}
		u, err := url.Parse(raw)
		if err != nil || !u.IsAbs() {
			continue
		}
		s := u.String()
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

func earthCamManifestPlayable(ctx context.Context, client *http.Client, pageURL, manifestURL string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, manifestURL, nil)
	if err != nil {
		return false
	}
	req.Header.Set("User-Agent", earthCamUserAgent)
	req.Header.Set("Accept", "application/vnd.apple.mpegurl,application/x-mpegURL,*/*")
	req.Header.Set("Referer", pageURL)
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return false
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return err == nil && strings.Contains(string(b), "#EXTM3U")
}

func earthCamInputHeaders(pageURL string) string {
	pageURL = strings.TrimSpace(pageURL)
	if pageURL == "" {
		return ""
	}
	return "Referer: " + pageURL + "\r\nUser-Agent: " + earthCamUserAgent + "\r\n"
}

func resolveHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
				Control:   resolveDialControl,
			}).DialContext,
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return fmt.Errorf("too many redirects")
			}
			if _, err := resolveValidateURL(req.URL.String()); err != nil {
				return fmt.Errorf("redirect target rejected: %w", err)
			}
			return nil
		},
	}
}

// hasIndirectMarker reports whether a URL still carries an internal indirect
// source marker that must be resolved before capture. "!hls" is the only such
// marker the catalog uses today; keyed generically so any future marker is also
// caught rather than passed through to ffmpeg.
func hasIndirectMarker(streamURL string) bool {
	return strings.Contains(strings.ToLower(streamURL), "!hls")
}

func resolveYouTubeStreamURL(ctx context.Context, watchURL string) (string, error) {
	resolveCtx := ctx
	cancel := func() {}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		resolveCtx, cancel = context.WithTimeout(ctx, 45*time.Second)
	}
	defer cancel()
	bin := strings.TrimSpace(os.Getenv("YT_DLP_BIN"))
	if bin == "" {
		bin = "yt-dlp"
	}
	args := ytDLPResolveArgs(watchURL)
	if cookies := strings.TrimSpace(os.Getenv("YT_DLP_COOKIES_FILE")); cookies != "" {
		args = append(args, "--cookies", cookies)
	}
	if browser := strings.TrimSpace(os.Getenv("YT_DLP_COOKIES_FROM_BROWSER")); browser != "" {
		args = append(args, "--cookies-from-browser", browser)
	}
	var lastErr error
	for attempt := 1; attempt <= 2; attempt++ {
		cmd := exec.CommandContext(resolveCtx, bin, args...)
		out, err := cmd.CombinedOutput()
		if streamURL := firstHTTPURL(string(out)); streamURL != "" {
			return streamURL, nil
		}
		if err == nil {
			lastErr = fmt.Errorf("yt-dlp returned no stream URL for %s", watchURL)
		} else {
			lastErr = fmt.Errorf("yt-dlp failed for %s: %w (%s)", watchURL, err, strings.TrimSpace(string(out)))
		}
		if attempt == 2 {
			break
		}
		select {
		case <-resolveCtx.Done():
			return "", lastErr
		case <-time.After(2 * time.Second):
		}
	}
	return "", lastErr
}

func firstHTTPURL(out string) string {
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "http://") || strings.HasPrefix(line, "https://") {
			return line
		}
	}
	return ""
}

func ytDLPResolveArgs(watchURL string) []string {
	args := []string{"-g", "--no-warnings", "--no-playlist"}
	if format := strings.TrimSpace(os.Getenv("YT_DLP_FORMAT")); format != "" {
		args = append(args, "-f", format)
	}
	if sortExpr := strings.TrimSpace(os.Getenv("YT_DLP_FORMAT_SORT")); sortExpr != "" {
		args = append(args, "-S", sortExpr)
	}
	return append(args, watchURL)
}

// resolveValidateURL and resolveDialControl are the SSRF guards applied to the
// indirect-resolve fetch (host pre-check, per-redirect re-check, and a dialer
// Control that rejects any private/metadata socket address). They are package
// vars so same-package tests can point them at a loopback test server;
// production always uses the netguard implementations.
var (
	resolveValidateURL = netguard.ValidatePublicURL
	resolveDialControl = netguard.ControlReject
)

func resolveIndirectURL(ctx context.Context, rawURL string, timeout time.Duration) (string, bool, error) {
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	// SSRF: rawURL is user-supplied (recorder), so validate its host before the
	// fetch, and guard every redirect hop + the actual socket dial against
	// private/metadata/RFC1918 addresses (a 302 to 169.254.169.254 or a DNS
	// rebind would otherwise be followed and its body returned).
	if _, err := resolveValidateURL(rawURL); err != nil {
		return "", false, fmt.Errorf("resolve target rejected: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return "", false, fmt.Errorf("build resolve request: %w", err)
	}
	req.Header.Set("User-Agent", "stoarama-capture/1.0")
	client := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
				Control:   resolveDialControl,
			}).DialContext,
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return fmt.Errorf("too many redirects resolving stream reference")
			}
			if _, err := resolveValidateURL(req.URL.String()); err != nil {
				return fmt.Errorf("redirect target rejected: %w", err)
			}
			return nil
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", false, fmt.Errorf("resolve request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", false, fmt.Errorf("resolve request status=%d", resp.StatusCode)
	}
	if resp.Request != nil && resp.Request.URL != nil {
		finalURL := strings.TrimSpace(resp.Request.URL.String())
		if finalURL != "" && finalURL != strings.TrimSpace(rawURL) {
			if strings.HasPrefix(finalURL, "http://") || strings.HasPrefix(finalURL, "https://") {
				return finalURL, true, nil
			}
		}
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 32*1024))
	if err != nil {
		return "", false, fmt.Errorf("read resolve body: %w", err)
	}
	body := strings.TrimSpace(string(b))
	if body == "" {
		return "", false, nil
	}
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "http://") || strings.HasPrefix(line, "https://") {
			return line, true, nil
		}
	}
	return "", false, nil
}

func isYouTubeURL(raw string) bool {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return false
	}
	host := strings.ToLower(strings.TrimSpace(u.Hostname()))
	return host == "youtube.com" || host == "www.youtube.com" || host == "m.youtube.com" || host == "youtu.be" || strings.HasSuffix(host, ".youtube.com")
}
