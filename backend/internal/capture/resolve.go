package capture

import (
	"context"
	"encoding/base64"
	"encoding/json"
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

	if host := sourcePageHost(sourcePageURL); hostMatches(host, "worldcam.eu") || hostMatches(host, "worldcam.live") {
		u, referer, err := resolveWorldCamCaptureInput(ctx, sourcePageURL, 20*time.Second)
		if err != nil {
			return "", false, "", err
		}
		return u, false, worldCamInputHeaders(referer), nil
	}

	if IsResolvableSourcePage(provider, sourcePageURL) {
		u, err := resolveKnownSourcePage(ctx, sourcePageURL, 20*time.Second)
		if err != nil {
			return "", false, "", err
		}
		return u, false, "", nil
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

// IsResolvableSourcePage reports whether capture has a stable runtime resolver
// for a page URL. Importers use this to distinguish an offline supported camera
// from a page whose player format is not implemented yet.
func IsResolvableSourcePage(provider, sourcePageURL string) bool {
	provider = strings.ToUpper(strings.TrimSpace(provider))
	if isSkylineStream(provider, sourcePageURL, sourcePageURL) ||
		isEarthCamStream(provider, sourcePageURL, sourcePageURL) {
		return true
	}
	host := sourcePageHost(sourcePageURL)
	return hostMatches(host, "ipcamlive.com") ||
		hostMatches(host, "worldcam.eu") ||
		hostMatches(host, "worldcam.live") ||
		hostMatches(host, "webcamera.pl") ||
		hostMatches(host, "zachodnia.tv") ||
		hostMatches(host, "embed.karkonosze.online") ||
		hostMatches(host, "lubliniec.aztv.pl")
}

func sourcePageHost(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(u.Hostname()))
}

func hostMatches(host, root string) bool {
	return host == root || strings.HasSuffix(host, "."+root)
}

func resolveKnownSourcePage(ctx context.Context, pageURL string, timeout time.Duration) (string, error) {
	host := sourcePageHost(pageURL)
	switch {
	case hostMatches(host, "ipcamlive.com"):
		return resolveIPCamLiveManifestURL(ctx, pageURL, timeout)
	case hostMatches(host, "worldcam.eu"), hostMatches(host, "worldcam.live"):
		return resolveWorldCamManifestURL(ctx, pageURL, timeout)
	case hostMatches(host, "webcamera.pl"):
		return resolveWebCameraManifestURL(ctx, pageURL, timeout)
	case hostMatches(host, "zachodnia.tv"), hostMatches(host, "embed.karkonosze.online"):
		return resolveKarkonoszeManifestURL(ctx, pageURL, timeout)
	case hostMatches(host, "lubliniec.aztv.pl"):
		return resolveEmbeddedManifestURL(ctx, pageURL, timeout, "aztv")
	default:
		return "", fmt.Errorf("source page has no supported runtime resolver")
	}
}

func fetchSourcePage(ctx context.Context, pageURL, referer string, timeout time.Duration) (string, error) {
	if _, err := resolveValidateURL(pageURL); err != nil {
		return "", fmt.Errorf("source page rejected: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pageURL, nil)
	if err != nil {
		return "", fmt.Errorf("build source page request: %w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; stoarama-capture/1.0)")
	req.Header.Set("Accept", "text/html,application/xhtml+xml")
	if strings.TrimSpace(referer) != "" {
		req.Header.Set("Referer", referer)
	}
	client := resolveHTTPClient(timeout)
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("source page request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("source page request status=%d", resp.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
	if err != nil {
		return "", fmt.Errorf("read source page: %w", err)
	}
	return string(b), nil
}

var (
	ipCamAliasRE      = regexp.MustCompile(`(?i)\bvar\s+alias\s*=\s*['"]([^'"]+)['"]`)
	ipCamTokenRE      = regexp.MustCompile(`(?i)\bvar\s+token\s*=\s*['"]([^'"]+)['"]`)
	ipCamAddressRE    = regexp.MustCompile(`(?i)\bvar\s+address\s*=\s*['"]([^'"]+)['"]`)
	ipCamStreamIDRE   = regexp.MustCompile(`(?i)\bvar\s+streamid\s*=\s*['"]([^'"]+)['"]`)
	worldCamIframeRE  = regexp.MustCompile(`(?i)<iframe[^>]+\bsrc=["']([^"']+)["']`)
	worldCamSourceRE  = regexp.MustCompile(`(?i)"source"\s*:\s*"([A-Za-z0-9+/=]+)"`)
	webCameraSourceRE = regexp.MustCompile(`(?i)"video_src"\s*:\s*("(?:\\.|[^"\\])*")`)
	embeddedHLSURLRE  = regexp.MustCompile(`(?i)https?[^"'<>[:space:]]+?\.m3u8[^"'<>[:space:]]*`)
)

func resolveIPCamLiveManifestURL(ctx context.Context, pageURL string, timeout time.Duration) (string, error) {
	page, err := fetchSourcePage(ctx, pageURL, "", timeout)
	if err != nil {
		return "", fmt.Errorf("ipcamlive page: %w", err)
	}
	// Some municipal sites embed IPCamLive's trusted player directly instead
	// of linking to a public camera landing page. In that case the fetched page
	// already contains the address and stream id and no short-lived token is
	// needed.
	if address, streamID := firstMatch(ipCamAddressRE, page), firstMatch(ipCamStreamIDRE, page); address != "" && streamID != "" {
		return ipCamManifestURL(address, streamID)
	}
	alias := firstMatch(ipCamAliasRE, page)
	token := firstMatch(ipCamTokenRE, page)
	if alias == "" || token == "" {
		return "", fmt.Errorf("ipcamlive page did not contain player credentials")
	}
	base, _ := url.Parse(pageURL)
	player := base.ResolveReference(&url.URL{Path: "/player/player.php"})
	q := player.Query()
	q.Set("alias", alias)
	q.Set("autoplay", "1")
	q.Set("token", token)
	player.RawQuery = q.Encode()
	playerHTML, err := fetchSourcePage(ctx, player.String(), pageURL, timeout)
	if err != nil {
		return "", fmt.Errorf("ipcamlive player: %w", err)
	}
	address := firstMatch(ipCamAddressRE, playerHTML)
	streamID := firstMatch(ipCamStreamIDRE, playerHTML)
	if address == "" || streamID == "" {
		return "", fmt.Errorf("ipcamlive camera is unavailable")
	}
	return ipCamManifestURL(address, streamID)
}

func ipCamManifestURL(address, streamID string) (string, error) {
	manifestBase, err := url.Parse(address)
	if err != nil || !hostMatches(strings.ToLower(manifestBase.Hostname()), "ipcamlive.com") {
		return "", fmt.Errorf("ipcamlive player returned an invalid stream host")
	}
	manifestBase.Scheme = "https"
	manifestBase.Path = "/streams/" + url.PathEscape(streamID) + "/stream.m3u8"
	manifestBase.RawQuery = ""
	manifestBase.Fragment = ""
	return manifestBase.String(), nil
}

func resolveWorldCamManifestURL(ctx context.Context, pageURL string, timeout time.Duration) (string, error) {
	manifest, _, err := resolveWorldCamCaptureInput(ctx, pageURL, timeout)
	return manifest, err
}

func resolveWorldCamCaptureInput(ctx context.Context, pageURL string, timeout time.Duration) (string, string, error) {
	page, err := fetchSourcePage(ctx, pageURL, "", timeout)
	if err != nil {
		return "", "", fmt.Errorf("worldcam page: %w", err)
	}
	embedURL := pageURL
	if sourcePageHost(pageURL) != "worldcam.live" {
		embedURL = firstMatch(worldCamIframeRE, page)
		if embedURL == "" || !hostMatches(sourcePageHost(embedURL), "worldcam.live") {
			return "", "", fmt.Errorf("worldcam page did not contain a trusted player embed")
		}
		page, err = fetchSourcePage(ctx, embedURL, pageURL, timeout)
		if err != nil {
			return "", "", fmt.Errorf("worldcam player: %w", err)
		}
	}
	encoded := firstMatch(worldCamSourceRE, page)
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(raw) == 0 {
		return "", "", fmt.Errorf("worldcam player did not contain a valid manifest")
	}
	manifest := strings.TrimSpace(string(raw))
	if _, err := resolveValidateURL(manifest); err != nil || !isHLSManifestURL(manifest) {
		return "", "", fmt.Errorf("worldcam manifest rejected")
	}
	return manifest, embedURL, nil
}

func worldCamInputHeaders(referer string) string {
	return "Referer: " + referer + "\r\nUser-Agent: Mozilla/5.0\r\n"
}

func resolveWebCameraManifestURL(ctx context.Context, pageURL string, timeout time.Duration) (string, error) {
	page, err := fetchSourcePage(ctx, pageURL, "", timeout)
	if err != nil {
		return "", fmt.Errorf("webcamera page: %w", err)
	}
	encoded := firstMatch(webCameraSourceRE, page)
	var escaped string
	if encoded == "" || json.Unmarshal([]byte(encoded), &escaped) != nil {
		return "", fmt.Errorf("webcamera page did not contain a valid player source")
	}
	manifest := rot13(escaped)
	if _, err := resolveValidateURL(manifest); err != nil || !isHLSManifestURL(manifest) {
		return "", fmt.Errorf("webcamera manifest rejected")
	}
	return manifest, nil
}

func resolveKarkonoszeManifestURL(ctx context.Context, pageURL string, timeout time.Duration) (string, error) {
	page, err := municipalFetchSourcePage(ctx, pageURL, "", timeout)
	if err != nil {
		return "", fmt.Errorf("karkonosze page: %w", err)
	}
	if hostMatches(sourcePageHost(pageURL), "zachodnia.tv") {
		embedURL := trustedKarkonoszeEmbedURL(page)
		if embedURL == "" {
			return "", fmt.Errorf("karkonosze page did not contain a trusted player embed")
		}
		page, err = municipalFetchSourcePage(ctx, embedURL, pageURL, timeout)
		if err != nil {
			return "", fmt.Errorf("karkonosze player: %w", err)
		}
	}
	return embeddedManifestURL(page, "karkonosze")
}

func trustedKarkonoszeEmbedURL(page string) string {
	for _, match := range worldCamIframeRE.FindAllStringSubmatch(page, -1) {
		if len(match) > 1 {
			candidate := html.UnescapeString(match[1])
			if hostMatches(sourcePageHost(candidate), "embed.karkonosze.online") {
				return candidate
			}
		}
	}
	return ""
}

func resolveEmbeddedManifestURL(ctx context.Context, pageURL string, timeout time.Duration, label string) (string, error) {
	page, err := municipalFetchSourcePage(ctx, pageURL, "", timeout)
	if err != nil {
		return "", fmt.Errorf("%s page: %w", label, err)
	}
	return embeddedManifestURL(page, label)
}

var municipalFetchSourcePage = fetchSourcePage

func embeddedManifestURL(page, label string) (string, error) {
	manifest := embeddedManifestCandidate(page)
	if _, err := resolveValidateURL(manifest); err != nil || !isHLSManifestURL(manifest) {
		return "", fmt.Errorf("%s page did not contain a valid manifest", label)
	}
	return manifest, nil
}

func embeddedManifestCandidate(page string) string {
	manifest := firstFullMatch(embeddedHLSURLRE, page)
	return html.UnescapeString(strings.ReplaceAll(manifest, `\/`, `/`))
}

func firstMatch(re *regexp.Regexp, value string) string {
	m := re.FindStringSubmatch(value)
	if len(m) < 2 {
		return ""
	}
	return strings.TrimSpace(html.UnescapeString(m[1]))
}

func firstFullMatch(re *regexp.Regexp, value string) string {
	return strings.TrimSpace(re.FindString(value))
}

func rot13(value string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z':
			return 'a' + (r-'a'+13)%26
		case r >= 'A' && r <= 'Z':
			return 'A' + (r-'A'+13)%26
		default:
			return r
		}
	}, value)
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
