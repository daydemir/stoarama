package capture

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"time"
)

// ResolveCaptureInput converts provider/page URLs into a direct capture input URL.
// It mirrors legacy local behavior for providers like YouTube and KBS.
func ResolveCaptureInput(ctx context.Context, provider, streamURL, sourcePageURL string) (resolvedURL string, isImage bool, err error) {
	provider = strings.ToUpper(strings.TrimSpace(provider))
	streamURL = strings.TrimSpace(streamURL)
	sourcePageURL = strings.TrimSpace(sourcePageURL)

	if streamURL == "" {
		if sourcePageURL == "" {
			return "", false, fmt.Errorf("stream has no capture URL")
		}
		streamURL = sourcePageURL
	}

	if provider == "KBS" && strings.Contains(streamURL, "!hls") {
		if u, ok, err := resolveIndirectURL(ctx, streamURL, 20*time.Second); err != nil {
			return "", false, err
		} else if ok {
			return u, false, nil
		}
	}

	if isYouTubeURL(streamURL) {
		u, err := resolveYouTubeStreamURL(ctx, streamURL)
		if err != nil {
			return "", false, err
		}
		return u, false, nil
	}

	if looksLikeImageURL(streamURL) {
		return streamURL, true, nil
	}

	if strings.Contains(streamURL, "!hls") {
		if u, ok, err := resolveIndirectURL(ctx, streamURL, 20*time.Second); err != nil {
			return "", false, err
		} else if ok {
			return u, false, nil
		}
	}

	return streamURL, false, nil
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
		if err == nil {
			for _, line := range strings.Split(string(out), "\n") {
				line = strings.TrimSpace(line)
				if strings.HasPrefix(line, "http://") || strings.HasPrefix(line, "https://") {
					return line, nil
				}
			}
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

func resolveIndirectURL(ctx context.Context, rawURL string, timeout time.Duration) (string, bool, error) {
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return "", false, fmt.Errorf("build resolve request: %w", err)
	}
	req.Header.Set("User-Agent", "stoarama-capture/1.0")
	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return "", false, fmt.Errorf("resolve request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", false, fmt.Errorf("resolve request status=%d", resp.StatusCode)
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
