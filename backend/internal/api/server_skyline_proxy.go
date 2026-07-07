package api

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/netguard"
	"github.com/daydemir/stoarama/backend/internal/util"
)

const skylineProxyMaxManifestBytes = 1024 * 1024

// handleDashboardStreamSkylineManifest serves a same-origin Skyline manifest for
// the inline browser player. Skyline returns a "copyright_violation" placeholder
// when the browser fetches hd-auth with a foreign Origin header. Fetching the
// manifest server-side avoids that browser-only anti-hotlink path, then rewrites
// segment URLs through the same-origin segment proxy below.
func (s *Server) handleDashboardStreamSkylineManifest(w http.ResponseWriter, r *http.Request) {
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	stream, err := s.getStreamByID(r.Context(), id)
	if err != nil {
		util.WriteError(w, http.StatusNotFound, err.Error())
		return
	}
	if !strings.EqualFold(strings.TrimSpace(stream.Provider), "SKYLINEWEBCAMS") {
		util.WriteError(w, http.StatusNotFound, "skyline proxy is only available for SkylineWebcams streams")
		return
	}

	resolveCtx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	manifestURL, isImage, err := capture.ResolveCaptureInput(resolveCtx, stream.Provider, stream.SourceURL, stream.SourcePageURL)
	if err != nil {
		util.WriteError(w, http.StatusBadGateway, fmt.Sprintf("resolve skyline stream source: %v", err))
		return
	}
	if isImage {
		util.WriteError(w, http.StatusUnprocessableEntity, "image sources are not playable inline")
		return
	}
	body, err := fetchSkylineProxyURL(resolveCtx, manifestURL, skylineProxyMaxManifestBytes)
	if err != nil {
		util.WriteError(w, http.StatusBadGateway, fmt.Sprintf("fetch skyline manifest: %v", err))
		return
	}
	if strings.Contains(string(body), "copyright_violation") {
		util.WriteError(w, http.StatusBadGateway, "skyline returned anti-hotlink placeholder manifest")
		return
	}
	rewritten, err := rewriteSkylineManifest(id, manifestURL, string(body))
	if err != nil {
		util.WriteError(w, http.StatusBadGateway, fmt.Sprintf("rewrite skyline manifest: %v", err))
		return
	}
	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, rewritten)
}

func (s *Server) handleDashboardStreamSkylineSegment(w http.ResponseWriter, r *http.Request) {
	_, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	raw := strings.TrimSpace(r.URL.Query().Get("u"))
	if raw == "" {
		util.WriteError(w, http.StatusBadRequest, "missing segment url")
		return
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, "bad segment url")
		return
	}
	segmentURL := string(decoded)
	if err := validateSkylineSegmentURL(segmentURL); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, segmentURL, nil)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, "bad segment url")
		return
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; stoarama-skyline-proxy/1.0)")
	resp, err := skylineProxyHTTPClient(30 * time.Second).Do(req)
	if err != nil {
		util.WriteError(w, http.StatusBadGateway, fmt.Sprintf("fetch skyline segment: %v", err))
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		util.WriteError(w, http.StatusBadGateway, fmt.Sprintf("skyline segment status=%d", resp.StatusCode))
		return
	}
	contentType := strings.TrimSpace(resp.Header.Get("Content-Type"))
	if contentType == "" {
		contentType = "video/mp2t"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, resp.Body)
}

func fetchSkylineProxyURL(ctx context.Context, rawURL string, maxBytes int64) ([]byte, error) {
	if _, err := netguard.ValidatePublicURL(rawURL); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; stoarama-skyline-proxy/1.0)")
	req.Header.Set("Accept", "application/vnd.apple.mpegurl,application/x-mpegURL,*/*")
	resp, err := skylineProxyHTTPClient(20 * time.Second).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("status=%d", resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, maxBytes))
}

func skylineProxyHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
				Control:   netguard.ControlReject,
			}).DialContext,
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return fmt.Errorf("too many redirects")
			}
			if _, err := netguard.ValidatePublicURL(req.URL.String()); err != nil {
				return err
			}
			return nil
		},
	}
}

func rewriteSkylineManifest(streamID int64, manifestURL, manifest string) (string, error) {
	base, err := url.Parse(manifestURL)
	if err != nil {
		return "", err
	}
	lines := strings.Split(manifest, "\n")
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		u, err := url.Parse(trimmed)
		if err != nil {
			return "", err
		}
		if !u.IsAbs() {
			u = base.ResolveReference(u)
		}
		if err := validateSkylineSegmentURL(u.String()); err != nil {
			return "", err
		}
		encoded := base64.RawURLEncoding.EncodeToString([]byte(u.String()))
		lines[i] = fmt.Sprintf("/api/v1/dashboard/streams/%d/skyline-segment?u=%s", streamID, url.QueryEscape(encoded))
	}
	out := strings.Join(lines, "\n")
	if !strings.HasSuffix(out, "\n") {
		out += "\n"
	}
	return out, nil
}

func validateSkylineSegmentURL(rawURL string) error {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return fmt.Errorf("bad skyline segment url")
	}
	if u.Scheme != "https" {
		return fmt.Errorf("skyline segment must use https")
	}
	host := strings.ToLower(strings.TrimSpace(u.Hostname()))
	if !strings.HasPrefix(host, "hddn") || !strings.HasSuffix(host, ".skylinewebcams.com") {
		return fmt.Errorf("skyline segment host not allowed")
	}
	if !strings.HasSuffix(strings.ToLower(u.Path), ".ts") {
		return fmt.Errorf("skyline segment path not allowed")
	}
	if _, err := netguard.ValidatePublicURL(u.String()); err != nil {
		return err
	}
	return nil
}
