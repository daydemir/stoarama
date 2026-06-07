package capture

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"image"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"
)

type Frame struct {
	Bytes      []byte
	MIMEType   string
	Width      int
	Height     int
	SHA256     string
	SizeBytes  int64
	SourceKind string
}

func CaptureFrame(ctx context.Context, sourceURL string) (Frame, error) {
	if sourceURL == "" {
		return Frame{}, fmt.Errorf("source_url is empty")
	}
	if looksLikeImageURL(sourceURL) {
		b, mimeType, err := fetchImage(ctx, sourceURL)
		if err != nil {
			return Frame{}, err
		}
		return buildFrame(b, mimeType, "snapshot_url")
	}
	b, err := captureWithFFmpeg(ctx, sourceURL)
	if err != nil {
		return Frame{}, err
	}
	return buildFrame(b, "image/jpeg", "live")
}

func BuildFrameFromBytes(b []byte, mimeType, sourceKind string) (Frame, error) {
	return buildFrame(b, mimeType, sourceKind)
}

func LooksLikeImageURL(u string) bool {
	return looksLikeImageURL(u)
}

func fetchImage(ctx context.Context, u string) ([]byte, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, "", fmt.Errorf("build request: %w", err)
	}
	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("http get image: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, "", fmt.Errorf("image request failed status=%d", resp.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 25<<20))
	if err != nil {
		return nil, "", fmt.Errorf("read image body: %w", err)
	}
	ct := resp.Header.Get("Content-Type")
	if ct == "" {
		ct = http.DetectContentType(b)
	}
	if !strings.HasPrefix(ct, "image/") {
		return nil, "", fmt.Errorf("content-type is not image: %s", ct)
	}
	return b, ct, nil
}

func ffmpegBin() string {
	if v := strings.TrimSpace(os.Getenv("FFMPEG_BIN")); v != "" {
		return v
	}
	return "ffmpeg"
}

func captureWithFFmpeg(ctx context.Context, sourceURL string) ([]byte, error) {
	tmpDir, err := os.MkdirTemp("", "capture-frame-*")
	if err != nil {
		return nil, fmt.Errorf("mktemp: %w", err)
	}
	defer os.RemoveAll(tmpDir)
	outPath := filepath.Join(tmpDir, "frame.jpg")
	cmd := exec.CommandContext(ctx,
		ffmpegBin(),
		"-y",
		"-loglevel", "error",
		"-i", sourceURL,
		"-frames:v", "1",
		"-q:v", "2",
		outPath,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("ffmpeg capture failed: %w (%s)", err, strings.TrimSpace(string(out)))
	}
	b, err := os.ReadFile(outPath)
	if err != nil {
		return nil, fmt.Errorf("read captured frame: %w", err)
	}
	return b, nil
}

func buildFrame(b []byte, mimeType string, sourceKind string) (Frame, error) {
	cfg, _, err := image.DecodeConfig(bytes.NewReader(b))
	if err != nil {
		return Frame{}, fmt.Errorf("decode image config: %w", err)
	}
	sum := sha256.Sum256(b)
	return Frame{
		Bytes:      b,
		MIMEType:   mimeType,
		Width:      cfg.Width,
		Height:     cfg.Height,
		SHA256:     hex.EncodeToString(sum[:]),
		SizeBytes:  int64(len(b)),
		SourceKind: sourceKind,
	}, nil
}

func looksLikeImageURL(u string) bool {
	raw := strings.TrimSpace(u)
	if raw == "" {
		return false
	}
	lu := strings.ToLower(raw)
	for _, s := range []string{".jpg", ".jpeg", ".png", ".webp"} {
		if strings.Contains(lu, s) {
			return true
		}
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return false
	}
	pathLower := strings.ToLower(strings.TrimSpace(parsed.Path))
	if pathLower == "" {
		return false
	}
	base := path.Base(pathLower)
	switch base {
	case "image", "snapshot", "still":
		return true
	}
	for _, marker := range []string{"/snapshot", "/snapshots/", "/still/", "/image/"} {
		if strings.Contains(pathLower, marker) {
			return true
		}
	}
	return false
}
