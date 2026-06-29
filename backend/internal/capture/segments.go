package capture

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	_ "image/jpeg"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	SegmentTargetFPS       = 30
	DefaultSegmentDuration = 30 * time.Second
)

type Segment struct {
	Path         string
	MIMEType     string
	SizeBytes    int64
	SHA256       string
	SourceKind   string
	StartAt      time.Time
	EndAt        time.Time
	DurationMs   int64
	Container    string
	ActualFPS    *float64
	VideoCodec   string
	AudioCodec   string
	AudioPresent bool
	Thumbnail    *SegmentThumbnail
}

type SegmentThumbnail struct {
	Path      string
	MIMEType  string
	SizeBytes int64
	SHA256    string
	Width     int
	Height    int
}

func SegmentCaptureTimeout(duration time.Duration) time.Duration {
	return duration + 90*time.Second
}

// CaptureSegment records a clip from sourceURL. pinHost, when non-empty, is the
// original hostname carried as the HTTP Host header / TLS SNI while sourceURL
// already points at the SSRF-validated literal IP, pinning the ffmpeg socket to
// that address. Pass "" to leave DNS resolution to ffmpeg.
func CaptureSegment(ctx context.Context, sourceURL string, duration time.Duration, pinHost string) (Segment, error) {
	if strings.TrimSpace(sourceURL) == "" {
		return Segment{}, fmt.Errorf("source_url is empty")
	}
	if duration <= 0 {
		return Segment{}, fmt.Errorf("segment duration must be > 0")
	}

	tmpDir, err := os.MkdirTemp("", "capture-segment-*")
	if err != nil {
		return Segment{}, fmt.Errorf("mktemp: %w", err)
	}

	startAt := time.Now().UTC()
	outPath := filepath.Join(tmpDir, "segment.mp4")
	args := buildFFmpegSegmentArgs(sourceURL, outPath, duration, pinHost)
	cmd := exec.CommandContext(ctx, ffmpegBin(), args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		return Segment{}, fmt.Errorf("ffmpeg segment failed: %w (%s)", err, strings.TrimSpace(string(out)))
	}

	info, err := os.Stat(outPath)
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		return Segment{}, fmt.Errorf("stat captured segment: %w", err)
	}
	body, err := os.ReadFile(outPath)
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		return Segment{}, fmt.Errorf("read captured segment: %w", err)
	}
	sum := sha256.Sum256(body)

	meta, metaErr := probeSegment(ctx, outPath)
	endAt := time.Now().UTC()
	durationMs := int64(duration / time.Millisecond)
	videoCodec := "h264"
	audioCodec := ""
	audioPresent := false
	var actualFPS *float64
	if metaErr == nil {
		if meta.DurationMs > 0 {
			durationMs = meta.DurationMs
		}
		actualFPS = meta.ActualFPS
		if meta.VideoCodec != "" {
			videoCodec = meta.VideoCodec
		}
		audioCodec = meta.AudioCodec
		audioPresent = meta.AudioPresent
		if durationMs > 0 {
			endAt = startAt.Add(time.Duration(durationMs) * time.Millisecond)
		}
	}

	thumb, thumbErr := extractSegmentThumbnail(ctx, outPath)
	if thumbErr != nil {
		thumb = nil
	}

	return Segment{
		Path:         outPath,
		MIMEType:     "video/mp4",
		SizeBytes:    info.Size(),
		SHA256:       hex.EncodeToString(sum[:]),
		SourceKind:   "live",
		StartAt:      startAt,
		EndAt:        endAt,
		DurationMs:   durationMs,
		Container:    "mp4",
		ActualFPS:    actualFPS,
		VideoCodec:   videoCodec,
		AudioCodec:   audioCodec,
		AudioPresent: audioPresent,
		Thumbnail:    thumb,
	}, nil
}

// ProbeReachable verifies that sourceURL opens and yields at least one packet
// within ctx's deadline, without writing a file. It is used by the recorder
// create flow to fail fast on an unreachable/unsupported URL. The caller is
// responsible for SSRF-validating sourceURL first; pinHost is an optional HTTP
// Host header override (empty for the hostname path, where ffmpeg derives Host
// and TLS SNI from the URL). It uses the same ffmpeg binary resolution as capture
// so deployments need only vendor ffmpeg.
//
// On failure it always returns a sanitized "stream not reachable" error: a child
// killed by a signal (segfault / SIGKILL on timeout) never leaks the raw
// "signal: segmentation fault (core dumped)" string to the caller, and a normal
// non-zero exit returns the same clean message. The ffmpeg stderr is never
// interpolated, so an IP-rewritten or low-level error can never surface to the UI.
func ProbeReachable(ctx context.Context, sourceURL string, pinHost string) error {
	if strings.TrimSpace(sourceURL) == "" {
		return fmt.Errorf("source_url is empty")
	}
	args := []string{"-nostdin", "-loglevel", "error"}
	args = appendFFmpegHTTPInputArgs(args, sourceURL, false, 0, pinHost)
	args = append(args,
		"-i", sourceURL,
		"-map", "0:v:0",
		"-frames:v", "1",
		"-f", "null",
		"-",
	)
	cmd := exec.CommandContext(ctx, ffmpegBin(), args...)
	if err := cmd.Run(); err != nil {
		return sanitizeProbeError(err)
	}
	return nil
}

// SingleFrameSegmentDuration is the short clip the survey records before pulling
// one frame from it. It is the smallest window that reliably yields a keyframe
// across these streams while keeping the per-stream grab bounded.
const SingleFrameSegmentDuration = 2 * time.Second

// CaptureSingleFrame grabs ONE video frame from a resolved video sourceURL and
// returns it as a JPEG Frame, for the survey's video path.
//
// It does NOT decode the live network stream to a JPEG in one ffmpeg pass: the
// Render static ffmpeg segfaults on that (proven by prod cron logs: "ffmpeg
// single-frame capture failed: signal: segmentation fault" on every hls /
// http_video stream). Instead it runs the recorder's two proven, non-crashing
// steps on this exact ffmpeg/streams: first record a short clip with -c copy
// (the buildFFmpegSegmentArgs path, no decode), then extract one frame from that
// LOCAL file (decode of a local mp4, the operation the recorder runs millions of
// times for thumbnails). This reuses buildFFmpegSegmentArgs and ffmpegBin() so
// no new ffmpeg primitives are introduced.
//
// pinHost, when non-empty, is carried as the HTTP Host header; pass "" to let
// ffmpeg derive Host/SNI from the URL. The caller bounds ctx so a dead stream
// fails fast. On failure the underlying ffmpeg CombinedOutput is wrapped into
// the error so the real stderr is visible to verification.
func CaptureSingleFrame(ctx context.Context, sourceURL string, pinHost string) (Frame, error) {
	if strings.TrimSpace(sourceURL) == "" {
		return Frame{}, fmt.Errorf("source_url is empty")
	}
	tmpDir, err := os.MkdirTemp("", "capture-single-frame-*")
	if err != nil {
		return Frame{}, fmt.Errorf("mktemp: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	// Step 1: record a short clip with -c copy (no decode -> no segfault).
	segPath := filepath.Join(tmpDir, "segment.mp4")
	segArgs := buildFFmpegSegmentArgs(sourceURL, segPath, SingleFrameSegmentDuration, pinHost)
	segCmd := exec.CommandContext(ctx, ffmpegBin(), segArgs...)
	if out, err := segCmd.CombinedOutput(); err != nil {
		return Frame{}, fmt.Errorf("record single-frame segment: %w (%s)", err, strings.TrimSpace(string(out)))
	}

	// Step 2: decode one frame from the LOCAL clip to a JPEG.
	framePath := filepath.Join(tmpDir, "single-frame.jpg")
	frameCmd := exec.CommandContext(ctx,
		ffmpegBin(),
		"-y",
		"-nostdin",
		"-loglevel", "error",
		"-i", segPath,
		"-frames:v", "1",
		"-q:v", "2",
		framePath,
	)
	if out, err := frameCmd.CombinedOutput(); err != nil {
		return Frame{}, fmt.Errorf("extract single frame from segment: %w (%s)", err, strings.TrimSpace(string(out)))
	}
	b, err := os.ReadFile(framePath)
	if err != nil {
		return Frame{}, fmt.Errorf("read single frame: %w", err)
	}
	return buildFrame(b, "image/jpeg", "live")
}

// sanitizeProbeError maps any ffmpeg probe failure to a clean user-facing error.
// It distinguishes a signal-killed child (segfault, or SIGKILL from a probe
// timeout) from a normal non-zero exit, but in neither case does it interpolate
// the raw exec error or ffmpeg stderr, so "signal: segmentation fault (core
// dumped)" can never reach the UI.
func sanitizeProbeError(err error) error {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		if ps := exitErr.ProcessState; ps != nil && !ps.Exited() {
			// Killed by a signal (crash or timeout-driven kill): report cleanly
			// as not opening, without the raw signal string.
			return fmt.Errorf("stream not reachable: stream did not open")
		}
		return fmt.Errorf("stream not reachable")
	}
	// ctx deadline/cancel or a binary-not-found style error: still clean.
	return fmt.Errorf("stream not reachable")
}

func CleanupSegment(seg Segment) {
	if strings.TrimSpace(seg.Path) == "" {
		return
	}
	_ = os.RemoveAll(filepath.Dir(seg.Path))
}

func buildFFmpegSegmentArgs(sourceURL string, outPath string, duration time.Duration, pinHost string) []string {
	seconds := strconv.FormatFloat(duration.Seconds(), 'f', -1, 64)
	args := []string{
		"-y",
		"-nostdin",
		"-loglevel", "error",
	}
	args = appendFFmpegHTTPInputArgs(args, sourceURL, true, 10, pinHost)
	args = append(args,
		"-fflags", "+discardcorrupt",
		"-i", sourceURL,
		"-t", seconds,
		"-map", "0:v:0",
		"-map", "0:a?",
		"-c", "copy",
		outPath,
	)
	return args
}

func extractSegmentThumbnail(ctx context.Context, segmentPath string) (*SegmentThumbnail, error) {
	thumbPath := filepath.Join(filepath.Dir(segmentPath), "thumbnail.jpg")
	thumbCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(thumbCtx,
		ffmpegBin(),
		"-y",
		"-loglevel", "error",
		"-ss", "1",
		"-i", segmentPath,
		"-frames:v", "1",
		"-vf", "scale=240:-1:force_original_aspect_ratio=decrease",
		"-q:v", "8",
		thumbPath,
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("ffmpeg thumbnail failed: %w (%s)", err, strings.TrimSpace(string(out)))
	}
	info, err := os.Stat(thumbPath)
	if err != nil {
		return nil, fmt.Errorf("stat thumbnail: %w", err)
	}
	body, err := os.ReadFile(thumbPath)
	if err != nil {
		return nil, fmt.Errorf("read thumbnail: %w", err)
	}
	sum := sha256.Sum256(body)
	cfg, _, err := image.DecodeConfig(bytes.NewReader(body))
	if err != nil {
		cfg = image.Config{}
	}
	return &SegmentThumbnail{
		Path:      thumbPath,
		MIMEType:  "image/jpeg",
		SizeBytes: info.Size(),
		SHA256:    hex.EncodeToString(sum[:]),
		Width:     cfg.Width,
		Height:    cfg.Height,
	}, nil
}

type ffprobeMeta struct {
	DurationMs   int64
	ActualFPS    *float64
	VideoCodec   string
	AudioCodec   string
	AudioPresent bool
}

func probeSegment(ctx context.Context, path string) (ffprobeMeta, error) {
	probeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(probeCtx,
		"ffprobe",
		"-v", "error",
		"-show_entries", "format=duration:stream=codec_type,codec_name,avg_frame_rate,r_frame_rate",
		"-of", "json",
		path,
	)
	out, err := cmd.Output()
	if err != nil {
		return ffprobeMeta{}, err
	}
	var payload struct {
		Format struct {
			Duration string `json:"duration"`
		} `json:"format"`
		Streams []struct {
			CodecType    string `json:"codec_type"`
			CodecName    string `json:"codec_name"`
			AvgFrameRate string `json:"avg_frame_rate"`
			RFrameRate   string `json:"r_frame_rate"`
		} `json:"streams"`
	}
	if err := json.Unmarshal(out, &payload); err != nil {
		return ffprobeMeta{}, err
	}
	meta := ffprobeMeta{}
	if payload.Format.Duration != "" {
		if secs, err := strconv.ParseFloat(payload.Format.Duration, 64); err == nil && secs > 0 {
			meta.DurationMs = int64(secs * 1000)
		}
	}
	for _, stream := range payload.Streams {
		switch strings.TrimSpace(stream.CodecType) {
		case "video":
			if meta.VideoCodec == "" {
				meta.VideoCodec = strings.TrimSpace(stream.CodecName)
			}
			if meta.ActualFPS == nil {
				meta.ActualFPS = parseFrameRate(strings.TrimSpace(stream.AvgFrameRate))
			}
		case "audio":
			if meta.AudioCodec == "" {
				meta.AudioCodec = strings.TrimSpace(stream.CodecName)
			}
			meta.AudioPresent = true
		}
	}
	return meta, nil
}

func parseFrameRate(raw string) *float64 {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "0/0" {
		return nil
	}
	parts := strings.Split(raw, "/")
	if len(parts) == 1 {
		value, err := strconv.ParseFloat(parts[0], 64)
		if err != nil || value <= 0 {
			return nil
		}
		return &value
	}
	if len(parts) != 2 {
		return nil
	}
	num, errNum := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	den, errDen := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if errNum != nil || errDen != nil || num <= 0 || den <= 0 {
		return nil
	}
	value := num / den
	return &value
}
