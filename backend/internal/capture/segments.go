package capture

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	SegmentTargetFPS = 30
	SegmentDuration  = 30 * time.Second
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

func SegmentCaptureTimeout() time.Duration {
	return SegmentDuration + 90*time.Second
}

func CaptureSegment(ctx context.Context, sourceURL string) (Segment, error) {
	if strings.TrimSpace(sourceURL) == "" {
		return Segment{}, fmt.Errorf("source_url is empty")
	}

	tmpDir, err := os.MkdirTemp("", "capture-segment-*")
	if err != nil {
		return Segment{}, fmt.Errorf("mktemp: %w", err)
	}

	startAt := time.Now().UTC()
	outPath := filepath.Join(tmpDir, "segment.mp4")
	sourceFPS := probeSourceFrameRate(ctx, sourceURL)
	args := buildFFmpegSegmentArgs(sourceURL, outPath, sourceFPS == nil)
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
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
	durationMs := int64(SegmentDuration / time.Millisecond)
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

func CleanupSegment(seg Segment) {
	if strings.TrimSpace(seg.Path) == "" {
		return
	}
	_ = os.RemoveAll(filepath.Dir(seg.Path))
}

func buildFFmpegSegmentArgs(sourceURL string, outPath string, normalizeUnknownFPS bool) []string {
	seconds := strconv.FormatFloat(SegmentDuration.Seconds(), 'f', -1, 64)
	args := []string{
		"-y",
		"-nostdin",
		"-loglevel", "error",
	}
	args = appendFFmpegHTTPInputArgs(args, sourceURL, true, 10)
	args = append(args,
		"-fflags", "+discardcorrupt",
		"-i", sourceURL,
		"-t", seconds,
		"-map", "0:v:0",
		"-map", "0:a?",
	)
	if normalizeUnknownFPS {
		args = append(args, "-vf", fmt.Sprintf("fps=%d", SegmentTargetFPS))
	}
	args = append(args,
		"-c:v", "libx264",
		"-preset", "ultrafast",
		"-pix_fmt", "yuv420p",
		"-c:a", "aac",
		"-b:a", "96k",
		outPath,
	)
	return args
}

func probeSourceFrameRate(ctx context.Context, sourceURL string) *float64 {
	probeCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()
	cmd := exec.CommandContext(probeCtx,
		"ffprobe",
		"-v", "error",
		"-select_streams", "v:0",
		"-show_entries", "stream=avg_frame_rate,r_frame_rate",
		"-of", "json",
		sourceURL,
	)
	out, err := cmd.Output()
	if err != nil {
		return nil
	}
	var payload struct {
		Streams []sourceFrameRateProbeStream `json:"streams"`
	}
	if err := json.Unmarshal(out, &payload); err != nil {
		return nil
	}
	return sourceFrameRateFromProbe(payload.Streams)
}

type sourceFrameRateProbeStream struct {
	AvgFrameRate string `json:"avg_frame_rate"`
	RFrameRate   string `json:"r_frame_rate"`
}

func sourceFrameRateFromProbe(streams []sourceFrameRateProbeStream) *float64 {
	for _, stream := range streams {
		if fps := parseFrameRate(stream.AvgFrameRate); fps != nil {
			return fps
		}
	}
	return nil
}

func extractSegmentThumbnail(ctx context.Context, segmentPath string) (*SegmentThumbnail, error) {
	thumbPath := filepath.Join(filepath.Dir(segmentPath), "thumbnail.jpg")
	thumbCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(thumbCtx,
		"ffmpeg",
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
				frameRate := strings.TrimSpace(stream.AvgFrameRate)
				if frameRate == "" {
					frameRate = strings.TrimSpace(stream.RFrameRate)
				}
				meta.ActualFPS = parseFrameRate(frameRate)
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
