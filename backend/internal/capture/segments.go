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
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	SegmentTargetFPS       = 30
	DefaultSegmentDuration = 30 * time.Second
	// ContinuousSegmentPollInterval is how often CaptureContinuous scans the
	// output dir for newly finalized segments. ffmpeg's segment muxer has no
	// per-segment callback, so a segment is detected as final once a strictly
	// newer segment file has appeared (the muxer moved on and closed its trailer).
	ContinuousSegmentPollInterval = 2 * time.Second
	// continuousShutdownGrace bounds how long CaptureContinuous waits for ffmpeg
	// to exit cleanly after a SIGINT (clean MP4 trailer on the last segment).
	continuousShutdownGrace = 20 * time.Second
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
//
// targetFPS, when non-nil and > 0, normalizes the captured clip to that exact
// frame rate by re-encoding (you cannot change fps with -c copy). Pass nil for
// the Source/native path, which stream-copies and preserves the source fps with
// no re-encode (the cheap default).
func CaptureSegment(ctx context.Context, sourceURL string, duration time.Duration, pinHost string, targetFPS *int) (Segment, error) {
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
	args := buildFFmpegSegmentArgs(sourceURL, outPath, duration, pinHost, targetFPS)
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

// CaptureContinuous records sourceURL gaplessly into back-to-back .mp4 segments
// of clipDuration each, for as long as ctx is live, by holding ONE persistent
// ffmpeg open with the segment muxer (NOT one ffmpeg connect per clip, which
// leaves a reconnect gap at every clip boundary). It reuses the exact input and
// encode forks of CaptureSegment: targetFPS==nil stream-copies (-c copy, cheap,
// gapless, segments cut on input keyframes); a fixed targetFPS re-encodes to that
// exact rate, producing exact clipDuration segments.
//
// Because ffmpeg has no per-segment callback, finalization is detected by polling
// outDir: a segment file is FINAL once a strictly newer segment file has appeared
// (the muxer has moved on and closed the prior trailer). Each finalized segment is
// probed (reusing probeSegment on the OUTPUT file) and handed to onSegment exactly
// once. The segment's StartAt is parsed from its strftime filename (authoritative,
// idempotent, ordered) and EndAt is StartAt+duration.
//
// On ctx cancel (window close) ffmpeg is stopped with SIGINT (clean trailer rather
// than Kill), then one final sweep finalizes the last in-progress segment so no
// captured footage is dropped at the boundary. The caller's onSegment is expected
// to delete the segment file (CleanupSegment) after a successful ingest so outDir
// does not grow unbounded over a long window. onSegment returning an error aborts
// the whole window (SIGINT ffmpeg, return the error).
//
// pinHost mirrors CaptureSegment (HTTP Host override for the IP-pinned path);
// pass "" to let ffmpeg derive Host/SNI from the URL.
func CaptureContinuous(ctx context.Context, sourceURL string, clipDuration time.Duration, pinHost string, targetFPS *int, outDir string, onSegment func(Segment) error) error {
	if strings.TrimSpace(sourceURL) == "" {
		return fmt.Errorf("source_url is empty")
	}
	if clipDuration <= 0 {
		return fmt.Errorf("segment duration must be > 0")
	}
	if strings.TrimSpace(outDir) == "" {
		return fmt.Errorf("outDir is empty")
	}
	if onSegment == nil {
		return fmt.Errorf("onSegment callback is required")
	}

	outPattern := filepath.Join(outDir, "seg-%Y%m%d-%H%M%S.mp4")
	args := buildFFmpegContinuousArgs(sourceURL, outPattern, clipDuration, pinHost, targetFPS)
	cmd := exec.Command(ffmpegBin(), args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start continuous ffmpeg: %w", err)
	}

	// waitErr is filled by the ffmpeg waiter goroutine.
	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()

	processed := make(map[string]bool)
	ticker := time.NewTicker(ContinuousSegmentPollInterval)
	defer ticker.Stop()

	// stopFFmpeg sends SIGINT for a clean trailer, then waits a bounded grace for
	// the process to exit (falling back to Kill so we never hang on a wedged child).
	stopFFmpeg := func() {
		if cmd.Process != nil {
			_ = cmd.Process.Signal(os.Interrupt)
		}
		select {
		case <-waitErr:
		case <-time.After(continuousShutdownGrace):
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			<-waitErr
		}
	}

	// sweepFinal scans outDir and hands every newly-finalized segment to onSegment.
	// finalizeAll=false treats a segment as final only when a strictly newer one
	// exists (steady state); finalizeAll=true treats every unprocessed segment as
	// final (post-SIGINT sweep, when ffmpeg has closed the last trailer).
	sweepFinal := func(finalizeAll bool) error {
		segs, err := sortedSegments(outDir)
		if err != nil {
			return err
		}
		for i, path := range segs {
			if processed[path] {
				continue
			}
			isLast := i == len(segs)-1
			if isLast && !finalizeAll {
				// The newest segment is still being written; leave it for a later poll.
				continue
			}
			seg, err := finalizeSegment(ctx, path)
			if err != nil {
				return err
			}
			processed[path] = true
			if err := onSegment(seg); err != nil {
				return err
			}
		}
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			stopFFmpeg()
			// Final sweep: the last open segment now has a clean trailer.
			if err := sweepFinal(true); err != nil {
				return err
			}
			return nil
		case err := <-waitErr:
			// ffmpeg exited on its own (stream ended or a hard error). Sweep whatever
			// finalized segments remain, then surface the error if it was non-clean.
			if sweepErr := sweepFinal(true); sweepErr != nil {
				return sweepErr
			}
			if err != nil {
				return fmt.Errorf("continuous ffmpeg exited: %w (%s)", err, strings.TrimSpace(stderr.String()))
			}
			return nil
		case <-ticker.C:
			if err := sweepFinal(false); err != nil {
				stopFFmpeg()
				return err
			}
		}
	}
}

// sortedSegments returns the seg-*.mp4 files in outDir sorted chronologically.
// The strftime naming (seg-YYYYMMDD-HHMMSS.mp4) sorts lexically == chronologically.
func sortedSegments(outDir string) ([]string, error) {
	matches, err := filepath.Glob(filepath.Join(outDir, "seg-*.mp4"))
	if err != nil {
		return nil, fmt.Errorf("glob segments: %w", err)
	}
	sort.Strings(matches)
	return matches, nil
}

// finalizeSegment probes a finalized segment file and builds its Segment. The
// StartAt is parsed from the strftime filename (UTC), so the per-segment object
// key the worker derives downstream is deterministic and ordered.
func finalizeSegment(ctx context.Context, path string) (Segment, error) {
	startAt, err := parseSegmentStart(filepath.Base(path))
	if err != nil {
		return Segment{}, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return Segment{}, fmt.Errorf("stat segment: %w", err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return Segment{}, fmt.Errorf("read segment: %w", err)
	}
	sum := sha256.Sum256(body)

	meta, metaErr := probeSegment(ctx, path)
	durationMs := int64(0)
	videoCodec := "h264"
	audioCodec := ""
	audioPresent := false
	var actualFPS *float64
	if metaErr == nil {
		durationMs = meta.DurationMs
		actualFPS = meta.ActualFPS
		if meta.VideoCodec != "" {
			videoCodec = meta.VideoCodec
		}
		audioCodec = meta.AudioCodec
		audioPresent = meta.AudioPresent
	}
	endAt := startAt
	if durationMs > 0 {
		endAt = startAt.Add(time.Duration(durationMs) * time.Millisecond)
	}
	return Segment{
		Path:         path,
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
	}, nil
}

// parseSegmentStart parses the strftime segment filename seg-YYYYMMDD-HHMMSS.mp4
// into its UTC start instant (the wall-clock the muxer opened it at).
func parseSegmentStart(name string) (time.Time, error) {
	trimmed := strings.TrimSuffix(strings.TrimPrefix(name, "seg-"), ".mp4")
	t, err := time.ParseInLocation("20060102-150405", trimmed, time.UTC)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse segment start from %q: %w", name, err)
	}
	return t, nil
}

// buildFFmpegContinuousArgs mirrors buildFFmpegSegmentArgs (same input + encode
// forks) but swaps the single-clip -t for the segment muxer, so one persistent
// ffmpeg writes a finalized .mp4 every clipDuration with no reconnect gap.
func buildFFmpegContinuousArgs(sourceURL string, outPattern string, clipDuration time.Duration, pinHost string, targetFPS *int) []string {
	seconds := strconv.FormatFloat(clipDuration.Seconds(), 'f', -1, 64)
	args := []string{
		"-y",
		"-nostdin",
		"-loglevel", "error",
	}
	args = appendFFmpegHTTPInputArgs(args, sourceURL, true, 10, pinHost)
	args = append(args,
		"-fflags", "+discardcorrupt",
		"-i", sourceURL,
		"-map", "0:v:0",
		"-map", "0:a?",
	)
	if targetFPS != nil && *targetFPS > 0 {
		// Fixed-fps path: re-encode to the chosen rate so segments are exactly
		// clipDuration. Identical to buildFFmpegSegmentArgs's re-encode fork.
		args = append(args,
			"-vf", fmt.Sprintf("fps=%d", *targetFPS),
			"-c:v", "libx264",
			"-preset", "veryfast",
			"-crf", "23",
			"-pix_fmt", "yuv420p",
			"-c:a", "aac",
			"-b:a", "128k",
		)
	} else {
		// Source/native path: stream-copy. Segment cuts land on input keyframes, so
		// each segment is ~clipDuration; this is the cheap, gapless default.
		args = append(args, "-c", "copy")
	}
	args = append(args,
		"-f", "segment",
		"-segment_time", seconds,
		"-reset_timestamps", "1",
		"-segment_format", "mp4",
		"-strftime", "1",
		outPattern,
	)
	return args
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

	// Step 1: record a short clip with -c copy (no decode -> no segfault). The
	// survey path always uses Source/native (nil), so this stays a pure copy.
	segPath := filepath.Join(tmpDir, "segment.mp4")
	segArgs := buildFFmpegSegmentArgs(sourceURL, segPath, SingleFrameSegmentDuration, pinHost, nil)
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

// RemoveSegmentFile deletes ONLY the single segment file, not its parent
// directory. CaptureContinuous shares one output dir that the persistent ffmpeg
// is still writing into, so the per-segment cleanup must remove the finalized
// file alone; removing the dir (as CleanupSegment does for the per-clip path)
// would make ffmpeg fail to open the next segment.
func RemoveSegmentFile(seg Segment) {
	if strings.TrimSpace(seg.Path) == "" {
		return
	}
	_ = os.Remove(seg.Path)
}

func buildFFmpegSegmentArgs(sourceURL string, outPath string, duration time.Duration, pinHost string, targetFPS *int) []string {
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
	)
	if targetFPS != nil && *targetFPS > 0 {
		// Fixed-fps path: normalize the clip to the chosen rate. Changing fps
		// requires a re-encode (-c copy cannot), so transcode video with the
		// canonical `fps` filter (it duplicates frames to upsample, e.g. 10->30,
		// and drops frames to downsample, e.g. 60->15) and re-encode audio so the
		// output container is consistent. `-map 0:a?` keeps audio optional, so a
		// video-only stream still produces a valid file.
		args = append(args,
			"-vf", fmt.Sprintf("fps=%d", *targetFPS),
			"-c:v", "libx264",
			"-preset", "veryfast",
			"-crf", "23",
			"-pix_fmt", "yuv420p",
			"-c:a", "aac",
			"-b:a", "128k",
			outPath,
		)
		return args
	}
	// Source/native path: stream-copy, preserving the source frame rate with no
	// re-encode. This is the cheap default.
	args = append(args,
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
