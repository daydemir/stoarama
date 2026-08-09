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
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

var errContinuousSegmentDelivery = errors.New("continuous segment delivery failed")

// ErrContinuousSegmentDuplicate lets the recording worker acknowledge a replayed
// file without advancing CaptureContinuous's media clock. The worker owns the
// job-scoped SHA set because it spans ffmpeg reconnect attempts.
var ErrContinuousSegmentDuplicate = errors.New("continuous segment is a duplicate replay")

const (
	SegmentTargetFPS       = 30
	DefaultSegmentDuration = 30 * time.Second
	// ContinuousSegmentPollInterval is how often CaptureContinuous scans the
	// output dir for newly finalized segments. ffmpeg's segment muxer has no
	// per-segment callback, so a segment is detected as final once a strictly
	// newer segment file has appeared (the muxer moved on and closed its trailer).
	ContinuousSegmentPollInterval = 2 * time.Second
	// continuousStartupTimeout bounds how long a live ffmpeg child may produce no
	// output bytes. A process can remain alive after its source has stalled, so
	// process liveness alone is not capture progress.
	continuousStartupTimeout = 30 * time.Second
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
	VideoWidth   int
	VideoHeight  int
	// CaptureSequence is assigned by the recording worker across every ffmpeg
	// reconnect in one job. It is not media metadata; it preserves the recorder's
	// authoritative concatenation order even when a source's wall-clock labels
	// jump or overlap.
	CaptureSequence int64
	Thumbnail       *SegmentThumbnail
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
	return CaptureSegmentWithHeaders(ctx, sourceURL, duration, pinHost, targetFPS, "")
}

func CaptureSegmentWithHeaders(ctx context.Context, sourceURL string, duration time.Duration, pinHost string, targetFPS *int, inputHeaders string) (Segment, error) {
	return CaptureSegmentInDirWithHeaders(ctx, sourceURL, duration, pinHost, targetFPS, "", inputHeaders)
}

// CaptureSegmentInDirWithHeaders is CaptureSegmentWithHeaders with an explicit
// parent directory. Relays use their persistent app-owned temp root so a crash
// can be scavenged on restart; an empty parent preserves the cloud worker's
// OS-temporary behavior.
func CaptureSegmentInDirWithHeaders(ctx context.Context, sourceURL string, duration time.Duration, pinHost string, targetFPS *int, tempDir, inputHeaders string) (Segment, error) {
	if strings.TrimSpace(sourceURL) == "" {
		return Segment{}, fmt.Errorf("source_url is empty")
	}
	if duration <= 0 {
		return Segment{}, fmt.Errorf("segment duration must be > 0")
	}

	tmpDir, err := os.MkdirTemp(tempDir, "capture-segment-*")
	if err != nil {
		return Segment{}, fmt.Errorf("mktemp: %w", err)
	}

	startAt := time.Now().UTC()
	outPath := filepath.Join(tmpDir, "segment.mp4")
	args := buildFFmpegSegmentArgsWithHeaders(sourceURL, outPath, duration, pinHost, targetFPS, inputHeaders)
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
	videoWidth, videoHeight := 0, 0
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
		videoWidth, videoHeight = meta.VideoWidth, meta.VideoHeight
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
		VideoWidth:   videoWidth,
		VideoHeight:  videoHeight,
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
// ORDERING CONTRACT: onSegment is called from this one goroutine, serially and in
// media-timeline order, and the StartAt/EndAt chaining (nextStart) is completed
// before each call. A caller MAY therefore hand the segment to its own upload pool
// and return before delivery finishes: concurrent, out-of-order completion cannot
// affect the timeline. Such a caller must report a deferred delivery failure on a
// LATER onSegment call (this loop only learns about failures through the return
// value) and must join its outstanding deliveries after CaptureContinuous returns.
//
// pinHost mirrors CaptureSegment (HTTP Host override for the IP-pinned path);
// pass "" to let ffmpeg derive Host/SNI from the URL.
func CaptureContinuous(ctx context.Context, sourceURL string, clipDuration time.Duration, pinHost string, targetFPS *int, outDir string, onSegment func(Segment) error) error {
	return CaptureContinuousWithHeaders(ctx, sourceURL, clipDuration, pinHost, targetFPS, outDir, onSegment, "")
}

func CaptureContinuousWithHeaders(ctx context.Context, sourceURL string, clipDuration time.Duration, pinHost string, targetFPS *int, outDir string, onSegment func(Segment) error, inputHeaders string) error {
	return captureContinuousWithHeaders(ctx, sourceURL, clipDuration, pinHost, targetFPS, outDir, onSegment, inputHeaders, continuousStartupTimeout, continuousProgressTimeout(sourceURL, clipDuration))
}

func continuousProgressTimeout(sourceURL string, clipDuration time.Duration) time.Duration {
	timeout := clipDuration + 15*time.Second
	if isHLSInputURL(sourceURL) && timeout > 30*time.Second {
		return 30 * time.Second
	}
	return timeout
}

func captureContinuousWithHeaders(ctx context.Context, sourceURL string, clipDuration time.Duration, pinHost string, targetFPS *int, outDir string, onSegment func(Segment) error, inputHeaders string, startupTimeout, progressTimeout time.Duration) error {
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
	if startupTimeout <= 0 || progressTimeout <= 0 {
		return fmt.Errorf("continuous watchdog timeouts must be > 0")
	}
	err := captureContinuousAttempt(ctx, sourceURL, clipDuration, pinHost, targetFPS, outDir, onSegment, inputHeaders, startupTimeout, progressTimeout, true)
	if !isMalformedAudioMuxError(err) {
		return err
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return errors.Join(err, ctxErr)
	}
	clean, cleanupErr := removeZeroLengthContinuousSegments(outDir)
	if cleanupErr != nil {
		return errors.Join(err, cleanupErr)
	}
	if !clean {
		// Never change stream selection after FFmpeg produced media. That could mix
		// audio-bearing and video-only files in one attempt or duplicate footage.
		return err
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return errors.Join(err, ctxErr)
	}
	// Some HLS feeds advertise AAC but never provide a sample rate. MP4 cannot
	// write that track and exits before producing any video. Retry once without
	// audio; video remains a lossless stream copy and healthy audio is preserved
	// on every source that did not hit this exact muxer failure.
	return captureContinuousAttempt(ctx, sourceURL, clipDuration, pinHost, targetFPS, outDir, onSegment, inputHeaders, startupTimeout, progressTimeout, false)
}

func captureContinuousAttempt(ctx context.Context, sourceURL string, clipDuration time.Duration, pinHost string, targetFPS *int, outDir string, onSegment func(Segment) error, inputHeaders string, startupTimeout, progressTimeout time.Duration, includeAudio bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	outPattern := filepath.Join(outDir, "seg-%Y%m%d-%H%M%S.mp4")
	args := buildFFmpegContinuousArgsWithHeadersAndAudio(sourceURL, outPattern, clipDuration, pinHost, targetFPS, inputHeaders, includeAudio)
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
	// nextStart chains segment start instants along the CONTINUOUS media timeline:
	// the first finalized segment anchors at its strftime open instant (the true
	// wall-clock start of capture), and every later segment starts exactly where
	// the previous one ended (prev.Start + prev media duration). This reflects the
	// gapless reality of the single persistent muxer (it writes every decoded
	// packet into exactly one back-to-back segment) and avoids a phantom gap from
	// using each file's whole-second wall-clock strftime label as the anchor, which
	// drifts when a live source delivers slightly off real-time.
	var nextStart time.Time
	startedAt := time.Now()
	lastProgressAt := startedAt
	lastOutputSizes := map[string]int64{}
	var sawProgress bool
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
			seg, err := finalizeSegment(ctx, path, clipDuration)
			if err != nil {
				return err
			}
			if err := deliverContinuousSegment(processed, path, seg, &nextStart, onSegment); err != nil {
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
			sweepErr := sweepFinal(true)
			if err != nil {
				return errors.Join(
					fmt.Errorf("continuous ffmpeg exited: %w (%s)", err, strings.TrimSpace(stderr.String())),
					sweepErr,
				)
			}
			return sweepErr
		case <-ticker.C:
			outputSizes, err := continuousOutputSizes(outDir)
			if err != nil {
				stopFFmpeg()
				return err
			}
			now := time.Now()
			if continuousOutputAdvanced(lastOutputSizes, outputSizes) {
				lastProgressAt = now
				sawProgress = true
			}
			lastOutputSizes = outputSizes
			if err := continuousWatchdogError(now, startedAt, lastProgressAt, sawProgress, startupTimeout, progressTimeout); err != nil {
				stopFFmpeg()
				if sweepErr := sweepFinal(true); sweepErr != nil {
					return errors.Join(err, fmt.Errorf("finalize stalled output: %w", sweepErr))
				}
				return err
			}
			if err := sweepFinal(false); err != nil {
				stopFFmpeg()
				if !errors.Is(err, errContinuousSegmentDelivery) {
					if finalErr := sweepFinal(true); finalErr != nil {
						return errors.Join(err, fmt.Errorf("finalize after sweep failure: %w", finalErr))
					}
				}
				return err
			}
		}
	}
}

func isMalformedAudioMuxError(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "sample rate not set") &&
		strings.Contains(text, "could not write header (incorrect codec parameters ?): invalid argument")
}

// removeZeroLengthContinuousSegments makes the one safe retry case explicit:
// no usable media may exist. It removes only empty files left by FFmpeg's failed
// header write and refuses fallback if any segment contains bytes.
func removeZeroLengthContinuousSegments(outDir string) (bool, error) {
	paths, err := filepath.Glob(filepath.Join(outDir, "seg-*.mp4"))
	if err != nil {
		return false, fmt.Errorf("glob failed continuous output: %w", err)
	}
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return false, fmt.Errorf("stat failed continuous output: %w", err)
		}
		if info.Size() != 0 {
			return false, nil
		}
	}
	for _, path := range paths {
		if err := os.Remove(path); err != nil {
			return false, fmt.Errorf("remove empty continuous output: %w", err)
		}
	}
	return true, nil
}

func deliverContinuousSegment(processed map[string]bool, path string, segment Segment, nextStart *time.Time, deliver func(Segment) error) error {
	// A persistent segment muxer writes packets to these files in capture order,
	// making probed media durations the authoritative intra-attempt timeline.
	// Never jump backward to a strftime wall-clock label: a DVR-backed HLS tail
	// can be consumed faster than real time, and the old 90-second re-anchor
	// manufactured overlaps between otherwise sequential files. Retaining every
	// unique media second can put this logical media timeline ahead of wall time;
	// the recording-health drift signal keeps that separate semantic issue visible.
	if !nextStart.IsZero() {
		segment.StartAt = *nextStart
		segment.EndAt = segment.StartAt.Add(time.Duration(segment.DurationMs) * time.Millisecond)
	}
	if err := deliver(segment); err != nil {
		if errors.Is(err, ErrContinuousSegmentDuplicate) {
			processed[path] = true
			return nil
		}
		return fmt.Errorf("%w: %w", errContinuousSegmentDelivery, err)
	}
	processed[path] = true
	if segment.DurationMs > 0 {
		*nextStart = segment.EndAt
	} else {
		// Keep the next clip's millisecond idempotency key distinct even when
		// ffprobe cannot determine this finalized segment's duration.
		*nextStart = segment.StartAt.Add(time.Millisecond)
	}
	return nil
}

func continuousOutputSizes(outDir string) (map[string]int64, error) {
	entries, err := os.ReadDir(outDir)
	if err != nil {
		return nil, fmt.Errorf("read continuous output: %w", err)
	}
	sizes := make(map[string]int64, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "seg-") || filepath.Ext(entry.Name()) != ".mp4" {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("stat continuous output %s: %w", entry.Name(), err)
		}
		sizes[entry.Name()] = info.Size()
	}
	return sizes, nil
}

func continuousOutputAdvanced(previous, current map[string]int64) bool {
	for path, size := range current {
		if prior, ok := previous[path]; size > 0 && (!ok || size > prior) {
			return true
		}
	}
	return false
}

func continuousWatchdogError(now, startedAt, lastProgressAt time.Time, sawProgress bool, startupTimeout, progressTimeout time.Duration) error {
	if !sawProgress {
		if now.Sub(startedAt) >= startupTimeout {
			return fmt.Errorf("continuous ffmpeg startup stalled: no output for %s", startupTimeout)
		}
		return nil
	}
	if now.Sub(lastProgressAt) >= progressTimeout {
		return fmt.Errorf("continuous ffmpeg progress stalled: no output growth for %s", progressTimeout)
	}
	return nil
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
func finalizeSegment(ctx context.Context, path string, fallbackSpan time.Duration) (Segment, error) {
	startAt, err := parseSegmentStart(filepath.Base(path))
	if err != nil {
		return Segment{}, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return Segment{}, fmt.Errorf("stat segment: %w", err)
	}
	if info.Size() == 0 {
		return Segment{}, fmt.Errorf("finalized segment is empty")
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
	videoWidth, videoHeight := 0, 0
	if metaErr == nil {
		durationMs = meta.DurationMs
		actualFPS = meta.ActualFPS
		if meta.VideoCodec != "" {
			videoCodec = meta.VideoCodec
		}
		audioCodec = meta.AudioCodec
		audioPresent = meta.AudioPresent
		videoWidth, videoHeight = meta.VideoWidth, meta.VideoHeight
	}
	// A probe miss (or a degenerate probe reporting <=0 duration) must not collapse
	// the segment to zero width: fall back to the muxer's expected cut span so the
	// clip carries a real duration/end and the continuous timeline stays gapless.
	if durationMs <= 0 && fallbackSpan > 0 {
		durationMs = fallbackSpan.Milliseconds()
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
		VideoWidth:   videoWidth,
		VideoHeight:  videoHeight,
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
	return buildFFmpegContinuousArgsWithHeaders(sourceURL, outPattern, clipDuration, pinHost, targetFPS, "")
}

func buildFFmpegContinuousArgsWithHeaders(sourceURL string, outPattern string, clipDuration time.Duration, pinHost string, targetFPS *int, inputHeaders string) []string {
	return buildFFmpegContinuousArgsWithHeadersAndAudio(sourceURL, outPattern, clipDuration, pinHost, targetFPS, inputHeaders, true)
}

func buildFFmpegContinuousArgsWithHeadersAndAudio(sourceURL string, outPattern string, clipDuration time.Duration, pinHost string, targetFPS *int, inputHeaders string, includeAudio bool) []string {
	seconds := strconv.FormatFloat(clipDuration.Seconds(), 'f', -1, 64)
	args := []string{
		"-y",
		"-nostdin",
		"-loglevel", "error",
	}
	args = appendFFmpegHTTPInputArgsWithHeaders(args, sourceURL, true, 10, pinHost, inputHeaders)
	args = appendHLSLiveEdgeInputArgs(args, sourceURL)
	args = append(args,
		"-fflags", "+discardcorrupt",
		"-i", sourceURL,
		"-map", "0:v:0",
	)
	if includeAudio {
		args = append(args, "-map", "0:a?")
	}
	if targetFPS != nil && *targetFPS > 0 {
		// Fixed-fps path: re-encode to the chosen rate so segments are exactly
		// clipDuration. Identical to buildFFmpegSegmentArgs's re-encode fork.
		args = append(args,
			"-vf", fmt.Sprintf("fps=%d", *targetFPS),
			"-c:v", "libx264",
			"-preset", "veryfast",
			"-crf", "23",
			"-pix_fmt", "yuv420p",
		)
		if includeAudio {
			args = append(args, "-c:a", "aac", "-b:a", "128k")
		}
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

// appendHLSLiveEdgeInputArgs keeps a restarted continuous recorder at the live
// edge instead of FFmpeg's default three-segment DVR offset. That default lets
// an HLS source's already-published tail drain faster than wall time after each
// restart. The recorder then chains those media durations onto wall-clock file
// labels and eventually has to jump backward, creating overlapping clip times.
// The continuous HLS progress watchdog is capped at 30 seconds so a persistently
// dead or expired signed URL returns to recordingworker's outer loop, which
// re-resolves a fresh URL, rather than retrying the stale URL indefinitely.
// Do not set reconnect_at_eof for HLS. EOF is the normal end of each finite
// playlist HTTP response; reconnecting that response prevents FFmpeg's HLS
// demuxer from completing the manifest and can produce zero media forever.
//
// These are input-scoped HLS/HTTP options, so they must appear before -i and
// must not be sent to ordinary HTTP video inputs. A resolved manifest's URL
// path is the reliable discriminator. Signed/API endpoints may instead declare
// their format explicitly in the query.
func appendHLSLiveEdgeInputArgs(args []string, sourceURL string) []string {
	if !isHLSInputURL(sourceURL) {
		return args
	}
	return append(args,
		"-live_start_index", "-1",
	)
}

func isHLSManifestURL(sourceURL string) bool {
	u, err := url.Parse(strings.TrimSpace(sourceURL))
	if err != nil || !strings.EqualFold(filepath.Ext(u.Path), ".m3u8") {
		return false
	}
	return true
}

func isHLSInputURL(sourceURL string) bool {
	if isHLSManifestURL(sourceURL) {
		return true
	}
	u, err := url.Parse(strings.TrimSpace(sourceURL))
	if err != nil {
		return false
	}
	format := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(u.Query().Get("format"))), ".")
	return format == "m3u8" || format == "hls"
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
	return ProbeReachableWithHeaders(ctx, sourceURL, pinHost, "")
}

func ProbeReachableWithHeaders(ctx context.Context, sourceURL string, pinHost string, inputHeaders string) error {
	if strings.TrimSpace(sourceURL) == "" {
		return fmt.Errorf("source_url is empty")
	}
	args := []string{"-nostdin", "-loglevel", "error"}
	args = appendFFmpegHTTPInputArgsWithHeaders(args, sourceURL, false, 0, pinHost, inputHeaders)
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
	return CaptureSingleFrameWithHeaders(ctx, sourceURL, pinHost, "")
}

func CaptureSingleFrameWithHeaders(ctx context.Context, sourceURL string, pinHost string, inputHeaders string) (Frame, error) {
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
	segArgs := buildFFmpegSegmentArgsWithHeaders(sourceURL, segPath, SingleFrameSegmentDuration, pinHost, nil, inputHeaders)
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
	return buildFFmpegSegmentArgsWithHeaders(sourceURL, outPath, duration, pinHost, targetFPS, "")
}

func buildFFmpegSegmentArgsWithHeaders(sourceURL string, outPath string, duration time.Duration, pinHost string, targetFPS *int, inputHeaders string) []string {
	seconds := strconv.FormatFloat(duration.Seconds(), 'f', -1, 64)
	args := []string{
		"-y",
		"-nostdin",
		"-loglevel", "error",
	}
	args = appendFFmpegHTTPInputArgsWithHeaders(args, sourceURL, true, 10, pinHost, inputHeaders)
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
	VideoWidth   int
	VideoHeight  int
}

// ValidateSegmentFile proves that a stored clip is a decodable media container
// with a positive duration and a video stream. It intentionally performs no
// transcoding and therefore cannot change recording quality; the health sweeper
// uses it on a downloaded sample to catch missing/truncated MP4 trailers and
// other files that exist in object storage but cannot be stitched or used by CV.
func ValidateSegmentFile(ctx context.Context, path string) error {
	meta, err := probeSegment(ctx, path)
	if err != nil {
		return fmt.Errorf("ffprobe: %w", err)
	}
	if meta.DurationMs <= 0 {
		return fmt.Errorf("ffprobe returned no positive duration")
	}
	if strings.TrimSpace(meta.VideoCodec) == "" {
		return fmt.Errorf("ffprobe returned no video stream")
	}
	return nil
}

// ValidateConcatFiles creates the same quality-preserving stream-copy MP4 that
// clip exports document, then strictly decodes that derived file. Inputs are
// read-only and never transformed. Success proves both muxability and decoder
// consumption of the actual long-video artifact users will create.
func ValidateConcatFiles(ctx context.Context, paths []string) error {
	if len(paths) < 2 {
		return fmt.Errorf("concat validation requires at least two clips")
	}
	manifest, err := os.CreateTemp("", "stoarama-concat-*.txt")
	if err != nil {
		return fmt.Errorf("create concat manifest: %w", err)
	}
	manifestPath := manifest.Name()
	defer os.Remove(manifestPath)
	for _, path := range paths {
		// Paths come from os.CreateTemp and cannot contain a newline. Single quotes
		// are escaped using the concat demuxer's documented shell-like syntax.
		if strings.ContainsAny(path, "\r\n") {
			_ = manifest.Close()
			return fmt.Errorf("concat path contains a newline")
		}
		escaped := strings.ReplaceAll(path, "'", "'\\''")
		if _, err := fmt.Fprintf(manifest, "file '%s'\n", escaped); err != nil {
			_ = manifest.Close()
			return fmt.Errorf("write concat manifest: %w", err)
		}
	}
	if err := manifest.Close(); err != nil {
		return fmt.Errorf("close concat manifest: %w", err)
	}
	output, err := os.CreateTemp("", "stoarama-stitched-*.mp4")
	if err != nil {
		return fmt.Errorf("create stitched output: %w", err)
	}
	outputPath := output.Name()
	_ = output.Close()
	_ = os.Remove(outputPath)
	defer os.Remove(outputPath)

	probeCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	copyCmd := exec.CommandContext(probeCtx, ffmpegBin(),
		"-v", "error", "-xerror", "-err_detect", "explode",
		"-f", "concat", "-safe", "0", "-i", manifestPath,
		"-map", "0:v:0", "-map", "0:a?", "-c", "copy",
		"-avoid_negative_ts", "make_zero", "-movflags", "+faststart", "-y", outputPath)
	if err := runFFmpegHealthCommand(copyCmd); err != nil {
		return fmt.Errorf("ffmpeg lossless stitch: %w", err)
	}
	decodeCmd := exec.CommandContext(probeCtx, ffmpegBin(),
		"-v", "error", "-xerror", "-err_detect", "explode", "-i", outputPath,
		"-map", "0:v:0", "-map", "0:a?", "-f", "null", "-")
	if err := runFFmpegHealthCommand(decodeCmd); err != nil {
		return fmt.Errorf("ffmpeg stitched decode: %w", err)
	}
	return nil
}

const ffmpegHealthOutputLimit = 16 << 10

type boundedCommandOutput struct {
	buf       bytes.Buffer
	truncated bool
}

func (w *boundedCommandOutput) Write(p []byte) (int, error) {
	written := len(p)
	remaining := ffmpegHealthOutputLimit - w.buf.Len()
	if remaining > 0 {
		if len(p) > remaining {
			p = p[:remaining]
			w.truncated = true
		}
		_, _ = w.buf.Write(p)
	} else {
		w.truncated = true
	}
	return written, nil
}

func runFFmpegHealthCommand(cmd *exec.Cmd) error {
	var output boundedCommandOutput
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Run(); err != nil {
		text := strings.TrimSpace(output.buf.String())
		if output.truncated {
			text += " …[truncated]"
		}
		return fmt.Errorf("%w (%s)", err, text)
	}
	return nil
}

func probeSegment(ctx context.Context, path string) (ffprobeMeta, error) {
	probeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(probeCtx,
		ffprobeBin(),
		"-v", "error",
		"-show_entries", "format=duration:stream=codec_type,codec_name,avg_frame_rate,r_frame_rate,width,height",
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
			Width        int    `json:"width"`
			Height       int    `json:"height"`
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
				meta.VideoWidth = stream.Width
				meta.VideoHeight = stream.Height
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

func ffprobeBin() string {
	if bin := strings.TrimSpace(os.Getenv("FFPROBE_BIN")); bin != "" {
		return bin
	}
	return "ffprobe"
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
