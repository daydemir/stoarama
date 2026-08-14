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
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/daydemir/stoarama/backend/internal/surrenderplan"
	"github.com/google/uuid"
)

var errContinuousSegmentDelivery = errors.New("continuous segment delivery failed")

// ErrContinuousSegmentDuplicate lets the recording worker acknowledge a replayed
// file without advancing CaptureContinuous's media clock. The worker owns the
// job-scoped SHA set because it spans ffmpeg reconnect attempts.
var ErrContinuousSegmentDuplicate = errors.New("continuous segment is a duplicate replay")

// ErrContinuousNoOutput identifies watchdog exits eligible for the separately
// fenced frozen-live-edge observer. It says nothing about the playlist itself.
var ErrContinuousNoOutput = errors.New("continuous ffmpeg made no output progress")

// ErrContinuousStopRequired means the server froze the source/config snapshot
// used by this finite producer and required it to stop opening new artifacts.
// It is deliberately distinct from context cancellation: callers must retain
// and account for the namespace returned by the stop barrier.
var ErrContinuousStopRequired = errors.New("continuous producer stop required")

// ContinuousStopBarrier runs only after the entire FFmpeg process group has
// been confirmed stopped. It must atomically move the output namespace out of
// FFmpeg's reach, install a non-directory sentinel at the old pathname, and
// durably acknowledge the exact retained inventory before returning its new
// directory. No callback runs after FFmpeg has been continued.
type ContinuousStopBarrier func(context.Context, string) (string, error)

type continuousNoOutputError struct{ message string }

func (e *continuousNoOutputError) Error() string { return e.message }
func (e *continuousNoOutputError) Unwrap() error { return ErrContinuousNoOutput }

// IsCleanContinuousNoOutput is true only for the standalone watchdog outcome.
// A joined finalization/delivery error must not be reclassified as a frozen
// source, even though errors.Is still exposes ErrContinuousNoOutput within it.
func IsCleanContinuousNoOutput(err error) bool {
	if err == ErrContinuousNoOutput {
		return true
	}
	_, ok := err.(*continuousNoOutputError)
	return ok
}

const (
	SegmentTargetFPS                      = 30
	DefaultSegmentDuration                = 30 * time.Second
	TimestampVersionContinuousSourcePTSV1 = "continuous-source-pts-v1"
	TimestampProbeComplete                = "per_clip_probe_complete"
	TimestampProbeUnknown                 = "per_clip_probe_unknown"
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
	// CaptureAttemptSequence identifies one persistent FFmpeg process. Timestamp
	// continuity is meaningful only inside the same attempt; reconnects are hard
	// generation boundaries even if their numeric timestamps happen to align.
	CaptureAttemptID        string
	TimestampContract       *TimestampContract
	TimestampContractStatus string
	TimestampContractReason string
	// Local phase durations are ephemeral relay diagnostics. They are never sent
	// with clip metadata and contain no paths, URLs, tokens, or error text.
	FinalizeReadDuration  time.Duration
	FinalizeHashDuration  time.Duration
	FinalizeProbeDuration time.Duration
	DeliveryQueuedAt      time.Time
	Thumbnail             *SegmentThumbnail
}

type SegmentThumbnail struct {
	Path      string
	MIMEType  string
	SizeBytes int64
	SHA256    string
	Width     int
	Height    int
}

// TimestampContract is exact, source-copy timing evidence from the finalized
// MP4. Integer timestamps remain in each track's declared time base; consumers
// must never infer seam continuity from wall-clock duration or average FPS.
type TimestampContract struct {
	Version        int                   `json:"version"`
	Mode           string                `json:"mode"`
	AudioSelection string                `json:"audio_selection"`
	Tracks         []TrackTimingContract `json:"tracks"`
}

type TrackTimingContract struct {
	StreamIndex          int    `json:"stream_index"`
	MediaType            string `json:"media_type"`
	TimeBaseNum          int64  `json:"time_base_num"`
	TimeBaseDen          int64  `json:"time_base_den"`
	FirstTimestamp       int64  `json:"first_timestamp"`
	LastTimestamp        int64  `json:"last_timestamp"`
	LastDuration         int64  `json:"last_duration"`
	UnitCount            int64  `json:"unit_count"`
	SampleRate           int64  `json:"sample_rate,omitempty"`
	LastSampleCount      int64  `json:"last_sample_count,omitempty"`
	CodecSignatureSHA256 string `json:"codec_signature_sha256"`
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
	return captureSegmentInDirWithHeaders(ctx, sourceURL, duration, pinHost, targetFPS, tempDir, inputHeaders, true)
}

// CaptureSegmentInDirWithHeadersNoThumbnail is the source-native canary path.
// It skips thumbnail extraction entirely, so no video or image encoding occurs.
func CaptureSegmentInDirWithHeadersNoThumbnail(ctx context.Context, sourceURL string, duration time.Duration, pinHost string, tempDir, inputHeaders string) (Segment, error) {
	return captureSegmentInDirWithHeaders(ctx, sourceURL, duration, pinHost, nil, tempDir, inputHeaders, false)
}

func captureSegmentInDirWithHeaders(ctx context.Context, sourceURL string, duration time.Duration, pinHost string, targetFPS *int, tempDir, inputHeaders string, createThumbnail bool) (Segment, error) {
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

	var thumb *SegmentThumbnail
	if createThumbnail {
		var thumbErr error
		thumb, thumbErr = extractSegmentThumbnail(ctx, outPath)
		if thumbErr != nil {
			thumb = nil
		}
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
	return captureContinuousWithHeadersMode(ctx, sourceURL, clipDuration, pinHost, targetFPS, outDir, onSegment, inputHeaders, continuousStartupTimeout, continuousProgressTimeout(sourceURL, clipDuration), false, nil)
}

func CaptureContinuousWithTimestampContract(ctx context.Context, sourceURL string, clipDuration time.Duration, pinHost string, targetFPS *int, outDir string, onSegment func(Segment) error, inputHeaders string) error {
	if targetFPS != nil {
		return fmt.Errorf("timestamp contract requires native source-copy capture")
	}
	return captureContinuousWithHeadersMode(ctx, sourceURL, clipDuration, pinHost, targetFPS, outDir, onSegment, inputHeaders, continuousStartupTimeout, continuousProgressTimeout(sourceURL, clipDuration), true, nil)
}

// CaptureContinuousWithFinitePlan preserves the one-process source-copy capture
// path while replacing the open-ended segment cadence with the exact finite
// split plan accepted by the server. The plan bounds file cardinality before
// exec; it never changes codec selection.
func CaptureContinuousWithFinitePlan(ctx context.Context, sourceURL string, clipDuration time.Duration, pinHost string, targetFPS *int, outDir string, onSegment func(Segment) error, inputHeaders string, timestampContract bool, plan surrenderplan.Plan) error {
	return CaptureContinuousWithFinitePlanAndStop(ctx, sourceURL, clipDuration, pinHost, targetFPS, outDir, onSegment, inputHeaders, timestampContract, plan, nil, nil)
}

// CaptureContinuousWithFinitePlanAndStop is the finite-plan capture path with
// the append-only source/config stop protocol. stopRequired is separate from
// ctx so a source mutation cannot be mistaken for an ordinary window close.
func CaptureContinuousWithFinitePlanAndStop(ctx context.Context, sourceURL string, clipDuration time.Duration, pinHost string, targetFPS *int, outDir string, onSegment func(Segment) error, inputHeaders string, timestampContract bool, plan surrenderplan.Plan, stopRequired <-chan struct{}, stopBarrier ContinuousStopBarrier) error {
	if targetFPS != nil && timestampContract {
		return fmt.Errorf("timestamp contract requires native source-copy capture")
	}
	if stopRequired != nil && stopBarrier == nil {
		return fmt.Errorf("finite capture stop signal requires a stop barrier")
	}
	return captureContinuousWithHeadersModeAndStop(ctx, sourceURL, clipDuration, pinHost, targetFPS, outDir, onSegment, inputHeaders, continuousStartupTimeout, continuousProgressTimeout(sourceURL, clipDuration), timestampContract, &plan, stopRequired, stopBarrier)
}

func continuousProgressTimeout(sourceURL string, clipDuration time.Duration) time.Duration {
	timeout := clipDuration + 15*time.Second
	if isHLSInputURL(sourceURL) && timeout > 30*time.Second {
		return 30 * time.Second
	}
	return timeout
}

func captureContinuousWithHeaders(ctx context.Context, sourceURL string, clipDuration time.Duration, pinHost string, targetFPS *int, outDir string, onSegment func(Segment) error, inputHeaders string, startupTimeout, progressTimeout time.Duration) error {
	return captureContinuousWithHeadersMode(ctx, sourceURL, clipDuration, pinHost, targetFPS, outDir, onSegment, inputHeaders, startupTimeout, progressTimeout, false, nil)
}

func captureContinuousWithHeadersMode(ctx context.Context, sourceURL string, clipDuration time.Duration, pinHost string, targetFPS *int, outDir string, onSegment func(Segment) error, inputHeaders string, startupTimeout, progressTimeout time.Duration, timestampContract bool, finitePlan *surrenderplan.Plan) error {
	return captureContinuousWithHeadersModeAndStop(ctx, sourceURL, clipDuration, pinHost, targetFPS, outDir, onSegment, inputHeaders, startupTimeout, progressTimeout, timestampContract, finitePlan, nil, nil)
}

func captureContinuousWithHeadersModeAndStop(ctx context.Context, sourceURL string, clipDuration time.Duration, pinHost string, targetFPS *int, outDir string, onSegment func(Segment) error, inputHeaders string, startupTimeout, progressTimeout time.Duration, timestampContract bool, finitePlan *surrenderplan.Plan, stopRequired <-chan struct{}, stopBarrier ContinuousStopBarrier) error {
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
	if finitePlan != nil {
		if finitePlan.ClipDurationSecond != int(clipDuration/time.Second) || finitePlan.ArtifactCount < 1 || finitePlan.ArtifactCount > surrenderplan.MaxArtifacts || finitePlan.DurationMicro <= 0 || len(finitePlan.SplitTimesArgument) > surrenderplan.MaxSegmentTimesArgumentLen {
			return fmt.Errorf("finite capture plan differs from capture parameters")
		}
	}
	err := captureContinuousAttemptWithStop(ctx, sourceURL, clipDuration, pinHost, targetFPS, outDir, onSegment, inputHeaders, startupTimeout, progressTimeout, true, timestampContract, finitePlan, stopRequired, stopBarrier)
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
	return captureContinuousAttemptWithStop(ctx, sourceURL, clipDuration, pinHost, targetFPS, outDir, onSegment, inputHeaders, startupTimeout, progressTimeout, false, timestampContract, finitePlan, stopRequired, stopBarrier)
}

func captureContinuousAttempt(ctx context.Context, sourceURL string, clipDuration time.Duration, pinHost string, targetFPS *int, outDir string, onSegment func(Segment) error, inputHeaders string, startupTimeout, progressTimeout time.Duration, includeAudio, timestampContract bool, finitePlan *surrenderplan.Plan) error {
	return captureContinuousAttemptWithStop(ctx, sourceURL, clipDuration, pinHost, targetFPS, outDir, onSegment, inputHeaders, startupTimeout, progressTimeout, includeAudio, timestampContract, finitePlan, nil, nil)
}

func captureContinuousAttemptWithStop(ctx context.Context, sourceURL string, clipDuration time.Duration, pinHost string, targetFPS *int, outDir string, onSegment func(Segment) error, inputHeaders string, startupTimeout, progressTimeout time.Duration, includeAudio, timestampContract bool, finitePlan *surrenderplan.Plan, stopRequired <-chan struct{}, stopBarrier ContinuousStopBarrier) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	outPattern := filepath.Join(outDir, "seg-%Y%m%d-%H%M%S.mp4")
	captureAttemptID := ""
	if timestampContract {
		captureAttemptID = uuid.NewString()
	}
	args := buildFFmpegContinuousArgsWithHeadersAndAudioAndTimestamps(sourceURL, outPattern, clipDuration, pinHost, targetFPS, inputHeaders, includeAudio, timestampContract)
	if finitePlan != nil {
		args = applyFiniteContinuousPlan(args, *finitePlan)
		argMax, err := platformExecArgumentLimit()
		if err != nil || !surrenderplan.ExecFits(ffmpegBin(), args, os.Environ(), argMax) {
			return fmt.Errorf("finite capture plan does not fit the platform exec argument limit")
		}
	}
	cmd := exec.Command(ffmpegBin(), args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
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
	var stopOnce sync.Once
	stopFFmpeg := func() {
		stopOnce.Do(func() {
			if cmd.Process != nil {
				_ = signalContinuousProcessGroup(cmd.Process.Pid, syscall.SIGINT)
			}
			select {
			case <-waitErr:
			case <-time.After(continuousShutdownGrace):
				if cmd.Process != nil {
					_ = signalContinuousProcessGroup(cmd.Process.Pid, syscall.SIGKILL)
				}
				<-waitErr
			}
		})
	}

	retainedOutputDir := outDir
	stopAtBarrier := func() error {
		if cmd.Process == nil || stopBarrier == nil {
			return fmt.Errorf("continuous stop barrier is unavailable")
		}
		if err := signalContinuousProcessGroup(cmd.Process.Pid, syscall.SIGSTOP); err != nil {
			return fmt.Errorf("stop continuous process group: %w", err)
		}
		if err := waitContinuousProcessStopped(cmd.Process.Pid, 5*time.Second); err != nil {
			_ = signalContinuousProcessGroup(cmd.Process.Pid, syscall.SIGCONT)
			return err
		}
		barrierCtx, cancelBarrier := context.WithTimeout(context.Background(), 30*time.Second)
		movedDir, err := stopBarrier(barrierCtx, outDir)
		cancelBarrier()
		if err != nil {
			_ = signalContinuousProcessGroup(cmd.Process.Pid, syscall.SIGINT)
			_ = signalContinuousProcessGroup(cmd.Process.Pid, syscall.SIGCONT)
			stopFFmpeg()
			return fmt.Errorf("acknowledge continuous stop inventory: %w", err)
		}
		if strings.TrimSpace(movedDir) == "" || movedDir == outDir {
			_ = signalContinuousProcessGroup(cmd.Process.Pid, syscall.SIGINT)
			_ = signalContinuousProcessGroup(cmd.Process.Pid, syscall.SIGCONT)
			stopFFmpeg()
			return fmt.Errorf("continuous stop barrier did not isolate output namespace")
		}
		retainedOutputDir = movedDir
		// SIGINT is queued while every process in the group remains stopped. Only
		// after the namespace ACK is durable may the group continue and reap.
		_ = signalContinuousProcessGroup(cmd.Process.Pid, syscall.SIGINT)
		_ = signalContinuousProcessGroup(cmd.Process.Pid, syscall.SIGCONT)
		stopFFmpeg()
		return nil
	}

	// sweepFinal scans outDir and hands every newly-finalized segment to onSegment.
	// finalizeAll=false treats a segment as final only when a strictly newer one
	// exists (steady state); finalizeAll=true treats every unprocessed segment as
	// final (post-SIGINT sweep, when ffmpeg has closed the last trailer).
	sweepFinal := func(probeCtx context.Context, finalizeAll bool) error {
		segs, err := sortedSegments(retainedOutputDir)
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
			seg, err := finalizeSegmentWithTimestampContract(probeCtx, path, clipDuration, timestampContract)
			if err != nil {
				return err
			}
			seg.CaptureAttemptID = captureAttemptID
			if err := deliverContinuousSegment(processed, path, seg, &nextStart, onSegment); err != nil {
				return err
			}
		}
		return nil
	}

	for {
		select {
		case <-stopRequired:
			if err := stopAtBarrier(); err != nil {
				stopFFmpeg()
				return err
			}
			finalizeCtx, cancelFinalize := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancelFinalize()
			if err := sweepFinal(finalizeCtx, true); err != nil {
				return errors.Join(ErrContinuousStopRequired, err)
			}
			return ErrContinuousStopRequired
		case <-ctx.Done():
			stopFFmpeg()
			// Final sweep: the last open segment now has a clean trailer.
			finalizeCtx, cancelFinalize := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancelFinalize()
			if err := sweepFinal(finalizeCtx, true); err != nil {
				return err
			}
			return nil
		case err := <-waitErr:
			// ffmpeg exited on its own (stream ended or a hard error). Sweep whatever
			// finalized segments remain, then surface the error if it was non-clean.
			sweepErr := sweepFinal(ctx, true)
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
				finalizeCtx, cancelFinalize := context.WithTimeout(context.Background(), 30*time.Second)
				if sweepErr := sweepFinal(finalizeCtx, true); sweepErr != nil {
					cancelFinalize()
					return errors.Join(err, fmt.Errorf("finalize stalled output: %w", sweepErr))
				}
				cancelFinalize()
				return err
			}
			if err := sweepFinal(ctx, false); err != nil {
				stopFFmpeg()
				if !errors.Is(err, errContinuousSegmentDelivery) {
					finalizeCtx, cancelFinalize := context.WithTimeout(context.Background(), 30*time.Second)
					if finalErr := sweepFinal(finalizeCtx, true); finalErr != nil {
						cancelFinalize()
						return errors.Join(err, fmt.Errorf("finalize after sweep failure: %w", finalErr))
					}
					cancelFinalize()
				}
				return err
			}
		}
	}
}

var readExecArgumentLimit = func() (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "/usr/bin/getconf", "ARG_MAX").Output()
	if err != nil || len(output) > 32 {
		return 0, fmt.Errorf("read ARG_MAX")
	}
	value, err := strconv.Atoi(strings.TrimSpace(string(output)))
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("invalid ARG_MAX")
	}
	return value, nil
}

var signalContinuousProcessGroup = func(pid int, signal syscall.Signal) error {
	if pid <= 0 {
		return fmt.Errorf("invalid continuous process id")
	}
	return syscall.Kill(-pid, signal)
}

var readContinuousProcessGroupStates = func(pid int) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "/bin/ps", "-axo", "pgid=,state=").Output()
	if err != nil {
		return nil, err
	}
	var states []byte
	for _, line := range strings.Split(string(output), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		pgid, parseErr := strconv.Atoi(fields[0])
		if parseErr == nil && pgid == pid && fields[1] != "" {
			states = append(states, fields[1][0])
		}
	}
	if len(states) == 0 {
		return nil, fmt.Errorf("continuous process group is empty")
	}
	return states, nil
}

func waitContinuousProcessStopped(pid int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		states, err := readContinuousProcessGroupStates(pid)
		if err == nil {
			allStopped := true
			for _, state := range states {
				if state != 'T' && state != 't' {
					allStopped = false
					break
				}
			}
			if allStopped {
				return nil
			}
		}
		if !time.Now().Before(deadline) {
			if err != nil {
				return fmt.Errorf("confirm continuous process group stopped: %w", err)
			}
			return fmt.Errorf("continuous process group did not stop")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func platformExecArgumentLimit() (int, error) {
	return readExecArgumentLimit()
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
			return &continuousNoOutputError{message: fmt.Sprintf("continuous ffmpeg startup stalled: no output for %s", startupTimeout)}
		}
		return nil
	}
	if now.Sub(lastProgressAt) >= progressTimeout {
		return &continuousNoOutputError{message: fmt.Sprintf("continuous ffmpeg progress stalled: no output growth for %s", progressTimeout)}
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
	return finalizeSegmentWithTimestampContract(ctx, path, fallbackSpan, false)
}

// RecoverContinuousSegment reconstructs the exact finalized-media metadata a
// recorder needs to finish an upload-only recovery grant after its capture
// process crashed. It never launches ffmpeg or opens a source URL.
func RecoverContinuousSegment(ctx context.Context, path string, fallbackSpan time.Duration) (Segment, error) {
	return finalizeSegmentWithTimestampContract(ctx, path, fallbackSpan, true)
}

// RecoverContinuousSegmentFile derives recovery metadata from one already-open
// no-follow file identity.  The descriptor is passed to ffprobe as fd 3, so no
// pathname lookup can substitute bytes between inventory, hashing, and probes.
func RecoverContinuousSegmentFile(ctx context.Context, file *os.File, leaf string, fallbackSpan time.Duration) (Segment, error) {
	if file == nil || filepath.Base(leaf) != leaf {
		return Segment{}, fmt.Errorf("invalid recovery segment file")
	}
	startAt, err := parseSegmentStart(leaf)
	if err != nil {
		return Segment{}, err
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 {
		return Segment{}, fmt.Errorf("stat recovery segment: %w", err)
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return Segment{}, err
	}
	readStarted := time.Now()
	h := sha256.New()
	written, err := io.Copy(h, io.LimitReader(file, info.Size()+1))
	if err != nil || written != info.Size() {
		return Segment{}, fmt.Errorf("read recovery segment: %w", err)
	}
	readDuration := time.Since(readStarted)
	hashStarted := time.Now()
	sha := hex.EncodeToString(h.Sum(nil))
	hashDuration := time.Since(hashStarted)
	probeStarted := time.Now()
	meta, metaErr := probeSegmentFile(ctx, file)
	contract, contractErr := probeTimestampContractFile(ctx, file)
	probeDuration := time.Since(probeStarted)
	status, reason := TimestampProbeComplete, ""
	if contractErr != nil {
		contract, status, reason = nil, TimestampProbeUnknown, timestampContractErrorCode(contractErr)
	}
	durationMs := meta.DurationMs
	if durationMs <= 0 {
		durationMs = fallbackSpan.Milliseconds()
	}
	videoCodec, audioCodec, audioPresent := meta.VideoCodec, meta.AudioCodec, meta.AudioPresent
	if videoCodec == "" {
		videoCodec = "h264"
	}
	if metaErr != nil {
		meta = ffprobeMeta{VideoCodec: videoCodec, AudioCodec: audioCodec}
	}
	if status == TimestampProbeComplete {
		audioPresent = timestampContractHasAudio(contract)
	}
	return Segment{Path: file.Name(), SourceKind: "live", StartAt: startAt, EndAt: startAt.Add(time.Duration(durationMs) * time.Millisecond), DurationMs: durationMs,
		SizeBytes: info.Size(), SHA256: sha, MIMEType: "video/mp4", Container: "mp4", VideoCodec: videoCodec,
		AudioCodec: audioCodec, AudioPresent: audioPresent, ActualFPS: meta.ActualFPS, VideoWidth: meta.VideoWidth,
		VideoHeight: meta.VideoHeight, FinalizeReadDuration: readDuration, FinalizeHashDuration: hashDuration, FinalizeProbeDuration: probeDuration,
		TimestampContract: contract, TimestampContractStatus: status, TimestampContractReason: reason}, nil
}

func finalizeSegmentWithTimestampContract(ctx context.Context, path string, fallbackSpan time.Duration, timestampContractEnabled bool) (Segment, error) {
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
	readStarted := time.Now()
	body, err := os.ReadFile(path)
	if err != nil {
		return Segment{}, fmt.Errorf("read segment: %w", err)
	}
	readDuration := time.Since(readStarted)
	hashStarted := time.Now()
	sum := sha256.Sum256(body)
	hashDuration := time.Since(hashStarted)

	probeStarted := time.Now()
	meta, metaErr := probeSegment(ctx, path)
	var timestampContract *TimestampContract
	timestampStatus, timestampReason := "", ""
	if timestampContractEnabled {
		var timestampErr error
		timestampContract, timestampErr = probeTimestampContract(ctx, path)
		timestampStatus = TimestampProbeComplete
		if timestampErr != nil {
			timestampContract, timestampStatus, timestampReason = nil, TimestampProbeUnknown, timestampContractErrorCode(timestampErr)
		}
	}
	probeDuration := time.Since(probeStarted)
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
	// A COMPLETE timestamp contract is derived from the exact finalized bytes
	// and is authoritative about the output track set. The ordinary metadata
	// probe is best-effort; if it misses audio while the richer probe proves an
	// audio track, preserve the clip and keep ingest metadata coherent with the
	// immutable contract instead of rejecting valid footage downstream.
	if timestampStatus == TimestampProbeComplete {
		audioPresent = timestampContractHasAudio(timestampContract)
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
		Path:                    path,
		MIMEType:                "video/mp4",
		SizeBytes:               info.Size(),
		FinalizeReadDuration:    readDuration,
		FinalizeHashDuration:    hashDuration,
		FinalizeProbeDuration:   probeDuration,
		SHA256:                  hex.EncodeToString(sum[:]),
		SourceKind:              "live",
		StartAt:                 startAt,
		EndAt:                   endAt,
		DurationMs:              durationMs,
		Container:               "mp4",
		ActualFPS:               actualFPS,
		VideoCodec:              videoCodec,
		AudioCodec:              audioCodec,
		AudioPresent:            audioPresent,
		VideoWidth:              videoWidth,
		VideoHeight:             videoHeight,
		TimestampContract:       timestampContract,
		TimestampContractStatus: timestampStatus,
		TimestampContractReason: timestampReason,
	}, nil
}

func timestampContractHasAudio(contract *TimestampContract) bool {
	if contract == nil {
		return false
	}
	for _, track := range contract.Tracks {
		if track.MediaType == "audio" {
			return true
		}
	}
	return false
}

func timestampContractErrorCode(err error) string {
	text := err.Error()
	switch {
	case strings.Contains(text, "terminal duration"):
		return "missing_terminal_duration"
	case strings.Contains(text, "sample count"):
		return "missing_audio_sample_count"
	case strings.Contains(text, "time base"):
		return "invalid_time_base"
	case strings.Contains(text, "output exceeds"):
		return "probe_output_limit"
	default:
		return "probe_unavailable"
	}
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
	return buildFFmpegContinuousArgsWithHeadersAndAudioAndTimestamps(sourceURL, outPattern, clipDuration, pinHost, targetFPS, inputHeaders, includeAudio, false)
}

func buildFFmpegContinuousArgsWithHeadersAndAudioAndTimestamps(sourceURL string, outPattern string, clipDuration time.Duration, pinHost string, targetFPS *int, inputHeaders string, includeAudio, timestampContractEnabled bool) []string {
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
		if timestampContractEnabled {
			// The v1 contract carries exactly one optional audio timing domain.
			// Never capture extra tracks that its COMPLETE evidence cannot represent.
			args = append(args, "-map", "0:a:0?")
		} else {
			args = append(args, "-map", "0:a?")
		}
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
	)
	if timestampContractEnabled {
		// Preserve the persistent process's muxed source-copy timestamp domain.
		// Never normalize each file independently: that destroys seam evidence.
		args = append(args, "-reset_timestamps", "0", "-avoid_negative_ts", "disabled")
	} else {
		// This is the deployed legacy byte path. Keep it exact until a job is
		// explicitly admitted to the timestamp-contract canary.
		args = append(args, "-reset_timestamps", "1")
	}
	args = append(args,
		"-segment_format", "mp4",
		"-strftime", "1",
		outPattern,
	)
	return args
}

func applyFiniteContinuousPlan(args []string, plan surrenderplan.Plan) []string {
	bounded := make([]string, 0, len(args)+4)
	insertedTimestampOrigin := false
	for index := 0; index < len(args); index++ {
		if args[index] == "-segment_time" && index+1 < len(args) {
			bounded = append(bounded, "-segment_times", plan.SplitTimesArgument)
			index++
			continue
		}
		if !insertedTimestampOrigin && args[index] == "-i" {
			bounded = append(bounded, "-copyts", "-start_at_zero")
			insertedTimestampOrigin = true
		}
		if index == len(args)-1 {
			bounded = append(bounded, "-t", canonicalMicroseconds(plan.DurationMicro))
		}
		bounded = append(bounded, args[index])
	}
	return bounded
}

func canonicalMicroseconds(value int64) string {
	seconds, micros := value/1_000_000, value%1_000_000
	if micros == 0 {
		return strconv.FormatInt(seconds, 10)
	}
	fraction := strings.TrimRight(fmt.Sprintf("%06d", micros), "0")
	return strconv.FormatInt(seconds, 10) + "." + fraction
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

// ExtractSegmentThumbnail returns a bounded decoded-frame proof from finalized
// bytes. Callers own the returned file and must remove it with their probe temp
// directory. Capture bytes are never changed.
func ExtractSegmentThumbnail(ctx context.Context, segmentPath string) (*SegmentThumbnail, error) {
	return extractSegmentThumbnail(ctx, segmentPath)
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

// SegmentFileMetadata is the server-verifiable native media contract read from
// finalized bytes. It deliberately excludes paths and source identifiers.
type SegmentFileMetadata struct {
	DurationMs   int64
	ActualFPS    *float64
	VideoCodec   string
	AudioCodec   string
	AudioPresent bool
	VideoWidth   int
	VideoHeight  int
}

// InspectSegmentFile validates and returns typed native facts from exact local
// bytes. Admission code uses this after an exact object-generation download;
// worker-authored ffprobe facts are not authority.
func InspectSegmentFile(ctx context.Context, path string) (SegmentFileMetadata, error) {
	meta, err := probeSegment(ctx, path)
	if err != nil {
		return SegmentFileMetadata{}, fmt.Errorf("ffprobe: %w", err)
	}
	if meta.DurationMs <= 0 || strings.TrimSpace(meta.VideoCodec) == "" {
		return SegmentFileMetadata{}, fmt.Errorf("ffprobe returned incomplete media facts")
	}
	return SegmentFileMetadata{
		DurationMs: meta.DurationMs, ActualFPS: meta.ActualFPS,
		VideoCodec: meta.VideoCodec, AudioCodec: meta.AudioCodec,
		AudioPresent: meta.AudioPresent, VideoWidth: meta.VideoWidth, VideoHeight: meta.VideoHeight,
	}, nil
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

// ValidateSegmentDecode reads the captured file through FFmpeg's strict decoder
// into a null sink. It creates no output media and performs no re-encoding.
func ValidateSegmentDecode(ctx context.Context, path string) error {
	meta, err := probeSegment(ctx, path)
	if err != nil {
		return fmt.Errorf("ffprobe before strict decode: %w", err)
	}
	if strings.TrimSpace(meta.VideoCodec) == "" {
		return fmt.Errorf("strict decode requires a video stream")
	}
	decodeCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(decodeCtx, ffmpegBin(),
		"-v", "error", "-xerror", "-err_detect", "explode", "-i", path,
		"-map", "0:v:0", "-map", "0:a?", "-f", "null", "-")
	if err := runFFmpegHealthCommand(cmd); err != nil {
		return fmt.Errorf("ffmpeg strict decode: %w", err)
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
	return probeSegmentCommand(ctx, path, nil)
}

func probeSegmentFile(ctx context.Context, file *os.File) (ffprobeMeta, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return ffprobeMeta{}, err
	}
	return probeSegmentCommand(ctx, "/dev/fd/3", file)
}

func probeSegmentCommand(ctx context.Context, path string, file *os.File) (ffprobeMeta, error) {
	probeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(probeCtx,
		ffprobeBin(),
		"-v", "error",
		"-show_entries", "format=duration:stream=codec_type,codec_name,avg_frame_rate,r_frame_rate,width,height",
		"-of", "json",
		path,
	)
	if file != nil {
		cmd.ExtraFiles = []*os.File{file}
	}
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

const timestampProbeOutputLimit = 16 << 20

type timestampProbeOutput struct {
	buf bytes.Buffer
}

func (w *timestampProbeOutput) Write(p []byte) (int, error) {
	if w.buf.Len()+len(p) > timestampProbeOutputLimit {
		return 0, fmt.Errorf("timestamp probe output exceeds limit")
	}
	return w.buf.Write(p)
}

// probeTimestampContract derives the immutable seam evidence from the exact
// finalized bytes. Decoded best-effort timestamps are used for presentation
// order (packet PTS order is not presentation order with B-frames); audio keeps
// its own sample-domain end evidence.
func probeTimestampContract(ctx context.Context, path string) (*TimestampContract, error) {
	return probeTimestampContractCommand(ctx, path, nil)
}

func probeTimestampContractFile(ctx context.Context, file *os.File) (*TimestampContract, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return probeTimestampContractCommand(ctx, "/dev/fd/3", file)
}

func probeTimestampContractCommand(ctx context.Context, path string, file *os.File) (*TimestampContract, error) {
	// A clean window shutdown cancels capture before the final muxer trailer is
	// swept. The finalized immutable bytes still require bounded provenance.
	probeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(probeCtx, ffprobeBin(), "-v", "error", "-show_frames", "-show_streams", "-show_data",
		"-show_entries", "stream=index,codec_type,codec_name,codec_tag_string,profile,level,width,height,pix_fmt,time_base,extradata,sample_rate,channels,channel_layout:frame=stream_index,media_type,best_effort_timestamp,pkt_dts,duration,pkt_duration,nb_samples",
		"-of", "json", path)
	if file != nil {
		cmd.ExtraFiles = []*os.File{file}
	}
	var out timestampProbeOutput
	cmd.Stdout = &out
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	var payload struct {
		Streams []struct {
			Index         int    `json:"index"`
			CodecType     string `json:"codec_type"`
			CodecName     string `json:"codec_name"`
			CodecTag      string `json:"codec_tag_string"`
			Profile       string `json:"profile"`
			PixFmt        string `json:"pix_fmt"`
			TimeBase      string `json:"time_base"`
			ExtraData     string `json:"extradata"`
			SampleRate    string `json:"sample_rate"`
			ChannelLayout string `json:"channel_layout"`
			Level         int    `json:"level"`
			Width         int    `json:"width"`
			Height        int    `json:"height"`
			Channels      int    `json:"channels"`
		} `json:"streams"`
		Frames []struct {
			StreamIndex         int         `json:"stream_index"`
			MediaType           string      `json:"media_type"`
			BestEffortTimestamp json.Number `json:"best_effort_timestamp"`
			PacketDTS           json.Number `json:"pkt_dts"`
			Duration            json.Number `json:"duration"`
			PacketDuration      json.Number `json:"pkt_duration"`
			NBSamples           int64       `json:"nb_samples"`
		} `json:"frames"`
	}
	if err := json.Unmarshal(out.buf.Bytes(), &payload); err != nil {
		return nil, err
	}
	videoStreams, audioStreams := 0, 0
	for _, stream := range payload.Streams {
		switch stream.CodecType {
		case "video":
			videoStreams++
		case "audio":
			audioStreams++
		default:
			return nil, fmt.Errorf("unsupported output stream type")
		}
	}
	if videoStreams != 1 || audioStreams > 1 {
		return nil, fmt.Errorf("unsupported output stream cardinality")
	}
	contract := &TimestampContract{Version: 1, Mode: "muxed_source_copy", AudioSelection: "first_optional"}
	for _, mediaType := range []string{"video", "audio"} {
		streamPos := -1
		for i := range payload.Streams {
			if payload.Streams[i].CodecType == mediaType {
				streamPos = i
				break
			}
		}
		if streamPos < 0 {
			continue
		}
		s := payload.Streams[streamPos]
		num, den, err := parsePositiveRational(s.TimeBase)
		if err != nil {
			return nil, err
		}
		track := TrackTimingContract{StreamIndex: s.Index, MediaType: mediaType, TimeBaseNum: num, TimeBaseDen: den}
		if mediaType == "audio" {
			track.SampleRate, err = strconv.ParseInt(strings.TrimSpace(s.SampleRate), 10, 64)
			if err != nil || track.SampleRate <= 0 {
				return nil, fmt.Errorf("invalid audio sample rate")
			}
		}
		parts := []string{mediaType, s.CodecName, s.CodecTag, s.Profile, s.PixFmt,
			strconv.Itoa(s.Level), strconv.Itoa(s.Width), strconv.Itoa(s.Height), strconv.Itoa(s.Channels),
			s.ChannelLayout, s.ExtraData, s.SampleRate}
		var signature strings.Builder
		for _, part := range parts {
			fmt.Fprintf(&signature, "%d:%s|", len(part), part)
		}
		sum := sha256.Sum256([]byte(signature.String()))
		track.CodecSignatureSHA256 = hex.EncodeToString(sum[:])
		for _, f := range payload.Frames {
			if f.StreamIndex != s.Index || f.MediaType != mediaType {
				continue
			}
			pts, err := strconv.ParseInt(string(f.BestEffortTimestamp), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("missing %s presentation timestamp", mediaType)
			}
			duration, err := timestampFrameDuration(f.Duration, f.PacketDuration)
			if err != nil {
				return nil, fmt.Errorf("missing %s terminal duration: %w", mediaType, err)
			}
			if track.UnitCount == 0 {
				track.FirstTimestamp = pts
			} else if pts < track.LastTimestamp {
				return nil, fmt.Errorf("nonmonotonic decoded %s presentation timestamps", mediaType)
			}
			track.LastTimestamp, track.LastDuration = pts, duration
			track.UnitCount++
			if mediaType == "audio" {
				if f.NBSamples <= 0 {
					return nil, fmt.Errorf("missing audio sample count")
				}
				track.LastSampleCount = f.NBSamples
			}
		}
		if track.UnitCount == 0 {
			return nil, fmt.Errorf("timestamp probe returned no %s frames", mediaType)
		}
		contract.Tracks = append(contract.Tracks, track)
	}
	if len(contract.Tracks) == 0 || contract.Tracks[0].MediaType != "video" {
		return nil, fmt.Errorf("timestamp probe returned no video track")
	}
	return contract, nil
}

// timestampFrameDuration accepts the decoded-frame field emitted by current
// ffprobe (duration) and the legacy name (pkt_duration). If both are present,
// they must describe the same positive integer tick count; ambiguous evidence
// remains UNKNOWN rather than becoming a false complete contract.
func timestampFrameDuration(duration, packetDuration json.Number) (int64, error) {
	parse := func(name string, raw json.Number) (int64, bool, error) {
		text := strings.TrimSpace(string(raw))
		if text == "" {
			return 0, false, nil
		}
		value, err := strconv.ParseInt(text, 10, 64)
		if err != nil || value <= 0 {
			return 0, true, fmt.Errorf("invalid %s", name)
		}
		return value, true, nil
	}
	decoded, hasDecoded, err := parse("duration", duration)
	if err != nil {
		return 0, err
	}
	legacy, hasLegacy, err := parse("pkt_duration", packetDuration)
	if err != nil {
		return 0, err
	}
	if !hasDecoded && !hasLegacy {
		return 0, fmt.Errorf("duration absent")
	}
	if hasDecoded && hasLegacy && decoded != legacy {
		return 0, fmt.Errorf("duration fields conflict")
	}
	if hasDecoded {
		return decoded, nil
	}
	return legacy, nil
}

func parsePositiveRational(raw string) (int64, int64, error) {
	parts := strings.Split(strings.TrimSpace(raw), "/")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid time base")
	}
	n, errN := strconv.ParseInt(parts[0], 10, 64)
	d, errD := strconv.ParseInt(parts[1], 10, 64)
	if errN != nil || errD != nil || n <= 0 || d <= 0 {
		return 0, 0, fmt.Errorf("invalid time base")
	}
	return n, d, nil
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
