package capture

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

type youtubeLiveAdapter struct{}

type hlsLiveAdapter struct{}

type imagePollAdapter struct{}

type ffmpegDirectAdapter struct{}

func (a *youtubeLiveAdapter) Mode() Mode { return ModeYouTubeLive }

func (a *youtubeLiveAdapter) Supports(spec StreamSpec) bool {
	u := strings.TrimSpace(spec.StreamURL)
	if u == "" {
		u = strings.TrimSpace(spec.SourcePageURL)
	}
	return isYouTubeURL(u)
}

func (a *youtubeLiveAdapter) Resolve(ctx context.Context, spec StreamSpec) (ResolvedSource, error) {
	u := strings.TrimSpace(spec.StreamURL)
	if u == "" {
		u = strings.TrimSpace(spec.SourcePageURL)
	}
	if u == "" {
		return ResolvedSource{}, fmt.Errorf("youtube_live requires source_url or source_page_url")
	}
	resolved, err := resolveYouTubeStreamURL(ctx, u)
	if err != nil {
		return ResolvedSource{}, err
	}
	return ResolvedSource{URL: resolved, IsImage: false, RefreshAfter: 10 * time.Minute, Mode: ModeYouTubeLive}, nil
}

func (a *youtubeLiveAdapter) StartSession(ctx context.Context, spec StreamSpec, src ResolvedSource, emit EmitFrameFunc) error {
	return startFFmpegSession(ctx, spec, src, emit)
}

func (a *hlsLiveAdapter) Mode() Mode { return ModeHLSLive }

func (a *hlsLiveAdapter) Supports(spec StreamSpec) bool {
	u := strings.ToLower(strings.TrimSpace(spec.StreamURL))
	if u == "" {
		u = strings.ToLower(strings.TrimSpace(spec.SourcePageURL))
	}
	return strings.Contains(u, "!hls") || strings.Contains(u, ".m3u8")
}

func (a *hlsLiveAdapter) Resolve(ctx context.Context, spec StreamSpec) (ResolvedSource, error) {
	u := strings.TrimSpace(spec.StreamURL)
	if u == "" {
		u = strings.TrimSpace(spec.SourcePageURL)
	}
	if u == "" {
		return ResolvedSource{}, fmt.Errorf("hls_live requires source_url or source_page_url")
	}
	for range 3 {
		lower := strings.ToLower(u)
		if strings.Contains(lower, ".m3u8") {
			break
		}
		if !strings.Contains(lower, "!hls") && !strings.HasPrefix(lower, "http://") && !strings.HasPrefix(lower, "https://") {
			break
		}
		resolved, ok, err := resolveIndirectURL(ctx, u, 20*time.Second)
		if err != nil {
			return ResolvedSource{}, err
		}
		if !ok || strings.TrimSpace(resolved) == "" || strings.TrimSpace(resolved) == strings.TrimSpace(u) {
			break
		}
		u = resolved
	}
	if !strings.Contains(strings.ToLower(u), ".m3u8") {
		return ResolvedSource{}, fmt.Errorf("hls_live expected m3u8 URL after resolve")
	}
	return ResolvedSource{URL: u, IsImage: false, RefreshAfter: 30 * time.Minute, Mode: ModeHLSLive}, nil
}

func (a *hlsLiveAdapter) StartSession(ctx context.Context, spec StreamSpec, src ResolvedSource, emit EmitFrameFunc) error {
	return startFFmpegSession(ctx, spec, src, emit)
}

func (a *imagePollAdapter) Mode() Mode { return ModeImagePoll }

func (a *imagePollAdapter) Supports(spec StreamSpec) bool {
	u := strings.ToLower(strings.TrimSpace(spec.StreamURL))
	if u == "" {
		u = strings.ToLower(strings.TrimSpace(spec.SourcePageURL))
	}
	return looksLikeImageURL(u)
}

func (a *imagePollAdapter) Resolve(_ context.Context, spec StreamSpec) (ResolvedSource, error) {
	u := strings.TrimSpace(spec.StreamURL)
	if u == "" {
		u = strings.TrimSpace(spec.SourcePageURL)
	}
	if u == "" {
		return ResolvedSource{}, fmt.Errorf("image_poll requires source_url or source_page_url")
	}
	return ResolvedSource{URL: u, IsImage: true, RefreshAfter: 0, Mode: ModeImagePoll}, nil
}

func (a *imagePollAdapter) StartSession(ctx context.Context, spec StreamSpec, src ResolvedSource, emit EmitFrameFunc) error {
	intervalSec := spec.CaptureIntervalSec
	if intervalSec <= 0 {
		intervalSec = GetConfigInt(spec.CaptureConfig, "poll_interval_sec", 1)
	}
	if intervalSec <= 0 {
		intervalSec = 1
	}
	interval := time.Duration(intervalSec) * time.Second
	if err := emitImageFrame(ctx, src.URL, emit); err != nil {
		return err
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := emitImageFrame(ctx, src.URL, emit); err != nil {
				return err
			}
		}
	}
}

func (a *ffmpegDirectAdapter) Mode() Mode { return ModeFFmpegDirect }

func (a *ffmpegDirectAdapter) Supports(spec StreamSpec) bool {
	u := strings.TrimSpace(spec.StreamURL)
	if u == "" {
		u = strings.TrimSpace(spec.SourcePageURL)
	}
	return u != ""
}

func (a *ffmpegDirectAdapter) Resolve(_ context.Context, spec StreamSpec) (ResolvedSource, error) {
	u := strings.TrimSpace(spec.StreamURL)
	if u == "" {
		u = strings.TrimSpace(spec.SourcePageURL)
	}
	if u == "" {
		return ResolvedSource{}, fmt.Errorf("ffmpeg_direct requires source_url or source_page_url")
	}
	if looksLikeImageURL(u) {
		return ResolvedSource{}, fmt.Errorf("ffmpeg_direct requires a video stream URL; use image_poll for still-image URLs")
	}
	return ResolvedSource{URL: u, IsImage: false, RefreshAfter: 0, Mode: ModeFFmpegDirect}, nil
}

func (a *ffmpegDirectAdapter) StartSession(ctx context.Context, spec StreamSpec, src ResolvedSource, emit EmitFrameFunc) error {
	return startFFmpegSession(ctx, spec, src, emit)
}

func emitImageFrame(ctx context.Context, u string, emit EmitFrameFunc) error {
	b, mimeType, err := fetchImage(ctx, u)
	if err != nil {
		return err
	}
	frame, err := buildFrame(b, mimeType, "snapshot_url")
	if err != nil {
		return err
	}
	return emit(ctx, frame, time.Now().UTC())
}

func startFFmpegSession(ctx context.Context, spec StreamSpec, src ResolvedSource, emit EmitFrameFunc) error {
	captureIntervalSec := spec.CaptureIntervalSec
	if captureIntervalSec <= 0 {
		captureIntervalSec = 1
	}
	targetFPS := SegmentTargetFPS
	maxFrameBytes := spec.MaxFrameBytes
	if maxFrameBytes <= 0 {
		maxFrameBytes = 25 << 20
	}
	stallTimeout := frameStallTimeout(spec, captureIntervalSec)
	pacer := newFramePacer(time.Duration(captureIntervalSec) * time.Second)
	sessionCtx, cancelSession := context.WithCancel(ctx)
	defer cancelSession()
	cmdArgs := buildFFmpegSessionArgs(spec, src.URL, targetFPS)
	cmd := exec.CommandContext(sessionCtx, "ffmpeg", cmdArgs...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("ffmpeg stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("ffmpeg stderr pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("ffmpeg start: %w", err)
	}

	lb := &limitedBuffer{max: 4096}
	var stderrWG sync.WaitGroup
	stderrWG.Add(1)
	go func() {
		defer stderrWG.Done()
		_, _ = io.Copy(lb, stderr)
	}()

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- cmd.Wait()
	}()

	reader := bufio.NewReaderSize(stdout, 64*1024)
	readCh := make(chan frameReadResult, 1)
	go func() {
		for {
			frameBytes, err := readJPEGFrame(reader, maxFrameBytes)
			select {
			case readCh <- frameReadResult{frameBytes: frameBytes, err: err}:
			case <-sessionCtx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	stallTimer := time.NewTimer(stallTimeout)
	defer stallTimer.Stop()
	resetStallTimer := func() {
		if !stallTimer.Stop() {
			select {
			case <-stallTimer.C:
			default:
			}
		}
		stallTimer.Reset(stallTimeout)
	}

	for {
		select {
		case <-ctx.Done():
			cancelSession()
			<-waitCh
			stderrWG.Wait()
			return ctx.Err()
		case <-stallTimer.C:
			cancelSession()
			waitErr := <-waitCh
			stderrWG.Wait()
			stderrText := strings.TrimSpace(lb.String())
			if waitErr != nil {
				if stderrText != "" {
					return fmt.Errorf("ffmpeg stalled for %s; ffmpeg exited: %w (%s)", stallTimeout, waitErr, stderrText)
				}
				return fmt.Errorf("ffmpeg stalled for %s; ffmpeg exited: %w", stallTimeout, waitErr)
			}
			if stderrText != "" {
				return fmt.Errorf("ffmpeg stalled for %s (%s)", stallTimeout, stderrText)
			}
			return fmt.Errorf("ffmpeg stalled for %s", stallTimeout)
		case rr := <-readCh:
			if rr.err != nil {
				waitErr := <-waitCh
				stderrWG.Wait()
				stderrText := strings.TrimSpace(lb.String())
				if rr.err == io.EOF {
					if waitErr != nil {
						if stderrText != "" {
							return fmt.Errorf("ffmpeg exited: %w (%s)", waitErr, stderrText)
						}
						return fmt.Errorf("ffmpeg exited: %w", waitErr)
					}
					return io.EOF
				}
				if waitErr != nil {
					if stderrText != "" {
						return fmt.Errorf("read frame: %w; ffmpeg exited: %w (%s)", rr.err, waitErr, stderrText)
					}
					return fmt.Errorf("read frame: %w; ffmpeg exited: %w", rr.err, waitErr)
				}
				if stderrText != "" {
					return fmt.Errorf("read frame: %w (%s)", rr.err, stderrText)
				}
				return fmt.Errorf("read frame: %w", rr.err)
			}

			resetStallTimer()
			frame, err := BuildFrameFromBytes(rr.frameBytes, "image/jpeg", "live")
			if err != nil {
				continue
			}
			if err := pacer.Wait(ctx); err != nil {
				return err
			}
			if err := emit(ctx, frame, time.Now().UTC()); err != nil {
				return err
			}
		}
	}
}

func buildFFmpegSessionArgs(spec StreamSpec, srcURL string, targetFPS int) []string {
	cfg := spec.CaptureConfig
	threads := GetConfigInt(cfg, "ffmpeg_threads", envInt("CAPTURE_FFMPEG_THREADS", 1))
	if threads < 1 {
		threads = 1
	}
	if threads > 8 {
		threads = 8
	}

	jpegQ := GetConfigInt(cfg, "ffmpeg_jpeg_quality", envInt("CAPTURE_FFMPEG_JPEG_Q", 2))
	if jpegQ < 1 {
		jpegQ = 1
	}
	if jpegQ > 31 {
		jpegQ = 31
	}

	hwaccel := strings.TrimSpace(GetConfigString(cfg, "ffmpeg_hwaccel", strings.TrimSpace(os.Getenv("CAPTURE_FFMPEG_HWACCEL"))))
	if strings.EqualFold(hwaccel, "none") {
		hwaccel = ""
	}

	useReconnect := GetConfigBool(cfg, "ffmpeg_reconnect", envBool("CAPTURE_FFMPEG_RECONNECT", true))
	reconnectDelayMax := GetConfigInt(cfg, "ffmpeg_reconnect_delay_max_sec", envInt("CAPTURE_FFMPEG_RECONNECT_DELAY_MAX_SEC", 2))
	if reconnectDelayMax < 1 {
		reconnectDelayMax = 1
	}
	if reconnectDelayMax > 60 {
		reconnectDelayMax = 60
	}

	args := []string{
		"-nostdin",
		"-hide_banner",
		"-loglevel", "error",
		"-threads", strconv.Itoa(threads),
		"-fflags", "+nobuffer",
	}
	if hwaccel != "" {
		args = append(args, "-hwaccel", hwaccel)
	}
	args = appendFFmpegHTTPInputArgs(args, srcURL, useReconnect, reconnectDelayMax, "")

	args = append(args,
		"-i", srcURL,
		"-map", "0:v:0",
		"-vf", fmt.Sprintf("fps=%d", targetFPS),
		"-q:v", strconv.Itoa(jpegQ),
		"-an",
		"-sn",
		"-dn",
		"-f", "image2pipe",
		"-vcodec", "mjpeg",
		"pipe:1",
	)
	return args
}

func envInt(key string, def int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	return n
}

func envBool(key string, def bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	switch strings.ToLower(raw) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return def
	}
}

type frameReadResult struct {
	frameBytes []byte
	err        error
}

func frameStallTimeout(spec StreamSpec, captureIntervalSec int) time.Duration {
	timeoutSec := GetConfigInt(spec.CaptureConfig, "frame_stall_timeout_sec", 0)
	if timeoutSec <= 0 {
		timeoutSec = envInt("CAPTURE_FRAME_STALL_TIMEOUT_SEC", 0)
	}
	if timeoutSec <= 0 {
		timeoutSec = captureIntervalSec * 8
		if timeoutSec < 20 {
			timeoutSec = 20
		}
	}
	if timeoutSec < 5 {
		timeoutSec = 5
	}
	if timeoutSec > 300 {
		timeoutSec = 300
	}
	return time.Duration(timeoutSec) * time.Second
}

type framePacer struct {
	interval time.Duration
	next     time.Time
}

func newFramePacer(interval time.Duration) *framePacer {
	if interval <= 0 {
		interval = time.Second
	}
	return &framePacer{interval: interval}
}

func (p *framePacer) Wait(ctx context.Context) error {
	if p == nil || p.interval <= 0 {
		return nil
	}
	now := time.Now()
	if p.next.IsZero() {
		p.next = now.Add(p.interval)
		return nil
	}
	wait := time.Until(p.next)
	if wait > 0 {
		t := time.NewTimer(wait)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
	now = time.Now()
	if now.After(p.next.Add(3 * p.interval)) {
		p.next = now.Add(p.interval)
		return nil
	}
	p.next = p.next.Add(p.interval)
	return nil
}

func readJPEGFrame(r *bufio.Reader, maxFrameBytes int) ([]byte, error) {
	if maxFrameBytes <= 0 {
		maxFrameBytes = 25 << 20
	}
	const (
		jpegSOI0 = 0xFF
		jpegSOI1 = 0xD8
		jpegEOI0 = 0xFF
		jpegEOI1 = 0xD9
	)
	inFrame := false
	prev := byte(0)
	frame := make([]byte, 0, 512*1024)

	for {
		b, err := r.ReadByte()
		if err != nil {
			if err == io.EOF && len(frame) > 0 {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, err
		}
		if !inFrame {
			if prev == jpegSOI0 && b == jpegSOI1 {
				inFrame = true
				frame = append(frame[:0], jpegSOI0, jpegSOI1)
			}
			prev = b
			continue
		}

		frame = append(frame, b)
		if len(frame) > maxFrameBytes {
			return nil, fmt.Errorf("frame exceeded max size %d bytes", maxFrameBytes)
		}
		if prev == jpegEOI0 && b == jpegEOI1 {
			out := make([]byte, len(frame))
			copy(out, frame)
			return out, nil
		}
		prev = b
	}
}

type limitedBuffer struct {
	max int
	b   []byte
}

func (l *limitedBuffer) Write(p []byte) (int, error) {
	n := len(p)
	if l.max <= 0 || len(l.b) >= l.max {
		return n, nil
	}
	remain := l.max - len(l.b)
	if remain > len(p) {
		remain = len(p)
	}
	l.b = append(l.b, p[:remain]...)
	return n, nil
}

func (l *limitedBuffer) String() string {
	return string(l.b)
}
