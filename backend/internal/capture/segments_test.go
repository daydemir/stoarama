package capture

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestBuildFFmpegSegmentArgsHTTPVideo(t *testing.T) {
	args := buildFFmpegSegmentArgs("https://example.com/live.mp4", "/tmp/segment.mp4", DefaultSegmentDuration, "", nil)
	joined := strings.Join(args, " ")

	for _, unwanted := range []string{
		"-http_multiple",
		"-http_persistent",
	} {
		if strings.Contains(joined, unwanted) {
			t.Fatalf("did not expect %q in args: %s", unwanted, joined)
		}
	}

	for _, want := range []string{
		"-reconnect 1",
		"-reconnect_streamed 1",
		"-reconnect_on_network_error 1",
		"-reconnect_on_http_error 4xx,5xx",
		"-reconnect_delay_max 10",
		"-rw_timeout 15000000",
		"-timeout 15000000",
		"-nostdin",
		"-fflags +discardcorrupt",
		"-i https://example.com/live.mp4",
		"-t 30",
		"-map 0:v:0",
		"-map 0:a?",
		"-c copy",
		"/tmp/segment.mp4",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected %q in args: %s", want, joined)
		}
	}
	if strings.Contains(joined, "fps=30") {
		t.Fatalf("segment capture should preserve source frame rate, got args: %s", joined)
	}
	for _, unwanted := range []string{"libx264", "-preset", "-pix_fmt", "-c:a", "-b:a"} {
		if strings.Contains(joined, unwanted) {
			t.Fatalf("segment capture should not transcode with %q, got args: %s", unwanted, joined)
		}
	}
}

func TestBuildFFmpegSegmentArgsUsesRequestedDuration(t *testing.T) {
	args := buildFFmpegSegmentArgs("https://example.com/live.mp4", "/tmp/segment.mp4", 90*time.Second, "", nil)
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "-t 90") {
		t.Fatalf("expected 90s segment duration in args: %s", joined)
	}
}

// TestBuildFFmpegSegmentArgsFixedFPS asserts that a non-nil targetFPS switches
// the capture from stream-copy to a re-encode that normalizes the clip to the
// chosen rate: the canonical `fps` filter, an H.264 video encode, and an AAC
// audio encode, with NO `-c copy` (you cannot change fps while copying).
func TestBuildFFmpegSegmentArgsFixedFPS(t *testing.T) {
	fps := 15
	args := buildFFmpegSegmentArgs("https://example.com/live.mp4", "/tmp/segment.mp4", DefaultSegmentDuration, "", &fps)
	joined := strings.Join(args, " ")
	for _, want := range []string{
		"-vf fps=15",
		"-c:v libx264",
		"-preset veryfast",
		"-crf 23",
		"-pix_fmt yuv420p",
		"-c:a aac",
		"-b:a 128k",
		"/tmp/segment.mp4",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected %q in fixed-fps args: %s", want, joined)
		}
	}
	if strings.Contains(joined, "-c copy") {
		t.Fatalf("fixed-fps capture must re-encode, not -c copy, got args: %s", joined)
	}
}

// writeFakeFFmpeg writes a tiny executable shell stub to a temp dir and returns
// its path, for driving ProbeReachable's failure handling without real ffmpeg.
func writeFakeFFmpeg(t *testing.T, script string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "ffmpeg")
	if err := os.WriteFile(p, []byte("#!/bin/sh\n"+script+"\n"), 0o755); err != nil {
		t.Fatalf("write fake ffmpeg: %v", err)
	}
	return p
}

// TestProbeReachableSanitizesCrash asserts that a child killed by a signal
// (segfault) and a normal non-zero exit both yield a clean "stream not reachable"
// message with no raw "signal:" / "core dumped" substring leaking to the UI.
func TestProbeReachableSanitizesCrash(t *testing.T) {
	cases := []struct {
		name   string
		script string
	}{
		// `kill -SEGV $$` makes the stub die by SIGSEGV, reproducing the by-IP
		// ffmpeg segfault the probe used to leak.
		{"segfault", "kill -SEGV $$"},
		// A plain non-zero exit, like ffmpeg's exit status 8 on a 4XX.
		{"nonzero-exit", "echo 'Server returned 4XX' 1>&2; exit 8"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("FFMPEG_BIN", writeFakeFFmpeg(t, tc.script))
			err := ProbeReachable(context.Background(), "https://example.com/live.m3u8", "")
			if err == nil {
				t.Fatalf("expected an error from a failing probe")
			}
			msg := err.Error()
			if !strings.HasPrefix(msg, "stream not reachable") {
				t.Fatalf("expected sanitized message, got %q", msg)
			}
			for _, leak := range []string{"signal:", "core dumped", "Server returned 4XX", "exit status"} {
				if strings.Contains(msg, leak) {
					t.Fatalf("probe error leaked %q in %q", leak, msg)
				}
			}
		})
	}
}

// TestCaptureSingleFrameRecordsThenExtracts asserts the survey video helper runs
// the recorder's two-step path on ffmpegBin() (FFMPEG_BIN override): step 1
// records a -c copy segment with the network input args (the fix for the live
// decode-to-jpeg segfault), step 2 decodes one frame from the LOCAL segment
// file, and the produced JPEG is built into a Frame. The fake ffmpeg appends
// every invocation's args to a log and writes a fixture to each output path.
func TestCaptureSingleFrameRecordsThenExtracts(t *testing.T) {
	// A 1x1 JPEG so buildFrame's DecodeConfig succeeds.
	jpeg := []byte{
		0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46, 0x00, 0x01, 0x01, 0x00, 0x00, 0x01,
		0x00, 0x01, 0x00, 0x00, 0xFF, 0xDB, 0x00, 0x43, 0x00, 0x08, 0x06, 0x06, 0x07, 0x06, 0x05, 0x08,
		0x07, 0x07, 0x07, 0x09, 0x09, 0x08, 0x0A, 0x0C, 0x14, 0x0D, 0x0C, 0x0B, 0x0B, 0x0C, 0x19, 0x12,
		0x13, 0x0F, 0x14, 0x1D, 0x1A, 0x1F, 0x1E, 0x1D, 0x1A, 0x1C, 0x1C, 0x20, 0x24, 0x2E, 0x27, 0x20,
		0x22, 0x2C, 0x23, 0x1C, 0x1C, 0x28, 0x37, 0x29, 0x2C, 0x30, 0x31, 0x34, 0x34, 0x34, 0x1F, 0x27,
		0x39, 0x3D, 0x38, 0x32, 0x3C, 0x2E, 0x33, 0x34, 0x32, 0xFF, 0xC0, 0x00, 0x0B, 0x08, 0x00, 0x01,
		0x00, 0x01, 0x01, 0x01, 0x11, 0x00, 0xFF, 0xC4, 0x00, 0x14, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0xFF, 0xC4, 0x00, 0x14,
		0x10, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0xFF, 0xDA, 0x00, 0x08, 0x01, 0x01, 0x00, 0x00, 0x3F, 0x00, 0xD2, 0xCF, 0x20, 0xFF,
		0xD9,
	}
	fixtureDir := t.TempDir()
	fixture := filepath.Join(fixtureDir, "fixture.jpg")
	if err := os.WriteFile(fixture, jpeg, 0o644); err != nil {
		t.Fatalf("write fixture jpeg: %v", err)
	}
	argsLog := filepath.Join(fixtureDir, "args.log")
	// Stub: append this invocation's args as one line to argsLog, then write the
	// fixture to the last arg (output path) so both the segment and the frame
	// outputs exist. The local-file extract step (-i <segment.mp4>) thus reads a
	// real file and produces a decodable JPEG.
	script := `printf '%s ' "$@" >> ` + argsLog + `
printf '\n' >> ` + argsLog + `
eval "out=\${$#}"
cp ` + fixture + ` "$out"`
	t.Setenv("FFMPEG_BIN", writeFakeFFmpeg(t, script))

	frame, err := CaptureSingleFrame(context.Background(), "https://example.com/live.m3u8", "")
	if err != nil {
		t.Fatalf("CaptureSingleFrame: %v", err)
	}
	if frame.MIMEType != "image/jpeg" || frame.SourceKind != "live" {
		t.Fatalf("unexpected frame: mime=%q kind=%q", frame.MIMEType, frame.SourceKind)
	}
	if frame.Width != 1 || frame.Height != 1 || len(frame.Bytes) == 0 || frame.SHA256 == "" {
		t.Fatalf("frame not built from jpeg: %+v", frame)
	}

	dumped, err := os.ReadFile(argsLog)
	if err != nil {
		t.Fatalf("read args log: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(dumped)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected exactly 2 ffmpeg invocations (record + extract), got %d: %q", len(lines), dumped)
	}
	// Step 1: record a -c copy segment from the live URL with the network args.
	record := lines[0]
	for _, want := range []string{
		"-rw_timeout 15000000",
		"-timeout 15000000",
		"-protocol_whitelist https,tls,tcp,http,crypto,data",
		"-fflags +discardcorrupt",
		"-i https://example.com/live.m3u8",
		"-map 0:v:0",
		"-c copy",
		"segment.mp4",
	} {
		if !strings.Contains(record, want) {
			t.Fatalf("expected %q in record args: %s", want, record)
		}
	}
	// Step 2: extract one frame from the LOCAL segment file (not the live URL).
	extract := lines[1]
	for _, want := range []string{"segment.mp4", "-frames:v 1", "single-frame.jpg"} {
		if !strings.Contains(extract, want) {
			t.Fatalf("expected %q in extract args: %s", want, extract)
		}
	}
	if strings.Contains(extract, "https://example.com/live.m3u8") {
		t.Fatalf("extract step must read the local segment, not the live URL: %s", extract)
	}
}

func TestParseFrameRate(t *testing.T) {
	tests := map[string]float64{
		"25/1":       25,
		"30000/1001": 29.97002997002997,
		"30":         30,
	}
	for raw, want := range tests {
		got := parseFrameRate(raw)
		if got == nil {
			t.Fatalf("parseFrameRate(%q)=nil", raw)
		}
		if diff := *got - want; diff < -0.000001 || diff > 0.000001 {
			t.Fatalf("parseFrameRate(%q)=%v want %v", raw, *got, want)
		}
	}
	for _, raw := range []string{"", "0/0", "bad", "1/0"} {
		if got := parseFrameRate(raw); got != nil {
			t.Fatalf("parseFrameRate(%q)=%v want nil", raw, *got)
		}
	}
}

func TestContinuousWatchdogStartupAndProgressTimeouts(t *testing.T) {
	started := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	lastProgress := started.Add(62 * time.Second)
	if err := continuousWatchdogError(started.Add(29*time.Second), started, started, false, 30*time.Second, 75*time.Second); err != nil {
		t.Fatalf("watchdog expired before startup timeout: %v", err)
	}
	if err := continuousWatchdogError(started.Add(30*time.Second), started, started, false, 30*time.Second, 75*time.Second); err == nil || !strings.Contains(err.Error(), "startup stalled") {
		t.Fatalf("startup timeout error=%v", err)
	}
	if err := continuousWatchdogError(started.Add(136*time.Second), started, lastProgress, true, 30*time.Second, 75*time.Second); err != nil {
		t.Fatalf("watchdog expired before progress timeout: %v", err)
	}
	if err := continuousWatchdogError(started.Add(137*time.Second), started, lastProgress, true, 30*time.Second, 75*time.Second); err == nil || !strings.Contains(err.Error(), "progress stalled") {
		t.Fatalf("progress timeout error=%v", err)
	}
}

func TestContinuousOutputAdvancedIgnoresDeletionAndDetectsEqualTotalReplacement(t *testing.T) {
	if continuousOutputAdvanced(map[string]int64{"a.mp4": 100}, map[string]int64{}) {
		t.Fatalf("file deletion must not count as capture progress")
	}
	if !continuousOutputAdvanced(
		map[string]int64{"a.mp4": 100},
		map[string]int64{"b.mp4": 100},
	) {
		t.Fatalf("new segment must count as progress even when aggregate bytes are unchanged")
	}
	if continuousOutputAdvanced(
		map[string]int64{"a.mp4": 100},
		map[string]int64{"a.mp4": 100},
	) {
		t.Fatalf("unchanged segment must not count as progress")
	}
}

func TestFinalizedSegmentRetryKeepsTimelineIdentity(t *testing.T) {
	processed := map[string]bool{}
	nextStart := time.Time{}
	attempts := 0
	delivered := make([]Segment, 0, 2)
	segment := Segment{
		StartAt:    time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC),
		EndAt:      time.Date(2026, 7, 28, 12, 1, 0, 0, time.UTC),
		DurationMs: 60_000,
	}
	deliver := func(got Segment) error {
		attempts++
		delivered = append(delivered, got)
		if attempts == 1 {
			return errors.New("temporary delivery failure")
		}
		return nil
	}
	if err := deliverContinuousSegment(processed, "segment.mp4", segment, &nextStart, deliver); err == nil {
		t.Fatal("first delivery unexpectedly succeeded")
	}
	if processed["segment.mp4"] {
		t.Fatal("failed delivery marked segment processed")
	}
	if !nextStart.IsZero() {
		t.Fatalf("failed delivery advanced timeline to %s", nextStart)
	}
	if err := deliverContinuousSegment(processed, "segment.mp4", segment, &nextStart, deliver); err != nil {
		t.Fatal(err)
	}
	if !processed["segment.mp4"] || attempts != 2 {
		t.Fatalf("processed=%v attempts=%d", processed["segment.mp4"], attempts)
	}
	if !delivered[0].StartAt.Equal(delivered[1].StartAt) || !delivered[0].EndAt.Equal(delivered[1].EndAt) {
		t.Fatalf("retry changed segment identity: first=%+v second=%+v", delivered[0], delivered[1])
	}
}

func TestFinalizedSegmentRetryWithExistingTimelineDoesNotAdvanceUntilAck(t *testing.T) {
	processed := map[string]bool{}
	initialNext := time.Date(2026, 7, 28, 13, 0, 0, 0, time.UTC)
	nextStart := initialNext
	// The chain leads this segment's strftime label; media order still wins, so
	// the assertions below are about retry identity.
	segment := Segment{
		StartAt:    time.Date(2026, 7, 28, 12, 59, 30, 0, time.UTC),
		EndAt:      time.Date(2026, 7, 28, 13, 0, 30, 0, time.UTC),
		DurationMs: 60_000,
	}
	var delivered []Segment
	deliver := func(got Segment) error {
		delivered = append(delivered, got)
		if len(delivered) == 1 {
			return errors.New("temporary delivery failure")
		}
		return nil
	}
	if err := deliverContinuousSegment(processed, "segment.mp4", segment, &nextStart, deliver); err == nil {
		t.Fatal("first delivery unexpectedly succeeded")
	}
	if !nextStart.Equal(initialNext) {
		t.Fatalf("failed delivery advanced timeline to %s", nextStart)
	}
	if err := deliverContinuousSegment(processed, "segment.mp4", segment, &nextStart, deliver); err != nil {
		t.Fatal(err)
	}
	if len(delivered) != 2 || !delivered[0].StartAt.Equal(initialNext) || !delivered[1].StartAt.Equal(initialNext) {
		t.Fatalf("retry changed existing timeline identity: %+v", delivered)
	}
	if want := initialNext.Add(time.Minute); !nextStart.Equal(want) {
		t.Fatalf("acknowledged timeline=%s want %s", nextStart, want)
	}
}

func TestZeroDurationFinalizedSegmentsUseDistinctTimelineKeys(t *testing.T) {
	processed := map[string]bool{}
	nextStart := time.Date(2026, 7, 28, 13, 0, 0, 0, time.UTC)
	starts := make([]time.Time, 0, 2)
	deliver := func(segment Segment) error {
		starts = append(starts, segment.StartAt)
		return nil
	}
	segment := Segment{DurationMs: 0}
	if err := deliverContinuousSegment(processed, "first.mp4", segment, &nextStart, deliver); err != nil {
		t.Fatal(err)
	}
	if err := deliverContinuousSegment(processed, "second.mp4", segment, &nextStart, deliver); err != nil {
		t.Fatal(err)
	}
	if len(starts) != 2 || starts[0].Equal(starts[1]) {
		t.Fatalf("zero-duration starts=%v want distinct idempotency keys", starts)
	}
	if want := starts[0].Add(time.Millisecond); !starts[1].Equal(want) {
		t.Fatalf("second start=%s want %s", starts[1], want)
	}
}

func TestDuplicateContinuousSegmentDoesNotAdvanceMediaClock(t *testing.T) {
	processed := map[string]bool{}
	nextStart := time.Date(2026, 8, 3, 13, 10, 0, 0, time.UTC)
	segment := Segment{
		StartAt:    nextStart.Add(-time.Minute),
		EndAt:      nextStart,
		DurationMs: 60_000,
	}
	if err := deliverContinuousSegment(processed, "replayed.mp4", segment, &nextStart, func(Segment) error {
		return ErrContinuousSegmentDuplicate
	}); err != nil {
		t.Fatal(err)
	}
	if !processed["replayed.mp4"] {
		t.Fatal("duplicate file was not marked processed")
	}
	want := time.Date(2026, 8, 3, 13, 10, 0, 0, time.UTC)
	if !nextStart.Equal(want) {
		t.Fatalf("duplicate advanced media clock to %s want %s", nextStart, want)
	}
}

func TestContinuousChainNeverMovesBackwardWhenMediaOutrunsWallClock(t *testing.T) {
	// A DVR-backed playlist can hand ffmpeg more media than wall-clock time. Files
	// from one persistent muxer remain in media order, so a large lead must stay
	// end-to-start rather than jumping backward to the filename label.
	label := time.Date(2026, 7, 31, 19, 30, 0, 0, time.UTC)
	drifted := label.Add(3 * time.Minute)
	nextStart := drifted
	var got Segment
	deliver := func(segment Segment) error {
		got = segment
		return nil
	}
	segment := Segment{StartAt: label, EndAt: label.Add(time.Minute), DurationMs: 60000}
	if err := deliverContinuousSegment(map[string]bool{}, "drifted.mp4", segment, &nextStart, deliver); err != nil {
		t.Fatal(err)
	}
	if !got.StartAt.Equal(drifted) {
		t.Fatalf("drifted start=%s want monotonic chain at %s", got.StartAt, drifted)
	}
	if want := drifted.Add(time.Minute); !nextStart.Equal(want) {
		t.Fatalf("timeline=%s want %s", nextStart, want)
	}

	// Within the limit the chain still wins, so ordinary sub-second jitter does
	// not reintroduce a per-segment gap.
	nextStart = label.Add(500 * time.Millisecond)
	chained := nextStart
	if err := deliverContinuousSegment(map[string]bool{}, "jitter.mp4", segment, &nextStart, deliver); err != nil {
		t.Fatal(err)
	}
	if !got.StartAt.Equal(chained) {
		t.Fatalf("jittered start=%s want chained %s", got.StartAt, chained)
	}

	// A chain that lags the label also keeps chaining; there is no reconnect gap
	// between files written by one persistent muxer.
	nextStart = label.Add(-2 * time.Hour)
	lagging := nextStart
	if err := deliverContinuousSegment(map[string]bool{}, "lagging.mp4", segment, &nextStart, deliver); err != nil {
		t.Fatal(err)
	}
	if !got.StartAt.Equal(lagging) {
		t.Fatalf("lagging start=%s want chained %s", got.StartAt, lagging)
	}
}

func TestCaptureContinuousStopsAliveStalledChild(t *testing.T) {
	tests := []struct {
		name           string
		script         string
		startupTimeout time.Duration
		want           string
	}{
		{
			name:           "no startup output",
			script:         "#!/bin/sh\ntrap 'exit 0' INT TERM\nwhile :; do sleep 0.1; done\n",
			startupTimeout: 50 * time.Millisecond,
			want:           "startup stalled",
		},
		{
			name:           "output stops growing",
			script:         "#!/bin/sh\nfor last do :; done\nout=${last%/*}/seg-20260723-120000.mp4\nprintf x > \"$out\"\ntrap 'exit 0' INT TERM\nwhile :; do sleep 0.1; done\n",
			startupTimeout: time.Second,
			want:           "progress stalled",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			temp := t.TempDir()
			ffmpeg := filepath.Join(temp, "ffmpeg")
			if err := os.WriteFile(ffmpeg, []byte(test.script), 0o755); err != nil {
				t.Fatalf("write fake ffmpeg: %v", err)
			}
			t.Setenv("FFMPEG_BIN", ffmpeg)
			output := filepath.Join(temp, "output")
			if err := os.Mkdir(output, 0o755); err != nil {
				t.Fatalf("create output dir: %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
			defer cancel()
			err := captureContinuousWithHeaders(
				ctx, "https://example.com/live.m3u8", time.Second, "", nil, output,
				func(Segment) error { return nil }, "", test.startupTimeout, 50*time.Millisecond,
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("capture error=%v want %s", err, test.want)
			}
		})
	}
}

func TestCaptureContinuousDoesNotRedeliverAfterCallbackFailure(t *testing.T) {
	temp := t.TempDir()
	ffmpeg := filepath.Join(temp, "ffmpeg")
	script := "#!/bin/sh\nfor last do :; done\nout=${last%/*}\nprintf first > \"$out/seg-20260728-120000.mp4\"\nprintf second > \"$out/seg-20260728-120001.mp4\"\ntrap 'exit 0' INT TERM\nwhile :; do sleep 0.1; done\n"
	if err := os.WriteFile(ffmpeg, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake ffmpeg: %v", err)
	}
	t.Setenv("FFMPEG_BIN", ffmpeg)
	output := filepath.Join(temp, "output")
	if err := os.Mkdir(output, 0o755); err != nil {
		t.Fatalf("create output dir: %v", err)
	}

	deliveryErr := errors.New("delivery failed")
	deliveries := 0
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()
	err := captureContinuousWithHeaders(
		ctx, "https://example.com/live.m3u8", time.Second, "", nil, output,
		func(Segment) error {
			deliveries++
			return deliveryErr
		}, "", time.Second, 5*time.Second,
	)
	if !errors.Is(err, deliveryErr) {
		t.Fatalf("capture error=%v want delivery failure", err)
	}
	if deliveries != 1 {
		t.Fatalf("delivery callback called %d times, want once", deliveries)
	}
}

func TestCaptureContinuousFallsBackToVideoOnlyForMalformedAudio(t *testing.T) {
	temp := t.TempDir()
	ffmpeg := filepath.Join(temp, "ffmpeg")
	logPath := filepath.Join(temp, "args.log")
	script := `#!/bin/sh
printf '%s\n' "$*" >> "$FFMPEG_ARGS_LOG"
for last do :; done
out=${last%/*}
case " $* " in
  *" -map 0:a? "*)
    : > "$out/seg-20260807-120000.mp4"
    echo 'sample rate not set' >&2
    echo 'Could not write header (incorrect codec parameters ?): Invalid argument' >&2
    exit 1
    ;;
esac
printf first > "$out/seg-20260807-120000.mp4"
printf second > "$out/seg-20260807-120001.mp4"
trap 'exit 0' INT TERM
while :; do sleep 0.1; done
`
	if err := os.WriteFile(ffmpeg, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake ffmpeg: %v", err)
	}
	t.Setenv("FFMPEG_BIN", ffmpeg)
	t.Setenv("FFMPEG_ARGS_LOG", logPath)
	output := filepath.Join(temp, "output")
	if err := os.Mkdir(output, 0o755); err != nil {
		t.Fatalf("create output dir: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	deliveries := 0
	var deliveredSizes []int64
	err := captureContinuousWithHeaders(
		ctx, "https://example.com/live.m3u8", time.Second, "", nil, output,
		func(seg Segment) error {
			deliveries++
			deliveredSizes = append(deliveredSizes, seg.SizeBytes)
			cancel()
			return nil
		}, "", time.Second, 5*time.Second,
	)
	if err != nil {
		t.Fatalf("capture malformed-audio fallback: %v", err)
	}
	if deliveries == 0 {
		t.Fatal("video-only fallback produced no segment")
	}
	for _, size := range deliveredSizes {
		if size == 0 {
			t.Fatalf("delivered sizes=%v; empty first-attempt artifact reached callback", deliveredSizes)
		}
	}
	logBody, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read ffmpeg args: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(logBody)), "\n")
	if len(lines) != 2 {
		t.Fatalf("ffmpeg attempts=%d want 2: %q", len(lines), logBody)
	}
	if !strings.Contains(lines[0], "-map 0:a?") {
		t.Fatalf("first attempt must preserve audio: %s", lines[0])
	}
	if strings.Contains(lines[1], "-map 0:a?") || !strings.Contains(lines[1], "-map 0:v:0") {
		t.Fatalf("fallback must be video-only: %s", lines[1])
	}
}

func TestMalformedAudioFallbackRefusesExistingMedia(t *testing.T) {
	out := t.TempDir()
	path := filepath.Join(out, "seg-20260807-120000.mp4")
	if err := os.WriteFile(path, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	clean, err := removeZeroLengthContinuousSegments(out)
	if err != nil {
		t.Fatal(err)
	}
	if clean {
		t.Fatal("fallback allowed despite existing media")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("existing media was altered: %v", err)
	}
}

func TestMalformedAudioMuxErrorRequiresExactHeaderFailure(t *testing.T) {
	known := errors.New("sample rate not set\nCould not write header (incorrect codec parameters ?): Invalid argument")
	if !isMalformedAudioMuxError(known) {
		t.Fatal("known malformed-audio header failure was not recognized")
	}
	nearMatch := errors.New("sample rate not set\nCould not write header: permission denied")
	if isMalformedAudioMuxError(nearMatch) {
		t.Fatal("unrelated header failure triggered audio removal")
	}
}

func TestMalformedAudioFallbackDoesNotRestartAfterCancellation(t *testing.T) {
	temp := t.TempDir()
	ffmpeg := filepath.Join(temp, "ffmpeg")
	logPath := filepath.Join(temp, "args.log")
	script := `#!/bin/sh
echo invoked >> "$FFMPEG_ARGS_LOG"
sleep 0.1
echo 'sample rate not set' >&2
echo 'Could not write header (incorrect codec parameters ?): Invalid argument' >&2
exit 1
`
	if err := os.WriteFile(ffmpeg, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FFMPEG_BIN", ffmpeg)
	t.Setenv("FFMPEG_ARGS_LOG", logPath)
	output := filepath.Join(temp, "output")
	if err := os.Mkdir(output, 0o755); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- captureContinuousWithHeaders(ctx, "https://example.com/live.m3u8", time.Second, "", nil, output, func(Segment) error { return nil }, "", time.Second, time.Second)
	}()
	deadline := time.Now().Add(time.Second)
	for {
		body, _ := os.ReadFile(logPath)
		if strings.Contains(string(body), "invoked") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first FFmpeg attempt did not start")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	// Normal window cancellation returns nil when ctx.Done wins the select; if
	// FFmpeg exit wins, the wrapper returns the joined cancellation. Either is
	// valid, but neither may start the video-only retry.
	<-errCh
	body, readErr := os.ReadFile(logPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if got := strings.Count(string(body), "invoked"); got != 1 {
		t.Fatalf("ffmpeg invocations=%d want 1: %q", got, body)
	}
}

func TestCaptureContinuousRetriesFinalSweepAfterFinalizeFailure(t *testing.T) {
	temp := t.TempDir()
	ffmpeg := filepath.Join(temp, "ffmpeg")
	script := "#!/bin/sh\nfor last do :; done\nout=${last%/*}\nprintf invalid > \"$out/seg-000.mp4\"\nprintf valid > \"$out/seg-20260728-120001.mp4\"\ntrap 'exit 0' INT TERM\nwhile :; do sleep 0.1; done\n"
	if err := os.WriteFile(ffmpeg, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake ffmpeg: %v", err)
	}
	t.Setenv("FFMPEG_BIN", ffmpeg)
	output := filepath.Join(temp, "output")
	if err := os.Mkdir(output, 0o755); err != nil {
		t.Fatalf("create output dir: %v", err)
	}

	deliveries := 0
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()
	err := captureContinuousWithHeaders(
		ctx, "https://example.com/live.m3u8", time.Second, "", nil, output,
		func(Segment) error {
			deliveries++
			return nil
		}, "", time.Second, 5*time.Second,
	)
	if err == nil || !strings.Contains(err.Error(), "finalize after sweep failure") {
		t.Fatalf("capture error=%v, want final-sweep recovery error", err)
	}
	if deliveries != 0 {
		t.Fatalf("delivery callback called %d times, want zero", deliveries)
	}
}

// TestBuildFFmpegContinuousArgsSourceCopy asserts the continuous (segment-muxer)
// args for the Source/native path: stream-copy (-c copy), the segment muxer tail
// with the requested segment_time, strftime naming, and NO -t single-clip flag.
func TestBuildFFmpegContinuousArgsSourceCopy(t *testing.T) {
	args := buildFFmpegContinuousArgs("https://example.com/live.m3u8", "/out/seg-%Y%m%d-%H%M%S.mp4", 60*time.Second, "", nil)
	joined := strings.Join(args, " ")
	for _, want := range []string{
		"-nostdin",
		"-live_start_index -1",
		"-fflags +discardcorrupt",
		"-i https://example.com/live.m3u8",
		"-map 0:v:0",
		"-map 0:a?",
		"-c copy",
		"-f segment",
		"-segment_time 60",
		"-reset_timestamps 1",
		"-segment_format mp4",
		"-strftime 1",
		"/out/seg-%Y%m%d-%H%M%S.mp4",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected %q in continuous args: %s", want, joined)
		}
	}
	liveEdge := slices.Index(args, "-live_start_index")
	input := slices.Index(args, "-i")
	if liveEdge < 0 || input < 0 || liveEdge > input {
		t.Fatalf("HLS live-edge option must be input-scoped before -i: %s", joined)
	}
	if slices.Contains(args, "-reconnect_at_eof") {
		t.Fatalf("HLS manifest EOF is normal and must not be reconnected: %s", joined)
	}
	// The persistent muxer must NOT carry the single-clip -t bound.
	for _, field := range args {
		if field == "-t" {
			t.Fatalf("continuous args must not include -t: %s", joined)
		}
	}
	for _, unwanted := range []string{"libx264", "fps="} {
		if strings.Contains(joined, unwanted) {
			t.Fatalf("source/native continuous capture should not transcode (%q): %s", unwanted, joined)
		}
	}
}

func requireArgPair(t *testing.T, args []string, flag, want string) {
	t.Helper()
	i := slices.Index(args, flag)
	if i < 0 || i+1 >= len(args) || args[i+1] != want {
		t.Fatalf("expected exact %s %s in args: %s", flag, want, strings.Join(args, " "))
	}
}

func TestContinuousProgressTimeoutBoundsHLSReconnect(t *testing.T) {
	if got := continuousProgressTimeout("https://example.com/live.m3u8?token=x", 60*time.Second); got != 30*time.Second {
		t.Fatalf("HLS progress timeout=%v want 30s", got)
	}
	if got := continuousProgressTimeout("https://example.com/manifest?format=.m3u8", 60*time.Second); got != 30*time.Second {
		t.Fatalf("query-declared HLS progress timeout=%v want 30s", got)
	}
	if got := continuousProgressTimeout("https://example.com/live.mp4", 60*time.Second); got != 75*time.Second {
		t.Fatalf("direct-video progress timeout=%v want 75s", got)
	}
	if got := continuousProgressTimeout("https://example.com/live.m3u8", 10*time.Second); got != 25*time.Second {
		t.Fatalf("short HLS progress timeout=%v want 25s", got)
	}
}

func TestBuildFFmpegContinuousArgsDoesNotSendHLSOptionToHTTPVideo(t *testing.T) {
	args := buildFFmpegContinuousArgs("https://example.com/live.mp4?token=secret", "/out/seg-%Y%m%d-%H%M%S.mp4", 60*time.Second, "", nil)
	for _, option := range []string{"-reconnect_at_eof", "-live_start_index"} {
		if slices.Contains(args, option) {
			t.Fatalf("non-HLS input received HLS-only option %q: %s", option, strings.Join(args, " "))
		}
	}
}

func TestAppendHLSLiveEdgeInputArgsURLClassification(t *testing.T) {
	tests := []struct {
		name      string
		sourceURL string
		wantHLS   bool
	}{
		{name: "signed HLS URL", sourceURL: "https://example.com/live.m3u8?token=test", wantHLS: true},
		{name: "API HLS format", sourceURL: "https://example.com/manifest?format=.m3u8", wantHLS: true},
		{name: "API HLS name", sourceURL: "https://example.com/manifest?format=hls", wantHLS: true},
		{name: "irrelevant query value", sourceURL: "https://example.com/live.mp4?next=.m3u8", wantHLS: false},
		{name: "malformed URL", sourceURL: "https://example.com/live%ZZ.m3u8", wantHLS: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := []string{"-nostdin"}
			args := appendHLSLiveEdgeInputArgs(slices.Clone(base), tt.sourceURL)
			if got := slices.Contains(args, "-live_start_index"); got != tt.wantHLS {
				t.Fatalf("HLS live-edge option presence=%t want=%t: %v", got, tt.wantHLS, args)
			}
			if slices.Contains(args, "-reconnect_at_eof") {
				t.Fatalf("HLS manifest EOF reconnect must remain disabled: %v", args)
			}
			if !tt.wantHLS && !slices.Equal(args, base) {
				t.Fatalf("non-HLS args changed: got=%v want=%v", args, base)
			}
		})
	}
}

func TestHLSManifestURLKeepsResolverValidationPathStrict(t *testing.T) {
	if isHLSManifestURL("https://example.com/manifest?format=.m3u8") {
		t.Fatal("query-declared runtime HLS input passed strict resolver manifest validation")
	}
}

// TestBuildFFmpegContinuousArgsFixedFPS asserts the fixed-fps continuous path
// re-encodes to the chosen rate AND keeps the same segment-muxer tail, still
// without -t.
func TestBuildFFmpegContinuousArgsFixedFPS(t *testing.T) {
	fps := 15
	args := buildFFmpegContinuousArgs("https://example.com/live.m3u8", "/out/seg-%Y%m%d-%H%M%S.mp4", 60*time.Second, "", &fps)
	joined := strings.Join(args, " ")
	for _, want := range []string{
		"-vf fps=15",
		"-c:v libx264",
		"-preset veryfast",
		"-crf 23",
		"-pix_fmt yuv420p",
		"-c:a aac",
		"-b:a 128k",
		"-f segment",
		"-segment_time 60",
		"-reset_timestamps 1",
		"-segment_format mp4",
		"-strftime 1",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected %q in fixed-fps continuous args: %s", want, joined)
		}
	}
	for _, field := range args {
		if field == "-t" {
			t.Fatalf("continuous args must not include -t: %s", joined)
		}
	}
	if strings.Contains(joined, "-c copy") {
		t.Fatalf("fixed-fps continuous capture must not stream-copy: %s", joined)
	}
}

// TestParseSegmentStart asserts the strftime filename parses to the correct UTC
// instant, the authoritative segment start used for the per-segment object key.
func TestParseSegmentStart(t *testing.T) {
	got, err := parseSegmentStart("seg-20260630-091500.mp4")
	if err != nil {
		t.Fatalf("parseSegmentStart error: %v", err)
	}
	want := time.Date(2026, 6, 30, 9, 15, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("parseSegmentStart=%v want %v", got, want)
	}
	if _, err := parseSegmentStart("not-a-segment.mp4"); err == nil {
		t.Fatalf("expected parse error for malformed segment name")
	}
}

func TestValidateConcatFiles(t *testing.T) {
	argsFile := filepath.Join(t.TempDir(), "args")
	t.Setenv("ARGS_FILE", argsFile)
	t.Setenv("FFMPEG_BIN", writeFakeFFmpeg(t, `printf '%s\n' "$@" >> "$ARGS_FILE"; printf '%s\n' '__CALL__' >> "$ARGS_FILE"`))

	first := filepath.Join(t.TempDir(), "first clip.mp4")
	second := filepath.Join(t.TempDir(), "second clip.mp4")
	if err := ValidateConcatFiles(context.Background(), []string{first, second}); err != nil {
		t.Fatalf("ValidateConcatFiles: %v", err)
	}
	args, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatalf("read fake ffmpeg args: %v", err)
	}
	joined := string(args)
	calls := strings.Split(strings.TrimSpace(joined), "__CALL__")
	if len(calls) != 3 || strings.TrimSpace(calls[2]) != "" {
		t.Fatalf("expected stitch and strict decode calls:\n%s", joined)
	}
	stitch, decode := calls[0], calls[1]
	for _, want := range []string{"-xerror\n", "-err_detect\nexplode\n", "-f\nconcat\n", "-safe\n0\n", "-map\n0:v:0\n", "-map\n0:a?\n", "-c\ncopy\n", "-avoid_negative_ts\nmake_zero\n", "-movflags\n+faststart\n", "-y\n"} {
		if !strings.Contains(stitch, want) {
			t.Fatalf("stitch call missing %q:\n%s", want, stitch)
		}
	}
	stitchArgs := strings.Split(strings.TrimSpace(stitch), "\n")
	stitchedPath := stitchArgs[len(stitchArgs)-1]
	for _, want := range []string{"-xerror\n", "-err_detect\nexplode\n", "-i\n" + stitchedPath + "\n", "-map\n0:v:0\n", "-map\n0:a?\n", "-f\nnull\n-\n"} {
		if !strings.Contains(decode, want) {
			t.Fatalf("decode call missing %q:\n%s", want, decode)
		}
	}
}

func TestValidateConcatFilesRejectsTooFewClips(t *testing.T) {
	if err := ValidateConcatFiles(context.Background(), []string{"one.mp4"}); err == nil {
		t.Fatal("expected fewer than two clips to be rejected")
	}
}
