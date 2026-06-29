package capture

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBuildFFmpegSegmentArgsHTTPVideo(t *testing.T) {
	args := buildFFmpegSegmentArgs("https://example.com/live.mp4", "/tmp/segment.mp4", DefaultSegmentDuration, "")
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
	args := buildFFmpegSegmentArgs("https://example.com/live.mp4", "/tmp/segment.mp4", 90*time.Second, "")
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "-t 90") {
		t.Fatalf("expected 90s segment duration in args: %s", joined)
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
