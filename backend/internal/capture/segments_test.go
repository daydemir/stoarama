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
