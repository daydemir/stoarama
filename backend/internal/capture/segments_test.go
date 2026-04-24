package capture

import (
	"strings"
	"testing"
)

func TestBuildFFmpegSegmentArgsHTTPVideo(t *testing.T) {
	args := buildFFmpegSegmentArgs("https://example.com/live.mp4", "/tmp/segment.mp4", false)
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
		"-nostdin",
		"-fflags +discardcorrupt",
		"-i https://example.com/live.mp4",
		"-t 30",
		"-map 0:v:0",
		"-map 0:a?",
		"-c:v libx264",
		"-preset ultrafast",
		"-pix_fmt yuv420p",
		"-c:a aac",
		"-b:a 96k",
		"/tmp/segment.mp4",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected %q in args: %s", want, joined)
		}
	}
	if strings.Contains(joined, "fps=30") {
		t.Fatalf("segment capture should preserve source frame rate, got args: %s", joined)
	}
}

func TestBuildFFmpegSegmentArgsNormalizesUnknownFPS(t *testing.T) {
	args := buildFFmpegSegmentArgs("https://example.com/live.m3u8", "/tmp/segment.mp4", true)
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "-vf fps=30") {
		t.Fatalf("unknown fps segment capture should default to 30fps, got args: %s", joined)
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
