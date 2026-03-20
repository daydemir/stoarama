package capture

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestFrameStallTimeout(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		spec     StreamSpec
		interval int
		want     time.Duration
	}{
		{
			name:     "default uses interval floor",
			spec:     StreamSpec{},
			interval: 1,
			want:     20 * time.Second,
		},
		{
			name: "explicit config wins",
			spec: StreamSpec{
				CaptureConfig: map[string]any{"frame_stall_timeout_sec": 45},
			},
			interval: 1,
			want:     45 * time.Second,
		},
		{
			name: "explicit config clamps low",
			spec: StreamSpec{
				CaptureConfig: map[string]any{"frame_stall_timeout_sec": 1},
			},
			interval: 1,
			want:     5 * time.Second,
		},
		{
			name: "explicit config clamps high",
			spec: StreamSpec{
				CaptureConfig: map[string]any{"frame_stall_timeout_sec": 9999},
			},
			interval: 1,
			want:     300 * time.Second,
		},
		{
			name:     "default scales with interval",
			spec:     StreamSpec{},
			interval: 8,
			want:     64 * time.Second,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := frameStallTimeout(tc.spec, tc.interval)
			if got != tc.want {
				t.Fatalf("frameStallTimeout()=%s want=%s", got, tc.want)
			}
		})
	}
}

func TestBuildFFmpegSessionArgsDefaults(t *testing.T) {
	t.Setenv("CAPTURE_FFMPEG_THREADS", "")
	t.Setenv("CAPTURE_FFMPEG_JPEG_Q", "")
	t.Setenv("CAPTURE_FFMPEG_HWACCEL", "")
	t.Setenv("CAPTURE_FFMPEG_RECONNECT", "")
	t.Setenv("CAPTURE_FFMPEG_RECONNECT_DELAY_MAX_SEC", "")

	args := buildFFmpegSessionArgs(StreamSpec{}, "https://example.com/live.m3u8", 1)
	joined := strings.Join(args, " ")
	for _, want := range []string{
		"-threads 1",
		"-q:v 2",
		"-reconnect 1",
		"-reconnect_streamed 1",
		"-map 0:v:0",
		"-vf fps=1",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("args missing %q: %s", want, joined)
		}
	}
}

func TestBuildFFmpegSessionArgsOverrides(t *testing.T) {
	spec := StreamSpec{
		CaptureConfig: map[string]any{
			"ffmpeg_threads":                 3,
			"ffmpeg_jpeg_quality":            5,
			"ffmpeg_hwaccel":                 "videotoolbox",
			"ffmpeg_reconnect":               false,
			"ffmpeg_reconnect_delay_max_sec": 9,
		},
	}
	args := buildFFmpegSessionArgs(spec, "https://example.com/live.m3u8", 1)
	joined := strings.Join(args, " ")
	for _, want := range []string{
		"-threads 3",
		"-q:v 5",
		"-hwaccel videotoolbox",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("args missing %q: %s", want, joined)
		}
	}
	if strings.Contains(joined, "-reconnect 1") {
		t.Fatalf("did not expect reconnect args: %s", joined)
	}
}

func TestFFmpegDirectResolveRejectsStillImageURLs(t *testing.T) {
	t.Parallel()

	a := &ffmpegDirectAdapter{}
	_, err := a.Resolve(context.Background(), StreamSpec{
		StreamURL: "https://example.com/api/camera/image",
	})
	if err == nil {
		t.Fatalf("expected error for still-image URL")
	}
	if !strings.Contains(err.Error(), "use image_poll") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFFmpegDirectResolveAcceptsVideoURLs(t *testing.T) {
	t.Parallel()

	a := &ffmpegDirectAdapter{}
	src, err := a.Resolve(context.Background(), StreamSpec{
		StreamURL: "rtsp://example.com/live",
	})
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	if src.Mode != ModeFFmpegDirect {
		t.Fatalf("mode=%q want %q", src.Mode, ModeFFmpegDirect)
	}
	if src.IsImage {
		t.Fatalf("expected non-image source")
	}
}

func TestYouTubeRelayResolveRequiresRelayPullURL(t *testing.T) {
	t.Parallel()

	a := &youtubeRelayAdapter{}
	_, err := a.Resolve(context.Background(), StreamSpec{
		StreamURL: "https://www.youtube.com/watch?v=abc123",
	})
	if err == nil {
		t.Fatalf("expected error when relay_pull_url is missing")
	}
	if !strings.Contains(err.Error(), "requires capture_config.relay_pull_url") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestYouTubeRelayResolveAcceptsRelayPullURL(t *testing.T) {
	t.Parallel()

	a := &youtubeRelayAdapter{}
	src, err := a.Resolve(context.Background(), StreamSpec{
		CaptureConfig: map[string]any{
			"relay_pull_url": "http://10.77.0.2:18080/relay/123?token=abc",
		},
	})
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	if src.Mode != ModeYouTubeRelay {
		t.Fatalf("mode=%q want %q", src.Mode, ModeYouTubeRelay)
	}
	if src.URL != "http://10.77.0.2:18080/relay/123?token=abc" {
		t.Fatalf("url=%q", src.URL)
	}
}
