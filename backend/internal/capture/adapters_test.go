package capture

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/netguard"
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

func TestHLSResolveFollowsIndirectBodyAndRedirectToM3U8(t *testing.T) {
	// Not parallel: this overrides the package SSRF guards so the loopback
	// httptest server is reachable, then restores them via t.Cleanup before any
	// parallel test resumes. The guards are exercised against loopback in
	// TestHLSResolveRejectsLoopbackIndirect.
	resolveValidateURL = func(string) (net.IP, error) { return net.IPv4(127, 0, 0, 1), nil }
	resolveDialControl = func(string, string, syscall.RawConn) error { return nil }
	t.Cleanup(func() {
		resolveValidateURL = netguard.ValidatePublicURL
		resolveDialControl = netguard.ControlReject
	})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	mux.HandleFunc("/manifest.m3u8", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = w.Write([]byte("#EXTM3U\n"))
	})
	mux.HandleFunc("/redirect", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, server.URL+"/manifest.m3u8", http.StatusFound)
	})
	mux.HandleFunc("/indirect!hls", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(server.URL + "/redirect\n"))
	})

	a := &hlsLiveAdapter{}
	src, err := a.Resolve(context.Background(), StreamSpec{
		StreamURL: server.URL + "/indirect!hls",
	})
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	if src.Mode != ModeHLSLive {
		t.Fatalf("mode=%q want %q", src.Mode, ModeHLSLive)
	}
	if src.URL != server.URL+"/manifest.m3u8" {
		t.Fatalf("url=%q want %q", src.URL, server.URL+"/manifest.m3u8")
	}
}

// TestHLSResolveRejectsLoopbackIndirect proves the SSRF guard rejects an indirect
// '!hls' reference whose host is loopback before any fetch happens (the recorder
// resolves user-supplied references, so this guard prevents pointing the resolve
// fetch at metadata/RFC1918/loopback). Runs with the production guards.
func TestHLSResolveRejectsLoopbackIndirect(t *testing.T) {
	t.Parallel()

	hit := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hit = true
		_, _ = w.Write([]byte("https://example.com/x.m3u8\n"))
	}))
	defer server.Close()

	a := &hlsLiveAdapter{}
	if _, err := a.Resolve(context.Background(), StreamSpec{StreamURL: server.URL + "/indirect!hls"}); err == nil {
		t.Fatal("expected resolve to be rejected for a loopback indirect reference")
	}
	if hit {
		t.Fatal("guard should reject before the fetch reaches the loopback server")
	}
}
