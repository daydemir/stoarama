package capture

import "testing"

func TestNormalizeMode(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   string
		want Mode
	}{
		{name: "empty defaults auto", in: "", want: ModeAuto},
		{name: "known mode", in: "youtube_live", want: ModeYouTubeLive},
		{name: "relay mode", in: "youtube_relay", want: ModeYouTubeRelay},
		{name: "trim/lower", in: "  HLS_LIVE ", want: ModeHLSLive},
		{name: "unknown unsupported", in: "weird", want: ModeUnsupported},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := NormalizeMode(tc.in)
			if got != tc.want {
				t.Fatalf("NormalizeMode(%q)=%q want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestClassifyMode(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		spec StreamSpec
		want Mode
	}{
		{
			name: "youtube watch url",
			spec: StreamSpec{StreamURL: "https://www.youtube.com/watch?v=abc123"},
			want: ModeYouTubeLive,
		},
		{
			name: "kbs hls indirection",
			spec: StreamSpec{StreamURL: "https://example.com/path!hls"},
			want: ModeHLSLive,
		},
		{
			name: "m3u8",
			spec: StreamSpec{StreamURL: "https://example.com/live/index.m3u8"},
			want: ModeHLSLive,
		},
		{
			name: "image",
			spec: StreamSpec{StreamURL: "https://example.com/cam.jpg"},
			want: ModeImagePoll,
		},
		{
			name: "image endpoint path",
			spec: StreamSpec{StreamURL: "https://example.com/api/camera/image"},
			want: ModeImagePoll,
		},
		{
			name: "rtsp fallback",
			spec: StreamSpec{StreamURL: "rtsp://example.com/live"},
			want: ModeFFmpegDirect,
		},
		{
			name: "empty unsupported",
			spec: StreamSpec{},
			want: ModeUnsupported,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ClassifyMode(tc.spec)
			if got != tc.want {
				t.Fatalf("ClassifyMode()=%q want %q", got, tc.want)
			}
		})
	}
}

func TestEffectiveMode(t *testing.T) {
	t.Parallel()

	spec := StreamSpec{
		StreamURL:   "https://example.com/live/index.m3u8",
		CaptureMode: ModeAuto,
	}
	if got := EffectiveMode(spec); got != ModeHLSLive {
		t.Fatalf("EffectiveMode(auto)=%q want %q", got, ModeHLSLive)
	}

	spec.CaptureMode = ModeImagePoll
	if got := EffectiveMode(spec); got != ModeImagePoll {
		t.Fatalf("EffectiveMode(explicit)=%q want %q", got, ModeImagePoll)
	}

	spec = StreamSpec{CaptureMode: ModeYouTubeRelay}
	if got := EffectiveMode(spec); got != ModeYouTubeLive {
		t.Fatalf("EffectiveMode(relay)=%q want %q", got, ModeYouTubeLive)
	}
}

func TestGetConfigInt(t *testing.T) {
	t.Parallel()

	if got := GetConfigInt(nil, "target_fps", 7); got != 7 {
		t.Fatalf("GetConfigInt(nil)= %d want 7", got)
	}
	cfg := map[string]any{"target_fps": float64(5)}
	if got := GetConfigInt(cfg, "target_fps", 1); got != 5 {
		t.Fatalf("GetConfigInt(float64)= %d want 5", got)
	}
	cfg = map[string]any{"target_fps": -3}
	if got := GetConfigInt(cfg, "target_fps", 2); got != 2 {
		t.Fatalf("GetConfigInt(invalid)= %d want 2", got)
	}
}

func TestGetConfigString(t *testing.T) {
	t.Parallel()

	if got := GetConfigString(nil, "ffmpeg_hwaccel", "none"); got != "none" {
		t.Fatalf("GetConfigString(nil)= %q want none", got)
	}
	cfg := map[string]any{"ffmpeg_hwaccel": " videotoolbox "}
	if got := GetConfigString(cfg, "ffmpeg_hwaccel", "none"); got != "videotoolbox" {
		t.Fatalf("GetConfigString(string)= %q want videotoolbox", got)
	}
	cfg = map[string]any{"ffmpeg_hwaccel": 1}
	if got := GetConfigString(cfg, "ffmpeg_hwaccel", "none"); got != "none" {
		t.Fatalf("GetConfigString(non-string)= %q want none", got)
	}
}

func TestGetConfigBool(t *testing.T) {
	t.Parallel()

	if got := GetConfigBool(nil, "ffmpeg_reconnect", true); !got {
		t.Fatalf("GetConfigBool(nil)=false want true")
	}
	cfg := map[string]any{"ffmpeg_reconnect": false}
	if got := GetConfigBool(cfg, "ffmpeg_reconnect", true); got {
		t.Fatalf("GetConfigBool(bool)=true want false")
	}
	cfg = map[string]any{"ffmpeg_reconnect": "0"}
	if got := GetConfigBool(cfg, "ffmpeg_reconnect", true); got {
		t.Fatalf("GetConfigBool(string 0)=true want false")
	}
	cfg = map[string]any{"ffmpeg_reconnect": "yes"}
	if got := GetConfigBool(cfg, "ffmpeg_reconnect", false); !got {
		t.Fatalf("GetConfigBool(string yes)=false want true")
	}
	cfg = map[string]any{"ffmpeg_reconnect": "garbage"}
	if got := GetConfigBool(cfg, "ffmpeg_reconnect", true); !got {
		t.Fatalf("GetConfigBool(invalid)=false want true default")
	}
}
