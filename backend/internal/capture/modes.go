package capture

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Mode string

const (
	ModeAuto         Mode = "auto"
	ModeYouTubeLive  Mode = "youtube_live"
	ModeYouTubeRelay Mode = "youtube_relay"
	ModeHLSLive      Mode = "hls_live"
	ModeImagePoll    Mode = "image_poll"
	ModeFFmpegDirect Mode = "ffmpeg_direct"
	ModeUnsupported  Mode = "unsupported"
)

const (
	RuntimeIdle        = "idle"
	RuntimeResolving   = "resolving"
	RuntimeRunning     = "running"
	RuntimeUnsupported = "unsupported"
	RuntimeError       = "error"
	RuntimeStopped     = "stopped"
)

type StreamSpec struct {
	ID                 int64
	Provider           string
	StreamURL          string
	SourcePageURL      string
	CaptureMode        Mode
	CaptureConfig      map[string]any
	CaptureIntervalSec int
	TargetFPS          int
	MaxFrameBytes      int
}

type ResolvedSource struct {
	URL          string
	IsImage      bool
	RefreshAfter time.Duration
	Mode         Mode
	InputHeaders string
}

type EmitFrameFunc func(ctx context.Context, frame Frame, capturedAt time.Time) error

type Adapter interface {
	Mode() Mode
	Resolve(ctx context.Context, spec StreamSpec) (ResolvedSource, error)
	StartSession(ctx context.Context, spec StreamSpec, src ResolvedSource, emit EmitFrameFunc) error
	Supports(spec StreamSpec) bool
}

type Registry struct {
	adapters map[Mode]Adapter
}

func NewRegistry(adapters ...Adapter) (*Registry, error) {
	m := make(map[Mode]Adapter, len(adapters))
	for _, a := range adapters {
		if a == nil {
			continue
		}
		mode := a.Mode()
		if mode == "" {
			return nil, fmt.Errorf("adapter mode is empty")
		}
		if _, exists := m[mode]; exists {
			return nil, fmt.Errorf("duplicate capture adapter for mode %q", mode)
		}
		m[mode] = a
	}
	return &Registry{adapters: m}, nil
}

func NewDefaultRegistry() (*Registry, error) {
	return NewRegistry(
		&youtubeLiveAdapter{},
		&hlsLiveAdapter{},
		&imagePollAdapter{},
		&ffmpegDirectAdapter{},
	)
}

func (r *Registry) Get(mode Mode) (Adapter, bool) {
	if r == nil {
		return nil, false
	}
	a, ok := r.adapters[mode]
	return a, ok
}

func (r *Registry) Modes() []Mode {
	if r == nil {
		return nil
	}
	out := make([]Mode, 0, len(r.adapters))
	for m := range r.adapters {
		out = append(out, m)
	}
	return out
}

func NormalizeMode(raw string) Mode {
	s := strings.TrimSpace(strings.ToLower(raw))
	switch Mode(s) {
	case ModeAuto, ModeYouTubeLive, ModeYouTubeRelay, ModeHLSLive, ModeImagePoll, ModeFFmpegDirect, ModeUnsupported:
		return Mode(s)
	default:
		if s == "" {
			return ModeAuto
		}
		return ModeUnsupported
	}
}

func ClassifyMode(spec StreamSpec) Mode {
	u := strings.TrimSpace(spec.StreamURL)
	if u == "" {
		u = strings.TrimSpace(spec.SourcePageURL)
	}
	if u == "" {
		return ModeUnsupported
	}
	if isYouTubeURL(u) {
		return ModeYouTubeLive
	}
	if strings.Contains(strings.ToLower(u), "!hls") {
		return ModeHLSLive
	}
	if strings.Contains(strings.ToLower(u), ".m3u8") {
		return ModeHLSLive
	}
	if looksLikeImageURL(u) {
		return ModeImagePoll
	}
	if parsed, err := url.Parse(u); err == nil {
		scheme := strings.ToLower(parsed.Scheme)
		if scheme == "http" || scheme == "https" || scheme == "rtsp" {
			return ModeFFmpegDirect
		}
	}
	return ModeUnsupported
}

func EffectiveMode(spec StreamSpec) Mode {
	mode := spec.CaptureMode
	if mode == "" {
		mode = ModeAuto
	}
	if mode == ModeAuto {
		return ClassifyMode(spec)
	}
	if mode == ModeYouTubeRelay {
		return ModeYouTubeLive
	}
	return mode
}

func GetConfigInt(cfg map[string]any, key string, def int) int {
	if cfg == nil {
		return def
	}
	v, ok := cfg[key]
	if !ok {
		return def
	}
	switch x := v.(type) {
	case int:
		if x > 0 {
			return x
		}
	case int32:
		if x > 0 {
			return int(x)
		}
	case int64:
		if x > 0 {
			return int(x)
		}
	case float64:
		if x > 0 {
			return int(x)
		}
	case float32:
		if x > 0 {
			return int(x)
		}
	}
	return def
}

func GetConfigString(cfg map[string]any, key, def string) string {
	if cfg == nil {
		return def
	}
	v, ok := cfg[key]
	if !ok {
		return def
	}
	s, ok := v.(string)
	if !ok {
		return def
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return def
	}
	return s
}

func GetConfigBool(cfg map[string]any, key string, def bool) bool {
	if cfg == nil {
		return def
	}
	v, ok := cfg[key]
	if !ok {
		return def
	}
	switch x := v.(type) {
	case bool:
		return x
	case int:
		return x != 0
	case int32:
		return x != 0
	case int64:
		return x != 0
	case float32:
		return x != 0
	case float64:
		return x != 0
	case string:
		s := strings.TrimSpace(strings.ToLower(x))
		if s == "" {
			return def
		}
		switch s {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
			return false
		default:
			parsed, err := strconv.ParseBool(s)
			if err != nil {
				return def
			}
			return parsed
		}
	default:
		return def
	}
}
