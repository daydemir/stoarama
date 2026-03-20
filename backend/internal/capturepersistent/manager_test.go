package capturepersistent

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
)

func TestNewManagerModeAllowlistNormalizesAndDropsInvalid(t *testing.T) {
	mgr := NewManager(nil, nil, ManagerConfig{
		ModeAllowlist: []capture.Mode{
			capture.Mode(" youtube_live "),
			capture.Mode("HLS_LIVE"),
			capture.Mode("not_a_mode"),
		},
	})

	if len(mgr.modeFilter) != 2 {
		t.Fatalf("mode filter size = %d, want 2", len(mgr.modeFilter))
	}
	if _, ok := mgr.modeFilter[capture.ModeYouTubeLive]; !ok {
		t.Fatalf("expected youtube_live in mode filter")
	}
	if _, ok := mgr.modeFilter[capture.ModeHLSLive]; !ok {
		t.Fatalf("expected hls_live in mode filter")
	}
	if _, ok := mgr.modeFilter[capture.ModeUnsupported]; ok {
		t.Fatalf("did not expect unsupported mode in mode filter")
	}
}

func TestManagerModeAllowed(t *testing.T) {
	mgrNoFilter := NewManager(nil, nil, ManagerConfig{})
	if !mgrNoFilter.modeAllowed(streamConfig{
		CaptureMode: capture.ModeUnsupported,
		StreamURL:   "ftp://example.com/not-supported",
	}) {
		t.Fatalf("expected modeAllowed=true when no allowlist is configured")
	}

	mgrYouTubeOnly := NewManager(nil, nil, ManagerConfig{
		ModeAllowlist: []capture.Mode{capture.ModeYouTubeLive},
	})
	tests := []struct {
		name string
		cfg  streamConfig
		want bool
	}{
		{
			name: "explicit youtube mode is allowed",
			cfg: streamConfig{
				CaptureMode: capture.ModeYouTubeLive,
				StreamURL:   "https://www.youtube.com/watch?v=abc123",
			},
			want: true,
		},
		{
			name: "auto youtube classify is allowed",
			cfg: streamConfig{
				CaptureMode: capture.ModeAuto,
				StreamURL:   "https://youtu.be/abc123",
			},
			want: true,
		},
		{
			name: "non-youtube mode is filtered out",
			cfg: streamConfig{
				CaptureMode: capture.ModeHLSLive,
				StreamURL:   "https://example.com/live.m3u8",
			},
			want: false,
		},
		{
			name: "unsupported auto mode is filtered out",
			cfg: streamConfig{
				CaptureMode: capture.ModeAuto,
				StreamURL:   "ftp://example.com/live",
			},
			want: false,
		},
	}

	for _, tc := range tests {
		if got := mgrYouTubeOnly.modeAllowed(tc.cfg); got != tc.want {
			t.Fatalf("%s: modeAllowed=%v want=%v", tc.name, got, tc.want)
		}
	}
}

func TestManagerShouldRecordingHeartbeat(t *testing.T) {
	mgr := NewManager(nil, nil, ManagerConfig{RecordingHeartbeat: true})
	if !mgr.shouldRecordingHeartbeat(capture.ModeYouTubeLive) {
		t.Fatalf("expected recording heartbeat for youtube_live")
	}
	if mgr.shouldRecordingHeartbeat(capture.ModeHLSLive) {
		t.Fatalf("did not expect recording heartbeat for hls_live")
	}

	disabled := NewManager(nil, nil, ManagerConfig{RecordingHeartbeat: false})
	if disabled.shouldRecordingHeartbeat(capture.ModeYouTubeLive) {
		t.Fatalf("did not expect recording heartbeat when disabled")
	}
}

func TestNewManagerDefaultQueueConfig(t *testing.T) {
	mgr := NewManager(nil, nil, ManagerConfig{})
	if mgr.cfg.FrameQueueSize != 64 {
		t.Fatalf("frame queue size=%d want=64", mgr.cfg.FrameQueueSize)
	}
	if mgr.cfg.FrameEnqueueTimeout != 3*time.Second {
		t.Fatalf("frame enqueue timeout=%s want=3s", mgr.cfg.FrameEnqueueTimeout)
	}
	if mgr.cfg.FrameWriterWorkers != 2 {
		t.Fatalf("frame writer workers=%d want=2", mgr.cfg.FrameWriterWorkers)
	}
}

func TestManagerEmitFrameFailsFastOnBackpressure(t *testing.T) {
	mgr := NewManager(nil, nil, ManagerConfig{
		FrameQueueSize:      1,
		FrameEnqueueTimeout: 15 * time.Millisecond,
	})
	frameCh := make(chan frameEvent, 1)
	frameCh <- frameEvent{}

	emit := mgr.emitFrame(frameCh, capture.ModeYouTubeLive, "https://example.com/live.m3u8")
	err := emit(context.Background(), capture.Frame{}, time.Now().UTC())
	if err == nil {
		t.Fatalf("expected backpressure error")
	}
	if !strings.Contains(err.Error(), "frame sink backpressure") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPersistCallContext(t *testing.T) {
	base := context.Background()
	detachedCtx, detachedCancel := persistCallContext(base)
	defer detachedCancel()
	if detachedCtx == base {
		t.Fatalf("expected detached context for persist calls")
	}
	if detachedCtx.Err() != nil {
		t.Fatalf("detached context should be active, got err=%v", detachedCtx.Err())
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	flushCtx, flushCancel := persistCallContext(canceled)
	defer flushCancel()
	if flushCtx == canceled {
		t.Fatalf("expected replacement context after parent cancel")
	}
	if flushCtx.Err() != nil {
		t.Fatalf("replacement context should be active, got err=%v", flushCtx.Err())
	}
}

func TestPrioritizeAndCap(t *testing.T) {
	streams := []streamConfig{
		{ID: 8},
		{ID: 2},
		{ID: 5},
		{ID: 3},
	}

	got := prioritizeAndCap(streams, 2)
	if len(got) != 2 {
		t.Fatalf("len=%d want=2", len(got))
	}
	if got[0].ID != 2 || got[1].ID != 3 {
		t.Fatalf("unexpected order/cap: got ids=%d,%d want=2,3", got[0].ID, got[1].ID)
	}
}
