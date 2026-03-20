package captureapipersistent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/captureapi"
)

func TestLoadDesiredStreamsWithStreamFilterIncludesNonRecordedStream(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/dashboard/recording/settings", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Fatalf("authorization header=%q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"interval_sec": 3,
			"updated_at":   time.Now().UTC().Format(time.RFC3339Nano),
		})
	})
	mux.HandleFunc("/api/v1/dashboard/streams", func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected list endpoint call: %s", r.URL.Path)
	})
	mux.HandleFunc("/api/v1/dashboard/streams/123", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Fatalf("authorization header=%q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"stream": map[string]any{
				"id":                    123,
				"provider":              "GIGAEYES",
				"source_url":            "https://www.youtube.com/watch?v=abc123",
				"source_page_url":       "",
				"recording_state":       "off",
				"capture_type":          "youtube_watch",
				"execution_config_json": map[string]any{"target_fps": 1},
			},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client, err := captureapi.NewClient(captureapi.ClientConfig{
		BaseURL:  srv.URL,
		APIToken: "test-token",
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	mgr := NewManager(client, ManagerConfig{
		StreamIDs:     []int64{123},
		ModeAllowlist: []capture.Mode{capture.ModeYouTubeLive},
	})
	streams, err := mgr.loadDesiredStreams(context.Background())
	if err != nil {
		t.Fatalf("load desired streams: %v", err)
	}
	if len(streams) != 1 {
		t.Fatalf("streams len=%d want=1", len(streams))
	}
	if streams[0].ID != 123 {
		t.Fatalf("stream id=%d want=123", streams[0].ID)
	}
	if streams[0].CaptureIntervalSec != 3 {
		t.Fatalf("capture interval=%d want=3", streams[0].CaptureIntervalSec)
	}
}

func TestLoadAssignedStreamsUsesAssignmentModeAndRelayPullURL(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/dashboard/recording/settings", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"interval_sec": 1,
			"updated_at":   time.Now().UTC().Format(time.RFC3339Nano),
		})
	})
	mux.HandleFunc("/api/v1/recording/assignments", func(w http.ResponseWriter, r *http.Request) {
		if got := strings.TrimSpace(r.URL.Query().Get("server_id")); got != "sink-1" {
			t.Fatalf("server_id=%q want sink-1", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{
				{
					"stream_id":             777,
					"server_id":             "sink-1",
					"execution_class":       "youtube_relay",
					"assignment_revision":   3,
					"provider":              "youtube",
					"source_url":            "https://www.youtube.com/watch?v=abc123",
					"source_page_url":       "",
					"capture_type":          "youtube_watch",
					"execution_config_json": map[string]any{},
					"relay_pull_url":        "https://rr.example/live.m3u8",
					"relay_status":          "source_ready",
				},
			},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client, err := captureapi.NewClient(captureapi.ClientConfig{
		BaseURL:  srv.URL,
		APIToken: "test-token",
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	mgr := NewManager(client, ManagerConfig{
		ServerID:      "sink-1",
		ModeAllowlist: []capture.Mode{capture.ModeYouTubeRelay},
	})
	streams, err := mgr.loadDesiredStreams(context.Background())
	if err != nil {
		t.Fatalf("load desired streams: %v", err)
	}
	if len(streams) != 1 {
		t.Fatalf("len(streams)=%d want=1", len(streams))
	}
	if streams[0].CaptureMode != capture.ModeYouTubeRelay {
		t.Fatalf("capture_mode=%s want=%s", streams[0].CaptureMode, capture.ModeYouTubeRelay)
	}
	if got := strings.TrimSpace(capture.GetConfigString(streams[0].CaptureConfig, "relay_pull_url", "")); got != "https://rr.example/live.m3u8" {
		t.Fatalf("relay_pull_url=%q want https://rr.example/live.m3u8", got)
	}
}

func TestNewManagerModeAllowlistNormalizesAndDropsInvalid(t *testing.T) {
	mgr := NewManager(&captureapi.Client{}, ManagerConfig{
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
	mgrNoFilter := NewManager(&captureapi.Client{}, ManagerConfig{})
	if !mgrNoFilter.modeAllowed(streamConfig{
		CaptureMode: capture.ModeUnsupported,
		StreamURL:   "ftp://example.com/not-supported",
	}) {
		t.Fatalf("expected modeAllowed=true when no allowlist is configured")
	}

	mgrYouTubeOnly := NewManager(&captureapi.Client{}, ManagerConfig{
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
	mgr := NewManager(&captureapi.Client{}, ManagerConfig{RecordingHeartbeat: true})
	if !mgr.shouldRecordingHeartbeat(capture.ModeYouTubeLive) {
		t.Fatalf("expected recording heartbeat for youtube_live")
	}
	if mgr.shouldRecordingHeartbeat(capture.ModeHLSLive) {
		t.Fatalf("did not expect recording heartbeat for hls_live")
	}

	disabled := NewManager(&captureapi.Client{}, ManagerConfig{RecordingHeartbeat: false})
	if disabled.shouldRecordingHeartbeat(capture.ModeYouTubeLive) {
		t.Fatalf("did not expect recording heartbeat when disabled")
	}
}

func TestManagerShouldRelayRouteStatus(t *testing.T) {
	base := streamConfig{
		ID:                 777,
		Assigned:           true,
		AssignmentRevision: 2,
	}
	mgr := NewManager(&captureapi.Client{}, ManagerConfig{
		ServerID: "do-sink-1",
	})
	if !mgr.shouldRelayRouteStatus(base, capture.ModeYouTubeRelay) {
		t.Fatalf("expected relay route status for assigned youtube_relay stream")
	}
	if mgr.shouldRelayRouteStatus(base, capture.ModeYouTubeLive) {
		t.Fatalf("did not expect relay route status for youtube_live")
	}
	noServer := NewManager(&captureapi.Client{}, ManagerConfig{})
	if noServer.shouldRelayRouteStatus(base, capture.ModeYouTubeRelay) {
		t.Fatalf("did not expect relay route status without server id")
	}
	notAssigned := base
	notAssigned.Assigned = false
	if mgr.shouldRelayRouteStatus(notAssigned, capture.ModeYouTubeRelay) {
		t.Fatalf("did not expect relay route status when stream is not assignment-managed")
	}
	noRevision := base
	noRevision.AssignmentRevision = 0
	if mgr.shouldRelayRouteStatus(noRevision, capture.ModeYouTubeRelay) {
		t.Fatalf("did not expect relay route status without assignment revision")
	}
}

func TestLoadDesiredStreamsRequiresServerIDWithoutStreamFilter(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/dashboard/recording/settings", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"interval_sec": 1,
			"updated_at":   time.Now().UTC().Format(time.RFC3339Nano),
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client, err := captureapi.NewClient(captureapi.ClientConfig{
		BaseURL:  srv.URL,
		APIToken: "test-token",
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	mgr := NewManager(client, ManagerConfig{
		ServerID: "",
	})
	_, err = mgr.loadDesiredStreams(context.Background())
	if err == nil {
		t.Fatalf("expected error when server_id is empty")
	}
	if !strings.Contains(err.Error(), "server_id is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewManagerDefaultQueueConfig(t *testing.T) {
	mgr := NewManager(&captureapi.Client{}, ManagerConfig{})
	if mgr.cfg.FrameQueueSize != 64 {
		t.Fatalf("frame queue size=%d want=64", mgr.cfg.FrameQueueSize)
	}
	if mgr.cfg.FrameEnqueueTimeout != 3*time.Second {
		t.Fatalf("frame enqueue timeout=%s want=3s", mgr.cfg.FrameEnqueueTimeout)
	}
	if mgr.cfg.FrameWriterWorkers != 2 {
		t.Fatalf("frame writer workers=%d want=2", mgr.cfg.FrameWriterWorkers)
	}
	if mgr.cfg.ProcessHeartbeatInterval != 15*time.Second {
		t.Fatalf("process heartbeat interval=%s want=15s", mgr.cfg.ProcessHeartbeatInterval)
	}
	if mgr.cfg.ProcessHeartbeatLeaseSec != 45 {
		t.Fatalf("process heartbeat lease sec=%d want=45", mgr.cfg.ProcessHeartbeatLeaseSec)
	}
	if mgr.cfg.ProcessStartReason != "capture_session_start" {
		t.Fatalf("process start reason=%q want=capture_session_start", mgr.cfg.ProcessStartReason)
	}
}

func TestManagerShouldProcessTelemetry(t *testing.T) {
	base := streamConfig{
		ID:                 123,
		Assigned:           true,
		AssignmentRevision: 7,
	}
	mgr := NewManager(&captureapi.Client{}, ManagerConfig{
		ServerID:         "do-123",
		ProcessTelemetry: true,
	})
	if !mgr.shouldProcessTelemetry(base) {
		t.Fatalf("expected process telemetry enabled for assigned stream with assignment revision")
	}

	mgrNoFlag := NewManager(&captureapi.Client{}, ManagerConfig{
		ServerID:         "do-123",
		ProcessTelemetry: false,
	})
	if mgrNoFlag.shouldProcessTelemetry(base) {
		t.Fatalf("did not expect process telemetry when disabled")
	}

	mgrNoServer := NewManager(&captureapi.Client{}, ManagerConfig{
		ServerID:         "",
		ProcessTelemetry: true,
	})
	if mgrNoServer.shouldProcessTelemetry(base) {
		t.Fatalf("did not expect process telemetry without server id")
	}

	notAssigned := base
	notAssigned.Assigned = false
	if mgr.shouldProcessTelemetry(notAssigned) {
		t.Fatalf("did not expect process telemetry for stream without assignment")
	}

	noRevision := base
	noRevision.AssignmentRevision = 0
	if mgr.shouldProcessTelemetry(noRevision) {
		t.Fatalf("did not expect process telemetry for stream without assignment revision")
	}
}

func TestManagerEmitFrameFailsFastOnBackpressure(t *testing.T) {
	mgr := NewManager(&captureapi.Client{}, ManagerConfig{
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
