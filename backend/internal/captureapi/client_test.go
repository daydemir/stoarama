package captureapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
)

func TestListRecordedStreamsOnOnly(t *testing.T) {
	t.Parallel()

	seenStates := make([]string, 0, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Path; got != "/api/v1/capture/streams" {
			t.Fatalf("path=%q want=/api/v1/capture/streams", got)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Fatalf("authorization header=%q", got)
		}
		state := r.URL.Query().Get("recording_state")
		seenStates = append(seenStates, state)
		if state != "on" {
			http.Error(w, "bad recording_state", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"total": 1,
			"items": []map[string]any{
				{
					"stream": map[string]any{
						"id":              10,
						"recording_state": "on",
						"capture_type":    "youtube_watch",
						"source_url":      "https://www.youtube.com/watch?v=abc123",
					},
				},
			},
		})
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		BaseURL:  server.URL,
		APIToken: "test-token",
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	got, err := client.ListRecordedStreams(context.Background(), 500)
	if err != nil {
		t.Fatalf("ListRecordedStreams: %v", err)
	}
	if len(seenStates) != 1 || seenStates[0] != "on" {
		t.Fatalf("seen states=%v want=[on]", seenStates)
	}
	if len(got) != 1 {
		t.Fatalf("len=%d want=1", len(got))
	}
	if got[0].ID != 10 {
		t.Fatalf("id=%d want=10", got[0].ID)
	}
}

func TestGetStreamUsesCaptureDetailEndpoint(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Path; got != "/api/v1/capture/streams/42" {
			t.Fatalf("path=%q want=/api/v1/capture/streams/42", got)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Fatalf("authorization header=%q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"stream": map[string]any{
				"id":              42,
				"recording_state": "off",
				"capture_type":    "hls",
				"execution_class": "video_live",
				"source_url":      "https://example.com/live.m3u8",
			},
		})
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		BaseURL:  server.URL,
		APIToken: "test-token",
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	stream, err := client.GetStream(context.Background(), 42)
	if err != nil {
		t.Fatalf("GetStream: %v", err)
	}
	if stream.ID != 42 {
		t.Fatalf("id=%d want=42", stream.ID)
	}
	if stream.CaptureType != "hls" {
		t.Fatalf("capture_type=%q want=hls", stream.CaptureType)
	}
}

func TestIngestSuccessRetriesOnTransientFailure(t *testing.T) {
	t.Parallel()

	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if r.URL.Path != "/api/v1/capture/ingest" {
			t.Fatalf("path=%q want=/api/v1/capture/ingest", r.URL.Path)
		}
		if attempts < 3 {
			http.Error(w, "bad gateway", http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"consecutive_errors":0,"unsupported":false}`))
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		BaseURL:    server.URL,
		APIToken:   "test-token",
		HTTPClient: &http.Client{Timeout: 2 * time.Second},
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	err = client.IngestSuccess(context.Background(), IngestSuccessRequest{
		StreamID:      123,
		CapturedAt:    time.Now().UTC(),
		SourceKind:    "live",
		EffectiveMode: capture.ModeYouTubeLive,
		ResolvedURL:   "https://example.com/live.m3u8",
		MIMEType:      "image/jpeg",
		FrameBytes:    []byte{0xff, 0xd8, 0xff},
	})
	if err != nil {
		t.Fatalf("IngestSuccess: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d want=3", attempts)
	}
}

func TestIngestSuccessDoesNotRetryOnClientError(t *testing.T) {
	t.Parallel()

	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		BaseURL:    server.URL,
		APIToken:   "test-token",
		HTTPClient: &http.Client{Timeout: 2 * time.Second},
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	err = client.IngestSuccess(context.Background(), IngestSuccessRequest{
		StreamID:      123,
		CapturedAt:    time.Now().UTC(),
		SourceKind:    "live",
		EffectiveMode: capture.ModeYouTubeLive,
		ResolvedURL:   "https://example.com/live.m3u8",
		MIMEType:      "image/jpeg",
		FrameBytes:    []byte{0xff, 0xd8, 0xff},
	})
	if err == nil {
		t.Fatalf("expected error")
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d want=1", attempts)
	}
}

func TestListRecordingAssignmentsPaginatesUntilShortPage(t *testing.T) {
	t.Parallel()

	requests := make([]string, 0, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Path; got != "/api/v1/service/recording/assignments" {
			t.Fatalf("path=%q want=/api/v1/service/recording/assignments", got)
		}
		requests = append(requests, r.URL.RawQuery)
		offset := r.URL.Query().Get("offset")
		switch offset {
		case "0":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{"stream_id": 1, "server_id": "server-a", "execution_class": "video_live"},
					{"stream_id": 2, "server_id": "server-a", "execution_class": "video_live"},
				},
				"total": 3,
			})
		case "2":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{"stream_id": 3, "server_id": "server-a", "execution_class": "video_live"},
				},
				"total": 3,
			})
		default:
			t.Fatalf("unexpected offset=%q", offset)
		}
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		BaseURL:  server.URL,
		APIToken: "test-token",
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	items, err := client.ListRecordingAssignments(context.Background(), "server-a", "", 2, 0)
	if err != nil {
		t.Fatalf("ListRecordingAssignments: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("len(items)=%d want=3", len(items))
	}
	if len(requests) != 2 {
		t.Fatalf("requests=%d want=2", len(requests))
	}
}

func TestListRecordingAssignmentsReturnsLegacyRelayRows(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Path; got != "/api/v1/service/recording/assignments" {
			t.Fatalf("path=%q want=/api/v1/service/recording/assignments", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{
				{
					"stream_id":       1,
					"server_id":       "server-a",
					"execution_class": "youtube_relay",
				},
				{
					"stream_id":       2,
					"server_id":       "server-a",
					"execution_class": "video_live",
				},
			},
			"total": 2,
		})
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		BaseURL:  server.URL,
		APIToken: "test-token",
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	items, err := client.ListRecordingAssignments(context.Background(), "server-a", "", 10, 0)
	if err != nil {
		t.Fatalf("ListRecordingAssignments: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("len(items)=%d want=2", len(items))
	}
	got := []int64{items[0].StreamID, items[1].StreamID}
	want := []int64{1, 2}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("stream ids=%v want=%v", got, want)
		}
	}
}
