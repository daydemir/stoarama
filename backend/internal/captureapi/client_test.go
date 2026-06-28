package captureapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
)

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
