package recordingapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReserveClipUploadKeepsRollbackCompatibleIdempotencyKey(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Idempotency-Key"); got != "recording-seg-42-1234" {
			t.Fatalf("Idempotency-Key=%q", got)
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if payload["job_id"] != float64(42) || payload["segment_start_ms"] != float64(1234) {
			t.Fatalf("payload=%v", payload)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"intent_id":"intent-1"}`))
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{BaseURL: server.URL, NodeToken: "test-token"})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := client.ReserveClipUpload(context.Background(), 42, "video/mp4", 1234)
	if err != nil {
		t.Fatal(err)
	}
	if intent.IntentID != "intent-1" {
		t.Fatalf("intent=%q", intent.IntentID)
	}
}

func TestUploadUsesLongerTimeoutThanAPIRequests(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		if r.Method == http.MethodPut {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"job":null}`))
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		BaseURL:    server.URL,
		NodeToken:  "test-token",
		HTTPClient: &http.Client{Timeout: 10 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	path := filepath.Join(t.TempDir(), "clip.mp4")
	if err := os.WriteFile(path, []byte("clip"), 0o600); err != nil {
		t.Fatalf("write clip: %v", err)
	}
	if err := client.UploadFile(context.Background(), server.URL, path, "video/mp4"); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if _, err := client.LeaseRecordingJob(context.Background()); err == nil {
		t.Fatal("expected API request to retain its shorter timeout")
	}
}

func TestHeartbeatReturnsConfirmedLeaseExpiry(t *testing.T) {
	want := time.Date(2026, 7, 23, 23, 30, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"cancel":false,"lease_expires_at":"` + want.Format(time.RFC3339) + `"}`))
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{BaseURL: server.URL, NodeToken: "test-token"})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	canceled, got, err := client.HeartbeatRecordingJob(context.Background(), 42)
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if canceled || !got.Equal(want) {
		t.Fatalf("heartbeat canceled=%t lease=%s want false/%s", canceled, got, want)
	}
}
