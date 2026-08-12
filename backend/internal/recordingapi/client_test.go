package recordingapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReserveClipUploadKeepsRollbackCompatibleIdempotencyKey(t *testing.T) {
	type receivedRequest struct {
		idempotencyKey string
		leaseToken     string
		payload        map[string]any
		err            error
	}
	received := make(chan receivedRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		err := json.NewDecoder(r.Body).Decode(&payload)
		received <- receivedRequest{
			idempotencyKey: r.Header.Get("Idempotency-Key"),
			leaseToken:     r.Header.Get(leaseTokenHeader),
			payload:        payload,
			err:            err,
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"intent_id":"intent-1"}`))
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{BaseURL: server.URL, NodeToken: "test-token"})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := client.ReserveClipUpload(context.Background(), 42, "lease-42", "video/mp4", "abc123", 1234)
	if err != nil {
		t.Fatal(err)
	}
	if intent.IntentID != "intent-1" {
		t.Fatalf("intent=%q", intent.IntentID)
	}
	request := <-received
	if request.err != nil {
		t.Fatal(request.err)
	}
	if request.idempotencyKey != "recording-seg-42-1234" {
		t.Fatalf("Idempotency-Key=%q", request.idempotencyKey)
	}
	if request.leaseToken != "lease-42" {
		t.Fatalf("lease token header=%q", request.leaseToken)
	}
	if request.payload["job_id"] != float64(42) || request.payload["segment_start_ms"] != float64(1234) {
		t.Fatalf("payload=%v", request.payload)
	}
	if request.payload["sha256"] != "abc123" {
		t.Fatalf("sha256 payload=%v", request.payload["sha256"])
	}
}

func TestLeaseAdvertisesGenerationSupport(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get(leaseTokenSupportedHeader); got != "true" {
			t.Errorf("generation support header=%q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"job":null}`))
	}))
	defer server.Close()
	client, err := NewClient(ClientConfig{BaseURL: server.URL, NodeToken: "test-token"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.LeaseRecordingJob(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRecordingCanaryReservationUsesNodeAuthAndExactRecording(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/node/recordings/445/canary-reservations" {
			t.Errorf("path=%s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer node-token" {
			t.Errorf("authorization=%q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"reservation_id":"123e4567-e89b-12d3-a456-426614174000","recording_id":445,"node_id":150,"stream_id":17342,"provider":"YOUTUBE","source_url":"https://example.test/watch","safe_until":"2026-08-12T12:00:00Z"}`))
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{BaseURL: server.URL, NodeToken: "node-token"})
	if err != nil {
		t.Fatal(err)
	}
	spec, err := client.StartRecordingCanary(context.Background(), 445)
	if err != nil {
		t.Fatal(err)
	}
	if spec.RecordingID != 445 || spec.NodeID != 150 || spec.StreamID != 17342 {
		t.Fatalf("unexpected canary spec: %+v", spec)
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
	canceled, got, err := client.HeartbeatRecordingJob(context.Background(), 42, "")
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if canceled || !got.Equal(want) {
		t.Fatalf("heartbeat canceled=%t lease=%s want false/%s", canceled, got, want)
	}
}

func TestTouchDropletReportsBuildSHA(t *testing.T) {
	sha := strings.Repeat("a", 40)
	got := make(chan map[string]string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/recording/droplets/heartbeat" {
			t.Errorf("path=%s", r.URL.Path)
		}
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		got <- body
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	client, err := NewClient(ClientConfig{BaseURL: server.URL, NodeToken: "test-token"})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.TouchDroplet(context.Background(), "  "+strings.ToUpper(sha)+"  "); err != nil {
		t.Fatal(err)
	}
	if body := <-got; body["build_sha"] != sha {
		t.Fatalf("build_sha=%q want %q", body["build_sha"], sha)
	}
}
