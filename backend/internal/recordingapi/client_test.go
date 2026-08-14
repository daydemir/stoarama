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

	"github.com/daydemir/stoarama/backend/internal/capture"
	"golang.org/x/sys/unix"
)

func TestIngestClipTimestampProvenancePayloadModes(t *testing.T) {
	tests := []struct {
		name          string
		status        string
		reason        string
		contract      *capture.TimestampContract
		wantVersion   bool
		wantTimestamp bool
	}{
		{name: "legacy"},
		{name: "complete", status: capture.TimestampProbeComplete, contract: &capture.TimestampContract{Version: 1, Mode: "muxed_source_copy", AudioSelection: "first_optional"}, wantVersion: true, wantTimestamp: true},
		{name: "unknown", status: capture.TimestampProbeUnknown, reason: "missing_terminal_duration", wantTimestamp: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var got map[string]any
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
					t.Error(err)
				}
				_, _ = w.Write([]byte(`{"clip_id":1}`))
			}))
			defer server.Close()
			client, err := NewClient(ClientConfig{BaseURL: server.URL, NodeToken: "test-token"})
			if err != nil {
				t.Fatal(err)
			}
			req := IngestClipRequest{JobID: 1, LeaseToken: "lease", CaptureSequence: 1}
			if tc.wantTimestamp {
				req.CaptureAttemptID = "123e4567-e89b-12d3-a456-426614174000"
				req.TimestampContractStatus = tc.status
				req.TimestampContractReason = tc.reason
				req.TimestampContract = tc.contract
			}
			if _, err := client.IngestClip(context.Background(), req); err != nil {
				t.Fatal(err)
			}
			_, hasVersion := got["timestamp_contract_version"]
			if hasVersion != tc.wantVersion {
				t.Fatalf("payload=%v version_present=%v want %v", got, hasVersion, tc.wantVersion)
			}
			_, hasStatus := got["timestamp_contract_status"]
			if hasStatus != tc.wantTimestamp {
				t.Fatalf("payload=%v status_present=%v want %v", got, hasStatus, tc.wantTimestamp)
			}
		})
	}
}

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

func TestLeaseReadsTimestampContractCapability(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"job":{"job_id":1,"recording_id":2,"timestamp_contract_supported":true}}`))
	}))
	defer server.Close()
	client, err := NewClient(ClientConfig{BaseURL: server.URL, NodeToken: "test-token"})
	if err != nil {
		t.Fatal(err)
	}
	job, err := client.LeaseRecordingJob(context.Background())
	if err != nil || job == nil || !job.TimestampContractSupported {
		t.Fatalf("job=%+v err=%v", job, err)
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
	if spec.RecordingID != 445 || spec.NodeID != 150 || spec.StreamID != 17342 ||
		spec.ReservationID != "123e4567-e89b-12d3-a456-426614174000" ||
		!spec.SafeUntil.Equal(time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("unexpected canary spec: %+v", spec)
	}
}

func TestRecordingCanaryCompletionUsesNodeAuthExactReservationAndProof(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		want := "/api/v1/node/recordings/445/canary-reservations/123e4567-e89b-12d3-a456-426614174000/complete"
		if r.URL.Path != want {
			t.Errorf("path=%s want=%s", r.URL.Path, want)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer node-token" {
			t.Errorf("authorization=%q", got)
		}
		var result RecordingCanaryResult
		if err := json.NewDecoder(r.Body).Decode(&result); err != nil {
			t.Errorf("decode result: %v", err)
		}
		if result.SHA256 != strings.Repeat("a", 64) || !result.NativeCopy || result.Uploaded {
			t.Errorf("result=%+v", result)
		}
		_, _ = w.Write([]byte(`{"recorded":true}`))
	}))
	defer server.Close()
	client, err := NewClient(ClientConfig{BaseURL: server.URL, NodeToken: "node-token"})
	if err != nil {
		t.Fatal(err)
	}
	err = client.CompleteRecordingCanary(context.Background(), 445, "123e4567-e89b-12d3-a456-426614174000", RecordingCanaryResult{
		DurationMS: 15_000, SizeBytes: 1234, SHA256: strings.Repeat("a", 64), VideoCodec: "h264",
		ProbeOK: true, DecodeOK: true, NativeCopy: true, RelayVersion: "a1adf4b9", SourceRevision: strings.Repeat("b", 40),
	})
	if err != nil {
		t.Fatal(err)
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

func TestUploadFileExactRejectsPathReplacementBeforePUT(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, err := NewClient(ClientConfig{BaseURL: server.URL, NodeToken: "test-token"})
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	path := filepath.Join(directory, "seg-20260814-120000.mp4")
	if err = os.WriteFile(path, []byte("acknowledged"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stat unix.Stat_t
	if err = unix.Stat(path, &stat); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(path, path+".original"); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, []byte(strings.Repeat("x", len("acknowledged"))), 0o600); err != nil {
		t.Fatal(err)
	}
	if err = client.UploadFileExact(context.Background(), server.URL, path, "video/mp4", uint64(stat.Dev), uint64(stat.Ino), int64(len("acknowledged"))); err == nil {
		t.Fatal("replacement inode was uploaded")
	}
	if calls != 0 {
		t.Fatalf("upload calls=%d", calls)
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
