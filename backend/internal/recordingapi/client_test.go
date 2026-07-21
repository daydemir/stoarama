package recordingapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

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
