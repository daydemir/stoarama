package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/config"
)

func TestNodesRelayUpdateRequestSendsOnlyTypedIdentity(t *testing.T) {
	received := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/account/nodes/66/relay-update-intents" {
			t.Errorf("request=%s %s", r.Method, r.URL.Path)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		received <- body
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"pending"}`))
	}))
	defer server.Close()
	runNodesRelayUpdate(context.Background(), config.Config{APIToken: "test-token"}, []string{
		"request", "--backend-api-url", server.URL, "--node-id", "66",
		"--request-id", "00000000-0000-4000-8000-000000000142", "--expected-promoted-version", "new123",
	})
	body := <-received
	if len(body) != 2 || body["request_id"] != "00000000-0000-4000-8000-000000000142" || body["expected_promoted_version"] != "new123" {
		t.Fatalf("payload=%v", body)
	}
}
