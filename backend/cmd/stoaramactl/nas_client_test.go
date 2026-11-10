package main

import (
	"context"
	"github.com/daydemir/stoarama/backend/internal/config"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNASClientReleaseQuarantine(t *testing.T) {
	var method, path string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, path = r.Method, r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"cleanup_release_generation":2}`))
	}))
	defer server.Close()
	var out strings.Builder
	runNASClientTo(context.Background(), config.Config{APIToken: "fixture"}, []string{"release-quarantine", "--connection-id", "13", "--backend-api-url", server.URL}, &out)
	if method != http.MethodPost || path != "/api/v1/service/nas-connections/13/release-quarantine" || !strings.Contains(out.String(), `"cleanup_release_generation":2`) {
		t.Fatalf("%s %s %s", method, path, out.String())
	}
}
