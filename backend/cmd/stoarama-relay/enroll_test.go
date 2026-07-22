package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestEnrollPersistsCandidateManifest(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/nodes/enroll" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if _, exists := payload["relay_max_streams"]; exists {
			t.Fatal("enrollment must leave relay capacity to the server")
		}
		capabilities, _ := payload["capabilities_json"].(map[string]any)
		if _, exists := capabilities["max_concurrent_streams"]; exists {
			t.Fatal("enrollment must not advertise a client-side capacity")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"node":{"id":42},"node_token":"sin_test"}`))
	}))
	defer server.Close()

	manifest := releaseManifest("latest-candidate1.json")
	err := runEnroll([]string{
		"--token", "sie_test",
		"--api-url", server.URL,
		"--update-manifest", string(manifest),
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.UpdateManifest != manifest {
		t.Fatalf("update manifest = %q, want %q", cfg.UpdateManifest, manifest)
	}
}

func TestEnrollRejectsMutableUpdateManifest(t *testing.T) {
	err := runEnroll([]string{"--token", "sie_test", "--update-manifest", "latest.json"})
	if err == nil {
		t.Fatal("mutable update manifest accepted")
	}
}
