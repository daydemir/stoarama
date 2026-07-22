package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigIgnoresLegacyConcurrency(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	writeTestRelayConfig(t)

	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.NodeID != 1 {
		t.Fatalf("NodeID=%d want 1", cfg.NodeID)
	}
}

func writeTestRelayConfig(t *testing.T) {
	t.Helper()
	dir, err := stoaramaHome()
	if err != nil {
		t.Fatalf("stoaramaHome: %v", err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	body := []byte(`{"node_id":1,"node_token":"sin_test","api_url":"https://stoarama.com","concurrency":4}`)
	if err := os.WriteFile(filepath.Join(dir, "config.json"), body, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
}
