package main

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestDefaultConcurrencyIsSix(t *testing.T) {
	if defaultConcurrency != 6 {
		t.Fatalf("defaultConcurrency=%d want 6", defaultConcurrency)
	}
}

func TestLoadConfigUpgradesLegacyDefaultConcurrency(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	writeTestRelayConfig(t, 5)

	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.Concurrency != 6 {
		t.Fatalf("Concurrency=%d want 6", cfg.Concurrency)
	}
}

func TestLoadConfigPreservesExplicitConcurrency(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	writeTestRelayConfig(t, 4)

	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.Concurrency != 4 {
		t.Fatalf("Concurrency=%d want 4", cfg.Concurrency)
	}
}

func writeTestRelayConfig(t *testing.T, concurrency int) {
	t.Helper()
	dir, err := stoaramaHome()
	if err != nil {
		t.Fatalf("stoaramaHome: %v", err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	body := []byte(`{"node_id":1,"node_token":"sin_test","api_url":"https://stoarama.com","concurrency":` + strconv.Itoa(concurrency) + `}`)
	if err := os.WriteFile(filepath.Join(dir, "config.json"), body, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
}
