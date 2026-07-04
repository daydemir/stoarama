package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	defaultAPIURL      = "https://stoarama.com"
	defaultConcurrency = 5
)

// relayConfig is the persisted enrollment state at ~/.stoarama/config.json (0600).
type relayConfig struct {
	NodeID      int64     `json:"node_id"`
	NodeToken   string    `json:"node_token"`
	APIURL      string    `json:"api_url"`
	Concurrency int       `json:"concurrency"`
	InstalledAt time.Time `json:"installed_at"`
}

func stoaramaHome() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home dir: %w", err)
	}
	return filepath.Join(home, ".stoarama"), nil
}

func configPath() (string, error) {
	h, err := stoaramaHome()
	if err != nil {
		return "", err
	}
	return filepath.Join(h, "config.json"), nil
}

func binDir() (string, error) {
	h, err := stoaramaHome()
	if err != nil {
		return "", err
	}
	return filepath.Join(h, "bin"), nil
}

func loadConfig() (relayConfig, error) {
	var cfg relayConfig
	p, err := configPath()
	if err != nil {
		return cfg, err
	}
	b, err := os.ReadFile(p)
	if err != nil {
		return cfg, fmt.Errorf("read relay config %s (run 'stoarama-relay enroll' first): %w", p, err)
	}
	if err := json.Unmarshal(b, &cfg); err != nil {
		return cfg, fmt.Errorf("parse relay config %s: %w", p, err)
	}
	cfg.APIURL = strings.TrimRight(strings.TrimSpace(cfg.APIURL), "/")
	if cfg.APIURL == "" {
		cfg.APIURL = defaultAPIURL
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = defaultConcurrency
	}
	if strings.TrimSpace(cfg.NodeToken) == "" {
		return cfg, fmt.Errorf("relay config %s has no node_token; re-run enroll", p)
	}
	return cfg, nil
}

func saveConfig(cfg relayConfig) error {
	h, err := stoaramaHome()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(h, 0o700); err != nil {
		return fmt.Errorf("create %s: %w", h, err)
	}
	p := filepath.Join(h, "config.json")
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal relay config: %w", err)
	}
	if err := os.WriteFile(p, b, 0o600); err != nil {
		return fmt.Errorf("write relay config %s: %w", p, err)
	}
	return nil
}
