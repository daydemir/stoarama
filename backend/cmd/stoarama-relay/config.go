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

// experimentalCookieEnv opts the relay into the DORMANT with-cookies YouTube path
// (cookie-file resolve for private/members streams). Default (unset) = cookieless.
const experimentalCookieEnv = "STOARAMA_RELAY_YT_COOKIES"

// experimentalCookieMode reports whether the with-cookies path is opted in.
//
// COOKIELESS-DEFAULT DESIGN NOTE (decision 2026-07-04): the relay records generally
// PUBLIC streams and resolves YouTube COOKIELESS by default. yt-dlp's android client
// resolves public YouTube from a residential IP with no cookies and no JS runtime.
// The with-cookies path (link-youtube export + cookie-file resolve) is kept present
// but DORMANT: the installer never runs it and it is not the default run mode. It is
// deferred because a cookie'd resolve uses yt-dlp's WEB client, which must solve the
// n-challenge and therefore needs a bundled JS runtime (Deno) we do NOT ship; without
// it the web client returns "No video formats found". REVISIT (enable this opt-in and
// bundle Deno) only if/when the cookieless android-client bypass stops working.
func experimentalCookieMode() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(experimentalCookieEnv)))
	return v == "1" || v == "true" || v == "yes"
}

// cookiesFilePath is the Netscape-format cookie jar the GUI-session `link-youtube`
// export writes (0600) and, under the experimental with-cookies opt-in, the run loop
// reads with `yt-dlp --cookies`. It needs no macOS Keychain grant, unlike
// `--cookies-from-browser chrome`. Ignored entirely in the default cookieless path.
func cookiesFilePath() (string, error) {
	h, err := stoaramaHome()
	if err != nil {
		return "", err
	}
	return filepath.Join(h, "cookies.txt"), nil
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
