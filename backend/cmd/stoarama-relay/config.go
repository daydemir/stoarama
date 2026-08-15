package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/daydemir/stoarama/backend/internal/recordingapi"
	"github.com/daydemir/stoarama/backend/internal/relaylimits"
)

const defaultAPIURL = "https://stoarama.com"

// relayWorkerCeiling is not a capacity setting. It only bounds local goroutines;
// the authenticated lease endpoint enforces the org-configured node and group
// limits before returning work. Sharing the API's accepted maximum means no
// client-side setting can silently reduce server-authorized capacity.
const relayWorkerCeiling = relaylimits.MaxStreams

// relayConfig is the persisted enrollment state at ~/.stoarama/config.json (0600).
type relayConfig struct {
	NodeID              int64                           `json:"node_id"`
	NodeToken           string                          `json:"node_token"`
	APIURL              string                          `json:"api_url"`
	InstalledAt         time.Time                       `json:"installed_at"`
	UpdateManifest      releaseManifest                 `json:"update_manifest,omitempty"`
	PendingUpdateIntent *recordingapi.RelayUpdateIntent `json:"pending_update_intent,omitempty"`
	PendingUpdateResult *recordingapi.RelayUpdateResult `json:"pending_update_result,omitempty"`
}

var relayConfigMu sync.Mutex

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
// resolves public YouTube from a residential IP with no cookies. A pinned Deno
// runtime is now bundled because some public live streams require challenge solving.
// The with-cookies path (link-youtube export + cookie-file resolve) is kept present
// but DORMANT: the installer never runs it and it is not the default run mode. It is
// deferred because a cookie'd resolve uses yt-dlp's WEB client, which must solve the
// n-challenge and uses the same bundled Deno runtime. The opt-in remains dormant
// because cookie handling, not runtime availability, is the remaining risk.
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
	if strings.TrimSpace(cfg.NodeToken) == "" {
		return cfg, fmt.Errorf("relay config %s has no node_token; re-run enroll", p)
	}
	if cfg.UpdateManifest != "" && !cfg.UpdateManifest.valid() {
		return cfg, fmt.Errorf("relay config %s has invalid update_manifest", p)
	}
	if intent := cfg.PendingUpdateIntent; intent != nil {
		if err := validateRelayUpdateIntent(cfg.NodeID, *intent); err != nil || cfg.UpdateManifest != releaseManifest(intent.ManifestName) {
			return cfg, fmt.Errorf("relay config %s has invalid pending_update_intent", p)
		}
	}
	if result := cfg.PendingUpdateResult; result != nil {
		manifest, manifestErr := immutableReleaseManifest(result.TargetVersion)
		if manifestErr != nil || string(manifest) != result.ManifestName || !lowerHexSHA256(result.ManifestSHA256) ||
			!lowerHexSHA256(result.RelayArtifactSHA256) || !lowerHexSHA256(result.InstalledSHA256) || strings.TrimSpace(result.RequestID) == "" {
			return cfg, fmt.Errorf("relay config %s has invalid pending_update_result", p)
		}
		if cfg.PendingUpdateIntent == nil || cfg.PendingUpdateIntent.RequestID != result.RequestID ||
			cfg.PendingUpdateIntent.TargetVersion != result.TargetVersion || cfg.PendingUpdateIntent.ManifestName != result.ManifestName ||
			cfg.PendingUpdateIntent.ManifestSHA256 != result.ManifestSHA256 || cfg.PendingUpdateIntent.RelayArtifactSHA256 != result.RelayArtifactSHA256 ||
			cfg.PendingUpdateIntent.InstalledSHA256 != result.InstalledSHA256 {
			return cfg, fmt.Errorf("relay config %s has orphaned pending_update_result", p)
		}
	}
	return cfg, nil
}

func lowerHexSHA256(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	for _, r := range value {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
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
	return atomicWriteFile(p, b, 0o600)
}
