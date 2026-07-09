package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// selfUpdateInterval is how often the run loop checks latest.json for a newer relay
// binary + yt-dlp. Hourly keeps remote diagnostics/security fixes from waiting a day.
const selfUpdateInterval = time.Hour

// latestArtifact is one downloadable file entry in latest.json, keyed by the
// "{os}-{arch}" target, with the sha256 the client must verify before installing.
type latestArtifact struct {
	Artifact string `json:"artifact"`
	SHA256   string `json:"sha256"`
}

// latestJSON is the release manifest published at relay-releases/latest.json.
type latestJSON struct {
	Version string                    `json:"version"`
	Relay   map[string]latestArtifact `json:"relay"`
	Ytdlp   map[string]latestArtifact `json:"ytdlp"`
}

// runSelfUpdate fetches latest.json, verifies the sha256 of the downloaded
// artifact, and atomically replaces the relay binary (and refreshes yt-dlp when a
// matching entry is present), then restarts the service.
func runSelfUpdate(args []string) error {
	fs := flag.NewFlagSet("self-update", flag.ContinueOnError)
	apiURL := fs.String("api-url", "", "Stoarama API base URL (default: from config)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	base := strings.TrimRight(strings.TrimSpace(*apiURL), "/")
	if base == "" {
		if cfg, err := loadConfig(); err == nil {
			base = cfg.APIURL
		}
	}
	if base == "" {
		base = defaultAPIURL
	}

	lj, err := fetchLatest(base)
	if err != nil {
		return err
	}
	target := runtime.GOOS + "-" + runtime.GOARCH

	rel, ok := lj.Relay[target]
	if !ok {
		return fmt.Errorf("no relay artifact for %s in latest.json", target)
	}
	if lj.Version != "" && lj.Version == version {
		fmt.Printf("relay already up to date (%s)\n", version)
	} else {
		if err := updateRelayBinary(base, rel); err != nil {
			return err
		}
		fmt.Printf("updated relay %s -> %s\n", version, lj.Version)
	}

	// yt-dlp refresh follows the same download+verify+atomic-rename pattern.
	if yt, ok := lj.Ytdlp[target]; ok && strings.TrimSpace(yt.SHA256) != "" {
		if err := updateExecutable(base, yt, "yt-dlp"); err != nil {
			fmt.Fprintf(os.Stderr, "yt-dlp refresh skipped: %v\n", err)
		} else {
			fmt.Println("yt-dlp refreshed")
		}
	}

	restartService()
	return nil
}

// selfUpdateLoop periodically checks latest.json and applies sha256-verified relay +
// yt-dlp updates, reusing the same download/verify/atomic-rename path as the manual
// `self-update` command. It logs every outcome. The first check runs one interval
// after start (not immediately) so a fresh install does not restart itself on boot.
// When the relay binary itself changes it kickstarts the service so the new binary is
// exec'd; a yt-dlp-only refresh is picked up by the next capture without a restart.
func selfUpdateLoop(ctx context.Context, base string) {
	ticker := time.NewTicker(selfUpdateInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			checkAndApplyUpdate(base)
		}
	}
}

// checkAndApplyUpdate performs one self-update pass: fetch the manifest, apply a newer
// relay binary and refresh yt-dlp (both sha256-verified), and restart only if the relay
// binary actually changed. Errors are logged and swallowed so a bad manifest or a
// transient network failure never takes the relay down.
func checkAndApplyUpdate(base string) {
	lj, err := fetchLatest(base)
	if err != nil {
		log.Printf("relay self-update: fetch latest.json: %v", err)
		return
	}
	target := runtime.GOOS + "-" + runtime.GOARCH

	relayUpdated := false
	if rel, ok := lj.Relay[target]; ok && lj.Version != "" && lj.Version != version {
		if err := updateRelayBinary(base, rel); err != nil {
			log.Printf("relay self-update: relay %s -> %s failed: %v", version, lj.Version, err)
		} else {
			log.Printf("relay self-update: relay %s -> %s applied", version, lj.Version)
			relayUpdated = true
		}
	} else {
		log.Printf("relay self-update: relay up to date (%s)", version)
	}

	if yt, ok := lj.Ytdlp[target]; ok && strings.TrimSpace(yt.SHA256) != "" {
		if err := updateExecutable(base, yt, "yt-dlp"); err != nil {
			log.Printf("relay self-update: yt-dlp refresh failed: %v", err)
		} else {
			log.Printf("relay self-update: yt-dlp refreshed")
		}
	}

	if relayUpdated {
		log.Printf("relay self-update: restarting service to load new binary")
		restartService()
	}
}

func fetchLatest(base string) (latestJSON, error) {
	var lj latestJSON
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/relay/download/latest.json", nil)
	if err != nil {
		return lj, fmt.Errorf("build latest.json request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return lj, fmt.Errorf("fetch latest.json: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return lj, fmt.Errorf("fetch latest.json: status=%d", resp.StatusCode)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&lj); err != nil {
		return lj, fmt.Errorf("decode latest.json: %w", err)
	}
	return lj, nil
}

// updateRelayBinary downloads the relay tarball, verifies its sha256, extracts the
// stoarama-relay entry, and atomically replaces the installed binary.
func updateRelayBinary(base string, art latestArtifact) error {
	data, err := downloadVerified(base, art)
	if err != nil {
		return err
	}
	binBytes, err := extractTarGzEntry(data, "stoarama-relay")
	if err != nil {
		return err
	}
	dst, err := binDir()
	if err != nil {
		return err
	}
	return atomicWriteExecutable(filepath.Join(dst, "stoarama-relay"), binBytes)
}

// updateExecutable downloads a raw executable (e.g. yt-dlp), verifies its sha256,
// and atomically replaces the installed file at binDir/name.
func updateExecutable(base string, art latestArtifact, name string) error {
	data, err := downloadVerified(base, art)
	if err != nil {
		return err
	}
	dst, err := binDir()
	if err != nil {
		return err
	}
	return atomicWriteExecutable(filepath.Join(dst, name), data)
}

func downloadVerified(base string, art latestArtifact) ([]byte, error) {
	name := strings.TrimSpace(art.Artifact)
	if name == "" {
		return nil, fmt.Errorf("artifact name missing in latest.json")
	}
	want := strings.ToLower(strings.TrimSpace(art.SHA256))
	if want == "" {
		return nil, fmt.Errorf("sha256 missing for %s in latest.json", name)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/relay/download/"+name, nil)
	if err != nil {
		return nil, fmt.Errorf("build download request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("download %s: %w", name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download %s: status=%d", name, resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 512<<20))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", name, err)
	}
	sum := sha256.Sum256(data)
	if got := hex.EncodeToString(sum[:]); got != want {
		return nil, fmt.Errorf("sha256 mismatch for %s: got %s want %s", name, got, want)
	}
	return data, nil
}

func extractTarGzEntry(data []byte, want string) ([]byte, error) {
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("open gzip: %w", err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read tar: %w", err)
		}
		if filepath.Base(hdr.Name) == want && hdr.Typeflag == tar.TypeReg {
			b, err := io.ReadAll(tr)
			if err != nil {
				return nil, fmt.Errorf("read tar entry %s: %w", want, err)
			}
			return b, nil
		}
	}
	return nil, fmt.Errorf("entry %s not found in tarball", want)
}

// atomicWriteExecutable writes to a sibling .new file then os.Rename over the
// target, so a crash mid-write never leaves a truncated binary in place.
func atomicWriteExecutable(dst string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("create %s: %w", filepath.Dir(dst), err)
	}
	tmp := dst + ".new"
	if err := os.WriteFile(tmp, data, 0o755); err != nil {
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("replace %s: %w", dst, err)
	}
	return nil
}
