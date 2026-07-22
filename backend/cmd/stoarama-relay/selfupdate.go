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
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
)

var lastUpdaterUnix atomic.Int64

// selfUpdateInterval is how often the run loop checks latest.json for a newer relay
// binary + yt-dlp. Ten minutes keeps remote relay fixes quick to iterate.
const selfUpdateInterval = 10 * time.Minute

// latestArtifact is one downloadable file entry in latest.json, keyed by the
// "{os}-{arch}" target, with the sha256 the client must verify before installing.
type latestArtifact struct {
	Artifact string `json:"artifact"`
	SHA256   string `json:"sha256"`
}

// latestJSON is the release manifest published at relay-releases/latest.json.
type latestJSON struct {
	Version         string                    `json:"version"`
	Relay           map[string]latestArtifact `json:"relay"`
	Ytdlp           map[string]latestArtifact `json:"ytdlp"`
	PreviousVersion string                    `json:"previous_version"`
	PreviousRelay   map[string]latestArtifact `json:"previous_relay"`
}

type releaseManifest string

const liveReleaseManifest releaseManifest = "latest.json"

func (m releaseManifest) valid() bool {
	name := string(m)
	if name == string(liveReleaseManifest) {
		return true
	}
	if filepath.Base(name) != name || !strings.HasPrefix(name, "latest-") || !strings.HasSuffix(name, ".json") {
		return false
	}
	releaseVersion := strings.TrimSuffix(strings.TrimPrefix(name, "latest-"), ".json")
	if releaseVersion == "" || strings.Contains(releaseVersion, "..") ||
		!asciiAlphanumeric(releaseVersion[0]) || !asciiAlphanumeric(releaseVersion[len(releaseVersion)-1]) {
		return false
	}
	for _, r := range releaseVersion {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '.' && r != '_' && r != '-' {
			return false
		}
	}
	return true
}

func asciiAlphanumeric(b byte) bool {
	return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9'
}

func (m releaseManifest) version() (string, bool) {
	if !m.valid() || m == liveReleaseManifest {
		return "", false
	}
	version := strings.TrimSuffix(strings.TrimPrefix(string(m), "latest-"), ".json")
	return version, version != ""
}

func immutableReleaseManifest(releaseVersion string) (releaseManifest, error) {
	manifest := releaseManifest("latest-" + releaseVersion + ".json")
	if _, ok := manifest.version(); !ok {
		return "", fmt.Errorf("invalid relay version %q", releaseVersion)
	}
	return manifest, nil
}

// runSelfUpdate fetches latest.json, verifies the sha256 of the downloaded
// artifact, and atomically replaces the relay binary (and refreshes yt-dlp when a
// matching entry is present), then restarts the service.
func runSelfUpdate(args []string) error {
	fs := flag.NewFlagSet("self-update", flag.ContinueOnError)
	apiURL := fs.String("api-url", "", "Stoarama API base URL (default: from config)")
	manifestName := fs.String("manifest", string(liveReleaseManifest), "release manifest name")
	rollback := fs.Bool("rollback", false, "restore the previous relay binary")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *rollback {
		target, previous, previousManifest, err := previousRelay()
		if err != nil {
			return err
		}
		if err := setUpdateManifest(previousManifest); err != nil {
			return err
		}
		if err := atomicWriteExecutable(target, previous); err != nil {
			return err
		}
		return restartAfterSelfUpdate()
	}
	manifest := releaseManifest(*manifestName)
	base := strings.TrimRight(strings.TrimSpace(*apiURL), "/")
	if base == "" {
		if cfg, err := loadConfig(); err == nil {
			base = cfg.APIURL
		}
	}
	if base == "" {
		base = defaultAPIURL
	}

	lj, err := fetchLatest(base, manifest)
	if err != nil {
		return err
	}
	if strings.TrimSpace(lj.Version) == "" {
		return fmt.Errorf("release manifest has no version")
	}
	target := runtime.GOOS + "-" + runtime.GOARCH

	rel, ok := lj.Relay[target]
	if !ok {
		return fmt.Errorf("no relay artifact for %s in latest.json", target)
	}
	if err := setUpdateManifest(manifest); err != nil {
		return err
	}
	diskUpdated, err := updateRelayBinary(base, rel)
	if err != nil {
		if manifest != liveReleaseManifest {
			_ = setUpdateManifest(liveReleaseManifest)
		}
		return err
	}
	relayUpdated := diskUpdated || lj.Version != version
	if !relayUpdated {
		fmt.Printf("relay already up to date (%s)\n", version)
	} else {
		fmt.Printf("updated relay %s -> %s\n", version, lj.Version)
	}

	// yt-dlp refresh follows the same download+verify+atomic-rename pattern.
	if yt, ok := lj.Ytdlp[target]; ok && strings.TrimSpace(yt.SHA256) != "" {
		updated, err := updateExecutableIfChanged(base, yt, "yt-dlp")
		if err != nil {
			fmt.Fprintf(os.Stderr, "yt-dlp refresh skipped: %v\n", err)
		} else if updated {
			fmt.Println("yt-dlp refreshed")
		} else {
			fmt.Println("yt-dlp already up to date")
		}
	}
	if relayUpdated {
		return restartAfterSelfUpdate()
	}
	return nil
}

// selfUpdateLoop periodically checks latest.json and applies sha256-verified relay +
// yt-dlp updates, reusing the same download/verify/atomic-rename path as the manual
// `self-update` command. It logs every outcome. The first check runs one interval
// after start (not immediately) so a fresh install does not restart itself on boot.
// When the relay binary itself changes it kickstarts the service so the new binary is
// exec'd; a yt-dlp-only refresh is picked up by the next capture without a restart.
func selfUpdateLoop(ctx context.Context, cfg relayConfig) {
	ticker := time.NewTicker(selfUpdateInterval)
	defer ticker.Stop()
	manifest := cfg.updateManifest()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if manifest != liveReleaseManifest {
				live, err := fetchLatest(cfg.APIURL, liveReleaseManifest)
				if err != nil {
					log.Printf("relay self-update: check live promotion: %v", err)
				} else if next := updateManifestAfterPromotion(manifest, live.Version, version); next != manifest {
					if err := setUpdateManifest(liveReleaseManifest); err != nil {
						log.Printf("relay self-update: clear candidate pin: %v", err)
					} else {
						manifest = next
						log.Printf("relay self-update: candidate promoted; following latest.json")
					}
				}
			}
			checkAndApplyUpdate(cfg.APIURL, manifest)
		}
	}
}

// checkAndApplyUpdate performs one self-update pass: fetch the manifest, apply a newer
// relay binary and refresh yt-dlp (both sha256-verified), and restart only if the relay
// binary actually changed. Errors are logged and swallowed so a bad manifest or a
// transient network failure never takes the relay down.
func checkAndApplyUpdate(base string, manifest releaseManifest) {
	lastUpdaterUnix.Store(time.Now().UTC().UnixNano())
	lj, err := fetchLatest(base, manifest)
	if err != nil {
		log.Printf("relay self-update: fetch latest.json: %v", err)
		return
	}
	target := runtime.GOOS + "-" + runtime.GOARCH

	relayUpdated := false
	if rel, ok := lj.Relay[target]; ok && lj.Version != "" && lj.Version != version {
		if _, err := updateRelayBinary(base, rel); err != nil {
			log.Printf("relay self-update: relay %s -> %s failed: %v", version, lj.Version, err)
		} else {
			log.Printf("relay self-update: relay %s -> %s applied", version, lj.Version)
			relayUpdated = true
		}
	} else {
		log.Printf("relay self-update: relay up to date (%s)", version)
	}

	if yt, ok := lj.Ytdlp[target]; ok && strings.TrimSpace(yt.SHA256) != "" {
		updated, err := updateExecutableIfChanged(base, yt, "yt-dlp")
		if err != nil {
			log.Printf("relay self-update: yt-dlp refresh failed: %v", err)
		} else if updated {
			log.Printf("relay self-update: yt-dlp refreshed")
		} else {
			log.Printf("relay self-update: yt-dlp up to date")
		}
	}

	if relayUpdated {
		log.Printf("relay self-update: restarting service to load new binary")
		if err := restartAfterSelfUpdate(); err != nil {
			log.Printf("relay self-update: restart failed: %v", err)
		}
	}
}

func fetchLatest(base string, manifest releaseManifest) (latestJSON, error) {
	var lj latestJSON
	if !manifest.valid() {
		return lj, fmt.Errorf("invalid release manifest %q", manifest)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/relay/download/"+string(manifest), nil)
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

func (cfg relayConfig) updateManifest() releaseManifest {
	if cfg.UpdateManifest.valid() {
		return cfg.UpdateManifest
	}
	return liveReleaseManifest
}

func updateManifestAfterPromotion(current releaseManifest, liveVersion, runningVersion string) releaseManifest {
	if current != liveReleaseManifest && liveVersion == runningVersion {
		return liveReleaseManifest
	}
	return current
}

func setUpdateManifest(manifest releaseManifest) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	desired := manifest
	if desired == liveReleaseManifest {
		desired = ""
	}
	if cfg.UpdateManifest == desired {
		return nil
	}
	cfg.UpdateManifest = desired
	return saveConfig(cfg)
}

// updateRelayBinary downloads the relay tarball, verifies its sha256, extracts the
// stoarama-relay entry, and atomically replaces the installed binary.
func updateRelayBinary(base string, art latestArtifact) (bool, error) {
	data, err := downloadVerified(base, art)
	if err != nil {
		return false, err
	}
	binBytes, err := extractTarGzEntry(data, "stoarama-relay")
	if err != nil {
		return false, err
	}
	dst, err := binDir()
	if err != nil {
		return false, err
	}
	target := filepath.Join(dst, "stoarama-relay")
	previousManifest, err := installedRelayManifest(target)
	if err != nil {
		return false, err
	}
	return replaceRelayBinary(target, binBytes, previousManifest)
}

func replaceRelayBinary(target string, data []byte, previousManifest releaseManifest) (bool, error) {
	current, err := os.ReadFile(target)
	if err != nil && !os.IsNotExist(err) {
		return false, fmt.Errorf("read current relay: %w", err)
	}
	if err == nil {
		if bytes.Equal(current, data) {
			return false, nil
		}
		if err := storePreviousRelay(target, current, previousManifest); err != nil {
			return false, err
		}
	}
	if err := atomicWriteExecutable(target, data); err != nil {
		return false, err
	}
	return true, nil
}

func storePreviousRelay(target string, previous []byte, manifest releaseManifest) error {
	previousVersion, ok := manifest.version()
	if !ok {
		return fmt.Errorf("previous relay manifest is not immutable")
	}
	if err := atomicWriteExecutable(target+".previous-"+previousVersion, previous); err != nil {
		return fmt.Errorf("preserve previous relay: %w", err)
	}
	if err := atomicWriteFile(target+".previous-manifest", []byte(manifest), 0o600); err != nil {
		return fmt.Errorf("preserve previous relay manifest: %w", err)
	}
	return nil
}

func ensureRollbackBaseline(cfg relayConfig) error {
	dst, err := binDir()
	if err != nil {
		return err
	}
	return ensureRollbackBaselineAt(cfg, filepath.Join(dst, "stoarama-relay"))
}

func ensureRollbackBaselineAt(cfg relayConfig, target string) error {
	if _, _, _, err := previousRelayAt(target); err == nil {
		return nil
	}
	manifest, err := immutableReleaseManifest(version)
	if err != nil {
		return err
	}
	lj, err := fetchLatest(cfg.APIURL, manifest)
	if err != nil {
		return err
	}
	previousManifest, err := immutableReleaseManifest(lj.PreviousVersion)
	if err != nil {
		return fmt.Errorf("release manifest has no valid previous_version: %w", err)
	}
	art, ok := lj.PreviousRelay[runtime.GOOS+"-"+runtime.GOARCH]
	if !ok {
		return fmt.Errorf("release manifest has no previous relay for this target")
	}
	data, err := downloadVerified(cfg.APIURL, art)
	if err != nil {
		return err
	}
	previous, err := extractTarGzEntry(data, "stoarama-relay")
	if err != nil {
		return err
	}
	return storePreviousRelay(target, previous, previousManifest)
}

func installedRelayManifest(target string) (releaseManifest, error) {
	if _, err := os.Stat(target); os.IsNotExist(err) {
		return "", nil
	} else if err != nil {
		return "", fmt.Errorf("stat current relay: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, target, "version").Output()
	if err != nil {
		return "", fmt.Errorf("read current relay version: %w", err)
	}
	fields := strings.Fields(string(out))
	if len(fields) != 2 || fields[0] != "stoarama-relay" {
		return "", fmt.Errorf("invalid current relay version output")
	}
	return immutableReleaseManifest(fields[1])
}

func previousRelay() (string, []byte, releaseManifest, error) {
	dst, err := binDir()
	if err != nil {
		return "", nil, "", err
	}
	return previousRelayAt(filepath.Join(dst, "stoarama-relay"))
}

func previousRelayAt(target string) (string, []byte, releaseManifest, error) {
	manifestBytes, err := os.ReadFile(target + ".previous-manifest")
	if err != nil {
		return "", nil, "", fmt.Errorf("read previous relay manifest: %w", err)
	}
	manifest := releaseManifest(strings.TrimSpace(string(manifestBytes)))
	previousVersion, ok := manifest.version()
	if !ok {
		return "", nil, "", fmt.Errorf("invalid previous relay manifest")
	}
	previous, err := os.ReadFile(target + ".previous-" + previousVersion)
	if err != nil {
		return "", nil, "", fmt.Errorf("read previous relay: %w", err)
	}
	return target, previous, manifest, nil
}

// updateExecutableIfChanged verifies the installed executable first, downloading
// and atomically replacing it only when its manifest digest differs.
func updateExecutableIfChanged(base string, art latestArtifact, name string) (bool, error) {
	dst, err := binDir()
	if err != nil {
		return false, err
	}
	return updateExecutableAtPathIfChanged(base, art, filepath.Join(dst, name))
}

func updateExecutableAtPathIfChanged(base string, art latestArtifact, target string) (bool, error) {
	match, err := fileMatchesSHA256(target, art.SHA256)
	if err != nil && !os.IsNotExist(err) {
		return false, err
	}
	if match {
		return false, nil
	}
	data, err := downloadVerified(base, art)
	if err != nil {
		return false, err
	}
	return true, atomicWriteExecutable(target, data)
}

func fileMatchesSHA256(path, want string) (bool, error) {
	want = strings.ToLower(strings.TrimSpace(want))
	if len(want) != sha256.Size*2 {
		return false, fmt.Errorf("invalid sha256 for %s", filepath.Base(path))
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return false, err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]) == want, nil
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
	return atomicWriteFile(dst, data, 0o755)
}

func atomicWriteFile(dst string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("create %s: %w", filepath.Dir(dst), err)
	}
	tmp := dst + ".new"
	if err := os.WriteFile(tmp, data, mode); err != nil {
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("replace %s: %w", dst, err)
	}
	return nil
}
