package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
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
	"sync"
	"sync/atomic"
	"time"

	"github.com/daydemir/stoarama/backend/internal/recordingapi"
)

var lastUpdaterUnix atomic.Int64
var releasePublicKeyBase64 string
var restartRelayAfterSelfUpdate = restartAfterSelfUpdate
var selfUpdateIdlePollInterval = time.Second

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
	Deno            map[string]latestArtifact `json:"deno"`
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
		return restartRelayAfterSelfUpdate()
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
	ytdlpPresent, _, err := refreshExecutableDependency(base, lj.Ytdlp, target, "yt-dlp")
	if err != nil {
		return fmt.Errorf("refresh yt-dlp before relay activation: %w", err)
	}
	denoPresent, _, err := refreshExecutableDependency(base, lj.Deno, target, "deno")
	if err != nil {
		return fmt.Errorf("refresh Deno before relay activation: %w", err)
	}
	if denoPresent && !ytdlpPresent {
		return fmt.Errorf("release includes Deno without a pinned yt-dlp artifact for %s", target)
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

	runtimeActivationNeeded := false
	if denoPresent && strings.TrimSpace(os.Getenv("YT_DLP_JS_RUNTIME")) == "" {
		if bd, pathErr := binDir(); pathErr == nil && ytdlpJSRuntimeReady(bd, filepath.Join(bd, "yt-dlp")) {
			runtimeActivationNeeded = true
		} else {
			fmt.Fprintln(os.Stderr, "Deno runtime activation held: installed dependencies failed capability checks")
		}
	}
	if relayUpdated || runtimeActivationNeeded {
		return restartRelayAfterSelfUpdate()
	}
	return nil
}

// selfUpdateLoop periodically checks latest.json and applies sha256-verified relay +
// yt-dlp updates, reusing the same download/verify/atomic-rename path as the manual
// `self-update` command. It logs every outcome. The first check runs one interval
// after start (not immediately) so a fresh install does not restart itself on boot.
// When the relay binary itself changes it kickstarts the service so the new binary is
// exec'd; a yt-dlp-only refresh is picked up by the next capture without a restart.
func selfUpdateLoop(ctx context.Context, cfg relayConfig, updates <-chan recordingapi.RelayHeartbeatUpdate, activeJobs *atomic.Int64, leaseGate *sync.RWMutex, drainForUpdate *atomic.Bool) {
	ticker := time.NewTicker(selfUpdateInterval)
	defer ticker.Stop()
	manifest := cfg.updateManifest()
	pendingIntent := cfg.PendingUpdateIntent
	for {
		select {
		case <-ctx.Done():
			return
		case update := <-updates:
			if update.AckRequestID != "" {
				if err := clearPendingRelayUpdate(update.AckRequestID); err != nil {
					log.Printf("relay update intent ack persistence failed: %v", err)
					continue
				}
				if drainForUpdate != nil {
					drainForUpdate.Store(false)
				}
				pendingIntent = nil
			}
			if update.Intent == nil {
				continue
			}
			intent := *update.Intent
			if err := validateRelayUpdateIntent(cfg.NodeID, intent); err != nil {
				log.Printf("relay update intent rejected: %v", err)
				continue
			}
			pinned := releaseManifest(intent.ManifestName)
			if err := setPendingRelayUpdateIntent(intent); err != nil {
				log.Printf("relay update intent pin failed: %v", err)
				continue
			}
			manifest = pinned
			pendingIntent = &intent
			applyRelayUpdateAfterDrain(ctx, cfg.APIURL, manifest, &intent, activeJobs, leaseGate, drainForUpdate)
		case <-ticker.C:
			if manifest != liveReleaseManifest && pendingIntent == nil {
				live, err := fetchLatest(cfg.APIURL, liveReleaseManifest)
				if err != nil {
					log.Printf("relay self-update: check live promotion: %v", err)
				} else if pinned, pinnedErr := fetchLatest(cfg.APIURL, manifest); pinnedErr != nil {
					log.Printf("relay self-update: check candidate promotion: %v", pinnedErr)
				} else if next := updateManifestAfterPromotion(manifest, pinned.PreviousVersion, live.Version, live.PreviousVersion, version); next != manifest {
					if err := setUpdateManifest(liveReleaseManifest); err != nil {
						log.Printf("relay self-update: clear candidate pin: %v", err)
					} else {
						manifest = next
						log.Printf("relay self-update: candidate promoted; following latest.json")
					}
				}
			}
			applyRelayUpdateAfterDrain(ctx, cfg.APIURL, manifest, pendingIntent, activeJobs, leaseGate, drainForUpdate)
		}
	}
}

func applyRelayUpdateAfterDrain(ctx context.Context, base string, manifest releaseManifest, intent *recordingapi.RelayUpdateIntent, activeJobs *atomic.Int64, leaseGate *sync.RWMutex, drainForUpdate *atomic.Bool) {
	if !checkAndApplyUpdateExpected(base, manifest, intent, activeJobs, leaseGate, drainForUpdate) {
		return
	}
	ticker := time.NewTicker(selfUpdateIdlePollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !checkAndApplyUpdateExpected(base, manifest, intent, activeJobs, leaseGate, drainForUpdate) {
				return
			}
		}
	}
}

// checkAndApplyUpdate performs one self-update pass: fetch the manifest, apply a newer
// relay binary and refresh yt-dlp (both sha256-verified), and restart only if the relay
// binary actually changed. Errors are logged and swallowed so a bad manifest or a
// transient network failure never takes the relay down.
func checkAndApplyUpdate(base string, manifest releaseManifest, activeJobs *atomic.Int64, leaseGate *sync.RWMutex, drainForUpdate *atomic.Bool) bool {
	return checkAndApplyUpdateExpected(base, manifest, nil, activeJobs, leaseGate, drainForUpdate)
}

func checkAndApplyUpdateExpected(base string, manifest releaseManifest, intent *recordingapi.RelayUpdateIntent, activeJobs *atomic.Int64, leaseGate *sync.RWMutex, drainForUpdate *atomic.Bool) bool {
	lastUpdaterUnix.Store(time.Now().UTC().UnixNano())
	if drainForUpdate != nil {
		drainForUpdate.Store(true)
	}
	unlock, deferred, count := lockRelayRestartGate(leaseGate, activeJobs)
	if deferred {
		unlock()
		log.Printf("relay self-update: draining %d recording job(s) before any update mutation", count)
		return true
	}
	defer unlock()
	holdDrain := intent != nil
	defer func() {
		if !holdDrain && drainForUpdate != nil {
			drainForUpdate.Store(false)
		}
	}()
	expectedVersion, expectedSHA := "", ""
	if intent != nil {
		expectedVersion, expectedSHA = intent.TargetVersion, intent.ManifestSHA256
	}
	lj, err := fetchLatestExpected(base, manifest, expectedVersion, expectedSHA)
	if err != nil {
		log.Printf("relay self-update: fetch latest.json: %v", err)
		return false
	}
	if expectedVersion != "" && !relayIntentUpgradeAllowed(version, lj) {
		log.Printf("relay self-update: typed intent target %s is not an immediate successor of %s", lj.Version, version)
		return false
	}
	target := runtime.GOOS + "-" + runtime.GOARCH
	if intent != nil {
		relayArtifact, ok := lj.Relay[target]
		if !ok || strings.TrimSpace(relayArtifact.SHA256) != intent.RelayArtifactSHA256 {
			log.Printf("relay update intent: signed relay artifact digest differs")
			return false
		}
	}
	ytdlpPresent, _, err := refreshExecutableDependency(base, lj.Ytdlp, target, "yt-dlp")
	if err != nil {
		log.Printf("relay self-update: yt-dlp refresh failed; relay activation held: %v", err)
		return false
	}
	denoPresent, _, err := refreshExecutableDependency(base, lj.Deno, target, "deno")
	if err != nil {
		log.Printf("relay self-update: Deno refresh failed; relay activation held: %v", err)
		return false
	}
	if denoPresent && !ytdlpPresent {
		log.Printf("relay self-update: release includes Deno without pinned yt-dlp for %s; relay activation held", target)
		return false
	}
	runtimeActivationNeeded := false
	if denoPresent && strings.TrimSpace(os.Getenv("YT_DLP_JS_RUNTIME")) == "" {
		if bd, pathErr := binDir(); pathErr == nil && ytdlpJSRuntimeReady(bd, filepath.Join(bd, "yt-dlp")) {
			runtimeActivationNeeded = true
		} else {
			log.Printf("relay self-update: Deno runtime activation held; installed dependencies failed capability checks")
		}
	}

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

	if intent != nil && (relayUpdated || lj.Version == version) {
		installedSHA, err := installedRelaySHA256()
		if err != nil {
			log.Printf("relay update intent: hash installed relay: %v", err)
			return false
		}
		if installedSHA != intent.InstalledSHA256 {
			log.Printf("relay update intent: installed relay digest differs")
			return false
		}
		result := recordingapi.RelayUpdateResult{RequestID: intent.RequestID, TargetVersion: intent.TargetVersion,
			ManifestName: intent.ManifestName, ManifestSHA256: intent.ManifestSHA256,
			RelayArtifactSHA256: intent.RelayArtifactSHA256, InstalledSHA256: installedSHA, AppliedAt: time.Now().UTC().Format(time.RFC3339Nano)}
		if err := setPendingRelayUpdateResult(result); err != nil {
			log.Printf("relay update intent: persist applied proof: %v", err)
			return false
		}
	}
	if relayUpdated || runtimeActivationNeeded {
		holdDrain = true
		log.Printf("relay self-update: restarting service to load new binary")
		if err := restartRelayAfterSelfUpdate(); err != nil {
			log.Printf("relay self-update: restart failed: %v", err)
			if drainForUpdate != nil {
				drainForUpdate.Store(false)
			}
		}
	}
	return false
}

func relayIntentUpgradeAllowed(current string, target latestJSON) bool {
	return strings.TrimSpace(current) != "" && target.Version != current && target.PreviousVersion == current
}

func validateRelayUpdateIntent(nodeID int64, intent recordingapi.RelayUpdateIntent) error {
	if intent.NodeID != nodeID || intent.NodeID <= 0 {
		return fmt.Errorf("node identity differs")
	}
	if strings.TrimSpace(intent.RequestID) == "" || intent.Status != "pending" {
		return fmt.Errorf("request identity or status is invalid")
	}
	manifest, err := immutableReleaseManifest(intent.TargetVersion)
	if err != nil || string(manifest) != intent.ManifestName {
		return fmt.Errorf("manifest identity is invalid")
	}
	digest, err := hex.DecodeString(intent.ManifestSHA256)
	if err != nil || len(digest) != sha256.Size || intent.ManifestSHA256 != strings.ToLower(intent.ManifestSHA256) {
		return fmt.Errorf("manifest digest is invalid")
	}
	if !lowerHexSHA256(intent.RelayArtifactSHA256) {
		return fmt.Errorf("relay artifact digest is invalid")
	}
	if !lowerHexSHA256(intent.InstalledSHA256) {
		return fmt.Errorf("installed relay digest is invalid")
	}
	return nil
}

func refreshExecutableDependency(base string, artifacts map[string]latestArtifact, target, name string) (bool, bool, error) {
	artifact, ok := artifacts[target]
	if !ok || strings.TrimSpace(artifact.SHA256) == "" {
		return false, false, nil
	}
	updated, err := updateExecutableIfChanged(base, artifact, name)
	if err != nil {
		return true, false, err
	}
	if updated {
		log.Printf("relay self-update: %s refreshed", name)
	} else {
		log.Printf("relay self-update: %s up to date", name)
	}
	return true, updated, nil
}

func lockRelayRestartGate(leaseGate *sync.RWMutex, activeJobs *atomic.Int64) (func(), bool, int64) {
	if leaseGate != nil {
		leaseGate.Lock()
	}
	deferred, count := relaySelfUpdateDeferred(activeJobs)
	return func() {
		if leaseGate != nil {
			leaseGate.Unlock()
		}
	}, deferred, count
}

func relaySelfUpdateDeferred(activeJobs *atomic.Int64) (bool, int64) {
	if activeJobs == nil {
		return false, 0
	}
	count := activeJobs.Load()
	return count > 0, count
}

func fetchLatest(base string, manifest releaseManifest) (latestJSON, error) {
	return fetchLatestExpected(base, manifest, "", "")
}

func fetchLatestExpected(base string, manifest releaseManifest, expectedVersion, expectedSHA string) (latestJSON, error) {
	var lj latestJSON
	if !manifest.valid() {
		return lj, fmt.Errorf("invalid release manifest %q", manifest)
	}
	publicKeys, err := releasePublicKeys()
	if err != nil {
		return lj, err
	}
	manifestBytes, err := fetchReleaseFile(base, string(manifest), 1<<20)
	if err != nil {
		return lj, err
	}
	signatureBytes, err := fetchReleaseFile(base, string(manifest)+".sig", 1<<10)
	if err != nil {
		return lj, err
	}
	signature, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(signatureBytes)))
	if err != nil || len(signature) != ed25519.SignatureSize {
		return lj, fmt.Errorf("decode %s signature", manifest)
	}
	verified := false
	for _, publicKey := range publicKeys {
		if ed25519.Verify(publicKey, manifestBytes, signature) {
			verified = true
			break
		}
	}
	if !verified {
		return lj, fmt.Errorf("verify %s signature", manifest)
	}
	if expectedSHA != "" {
		sum := sha256.Sum256(manifestBytes)
		if hex.EncodeToString(sum[:]) != expectedSHA {
			return lj, fmt.Errorf("%s differs from update intent snapshot", manifest)
		}
	}
	if err := json.Unmarshal(manifestBytes, &lj); err != nil {
		return lj, fmt.Errorf("decode latest.json: %w", err)
	}
	if expectedVersion != "" && lj.Version != expectedVersion {
		return latestJSON{}, fmt.Errorf("%s version differs from update intent", manifest)
	}
	return lj, nil
}

func releasePublicKeys() ([]ed25519.PublicKey, error) {
	encodedKeys := strings.Split(strings.TrimSpace(releasePublicKeyBase64), ",")
	keys := make([]ed25519.PublicKey, 0, len(encodedKeys))
	for _, encoded := range encodedKeys {
		publicKey, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
		if err != nil || len(publicKey) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("relay release public key is invalid")
		}
		keys = append(keys, ed25519.PublicKey(publicKey))
	}
	return keys, nil
}

func fetchReleaseFile(base, name string, maxBytes int64) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/relay/download/"+name, nil)
	if err != nil {
		return nil, fmt.Errorf("build %s request: %w", name, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch %s: status=%d", name, resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", name, err)
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("%s exceeds %d bytes", name, maxBytes)
	}
	return data, nil
}

func (cfg relayConfig) updateManifest() releaseManifest {
	if cfg.UpdateManifest.valid() {
		return cfg.UpdateManifest
	}
	return liveReleaseManifest
}

func updateManifestAfterPromotion(current releaseManifest, pinnedPreviousVersion, liveVersion, livePreviousVersion, runningVersion string) releaseManifest {
	pinnedVersion, pinned := current.version()
	if !pinned || pinnedVersion != runningVersion {
		return current
	}
	// Follow live after promotion, or after the live channel has moved beyond the
	// release the candidate was based on. The latter recovers idle canaries whose
	// promotion window was missed without letting a pre-promotion canary downgrade
	// to the older live build.
	if liveVersion == runningVersion || livePreviousVersion == runningVersion ||
		(strings.TrimSpace(pinnedPreviousVersion) != "" && liveVersion != pinnedPreviousVersion) {
		return liveReleaseManifest
	}
	return current
}

func setUpdateManifest(manifest releaseManifest) error {
	relayConfigMu.Lock()
	defer relayConfigMu.Unlock()
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

func setPendingRelayUpdateResult(result recordingapi.RelayUpdateResult) error {
	relayConfigMu.Lock()
	defer relayConfigMu.Unlock()
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	if cfg.PendingUpdateIntent == nil || cfg.PendingUpdateIntent.RequestID != result.RequestID {
		return fmt.Errorf("relay update result has no matching pending intent")
	}
	if cfg.PendingUpdateResult != nil && *cfg.PendingUpdateResult == result {
		return nil
	}
	cfg.PendingUpdateResult = &result
	return saveConfig(cfg)
}

func setPendingRelayUpdateIntent(intent recordingapi.RelayUpdateIntent) error {
	relayConfigMu.Lock()
	defer relayConfigMu.Unlock()
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	if cfg.PendingUpdateIntent != nil && *cfg.PendingUpdateIntent != intent {
		return fmt.Errorf("another relay update intent is already pending")
	}
	manifest := releaseManifest(intent.ManifestName)
	cfg.UpdateManifest = manifest
	cfg.PendingUpdateIntent = &intent
	return saveConfig(cfg)
}

func clearPendingRelayUpdate(requestID string) error {
	relayConfigMu.Lock()
	defer relayConfigMu.Unlock()
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	if cfg.PendingUpdateResult == nil || cfg.PendingUpdateResult.RequestID != requestID ||
		cfg.PendingUpdateIntent == nil || cfg.PendingUpdateIntent.RequestID != requestID {
		return nil
	}
	cfg.PendingUpdateResult = nil
	cfg.PendingUpdateIntent = nil
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

func installedRelaySHA256() (string, error) {
	dir, err := binDir()
	if err != nil {
		return "", err
	}
	data, err := os.ReadFile(filepath.Join(dir, "stoarama-relay"))
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
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
