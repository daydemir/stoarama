package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRelaySelfUpdateDefersForActiveCapture(t *testing.T) {
	if deferUpdate, count := relaySelfUpdateDeferred(nil); deferUpdate || count != 0 {
		t.Fatalf("nil active counter deferred=%t count=%d", deferUpdate, count)
	}
	var active atomic.Int64
	if deferUpdate, count := relaySelfUpdateDeferred(&active); deferUpdate || count != 0 {
		t.Fatalf("idle relay deferred=%t count=%d", deferUpdate, count)
	}
	active.Store(3)
	if deferUpdate, count := relaySelfUpdateDeferred(&active); !deferUpdate || count != 3 {
		t.Fatalf("active relay deferred=%t count=%d, want true/3", deferUpdate, count)
	}
}

func TestRelayRestartGateWaitsForLeaseAdmission(t *testing.T) {
	var gate sync.RWMutex
	var active atomic.Int64
	gate.RLock() // Model the worker immediately before LeaseRecordingJob.

	type result struct {
		deferred bool
		count    int64
	}
	done := make(chan result, 1)
	go func() {
		unlock, deferred, count := lockRelayRestartGate(&gate, &active)
		defer unlock()
		done <- result{deferred: deferred, count: count}
	}()

	select {
	case got := <-done:
		t.Fatalf("restart gate crossed in-flight lease admission: %+v", got)
	case <-time.After(25 * time.Millisecond):
	}

	// A successful lease becomes visible before the worker releases its read lock.
	active.Add(1)
	gate.RUnlock()
	select {
	case got := <-done:
		if !got.deferred || got.count != 1 {
			t.Fatalf("restart gate deferred=%t count=%d, want true/1", got.deferred, got.count)
		}
	case <-time.After(time.Second):
		t.Fatal("restart gate did not resume after lease admission completed")
	}
}

func TestUpdateExecutableIfChangedSkipsMatchingFile(t *testing.T) {
	data := []byte("already installed")
	path := filepath.Join(t.TempDir(), "yt-dlp")
	if err := os.WriteFile(path, data, 0o755); err != nil {
		t.Fatal(err)
	}

	updated, err := updateExecutableAtPathIfChanged("http://127.0.0.1:1", latestArtifact{
		Artifact: "yt-dlp-test",
		SHA256:   testSHA256(data),
	}, path)
	if err != nil {
		t.Fatal(err)
	}
	if updated {
		t.Fatal("matching executable was updated")
	}
}

func TestUpdateExecutableIfChangedReplacesMismatchedFile(t *testing.T) {
	want := []byte("new executable")
	path := filepath.Join(t.TempDir(), "yt-dlp")
	if err := os.WriteFile(path, []byte("old executable"), 0o755); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(want)
	}))
	defer server.Close()

	updated, err := updateExecutableAtPathIfChanged(server.URL, latestArtifact{
		Artifact: "yt-dlp-test",
		SHA256:   testSHA256(want),
	}, path)
	if err != nil {
		t.Fatal(err)
	}
	if !updated {
		t.Fatal("mismatched executable was not updated")
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("installed bytes = %q, want %q", got, want)
	}
}

func TestRunSelfUpdateActivationSafety(t *testing.T) {
	t.Run("legacy manifest without Deno", func(t *testing.T) {
		current := setupSelfUpdateHome(t, "legacy1")
		newRelay := relayScript("legacy2")
		ytdlp := []byte("#!/bin/sh\nprintf '%s\\n' 'legacy yt-dlp'\n")
		manifest := latestJSON{
			Version: "legacy2",
			Relay:   map[string]latestArtifact{testRelayTarget(): testArtifact("relay.tar.gz", testRelayTarball(t, newRelay))},
			Ytdlp:   map[string]latestArtifact{testRelayTarget(): testArtifact("yt-dlp", ytdlp)},
		}
		server := serveTestRelayRelease(t, manifest, map[string][]byte{
			"relay.tar.gz": testRelayTarball(t, newRelay),
			"yt-dlp":       ytdlp,
		})
		restarts := captureSelfUpdateRestarts(t)

		if err := runSelfUpdate([]string{"--api-url", server.URL}); err != nil {
			t.Fatal(err)
		}
		if *restarts != 1 {
			t.Fatalf("restarts=%d want 1", *restarts)
		}
		if got, err := os.ReadFile(current); err != nil || !bytes.Equal(got, newRelay) {
			t.Fatalf("relay updated=%t err=%v", bytes.Equal(got, newRelay), err)
		}
		if _, err := os.Stat(filepath.Join(filepath.Dir(current), "deno")); !os.IsNotExist(err) {
			t.Fatalf("legacy manifest installed Deno: %v", err)
		}
	})

	t.Run("Deno without pinned yt-dlp is rejected", func(t *testing.T) {
		current := setupSelfUpdateHome(t, "badpair1")
		original, err := os.ReadFile(current)
		if err != nil {
			t.Fatal(err)
		}
		newRelay := relayScript("badpair2")
		deno := []byte("#!/bin/sh\nprintf '%s\\n' 'deno 2.8.1'\n")
		relayTar := testRelayTarball(t, newRelay)
		manifest := latestJSON{
			Version: "badpair2",
			Relay:   map[string]latestArtifact{testRelayTarget(): testArtifact("relay.tar.gz", relayTar)},
			Deno:    map[string]latestArtifact{testRelayTarget(): testArtifact("deno", deno)},
		}
		server := serveTestRelayRelease(t, manifest, map[string][]byte{"relay.tar.gz": relayTar, "deno": deno})
		captureSelfUpdateRestarts(t)

		if err := runSelfUpdate([]string{"--api-url", server.URL}); err == nil || !strings.Contains(err.Error(), "without a pinned yt-dlp") {
			t.Fatalf("error=%v", err)
		}
		if got, err := os.ReadFile(current); err != nil || !bytes.Equal(got, original) {
			t.Fatalf("relay changed before dependency validation: err=%v", err)
		}
	})

	t.Run("dependency failure holds relay activation", func(t *testing.T) {
		current := setupSelfUpdateHome(t, "depfail1")
		original, err := os.ReadFile(current)
		if err != nil {
			t.Fatal(err)
		}
		newRelay := relayScript("depfail2")
		relayTar := testRelayTarball(t, newRelay)
		manifest := latestJSON{
			Version: "depfail2",
			Relay:   map[string]latestArtifact{testRelayTarget(): testArtifact("relay.tar.gz", relayTar)},
			Ytdlp:   map[string]latestArtifact{testRelayTarget(): testArtifact("missing-yt-dlp", []byte("expected"))},
		}
		server := serveTestRelayRelease(t, manifest, map[string][]byte{"relay.tar.gz": relayTar})
		captureSelfUpdateRestarts(t)

		if err := runSelfUpdate([]string{"--api-url", server.URL}); err == nil || !strings.Contains(err.Error(), "refresh yt-dlp") {
			t.Fatalf("error=%v", err)
		}
		if got, err := os.ReadFile(current); err != nil || !bytes.Equal(got, original) {
			t.Fatalf("relay changed after dependency failure: err=%v", err)
		}
	})
}

func TestCheckAndApplyUpdateRestartsForReadyDenoActivation(t *testing.T) {
	setupSelfUpdateHome(t, "runtime1")
	ytdlp := []byte("#!/bin/sh\nprintf '%s\\n' '--js-runtimes RUNTIME[:PATH]'\n")
	deno := []byte("#!/bin/sh\nprintf '%s\\n' 'deno 2.8.1'\n")
	manifest := latestJSON{
		Version: "runtime1",
		Ytdlp:   map[string]latestArtifact{testRelayTarget(): testArtifact("yt-dlp", ytdlp)},
		Deno:    map[string]latestArtifact{testRelayTarget(): testArtifact("deno", deno)},
	}
	server := serveTestRelayRelease(t, manifest, map[string][]byte{"yt-dlp": ytdlp, "deno": deno})
	restarts := captureSelfUpdateRestarts(t)
	t.Setenv("YT_DLP_JS_RUNTIME", "")

	checkAndApplyUpdate(server.URL, liveReleaseManifest, &atomic.Int64{}, &sync.RWMutex{})
	if *restarts != 1 {
		t.Fatalf("restarts=%d want 1", *restarts)
	}
}

func setupSelfUpdateHome(t *testing.T, runningVersion string) string {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	dir, err := binDir()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	current := filepath.Join(dir, "stoarama-relay")
	if err := os.WriteFile(current, relayScript(runningVersion), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := saveConfig(relayConfig{NodeID: 1, NodeToken: "test", APIURL: "https://stoarama.invalid"}); err != nil {
		t.Fatal(err)
	}
	oldVersion := version
	version = runningVersion
	t.Cleanup(func() { version = oldVersion })
	return current
}

func captureSelfUpdateRestarts(t *testing.T) *int {
	t.Helper()
	restarts := new(int)
	oldRestart := restartRelayAfterSelfUpdate
	restartRelayAfterSelfUpdate = func() error {
		*restarts++
		return nil
	}
	t.Cleanup(func() { restartRelayAfterSelfUpdate = oldRestart })
	return restarts
}

func serveTestRelayRelease(t *testing.T, manifest latestJSON, artifacts map[string][]byte) *httptest.Server {
	t.Helper()
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	signature := setTestReleaseSigningKey(t, manifestBytes)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/relay/download/")
		switch name {
		case "latest.json":
			_, _ = w.Write(manifestBytes)
		case "latest.json.sig":
			_, _ = w.Write([]byte(signature))
		default:
			if artifact, ok := artifacts[name]; ok {
				_, _ = w.Write(artifact)
				return
			}
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func testRelayTarget() string {
	return runtime.GOOS + "-" + runtime.GOARCH
}

func testArtifact(name string, data []byte) latestArtifact {
	return latestArtifact{Artifact: name, SHA256: testSHA256(data)}
}

func relayScript(releaseVersion string) []byte {
	return []byte("#!/bin/sh\nprintf 'stoarama-relay %s\\n' '" + releaseVersion + "'\n")
}

func TestReplaceRelayBinaryPreservesPrevious(t *testing.T) {
	target := filepath.Join(t.TempDir(), "stoarama-relay")
	if err := os.WriteFile(target, []byte("old"), 0o755); err != nil {
		t.Fatal(err)
	}
	previousManifest := releaseManifest("latest-old12345.json")
	if _, err := replaceRelayBinary(target, []byte("new"), previousManifest); err != nil {
		t.Fatal(err)
	}
	previous, err := os.ReadFile(target + ".previous-old12345")
	if err != nil {
		t.Fatal(err)
	}
	if string(previous) != "old" {
		t.Fatalf("previous bytes = %q", previous)
	}
	manifestBytes, err := os.ReadFile(target + ".previous-manifest")
	if err != nil {
		t.Fatal(err)
	}
	if string(manifestBytes) != string(previousManifest) {
		t.Fatalf("previous manifest = %q", manifestBytes)
	}
	updated, err := replaceRelayBinary(target, []byte("new"), releaseManifest("latest-new12345.json"))
	if err != nil {
		t.Fatal(err)
	}
	if updated {
		t.Fatal("identical relay binary was replaced")
	}
	previous, err = os.ReadFile(target + ".previous-old12345")
	if err != nil {
		t.Fatal(err)
	}
	if string(previous) != "old" {
		t.Fatalf("second update overwrote previous bytes: %q", previous)
	}
	manifestBytes, err = os.ReadFile(target + ".previous-manifest")
	if err != nil {
		t.Fatal(err)
	}
	if string(manifestBytes) != string(previousManifest) {
		t.Fatalf("second update overwrote previous manifest: %q", manifestBytes)
	}
}

func TestFailedBackupCommitKeepsPreviousPair(t *testing.T) {
	target := filepath.Join(t.TempDir(), "stoarama-relay")
	stableManifest := releaseManifest("latest-stable1.json")
	if err := os.WriteFile(target, []byte("current"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target+".previous-stable1", []byte("stable"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target+".previous-manifest", []byte(stableManifest), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(target+".previous-manifest.new", 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := replaceRelayBinary(target, []byte("new"), releaseManifest("latest-current1.json")); err == nil {
		t.Fatal("backup marker commit unexpectedly succeeded")
	}
	_, previous, manifest, err := previousRelayAt(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(previous) != "stable" || manifest != stableManifest {
		t.Fatalf("rollback pair=(%q,%q), want stable pair", previous, manifest)
	}
}

func TestEnsureRollbackBaselineBootstrapsOldUpdater(t *testing.T) {
	oldVersion := version
	version = "candidate1"
	t.Cleanup(func() { version = oldVersion })
	previousBinary := []byte("stable relay")
	artifact := testRelayTarball(t, previousBinary)
	manifest := latestJSON{
		Version:         "candidate1",
		PreviousVersion: "stable1",
		PreviousRelay: map[string]latestArtifact{
			runtime.GOOS + "-" + runtime.GOARCH: {
				Artifact: "stable.tar.gz",
				SHA256:   testSHA256(artifact),
			},
		},
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	signature := setTestReleaseSigningKey(t, manifestBytes)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/relay/download/latest-candidate1.json":
			_, _ = w.Write(manifestBytes)
		case "/relay/download/latest-candidate1.json.sig":
			_, _ = w.Write([]byte(signature))
		case "/relay/download/stable.tar.gz":
			_, _ = w.Write(artifact)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	target := filepath.Join(t.TempDir(), "stoarama-relay")
	if err := os.WriteFile(target, []byte("candidate relay"), 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := relayConfig{APIURL: server.URL}
	if err := ensureRollbackBaselineAt(cfg, target); err != nil {
		t.Fatal(err)
	}
	_, previous, previousManifest, err := previousRelayAt(target)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(previous, previousBinary) || previousManifest != "latest-stable1.json" {
		t.Fatalf("rollback baseline=(%q,%q)", previous, previousManifest)
	}
}

func TestPrepareSelfUpdatesToleratesUnconfiguredBuild(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	oldVersion := version
	oldPublicKey := releasePublicKeyBase64
	version = "dev"
	releasePublicKeyBase64 = ""
	t.Cleanup(func() {
		version = oldVersion
		releasePublicKeyBase64 = oldPublicKey
	})
	enabled, err := prepareSelfUpdates(relayConfig{APIURL: "https://stoarama.invalid"})
	if err != nil {
		t.Fatal(err)
	}
	if enabled {
		t.Fatal("self-updates enabled without a signed rollback baseline")
	}
}

func TestPrepareSelfUpdatesFailsClosedForConfiguredBuild(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	oldVersion := version
	oldPublicKey := releasePublicKeyBase64
	version = "signed1"
	releasePublicKeyBase64 = base64.StdEncoding.EncodeToString(make([]byte, ed25519.PublicKeySize))
	t.Cleanup(func() {
		version = oldVersion
		releasePublicKeyBase64 = oldPublicKey
	})
	enabled, err := prepareSelfUpdates(relayConfig{APIURL: "://invalid"})
	if err == nil || enabled {
		t.Fatalf("prepareSelfUpdates=(%v,%v) want disabled error", enabled, err)
	}
}

func TestFetchLatestRequiresValidManifestSignature(t *testing.T) {
	manifest := []byte(`{"version":"signed1","relay":{},"ytdlp":{},"previous_version":"old1","previous_relay":{}}`)
	seed := sha256.Sum256([]byte("stoarama relay manifest test key"))
	privateKey := ed25519.NewKeyFromSeed(seed[:])
	publicKey := privateKey.Public().(ed25519.PublicKey)
	oldPublicKey := releasePublicKeyBase64
	releasePublicKeyBase64 = base64.StdEncoding.EncodeToString(publicKey)
	t.Cleanup(func() { releasePublicKeyBase64 = oldPublicKey })

	for _, test := range []struct {
		name      string
		manifest  []byte
		signature []byte
		wantError bool
	}{
		{name: "valid", manifest: manifest, signature: ed25519.Sign(privateKey, manifest)},
		{name: "tampered", manifest: append(append([]byte(nil), manifest...), ' '), signature: ed25519.Sign(privateKey, manifest), wantError: true},
		{name: "wrong key", manifest: manifest, signature: ed25519.Sign(ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize)), manifest), wantError: true},
		{name: "unsigned", manifest: manifest, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/relay/download/latest.json":
					_, _ = w.Write(test.manifest)
				case "/relay/download/latest.json.sig":
					if len(test.signature) == 0 {
						http.NotFound(w, r)
						return
					}
					_, _ = w.Write([]byte(base64.StdEncoding.EncodeToString(test.signature)))
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			got, err := fetchLatest(server.URL, liveReleaseManifest)
			if test.wantError {
				if err == nil {
					t.Fatal("unsigned or invalid manifest accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.Version != "signed1" {
				t.Fatalf("version=%q", got.Version)
			}
		})
	}
}

func TestFetchLatestAcceptsOverlappingRotationKey(t *testing.T) {
	manifest := []byte(`{"version":"rotated1","relay":{},"ytdlp":{},"previous_version":"old1","previous_relay":{}}`)
	oldSeed := sha256.Sum256([]byte("old relay signing key"))
	newSeed := sha256.Sum256([]byte("new relay signing key"))
	oldKey := ed25519.NewKeyFromSeed(oldSeed[:])
	newKey := ed25519.NewKeyFromSeed(newSeed[:])
	oldPublicKey := releasePublicKeyBase64
	releasePublicKeyBase64 = strings.Join([]string{
		base64.StdEncoding.EncodeToString(oldKey.Public().(ed25519.PublicKey)),
		base64.StdEncoding.EncodeToString(newKey.Public().(ed25519.PublicKey)),
	}, ",")
	t.Cleanup(func() { releasePublicKeyBase64 = oldPublicKey })

	signature := ed25519.Sign(newKey, manifest)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/relay/download/latest.json":
			_, _ = w.Write(manifest)
		case "/relay/download/latest.json.sig":
			_, _ = w.Write([]byte(base64.StdEncoding.EncodeToString(signature)))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	got, err := fetchLatest(server.URL, liveReleaseManifest)
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != "rotated1" {
		t.Fatalf("version=%q", got.Version)
	}
}

func TestReleasePublicKeysRejectsMalformedRotationSet(t *testing.T) {
	oldPublicKey := releasePublicKeyBase64
	releasePublicKeyBase64 = base64.StdEncoding.EncodeToString(make([]byte, ed25519.PublicKeySize)) + ",invalid"
	t.Cleanup(func() { releasePublicKeyBase64 = oldPublicKey })
	if _, err := releasePublicKeys(); err == nil {
		t.Fatal("malformed overlapping key set accepted")
	}
}

func setTestReleaseSigningKey(t *testing.T, manifest []byte) string {
	t.Helper()
	seed := sha256.Sum256([]byte(t.Name()))
	privateKey := ed25519.NewKeyFromSeed(seed[:])
	oldPublicKey := releasePublicKeyBase64
	releasePublicKeyBase64 = base64.StdEncoding.EncodeToString(privateKey.Public().(ed25519.PublicKey))
	t.Cleanup(func() { releasePublicKeyBase64 = oldPublicKey })
	return base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, manifest))
}

func testRelayTarball(t *testing.T, binary []byte) []byte {
	t.Helper()
	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: "stoarama-relay", Mode: 0o755, Size: int64(len(binary))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(binary); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return compressed.Bytes()
}

func TestValidManifestName(t *testing.T) {
	for _, name := range []string{"latest.json", "latest-abcdef12.json", "latest-v1.2.3.json"} {
		if !releaseManifest(name).valid() {
			t.Fatalf("expected %q to be valid", name)
		}
	}
	for _, name := range []string{"candidate.json", "latestcandidate.json", "latest-.json", "latest-1..2.json", "latest-1..json", "latest-_bad.json", "latest-bad_.json", "../latest.json", "latest/other.json"} {
		if releaseManifest(name).valid() {
			t.Fatalf("expected %q to be invalid", name)
		}
	}
}

func TestCandidateManifestStaysPinnedUntilPromotion(t *testing.T) {
	candidate := releaseManifest("latest-new12345.json")
	if got := updateManifestAfterPromotion(candidate, "old12345", "older1234", "new12345"); got != candidate {
		t.Fatalf("candidate changed before promotion: %q", got)
	}
	if got := updateManifestAfterPromotion(candidate, "new12345", "old12345", "new12345"); got != liveReleaseManifest {
		t.Fatalf("candidate remained pinned after promotion: %q", got)
	}
	if got := updateManifestAfterPromotion(candidate, "newer1234", "new12345", "new12345"); got != liveReleaseManifest {
		t.Fatalf("late candidate remained pinned after next promotion: %q", got)
	}
	if got := updateManifestAfterPromotion(candidate, "newer1234", "new12345", "other1234"); got != candidate {
		t.Fatalf("non-running candidate changed: %q", got)
	}
}

func testSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
