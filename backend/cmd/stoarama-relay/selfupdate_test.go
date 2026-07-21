package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

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
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/relay/download/latest-candidate1.json":
			_ = json.NewEncoder(w).Encode(manifest)
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
	if got := updateManifestAfterPromotion(candidate, "old12345", "new12345"); got != candidate {
		t.Fatalf("candidate changed before promotion: %q", got)
	}
	if got := updateManifestAfterPromotion(candidate, "new12345", "new12345"); got != liveReleaseManifest {
		t.Fatalf("candidate remained pinned after promotion: %q", got)
	}
}

func testSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
