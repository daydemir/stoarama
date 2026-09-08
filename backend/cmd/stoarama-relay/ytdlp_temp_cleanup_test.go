package main

import (
	"bytes"
	"crypto/sha256"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCleanupYTDLPTempDryRunSelectsOnlySafeStaleDirectories(t *testing.T) {
	root := t.TempDir()
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	old := now.Add(-16 * time.Minute)
	young := now.Add(-14 * time.Minute)

	stale := filepath.Join(root, "_MEIstale1")
	active := filepath.Join(root, "_MEIactive1")
	recent := filepath.Join(root, "_MEIrecent1")
	unrelated := filepath.Join(root, "capture-continuous-1")
	outside := filepath.Join(root, "outside")
	for _, path := range []string{stale, active, recent, unrelated, outside} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(stale, "runtime.bin"), []byte("runtime"), 0o600); err != nil {
		t.Fatal(err)
	}
	fingerprint := writeYTDLPFingerprintFixture(t, stale, "trusted")
	writeYTDLPFingerprintFixture(t, active, "trusted")
	writeYTDLPFingerprintFixture(t, recent, "trusted")
	for _, path := range []string{stale, active, unrelated} {
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chtimes(recent, young, young); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "_MEIlink1")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	result, err := cleanupYTDLPTemp(ytdlpTempCleanupConfig{
		TempRoot:       root,
		Now:            now,
		ActiveTempDirs: map[string]struct{}{active: {}},
		Fingerprints:   map[[sha256.Size]byte]struct{}{fingerprint: {}},
		Apply:          false,
		Output:         &output,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.CandidateCount != 1 || result.RemovedCount != 0 {
		t.Fatalf("result=%+v", result)
	}
	if _, err := os.Stat(stale); err != nil {
		t.Fatalf("dry run removed stale directory: %v", err)
	}
	if _, err := os.Stat(filepath.Join(outside)); err != nil {
		t.Fatalf("symlink target changed: %v", err)
	}
	if got := output.String(); !strings.Contains(got, "DRY RUN") || !strings.Contains(got, "1 stale yt-dlp extraction") {
		t.Fatalf("output=%q", got)
	}
}

func TestCleanupYTDLPTempApplyRemovesOnlyPlannedDirectory(t *testing.T) {
	root := t.TempDir()
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	stale := filepath.Join(root, "_MEIstale2")
	keep := filepath.Join(root, "_MEIkeep2")
	media := filepath.Join(root, "capture-continuous-2")
	out := filepath.Join(root, "outside")
	for _, path := range []string{stale, keep, media, out} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	fingerprint := writeYTDLPFingerprintFixture(t, stale, "trusted")
	writeYTDLPFingerprintFixture(t, keep, "trusted")
	link := filepath.Join(root, "_MEIlink2")
	if err := os.Symlink(out, link); err != nil {
		t.Fatal(err)
	}
	old := now.Add(-20 * time.Minute)
	for _, path := range []string{stale, keep, media} {
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatal(err)
		}
	}

	result, err := cleanupYTDLPTemp(ytdlpTempCleanupConfig{
		TempRoot:       root,
		Now:            now,
		ActiveTempDirs: map[string]struct{}{keep: {}},
		RefreshActive:  func() (map[string]struct{}, error) { return map[string]struct{}{keep: {}}, nil },
		Fingerprints:   map[[sha256.Size]byte]struct{}{fingerprint: {}},
		Apply:          true,
		Output:         &bytes.Buffer{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.CandidateCount != 1 || result.RemovedCount != 1 {
		t.Fatalf("result=%+v", result)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale directory remains: %v", err)
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatalf("active directory changed: %v", err)
	}
	for _, path := range []string{media, out, link} {
		if _, err := os.Lstat(path); err != nil {
			t.Fatalf("non-candidate %s changed: %v", path, err)
		}
	}
}

func TestCleanupYTDLPTempApplyFailsClosedWithoutFreshProcessProof(t *testing.T) {
	root := t.TempDir()
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	stale := filepath.Join(root, "_MEIstale3")
	if err := os.Mkdir(stale, 0o700); err != nil {
		t.Fatal(err)
	}
	old := now.Add(-20 * time.Minute)
	fingerprint := writeYTDLPFingerprintFixture(t, stale, "trusted")
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatal(err)
	}

	_, err := cleanupYTDLPTemp(ytdlpTempCleanupConfig{
		TempRoot:       root,
		Now:            now,
		ActiveTempDirs: map[string]struct{}{},
		Fingerprints:   map[[sha256.Size]byte]struct{}{fingerprint: {}},
		Apply:          true,
		Output:         &bytes.Buffer{},
	})
	if err == nil || !strings.Contains(err.Error(), "fresh live yt-dlp lsof proof") {
		t.Fatalf("error=%v", err)
	}
	if _, statErr := os.Stat(stale); statErr != nil {
		t.Fatalf("failed-closed apply changed candidate: %v", statErr)
	}
}

func TestCleanupYTDLPTempApplyRefusesCandidateThatBecomesActive(t *testing.T) {
	root := t.TempDir()
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	candidate := filepath.Join(root, "_MEIracing5")
	if err := os.Mkdir(candidate, 0o700); err != nil {
		t.Fatal(err)
	}
	fingerprint := writeYTDLPFingerprintFixture(t, candidate, "trusted")
	old := now.Add(-20 * time.Minute)
	if err := os.Chtimes(candidate, old, old); err != nil {
		t.Fatal(err)
	}

	_, err := cleanupYTDLPTemp(ytdlpTempCleanupConfig{
		TempRoot:       root,
		Now:            now,
		ActiveTempDirs: map[string]struct{}{},
		RefreshActive:  func() (map[string]struct{}, error) { return map[string]struct{}{candidate: {}}, nil },
		Fingerprints:   map[[sha256.Size]byte]struct{}{fingerprint: {}},
		Apply:          true,
		Output:         &bytes.Buffer{},
	})
	if err == nil || !strings.Contains(err.Error(), "refusing active") {
		t.Fatalf("error=%v", err)
	}
	if _, statErr := os.Stat(candidate); statErr != nil {
		t.Fatalf("newly active candidate changed: %v", statErr)
	}
}

func TestCleanupYTDLPTempRejectsUnrelatedPyInstallerFingerprint(t *testing.T) {
	root := t.TempDir()
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	trusted := filepath.Join(root, "_MEItrusted4")
	unrelated := filepath.Join(root, "_MEIother4")
	for _, path := range []string{trusted, unrelated} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	fingerprint := writeYTDLPFingerprintFixture(t, trusted, "trusted")
	writeYTDLPFingerprintFixture(t, unrelated, "another-pyinstaller-app")
	old := now.Add(-20 * time.Minute)
	for _, path := range []string{trusted, unrelated} {
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatal(err)
		}
	}

	result, err := cleanupYTDLPTemp(ytdlpTempCleanupConfig{
		TempRoot:       root,
		Now:            now,
		ActiveTempDirs: map[string]struct{}{},
		RefreshActive:  func() (map[string]struct{}, error) { return map[string]struct{}{}, nil },
		Fingerprints:   map[[sha256.Size]byte]struct{}{fingerprint: {}},
		Apply:          true,
		Output:         &bytes.Buffer{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.RemovedCount != 1 {
		t.Fatalf("result=%+v", result)
	}
	if _, err := os.Stat(trusted); !os.IsNotExist(err) {
		t.Fatalf("trusted stale extraction remains: %v", err)
	}
	if _, err := os.Stat(unrelated); err != nil {
		t.Fatalf("unrelated PyInstaller extraction changed: %v", err)
	}
}

func writeYTDLPFingerprintFixture(t *testing.T, root, contents string) [sha256.Size]byte {
	t.Helper()
	for _, relative := range ytdlpFingerprintFiles {
		path := filepath.Join(root, filepath.FromSlash(relative))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(contents+relative), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	fingerprint, err := ytdlpExtractionFingerprint(root)
	if err != nil {
		t.Fatal(err)
	}
	return fingerprint
}

func TestParseLsofYTDLPTempDirsKeepsOnlyImmediateMEIRoot(t *testing.T) {
	root := "/private/var/folders/example/T"
	input := strings.Join([]string{
		"p123",
		"n" + root + "/_MEIactive/python3.14/lib-dynload/zlib.so",
		"n" + root + "/capture-continuous-1/segment.mp4",
		"n/private/var/folders/other/T/_MEIoutside/runtime.bin",
		"n" + root + "/_MEIalsoactive",
	}, "\n")

	got := parseLsofYTDLPTempDirs(root, []byte(input))
	for _, path := range []string{root + "/_MEIactive", root + "/_MEIalsoactive"} {
		if _, ok := got[path]; !ok {
			t.Fatalf("missing %s in %#v", path, got)
		}
	}
	if len(got) != 2 {
		t.Fatalf("got=%#v", got)
	}
}

func TestLiveYTDLPTempDirsFindsExactLiveExecutableWorkingDirectory(t *testing.T) {
	if _, err := exec.LookPath("pgrep"); err != nil {
		t.Skip("pgrep unavailable")
	}
	if _, err := exec.LookPath("lsof"); err != nil {
		t.Skip("lsof unavailable")
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	active := filepath.Join(root, "_MEIhelper6")
	if err := os.Mkdir(active, 0o700); err != nil {
		t.Fatal(err)
	}
	ytdlp, err := filepath.Abs(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(ytdlp, "-test.run=TestYTDLPTempHelperProcess")
	cmd.Env = append(os.Environ(), "STOARAMA_YTDLP_TEMP_HELPER=1")
	cmd.Dir = active
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	var got map[string]struct{}
	err = nil
	for attempt := 0; attempt < 20; attempt++ {
		got, err = liveYTDLPTempDirs(ytdlp, root)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := got[active]; ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, ok := got[active]; !ok {
		t.Fatalf("active roots=%#v", got)
	}
}

func TestLivePyInstallerTempDirsPreventsRealApply(t *testing.T) {
	if _, err := exec.LookPath("lsof"); err != nil {
		t.Skip("lsof unavailable")
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	candidate := filepath.Join(root, "_MEIliveglobal7")
	if err := os.Mkdir(candidate, 0o700); err != nil {
		t.Fatal(err)
	}
	fingerprint := writeYTDLPFingerprintFixture(t, candidate, "trusted")
	old := time.Now().Add(-20 * time.Minute)
	if err := os.Chtimes(candidate, old, old); err != nil {
		t.Fatal(err)
	}
	helper, err := filepath.Abs(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(helper, "-test.run=TestYTDLPTempHelperProcess")
	cmd.Env = append(os.Environ(), "STOARAMA_YTDLP_TEMP_HELPER=1")
	cmd.Dir = candidate
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	var active map[string]struct{}
	for attempt := 0; attempt < 20; attempt++ {
		active, err = livePyInstallerTempDirs(root)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := active[candidate]; ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, ok := active[candidate]; !ok {
		t.Fatalf("real global lsof roots=%#v", active)
	}

	result, err := cleanupYTDLPTemp(ytdlpTempCleanupConfig{
		TempRoot:       root,
		Now:            time.Now(),
		ActiveTempDirs: active,
		RefreshActive:  func() (map[string]struct{}, error) { return livePyInstallerTempDirs(root) },
		Fingerprints:   map[[sha256.Size]byte]struct{}{fingerprint: {}},
		Apply:          true,
		Output:         &bytes.Buffer{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.CandidateCount != 0 || result.RemovedCount != 0 {
		t.Fatalf("result=%+v", result)
	}
	if _, err := os.Stat(candidate); err != nil {
		t.Fatalf("live candidate changed: %v", err)
	}
}

func TestYTDLPTempHelperProcess(t *testing.T) {
	if os.Getenv("STOARAMA_YTDLP_TEMP_HELPER") != "1" {
		return
	}
	time.Sleep(30 * time.Second)
	os.Exit(0)
}
