package main

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/capture"
)

type zipEntry struct {
	name string
	body string
	mode fs.FileMode
}

func buildYTDLPDistZip(t *testing.T, entries []zipEntry) ([]byte, latestArtifact) {
	t.Helper()
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	for _, entry := range entries {
		header := &zip.FileHeader{Name: entry.name, Method: zip.Deflate}
		header.SetMode(entry.mode)
		f, err := w.CreateHeader(header)
		if err != nil {
			t.Fatal(err)
		}
		if entry.body != "" {
			if _, err := f.Write([]byte(entry.body)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(buf.Bytes())
	return buf.Bytes(), latestArtifact{Artifact: "yt-dlp-dist-test.zip", SHA256: hex.EncodeToString(sum[:])}
}

func onedirEntries(version string) []zipEntry {
	return []zipEntry{
		{name: "_internal/", mode: fs.ModeDir | 0o755},
		{name: "_internal/base_library.zip", body: "runtime", mode: 0o644},
		{name: "yt-dlp_macos", body: "#!/bin/sh\necho " + version + "\n", mode: 0o755},
	}
}

func TestInstallYTDLPDistActivatesVerifiedReleaseAndKeepsPrevious(t *testing.T) {
	t.Setenv(capture.YTDLPRuntimeTempRootEnv, t.TempDir())
	bd := t.TempDir()
	root := ytdlpDistRoot(bd)
	if got := installedYTDLPPath(bd); got != filepath.Join(bd, "yt-dlp") {
		t.Fatalf("without a dist install the relay must use bin/yt-dlp, got %s", got)
	}

	downloads := 0
	install := func(data []byte, art latestArtifact) bool {
		t.Helper()
		updated, err := installYTDLPDist(root, art, func() ([]byte, error) { downloads++; return data, nil })
		if err != nil {
			t.Fatal(err)
		}
		return updated
	}
	first, firstArt := buildYTDLPDistZip(t, onedirEntries("2026.08.19"))
	if !install(first, firstArt) {
		t.Fatal("first install did not report an update")
	}
	want := filepath.Join(root, "current", "yt-dlp_macos")
	if got := installedYTDLPPath(bd); got != want {
		t.Fatalf("installed path=%s want %s", got, want)
	}
	if layout := ytdlpLayout(bd, installedYTDLPPath(bd)); layout != "onedir" {
		t.Fatalf("layout=%s", layout)
	}
	if info, err := os.Stat(want); err != nil || info.Mode().Perm()&0o100 == 0 {
		t.Fatalf("entrypoint not executable: %v %v", info, err)
	}
	if install(first, firstArt) || downloads != 1 {
		t.Fatalf("unchanged release was re-downloaded or re-activated (downloads=%d)", downloads)
	}

	second, secondArt := buildYTDLPDistZip(t, onedirEntries("2026.09.01"))
	if !install(second, secondArt) {
		t.Fatal("second release did not activate")
	}
	if target, _ := os.Readlink(filepath.Join(root, "previous")); target != firstArt.SHA256 {
		t.Fatalf("previous=%q want first release", target)
	}
	third, thirdArt := buildYTDLPDistZip(t, onedirEntries("2026.10.01"))
	install(third, thirdArt)
	if _, err := os.Stat(filepath.Join(root, firstArt.SHA256)); !os.IsNotExist(err) {
		t.Fatalf("release older than previous was not pruned: %v", err)
	}
	for _, keep := range []string{secondArt.SHA256, thirdArt.SHA256} {
		if _, err := os.Stat(filepath.Join(root, keep)); err != nil {
			t.Fatalf("current/previous release removed: %v", err)
		}
	}
}

func TestInstallYTDLPDistRejectsUnsafeOrBrokenArchives(t *testing.T) {
	t.Setenv(capture.YTDLPRuntimeTempRootEnv, t.TempDir())
	for _, test := range []struct {
		name    string
		entries []zipEntry
	}{
		{name: "parent escape", entries: append(onedirEntries("x"), zipEntry{name: "../escape", body: "x", mode: 0o644})},
		{name: "symlink", entries: append(onedirEntries("x"), zipEntry{name: "link", body: "/etc/passwd", mode: fs.ModeSymlink | 0o777})},
		{name: "no runtime", entries: []zipEntry{{name: "yt-dlp_macos", body: "#!/bin/sh\necho x\n", mode: 0o755}}},
		{name: "entrypoint fails", entries: []zipEntry{
			{name: "_internal/", mode: fs.ModeDir | 0o755},
			{name: "yt-dlp_macos", body: "#!/bin/sh\nexit 3\n", mode: 0o755},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			bd := t.TempDir()
			root := ytdlpDistRoot(bd)
			data, art := buildYTDLPDistZip(t, test.entries)
			if _, err := installYTDLPDist(root, art, func() ([]byte, error) { return data, nil }); err == nil {
				t.Fatal("unsafe or broken archive was installed")
			}
			if got := installedYTDLPPath(bd); got != filepath.Join(bd, "yt-dlp") {
				t.Fatalf("failed install changed the active yt-dlp to %s", got)
			}
			entries, _ := os.ReadDir(root)
			for _, entry := range entries {
				if strings.HasPrefix(entry.Name(), ".staging-") || entry.Name() == art.SHA256 {
					t.Fatalf("failed install left %s behind", entry.Name())
				}
			}
			if _, err := os.Stat(filepath.Join(filepath.Dir(bd), "escape")); !os.IsNotExist(err) {
				t.Fatal("archive wrote outside the release directory")
			}
		})
	}
}

func TestYTDLPBinaryChangedRequiresRestartOnlyForADifferentBuild(t *testing.T) {
	if ytdlpBinaryChanged("", "/bin/yt-dlp") {
		t.Fatal("unset YT_DLP_BIN (CLI self-update) must not request a restart")
	}
	if ytdlpBinaryChanged("/b/yt-dlp-dist/current/yt-dlp_macos", "/b/yt-dlp-dist/current/yt-dlp_macos") {
		t.Fatal("same build requested a restart")
	}
	if !ytdlpBinaryChanged("/b/yt-dlp", "/b/yt-dlp-dist/current/yt-dlp_macos") {
		t.Fatal("switch from single-file to one-directory build did not request a restart")
	}
}
