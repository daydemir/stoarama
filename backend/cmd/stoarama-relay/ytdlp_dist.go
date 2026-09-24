package main

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
)

// The relay runs yt-dlp from PyInstaller's one-directory build. The one-file
// build unpacks its whole runtime into a fresh _MEI* temp directory on every
// launch (8-11s on macOS) and leaks that ~71MB directory whenever it is killed.
// The one-directory build is unpacked once, here, and launched in place.
//
// Layout under ~/.stoarama/bin/yt-dlp-dist/:
//
//	<sha256>/           one verified, unpacked release (entrypoint + _internal/)
//	current -> <sha256> the release the relay runs
//	previous -> <sha256> the release current replaced, kept for rollback
//
// Each release directory is unpacked beside its final name and renamed into
// place only after it has been verified to start, and the current/previous
// links are replaced with an atomic rename, so an interrupted update never
// leaves a partially unpacked tree behind the running path.
const (
	ytdlpDistDirName      = "yt-dlp-dist"
	ytdlpDistCurrent      = "current"
	ytdlpDistPrevious     = "previous"
	ytdlpDistMarker       = ".stoarama-sha256"
	ytdlpDistMaxFiles     = 5000
	ytdlpDistMaxBytes     = 1 << 30
	ytdlpDistStaleStaging = time.Hour
)

var ytdlpDistVerifyTimeout = 2 * time.Minute

func ytdlpDistRoot(binDir string) string { return filepath.Join(binDir, ytdlpDistDirName) }

// installedYTDLPPath returns the yt-dlp the relay should run: the current
// one-directory entrypoint when one is installed, otherwise the legacy
// single-file bin/yt-dlp.
func installedYTDLPPath(binDir string) string {
	if entry, err := ytdlpDistEntrypoint(filepath.Join(ytdlpDistRoot(binDir), ytdlpDistCurrent)); err == nil {
		return filepath.Join(ytdlpDistRoot(binDir), ytdlpDistCurrent, entry)
	}
	return filepath.Join(binDir, "yt-dlp")
}

// ytdlpLayout reports which yt-dlp build a path belongs to, for heartbeats.
func ytdlpLayout(binDir, ytdlp string) string {
	if strings.HasPrefix(ytdlp, ytdlpDistRoot(binDir)+string(filepath.Separator)) {
		return "onedir"
	}
	return "onefile"
}

// ytdlpDistEntrypoint finds the single top-level yt-dlp* executable of an
// unpacked one-directory release and requires its _internal runtime beside it.
func ytdlpDistEntrypoint(dir string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	entry := ""
	internal := false
	for _, e := range entries {
		switch {
		case e.Name() == "_internal" && e.IsDir():
			internal = true
		case strings.HasPrefix(e.Name(), "yt-dlp") && e.Type().IsRegular():
			info, err := e.Info()
			if err != nil {
				return "", err
			}
			if info.Mode().Perm()&0o100 == 0 {
				continue
			}
			if entry != "" {
				return "", fmt.Errorf("yt-dlp release has more than one entrypoint")
			}
			entry = e.Name()
		}
	}
	if entry == "" || !internal {
		return "", fmt.Errorf("yt-dlp release is missing its entrypoint or _internal runtime")
	}
	return entry, nil
}

// refreshYTDLPDist installs the manifest's one-directory yt-dlp for target and
// makes it current. It reports whether the manifest carries one and whether the
// current release changed.
func refreshYTDLPDist(base string, artifacts map[string]latestArtifact, target string) (bool, bool, error) {
	artifact, ok := artifacts[target]
	if !ok || strings.TrimSpace(artifact.SHA256) == "" {
		return false, false, nil
	}
	bd, err := binDir()
	if err != nil {
		return true, false, err
	}
	updated, err := installYTDLPDist(ytdlpDistRoot(bd), artifact, func() ([]byte, error) {
		return downloadVerified(base, artifact)
	})
	if err != nil {
		return true, false, err
	}
	if updated {
		log.Printf("relay self-update: yt-dlp one-directory build refreshed")
	} else {
		log.Printf("relay self-update: yt-dlp one-directory build up to date")
	}
	return true, updated, nil
}

func installYTDLPDist(root string, artifact latestArtifact, download func() ([]byte, error)) (bool, error) {
	sum := strings.ToLower(strings.TrimSpace(artifact.SHA256))
	if len(sum) != 64 || strings.Trim(sum, "0123456789abcdef") != "" {
		return false, fmt.Errorf("invalid sha256 for yt-dlp one-directory build")
	}
	if target, err := os.Readlink(filepath.Join(root, ytdlpDistCurrent)); err == nil && target == sum && ytdlpDistReleaseReady(filepath.Join(root, sum), sum) {
		return false, nil
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return false, fmt.Errorf("create yt-dlp dist root: %w", err)
	}
	release := filepath.Join(root, sum)
	if !ytdlpDistReleaseReady(release, sum) {
		data, err := download()
		if err != nil {
			return false, err
		}
		if err := unpackYTDLPDist(root, release, sum, data); err != nil {
			return false, err
		}
	}
	if err := activateYTDLPDist(root, sum); err != nil {
		return false, err
	}
	pruneYTDLPDist(root)
	return true, nil
}

func ytdlpDistReleaseReady(release, sum string) bool {
	marker, err := os.ReadFile(filepath.Join(release, ytdlpDistMarker))
	if err != nil || strings.TrimSpace(string(marker)) != sum {
		return false
	}
	_, err = ytdlpDistEntrypoint(release)
	return err == nil
}

func unpackYTDLPDist(root, release, sum string, data []byte) error {
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	staging := filepath.Join(root, ".staging-"+hex.EncodeToString(nonce[:]))
	if err := os.Mkdir(staging, 0o755); err != nil {
		return fmt.Errorf("create yt-dlp staging: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = os.RemoveAll(staging)
		}
	}()
	if err := extractZipInto(staging, data); err != nil {
		return fmt.Errorf("unpack yt-dlp one-directory build: %w", err)
	}
	entry, err := ytdlpDistEntrypoint(staging)
	if err != nil {
		return err
	}
	// Prove the unpacked build starts before any running relay can select it.
	ctx, cancel := context.WithTimeout(context.Background(), ytdlpDistVerifyTimeout)
	defer cancel()
	out, err := capture.RunYTDLPCommandOutput(ctx, filepath.Join(staging, entry), "--version")
	if err != nil || strings.TrimSpace(string(out)) == "" {
		return fmt.Errorf("verify yt-dlp one-directory build: %v", err)
	}
	if err := os.WriteFile(filepath.Join(staging, ytdlpDistMarker), []byte(sum+"\n"), 0o644); err != nil {
		return err
	}
	if err := os.RemoveAll(release); err != nil {
		return fmt.Errorf("replace incomplete yt-dlp release: %w", err)
	}
	if err := os.Rename(staging, release); err != nil {
		return fmt.Errorf("commit yt-dlp release: %w", err)
	}
	committed = true
	return nil
}

// extractZipInto unpacks a PyInstaller zip. It accepts only relative regular
// files and directories that stay inside dir, and bounds file count and size.
func extractZipInto(dir string, data []byte) error {
	reader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return err
	}
	if len(reader.File) > ytdlpDistMaxFiles {
		return fmt.Errorf("archive has %d entries", len(reader.File))
	}
	var total int64
	for _, file := range reader.File {
		name := path.Clean(file.Name)
		if name == "." || path.IsAbs(file.Name) || name == ".." || strings.HasPrefix(name, "../") || strings.Contains(file.Name, "\\") {
			return fmt.Errorf("unsafe archive path %q", file.Name)
		}
		dest := filepath.Join(dir, filepath.FromSlash(name))
		mode := file.Mode()
		switch {
		case mode.IsDir():
			if err := os.MkdirAll(dest, 0o755); err != nil {
				return err
			}
			continue
		case !mode.IsRegular():
			return fmt.Errorf("archive entry %q is not a regular file", file.Name)
		}
		total += int64(file.UncompressedSize64)
		if file.UncompressedSize64 > ytdlpDistMaxBytes || total > ytdlpDistMaxBytes {
			return fmt.Errorf("archive exceeds %d bytes", ytdlpDistMaxBytes)
		}
		if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
			return err
		}
		if err := extractZipFile(file, dest, mode.Perm()|0o600); err != nil {
			return err
		}
	}
	return nil
}

func extractZipFile(file *zip.File, dest string, perm fs.FileMode) error {
	src, err := file.Open()
	if err != nil {
		return err
	}
	defer src.Close()
	out, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE|os.O_EXCL, perm&0o755)
	if err != nil {
		return err
	}
	written, copyErr := io.Copy(out, io.LimitReader(src, int64(file.UncompressedSize64)+1))
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	if written != int64(file.UncompressedSize64) {
		return fmt.Errorf("archive entry %q size mismatch", file.Name)
	}
	return closeErr
}

func activateYTDLPDist(root, sum string) error {
	current := filepath.Join(root, ytdlpDistCurrent)
	if old, err := os.Readlink(current); err == nil && old != sum {
		if err := replaceSymlink(root, ytdlpDistPrevious, old); err != nil {
			return fmt.Errorf("preserve previous yt-dlp release: %w", err)
		}
	}
	if err := replaceSymlink(root, ytdlpDistCurrent, sum); err != nil {
		return fmt.Errorf("activate yt-dlp release: %w", err)
	}
	return nil
}

func replaceSymlink(dir, name, target string) error {
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	tmp := filepath.Join(dir, ".link-"+hex.EncodeToString(nonce[:]))
	if err := os.Symlink(target, tmp); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, name)); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// pruneYTDLPDist removes releases other than current and previous, and staging
// directories abandoned by an interrupted update. It never follows links.
func pruneYTDLPDist(root string) {
	keep := map[string]bool{}
	for _, name := range []string{ytdlpDistCurrent, ytdlpDistPrevious} {
		if target, err := os.Readlink(filepath.Join(root, name)); err == nil {
			keep[target] = true
		}
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == ytdlpDistCurrent || name == ytdlpDistPrevious || keep[name] {
			continue
		}
		stale := strings.HasPrefix(name, ".staging-") || strings.HasPrefix(name, ".link-")
		if stale {
			info, err := entry.Info()
			if err != nil || time.Since(info.ModTime()) < ytdlpDistStaleStaging {
				continue
			}
		} else if len(name) != 64 || !entry.IsDir() {
			continue
		}
		if err := os.RemoveAll(filepath.Join(root, name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			log.Printf("relay self-update: prune yt-dlp release: %v", err)
		}
	}
}

// ensureYTDLPDistForRunningRelease installs the one-directory yt-dlp that this
// relay binary's own immutable release manifest pins, so a relay that just
// self-updated from a pre-dist release switches to it on its first start. It
// reports whether a one-directory build is installed and current. Failure is
// logged and leaves the installed build in place; the heartbeat's ytdlp_layout
// reports which build runs.
func ensureYTDLPDistForRunningRelease(cfg relayConfig) bool {
	if strings.TrimSpace(releasePublicKeyBase64) == "" {
		return false
	}
	manifest, err := immutableReleaseManifest(version)
	if err != nil {
		return false
	}
	lj, err := fetchLatest(cfg.APIURL, manifest)
	if err != nil {
		log.Printf("relay yt-dlp one-directory install skipped: fetch %s: %v", manifest, err)
		return false
	}
	target := runtime.GOOS + "-" + runtime.GOARCH
	present, _, err := refreshYTDLPDist(cfg.APIURL, lj.YtdlpDist, target)
	switch {
	case err != nil:
		log.Printf("relay yt-dlp one-directory install failed; running installed yt-dlp: %v", err)
		return false
	case !present:
		log.Printf("relay release %s has no one-directory yt-dlp for %s; running installed yt-dlp", version, target)
		return false
	}
	return true
}
