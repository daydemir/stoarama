package main

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const ytdlpTempMinimumAge = 15 * time.Minute

var ytdlpTempBasename = regexp.MustCompile(`^_MEI[[:alnum:]]+$`)

type ytdlpTempCleanupConfig struct {
	TempRoot       string
	Now            time.Time
	ActiveTempDirs map[string]struct{}
	RefreshActive  func() (map[string]struct{}, error)
	Fingerprints   map[[sha256.Size]byte]struct{}
	Apply          bool
	Output         io.Writer
}

type ytdlpTempCleanupResult struct {
	CandidateCount int
	RemovedCount   int
	AllocatedBytes int64
}

type ytdlpTempCandidate struct {
	path           string
	device         uint64
	inode          uint64
	allocatedBytes int64
	fingerprint    [sha256.Size]byte
}

func runCleanupYTDLPTemp(args []string) error {
	fs := flag.NewFlagSet("cleanup-ytdlp-temp", flag.ContinueOnError)
	apply := fs.Bool("apply", false, "remove eligible stale yt-dlp extraction directories")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("cleanup-ytdlp-temp accepts no positional arguments")
	}

	tempRoot, err := filepath.EvalSymlinks(os.TempDir())
	if err != nil {
		return fmt.Errorf("resolve TMPDIR: %w", err)
	}
	bin, err := binDir()
	if err != nil {
		return err
	}
	ytdlp, err := filepath.Abs(filepath.Join(bin, "yt-dlp"))
	if err != nil {
		return fmt.Errorf("resolve installed yt-dlp: %w", err)
	}
	active, err := livePyInstallerTempDirs(tempRoot)
	if err != nil {
		return err
	}
	exactYTDLP, err := liveYTDLPTempDirs(ytdlp, tempRoot)
	if err != nil {
		return err
	}
	fingerprints, err := trustedYTDLPFingerprints(exactYTDLP)
	if err != nil {
		return err
	}
	_, err = cleanupYTDLPTemp(ytdlpTempCleanupConfig{
		TempRoot:       tempRoot,
		Now:            time.Now(),
		ActiveTempDirs: active,
		RefreshActive: func() (map[string]struct{}, error) {
			return livePyInstallerTempDirs(tempRoot)
		},
		Fingerprints: fingerprints,
		Apply:        *apply,
		Output:       os.Stdout,
	})
	return err
}

func cleanupYTDLPTemp(cfg ytdlpTempCleanupConfig) (ytdlpTempCleanupResult, error) {
	if cfg.Output == nil {
		return ytdlpTempCleanupResult{}, errors.New("cleanup output is required")
	}
	root, err := requireOwnedRealDirectory(cfg.TempRoot)
	if err != nil {
		return ytdlpTempCleanupResult{}, fmt.Errorf("unsafe TMPDIR: %w", err)
	}
	if root == string(filepath.Separator) {
		return ytdlpTempCleanupResult{}, errors.New("TMPDIR cannot be the filesystem root")
	}

	entries, err := os.ReadDir(root)
	if err != nil {
		return ytdlpTempCleanupResult{}, fmt.Errorf("read TMPDIR: %w", err)
	}
	if len(cfg.Fingerprints) == 0 {
		return ytdlpTempCleanupResult{}, errors.New("no live exact installed yt-dlp extraction fingerprint is available")
	}
	cutoff := cfg.Now.Add(-ytdlpTempMinimumAge)
	candidates := make([]ytdlpTempCandidate, 0)
	for _, entry := range entries {
		if !ytdlpTempBasename.MatchString(entry.Name()) || entry.Type()&os.ModeSymlink != 0 {
			continue
		}
		path := filepath.Join(root, entry.Name())
		if overlapsActiveYTDLPTemp(path, cfg.ActiveTempDirs) {
			continue
		}
		info, err := os.Lstat(path)
		if err != nil {
			return ytdlpTempCleanupResult{}, fmt.Errorf("inspect %s: %w", entry.Name(), err)
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !info.IsDir() || !ok || stat.Uid != uint32(os.Getuid()) || info.ModTime().After(cutoff) {
			continue
		}
		fingerprint, err := ytdlpExtractionFingerprint(path)
		if err != nil {
			continue
		}
		if _, trusted := cfg.Fingerprints[fingerprint]; !trusted {
			continue
		}
		allocated, err := allocatedDirectoryBytes(path)
		if err != nil {
			return ytdlpTempCleanupResult{}, fmt.Errorf("measure %s: %w", entry.Name(), err)
		}
		candidates = append(candidates, ytdlpTempCandidate{
			path:           path,
			device:         uint64(stat.Dev),
			inode:          stat.Ino,
			allocatedBytes: allocated,
			fingerprint:    fingerprint,
		})
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].path < candidates[j].path })

	result := ytdlpTempCleanupResult{CandidateCount: len(candidates)}
	hash := sha256.New()
	for _, candidate := range candidates {
		result.AllocatedBytes += candidate.allocatedBytes
		fmt.Fprintln(hash, candidate.path)
	}
	digest := fmt.Sprintf("%x", hash.Sum(nil))
	if !cfg.Apply {
		fmt.Fprintf(cfg.Output, "DRY RUN: %d stale yt-dlp extraction directories eligible; %d allocated bytes; path_sha256=%s; rerun with --apply to remove only these disposable runtimes\n", result.CandidateCount, result.AllocatedBytes, digest)
		return result, nil
	}
	if cfg.RefreshActive == nil {
		return result, errors.New("apply requires a fresh live yt-dlp lsof proof")
	}
	freshActive, err := cfg.RefreshActive()
	if err != nil {
		return result, fmt.Errorf("refresh live PyInstaller exclusions: %w", err)
	}
	for _, candidate := range candidates {
		if err := recheckYTDLPTempCandidate(candidate, cutoff, freshActive, cfg.Fingerprints); err != nil {
			return result, err
		}
		if err := os.RemoveAll(candidate.path); err != nil {
			return result, fmt.Errorf("remove %s: %w", filepath.Base(candidate.path), err)
		}
		result.RemovedCount++
	}
	fmt.Fprintf(cfg.Output, "removed %d stale yt-dlp extraction directories; reclaimed up to %d allocated bytes; path_sha256=%s\n", result.RemovedCount, result.AllocatedBytes, digest)
	return result, nil
}

func requireOwnedRealDirectory(path string) (string, error) {
	if !filepath.IsAbs(path) {
		return "", errors.New("path is not absolute")
	}
	clean := filepath.Clean(path)
	info, err := os.Lstat(clean)
	if err != nil {
		return "", err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !ok || stat.Uid != uint32(os.Getuid()) {
		return "", errors.New("path is not a real directory owned by the relay user")
	}
	return clean, nil
}

func recheckYTDLPTempCandidate(candidate ytdlpTempCandidate, cutoff time.Time, active map[string]struct{}, trusted map[[sha256.Size]byte]struct{}) error {
	if overlapsActiveYTDLPTemp(candidate.path, active) {
		return fmt.Errorf("refusing active yt-dlp extraction %s", filepath.Base(candidate.path))
	}
	info, err := os.Lstat(candidate.path)
	if err != nil {
		return fmt.Errorf("recheck %s: %w", filepath.Base(candidate.path), err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !ok || stat.Uid != uint32(os.Getuid()) ||
		uint64(stat.Dev) != candidate.device || stat.Ino != candidate.inode || info.ModTime().After(cutoff) {
		return fmt.Errorf("refusing changed yt-dlp extraction %s", filepath.Base(candidate.path))
	}
	fingerprint, err := ytdlpExtractionFingerprint(candidate.path)
	if err != nil || fingerprint != candidate.fingerprint {
		return fmt.Errorf("refusing changed yt-dlp fingerprint %s", filepath.Base(candidate.path))
	}
	if _, ok := trusted[fingerprint]; !ok {
		return fmt.Errorf("refusing untrusted yt-dlp fingerprint %s", filepath.Base(candidate.path))
	}
	return nil
}

func overlapsActiveYTDLPTemp(candidate string, active map[string]struct{}) bool {
	for path := range active {
		if path == candidate || pathWithin(candidate, path) || pathWithin(path, candidate) {
			return true
		}
	}
	return false
}

func pathWithin(parent, child string) bool {
	relative, err := filepath.Rel(parent, child)
	return err == nil && relative != "." && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func allocatedDirectoryBytes(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if stat, ok := info.Sys().(*syscall.Stat_t); ok {
			total += stat.Blocks * 512
		}
		return nil
	})
	return total, err
}

var ytdlpFingerprintFiles = []string{
	"base_library.zip",
	"yt_dlp_ejs/yt/solver/core.min.js",
	"yt_dlp_ejs/yt/solver/lib.min.js",
}

func trustedYTDLPFingerprints(active map[string]struct{}) (map[[sha256.Size]byte]struct{}, error) {
	fingerprints := map[[sha256.Size]byte]struct{}{}
	for root := range active {
		fingerprint, err := ytdlpExtractionFingerprint(root)
		if err != nil {
			continue
		}
		fingerprints[fingerprint] = struct{}{}
	}
	if len(fingerprints) == 0 {
		return nil, errors.New("no complete live exact installed yt-dlp extraction was available for provenance proof")
	}
	return fingerprints, nil
}

func ytdlpExtractionFingerprint(root string) ([sha256.Size]byte, error) {
	hash := sha256.New()
	for _, relative := range ytdlpFingerprintFiles {
		path := filepath.Join(root, filepath.FromSlash(relative))
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return [sha256.Size]byte{}, fmt.Errorf("missing real yt-dlp marker %s", relative)
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return [sha256.Size]byte{}, fmt.Errorf("read yt-dlp marker %s: %w", relative, err)
		}
		fmt.Fprintf(hash, "%s\x00", relative)
		hash.Write(contents)
	}
	var fingerprint [sha256.Size]byte
	copy(fingerprint[:], hash.Sum(nil))
	return fingerprint, nil
}

func livePyInstallerTempDirs(tempRoot string) (map[string]struct{}, error) {
	lsof, err := exec.LookPath("lsof")
	if err != nil {
		return nil, errors.New("lsof is required to prove every live PyInstaller extraction directory")
	}
	output, err := exec.Command(lsof, "-n", "-Fn").Output()
	if err != nil {
		return nil, fmt.Errorf("enumerate live process files: %w", err)
	}
	return parseLsofYTDLPTempDirs(tempRoot, output), nil
}

func liveYTDLPTempDirs(ytdlp, tempRoot string) (map[string]struct{}, error) {
	pgrep, err := exec.LookPath("pgrep")
	if err != nil {
		return nil, errors.New("pgrep is required to prove live yt-dlp processes")
	}
	lsof, err := exec.LookPath("lsof")
	if err != nil {
		return nil, errors.New("lsof is required to prove live yt-dlp extraction directories")
	}
	pattern := "^" + regexp.QuoteMeta(ytdlp) + `([[:space:]]|$)`
	output, err := exec.Command(pgrep, "-f", pattern).Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
			return map[string]struct{}{}, nil
		}
		return nil, fmt.Errorf("enumerate exact yt-dlp processes: %w", err)
	}
	active := map[string]struct{}{}
	for _, field := range bytes.Fields(output) {
		pid, err := strconv.Atoi(string(field))
		if err != nil || pid <= 0 {
			return nil, fmt.Errorf("invalid yt-dlp process id %q", field)
		}
		openFiles, err := exec.Command(lsof, "-a", "-p", strconv.Itoa(pid), "-Fn").Output()
		if err != nil {
			if errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) {
				continue
			}
			return nil, fmt.Errorf("prove open files for live yt-dlp pid %d: %w", pid, err)
		}
		for path := range parseLsofYTDLPTempDirs(tempRoot, openFiles) {
			active[path] = struct{}{}
		}
	}
	return active, nil
}

func parseLsofYTDLPTempDirs(tempRoot string, output []byte) map[string]struct{} {
	root := filepath.Clean(tempRoot)
	prefix := root + string(filepath.Separator)
	active := map[string]struct{}{}
	for _, line := range strings.Split(string(output), "\n") {
		if !strings.HasPrefix(line, "n"+prefix) {
			continue
		}
		relative := strings.TrimPrefix(line[1:], prefix)
		name := strings.SplitN(relative, string(filepath.Separator), 2)[0]
		if ytdlpTempBasename.MatchString(name) {
			active[filepath.Join(root, name)] = struct{}{}
		}
	}
	return active
}
