package main

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"golang.org/x/sys/unix"
)

const (
	maxCABundleBytes    = 16 << 20
	maxFFmpegBinarySize = 512 << 20
)

type caBundleCandidate struct {
	source          string
	path            string
	allowedResolved []string
}

type caBundleEvidence struct {
	source string
	path   string
	sha256 string
}

type ffmpegRuntimeEvidence struct {
	Qualified      bool   `json:"qualified"`
	Origin         string `json:"origin"`
	Version        string `json:"version,omitempty"`
	BinarySHA256   string `json:"binary_sha256,omitempty"`
	CABundleSource string `json:"ca_bundle_source,omitempty"`
	CABundleSHA256 string `json:"ca_bundle_sha256,omitempty"`
	NetworkProbe   string `json:"network_probe"`
}

var darwinCABundleCandidates = []caBundleCandidate{
	// Prefer the OS-maintained root set. Homebrew OpenSSL honors SSL_CERT_FILE,
	// and this path is stable across formula upgrades.
	{source: "macos_system", path: "/etc/ssl/cert.pem", allowedResolved: []string{"/etc/ssl/cert.pem", "/private/etc/ssl/cert.pem"}},
	{source: "homebrew_ca_certificates", path: "/opt/homebrew/etc/ca-certificates/cert.pem", allowedResolved: []string{"/opt/homebrew/etc/ca-certificates/cert.pem"}},
	{source: "homebrew_ca_certificates", path: "/usr/local/etc/ca-certificates/cert.pem", allowedResolved: []string{"/usr/local/etc/ca-certificates/cert.pem"}},
	// These normally resolve to the ca-certificates paths above, but retain a
	// bounded fallback for an otherwise valid Homebrew OpenSSL installation.
	{source: "homebrew_openssl", path: "/opt/homebrew/etc/openssl@3/cert.pem", allowedResolved: []string{"/opt/homebrew/etc/openssl@3/cert.pem", "/opt/homebrew/etc/ca-certificates/cert.pem"}},
	{source: "homebrew_openssl", path: "/usr/local/etc/openssl@3/cert.pem", allowedResolved: []string{"/usr/local/etc/openssl@3/cert.pem", "/usr/local/etc/ca-certificates/cert.pem"}},
}

// configureRelayTLSRuntime makes the CA source deterministic for every Darwin
// ffmpeg/ffprobe child. The candidates are compiled-in trusted locations; an
// inherited arbitrary path is never accepted. Linux retains its deployment-
// supplied environment unchanged.
func configureRelayTLSRuntime() error {
	return configureRelayTLSRuntimeForOS(runtime.GOOS, darwinCABundleCandidates)
}

func configureRelayTLSRuntimeForOS(goos string, candidates []caBundleCandidate) error {
	if goos != "darwin" {
		return nil
	}
	evidence, err := selectCABundle(candidates)
	if err != nil {
		return fmt.Errorf("no verified Darwin CA bundle available")
	}
	if err := os.Setenv("SSL_CERT_FILE", evidence.path); err != nil {
		return fmt.Errorf("set Darwin CA bundle: %w", err)
	}
	if err := os.Unsetenv("SSL_CERT_DIR"); err != nil {
		return fmt.Errorf("clear Darwin CA directory override: %w", err)
	}
	return nil
}

func selectCABundle(candidates []caBundleCandidate) (caBundleEvidence, error) {
	for _, candidate := range candidates {
		evidence, err := inspectCABundle(candidate)
		if err == nil {
			return evidence, nil
		}
	}
	return caBundleEvidence{}, fmt.Errorf("no candidate passed validation")
}

func inspectCABundle(candidate caBundleCandidate) (caBundleEvidence, error) {
	if candidate.source == "" || candidate.path == "" || !filepath.IsAbs(candidate.path) {
		return caBundleEvidence{}, fmt.Errorf("invalid candidate")
	}
	resolved, err := filepath.EvalSymlinks(candidate.path)
	if err != nil {
		return caBundleEvidence{}, err
	}
	allowed := false
	for _, expected := range candidate.allowedResolved {
		if filepath.Clean(resolved) == filepath.Clean(expected) {
			allowed = true
			break
		}
	}
	if !allowed {
		return caBundleEvidence{}, fmt.Errorf("CA bundle resolved outside its trusted location")
	}
	f, err := openReadOnlyNoFollow(resolved)
	if err != nil {
		return caBundleEvidence{}, err
	}
	defer f.Close()
	before, err := f.Stat()
	if err != nil {
		return caBundleEvidence{}, err
	}
	if !before.Mode().IsRegular() || before.Size() <= 0 || before.Size() > maxCABundleBytes {
		return caBundleEvidence{}, fmt.Errorf("CA bundle is not a bounded regular file")
	}
	contents, err := io.ReadAll(io.LimitReader(f, maxCABundleBytes+1))
	if err != nil || len(contents) == 0 || len(contents) > maxCABundleBytes {
		return caBundleEvidence{}, fmt.Errorf("read bounded CA bundle")
	}
	after, err := f.Stat()
	if err != nil || !os.SameFile(before, after) || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return caBundleEvidence{}, fmt.Errorf("CA bundle changed while reading")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(contents) {
		return caBundleEvidence{}, fmt.Errorf("CA bundle contains no certificates")
	}
	digest := sha256.Sum256(contents)
	return caBundleEvidence{source: candidate.source, path: resolved, sha256: hex.EncodeToString(digest[:])}, nil
}

func attestFFmpegRuntime(binDir, active, version, networkProbe string) ffmpegRuntimeEvidence {
	return attestFFmpegRuntimeForOS(runtime.GOOS, binDir, active, version, networkProbe, darwinCABundleCandidates)
}

func attestFFmpegRuntimeForOS(goos, binDir, active, version, networkProbe string, caCandidates []caBundleCandidate) ffmpegRuntimeEvidence {
	evidence := ffmpegRuntimeEvidence{Origin: "unmanaged", Version: version, NetworkProbe: networkProbe}
	resolved, digest, err := inspectFFmpegBinary(active)
	if err != nil {
		return evidence
	}
	evidence.BinarySHA256 = digest
	resolvedBinDir := binDir
	if candidate, resolveErr := filepath.EvalSymlinks(binDir); resolveErr == nil {
		resolvedBinDir = candidate
	}
	evidence.Origin = classifyFFmpegOrigin(goos, resolvedBinDir, resolved)

	structurallyQualified := version != "" && digest != ""
	switch goos {
	case "darwin":
		structurallyQualified = structurallyQualified && (evidence.Origin == "homebrew" || evidence.Origin == "system")
		ca, caErr := selectCABundle(caCandidates)
		if caErr == nil && os.Getenv("SSL_CERT_FILE") == ca.path && os.Getenv("SSL_CERT_DIR") == "" {
			evidence.CABundleSource = ca.source
			evidence.CABundleSHA256 = ca.sha256
		} else {
			structurallyQualified = false
		}
	default:
		structurallyQualified = structurallyQualified && (evidence.Origin == "relay_bundle" || evidence.Origin == "system")
	}
	evidence.Qualified = structurallyQualified && networkProbe == "host_reached"
	return evidence
}

func inspectFFmpegBinary(path string) (resolved, digest string, err error) {
	resolved, err = filepath.EvalSymlinks(path)
	if err != nil {
		return "", "", err
	}
	f, err := openReadOnlyNoFollow(resolved)
	if err != nil {
		return "", "", err
	}
	defer f.Close()
	before, err := f.Stat()
	if err != nil {
		return "", "", err
	}
	if !before.Mode().IsRegular() || before.Mode()&0111 == 0 || before.Size() <= 0 || before.Size() > maxFFmpegBinarySize {
		return "", "", fmt.Errorf("ffmpeg is not a bounded executable regular file")
	}
	h := sha256.New()
	written, err := io.Copy(h, io.LimitReader(f, maxFFmpegBinarySize+1))
	if err != nil || written != before.Size() {
		return "", "", fmt.Errorf("hash ffmpeg binary")
	}
	after, err := f.Stat()
	if err != nil || !os.SameFile(before, after) || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return "", "", fmt.Errorf("ffmpeg binary changed while hashing")
	}
	return resolved, hex.EncodeToString(h.Sum(nil)), nil
}

func classifyFFmpegOrigin(goos, binDir, resolved string) string {
	cleanResolved := filepath.Clean(resolved)
	if cleanResolved == "/usr/bin/ffmpeg" {
		return "system"
	}
	if goos == "darwin" && (hasPathPrefix(cleanResolved, "/opt/homebrew/Cellar/ffmpeg") ||
		hasPathPrefix(cleanResolved, "/usr/local/Cellar/ffmpeg")) {
		return "homebrew"
	}
	cleanBinDir := filepath.Clean(binDir)
	if goos != "darwin" && hasPathPrefix(cleanResolved, cleanBinDir) {
		return "relay_bundle"
	}
	return "unmanaged"
}

func hasPathPrefix(path, root string) bool {
	rel, err := filepath.Rel(filepath.Clean(root), filepath.Clean(path))
	return err == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}

func openReadOnlyNoFollow(path string) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), filepath.Base(path))
	if f == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("open verified file")
	}
	return f, nil
}
