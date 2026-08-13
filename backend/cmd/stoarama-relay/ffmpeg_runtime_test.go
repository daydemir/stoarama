package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestConfigureRelayTLSRuntimeSelectsVerifiedBundleAndChildInheritsIt(t *testing.T) {
	dir := t.TempDir()
	invalid := filepath.Join(dir, "invalid.pem")
	if err := os.WriteFile(invalid, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	valid := filepath.Join(dir, "roots.pem")
	if err := os.WriteFile(valid, testCAPEM(t), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "selected.pem")
	if err := os.Symlink(valid, link); err != nil {
		t.Fatal(err)
	}

	t.Setenv("SSL_CERT_FILE", "/untrusted/inherited.pem")
	t.Setenv("SSL_CERT_DIR", "/untrusted/inherited-dir")
	canonicalValid, err := filepath.EvalSymlinks(valid)
	if err != nil {
		t.Fatal(err)
	}
	if err := configureRelayTLSRuntimeForOS("darwin", []caBundleCandidate{
		{source: "invalid", path: invalid, allowedResolved: []string{invalid}},
		{source: "test_system", path: link, allowedResolved: []string{canonicalValid}},
	}); err != nil {
		t.Fatal(err)
	}
	if got := os.Getenv("SSL_CERT_FILE"); got != canonicalValid {
		t.Fatalf("SSL_CERT_FILE=%q want canonical verified bundle", got)
	}
	if got := os.Getenv("SSL_CERT_DIR"); got != "" {
		t.Fatalf("SSL_CERT_DIR=%q want empty", got)
	}

	envLog := filepath.Join(dir, "child-env")
	ffmpeg := filepath.Join(dir, "ffmpeg")
	script := `#!/bin/sh
if [ "${1:-}" = "-version" ]; then
  printf '%s\n' 'ffmpeg version 8.1.2'
  exit 0
fi
printf '%s\n%s\n' "${SSL_CERT_FILE:-}" "${SSL_CERT_DIR:-}" > "${FF_ENV_LOG}"
printf '%s\n' 'Server returned 404 Not Found' >&2
exit 1
`
	if err := os.WriteFile(ffmpeg, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FF_ENV_LOG", envLog)
	if got := ffmpegNetworkProbe(ffmpeg); got != "host_reached" {
		t.Fatalf("network probe=%q", got)
	}
	contents, err := os.ReadFile(envLog)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(contents), canonicalValid+"\n\n"; got != want {
		t.Fatalf("child environment=%q want %q", got, want)
	}
}

func TestConfigureRelayTLSRuntimeLeavesLinuxEnvironmentUnchanged(t *testing.T) {
	t.Setenv("SSL_CERT_FILE", "/deployment/ca.pem")
	t.Setenv("SSL_CERT_DIR", "/deployment/certs")
	if err := configureRelayTLSRuntimeForOS("linux", nil); err != nil {
		t.Fatal(err)
	}
	if got := os.Getenv("SSL_CERT_FILE"); got != "/deployment/ca.pem" {
		t.Fatalf("SSL_CERT_FILE changed to %q", got)
	}
	if got := os.Getenv("SSL_CERT_DIR"); got != "/deployment/certs" {
		t.Fatalf("SSL_CERT_DIR changed to %q", got)
	}
}

func TestSelectCABundleFailsClosed(t *testing.T) {
	dir := t.TempDir()
	for name, contents := range map[string][]byte{
		"empty.pem":   nil,
		"garbage.pem": []byte("garbage"),
	} {
		if err := os.WriteFile(filepath.Join(dir, name), contents, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := selectCABundle([]caBundleCandidate{
		{source: "empty", path: filepath.Join(dir, "empty.pem"), allowedResolved: []string{filepath.Join(dir, "empty.pem")}},
		{source: "garbage", path: filepath.Join(dir, "garbage.pem"), allowedResolved: []string{filepath.Join(dir, "garbage.pem")}},
		{source: "directory", path: dir, allowedResolved: []string{dir}},
	}); err == nil {
		t.Fatal("invalid CA candidates were accepted")
	}
	valid := filepath.Join(dir, "valid.pem")
	if err := os.WriteFile(valid, testCAPEM(t), 0o600); err != nil {
		t.Fatal(err)
	}
	escaped := filepath.Join(dir, "escaped.pem")
	if err := os.Symlink(valid, escaped); err != nil {
		t.Fatal(err)
	}
	if _, err := selectCABundle([]caBundleCandidate{{source: "escaped", path: escaped, allowedResolved: []string{escaped}}}); err == nil {
		t.Fatal("CA symlink escaping its trusted resolved path was accepted")
	}
}

func TestFFmpegRuntimeAttestationIsRedactedAndFailClosed(t *testing.T) {
	dir := t.TempDir()
	ffmpeg := filepath.Join(dir, "ffmpeg")
	if err := os.WriteFile(ffmpeg, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	evidence := attestFFmpegRuntimeForOS("linux", dir, ffmpeg, "8.1.2", "host_reached", nil)
	if !evidence.Qualified || evidence.Origin != "relay_bundle" || len(evidence.BinarySHA256) != 64 {
		t.Fatalf("unexpected qualified evidence: %+v", evidence)
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), dir) || strings.Contains(string(encoded), ffmpeg) {
		t.Fatalf("runtime evidence leaked a path: %s", encoded)
	}

	for _, probe := range []string{"tls_verify_failed", "dns_failed", "other_failure", "timeout"} {
		got := attestFFmpegRuntimeForOS("linux", dir, ffmpeg, "8.1.2", probe, nil)
		if got.Qualified {
			t.Fatalf("probe %q qualified runtime", probe)
		}
	}
	outside := filepath.Join(t.TempDir(), "ffmpeg")
	if err := os.WriteFile(outside, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if got := attestFFmpegRuntimeForOS("linux", dir, outside, "8.1.2", "host_reached", nil); got.Qualified || got.Origin != "unmanaged" {
		t.Fatalf("unmanaged runtime evidence: %+v", got)
	}
}

func TestClassifyFFmpegOrigin(t *testing.T) {
	tests := []struct {
		name, goos, binDir, resolved, want string
	}{
		{name: "apple-silicon-homebrew", goos: "darwin", binDir: "/Users/test/.stoarama/bin", resolved: "/opt/homebrew/Cellar/ffmpeg/8.1.2/bin/ffmpeg", want: "homebrew"},
		{name: "intel-homebrew", goos: "darwin", binDir: "/Users/test/.stoarama/bin", resolved: "/usr/local/Cellar/ffmpeg/8.1/bin/ffmpeg", want: "homebrew"},
		{name: "darwin-unmanaged-local", goos: "darwin", binDir: "/Users/test/.stoarama/bin", resolved: "/Users/test/.stoarama/bin/ffmpeg", want: "unmanaged"},
		{name: "linux-bundle", goos: "linux", binDir: "/home/test/.stoarama/bin", resolved: "/home/test/.stoarama/bin/ffmpeg", want: "relay_bundle"},
		{name: "system", goos: "linux", binDir: "/home/test/.stoarama/bin", resolved: "/usr/bin/ffmpeg", want: "system"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyFFmpegOrigin(tc.goos, tc.binDir, tc.resolved); got != tc.want {
				t.Fatalf("origin=%q want %q", got, tc.want)
			}
		})
	}
}

func TestRefreshFFmpegTelemetryRecoversAndRevokes(t *testing.T) {
	var calls atomic.Int64
	loaded := make(chan int64, 4)
	release := make(chan struct{}, 2)
	load := func(string) *ffmpegTelemetry {
		call := calls.Add(1)
		loaded <- call
		if call == 2 || call == 3 {
			<-release
		}
		qualified := call == 2
		return &ffmpegTelemetry{runtime: ffmpegRuntimeEvidence{Qualified: qualified}}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var destination atomic.Pointer[ffmpegTelemetry]
	done := make(chan struct{})
	go func() {
		refreshFFmpegTelemetry(ctx, "unused", time.Millisecond, &destination, load)
		close(done)
	}()
	waitForLoad := func(want int64) {
		t.Helper()
		select {
		case got := <-loaded:
			if got != want {
				t.Fatalf("load call=%d want %d", got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("load call %d did not start", want)
		}
	}
	assertQualified := func(want bool) {
		t.Helper()
		current := destination.Load()
		if current == nil || current.runtime.Qualified != want {
			t.Fatalf("qualified=%v want %v", current != nil && current.runtime.Qualified, want)
		}
	}

	// The next load cannot start until the prior result has been stored. Hold
	// calls two and three so each transition is observed before it can be
	// overwritten by the following refresh.
	waitForLoad(1)
	waitForLoad(2)
	assertQualified(false)
	release <- struct{}{}
	waitForLoad(3)
	assertQualified(true)
	release <- struct{}{}
	waitForLoad(4)
	assertQualified(false)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("telemetry refresher did not stop")
	}
}

func testCAPEM(t *testing.T) []byte {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Stoarama test root"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
