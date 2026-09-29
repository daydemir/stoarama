package capture

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFFmpegTLSHostScopeAndCleanup(t *testing.T) {
	rootPEM := publicTestRoots(t)
	rootsPath := filepath.Join(t.TempDir(), "roots.pem")
	if err := os.WriteFile(rootsPath, rootPEM, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SSL_CERT_FILE", rootsPath)
	for _, sourceURL := range []string{
		"https://topiscctv1.eseoul.go.kr/live.m3u8", "https://TOPISCCTV1.ESEOUL.GO.KR:443/live.m3u8",
		"https://online2.kamery24.org/cam/debica.m3u8",
		"https://other.eseoul.go.kr/live.m3u8", "https://topiscctv1.eseoul.go.kr.evil.test/live.m3u8",
		"https://topiscctv1.eseoul.go.kr@evil.test/live.m3u8", "http://topiscctv1.eseoul.go.kr/live.m3u8", "rtsp://example.com/live",
	} {
		t.Run(sourceURL, func(t *testing.T) {
			cmd := exec.Command("unused")
			cleanup, err := configureFFmpegInputTLS(cmd, sourceURL, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer cleanup()
			wantChanged := strings.HasPrefix(sourceURL, "https://topiscctv1.eseoul.go.kr/live") || strings.Contains(sourceURL, "TOPISCCTV1") || strings.Contains(sourceURL, "online2.kamery24.org")
			if !wantChanged {
				if cmd.Env != nil {
					t.Fatal("unrelated source environment changed")
				}
				return
			}
			path := childCAFile(cmd)
			if path == rootsPath || path == "" {
				t.Fatal("no child-specific bundle")
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.HasPrefix(data, rootPEM) {
				t.Fatal("original roots lost")
			}
			if os.Getenv("SSL_CERT_FILE") != rootsPath {
				t.Fatal("process environment changed")
			}
			info, err := os.Stat(path)
			if err != nil || info.Mode().Perm() != 0600 {
				t.Fatal("CA bundle is not private")
			}
			cleanup()
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("bundle retained: %v", err)
			}
		})
	}
}

func publicTestRoots(t *testing.T) []byte {
	t.Helper()
	var roots []byte
	for _, name := range []string{"globalsign-root-r6.pem", "isrg-root-x1.pem"} {
		data, err := os.ReadFile(filepath.Join("testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		roots = append(roots, data...)
	}
	return roots
}

func TestSupplementalCAsRequireTrustedRoots(t *testing.T) {
	roots := publicTestRoots(t)
	for _, supplemental := range [][]byte{seoulIntermediate, kameryIntermediates} {
		if err := verifySupplementalCAs(roots, supplemental); err != nil {
			t.Fatal(err)
		}
	}
	rogue, _, _ := testTLSCA(t, "rogue", nil, nil)
	for _, supplemental := range [][]byte{rogue, []byte("garbage"), nil} {
		if err := verifySupplementalCAs(roots, supplemental); err == nil {
			t.Fatal("untrusted supplemental CA accepted")
		}
	}
}

func TestFFmpegTLSExplicitBundleFailsClosed(t *testing.T) {
	for _, content := range []string{"", "not a PEM"} {
		path := filepath.Join(t.TempDir(), "invalid.pem")
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command("unused")
		cmd.Env = []string{"SSL_CERT_FILE=" + path}
		cleanup, err := configureFFmpegInputTLS(cmd, "https://topiscctv1.eseoul.go.kr/live.m3u8", t.TempDir())
		cleanup()
		if err == nil {
			t.Fatal("invalid explicit CA bundle replaced with system roots")
		}
	}
}

func TestCaptureEntryPointsUseTLSBundle(t *testing.T) {
	for _, continuous := range []bool{false, true} {
		t.Run(fmt.Sprintf("continuous=%t", continuous), func(t *testing.T) {
			dir := t.TempDir()
			roots := publicTestRoots(t)
			rootPath := filepath.Join(dir, "roots.pem")
			if err := os.WriteFile(rootPath, roots, 0600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("SSL_CERT_FILE", rootPath)
			t.Setenv("FF_TLS_CONTENTS", filepath.Join(dir, "child-bundle"))
			t.Setenv("FF_TLS_PATH", filepath.Join(dir, "child-path"))
			ffmpeg := filepath.Join(dir, "ffmpeg")
			script := "#!/bin/sh\ncat \"$SSL_CERT_FILE\" > \"$FF_TLS_CONTENTS\"\nprintf '%s' \"$SSL_CERT_FILE\" > \"$FF_TLS_PATH\"\n"
			if continuous {
				script += "exit 1\n"
			}
			if err := os.WriteFile(ffmpeg, []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("FFMPEG_BIN", ffmpeg)
			const source = "https://topiscctv1.eseoul.go.kr/live.m3u8"
			if continuous {
				err := CaptureContinuousWithHeaders(context.Background(), source, time.Second, "", nil, dir, func(Segment) error { return nil }, "")
				if err == nil {
					t.Fatal("fake FFmpeg failure missing")
				}
			} else if err := ProbeReachable(context.Background(), source, ""); err != nil {
				t.Fatal(err)
			}
			contents, err := os.ReadFile(filepath.Join(dir, "child-bundle"))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.HasPrefix(contents, roots) || !bytes.Contains(contents, seoulIntermediate) {
				t.Fatal("capture did not receive augmented bundle")
			}
			path, err := os.ReadFile(filepath.Join(dir, "child-path"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(string(path)); !os.IsNotExist(err) {
				t.Fatalf("capture retained CA file: %v", err)
			}
		})
	}
}

func TestBundledIntermediateFingerprints(t *testing.T) {
	for name, fixture := range map[string]struct {
		pem    []byte
		hashes []string
	}{
		"seoul":  {seoulIntermediate, []string{"a883559231f8388daf35ce41c8101040ae8fd9b656434247b9475af592cc08ca"}},
		"kamery": {kameryIntermediates, []string{"238b85a0099c65b970477d5724f1a1d475ce5058cffe4efa8733899bdb863c47", "072639d0b140d5bffae16ad9c3f6cc6086040621f51ee61a6d46a8915c07cf76"}},
	} {
		t.Run(name, func(t *testing.T) {
			remaining := fixture.pem
			for _, hash := range fixture.hashes {
				block, rest := pem.Decode(remaining)
				if block == nil {
					t.Fatal("missing certificate")
				}
				remaining = rest
				cert, err := x509.ParseCertificate(block.Bytes)
				if err != nil || !cert.IsCA {
					t.Fatal("fixture is not a CA certificate")
				}
				sum := sha256.Sum256(cert.Raw)
				if got := hex.EncodeToString(sum[:]); got != hash {
					t.Fatalf("fingerprint=%s want %s", got, hash)
				}
			}
			if len(bytes.TrimSpace(remaining)) != 0 {
				t.Fatal("unexpected extra certificate")
			}
		})
	}
}

// A real FFmpeg handshake proves that -ca_file fixes only the master while the
// per-child environment reaches variant playlists and media. All network I/O
// in this test stays on loopback, with a generated root and a leaf-only server.
func TestFFmpegTLSIncompleteHLSChain(t *testing.T) {
	ffmpeg, err := exec.LookPath(ffmpegBin())
	if err != nil {
		t.Skip("ffmpeg unavailable")
	}
	help, err := exec.Command(ffmpeg, "-hide_banner", "-h", "protocol=tls").CombinedOutput()
	version, _ := exec.Command(ffmpeg, "-version").CombinedOutput()
	if err != nil || !bytes.Contains(version, []byte("--enable-openssl")) || !bytes.Contains(help, []byte("(default true)")) {
		t.Skip("requires OpenSSL FFmpeg with verification enabled by default")
	}
	rootPEM, root, rootKey := testTLSCA(t, "root", nil, nil)
	intermediatePEM, intermediate, intermediateKey := testTLSCA(t, "intermediate", root, rootKey)
	rootPath := filepath.Join(t.TempDir(), "roots.pem")
	if err := os.WriteFile(rootPath, rootPEM, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SSL_CERT_FILE", rootPath)
	t.Setenv("SSL_CERT_DIR", t.TempDir())
	segment := generateHLSFixtureSegment(t, ffmpeg, t.TempDir())
	for _, kind := range []string{"valid", "wrong_hostname", "expired"} {
		t.Run(kind, func(t *testing.T) {
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/master.m3u8":
					fmt.Fprint(w, "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=100000\nvariant.m3u8\n")
				case "/variant.m3u8":
					fmt.Fprint(w, "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2.0,\nsegment.ts\n#EXT-X-ENDLIST\n")
				case "/segment.ts":
					_, _ = w.Write(segment)
				default:
					http.NotFound(w, r)
				}
			}))
			leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			leaf := &x509.Certificate{SerialNumber: big.NewInt(3), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, KeyUsage: x509.KeyUsageDigitalSignature}
			if kind == "wrong_hostname" {
				leaf.IPAddresses = nil
				leaf.DNSNames = []string{"wrong.invalid"}
			}
			if kind == "expired" {
				leaf.NotAfter = time.Now().Add(-time.Minute)
			}
			der, err := x509.CreateCertificate(rand.Reader, leaf, intermediate, &leafKey.PublicKey, intermediateKey)
			if err != nil {
				t.Fatal(err)
			}
			server.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: leafKey}}}
			server.StartTLS()
			defer server.Close()
			command := func() (*exec.Cmd, func()) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				return exec.CommandContext(ctx, ffmpeg, "-nostdin", "-v", "error", "-i", server.URL+"/master.m3u8", "-frames:v", "1", "-f", "null", "-"), cancel
			}
			if kind == "valid" {
				cmd, cancel := command()
				cleanup, err := configureFFmpegCABundle(cmd, intermediatePEM, t.TempDir())
				if err != nil {
					cancel()
					t.Fatal(err)
				}
				// Deliberately pass this file as an option only, restoring the
				// root-only environment for subsequent HLS requests.
				cmd.Args = append(cmd.Args[:1], append([]string{"-ca_file", childCAFile(cmd)}, cmd.Args[1:]...)...)
				cmd.Env = nil
				out, runErr := cmd.CombinedOutput()
				cancel()
				cleanup()
				if runErr == nil || !bytes.Contains(out, []byte("certificate verify failed")) {
					t.Fatalf("option-only unexpectedly worked: %v %s", runErr, out)
				}
			}
			cmd, cancel := command()
			defer cancel()
			cleanup, err := configureFFmpegCABundle(cmd, intermediatePEM, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer cleanup()
			out, runErr := cmd.CombinedOutput()
			if kind == "valid" && runErr != nil {
				t.Fatalf("incomplete chain not repaired: %v %s", runErr, out)
			}
			if kind != "valid" && (runErr == nil || !bytes.Contains(out, []byte("certificate verify failed"))) {
				t.Fatalf("invalid leaf accepted: %v %s", runErr, out)
			}
		})
	}
}

func childCAFile(cmd *exec.Cmd) string {
	for _, entry := range cmd.Env {
		if value, ok := strings.CutPrefix(entry, "SSL_CERT_FILE="); ok {
			return value
		}
	}
	return ""
}

func testTLSCA(t *testing.T, name string, parent *x509.Certificate, parentKey *ecdsa.PrivateKey) ([]byte, *x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	if parent == nil {
		parent = cert
		parentKey = key
	} else {
		cert.SerialNumber = big.NewInt(2)
	}
	der, err := x509.CreateCertificate(rand.Reader, cert, parent, &key.PublicKey, parentKey)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), parsed, key
}
