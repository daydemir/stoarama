package capture

import (
	"crypto/x509"
	_ "embed"
	"encoding/pem"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"strings"
)

// These public intermediates fill chains omitted by the named origins. They
// are not leaf pins; expiration and hostname checks remain FFmpeg's responsibility.
//
//go:embed certs/globalsign-gcc-r6-alphassl-2025.pem
var seoulIntermediate []byte

//go:embed certs/letsencrypt-yr2-chain.pem
var kameryIntermediates []byte

// configureFFmpegInputTLS changes only this child's trust file. HLS does not
// propagate -ca_file to child playlists/segments, whereas OpenSSL's default
// store reads SSL_CERT_FILE on every connection. GnuTLS builds ignore this
// environment setting and retain their existing system-trust behavior.
func configureFFmpegInputTLS(cmd *exec.Cmd, sourceURL, tempDir string) (func(), error) {
	cleanup := func() {}
	u, err := url.Parse(sourceURL)
	if err != nil || u.Scheme != "https" {
		return cleanup, nil
	}
	var intermediate []byte
	switch strings.ToLower(u.Hostname()) {
	case "topiscctv1.eseoul.go.kr":
		intermediate = seoulIntermediate
	case "online2.kamery24.org":
		intermediate = kameryIntermediates
	default:
		return cleanup, nil
	}
	return configureFFmpegCABundle(cmd, intermediate, tempDir)
}

func configureFFmpegCABundle(cmd *exec.Cmd, intermediate []byte, tempDir string) (func(), error) {
	cleanup := func() {}
	env := cmd.Environ()
	var explicit string
	for _, entry := range env {
		if value, ok := strings.CutPrefix(entry, "SSL_CERT_FILE="); ok {
			explicit = value
		}
	}
	candidates := []string{explicit}
	if explicit == "" {
		candidates = []string{"/etc/ssl/certs/ca-certificates.crt", "/etc/pki/tls/certs/ca-bundle.crt", "/etc/ssl/cert.pem", "/opt/homebrew/etc/ca-certificates/cert.pem", "/usr/local/etc/ca-certificates/cert.pem"}
	}
	var roots []byte
	for _, candidate := range candidates {
		f, err := os.Open(candidate)
		if err != nil {
			continue
		}
		const maxBundleBytes = 16 << 20
		contents, readErr := io.ReadAll(io.LimitReader(f, maxBundleBytes+1))
		_ = f.Close()
		pool := x509.NewCertPool()
		if readErr == nil && len(contents) <= maxBundleBytes && pool.AppendCertsFromPEM(contents) {
			roots = contents
			break
		}
	}
	if len(roots) == 0 {
		return cleanup, fmt.Errorf("ffmpeg TLS: no readable CA bundle for incomplete-chain origin")
	}
	// An intermediate may supplement a trusted chain, but must never introduce
	// an independent trust anchor. Validate its path and validity against the
	// selected roots before giving it to the child.
	if err := verifySupplementalCAs(roots, intermediate); err != nil {
		return cleanup, err
	}
	f, err := os.CreateTemp(tempDir, "capture-ca-*.pem")
	if err != nil {
		return cleanup, fmt.Errorf("ffmpeg TLS bundle: %w", err)
	}
	cleanup = func() { _ = os.Remove(f.Name()) }
	_, writeErr := f.Write(append(append(roots, '\n'), intermediate...))
	closeErr := f.Close()
	if writeErr != nil || closeErr != nil {
		cleanup()
		return func() {}, fmt.Errorf("ffmpeg TLS: write CA bundle")
	}
	childEnv := make([]string, 0, len(env)+1)
	for _, entry := range env {
		if !strings.HasPrefix(entry, "SSL_CERT_FILE=") {
			childEnv = append(childEnv, entry)
		}
	}
	cmd.Env = append(childEnv, "SSL_CERT_FILE="+f.Name())
	return cleanup, nil
}

func verifySupplementalCAs(roots, supplemental []byte) error {
	rootPool := x509.NewCertPool()
	rootPool.AppendCertsFromPEM(roots)
	intermediatePool := x509.NewCertPool()
	var certs []*x509.Certificate
	for len(strings.TrimSpace(string(supplemental))) > 0 {
		block, remaining := pem.Decode(supplemental)
		if block == nil || block.Type != "CERTIFICATE" {
			return fmt.Errorf("ffmpeg TLS: invalid supplemental CA")
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !cert.IsCA {
			return fmt.Errorf("ffmpeg TLS: supplemental certificate is not a CA")
		}
		certs = append(certs, cert)
		intermediatePool.AddCert(cert)
		supplemental = remaining
	}
	if len(certs) == 0 {
		return fmt.Errorf("ffmpeg TLS: no supplemental CA")
	}
	for _, cert := range certs {
		if _, err := cert.Verify(x509.VerifyOptions{Roots: rootPool, Intermediates: intermediatePool, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}}); err != nil {
			return fmt.Errorf("ffmpeg TLS: supplemental CA does not chain to selected roots: %w", err)
		}
	}
	return nil
}
