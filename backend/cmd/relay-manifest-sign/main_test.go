package main

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestSignerRoundTripRejectsTamperingAndWrongKey(t *testing.T) {
	dir := t.TempDir()
	seed := sha256.Sum256([]byte("release signer test"))
	privateKey := ed25519.NewKeyFromSeed(seed[:])
	keyPath := filepath.Join(dir, "private.key")
	manifestPath := filepath.Join(dir, "latest.json")
	signaturePath := filepath.Join(dir, "latest.json.sig")
	if err := os.WriteFile(keyPath, []byte(base64.StdEncoding.EncodeToString(privateKey)), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, []byte(`{"version":"test"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	signer := buildSigner(t, dir)
	publicKey := strings.TrimSpace(runSigner(t, signer, "public", "--private-key-file", keyPath))
	runSigner(t, signer, "validate-public", "--public-key", publicKey)
	runSigner(t, signer, "validate-public", "--public-key", publicKey+","+publicKey)
	assertSignerFails(t, signer, "validate-public", "--public-key", "invalid")
	assertSignerFails(t, signer, "validate-public", "--public-key", publicKey+",invalid")
	runSigner(t, signer, "sign", "--private-key-file", keyPath, "--input", manifestPath, "--output", signaturePath)
	runSigner(t, signer, "verify", "--public-key", publicKey, "--input", manifestPath, "--signature", signaturePath)
	wrongSeed := sha256.Sum256([]byte("wrong release signer test"))
	wrongPublic := ed25519.NewKeyFromSeed(wrongSeed[:]).Public().(ed25519.PublicKey)
	runSigner(t, signer, "verify", "--public-key", base64.StdEncoding.EncodeToString(wrongPublic)+","+publicKey, "--input", manifestPath, "--signature", signaturePath)
	assertSignerFails(t, signer, "verify", "--public-key", publicKey+",invalid", "--input", manifestPath, "--signature", signaturePath)

	if err := os.WriteFile(manifestPath, []byte(`{"version":"tampered"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	assertSignerFails(t, signer, "verify", "--public-key", publicKey, "--input", manifestPath, "--signature", signaturePath)
	assertSignerFails(t, signer, "verify", "--public-key", base64.StdEncoding.EncodeToString(wrongPublic), "--input", manifestPath, "--signature", signaturePath)
}

func TestRelayReleaseScriptsParse(t *testing.T) {
	for _, script := range []string{"release-relay.sh", "relay-release-immutable.sh", "relay-release-immutable-test.sh", "promote-relay.sh", "relay-install.sh"} {
		cmd := exec.Command("bash", "-n", filepath.Join("..", "..", "scripts", script))
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%s: %v\n%s", script, err, output)
		}
	}
}

func TestRelayPromotionPublishesSignatureBeforeManifest(t *testing.T) {
	script, err := os.ReadFile(filepath.Join("..", "..", "scripts", "promote-relay.sh"))
	if err != nil {
		t.Fatal(err)
	}
	body := string(script)
	signatureUpload := `aws s3 cp "${candidate_signature}" "s3://${R2_BUCKET}/relay-releases/latest.json.sig"`
	manifestUpload := `aws s3 cp "${candidate}" "s3://${R2_BUCKET}/relay-releases/latest.json"`
	signatureAt := strings.Index(body, signatureUpload)
	manifestAt := strings.Index(body, manifestUpload)
	if signatureAt < 0 || manifestAt < 0 || signatureAt > manifestAt {
		t.Fatalf("promotion must publish signature before manifest: signature=%d manifest=%d", signatureAt, manifestAt)
	}
}

func TestRelayPromotionRequiresSignedCandidateAndExplicitUnsignedBootstrap(t *testing.T) {
	script, err := os.ReadFile(filepath.Join("..", "..", "scripts", "promote-relay.sh"))
	if err != nil {
		t.Fatal(err)
	}
	body := string(script)
	candidateSignature := `download "latest-${VERSION}.json.sig" "${candidate_signature}"`
	candidateVerify := `--signature "${candidate_signature}"`
	bootstrapGuard := `"${RELAY_SIGNING_BOOTSTRAP:-}" != "1"`
	immutableCompare := `cmp -s "${live}" "${unsigned_immutable}"`
	for _, required := range []string{candidateSignature, candidateVerify, bootstrapGuard, immutableCompare} {
		if !strings.Contains(body, required) {
			t.Fatalf("promotion script missing %q", required)
		}
	}
	if strings.Index(body, candidateVerify) > strings.Index(body, `download "latest.json" "${live}"`) {
		t.Fatal("candidate signature must be verified before inspecting live bootstrap state")
	}
}

func TestRelayPublisherUsesConditionalImmutableWrites(t *testing.T) {
	script, err := os.ReadFile(filepath.Join("..", "..", "scripts", "relay-release-immutable.sh"))
	if err != nil {
		t.Fatal(err)
	}
	release, err := os.ReadFile(filepath.Join("..", "..", "scripts", "release-relay.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(release), `. "${ROOT_DIR}/scripts/relay-release-immutable.sh"`) {
		t.Fatal("release publisher does not source immutable-write helper")
	}
	body := string(script) + string(release)
	for _, required := range []string{
		`aws s3api put-object`,
		`--if-none-match '*'`,
		`cmp -s "${source}" "${existing}"`,
		`for attempt in 1 2 3`,
		`r2_put "${previous_latest_signature}"`,
		`refusing to overwrite non-identical immutable relay artifact`,
	} {
		if !strings.Contains(body, required) {
			t.Fatalf("release publisher missing immutable-write guard %q", required)
		}
	}
	legacyOverwrite := `aws s3 cp "$1" "s3://${R2_BUCKET}/relay-releases/$2"`
	if strings.Contains(body, legacyOverwrite) {
		t.Fatal("release publisher still contains unconditional immutable artifact overwrite")
	}
}

func TestRelayPublisherImmutableWriteBehavior(t *testing.T) {
	cmd := exec.Command("bash", filepath.Join("..", "..", "scripts", "relay-release-immutable-test.sh"))
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("immutable publisher behavior: %v\n%s", err, output)
	}
}

func buildSigner(t *testing.T, dir string) string {
	t.Helper()
	binary := filepath.Join(dir, "relay-manifest-sign")
	cmd := exec.Command("go", "build", "-o", binary, ".")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build relay-manifest-sign: %v\n%s", err, output)
	}
	return binary
}

func runSigner(t *testing.T, binary string, args ...string) string {
	t.Helper()
	cmd := exec.Command(binary, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("relay-manifest-sign %v: %v\n%s", args, err, output)
	}
	return string(output)
}

func assertSignerFails(t *testing.T, binary string, args ...string) {
	t.Helper()
	cmd := exec.Command(binary, args...)
	if output, err := cmd.CombinedOutput(); err == nil {
		t.Fatalf("relay-manifest-sign %v unexpectedly succeeded\n%s", args, output)
	}
}
