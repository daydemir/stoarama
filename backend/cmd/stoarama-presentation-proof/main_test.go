package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/presentationprobe"
)

func capabilityFixture(t *testing.T, name string) string {
	t.Helper()
	return filepath.Join("..", "..", "internal", "presentationprobe", "testdata", "capability", name)
}

func names(t *testing.T, root string) []string {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, len(entries))
	for i, entry := range entries {
		out[i] = entry.Name()
	}
	return out
}

func TestOnlyCLICommandVerifiesPinnedUnrunnableCapabilityWithoutWrites(t *testing.T) {
	envelope, err := filepath.Abs(capabilityFixture(t, "darwin-arm64-25F84.envelope.json"))
	if err != nil {
		t.Fatal(err)
	}
	signature, err := filepath.Abs(capabilityFixture(t, "darwin-arm64-25F84.envelope.sig"))
	if err != nil {
		t.Fatal(err)
	}
	working := t.TempDir()
	oldWorking, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(working); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(oldWorking) })
	before := names(t, working)
	var stdout bytes.Buffer
	err = run([]string{"verify-capability", "--envelope", envelope, "--signature", signature}, &stdout)
	if err != nil {
		t.Fatal(err)
	}
	want := "capability_signature_verified class=development target=darwin-arm64 status=target_unrunnable production_eligible=false\n"
	if stdout.String() != want {
		t.Fatalf("output = %q", stdout.String())
	}
	if after := names(t, working); !reflect.DeepEqual(before, after) {
		t.Fatalf("CLI wrote files: before=%v after=%v", before, after)
	}
	for _, args := range [][]string{{"verify-artifact"}, {"verify-corpus"}, {"audit-executable"}, {"verify-capability", "--root-id", "caller"}, {"verify-capability", "--public-key-hex", strings.Repeat("0", 64)}} {
		if err := run(args, &bytes.Buffer{}); err == nil {
			t.Fatalf("forbidden CLI surface accepted: %v", args)
		}
	}
}

func TestRealCLIRejectsCanonicalSelfSignedRootAndClassSubstitution(t *testing.T) {
	canonical, err := os.ReadFile(capabilityFixture(t, "darwin-arm64-25F84.envelope.json"))
	if err != nil {
		t.Fatal(err)
	}
	var envelope presentationprobe.CapabilityEnvelope
	if err := json.Unmarshal(canonical, &envelope); err != nil {
		t.Fatal(err)
	}
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	envelope.RootID = "caller-root"
	envelope.ProvenanceClass = presentationprobe.ProvenanceCI
	envelope.Evidence.ProvenanceClass = presentationprobe.ProvenanceCI
	evidenceBytes, err := json.Marshal(envelope.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	envelope.EvidenceSHA256 = presentationprobe.SHA256(evidenceBytes)
	raw, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	var canonicalRoundTrip presentationprobe.CapabilityEnvelope
	if err := json.Unmarshal(raw, &canonicalRoundTrip); err != nil {
		t.Fatal(err)
	}
	roundTrip, err := json.Marshal(canonicalRoundTrip)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, roundTrip) {
		t.Fatal("typed capability envelope did not preserve canonical bytes")
	}
	sig := ed25519.Sign(private, append([]byte(presentationprobe.CapabilityDomain+"\x00"), raw...))
	root := t.TempDir()
	envelopePath := filepath.Join(root, "input.json")
	signaturePath := filepath.Join(root, "input.sig")
	if err := os.WriteFile(envelopePath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(signaturePath, []byte(hex.EncodeToString(sig)), 0o600); err != nil {
		t.Fatal(err)
	}
	before := names(t, root)
	var output bytes.Buffer
	if err := run([]string{"verify-capability", "--envelope", envelopePath, "--signature", signaturePath}, &output); err == nil {
		t.Fatal("canonical self-signed/root/class substitution accepted")
	}
	if output.Len() != 0 {
		t.Fatalf("failed verification emitted success output: %q", output.String())
	}
	if after := names(t, root); !reflect.DeepEqual(before, after) {
		t.Fatalf("failed CLI wrote files: before=%v after=%v", before, after)
	}
}

func TestRealCLIRejectsPinnedEnvelopeRootAndClassSubstitution(t *testing.T) {
	original, err := os.ReadFile(capabilityFixture(t, "darwin-arm64-25F84.envelope.json"))
	if err != nil {
		t.Fatal(err)
	}
	originalSignature, err := os.ReadFile(capabilityFixture(t, "darwin-arm64-25F84.envelope.sig"))
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func([]byte) []byte{
		"unknown root": func(raw []byte) []byte {
			return bytes.Replace(raw, []byte(`"root_id":"stoarama-c2-dev-capability-2026-08"`), []byte(`"root_id":"unknown-root"`), 1)
		},
		"wrong envelope class": func(raw []byte) []byte {
			return bytes.Replace(raw, []byte(`"provenance_class":"development"`), []byte(`"provenance_class":"ci_test"`), 1)
		},
		"wrong evidence class": func(raw []byte) []byte {
			first := bytes.Index(raw, []byte(`"provenance_class":"development"`))
			if first < 0 {
				return raw
			}
			offset := first + len(`"provenance_class":"development"`)
			tail := bytes.Replace(raw[offset:], []byte(`"provenance_class":"development"`), []byte(`"provenance_class":"ci_test"`), 1)
			return append(append([]byte(nil), raw[:offset]...), tail...)
		},
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			envelopePath := filepath.Join(root, "input.json")
			signaturePath := filepath.Join(root, "input.sig")
			if err := os.WriteFile(envelopePath, mutate(append([]byte(nil), original...)), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(signaturePath, originalSignature, 0o600); err != nil {
				t.Fatal(err)
			}
			var output bytes.Buffer
			if err := run([]string{"verify-capability", "--envelope", envelopePath, "--signature", signaturePath}, &output); err == nil || output.Len() != 0 {
				t.Fatalf("substitution accepted or emitted output: err=%v output=%q", err, output.String())
			}
		})
	}
}
