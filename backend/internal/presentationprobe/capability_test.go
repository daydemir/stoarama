package presentationprobe

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func pinnedDarwinEvidence(t *testing.T) TargetCapabilityEvidence {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "capability", "darwin-arm64-25F84.json"))
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := parseCapabilityEvidence(data)
	if err != nil {
		t.Fatal(err)
	}
	return evidence
}

func unavailableEvidence(targetOS, targetArch string) TargetCapabilityEvidence {
	return TargetCapabilityEvidence{
		Schema: CapabilitySchema, ProvenanceClass: ProvenanceCI,
		TargetOS: targetOS, TargetArch: targetArch, ContractSHA256: ContractRevisionSHA256,
		AssessmentKind: "toolchain_unavailable", HostOS: "darwin", HostArch: "arm64",
		ProductVersion: "26.5.2", OSBuild: "25F84", KernelRelease: "25.5.0",
		KernelBuild:       "Darwin-25.5.0-25F84-arm64",
		ObservationSource: SHA256([]byte("C2 toolchain inventory v1")),
		ObservationLog:    SHA256([]byte("no frozen runnable native artifact")),
		StaticProbe:       &StaticProbeToolEvidence{ToolchainAvailable: false, Reason: "frozen native target implementation absent"},
	}
}

func TestPinnedDarwinCapabilityIsExactlyUnrunnable(t *testing.T) {
	evidence := pinnedDarwinEvidence(t)
	seatbelt, err := os.ReadFile(filepath.Join("testdata", "capability", "spikes", "seatbelt_fd_probe.c"))
	if err != nil {
		t.Fatal(err)
	}
	memory, err := os.ReadFile(filepath.Join("testdata", "capability", "spikes", "rlimit_darwin_probe.c"))
	if err != nil {
		t.Fatal(err)
	}
	sourceDigest := SHA256([]byte(SHA256(seatbelt) + "\n" + SHA256(memory) + "\n"))
	log, err := os.ReadFile(filepath.Join("testdata", "capability", "darwin-arm64-25F84.normalized.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if sourceDigest != evidence.ObservationSource || SHA256([]byte(strings.TrimSuffix(string(log), "\n"))) != evidence.ObservationLog {
		t.Fatal("pinned observation source or normalized log digest drifted")
	}
	assessment, err := assessTargetCapability(evidence)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"hard_memory_limit_unavailable", "seatbelt_preopened_vnode_readable"}
	if assessment.Status != CapabilityUnrunnable || !reflect.DeepEqual(assessment.Reasons, want) {
		t.Fatalf("assessment = %#v, want status %q reasons %v", assessment, CapabilityUnrunnable, want)
	}
	if err := requireTargetRunnable(evidence); !errors.Is(err, ErrTargetUnrunnable) {
		t.Fatalf("runnable gate = %v, want target_unrunnable", err)
	}
}

func TestAllC2TargetMatrixEntriesFailClosedWithoutOutput(t *testing.T) {
	targets := [][2]string{{"darwin", "arm64"}, {"darwin", "amd64"}, {"linux", "arm64"}, {"linux", "amd64"}}
	for _, target := range targets {
		t.Run(target[0]+"-"+target[1], func(t *testing.T) {
			evidence := unavailableEvidence(target[0], target[1])
			if target == [2]string{"darwin", "arm64"} {
				evidence = pinnedDarwinEvidence(t)
			}
			root := t.TempDir()
			before, err := os.ReadDir(root)
			if err != nil {
				t.Fatal(err)
			}
			if err := requireTargetRunnable(evidence); !errors.Is(err, ErrTargetUnrunnable) {
				t.Fatalf("gate = %v", err)
			}
			after, err := os.ReadDir(root)
			if err != nil {
				t.Fatal(err)
			}
			if len(before) != 0 || len(after) != 0 {
				t.Fatal("unrunnable assessment emitted artifact, proof, snapshot, or facts")
			}
		})
	}
}

func TestCapabilityEnvelopeSignatureAndTamperRejection(t *testing.T) {
	canonical, err := os.ReadFile(filepath.Join("testdata", "capability", "darwin-arm64-25F84.envelope.json"))
	if err != nil {
		t.Fatal(err)
	}
	signatureHex, err := os.ReadFile(filepath.Join("testdata", "capability", "darwin-arm64-25F84.envelope.sig"))
	if err != nil {
		t.Fatal(err)
	}
	signature, err := DecodeSignatureHex(string(bytes.TrimSpace(signatureHex)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyPinnedCapabilityEnvelope(canonical, signature); err != nil {
		t.Fatal(err)
	}

	mutated := append([]byte(nil), canonical...)
	mutated[len(mutated)-2] ^= 1
	if _, err := VerifyPinnedCapabilityEnvelope(mutated, signature); err == nil {
		t.Fatal("tampered capability envelope accepted")
	}
	badSignature := append([]byte(nil), signature...)
	badSignature[0] ^= 1
	if _, err := VerifyPinnedCapabilityEnvelope(canonical, badSignature); err == nil {
		t.Fatal("tampered capability signature accepted")
	}

	// A caller-generated root cannot become authoritative merely by signing a
	// canonical envelope with its own key.
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	var selfSigned CapabilityEnvelope
	decoder := json.NewDecoder(bytes.NewReader(canonical))
	if err := decoder.Decode(&selfSigned); err != nil {
		t.Fatal(err)
	}
	selfSigned.RootID = "caller-root"
	selfBytes, err := canonicalCapabilityEnvelope(selfSigned)
	if err != nil {
		t.Fatal(err)
	}
	selfSignature := ed25519.Sign(private, append([]byte(CapabilityDomain+"\x00"), selfBytes...))
	if _, err := verifyCapabilityEnvelope(selfBytes, selfSignature, map[string]ed25519.PublicKey{"caller-root": public}); err != nil {
		t.Fatalf("self-signed control is malformed: %v", err)
	}
	if _, err := VerifyPinnedCapabilityEnvelope(selfBytes, selfSignature); err == nil {
		t.Fatal("caller-chosen self-signed root accepted")
	}
}

func TestPinnedDarwinEvidenceCannotBeGeneralized(t *testing.T) {
	for name, mutate := range map[string]func(*TargetCapabilityEvidence){
		"OS build": func(e *TargetCapabilityEvidence) { e.OSBuild = "25F85" },
		"kernel":   func(e *TargetCapabilityEvidence) { e.KernelRelease = "25.6.0" },
		"contract": func(e *TargetCapabilityEvidence) { e.ContractSHA256 = SHA256([]byte("later contract")) },
		"arch":     func(e *TargetCapabilityEvidence) { e.TargetArch = "amd64" },
		"Seatbelt": func(e *TargetCapabilityEvidence) { e.Seatbelt.PreopenedOutsidePReadBytes = 0 },
		"memory":   func(e *TargetCapabilityEvidence) { e.Memory.RlimitASSetErrno = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			evidence := pinnedDarwinEvidence(t)
			mutate(&evidence)
			if _, err := assessTargetCapability(evidence); err == nil {
				t.Fatal("mutated pinned evidence accepted")
			}
		})
	}
}

func TestFutureHandoffCollisionRuleRemainsFrozen(t *testing.T) {
	if FutureHandoffTemporaryFD != 8 {
		t.Fatalf("future collision-safe handoff floor = %d", FutureHandoffTemporaryFD)
	}
}

func TestCapabilityContractDigestMatchesMergedDocument(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "CAPTURE_STITCH_PRESENTATION_V2.md"))
	if err != nil {
		t.Fatal(err)
	}
	if got := SHA256(data); got != ContractRevisionSHA256 {
		t.Fatalf("contract digest = %s, update requires a new reviewed capability assessment", got)
	}
}
