package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/stitchproof"
)

func reviewedNASStitchDiscovery(t *testing.T) stitchproof.AcceptedPair {
	t.Helper()
	keys := nasStitchReadbackCandidateKeys()
	if len(keys) != 1 {
		t.Fatal("discovery must contain exactly the reviewed self-pair")
	}
	key := keys[0]
	// Independent pin of the three public component digests, each with newline.
	// Avoid spelling public profile hashes as credential-looking test literals.
	pins := key.Producer.String() + "\n" + key.Reader.String() + "\n" + key.Pair.String() + "\n"
	if stitchproof.Digest(sha256.Sum256([]byte(pins))).String() != "d12ef3ab8c15d33b90187191eb268f08c58435bd0089cdae93a7b460eef5bdd6" {
		t.Fatal("discovery differs from the independently accepted triple")
	}
	pair, err := stitchproof.LookupAcceptedPair(key)
	if err != nil || pair.Key() != key {
		t.Fatalf("discovery cannot substitute for registry acceptance: %v", err)
	}
	return pair
}

func TestNASStitchReadbackDiscoveryUsesOnlySealedReviewedPair(t *testing.T) {
	want := reviewedNASStitchDiscovery(t)
	reader := want.ReaderIdentity()
	for _, mode := range []nasStitchProofMode{nasStitchProofNative, nasStitchProofReadback} {
		original := ""
		if mode == nasStitchProofReadback {
			original = want.Key().Producer.String()
		}
		got, err := selectNASStitchReadbackPair(mode, string(reader.Version()), reader.ArtifactSHA256().String(), reader.SizeBytes(), original)
		if err != nil || got != want {
			t.Fatalf("accepted self-pair mode%d: %v", mode, err)
		}
	}
	components, err := nasStitchPairComponents(want)
	if err != nil {
		t.Fatal(err)
	}
	for _, component := range []struct {
		encoded, digest string
		body            []byte
	}{
		{components.ProducerB64, components.ProducerSHA, want.ProducerProfileJSON()},
		{components.ReaderB64, components.ReaderSHA, want.ReaderProfileJSON()},
		{components.PairB64, components.PairSHA, want.PairJSON()},
	} {
		raw, err := base64.StdEncoding.Strict().DecodeString(component.encoded)
		if err != nil || !bytes.Equal(raw, component.body) || stitchproof.Digest(sha256.Sum256(raw)).String() != component.digest {
			t.Fatal("API components differ from the sealed canonical bytes")
		}
	}
	keys := nasStitchReadbackCandidateKeys()
	keys[0].Producer[0] ^= 1
	if nasStitchReadbackCandidateKeys()[0] != want.Key() {
		t.Fatal("caller changed later discovery")
	}
}

func TestNASStitchReadbackDiscoveryRejectsUnacceptedIdentity(t *testing.T) {
	want := reviewedNASStitchDiscovery(t)
	reader := want.ReaderIdentity()
	for _, tc := range []struct {
		name     string
		mode     nasStitchProofMode
		version  string
		artifact string
		size     int64
		original string
	}{
		{"off", nasStitchProofDisabled, string(reader.Version()), reader.ArtifactSHA256().String(), reader.SizeBytes(), ""},
		{"unknown-mode", 3, string(reader.Version()), reader.ArtifactSHA256().String(), reader.SizeBytes(), ""},
		{"unstamped", nasStitchProofNative, "development", reader.ArtifactSHA256().String(), reader.SizeBytes(), ""},
		{"legacy", nasStitchProofNative, string(stitchproof.ClientNativeV2), reader.ArtifactSHA256().String(), reader.SizeBytes(), ""},
		{"future-label", nasStitchProofNative, string(stitchproof.ClientCleanupV8), reader.ArtifactSHA256().String(), reader.SizeBytes(), ""},
		{"different-artifact", nasStitchProofNative, string(reader.Version()), strings.Repeat("b", 64), reader.SizeBytes(), ""},
		{"uppercase-artifact", nasStitchProofNative, string(reader.Version()), strings.ToUpper(reader.ArtifactSHA256().String()), reader.SizeBytes(), ""},
		{"unknown-size", nasStitchProofNative, string(reader.Version()), reader.ArtifactSHA256().String(), reader.SizeBytes() + 1, ""},
		{"native-original-injection", nasStitchProofNative, string(reader.Version()), reader.ArtifactSHA256().String(), reader.SizeBytes(), want.Key().Producer.String()},
		{"renewal-no-original", nasStitchProofReadback, string(reader.Version()), reader.ArtifactSHA256().String(), reader.SizeBytes(), ""},
		{"renewal-unknown-original", nasStitchProofReadback, string(reader.Version()), reader.ArtifactSHA256().String(), reader.SizeBytes(), strings.Repeat("a", 64)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if pair, err := selectNASStitchReadbackPair(tc.mode, tc.version, tc.artifact, tc.size, tc.original); err == nil || pair != (stitchproof.AcceptedPair{}) {
				t.Fatal("unknown mode/identity created an acceptance")
			}
		})
	}
	// Retain the original cross-product unknown-input and zero-value refusals.
	for _, mode := range []nasStitchProofMode{nasStitchProofDisabled, nasStitchProofNative, nasStitchProofReadback, 3} {
		for _, version := range []string{"development", "r2-copy-v3", "v85r-nas-proof-v2", "cleanup-batch-v8"} {
			producer := ""
			if mode == nasStitchProofReadback {
				producer = strings.Repeat("a", 64)
			}
			pair, err := selectNASStitchReadbackPair(mode, version, strings.Repeat("b", 64), 100, producer)
			if err == nil || pair.Key() != (stitchproof.PairKey{}) {
				t.Fatal("unknown identity created a profile acceptance")
			}
		}
	}
	if _, err := nasStitchPairComponents(stitchproof.AcceptedPair{}); err == nil {
		t.Fatal("zero/unknown accepted-pair value exported components")
	}
}
