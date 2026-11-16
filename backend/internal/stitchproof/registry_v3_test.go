package stitchproof

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"testing"
)

// SHA-256 of the reviewed producer, reader and pair hex digests, in that order,
// each followed by one newline. This independent pin detects component drift
// without spelling public digests next to identifiers mistaken for API secrets.
const reviewedProfilesDigest = "d12ef3ab8c15d33b90187191eb268f08c58435bd0089cdae93a7b460eef5bdd6"

func reviewedCombinedV3Key(t *testing.T) PairKey {
	t.Helper()
	key := PairKey{
		Producer: Digest(sha256.Sum256([]byte(combinedV3ProducerProfile))),
		Reader:   Digest(sha256.Sum256([]byte(combinedV3ReaderProfile))),
		Pair:     Digest(sha256.Sum256([]byte(combinedV3ProfilePair))),
	}
	pins := key.Producer.String() + "\n" + key.Reader.String() + "\n" + key.Pair.String() + "\n"
	if Digest(sha256.Sum256([]byte(pins))).String() != reviewedProfilesDigest {
		t.Fatal("compiled components differ from the independently reviewed pins")
	}
	return key
}

func TestR2StitchRegistryAcceptsOnlyReviewedCombinedV3Bytes(t *testing.T) {
	key := reviewedCombinedV3Key(t)
	pair, err := LookupAcceptedPair(key)
	if err != nil || pair.Key() != key {
		t.Fatalf("reviewed self-pair refused: %v", err)
	}
	artifact, err := ParseDigest("5e29ba249eb1163b8bd2bac43b8f9f44163878914d00eaf5ecd68a8d0f949ead")
	if err != nil {
		t.Fatal(err)
	}
	for _, identity := range []ClientIdentity{pair.ProducerIdentity(), pair.ReaderIdentity()} {
		if identity.Version() != ClientCopyV3 || identity.ArtifactSHA256() != artifact || identity.SizeBytes() != 4996096 {
			t.Fatal("sealed identity differs from reviewed executing artifact")
		}
	}
	for _, component := range []struct {
		body []byte
		sha  Digest
		size int
	}{
		{pair.ProducerProfileJSON(), key.Producer, 1911},
		{pair.ReaderProfileJSON(), key.Reader, 1880},
		{pair.PairJSON(), key.Pair, 1832},
	} {
		if len(component.body) != component.size || Digest(sha256.Sum256(component.body)) != component.sha {
			t.Fatal("complete profile bytes differ from reviewed component")
		}
		var value map[string]any
		if err := json.Unmarshal(component.body, &value); err != nil {
			t.Fatal(err)
		}
		canonical, err := json.Marshal(value)
		if err != nil || !bytes.Equal(canonical, component.body) {
			t.Fatal("reviewed profile is not canonical JSON")
		}
	}
}

func TestR2StitchRegistryRejectsEveryAlteredCombinedKeyComponent(t *testing.T) {
	key := reviewedCombinedV3Key(t)
	for component := 0; component < 3; component++ {
		for offset := 0; offset < sha256.Size; offset++ {
			changed := key
			fields := []*Digest{&changed.Producer, &changed.Reader, &changed.Pair}
			fields[component][offset] ^= 1
			if got, err := LookupAcceptedPair(changed); err == nil || got != (AcceptedPair{}) {
				t.Fatalf("altered key component %d byte %d admitted", component, offset)
			}
		}
	}
	if got, err := LookupAcceptedPair(PairKey{Producer: key.Reader, Reader: key.Producer, Pair: key.Pair}); err == nil || got != (AcceptedPair{}) {
		t.Fatal("swapped original producer/current reader admitted")
	}
}

func TestR2StitchRegistryReviewedLookupReturnsIndependentValues(t *testing.T) {
	key := reviewedCombinedV3Key(t)
	first, err := LookupAcceptedPair(key)
	if err != nil {
		t.Fatal(err)
	}
	for _, get := range []func() []byte{first.ProducerProfileJSON, first.ReaderProfileJSON, first.PairJSON} {
		body := get()
		body[0] ^= 1
	}
	key.Producer[0] ^= 1
	artifact := first.ReaderIdentity().ArtifactSHA256()
	artifact[0] ^= 1
	second, err := LookupAcceptedPair(reviewedCombinedV3Key(t))
	if err != nil || first != second {
		t.Fatal("caller mutation changed sealed registry value")
	}
	for _, component := range []struct {
		body []byte
		sha  Digest
	}{
		{second.ProducerProfileJSON(), second.Key().Producer},
		{second.ReaderProfileJSON(), second.Key().Reader},
		{second.PairJSON(), second.Key().Pair},
	} {
		if Digest(sha256.Sum256(component.body)) != component.sha {
			t.Fatal("caller mutation changed subsequent profile bytes")
		}
	}
}
