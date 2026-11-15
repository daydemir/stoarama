package stitchproof

import (
	"bytes"
	"crypto/sha256"
	"strings"
	"testing"
)

func TestR2StitchRegistryDigestStrictEncoding(t *testing.T) {
	expected := Digest(sha256.Sum256([]byte("synthetic profile")))
	actual, err := ParseDigest(expected.String())
	if err != nil || actual != expected {
		t.Fatalf("digest roundtrip: %v, %v", actual, err)
	}
	for _, value := range []string{"", "0", strings.Repeat("0", 63), strings.Repeat("0", 65), strings.Repeat("g", 64), strings.ToUpper(expected.String()), " " + expected.String()} {
		if _, err := ParseDigest(value); err == nil {
			t.Errorf("accepted malformed digest %q", value)
		}
	}
}

func TestR2StitchRegistryHasNoAcceptedPairs(t *testing.T) {
	producer := Digest(sha256.Sum256([]byte("unaccepted producer")))
	reader := Digest(sha256.Sum256([]byte("unaccepted reader")))
	pair := Digest(sha256.Sum256([]byte("unaccepted relationship")))
	legacy, err := ParseDigest(ClientSHA)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []PairKey{{}, {Producer: producer}, {Reader: reader}, {Pair: pair},
		{Producer: producer, Reader: reader, Pair: pair}, {Producer: reader, Reader: producer, Pair: pair},
		{Producer: legacy, Reader: legacy, Pair: pair}} {
		got, err := LookupAcceptedPair(key)
		if err == nil || got != (AcceptedPair{}) {
			t.Fatalf("unreviewed key returned acceptance: %v, %v", key, err)
		}
	}
}

func TestR2StitchRegistryGettersOwnTheirValuesAndBytes(t *testing.T) {
	// Synthetic private fixture only: this is never a production registry entry.
	producerJSON := `{"synthetic":"producer"}`
	readerJSON := `{"synthetic":"reader"}`
	pairJSON := `{"synthetic":"relationship"}`
	key := PairKey{Producer: Digest(sha256.Sum256([]byte(producerJSON))),
		Reader: Digest(sha256.Sum256([]byte(readerJSON))), Pair: Digest(sha256.Sum256([]byte(pairJSON)))}
	producer := ClientIdentity{version: ClientNativeV2, artifactSHA256: Digest{1}, sizeBytes: 123}
	reader := ClientIdentity{version: ClientCopyV3, artifactSHA256: Digest{2}, sizeBytes: 456}
	fixture := AcceptedPair{key: key, producer: producer, reader: reader,
		producerProfile: producerJSON, readerProfile: readerJSON, pair: pairJSON}
	for _, sample := range []struct {
		get  func() []byte
		want string
	}{{fixture.ProducerProfileJSON, producerJSON}, {fixture.ReaderProfileJSON, readerJSON}, {fixture.PairJSON, pairJSON}} {
		first, second := sample.get(), sample.get()
		first[0] = '!'
		if !bytes.Equal(second, []byte(sample.want)) || !bytes.Equal(sample.get(), []byte(sample.want)) {
			t.Fatal("caller mutated immutable registry bytes")
		}
	}
	copyKey := fixture.Key()
	copyKey.Producer[0] ^= 1
	if fixture.Key() != key {
		t.Fatal("caller mutated registry key")
	}
	for _, sample := range []struct {
		got, want ClientIdentity
	}{{fixture.ProducerIdentity(), producer}, {fixture.ReaderIdentity(), reader}} {
		if sample.got.Version() != sample.want.version || sample.got.ArtifactSHA256() != sample.want.artifactSHA256 || sample.got.SizeBytes() != sample.want.sizeBytes {
			t.Fatal("identity getters changed artifact binding")
		}
		digest := sample.got.ArtifactSHA256()
		digest[0] ^= 1
		if sample.got.ArtifactSHA256() != sample.want.artifactSHA256 {
			t.Fatal("caller mutated artifact identity")
		}
	}
	if got, err := LookupAcceptedPair(key); err == nil || got != (AcceptedPair{}) {
		t.Fatal("synthetic getter fixture leaked into production acceptance")
	}
}
