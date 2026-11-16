package api

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"

	"github.com/daydemir/stoarama/backend/internal/stitchproof"
)

type nasStitchProofMode int

const (
	nasStitchProofDisabled nasStitchProofMode = iota
	nasStitchProofNative
	nasStitchProofReadback
)

// Discovery conveys no acceptance: the one reviewed self-pair must still
// resolve through the gate-owned sealed registry. Return a value so callers
// cannot change later discovery by mutating a shared table.
func nasStitchReadbackCandidateKeys() [1]stitchproof.PairKey {
	return [1]stitchproof.PairKey{{
		Producer: stitchproof.Digest{0xa4, 0x4d, 0x85, 0x3a, 0x56, 0xa8, 0xc4, 0xde, 0xd2, 0x94, 0xaf, 0x3d, 0x35, 0xa3, 0xd8, 0xdd, 0x9f, 0xc1, 0x21, 0x5c, 0xfc, 0x1b, 0x55, 0x2b, 0xc2, 0x2c, 0x44, 0xd9, 0xe0, 0x22, 0xa5, 0x97},
		Reader:   stitchproof.Digest{0xf0, 0x3a, 0xfd, 0x96, 0x6d, 0xad, 0x49, 0x23, 0x6e, 0x45, 0x21, 0x81, 0x6a, 0xde, 0xe0, 0x23, 0xed, 0xa8, 0x63, 0x77, 0x43, 0xfb, 0x59, 0xc0, 0x7c, 0xe9, 0x5f, 0x25, 0x28, 0x64, 0xb2, 0x08},
		Pair:     stitchproof.Digest{0xb0, 0x11, 0xfd, 0xa9, 0x5a, 0x83, 0xa8, 0xab, 0xd2, 0x80, 0x2b, 0x2b, 0xed, 0x23, 0xd0, 0x1f, 0x86, 0x0d, 0x43, 0xe3, 0x0e, 0x13, 0xe0, 0xbb, 0x7c, 0xea, 0x60, 0x5a, 0x16, 0x30, 0x4a, 0x5b},
	}}
}

type nasStitchProfileComponents struct {
	ProducerSHA string `json:"producer_profile_sha256"`
	ReaderSHA   string `json:"reader_profile_sha256"`
	PairSHA     string `json:"profile_pair_sha256"`
	ProducerB64 string `json:"producer_profile_b64"`
	ReaderB64   string `json:"reader_profile_b64"`
	PairB64     string `json:"profile_pair_b64"`
}

func nasStitchPairComponents(pair stitchproof.AcceptedPair) (nasStitchProfileComponents, error) {
	key := pair.Key()
	producer, reader, binding := pair.ProducerProfileJSON(), pair.ReaderProfileJSON(), pair.PairJSON()
	for _, component := range []struct {
		body []byte
		sha  stitchproof.Digest
	}{{producer, key.Producer}, {reader, key.Reader}, {binding, key.Pair}} {
		if component.sha == (stitchproof.Digest{}) || len(component.body) == 0 || len(component.body) > 65536 ||
			stitchproof.Digest(sha256.Sum256(component.body)) != component.sha {
			return nasStitchProfileComponents{}, errors.New("sealed profile component unavailable")
		}
	}
	return nasStitchProfileComponents{ProducerSHA: key.Producer.String(), ReaderSHA: key.Reader.String(), PairSHA: key.Pair.String(),
		ProducerB64: base64.StdEncoding.EncodeToString(producer), ReaderB64: base64.StdEncoding.EncodeToString(reader),
		PairB64: base64.StdEncoding.EncodeToString(binding)}, nil
}

func sameNASStitchClient(a, b stitchproof.ClientIdentity) bool {
	return a.Version() == b.Version() && a.ArtifactSHA256() == b.ArtifactSHA256() && a.SizeBytes() == b.SizeBytes()
}

func selectNASStitchReadbackPair(mode nasStitchProofMode, version, artifactSHA string, artifactBytes int64, originalProducer string) (stitchproof.AcceptedPair, error) {
	var zero stitchproof.AcceptedPair
	artifact, err := stitchproof.ParseDigest(artifactSHA)
	if err != nil || artifact == (stitchproof.Digest{}) || artifactBytes <= 0 || artifactBytes > 8*1024*1024 ||
		version != string(stitchproof.ClientCopyV3) || (mode != nasStitchProofNative && mode != nasStitchProofReadback) {
		return zero, errors.New("exact current reader discovery identity required")
	}
	var producer stitchproof.Digest
	if mode == nasStitchProofReadback {
		producer, err = stitchproof.ParseDigest(originalProducer)
		if err != nil || producer == (stitchproof.Digest{}) {
			return zero, errors.New("original producer profile required for renewal")
		}
	} else if originalProducer != "" {
		return zero, errors.New("native admission cannot substitute an original producer")
	}
	var selected stitchproof.AcceptedPair
	found := false
	for _, key := range nasStitchReadbackCandidateKeys() {
		pair, err := stitchproof.LookupAcceptedPair(key)
		if err != nil {
			return zero, errors.New("compiled discovery key is not accepted")
		}
		if pair.Key() != key {
			return zero, errors.New("sealed pair key differs")
		}
		reader := pair.ReaderIdentity()
		if reader.Version() != stitchproof.ClientCopyV3 || reader.ArtifactSHA256() != artifact || reader.SizeBytes() != artifactBytes {
			continue
		}
		if mode == nasStitchProofNative && !sameNASStitchClient(pair.ProducerIdentity(), reader) {
			continue
		}
		if mode == nasStitchProofReadback && key.Producer != producer {
			continue
		}
		if found {
			return zero, errors.New("current reader profile discovery ambiguous")
		}
		selected, found = pair, true
	}
	if !found {
		return zero, errors.New("no accepted producer/current-reader pair")
	}
	if _, err := nasStitchPairComponents(selected); err != nil {
		return zero, err
	}
	return selected, nil
}
