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

// Discovery keys confer no acceptance. Each key must independently resolve
// through the gate-owned sealed registry; its production table is also empty.
// Adding final exact keys requires separately reviewed full profile acceptance.
var nasStitchReadbackCandidateKeys = [...]stitchproof.PairKey{}

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
	for _, key := range nasStitchReadbackCandidateKeys {
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
