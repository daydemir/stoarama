package stitchproof

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

// Digest is an exact SHA-256 value, independent of artifact paths or JSON syntax.
type Digest [sha256.Size]byte

func ParseDigest(value string) (Digest, error) {
	var result Digest
	if !digestPattern.MatchString(value) {
		return result, errors.New("SHA-256 must be 64 lowercase hexadecimal characters")
	}
	_, err := hex.Decode(result[:], []byte(value))
	return result, err
}

func (d Digest) String() string { return hex.EncodeToString(d[:]) }

// PairKey binds both complete profile files and their exact relationship file.
// A client version or artifact digest alone never identifies an accepted pair.
type PairKey struct {
	Producer Digest
	Reader   Digest
	Pair     Digest
}

// ClientVersion identifies a capability label; it does not establish acceptance.
type ClientVersion string

const (
	ClientNativeV2  ClientVersion = "v85r-nas-proof-v2"
	ClientCopyV3    ClientVersion = "r2-copy-v3"
	ClientCleanupV8 ClientVersion = "cleanup-batch-v8"
)

// ClientIdentity is the executing artifact identity from an accepted profile.
// Its fields are private so callers cannot construct a positive identity.
type ClientIdentity struct {
	version        ClientVersion
	artifactSHA256 Digest
	sizeBytes      int64
}

func (c ClientIdentity) Version() ClientVersion { return c.version }
func (c ClientIdentity) ArtifactSHA256() Digest { return c.artifactSHA256 }
func (c ClientIdentity) SizeBytes() int64       { return c.sizeBytes }

// AcceptedPair carries immutable, separately reviewed profile bytes. It is not
// stitching Authority: proof replay, current observations and all other gates
// remain mandatory. Its zero value represents no accepted pair.
type AcceptedPair struct {
	key             PairKey
	producer        ClientIdentity
	reader          ClientIdentity
	producerProfile string
	readerProfile   string
	pair            string
}

func (p AcceptedPair) Key() PairKey                     { return p.key }
func (p AcceptedPair) ProducerIdentity() ClientIdentity { return p.producer }
func (p AcceptedPair) ReaderIdentity() ClientIdentity   { return p.reader }
func (p AcceptedPair) ProducerProfileJSON() []byte      { return []byte(p.producerProfile) }
func (p AcceptedPair) ReaderProfileJSON() []byte        { return []byte(p.readerProfile) }
func (p AcceptedPair) PairJSON() []byte                 { return []byte(p.pair) }

// LookupAcceptedPair is the sole registry admission boundary. Only the complete
// reviewed combined-v3 self-pair resolves. A registry value is not Authority;
// actual proof replay, current observations and every retirement gate remain.
func LookupAcceptedPair(key PairKey) (AcceptedPair, error) {
	expected := PairKey{
		Producer: Digest{0xa4, 0x4d, 0x85, 0x3a, 0x56, 0xa8, 0xc4, 0xde, 0xd2, 0x94, 0xaf, 0x3d, 0x35, 0xa3, 0xd8, 0xdd, 0x9f, 0xc1, 0x21, 0x5c, 0xfc, 0x1b, 0x55, 0x2b, 0xc2, 0x2c, 0x44, 0xd9, 0xe0, 0x22, 0xa5, 0x97},
		Reader:   Digest{0xf0, 0x3a, 0xfd, 0x96, 0x6d, 0xad, 0x49, 0x23, 0x6e, 0x45, 0x21, 0x81, 0x6a, 0xde, 0xe0, 0x23, 0xed, 0xa8, 0x63, 0x77, 0x43, 0xfb, 0x59, 0xc0, 0x7c, 0xe9, 0x5f, 0x25, 0x28, 0x64, 0xb2, 0x08},
		Pair:     Digest{0xb0, 0x11, 0xfd, 0xa9, 0x5a, 0x83, 0xa8, 0xab, 0xd2, 0x80, 0x2b, 0x2b, 0xed, 0x23, 0xd0, 0x1f, 0x86, 0x0d, 0x43, 0xe3, 0x0e, 0x13, 0xe0, 0xbb, 0x7c, 0xea, 0x60, 0x5a, 0x16, 0x30, 0x4a, 0x5b},
	}
	if key != expected {
		return AcceptedPair{}, errors.New("stitch producer/reader pair has not been accepted")
	}
	identity := ClientIdentity{version: ClientCopyV3,
		artifactSHA256: Digest{0x5e, 0x29, 0xba, 0x24, 0x9e, 0xb1, 0x16, 0x3b, 0x8b, 0xd2, 0xba, 0xc4, 0x3b, 0x8f, 0x9f, 0x44, 0x16, 0x38, 0x78, 0x91, 0x4d, 0x00, 0xea, 0xf5, 0xec, 0xd6, 0x8a, 0x8d, 0x0f, 0x94, 0x9e, 0xad},
		sizeBytes:      4996096}
	return AcceptedPair{key: key, producer: identity, reader: identity,
		producerProfile: combinedV3ProducerProfile, readerProfile: combinedV3ReaderProfile,
		pair: combinedV3ProfilePair}, nil
}
