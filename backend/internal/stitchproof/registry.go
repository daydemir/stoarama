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

// LookupAcceptedPair is the sole registry admission boundary. No dual-profile
// pair has been accepted yet, so every key refuses. Legacy native-proof pins do
// not imply dual-profile acceptance; future entries require separate review of
// the complete profiles, relationship and final executing artifact bytes.
func LookupAcceptedPair(PairKey) (AcceptedPair, error) {
	return AcceptedPair{}, errors.New("stitch producer/reader pair has not been accepted")
}
