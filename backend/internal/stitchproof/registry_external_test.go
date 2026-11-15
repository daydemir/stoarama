package stitchproof_test

import "github.com/daydemir/stoarama/backend/internal/stitchproof"

// The API lane imports these signatures without a duplicate registry or parser.
var _ func(stitchproof.PairKey) (stitchproof.AcceptedPair, error) = stitchproof.LookupAcceptedPair
var _ interface {
	Key() stitchproof.PairKey
	ProducerProfileJSON() []byte
	ReaderProfileJSON() []byte
	PairJSON() []byte
	ProducerIdentity() stitchproof.ClientIdentity
	ReaderIdentity() stitchproof.ClientIdentity
} = stitchproof.AcceptedPair{}
var _ interface {
	Version() stitchproof.ClientVersion
	ArtifactSHA256() stitchproof.Digest
	SizeBytes() int64
} = stitchproof.ClientIdentity{}
