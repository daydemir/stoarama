package api

import (
	"strings"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/stitchproof"
)

func TestNASStitchReadbackDiscoveryCannotManufactureAcceptance(t *testing.T) {
	if len(nasStitchReadbackCandidateKeys) != 0 {
		t.Fatal("draft API cannot publish a positive discovery placeholder")
	}
	for _, mode := range []nasStitchProofMode{nasStitchProofDisabled, nasStitchProofNative, nasStitchProofReadback, 3} {
		for _, version := range []string{"development", "r2-copy-v3", "v85r-nas-proof-v2", "cleanup-batch-v8"} {
			producer := ""
			if mode == nasStitchProofReadback {
				producer = strings.Repeat("a", 64)
			}
			pair, err := selectNASStitchReadbackPair(mode, version, strings.Repeat("b", 64), 100, producer)
			if err == nil || pair.Key() != (stitchproof.PairKey{}) {
				t.Fatal("empty registry/discovery created a profile acceptance")
			}
		}
	}
	if _, err := nasStitchPairComponents(stitchproof.AcceptedPair{}); err == nil {
		t.Fatal("zero/unknown accepted-pair value exported components")
	}
}
