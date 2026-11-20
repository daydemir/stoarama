package main

import (
	"errors"
	"slices"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

// durhamStagePicks derives the exact decided and keep-everything stream sets a
// stage retirement may act on, from one independently hash-pinned owner picks
// file. It never takes them from the evidence document: the evidence's lists
// are only compared against this derivation.
//
// The decision rule is the existing reviewed one, unchanged: a stream is decided
// only when r2RetireEffectiveChoice accepts its saved pick against the embedded
// policy window for that stream, and it is keep-everything only when that
// accepted choice is exactly `all`. A pick made against a different window, an
// unknown choice, or a stream the policy does not cover stays undecided, so a
// stage hour on it fails closed upstream.
func durhamStagePicks(path, digest string, windows map[int64]api.Keep28Window) (r2.StitchStagePicks, error) {
	picks, err := loadR2RetirePicks(path)
	if err != nil {
		return r2.StitchStagePicks{}, err
	}
	// The caller's digest is the independent pin. It must be the digest of the
	// bytes just parsed, so an approved digest cannot ride along on a swapped
	// picks file.
	if !r2RetireLowerHex64(digest) || picks.SHA256 != digest {
		return r2.StitchStagePicks{}, errors.New("owner picks differ from the independently approved digest")
	}
	out := r2.StitchStagePicks{SHA256: picks.SHA256}
	for id, pick := range picks.Choices {
		window, ok := windows[id]
		if !ok {
			// A stream outside the embedded policy has no reviewed window, so
			// its pick is undecided rather than silently accepted.
			continue
		}
		choice := r2RetireEffectiveChoice(pick, true, window)
		if choice == "" {
			continue
		}
		out.Decided = append(out.Decided, id)
		if choice == "all" {
			out.KE = append(out.KE, id)
		}
	}
	slices.Sort(out.Decided)
	slices.Sort(out.KE)
	return out, nil
}
