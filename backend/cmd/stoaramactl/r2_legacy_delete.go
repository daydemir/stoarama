package main

import "github.com/daydemir/stoarama/backend/internal/r2"

func refuseLegacyR2Apply(apply bool) error {
	if apply {
		return r2.ErrLegacyDeleteRetired
	}
	return nil
}
