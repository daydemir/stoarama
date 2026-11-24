package main

import (
	"sync"

	"github.com/daydemir/stoarama/backend/internal/api"
)

// r2KeepEverything is the active keep20 policy's Keep Everything set. Owner
// rule: Keep Everything footage stays in R2 (KE NAS copies leave against it),
// so no R2 retirement path may select a KE stream, whatever its pick says.
var r2KeepEverything = sync.OnceValues(func() (map[int64]bool, error) {
	_, windows, err := api.CleanupWindowPolicy()
	if err != nil {
		return nil, err
	}
	ke := map[int64]bool{}
	for id, w := range windows {
		if w.KeepEverything {
			ke[id] = true
		}
	}
	return ke, nil
})

// keepEverythingStream: the stream is Keep Everything by the keep20 policy or
// the owner's pick "all".
func (p inverseProtection) keepEverythingStream(stream int64) bool {
	return p.Picks.Choices[stream].Choice == "all" || r2RetireKeepEverything(stream)
}

// r2RetireKeepEverything is true when the stream is Keep Everything or the
// policy cannot be read (fail closed).
func r2RetireKeepEverything(stream int64) bool {
	ke, err := r2KeepEverything()
	return err != nil || ke[stream]
}
