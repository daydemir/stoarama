package main

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
)

func TestR2FenceProjectionPreservesVetoesAndCannotExecute(t *testing.T) {
	_, windows, err := api.Keep28Policy()
	if err != nil {
		t.Fatal(err)
	}
	w := windows[3]
	base := r2tCandidate("raw", 3, w.Timezone, w.Start.Add(-2*time.Hour), w.Start.Add(-time.Hour))
	base.RelativePath = "p/outside.mp4"
	picks := r2RetirePicks{Choices: map[int64]r2RetirePick{3: {Choice: "none", KeepStartUTC: w.Start.Format(time.RFC3339), KeepEndUTC: w.End.Format(time.RFC3339)}}}
	scope := r2RetireScope{AccountID: 47, ConnectionID: 13, Bucket: base.Bucket, NASBacked: true}
	for name, mutate := range map[string]func(*r2RetireCandidate){
		"hold":     func(c *r2RetireCandidate) { c.Facts.Held = true },
		"joined":   func(c *r2RetireCandidate) { c.Facts.JoinedRef = true },
		"shared":   func(c *r2RetireCandidate) { c.Facts.SharedKey = true },
		"restore":  func(c *r2RetireCandidate) { c.Facts.RestorePending = true },
		"presence": func(c *r2RetireCandidate) { c.Facts.ListingPresence = true },
		"kept": func(c *r2RetireCandidate) {
			c.StartAt = w.Start
			c.EndAt = w.Start.Add(time.Minute)
			c.SourceStart = c.StartAt
			c.SourceEnd = c.EndAt
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := base
			mutate(&c)
			p := &r2RetireProjection{}
			p.add(c, scope, picks, windows, false)
			if p.FenceListingPotential.Objects != 0 {
				t.Fatal("veto bypassed")
			}
		})
	}
	p := &r2RetireProjection{}
	p.add(base, scope, picks, windows, false)
	if p.FenceListingPotential.Objects != 1 || p.ListingPotential.Objects != 0 {
		t.Fatal("unsettled delivery was not projected only under fence")
	}
	path := t.TempDir()
	if err := p.write(path, r2RetireHeader{Version: r2RetireManifestVersion, NASBacked: true}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := readR2RetireManifest(filepath.Join(path, "after-fence-and-listing-upper-bound.jsonl")); err == nil {
		t.Fatal("forecast accepted for deletion")
	}
	for _, pick := range []string{"all", ""} {
		changed := r2RetirePicks{Choices: map[int64]r2RetirePick{3: picks.Choices[3]}}
		v := changed.Choices[3]
		v.Choice = pick
		changed.Choices[3] = v
		p := &r2RetireProjection{}
		p.add(base, scope, changed, windows, false)
		if p.FenceListingPotential.Objects != 0 {
			t.Fatal("owner pick bypassed")
		}
	}
	for _, path := range []string{base.RelativePath, "alias/outside.mp4"} {
		p := &r2RetireProjection{unsafePaths: map[string]bool{path: true}, rawAliases: map[int64]string{base.SourceID: "alias/outside.mp4"}}
		p.add(base, scope, picks, windows, false)
		if p.FenceListingPotential.Objects != 0 {
			t.Fatal("kept path writer bypassed")
		}
	}
}
