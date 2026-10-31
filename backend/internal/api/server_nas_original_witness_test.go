package api

import (
	"strings"
	"testing"
	"time"
)

func TestOriginalWitnessProofCannotInventCoverage(t *testing.T) {
	c := &nasOriginalWitnessContext{PolicyVersion: nasOriginalWitnessPolicy, RegisteredStartUTC: time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC), RegisteredEndUTC: time.Date(2026, 7, 20, 12, 1, 0, 0, time.UTC), Timezone: "UTC", LocalDate: "2026-07-20"}
	base := nasOriginalWitnessProof{PolicyVersion: nasOriginalWitnessPolicy, Status: "verified", VideoFrames: 2, StrictFullDecode: true, IntervalsUS: [][2]int64{{0, 60000000}}, FFmpegSHA256: strings.Repeat("a", 64), FFprobeSHA256: strings.Repeat("b", 64)}
	if err := validateNASOriginalWitness(&base, c); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*nasOriginalWitnessProof){
		"no decode":           func(p *nasOriginalWitnessProof) { p.StrictFullDecode = false },
		"no frames":           func(p *nasOriginalWitnessProof) { p.VideoFrames = 0 },
		"no tool identity":    func(p *nasOriginalWitnessProof) { p.FFmpegSHA256 = "" },
		"negative":            func(p *nasOriginalWitnessProof) { p.IntervalsUS = [][2]int64{{-1, 1}} },
		"over cap":            func(p *nasOriginalWitnessProof) { p.IntervalsUS = [][2]int64{{0, 60000001}} },
		"overlap":             func(p *nasOriginalWitnessProof) { p.IntervalsUS = [][2]int64{{0, 20}, {10, 30}} },
		"unmerged":            func(p *nasOriginalWitnessProof) { p.IntervalsUS = [][2]int64{{0, 20}, {20, 30}} },
		"empty":               func(p *nasOriginalWitnessProof) { p.IntervalsUS = nil },
		"unknown policy":      func(p *nasOriginalWitnessProof) { p.PolicyVersion = "invented" },
		"unverified coverage": func(p *nasOriginalWitnessProof) { p.Status = "unverified"; p.Reason = "unsupported" },
	} {
		p := base
		mutate(&p)
		if validateNASOriginalWitness(&p, c) == nil {
			t.Errorf("accepted %s", name)
		}
	}
	unknown := nasOriginalWitnessProof{PolicyVersion: nasOriginalWitnessPolicy, Status: "unverified", Reason: "unsupported timing"}
	if err := validateNASOriginalWitness(&unknown, c); err != nil {
		t.Fatal(err)
	}
	c.LocalDate = "2026-07-21"
	if validateNASOriginalWitness(&base, c) == nil {
		t.Fatal("wrong local date accepted")
	}
}
