package main

import (
	"testing"
	"time"
)

func validCampaignSeedPolicyFixture() campaignSeedManifest {
	makeEntries := func(n int) []campaignSeedEntry {
		out := make([]campaignSeedEntry, n)
		for i := range out {
			out[i] = campaignSeedEntry{RecordingID: int64(i + 1), SceneIdentitySHA256: string(make([]byte, 64)), Role: "primary", Status: "protect", Rank: i + 1, ReasonCodes: []string{"fixture"}}
		}
		return out
	}
	return campaignSeedManifest{Tracks: []campaignSeedTrack{{Key: "delivery30", DeadlineAt: time.Date(2026, 8, 19, 23, 59, 59, 0, time.UTC), TargetCount: 30, GradeFloor: "GOOD", Entries: makeEntries(30)}, {Key: "repair17", DeadlineAt: time.Date(2026, 8, 26, 23, 59, 59, 0, time.UTC), TargetCount: 17, GradeFloor: "GOOD", Entries: makeEntries(17)}}}
}
func TestValidateCampaignSeedPolicy(t *testing.T) {
	m := validCampaignSeedPolicyFixture()
	if err := validateCampaignSeedPolicy(m); err != nil {
		t.Fatal(err)
	}
	cases := []func(*campaignSeedManifest){func(x *campaignSeedManifest) { x.Tracks[0].TargetCount = 1 }, func(x *campaignSeedManifest) { x.Tracks[1].Key = "delivery30" }, func(x *campaignSeedManifest) { x.Tracks[0].DeadlineAt = x.Tracks[0].DeadlineAt.Add(time.Second) }, func(x *campaignSeedManifest) { x.Tracks[1].RequiredConsecutiveWindows = 14 }}
	for i, mut := range cases {
		x := validCampaignSeedPolicyFixture()
		mut(&x)
		if validateCampaignSeedPolicy(x) == nil {
			t.Fatalf("case %d accepted", i)
		}
	}
}
