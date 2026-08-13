package dropletpool

import (
	"strings"
	"testing"
)

func testCommonPolicy(t *testing.T) (CommonPolicyIdentity, string) {
	t.Helper()
	p := CommonPolicyIdentity{
		BuildSHA: strings.Repeat("a", 40), Capacity: 5, HeartbeatSec: 15, PollSec: 5,
		BackendAPIURL: "https://api.stoarama.example", FirewallID: "fw-rollout-v2",
		FirewallRulesSHA256: strings.Repeat("b", 64), FirewallTag: recorderFirewallTag,
	}
	digest, err := p.Digest()
	if err != nil {
		t.Fatal(err)
	}
	return p, digest
}

func TestRolloutIdentitiesSeparateCommonAndPerWorkerPolicy(t *testing.T) {
	_, common := testCommonPolicy(t)
	a := RuntimeConfigIdentity{CommonPolicySHA256: common, WorkerID: "stoarama-rec-rollout-1-1"}
	b := RuntimeConfigIdentity{CommonPolicySHA256: common, WorkerID: "stoarama-rec-rollout-1-2"}
	ad, err := a.Digest()
	if err != nil {
		t.Fatal(err)
	}
	bd, err := b.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if ad == bd || len(ad) != 64 || len(bd) != 64 {
		t.Fatalf("runtime identities not worker-specific: %q %q", ad, bd)
	}
	withRecording := RuntimeConfigIdentity{CommonPolicySHA256: common, WorkerID: a.WorkerID, RecordingIDs: []int64{437}}
	allowlist, err := withRecording.Allowlist()
	if err != nil || allowlist != "stoarama-rec-rollout-1-1/437" {
		t.Fatalf("exact allowlist=%q err=%v", allowlist, err)
	}
}

func TestValidateRolloutProviderFactsRequiresExactTagCoverageAndHeadroom(t *testing.T) {
	facts := RolloutProviderFacts{
		AccountDropletLimit: 25, AccountDropletCount: 10,
		PoolDropletIDs: []int64{1, 2, 3, 4, 5, 6, 7, 8},
		FirewallID:     "fw-rollout-v2", FirewallStatus: "succeeded",
		FirewallTags: []string{recorderFirewallTag}, FirewallRulesSHA256: strings.Repeat("b", 64),
	}
	if err := validateRolloutProviderFacts(facts, facts.FirewallID, facts.FirewallRulesSHA256, recorderFirewallTag, 8, 1); err != nil {
		t.Fatalf("valid facts: %v", err)
	}
	for name, mutate := range map[string]func(*RolloutProviderFacts){
		"explicit ids instead of tag": func(f *RolloutProviderFacts) {
			f.FirewallTags = nil
			f.FirewallDropletIDs = append([]int64(nil), f.PoolDropletIDs...)
		},
		"extra tag":           func(f *RolloutProviderFacts) { f.FirewallTags = append(f.FirewallTags, "other") },
		"mixed membership":    func(f *RolloutProviderFacts) { f.FirewallDropletIDs = []int64{1} },
		"no surge quota":      func(f *RolloutProviderFacts) { f.AccountDropletLimit = f.AccountDropletCount },
		"missing pool worker": func(f *RolloutProviderFacts) { f.PoolDropletIDs = f.PoolDropletIDs[:7] },
	} {
		t.Run(name, func(t *testing.T) {
			bad := facts
			bad.PoolDropletIDs = append([]int64(nil), facts.PoolDropletIDs...)
			bad.FirewallTags = append([]string(nil), facts.FirewallTags...)
			mutate(&bad)
			if err := validateRolloutProviderFacts(bad, facts.FirewallID, facts.FirewallRulesSHA256, recorderFirewallTag, 8, 1); err == nil {
				t.Fatal("accepted unsafe provider facts")
			}
		})
	}
}

func TestBuildUserDataPropagatesExactEmptyOrPairedAllowlist(t *testing.T) {
	_, common := testCommonPolicy(t)
	worker := "stoarama-rec-rollout-1-1"
	runtimeSHA, err := (RuntimeConfigIdentity{CommonPolicySHA256: common, WorkerID: worker}).Digest()
	if err != nil {
		t.Fatal(err)
	}
	body, err := BuildUserData(UserDataConfig{ServerID: worker, NodeToken: "sin_test", BackendAPIURL: "https://api.stoarama.example", Capacity: 5, BuildSHA: strings.Repeat("a", 40), RepoURL: "https://example.com/repo.git", CommonPolicySHA256: common, RuntimeConfigSHA256: runtimeSHA})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"RECORDER_COMMON_POLICY_SHA256='" + common + "'", "RECORDER_RUNTIME_CONFIG_SHA256='" + runtimeSHA + "'", "RECORDING_FROZEN_HLS_QUIESCENCE_ALLOWLIST=''"} {
		if !strings.Contains(body, want) {
			t.Fatalf("cloud-init missing %q", want)
		}
	}
	if _, err := BuildUserData(UserDataConfig{ServerID: worker, NodeToken: "sin_test", BackendAPIURL: "https://api.stoarama.example", FrozenHLSAllowlist: "other/437"}); err == nil {
		t.Fatal("accepted allowlist for another worker")
	}
}
