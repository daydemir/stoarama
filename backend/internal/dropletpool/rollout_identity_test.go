package dropletpool

import (
	"bytes"
	"strings"
	"testing"
)

func renderedKernelPolicyFixtures() ([]byte, []byte) {
	var v4, v6 []string
	for _, rule := range expectedKernelFirewallRules {
		family, line, _ := strings.Cut(rule, "|")
		if family == "v4" {
			v4 = append(v4, line)
		} else {
			v6 = append(v6, line)
		}
	}
	return []byte(strings.Join(v4, "\n") + "\n"), []byte(strings.Join(v6, "\n") + "\n")
}

func testCommonPolicy(t *testing.T) (CommonPolicyIdentity, string) {
	t.Helper()
	p := CommonPolicyIdentity{
		BuildSHA: strings.Repeat("a", 40), Capacity: 5, HeartbeatSec: 15, PollSec: 5,
		BackendAPIURL: "https://api.stoarama.example", FirewallID: "fw-rollout-v2",
		FirewallRulesSHA256: strings.Repeat("b", 64), FirewallTag: recorderFirewallTag,
		HostFirewallScriptSHA256: strings.Repeat("c", 64), KernelFirewallPolicySHA256: strings.Repeat("d", 64),
	}
	digest, err := p.Digest()
	if err != nil {
		t.Fatal(err)
	}
	return p, digest
}

func TestWorkerRolloutFactsAreServerDerivedAndBounded(t *testing.T) {
	policy := strings.Join(sortedExpectedKernelFirewallRules(), "\n") + "\n"
	facts := WorkerRolloutFacts{
		ExecutableBuildSHA: strings.Repeat("a", 40), Capacity: 5, HeartbeatSec: 15, PollSec: 5,
		BackendAPIURL: "https://API.stoarama.example/", HostFirewallScript: "#!/bin/sh\nexit 0\n", KernelPolicy: policy,
	}
	got, err := ValidateWorkerRolloutFacts(facts)
	if err != nil {
		t.Fatal(err)
	}
	if got.BackendAPIURL != "https://api.stoarama.example" || len(got.HostFirewallScriptSHA256) != 64 || len(got.KernelFirewallPolicySHA256) != 64 || len(got.ObservationSHA256) != 64 {
		t.Fatalf("unexpected normalized observation: %+v", got)
	}
	bad := facts
	bad.HostFirewallScript = strings.Repeat("x", maxHostFirewallScriptBytes+1)
	if _, err := ValidateWorkerRolloutFacts(bad); err == nil {
		t.Fatal("accepted oversized raw firewall script")
	}
	bad = facts
	bad.KernelPolicy = "v4|caller-selected\n"
	if _, err := ValidateWorkerRolloutFacts(bad); err == nil {
		t.Fatal("accepted caller-selected kernel policy")
	}
}

func TestDeriveKernelFirewallPolicyRejectsPrecedingOutputBypass(t *testing.T) {
	v4, v6 := renderedKernelPolicyFixtures()
	v4 = bytes.Replace(v4, []byte("-A OUTPUT -j STOARAMA_EGRESS"), []byte("-A OUTPUT -d 0.0.0.0/0 -j ACCEPT\n-A OUTPUT -j STOARAMA_EGRESS"), 1)
	if _, err := DeriveKernelFirewallPolicy(v4, v6); err == nil {
		t.Fatal("accepted an OUTPUT ACCEPT before the Stoarama hook")
	}
	v4, v6 = renderedKernelPolicyFixtures()
	v6 = bytes.Replace(v6, []byte("-A OUTPUT -j STOARAMA_EGRESS"), []byte("-A OUTPUT -j ACCEPT\n-A OUTPUT -j STOARAMA_EGRESS"), 1)
	if _, err := DeriveKernelFirewallPolicy(v4, v6); err == nil {
		t.Fatal("accepted an IPv6 OUTPUT ACCEPT before the Stoarama hook")
	}
	v4, v6 = renderedKernelPolicyFixtures()
	v4 = bytes.Replace(v4, []byte("-P OUTPUT ACCEPT\n"), nil, 1)
	if _, err := DeriveKernelFirewallPolicy(v4, v6); err == nil {
		t.Fatal("accepted a missing IPv4 OUTPUT default policy")
	}
}

func TestDeriveKernelFirewallPolicyRejectsMissingAndExtraRules(t *testing.T) {
	v4Bytes, v6Bytes := renderedKernelPolicyFixtures()
	v4 := strings.Split(strings.TrimSuffix(string(v4Bytes), "\n"), "\n")
	v6 := strings.Split(strings.TrimSuffix(string(v6Bytes), "\n"), "\n")
	valid, err := DeriveKernelFirewallPolicy(v4Bytes, v6Bytes)
	if err != nil || valid != strings.Join(sortedExpectedKernelFirewallRules(), "\n")+"\n" {
		t.Fatalf("derive valid policy: %q %v", valid, err)
	}
	if _, err := DeriveKernelFirewallPolicy([]byte(strings.Join(v4[:len(v4)-1], "\n")+"\n"), []byte(strings.Join(v6, "\n")+"\n")); err == nil {
		t.Fatal("accepted missing firewall rule")
	}
	if _, err := DeriveKernelFirewallPolicy([]byte(strings.Join(append(v4, "-A STOARAMA_EGRESS -d 203.0.113.0/24 -j REJECT"), "\n")+"\n"), []byte(strings.Join(v6, "\n")+"\n")); err == nil {
		t.Fatal("accepted extra firewall rule")
	}
}

func TestValidateRolloutProviderFactsRequiresExactTagCoverageAndHeadroom(t *testing.T) {
	facts := RolloutProviderFacts{
		AccountDropletLimit: 25, AccountDropletCount: 10,
		PoolDropletIDs:    []int64{1, 2, 3, 4, 5, 6, 7, 8},
		NonPoolDropletIDs: []int64{101, 102},
		FirewallID:        "fw-rollout-v2", FirewallStatus: "succeeded",
		FirewallTags: []string{recorderFirewallTag}, FirewallRulesSHA256: strings.Repeat("b", 64),
	}
	facts.NonPoolDroplets = []DODroplet{{ID: 101, Name: "other-one", Region: "nyc1", Size: "s-1vcpu-1gb", Status: "active"}, {ID: 102, Name: "other-two", Region: "nyc1", Size: "s-1vcpu-1gb", Status: "active"}}
	for i, id := range facts.PoolDropletIDs {
		facts.PoolDroplets = append(facts.PoolDroplets, DODroplet{ID: id, Name: "stoarama-rec-test-" + string(rune('a'+i)), Region: "nyc1", Size: "s-2vcpu-4gb", Status: "active", ProjectMember: true, Tags: []string{recorderFirewallTag}})
	}
	if err := validateRolloutProviderFacts(facts, facts.FirewallID, facts.FirewallRulesSHA256, recorderFirewallTag, 8, 1); err != nil {
		t.Fatalf("valid facts: %v", err)
	}
	changed := append([]DODroplet(nil), facts.NonPoolDroplets...)
	changed[0].Name = "replacement-with-same-provider-id"
	if nonPoolSetDigest(changed) == nonPoolSetDigest(facts.NonPoolDroplets) {
		t.Fatal("frozen non-pool identity ignored canonical object fields")
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

func TestBuildUserDataAlwaysKeepsFrozenHLSDisabledAndPinsExecutableBuild(t *testing.T) {
	worker := "stoarama-rec-rollout-1-1"
	build := strings.Repeat("a", 40)
	body, err := BuildUserData(UserDataConfig{ServerID: worker, NodeToken: "sin_test", BackendAPIURL: "https://api.stoarama.example", Capacity: 5, BuildSHA: build, RepoURL: "https://example.com/repo.git"})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"RECORDING_FROZEN_HLS_QUIESCENCE_ALLOWLIST=''", "-X main.recorderExecutableBuildSHA=$HEAD_SHA"} {
		if !strings.Contains(body, want) {
			t.Fatalf("cloud-init missing %q", want)
		}
	}
	for _, forbidden := range []string{"RECORDER_COMMON_POLICY_SHA256", "RECORDER_RUNTIME_CONFIG_SHA256", "DROPLET_POOL_CONFIG_PLAN"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("cloud-init retained dormant authority %q", forbidden)
		}
	}
}
