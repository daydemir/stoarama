package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func inverseUseOwnerFenceEvidence(t *testing.T, o *inverseApplyOptions) {
	t.Helper()
	var e inverseDrainEvidence
	if err := inverseReadPinned(o.DrainEvidence, o.DrainSHA, &e); err != nil {
		t.Fatal(err)
	}
	e.Proof = inverseOwnerFenceProof
	e.AcceptedPUTCompletion, e.DeployedWriterCoverage = "", ""
	e.FencePolicy = &inverseFencePolicyEvidence{PartitionSHA256: o.ManifestSHA, SummarySHA256: o.SummarySHA, LastModifiedBefore: inverseRetirementCutoff(), OwnerDecision: inverseEvidenceReference{Path: "../../../docs/records/r2-owner-fence-policy-20261118.md", SHA256: inverseOwnerDecisionSHA256}, ResidualAcceptance: inverseApprovedFenceResidual}
	data, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	o.DrainEvidence = filepath.Join(t.TempDir(), "owner-policy.json")
	if err = os.WriteFile(o.DrainEvidence, data, 0600); err != nil {
		t.Fatal(err)
	}
	o.DrainSHA, err = inverseFileSHA(o.DrainEvidence)
	if err != nil {
		t.Fatal(err)
	}
}

func TestR2InverseDrainProofScopeAndEvidence(t *testing.T) {
	activation := inverseRetirementCutoff().Add(37031 * time.Microsecond)
	deadline := activation.Add(6 * 24 * time.Hour)
	o := inverseApplyOptions{ManifestSHA: r2tSHA, SummarySHA: r2tSHA}
	valid := func() inverseDrainEvidence {
		return inverseDrainEvidence{Proof: inverseOwnerFenceProof, Bucket: "stoarama", FenceActivatedAt: activation, DrainNotBefore: deadline, ObservedAt: deadline.Add(time.Minute), FencePolicy: &inverseFencePolicyEvidence{PartitionSHA256: r2tSHA, SummarySHA256: r2tSHA, LastModifiedBefore: inverseRetirementCutoff(), OwnerDecision: inverseEvidenceReference{Path: "../../../docs/records/r2-owner-fence-policy-20261118.md", SHA256: inverseOwnerDecisionSHA256}, ResidualAcceptance: inverseApprovedFenceResidual}}
	}
	for _, lane := range []inverseLane{inverseOutsideKeep28, inverseNASKeep28} {
		opts := o
		opts.Lane = lane
		if err := valid().validateProof(opts); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		name   string
		mutate func(*inverseDrainEvidence, *inverseApplyOptions)
	}{
		{"missing route", func(e *inverseDrainEvidence, _ *inverseApplyOptions) { e.Proof = "" }},
		{"retired route", func(e *inverseDrainEvidence, _ *inverseApplyOptions) { e.Proof = "non28_key_policy_v1" }},
		{"missing policy", func(e *inverseDrainEvidence, _ *inverseApplyOptions) { e.FencePolicy = nil }},
		{"completion claim", func(e *inverseDrainEvidence, _ *inverseApplyOptions) { e.AcceptedPUTCompletion = "completed" }},
		{"coverage claim", func(e *inverseDrainEvidence, _ *inverseApplyOptions) { e.DeployedWriterCoverage = "covered" }},
		{"bucket", func(e *inverseDrainEvidence, _ *inverseApplyOptions) { e.Bucket = "other" }},
		{"different fence second", func(e *inverseDrainEvidence, _ *inverseApplyOptions) {
			e.FenceActivatedAt = e.FenceActivatedAt.Add(time.Second)
		}},
		{"cutoff", func(e *inverseDrainEvidence, _ *inverseApplyOptions) {
			e.FencePolicy.LastModifiedBefore = e.FencePolicy.LastModifiedBefore.Add(time.Second)
		}},
		{"deadline before activation", func(e *inverseDrainEvidence, _ *inverseApplyOptions) { e.DrainNotBefore = e.FenceActivatedAt }},
		{"partition", func(_ *inverseDrainEvidence, o *inverseApplyOptions) { o.ManifestSHA = "changed" }},
		{"summary", func(_ *inverseDrainEvidence, o *inverseApplyOptions) { o.SummarySHA = "changed" }},
		{"race not accepted", func(e *inverseDrainEvidence, _ *inverseApplyOptions) { e.FencePolicy.ResidualAcceptance = "" }},
		{"owner record hash", func(e *inverseDrainEvidence, _ *inverseApplyOptions) { e.FencePolicy.OwnerDecision.SHA256 = r2tSHA }},
		{"owner record missing", func(e *inverseDrainEvidence, _ *inverseApplyOptions) { e.FencePolicy.OwnerDecision.Path += ".missing" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := valid()
			opts := o
			tc.mutate(&e, &opts)
			if err := e.validateProof(opts); err == nil {
				t.Fatal("ambiguous proof accepted")
			}
		})
	}
	completion := inverseDrainEvidence{Proof: inversePUTCompletionProof, AcceptedPUTCompletion: "terminal provider evidence", DeployedWriterCoverage: "complete writer coverage"}
	if err := completion.validateProof(o); err != nil {
		t.Fatal(err)
	}
	completion.FencePolicy = valid().FencePolicy
	if err := completion.validateProof(o); err == nil {
		t.Fatal("mixed completion route accepted")
	}
}

func TestR2InverseLastModifiedCutoff(t *testing.T) {
	cutoff := inverseRetirementCutoff()
	now := cutoff.Add(time.Hour)
	if !inverseLastModifiedAllowed(cutoff.Add(-time.Nanosecond), now, now) {
		t.Fatal("pre-fence object required seven days")
	}
	for _, modified := range []time.Time{{}, cutoff, cutoff.Add(time.Nanosecond), now.Add(time.Hour)} {
		if inverseLastModifiedAllowed(modified, now, now) {
			t.Fatal("unsafe LastModified accepted", modified)
		}
	}
	if inverseLastModifiedAllowed(cutoff.Add(-time.Second), cutoff.Add(-2*time.Second), now) {
		t.Fatal("post-partition object accepted")
	}
	if inverseLastModifiedAllowed(cutoff.Add(-time.Second), now, cutoff.Add(-2*time.Second)) {
		t.Fatal("future object accepted")
	}
}
