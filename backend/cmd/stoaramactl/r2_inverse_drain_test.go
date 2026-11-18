package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestR2InverseDrainProofScopeAndEvidence(t *testing.T) {
	activation := time.Date(2026, 11, 12, 14, 28, 47, 0, time.UTC)
	deadline := activation.Add(7*24*time.Hour + 2*time.Minute)
	path := filepath.Join(t.TempDir(), "reviewed-evidence.json")
	if err := os.WriteFile(path, []byte(`{"synthetic":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	digest, err := inverseFileSHA(path)
	if err != nil {
		t.Fatal(err)
	}
	ref := inverseEvidenceReference{Path: path, SHA256: digest}
	o := inverseApplyOptions{ManifestSHA: r2tSHA, SummarySHA: r2tSHA}
	valid := func() inverseDrainEvidence {
		return inverseDrainEvidence{Proof: inverseNon28KeyPolicyProof, FenceActivatedAt: activation, DrainNotBefore: deadline, ObservedAt: deadline.Add(time.Minute),
			KeyPolicy: &inverseKeyPolicyEvidence{PartitionSHA256: r2tSHA, SummarySHA256: r2tSHA, IssuanceBoundEvidence: ref, OperatorSigningEvidence: ref, OperatorSigningStoppedAt: activation, OperatorSigningObservedThrough: deadline.Add(time.Minute), OperatorLastCapabilityExpiresAt: activation.Add(2 * time.Hour), ResidualAcceptance: inverseApprovedKeyResidual}}
	}
	if err := valid().validateProof(o); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*inverseDrainEvidence, *inverseApplyOptions)
	}{
		{"B2", func(_ *inverseDrainEvidence, o *inverseApplyOptions) { o.Lane = inverseNASKeep28 }},
		{"missing route", func(e *inverseDrainEvidence, _ *inverseApplyOptions) { e.Proof = "" }},
		{"unknown route", func(e *inverseDrainEvidence, _ *inverseApplyOptions) { e.Proof = "quiet_window" }},
		{"missing policy", func(e *inverseDrainEvidence, _ *inverseApplyOptions) { e.KeyPolicy = nil }},
		{"mixed proof", func(e *inverseDrainEvidence, _ *inverseApplyOptions) { e.AcceptedPUTCompletion = "expired" }},
		{"changed partition", func(_ *inverseDrainEvidence, o *inverseApplyOptions) { o.ManifestSHA = "changed" }},
		{"changed summary", func(_ *inverseDrainEvidence, o *inverseApplyOptions) { o.SummarySHA = "changed" }},
		{"missing issuance", func(e *inverseDrainEvidence, _ *inverseApplyOptions) {
			e.KeyPolicy.IssuanceBoundEvidence = inverseEvidenceReference{}
		}},
		{"missing operator", func(e *inverseDrainEvidence, _ *inverseApplyOptions) {
			e.KeyPolicy.OperatorSigningEvidence = inverseEvidenceReference{}
		}},
		{"unaccepted residual", func(e *inverseDrainEvidence, _ *inverseApplyOptions) { e.KeyPolicy.ResidualAcceptance = "" }},
		{"wrong evidence digest", func(e *inverseDrainEvidence, _ *inverseApplyOptions) {
			e.KeyPolicy.IssuanceBoundEvidence.SHA256 = r2tSHA
		}},
		{"unavailable evidence", func(e *inverseDrainEvidence, _ *inverseApplyOptions) {
			e.KeyPolicy.OperatorSigningEvidence.Path = path + ".missing"
		}},
		{"stale operator observation", func(e *inverseDrainEvidence, _ *inverseApplyOptions) {
			e.KeyPolicy.OperatorSigningObservedThrough = e.ObservedAt.Add(-time.Second)
		}},
		{"late signer", func(e *inverseDrainEvidence, _ *inverseApplyOptions) {
			e.KeyPolicy.OperatorSigningStoppedAt = deadline.Add(-time.Hour)
		}},
		{"live operator capability", func(e *inverseDrainEvidence, _ *inverseApplyOptions) {
			e.KeyPolicy.OperatorLastCapabilityExpiresAt = deadline.Add(time.Second)
		}},
		{"missing signer time", func(e *inverseDrainEvidence, _ *inverseApplyOptions) {
			e.KeyPolicy.OperatorSigningStoppedAt = time.Time{}
		}},
		{"missing operator expiry", func(e *inverseDrainEvidence, _ *inverseApplyOptions) {
			e.KeyPolicy.OperatorLastCapabilityExpiresAt = time.Time{}
		}},
		{"shortened fence", func(e *inverseDrainEvidence, _ *inverseApplyOptions) { e.DrainNotBefore = deadline.Add(-time.Minute) }},
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
	completion := inverseDrainEvidence{Proof: inversePUTCompletionProof, AcceptedPUTCompletion: "terminal provider evidence"}
	for _, lane := range []inverseLane{inverseOutsideKeep28, inverseNASKeep28} {
		opts := o
		opts.Lane = lane
		if err := completion.validateProof(opts); err != nil {
			t.Fatal(err)
		}
	}
	completion.KeyPolicy = valid().KeyPolicy
	if err := completion.validateProof(o); err == nil {
		t.Fatal("mixed completion route accepted")
	}
}
