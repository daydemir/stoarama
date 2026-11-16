package api

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNASStitchNativeV3PreservesPolicyInputsAndPriorNamespaces(t *testing.T) {
	p := proofPlanFixture()
	old, err := boundedNASStitchProof(p, nasStitchProofSourceSHA)
	if err != nil {
		t.Fatal(err)
	}
	const run = "11111111-1111-4111-8111-111111111111"
	current, err := boundedNASStitchNativeV3(p, nasStitchProofSourceSHA, run)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(current.Plan.OutputFolder, "_V85r_proof_keep28_v3/") || !strings.HasSuffix(current.Plan.OutputFolder, "/run-"+run) || current.PlanSHA == old.PlanSHA {
		t.Fatal("fresh attempt did not obtain its own immutable namespace/hash")
	}
	restored := current.Plan
	restored.OutputFolder = old.Plan.OutputFolder
	a, _ := json.Marshal(restored)
	b, _ := json.Marshal(old.Plan)
	if string(a) != string(b) || current.Kind != old.Kind || current.ID != old.ID || current.SourcePlanSHA != old.SourcePlanSHA {
		t.Fatal("namespace generation changed source/policy/native contract")
	}
	next, err := boundedNASStitchNativeV3(proofPlanFixture(), nasStitchProofSourceSHA, "22222222-2222-4222-8222-222222222222")
	if err != nil || next.PlanSHA == current.PlanSHA || next.Plan.OutputFolder == current.Plan.OutputFolder {
		t.Fatal("distinct runs reused a publication directory")
	}
	for _, invalid := range []string{"", "unknown", "../run", "00000000-0000-0000-0000-000000000000"} {
		if _, err := boundedNASStitchNativeV3(proofPlanFixture(), nasStitchProofSourceSHA, invalid); err == nil {
			t.Fatal("invalid run directory admitted")
		}
	}
	p = proofPlanFixture()
	p.Clips[1].SHA256 = strings.Repeat("0", 64)
	if _, err := boundedNASStitchNativeV3(p, nasStitchProofSourceSHA, run); err == nil {
		t.Fatal("fresh namespace substituted changed source footage")
	}
}
