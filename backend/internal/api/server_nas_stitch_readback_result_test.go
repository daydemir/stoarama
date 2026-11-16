package api

import (
	"encoding/json"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/stitchproof"
)

func TestNASStitchReadbackResultProjectsWithoutChangingFullNativeBytes(t *testing.T) {
	plan, result := stitchUnitFixture()
	plan.Clips, plan.Eligible, plan.Joins, plan.Frozen = plan.Clips[:2], plan.Eligible[:2], plan.Joins[:1], plan.Frozen[:1]
	result.Seams, result.LeftOut, result.Repairs, result.Token = result.Seams[:1], []int64{}, []nasStitchRepair{}, ""
	receipt, _, _ := syntheticNASStitchReadback(t)
	receipt.NativePlanSHA = result.PlanSHA
	o := result.Outputs[0]
	receipt.Outputs = []stitchproof.Media{{RelativePath: o.Path, SHA256: o.SHA, SizeBytes: o.Size}}
	base, _ := json.Marshal(result)
	var fields map[string]any
	json.Unmarshal(base, &fields)
	delete(fields, "lease_token")
	fields["outputs"].([]any)[0].(map[string]any)["media_seconds"] = 120.0
	raw, _ := json.Marshal(fields)
	before := string(raw)
	if err := validateNASStitchReadbackResult(raw, plan, receipt); err != nil {
		t.Fatal(err)
	}
	if string(raw) != before {
		t.Fatal("native registration bytes rewritten by projection")
	}
	for _, mutate := range []func(map[string]any){
		func(v map[string]any) { delete(v, "error") },
		func(v map[string]any) { v["lease_token"] = "" },
		func(v map[string]any) { v["outputs"].([]any)[0].(map[string]any)["full_decode"] = false },
		func(v map[string]any) { v["outputs"].([]any)[0].(map[string]any)["media_seconds"] = 0 },
		func(v map[string]any) { delete(v["outputs"].([]any)[0].(map[string]any), "media_seconds") },
		func(v map[string]any) { v["outputs"].([]any)[0].(map[string]any)["unknown"] = true },
		func(v map[string]any) { v["outputs"].([]any)[0].(map[string]any)["relative_path"] = "../raw.mp4" },
		func(v map[string]any) { v["left_out"] = []any{1} },
	} {
		var changed map[string]any
		json.Unmarshal(raw, &changed)
		mutate(changed)
		body, _ := json.Marshal(changed)
		if validateNASStitchReadbackResult(body, plan, receipt) == nil {
			t.Fatal("incomplete, changed or non-joined result accepted")
		}
	}
}
