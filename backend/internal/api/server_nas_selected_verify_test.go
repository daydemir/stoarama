package api

import (
	"strings"
	"testing"
)

func TestValidateNASSelectedVerifyResult(t *testing.T) {
	valid := nasSelectedVerifyResultRequest{Attempt: 1, PlanSHA256: strings.Repeat("a", 64), Outcome: "match", DurationMS: 2}
	if err := validateNASSelectedVerifyResult(valid); err != nil {
		t.Fatalf("valid: %v", err)
	}
	for name, mutate := range map[string]func(*nasSelectedVerifyResultRequest){
		"attempt":  func(r *nasSelectedVerifyResultRequest) { r.Attempt = 0 },
		"plan":     func(r *nasSelectedVerifyResultRequest) { r.PlanSHA256 = "bad" },
		"outcome":  func(r *nasSelectedVerifyResultRequest) { r.Outcome = "copied" },
		"duration": func(r *nasSelectedVerifyResultRequest) { r.DurationMS = -1 },
		"error":    func(r *nasSelectedVerifyResultRequest) { r.Error = strings.Repeat("x", 501) },
	} {
		req := valid
		mutate(&req)
		if err := validateNASSelectedVerifyResult(req); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}
