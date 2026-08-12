package mitscltop50

import (
	"encoding/json"
	"os"
	"regexp"
	"testing"
)

type sourceEvidence struct {
	SchemaVersion int `json:"schema_version"`
	ProviderCaps  []struct {
		FailureDomain string `json:"failure_domain"`
		Maximum       int    `json:"max_probationary_scenes"`
	} `json:"provider_caps"`
	Candidates []struct {
		Rank                 int    `json:"rank"`
		StreamID             int64  `json:"stream_id"`
		Decision             string `json:"decision"`
		Provider             string `json:"provider"`
		FailureDomain        string `json:"failure_domain"`
		AuthoritativeBinding struct {
			Kind             string `json:"kind"`
			ProviderSourceID string `json:"provider_source_id"`
			Confidence       string `json:"confidence"`
		} `json:"authoritative_binding"`
		ReasonCodes []string `json:"reason_codes"`
		RiskCodes   []string `json:"risk_codes"`
	} `json:"candidates"`
}

func TestSourceEvidenceContract(t *testing.T) {
	raw, err := os.ReadFile("source-evidence.json")
	if err != nil {
		t.Fatal(err)
	}
	if regexp.MustCompile(`(?i)(\.m3u8|X-Amz-|wowzatoken|bearer\s|access[_-]?key|secret[_-]?key)`).Match(raw) {
		t.Fatal("source evidence contains a live media URL or credential-shaped value")
	}
	var catalog sourceEvidence
	if err := json.Unmarshal(raw, &catalog); err != nil {
		t.Fatal(err)
	}
	if catalog.SchemaVersion != 1 || len(catalog.Candidates) != 15 {
		t.Fatalf("schema=%d candidates=%d", catalog.SchemaVersion, len(catalog.Candidates))
	}
	caps := map[string]int{}
	for _, cap := range catalog.ProviderCaps {
		caps[cap.FailureDomain] = cap.Maximum
	}
	seen := map[int64]bool{}
	selected := []int64{}
	counts := map[string]int{}
	for i, candidate := range catalog.Candidates {
		if candidate.Rank != i+1 || candidate.StreamID <= 0 || seen[candidate.StreamID] {
			t.Fatalf("candidate %d has invalid rank/id", i)
		}
		seen[candidate.StreamID] = true
		if candidate.Provider == "" || candidate.FailureDomain == "" || candidate.AuthoritativeBinding.Kind == "" || candidate.AuthoritativeBinding.ProviderSourceID == "" || candidate.AuthoritativeBinding.Confidence == "" || len(candidate.ReasonCodes) == 0 || len(candidate.RiskCodes) == 0 {
			t.Fatalf("candidate %d lacks required evidence", candidate.StreamID)
		}
		counts[candidate.FailureDomain]++
		if candidate.Decision == "selected_addition" {
			selected = append(selected, candidate.StreamID)
		}
	}
	wantSelected := []int64{17200, 415, 78, 17237}
	for i := range wantSelected {
		if selected[i] != wantSelected[i] {
			t.Fatalf("selected additions=%v", selected)
		}
	}
	for failureDomain, maximum := range caps {
		if counts[failureDomain] > maximum {
			t.Fatalf("%s candidates=%d cap=%d", failureDomain, counts[failureDomain], maximum)
		}
	}
}
