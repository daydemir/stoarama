package joinedrecording

import (
	"errors"
	"testing"
	"time"
)

func TestSealHourRequestRejectsInvalidWorkerEvidenceBeforeCallback(t *testing.T) {
	source := testSource(1, time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC))
	req := testRequest([]SourceClip{source})
	ledger, err := testLedger(req, req.LocalDate)
	if err != nil {
		t.Fatal(err)
	}
	req.AllocationLedgerSHA = ledger.LedgerSHA256
	verification := passingVerification()
	verification.Status = "failed"
	built := []BuiltOutput{{SizeBytes: source.Object.SizeBytes, SHA256: source.Object.SHA256, SourceCount: 1, Verification: verification}}
	req.BuiltArtifacts = []BuiltArtifactIdentity{{SizeBytes: built[0].SizeBytes, SHA256: built[0].SHA256, MediaToolIdentity: req.MediaTool.IdentitySHA256}}
	plan, err := BuildPlan(req)
	if err != nil {
		t.Fatal(err)
	}

	_, err = sealHourRequest(PreflightHourClaim{HourID: plan.HourID}, plan, built, nil)
	if !errors.Is(err, ErrPreflightSealRequestInvalid) {
		t.Fatalf("invalid seal request error=%v", err)
	}
}
