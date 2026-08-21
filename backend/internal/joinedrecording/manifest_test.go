package joinedrecording

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/stitchcert"
)

func TestBatchIndexV1Golden(t *testing.T) {
	index, ledgers := testBatchIndex(t)
	_, canonical, _, err := BuildBatchIndex(index, testLedgerResolver(ledgers))
	if err != nil {
		t.Fatal(err)
	}
	assertGolden(t, "testdata/batch_index_v1.golden.json", canonical)
}

func TestFrozenDenominatorIsCanonicalOrderedLedgerProjection(t *testing.T) {
	index, ledgers := testBatchIndex(t)
	want := index.FrozenDenominatorSHA256
	if want != "fcd56878c3c1b26e5b99995ffc2ad4c31ad9f8fef1ca3289fb2701d044e419d9" {
		t.Fatalf("canonical denominator fixture changed: %s", want)
	}
	if got, err := ComputeFrozenDenominatorSHA256(index.AllocationLedgers); err != nil || got != want {
		t.Fatalf("denominator projection=%s err=%v, want %s", got, err, want)
	}

	mutations := []func([]AllocationLedgerRef){
		func(ledgers []AllocationLedgerRef) { ledgers[0].SourceClaimSHA256 = strings.Repeat("9", 64) },
		func(ledgers []AllocationLedgerRef) { ledgers[0].SourceCount++ },
		func(ledgers []AllocationLedgerRef) { ledgers[0].SourceBytes++ },
		func(ledgers []AllocationLedgerRef) { ledgers[0], ledgers[1] = ledgers[1], ledgers[0] },
	}
	for i, mutate := range mutations {
		ledgers := append([]AllocationLedgerRef(nil), index.AllocationLedgers...)
		mutate(ledgers)
		got, err := ComputeFrozenDenominatorSHA256(ledgers)
		if err == nil && got == want {
			t.Fatalf("denominator mutation %d preserved digest", i+1)
		}
	}

	index.FrozenDenominatorSHA256 = strings.Repeat("a", 64)
	index.BatchGenerationSHA256, _ = ComputeBatchGenerationSHA256(index)
	if _, _, _, err := BuildBatchIndex(index, testLedgerResolver(ledgers)); err == nil {
		t.Fatal("batch index accepted caller-supplied denominator digest")
	}

	attack := index
	attack.AllocationLedgers = append([]AllocationLedgerRef(nil), index.AllocationLedgers...)
	attack.AllocationLedgers[0].SourceClaimSHA256 = strings.Repeat("9", 64)
	attack.FrozenDenominatorSHA256, _ = ComputeFrozenDenominatorSHA256(attack.AllocationLedgers)
	attack.BatchGenerationSHA256, _ = ComputeBatchGenerationSHA256(attack)
	if _, _, _, err := BuildBatchIndex(attack, testLedgerResolver(ledgers)); err == nil {
		t.Fatal("batch index accepted recomputed hashes over a mutated ledger reference")
	}
}

func testBatchIndex(t *testing.T) (BatchIndex, []StreamDayAllocation) {
	t.Helper()
	const batchID = "goodplus-20260821-generation-1"
	start := time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC)
	baseRequest := testRequest([]SourceClip{testSource(1, start)})
	hours := make([]BatchIndexHour, 0, 14*12)
	ledgerRefs := make([]AllocationLedgerRef, 0, 14)
	ledgers := make([]StreamDayAllocation, 0, 14)
	for day := 0; day < 14; day++ {
		localDate := time.Date(2026, time.May, 4+day, 0, 0, 0, 0, time.UTC).Format("2006-01-02")
		ledgerRequest := baseRequest
		ledgerRequest.LocalDate = localDate
		if day > 0 {
			ledgerRequest.Sources = nil
		}
		ledger, err := testLedger(ledgerRequest, localDate)
		if err != nil {
			t.Fatal(err)
		}
		ledgerRef, err := BuildAllocationLedgerRef(int64(90+day), ledger)
		if err != nil {
			t.Fatal(err)
		}
		ledgers = append(ledgers, ledger)
		ledgerRefs = append(ledgerRefs, ledgerRef)
		for i := 0; i < 12; i++ {
			hourID := ledgerRef.ScheduledHourIDs[i]
			relative := "coverage/hours/" + hourID + ".json"
			status, sourceCount, sourceBytes, mediaCount := HourStatusGapOnly, 0, int64(0), 0
			if day == 0 && i == 0 {
				status, sourceCount, sourceBytes, mediaCount = HourStatusMedia, 1, 10, 1
			}
			hours = append(hours, BatchIndexHour{HourManifestArtifactID: int64(1000 + day*12 + i), HourID: hourID, RecordingID: 377, LocalDate: localDate, DeliveryHour: i + 1, Status: status, RelativePath: relative, ObjectKey: "joined/" + batchID + "/" + relative, SizeBytes: 1000, SHA256: strings.Repeat("b", 64), SourceCount: sourceCount, SourceBytes: sourceBytes, MediaArtifactCount: mediaCount})
		}
	}
	mediaTool, _ := SealMediaToolEvidence(MediaToolEvidence{FFmpegVersion: "ffmpeg pinned", FFmpegSHA256: strings.Repeat("e", 64), FFprobeVersion: "ffprobe pinned", FFprobeSHA256: strings.Repeat("f", 64)})
	metadata := baseRequest.Metadata
	cutoff := time.Date(2026, time.August, 21, 6, 59, 7, 0, time.UTC)
	index := BatchIndex{SchemaVersion: 1, PolicyVersion: PlanPolicyVersion, AllocationSchemaVersion: 1, HourManifestSchemaVersion: 1, BatchID: batchID, Generation: 1, FrozenAt: cutoff, RecordingIDs: []int64{377}, RecordingIDSHA256: recordingIDsSHA([]int64{377}), FrozenRecordings: []FrozenRecording{{RecordingID: 377, PriorityOrdinal: 1, EligibilityTier: "good+", EligibilityCutoff: cutoff, CompletedAt: cutoff.Add(-time.Hour), Timezone: "UTC", FolderName: "01_Europe_Italy_Bevagna_Piazza_Silvestri", NamingMetadata: metadata}}, MediaTool: mediaTool, ExpectedLedgerCount: 14, ScheduledHourCount: 168, SourceClipCount: 1, SourceBytes: 10, FinalMediaCount: 1, AllocationLedgers: ledgerRefs, Hours: hours}
	index.FrozenDenominatorSHA256, _ = ComputeFrozenDenominatorSHA256(index.AllocationLedgers)
	index.BatchGenerationSHA256, _ = ComputeBatchGenerationSHA256(index)
	return index, ledgers
}

func testLedgerResolver(ledgers []StreamDayAllocation) AllocationLedgerResolver {
	return func(ref AllocationLedgerRef) (StreamDayAllocation, error) {
		for _, ledger := range ledgers {
			if ledger.RecordingID == ref.RecordingID && ledger.LocalDate == ref.LocalDate {
				return ledger, nil
			}
		}
		return StreamDayAllocation{}, os.ErrNotExist
	}
}

func TestAllocationLedgerV1Golden(t *testing.T) {
	start := time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC)
	req := testRequest([]SourceClip{testSource(1, start), testSource(2, start.Add(time.Minute))})
	draft, err := AllocateStreamDay(req)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := SealStreamDayAllocation(draft)
	if err != nil {
		t.Fatal(err)
	}
	canonical, sha, err := CanonicalAllocationLedgerArtifact(ledger)
	if err != nil || !lowerHex64(sha) {
		t.Fatal(err)
	}
	assertGolden(t, "testdata/allocation_ledger_v1.golden.json", canonical)
}

func TestHourManifestV1Golden(t *testing.T) {
	plan := oneOutputPlan(t)
	built := []BuiltOutput{{SizeBytes: plan.Outputs[0].ExpectedSize, SHA256: plan.Outputs[0].ExpectedSHA, SourceCount: 1, Verification: passingVerification(), SplitEvidence: []MaximalityEvidence{}}}
	allocation, ledger := testAllocation(plan)
	manifest, canonical, sha, err := BuildHourManifest(HourManifestInput{Plan: plan, Allocation: allocation, AllocationLedger: ledger, MediaArtifactIDs: []int64{88}, Built: built})
	if err != nil {
		t.Fatal(err)
	}
	assertGolden(t, "testdata/hour_manifest_v1.golden.json", canonical)
	if manifest.Status != HourStatusMedia || !lowerHex64(sha) {
		t.Fatalf("invalid canonical media manifest: %+v", manifest)
	}
}

func TestHourManifestAllocationUsesCanonicalLedgerProjection(t *testing.T) {
	plan := oneOutputPlan(t)
	allocation, ledger := testAllocation(plan)
	want, err := BuildHourManifestAllocation(allocation.ArtifactID, plan, ledger)
	if err != nil || !sameCanonical([]HourManifestAllocation{allocation}, []HourManifestAllocation{want}) {
		t.Fatalf("canonical allocation=%+v err=%v", want, err)
	}
	if err := ValidateHourManifestAllocation(allocation, plan, ledger); err != nil {
		t.Fatal(err)
	}
	allocation.HourSourceSHA256 = strings.Repeat("0", 64)
	if err := ValidateHourManifestAllocation(allocation, plan, ledger); err == nil {
		t.Fatal("mutated hour allocation validated")
	}
}

func TestHourManifestGapAndQuarantineAreDistinctTerminalCoverage(t *testing.T) {
	start := time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC)
	req := testRequest([]SourceClip{testSource(1, start)})
	sources := append([]SourceClip(nil), req.Sources...)
	req.Sources = nil
	gapLedger, err := testLedger(req, "2026-05-04")
	if err != nil {
		t.Fatal(err)
	}
	req.AllocationLedgerSHA = gapLedger.LedgerSHA256
	gap, err := BuildGapOnlyHourPlan(req, "2026-05-04", 1, "no_source_clips")
	if err != nil {
		t.Fatal(err)
	}
	gapAllocation, gapLedger := testAllocation(gap)
	gapManifest, gapJSON, _, err := BuildHourManifest(HourManifestInput{Plan: gap, Allocation: gapAllocation, AllocationLedger: gapLedger})
	if err != nil || gapManifest.Status != HourStatusGapOnly || gapManifest.QuarantineReasonCode != "" || len(gapManifest.Media) != 0 || len(gapManifest.Sources) != 0 {
		t.Fatalf("invalid gap-only manifest: %+v %v", gapManifest, err)
	}
	assertGolden(t, "testdata/hour_manifest_gap_only_v1.golden.json", gapJSON)
	req.Sources = sources
	quarantineLedger, err := testLedger(req, "2026-05-04")
	if err != nil {
		t.Fatal(err)
	}
	req.AllocationLedgerSHA = quarantineLedger.LedgerSHA256
	quarantine, err := BuildQuarantineOnlyHourPlan(req, "2026-05-04", 1, "ambiguous_audio_seam")
	if err != nil {
		t.Fatal(err)
	}
	evidence := []QuarantineEvidence{testQuarantineEvidence(quarantine, []int64{1}, "ambiguous_audio_seam")}
	quarantineAllocation, quarantineLedgerRef := testAllocation(quarantine)
	quarantineManifest, quarantineJSON, _, err := BuildHourManifest(HourManifestInput{Plan: quarantine, Allocation: quarantineAllocation, AllocationLedger: quarantineLedgerRef, QuarantineEvidence: evidence})
	if err != nil || quarantineManifest.Status != HourStatusQuarantineOnly || quarantineManifest.QuarantineReasonCode != "ambiguous_audio_seam" || len(quarantineManifest.Media) != 0 || len(quarantineManifest.Sources) != 1 {
		t.Fatalf("invalid quarantine-only manifest: %+v %v", quarantineManifest, err)
	}
	assertGolden(t, "testdata/hour_manifest_quarantine_only_v1.golden.json", quarantineJSON)
}

func TestHourManifestMediaCanAccountForQuarantinedSource(t *testing.T) {
	start := time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC)
	included, quarantined := testSource(1, start), testSource(2, start.Add(time.Minute))
	req := testRequest([]SourceClip{included})
	req.QuarantinedSources = []SourceClip{quarantined}
	plan, err := buildTestPlan(req)
	if err != nil {
		t.Fatal(err)
	}
	built := []BuiltOutput{{SizeBytes: plan.Outputs[0].ExpectedSize, SHA256: plan.Outputs[0].ExpectedSHA, SourceCount: 1, Verification: passingVerification()}}
	evidence := []QuarantineEvidence{testQuarantineEvidence(plan, []int64{2}, "corrupt_source_media")}
	allocation, allocationLedger := testAllocation(plan)
	manifest, canonical, _, err := BuildHourManifest(HourManifestInput{Plan: plan, Allocation: allocation, AllocationLedger: allocationLedger, MediaArtifactIDs: []int64{88}, Built: built, QuarantineEvidence: evidence})
	if err != nil {
		t.Fatal(err)
	}
	assertGolden(t, "testdata/hour_manifest_mixed_v1.golden.json", canonical)
	if manifest.Status != HourStatusMedia || len(manifest.Media) != 1 || len(manifest.SourceDispositions) != 2 || manifest.SourceDispositions[1].Disposition != "quarantined" {
		t.Fatalf("mixed media/quarantine coverage differs: %+v", manifest)
	}
}

func TestHourManifestRejectsMutatedLedgerAndDowngradedEvidence(t *testing.T) {
	plan := oneOutputPlan(t)
	built := []BuiltOutput{{SizeBytes: plan.Outputs[0].ExpectedSize, SHA256: plan.Outputs[0].ExpectedSHA, SourceCount: 1, Verification: passingVerification()}}
	allocation, ledger := testAllocation(plan)
	unsafePlan := plan
	unsafePlan.Sources = append([]SourceClip(nil), plan.Sources...)
	unsafePlan.Sources[0].Endpoint = "https://cap.test/?secret=value"
	unsafePlan.Outputs = append([]OutputPlan(nil), plan.Outputs...)
	unsafePlan.Outputs[0].Sources = append([]SourceClip(nil), plan.Outputs[0].Sources...)
	unsafePlan.Outputs[0].Sources[0].Endpoint = unsafePlan.Sources[0].Endpoint
	if _, _, _, err := BuildHourManifest(HourManifestInput{Plan: unsafePlan, Allocation: allocation, AllocationLedger: ledger, MediaArtifactIDs: []int64{88}, Built: built}); err == nil {
		t.Fatal("manifest accepted a source endpoint containing query credentials")
	}

	mutatedLedger := ledger
	mutatedLedger.QualificationSHA = strings.Repeat("9", 64)
	logicalLedger := mutatedLedger
	logicalLedger.LedgerSHA256 = ""
	mutatedLedger.LedgerSHA256, _, _ = stitchcert.CanonicalSHA(logicalLedger)
	mutatedBytes, mutatedSHA, err := CanonicalAllocationLedgerArtifact(mutatedLedger)
	if err != nil {
		t.Fatal(err)
	}
	mutatedAllocation := allocation
	mutatedAllocation.SizeBytes = int64(len(mutatedBytes))
	mutatedAllocation.SHA256 = mutatedSHA
	mutatedAllocation.LedgerSHA256 = mutatedLedger.LedgerSHA256
	if _, _, _, err := BuildHourManifest(HourManifestInput{Plan: plan, Allocation: mutatedAllocation, AllocationLedger: mutatedLedger, MediaArtifactIDs: []int64{88}, Built: built}); err == nil {
		t.Fatal("self-consistent allocation ledger for another hour claim was accepted")
	}

	start := time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC)
	included, quarantined := testSource(1, start), testSource(2, start.Add(time.Minute))
	req := testRequest([]SourceClip{included})
	req.QuarantinedSources = []SourceClip{quarantined}
	mixed, err := buildTestPlan(req)
	if err != nil {
		t.Fatal(err)
	}
	mixedBuilt := []BuiltOutput{{SizeBytes: mixed.Outputs[0].ExpectedSize, SHA256: mixed.Outputs[0].ExpectedSHA, SourceCount: 1, Verification: passingVerification()}}
	evidence := testQuarantineEvidence(mixed, []int64{2}, "corrupt_source_media")
	evidence.AttemptCount = 1
	proof := struct {
		SourceClaimSHA256 string `json:"source_claim_sha256"`
		ReasonCode        string `json:"reason_code"`
		FailureSHA256     string `json:"failure_sha256"`
		PolicyVersion     string `json:"policy_version"`
		MediaToolIdentity string `json:"media_tool_identity"`
		RepeatCount       int    `json:"repeat_count"`
	}{evidence.SourceClaimSHA256, evidence.ReasonCode, evidence.FailureSHA256, evidence.PolicyVersion, evidence.MediaToolIdentity, evidence.AttemptCount}
	evidence.EvidenceSHA256, _, _ = stitchcert.CanonicalSHA(proof)
	mixedAllocation, mixedLedger := testAllocation(mixed)
	if _, _, _, err := BuildHourManifest(HourManifestInput{Plan: mixed, Allocation: mixedAllocation, AllocationLedger: mixedLedger, MediaArtifactIDs: []int64{88}, Built: mixedBuilt, QuarantineEvidence: []QuarantineEvidence{evidence}}); err == nil {
		t.Fatal("self-consistent one-attempt quarantine evidence was accepted")
	}
}

func testQuarantineEvidence(plan BatchPlan, ids []int64, reason string) QuarantineEvidence {
	sources, _ := sourceSubsetByIDs(plan.Sources, ids)
	sourceClaim, _ := candidateSourceClaimSHA(sources)
	facts := json.RawMessage(`{"category":"fixture_failure"}`)
	failureSHA, _, _ := stitchcert.CanonicalSHA(facts)
	proof := struct {
		SourceClaimSHA256 string `json:"source_claim_sha256"`
		ReasonCode        string `json:"reason_code"`
		FailureSHA256     string `json:"failure_sha256"`
		PolicyVersion     string `json:"policy_version"`
		MediaToolIdentity string `json:"media_tool_identity"`
		RepeatCount       int    `json:"repeat_count"`
	}{sourceClaim, reason, failureSHA, PlanPolicyVersion, plan.MediaTool.IdentitySHA256, 2}
	evidenceSHA, _, _ := stitchcert.CanonicalSHA(proof)
	return QuarantineEvidence{ReasonCode: reason, SourceClipIDs: ids, SourceClaimSHA256: sourceClaim, PolicyVersion: PlanPolicyVersion, NormalizedFacts: facts, FailureSHA256: failureSHA, EvidenceSHA256: evidenceSHA, AttemptCount: 2, MediaToolIdentity: plan.MediaTool.IdentitySHA256}
}

func assertGolden(t *testing.T, name string, canonical []byte) {
	t.Helper()
	if os.Getenv("UPDATE_JOINED_GOLDENS") == "1" {
		if err := os.WriteFile(name, append(append([]byte{}, canonical...), '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(name)
	if err != nil || !bytes.Equal(canonical, bytes.TrimSpace(want)) {
		t.Fatalf("canonical fixture changed\n%s", canonical)
	}
}
