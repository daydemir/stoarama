package joinedrecording

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/stitchcert"
)

func TestBatchIndexV1Golden(t *testing.T) {
	index, ledgers, manifests := testBatchIndex(t)
	_, canonical, _, err := BuildBatchIndex(index, testLedgerResolver(ledgers), testHourResolver(manifests))
	if err != nil {
		t.Fatal(err)
	}
	assertGolden(t, "testdata/batch_index_v1.golden.json", canonical)
}

func TestFrozenDenominatorIsCanonicalOrderedLedgerProjection(t *testing.T) {
	index, ledgers, manifests := testBatchIndex(t)
	canonicalIndex := testBatchIndexCopy(index)
	want := index.FrozenDenominatorSHA256
	if want != "39a20a79bd66c56fdfaad7113c4b8f4034a4d00fe1e8ec1c81a3a91206f5c3bd" {
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

	denominatorAttack := testBatchIndexCopy(canonicalIndex)
	denominatorAttack.FrozenDenominatorSHA256 = strings.Repeat("a", 64)
	denominatorAttack.BatchGenerationSHA256, _ = ComputeBatchGenerationSHA256(denominatorAttack)
	if _, _, _, err := BuildBatchIndex(denominatorAttack, testLedgerResolver(ledgers), testHourResolver(manifests)); err == nil {
		t.Fatal("batch index accepted caller-supplied denominator digest")
	}

	attack := testBatchIndexCopy(canonicalIndex)
	attack.AllocationLedgers[0].SourceClaimSHA256 = strings.Repeat("9", 64)
	attack.FrozenDenominatorSHA256, _ = ComputeFrozenDenominatorSHA256(attack.AllocationLedgers)
	attack.BatchGenerationSHA256, _ = ComputeBatchGenerationSHA256(attack)
	if _, _, _, err := BuildBatchIndex(attack, testLedgerResolver(ledgers), testHourResolver(manifests)); err == nil {
		t.Fatal("batch index accepted recomputed hashes over a mutated ledger reference")
	}

	hourAttack := testBatchIndexCopy(canonicalIndex)
	hourAttack.Hours[0].SHA256 = strings.Repeat("9", 64)
	hourAttack.BatchGenerationSHA256, _ = ComputeBatchGenerationSHA256(hourAttack)
	if _, _, _, err := BuildBatchIndex(hourAttack, testLedgerResolver(ledgers), testHourResolver(manifests)); err == nil {
		t.Fatal("batch index accepted recomputed hashes over a mutated hour-manifest reference")
	}

	t.Run("same-size source substitution", func(t *testing.T) {
		mutated := append([]HourManifest(nil), manifests...)
		mutated[0] = cloneHourManifest(t, manifests[0])
		mutated[0].Sources[0].Object.Key = "raw/recording-377/clip-1-substituted.mp4"
		claimSHA, _, err := CanonicalSourceClaim(mutated[0].Sources)
		if err != nil {
			t.Fatal(err)
		}
		mutated[0].SourceClaimSHA256 = claimSHA
		mutated[0].Allocation.HourSourceSHA256 = claimSHA
		ref, err := BuildBatchIndexHour(canonicalIndex.Hours[0].HourManifestArtifactID, mutated[0])
		if err != nil {
			t.Fatalf("mutated manifest should remain independently canonical: %v", err)
		}
		mutatedIndex := testBatchIndexCopy(canonicalIndex)
		mutatedIndex.Hours[0] = ref
		if _, _, _, err := BuildBatchIndex(mutatedIndex, testLedgerResolver(ledgers), testHourResolver(mutated)); err == nil {
			t.Fatal("batch index accepted equal-count, equal-byte source substitution")
		}
	})

	for name, mutate := range map[string]func(*HourManifest){
		"logical ledger substitution":  func(manifest *HourManifest) { manifest.Allocation.LedgerSHA256 = strings.Repeat("9", 64) },
		"ledger artifact substitution": func(manifest *HourManifest) { manifest.Allocation.ArtifactID++ },
	} {
		t.Run(name, func(t *testing.T) {
			mutated := append([]HourManifest(nil), manifests...)
			mutated[0] = cloneHourManifest(t, manifests[0])
			mutate(&mutated[0])
			ref, err := BuildBatchIndexHour(canonicalIndex.Hours[0].HourManifestArtifactID, mutated[0])
			if err != nil {
				t.Fatalf("mutated manifest should remain independently canonical: %v", err)
			}
			mutatedIndex := testBatchIndexCopy(canonicalIndex)
			mutatedIndex.Hours[0] = ref
			if _, _, _, err := BuildBatchIndex(mutatedIndex, testLedgerResolver(ledgers), testHourResolver(mutated)); err == nil {
				t.Fatal("batch index accepted an hour manifest bound to another allocation ledger")
			}
		})
	}

	for name, mutate := range map[string]func(*HourManifest){
		"invalid timezone":  func(manifest *HourManifest) { manifest.Timezone = "Not/A_Zone" },
		"wrong UTC offset":  func(manifest *HourManifest) { manifest.Media[0].UTCOffsetSeconds++ },
		"wrong part number": func(manifest *HourManifest) { manifest.Media[0].Part++ },
		"invalid maximality fact": func(manifest *HourManifest) {
			manifest.Media[0].MaximalityEvidence = []MaximalityEvidence{{ReasonCode: "deterministic_media_split"}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			mutated := cloneHourManifest(t, manifests[0])
			mutate(&mutated)
			if _, err := BuildBatchIndexHour(canonicalIndex.Hours[0].HourManifestArtifactID, mutated); err == nil {
				t.Fatal("invalid canonical hour manifest produced a batch-index reference")
			}
		})
	}

	for name, mutate := range map[string]func(*HourManifest){
		"delivery path substitution": func(manifest *HourManifest) { manifest.Media[0].RelativePath = "other/place.mp4" },
		"media tool substitution": func(manifest *HourManifest) {
			tool, err := SealMediaToolEvidence(MediaToolEvidence{FFmpegVersion: "other ffmpeg", FFmpegSHA256: strings.Repeat("1", 64), FFprobeVersion: "other ffprobe", FFprobeSHA256: strings.Repeat("2", 64)})
			if err != nil {
				t.Fatal(err)
			}
			manifest.MediaTool = tool
			manifest.Media[0].MediaToolIdentity = tool.IdentitySHA256
		},
	} {
		t.Run(name, func(t *testing.T) {
			mutated := append([]HourManifest(nil), manifests...)
			mutated[0] = cloneHourManifest(t, manifests[0])
			mutate(&mutated[0])
			ref, err := BuildBatchIndexHour(canonicalIndex.Hours[0].HourManifestArtifactID, mutated[0])
			if err != nil {
				t.Fatalf("mutated manifest should remain independently canonical: %v", err)
			}
			mutatedIndex := testBatchIndexCopy(canonicalIndex)
			mutatedIndex.Hours[0] = ref
			if _, _, _, err := BuildBatchIndex(mutatedIndex, testLedgerResolver(ledgers), testHourResolver(mutated)); err == nil {
				t.Fatal("batch index accepted a manifest outside frozen naming or tool identity")
			}
		})
	}

	t.Run("media artifact collision", func(t *testing.T) {
		mutated := append([]HourManifest(nil), manifests...)
		mutated[0] = cloneHourManifest(t, manifests[0])
		collisionID := canonicalIndex.Hours[1].HourManifestArtifactID
		mutated[0].Media[0].ArtifactID = collisionID
		mutated[0].SourceDispositions[0].MediaArtifactID = collisionID
		ref, err := BuildBatchIndexHour(canonicalIndex.Hours[0].HourManifestArtifactID, mutated[0])
		if err != nil {
			t.Fatal(err)
		}
		mutatedIndex := testBatchIndexCopy(canonicalIndex)
		mutatedIndex.Hours[0] = ref
		if _, _, _, err := BuildBatchIndex(mutatedIndex, testLedgerResolver(ledgers), testHourResolver(mutated)); err == nil {
			t.Fatal("media artifact ID collided with a canonical hour artifact ID")
		}
	})

	t.Run("cross-day neighbor substitution", func(t *testing.T) {
		mutatedLedgers := append([]StreamDayAllocation(nil), ledgers...)
		mutatedLedgers[1] = cloneAllocationLedger(t, ledgers[1])
		mutatedTime := mutatedLedgers[1].CrossDayBoundaries[0].PreviousPresentationEndUTC.Add(time.Second)
		mutatedLedgers[1].CrossDayBoundaries[0].PreviousPresentationEndUTC = &mutatedTime
		logical := mutatedLedgers[1]
		logical.LedgerSHA256 = ""
		mutatedLedgers[1].LedgerSHA256, _, _ = stitchcert.CanonicalSHA(logical)
		mutatedRef, err := BuildAllocationLedgerRef(canonicalIndex.AllocationLedgers[1].ArtifactID, mutatedLedgers[1])
		if err != nil {
			t.Fatalf("mutated one-sided boundary should remain independently canonical: %v", err)
		}
		mutatedIndex := testBatchIndexCopy(canonicalIndex)
		mutatedIndex.AllocationLedgers[1] = mutatedRef
		mutatedIndex.FrozenDenominatorSHA256, _ = ComputeFrozenDenominatorSHA256(mutatedIndex.AllocationLedgers)
		mutatedIndex.BatchGenerationSHA256, _ = ComputeBatchGenerationSHA256(mutatedIndex)
		if _, _, _, err := BuildBatchIndex(mutatedIndex, testLedgerResolver(mutatedLedgers), testHourResolver(manifests)); err == nil {
			t.Fatal("consecutive ledgers accepted conflicting cross-day neighbor facts")
		}
	})
}

func testBatchIndex(t *testing.T) (BatchIndex, []StreamDayAllocation, []HourManifest) {
	t.Helper()
	const batchID = "goodplus-20260821-generation-1"
	start := time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC)
	baseRequest := testRequest([]SourceClip{testSource(1, start)})
	hours := make([]BatchIndexHour, 0, 14*12)
	ledgerRefs := make([]AllocationLedgerRef, 0, 14)
	ledgers := make([]StreamDayAllocation, 0, 14)
	manifests := make([]HourManifest, 0, 14*12)
	for day := 0; day < 14; day++ {
		localDate := time.Date(2026, time.May, 4+day, 0, 0, 0, 0, time.UTC).Format("2006-01-02")
		ledgerRequest := baseRequest
		ledgerRequest.LocalDate = localDate
		if day > 0 {
			ledgerRequest.Sources = nil
		}
		if day == 1 {
			previous := baseRequest.Sources[0]
			ledgerRequest.PreviousDayLast = &previous
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
			hourRequest := baseRequest
			hourRequest.LocalDate = localDate
			hourRequest.DeliveryHour = i + 1
			hourRequest.AllocationLedgerSHA = ledger.LedgerSHA256
			hourRequest.Sources = nil
			var plan BatchPlan
			var built []BuiltOutput
			var mediaArtifactIDs []int64
			if day == 0 && i == 0 {
				hourRequest.Sources = baseRequest.Sources
				plan, err = buildTestPlan(hourRequest)
				if err == nil {
					built = []BuiltOutput{{SizeBytes: plan.Outputs[0].ExpectedSize, SHA256: plan.Outputs[0].ExpectedSHA, SourceCount: 1, Verification: passingVerification()}}
					mediaArtifactIDs = []int64{5000}
				}
			} else {
				plan, err = BuildGapOnlyHourPlan(hourRequest, localDate, i+1, "no_source_clips")
			}
			if err != nil {
				t.Fatal(err)
			}
			allocation, err := BuildHourManifestAllocation(ledgerRef.ArtifactID, plan, ledger)
			if err != nil {
				t.Fatal(err)
			}
			manifest, _, _, err := BuildHourManifest(HourManifestInput{Plan: plan, Allocation: allocation, AllocationLedger: ledger, MediaArtifactIDs: mediaArtifactIDs, Built: built})
			if err != nil {
				t.Fatal(err)
			}
			hourRef, err := BuildBatchIndexHour(int64(1000+day*12+i), manifest)
			if err != nil {
				t.Fatal(err)
			}
			manifests = append(manifests, manifest)
			hours = append(hours, hourRef)
		}
	}
	mediaTool, _ := SealMediaToolEvidence(MediaToolEvidence{FFmpegVersion: "ffmpeg pinned", FFmpegSHA256: strings.Repeat("e", 64), FFprobeVersion: "ffprobe pinned", FFprobeSHA256: strings.Repeat("f", 64)})
	metadata := baseRequest.Metadata
	cutoff := time.Date(2026, time.August, 21, 6, 59, 7, 0, time.UTC)
	index := BatchIndex{SchemaVersion: 1, PolicyVersion: PlanPolicyVersion, AllocationSchemaVersion: 1, HourManifestSchemaVersion: 1, BatchID: batchID, Generation: 1, FrozenAt: cutoff, RecordingIDs: []int64{377}, RecordingIDSHA256: recordingIDsSHA([]int64{377}), FrozenRecordings: []FrozenRecording{{RecordingID: 377, PriorityOrdinal: 1, EligibilityTier: "good+", EligibilityCutoff: cutoff, CompletedAt: cutoff.Add(-time.Hour), Timezone: "UTC", FolderName: "01_Europe_Italy_Bevagna_Piazza_Silvestri", NamingMetadata: metadata}}, MediaTool: mediaTool, ExpectedLedgerCount: 14, ScheduledHourCount: 168, SourceClipCount: 1, SourceBytes: 10, FinalMediaCount: 1, AllocationLedgers: ledgerRefs, Hours: hours}
	index.FrozenDenominatorSHA256, _ = ComputeFrozenDenominatorSHA256(index.AllocationLedgers)
	index.BatchGenerationSHA256, _ = ComputeBatchGenerationSHA256(index)
	return index, ledgers, manifests
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

func testHourResolver(manifests []HourManifest) HourManifestResolver {
	return func(ref BatchIndexHour) (HourManifest, error) {
		for _, manifest := range manifests {
			if manifest.HourID == ref.HourID {
				return manifest, nil
			}
		}
		return HourManifest{}, os.ErrNotExist
	}
}

func cloneHourManifest(t *testing.T, manifest HourManifest) HourManifest {
	t.Helper()
	_, canonical, err := stitchcert.CanonicalSHA(manifest)
	if err != nil {
		t.Fatal(err)
	}
	var cloned HourManifest
	if err := json.Unmarshal(canonical, &cloned); err != nil {
		t.Fatal(err)
	}
	return cloned
}

func cloneAllocationLedger(t *testing.T, ledger StreamDayAllocation) StreamDayAllocation {
	t.Helper()
	_, canonical, err := stitchcert.CanonicalSHA(ledger)
	if err != nil {
		t.Fatal(err)
	}
	var cloned StreamDayAllocation
	if err := json.Unmarshal(canonical, &cloned); err != nil {
		t.Fatal(err)
	}
	return cloned
}

func testBatchIndexCopy(index BatchIndex) BatchIndex {
	copy := index
	copy.AllocationLedgers = append([]AllocationLedgerRef(nil), index.AllocationLedgers...)
	copy.Hours = append([]BatchIndexHour(nil), index.Hours...)
	return copy
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

func TestCrossDayLedgerLinkCoversEmptyToNonemptyTransition(t *testing.T) {
	dayOne := time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC)
	next := testSource(2, dayOne.AddDate(0, 0, 1))
	emptyRequest := testRequest([]SourceClip{testSource(1, dayOne)})
	emptyRequest.Sources = nil
	emptyRequest.NextDayFirst = &next
	empty, err := testLedger(emptyRequest, "2026-05-04")
	if err != nil {
		t.Fatal(err)
	}
	nextRequest := testRequest([]SourceClip{next})
	nonempty, err := testLedger(nextRequest, "2026-05-05")
	if err != nil {
		t.Fatal(err)
	}
	if err := validateCrossDayLedgerLink(empty, nonempty); err != nil {
		t.Fatal(err)
	}
	mutated := cloneAllocationLedger(t, nonempty)
	wrongClipID := int64(999)
	mutated.CrossDayBoundaries[0].NextClipID = &wrongClipID
	if err := validateCrossDayLedgerLink(empty, mutated); err == nil {
		t.Fatal("empty-to-nonempty cross-day neighbor substitution validated")
	}
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
	quarantineSources := []SourceClip{sources[0], testSource(2, start.Add(time.Minute)), testSource(3, start.Add(2*time.Minute))}
	req = testRequest(quarantineSources)
	quarantineLedger, err := testLedger(req, "2026-05-04")
	if err != nil {
		t.Fatal(err)
	}
	req.AllocationLedgerSHA = quarantineLedger.LedgerSHA256
	quarantine, err := BuildQuarantineOnlyHourPlan(req, "2026-05-04", 1, "ambiguous_audio_seam")
	if err != nil {
		t.Fatal(err)
	}
	evidence := []QuarantineEvidence{testQuarantineEvidence(quarantine, []int64{1, 2, 3}, "ambiguous_audio_seam")}
	quarantineAllocation, quarantineLedgerRef := testAllocation(quarantine)
	quarantineManifest, quarantineJSON, _, err := BuildHourManifest(HourManifestInput{Plan: quarantine, Allocation: quarantineAllocation, AllocationLedger: quarantineLedgerRef, QuarantineEvidence: evidence})
	if err != nil || quarantineManifest.Status != HourStatusQuarantineOnly || quarantineManifest.QuarantineReasonCode != "ambiguous_audio_seam" || len(quarantineManifest.Media) != 0 || len(quarantineManifest.Sources) != 3 || len(quarantineManifest.Gaps) != 2 {
		t.Fatalf("invalid quarantine-only manifest: %+v %v", quarantineManifest, err)
	}
	assertGolden(t, "testdata/hour_manifest_quarantine_only_v1.golden.json", quarantineJSON)
	missingGap := cloneHourManifest(t, quarantineManifest)
	missingGap.Gaps = missingGap.Gaps[:1]
	if _, _, err := CanonicalHourManifestArtifact(missingGap); err == nil {
		t.Fatal("multi-source quarantine manifest omitted an adjacent source fact")
	}
	changedReason := cloneHourManifest(t, quarantineManifest)
	changedReason.Gaps[0].Reason = "signed_presentation_gap"
	if _, _, err := CanonicalHourManifestArtifact(changedReason); err == nil {
		t.Fatal("multi-source quarantine manifest changed its typed run-break reason")
	}
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

func TestQuarantineOnlyTwoSourcesPreservesItsOneAdjacency(t *testing.T) {
	start := time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC)
	req := testRequest([]SourceClip{testSource(1, start), testSource(2, start.Add(time.Minute))})
	ledger, err := testLedger(req, req.LocalDate)
	if err != nil {
		t.Fatal(err)
	}
	req.AllocationLedgerSHA = ledger.LedgerSHA256
	plan, err := BuildQuarantineOnlyHourPlan(req, req.LocalDate, 1, "corrupt_source_media")
	if err != nil {
		t.Fatal(err)
	}
	allocation, canonicalLedger := testAllocation(plan)
	evidence := []QuarantineEvidence{testQuarantineEvidence(plan, []int64{1, 2}, "corrupt_source_media")}
	manifest, _, _, err := BuildHourManifest(HourManifestInput{Plan: plan, Allocation: allocation, AllocationLedger: canonicalLedger, QuarantineEvidence: evidence})
	if err != nil || len(manifest.Gaps) != 1 || manifest.Gaps[0].PreviousClipID != 1 || manifest.Gaps[0].NextClipID != 2 {
		t.Fatalf("two-source quarantine adjacency differs: %+v err=%v", manifest.Gaps, err)
	}
}

func TestQuarantineOnlyThreeSourcesRequiresExactOrderedAdjacencies(t *testing.T) {
	start := time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC)
	sources := []SourceClip{testSource(1, start), testSource(2, start.Add(time.Minute+time.Second)), testSource(3, start.Add(2*time.Minute))}
	sources[1].SeamToPrevious = SeamEvidence{Verdict: "gap", Reason: "signed_presentation_gap", SignedGapNanoseconds: time.Second.Nanoseconds()}
	sources[2].SeamToPrevious = SeamEvidence{Verdict: "overlap", Reason: "signed_presentation_gap", SignedGapNanoseconds: -time.Second.Nanoseconds()}
	req := testRequest(sources)
	ledger, err := testLedger(req, req.LocalDate)
	if err != nil {
		t.Fatal(err)
	}
	req.AllocationLedgerSHA = ledger.LedgerSHA256
	plan, err := BuildQuarantineOnlyHourPlan(req, req.LocalDate, 1, "corrupt_source_media")
	if err != nil {
		t.Fatal(err)
	}
	allocation, canonicalLedger := testAllocation(plan)
	evidence := []QuarantineEvidence{testQuarantineEvidence(plan, []int64{1, 2, 3}, "corrupt_source_media")}
	manifest, _, _, err := BuildHourManifest(HourManifestInput{Plan: plan, Allocation: allocation, AllocationLedger: canonicalLedger, QuarantineEvidence: evidence})
	if err != nil || len(manifest.Gaps) != 2 || manifest.Gaps[0].PreviousClipID != 1 || manifest.Gaps[0].NextClipID != 2 || manifest.Gaps[0].SignedGapNanoseconds != time.Second.Nanoseconds() || manifest.Gaps[1].PreviousClipID != 2 || manifest.Gaps[1].NextClipID != 3 || manifest.Gaps[1].SignedGapNanoseconds != -time.Second.Nanoseconds() || manifest.Sources[1].SeamToPrevious != sources[1].SeamToPrevious || manifest.Sources[2].SeamToPrevious != sources[2].SeamToPrevious {
		t.Fatalf("three-source quarantine adjacencies differ: %+v err=%v", manifest.Gaps, err)
	}

	for name, mutate := range map[string]func(*HourManifest){
		"removed":        func(m *HourManifest) { m.Gaps = m.Gaps[:1] },
		"swapped":        func(m *HourManifest) { m.Gaps[0], m.Gaps[1] = m.Gaps[1], m.Gaps[0] },
		"duplicated":     func(m *HourManifest) { m.Gaps = append(m.Gaps, m.Gaps[0]) },
		"wrong neighbor": func(m *HourManifest) { m.Gaps[0].NextClipID = 3 },
	} {
		t.Run(name, func(t *testing.T) {
			mutated := cloneHourManifest(t, manifest)
			mutate(&mutated)
			if _, _, err := CanonicalHourManifestArtifact(mutated); err == nil {
				t.Fatal("malformed quarantine adjacency validated")
			}
		})
	}
}

func TestManifestRequiresOrderedMaximalityProofForZeroDurationSplit(t *testing.T) {
	start := time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC)
	sources := []SourceClip{testSource(1, start), testSource(2, start.Add(time.Minute)), testSource(3, start.Add(2*time.Minute))}
	sources[1].SeamToPrevious = SeamEvidence{Verdict: "incompatible", Reason: "decoded_audio_totals_mismatch"}
	req := testRequest(sources)
	plan, err := buildTestPlan(req)
	if err != nil || len(plan.Outputs) != 2 {
		t.Fatalf("split fixture plan differs: outputs=%d err=%v", len(plan.Outputs), err)
	}
	peels := []MaximalityEvidence{
		testMaximalityEvidence(plan, []int64{1, 2, 3}, "decoded_audio_totals_mismatch"),
		testMaximalityEvidence(plan, []int64{1, 2}, "decoded_audio_totals_mismatch"),
	}
	built := []BuiltOutput{
		{SizeBytes: plan.Outputs[0].ExpectedSize, SHA256: plan.Outputs[0].ExpectedSHA, SourceCount: 1, Verification: passingVerification(), SplitEvidence: peels},
		{SizeBytes: plan.Outputs[1].ExpectedSize, SHA256: plan.Outputs[1].ExpectedSHA, SourceCount: 2, Verification: passingVerification()},
	}
	allocation, ledger := testAllocation(plan)
	manifest, _, _, err := BuildHourManifest(HourManifestInput{Plan: plan, Allocation: allocation, AllocationLedger: ledger, MediaArtifactIDs: []int64{88, 89}, Built: built})
	if err != nil {
		t.Fatal(err)
	}
	missing := cloneHourManifest(t, manifest)
	missing.Media[0].MaximalityEvidence = nil
	if _, _, err := CanonicalHourManifestArtifact(missing); err == nil {
		t.Fatal("zero-duration media split omitted its typed maximality proof")
	}
	reordered := cloneHourManifest(t, manifest)
	reordered.Media[0].MaximalityEvidence[0], reordered.Media[0].MaximalityEvidence[1] = reordered.Media[0].MaximalityEvidence[1], reordered.Media[0].MaximalityEvidence[0]
	if _, _, err := CanonicalHourManifestArtifact(reordered); err == nil {
		t.Fatal("maximality peel evidence order was not canonical")
	}
	changedReason := cloneHourManifest(t, manifest)
	changedReason.Gaps[0].Reason = "other_deterministic_split"
	if _, _, err := CanonicalHourManifestArtifact(changedReason); err == nil {
		t.Fatal("zero-duration split reason was detached from typed maximality proof")
	}
}

func TestHourManifestRejectsMutatedLedgerAndDowngradedEvidence(t *testing.T) {
	plan := oneOutputPlan(t)
	built := []BuiltOutput{{SizeBytes: plan.Outputs[0].ExpectedSize, SHA256: plan.Outputs[0].ExpectedSHA, SourceCount: 1, Verification: passingVerification()}}
	allocation, ledger := testAllocation(plan)
	validManifest, _, _, err := BuildHourManifest(HourManifestInput{Plan: plan, Allocation: allocation, AllocationLedger: ledger, MediaArtifactIDs: []int64{88}, Built: built})
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*HourManifest){
		"invalid audio contract": func(manifest *HourManifest) { manifest.Sources[0].AudioContract = &AudioSequenceContract{} },
		"audio contract without verified audio": func(manifest *HourManifest) {
			manifest.Sources[0].AudioContract = &AudioSequenceContract{CodecName: "aac", SampleRate: 48000, Channels: 2, ChannelLayout: "stereo"}
		},
		"media over conditional put cap": func(manifest *HourManifest) { manifest.Media[0].SizeBytes = r2.MaxConditionalPutBytes + 1 },
	} {
		t.Run(name, func(t *testing.T) {
			mutated := cloneHourManifest(t, validManifest)
			mutate(&mutated)
			if _, _, err := CanonicalHourManifestArtifact(mutated); err == nil {
				t.Fatal("unsafe self-rehashed manifest validated")
			}
		})
	}
	unsafePlan := plan
	unsafePlan.Sources = append([]SourceClip(nil), plan.Sources...)
	unsafePlan.Sources[0].Endpoint = testSourceEndpoint + "/?secret=value"
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

func testMaximalityEvidence(plan BatchPlan, ids []int64, reason string) MaximalityEvidence {
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
	return MaximalityEvidence{CandidateClipIDs: ids, ReasonCode: reason, SourceClaimSHA256: sourceClaim, PolicyVersion: PlanPolicyVersion, EvidenceSHA256: evidenceSHA, FailureFacts: facts, FailureSHA256: failureSHA, RepeatCount: 2, MediaToolIdentity: plan.MediaTool.IdentitySHA256}
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
