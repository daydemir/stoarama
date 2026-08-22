package joinedrecording

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/stitchcert"
)

func TestBatchIndexV1Golden(t *testing.T) {
	index, ledgers, manifests := testBatchIndex(t)
	_, canonical, _, err := BuildBatchIndex(index, testSelectionResolver(index, ledgers), testLedgerResolver(ledgers), testHourResolver(manifests))
	if err != nil {
		t.Fatal(err)
	}
	assertGolden(t, "testdata/batch_index_v1.golden.json", canonical)
}

func TestBatchIndexBindsRecordingSelectionEvidence(t *testing.T) {
	index, ledgers, manifests := testBatchIndex(t)
	baseline, denominator := index.BatchGenerationSHA256, index.FrozenDenominatorSHA256
	for name, mutate := range map[string]func(*FrozenRecording){
		"tier":                 func(recording *FrozenRecording) { recording.SelectionTier = "fine+" },
		"qualification window": func(recording *FrozenRecording) { recording.QualificationSHA256 = strings.Repeat("6", 64) },
		"completion":           func(recording *FrozenRecording) { recording.CompletedAt = recording.CompletedAt.Add(time.Second) },
	} {
		mutated := testBatchIndexCopy(index)
		mutated.FrozenRecordings = append([]FrozenRecording(nil), index.FrozenRecordings...)
		mutate(&mutated.FrozenRecordings[0])
		digest, err := ComputeBatchGenerationSHA256(mutated)
		if err != nil || digest == baseline {
			t.Fatalf("%s selection evidence did not change batch identity: digest=%s err=%v", name, digest, err)
		}
		denominatorDigest, denominatorErr := ComputeFrozenDenominatorSHA256(mutated.SelectionAuthority, mutated.FrozenRecordings, frozenDenominatorDays(mutated.AllocationLedgers))
		if denominatorErr == nil && denominatorDigest == denominator {
			t.Fatalf("%s recording selection did not change denominator identity", name)
		}
	}
	for name, mutate := range map[string]func(*SelectionAuthority){
		"basis":       func(authority *SelectionAuthority) { authority.SelectionBasis = "other" },
		"ordered IDs": func(authority *SelectionAuthority) { authority.OrderedRecordingIDSHA256 = strings.Repeat("9", 64) },
		"cutoff":      func(authority *SelectionAuthority) { authority.Cutoff = authority.Cutoff.Add(time.Second) },
		"run":         func(authority *SelectionAuthority) { authority.QualificationRunID++ },
		"run frozen at": func(authority *SelectionAuthority) {
			authority.QualificationRunFrozenAt = authority.QualificationRunFrozenAt.Add(time.Second)
		},
		"rule":    func(authority *SelectionAuthority) { authority.QualificationRuleVersion += "-changed" },
		"cohort":  func(authority *SelectionAuthority) { authority.QualificationCohortSHA256 = strings.Repeat("8", 64) },
		"windows": func(authority *SelectionAuthority) { authority.QualificationWindowsSHA256 = strings.Repeat("7", 64) },
		"selected windows": func(authority *SelectionAuthority) {
			authority.SelectedQualificationWindowsSHA256 = strings.Repeat("6", 64)
		},
	} {
		mutated := testBatchIndexCopy(index)
		mutate(&mutated.SelectionAuthority)
		digest, err := ComputeBatchGenerationSHA256(mutated)
		if err != nil || digest == baseline {
			t.Fatalf("%s selection authority did not change batch identity: digest=%s err=%v", name, digest, err)
		}
		denominatorDigest, denominatorErr := ComputeFrozenDenominatorSHA256(mutated.SelectionAuthority, mutated.FrozenRecordings, frozenDenominatorDays(mutated.AllocationLedgers))
		if denominatorErr == nil && denominatorDigest == denominator {
			t.Fatalf("%s selection authority did not change denominator identity", name)
		}
	}

	mutated := testBatchIndexCopy(index)
	mutated.FrozenRecordings = append([]FrozenRecording(nil), index.FrozenRecordings...)
	mutated.FrozenRecordings[0].SelectionTier = "fine+"
	mutated.BatchGenerationSHA256, _ = ComputeBatchGenerationSHA256(mutated)
	if _, _, _, err := BuildBatchIndex(mutated, testSelectionResolver(index, ledgers), testLedgerResolver(ledgers), testHourResolver(manifests)); err == nil {
		t.Fatal("non-good+ Tier-1 recording selection was accepted")
	}
	_, canonical, _, err := BuildBatchIndex(index, testSelectionResolver(index, ledgers), testLedgerResolver(ledgers), testHourResolver(manifests))
	if err != nil {
		t.Fatal(err)
	}
	staleJSON := bytes.Replace(canonical, []byte(`"selection_tier":"good+"`), []byte(`"eligibility_tier":"good+"`), 1)
	var stale BatchIndex
	if bytes.Equal(staleJSON, canonical) {
		t.Fatal("selection tier mutation did not change canonical JSON")
	}
	if err := json.Unmarshal(staleJSON, &stale); err == nil {
		t.Fatal("stale eligibility_tier wire field was decoded")
	}

	selfRehashed := testBatchIndexCopy(index)
	selfRehashed.SelectionAuthority.QualificationRunID++
	selfRehashed.FrozenDenominatorSHA256, _ = ComputeFrozenDenominatorSHA256(selfRehashed.SelectionAuthority, selfRehashed.FrozenRecordings, frozenDenominatorDays(selfRehashed.AllocationLedgers))
	selfRehashed.BatchGenerationSHA256, _ = ComputeBatchGenerationSHA256(selfRehashed)
	if _, _, _, err := BuildBatchIndex(selfRehashed, testSelectionResolver(index, ledgers), testLedgerResolver(ledgers), testHourResolver(manifests)); err == nil {
		t.Fatal("self-rehashed caller selection authority replaced server-frozen evidence")
	}

}

func TestBatchIndexResolvesExactQualificationWindowsAndPersistedDenominator(t *testing.T) {
	index, ledgers, manifests := testBatchIndex(t)
	evidence, err := testSelectionResolver(index, ledgers)()
	if err != nil {
		t.Fatal(err)
	}

	shiftedIndex, shiftedLedgers, shiftedManifests := testBatchIndexFrom(t, time.Date(2026, time.June, 1, 8, 0, 0, 0, time.UTC), 10_009)
	if _, _, _, err := BuildBatchIndex(shiftedIndex, testSelectionResolver(shiftedIndex, shiftedLedgers), testLedgerResolver(shiftedLedgers), testHourResolver(shiftedManifests)); err != nil {
		t.Fatalf("fully self-rehashed shifted qualification fixture is not canonical: %v", err)
	}
	if _, _, _, err := BuildBatchIndex(shiftedIndex, testSelectionResolver(index, ledgers), testLedgerResolver(shiftedLedgers), testHourResolver(shiftedManifests)); err == nil {
		t.Fatal("fully self-rehashed shifted qualification evidence replaced the server-frozen batch")
	}

	wrongDenominator := evidence
	wrongDenominator.FrozenDenominatorSHA256 = strings.Repeat("9", 64)
	if _, _, _, err := BuildBatchIndex(index, func() (FrozenBatchEvidence, error) { return wrongDenominator, nil }, testLedgerResolver(ledgers), testHourResolver(manifests)); err == nil {
		t.Fatal("wrong persisted apply-time denominator was accepted")
	}
	if _, _, _, err := BuildBatchIndex(index, nil, testLedgerResolver(ledgers), testHourResolver(manifests)); err == nil {
		t.Fatal("missing frozen-batch resolver was accepted")
	}

	nonCutoffIndex, nonCutoffLedgers, nonCutoffManifests := testBatchIndexFromWindowFrozenAt(t, time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC), 9, testBatchCutoff().Add(time.Second))
	if _, _, _, err := canonicalSealedBatchIndex(nonCutoffIndex); err != nil {
		t.Fatalf("non-cutoff qualification fixture is not otherwise canonical: %v", err)
	}
	if _, _, _, err := BuildBatchIndex(nonCutoffIndex, testSelectionResolverWithWindowFrozenAt(nonCutoffIndex, nonCutoffLedgers, testBatchCutoff().Add(time.Second)), testLedgerResolver(nonCutoffLedgers), testHourResolver(nonCutoffManifests)); err == nil {
		t.Fatal("qualification window frozen after the authoritative cutoff")
	}

	offsetCutoff := testBatchCutoff().In(time.FixedZone("EDT", -4*60*60))
	offsetAuthority := index.SelectionAuthority
	offsetAuthority.Cutoff = offsetCutoff
	if ValidateSelectionAuthority(offsetAuthority, index.RecordingIDs) == nil {
		t.Fatal("same-instant non-UTC selection cutoff representation was accepted")
	}
	offsetIndex, offsetLedgers, offsetManifests := testBatchIndexFromWindowFrozenAt(t, time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC), 9, offsetCutoff)
	if _, _, _, err := canonicalSealedBatchIndex(offsetIndex); err != nil {
		t.Fatalf("offset qualification fixture is not otherwise canonical: %v", err)
	}
	if _, _, _, err := BuildBatchIndex(offsetIndex, testSelectionResolverWithWindowFrozenAt(offsetIndex, offsetLedgers, offsetCutoff), testLedgerResolver(offsetLedgers), testHourResolver(offsetManifests)); err == nil {
		t.Fatal("same-instant non-UTC qualification cutoff representation was accepted")
	}

	completionIndex := testBatchIndexCopy(index)
	completionIndex.FrozenRecordings = append([]FrozenRecording(nil), index.FrozenRecordings...)
	completionIndex.FrozenRecordings[0].CompletedAt = completionIndex.FrozenRecordings[0].CompletedAt.Add(-time.Second)
	completionIndex.FrozenDenominatorSHA256, _ = ComputeFrozenDenominatorSHA256(completionIndex.SelectionAuthority, completionIndex.FrozenRecordings, frozenDenominatorDays(completionIndex.AllocationLedgers))
	completionIndex.BatchGenerationSHA256, _ = ComputeBatchGenerationSHA256(completionIndex)
	completionEvidence := evidence
	completionEvidence.FrozenRecordings = append([]FrozenRecording(nil), completionIndex.FrozenRecordings...)
	completionEvidence.FrozenDenominatorSHA256 = completionIndex.FrozenDenominatorSHA256
	if _, _, _, err := canonicalSealedBatchIndex(completionIndex); err != nil {
		t.Fatalf("wrong-completion fixture is not otherwise canonical: %v", err)
	}
	if _, _, _, err := BuildBatchIndex(completionIndex, func() (FrozenBatchEvidence, error) { return completionEvidence, nil }, testLedgerResolver(ledgers), testHourResolver(manifests)); err == nil {
		t.Fatal("recording completion differed from the maximum qualified-day completion")
	}

	mutatedLedgers := append([]StreamDayAllocation(nil), ledgers...)
	mutatedLedgers[1] = cloneAllocationLedger(t, ledgers[1])
	mutatedLedgers[1].QualificationDay.JobID++
	logical := mutatedLedgers[1]
	logical.LedgerSHA256 = ""
	mutatedLedgers[1].LedgerSHA256, _, _ = stitchcert.CanonicalSHA(logical)
	mutatedRef, err := BuildAllocationLedgerRef(index.AllocationLedgers[1].ArtifactID, mutatedLedgers[1])
	if err != nil {
		t.Fatal(err)
	}
	mutatedIndex := testBatchIndexCopy(index)
	mutatedIndex.AllocationLedgers[1] = mutatedRef
	mutatedIndex.BatchGenerationSHA256, _ = ComputeBatchGenerationSHA256(mutatedIndex)
	if _, _, _, err := BuildBatchIndex(mutatedIndex, testSelectionResolver(index, ledgers), testLedgerResolver(mutatedLedgers), testHourResolver(manifests)); err == nil {
		t.Fatal("self-rehashed ledger day differed from its exact sealed qualification window")
	}
}

func TestFrozenDenominatorUsesOnlyApplyTimeSourceFacts(t *testing.T) {
	source := testSource(1, time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC))
	day := QualifiedDay{LocalDate: "2026-05-04", QualificationWindowOrdinal: 1, JobID: source.RecordingJobID, WindowStart: source.StartUTC, WindowEnd: source.StartUTC.Add(12 * time.Hour), CompletedAt: source.StartUTC.Add(12 * time.Hour)}
	qualificationSHA := strings.Repeat("a", 64)
	baseline, err := BuildFrozenDenominatorDayProjection(source.RecordingID, day, qualificationSHA, FrozenSourceSnapshots([]SourceClip{source}))
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*SourceClip){
		"etag":    func(source *SourceClip) { source.Object.ETag = "changed" },
		"version": func(source *SourceClip) { source.Object.VersionID = "changed" },
		"audio":   func(source *SourceClip) { source.AudioContract = &AudioSequenceContract{} },
		"derived seam": func(source *SourceClip) {
			source.SeamToPrevious = SeamEvidence{Verdict: "gap", Reason: "changed", SignedGapNanoseconds: 1}
		},
	} {
		mutated := source
		mutate(&mutated)
		got, err := BuildFrozenDenominatorDayProjection(source.RecordingID, day, qualificationSHA, FrozenSourceSnapshots([]SourceClip{mutated}))
		if err != nil || got != baseline {
			t.Fatalf("later %s fact changed apply-time denominator: got=%+v err=%v", name, got, err)
		}
	}
	released := source.StartUTC.Add(time.Hour)
	for name, mutate := range map[string]func(*FrozenSourceSnapshot){
		"storage destination": func(source *FrozenSourceSnapshot) { source.StorageDestinationID++ },
		"object key":          func(source *FrozenSourceSnapshot) { source.ObjectKey += "-changed" },
		"timing":              func(source *FrozenSourceSnapshot) { source.StartUTC = source.StartUTC.Add(time.Nanosecond) },
		"size":                func(source *FrozenSourceSnapshot) { source.SizeBytes++ },
		"ingest SHA":          func(source *FrozenSourceSnapshot) { source.IngestSHA256 = strings.Repeat("9", 64) },
		"release":             func(source *FrozenSourceSnapshot) { source.ReleasedAt = &released },
	} {
		snapshots := FrozenSourceSnapshots([]SourceClip{source})
		mutate(&snapshots[0])
		got, err := BuildFrozenDenominatorDayProjection(source.RecordingID, day, qualificationSHA, snapshots)
		if err == nil && got == baseline {
			t.Fatalf("apply-time %s mutation preserved denominator day", name)
		}
	}
	two := testSource(2, source.StartUTC.Add(time.Minute))
	_, err = BuildFrozenDenominatorDayProjection(source.RecordingID, day, qualificationSHA, FrozenSourceSnapshots([]SourceClip{source, two}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := BuildFrozenDenominatorDayProjection(source.RecordingID, day, qualificationSHA, FrozenSourceSnapshots([]SourceClip{two, source})); err == nil {
		t.Fatal("non-chronological apply-time source order was accepted")
	}
	for name, mutate := range map[string]func(*FrozenSourceSnapshot){
		"zero storage destination": func(second *FrozenSourceSnapshot) { second.StorageDestinationID = 0 },
		"duplicate locator":        func(second *FrozenSourceSnapshot) { second.ObjectKey = source.Object.Key },
		"outside window": func(second *FrozenSourceSnapshot) {
			second.StartUTC = day.WindowEnd
			second.EndUTC = day.WindowEnd.Add(time.Minute)
		},
		"long duration":       func(second *FrozenSourceSnapshot) { second.EndUTC = second.StartUTC.Add(16 * time.Minute) },
		"provider whitespace": func(second *FrozenSourceSnapshot) { second.Provider = " r2" },
		"region whitespace":   func(second *FrozenSourceSnapshot) { second.Region = " auto" },
		"bucket whitespace":   func(second *FrozenSourceSnapshot) { second.Bucket = " recordings" },
	} {
		snapshots := FrozenSourceSnapshots([]SourceClip{source, two})
		mutate(&snapshots[1])
		if _, err := BuildFrozenDenominatorDayProjection(source.RecordingID, day, qualificationSHA, snapshots); err == nil {
			t.Fatalf("%s apply-time source was accepted", name)
		}
	}
}

func TestFrozenDenominatorAcceptsManagedR2Source(t *testing.T) {
	source := testSource(1, time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC))
	source.Provider = "r2_managed"
	day := QualifiedDay{LocalDate: "2026-05-04", QualificationWindowOrdinal: 1,
		JobID: source.RecordingJobID, WindowStart: source.StartUTC, WindowEnd: source.StartUTC.Add(12 * time.Hour),
		CompletedAt: source.StartUTC.Add(12 * time.Hour)}
	if _, err := BuildFrozenDenominatorDayProjection(source.RecordingID, day, strings.Repeat("a", 64),
		FrozenSourceSnapshots([]SourceClip{source})); err != nil {
		t.Fatalf("managed R2 source rejected: %v", err)
	}
}

func TestValidFrozenSourceStorageProviderPolicy(t *testing.T) {
	for _, test := range []struct {
		name                     string
		provider, region, bucket string
		want                     bool
	}{
		{"direct R2", "r2", "auto", "recordings", true},
		{"managed R2", "r2_managed", "auto", "recordings", true},
		{"S3 compatible", "s3_compatible", "auto", "recordings", false},
		{"WebDAV", "webdav", "auto", "recordings", false},
		{"provider whitespace", " r2", "auto", "recordings", false},
		{"region", "r2_managed", "us-east-1", "recordings", false},
		{"bucket whitespace", "r2_managed", "auto", " recordings", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := validFrozenSourceStorage(test.provider, test.region, test.bucket); got != test.want {
				t.Fatalf("validFrozenSourceStorage(%q,%q,%q)=%v want %v",
					test.provider, test.region, test.bucket, got, test.want)
			}
		})
	}
}

func TestFrozenDenominatorIsCanonicalOrderedLedgerProjection(t *testing.T) {
	index, ledgers, manifests := testBatchIndex(t)
	canonicalIndex := testBatchIndexCopy(index)
	want := index.FrozenDenominatorSHA256
	if want != "09b38447860262fa11c65080fae50f18a656b2bd051ff78d2e853a6a2a6c7135" {
		t.Fatalf("canonical denominator fixture changed: %s", want)
	}
	if got, err := ComputeFrozenDenominatorSHA256(index.SelectionAuthority, index.FrozenRecordings, frozenDenominatorDays(index.AllocationLedgers)); err != nil || got != want {
		t.Fatalf("denominator projection=%s err=%v, want %s", got, err, want)
	}

	mutations := []func([]AllocationLedgerRef){
		func(ledgers []AllocationLedgerRef) { ledgers[0].FrozenSourceSHA256 = strings.Repeat("9", 64) },
		func(ledgers []AllocationLedgerRef) { ledgers[0].SourceCount++ },
		func(ledgers []AllocationLedgerRef) { ledgers[0].SourceBytes++ },
		func(ledgers []AllocationLedgerRef) { ledgers[0], ledgers[1] = ledgers[1], ledgers[0] },
	}
	for i, mutate := range mutations {
		ledgers := append([]AllocationLedgerRef(nil), index.AllocationLedgers...)
		mutate(ledgers)
		got, err := ComputeFrozenDenominatorSHA256(index.SelectionAuthority, index.FrozenRecordings, frozenDenominatorDays(ledgers))
		if err == nil && got == want {
			t.Fatalf("denominator mutation %d preserved digest", i+1)
		}
	}
	for name, mutate := range map[string]func([]AllocationLedgerRef){
		"post-HEAD source claim": func(ledgers []AllocationLedgerRef) { ledgers[0].SourceClaimSHA256 = strings.Repeat("9", 64) },
		"ledger artifact":        func(ledgers []AllocationLedgerRef) { ledgers[0].ArtifactID++ },
	} {
		ledgers := append([]AllocationLedgerRef(nil), index.AllocationLedgers...)
		mutate(ledgers)
		got, err := ComputeFrozenDenominatorSHA256(index.SelectionAuthority, index.FrozenRecordings, frozenDenominatorDays(ledgers))
		if err != nil || got != want {
			t.Fatalf("%s redefined apply-time denominator: got=%s err=%v", name, got, err)
		}
	}

	denominatorAttack := testBatchIndexCopy(canonicalIndex)
	denominatorAttack.FrozenDenominatorSHA256 = strings.Repeat("a", 64)
	denominatorAttack.BatchGenerationSHA256, _ = ComputeBatchGenerationSHA256(denominatorAttack)
	if _, _, _, err := BuildBatchIndex(denominatorAttack, testSelectionResolver(index, ledgers), testLedgerResolver(ledgers), testHourResolver(manifests)); err == nil {
		t.Fatal("batch index accepted caller-supplied denominator digest")
	}

	mutatedSources := append([]StreamDayAllocation(nil), ledgers...)
	mutatedSources[0] = cloneAllocationLedger(t, ledgers[0])
	mutatedSources[0].Sources[0].StorageDestinationID++
	mutatedSources[0].FrozenSourceSHA256, _, _ = stitchcert.CanonicalSHA(FrozenSourceSnapshots(mutatedSources[0].Sources))
	mutatedSources[0].SourceClaimSHA256, _, _ = CanonicalSourceClaim(mutatedSources[0].Sources)
	mutatedSources[0].HourSourceSHA256[0], _, _ = CanonicalSourceClaim(mutatedSources[0].Sources)
	logicalMutatedSources := mutatedSources[0]
	logicalMutatedSources.LedgerSHA256 = ""
	mutatedSources[0].LedgerSHA256, _, _ = stitchcert.CanonicalSHA(logicalMutatedSources)
	mutatedSourceRef, err := BuildAllocationLedgerRef(canonicalIndex.AllocationLedgers[0].ArtifactID, mutatedSources[0])
	if err != nil {
		t.Fatalf("canonical storage-destination substitution fixture failed: %v", err)
	}
	mutatedSourceIndex := testBatchIndexCopy(canonicalIndex)
	mutatedSourceIndex.AllocationLedgers[0] = mutatedSourceRef
	mutatedManifests := append([]HourManifest(nil), manifests...)
	for hour := 0; hour < 12; hour++ {
		mutatedManifests[hour] = cloneHourManifest(t, manifests[hour])
		if hour == 0 {
			mutatedManifests[hour].Sources[0].StorageDestinationID++
			mutatedManifests[hour].SourceClaimSHA256 = mutatedSources[0].HourSourceSHA256[hour]
		}
		boundaries, crossDay := hourBoundarySubset(mutatedSources[0], hour+1)
		mutatedManifests[hour].Allocation = HourManifestAllocation{ArtifactID: mutatedSourceRef.ArtifactID, RelativePath: mutatedSourceRef.RelativePath, ObjectKey: mutatedSourceRef.ObjectKey, SizeBytes: mutatedSourceRef.SizeBytes, SHA256: mutatedSourceRef.SHA256, LedgerSHA256: mutatedSourceRef.LedgerSHA256, HourSourceSHA256: mutatedSources[0].HourSourceSHA256[hour], Boundaries: boundaries, CrossDayBoundaries: crossDay}
		mutatedHourRef, err := BuildBatchIndexHour(canonicalIndex.Hours[hour].HourManifestArtifactID, mutatedManifests[hour])
		if err != nil {
			t.Fatalf("canonical storage-destination hour %d fixture failed: %v", hour+1, err)
		}
		mutatedSourceIndex.Hours[hour] = mutatedHourRef
	}
	mutatedSourceIndex.FrozenDenominatorSHA256, _ = ComputeFrozenDenominatorSHA256(mutatedSourceIndex.SelectionAuthority, mutatedSourceIndex.FrozenRecordings, frozenDenominatorDays(mutatedSourceIndex.AllocationLedgers))
	mutatedSourceIndex.BatchGenerationSHA256, _ = ComputeBatchGenerationSHA256(mutatedSourceIndex)
	if _, _, _, err := BuildBatchIndex(mutatedSourceIndex, testSelectionResolver(mutatedSourceIndex, mutatedSources), testLedgerResolver(mutatedSources), testHourResolver(mutatedManifests)); err != nil {
		t.Fatalf("fully self-rehashed source-destination fixture is not canonical: %v", err)
	}
	if _, _, _, err := BuildBatchIndex(mutatedSourceIndex, testSelectionResolver(index, ledgers), testLedgerResolver(mutatedSources), testHourResolver(mutatedManifests)); err == nil {
		t.Fatal("fully self-rehashed source destination replaced persisted apply-time evidence")
	}

	attack := testBatchIndexCopy(canonicalIndex)
	attack.AllocationLedgers[0].SourceClaimSHA256 = strings.Repeat("9", 64)
	attack.FrozenDenominatorSHA256, _ = ComputeFrozenDenominatorSHA256(attack.SelectionAuthority, attack.FrozenRecordings, frozenDenominatorDays(attack.AllocationLedgers))
	attack.BatchGenerationSHA256, _ = ComputeBatchGenerationSHA256(attack)
	if _, _, _, err := BuildBatchIndex(attack, testSelectionResolver(index, ledgers), testLedgerResolver(ledgers), testHourResolver(manifests)); err == nil {
		t.Fatal("batch index accepted recomputed hashes over a mutated ledger reference")
	}

	hourAttack := testBatchIndexCopy(canonicalIndex)
	hourAttack.Hours[0].SHA256 = strings.Repeat("9", 64)
	hourAttack.BatchGenerationSHA256, _ = ComputeBatchGenerationSHA256(hourAttack)
	if _, _, _, err := BuildBatchIndex(hourAttack, testSelectionResolver(index, ledgers), testLedgerResolver(ledgers), testHourResolver(manifests)); err == nil {
		t.Fatal("batch index accepted recomputed hashes over a mutated hour-manifest reference")
	}

	ordinalLedgers := append([]StreamDayAllocation(nil), ledgers...)
	ordinalLedgers[0] = cloneAllocationLedger(t, ledgers[0])
	ordinalLedgers[0].QualificationDay.QualificationWindowOrdinal = 2
	logicalLedger := ordinalLedgers[0]
	logicalLedger.LedgerSHA256 = ""
	ordinalLedgers[0].LedgerSHA256, _, _ = stitchcert.CanonicalSHA(logicalLedger)
	ordinalRef, err := BuildAllocationLedgerRef(canonicalIndex.AllocationLedgers[0].ArtifactID, ordinalLedgers[0])
	if err != nil {
		t.Fatalf("independently valid day ordinal fixture failed: %v", err)
	}
	ordinalIndex := testBatchIndexCopy(canonicalIndex)
	ordinalIndex.AllocationLedgers[0] = ordinalRef
	ordinalIndex.FrozenDenominatorSHA256, _ = ComputeFrozenDenominatorSHA256(ordinalIndex.SelectionAuthority, ordinalIndex.FrozenRecordings, frozenDenominatorDays(ordinalIndex.AllocationLedgers))
	ordinalIndex.BatchGenerationSHA256, _ = ComputeBatchGenerationSHA256(ordinalIndex)
	if _, _, _, err := BuildBatchIndex(ordinalIndex, testSelectionResolver(index, ledgers), testLedgerResolver(ordinalLedgers), testHourResolver(manifests)); err == nil {
		t.Fatal("ledger date accepted another qualification-window ordinal")
	}

	mixedLedgers := append([]StreamDayAllocation(nil), ledgers...)
	mixedLedgers[1] = cloneAllocationLedger(t, ledgers[1])
	mixedLedgers[1].QualificationSHA = strings.Repeat("8", 64)
	frozenDay, err := BuildFrozenDenominatorDayProjection(mixedLedgers[1].RecordingID, mixedLedgers[1].QualificationDay, mixedLedgers[1].QualificationSHA, FrozenSourceSnapshots(mixedLedgers[1].Sources))
	if err != nil {
		t.Fatal(err)
	}
	mixedLedgers[1].FrozenSourceSHA256 = frozenDay.FrozenSourceSHA256
	logicalMixed := mixedLedgers[1]
	logicalMixed.LedgerSHA256 = ""
	mixedLedgers[1].LedgerSHA256, _, _ = stitchcert.CanonicalSHA(logicalMixed)
	mixedRef, err := BuildAllocationLedgerRef(canonicalIndex.AllocationLedgers[1].ArtifactID, mixedLedgers[1])
	if err != nil {
		t.Fatalf("independently canonical mixed-window ledger failed: %v", err)
	}
	mixedIndex := testBatchIndexCopy(canonicalIndex)
	mixedIndex.AllocationLedgers[1] = mixedRef
	mixedIndex.FrozenDenominatorSHA256, _ = ComputeFrozenDenominatorSHA256(mixedIndex.SelectionAuthority, mixedIndex.FrozenRecordings, frozenDenominatorDays(mixedIndex.AllocationLedgers))
	mixedIndex.BatchGenerationSHA256, _ = ComputeBatchGenerationSHA256(mixedIndex)
	if _, _, _, err := BuildBatchIndex(mixedIndex, testSelectionResolver(index, ledgers), testLedgerResolver(mixedLedgers), testHourResolver(manifests)); err == nil {
		t.Fatal("one recording mixed qualification-window identities across its 14 ledgers")
	}

	lateRun := testBatchIndexCopy(canonicalIndex)
	lateRun.SelectionAuthority.QualificationRunFrozenAt = time.Date(2026, time.June, 1, 0, 0, 0, 0, time.UTC)
	lateRun.FrozenDenominatorSHA256, _ = ComputeFrozenDenominatorSHA256(lateRun.SelectionAuthority, lateRun.FrozenRecordings, frozenDenominatorDays(lateRun.AllocationLedgers))
	lateRun.BatchGenerationSHA256, _ = ComputeBatchGenerationSHA256(lateRun)
	if _, _, _, err := BuildBatchIndex(lateRun, testSelectionResolver(lateRun, ledgers), testLedgerResolver(ledgers), testHourResolver(manifests)); err == nil {
		t.Fatal("qualification run frozen after its scheduled windows was accepted")
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
		if _, _, _, err := BuildBatchIndex(mutatedIndex, testSelectionResolver(index, ledgers), testLedgerResolver(ledgers), testHourResolver(mutated)); err == nil {
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
			if _, _, _, err := BuildBatchIndex(mutatedIndex, testSelectionResolver(index, ledgers), testLedgerResolver(ledgers), testHourResolver(mutated)); err == nil {
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
			if _, _, _, err := BuildBatchIndex(mutatedIndex, testSelectionResolver(index, ledgers), testLedgerResolver(ledgers), testHourResolver(mutated)); err == nil {
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
		if _, _, _, err := BuildBatchIndex(mutatedIndex, testSelectionResolver(index, ledgers), testLedgerResolver(ledgers), testHourResolver(mutated)); err == nil {
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
		mutatedIndex.FrozenDenominatorSHA256, _ = ComputeFrozenDenominatorSHA256(mutatedIndex.SelectionAuthority, mutatedIndex.FrozenRecordings, frozenDenominatorDays(mutatedIndex.AllocationLedgers))
		mutatedIndex.BatchGenerationSHA256, _ = ComputeBatchGenerationSHA256(mutatedIndex)
		if _, _, _, err := BuildBatchIndex(mutatedIndex, testSelectionResolver(index, ledgers), testLedgerResolver(mutatedLedgers), testHourResolver(manifests)); err == nil {
			t.Fatal("consecutive ledgers accepted conflicting cross-day neighbor facts")
		}
	})
}

func testBatchIndex(t *testing.T) (BatchIndex, []StreamDayAllocation, []HourManifest) {
	return testBatchIndexFrom(t, time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC), 9)
}

func testBatchIndexFrom(t *testing.T, start time.Time, recordingJobID int64) (BatchIndex, []StreamDayAllocation, []HourManifest) {
	return testBatchIndexFromWindowFrozenAt(t, start, recordingJobID, testBatchCutoff())
}

func testBatchCutoff() time.Time {
	return time.Date(2026, time.August, 21, 6, 59, 7, 0, time.UTC)
}

func testBatchIndexFromWindowFrozenAt(t *testing.T, start time.Time, recordingJobID int64, qualificationFrozenAt time.Time) (BatchIndex, []StreamDayAllocation, []HourManifest) {
	t.Helper()
	const batchID = "goodplus-20260821-generation-1"
	source := testSource(1, start)
	source.RecordingJobID = recordingJobID
	baseRequest := testRequest([]SourceClip{source})
	baseRequest.Qualification.FrozenAt = qualificationFrozenAt
	baseRequest.Qualification.EvidenceSHA = ""
	var err error
	baseRequest.Qualification, err = SealQualificationWindow(baseRequest.Qualification)
	if err != nil {
		t.Fatal(err)
	}
	hours := make([]BatchIndexHour, 0, 14*12)
	ledgerRefs := make([]AllocationLedgerRef, 0, 14)
	ledgers := make([]StreamDayAllocation, 0, 14)
	manifests := make([]HourManifest, 0, 14*12)
	for day := 0; day < 14; day++ {
		localDate := start.AddDate(0, 0, day).Format("2006-01-02")
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
	cutoff := testBatchCutoff()
	completedAt := baseRequest.Qualification.Days[0].CompletedAt
	for _, day := range baseRequest.Qualification.Days[1:] {
		if day.CompletedAt.After(completedAt) {
			completedAt = day.CompletedAt
		}
	}
	frozenRecordings := []FrozenRecording{{RecordingID: 377, PriorityOrdinal: 1, SelectionTier: "good+", QualificationSHA256: baseRequest.Qualification.EvidenceSHA, CompletedAt: completedAt, Timezone: "UTC", FolderName: "01_Europe_Italy_Bevagna_Piazza_Silvestri", NamingMetadata: metadata}}
	selectedWindowsSHA, _ := SelectedQualificationWindowsSHA256(frozenRecordings)
	authority := SelectionAuthority{SelectionBasis: OperatorApprovedSelectionBasis, OrderedRecordingIDSHA256: recordingIDsSHA([]int64{377}), Cutoff: cutoff, QualificationRunID: 44, QualificationRunFrozenAt: time.Date(2026, time.April, 1, 0, 0, 0, 0, time.UTC), QualificationRuleVersion: "recording-qualification-v1", QualificationCohortSHA256: strings.Repeat("c", 64), QualificationWindowsSHA256: strings.Repeat("d", 64), SelectedQualificationWindowsSHA256: selectedWindowsSHA}
	index := BatchIndex{SchemaVersion: 1, PolicyVersion: PlanPolicyVersion, AllocationSchemaVersion: 1, HourManifestSchemaVersion: 1, BatchID: batchID, Generation: 1, FrozenAt: cutoff, RecordingIDs: []int64{377}, SelectionAuthority: authority, FrozenRecordings: frozenRecordings, MediaTool: mediaTool, ExpectedLedgerCount: 14, ScheduledHourCount: 168, SourceClipCount: 1, SourceBytes: 10, FinalMediaCount: 1, AllocationLedgers: ledgerRefs, Hours: hours}
	index.FrozenDenominatorSHA256, _ = ComputeFrozenDenominatorSHA256(index.SelectionAuthority, index.FrozenRecordings, frozenDenominatorDays(index.AllocationLedgers))
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

func testSelectionResolver(index BatchIndex, ledgers []StreamDayAllocation) FrozenBatchEvidenceResolver {
	return testSelectionResolverWithWindowFrozenAt(index, ledgers, index.SelectionAuthority.Cutoff)
}

func testSelectionResolverWithWindowFrozenAt(index BatchIndex, ledgers []StreamDayAllocation, frozenAt time.Time) FrozenBatchEvidenceResolver {
	return func() (FrozenBatchEvidence, error) {
		windows := make([]QualificationWindow, len(index.FrozenRecordings))
		for i, recording := range index.FrozenRecordings {
			days := make([]QualifiedDay, 14)
			for day := range days {
				days[day] = ledgers[i*14+day].QualificationDay
			}
			windows[i] = QualificationWindow{RecordingID: recording.RecordingID, Timezone: recording.Timezone, Days: days, FrozenAt: frozenAt, EvidenceSHA: recording.QualificationSHA256}
		}
		return FrozenBatchEvidence{
			SelectionAuthority:      index.SelectionAuthority,
			FrozenRecordings:        append([]FrozenRecording(nil), index.FrozenRecordings...),
			QualificationWindows:    windows,
			FrozenDenominatorSHA256: index.FrozenDenominatorSHA256,
		}, nil
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

func TestHourManifestAllocationRejectsOutOfRangePlanHourWithoutPanic(t *testing.T) {
	for _, localHour := range []int{0, 13} {
		t.Run(fmt.Sprintf("hour_%d", localHour), func(t *testing.T) {
			plan := oneOutputPlan(t)
			allocation, ledger := testAllocation(plan)
			plan.LocalHour = localHour
			plan.HourID = canonicalHourIDValue(plan.BatchID, plan.RecordingID, plan.LocalDate, localHour, plan.Generation)
			plan.CoverageObjectKey = canonicalBatchCoverageKey(plan)
			if _, err := BuildHourManifestAllocation(allocation.ArtifactID, plan, ledger); err == nil {
				t.Fatal("out-of-range plan hour reached allocation indexing")
			}
		})
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
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(canonical, bytes.TrimSpace(want)) {
		t.Fatalf("canonical fixture changed\n%s", canonical)
	}
}
