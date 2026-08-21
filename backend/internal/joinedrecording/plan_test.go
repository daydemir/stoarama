package joinedrecording

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/recordingnaming"
	"github.com/daydemir/stoarama/backend/internal/stitchcert"
)

const (
	testSourceAuthority = "0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com"
	testSourceEndpoint  = "https://" + testSourceAuthority
)

func testSource(id int64, start time.Time) SourceClip {
	return SourceClip{
		ClipID: id, RecordingID: 377, RecordingJobID: 9, Provider: "r2", Endpoint: testSourceEndpoint, Region: "auto", Bucket: "recordings", StartUTC: start, EndUTC: start.Add(time.Minute),
		Object:         ObjectIdentity{Key: "raw/" + start.Format("150405") + ".mp4", ETag: "etag", SizeBytes: 10, SHA256: strings.Repeat("b", 64)},
		SeamToPrevious: SeamEvidence{Verdict: "continuous", Reason: "packet_frame_audio_proof"},
	}
}

func testRequest(sources []SourceClip) PlanRequest {
	recordingID := sources[0].RecordingID
	firstDay := time.Date(sources[0].StartUTC.Year(), sources[0].StartUTC.Month(), sources[0].StartUTC.Day(), 8, 0, 0, 0, time.UTC)
	days := make([]QualifiedDay, 14)
	for i := range days {
		start := firstDay.AddDate(0, 0, i)
		jobID := sources[0].RecordingJobID
		if i > 0 {
			jobID += int64(1000 + i)
		}
		days[i] = QualifiedDay{LocalDate: start.Format("2006-01-02"), JobID: jobID, WindowStart: start, WindowEnd: start.Add(12 * time.Hour), CompletedAt: start.Add(12 * time.Hour), QualityTier: "good+"}
	}
	qualification, err := SealQualificationWindow(QualificationWindow{RecordingID: recordingID, Timezone: "UTC", Days: days, FrozenAt: time.Date(2027, time.January, 1, 0, 0, 0, 0, time.UTC)})
	if err != nil {
		panic(err)
	}
	mediaTool, err := SealMediaToolEvidence(MediaToolEvidence{FFmpegVersion: "ffmpeg pinned", FFmpegSHA256: strings.Repeat("e", 64), FFprobeVersion: "ffprobe pinned", FFprobeSHA256: strings.Repeat("f", 64)})
	if err != nil {
		panic(err)
	}
	local := sources[0].StartUTC
	return PlanRequest{BatchID: "goodplus-20260821-generation-1", Generation: 1, RecordingID: recordingID, Timezone: "UTC", LocalDate: local.Format("2006-01-02"), DeliveryHour: local.Hour() - 7, Metadata: recordingnaming.Metadata{PlazaID: "1", Continent: "Europe", Country: "Italy", City: "Bevagna", PlazaName: "Piazza Silvestri"}, Qualification: qualification, AllocationLedgerSHA: strings.Repeat("c", 64), MediaTool: mediaTool, Sources: sources}
}

func buildTestPlan(req PlanRequest) (BatchPlan, error) {
	ledger, err := testLedger(req, req.LocalDate)
	if err != nil {
		return BatchPlan{}, err
	}
	req.AllocationLedgerSHA = ledger.LedgerSHA256
	draft, err := DiscoverHourPlan(req)
	if err != nil {
		return BatchPlan{}, err
	}
	parts := len(draft.Parts)
	req.BuiltArtifacts = make([]BuiltArtifactIdentity, parts)
	for i := range req.BuiltArtifacts {
		sum := sha256.Sum256([]byte(fmt.Sprintf("%d/%d/%d", req.RecordingID, req.Sources[0].ClipID, i)))
		req.BuiltArtifacts[i] = BuiltArtifactIdentity{SizeBytes: int64(100 + i), SHA256: hex.EncodeToString(sum[:]), MediaToolIdentity: req.MediaTool.IdentitySHA256}
	}
	return BuildPlan(req)
}

func testLedger(req PlanRequest, localDate string) (StreamDayAllocation, error) {
	accounted, err := mergeAccountedSources(req.Sources, req.QuarantinedSources)
	if err != nil {
		return StreamDayAllocation{}, err
	}
	ledgerReq := req
	ledgerReq.Sources, ledgerReq.QuarantinedSources = accounted, nil
	var draft StreamDayDraft
	if len(accounted) == 0 {
		draft, err = BuildGapOnlyStreamDay(ledgerReq, localDate)
	} else {
		draft, err = AllocateStreamDay(ledgerReq)
	}
	if err != nil {
		return StreamDayAllocation{}, err
	}
	return SealStreamDayAllocation(draft)
}

func TestBuildPlanCleanHour(t *testing.T) {
	start := time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC)
	sources := make([]SourceClip, 60)
	for i := range sources {
		sources[i] = testSource(int64(i+1), start.Add(time.Duration(i)*time.Minute))
	}
	plan, err := buildTestPlan(testRequest(sources))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Outputs) != 1 || plan.Outputs[0].Part != 1 || plan.Outputs[0].Parts != 1 || !strings.Contains(plan.Outputs[0].RelativePath, "hour_01_080000-090000.mp4") {
		t.Fatalf("unexpected clean plan: %+v", plan.Outputs)
	}
	if plan.ExpectedOutputCount != 1 || plan.Outputs[0].ObjectKey != "joined/"+plan.BatchID+"/objects/"+plan.Outputs[0].ContentID+".mp4" {
		t.Fatalf("unsealed plan: %+v", plan)
	}
}

func TestBuildPlanKnownGapCreatesVisiblePartsAndNoOmission(t *testing.T) {
	start := time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC)
	sources := []SourceClip{testSource(1, start), testSource(2, start.Add(time.Minute)), testSource(3, start.Add(4*time.Minute)), testSource(4, start.Add(5*time.Minute))}
	sources[2].SeamToPrevious = SeamEvidence{Verdict: "gap", Reason: "missing_capture_sequence", SignedGapNanoseconds: 120 * int64(time.Second)}
	plan, err := buildTestPlan(testRequest(sources))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Outputs) != 2 || len(plan.Gaps) != 1 || plan.Gaps[0].SignedGapNanoseconds != 120*int64(time.Second) {
		t.Fatalf("gap not preserved: outputs=%d gaps=%+v", len(plan.Outputs), plan.Gaps)
	}
	seen := map[int64]bool{}
	for i, output := range plan.Outputs {
		if output.Part != i+1 || output.Parts != 2 || !strings.Contains(output.RelativePath, "_part_0") {
			t.Fatalf("bad visible part: %+v", output)
		}
		for _, source := range output.Sources {
			if seen[source.ClipID] {
				t.Fatal("source assigned twice")
			}
			seen[source.ClipID] = true
		}
	}
	if len(seen) != len(sources) {
		t.Fatal("source omitted")
	}
}

func TestBuildPlanRejectsFalseContinuousSeamOverRealGap(t *testing.T) {
	start := time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC)
	sources := []SourceClip{testSource(1, start), testSource(2, start.Add(3*time.Minute))}
	sources[1].SeamToPrevious = SeamEvidence{Verdict: "continuous", Reason: "asserted_without_timing", SignedGapNanoseconds: 0}
	if _, err := buildTestPlan(testRequest(sources)); err == nil {
		t.Fatal("false continuous seam over a real gap was accepted")
	}
}

func TestSourceClaimIgnoresDownloadedAudioAndSeamEvidence(t *testing.T) {
	source := testSource(1, time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC))
	want, _, err := CanonicalSourceClaim([]SourceClip{source})
	if err != nil {
		t.Fatal(err)
	}
	source.AudioContract = &AudioSequenceContract{CodecName: "aac", SampleRate: 48000, Channels: 2, ChannelLayout: "stereo"}
	source.SeamToPrevious = SeamEvidence{Verdict: "incompatible", Reason: "derived_probe", SignedGapNanoseconds: -1}
	got, _, err := CanonicalSourceClaim([]SourceClip{source})
	if err != nil || got != want {
		t.Fatalf("derived media evidence changed source claim: %s != %s (%v)", got, want, err)
	}
	if err := ValidateSourceClaim([]SourceClip{source}, want); err != nil {
		t.Fatalf("canonical source claim did not validate: %v", err)
	}
	if err := ValidateSourceClaim([]SourceClip{source}, strings.Repeat("0", 64)); err == nil {
		t.Fatal("mismatched canonical source claim validated")
	}
}

func TestCanonicalHourAndRecordingIdentityHelpers(t *testing.T) {
	hourID, err := CanonicalHourID("batch-1", 377, "2026-05-04", 1, 2)
	if err != nil || hourID != "batch-1__recording-377__date-2026-05-04__hour-01__generation-2" {
		t.Fatalf("canonical hour identity=%q err=%v", hourID, err)
	}
	if _, err := CanonicalHourID("batch-1", 377, "2026-05-04", 13, 2); err == nil {
		t.Fatal("invalid delivery hour received an identity")
	}
	ledgerID, err := CanonicalLedgerID("batch-1", 377, "2026-05-04", 2)
	if err != nil || ledgerID != "batch-1__recording-377__date-2026-05-04__generation-2" {
		t.Fatalf("canonical ledger identity=%q err=%v", ledgerID, err)
	}
	payload := []byte("377\n335\n")
	want := sha256.Sum256(payload)
	got, err := RecordingIDsSHA256([]int64{377, 335})
	if err != nil || got != hex.EncodeToString(want[:]) {
		t.Fatalf("recording identity hash=%q err=%v", got, err)
	}
	if _, err := RecordingIDsSHA256([]int64{377, 377}); err == nil {
		t.Fatal("duplicate recording identity list hashed")
	}
}

func TestMixedQuarantineBreaksBothAdjacentMediaRuns(t *testing.T) {
	start := time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC)
	first, quarantined, third := testSource(1, start), testSource(2, start.Add(time.Minute)), testSource(3, start.Add(2*time.Minute))
	req := testRequest([]SourceClip{first, third})
	req.QuarantinedSources = []SourceClip{quarantined}
	plan, err := buildTestPlan(req)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Outputs) != 2 || len(plan.Gaps) != 2 || plan.Outputs[0].Sources[0].ClipID != 1 || plan.Outputs[1].Sources[0].ClipID != 3 || plan.Gaps[0].PreviousClipID != 1 || plan.Gaps[0].NextClipID != 2 || plan.Gaps[1].PreviousClipID != 2 || plan.Gaps[1].NextClipID != 3 {
		t.Fatalf("mixed source order bridged quarantine: %+v", plan)
	}
}

func TestBuildPlanCuscoLikeIncompatibleStateNeverBridges(t *testing.T) {
	start := time.Date(2026, time.May, 4, 9, 0, 0, 0, time.UTC)
	sources := []SourceClip{testSource(1, start), testSource(2, start.Add(time.Minute)), testSource(3, start.Add(2*time.Minute))}
	sources[2].SeamToPrevious = SeamEvidence{Verdict: "incompatible", Reason: "decoder_state_changed"}
	plan, err := buildTestPlan(testRequest(sources))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Outputs) != 2 || plan.Gaps[0].Reason != "decoder_state_changed" || plan.Outputs[0].Hour != 2 {
		t.Fatalf("incompatible seam bridged: %+v", plan)
	}
}

func TestStreamDayAllocationProvesNoOverlapOrOmission(t *testing.T) {
	start := time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC)
	sources := make([]SourceClip, 120)
	for i := range sources {
		sources[i] = testSource(int64(i+1), start.Add(time.Duration(i)*time.Minute))
	}
	req := testRequest(sources)
	draft, err := AllocateStreamDay(req)
	if err != nil {
		t.Fatal(err)
	}
	firstReq := req
	firstReq.Sources = sources[:60]
	first, err := buildTestPlan(firstReq)
	if err != nil {
		t.Fatal(err)
	}
	secondReq := req
	secondReq.Sources = sources[60:]
	second, err := buildTestPlan(secondReq)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidatePlan(first); err != nil {
		t.Fatalf("first hour plan invalid: %v", err)
	}
	if err := ValidatePlan(second); err != nil {
		t.Fatalf("second hour plan invalid: %v", err)
	}
	plans := []BatchPlan{first, second}
	gapReq := req
	gapReq.Sources = nil
	gapReq.BuiltArtifacts = nil
	for hour := 3; hour <= 12; hour++ {
		gap, gapErr := BuildGapOnlyHourPlan(gapReq, first.LocalDate, hour, "no_source_clips")
		if gapErr != nil {
			t.Fatal(gapErr)
		}
		plans = append(plans, gap)
	}
	ledger, err := SealStreamDayAllocation(draft)
	if err != nil || ValidateStreamDayAllocation(ledger, draft) != nil {
		t.Fatalf("valid stream-day allocation rejected: %+v %v", ledger, err)
	}
	omitted := draft
	omitted.Hours = append([]HourAllocation(nil), draft.Hours...)
	omitted.Hours[1].Sources = nil
	if ValidateStreamDayAllocation(ledger, omitted) == nil {
		t.Fatal("omitted source was accepted")
	}
}

func TestAllocateStreamDayChoosesClosestVerifiedBoundarySeam(t *testing.T) {
	for _, tc := range []struct {
		name          string
		start         time.Time
		end           time.Time
		wantFirstHour int
	}{
		{name: "after boundary nearest", start: time.Date(2026, time.May, 4, 8, 57, 0, 0, time.UTC), end: time.Date(2026, time.May, 4, 9, 1, 0, 0, time.UTC), wantFirstHour: 1},
		{name: "before boundary nearest", start: time.Date(2026, time.May, 4, 8, 59, 0, 0, time.UTC), end: time.Date(2026, time.May, 4, 9, 3, 0, 0, time.UTC), wantFirstHour: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			first := testSource(1, tc.start)
			first.EndUTC = tc.end
			second := testSource(2, tc.end)
			req := testRequest([]SourceClip{first, second})
			draft, err := AllocateStreamDay(req)
			if err != nil {
				t.Fatal(err)
			}
			boundary := draft.Boundaries[0]
			if len(draft.Hours[0].Sources) != tc.wantFirstHour || len(draft.Boundaries) != 11 || (boundary.ActualSeamUTC == nil) == (boundary.PreviousClipID != nil && boundary.NextClipID != nil) {
				t.Fatalf("wrong closest-seam allocation: %+v", draft.Boundaries[0])
			}
		})
	}
}

func TestAllocateStreamDayAcceptsEqualStartsBySourceID(t *testing.T) {
	start := time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC)
	first, second := testSource(1, start), testSource(2, start)
	second.EndUTC = start.Add(2 * time.Minute)
	second.Object.Key = "raw/equal-start-2.mp4"
	draft, err := AllocateStreamDay(testRequest([]SourceClip{first, second}))
	if err != nil || len(draft.Hours[0].Sources) != 2 || draft.Hours[0].Sources[0].ClipID != 1 || draft.Hours[0].Sources[1].ClipID != 2 {
		t.Fatalf("equal-start frozen ordering differs: %+v %v", draft, err)
	}
}

func TestSealStreamDayRejectsFabricatedBoundary(t *testing.T) {
	start := time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC)
	draft, err := AllocateStreamDay(testRequest([]SourceClip{testSource(1, start), testSource(2, start.Add(time.Minute))}))
	if err != nil {
		t.Fatal(err)
	}
	draft.Boundaries[0].ScheduledUTC = draft.Boundaries[0].ScheduledUTC.Add(time.Nanosecond)
	if _, err := SealStreamDayAllocation(draft); err == nil {
		t.Fatal("fabricated hour boundary was sealed")
	}
}

func TestCanonicalAllocationLedgerRejectsRehashedFabricatedBoundary(t *testing.T) {
	start := time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC)
	draft, err := AllocateStreamDay(testRequest([]SourceClip{testSource(1, start), testSource(2, start.Add(time.Minute))}))
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := SealStreamDayAllocation(draft)
	if err != nil {
		t.Fatal(err)
	}
	ledger.Boundaries[0].ScheduledUTC = ledger.Boundaries[0].ScheduledUTC.Add(time.Nanosecond)
	logical := ledger
	logical.LedgerSHA256 = ""
	ledger.LedgerSHA256, _, _ = stitchcert.CanonicalSHA(logical)
	if _, _, err := CanonicalAllocationLedgerArtifact(ledger); err == nil {
		t.Fatal("self-consistent hash accepted a fabricated boundary")
	}
}

func TestCanonicalAllocationLedgerRejectsDuplicateStorageAndCrossDayForgery(t *testing.T) {
	start := time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC)
	req := testRequest([]SourceClip{testSource(1, start), testSource(2, start.Add(time.Minute))})
	previous := testSource(9, start.Add(-12*time.Hour))
	previous.EndUTC = start.Add(-12*time.Hour + time.Minute)
	req.PreviousDayLast = &previous
	draft, err := AllocateStreamDay(req)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := SealStreamDayAllocation(draft)
	if err != nil {
		t.Fatal(err)
	}

	duplicate := ledger
	duplicate.Sources = append([]SourceClip(nil), ledger.Sources...)
	duplicate.Sources[1].Object = duplicate.Sources[0].Object
	resealMutatedLedger(&duplicate)
	if _, _, err := CanonicalAllocationLedgerArtifact(duplicate); err == nil {
		t.Fatal("duplicate frozen storage object accepted")
	}

	wrongEdge := ledger
	wrongEdge.CrossDayBoundaries = append([]CrossDayBoundary(nil), ledger.CrossDayBoundaries...)
	wrong := int64(999)
	wrongEdge.CrossDayBoundaries[0].NextClipID = &wrong
	resealMutatedLedger(&wrongEdge)
	if _, _, err := CanonicalAllocationLedgerArtifact(wrongEdge); err == nil {
		t.Fatal("cross-day boundary detached from first source")
	}

	wrongSkew := ledger
	wrongSkew.CrossDayBoundaries = append([]CrossDayBoundary(nil), ledger.CrossDayBoundaries...)
	*wrongSkew.CrossDayBoundaries[0].BoundarySkewNanoseconds++
	resealMutatedLedger(&wrongSkew)
	if _, _, err := CanonicalAllocationLedgerArtifact(wrongSkew); err == nil {
		t.Fatal("fabricated cross-day skew accepted")
	}
}

func TestAllocationLedgerRejectsUnsafeIdentityAndCrossBatchPublication(t *testing.T) {
	if _, _, err := CanonicalAllocationLedgerPaths("../escape", 377, "2026-05-04"); err == nil {
		t.Fatal("unsafe batch path accepted")
	}
	ledger, err := testLedger(testRequest([]SourceClip{testSource(1, time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC))}), "2026-05-04")
	if err != nil {
		t.Fatal(err)
	}
	canonical, sha, err := CanonicalAllocationLedgerArtifact(ledger)
	if err != nil {
		t.Fatal(err)
	}
	claim := LedgerPublicationClaim{ProtocolVersion: JoinedProtocolVersion, ArtifactID: 1, ScopeID: canonicalLedgerID("other-batch", ledger.RecordingID, ledger.LocalDate, ledger.Generation), LeaseID: strings.Repeat("L", 43), OperationToken: strings.Repeat("t", 32), LeaseExpires: time.Now().Add(time.Hour), StorageAuthority: testSourceAuthority, StorageBucket: "recordings", BatchID: "other-batch", Ledger: ledger, ExpectedSize: int64(len(canonical)), ExpectedSHA256: sha}
	if _, _, err := claim.Validate(time.Now()); err == nil {
		t.Fatal("ledger published through another batch identity")
	}
}

func resealMutatedLedger(ledger *StreamDayAllocation) {
	hours := make([][]SourceClip, 12)
	cursor := 0
	for i, hour := range ledger.Hours {
		for range hour.SourceClipIDs {
			hours[i] = append(hours[i], ledger.Sources[cursor])
			cursor++
		}
		ledger.HourSourceSHA256[i], _, _ = sourceClaimSHA(hours[i])
	}
	ledger.SourceClaimSHA256, _, _ = sourceClaimSHA(ledger.Sources)
	logical := *ledger
	logical.LedgerSHA256 = ""
	ledger.LedgerSHA256, _, _ = stitchcert.CanonicalSHA(logical)
}

func TestGapOnlyStreamDayPreservesOneSidedNeighborsAcrossDST(t *testing.T) {
	localDate := "2026-11-01"
	localStart := time.Date(2026, time.November, 1, 8, 0, 0, 0, time.FixedZone("placeholder", -5*60*60))
	req := testRequest([]SourceClip{testSource(1, localStart.UTC())})
	req.Sources = nil
	req.Timezone = "America/New_York"
	req.LocalDate = localDate
	loc, _ := time.LoadLocation(req.Timezone)
	days := make([]QualifiedDay, 14)
	firstDay, _ := time.ParseInLocation("2006-01-02", localDate, loc)
	for i := range days {
		date := firstDay.AddDate(0, 0, i)
		start := time.Date(date.Year(), date.Month(), date.Day(), 8, 0, 0, 0, loc)
		end := time.Date(date.Year(), date.Month(), date.Day(), 20, 0, 0, 0, loc)
		days[i] = QualifiedDay{LocalDate: date.Format("2006-01-02"), JobID: int64(900 + i), WindowStart: start.UTC(), WindowEnd: end.UTC(), CompletedAt: end.UTC(), QualityTier: "good+"}
	}
	req.Qualification, _ = SealQualificationWindow(QualificationWindow{RecordingID: req.RecordingID, Timezone: req.Timezone, Days: days, FrozenAt: time.Date(2027, time.January, 1, 0, 0, 0, 0, time.UTC)})
	previousStart := time.Date(2026, time.October, 31, 19, 59, 0, 0, loc).UTC()
	nextStart := time.Date(2026, time.November, 2, 8, 0, 0, 0, loc).UTC()
	previous, next := testSource(8, previousStart), testSource(9, nextStart)
	previous.EndUTC = time.Date(2026, time.October, 31, 20, 0, 0, 0, loc).UTC()
	next.EndUTC = next.StartUTC.Add(time.Minute)
	previous.RecordingID, next.RecordingID = req.RecordingID, req.RecordingID
	req.PreviousDayLast, req.NextDayFirst = &previous, &next
	draft, err := BuildGapOnlyStreamDay(req, localDate)
	if err != nil {
		t.Fatal(err)
	}
	if draft.CrossDayBoundaries[0].PreviousClipID == nil || *draft.CrossDayBoundaries[0].PreviousClipID != previous.ClipID || draft.CrossDayBoundaries[0].NextClipID != nil || draft.CrossDayBoundaries[1].PreviousClipID != nil || draft.CrossDayBoundaries[1].NextClipID == nil || *draft.CrossDayBoundaries[1].NextClipID != next.ClipID {
		t.Fatalf("empty-day neighbor evidence was dropped: %+v", draft.CrossDayBoundaries)
	}
	ledger, err := SealStreamDayAllocation(draft)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := CanonicalAllocationLedgerArtifact(ledger); err != nil {
		t.Fatal(err)
	}
}

func TestAllocateStreamDayRejectsCallerLocalDateMismatch(t *testing.T) {
	req := testRequest([]SourceClip{testSource(1, time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC))})
	req.LocalDate = "2026-05-05"
	if _, err := AllocateStreamDay(req); err == nil {
		t.Fatal("caller-supplied wrong local date accepted")
	}
}

func TestFrozenTier1PayloadAndSealedEvidence(t *testing.T) {
	sum := sha256.Sum256(Tier1Payload())
	if got := hex.EncodeToString(sum[:]); got != Tier1RecordingIDSHA {
		t.Fatalf("tier-1 hash=%s", got)
	}
	if len(Tier1RecordingIDs) != 33 {
		t.Fatalf("tier-1 count=%d", len(Tier1RecordingIDs))
	}
}
