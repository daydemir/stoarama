package joinedrecording

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/recordingnaming"
)

func testSource(id int64, start time.Time) SourceClip {
	return SourceClip{
		ClipID: id, RecordingID: 377, RecordingJobID: 9, CaptureGeneration: "generation", CaptureSequence: id,
		NativeSignatureSHA256: strings.Repeat("a", 64), StartUTC: start, EndUTC: start.Add(time.Minute),
		Object:         ObjectIdentity{Key: "raw/" + start.Format("150405") + ".mp4", ETag: "etag", SizeBytes: 10, SHA256: strings.Repeat("b", 64)},
		SeamToPrevious: SeamEvidence{Verdict: "continuous", Reason: "packet_frame_audio_proof"},
	}
}

func testRequest(sources []SourceClip) PlanRequest {
	return PlanRequest{CampaignID: "goodplus-20260821", RecordingID: 377, Timezone: "UTC", Metadata: recordingnaming.Metadata{PlazaID: "1", Continent: "Europe", Country: "Italy", City: "Bevagna", PlazaName: "Piazza Silvestri"}, Sources: sources}
}

func TestBuildPlanCleanHour(t *testing.T) {
	start := time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC)
	sources := make([]SourceClip, 60)
	for i := range sources {
		sources[i] = testSource(int64(i+1), start.Add(time.Duration(i)*time.Minute))
	}
	plan, err := BuildPlan(testRequest(sources))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Outputs) != 1 || plan.Outputs[0].Part != 1 || plan.Outputs[0].Parts != 1 || !strings.Contains(plan.Outputs[0].RelativePath, "hour_01_080000-090000.mp4") {
		t.Fatalf("unexpected clean plan: %+v", plan.Outputs)
	}
	if plan.ExpectedOutputCount != 1 || !strings.HasPrefix(plan.Outputs[0].ObjectKey, "joined/"+plan.BatchID+"/") || !lowerHex64(plan.PlanSHA256) {
		t.Fatalf("unsealed plan: %+v", plan)
	}
}

func TestBuildPlanKnownGapCreatesVisiblePartsAndNoOmission(t *testing.T) {
	start := time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC)
	sources := []SourceClip{testSource(1, start), testSource(2, start.Add(time.Minute)), testSource(3, start.Add(4*time.Minute)), testSource(4, start.Add(5*time.Minute))}
	sources[2].SeamToPrevious = SeamEvidence{Verdict: "gap", Reason: "missing_capture_sequence", GapSeconds: 120}
	plan, err := BuildPlan(testRequest(sources))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Outputs) != 2 || len(plan.Gaps) != 1 || plan.Gaps[0].Seconds != 120 {
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

func TestBuildPlanCuscoLikeIncompatibleStateNeverBridges(t *testing.T) {
	start := time.Date(2026, time.May, 4, 9, 0, 0, 0, time.UTC)
	sources := []SourceClip{testSource(1, start), testSource(2, start.Add(time.Minute)), testSource(3, start.Add(2*time.Minute))}
	sources[2].NativeSignatureSHA256 = strings.Repeat("c", 64)
	sources[2].SeamToPrevious = SeamEvidence{Verdict: "incompatible", Reason: "decoder_state_changed"}
	plan, err := BuildPlan(testRequest(sources))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Outputs) != 2 || plan.Gaps[0].Reason != "decoder_state_changed" || plan.Outputs[0].Hour != 2 {
		t.Fatalf("incompatible seam bridged: %+v", plan)
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

func TestSealCampaignPinsAllTier1PlansInFrozenOrder(t *testing.T) {
	base := time.Date(2026, time.August, 21, 8, 0, 0, 0, time.UTC)
	campaign := CampaignManifest{CampaignID: "goodplus-20260821", Tier: 1, FrozenAt: Tier1FrozenAt, RecordingIDs: append([]int64(nil), Tier1RecordingIDs...), RecordingIDSHA256: Tier1RecordingIDSHA}
	plans := make([]BatchPlan, 0, len(Tier1RecordingIDs))
	for i, recordingID := range Tier1RecordingIDs {
		windowEnd := base.Add(time.Duration(i) * time.Hour)
		campaign.CompletionEvidence = append(campaign.CompletionEvidence, CompletionEvidence{RecordingID: recordingID, JobID: int64(i + 1), WindowEnd: windowEnd, CompletedAt: windowEnd, QualityTier: "good+"})
		source := testSource(int64(i+1), base)
		source.RecordingID = recordingID
		source.RecordingJobID = int64(i + 1)
		source.CaptureSequence = 1
		source.Object.Key = fmt.Sprintf("raw/%d.mp4", recordingID)
		req := testRequest([]SourceClip{source})
		req.RecordingID = recordingID
		req.Metadata.PlazaID = fmt.Sprintf("%d", i+1)
		plan, err := BuildPlan(req)
		if err != nil {
			t.Fatal(err)
		}
		plans = append(plans, plan)
	}
	sealed, err := SealCampaign(campaign, plans)
	if err != nil {
		t.Fatal(err)
	}
	if sealed.ExpectedOutputCount != 33 || !lowerHex64(sealed.PlanSHA256) || ValidateTier1Campaign(sealed) != nil {
		t.Fatalf("campaign not sealed: %+v", sealed)
	}
	plans[0], plans[1] = plans[1], plans[0]
	if _, err := SealCampaign(campaign, plans); err == nil {
		t.Fatal("out-of-order tier-1 plans were sealed")
	}
}
