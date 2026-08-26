package joinedrecording

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func oneOutputPlan(t *testing.T, artifact ...BuiltArtifactIdentity) BatchPlan {
	t.Helper()
	start := time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC)
	req := testRequest([]SourceClip{testSource(1, start)})
	var plan BatchPlan
	var err error
	if len(artifact) == 1 {
		ledger, ledgerErr := testLedger(req, req.LocalDate)
		if ledgerErr != nil {
			t.Fatal(ledgerErr)
		}
		req.AllocationLedgerSHA = ledger.LedgerSHA256
		artifact[0].MediaToolIdentity = req.MediaTool.IdentitySHA256
		req.BuiltArtifacts = artifact
		plan, err = BuildPlan(req)
	} else {
		plan, err = buildTestPlan(req)
	}
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func passingVerification() Verification {
	track := &TrackFingerprint{MediaType: "video", PacketCount: 1, PacketChainSHA256: strings.Repeat("a", 64), PacketTimingSHA256: strings.Repeat("b", 64), PacketTimeBases: []string{"1/1"}, FirstPacketPTSSeconds: "0", LastPacketPTSSeconds: "0", FirstPacketDTSSeconds: "0", LastPacketDTSSeconds: "0", PacketDurationSeconds: "1", DecodeTimelineSpanSeconds: "1", DecodedFrames: 1, FirstTimestamp: 0, LastTimestamp: 0, TimestampStatus: "monotonic"}
	sourceTrack := *track
	sourceTrack.TimestampStatus = "source_clips_independent"
	return Verification{Status: "passed", PacketPayloadOrderStatus: "passed", DecodedFrameTotalsStatus: "passed", DecodedAudioTotalsStatus: "passed", OutputTimestampStatus: "passed", StrictDecodeStatus: "passed", SourceFingerprint: MediaFingerprint{DurationSeconds: 60, Tracks: map[string]*TrackFingerprint{"video": &sourceTrack}}, OutputFingerprint: MediaFingerprint{DurationSeconds: 60, Tracks: map[string]*TrackFingerprint{"video": track}}}
}

func testAllocation(plan BatchPlan) (HourManifestAllocation, StreamDayAllocation) {
	req := PlanRequest{BatchID: plan.BatchID, Generation: plan.Generation, RecordingID: plan.RecordingID, Timezone: plan.Timezone, LocalDate: plan.LocalDate, DeliveryHour: plan.LocalHour, Qualification: plan.Qualification, Sources: plan.Sources}
	ledger, err := testLedger(req, plan.LocalDate)
	if err != nil {
		panic(err)
	}
	allocation, err := BuildHourManifestAllocation(55, plan, ledger)
	if err != nil {
		panic(err)
	}
	return allocation, ledger
}

func sealedClaim(t *testing.T, _ int64, plan BatchPlan, built []BuiltOutput, quarantine []QuarantineEvidence) WorkerClaim {
	t.Helper()
	artifactIDs := make([]int64, len(plan.Outputs))
	for i := range artifactIDs {
		artifactIDs[i] = int64(100 + i)
	}
	allocation, ledger := testAllocation(plan)
	manifest, manifestJSON, manifestSHA, err := BuildHourManifest(HourManifestInput{Plan: plan, Allocation: allocation, AllocationLedger: ledger, MediaArtifactIDs: artifactIDs, Built: built, QuarantineEvidence: quarantine})
	if err != nil {
		t.Fatal(err)
	}
	manifestArtifactID := int64(999)
	return WorkerClaim{ProtocolVersion: JoinedProtocolVersion, HourID: plan.HourID, LeaseID: strings.Repeat("L", 43), OperationToken: strings.Repeat("p", 32), LeaseExpires: time.Now().Add(time.Hour), StorageAuthority: testSourceAuthority, StorageBucket: "recordings", Plan: plan, Allocation: allocation, AllocationLedger: ledger, HourManifest: manifest, MediaArtifactIDs: artifactIDs, HourManifestArtifactID: manifestArtifactID, HourManifestExpectedSize: int64(len(manifestJSON)), HourManifestExpectedSHA: manifestSHA}
}

func testCreateResolver() CreateCapabilityResolver {
	return func(_ context.Context, claim WorkerClaim, artifactID int64) (ObjectCreateCapability, error) {
		if artifactID == claim.HourManifestArtifactID {
			return createCapabilityIdentity(artifactID, claim.StorageBucket, claim.Plan.CoverageObjectKey, "application/json", claim.HourManifestExpectedSize, claim.HourManifestExpectedSHA), nil
		}
		for i, id := range claim.MediaArtifactIDs {
			if id == artifactID {
				output := claim.Plan.Outputs[i]
				return createCapabilityIdentity(id, claim.StorageBucket, output.ObjectKey, "video/mp4", output.ExpectedSize, output.ExpectedSHA), nil
			}
		}
		return ObjectCreateCapability{}, errors.New("unknown artifact")
	}
}

func testReadResolver(client *memoryCapabilityClient) ReadCapabilityResolver {
	return func(_ context.Context, claim WorkerClaim, artifactID int64) (ObjectReadCapability, error) {
		key := claim.Plan.CoverageObjectKey
		for i, id := range claim.MediaArtifactIDs {
			if id == artifactID {
				key = claim.Plan.Outputs[i].ObjectKey
			}
		}
		body, ok := client.objects[key]
		if !ok {
			return ObjectReadCapability{}, errors.New("object unavailable")
		}
		return readCapability(artifactID, claim.StorageBucket, key, body), nil
	}
}

func bindTestScratch(t *testing.T, claim WorkerClaim, built []BuiltOutput, quarantine []QuarantineEvidence, directory string) SealedHourScratch {
	t.Helper()
	scratch, err := bindSealedHourScratch(verifiedHourScratch{HourID: claim.HourID, SourceClaimSHA256: claim.Plan.SourceClaimSHA256, OriginLeaseID: claim.LeaseID, Directory: directory, Built: built, Quarantine: quarantine}, claim)
	if err != nil {
		t.Fatal(err)
	}
	return scratch
}

func noHeartbeat(_ context.Context, _ string) (OperationCredentials, error) {
	return OperationCredentials{}, errors.New("unexpected heartbeat")
}

func TestPublishClaimedHourTimesEarlyLocalVerificationFailure(t *testing.T) {
	req := testRequest([]SourceClip{testSource(1, time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC))})
	req.Sources = nil
	ledger, err := testLedger(req, "2026-05-04")
	if err != nil {
		t.Fatal(err)
	}
	req.AllocationLedgerSHA = ledger.LedgerSHA256
	plan, err := BuildGapOnlyHourPlan(req, "2026-05-04", 2, "no_source_clips")
	if err != nil {
		t.Fatal(err)
	}
	claim := sealedClaim(t, 7, plan, nil, nil)
	sealedScratch, err := BindReclaimedGapOnlyHourScratch(claim, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sealedScratch.verified.Quarantine = []QuarantineEvidence{{}}
	var events []StageTimingEvent
	ctx := WithStageTimingObserver(context.Background(), func(event StageTimingEvent) { events = append(events, event) })
	_, err = publishClaimedHour(ctx, &memoryCapabilityClient{objects: map[string][]byte{}}, claim, sealedScratch, testCreateResolver(), func(context.Context, WorkerClaim, int64) (ObjectReadCapability, error) {
		return ObjectReadCapability{}, errors.New("unexpected read")
	}, func(context.Context, WorkerClaim, PublishedHour) error { return errors.New("unexpected finalize") })
	if err == nil || len(events) != 1 || events[0].Stage != "upload_verify" || events[0].Outcome != "error" {
		t.Fatalf("early verification timing events=%+v err=%v", events, err)
	}
	if !reflect.DeepEqual(events[0], StageTimingEvent{Stage: "upload_verify", ElapsedMS: events[0].ElapsedMS, Outcome: "error"}) {
		t.Fatalf("unexpected timing fields: %+v", events[0])
	}
}

func TestPublishClaimedHourReconcilesImmutableOrphanAndCleansOnlyScratch(t *testing.T) {
	media := []byte("verified joined media")
	mediaSum := sha256.Sum256(media)
	plan := oneOutputPlan(t, BuiltArtifactIdentity{SizeBytes: int64(len(media)), SHA256: hex.EncodeToString(mediaSum[:]), MediaToolIdentity: strings.Repeat("f", 64)})
	built := BuiltOutput{SizeBytes: int64(len(media)), SHA256: hex.EncodeToString(mediaSum[:]), SourceCount: 1, Verification: passingVerification()}
	claim := sealedClaim(t, 7, plan, []BuiltOutput{built}, nil)
	root := t.TempDir()
	scratch, err := claim.ScratchDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(scratch, 0700); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(root, "keep.txt")
	if err := os.WriteFile(outside, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(scratch, "downloaded-source.mp4"), []byte("source"), 0600); err != nil {
		t.Fatal(err)
	}
	mediaPath := filepath.Join(scratch, "joined.mp4")
	if err := os.WriteFile(mediaPath, media, 0600); err != nil {
		t.Fatal(err)
	}
	size, sha, err := localIdentity(mediaPath)
	if err != nil {
		t.Fatal(err)
	}
	built.Path, built.SizeBytes, built.SHA256 = mediaPath, size, sha
	sealedScratch := bindTestScratch(t, claim, []BuiltOutput{built}, nil, scratch)
	client := &memoryCapabilityClient{objects: map[string][]byte{}}
	if _, err := PublishClaimedHourRenewing(context.Background(), client, testSourceAuthority, claim, sealedScratch, noHeartbeat, testCreateResolver(), testReadResolver(client), func(context.Context, WorkerClaim, PublishedHour) error { return errors.New("database unavailable") }); err == nil {
		t.Fatal("database orphan was reported as finalized")
	}
	if _, err := os.Stat(mediaPath); err != nil {
		t.Fatal("scratch output removed before fenced finalize")
	}
	if len(client.objects) != 2 {
		t.Fatalf("objects=%d want media+hour coverage", len(client.objects))
	}
	var finalized bool
	published, err := PublishClaimedHourRenewing(context.Background(), client, testSourceAuthority, claim, sealedScratch, noHeartbeat, testCreateResolver(), testReadResolver(client), func(_ context.Context, got WorkerClaim, output PublishedHour) error {
		finalized = got.OperationToken == claim.OperationToken && output.HourID == claim.HourID
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !finalized || published.Outputs[0].Created || !lowerHex64(published.HourManifestSHA256) {
		t.Fatalf("orphan not reconciled: %+v", published)
	}
	if _, err := os.Stat(scratch); !os.IsNotExist(err) {
		t.Fatal("finalized current-lease scratch tree was not removed")
	}
	if data, err := os.ReadFile(outside); err != nil || string(data) != "keep" {
		t.Fatalf("cleanup changed data outside current lease scratch: %q err=%v", data, err)
	}
}

func TestWorkerClaimRejectsExpiredAndMutatedTasks(t *testing.T) {
	plan := oneOutputPlan(t)
	built := []BuiltOutput{{SizeBytes: plan.Outputs[0].ExpectedSize, SHA256: plan.Outputs[0].ExpectedSHA, SourceCount: len(plan.Outputs[0].Sources), Verification: passingVerification()}}
	claim := sealedClaim(t, 7, plan, built, nil)
	claim.LeaseExpires = time.Now().Add(-time.Second)
	if err := claim.Validate(time.Now()); err == nil {
		t.Fatal("expired claim accepted")
	}
	claim = sealedClaim(t, 7, plan, built, nil)
	claim.Plan.Outputs[0].ExpectedSHA = strings.Repeat("0", 64)
	if err := claim.Validate(time.Now()); err == nil {
		t.Fatal("mutated sealed task accepted")
	}
}

func TestReclaimedPublisherCannotBindPriorLeaseScratch(t *testing.T) {
	plan := oneOutputPlan(t)
	built := []BuiltOutput{{SizeBytes: plan.Outputs[0].ExpectedSize, SHA256: plan.Outputs[0].ExpectedSHA, SourceCount: len(plan.Outputs[0].Sources), Verification: passingVerification()}}
	prior := sealedClaim(t, 7, plan, built, nil)
	reclaimed := prior
	reclaimed.LeaseID = strings.Repeat("R", 43)
	reclaimed.OperationToken = strings.Repeat("r", 32)
	reclaimed.LeaseExpires = time.Now().Add(time.Hour)
	root := t.TempDir()
	priorDir, err := prior.ScratchDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := BindRebuiltSealedHourScratch(reclaimed, root, priorDir, built, nil); err == nil {
		t.Fatal("reclaimed lease bound prior lease scratch")
	}
	if _, err := BindReclaimedGapOnlyHourScratch(reclaimed, root); err == nil {
		t.Fatal("source-bearing reclaimed hour bound empty scratch")
	}
}

func TestPublishGapOnlyHourCreatesOnlyImmutableHourManifest(t *testing.T) {
	req := testRequest([]SourceClip{testSource(1, time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC))})
	req.Sources = nil
	ledger, err := testLedger(req, "2026-05-04")
	if err != nil {
		t.Fatal(err)
	}
	req.AllocationLedgerSHA = ledger.LedgerSHA256
	plan, err := BuildGapOnlyHourPlan(req, "2026-05-04", 2, "no_source_clips")
	if err != nil {
		t.Fatal(err)
	}
	claim := sealedClaim(t, 8, plan, nil, nil)
	client := &memoryCapabilityClient{objects: map[string][]byte{}}
	root := t.TempDir()
	sealedScratch, err := BindReclaimedGapOnlyHourScratch(claim, root)
	if err != nil {
		t.Fatal(err)
	}
	published, err := PublishClaimedHourRenewing(context.Background(), client, testSourceAuthority, claim, sealedScratch, noHeartbeat, testCreateResolver(), testReadResolver(client), func(context.Context, WorkerClaim, PublishedHour) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	manifestBytes := client.objects[plan.CoverageObjectKey]
	if len(client.objects) != 1 || len(published.Outputs) != 0 || published.HourManifestSHA256 != claim.HourManifestExpectedSHA || int64(len(manifestBytes)) != claim.HourManifestExpectedSize {
		t.Fatalf("gap-only publication emitted media: %+v objects=%d", published, len(client.objects))
	}
}

func TestReclaimedGapOnlyHourRejectsNonemptyScratch(t *testing.T) {
	req := testRequest([]SourceClip{testSource(1, time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC))})
	req.Sources = nil
	ledger, err := testLedger(req, "2026-05-04")
	if err != nil {
		t.Fatal(err)
	}
	req.AllocationLedgerSHA = ledger.LedgerSHA256
	plan, err := BuildGapOnlyHourPlan(req, "2026-05-04", 2, "no_source_clips")
	if err != nil {
		t.Fatal(err)
	}
	claim := sealedClaim(t, 8, plan, nil, nil)
	root := t.TempDir()
	directory, err := claim.ScratchDir(root)
	if err != nil || os.MkdirAll(directory, 0o700) != nil || os.WriteFile(filepath.Join(directory, "stale.part"), []byte("stale"), 0o600) != nil {
		t.Fatal(err)
	}
	if _, err := BindReclaimedGapOnlyHourScratch(claim, root); err == nil {
		t.Fatal("nonempty reclaimed gap-only scratch was accepted")
	}
}
