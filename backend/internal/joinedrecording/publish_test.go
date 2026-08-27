package joinedrecording

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

type partFailureCapabilityClient struct {
	*memoryCapabilityClient
	failingKey string
	status     int
	puts       map[string]int
}

type partRereadFailureCapabilityClient struct {
	*memoryCapabilityClient
	failingKey string
	mode       string
	puts       map[string]int
}

func (*partRereadFailureCapabilityClient) joinedRedirectSafe() {}

func (c *partRereadFailureCapabilityClient) Do(request *http.Request) (*http.Response, error) {
	key := request.URL.Query().Get("key")
	if request.Method == http.MethodPut {
		c.puts[key]++
	}
	if request.Method != http.MethodGet || key != c.failingKey {
		return c.memoryCapabilityClient.Do(request)
	}
	if c.mode == "transport" {
		return nil, errors.New("transport-url-token-secret")
	}
	body := c.objects[key]
	var response *http.Response
	switch c.mode {
	case "status":
		response = capabilityResponse(http.StatusServiceUnavailable, []byte("response-body-secret"), "", "")
	case "identity":
		response = capabilityResponse(http.StatusOK, body, "wrong-etag", "version")
	case "hash":
		mutated := append([]byte(nil), body...)
		mutated[0] ^= 0xff
		response = capabilityResponse(http.StatusOK, mutated, objectETag(body), "version")
	default:
		return nil, errors.New("unknown reread test mode")
	}
	response.Header.Set("x-amz-request-id", "request-id-secret-token=abc")
	response.Header.Set("x-amz-id-2", "extended-secret/bearer+value=")
	response.Header.Set("cf-ray", "ray-secret?query=credential")
	return response, nil
}

func (*partFailureCapabilityClient) joinedRedirectSafe() {}

func (c *partFailureCapabilityClient) Do(request *http.Request) (*http.Response, error) {
	key := request.URL.Query().Get("key")
	if request.Method == http.MethodPut {
		c.puts[key]++
		if key == c.failingKey {
			response := capabilityResponse(c.status, []byte("response-body-secret-sentinel"), "", "")
			response.Header.Set("x-amz-request-id", "request-id-secret-token=abc")
			response.Header.Set("x-amz-id-2", "extended-secret/bearer+value=")
			response.Header.Set("cf-ray", "ray-secret?query=credential")
			response.Header.Set("x-unsafe", "header-secret-sentinel")
			return response, nil
		}
	}
	return c.memoryCapabilityClient.Do(request)
}

func thirtyPartPublicationFixture(t *testing.T) (WorkerClaim, SealedHourScratch, []BuiltOutput, map[string][]byte) {
	t.Helper()
	start := time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC)
	sources := make([]SourceClip, 30)
	artifacts := make([]BuiltArtifactIdentity, 30)
	media := make(map[string][]byte, 30)
	for i := range sources {
		sources[i] = testSource(int64(i+1), start.Add(time.Duration(i)*2*time.Minute))
		if i > 0 {
			sources[i].SeamToPrevious = SeamEvidence{Verdict: "gap", Reason: "missing_capture_sequence", SignedGapNanoseconds: int64(time.Minute)}
		}
		body := []byte("verified-media-part-" + strconv.Itoa(i+1))
		sum := sha256.Sum256(body)
		artifacts[i] = BuiltArtifactIdentity{SizeBytes: int64(len(body)), SHA256: hex.EncodeToString(sum[:])}
		media[artifacts[i].SHA256] = body
	}
	req := testRequest(sources)
	ledger, err := testLedger(req, req.LocalDate)
	if err != nil {
		t.Fatal(err)
	}
	req.AllocationLedgerSHA = ledger.LedgerSHA256
	for i := range artifacts {
		artifacts[i].MediaToolIdentity = req.MediaTool.IdentitySHA256
	}
	req.BuiltArtifacts = artifacts
	plan, err := BuildPlan(req)
	if err != nil || len(plan.Outputs) != 30 {
		t.Fatalf("build 30-part plan outputs=%d err=%v", len(plan.Outputs), err)
	}
	built := make([]BuiltOutput, 30)
	directory := filepath.Join(t.TempDir(), strings.Repeat("L", 43))
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	for i := range built {
		body := media[plan.Outputs[i].ExpectedSHA]
		path := filepath.Join(directory, "part-"+strconv.Itoa(i+1)+".mp4")
		if err := os.WriteFile(path, body, 0o600); err != nil {
			t.Fatal(err)
		}
		built[i] = BuiltOutput{Path: path, SizeBytes: int64(len(body)), SHA256: plan.Outputs[i].ExpectedSHA,
			SourceCount: 1, Verification: passingVerification()}
	}
	claim := sealedClaim(t, 7, plan, built, nil)
	return claim, bindTestScratch(t, claim, built, nil, directory), built, media
}

func TestPublishClaimedHourReportsSafePart29PutFailure(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusTooManyRequests, http.StatusInternalServerError} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			claim, scratch, _, media := thirtyPartPublicationFixture(t)
			objects := map[string][]byte{}
			for i := 0; i < 28; i += 2 {
				objects[claim.Plan.Outputs[i].ObjectKey] = append([]byte(nil), media[claim.Plan.Outputs[i].ExpectedSHA]...)
			}
			client := &partFailureCapabilityClient{memoryCapabilityClient: &memoryCapabilityClient{objects: objects},
				failingKey: claim.Plan.Outputs[28].ObjectKey, status: status, puts: map[string]int{}}
			finalized := false
			_, err := publishClaimedHour(context.Background(), client, claim, scratch, testCreateResolver(), testReadResolver(client.memoryCapabilityClient), func(context.Context, WorkerClaim, PublishedHour) error {
				finalized = true
				return nil
			})
			var diagnostic *StorageCapabilityError
			if !errors.As(err, &diagnostic) || diagnostic.Operation != "put" || diagnostic.Reason != "status" || diagnostic.StatusCode != status ||
				diagnostic.ArtifactID != claim.MediaArtifactIDs[28] || diagnostic.Ordinal != 29 ||
				diagnostic.RequestID != (StorageRequestIDEvidence{SHA256: "f416bbd10bf2fc79f29cd10cde04b6ad722a9ad2543a3b302786783de84318ef", Length: 27}) ||
				diagnostic.ExtendedRequestID != (StorageRequestIDEvidence{SHA256: "223fd63859a8806048c89f92fe526f63b7780c6c710ae52dfece3eafc4c7dc78", Length: 29}) ||
				diagnostic.RayID != (StorageRequestIDEvidence{SHA256: "27e586c18e5fcae586df216614036f5962499068f6205891c9073c0da5457121", Length: 27}) {
				t.Fatalf("part29 diagnostic=%+v err=%v", diagnostic, err)
			}
			if finalized || client.puts[claim.Plan.Outputs[29].ObjectKey] != 0 {
				t.Fatalf("publication advanced after part29 finalized=%v part30_puts=%d", finalized, client.puts[claim.Plan.Outputs[29].ObjectKey])
			}
			for _, forbidden := range []string{"response-body-secret-sentinel", "header-secret-sentinel", "request-id-secret", "extended-secret", "ray-secret", claim.OperationToken, claim.Plan.Outputs[28].ExpectedSHA, "https://"} {
				if strings.Contains(err.Error(), forbidden) {
					t.Fatalf("unsafe diagnostic %q contains %q", err, forbidden)
				}
			}
		})
	}
}

func TestPublishClaimedHourPreservesPart29ReadCapabilityConflict(t *testing.T) {
	claim, scratch, _, _ := thirtyPartPublicationFixture(t)
	client := &memoryCapabilityClient{objects: map[string][]byte{}}
	conflict := errors.New("read-capability-conflict-sentinel")
	finalized := false
	_, err := publishClaimedHour(context.Background(), client, claim, scratch, testCreateResolver(), func(ctx context.Context, got WorkerClaim, artifactID int64) (ObjectReadCapability, error) {
		if artifactID == claim.MediaArtifactIDs[28] {
			return ObjectReadCapability{}, conflict
		}
		return testReadResolver(client)(ctx, got, artifactID)
	}, func(context.Context, WorkerClaim, PublishedHour) error {
		finalized = true
		return nil
	})
	var diagnostic *StorageCapabilityError
	if !errors.Is(err, conflict) || !errors.As(err, &diagnostic) || diagnostic.Operation != "reread_capability" || diagnostic.Reason != "capability" ||
		diagnostic.ArtifactID != claim.MediaArtifactIDs[28] || diagnostic.Ordinal != 29 || finalized {
		t.Fatalf("read conflict diagnostic=%+v finalized=%v err=%v", diagnostic, finalized, err)
	}
}

func TestPublishClaimedHourReportsSafePart29RereadFailures(t *testing.T) {
	for _, tc := range []struct {
		mode   string
		status int
	}{
		{mode: "transport"},
		{mode: "status", status: http.StatusServiceUnavailable},
		{mode: "identity", status: http.StatusOK},
		{mode: "hash", status: http.StatusOK},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			claim, scratch, _, _ := thirtyPartPublicationFixture(t)
			memory := &memoryCapabilityClient{objects: map[string][]byte{}}
			client := &partRereadFailureCapabilityClient{memoryCapabilityClient: memory,
				failingKey: claim.Plan.Outputs[28].ObjectKey, mode: tc.mode, puts: map[string]int{}}
			finalized := false
			_, err := publishClaimedHour(context.Background(), client, claim, scratch, testCreateResolver(), testReadResolver(memory), func(context.Context, WorkerClaim, PublishedHour) error {
				finalized = true
				return nil
			})
			var diagnostic *StorageCapabilityError
			if !errors.As(err, &diagnostic) || diagnostic.Operation != "reread" || diagnostic.Reason != tc.mode ||
				diagnostic.StatusCode != tc.status || diagnostic.ArtifactID != claim.MediaArtifactIDs[28] || diagnostic.Ordinal != 29 {
				t.Fatalf("part29 %s diagnostic=%+v err=%v", tc.mode, diagnostic, err)
			}
			if _, created := memory.objects[claim.Plan.Outputs[28].ObjectKey]; !created || finalized ||
				client.puts[claim.Plan.Outputs[29].ObjectKey] != 0 {
				t.Fatalf("reread %s advanced incorrectly created29=%v finalized=%v part30_puts=%d",
					tc.mode, created, finalized, client.puts[claim.Plan.Outputs[29].ObjectKey])
			}
			for _, forbidden := range []string{"transport-url-token-secret", "response-body-secret", "request-id-secret", "extended-secret", "ray-secret", claim.OperationToken, claim.Plan.Outputs[28].ExpectedSHA, "https://"} {
				if strings.Contains(err.Error(), forbidden) {
					t.Fatalf("unsafe reread diagnostic %q contains %q", err, forbidden)
				}
			}
			if tc.mode == "transport" {
				if diagnostic.RequestID != (StorageRequestIDEvidence{}) || diagnostic.ExtendedRequestID != (StorageRequestIDEvidence{}) || diagnostic.RayID != (StorageRequestIDEvidence{}) {
					t.Fatalf("transport failure invented request IDs: %+v", diagnostic)
				}
			} else if diagnostic.RequestID.SHA256 != "f416bbd10bf2fc79f29cd10cde04b6ad722a9ad2543a3b302786783de84318ef" || diagnostic.RequestID.Length != 27 {
				t.Fatalf("request ID evidence differs: %+v", diagnostic.RequestID)
			}
		})
	}
}

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
	if !reflect.DeepEqual(events[0], StageTimingEvent{Stage: "upload_verify", ElapsedMS: events[0].ElapsedMS, Outcome: "error",
		FailureStage: UploadVerifyFailureManifestIdentity, ArtifactID: claim.HourManifestArtifactID}) {
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
	originLeaseID := strings.Repeat("O", 43)
	scratch := filepath.Join(root, originLeaseID)
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
	sealedScratch, err := bindSealedHourScratch(verifiedHourScratch{HourID: claim.HourID,
		SourceClaimSHA256: claim.Plan.SourceClaimSHA256, OriginLeaseID: originLeaseID,
		Directory: scratch, Built: []BuiltOutput{built}}, claim)
	if err != nil {
		t.Fatal(err)
	}
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
		t.Fatal("finalized origin-lease scratch tree was not removed")
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
