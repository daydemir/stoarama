package joinedrecording

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

func refreshedRunner(ctx context.Context, initial OperationCredentials, heartbeat HeartbeatOperation, work func(context.Context, func() OperationCredentials) error) error {
	next, err := heartbeat(ctx, initial.OperationToken)
	if err != nil {
		return err
	}
	return work(ctx, func() OperationCredentials { return next })
}

func TestPreflightRenewingUsesRefreshedTokenAndSourceOnlySeams(t *testing.T) {
	tool, err := InspectMediaToolEvidence(context.Background())
	if err != nil {
		t.Skipf("pinned media tools unavailable: %v", err)
	}
	dir := t.TempDir()
	firstLocal := makeMediaClip(t, dir, "first.mp4", 0, false)
	secondLocal := makeMediaClip(t, dir, "second.mp4", 0, false)
	start := time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC)
	locals := []LocalSource{firstLocal, secondLocal}
	sources := make([]SourceClip, 2)
	objects := map[string][]byte{}
	for i := range sources {
		body, readErr := os.ReadFile(locals[i].Path)
		if readErr != nil {
			t.Fatal(readErr)
		}
		source := testSource(int64(i+1), start.Add(time.Duration(i)*time.Minute))
		source.Object = ObjectIdentity{Key: "raw/pipeline-" + string(rune('1'+i)) + ".mp4", VersionID: "version", ETag: objectETag(body), SizeBytes: int64(len(body)), SHA256: locals[i].SHA256}
		source.SeamToPrevious = SeamEvidence{}
		sources[i] = source
		objects[source.Object.Key] = body
	}
	req := testRequest(sources)
	req.MediaTool = tool
	ledger, err := testLedger(req, req.LocalDate)
	if err != nil {
		t.Fatal(err)
	}
	claimSHA, _, _ := sourceClaimSHA(sources)
	leaseID := strings.Repeat("L", 43)
	claim := PreflightHourClaim{ProtocolVersion: JoinedProtocolVersion, HourID: canonicalHourIDValue(req.BatchID, req.RecordingID, req.LocalDate, req.DeliveryHour, req.Generation), LeaseID: leaseID, OperationToken: strings.Repeat("a", 32), LeaseExpires: time.Now().Add(5 * time.Minute), BatchID: req.BatchID, Generation: req.Generation, RecordingID: req.RecordingID, Timezone: req.Timezone, LocalDate: req.LocalDate, LocalHour: req.DeliveryHour, Metadata: req.Metadata, AllocationLedgerSHA: ledger.LedgerSHA256, Qualification: req.Qualification, MediaTool: tool, SourceClaimSHA256: claimSHA, Sources: sources}
	refreshed := OperationCredentials{LeaseID: leaseID, OperationToken: strings.Repeat("b", 32), ExpiresAt: claim.LeaseExpires.Add(time.Minute)}
	client := &memoryCapabilityClient{objects: objects}
	heartbeat := func(_ context.Context, token string) (OperationCredentials, error) {
		if token != claim.OperationToken {
			t.Fatal("heartbeat did not use initial token")
		}
		return refreshed, nil
	}
	resolve := func(_ context.Context, got PreflightHourClaim, source SourceClip, operation string) (SourceReadCapability, error) {
		if got.OperationToken != refreshed.OperationToken || source.SeamToPrevious != (SeamEvidence{}) {
			t.Fatal("source capability did not use refreshed source-only claim")
		}
		capability := sourceReadCapability(source.Object.Key, source.Object.ETag, source.Object.VersionID, operation)
		capability.SizeBytes, capability.SHA256 = source.Object.SizeBytes, source.Object.SHA256
		return capability, nil
	}
	seal := func(_ context.Context, got PreflightHourClaim, request SealHourRequest) (WorkerClaim, error) {
		if got.OperationToken != refreshed.OperationToken || len(request.AccountedSources) != 2 || len(request.Media) == 0 {
			return WorkerClaim{}, errors.New("seal did not use refreshed complete evidence")
		}
		artifacts := make([]BuiltArtifactIdentity, len(request.Media))
		for i, media := range request.Media {
			artifacts[i] = BuiltArtifactIdentity{SizeBytes: media.SizeBytes, SHA256: media.SHA256, MediaToolIdentity: tool.IdentitySHA256}
		}
		plan, buildErr := BuildPlan(PlanRequest{BatchID: req.BatchID, Generation: req.Generation, RecordingID: req.RecordingID, Timezone: req.Timezone, LocalDate: req.LocalDate, DeliveryHour: req.DeliveryHour, Metadata: req.Metadata, Qualification: req.Qualification, AllocationLedgerSHA: ledger.LedgerSHA256, MediaTool: tool, Sources: request.AccountedSources, BuiltArtifacts: artifacts})
		if buildErr != nil {
			return WorkerClaim{}, buildErr
		}
		built := make([]BuiltOutput, len(request.Media))
		for i, media := range request.Media {
			built[i] = BuiltOutput{SizeBytes: media.SizeBytes, SHA256: media.SHA256, SourceCount: len(media.SourceClipIDs), Verification: media.Verification, SplitEvidence: media.MaximalityEvidence}
		}
		published := sealedClaim(t, 1, plan, built, request.Quarantine)
		published.LeaseID, published.OperationToken, published.LeaseExpires = strings.Repeat("P", 43), strings.Repeat("p", 32), time.Now().Add(time.Hour)
		return published, nil
	}
	sealed, scratch, err := runPreflightHourRenewing(context.Background(), claim, t.TempDir(), client, "cap.test", heartbeat, resolve, seal, refreshedRunner)
	if err != nil || sealed.HourID != claim.HourID || scratch.verified.HourID != claim.HourID || len(scratch.verified.Built) == 0 {
		t.Fatalf("preflight lifecycle failed: sealed=%+v scratch=%+v err=%v", sealed, scratch, err)
	}
}

func TestPreflightRejectsMismatchedInstalledToolBeforeSourceAccess(t *testing.T) {
	actual, err := InspectMediaToolEvidence(context.Background())
	if err != nil {
		t.Skipf("pinned media tools unavailable: %v", err)
	}
	want := actual
	want.FFmpegVersion += " mismatched"
	want, err = SealMediaToolEvidence(want)
	if err != nil {
		t.Fatal(err)
	}
	claim := PreflightHourClaim{LeaseID: strings.Repeat("L", 43), OperationToken: strings.Repeat("a", 32), LeaseExpires: time.Now().Add(time.Hour), MediaTool: want}
	var sourceAccessed, sealed bool
	_, _, err = runPreflightHourRenewing(context.Background(), claim, t.TempDir(), &memoryCapabilityClient{objects: map[string][]byte{}}, "cap.test", func(context.Context, string) (OperationCredentials, error) {
		return OperationCredentials{}, errors.New("unexpected heartbeat")
	}, func(context.Context, PreflightHourClaim, SourceClip, string) (SourceReadCapability, error) {
		sourceAccessed = true
		return SourceReadCapability{}, nil
	}, func(context.Context, PreflightHourClaim, SealHourRequest) (WorkerClaim, error) {
		sealed = true
		return WorkerClaim{}, nil
	}, func(ctx context.Context, initial OperationCredentials, _ HeartbeatOperation, work func(context.Context, func() OperationCredentials) error) error {
		return work(ctx, func() OperationCredentials { return initial })
	})
	if err == nil || sourceAccessed || sealed {
		t.Fatalf("tool mismatch accessed sources or sealed: err=%v source=%v sealed=%v", err, sourceAccessed, sealed)
	}
}

func TestLedgerAndBatchIndexRenewingUseRefreshedToken(t *testing.T) {
	leaseID := strings.Repeat("L", 43)
	initialToken, refreshedToken := strings.Repeat("a", 32), strings.Repeat("b", 32)
	expires := time.Now().Add(5 * time.Minute)
	heartbeat := func(context.Context, string) (OperationCredentials, error) {
		return OperationCredentials{LeaseID: leaseID, OperationToken: refreshedToken, ExpiresAt: expires.Add(time.Minute)}, nil
	}

	req := testRequest([]SourceClip{testSource(1, time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC))})
	ledger, err := testLedger(req, req.LocalDate)
	if err != nil {
		t.Fatal(err)
	}
	ledgerJSON, ledgerSHA, _ := CanonicalAllocationLedgerArtifact(ledger)
	ledgerClaim := LedgerPublicationClaim{ProtocolVersion: JoinedProtocolVersion, ArtifactID: 70, ScopeID: canonicalLedgerID(req.BatchID, req.RecordingID, req.LocalDate, req.Generation), LeaseID: leaseID, OperationToken: initialToken, LeaseExpires: expires, StorageAuthority: "cap.test", StorageBucket: "recordings", BatchID: req.BatchID, Ledger: ledger, ExpectedSize: int64(len(ledgerJSON)), ExpectedSHA256: ledgerSHA}
	if _, err := PublishAllocationLedgerRenewing(context.Background(), nil, "other.test", ledgerClaim, nil, nil, nil, nil); err == nil {
		t.Fatal("ledger publication trusted claim-supplied storage authority")
	}
	ledgerClient := &memoryCapabilityClient{objects: map[string][]byte{}}
	ledgerPublished, err := publishAllocationLedgerRenewing(context.Background(), ledgerClient, ledgerClaim, heartbeat,
		func(_ context.Context, got LedgerPublicationClaim) (ObjectCreateCapability, error) {
			if got.OperationToken != refreshedToken {
				t.Fatal("ledger create used stale token")
			}
			_, key, _ := CanonicalAllocationLedgerPaths(got.BatchID, got.Ledger.RecordingID, got.Ledger.LocalDate)
			return createCapabilityIdentity(got.ArtifactID, got.StorageBucket, key, "application/json", got.ExpectedSize, got.ExpectedSHA256), nil
		}, func(_ context.Context, got LedgerPublicationClaim) (ObjectReadCapability, error) {
			_, key, _ := CanonicalAllocationLedgerPaths(got.BatchID, got.Ledger.RecordingID, got.Ledger.LocalDate)
			return readCapability(got.ArtifactID, got.StorageBucket, key, ledgerClient.objects[key]), nil
		}, func(_ context.Context, got LedgerPublicationClaim, _ PublishedLedger) error {
			if got.OperationToken != refreshedToken {
				t.Fatal("ledger finalize used stale token")
			}
			return nil
		}, refreshedRunner)
	if err != nil || ledgerPublished.SHA256 != ledgerSHA {
		t.Fatalf("ledger publication failed: %+v %v", ledgerPublished, err)
	}

	index := testBatchIndex(t)
	_, indexJSON, indexSHA, _ := BuildBatchIndex(index)
	indexClaim := BatchIndexPublicationClaim{ProtocolVersion: JoinedProtocolVersion, ScopeID: index.BatchID, ArtifactID: 80, LeaseID: leaseID, OperationToken: initialToken, LeaseExpires: expires, StorageAuthority: "cap.test", StorageBucket: "recordings", Index: index, ExpectedSize: int64(len(indexJSON)), ExpectedSHA256: indexSHA}
	if _, err := PublishBatchIndexRenewing(context.Background(), nil, "other.test", indexClaim, nil, nil, nil, nil); err == nil {
		t.Fatal("batch-index publication trusted claim-supplied storage authority")
	}
	indexClient := &memoryCapabilityClient{objects: map[string][]byte{}}
	indexPublished, err := publishBatchIndexRenewing(context.Background(), indexClient, indexClaim, heartbeat,
		func(_ context.Context, got BatchIndexPublicationClaim) (ObjectCreateCapability, error) {
			if got.OperationToken != refreshedToken {
				t.Fatal("index create used stale token")
			}
			key, _ := CanonicalBatchIndexObjectKey(got.Index.BatchID)
			return createCapabilityIdentity(got.ArtifactID, got.StorageBucket, key, "application/json", got.ExpectedSize, got.ExpectedSHA256), nil
		}, func(_ context.Context, got BatchIndexPublicationClaim) (ObjectReadCapability, error) {
			key, _ := CanonicalBatchIndexObjectKey(got.Index.BatchID)
			return readCapability(got.ArtifactID, got.StorageBucket, key, indexClient.objects[key]), nil
		}, func(_ context.Context, got BatchIndexPublicationClaim, _ PublishedBatchIndex) error {
			if got.OperationToken != refreshedToken {
				t.Fatal("index finalize used stale token")
			}
			return nil
		}, refreshedRunner)
	if err != nil || indexPublished.SHA256 != indexSHA {
		t.Fatalf("batch-index publication failed: %+v %v", indexPublished, err)
	}

	var capabilityIssued, finalized bool
	canceledRunner := func(ctx context.Context, initial OperationCredentials, _ HeartbeatOperation, work func(context.Context, func() OperationCredentials) error) error {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		return work(canceled, func() OperationCredentials { return initial })
	}
	_, err = publishAllocationLedgerRenewing(context.Background(), &memoryCapabilityClient{objects: map[string][]byte{}}, ledgerClaim, heartbeat,
		func(context.Context, LedgerPublicationClaim) (ObjectCreateCapability, error) {
			capabilityIssued = true
			return ObjectCreateCapability{}, nil
		}, func(context.Context, LedgerPublicationClaim) (ObjectReadCapability, error) {
			capabilityIssued = true
			return ObjectReadCapability{}, nil
		}, func(context.Context, LedgerPublicationClaim, PublishedLedger) error {
			finalized = true
			return nil
		}, canceledRunner)
	if !errors.Is(err, context.Canceled) || capabilityIssued || finalized {
		t.Fatalf("canceled ledger publication issued authority or finalized: err=%v capability=%v finalized=%v", err, capabilityIssued, finalized)
	}
}

func TestRepeatableCorruptSourceProbeQuarantinesWithoutDroppingLaterSource(t *testing.T) {
	tool, err := InspectMediaToolEvidence(context.Background())
	if err != nil {
		t.Skipf("pinned media tools unavailable: %v", err)
	}
	dir := t.TempDir()
	good1 := makeMediaClip(t, dir, "good-1.mp4", 0, false)
	good3 := makeMediaClip(t, dir, "good-3.mp4", 0, false)
	badPath := dir + "/bad.mp4"
	badBody := []byte("not an mp4")
	if err := os.WriteFile(badPath, badBody, 0600); err != nil {
		t.Fatal(err)
	}
	badSum := sha256.Sum256(badBody)
	bad := LocalSource{ClipID: 2, Path: badPath, SizeBytes: int64(len(badBody)), SHA256: hex.EncodeToString(badSum[:])}
	locals := []LocalSource{good1, bad, good3}
	start := time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC)
	sources := []SourceClip{testSource(1, start), testSource(2, start.Add(time.Minute)), testSource(3, start.Add(2*time.Minute))}
	for i := range locals {
		locals[i].ClipID = sources[i].ClipID
		locals[i].SourceClaimSHA256, _, _ = sourceClaimSHA([]SourceClip{sources[i]})
	}
	included, _, quarantined, evidence, err := freezeDownloadedAudioForPreflight(context.Background(), sources, locals, tool.IdentitySHA256)
	if err != nil || len(included) != 2 || included[1].ClipID != 3 || len(quarantined) != 1 || quarantined[0].ClipID != 2 || len(evidence) != 1 || evidence[0].Evidence.RepeatCount != 2 {
		t.Fatalf("corrupt middle source was not narrowly quarantined: included=%+v quarantined=%+v evidence=%+v err=%v", included, quarantined, evidence, err)
	}
}
