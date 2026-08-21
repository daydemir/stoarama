package joinedrecording

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func exactSourceCapability(source SourceClip, operation string) SourceReadCapability {
	headers := map[string]string{"If-Match": quotedETag(source.Object.ETag)}
	method := map[string]string{"head": http.MethodHead, "get": http.MethodGet}[operation]
	request := signedRequest(method, source.Bucket, source.Object.Key, headers, source.Object.VersionID, 4*time.Minute)
	expires, _ := signedRequestExpiry(request)
	return SourceReadCapability{ProtocolVersion: JoinedProtocolVersion, Operation: operation,
		ObjectKey: source.Object.Key, SizeBytes: source.Object.SizeBytes, SHA256: source.Object.SHA256,
		ETag: source.Object.ETag, VersionID: source.Object.VersionID, ExpiresAt: expires, Request: request}
}

func TestRebuildSealedHourRejectsMalformedClaimBeforeRunnerOrStorage(t *testing.T) {
	plan := oneOutputPlan(t)
	built := []BuiltOutput{{SizeBytes: plan.Outputs[0].ExpectedSize, SHA256: plan.Outputs[0].ExpectedSHA,
		SourceCount: len(plan.Outputs[0].Sources), Verification: passingVerification()}}
	claim := sealedClaim(t, 7, plan, built, nil)
	claim.HourManifest.Media = nil
	var ran, resolved bool
	run := func(context.Context, OperationCredentials, HeartbeatOperation,
		func(context.Context, func() OperationCredentials) error) error {
		ran = true
		return nil
	}
	_, _, err := rebuildSealedHourRenewing(context.Background(), claim, t.TempDir(), nil,
		testSourceAuthority, noHeartbeat, func(context.Context, WorkerClaim, SourceClip, string) (SourceReadCapability, error) {
			resolved = true
			return SourceReadCapability{}, nil
		}, run)
	if err == nil || ran || resolved {
		t.Fatalf("malformed rebuild reached runner or storage: err=%v ran=%v resolved=%v", err, ran, resolved)
	}
}

func TestRebuildSealedHourUsesExactFrozenPartsAndCurrentLeaseScratch(t *testing.T) {
	sourceDir := t.TempDir()
	local := makeMediaClip(t, sourceDir, "source.mp4", 440, false)
	body, err := os.ReadFile(local.Path)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC)
	source := testSource(1, start)
	source.EndUTC = start.Add(time.Second)
	source.SeamToPrevious = SeamEvidence{}
	source.Object.Key = "raw/rebuild-source.mp4"
	source.Object.ETag = objectETag(body)
	source.Object.VersionID = "version"
	source.Object.SizeBytes = int64(len(body))
	sum := sha256.Sum256(body)
	source.Object.SHA256 = hex.EncodeToString(sum[:])
	local.ClipID, local.SizeBytes, local.SHA256 = source.ClipID, source.Object.SizeBytes, source.Object.SHA256
	claimSHA, _, err := sourceClaimSHA([]SourceClip{source})
	if err != nil {
		t.Fatal(err)
	}
	local.SourceClaimSHA256 = claimSHA
	tool, err := InspectMediaToolEvidence(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	req := testRequest([]SourceClip{source})
	req.MediaTool = tool
	ledger, err := testLedger(req, req.LocalDate)
	if err != nil {
		t.Fatal(err)
	}
	req.AllocationLedgerSHA = ledger.LedgerSHA256
	originalDir := t.TempDir()
	built, err := BuildSealedOutput(context.Background(), []LocalSource{local}, originalDir)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(filepath.Dir(built.Path))
	req.BuiltArtifacts = []BuiltArtifactIdentity{{SizeBytes: built.SizeBytes, SHA256: built.SHA256,
		MediaToolIdentity: tool.IdentitySHA256}}
	plan, err := BuildPlan(req)
	if err != nil {
		t.Fatal(err)
	}
	claim := sealedClaim(t, 7, plan, []BuiltOutput{built}, nil)
	root := t.TempDir()
	client := &memoryCapabilityClient{objects: map[string][]byte{source.Object.Key: body}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	renewed := OperationCredentials{LeaseID: claim.LeaseID, OperationToken: strings.Repeat("r", 32), ExpiresAt: claim.LeaseExpires.Add(time.Minute)}
	run := func(runCtx context.Context, initial OperationCredentials, _ HeartbeatOperation, work func(context.Context, func() OperationCredentials) error) error {
		if initial.OperationToken != claim.OperationToken || initial.ExpiresAt != claim.LeaseExpires {
			t.Fatalf("initial rebuild credentials differ: %+v", initial)
		}
		return work(runCtx, func() OperationCredentials { return renewed })
	}
	renewedClaim, scratch, err := rebuildSealedHourRenewing(ctx, claim, root, client, testSourceAuthority, noHeartbeat,
		func(_ context.Context, got WorkerClaim, gotSource SourceClip, operation string) (SourceReadCapability, error) {
			if got.HourID != claim.HourID || gotSource.ClipID != source.ClipID || got.OperationToken != renewed.OperationToken || got.LeaseExpires != renewed.ExpiresAt {
				t.Fatalf("rebuild source claim differs: hour=%s clip=%s", got.HourID, strconv.FormatInt(gotSource.ClipID, 10))
			}
			return exactSourceCapability(source, operation), nil
		}, run)
	if err != nil {
		t.Fatal(err)
	}
	if renewedClaim.OperationToken != renewed.OperationToken || renewedClaim.LeaseExpires != renewed.ExpiresAt {
		t.Fatalf("returned rebuild claim is stale: %+v", renewedClaim)
	}
	if scratch.publicationLeaseID != claim.LeaseID || len(scratch.verified.Built) != 1 ||
		scratch.verified.Built[0].SHA256 != plan.Outputs[0].ExpectedSHA ||
		filepath.Base(scratch.verified.Directory) != claim.LeaseID {
		t.Fatalf("rebuilt scratch differs: %+v", scratch.verified)
	}
}
