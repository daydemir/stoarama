package joinedrecording

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"
)

func TestDownloadClaimSourcesPinsHeadAndHashesBeforePublication(t *testing.T) {
	body := "exact raw clip bytes"
	sum := sha256.Sum256([]byte(body))
	source := testSource(1, time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC))
	source.Object.SizeBytes = int64(len(body))
	source.Object.SHA256 = hex.EncodeToString(sum[:])
	source.Object.ETag = "etag-" + hex.EncodeToString(sum[:4])
	source.Object.VersionID = "version"
	plan, err := buildTestPlan(testRequest([]SourceClip{source}))
	if err != nil {
		t.Fatal(err)
	}
	claim := PreflightHourClaim{ProtocolVersion: JoinedProtocolVersion, HourID: plan.HourID, LeaseID: strings.Repeat("L", 43), OperationToken: strings.Repeat("t", 32), LeaseExpires: time.Now().Add(time.Hour), BatchID: plan.BatchID, Generation: plan.Generation, RecordingID: plan.RecordingID, Timezone: plan.Timezone, LocalDate: plan.LocalDate, LocalHour: plan.LocalHour, AllocationLedgerSHA: plan.AllocationLedgerSHA, Qualification: plan.Qualification, MediaTool: plan.MediaTool, SourceClaimSHA256: plan.SourceClaimSHA256, Sources: sourceOnlyClips(plan.Outputs[0].Sources)}
	client := &memoryCapabilityClient{objects: map[string][]byte{source.Object.Key: []byte(body)}}
	locals, scratch, err := downloadClaimSources(context.Background(), claim, t.TempDir(), client, testSourceAuthority, func(_ context.Context, _ SourceClip, operation string) (SourceReadCapability, error) {
		capability := sourceReadCapability(source.Object.Key, source.Object.ETag, source.Object.VersionID, operation)
		capability.SizeBytes, capability.SHA256 = source.Object.SizeBytes, source.Object.SHA256
		return capability, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(locals) != 1 || !SafeScratchOutput(locals[0].Path, scratch) {
		t.Fatalf("bad local source: %+v", locals)
	}
	if err := verifyLocalIdentity(locals[0]); err != nil {
		t.Fatal(err)
	}
}
