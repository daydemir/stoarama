package api

import (
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
)

func TestValidateJoinedSealSourceIdentityReportsExactBoundary(t *testing.T) {
	start := time.Date(2026, time.August, 15, 8, 0, 0, 0, time.UTC)
	source := joinedrecording.SourceClip{
		ClipID: 1, RecordingID: 420, RecordingJobID: 2, StorageDestinationID: 3,
		Provider: "r2", Endpoint: "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com",
		Region: "auto", Bucket: "recordings", StartUTC: start, EndUTC: start.Add(time.Minute),
		Object: joinedrecording.ObjectIdentity{Key: "raw/one.mp4", ETag: "etag", SizeBytes: 10, SHA256: strings.Repeat("a", 64)},
	}
	claimSHA, _, err := joinedrecording.CanonicalSourceClaim([]joinedrecording.SourceClip{source})
	if err != nil {
		t.Fatal(err)
	}
	claim := joinedrecording.PreflightHourClaim{RecordingID: 420, SourceClaimSHA256: claimSHA, Sources: []joinedrecording.SourceClip{source}, MediaTool: joinedrecording.MediaToolEvidence{IdentitySHA256: strings.Repeat("b", 64)}}
	req := joinedrecording.SealHourRequest{ProtocolVersion: joinedrecording.JoinedProtocolVersion, HourID: "hour", SourceClaimSHA256: claimSHA,
		AccountedSources: []joinedrecording.SourceClip{source}, Media: []joinedrecording.SealHourMedia{{Ordinal: 1, SourceClipIDs: []int64{1}, SizeBytes: 1, SHA256: strings.Repeat("c", 64), Verification: joinedCanonicalPassingVerification()}}}
	if err := validateJoinedSealSourceIdentity(claim, req); err != nil {
		t.Fatalf("valid identity rejected: %v", err)
	}

	invalid := req
	invalid.Media = append([]joinedrecording.SealHourMedia(nil), req.Media...)
	invalid.Media[0].Verification.Status = ""
	if got := validateJoinedSealSourceIdentity(claim, invalid); got == nil || !strings.Contains(got.Error(), "validate joined hour seal request") {
		t.Fatalf("request validation boundary was hidden: %v", got)
	}
	differentClaim := claim
	differentClaim.SourceClaimSHA256 = strings.Repeat("d", 64)
	if got := validateJoinedSealSourceIdentity(differentClaim, req); got == nil || got.Error() != "joined hour seal source claim differs" {
		t.Fatalf("claim boundary was hidden: %v", got)
	}
	differentFrozen := claim
	differentFrozen.Sources[0].Object.SizeBytes++
	differentFrozen.SourceClaimSHA256 = claimSHA
	if got := validateJoinedSealSourceIdentity(differentFrozen, req); got == nil || got.Error() != "joined hour seal frozen sources differ" {
		t.Fatalf("frozen-source boundary was hidden: %v", got)
	}
}
