package api

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestJoinedDeliveryStatusResponseCarriesExactAcknowledgementIdentity(t *testing.T) {
	now := time.Date(2026, 8, 26, 11, 55, 0, 0, time.UTC)
	size := int64(1234)
	want := joinedDeliveryStatusResponse{
		BatchID: "batch-generation-1", ArtifactID: 492, ArtifactKind: "media", HourID: "hour-420",
		RelativePath: "plaza/August/Saturday/part.mp4", ExpectedSizeBytes: size,
		ExpectedSHA256: strings.Repeat("a", 64), PublicationState: "published", PublishedAt: &now,
		Acknowledged: true, VerifiedAt: &now, AcknowledgedPath: "plaza/August/Saturday/part.mp4",
		AcknowledgedSize: &size, AcknowledgedSHA256: strings.Repeat("a", 64), IdentityMatches: true,
		ConnectionID: 13, ConnectionProtocol: 1,
	}
	wire, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var got joinedDeliveryStatusResponse
	if err := json.Unmarshal(wire, &got); err != nil {
		t.Fatal(err)
	}
	if !got.Acknowledged || !got.IdentityMatches || got.VerifiedAt == nil || got.AcknowledgedSize == nil || *got.AcknowledgedSize != size {
		t.Fatalf("delivery status=%+v", got)
	}
}

func TestJoinedIdentityBlockersAreSafeAndLegacyCompatible(t *testing.T) {
	now := time.Now().UTC()
	for code := range connectionJoinedBlockers {
		request := connectionHeartbeatRequest{ClientVersion: "diagnostic-test", ClientPhase: "idle", ClientPreviousExit: "clean", JoinedProtocol: 1, JoinedDelivery: &connectionJoinedDelivery{ArtifactID: 3830, Blocker: code, AttemptedAt: &now}}
		if err := validateConnectionHeartbeat(request); err != nil {
			t.Fatalf("safe code %q rejected: %v", code, err)
		}
		if got := joinedAttemptBlockerClass(code); got != code {
			t.Fatalf("safe code %q became %q", code, got)
		}
	}
	if !connectionJoinedBlockers["path_conflict"] {
		t.Fatal("legacy client code removed")
	}
	for _, code := range []string{"dependency_identity", "manifest_identity", "existing_output_identity", "prepared_object_identity", "transfer_marker_identity"} {
		if !connectionJoinedBlockers[code] {
			t.Fatalf("missing diagnostic code %q", code)
		}
	}
	for _, unsafe := range []string{"https://private.invalid/?token=secret", "/private/nas/path", "manifest_identity extra", "download_error"} {
		request := connectionHeartbeatRequest{ClientVersion: "diagnostic-test", ClientPhase: "idle", ClientPreviousExit: "clean", JoinedProtocol: 1, JoinedDelivery: &connectionJoinedDelivery{ArtifactID: 3830, Blocker: unsafe, AttemptedAt: &now}}
		if err := validateConnectionHeartbeat(request); err == nil {
			t.Fatal("unrecognized telemetry accepted")
		}
		if got := joinedAttemptBlockerClass(unsafe); got != "present" {
			t.Fatalf("unrecognized detail exposed: %q", got)
		}
	}
	if got := joinedAttemptBlockerClass(""); got != "" {
		t.Fatal("absent blocker became present")
	}
}
