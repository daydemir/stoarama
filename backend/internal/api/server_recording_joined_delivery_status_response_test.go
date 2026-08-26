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
