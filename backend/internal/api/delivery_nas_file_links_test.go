package api

import (
	"strings"
	"testing"
)

func TestDeliveryPublicFileLinksMatchOldFileURLs(t *testing.T) {
	generation := strings.Repeat("a", 32)
	object := globalDeliveryObject{Key: "joined/test/objects/demo.mp4", ETag: "demo", SizeBytes: 42,
		RecordingID: 9, SHA256: strings.Repeat("b", 64), Path: "folder/demo.mp4"}
	numeric := deliveryPublicFile("17", generation, object)
	alias := deliveryPublicFile("sha256:"+object.SHA256, generation, object)
	want := "https://stoarama.com/api/v1/global-street-scores-delivery/file/17"
	if numeric.FileURL != want || numeric.PinnedFileURL != want+"?generation="+generation ||
		numeric.DownloadURL != want+"?download=1" || numeric.PinnedDownloadURL != want+"?download=1&generation="+generation {
		t.Fatalf("old link forms differ: %+v", numeric)
	}
	if !strings.Contains(alias.FileURL, "/sha256%3A") || alias.Identity != numeric.Identity {
		t.Fatalf("non-numeric alias drifted: %+v", alias)
	}
}
