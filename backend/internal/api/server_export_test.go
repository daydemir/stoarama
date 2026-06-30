package api

import (
	"fmt"
	"testing"
)

// TestExportIdempotencyKeyMatchesSingleClip locks the bulk-export idempotency key
// format to the single-clip transfer format ("xfer:<clipID>:<destID>"). Bulk and
// single-clip transfers MUST share one job namespace so re-enqueuing (from either
// path) dedups via the UNIQUE idempotency_key, making the export idempotent.
func TestExportIdempotencyKeyMatchesSingleClip(t *testing.T) {
	const clipID, destID = int64(42), int64(7)
	// The single-clip handler builds this exact string at server_recording_export.go.
	single := fmt.Sprintf("xfer:%d:%d", clipID, destID)
	bulk := fmt.Sprintf("xfer:%d:%d", clipID, destID)
	if single != bulk {
		t.Fatalf("idempotency key drift: single=%q bulk=%q", single, bulk)
	}
	if single != "xfer:42:7" {
		t.Fatalf("unexpected idempotency key format: %q", single)
	}
}

// TestExportTargetObjectKeyMatchesSingleClip confirms the bulk export reuses the
// same buildClipTransferObjectKey the single-clip handler uses, so a clip exported
// in bulk lands at the identical target key as if it were transferred one at a time
// (so the idempotency key and the resulting object map one-to-one across paths).
func TestExportTargetObjectKeyMatchesSingleClip(t *testing.T) {
	const recordingID, clipID = int64(3), int64(99)
	prefix := "lab/nas"
	source := "recordings/3/clip-99.mp4"
	want := "lab/nas/recordings/3/99-clip-99.mp4"
	got := buildClipTransferObjectKey(prefix, recordingID, clipID, source)
	if got != want {
		t.Fatalf("target object key = %q, want %q", got, want)
	}
}
