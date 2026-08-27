package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
)

func TestJoinedWorkerResponsePreservesCanonicalEvidenceBytes(t *testing.T) {
	for name, facts := range map[string]json.RawMessage{
		"html placeholders":    json.RawMessage(`{"category":"invalid_media_data","normalized_fact":"<address>&<scratch-file>"}`),
		"producer field order": json.RawMessage(`{"status":"failed","packet_payload_order_status":"passed","decoded_frame_totals_status":"failed","source_fingerprint":{"duration_seconds":1},"output_fingerprint":{"duration_seconds":2}}`),
	} {
		t.Run(name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			writeJoinedWorkerJSON(recorder, http.StatusOK, struct {
				Facts json.RawMessage `json:"facts"`
			}{Facts: facts})
			if recorder.Code != http.StatusOK || recorder.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("response metadata differs: code=%d content_type=%q", recorder.Code, recorder.Header().Get("Content-Type"))
			}
			if name == "html placeholders" && !bytes.Contains(recorder.Body.Bytes(), []byte(`<address>&<scratch-file>`)) {
				t.Fatalf("canonical placeholders changed: %s", recorder.Body.Bytes())
			}
			if name == "html placeholders" && (bytes.Contains(recorder.Body.Bytes(), []byte(`\u003c`)) || bytes.Contains(recorder.Body.Bytes(), []byte(`\u003e`)) || bytes.Contains(recorder.Body.Bytes(), []byte(`\u0026`))) {
				t.Fatalf("canonical placeholders were HTML escaped: %s", recorder.Body.Bytes())
			}
			var received struct {
				Facts json.RawMessage `json:"facts"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &received); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(received.Facts, facts) {
				t.Fatalf("canonical evidence changed across response: got=%s want=%s", received.Facts, facts)
			}
		})
	}
}

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
