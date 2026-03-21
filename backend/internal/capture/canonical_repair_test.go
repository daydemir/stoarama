package capture

import "testing"

func TestProposeCanonicalStreamRepairUsesResolvedCaptureType(t *testing.T) {
	proposal := ProposeCanonicalStreamRepair(CanonicalRepairInput{
		Provider:              "KBS",
		SourceURL:             "https://example.com/cam",
		SourcePageURL:         "https://example.com/cam",
		CurrentSourceFamily:   SourceFamilyVideoStream,
		CurrentCaptureType:    CaptureTypeHTTPVideo,
		CurrentExecutionClass: ExecutionClassVideoLive,
		ResolvedCaptureType:   CaptureTypeHLS,
	})
	if proposal.ProposedCaptureType != CaptureTypeHLS {
		t.Fatalf("proposed capture_type=%q want %q", proposal.ProposedCaptureType, CaptureTypeHLS)
	}
	if proposal.ProposedSourceFamily != SourceFamilyVideoManifest {
		t.Fatalf("proposed source_family=%q want %q", proposal.ProposedSourceFamily, SourceFamilyVideoManifest)
	}
	if proposal.ReviewRequired {
		t.Fatalf("review_required=true want false")
	}
}

func TestShouldOverrideExplicitCaptureType(t *testing.T) {
	if !shouldOverrideExplicitCaptureType(CaptureTypeHTTPVideo, CaptureTypeHLS) {
		t.Fatalf("expected hls override from explicit http_video")
	}
	if shouldOverrideExplicitCaptureType(CaptureTypeHTTPVideo, CaptureTypeHTTPVideo) {
		t.Fatalf("did not expect override when types already match")
	}
	if shouldOverrideExplicitCaptureType(CaptureTypeHLS, CaptureTypeHTTPVideo) {
		t.Fatalf("did not expect weak http_video override")
	}
}
