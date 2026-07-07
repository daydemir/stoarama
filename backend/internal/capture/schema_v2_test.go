package capture

import "testing"

func TestDeriveCanonicalStreamFieldsDetectsDefaults(t *testing.T) {
	fields, err := DeriveCanonicalStreamFields(
		"https://example.com/cam/playlist.m3u8",
		"https://example.com/cam",
		"",
		"",
		"",
	)
	if err != nil {
		t.Fatalf("derive canonical stream fields: %v", err)
	}
	if fields.CaptureType != CaptureTypeHLS {
		t.Fatalf("capture_type=%q want %q", fields.CaptureType, CaptureTypeHLS)
	}
	if fields.SourceFamily != SourceFamilyVideoManifest {
		t.Fatalf("source_family=%q want %q", fields.SourceFamily, SourceFamilyVideoManifest)
	}
	if fields.ExecutionClass != ExecutionClassVideoLive {
		t.Fatalf("execution_class=%q want %q", fields.ExecutionClass, ExecutionClassVideoLive)
	}
}

func TestDeriveCanonicalStreamFieldsRejectsUnknown(t *testing.T) {
	if _, err := DeriveCanonicalStreamFields("https://example.com/page", "", "", "", ""); err == nil {
		t.Fatalf("expected unknown capture type error")
	}
}

func TestDeriveCanonicalStreamFieldsOverridesExplicitVideoForImageURLs(t *testing.T) {
	fields, err := DeriveCanonicalStreamFields(
		"https://www.seattle.gov/trafficcams/images/3_Stewart_NS.jpg",
		"https://www.seattle.gov/trafficcams/",
		CaptureTypeHTTPVideo,
		SourceFamilyVideoStream,
		ExecutionClassVideoLive,
	)
	if err != nil {
		t.Fatalf("derive canonical stream fields: %v", err)
	}
	if fields.CaptureType != CaptureTypeStillImage {
		t.Fatalf("capture_type=%q want %q", fields.CaptureType, CaptureTypeStillImage)
	}
	if fields.SourceFamily != SourceFamilyStillImage {
		t.Fatalf("source_family=%q want %q", fields.SourceFamily, SourceFamilyStillImage)
	}
	if fields.ExecutionClass != ExecutionClassImagePoll {
		t.Fatalf("execution_class=%q want %q", fields.ExecutionClass, ExecutionClassImagePoll)
	}
}

func TestDeriveCanonicalStreamFieldsOverridesExplicitHTTPVideoForHLSURLs(t *testing.T) {
	fields, err := DeriveCanonicalStreamFields(
		"https://example.com/live/cam.m3u8",
		"https://example.com/cam",
		CaptureTypeHTTPVideo,
		SourceFamilyVideoStream,
		ExecutionClassVideoLive,
	)
	if err != nil {
		t.Fatalf("derive canonical stream fields: %v", err)
	}
	if fields.CaptureType != CaptureTypeHLS {
		t.Fatalf("capture_type=%q want %q", fields.CaptureType, CaptureTypeHLS)
	}
	if fields.SourceFamily != SourceFamilyVideoManifest {
		t.Fatalf("source_family=%q want %q", fields.SourceFamily, SourceFamilyVideoManifest)
	}
	if fields.ExecutionClass != ExecutionClassVideoLive {
		t.Fatalf("execution_class=%q want %q", fields.ExecutionClass, ExecutionClassVideoLive)
	}
}

func TestInferCaptureTypePrefersYouTubeAndImages(t *testing.T) {
	captureType, reason := InferCaptureType("YouTube", "https://youtu.be/abc123", "")
	if captureType != CaptureTypeYouTubeWatch || reason != "youtube_watch_url" {
		t.Fatalf("youtube infer = %q/%q", captureType, reason)
	}

	captureType, reason = InferCaptureType("", "https://example.com/cam.jpg", "")
	if captureType != CaptureTypeStillImage || reason != "still_image_url" {
		t.Fatalf("still image infer = %q/%q", captureType, reason)
	}

	captureType, reason = InferCaptureType("", "https://example.com/manifest/live.mpd", "")
	if captureType != CaptureTypeDASH || reason != "dash_url" {
		t.Fatalf("dash infer = %q/%q", captureType, reason)
	}
}

func TestDefaultExecutionClassForCaptureTypeUsesDirectForYouTube(t *testing.T) {
	if got := DefaultExecutionClassForCaptureType(CaptureTypeYouTubeWatch); got != ExecutionClassYouTubeDirect {
		t.Fatalf("execution_class=%q want %q", got, ExecutionClassYouTubeDirect)
	}
}

func TestDeriveCaptureProfileVideoDefaults(t *testing.T) {
	profile, err := DeriveCaptureProfile(
		"TOPIS",
		"https://example.com/live/cam.m3u8",
		"https://example.com/cam",
		"",
		"",
		"",
		map[string]any{},
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("derive capture profile: %v", err)
	}
	if profile.CaptureFamily != CaptureFamilyContinuousVideo {
		t.Fatalf("capture_family=%q want %q", profile.CaptureFamily, CaptureFamilyContinuousVideo)
	}
	if profile.ExpectedFPS == nil || *profile.ExpectedFPS != 30 {
		t.Fatalf("expected_fps=%v want 30", profile.ExpectedFPS)
	}
	if profile.ExpectedImageIntervalSec != nil {
		t.Fatalf("expected_image_interval_sec=%v want nil", profile.ExpectedImageIntervalSec)
	}
}

func TestDeriveCaptureProfileSeattleImageDefaults(t *testing.T) {
	profile, err := DeriveCaptureProfile(
		"SDOT",
		"https://www.seattle.gov/trafficcams/images/3_Stewart_NS.jpg",
		"https://www.seattle.gov/trafficcams/lakecity_145th.htm",
		"",
		"",
		"",
		map[string]any{},
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("derive capture profile: %v", err)
	}
	if profile.CaptureFamily != CaptureFamilySnapshotImage {
		t.Fatalf("capture_family=%q want %q", profile.CaptureFamily, CaptureFamilySnapshotImage)
	}
	if profile.ExpectedFPS != nil {
		t.Fatalf("expected_fps=%v want nil", profile.ExpectedFPS)
	}
	if profile.ExpectedImageIntervalSec == nil || *profile.ExpectedImageIntervalSec != 300 {
		t.Fatalf("expected_image_interval_sec=%v want 300", profile.ExpectedImageIntervalSec)
	}
}

func TestCaptureTypeMetadata(t *testing.T) {
	types := CaptureTypes()
	if len(types) == 0 {
		t.Fatalf("expected capture type metadata")
	}

	byValue := make(map[string]CaptureTypeInfo, len(types))
	for _, typ := range types {
		if typ.Value == "" {
			t.Fatalf("capture type value must not be empty: %#v", typ)
		}
		if typ.Label == "" {
			t.Fatalf("capture type label must not be empty: %#v", typ)
		}
		if _, exists := byValue[typ.Value]; exists {
			t.Fatalf("duplicate capture type metadata for %q", typ.Value)
		}
		byValue[typ.Value] = typ
	}

	for _, value := range []string{CaptureTypeYouTubeWatch, CaptureTypeHLS, CaptureTypeHTTPVideo} {
		typ, ok := byValue[value]
		if !ok {
			t.Fatalf("missing metadata for %q", value)
		}
		if !typ.Recordable || !typ.Video {
			t.Fatalf("%q metadata recordable/video = %v/%v, want true/true", value, typ.Recordable, typ.Video)
		}
		if label, ok := CaptureTypeLabel(value); !ok || label != typ.Label {
			t.Fatalf("label(%q) = %q/%v, want %q/true", value, label, ok, typ.Label)
		}
	}

	still := byValue[CaptureTypeStillImage]
	if still.Recordable || still.Video {
		t.Fatalf("still image metadata recordable/video = %v/%v, want false/false", still.Recordable, still.Video)
	}
}
