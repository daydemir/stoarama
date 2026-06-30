package capture

import (
	"fmt"
	"net/url"
	"strings"
)

const (
	CaptureTypeYouTubeWatch = "youtube_watch"
	CaptureTypeHLS          = "hls"
	CaptureTypeDASH         = "dash"
	CaptureTypeRTSP         = "rtsp"
	CaptureTypeRTMP         = "rtmp"
	CaptureTypeHTTPVideo    = "http_video"
	CaptureTypeStillImage   = "still_image"
	CaptureTypeWebRTC       = "webrtc"
	CaptureTypeUnknown      = "unknown"

	SourceFamilyWatchPage     = "watch_page"
	SourceFamilyVideoManifest = "video_manifest"
	SourceFamilyVideoStream   = "video_stream"
	SourceFamilyStillImage    = "still_image"
	SourceFamilyProviderAPI   = "provider_api"
	SourceFamilyEmbedPage     = "embed_page"

	ExecutionClassYouTubeDirect = "youtube_direct"
	ExecutionClassYouTubeRelay  = "youtube_relay"
	ExecutionClassVideoLive     = "video_live"
	ExecutionClassImagePoll     = "image_poll"

	CaptureFamilyContinuousVideo = "continuous_video"
	CaptureFamilySnapshotImage   = "snapshot_image"
)

type CanonicalStreamFields struct {
	SourceURL      string
	SourcePageURL  string
	SourceFamily   string
	CaptureType    string
	ExecutionClass string
}

type CaptureProfile struct {
	CanonicalStreamFields
	CaptureFamily            string
	ExpectedFPS              *float64
	ExpectedImageIntervalSec *int
}

func NormalizeCaptureType(raw string) (string, bool) {
	v := strings.TrimSpace(strings.ToLower(raw))
	switch v {
	case CaptureTypeYouTubeWatch, CaptureTypeHLS, CaptureTypeDASH, CaptureTypeRTSP, CaptureTypeRTMP, CaptureTypeHTTPVideo, CaptureTypeStillImage, CaptureTypeWebRTC, CaptureTypeUnknown:
		return v, true
	case string(ModeYouTubeLive), string(ModeYouTubeRelay):
		return CaptureTypeYouTubeWatch, true
	case string(ModeHLSLive):
		return CaptureTypeHLS, true
	case string(ModeImagePoll):
		return CaptureTypeStillImage, true
	case string(ModeFFmpegDirect):
		return CaptureTypeHTTPVideo, true
	case string(ModeAuto), string(ModeUnsupported), "":
		return CaptureTypeUnknown, v == ""
	default:
		return "", false
	}
}

func NormalizeExecutionClass(raw string) (string, bool) {
	v := strings.TrimSpace(strings.ToLower(raw))
	switch v {
	case ExecutionClassYouTubeDirect, ExecutionClassYouTubeRelay, ExecutionClassVideoLive, ExecutionClassImagePoll:
		return v, true
	case string(ModeYouTubeLive):
		return ExecutionClassYouTubeDirect, true
	case string(ModeHLSLive), string(ModeFFmpegDirect):
		return ExecutionClassVideoLive, true
	default:
		return "", false
	}
}

func NormalizeSourceFamily(raw string) (string, bool) {
	v := strings.TrimSpace(strings.ToLower(raw))
	switch v {
	case SourceFamilyWatchPage, SourceFamilyVideoManifest, SourceFamilyVideoStream, SourceFamilyStillImage, SourceFamilyProviderAPI, SourceFamilyEmbedPage:
		return v, true
	default:
		return "", false
	}
}

func NormalizeCaptureFamily(raw string) (string, bool) {
	v := strings.TrimSpace(strings.ToLower(raw))
	switch v {
	case CaptureFamilyContinuousVideo, CaptureFamilySnapshotImage:
		return v, true
	default:
		return "", false
	}
}

func DeriveCanonicalStreamFields(sourceURL string, sourcePageURL string, captureTypeRaw string, sourceFamilyRaw string, executionClassRaw string) (CanonicalStreamFields, error) {
	fields := CanonicalStreamFields{
		SourceURL:     strings.TrimSpace(sourceURL),
		SourcePageURL: strings.TrimSpace(sourcePageURL),
	}

	captureType := strings.TrimSpace(captureTypeRaw)
	inferredCaptureType, _ := InferCaptureType("", fields.SourceURL, fields.SourcePageURL)
	if captureType == "" {
		inferred := inferredCaptureType
		captureType = inferred
	}
	captureTypeValue, ok := NormalizeCaptureType(captureType)
	if !ok || captureTypeValue == CaptureTypeUnknown {
		return CanonicalStreamFields{}, fmt.Errorf("capture_type could not be derived; provide capture_type explicitly")
	}
	if shouldOverrideExplicitCaptureType(captureTypeValue, inferredCaptureType) {
		captureTypeValue = inferredCaptureType
		sourceFamilyRaw = ""
		executionClassRaw = ""
	}
	fields.CaptureType = captureTypeValue

	sourceFamily := strings.TrimSpace(sourceFamilyRaw)
	if sourceFamily == "" {
		sourceFamily = DefaultSourceFamilyForCaptureType(captureTypeValue)
	}
	sourceFamilyValue, ok := NormalizeSourceFamily(sourceFamily)
	if !ok || sourceFamilyValue == "" {
		return CanonicalStreamFields{}, fmt.Errorf("invalid source_family %q", sourceFamily)
	}
	fields.SourceFamily = sourceFamilyValue

	executionClass := strings.TrimSpace(executionClassRaw)
	if executionClass == "" {
		executionClass = DefaultExecutionClassForCaptureType(captureTypeValue)
	}
	executionClassValue, ok := NormalizeExecutionClass(executionClass)
	if !ok || executionClassValue == "" {
		return CanonicalStreamFields{}, fmt.Errorf("invalid execution_class %q", executionClass)
	}
	fields.ExecutionClass = executionClassValue
	return fields, nil
}

func DeriveCaptureProfile(provider string, sourceURL string, sourcePageURL string, captureTypeRaw string, sourceFamilyRaw string, executionClassRaw string, executionConfig map[string]any, explicitExpectedFPS *float64, explicitExpectedImageIntervalSec *int) (CaptureProfile, error) {
	fields, err := DeriveCanonicalStreamFields(sourceURL, sourcePageURL, captureTypeRaw, sourceFamilyRaw, executionClassRaw)
	if err != nil {
		return CaptureProfile{}, err
	}
	family, expectedFPS, expectedImageIntervalSec, err := deriveCaptureCadence(provider, fields.SourceURL, fields.SourcePageURL, fields.CaptureType, executionConfig, explicitExpectedFPS, explicitExpectedImageIntervalSec)
	if err != nil {
		return CaptureProfile{}, err
	}
	return CaptureProfile{
		CanonicalStreamFields:    fields,
		CaptureFamily:            family,
		ExpectedFPS:              expectedFPS,
		ExpectedImageIntervalSec: expectedImageIntervalSec,
	}, nil
}

func DefaultSourceFamilyForCaptureType(captureType string) string {
	ct, ok := NormalizeCaptureType(captureType)
	if !ok {
		return ""
	}
	switch ct {
	case CaptureTypeYouTubeWatch:
		return SourceFamilyWatchPage
	case CaptureTypeHLS, CaptureTypeDASH:
		return SourceFamilyVideoManifest
	case CaptureTypeRTSP, CaptureTypeRTMP, CaptureTypeHTTPVideo:
		return SourceFamilyVideoStream
	case CaptureTypeStillImage:
		return SourceFamilyStillImage
	case CaptureTypeWebRTC:
		return SourceFamilyEmbedPage
	default:
		return ""
	}
}

func DefaultExecutionClassForCaptureType(captureType string) string {
	ct, ok := NormalizeCaptureType(captureType)
	if !ok {
		return ""
	}
	switch ct {
	case CaptureTypeYouTubeWatch:
		return ExecutionClassYouTubeDirect
	case CaptureTypeStillImage:
		return ExecutionClassImagePoll
	case CaptureTypeHLS, CaptureTypeDASH, CaptureTypeRTSP, CaptureTypeRTMP, CaptureTypeHTTPVideo:
		return ExecutionClassVideoLive
	default:
		return ""
	}
}

func CaptureFamilyForCaptureType(captureType string) string {
	ct, ok := NormalizeCaptureType(captureType)
	if !ok {
		return ""
	}
	switch ct {
	case CaptureTypeStillImage:
		return CaptureFamilySnapshotImage
	default:
		return CaptureFamilyContinuousVideo
	}
}

func ModeToExecutionClass(mode Mode) string {
	switch NormalizeMode(string(mode)) {
	case ModeYouTubeLive, ModeYouTubeRelay:
		return ExecutionClassYouTubeDirect
	case ModeHLSLive, ModeFFmpegDirect:
		return ExecutionClassVideoLive
	case ModeImagePoll:
		return ExecutionClassImagePoll
	default:
		return ""
	}
}

func EffectiveExecutionClass(spec StreamSpec) string {
	return ModeToExecutionClass(EffectiveMode(spec))
}

func ModeToCaptureType(mode Mode) string {
	switch NormalizeMode(string(mode)) {
	case ModeYouTubeLive, ModeYouTubeRelay:
		return CaptureTypeYouTubeWatch
	case ModeHLSLive:
		return CaptureTypeHLS
	case ModeImagePoll:
		return CaptureTypeStillImage
	case ModeFFmpegDirect:
		return CaptureTypeHTTPVideo
	default:
		return CaptureTypeUnknown
	}
}

func ClassifyCaptureType(spec StreamSpec) string {
	return ModeToCaptureType(ClassifyMode(spec))
}

func DetectCaptureType(sourceURL string, sourcePageURL string) string {
	return ClassifyCaptureType(StreamSpec{
		StreamURL:     strings.TrimSpace(sourceURL),
		SourcePageURL: strings.TrimSpace(sourcePageURL),
	})
}

func InferCaptureType(provider string, sourceURL string, sourcePageURL string) (string, string) {
	providerValue := strings.TrimSpace(strings.ToLower(provider))
	candidates := []struct {
		value          string
		allowHTTPVideo bool
	}{
		{value: strings.TrimSpace(sourceURL), allowHTTPVideo: true},
		{value: strings.TrimSpace(sourcePageURL), allowHTTPVideo: false},
	}
	for _, candidate := range candidates {
		raw := candidate.value
		if raw == "" {
			continue
		}
		switch {
		case isYouTubeWatchURL(raw) || providerValue == "youtube":
			return CaptureTypeYouTubeWatch, "youtube_watch_url"
		case looksLikeImageURL(raw):
			return CaptureTypeStillImage, "still_image_url"
		case strings.Contains(strings.ToLower(raw), ".m3u8") || strings.Contains(strings.ToLower(raw), "!hls"):
			return CaptureTypeHLS, "hls_url"
		case strings.Contains(strings.ToLower(raw), ".mpd"):
			return CaptureTypeDASH, "dash_url"
		case strings.HasPrefix(strings.ToLower(raw), "rtsp://"):
			return CaptureTypeRTSP, "rtsp_url"
		case strings.HasPrefix(strings.ToLower(raw), "rtmp://"):
			return CaptureTypeRTMP, "rtmp_url"
		case candidate.allowHTTPVideo && looksLikeHTTPVideoURL(raw):
			return CaptureTypeHTTPVideo, "http_video_url"
		}
	}
	return "", ""
}

func deriveCaptureCadence(provider string, sourceURL string, sourcePageURL string, captureType string, executionConfig map[string]any, explicitExpectedFPS *float64, explicitExpectedImageIntervalSec *int) (string, *float64, *int, error) {
	family := CaptureFamilyForCaptureType(captureType)
	if family == "" {
		return "", nil, nil, fmt.Errorf("invalid capture family for capture_type %q", captureType)
	}
	switch family {
	case CaptureFamilyContinuousVideo:
		if explicitExpectedImageIntervalSec != nil {
			return "", nil, nil, fmt.Errorf("expected_image_interval_sec is only valid for snapshot_image streams")
		}
		if explicitExpectedFPS != nil && *explicitExpectedFPS != float64(SegmentTargetFPS) {
			return "", nil, nil, fmt.Errorf("expected_fps must be %d for continuous_video streams", SegmentTargetFPS)
		}
		value := float64(SegmentTargetFPS)
		return family, &value, nil, nil
	case CaptureFamilySnapshotImage:
		if explicitExpectedFPS != nil {
			return "", nil, nil, fmt.Errorf("expected_fps is only valid for continuous_video streams")
		}
		expectedImageIntervalSec := clonePositiveInt(explicitExpectedImageIntervalSec)
		if expectedImageIntervalSec == nil {
			if configured := GetConfigInt(executionConfig, "expected_image_interval_sec", 0); configured > 0 {
				expectedImageIntervalSec = &configured
			}
		}
		if expectedImageIntervalSec == nil {
			if providerLooksLikeSeattleTraffic(provider, sourceURL, sourcePageURL) {
				value := 300
				expectedImageIntervalSec = &value
			} else if configured := GetConfigInt(executionConfig, "poll_interval_sec", 0); configured > 1 {
				expectedImageIntervalSec = &configured
			} else {
				value := 60
				expectedImageIntervalSec = &value
			}
		}
		return family, nil, expectedImageIntervalSec, nil
	default:
		return "", nil, nil, fmt.Errorf("unsupported capture family %q", family)
	}
}

func providerLooksLikeSeattleTraffic(provider string, sourceURL string, sourcePageURL string) bool {
	p := strings.TrimSpace(strings.ToLower(provider))
	if p == "sdot" {
		return true
	}
	for _, raw := range []string{sourceURL, sourcePageURL} {
		value := strings.TrimSpace(strings.ToLower(raw))
		if value == "" {
			continue
		}
		if strings.Contains(value, "seattle.gov/trafficcams/images/") || strings.Contains(value, "seattle.gov/trafficcams/") {
			return true
		}
	}
	return false
}

func clonePositiveFloat64(value *float64) *float64 {
	if value == nil || *value <= 0 {
		return nil
	}
	out := *value
	return &out
}

func clonePositiveInt(value *int) *int {
	if value == nil || *value <= 0 {
		return nil
	}
	out := *value
	return &out
}

func ResolvedCaptureTypeFromURL(raw string) string {
	u := strings.TrimSpace(strings.ToLower(raw))
	switch {
	case u == "":
		return ""
	case strings.Contains(u, ".m3u8"):
		return CaptureTypeHLS
	case strings.Contains(u, ".mpd"):
		return CaptureTypeDASH
	case strings.HasPrefix(u, "rtsp://"):
		return CaptureTypeRTSP
	case strings.HasPrefix(u, "rtmp://"):
		return CaptureTypeRTMP
	case looksLikeImageURL(u):
		return CaptureTypeStillImage
	case strings.HasPrefix(u, "http://"), strings.HasPrefix(u, "https://"):
		return CaptureTypeHTTPVideo
	default:
		return CaptureTypeUnknown
	}
}

// IsYouTubeWatchURL reports whether raw is a YouTube watch/live page URL. It is
// the exported form of the internal detector used to classify capture types, so
// callers (e.g. the add-stream form) can pick the right provider namespace.
func IsYouTubeWatchURL(raw string) bool {
	return isYouTubeWatchURL(raw)
}

func isYouTubeWatchURL(raw string) bool {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return false
	}
	host := strings.ToLower(strings.TrimSpace(u.Hostname()))
	path := strings.ToLower(strings.TrimSpace(u.Path))
	switch host {
	case "youtube.com", "www.youtube.com", "m.youtube.com", "youtu.be":
		return true
	}
	return strings.Contains(host, "youtube") || strings.HasPrefix(path, "/watch")
}

func looksLikeHTTPVideoURL(raw string) bool {
	v := strings.ToLower(strings.TrimSpace(raw))
	if !strings.HasPrefix(v, "http://") && !strings.HasPrefix(v, "https://") {
		return false
	}
	for _, ext := range []string{".mp4", ".mov", ".mkv", ".ts", ".mjpeg", ".avi"} {
		if strings.Contains(v, ext) {
			return true
		}
	}
	return false
}

func LegacyModeForStream(captureType string, executionClass string) Mode {
	if executionClass != "" {
		if execClass, ok := NormalizeExecutionClass(executionClass); ok {
			switch execClass {
			case ExecutionClassYouTubeDirect, ExecutionClassYouTubeRelay:
				return ModeYouTubeLive
			case ExecutionClassImagePoll:
				return ModeImagePoll
			case ExecutionClassVideoLive:
				if ct, ok := NormalizeCaptureType(captureType); ok && ct == CaptureTypeHLS {
					return ModeHLSLive
				}
				return ModeFFmpegDirect
			}
		}
	}
	if ct, ok := NormalizeCaptureType(captureType); ok {
		switch ct {
		case CaptureTypeYouTubeWatch:
			return ModeYouTubeLive
		case CaptureTypeHLS:
			return ModeHLSLive
		case CaptureTypeStillImage:
			return ModeImagePoll
		case CaptureTypeDASH, CaptureTypeRTSP, CaptureTypeRTMP, CaptureTypeHTTPVideo:
			return ModeFFmpegDirect
		}
	}
	return ModeUnsupported
}
