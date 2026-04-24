package capture

import (
	"sort"
	"strings"
)

type CanonicalRepairInput struct {
	Provider              string
	SourceURL             string
	SourcePageURL         string
	CurrentSourceFamily   string
	CurrentCaptureType    string
	CurrentExecutionClass string
	ResolvedCaptureType   string
}

type CanonicalRepairProposal struct {
	ProposedSourceFamily   string
	ProposedCaptureType    string
	ProposedExecutionClass string
	WouldChange            bool
	ReviewRequired         bool
	Reasons                []string
}

func ProposeCanonicalStreamRepair(input CanonicalRepairInput) CanonicalRepairProposal {
	currentSourceFamily := ""
	if normalized, ok := NormalizeSourceFamily(input.CurrentSourceFamily); ok {
		currentSourceFamily = normalized
	}
	currentCaptureType := ""
	if normalized, ok := NormalizeCaptureType(input.CurrentCaptureType); ok {
		currentCaptureType = normalized
	}
	currentExecutionClass := ""
	if normalized, ok := NormalizeExecutionClass(input.CurrentExecutionClass); ok {
		currentExecutionClass = normalized
	}
	resolvedCaptureType := ""
	if normalized, ok := NormalizeCaptureType(input.ResolvedCaptureType); ok {
		resolvedCaptureType = normalized
	}

	reasons := make([]string, 0, 8)
	proposedCaptureType := currentCaptureType
	inferredCaptureType, inferredReason := InferCaptureType(input.Provider, input.SourceURL, input.SourcePageURL)
	switch {
	case inferredCaptureType == CaptureTypeYouTubeWatch:
		proposedCaptureType = inferredCaptureType
		reasons = append(reasons, inferredReason)
	case resolvedCaptureType != "" && resolvedCaptureType != CaptureTypeUnknown:
		proposedCaptureType = resolvedCaptureType
		reasons = append(reasons, "resolved_capture_type")
	case inferredCaptureType != "":
		proposedCaptureType = inferredCaptureType
		reasons = append(reasons, inferredReason)
	}
	if proposedCaptureType == "" {
		proposedCaptureType = CaptureTypeUnknown
		reasons = append(reasons, "capture_type_unknown")
	}

	proposedExecutionClass, executionReason, reviewForExecution := proposeExecutionClass(proposedCaptureType, currentExecutionClass)
	if executionReason != "" {
		reasons = append(reasons, executionReason)
	}
	proposedSourceFamily, sourceFamilyReason := proposeSourceFamily(proposedCaptureType, currentSourceFamily)
	if sourceFamilyReason != "" {
		reasons = append(reasons, sourceFamilyReason)
	}

	reviewRequired := reviewForExecution
	if proposedCaptureType == CaptureTypeUnknown || proposedExecutionClass == "" || proposedSourceFamily == "" {
		reviewRequired = true
	}
	if strings.TrimSpace(input.SourceURL) == "" && strings.TrimSpace(input.SourcePageURL) == "" {
		reviewRequired = true
		reasons = append(reasons, "missing_source_url")
	}
	if currentExecutionClass == ExecutionClassYouTubeRelay && proposedCaptureType != CaptureTypeYouTubeWatch {
		reviewRequired = true
		reasons = append(reasons, "relay_non_youtube_mismatch")
	}

	return CanonicalRepairProposal{
		ProposedSourceFamily:   proposedSourceFamily,
		ProposedCaptureType:    proposedCaptureType,
		ProposedExecutionClass: proposedExecutionClass,
		WouldChange: currentSourceFamily != proposedSourceFamily ||
			currentCaptureType != proposedCaptureType ||
			currentExecutionClass != proposedExecutionClass,
		ReviewRequired: reviewRequired,
		Reasons:        uniqueCanonicalReasons(reasons),
	}
}

func shouldOverrideExplicitCaptureType(currentCaptureType string, inferredCaptureType string) bool {
	current, ok := NormalizeCaptureType(currentCaptureType)
	if !ok {
		current = ""
	}
	inferred, ok := NormalizeCaptureType(inferredCaptureType)
	if !ok || inferred == "" || inferred == CaptureTypeUnknown || inferred == current {
		return false
	}
	switch inferred {
	case CaptureTypeYouTubeWatch, CaptureTypeStillImage, CaptureTypeHLS, CaptureTypeDASH, CaptureTypeRTSP, CaptureTypeRTMP:
		return true
	default:
		return false
	}
}

func proposeExecutionClass(captureType string, currentExecutionClass string) (string, string, bool) {
	switch captureType {
	case CaptureTypeYouTubeWatch:
		if currentExecutionClass == ExecutionClassYouTubeRelay {
			return ExecutionClassYouTubeDirect, "youtube_relay_hard_cut", false
		}
		return ExecutionClassYouTubeDirect, "youtube_direct_default", false
	case CaptureTypeStillImage:
		return ExecutionClassImagePoll, "image_poll_from_capture_type", false
	case CaptureTypeHLS, CaptureTypeDASH, CaptureTypeRTSP, CaptureTypeRTMP, CaptureTypeHTTPVideo:
		return ExecutionClassVideoLive, "video_live_from_capture_type", false
	case CaptureTypeWebRTC:
		if currentExecutionClass != "" {
			return currentExecutionClass, "keep_current_execution_class", true
		}
		return "", "webrtc_requires_review", true
	case CaptureTypeUnknown:
		if currentExecutionClass != "" {
			return currentExecutionClass, "keep_current_execution_class", true
		}
		return "", "unknown_requires_review", true
	default:
		if currentExecutionClass != "" {
			return currentExecutionClass, "keep_current_execution_class", true
		}
		return "", "execution_class_unknown", true
	}
}

func proposeSourceFamily(captureType, currentSourceFamily string) (string, string) {
	switch captureType {
	case CaptureTypeYouTubeWatch:
		return SourceFamilyWatchPage, "watch_page_from_capture_type"
	case CaptureTypeHLS, CaptureTypeDASH:
		return SourceFamilyVideoManifest, "video_manifest_from_capture_type"
	case CaptureTypeRTSP, CaptureTypeRTMP, CaptureTypeHTTPVideo:
		return SourceFamilyVideoStream, "video_stream_from_capture_type"
	case CaptureTypeStillImage:
		return SourceFamilyStillImage, "still_image_from_capture_type"
	case CaptureTypeWebRTC:
		return SourceFamilyEmbedPage, "embed_page_from_capture_type"
	case CaptureTypeUnknown:
		if currentSourceFamily != "" {
			return currentSourceFamily, "keep_current_source_family"
		}
		return "", "source_family_unknown"
	default:
		if currentSourceFamily != "" {
			return currentSourceFamily, "keep_current_source_family"
		}
		return "", "source_family_unknown"
	}
}

func uniqueCanonicalReasons(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, v := range in {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}
