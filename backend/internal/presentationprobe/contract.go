// Package presentationprobe implements the offline, non-production C2
// presentation-probe artifact contract. It deliberately has no API, database,
// relay, or NAS integration.
package presentationprobe

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

const (
	ParserSchema     = "presentation-edge-v2.0"
	ConfigDomain     = "presentation-probe-config-v2"
	ReportDomain     = "presentation-probe-report-v2"
	ArtifactDomain   = "presentation-probe-artifact-v2"
	CapabilityDomain = "presentation-probe-capability-v2"
	// ContractRevisionSHA256 is the exact SHA-256 of the merged normative
	// docs/CAPTURE_STITCH_PRESENTATION_V2.md consumed by this C2 assessment.
	// A documentation change requires a new assessment; evidence is never
	// generalized to a later contract.
	ContractRevisionSHA256 = "1f7977df2f9016ef727db357c74ae3f1cfe6a776e148d10e5d659bb0f98b422a"
	// FutureHandoffTemporaryFD documents the reviewed collision-safe native
	// protocol. A future launcher must first relocate both received fds to
	// distinct CLOEXEC descriptors >= this value before installing fds 3/4.
	// C2 deliberately ships no launcher or handoff implementation.
	FutureHandoffTemporaryFD = 8
	MaximumInputBytes        = int64(2 << 30)
	MaximumReadBytes         = int64(4 << 30)
	MaximumSeeks             = uint64(1_000_000)
	MaximumUnits             = uint64(100_000)
	MaximumStdoutBytes       = int64(32 << 20)
	MaximumStderrBytes       = int64(64 << 10)
	MaximumWallMilliseconds  = uint64(30_000)
	AVNoPTSValue             = int64(-1 << 63)
)

func ValidPacketFlags(value string) bool {
	switch value {
	case "none", "key", "discard", "corrupt", "key+discard", "key+corrupt", "discard+corrupt", "key+discard+corrupt":
		return true
	default:
		return false
	}
}

type ProvenanceClass string

const (
	ProvenanceCI          ProvenanceClass = "ci_test"
	ProvenanceDevelopment ProvenanceClass = "development"
)

func (p ProvenanceClass) Valid() bool {
	return p == ProvenanceCI || p == ProvenanceDevelopment
}

type AxisName string

const (
	AxisDemuxVideo        AxisName = "demux_video"
	AxisRawVideo          AxisName = "raw_video"
	AxisVideoPresentation AxisName = "video_presentation"
	AxisDemuxAudio        AxisName = "demux_audio"
	AxisRawAudio          AxisName = "raw_audio"
	AxisAudioSample       AxisName = "audio_sample"
)

var AxisOrder = [...]AxisName{
	AxisDemuxVideo,
	AxisRawVideo,
	AxisVideoPresentation,
	AxisDemuxAudio,
	AxisRawAudio,
	AxisAudioSample,
}

type AxisStatus string

const (
	AxisComplete   AxisStatus = "complete"
	AxisUnknown    AxisStatus = "unknown"
	AxisNotPresent AxisStatus = "not_present"
)

type AxisReason string

const (
	ReasonProbeTimeout          AxisReason = "probe_timeout"
	ReasonProbeResourceLimit    AxisReason = "probe_resource_limit"
	ReasonToolIncompatible      AxisReason = "tool_incompatible"
	ReasonProbeUnavailable      AxisReason = "probe_unavailable"
	ReasonPresentationAmbiguous AxisReason = "presentation_ambiguous"
	ReasonRawExtentUnavailable  AxisReason = "raw_extent_unavailable"
	ReasonAudioNotPresent       AxisReason = "audio_not_present"
)

var lowerHex64 = regexp.MustCompile(`^[0-9a-f]{64}$`)
var safeToken = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

func ValidSHA256(value string) bool { return lowerHex64.MatchString(value) }
func ValidToken(value string) bool  { return safeToken.MatchString(value) }

func SHA256(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func ValidateAxisReason(axis AxisName, status AxisStatus, reason AxisReason) error {
	if status == AxisComplete {
		if reason != "" {
			return errors.New("complete axis carries reason")
		}
		return nil
	}
	if status == AxisNotPresent {
		if axis != AxisDemuxAudio && axis != AxisRawAudio && axis != AxisAudioSample {
			return fmt.Errorf("%s cannot be not_present", axis)
		}
		if reason != ReasonAudioNotPresent {
			return errors.New("not_present audio axis must use audio_not_present")
		}
		return nil
	}
	if status != AxisUnknown {
		return fmt.Errorf("invalid axis status %q", status)
	}
	switch reason {
	case ReasonProbeTimeout, ReasonProbeResourceLimit, ReasonToolIncompatible,
		ReasonProbeUnavailable, ReasonPresentationAmbiguous, ReasonRawExtentUnavailable:
	default:
		return fmt.Errorf("invalid unknown reason %q", reason)
	}
	if reason == ReasonRawExtentUnavailable && axis != AxisRawVideo && axis != AxisRawAudio {
		return errors.New("raw_extent_unavailable is restricted to raw axes")
	}
	return nil
}

func ValidateASCII(value, field string, maximum int) error {
	if value == "" || len(value) > maximum {
		return fmt.Errorf("%s length outside 1..%d", field, maximum)
	}
	for _, r := range value {
		if r < 0x20 || r > 0x7e {
			return fmt.Errorf("%s is not printable ASCII", field)
		}
	}
	if strings.ContainsAny(value, "\r\n\x00") {
		return fmt.Errorf("%s contains a control character", field)
	}
	return nil
}
