package stitchcert

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	PolicyVersion = "native-window-v1"
	MaxClips      = 1024
	MaxSourceByte = int64(64 << 30)
	MaxRunByte    = int64(32 << 30)
)

type ManifestClip struct {
	Ordinal                  int       `json:"ordinal"`
	ClipID                   int64     `json:"clip_id"`
	RecordingID              int64     `json:"recording_id"`
	RecordingJobID           int64     `json:"recording_job_id"`
	RelativePath             string    `json:"relative_path"`
	SizeBytes                int64     `json:"size_bytes"`
	SHA256                   string    `json:"sha256"`
	ClipStartAt              time.Time `json:"clip_start_at"`
	ClipEndAt                time.Time `json:"clip_end_at"`
	CaptureGeneration        string    `json:"capture_generation"`
	CaptureSequence          int64     `json:"capture_sequence"`
	CaptureAttemptID         string    `json:"capture_attempt_id"`
	TimestampContractVersion string    `json:"timestamp_contract_version"`
	TimestampContractStatus  string    `json:"timestamp_contract_status"`
	TimestampContractReason  string    `json:"timestamp_contract_reason"`
	TimestampContractSHA256  string    `json:"timestamp_contract_sha256"`
}

type TimestampContract struct {
	Version        int                      `json:"version"`
	Mode           string                   `json:"mode"`
	AudioSelection string                   `json:"audio_selection"`
	Tracks         []TimestampContractTrack `json:"tracks"`
}

type TimestampContractTrack struct {
	StreamIndex          int    `json:"stream_index"`
	MediaType            string `json:"media_type"`
	TimeBaseNum          int64  `json:"time_base_num"`
	TimeBaseDen          int64  `json:"time_base_den"`
	FirstTimestamp       int64  `json:"first_timestamp"`
	LastTimestamp        int64  `json:"last_timestamp"`
	LastDuration         int64  `json:"last_duration"`
	UnitCount            int64  `json:"unit_count"`
	SampleRate           int64  `json:"sample_rate,omitempty"`
	LastSampleCount      int64  `json:"last_sample_count,omitempty"`
	CodecSignatureSHA256 string `json:"codec_signature_sha256"`
}

type Timeline struct {
	ExpectedSeconds          float64 `json:"expected_seconds"`
	CoveredSeconds           float64 `json:"covered_seconds"`
	CoveragePct              float64 `json:"coverage_pct"`
	LeadingGapSeconds        float64 `json:"leading_gap_seconds"`
	LargestInternalGapSecond float64 `json:"largest_internal_gap_seconds"`
	TrailingGapSeconds       float64 `json:"trailing_gap_seconds"`
	LargestGapSeconds        float64 `json:"largest_gap_seconds"`
	GapCount                 int     `json:"gap_count"`
	GapOver30sCount          int     `json:"gap_over_30s_count"`
	GapOver5mCount           int     `json:"gap_over_5m_count"`
	OverlapCount             int     `json:"overlap_count"`
	OverlapSeconds           float64 `json:"overlap_seconds"`
}

type ClipFact struct {
	ManifestClip
	SidecarSHA256               string                  `json:"sidecar_sha256"`
	FileIdentity                FileIdentity            `json:"file_identity"`
	NativeSignature             map[string]any          `json:"native_signature"`
	NativeSignatureSHA256       string                  `json:"native_signature_sha256"`
	StrictDecode                string                  `json:"strict_decode"`
	RecomputedTimestampContract *TimestampContract      `json:"recomputed_timestamp_contract,omitempty"`
	VideoTimeline               *StreamTimelineEvidence `json:"video_timeline,omitempty"`
	AudioPresent                bool                    `json:"audio_present"`
	AudioTimeline               *AudioTimelineEvidence  `json:"audio_timeline,omitempty"`
}

type FileIdentity struct {
	Size    int64 `json:"size"`
	MTimeNS int64 `json:"mtime_ns"`
	CTimeNS int64 `json:"ctime_ns"`
	Inode   int64 `json:"inode"`
	Device  int64 `json:"device"`
}

type RunFact struct {
	Ordinal               int    `json:"ordinal"`
	FirstClipOrdinal      int    `json:"first_clip_ordinal"`
	LastClipOrdinal       int    `json:"last_clip_ordinal"`
	ClipCount             int    `json:"clip_count"`
	NativeSignatureSHA256 string `json:"native_signature_sha256"`
	CaptureGeneration     string `json:"capture_generation"`
	CaptureAttemptID      string `json:"capture_attempt_id"`
	TimestampContract     string `json:"timestamp_contract_version"`
	BoundaryReason        string `json:"boundary_reason"`
	ValidationStatus      string `json:"validation_status"`
	SourceBytes           int64  `json:"source_bytes"`
}

// SeamFact proves whether two adjacent frozen clips may be placed in the same
// lossless native run. Ambiguous or real temporal gaps are safe run boundaries,
// never concatenated seams.
type SeamFact struct {
	Ordinal               int                 `json:"ordinal"`
	PreviousClipID        int64               `json:"previous_clip_id"`
	NextClipID            int64               `json:"next_clip_id"`
	CaptureGeneration     string              `json:"capture_generation"`
	PreviousSequence      int64               `json:"previous_capture_sequence"`
	NextSequence          int64               `json:"next_capture_sequence"`
	NativeSignatureSHA256 string              `json:"native_signature_sha256"`
	TimelineBasis         string              `json:"timeline_basis"`
	CaptureContract       string              `json:"capture_contract"`
	PreviousFrames        []SeamFrameEvidence `json:"previous_frames"`
	NextFrames            []SeamFrameEvidence `json:"next_frames"`
	Confidence            string              `json:"confidence"`
	Verdict               string              `json:"verdict"`
	Reason                string              `json:"reason"`
}

type SeamFrameEvidence struct {
	BestEffortTimestamp int64  `json:"best_effort_timestamp"`
	DurationTimestamp   int64  `json:"duration_timestamp"`
	TimeBaseNumerator   int64  `json:"time_base_numerator"`
	TimeBaseDenominator int64  `json:"time_base_denominator"`
	PacketDTS           *int64 `json:"packet_dts"`
	KeyFrame            bool   `json:"key_frame"`
	PictureType         string `json:"picture_type"`
	DecodedSHA256       string `json:"decoded_sha256"`
	PacketSHA256        string `json:"packet_sha256"`
}

// StreamTimelineEvidence is presentation-order evidence from one stored clip.
// Rational timestamp ticks are retained rather than converted to floats. A
// continuous clip has no duplicate, non-monotonic, or discontinuous step.
type StreamTimelineEvidence struct {
	FrameCount            int64 `json:"frame_count"`
	FirstTimestamp        int64 `json:"first_timestamp"`
	LastTimestamp         int64 `json:"last_timestamp"`
	LastDurationTimestamp int64 `json:"last_duration_timestamp"`
	TimeBaseNumerator     int64 `json:"time_base_numerator"`
	TimeBaseDenominator   int64 `json:"time_base_denominator"`
	DuplicateTimestamps   int64 `json:"duplicate_timestamp_count"`
	NonMonotonicSteps     int64 `json:"non_monotonic_step_count"`
	DiscontinuousSteps    int64 `json:"discontinuous_step_count"`
}

type AudioTimelineEvidence struct {
	SampleRate          int64 `json:"sample_rate"`
	FirstSample         int64 `json:"first_sample"`
	EndSample           int64 `json:"end_sample"`
	SampleCount         int64 `json:"sample_count"`
	DuplicateTimestamps int64 `json:"duplicate_timestamp_count"`
	NonMonotonicSteps   int64 `json:"non_monotonic_step_count"`
	DiscontinuousSteps  int64 `json:"discontinuous_step_count"`
}

type AudioSeamFact struct {
	Ordinal             int    `json:"ordinal"`
	PreviousClipID      int64  `json:"previous_clip_id"`
	NextClipID          int64  `json:"next_clip_id"`
	SampleRate          int64  `json:"sample_rate"`
	PreviousEndSample   int64  `json:"previous_end_sample"`
	NextStartSample     int64  `json:"next_start_sample"`
	PreviousSampleCount int64  `json:"previous_sample_count"`
	NextSampleCount     int64  `json:"next_sample_count"`
	CaptureAttemptID    string `json:"capture_attempt_id"`
	TimestampContract   string `json:"timestamp_contract_version"`
	Verdict             string `json:"verdict"`
	Reason              string `json:"reason"`
}

type Report struct {
	SchemaVersion                  int             `json:"schema_version"`
	PolicyVersion                  string          `json:"policy_version"`
	TaskID                         int64           `json:"task_id"`
	RecordingID                    int64           `json:"recording_id"`
	RecordingJobID                 int64           `json:"recording_job_id"`
	WindowStartAt                  time.Time       `json:"window_start_at"`
	WindowEndAt                    time.Time       `json:"window_end_at"`
	ClipManifestSHA256             string          `json:"clip_manifest_sha256"`
	InventoryGeneration            string          `json:"inventory_generation"`
	InventoryDigest                string          `json:"inventory_digest"`
	InventoryCompletedAt           time.Time       `json:"inventory_completed_at"`
	Status                         string          `json:"status"`
	NASByteDecodeStatus            string          `json:"nas_byte_decode_status"`
	NativeRunConcatStatus          string          `json:"native_run_concat_status"`
	WithinRunFrameAdjacencyStatus  string          `json:"within_run_frame_adjacency_status"`
	WithinRunAudioContinuityStatus string          `json:"within_run_audio_sample_continuity_status"`
	WindowContinuityStatus         string          `json:"window_continuity_status"`
	Timeline                       Timeline        `json:"timeline"`
	Clips                          []ClipFact      `json:"clips"`
	NativeRuns                     []RunFact       `json:"native_runs"`
	Seams                          []SeamFact      `json:"seams"`
	AudioSeams                     []AudioSeamFact `json:"audio_seams"`
	ReasonCodes                    []string        `json:"reason_codes"`
	ClientVersion                  string          `json:"client_version"`
	FFmpegVersion                  string          `json:"ffmpeg_version"`
	FFprobeVersion                 string          `json:"ffprobe_version"`
	StartedAt                      time.Time       `json:"started_at"`
	CompletedAt                    time.Time       `json:"completed_at"`
	SourceMediaModified            bool            `json:"source_media_modified"`
	Reencoded                      bool            `json:"reencoded"`
	PersistentOutput               bool            `json:"persistent_output_created"`
}

func CanonicalSHA(v any) (string, []byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(v)
	b := bytes.TrimSuffix(buffer.Bytes(), []byte("\n"))
	if err != nil {
		return "", nil, err
	}
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:]), b, nil
}

func ValidateManifest(clips []ManifestClip, recordingID, jobID int64, start, end time.Time) (int64, error) {
	if len(clips) < 1 || len(clips) > MaxClips || !end.After(start) {
		return 0, fmt.Errorf("invalid bounded manifest")
	}
	seenIDs := map[int64]bool{}
	seenPaths := map[string]bool{}
	seenOrder := map[string]bool{}
	lastSequence := map[string]int64{}
	closedGeneration := map[string]bool{}
	previousGeneration := ""
	var total int64
	for i, c := range clips {
		if c.Ordinal != i+1 || c.ClipID <= 0 || c.RecordingID != recordingID || c.RecordingJobID != jobID ||
			!safeRelativePath(c.RelativePath) || c.SizeBytes <= 0 || !lowerHex64(c.SHA256) ||
			!c.ClipEndAt.After(c.ClipStartAt) || !c.ClipEndAt.After(start) || !c.ClipStartAt.Before(end) ||
			c.CaptureGeneration == "" || c.CaptureSequence <= 0 {
			return 0, fmt.Errorf("clip %d has invalid exact identity", i+1)
		}
		if !validTimestampProvenance(c) {
			return 0, fmt.Errorf("clip %d has incoherent timestamp provenance", i+1)
		}
		order := fmt.Sprintf("%s/%d", c.CaptureGeneration, c.CaptureSequence)
		if seenIDs[c.ClipID] || seenPaths[c.RelativePath] || seenOrder[order] {
			return 0, fmt.Errorf("clip manifest contains duplicate identity")
		}
		seenIDs[c.ClipID], seenPaths[c.RelativePath], seenOrder[order] = true, true, true
		if c.CaptureGeneration != previousGeneration {
			if previousGeneration != "" {
				closedGeneration[previousGeneration] = true
			}
			if closedGeneration[c.CaptureGeneration] {
				return 0, fmt.Errorf("capture generation reappears noncontiguously")
			}
			if c.CaptureSequence != 1 {
				return 0, fmt.Errorf("capture generation is missing its leading clip")
			}
			previousGeneration = c.CaptureGeneration
		} else if c.CaptureSequence != lastSequence[c.CaptureGeneration]+1 {
			return 0, fmt.Errorf("capture sequence is incomplete")
		}
		lastSequence[c.CaptureGeneration] = c.CaptureSequence
		if i > 0 {
			p := clips[i-1]
			if c.ClipStartAt.Before(p.ClipStartAt) || (c.ClipStartAt.Equal(p.ClipStartAt) && c.ClipID <= p.ClipID) {
				return 0, fmt.Errorf("clip manifest is not in deterministic media order")
			}
		}
		if total > MaxSourceByte-c.SizeBytes {
			return 0, fmt.Errorf("clip manifest exceeds source byte bound")
		}
		total += c.SizeBytes
	}
	return total, nil
}

func validTimestampProvenance(c ManifestClip) bool {
	if c.CaptureAttemptID == "" {
		return c.TimestampContractVersion == "" && c.TimestampContractStatus == "" &&
			c.TimestampContractReason == "" && c.TimestampContractSHA256 == ""
	}
	switch c.TimestampContractStatus {
	case "per_clip_probe_complete":
		return c.TimestampContractVersion == "continuous-source-pts-v1" && c.TimestampContractReason == "" && lowerHex64(c.TimestampContractSHA256)
	case "per_clip_probe_unknown":
		return c.TimestampContractVersion == "" && c.TimestampContractSHA256 == "" && timestampUnknownReasons[c.TimestampContractReason]
	default:
		return false
	}
}

var timestampUnknownReasons = map[string]bool{
	"missing_terminal_duration":  true,
	"missing_audio_sample_count": true,
	"invalid_time_base":          true,
	"probe_output_limit":         true,
	"probe_unavailable":          true,
}

func HasCompleteTimestampProvenance(c ManifestClip) bool {
	return c.CaptureAttemptID != "" && c.TimestampContractStatus == "per_clip_probe_complete" &&
		c.TimestampContractVersion == "continuous-source-pts-v1" && lowerHex64(c.TimestampContractSHA256)
}

func MeasureTimeline(clips []ManifestClip, start, end time.Time) Timeline {
	expected := end.Sub(start).Seconds()
	t := Timeline{ExpectedSeconds: expected}
	t.LeadingGapSeconds = maxSeconds(0, clips[0].ClipStartAt.Sub(start).Seconds())
	cursor := start
	for _, clip := range clips {
		left, right := clip.ClipStartAt, clip.ClipEndAt
		if left.Before(start) {
			left = start
		}
		if right.After(end) {
			right = end
		}
		if !right.After(left) {
			continue
		}
		if left.After(cursor) {
			gap := left.Sub(cursor).Seconds()
			t.GapCount++
			if gap > 30 {
				t.GapOver30sCount++
			}
			if gap > 300 {
				t.GapOver5mCount++
			}
			if cursor.After(start) && gap > t.LargestInternalGapSecond {
				t.LargestInternalGapSecond = gap
			}
		} else if left.Before(cursor) {
			overlapEnd := right
			if overlapEnd.After(cursor) {
				overlapEnd = cursor
			}
			if overlapEnd.After(left) {
				t.OverlapCount++
				t.OverlapSeconds += overlapEnd.Sub(left).Seconds()
			}
		}
		if right.After(cursor) {
			t.CoveredSeconds += right.Sub(maxTime(left, cursor)).Seconds()
			cursor = right
		}
	}
	if cursor.Before(end) {
		t.TrailingGapSeconds = end.Sub(cursor).Seconds()
		t.GapCount++
		if t.TrailingGapSeconds > 30 {
			t.GapOver30sCount++
		}
		if t.TrailingGapSeconds > 300 {
			t.GapOver5mCount++
		}
	}
	t.LargestGapSeconds = maxSeconds(t.LeadingGapSeconds, maxSeconds(t.LargestInternalGapSecond, t.TrailingGapSeconds))
	if expected > 0 {
		t.CoveragePct = t.CoveredSeconds / expected * 100
	}
	return t
}

func maxSeconds(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func ValidateRuns(clips []ClipFact, runs []RunFact) error {
	if err := validateRunEvidence(clips, runs, true); err != nil {
		return err
	}
	if len(clips) == 0 || runs[len(runs)-1].LastClipOrdinal != len(clips) {
		return fmt.Errorf("passing runs omit clips")
	}
	return nil
}

func ValidateSeams(clips []ClipFact, runs []RunFact, seams []SeamFact) error {
	return validateSeamEvidence(clips, runs, seams, true)
}

// ValidatePartialSeams preserves one fact for every adjacent frozen clip even
// when historical capture provenance cannot prove exact source adjacency.
// An ambiguous seam never authorizes a run split or a frame-perfect claim.
func ValidatePartialSeams(clips []ClipFact, runs []RunFact, seams []SeamFact) error {
	return validateSeamEvidence(clips, runs, seams, false)
}

func validateSeamEvidence(clips []ClipFact, runs []RunFact, seams []SeamFact, requireExact bool) error {
	want := len(clips) - 1
	if want < 0 || len(seams) != want {
		return fmt.Errorf("seam evidence count does not match adjacent frozen clips")
	}
	runByFirst := map[int]RunFact{}
	for _, run := range runs {
		runByFirst[run.FirstClipOrdinal] = run
	}
	for i, seam := range seams {
		left, right := clips[i], clips[i+1]
		if seam.Ordinal != i+1 || seam.PreviousClipID != left.ClipID || seam.NextClipID != right.ClipID ||
			seam.PreviousSequence != left.CaptureSequence || seam.NextSequence != right.CaptureSequence {
			return fmt.Errorf("native seam identity is invalid")
		}
		boundary, split := runByFirst[i+2]
		objectiveBoundary := runBoundaryReason(left, right)
		if split {
			if objectiveBoundary == "" || boundary.BoundaryReason != objectiveBoundary {
				return fmt.Errorf("native run split does not match an objective boundary")
			}
			if seam.Verdict != "not_applicable" || seam.Reason != objectiveBoundary ||
				len(seam.PreviousFrames) != 0 || len(seam.NextFrames) != 0 {
				return fmt.Errorf("objective run boundary seam is invalid")
			}
			continue
		}
		if objectiveBoundary != "" {
			return fmt.Errorf("native run crosses an objective boundary")
		}
		if seam.CaptureGeneration != left.CaptureGeneration || seam.CaptureGeneration != right.CaptureGeneration ||
			seam.NativeSignatureSHA256 != left.NativeSignatureSHA256 || seam.NativeSignatureSHA256 != right.NativeSignatureSHA256 {
			return fmt.Errorf("native seam provenance differs from its run")
		}
		if seam.Verdict == "ambiguous" {
			if requireExact || seam.Reason != "continuous_source_pts_unavailable" || seam.TimelineBasis != "unavailable" ||
				seam.Confidence != "none" || len(seam.PreviousFrames) != 0 || len(seam.NextFrames) != 0 {
				return fmt.Errorf("ambiguous seam evidence is invalid")
			}
			continue
		}
		if seam.Verdict != "exact" || seam.Reason != "frame_adjacency_proven" || seam.Confidence != "high" ||
			left.CaptureAttemptID == "" || left.CaptureAttemptID != right.CaptureAttemptID ||
			!HasCompleteTimestampProvenance(left.ManifestClip) || !HasCompleteTimestampProvenance(right.ManifestClip) ||
			seam.NextSequence != seam.PreviousSequence+1 || seam.TimelineBasis != "continuous_source_pts_v1" ||
			seam.CaptureContract != left.TimestampContractVersion {
			return fmt.Errorf("native exact seam provenance or verdict is invalid")
		}
		if err := validateFrameEdge(seam.PreviousFrames); err != nil {
			return fmt.Errorf("previous seam edge: %w", err)
		}
		if err := validateFrameEdge(seam.NextFrames); err != nil {
			return fmt.Errorf("next seam edge: %w", err)
		}
		if !right.ClipStartAt.Equal(left.ClipEndAt) {
			return fmt.Errorf("timeline gap or overlap cannot be an exact native seam")
		}
		last := seam.PreviousFrames[len(seam.PreviousFrames)-1]
		first := seam.NextFrames[0]
		if first.TimeBaseNumerator != last.TimeBaseNumerator || first.TimeBaseDenominator != last.TimeBaseDenominator ||
			first.BestEffortTimestamp != last.BestEffortTimestamp+last.DurationTimestamp {
			return fmt.Errorf("seam is not exactly adjacent on the attested rational presentation timeline")
		}
	}
	return nil
}

func runBoundaryReason(left, right ClipFact) string {
	if left.CaptureGeneration != right.CaptureGeneration {
		return "capture_generation_change"
	}
	if left.CaptureAttemptID != right.CaptureAttemptID || left.TimestampContractVersion != right.TimestampContractVersion ||
		left.TimestampContractStatus != right.TimestampContractStatus {
		return "capture_attempt_change"
	}
	if left.NativeSignatureSHA256 != right.NativeSignatureSHA256 {
		return "native_signature_change"
	}
	if !left.ClipEndAt.Equal(right.ClipStartAt) {
		return "temporal_gap"
	}
	return ""
}

func ValidateAudioSeams(clips []ClipFact, seams []AudioSeamFact, requireExact bool) error {
	want := len(clips) - 1
	if want < 0 || len(seams) != want {
		return fmt.Errorf("audio seam evidence count does not match adjacent frozen clips")
	}
	for i, seam := range seams {
		left, right := clips[i], clips[i+1]
		if seam.Ordinal != i+1 || seam.PreviousClipID != left.ClipID || seam.NextClipID != right.ClipID {
			return fmt.Errorf("audio seam identity is invalid")
		}
		leftAudio, rightAudio := left.AudioPresent, right.AudioPresent
		if !leftAudio && !rightAudio {
			if seam.Verdict != "not_present" || seam.Reason != "audio_not_present" {
				return fmt.Errorf("audio-free seam is mislabeled")
			}
			continue
		}
		boundaryReason := runBoundaryReason(left, right)
		if leftAudio != rightAudio || boundaryReason != "" {
			if boundaryReason == "" {
				boundaryReason = "audio_stream_presence_change"
			}
			if seam.Verdict != "not_applicable" || seam.Reason != boundaryReason {
				return fmt.Errorf("objective audio boundary is invalid")
			}
			continue
		}
		if seam.Verdict == "ambiguous" {
			if requireExact || seam.Reason != "continuous_source_pts_unavailable" {
				return fmt.Errorf("ambiguous audio seam is invalid")
			}
			continue
		}
		if left.AudioTimeline == nil || right.AudioTimeline == nil {
			return fmt.Errorf("exact audio seam lacks per-clip sample evidence")
		}
		if seam.Verdict != "exact" || seam.Reason != "audio_sample_adjacency_proven" ||
			seam.SampleRate <= 0 || seam.SampleRate != left.AudioTimeline.SampleRate || seam.SampleRate != right.AudioTimeline.SampleRate ||
			seam.PreviousEndSample != seam.NextStartSample || seam.PreviousSampleCount <= 0 || seam.NextSampleCount <= 0 ||
			seam.CaptureAttemptID == "" || seam.CaptureAttemptID != left.CaptureAttemptID || seam.CaptureAttemptID != right.CaptureAttemptID ||
			seam.TimestampContract != "continuous-source-pts-v1" {
			return fmt.Errorf("exact audio seam is invalid")
		}
	}
	return nil
}

func ValidateAxisStatuses(report Report) error {
	axes := []string{report.NASByteDecodeStatus, report.NativeRunConcatStatus, report.WithinRunFrameAdjacencyStatus}
	for _, status := range axes {
		if status != "passed" && status != "failed" && status != "unknown" {
			return fmt.Errorf("invalid certification axis status")
		}
	}
	if report.WindowContinuityStatus != "passed" && report.WindowContinuityStatus != "partitioned" && report.WindowContinuityStatus != "failed" && report.WindowContinuityStatus != "unknown" {
		return fmt.Errorf("invalid whole-window continuity status")
	}
	if report.WithinRunAudioContinuityStatus != "passed" && report.WithinRunAudioContinuityStatus != "failed" && report.WithinRunAudioContinuityStatus != "unknown" && report.WithinRunAudioContinuityStatus != "not_present" {
		return fmt.Errorf("invalid audio continuity status")
	}
	if report.Status == "passed" {
		if report.NASByteDecodeStatus != "passed" || report.NativeRunConcatStatus != "passed" || report.WithinRunFrameAdjacencyStatus != "passed" ||
			(report.WithinRunAudioContinuityStatus != "passed" && report.WithinRunAudioContinuityStatus != "not_present") || report.WindowContinuityStatus != "passed" || len(report.NativeRuns) != 1 {
			return fmt.Errorf("passing report has an incomplete axis")
		}
	}
	if report.Status == "partial" {
		if report.NASByteDecodeStatus != "passed" || report.NativeRunConcatStatus != "passed" ||
			report.WindowContinuityStatus == "passed" {
			return fmt.Errorf("partial report does not preserve useful media proof or uncertainty")
		}
		if len(report.NativeRuns) == 1 && report.WindowContinuityStatus != "unknown" {
			return fmt.Errorf("single-run partial has an invalid whole-window status")
		}
		if len(report.NativeRuns) > 1 && report.WindowContinuityStatus != "partitioned" {
			return fmt.Errorf("multi-run partial is not explicitly partitioned")
		}
	}
	return nil
}

func validateFrameEdge(frames []SeamFrameEvidence) error {
	if len(frames) < 3 || len(frames) > 8 {
		return fmt.Errorf("frame edge is not bounded or lacks cadence context")
	}
	for i, frame := range frames {
		if frame.DurationTimestamp <= 0 || frame.TimeBaseNumerator <= 0 || frame.TimeBaseDenominator <= 0 ||
			!lowerHex64(frame.DecodedSHA256) || !lowerHex64(frame.PacketSHA256) || len(frame.PictureType) != 1 {
			return fmt.Errorf("frame evidence is incomplete")
		}
		if i > 0 {
			previous := frames[i-1]
			if frame.TimeBaseNumerator != previous.TimeBaseNumerator || frame.TimeBaseDenominator != previous.TimeBaseDenominator ||
				frame.BestEffortTimestamp != previous.BestEffortTimestamp+previous.DurationTimestamp {
				return fmt.Errorf("frames are not exactly adjacent in rational presentation order")
			}
		}
	}
	return nil
}

func ValidateClipTimelines(clips []ClipFact, requireVideoContinuity, requireAudioContinuity bool) error {
	for _, clip := range clips {
		if clip.VideoTimeline == nil {
			if requireVideoContinuity {
				return fmt.Errorf("missing per-clip video presentation evidence")
			}
			continue
		}
		v := clip.VideoTimeline
		if v.FrameCount <= 0 || v.TimeBaseNumerator <= 0 || v.TimeBaseDenominator <= 0 ||
			v.LastDurationTimestamp <= 0 || v.LastTimestamp < v.FirstTimestamp ||
			v.DuplicateTimestamps < 0 || v.NonMonotonicSteps < 0 || v.DiscontinuousSteps < 0 {
			return fmt.Errorf("invalid per-clip video presentation evidence")
		}
		if requireVideoContinuity && (v.DuplicateTimestamps != 0 || v.NonMonotonicSteps != 0 || v.DiscontinuousSteps != 0) {
			return fmt.Errorf("per-clip video presentation timeline is not continuous")
		}
		if clip.AudioTimeline != nil {
			if !clip.AudioPresent {
				return fmt.Errorf("audio timeline exists for audio-free clip")
			}
			a := clip.AudioTimeline
			if a.SampleRate <= 0 || a.SampleCount <= 0 || a.EndSample <= a.FirstSample ||
				a.DuplicateTimestamps < 0 || a.NonMonotonicSteps < 0 || a.DiscontinuousSteps < 0 ||
				a.EndSample-a.FirstSample != a.SampleCount {
				return fmt.Errorf("invalid per-clip audio sample evidence")
			}
			if requireAudioContinuity && (a.DuplicateTimestamps != 0 || a.NonMonotonicSteps != 0 || a.DiscontinuousSteps != 0) {
				return fmt.Errorf("per-clip audio timeline is not continuous")
			}
		}
		if requireAudioContinuity && clip.AudioPresent && clip.AudioTimeline == nil {
			return fmt.Errorf("missing per-clip audio sample evidence")
		}
	}
	return nil
}

func ValidatePartialRuns(clips []ClipFact, runs []RunFact) error {
	return validateRunEvidence(clips, runs, false)
}

func validateRunEvidence(clips []ClipFact, runs []RunFact, requirePass bool) error {
	if len(runs) == 0 {
		if requirePass {
			return fmt.Errorf("missing native runs")
		}
		return nil
	}
	if len(runs) < 1 || len(runs) > len(clips) {
		return fmt.Errorf("invalid run count")
	}
	next := 1
	for i, r := range runs {
		if r.Ordinal != i+1 || r.FirstClipOrdinal != next || r.LastClipOrdinal < r.FirstClipOrdinal ||
			r.ClipCount != r.LastClipOrdinal-r.FirstClipOrdinal+1 || r.LastClipOrdinal > len(clips) {
			return fmt.Errorf("runs are not a contiguous partition")
		}
		if i == 0 && r.BoundaryReason != "window_start" {
			return fmt.Errorf("first run boundary is invalid")
		}
		if i > 0 && r.BoundaryReason != "capture_generation_change" && r.BoundaryReason != "capture_attempt_change" && r.BoundaryReason != "native_signature_change" && r.BoundaryReason != "temporal_gap" {
			return fmt.Errorf("run boundary is invalid")
		}
		if requirePass && r.ClipCount == 1 && r.ValidationStatus != "single_clip_decode_only" {
			return fmt.Errorf("single clip run overclaims concat")
		}
		if requirePass && r.ClipCount > 1 && r.ValidationStatus != "lossless_concat_decode_passed" {
			return fmt.Errorf("multi clip run is not losslessly validated")
		}
		if !requirePass && r.ValidationStatus != "single_clip_decode_only" && r.ValidationStatus != "lossless_concat_decode_passed" && r.ValidationStatus != "failed" && r.ValidationStatus != "unknown" {
			return fmt.Errorf("partial run status is invalid")
		}
		if !lowerHex64(r.NativeSignatureSHA256) {
			return fmt.Errorf("run signature is invalid")
		}
		first := clips[r.FirstClipOrdinal-1]
		if r.CaptureGeneration != first.CaptureGeneration || r.CaptureAttemptID != first.CaptureAttemptID ||
			r.TimestampContract != first.TimestampContractVersion || (requirePass && first.StrictDecode != "passed") {
			return fmt.Errorf("run generation or decode proof is invalid")
		}
		var bytes int64
		for offset, c := range clips[r.FirstClipOrdinal-1 : r.LastClipOrdinal] {
			if c.NativeSignatureSHA256 != r.NativeSignatureSHA256 || c.CaptureGeneration != r.CaptureGeneration ||
				c.CaptureAttemptID != r.CaptureAttemptID || c.TimestampContractVersion != r.TimestampContract ||
				(requirePass && c.StrictDecode != "passed") {
				return fmt.Errorf("run mixes native signature, capture provenance, or decode state")
			}
			if offset > 0 {
				previous := clips[r.FirstClipOrdinal-1+offset-1]
				if !previous.ClipEndAt.Equal(c.ClipStartAt) {
					return fmt.Errorf("run crosses a timeline gap or overlap")
				}
			}
			if c.SizeBytes <= 0 || bytes > MaxRunByte-c.SizeBytes {
				return fmt.Errorf("run byte total overflows bound")
			}
			bytes += c.SizeBytes
		}
		if bytes != r.SourceBytes || bytes > MaxRunByte {
			return fmt.Errorf("run byte bound or total is invalid")
		}
		if i > 0 {
			prev := clips[r.FirstClipOrdinal-2]
			cur := clips[r.FirstClipOrdinal-1]
			actual := runBoundaryReason(prev, cur)
			if actual == "" {
				return fmt.Errorf("run split without an objective media boundary")
			}
			if r.BoundaryReason != actual {
				return fmt.Errorf("run boundary reason mismatch")
			}
		}
		next = r.LastClipOrdinal + 1
	}
	if requirePass && next != len(clips)+1 {
		return fmt.Errorf("runs omit clips")
	}
	return nil
}

func lowerHex64(v string) bool {
	if len(v) != 64 {
		return false
	}
	for _, r := range v {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

func safeRelativePath(v string) bool {
	if v == "" || len(v) > 1024 || !utf8.ValidString(v) || strings.Contains(v, "\\") || path.IsAbs(v) || path.Clean(v) != v {
		return false
	}
	for _, part := range strings.Split(v, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	for _, r := range v {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

func NormalizeReasons(in []string) ([]string, error) {
	if len(in) < 1 || len(in) > 16 {
		return nil, fmt.Errorf("reason code count is invalid")
	}
	out := append([]string(nil), in...)
	sort.Strings(out)
	for i, v := range out {
		if v == "" || len(v) > 80 || (i > 0 && v == out[i-1]) {
			return nil, fmt.Errorf("reason codes are invalid")
		}
	}
	return out, nil
}
