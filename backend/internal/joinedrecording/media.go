package joinedrecording

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/stitchcert"
)

const mediaCommandOutputLimit = 64 << 10

type LocalSource struct {
	ClipID            int64
	Path              string
	SizeBytes         int64
	SHA256            string
	SourceClaimSHA256 string
	AudioContract     *AudioSequenceContract
}

type MediaToolEvidence struct {
	FFmpegVersion  string `json:"ffmpeg_version"`
	FFmpegSHA256   string `json:"ffmpeg_sha256"`
	FFprobeVersion string `json:"ffprobe_version"`
	FFprobeSHA256  string `json:"ffprobe_sha256"`
	IdentitySHA256 string `json:"identity_sha256"`
}

func SealMediaToolEvidence(evidence MediaToolEvidence) (MediaToolEvidence, error) {
	evidence.IdentitySHA256 = ""
	if strings.TrimSpace(evidence.FFmpegVersion) == "" || strings.TrimSpace(evidence.FFprobeVersion) == "" || !lowerHex64(evidence.FFmpegSHA256) || !lowerHex64(evidence.FFprobeSHA256) {
		return MediaToolEvidence{}, fmt.Errorf("invalid pinned media tool evidence")
	}
	payload, err := json.Marshal(evidence)
	if err != nil {
		return MediaToolEvidence{}, err
	}
	sum := sha256.Sum256(payload)
	evidence.IdentitySHA256 = hex.EncodeToString(sum[:])
	return evidence, nil
}

func ValidateMediaToolEvidence(evidence MediaToolEvidence) error {
	want := evidence.IdentitySHA256
	sealed, err := SealMediaToolEvidence(evidence)
	if err != nil || sealed.IdentitySHA256 != want || !lowerHex64(want) {
		return fmt.Errorf("media tool evidence is not sealed")
	}
	return nil
}

func InspectMediaToolEvidence(ctx context.Context) (MediaToolEvidence, error) {
	ffmpegPath, err := exec.LookPath(ffmpegBinary())
	if err != nil {
		return MediaToolEvidence{}, err
	}
	ffprobePath, err := exec.LookPath(ffprobeBinary())
	if err != nil {
		return MediaToolEvidence{}, err
	}
	_, ffmpegSHA, err := localIdentity(ffmpegPath)
	if err != nil {
		return MediaToolEvidence{}, err
	}
	_, ffprobeSHA, err := localIdentity(ffprobePath)
	if err != nil {
		return MediaToolEvidence{}, err
	}
	ffmpegVersion, err := mediaToolVersion(ctx, ffmpegPath)
	if err != nil {
		return MediaToolEvidence{}, err
	}
	ffprobeVersion, err := mediaToolVersion(ctx, ffprobePath)
	if err != nil {
		return MediaToolEvidence{}, err
	}
	return SealMediaToolEvidence(MediaToolEvidence{FFmpegVersion: ffmpegVersion, FFmpegSHA256: ffmpegSHA, FFprobeVersion: ffprobeVersion, FFprobeSHA256: ffprobeSHA})
}

func mediaToolVersion(ctx context.Context, binary string) (string, error) {
	process := newBoundedMediaProcess(ctx, binary, "-version")
	cmd := process.cmd
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", err
	}
	var stderr limitedOutput
	cmd.Stderr = &stderr
	if err := process.Start(); err != nil {
		return "", err
	}
	out, readErr := io.ReadAll(io.LimitReader(stdout, mediaCommandOutputLimit+1))
	tooLarge := len(out) > mediaCommandOutputLimit
	if tooLarge || readErr != nil {
		_ = process.Kill()
	}
	waitErr := process.Wait()
	if readErr != nil {
		return "", fmt.Errorf("read media tool version: %w", readErr)
	}
	if tooLarge {
		return "", fmt.Errorf("media tool version exceeds bounded output")
	}
	line := strings.TrimSpace(strings.SplitN(string(out), "\n", 2)[0])
	if waitErr != nil {
		return "", fmt.Errorf("read media tool version: %w (%s)", waitErr, stderr.String())
	}
	if line == "" {
		return "", fmt.Errorf("read media tool version: empty output (%s)", stderr.String())
	}
	return line, nil
}

type AudioSequenceContract struct {
	CodecName       string `json:"codec_name"`
	SampleRate      int64  `json:"sample_rate"`
	Channels        int    `json:"channels"`
	ChannelLayout   string `json:"channel_layout"`
	InitialPadding  int64  `json:"initial_padding"`
	SkipSamples     int64  `json:"skip_samples"`
	DiscardPadding  int64  `json:"discard_padding"`
	CodecDelay      int64  `json:"codec_delay"`
	TrailingPadding int64  `json:"trailing_padding"`
	EditListKind    string `json:"edit_list_kind,omitempty"`
	EditListSHA256  string `json:"edit_list_sha256,omitempty"`
}

func validateAudioContract(c AudioSequenceContract) error {
	if strings.TrimSpace(c.CodecName) == "" || c.SampleRate <= 0 || c.Channels <= 0 || strings.TrimSpace(c.ChannelLayout) == "" || c.InitialPadding < 0 || c.SkipSamples < 0 || c.DiscardPadding < 0 || c.CodecDelay < 0 || c.TrailingPadding < 0 || ((c.EditListKind == "") != (c.EditListSHA256 == "")) || (c.EditListSHA256 != "" && !lowerHex64(c.EditListSHA256)) {
		return fmt.Errorf("invalid audio sequence contract")
	}
	return nil
}

type TrackFingerprint struct {
	MediaType                 string   `json:"media_type"`
	PacketCount               int64    `json:"packet_count"`
	PacketChainSHA256         string   `json:"packet_chain_sha256"`
	PacketTimingSHA256        string   `json:"packet_timing_sha256"`
	PacketTimeBases           []string `json:"packet_time_bases"`
	FirstPacketPTSSeconds     string   `json:"first_packet_pts_seconds"`
	LastPacketPTSSeconds      string   `json:"last_packet_pts_seconds"`
	FirstPacketDTSSeconds     string   `json:"first_packet_dts_seconds"`
	LastPacketDTSSeconds      string   `json:"last_packet_dts_seconds"`
	PacketDurationSeconds     string   `json:"packet_duration_seconds"`
	DecodeTimelineSpanSeconds string   `json:"decode_timeline_span_seconds"`
	DecodedFrames             int64    `json:"decoded_frames"`
	DecodedSamples            int64    `json:"decoded_samples,omitempty"`
	FirstTimestamp            int64    `json:"first_timestamp"`
	LastTimestamp             int64    `json:"last_timestamp"`
	TimestampStatus           string   `json:"timestamp_status"`
}

type MediaFingerprint struct {
	DurationSeconds      float64                      `json:"duration_seconds"`
	Tracks               map[string]*TrackFingerprint `json:"tracks"`
	DecodedVideoSHA256   string                       `json:"decoded_video_sha256,omitempty"`
	AudioContracts       []AudioSequenceContract      `json:"audio_sequence_contracts,omitempty"`
	EffectiveAudioBytes  int64                        `json:"effective_audio_bytes,omitempty"`
	EffectiveAudioFrames int64                        `json:"effective_audio_sample_frames,omitempty"`
	EffectiveAudioSHA256 string                       `json:"effective_audio_sha256,omitempty"`
}

type Verification struct {
	Status                     string                         `json:"status"`
	AcceptanceMode             string                         `json:"acceptance_mode,omitempty"`
	LosslessNormalization      *LosslessNormalizationEvidence `json:"lossless_normalization,omitempty"`
	PacketPayloadOrderStatus   string                         `json:"packet_payload_order_status"`
	DecodedFrameSequenceStatus string                         `json:"decoded_frame_sequence_status,omitempty"`
	DecodedFrameTotalsStatus   string                         `json:"decoded_frame_totals_status"`
	DecodedAudioTotalsStatus   string                         `json:"decoded_audio_totals_status"`
	OutputTimestampStatus      string                         `json:"output_timestamp_status"`
	StrictDecodeStatus         string                         `json:"strict_decode_status"`
	SourceFingerprint          MediaFingerprint               `json:"source_fingerprint"`
	OutputFingerprint          MediaFingerprint               `json:"output_fingerprint"`
}

// LosslessNormalizationEvidence records the exact, bounded codec contract
// used only after both stream-copy verification modes reject a candidate.
// QP 0 preserves decoded pixels; the ordered decoded-frame hash proves that
// the normalized output represents every source frame exactly once.
type LosslessNormalizationEvidence struct {
	Codec                         string          `json:"codec"`
	Preset                        string          `json:"preset"`
	Quantizer                     int             `json:"quantizer"`
	PixelFormat                   string          `json:"pixel_format"`
	FrameRate                     string          `json:"frame_rate"`
	SampleAspectRatio             string          `json:"sample_aspect_ratio"`
	ColorRange                    string          `json:"color_range"`
	ColorSpace                    string          `json:"color_space"`
	ColorTransfer                 string          `json:"color_transfer"`
	ColorPrimaries                string          `json:"color_primaries"`
	ChromaLocation                string          `json:"chroma_location"`
	FieldOrder                    string          `json:"field_order"`
	TimelineRule                  string          `json:"timeline_rule"`
	SourceDecodedFrames           int64           `json:"source_decoded_frames"`
	OutputDecodedFrames           int64           `json:"output_decoded_frames"`
	DecodedFrameSequenceSHA256    string          `json:"decoded_frame_sequence_sha256"`
	DecodedFrameFieldStatus       string          `json:"decoded_frame_field_status"`
	DecodedFrameFieldSHA256       string          `json:"decoded_frame_field_sha256"`
	SourceTimelineSignatureSHA256 string          `json:"source_timeline_signature_sha256"`
	OutputLimitBytes              int64           `json:"output_limit_bytes"`
	AudioStatus                   string          `json:"audio_status"`
	TriggerReasonCode             string          `json:"trigger_reason_code"`
	TriggerFailureFacts           json.RawMessage `json:"trigger_failure_facts"`
	TriggerFailureSHA256          string          `json:"trigger_failure_sha256"`
}

type BuiltOutput struct {
	Path          string
	SizeBytes     int64
	SHA256        string
	SourceCount   int
	Verification  Verification
	SplitEvidence []MaximalityEvidence
}

type MaximalityEvidence struct {
	CandidateClipIDs  []int64         `json:"candidate_clip_ids"`
	ReasonCode        string          `json:"reason_code"`
	SourceClaimSHA256 string          `json:"source_claim_sha256"`
	PolicyVersion     string          `json:"policy_version"`
	EvidenceSHA256    string          `json:"evidence_sha256"`
	FailureFacts      json.RawMessage `json:"normalized_failure_facts"`
	FailureSHA256     string          `json:"failure_sha256"`
	RepeatCount       int             `json:"repeat_count"`
	MediaToolIdentity string          `json:"media_tool_identity"`
}

type QuarantinedBuild struct {
	Source   LocalSource
	Evidence MaximalityEvidence
}

type HourPreflight struct {
	Sources     []SourceClip
	Built       []BuiltOutput
	Quarantined []SourceClip
	Quarantines []QuarantinedBuild
}

type deterministicMediaError struct {
	code           string
	evidenceSHA256 string
	evidence       json.RawMessage
	err            error
}

func (e *deterministicMediaError) Error() string { return e.code + ": " + e.err.Error() }
func (e *deterministicMediaError) Unwrap() error { return e.err }

func deterministicFailure(code string, evidence any, err error) error {
	digest, canonical, digestErr := stitchcert.CanonicalSHA(evidence)
	if digestErr != nil {
		return fmt.Errorf("canonical deterministic media evidence: %w", digestErr)
	}
	return &deterministicMediaError{code: code, evidenceSHA256: digest, evidence: canonical, err: err}
}

var unstableScratchName = regexp.MustCompile(`(?:attempt|joined|concat)-[A-Za-z0-9._-]+`)
var unstableProcessAddress = regexp.MustCompile(`0x[0-9a-f]+`)

func deterministicCommandFailure(ctx context.Context, code string, err error) error {
	if err == nil || ctx.Err() != nil || errors.Is(err, syscall.ENOSPC) {
		return err
	}
	lower := strings.ToLower(err.Error())
	for _, marker := range []string{"no space left", "disk full", "input/output error", "resource temporarily unavailable", "read-only file system"} {
		if strings.Contains(lower, marker) {
			return err
		}
	}
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		return err
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return err
	}
	category := ""
	for _, fact := range []struct{ marker, category string }{
		{"invalid data found when processing input", "invalid_media_data"},
		{"moov atom not found", "missing_container_index"},
		{"error reading header", "invalid_container_header"},
		{"could not find codec parameters", "missing_codec_parameters"},
		{"non monotonically increasing dts", "nonmonotonic_packet_timestamps"},
		{"malformed aac", "malformed_audio_payload"},
		{"invalid nal", "malformed_video_payload"},
		{"could not write header", "incompatible_output_container"},
	} {
		if strings.Contains(lower, fact.marker) {
			category = fact.category
			break
		}
	}
	if category == "" {
		return err
	}
	normalized := strings.Join(strings.Fields(unstableScratchName.ReplaceAllString(lower, "<scratch-file>")), " ")
	normalized = unstableProcessAddress.ReplaceAllString(normalized, "<address>")
	return deterministicFailure(code, struct {
		Category       string `json:"category"`
		ExitCode       int    `json:"exit_code"`
		NormalizedFact string `json:"normalized_fact"`
	}{category, exitErr.ExitCode(), normalized}, err)
}

func deterministicEvidenceFailure(ctx context.Context, code string, err error) error {
	if err == nil || ctx.Err() != nil {
		return err
	}
	if classified := deterministicCommandFailure(ctx, code, err); classified != err {
		return classified
	}
	lower := strings.ToLower(err.Error())
	category := ""
	for _, fact := range []struct{ marker, category string }{
		{"invalid media duration", "invalid_media_duration"},
		{"video stream missing", "video_stream_missing"},
		{"multiple video streams", "unsupported_video_stream_count"},
		{"multiple audio streams", "unsupported_audio_stream_count"},
		{"invalid audio sample rate", "invalid_audio_sample_rate"},
		{"invalid audio sequence contract", "invalid_audio_contract"},
		{"frozen audio sequence contract differs", "frozen_audio_contract_mismatch"},
		{"audio format changes", "audio_format_change"},
		{"missing packet payload hash", "missing_packet_hash"},
		{"missing exact packet timestamp evidence", "missing_packet_timestamp"},
		{"nonmonotonic packet timestamp", "nonmonotonic_packet_timestamp"},
		{"evidence has unknown stream", "unknown_evidence_stream"},
		{"packet duration overflow", "packet_duration_overflow"},
		{"nonmonotonic decoded", "nonmonotonic_decoded_timestamp"},
		{"incomplete ", "incomplete_packet_frame_evidence"},
		{"missing decoded", "missing_decoded_evidence"},
	} {
		if strings.Contains(lower, fact.marker) {
			category = fact.category
			break
		}
	}
	if category == "" {
		return err
	}
	return deterministicFailure(code, struct {
		Category string `json:"category"`
	}{category}, err)
}

// BuildLargestPassingPrefix is a pre-seal operation: it peels exactly one
// trailing clip after each failed candidate so the planner can seal immutable
// prefix/remainder tasks. It must never shorten an already claimed task.
func BuildLargestPassingPrefix(ctx context.Context, sources []LocalSource, scratchDir, mediaToolIdentity string) (BuiltOutput, error) {
	if len(sources) == 0 || strings.TrimSpace(scratchDir) == "" {
		return BuiltOutput{}, fmt.Errorf("bounded sources and scratch directory are required")
	}
	if !lowerHex64(mediaToolIdentity) {
		return BuiltOutput{}, fmt.Errorf("pinned media tool identity is required")
	}
	if err := os.MkdirAll(scratchDir, 0700); err != nil {
		return BuiltOutput{}, fmt.Errorf("create scratch: %w", err)
	}
	for _, source := range sources {
		if err := verifyLocalIdentity(source); err != nil {
			return BuiltOutput{}, err
		}
	}
	return buildLargestPassingPrefix(ctx, sources, scratchDir, mediaToolIdentity, buildIsolatedAttempt)
}

type isolatedBuildAttempt func(context.Context, []LocalSource, string) (BuiltOutput, error)
type mediaCandidateBudget func(string, int) time.Duration

func defaultMediaCandidateBudget(kind string, sourceCount int) time.Duration {
	if kind == "full" || kind == "full_repeat" {
		return 25 * time.Minute
	}
	budget := 2*time.Minute + time.Duration(sourceCount)*25*time.Second
	if budget > 25*time.Minute {
		return 25 * time.Minute
	}
	return budget
}

func buildLargestPassingPrefix(ctx context.Context, sources []LocalSource, scratchDir, mediaToolIdentity string, attempt isolatedBuildAttempt) (BuiltOutput, error) {
	var lastErr error
	evidence := []MaximalityEvidence{}
	for n := len(sources); n >= 1; n-- {
		built, err := attempt(ctx, sources[:n], scratchDir)
		if err == nil {
			built.SplitEvidence = append([]MaximalityEvidence(nil), evidence...)
			return built, nil
		}
		var firstDeterministic *deterministicMediaError
		if !errors.As(err, &firstDeterministic) {
			return BuiltOutput{}, err
		}
		if outputSizeFailure(firstDeterministic) {
			evidence = append(evidence, maximalityEvidence(sources[:n], firstDeterministic, 1, mediaToolIdentity))
			lastErr = err
			continue
		}
		_, repeatErr := attempt(ctx, sources[:n], scratchDir)
		if repeatErr == nil {
			return BuiltOutput{}, fmt.Errorf("candidate failure was not repeatable; retry preflight unchanged")
		}
		var secondDeterministic *deterministicMediaError
		if !errors.As(repeatErr, &secondDeterministic) || firstDeterministic.code != secondDeterministic.code || firstDeterministic.evidenceSHA256 != secondDeterministic.evidenceSHA256 {
			return BuiltOutput{}, fmt.Errorf("candidate failure changed across repeats; retry preflight unchanged (%s/%s)", firstDeterministic.evidenceSHA256, func() string {
				if secondDeterministic == nil {
					return "unclassified"
				}
				return secondDeterministic.evidenceSHA256
			}())
		}
		evidence = append(evidence, maximalityEvidence(sources[:n], firstDeterministic, 2, mediaToolIdentity))
		lastErr = repeatErr
	}
	return BuiltOutput{}, fmt.Errorf("no passing stream-copy prefix: %w", lastErr)
}

// buildIsolatedAttempt prevents an incomplete first build from influencing a
// deterministic retry. Failed attempts remove only their own worker scratch.
func buildIsolatedAttempt(ctx context.Context, sources []LocalSource, scratchDir string) (BuiltOutput, error) {
	attemptDir, err := os.MkdirTemp(scratchDir, "attempt-*")
	if err != nil {
		return BuiltOutput{}, fmt.Errorf("create isolated media attempt: %w", err)
	}
	built, err := buildAndVerify(ctx, sources, attemptDir)
	if err != nil {
		_ = os.RemoveAll(attemptDir)
		return BuiltOutput{}, err
	}
	return built, nil
}

func maximalityEvidence(sources []LocalSource, failure *deterministicMediaError, repeats int, mediaToolIdentity string) MaximalityEvidence {
	clipIDs := make([]int64, len(sources))
	claims := make([]struct {
		ClipID            int64  `json:"clip_id"`
		SourceClaimSHA256 string `json:"source_claim_sha256"`
	}, len(sources))
	for i := range sources {
		clipIDs[i] = sources[i].ClipID
		claims[i] = struct {
			ClipID            int64  `json:"clip_id"`
			SourceClaimSHA256 string `json:"source_claim_sha256"`
		}{sources[i].ClipID, sources[i].SourceClaimSHA256}
	}
	sourceClaimSHA, _, _ := stitchcert.CanonicalSHA(claims)
	proof := struct {
		SourceClaimSHA256 string `json:"source_claim_sha256"`
		ReasonCode        string `json:"reason_code"`
		FailureSHA256     string `json:"failure_sha256"`
		PolicyVersion     string `json:"policy_version"`
		MediaToolIdentity string `json:"media_tool_identity"`
		RepeatCount       int    `json:"repeat_count"`
	}{sourceClaimSHA, failure.code, failure.evidenceSHA256, PlanPolicyVersion, mediaToolIdentity, repeats}
	evidenceSHA, _, _ := stitchcert.CanonicalSHA(proof)
	return MaximalityEvidence{CandidateClipIDs: clipIDs, ReasonCode: failure.code, SourceClaimSHA256: sourceClaimSHA, PolicyVersion: PlanPolicyVersion, EvidenceSHA256: evidenceSHA, FailureFacts: append(json.RawMessage(nil), failure.evidence...), FailureSHA256: failure.evidenceSHA256, RepeatCount: repeats, MediaToolIdentity: mediaToolIdentity}
}

func buildAllPassingParts(ctx context.Context, sources []LocalSource, scratchDir, mediaToolIdentity string) ([]BuiltOutput, []QuarantinedBuild, error) {
	if len(sources) == 0 || strings.TrimSpace(scratchDir) == "" || !lowerHex64(mediaToolIdentity) {
		return nil, nil, fmt.Errorf("bounded sources, scratch, and pinned media tool identity are required")
	}
	if err := os.MkdirAll(scratchDir, 0700); err != nil {
		return nil, nil, fmt.Errorf("create scratch: %w", err)
	}
	for _, source := range sources {
		if err := verifyLocalIdentity(source); err != nil {
			return nil, nil, err
		}
	}
	return buildAllPassingPartsWithAttempt(ctx, sources, scratchDir, mediaToolIdentity, buildIsolatedAttempt)
}

var errMediaSplitNotIsolated = errors.New("media split could not be isolated")

// buildAllPassingPartsWithAttempt keeps preflight work linear in source media.
// Adjacent pairs only locate possible seams. The exact segment plus its next
// source must fail identically in two fresh attempts before that split is
// accepted. Any unexplained or changing failure aborts the whole provisional
// plan without returning partial outputs.
func buildAllPassingPartsWithAttempt(ctx context.Context, sources []LocalSource, scratchDir, mediaToolIdentity string, attempt isolatedBuildAttempt) (parts []BuiltOutput, quarantines []QuarantinedBuild, err error) {
	return buildAllPassingPartsWithPolicy(ctx, sources, scratchDir, mediaToolIdentity, attempt, defaultMediaCandidateBudget)
}

func buildAllPassingPartsWithPolicy(ctx context.Context, sources []LocalSource, scratchDir, mediaToolIdentity string, attempt isolatedBuildAttempt, budget mediaCandidateBudget) (parts []BuiltOutput, quarantines []QuarantinedBuild, err error) {
	return buildAllPassingPartsWithPairProofReuse(ctx, sources, scratchDir, mediaToolIdentity, attempt, budget, true)
}

type exactPairFailureProof struct {
	failure *deterministicMediaError
}

func cloneDeterministicMediaError(failure *deterministicMediaError) *deterministicMediaError {
	if failure == nil {
		return nil
	}
	return &deterministicMediaError{
		code:           failure.code,
		evidenceSHA256: failure.evidenceSHA256,
		evidence:       append(json.RawMessage(nil), failure.evidence...),
		err:            failure.err,
	}
}

func exactPairFailureProofKey(sources []LocalSource, locatorPosition int, policyVersion, mediaToolIdentity string) (string, error) {
	if len(sources) != 2 || locatorPosition < 0 || strings.TrimSpace(policyVersion) == "" || !lowerHex64(mediaToolIdentity) {
		return "", fmt.Errorf("invalid exact pair proof identity")
	}
	type sourceIdentity struct {
		ClipID            int64                  `json:"clip_id"`
		SizeBytes         int64                  `json:"size_bytes"`
		SHA256            string                 `json:"sha256"`
		SourceClaimSHA256 string                 `json:"source_claim_sha256"`
		AudioContract     *AudioSequenceContract `json:"audio_contract,omitempty"`
	}
	identity := struct {
		PolicyVersion     string           `json:"policy_version"`
		MediaToolIdentity string           `json:"media_tool_identity"`
		LocatorPosition   int              `json:"locator_position"`
		Sources           []sourceIdentity `json:"sources"`
	}{PolicyVersion: policyVersion, MediaToolIdentity: mediaToolIdentity, LocatorPosition: locatorPosition, Sources: make([]sourceIdentity, len(sources))}
	for i, source := range sources {
		if source.ClipID <= 0 || source.SizeBytes <= 0 || !lowerHex64(source.SHA256) || !lowerHex64(source.SourceClaimSHA256) || (source.AudioContract != nil && validateAudioContract(*source.AudioContract) != nil) {
			return "", fmt.Errorf("invalid exact pair source identity")
		}
		var audio *AudioSequenceContract
		if source.AudioContract != nil {
			copy := *source.AudioContract
			audio = &copy
		}
		identity.Sources[i] = sourceIdentity{source.ClipID, source.SizeBytes, source.SHA256, source.SourceClaimSHA256, audio}
	}
	digest, _, err := stitchcert.CanonicalSHA(identity)
	return digest, err
}

func reusableExactPairFailure(failure *deterministicMediaError) bool {
	if failure == nil {
		return false
	}
	switch failure.code {
	case "source_media_incompatible", "joined_output_probe_failure", "media_sequence_mismatch", "strict_decode_failure":
		return true
	default:
		return false
	}
}

// buildAllPassingPartsWithPairProofReuse may reuse only the two matching
// failures produced by the adjacent-pair locator. It never reuses a successful
// build or a source-attributed failure. The later extension must identify the
// same ordered pair at the same source position, and its exact files are
// rehashed before the proof replaces two fresh failing extension attempts.
func buildAllPassingPartsWithPairProofReuse(ctx context.Context, sources []LocalSource, scratchDir, mediaToolIdentity string, attempt isolatedBuildAttempt, budget mediaCandidateBudget, reusePairProofs bool) (parts []BuiltOutput, quarantines []QuarantinedBuild, err error) {
	if len(sources) == 0 {
		return nil, nil, fmt.Errorf("bounded sources are required")
	}
	pairProofs := make(map[string]exactPairFailureProof)
	attempts := 0
	maxAttempts := 6*len(sources) + 2
	run := func(kind string, candidate []LocalSource) (BuiltOutput, error) {
		attempts++
		if attempts > maxAttempts {
			return BuiltOutput{}, fmt.Errorf("%w: attempt budget exceeded", errMediaSplitNotIsolated)
		}
		attemptCtx := ctx
		cancel := func() {}
		if duration := budget(kind, len(candidate)); duration > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, duration)
		}
		defer cancel()
		started := time.Now()
		built, err := attempt(attemptCtx, candidate, scratchDir)
		emitStageTiming(ctx, "media_candidate_"+kind, time.Since(started), err)
		return built, err
	}
	ownedParts := make([]BuiltOutput, 0)
	cleanupParts := func() {
		for _, part := range ownedParts {
			discardIsolatedBuild(part, scratchDir)
		}
		ownedParts = nil
	}
	defer func() {
		if err != nil {
			cleanupParts()
		}
	}()

	full, firstErr := run("full", sources)
	if firstErr == nil {
		return []BuiltOutput{full}, nil, nil
	}
	firstFailure, ok := deterministicBuildFailure(firstErr)
	if !ok {
		if !errors.Is(firstErr, context.DeadlineExceeded) || ctx.Err() != nil {
			return nil, nil, firstErr
		}
	} else if outputSizeFailure(firstFailure) {
		timedAttempt := func(attemptCtx context.Context, candidate []LocalSource, attemptScratch string) (BuiltOutput, error) {
			started := time.Now()
			candidateCtx, cancel := context.WithTimeout(attemptCtx, budget("size", len(candidate)))
			defer cancel()
			built, err := attempt(candidateCtx, candidate, attemptScratch)
			emitStageTiming(ctx, "media_candidate_size", time.Since(started), err)
			return built, err
		}
		return buildSizeBoundParts(ctx, sources, scratchDir, mediaToolIdentity, timedAttempt)
	} else {
		if repeatedBuild, repeatFailure, repeatErr := repeatMatchingFailure(run, "full_repeat", sources, firstFailure); repeatErr != nil {
			return nil, nil, repeatErr
		} else if repeatFailure == nil {
			discardIsolatedBuild(repeatedBuild, scratchDir)
			return nil, nil, fmt.Errorf("%w: full candidate failure was not repeatable", errMediaSplitNotIsolated)
		}
		if len(sources) == 1 {
			return nil, []QuarantinedBuild{{Source: sources[0], Evidence: maximalityEvidence(sources, firstFailure, 2, mediaToolIdentity)}}, nil
		}
	}

	boundaries := make([]int, 0)
	for i := 0; i+1 < len(sources); i++ {
		pair := sources[i : i+2]
		built, pairErr := run("pair", pair)
		if pairErr == nil {
			discardIsolatedBuild(built, scratchDir)
			continue
		}
		pairFailure, deterministic := deterministicBuildFailure(pairErr)
		if !deterministic || outputSizeFailure(pairFailure) {
			return nil, nil, pairErr
		}
		if repeatedBuild, repeated, repeatErr := repeatMatchingFailure(run, "pair_repeat", pair, pairFailure); repeatErr != nil {
			return nil, nil, repeatErr
		} else if repeated == nil {
			discardIsolatedBuild(repeatedBuild, scratchDir)
			return nil, nil, fmt.Errorf("%w: adjacent failure changed across repeats", errMediaSplitNotIsolated)
		}
		if reusePairProofs && reusableExactPairFailure(pairFailure) {
			key, keyErr := exactPairFailureProofKey(pair, i, PlanPolicyVersion, mediaToolIdentity)
			if keyErr == nil {
				pairProofs[key] = exactPairFailureProof{failure: cloneDeterministicMediaError(pairFailure)}
			}
		}
		boundaries = append(boundaries, i+1)
	}
	if len(boundaries) == 0 {
		if firstFailure == nil {
			return nil, nil, errors.Join(errMediaSplitNotIsolated, firstErr)
		}
		remaining := sources
		failedExtension := firstFailure
		for len(remaining) > 1 {
			found := false
			for end := len(remaining) - 1; end > 0; end-- {
				candidate := remaining[:end]
				built, candidateErr := run("prefix", candidate)
				if candidateErr == nil {
					built.SplitEvidence = []MaximalityEvidence{maximalityEvidence(remaining[:end+1], failedExtension, 2, mediaToolIdentity)}
					parts = append(parts, built)
					ownedParts = append(ownedParts, built)
					remaining = remaining[end:]
					found = true
					break
				}
				failure, deterministic := deterministicBuildFailure(candidateErr)
				if !deterministic || outputSizeFailure(failure) {
					return nil, nil, errors.Join(errMediaSplitNotIsolated, firstErr, candidateErr)
				}
				if repeatedBuild, repeated, repeatErr := repeatMatchingFailure(run, "prefix_repeat", candidate, failure); repeatErr != nil {
					return nil, nil, errors.Join(firstErr, repeatErr)
				} else if repeated == nil {
					discardIsolatedBuild(repeatedBuild, scratchDir)
					return nil, nil, errors.Join(firstErr, fmt.Errorf("%w: prefix failure changed across repeats", errMediaSplitNotIsolated))
				}
				failedExtension = failure
			}
			if !found {
				return nil, nil, errors.Join(errMediaSplitNotIsolated, firstErr)
			}

			built, remainingErr := run("remaining", remaining)
			if remainingErr == nil {
				parts = append(parts, built)
				ownedParts = append(ownedParts, built)
				return parts, nil, nil
			}
			failure, deterministic := deterministicBuildFailure(remainingErr)
			if !deterministic || outputSizeFailure(failure) {
				return nil, nil, errors.Join(errMediaSplitNotIsolated, firstErr, remainingErr)
			}
			if repeatedBuild, repeated, repeatErr := repeatMatchingFailure(run, "remaining_repeat", remaining, failure); repeatErr != nil {
				return nil, nil, errors.Join(firstErr, repeatErr)
			} else if repeated == nil {
				discardIsolatedBuild(repeatedBuild, scratchDir)
				return nil, nil, errors.Join(firstErr, fmt.Errorf("%w: remaining failure changed across repeats", errMediaSplitNotIsolated))
			}
			if len(remaining) == 1 {
				quarantines = append(quarantines, QuarantinedBuild{Source: remaining[0], Evidence: maximalityEvidence(remaining, failure, 2, mediaToolIdentity)})
				return parts, quarantines, nil
			}
			failedExtension = failure
		}
		return nil, nil, errors.Join(errMediaSplitNotIsolated, firstErr)
	}

	ends := append(append([]int(nil), boundaries...), len(sources))
	start := 0
	for _, end := range ends {
		segment := sources[start:end]
		built, segmentErr := run("segment", segment)
		if segmentErr != nil {
			failure, deterministic := deterministicBuildFailure(segmentErr)
			if len(segment) != 1 || !deterministic || outputSizeFailure(failure) {
				return nil, nil, errors.Join(errMediaSplitNotIsolated, fmt.Errorf("isolated segment failed: %w", segmentErr))
			}
			if repeatedBuild, repeated, repeatErr := repeatMatchingFailure(run, "singleton_repeat", segment, failure); repeatErr != nil {
				return nil, nil, repeatErr
			} else if repeated == nil {
				discardIsolatedBuild(repeatedBuild, scratchDir)
				return nil, nil, fmt.Errorf("%w: singleton failure changed across repeats", errMediaSplitNotIsolated)
			}
			quarantines = append(quarantines, QuarantinedBuild{Source: segment[0], Evidence: maximalityEvidence(segment, failure, 2, mediaToolIdentity)})
			start = end
			continue
		}
		parts = append(parts, built)
		ownedParts = append(ownedParts, built)
		if end < len(sources) {
			extension := sources[start : end+1]
			if err := ctx.Err(); err != nil {
				return nil, nil, err
			}
			if reusePairProofs && len(extension) == 2 && ctx.Err() == nil {
				key, keyErr := exactPairFailureProofKey(extension, start, PlanPolicyVersion, mediaToolIdentity)
				proof, found := pairProofs[key]
				if keyErr == nil && found && reusableExactPairFailure(proof.failure) && verifyLocalIdentity(extension[0]) == nil && verifyLocalIdentity(extension[1]) == nil {
					if err := ctx.Err(); err != nil {
						return nil, nil, err
					}
					parts[len(parts)-1].SplitEvidence = []MaximalityEvidence{maximalityEvidence(extension, cloneDeterministicMediaError(proof.failure), 2, mediaToolIdentity)}
					emitStageTiming(ctx, "media_candidate_extension_pair_proof_reused", 0, nil)
					start = end
					continue
				}
			}
			extensionBuild, extensionErr := run("extension", extension)
			extensionFailure, deterministic := deterministicBuildFailure(extensionErr)
			if extensionErr == nil || !deterministic || outputSizeFailure(extensionFailure) {
				if extensionErr == nil {
					discardIsolatedBuild(extensionBuild, scratchDir)
					return nil, nil, fmt.Errorf("%w: exact boundary extension unexpectedly passed", errMediaSplitNotIsolated)
				}
				return nil, nil, errors.Join(errMediaSplitNotIsolated, fmt.Errorf("exact boundary extension did not fail deterministically: %w", extensionErr))
			}
			if repeatedBuild, repeated, repeatErr := repeatMatchingFailure(run, "extension_repeat", extension, extensionFailure); repeatErr != nil {
				return nil, nil, repeatErr
			} else if repeated == nil {
				discardIsolatedBuild(repeatedBuild, scratchDir)
				return nil, nil, fmt.Errorf("%w: boundary extension changed across repeats", errMediaSplitNotIsolated)
			}
			parts[len(parts)-1].SplitEvidence = []MaximalityEvidence{maximalityEvidence(extension, extensionFailure, 2, mediaToolIdentity)}
		}
		start = end
	}
	if start != len(sources) {
		return nil, nil, fmt.Errorf("%w: source accounting incomplete", errMediaSplitNotIsolated)
	}
	return parts, quarantines, nil
}

func deterministicBuildFailure(err error) (*deterministicMediaError, bool) {
	var failure *deterministicMediaError
	ok := errors.As(err, &failure)
	return failure, ok
}

func outputSizeFailure(failure *deterministicMediaError) bool {
	return failure != nil && (failure.code == "output_exceeds_put_cap" || failure.code == "lossless_normalization_expansion_cap")
}

func repeatMatchingFailure(run func(string, []LocalSource) (BuiltOutput, error), kind string, sources []LocalSource, first *deterministicMediaError) (BuiltOutput, *deterministicMediaError, error) {
	built, err := run(kind, sources)
	if err == nil {
		return built, nil, nil
	}
	second, ok := deterministicBuildFailure(err)
	if !ok {
		return BuiltOutput{}, nil, err
	}
	if first.code != second.code || first.evidenceSHA256 != second.evidenceSHA256 {
		return BuiltOutput{}, nil, fmt.Errorf("%w: deterministic failure changed across repeats", errMediaSplitNotIsolated)
	}
	return BuiltOutput{}, second, nil
}

func buildSizeBoundParts(ctx context.Context, sources []LocalSource, scratchDir, mediaToolIdentity string, attempt isolatedBuildAttempt) ([]BuiltOutput, []QuarantinedBuild, error) {
	remaining := sources
	parts := make([]BuiltOutput, 0, 1)
	quarantines := []QuarantinedBuild{}
	for len(remaining) > 0 {
		part, err := buildLargestPassingPrefix(ctx, remaining, scratchDir, mediaToolIdentity, attempt)
		if err != nil {
			var failure *deterministicMediaError
			if !errors.As(err, &failure) || outputSizeFailure(failure) {
				for _, provisional := range parts {
					discardIsolatedBuild(provisional, scratchDir)
				}
				return nil, nil, err
			}
			quarantines = append(quarantines, QuarantinedBuild{Source: remaining[0], Evidence: maximalityEvidence(remaining[:1], failure, 2, mediaToolIdentity)})
			remaining = remaining[1:]
			continue
		}
		if part.SourceCount <= 0 || part.SourceCount > len(remaining) {
			for _, provisional := range parts {
				discardIsolatedBuild(provisional, scratchDir)
			}
			discardIsolatedBuild(part, scratchDir)
			return nil, nil, fmt.Errorf("size-bounded preflight part made no bounded progress")
		}
		parts = append(parts, part)
		remaining = remaining[part.SourceCount:]
	}
	return parts, quarantines, nil
}

func discardIsolatedBuild(built BuiltOutput, scratchDir string) {
	if strings.TrimSpace(built.Path) == "" {
		return
	}
	attemptDir := filepath.Dir(built.Path)
	rel, err := filepath.Rel(scratchDir, attemptDir)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) || !strings.HasPrefix(filepath.Base(attemptDir), "attempt-") {
		return
	}
	_ = os.RemoveAll(attemptDir)
}

func PreflightHour(ctx context.Context, draft HourDraft, locals []LocalSource, scratchDir, mediaToolIdentity string) (HourPreflight, error) {
	byID := make(map[int64]LocalSource, len(locals))
	for _, local := range locals {
		if _, exists := byID[local.ClipID]; exists {
			return HourPreflight{}, fmt.Errorf("duplicate downloaded source")
		}
		byID[local.ClipID] = local
	}
	result := HourPreflight{}
	used := map[int64]bool{}
	var previousQuarantined *SourceClip
	for _, candidate := range draft.Parts {
		candidateLocals := make([]LocalSource, len(candidate.Sources))
		for i, source := range candidate.Sources {
			local, ok := byID[source.ClipID]
			if !ok || used[source.ClipID] {
				return HourPreflight{}, fmt.Errorf("candidate run source assignment differs")
			}
			used[source.ClipID] = true
			claimSHA, _, claimErr := sourceClaimSHA([]SourceClip{source})
			if claimErr != nil {
				return HourPreflight{}, claimErr
			}
			local.SourceClaimSHA256 = claimSHA
			candidateLocals[i] = local
		}
		parts, quarantines, err := buildAllPassingParts(ctx, candidateLocals, scratchDir, mediaToolIdentity)
		if err != nil {
			return HourPreflight{}, err
		}
		quarantineIDs := map[int64]QuarantinedBuild{}
		for _, quarantine := range quarantines {
			quarantineIDs[quarantine.Source.ClipID] = quarantine
		}
		offset := 0
		partIndex := 0
		for offset < len(candidate.Sources) {
			if quarantine, ok := quarantineIDs[candidate.Sources[offset].ClipID]; ok {
				result.Quarantined = append(result.Quarantined, candidate.Sources[offset])
				result.Quarantines = append(result.Quarantines, quarantine)
				quarantined := candidate.Sources[offset]
				previousQuarantined = &quarantined
				offset++
				continue
			}
			built := parts[partIndex]
			partSources := append([]SourceClip(nil), candidate.Sources[offset:offset+built.SourceCount]...)
			if previousQuarantined != nil {
				partSources[0].SeamToPrevious = SeamEvidence{Verdict: "incompatible", Reason: "source_quarantined", SignedGapNanoseconds: partSources[0].StartUTC.Sub(previousQuarantined.EndUTC).Nanoseconds()}
			} else if partIndex > 0 {
				reason := "deterministic_media_split"
				if evidence := result.Built[len(result.Built)-1].SplitEvidence; len(evidence) > 0 {
					adjacent := evidence[len(evidence)-1]
					if len(adjacent.CandidateClipIDs) != result.Built[len(result.Built)-1].SourceCount+1 || adjacent.CandidateClipIDs[len(adjacent.CandidateClipIDs)-1] != partSources[0].ClipID {
						return HourPreflight{}, fmt.Errorf("maximality evidence does not bind the adjacent failed extension")
					}
					reason = adjacent.ReasonCode
				}
				partSources[0].SeamToPrevious = SeamEvidence{Verdict: "incompatible", Reason: reason}
			}
			result.Sources = append(result.Sources, partSources...)
			result.Built = append(result.Built, built)
			offset += built.SourceCount
			partIndex++
			previousQuarantined = nil
		}
		if offset != len(candidate.Sources) || partIndex != len(parts) {
			return HourPreflight{}, fmt.Errorf("candidate run was not fully accounted")
		}
	}
	if len(used) != len(locals) {
		return HourPreflight{}, fmt.Errorf("downloaded source omitted from hour preflight")
	}
	return result, nil
}

// BuildSealedOutput verifies the complete frozen claim or fails. Runtime media
// failure invalidates that campaign generation; there is no runtime tail peel.
func BuildSealedOutput(ctx context.Context, sources []LocalSource, scratchDir string) (BuiltOutput, error) {
	if err := validateSealedBuildInputs(sources, scratchDir); err != nil {
		return BuiltOutput{}, err
	}
	return buildAndVerify(ctx, sources, scratchDir)
}

// BuildSealedOutputForVerification rebuilds the already-frozen media mode.
// Reproduction cannot depend on a later worker environment change: a sealed
// normalized artifact stays normalized, while a sealed stream-copy artifact
// never gains a fallback during rebuild.
func BuildSealedOutputForVerification(ctx context.Context, sources []LocalSource, scratchDir string, expected Verification) (BuiltOutput, error) {
	if err := validateSealedBuildInputs(sources, scratchDir); err != nil {
		return BuiltOutput{}, err
	}
	if validatePassedVerification(expected) != nil {
		return BuiltOutput{}, fmt.Errorf("sealed media verification evidence is invalid")
	}
	if expected.AcceptanceMode != "lossless_native_timeline_normalized" {
		return buildStreamCopyAndVerify(ctx, sources, scratchDir)
	}
	if len(sources) < 2 {
		return BuiltOutput{}, fmt.Errorf("sealed lossless normalization evidence is invalid")
	}
	evidence := expected.LosslessNormalization
	trigger := &deterministicMediaError{
		code:           evidence.TriggerReasonCode,
		evidenceSHA256: evidence.TriggerFailureSHA256,
		evidence:       append(json.RawMessage(nil), evidence.TriggerFailureFacts...),
		err:            fmt.Errorf("sealed stream-copy rejection"),
	}
	return buildLosslessNativeTimeline(ctx, sources, scratchDir, trigger)
}

func validateSealedBuildInputs(sources []LocalSource, scratchDir string) error {
	if len(sources) == 0 || strings.TrimSpace(scratchDir) == "" {
		return fmt.Errorf("bounded sources and scratch directory are required")
	}
	if err := os.MkdirAll(scratchDir, 0700); err != nil {
		return fmt.Errorf("create scratch: %w", err)
	}
	for _, source := range sources {
		if err := verifyLocalIdentity(source); err != nil {
			return err
		}
	}
	return nil
}

func buildAndVerify(ctx context.Context, sources []LocalSource, scratchDir string) (BuiltOutput, error) {
	if len(sources) == 1 || !losslessNormalizationEnabled() {
		return buildStreamCopyAndVerify(ctx, sources, scratchDir)
	}
	return buildWithLosslessFallback(ctx, sources, scratchDir, buildStreamCopyAndVerify, buildLosslessNativeTimeline)
}

func losslessNormalizationEnabled() bool {
	return os.Getenv("JOINED_LOSSLESS_NORMALIZATION_ENABLED") == "true"
}

type mediaBuilder func(context.Context, []LocalSource, string) (BuiltOutput, error)
type losslessMediaBuilder func(context.Context, []LocalSource, string, *deterministicMediaError) (BuiltOutput, error)

func buildWithLosslessFallback(ctx context.Context, sources []LocalSource, scratchDir string, streamCopy mediaBuilder, normalize losslessMediaBuilder) (BuiltOutput, error) {
	built, err := streamCopy(ctx, sources, scratchDir)
	if err == nil {
		return built, nil
	}
	failure, deterministic := deterministicBuildFailure(err)
	if !deterministic || failure.code != "media_sequence_mismatch" {
		return BuiltOutput{}, err
	}
	return normalize(ctx, sources, scratchDir, failure)
}

func buildStreamCopyAndVerify(ctx context.Context, sources []LocalSource, scratchDir string) (BuiltOutput, error) {
	var sourceBytes int64
	for _, source := range sources {
		if source.SizeBytes > r2.MaxConditionalPutBytes-sourceBytes {
			return BuiltOutput{}, deterministicFailure("output_exceeds_put_cap", struct {
				SourceBytes int64 `json:"source_bytes"`
			}{sourceBytes + source.SizeBytes}, fmt.Errorf("candidate exceeds conditional R2 PUT cap"))
		}
		sourceBytes += source.SizeBytes
	}
	if len(sources) == 1 {
		return copyAndVerifySingleton(ctx, sources[0], scratchDir)
	}
	manifest, err := os.CreateTemp(scratchDir, "concat-*.txt")
	if err != nil {
		return BuiltOutput{}, err
	}
	manifestPath := manifest.Name()
	defer os.Remove(manifestPath)
	for _, source := range sources {
		if strings.ContainsAny(source.Path, "\r\n") {
			_ = manifest.Close()
			return BuiltOutput{}, fmt.Errorf("source path contains newline")
		}
		escaped := strings.ReplaceAll(source.Path, "'", "'\\''")
		if _, err := fmt.Fprintf(manifest, "file '%s'\n", escaped); err != nil {
			_ = manifest.Close()
			return BuiltOutput{}, err
		}
	}
	if err := manifest.Close(); err != nil {
		return BuiltOutput{}, err
	}
	handle, err := os.CreateTemp(scratchDir, "joined-*.mp4")
	if err != nil {
		return BuiltOutput{}, err
	}
	outputPath := handle.Name()
	_ = handle.Close()
	_ = os.Remove(outputPath)
	keep := false
	defer func() {
		if !keep {
			_ = os.Remove(outputPath)
		}
	}()
	if err := runBounded(ctx, ffmpegBinary(), "-nostdin", "-v", "error", "-f", "concat", "-safe", "0", "-i", manifestPath, "-copyts", "-map", "0:v:0", "-map", "0:a?", "-c", "copy", "-movflags", "+faststart", outputPath); err != nil {
		return BuiltOutput{}, deterministicCommandFailure(ctx, "source_media_incompatible", fmt.Errorf("stream-copy join: %w", err))
	}
	verification, err := VerifyJoinedMedia(ctx, sources, outputPath)
	if err != nil {
		return BuiltOutput{}, err
	}
	size, sha, err := localIdentity(outputPath)
	if err != nil {
		return BuiltOutput{}, err
	}
	if size > r2.MaxConditionalPutBytes {
		return BuiltOutput{}, deterministicFailure("output_exceeds_put_cap", struct {
			OutputBytes int64 `json:"output_bytes"`
		}{size}, fmt.Errorf("joined output exceeds conditional R2 PUT cap"))
	}
	keep = true
	return BuiltOutput{Path: outputPath, SizeBytes: size, SHA256: sha, SourceCount: len(sources), Verification: verification}, nil
}

const losslessNormalizationExpansionLimit int64 = 7

// The planner can retain outputs for prior/current source sets P+A while an
// exact boundary extension A+x is encoded. Since P+A+x is within the frozen
// source set S, 7(P+A)+7(A+x) is at most 14S.
const losslessNormalizationScratchOutputMultiplier int64 = losslessNormalizationExpansionLimit * 2

type losslessVideoLayout struct {
	Width             int
	Height            int
	PixelFormat       string
	FrameRate         string
	RateNum           int64
	RateDen           int64
	SampleAspectRatio string
	ColorRange        string
	ColorSpace        string
	ColorTransfer     string
	ColorPrimaries    string
	ChromaLocation    string
	FieldOrder        string
}

const explicitProgressiveFrameStatus = "all_frames_match_explicit_progressive_layout"

type decodedFrameFieldProof struct {
	Frames int64
	SHA256 string
}

type unsupportedDecodedFrameField struct {
	SourceIndex  int
	FrameOrdinal int64
	Field        string
	Value        string
}

func (e *unsupportedDecodedFrameField) Error() string {
	return fmt.Sprintf("decoded frame %d has unsupported %s=%s", e.FrameOrdinal, e.Field, e.Value)
}

func buildLosslessNativeTimeline(ctx context.Context, sources []LocalSource, scratchDir string, trigger *deterministicMediaError) (BuiltOutput, error) {
	if len(sources) < 2 {
		return BuiltOutput{}, fmt.Errorf("lossless normalization requires multiple sources")
	}
	if trigger == nil || trigger.code != "media_sequence_mismatch" || !lowerHex64(trigger.evidenceSHA256) {
		return BuiltOutput{}, fmt.Errorf("lossless normalization requires the rejected stream-copy evidence")
	}
	triggerSHA, triggerFacts, triggerErr := stitchcert.CanonicalSHA(trigger.evidence)
	if triggerErr != nil || len(triggerFacts) == 0 || triggerSHA != trigger.evidenceSHA256 {
		return BuiltOutput{}, fmt.Errorf("lossless normalization stream-copy evidence is invalid")
	}
	layouts := make([]losslessVideoLayout, len(sources))
	timelineFacts := make([]struct {
		ClipID            int64  `json:"clip_id"`
		SourceClaimSHA256 string `json:"source_claim_sha256"`
		Width             int    `json:"width"`
		Height            int    `json:"height"`
		PixelFormat       string `json:"pixel_format"`
		FrameRate         string `json:"frame_rate"`
		SampleAspectRatio string `json:"sample_aspect_ratio"`
		ColorRange        string `json:"color_range"`
		ColorSpace        string `json:"color_space"`
		ColorTransfer     string `json:"color_transfer"`
		ColorPrimaries    string `json:"color_primaries"`
		ChromaLocation    string `json:"chroma_location"`
		FieldOrder        string `json:"field_order"`
	}, len(sources))
	var sourceBytes int64
	sourcePaths := make([]string, len(sources))
	for i, source := range sources {
		layout, hasAudio, err := probeLosslessVideoLayout(ctx, source.Path)
		if err != nil {
			return BuiltOutput{}, deterministicEvidenceFailure(ctx, "lossless_normalization_source_probe_failure", err)
		}
		if hasAudio {
			return BuiltOutput{}, deterministicFailure("lossless_normalization_audio_unsupported", struct {
				ClipID int64 `json:"clip_id"`
			}{source.ClipID}, fmt.Errorf("audio-bearing sources remain on the strict stream-copy path"))
		}
		if layout.PixelFormat != "yuv420p" {
			return BuiltOutput{}, deterministicFailure("lossless_normalization_pixel_format_unsupported", struct {
				ClipID      int64  `json:"clip_id"`
				PixelFormat string `json:"pixel_format"`
			}{source.ClipID, layout.PixelFormat}, fmt.Errorf("lossless normalization requires yuv420p input"))
		}
		if layout.FieldOrder != "progressive" {
			return BuiltOutput{}, deterministicFailure("lossless_normalization_field_order_unsupported", struct {
				ClipID     int64  `json:"clip_id"`
				FieldOrder string `json:"field_order"`
			}{source.ClipID, layout.FieldOrder}, fmt.Errorf("lossless normalization requires explicitly progressive input"))
		}
		if i > 0 && layout != layouts[0] {
			return BuiltOutput{}, deterministicFailure("lossless_normalization_layout_mismatch", struct {
				ClipID int64 `json:"clip_id"`
			}{source.ClipID}, fmt.Errorf("source video layout or native frame rate changes within candidate"))
		}
		layouts[i] = layout
		sourcePaths[i] = source.Path
		timelineFacts[i] = struct {
			ClipID            int64  `json:"clip_id"`
			SourceClaimSHA256 string `json:"source_claim_sha256"`
			Width             int    `json:"width"`
			Height            int    `json:"height"`
			PixelFormat       string `json:"pixel_format"`
			FrameRate         string `json:"frame_rate"`
			SampleAspectRatio string `json:"sample_aspect_ratio"`
			ColorRange        string `json:"color_range"`
			ColorSpace        string `json:"color_space"`
			ColorTransfer     string `json:"color_transfer"`
			ColorPrimaries    string `json:"color_primaries"`
			ChromaLocation    string `json:"chroma_location"`
			FieldOrder        string `json:"field_order"`
		}{source.ClipID, source.SourceClaimSHA256, layout.Width, layout.Height, layout.PixelFormat, layout.FrameRate, layout.SampleAspectRatio, layout.ColorRange, layout.ColorSpace, layout.ColorTransfer, layout.ColorPrimaries, layout.ChromaLocation, layout.FieldOrder}
		if source.SizeBytes > math.MaxInt64-sourceBytes {
			return BuiltOutput{}, fmt.Errorf("lossless normalization source size overflows")
		}
		sourceBytes += source.SizeBytes
	}
	sourceFrameFields, err := decodedFrameFieldSequenceProof(ctx, sourcePaths, layouts[0])
	if err != nil {
		var unsupported *unsupportedDecodedFrameField
		if errors.As(err, &unsupported) && unsupported.SourceIndex >= 0 && unsupported.SourceIndex < len(sources) {
			return BuiltOutput{}, deterministicFailure("lossless_normalization_frame_fields_unsupported", struct {
				ClipID       int64  `json:"clip_id"`
				FrameOrdinal int64  `json:"frame_ordinal"`
				Field        string `json:"field"`
				Value        string `json:"value"`
			}{sources[unsupported.SourceIndex].ClipID, unsupported.FrameOrdinal, unsupported.Field, unsupported.Value}, err)
		}
		return BuiltOutput{}, deterministicEvidenceFailure(ctx, "lossless_normalization_frame_field_probe_failure", err)
	}

	outputLimit := r2.MaxConditionalPutBytes
	if sourceBytes <= math.MaxInt64/losslessNormalizationExpansionLimit {
		if expanded := sourceBytes * losslessNormalizationExpansionLimit; expanded < outputLimit {
			outputLimit = expanded
		}
	}
	if outputLimit <= 0 {
		return BuiltOutput{}, fmt.Errorf("lossless normalization output limit is invalid")
	}
	timelineSHA, _, err := stitchcert.CanonicalSHA(timelineFacts)
	if err != nil {
		return BuiltOutput{}, err
	}
	manifest, err := os.CreateTemp(scratchDir, "lossless-concat-*.txt")
	if err != nil {
		return BuiltOutput{}, err
	}
	manifestPath := manifest.Name()
	defer os.Remove(manifestPath)
	for _, source := range sources {
		if strings.ContainsAny(source.Path, "\r\n") {
			_ = manifest.Close()
			return BuiltOutput{}, fmt.Errorf("source path contains newline")
		}
		escaped := strings.ReplaceAll(source.Path, "'", "'\\''")
		if _, err := fmt.Fprintf(manifest, "file '%s'\n", escaped); err != nil {
			_ = manifest.Close()
			return BuiltOutput{}, err
		}
	}
	if err := manifest.Close(); err != nil {
		return BuiltOutput{}, err
	}
	handle, err := os.CreateTemp(scratchDir, "joined-lossless-*.mp4")
	if err != nil {
		return BuiltOutput{}, err
	}
	outputPath := handle.Name()
	_ = handle.Close()
	_ = os.Remove(outputPath)
	keep := false
	defer func() {
		if !keep {
			_ = os.Remove(outputPath)
		}
	}()
	layout := layouts[0]
	timelineRule := fmt.Sprintf("settb=expr=%d/%d,setpts=N,setsar=%s", layout.RateDen, layout.RateNum, strings.ReplaceAll(layout.SampleAspectRatio, ":", "/"))
	encodeArgs := []string{"-nostdin", "-v", "error", "-xerror", "-err_detect", "explode", "-f", "concat", "-safe", "0", "-i", manifestPath, "-map", "0:v:0", "-an", "-vf", timelineRule, "-c:v", "libx264", "-preset", "veryfast", "-qp", "0", "-pix_fmt", "yuv420p", "-fps_mode", "passthrough", "-enc_time_base", fmt.Sprintf("%d:%d", layout.RateDen, layout.RateNum)}
	encodeArgs = appendLosslessDisplayMetadata(encodeArgs, layout)
	encodeArgs = append(encodeArgs, "-movflags", "+faststart", "-fs", strconv.FormatInt(outputLimit, 10), outputPath)
	if err := runBounded(ctx, ffmpegBinary(), encodeArgs...); err != nil {
		return BuiltOutput{}, deterministicCommandFailure(ctx, "lossless_normalization_encode_failure", err)
	}
	size, sha, err := localIdentity(outputPath)
	if err != nil {
		return BuiltOutput{}, err
	}
	if size >= outputLimit {
		return BuiltOutput{}, losslessOutputLimitFailure(size, outputLimit)
	}
	verification, err := verifyLosslessNormalizedMedia(ctx, sources, outputPath, layout)
	if err != nil {
		return BuiltOutput{}, deterministicFailure("lossless_normalization_verification_failure", verification, err)
	}
	sourceVideo, outputVideo := verification.SourceFingerprint.Tracks["video"], verification.OutputFingerprint.Tracks["video"]
	decodedSHA := verification.SourceFingerprint.DecodedVideoSHA256
	outputFrameFields, outputFieldErr := decodedFrameFieldSequenceProof(ctx, []string{outputPath}, layout)
	if outputFieldErr != nil {
		var unsupported *unsupportedDecodedFrameField
		if errors.As(outputFieldErr, &unsupported) {
			return BuiltOutput{}, deterministicFailure("lossless_normalization_output_frame_fields_invalid", struct {
				FrameOrdinal int64  `json:"frame_ordinal"`
				Field        string `json:"field"`
				Value        string `json:"value"`
			}{unsupported.FrameOrdinal, unsupported.Field, unsupported.Value}, outputFieldErr)
		}
		return BuiltOutput{}, deterministicEvidenceFailure(ctx, "lossless_normalization_output_frame_field_probe_failure", outputFieldErr)
	}
	if sourceVideo == nil || outputVideo == nil || sourceVideo.DecodedFrames <= 0 || sourceVideo.DecodedFrames != outputVideo.DecodedFrames || sourceFrameFields.Frames != sourceVideo.DecodedFrames || outputFrameFields.Frames != outputVideo.DecodedFrames || sourceFrameFields.SHA256 != outputFrameFields.SHA256 || !lowerHex64(decodedSHA) || decodedSHA != verification.OutputFingerprint.DecodedVideoSHA256 {
		return BuiltOutput{}, deterministicFailure("lossless_normalization_frame_mismatch", verification, fmt.Errorf("lossless decoded frame evidence differs"))
	}
	verification.AcceptanceMode = "lossless_native_timeline_normalized"
	verification.PacketPayloadOrderStatus = "not_applicable_lossless_normalization"
	verification.LosslessNormalization = &LosslessNormalizationEvidence{
		Codec:                         "libx264",
		Preset:                        "veryfast",
		Quantizer:                     0,
		PixelFormat:                   layout.PixelFormat,
		FrameRate:                     layout.FrameRate,
		SampleAspectRatio:             layout.SampleAspectRatio,
		ColorRange:                    layout.ColorRange,
		ColorSpace:                    layout.ColorSpace,
		ColorTransfer:                 layout.ColorTransfer,
		ColorPrimaries:                layout.ColorPrimaries,
		ChromaLocation:                layout.ChromaLocation,
		FieldOrder:                    layout.FieldOrder,
		TimelineRule:                  timelineRule,
		SourceDecodedFrames:           sourceVideo.DecodedFrames,
		OutputDecodedFrames:           outputVideo.DecodedFrames,
		DecodedFrameSequenceSHA256:    decodedSHA,
		DecodedFrameFieldStatus:       explicitProgressiveFrameStatus,
		DecodedFrameFieldSHA256:       sourceFrameFields.SHA256,
		SourceTimelineSignatureSHA256: timelineSHA,
		OutputLimitBytes:              outputLimit,
		AudioStatus:                   "absent",
		TriggerReasonCode:             trigger.code,
		TriggerFailureFacts:           append(json.RawMessage(nil), trigger.evidence...),
		TriggerFailureSHA256:          trigger.evidenceSHA256,
	}
	keep = true
	return BuiltOutput{Path: outputPath, SizeBytes: size, SHA256: sha, SourceCount: len(sources), Verification: verification}, nil
}

func losslessOutputLimitFailure(size, limit int64) error {
	code := "lossless_normalization_expansion_cap"
	if limit == r2.MaxConditionalPutBytes {
		code = "output_exceeds_put_cap"
	}
	return deterministicFailure(code, struct {
		OutputBytes int64 `json:"output_bytes"`
		LimitBytes  int64 `json:"limit_bytes"`
	}{size, limit}, fmt.Errorf("lossless normalized output reached its bounded size limit"))
}

func appendLosslessDisplayMetadata(args []string, layout losslessVideoLayout) []string {
	for _, option := range []struct {
		flag  string
		value string
	}{
		{"-color_range", layout.ColorRange},
		{"-colorspace", layout.ColorSpace},
		{"-color_trc", layout.ColorTransfer},
		{"-color_primaries", layout.ColorPrimaries},
		{"-chroma_sample_location", layout.ChromaLocation},
		{"-field_order", layout.FieldOrder},
	} {
		if value := strings.TrimSpace(option.value); value != "" && value != "unknown" {
			args = append(args, option.flag, value)
		}
	}
	return args
}

func verifyLosslessNormalizedMedia(ctx context.Context, sources []LocalSource, outputPath string, sourceLayout losslessVideoLayout) (Verification, error) {
	outputLayout, hasAudio, err := probeLosslessVideoLayout(ctx, outputPath)
	if err != nil {
		return Verification{}, fmt.Errorf("probe lossless output display metadata: %w", err)
	}
	if hasAudio || outputLayout != sourceLayout {
		return Verification{}, fmt.Errorf("lossless output display metadata differs from sources: source=%+v output=%+v audio=%t", sourceLayout, outputLayout, hasAudio)
	}
	expectedAccumulator := newMediaAccumulator()
	for sourceIndex, source := range sources {
		if err := probeMediaInto(ctx, source.Path, expectedAccumulator, source.AudioContract, true); err != nil {
			return Verification{}, fmt.Errorf("probe lossless source ordinal=%d clip_id=%d: %w", sourceIndex+1, source.ClipID, err)
		}
	}
	expected := expectedAccumulator.fingerprint()
	actual, err := probeMedia(ctx, outputPath)
	if err != nil {
		return Verification{}, fmt.Errorf("probe lossless output: %w", err)
	}
	verification := Verification{Status: "failed", AcceptanceMode: "lossless_native_timeline_normalized", PacketPayloadOrderStatus: "not_applicable_lossless_normalization", SourceFingerprint: expected, OutputFingerprint: actual}
	if len(expected.Tracks) != 1 || len(actual.Tracks) != 1 || expected.Tracks["audio"] != nil || actual.Tracks["audio"] != nil || actual.DurationSeconds <= 0 || math.Abs(actual.DurationSeconds-expected.DurationSeconds) > 2 {
		return verification, fmt.Errorf("lossless output duration or stream cardinality mismatch")
	}
	wantVideo, gotVideo := expected.Tracks["video"], actual.Tracks["video"]
	if wantVideo == nil || gotVideo == nil || wantVideo.TimestampStatus != "source_clips_independent" || gotVideo.TimestampStatus != "monotonic" || wantVideo.DecodedFrames <= 0 || wantVideo.DecodedFrames != gotVideo.DecodedFrames {
		return verification, fmt.Errorf("lossless output decoded frame totals or timeline mismatch")
	}
	sourcePaths := make([]string, len(sources))
	for i := range sources {
		sourcePaths[i] = sources[i].Path
	}
	wantFrames, wantSHA, err := decodedVideoSequenceIdentity(ctx, sourcePaths)
	if err != nil {
		return verification, fmt.Errorf("decode lossless source frame sequence: %w", err)
	}
	gotFrames, gotSHA, err := decodedVideoSequenceIdentity(ctx, []string{outputPath})
	if err != nil {
		return verification, fmt.Errorf("decode lossless output frame sequence: %w", err)
	}
	if wantFrames != wantVideo.DecodedFrames || gotFrames != gotVideo.DecodedFrames || wantFrames != gotFrames || wantSHA != gotSHA {
		return verification, fmt.Errorf("lossless output decoded frame sequence mismatch")
	}
	verification.SourceFingerprint.DecodedVideoSHA256 = wantSHA
	verification.OutputFingerprint.DecodedVideoSHA256 = gotSHA
	verification.DecodedFrameSequenceStatus = "passed"
	verification.DecodedFrameTotalsStatus = "passed"
	verification.DecodedAudioTotalsStatus = "passed"
	verification.OutputTimestampStatus = "passed"
	if err := runBounded(ctx, ffmpegBinary(), "-nostdin", "-v", "error", "-xerror", "-err_detect", "explode", "-i", outputPath, "-map", "0:v:0", "-an", "-f", "null", "-"); err != nil {
		return verification, fmt.Errorf("strict lossless output decode: %w", err)
	}
	verification.StrictDecodeStatus = "passed"
	verification.Status = "passed"
	return verification, nil
}

func validateLosslessNormalizationVerification(v Verification) error {
	evidence := v.LosslessNormalization
	if v.AcceptanceMode != "lossless_native_timeline_normalized" || evidence == nil || v.Status != "passed" || v.PacketPayloadOrderStatus != "not_applicable_lossless_normalization" || v.DecodedFrameSequenceStatus != "passed" || v.DecodedFrameTotalsStatus != "passed" || v.DecodedAudioTotalsStatus != "passed" || v.OutputTimestampStatus != "passed" || v.StrictDecodeStatus != "passed" {
		return fmt.Errorf("lossless normalization status evidence differs")
	}
	if evidence.Codec != "libx264" || evidence.Preset != "veryfast" || evidence.Quantizer != 0 || evidence.PixelFormat != "yuv420p" || evidence.AudioStatus != "absent" || evidence.TriggerReasonCode != "media_sequence_mismatch" || !lowerHex64(evidence.TriggerFailureSHA256) || !lowerHex64(evidence.DecodedFrameSequenceSHA256) || evidence.DecodedFrameFieldStatus != explicitProgressiveFrameStatus || !lowerHex64(evidence.DecodedFrameFieldSHA256) || !lowerHex64(evidence.SourceTimelineSignatureSHA256) || evidence.OutputLimitBytes <= 0 || evidence.OutputLimitBytes > r2.MaxConditionalPutBytes {
		return fmt.Errorf("lossless normalization codec evidence differs")
	}
	triggerSHA, triggerFacts, triggerErr := stitchcert.CanonicalSHA(evidence.TriggerFailureFacts)
	if triggerErr != nil || len(triggerFacts) == 0 || triggerSHA != evidence.TriggerFailureSHA256 || validateRejectedStreamCopyVerification(evidence.TriggerFailureFacts, v.SourceFingerprint) != nil {
		return fmt.Errorf("lossless normalization trigger evidence differs")
	}
	rate, ok := new(big.Rat).SetString(evidence.FrameRate)
	aspect, aspectOK := new(big.Rat).SetString(strings.ReplaceAll(evidence.SampleAspectRatio, ":", "/"))
	if !ok || rate.Sign() <= 0 || !rate.Num().IsInt64() || !rate.Denom().IsInt64() || !aspectOK || aspect.Sign() <= 0 || !aspect.Num().IsInt64() || !aspect.Denom().IsInt64() || evidence.TimelineRule != fmt.Sprintf("settb=expr=%d/%d,setpts=N,setsar=%s", rate.Denom().Int64(), rate.Num().Int64(), strings.ReplaceAll(evidence.SampleAspectRatio, ":", "/")) {
		return fmt.Errorf("lossless normalization timeline evidence differs")
	}
	if evidence.FieldOrder != "progressive" {
		return fmt.Errorf("lossless normalization field order differs")
	}
	for _, value := range []string{evidence.ColorRange, evidence.ColorSpace, evidence.ColorTransfer, evidence.ColorPrimaries, evidence.ChromaLocation} {
		if len(value) > 64 {
			return fmt.Errorf("lossless normalization display metadata differs")
		}
	}
	if validateFingerprint(v.SourceFingerprint, false) != nil || validateFingerprint(v.OutputFingerprint, true) != nil || len(v.SourceFingerprint.Tracks) != 1 || len(v.OutputFingerprint.Tracks) != 1 || v.SourceFingerprint.Tracks["audio"] != nil || v.OutputFingerprint.Tracks["audio"] != nil || math.Abs(v.SourceFingerprint.DurationSeconds-v.OutputFingerprint.DurationSeconds) > 2 {
		return fmt.Errorf("lossless normalization media fingerprint differs")
	}
	sourceVideo, outputVideo := v.SourceFingerprint.Tracks["video"], v.OutputFingerprint.Tracks["video"]
	if sourceVideo == nil || outputVideo == nil || sourceVideo.TimestampStatus != "source_clips_independent" || outputVideo.TimestampStatus != "monotonic" || sourceVideo.DecodedFrames <= 0 || sourceVideo.DecodedFrames != outputVideo.DecodedFrames || evidence.SourceDecodedFrames != sourceVideo.DecodedFrames || evidence.OutputDecodedFrames != outputVideo.DecodedFrames || v.SourceFingerprint.DecodedVideoSHA256 != evidence.DecodedFrameSequenceSHA256 || v.OutputFingerprint.DecodedVideoSHA256 != evidence.DecodedFrameSequenceSHA256 {
		return fmt.Errorf("lossless normalization decoded frame evidence differs")
	}
	if progressiveFrameFieldSequenceSHA256(evidence.SourceDecodedFrames) != evidence.DecodedFrameFieldSHA256 {
		return fmt.Errorf("lossless normalization decoded frame field evidence differs")
	}
	return nil
}

func progressiveFrameFieldSequenceSHA256(frames int64) string {
	payload := fmt.Sprintf("%s|%d\n", explicitProgressiveFrameStatus, frames)
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

func frameLayoutValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	return value
}

func validateDecodedFrameFieldLine(line string, sourceIndex int, frameOrdinal int64, layout losslessVideoLayout) error {
	fields := strings.Split(strings.TrimSpace(line), "|")
	values := make(map[string]string, 12)
	seen := make(map[string]bool, len(fields)-1)
	if len(fields) == 0 || fields[0] != "frame" {
		return fmt.Errorf("invalid decoded frame field evidence")
	}
	for _, field := range fields[1:] {
		key, value, ok := strings.Cut(field, "=")
		if !ok || seen[key] {
			return fmt.Errorf("invalid decoded frame field evidence")
		}
		seen[key] = true
		if strings.HasSuffix(key, ":side_data_type") {
			switch value {
			case "H.26[45] User Data Unregistered SEI message", "H.264 User Data Unregistered SEI message", "H.265 User Data Unregistered SEI message":
				continue
			}
		}
		switch key {
		case "width", "height", "pix_fmt", "sample_aspect_ratio", "interlaced_frame", "top_field_first", "repeat_pict", "color_range", "color_space", "color_primaries", "color_transfer", "chroma_location":
		default:
			return fmt.Errorf("unsupported decoded frame side data")
		}
		values[key] = value
	}
	expected := map[string]string{
		"width":               strconv.Itoa(layout.Width),
		"height":              strconv.Itoa(layout.Height),
		"pix_fmt":             layout.PixelFormat,
		"sample_aspect_ratio": layout.SampleAspectRatio,
		"interlaced_frame":    "0",
		"top_field_first":     "0",
		"repeat_pict":         "0",
		"color_range":         frameLayoutValue(layout.ColorRange),
		"color_space":         frameLayoutValue(layout.ColorSpace),
		"color_primaries":     frameLayoutValue(layout.ColorPrimaries),
		"color_transfer":      frameLayoutValue(layout.ColorTransfer),
		"chroma_location":     frameLayoutValue(layout.ChromaLocation),
	}
	for _, key := range []string{"width", "height", "pix_fmt", "sample_aspect_ratio", "interlaced_frame", "top_field_first", "repeat_pict", "color_range", "color_space", "color_primaries", "color_transfer", "chroma_location"} {
		if values[key] != expected[key] {
			value := values[key]
			if value == "" {
				value = "missing"
			}
			return &unsupportedDecodedFrameField{SourceIndex: sourceIndex, FrameOrdinal: frameOrdinal, Field: key, Value: value}
		}
	}
	return nil
}

func decodedFrameFieldSequenceProof(ctx context.Context, mediaPaths []string, layout losslessVideoLayout) (decodedFrameFieldProof, error) {
	var frames int64
	for sourceIndex, mediaPath := range mediaPaths {
		process := newBoundedMediaProcess(ctx, ffprobeBinary(), "-v", "error", "-err_detect", "explode", "-select_streams", "v:0", "-show_frames", "-show_entries", "frame=width,height,pix_fmt,sample_aspect_ratio,interlaced_frame,top_field_first,repeat_pict,color_range,color_space,color_primaries,color_transfer,chroma_location", "-of", "compact=p=1:nk=0", mediaPath)
		stdout, err := process.cmd.StdoutPipe()
		if err != nil {
			return decodedFrameFieldProof{}, err
		}
		var stderr limitedOutput
		process.cmd.Stderr = &stderr
		if err := process.Start(); err != nil {
			return decodedFrameFieldProof{}, err
		}
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 4096), 1<<20)
		var sourceFrames int64
		for scanner.Scan() {
			if err := ctx.Err(); err != nil {
				_ = process.Kill()
				_ = process.Wait()
				return decodedFrameFieldProof{}, err
			}
			sourceFrames++
			if err := validateDecodedFrameFieldLine(scanner.Text(), sourceIndex, sourceFrames, layout); err != nil {
				_ = process.Kill()
				_ = process.Wait()
				return decodedFrameFieldProof{}, err
			}
			frames++
		}
		if err := scanner.Err(); err != nil {
			_ = process.Kill()
			_ = process.Wait()
			return decodedFrameFieldProof{}, err
		}
		if err := process.Wait(); err != nil {
			return decodedFrameFieldProof{}, fmt.Errorf("ffprobe decoded frame fields: %w (%s)", err, stderr.String())
		}
		if sourceFrames == 0 {
			return decodedFrameFieldProof{}, fmt.Errorf("decoded frame field evidence is empty")
		}
	}
	if frames == 0 {
		return decodedFrameFieldProof{}, fmt.Errorf("decoded frame field evidence is empty")
	}
	return decodedFrameFieldProof{Frames: frames, SHA256: progressiveFrameFieldSequenceSHA256(frames)}, nil
}

func validateRejectedStreamCopyVerification(raw json.RawMessage, normalizedSource MediaFingerprint) error {
	var rejected Verification
	if decodeStrictJSON(raw, &rejected) != nil || rejected.Status != "failed" || rejected.AcceptanceMode != "" || rejected.LosslessNormalization != nil || rejected.PacketPayloadOrderStatus != "" || rejected.DecodedFrameSequenceStatus != "failed" || rejected.DecodedFrameTotalsStatus != "" || rejected.DecodedAudioTotalsStatus != "" || rejected.OutputTimestampStatus != "" || rejected.StrictDecodeStatus != "" {
		return fmt.Errorf("rejected stream-copy status evidence differs")
	}
	if validateFingerprint(rejected.SourceFingerprint, false) != nil || validateFingerprint(rejected.OutputFingerprint, false) != nil || !lowerHex64(rejected.SourceFingerprint.DecodedVideoSHA256) || !lowerHex64(rejected.OutputFingerprint.DecodedVideoSHA256) || !sameCanonical([]MediaFingerprint{rejected.SourceFingerprint}, []MediaFingerprint{normalizedSource}) {
		return fmt.Errorf("rejected stream-copy fingerprint evidence differs")
	}
	if compareFingerprints(rejected.SourceFingerprint, rejected.OutputFingerprint) == nil || validateDecodedEquivalentFingerprints(rejected.SourceFingerprint, rejected.OutputFingerprint) == nil {
		return fmt.Errorf("stream-copy evidence does not prove both acceptance modes rejected it")
	}
	return nil
}

func probeLosslessVideoLayout(ctx context.Context, mediaPath string) (losslessVideoLayout, bool, error) {
	process := newBoundedMediaProcess(ctx, ffprobeBinary(), "-v", "error", "-show_entries", "stream=codec_type,width,height,pix_fmt,avg_frame_rate,r_frame_rate,sample_aspect_ratio,color_range,color_space,color_transfer,color_primaries,chroma_location,field_order:stream_side_data=side_data_type,rotation", "-of", "json", mediaPath)
	stdout, err := process.cmd.StdoutPipe()
	if err != nil {
		return losslessVideoLayout{}, false, err
	}
	var stderr limitedOutput
	process.cmd.Stderr = &stderr
	if err := process.Start(); err != nil {
		return losslessVideoLayout{}, false, err
	}
	out, readErr := io.ReadAll(io.LimitReader(stdout, mediaCommandOutputLimit+1))
	tooLarge := len(out) > mediaCommandOutputLimit
	if tooLarge || readErr != nil {
		_ = process.Kill()
	}
	waitErr := process.Wait()
	if readErr != nil {
		return losslessVideoLayout{}, false, readErr
	}
	if tooLarge {
		return losslessVideoLayout{}, false, fmt.Errorf("ffprobe lossless layout exceeds bounded output")
	}
	if waitErr != nil {
		return losslessVideoLayout{}, false, fmt.Errorf("ffprobe lossless layout: %w (%s)", waitErr, stderr.String())
	}
	var payload struct {
		Streams []struct {
			CodecType         string `json:"codec_type"`
			Width             int    `json:"width"`
			Height            int    `json:"height"`
			PixelFormat       string `json:"pix_fmt"`
			AvgFrameRate      string `json:"avg_frame_rate"`
			RFrameRate        string `json:"r_frame_rate"`
			SampleAspectRatio string `json:"sample_aspect_ratio"`
			ColorRange        string `json:"color_range"`
			ColorSpace        string `json:"color_space"`
			ColorTransfer     string `json:"color_transfer"`
			ColorPrimaries    string `json:"color_primaries"`
			ChromaLocation    string `json:"chroma_location"`
			FieldOrder        string `json:"field_order"`
			SideDataList      []struct {
				SideDataType string `json:"side_data_type"`
				Rotation     int    `json:"rotation"`
			} `json:"side_data_list"`
		} `json:"streams"`
	}
	if err := json.Unmarshal(out, &payload); err != nil {
		return losslessVideoLayout{}, false, err
	}
	var layout losslessVideoLayout
	videoCount, hasAudio := 0, false
	for _, stream := range payload.Streams {
		switch stream.CodecType {
		case "audio":
			hasAudio = true
		case "video":
			videoCount++
			if len(stream.SideDataList) != 0 {
				return losslessVideoLayout{}, false, fmt.Errorf("lossless normalization does not support video side data")
			}
			rate := stream.AvgFrameRate
			rat, ok := new(big.Rat).SetString(rate)
			if !ok || rat.Sign() <= 0 {
				rate = stream.RFrameRate
				rat, ok = new(big.Rat).SetString(rate)
			}
			if !ok || rat.Sign() <= 0 || !rat.Num().IsInt64() || !rat.Denom().IsInt64() || stream.Width <= 0 || stream.Height <= 0 || strings.TrimSpace(stream.PixelFormat) == "" || strings.TrimSpace(stream.SampleAspectRatio) == "" {
				return losslessVideoLayout{}, false, fmt.Errorf("invalid native video layout")
			}
			layout = losslessVideoLayout{Width: stream.Width, Height: stream.Height, PixelFormat: stream.PixelFormat, FrameRate: rat.RatString(), RateNum: rat.Num().Int64(), RateDen: rat.Denom().Int64(), SampleAspectRatio: stream.SampleAspectRatio, ColorRange: stream.ColorRange, ColorSpace: stream.ColorSpace, ColorTransfer: stream.ColorTransfer, ColorPrimaries: stream.ColorPrimaries, ChromaLocation: stream.ChromaLocation, FieldOrder: stream.FieldOrder}
		}
	}
	if videoCount != 1 {
		return losslessVideoLayout{}, false, fmt.Errorf("lossless normalization requires exactly one video stream")
	}
	return layout, hasAudio, nil
}

func copyAndVerifySingleton(ctx context.Context, source LocalSource, scratchDir string) (BuiltOutput, error) {
	in, err := os.Open(source.Path)
	if err != nil {
		return BuiltOutput{}, err
	}
	defer in.Close()
	out, err := os.CreateTemp(scratchDir, "joined-*.mp4")
	if err != nil {
		return BuiltOutput{}, err
	}
	outputPath := out.Name()
	keep := false
	defer func() {
		_ = out.Close()
		if !keep {
			_ = os.Remove(outputPath)
		}
	}()
	written, copyErr := copyWithContext(ctx, out, in)
	if copyErr != nil {
		return BuiltOutput{}, copyErr
	}
	if written != source.SizeBytes {
		return BuiltOutput{}, fmt.Errorf("singleton copy size differs")
	}
	if err := out.Sync(); err != nil {
		return BuiltOutput{}, err
	}
	if err := out.Close(); err != nil {
		return BuiltOutput{}, err
	}
	verification, err := VerifyJoinedMedia(ctx, []LocalSource{source}, outputPath)
	if err != nil {
		return BuiltOutput{}, err
	}
	size, sha, err := localIdentity(outputPath)
	if err != nil || size != source.SizeBytes || sha != source.SHA256 {
		return BuiltOutput{}, fmt.Errorf("singleton exact-byte copy identity differs")
	}
	keep = true
	return BuiltOutput{Path: outputPath, SizeBytes: size, SHA256: sha, SourceCount: 1, Verification: verification}, nil
}

func VerifyJoinedMedia(ctx context.Context, sources []LocalSource, outputPath string) (Verification, error) {
	expectedAccumulator := newMediaAccumulator()
	for sourceIndex, source := range sources {
		if err := ctx.Err(); err != nil {
			return Verification{}, fmt.Errorf("probe source ordinal=%d clip_id=%d: %w", sourceIndex+1, source.ClipID, err)
		}
		if err := probeMediaInto(ctx, source.Path, expectedAccumulator, source.AudioContract, true); err != nil {
			if contextErr := ctx.Err(); contextErr != nil {
				return Verification{}, fmt.Errorf("probe source ordinal=%d clip_id=%d: %w", sourceIndex+1, source.ClipID, contextErr)
			}
			classified := deterministicEvidenceFailure(ctx, "corrupt_source_media", err)
			return Verification{}, fmt.Errorf("probe source ordinal=%d clip_id=%d: %w", sourceIndex+1, source.ClipID, classified)
		}
	}
	expected := expectedAccumulator.fingerprint()
	actual, err := probeMedia(ctx, outputPath)
	if err != nil {
		return Verification{}, deterministicEvidenceFailure(ctx, "joined_output_probe_failure", fmt.Errorf("probe joined output: %w", err))
	}
	verification := Verification{Status: "failed", SourceFingerprint: expected, OutputFingerprint: actual}
	if strictErr := compareFingerprints(expected, actual); strictErr != nil {
		decodedVideoSHA, relaxedErr := compareDecodedEquivalent(ctx, sources, outputPath, expected, actual)
		if relaxedErr != nil {
			sourcePaths := make([]string, len(sources))
			for i := range sources {
				sourcePaths[i] = sources[i].Path
			}
			wantFrames, wantSHA, wantErr := decodedVideoSequenceIdentity(ctx, sourcePaths)
			gotFrames, gotSHA, gotErr := decodedVideoSequenceIdentity(ctx, []string{outputPath})
			wantVideo, gotVideo := expected.Tracks["video"], actual.Tracks["video"]
			if wantErr != nil || gotErr != nil || wantVideo == nil || gotVideo == nil || wantFrames != wantVideo.DecodedFrames || gotFrames != gotVideo.DecodedFrames || !lowerHex64(wantSHA) || !lowerHex64(gotSHA) {
				return verification, fmt.Errorf("bind rejected stream-copy decoded evidence: source=%v output=%v", wantErr, gotErr)
			}
			verification.SourceFingerprint.DecodedVideoSHA256 = wantSHA
			verification.OutputFingerprint.DecodedVideoSHA256 = gotSHA
			verification.DecodedFrameSequenceStatus = "failed"
			return verification, deterministicFailure("media_sequence_mismatch", verification, fmt.Errorf("strict=%v; decoded_equivalent=%v", strictErr, relaxedErr))
		}
		verification.AcceptanceMode = "decoded_frame_equivalent"
		verification.DecodedFrameSequenceStatus = "passed"
		verification.SourceFingerprint.DecodedVideoSHA256 = decodedVideoSHA
		verification.OutputFingerprint.DecodedVideoSHA256 = decodedVideoSHA
	}
	verification.PacketPayloadOrderStatus = "passed"
	verification.DecodedFrameTotalsStatus = "passed"
	verification.DecodedAudioTotalsStatus = "passed"
	verification.OutputTimestampStatus = "passed"
	if err := runBounded(ctx, ffmpegBinary(), "-nostdin", "-v", "error", "-xerror", "-err_detect", "explode", "-i", outputPath, "-map", "0:v:0", "-map", "0:a?", "-f", "null", "-"); err != nil {
		classified := deterministicCommandFailure(ctx, "strict_decode_failure", err)
		if classified == err {
			return verification, fmt.Errorf("strict joined decode infrastructure: %w", err)
		}
		return verification, classified
	}
	verification.StrictDecodeStatus = "passed"
	verification.Status = "passed"
	return verification, nil
}

func compareDecodedEquivalent(ctx context.Context, sources []LocalSource, outputPath string, expected, actual MediaFingerprint) (string, error) {
	if len(expected.Tracks) != len(actual.Tracks) || actual.DurationSeconds <= 0 || math.Abs(actual.DurationSeconds-expected.DurationSeconds) > 2 {
		return "", fmt.Errorf("joined duration or stream cardinality mismatch")
	}
	for mediaType, want := range expected.Tracks {
		got := actual.Tracks[mediaType]
		if got == nil || want.TimestampStatus != "source_clips_independent" || got.TimestampStatus != "monotonic" || want.PacketCount != got.PacketCount || want.PacketChainSHA256 != got.PacketChainSHA256 || want.DecodedFrames <= 0 || want.DecodedFrames != got.DecodedFrames || (mediaType == "audio" && want.DecodedSamples != got.DecodedSamples) {
			return "", fmt.Errorf("joined %s packet payload, decoded totals, or timeline mismatch", mediaType)
		}
	}
	if expected.EffectiveAudioBytes != actual.EffectiveAudioBytes || expected.EffectiveAudioFrames != actual.EffectiveAudioFrames || expected.EffectiveAudioSHA256 != actual.EffectiveAudioSHA256 {
		return "", fmt.Errorf("joined effective decoded audio sequence mismatch")
	}
	if len(expected.AudioContracts) > 0 && (len(actual.AudioContracts) != 1 || !sameAudioFormat(expected.AudioContracts[0], actual.AudioContracts[0])) {
		return "", fmt.Errorf("joined effective audio format mismatch")
	}
	sourcePaths := make([]string, len(sources))
	for i := range sources {
		sourcePaths[i] = sources[i].Path
	}
	wantFrames, wantSHA, err := decodedVideoSequenceIdentity(ctx, sourcePaths)
	if err != nil {
		return "", fmt.Errorf("decode source frame sequence: %w", err)
	}
	gotFrames, gotSHA, err := decodedVideoSequenceIdentity(ctx, []string{outputPath})
	if err != nil {
		return "", fmt.Errorf("decode joined frame sequence: %w", err)
	}
	if wantFrames <= 0 || wantFrames != gotFrames || wantSHA != gotSHA {
		return "", fmt.Errorf("joined decoded video frame sequence mismatch")
	}
	return wantSHA, nil
}

func decodedVideoSequenceIdentity(ctx context.Context, mediaPaths []string) (int64, string, error) {
	sequence := sha256.New()
	var frames int64
	for _, mediaPath := range mediaPaths {
		process := newBoundedMediaProcess(ctx, ffmpegBinary(), "-nostdin", "-v", "error", "-xerror", "-err_detect", "explode", "-i", mediaPath, "-map", "0:v:0", "-an", "-sn", "-dn", "-f", "framemd5", "-hash", "sha256", "-")
		stdout, err := process.cmd.StdoutPipe()
		if err != nil {
			return 0, "", err
		}
		var stderr limitedOutput
		process.cmd.Stderr = &stderr
		if err := process.Start(); err != nil {
			return 0, "", err
		}
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 4096), 1<<20)
		for scanner.Scan() {
			if err := ctx.Err(); err != nil {
				_ = process.Kill()
				_ = process.Wait()
				return 0, "", err
			}
			line := strings.TrimSpace(scanner.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			fields := strings.Split(line, ",")
			if len(fields) != 6 {
				_ = process.Kill()
				_ = process.Wait()
				return 0, "", fmt.Errorf("invalid framemd5 evidence")
			}
			duration := strings.TrimSpace(fields[3])
			size := strings.TrimSpace(fields[4])
			frameSHA := strings.ToLower(strings.TrimSpace(fields[5]))
			if _, err := strconv.ParseInt(duration, 10, 64); err != nil {
				return 0, "", fmt.Errorf("invalid decoded frame duration")
			}
			if _, err := strconv.ParseInt(size, 10, 64); err != nil || !lowerHex64(frameSHA) {
				return 0, "", fmt.Errorf("invalid decoded frame identity")
			}
			_, _ = fmt.Fprintf(sequence, "%s|%s|%s\n", duration, size, frameSHA)
			frames++
		}
		if err := scanner.Err(); err != nil {
			_ = process.Kill()
			_ = process.Wait()
			return 0, "", err
		}
		if err := process.Wait(); err != nil {
			return 0, "", fmt.Errorf("framemd5 decode: %w (%s)", err, stderr.String())
		}
	}
	return frames, hex.EncodeToString(sequence.Sum(nil)), nil
}

func compareFingerprints(expected, actual MediaFingerprint) error {
	if len(expected.Tracks) != len(actual.Tracks) || actual.DurationSeconds <= 0 || math.Abs(actual.DurationSeconds-expected.DurationSeconds) > 2 {
		return fmt.Errorf("joined duration or stream cardinality mismatch")
	}
	for mediaType, want := range expected.Tracks {
		got := actual.Tracks[mediaType]
		if got == nil || want.TimestampStatus != "source_clips_independent" || got.TimestampStatus != "monotonic" || !sameStringSequence(want.PacketTimeBases, got.PacketTimeBases) || want.PacketDurationSeconds == "" || got.PacketDurationSeconds == "" || want.PacketDurationSeconds != got.PacketDurationSeconds || got.DecodeTimelineSpanSeconds != got.PacketDurationSeconds || want.FirstPacketPTSSeconds != got.FirstPacketPTSSeconds || want.LastPacketPTSSeconds != got.LastPacketPTSSeconds || want.FirstPacketDTSSeconds != got.FirstPacketDTSSeconds || want.LastPacketDTSSeconds != got.LastPacketDTSSeconds || want.PacketCount != got.PacketCount || want.PacketChainSHA256 != got.PacketChainSHA256 || want.PacketTimingSHA256 != got.PacketTimingSHA256 || want.DecodedFrames != got.DecodedFrames || (mediaType == "audio" && want.DecodedSamples != got.DecodedSamples) {
			return fmt.Errorf("joined %s packet/frame/timestamp mismatch", mediaType)
		}
	}
	if expected.EffectiveAudioBytes != actual.EffectiveAudioBytes || expected.EffectiveAudioFrames != actual.EffectiveAudioFrames || expected.EffectiveAudioSHA256 != actual.EffectiveAudioSHA256 {
		return fmt.Errorf("joined effective decoded audio sequence mismatch")
	}
	if len(expected.AudioContracts) > 0 {
		if len(actual.AudioContracts) != 1 || !sameAudioFormat(expected.AudioContracts[0], actual.AudioContracts[0]) {
			return fmt.Errorf("joined effective audio format mismatch")
		}
	}
	return nil
}

func sameStringSequence(a, b []string) bool {
	if len(a) == 0 || len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type trackAccumulator struct {
	fingerprint        TrackFingerprint
	packetHash         hash.Hash
	timingHash         hash.Hash
	packetDuration     *big.Rat
	lastNormalizedDTS  *big.Rat
	lastPacketDuration *big.Rat
}

type mediaAccumulator struct {
	duration       float64
	tracks         map[string]*trackAccumulator
	audioContracts []AudioSequenceContract
	effectiveAudio hash.Hash
	effectiveBytes int64
}

type probedStream struct {
	MediaType   string
	TimeBase    string
	TimeBaseNum int64
	TimeBaseDen int64
}

func newMediaAccumulator() *mediaAccumulator {
	return &mediaAccumulator{tracks: map[string]*trackAccumulator{}, effectiveAudio: sha256.New()}
}

func (a *mediaAccumulator) fingerprint() MediaFingerprint {
	out := MediaFingerprint{DurationSeconds: a.duration, Tracks: map[string]*TrackFingerprint{}, AudioContracts: append([]AudioSequenceContract(nil), a.audioContracts...), EffectiveAudioBytes: a.effectiveBytes}
	for mediaType, track := range a.tracks {
		copy := track.fingerprint
		copy.PacketChainSHA256 = hex.EncodeToString(track.packetHash.Sum(nil))
		copy.PacketTimingSHA256 = hex.EncodeToString(track.timingHash.Sum(nil))
		if track.packetDuration != nil {
			copy.PacketDurationSeconds = track.packetDuration.RatString()
		}
		if track.lastNormalizedDTS != nil && track.lastPacketDuration != nil {
			span := new(big.Rat).Add(track.lastNormalizedDTS, track.lastPacketDuration)
			copy.DecodeTimelineSpanSeconds = span.RatString()
		}
		out.Tracks[mediaType] = &copy
	}
	if a.effectiveBytes > 0 {
		out.EffectiveAudioSHA256 = hex.EncodeToString(a.effectiveAudio.Sum(nil))
		if len(a.audioContracts) > 0 && a.audioContracts[0].Channels > 0 {
			out.EffectiveAudioFrames = a.effectiveBytes / int64(4*a.audioContracts[0].Channels)
		}
	}
	return out
}

func probeMedia(ctx context.Context, mediaPath string) (MediaFingerprint, error) {
	accumulator := newMediaAccumulator()
	if err := probeMediaInto(ctx, mediaPath, accumulator, nil, false); err != nil {
		return MediaFingerprint{}, err
	}
	return accumulator.fingerprint(), nil
}

func probeMediaInto(ctx context.Context, mediaPath string, accumulator *mediaAccumulator, expectedAudio *AudioSequenceContract, enforceAudioContract bool) error {
	duration, indexType, observedAudio, err := probeMediaMetadata(ctx, mediaPath)
	if err != nil {
		return err
	}
	if enforceAudioContract && ((expectedAudio == nil) != (observedAudio == nil) || (expectedAudio != nil && !sameObservableAudioContract(*expectedAudio, *observedAudio))) {
		return fmt.Errorf("frozen audio sequence contract differs from exact source bytes")
	}
	if observedAudio != nil {
		if len(accumulator.audioContracts) > 0 && !sameAudioFormat(accumulator.audioContracts[0], *observedAudio) {
			return fmt.Errorf("audio format changes within output")
		}
		contract := *observedAudio
		if expectedAudio != nil {
			contract = *expectedAudio
		}
		accumulator.audioContracts = append(accumulator.audioContracts, contract)
	}
	for _, stream := range indexType {
		mediaType := stream.MediaType
		if accumulator.tracks[mediaType] == nil {
			accumulator.tracks[mediaType] = &trackAccumulator{fingerprint: TrackFingerprint{MediaType: mediaType, PacketTimeBases: []string{}}, packetHash: sha256.New(), timingHash: sha256.New(), packetDuration: new(big.Rat)}
		}
		track := accumulator.tracks[mediaType]
		if len(track.fingerprint.PacketTimeBases) == 0 || track.fingerprint.PacketTimeBases[len(track.fingerprint.PacketTimeBases)-1] != stream.TimeBase {
			track.fingerprint.PacketTimeBases = append(track.fingerprint.PacketTimeBases, stream.TimeBase)
		}
	}
	accumulator.duration += duration
	process := newBoundedMediaProcess(ctx, ffprobeBinary(), "-v", "error", "-err_detect", "explode", "-show_packets", "-show_frames", "-show_data_hash", "sha256", "-show_entries", "packet=stream_index,pts,dts,duration,data_hash:frame=stream_index,media_type,best_effort_timestamp,nb_samples", "-of", "compact=p=1:nk=0", mediaPath)
	cmd := process.cmd
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	var stderr limitedOutput
	cmd.Stderr = &stderr
	if err := process.Start(); err != nil {
		return err
	}
	timestampStatus := "monotonic"
	if enforceAudioContract {
		timestampStatus = "source_clips_independent"
	}
	consumeErr := consumeCompactEvidenceContext(ctx, stdout, indexType, accumulator, timestampStatus)
	if consumeErr != nil {
		_ = process.Kill()
	}
	waitErr := process.Wait()
	if consumeErr != nil {
		return consumeErr
	}
	if waitErr != nil {
		return fmt.Errorf("ffprobe evidence: %w (%s)", waitErr, stderr.String())
	}
	if observedAudio != nil {
		if err := hashEffectiveAudio(ctx, mediaPath, accumulator); err != nil {
			return err
		}
	}
	return nil
}

func probeMediaMetadata(ctx context.Context, mediaPath string) (float64, map[int]probedStream, *AudioSequenceContract, error) {
	process := newBoundedMediaProcess(ctx, ffprobeBinary(), "-v", "error", "-show_streams", "-show_format", "-show_entries", "format=duration:stream=index,codec_type,codec_name,sample_rate,channels,channel_layout,initial_padding,codec_delay,trailing_padding,start_pts,start_time,duration_ts,duration,time_base", "-of", "json", mediaPath)
	cmd := process.cmd
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return 0, nil, nil, err
	}
	var stderr limitedOutput
	cmd.Stderr = &stderr
	if err := process.Start(); err != nil {
		return 0, nil, nil, err
	}
	out, readErr := io.ReadAll(io.LimitReader(stdout, mediaCommandOutputLimit+1))
	tooLarge := len(out) > mediaCommandOutputLimit
	if tooLarge || readErr != nil {
		_ = process.Kill()
	}
	waitErr := process.Wait()
	if readErr != nil {
		return 0, nil, nil, readErr
	}
	if tooLarge {
		return 0, nil, nil, fmt.Errorf("ffprobe metadata exceeds bounded output")
	}
	if waitErr != nil {
		return 0, nil, nil, fmt.Errorf("ffprobe metadata: %w (%s)", waitErr, stderr.String())
	}
	var payload struct {
		Format struct {
			Duration string `json:"duration"`
		} `json:"format"`
		Streams []struct {
			Index           int    `json:"index"`
			CodecType       string `json:"codec_type"`
			CodecName       string `json:"codec_name"`
			SampleRate      string `json:"sample_rate"`
			Channels        int    `json:"channels"`
			ChannelLayout   string `json:"channel_layout"`
			InitialPadding  int64  `json:"initial_padding"`
			CodecDelay      int64  `json:"codec_delay"`
			TrailingPadding int64  `json:"trailing_padding"`
			StartPTS        int64  `json:"start_pts"`
			StartTime       string `json:"start_time"`
			DurationTS      int64  `json:"duration_ts"`
			Duration        string `json:"duration"`
			TimeBase        string `json:"time_base"`
		} `json:"streams"`
	}
	if err := json.Unmarshal(out, &payload); err != nil {
		return 0, nil, nil, err
	}
	duration, err := strconv.ParseFloat(payload.Format.Duration, 64)
	if err != nil || duration <= 0 {
		return 0, nil, nil, fmt.Errorf("invalid media duration")
	}
	indexType := map[int]probedStream{}
	seenType := map[string]bool{}
	var audio *AudioSequenceContract
	for _, stream := range payload.Streams {
		mediaType := strings.TrimSpace(stream.CodecType)
		if mediaType != "video" && mediaType != "audio" {
			continue
		}
		if seenType[mediaType] {
			return 0, nil, nil, fmt.Errorf("multiple %s streams are unsupported", mediaType)
		}
		seenType[mediaType] = true
		timeBaseNum, timeBaseDen, timeBaseErr := parseTimeBase(stream.TimeBase)
		if timeBaseErr != nil {
			return 0, nil, nil, timeBaseErr
		}
		indexType[stream.Index] = probedStream{MediaType: mediaType, TimeBase: stream.TimeBase, TimeBaseNum: timeBaseNum, TimeBaseDen: timeBaseDen}
		if mediaType == "audio" {
			sampleRate, parseErr := strconv.ParseInt(stream.SampleRate, 10, 64)
			if parseErr != nil {
				return 0, nil, nil, fmt.Errorf("invalid audio sample rate")
			}
			timeline := struct {
				StartPTS   int64  `json:"start_pts"`
				StartTime  string `json:"start_time"`
				DurationTS int64  `json:"duration_ts"`
				Duration   string `json:"duration"`
				TimeBase   string `json:"time_base"`
			}{stream.StartPTS, stream.StartTime, stream.DurationTS, stream.Duration, stream.TimeBase}
			timelineJSON, marshalErr := json.Marshal(timeline)
			if marshalErr != nil {
				return 0, nil, nil, marshalErr
			}
			timelineSHA := sha256.Sum256(timelineJSON)
			audio = &AudioSequenceContract{CodecName: stream.CodecName, SampleRate: sampleRate, Channels: stream.Channels, ChannelLayout: stream.ChannelLayout, InitialPadding: stream.InitialPadding, CodecDelay: stream.CodecDelay, TrailingPadding: stream.TrailingPadding, EditListKind: "decoder_timeline_v1", EditListSHA256: hex.EncodeToString(timelineSHA[:])}
		}
	}
	if !seenType["video"] {
		return 0, nil, nil, fmt.Errorf("video stream missing")
	}
	if audio != nil {
		skip, discard, sideErr := probeAudioTrimSideData(ctx, mediaPath)
		if sideErr != nil {
			return 0, nil, nil, sideErr
		}
		audio.SkipSamples, audio.DiscardPadding = skip, discard
		if err := validateAudioContract(*audio); err != nil {
			return 0, nil, nil, err
		}
	}
	return duration, indexType, audio, nil
}

func sameObservableAudioContract(a, b AudioSequenceContract) bool {
	return a.CodecName == b.CodecName && a.SampleRate == b.SampleRate && a.Channels == b.Channels && a.ChannelLayout == b.ChannelLayout && a.InitialPadding == b.InitialPadding && a.SkipSamples == b.SkipSamples && a.DiscardPadding == b.DiscardPadding && a.CodecDelay == b.CodecDelay && a.TrailingPadding == b.TrailingPadding && a.EditListKind == b.EditListKind && a.EditListSHA256 == b.EditListSHA256
}

func sameAudioFormat(a, b AudioSequenceContract) bool {
	return a.CodecName == b.CodecName && a.SampleRate == b.SampleRate && a.Channels == b.Channels && a.ChannelLayout == b.ChannelLayout
}

func probeAudioTrimSideData(ctx context.Context, mediaPath string) (int64, int64, error) {
	process := newBoundedMediaProcess(ctx, ffprobeBinary(), "-v", "error", "-select_streams", "a:0", "-show_packets", "-show_entries", "packet=side_data_list", "-of", "compact=p=1:nk=0", mediaPath)
	cmd := process.cmd
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return 0, 0, err
	}
	var stderr limitedOutput
	cmd.Stderr = &stderr
	if err := process.Start(); err != nil {
		return 0, 0, err
	}
	var skip, discard int64
	var parseErr error
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			_ = process.Kill()
			_ = process.Wait()
			return 0, 0, err
		}
		for _, field := range strings.Split(scanner.Text(), "|") {
			key, value, ok := strings.Cut(field, "=")
			if !ok {
				continue
			}
			var target *int64
			switch {
			case strings.HasSuffix(key, ":skip_samples"):
				target = &skip
			case strings.HasSuffix(key, ":discard_padding"):
				target = &discard
			default:
				continue
			}
			parsed, err := strconv.ParseInt(value, 10, 64)
			if err != nil || parsed < 0 || *target > math.MaxInt64-parsed {
				parseErr = fmt.Errorf("invalid audio trim side data")
				continue
			}
			*target += parsed
		}
	}
	if err := scanner.Err(); err != nil {
		_ = process.Kill()
		_ = process.Wait()
		return 0, 0, err
	}
	if err := process.Wait(); err != nil {
		return 0, 0, fmt.Errorf("ffprobe audio trim: %w (%s)", err, stderr.String())
	}
	if parseErr != nil {
		return 0, 0, parseErr
	}
	return skip, discard, nil
}

func hashEffectiveAudio(ctx context.Context, mediaPath string, accumulator *mediaAccumulator) error {
	process := newBoundedMediaProcess(ctx, ffmpegBinary(), "-nostdin", "-v", "error", "-xerror", "-err_detect", "explode", "-i", mediaPath, "-map", "0:a:0", "-vn", "-sn", "-dn", "-acodec", "pcm_s32le", "-f", "s32le", "-")
	cmd := process.cmd
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	var stderr limitedOutput
	cmd.Stderr = &stderr
	if err := process.Start(); err != nil {
		return err
	}
	n, copyErr := copyWithContext(ctx, accumulator.effectiveAudio, stdout)
	if copyErr != nil {
		_ = process.Kill()
	}
	waitErr := process.Wait()
	if copyErr != nil {
		return fmt.Errorf("hash effective decoded audio: %w", copyErr)
	}
	if waitErr != nil {
		return fmt.Errorf("decode effective audio: %w (%s)", waitErr, stderr.String())
	}
	if n <= 0 || accumulator.effectiveBytes > math.MaxInt64-n {
		return fmt.Errorf("invalid effective decoded audio length")
	}
	accumulator.effectiveBytes += n
	return nil
}

func consumeCompactEvidence(reader io.Reader, indexType map[int]probedStream, accumulator *mediaAccumulator, timestampStatus string) error {
	return consumeCompactEvidenceContext(context.Background(), reader, indexType, accumulator, timestampStatus)
}

func consumeCompactEvidenceContext(ctx context.Context, reader io.Reader, indexType map[int]probedStream, accumulator *mediaAccumulator, timestampStatus string) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	localFrameSeen := map[string]bool{}
	localLastFrame := map[string]int64{}
	localPacketSeen := map[string]bool{}
	localLastDTS := map[string]int64{}
	localFirstDTSSeconds := map[string]*big.Rat{}
	localTimelineBase := map[string]*big.Rat{}
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		line := scanner.Text()
		fields := strings.Split(line, "|")
		if len(fields) < 2 {
			continue
		}
		values := map[string]string{}
		for _, field := range fields[1:] {
			key, value, ok := strings.Cut(field, "=")
			if ok {
				values[key] = value
			}
		}
		streamIndex, err := strconv.Atoi(values["stream_index"])
		stream, ok := indexType[streamIndex]
		if err != nil || !ok || stream.MediaType == "" {
			return fmt.Errorf("evidence has unknown stream")
		}
		mediaType := stream.MediaType
		track := accumulator.tracks[mediaType]
		switch fields[0] {
		case "packet":
			packetSHA := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(values["data_hash"])), "sha256:")
			if !lowerHex64(packetSHA) {
				return fmt.Errorf("missing packet payload hash")
			}
			pts, ptsErr := strconv.ParseInt(values["pts"], 10, 64)
			dts, dtsErr := strconv.ParseInt(values["dts"], 10, 64)
			duration, durationErr := strconv.ParseInt(values["duration"], 10, 64)
			if ptsErr != nil || dtsErr != nil || durationErr != nil || duration < 0 {
				return fmt.Errorf("missing exact packet timestamp evidence")
			}
			// Decode timestamps define packet processing order. Presentation
			// timestamps may legitimately reorder for B-frames, but both must be
			// present and the decode timeline must advance.
			if localPacketSeen[mediaType] && dts <= localLastDTS[mediaType] {
				return fmt.Errorf("nonmonotonic packet timestamp for %s", mediaType)
			}
			ptsSeconds := ticksToRational(pts, stream.TimeBaseNum, stream.TimeBaseDen)
			dtsSeconds := ticksToRational(dts, stream.TimeBaseNum, stream.TimeBaseDen)
			durationSeconds := ticksToRational(duration, stream.TimeBaseNum, stream.TimeBaseDen)
			if track.packetDuration == nil {
				track.packetDuration = new(big.Rat)
			}
			if !localPacketSeen[mediaType] {
				localFirstDTSSeconds[mediaType] = new(big.Rat).Set(dtsSeconds)
				localTimelineBase[mediaType] = new(big.Rat).Set(track.packetDuration)
			}
			normalizedDTS := new(big.Rat).Add(localTimelineBase[mediaType], new(big.Rat).Sub(dtsSeconds, localFirstDTSSeconds[mediaType]))
			normalizedPTS := new(big.Rat).Add(localTimelineBase[mediaType], new(big.Rat).Sub(ptsSeconds, localFirstDTSSeconds[mediaType]))
			if track.fingerprint.PacketCount == 0 {
				track.fingerprint.FirstPacketPTSSeconds, track.fingerprint.FirstPacketDTSSeconds = normalizedPTS.RatString(), normalizedDTS.RatString()
			}
			localPacketSeen[mediaType], localLastDTS[mediaType] = true, dts
			track.fingerprint.LastPacketPTSSeconds, track.fingerprint.LastPacketDTSSeconds = normalizedPTS.RatString(), normalizedDTS.RatString()
			track.lastNormalizedDTS, track.lastPacketDuration = new(big.Rat).Set(normalizedDTS), new(big.Rat).Set(durationSeconds)
			track.packetDuration.Add(track.packetDuration, durationSeconds)
			_, _ = io.WriteString(track.packetHash, packetSHA+"\n")
			_, _ = fmt.Fprintf(track.timingHash, "%s|%s|%s\n", normalizedPTS.RatString(), normalizedDTS.RatString(), durationSeconds.RatString())
			track.fingerprint.PacketCount++
		case "frame":
			if values["media_type"] != mediaType {
				return fmt.Errorf("decoded frame stream type differs")
			}
			pts, err := strconv.ParseInt(values["best_effort_timestamp"], 10, 64)
			if err != nil {
				return fmt.Errorf("missing decoded %s timestamp", mediaType)
			}
			if localFrameSeen[mediaType] && pts <= localLastFrame[mediaType] {
				return fmt.Errorf("nonmonotonic decoded %s timestamp", mediaType)
			}
			if !localFrameSeen[mediaType] && track.fingerprint.DecodedFrames == 0 {
				track.fingerprint.FirstTimestamp = pts
			}
			localFrameSeen[mediaType] = true
			localLastFrame[mediaType] = pts
			track.fingerprint.LastTimestamp = pts
			track.fingerprint.DecodedFrames++
			if mediaType == "audio" {
				samples, err := strconv.ParseInt(values["nb_samples"], 10, 64)
				if err != nil || samples <= 0 {
					return fmt.Errorf("missing decoded audio samples")
				}
				track.fingerprint.DecodedSamples += samples
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	for _, stream := range indexType {
		mediaType := stream.MediaType
		track := accumulator.tracks[mediaType]
		if !localFrameSeen[mediaType] || !localPacketSeen[mediaType] || track.fingerprint.PacketCount == 0 {
			return fmt.Errorf("incomplete %s packet/frame evidence", mediaType)
		}
		track.fingerprint.TimestampStatus = timestampStatus
	}
	return nil
}

func copyWithContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	buffer := make([]byte, 128<<10)
	var written int64
	for {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		n, readErr := src.Read(buffer)
		if n > 0 {
			count, writeErr := dst.Write(buffer[:n])
			written += int64(count)
			if writeErr != nil {
				return written, writeErr
			}
			if count != n {
				return written, io.ErrShortWrite
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return written, nil
			}
			return written, readErr
		}
	}
}

func parseTimeBase(raw string) (int64, int64, error) {
	numeratorRaw, denominatorRaw, ok := strings.Cut(raw, "/")
	if !ok {
		return 0, 0, fmt.Errorf("invalid packet time base")
	}
	numerator, numeratorErr := strconv.ParseInt(numeratorRaw, 10, 64)
	denominator, denominatorErr := strconv.ParseInt(denominatorRaw, 10, 64)
	if numeratorErr != nil || denominatorErr != nil || numerator <= 0 || denominator <= 0 {
		return 0, 0, fmt.Errorf("invalid packet time base")
	}
	return numerator, denominator, nil
}

func ticksToRational(ticks, numerator, denominator int64) *big.Rat {
	n := new(big.Int).Mul(big.NewInt(ticks), big.NewInt(numerator))
	return new(big.Rat).SetFrac(n, big.NewInt(denominator))
}

func verifyLocalIdentity(source LocalSource) error {
	if source.ClipID <= 0 || source.SizeBytes <= 0 || !lowerHex64(source.SHA256) || !lowerHex64(source.SourceClaimSHA256) {
		return fmt.Errorf("invalid local source identity")
	}
	size, sha, err := localIdentity(source.Path)
	if err != nil {
		return err
	}
	if size != source.SizeBytes || sha != source.SHA256 {
		return fmt.Errorf("local source %d identity mismatch", source.ClipID)
	}
	return nil
}

func localIdentity(path string) (int64, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 {
		return 0, "", fmt.Errorf("invalid local media file")
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return 0, "", err
	}
	return info.Size(), hex.EncodeToString(h.Sum(nil)), nil
}

type limitedOutput struct {
	buf bytes.Buffer
}

func (w *limitedOutput) Write(p []byte) (int, error) {
	written := len(p)
	remaining := mediaCommandOutputLimit - w.buf.Len()
	if remaining > 0 {
		if len(p) > remaining {
			p = p[:remaining]
		}
		_, _ = w.buf.Write(p)
	}
	return written, nil
}

func (w *limitedOutput) String() string { return strings.TrimSpace(w.buf.String()) }

type boundedCommandError struct {
	cause  error
	output string
}

func (e *boundedCommandError) Error() string { return fmt.Sprintf("%v (%s)", e.cause, e.output) }
func (e *boundedCommandError) Unwrap() error { return e.cause }

func runBounded(ctx context.Context, name string, args ...string) error {
	process := newBoundedMediaProcess(ctx, name, args...)
	cmd := process.cmd
	var output limitedOutput
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := process.Start(); err != nil {
		return &boundedCommandError{cause: err, output: output.String()}
	}
	if err := process.Wait(); err != nil {
		return &boundedCommandError{cause: err, output: output.String()}
	}
	return nil
}

func ffmpegBinary() string {
	if value := strings.TrimSpace(os.Getenv("FFMPEG_BIN")); value != "" {
		return value
	}
	return "ffmpeg"
}

func ffprobeBinary() string {
	if value := strings.TrimSpace(os.Getenv("FFPROBE_BIN")); value != "" {
		return value
	}
	return "ffprobe"
}

func SafeScratchOutput(path, scratchDir string) bool {
	absPath, errPath := filepath.Abs(path)
	absScratch, errScratch := filepath.Abs(scratchDir)
	if errPath != nil || errScratch != nil {
		return false
	}
	rel, err := filepath.Rel(absScratch, absPath)
	return err == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
