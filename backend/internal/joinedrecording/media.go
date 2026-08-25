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
	cmd := exec.CommandContext(ctx, binary, "-version")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", err
	}
	var stderr limitedOutput
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return "", err
	}
	out, readErr := io.ReadAll(io.LimitReader(stdout, mediaCommandOutputLimit+1))
	tooLarge := len(out) > mediaCommandOutputLimit
	if tooLarge && cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
	waitErr := cmd.Wait()
	if readErr != nil {
		return "", fmt.Errorf("read media tool version: %w", readErr)
	}
	if tooLarge {
		return "", fmt.Errorf("media tool version exceeds bounded output")
	}
	line := strings.TrimSpace(strings.SplitN(string(out), "\n", 2)[0])
	if waitErr != nil || line == "" {
		return "", fmt.Errorf("read media tool version: %v (%s)", waitErr, stderr.String())
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
	AudioContracts       []AudioSequenceContract      `json:"audio_sequence_contracts,omitempty"`
	EffectiveAudioBytes  int64                        `json:"effective_audio_bytes,omitempty"`
	EffectiveAudioFrames int64                        `json:"effective_audio_sample_frames,omitempty"`
	EffectiveAudioSHA256 string                       `json:"effective_audio_sha256,omitempty"`
}

type Verification struct {
	Status                   string           `json:"status"`
	PacketPayloadOrderStatus string           `json:"packet_payload_order_status"`
	DecodedFrameTotalsStatus string           `json:"decoded_frame_totals_status"`
	DecodedAudioTotalsStatus string           `json:"decoded_audio_totals_status"`
	OutputTimestampStatus    string           `json:"output_timestamp_status"`
	StrictDecodeStatus       string           `json:"strict_decode_status"`
	SourceFingerprint        MediaFingerprint `json:"source_fingerprint"`
	OutputFingerprint        MediaFingerprint `json:"output_fingerprint"`
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
	var lastErr error
	evidence := []MaximalityEvidence{}
	for n := len(sources); n >= 1; n-- {
		built, err := buildIsolatedAttempt(ctx, sources[:n], scratchDir)
		if err == nil {
			built.SplitEvidence = append([]MaximalityEvidence(nil), evidence...)
			return built, nil
		}
		var firstDeterministic *deterministicMediaError
		if !errors.As(err, &firstDeterministic) {
			return BuiltOutput{}, err
		}
		if firstDeterministic.code == "output_exceeds_put_cap" {
			evidence = append(evidence, maximalityEvidence(sources[:n], firstDeterministic, 1, mediaToolIdentity))
			lastErr = err
			continue
		}
		_, repeatErr := buildIsolatedAttempt(ctx, sources[:n], scratchDir)
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
	remaining := sources
	parts := make([]BuiltOutput, 0, 1)
	quarantines := []QuarantinedBuild{}
	for len(remaining) > 0 {
		part, err := BuildLargestPassingPrefix(ctx, remaining, scratchDir, mediaToolIdentity)
		if err != nil {
			var failure *deterministicMediaError
			if !errors.As(err, &failure) || failure.code == "output_exceeds_put_cap" {
				return nil, nil, err
			}
			// BuildLargestPassingPrefix already proved this irreducible source in
			// two fresh isolated attempts before it can reach this branch.
			quarantines = append(quarantines, QuarantinedBuild{Source: remaining[0], Evidence: maximalityEvidence(remaining[:1], failure, 2, mediaToolIdentity)})
			remaining = remaining[1:]
			continue
		}
		if part.SourceCount <= 0 || part.SourceCount > len(remaining) {
			return nil, nil, fmt.Errorf("preflight part made no bounded progress")
		}
		parts = append(parts, part)
		remaining = remaining[part.SourceCount:]
	}
	return parts, quarantines, nil
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
		previousWasQuarantine := false
		for offset < len(candidate.Sources) {
			if quarantine, ok := quarantineIDs[candidate.Sources[offset].ClipID]; ok {
				result.Quarantined = append(result.Quarantined, candidate.Sources[offset])
				result.Quarantines = append(result.Quarantines, quarantine)
				offset++
				previousWasQuarantine = true
				continue
			}
			built := parts[partIndex]
			partSources := append([]SourceClip(nil), candidate.Sources[offset:offset+built.SourceCount]...)
			if previousWasQuarantine {
				partSources[0].SeamToPrevious = SeamEvidence{Verdict: "incompatible", Reason: "source_quarantined", SignedGapNanoseconds: partSources[0].StartUTC.Sub(candidate.Sources[offset-1].EndUTC).Nanoseconds()}
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
			previousWasQuarantine = false
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
	if len(sources) == 0 || strings.TrimSpace(scratchDir) == "" {
		return BuiltOutput{}, fmt.Errorf("bounded sources and scratch directory are required")
	}
	if err := os.MkdirAll(scratchDir, 0700); err != nil {
		return BuiltOutput{}, fmt.Errorf("create scratch: %w", err)
	}
	for _, source := range sources {
		if err := verifyLocalIdentity(source); err != nil {
			return BuiltOutput{}, err
		}
	}
	return buildAndVerify(ctx, sources, scratchDir)
}

func buildAndVerify(ctx context.Context, sources []LocalSource, scratchDir string) (BuiltOutput, error) {
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
	cmd := exec.CommandContext(ctx, ffmpegBinary(), "-nostdin", "-v", "error", "-f", "concat", "-safe", "0", "-i", manifestPath, "-copyts", "-map", "0:v:0", "-map", "0:a?", "-c", "copy", "-movflags", "+faststart", outputPath)
	if err := runBounded(cmd); err != nil {
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
	written, copyErr := io.Copy(out, in)
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
	if err := compareFingerprints(expected, actual); err != nil {
		return verification, deterministicFailure("media_sequence_mismatch", verification, err)
	}
	verification.PacketPayloadOrderStatus = "passed"
	verification.DecodedFrameTotalsStatus = "passed"
	verification.DecodedAudioTotalsStatus = "passed"
	verification.OutputTimestampStatus = "passed"
	decode := exec.CommandContext(ctx, ffmpegBinary(), "-nostdin", "-v", "error", "-xerror", "-err_detect", "explode", "-i", outputPath, "-map", "0:v:0", "-map", "0:a?", "-f", "null", "-")
	if err := runBounded(decode); err != nil {
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
	cmd := exec.CommandContext(ctx, ffprobeBinary(), "-v", "error", "-err_detect", "explode", "-show_packets", "-show_frames", "-show_data_hash", "sha256", "-show_entries", "packet=stream_index,pts,dts,duration,data_hash:frame=stream_index,media_type,best_effort_timestamp,nb_samples", "-of", "compact=p=1:nk=0", mediaPath)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	var stderr limitedOutput
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	timestampStatus := "monotonic"
	if enforceAudioContract {
		timestampStatus = "source_clips_independent"
	}
	consumeErr := consumeCompactEvidence(stdout, indexType, accumulator, timestampStatus)
	waitErr := cmd.Wait()
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
	cmd := exec.CommandContext(ctx, ffprobeBinary(), "-v", "error", "-show_streams", "-show_format", "-show_entries", "format=duration:stream=index,codec_type,codec_name,sample_rate,channels,channel_layout,initial_padding,codec_delay,trailing_padding,start_pts,start_time,duration_ts,duration,time_base", "-of", "json", mediaPath)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return 0, nil, nil, err
	}
	var stderr limitedOutput
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return 0, nil, nil, err
	}
	out, readErr := io.ReadAll(io.LimitReader(stdout, mediaCommandOutputLimit+1))
	tooLarge := len(out) > mediaCommandOutputLimit
	if tooLarge && cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
	waitErr := cmd.Wait()
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
	cmd := exec.CommandContext(ctx, ffprobeBinary(), "-v", "error", "-select_streams", "a:0", "-show_packets", "-show_entries", "packet=side_data_list", "-of", "compact=p=1:nk=0", mediaPath)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return 0, 0, err
	}
	var stderr limitedOutput
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return 0, 0, err
	}
	var skip, discard int64
	var parseErr error
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	for scanner.Scan() {
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
		return 0, 0, err
	}
	if err := cmd.Wait(); err != nil {
		return 0, 0, fmt.Errorf("ffprobe audio trim: %w (%s)", err, stderr.String())
	}
	if parseErr != nil {
		return 0, 0, parseErr
	}
	return skip, discard, nil
}

func hashEffectiveAudio(ctx context.Context, mediaPath string, accumulator *mediaAccumulator) error {
	cmd := exec.CommandContext(ctx, ffmpegBinary(), "-nostdin", "-v", "error", "-xerror", "-err_detect", "explode", "-i", mediaPath, "-map", "0:a:0", "-vn", "-sn", "-dn", "-acodec", "pcm_s32le", "-f", "s32le", "-")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	var stderr limitedOutput
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	n, copyErr := io.Copy(accumulator.effectiveAudio, stdout)
	waitErr := cmd.Wait()
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
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	localFrameSeen := map[string]bool{}
	localLastFrame := map[string]int64{}
	localPacketSeen := map[string]bool{}
	localLastDTS := map[string]int64{}
	localFirstDTSSeconds := map[string]*big.Rat{}
	localTimelineBase := map[string]*big.Rat{}
	for scanner.Scan() {
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

func runBounded(cmd *exec.Cmd) error {
	var output limitedOutput
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Run(); err != nil {
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
