package joinedrecording

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/stitchcert"
	"time"
)

type generatedEvidence struct {
	frames int
	line   int
	active *strings.Reader
}

func (r *generatedEvidence) Read(p []byte) (int, error) {
	for r.active == nil || r.active.Len() == 0 {
		if r.line >= r.frames*2 {
			return 0, io.EOF
		}
		if r.line%2 == 0 {
			packet := r.line / 2
			r.active = strings.NewReader(fmt.Sprintf("packet|stream_index=0|pts=%d|dts=%d|duration=1|data_hash=SHA256:%s\n", packet, packet, strings.Repeat("a", 64)))
		} else {
			r.active = strings.NewReader(fmt.Sprintf("frame|media_type=video|stream_index=0|best_effort_timestamp=%d\n", r.line/2))
		}
		r.line++
	}
	return r.active.Read(p)
}

func makeMediaClip(t *testing.T, dir, name string, tone int, audio bool) LocalSource {
	t.Helper()
	return makeVideoClip(t, dir, name, tone, audio, []string{"-c:v", "mpeg4", "-g", "10", "-bf", "2"})
}

func makeProgressiveLosslessSource(t *testing.T, dir, name string, tone int, audio bool) LocalSource {
	t.Helper()
	return makeVideoClip(t, dir, name, tone, audio, []string{"-c:v", "libx264", "-preset", "veryfast", "-field_order", "progressive"})
}

func makeVideoClip(t *testing.T, dir, name string, tone int, audio bool, videoArgs []string) LocalSource {
	t.Helper()
	if _, err := exec.LookPath(ffmpegBinary()); err != nil {
		t.Skip("ffmpeg unavailable")
	}
	if _, err := exec.LookPath(ffprobeBinary()); err != nil {
		t.Skip("ffprobe unavailable")
	}
	mediaPath := filepath.Join(dir, name)
	args := []string{"-nostdin", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=64x64:rate=10:duration=1"}
	if audio {
		args = append(args, "-f", "lavfi", "-i", "sine=frequency="+strconv.Itoa(tone)+":sample_rate=48000:duration=1", "-map", "0:v:0", "-map", "1:a:0", "-c:a", "aac", "-shortest")
	}
	args = append(args, videoArgs...)
	args = append(args, mediaPath)
	cmd := exec.Command(ffmpegBinary(), args...)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("make fixture: %v (%s)", err, output)
	}
	size, sha, err := localIdentity(mediaPath)
	if err != nil {
		t.Fatal(err)
	}
	local := LocalSource{Path: mediaPath, SizeBytes: size, SHA256: sha, SourceClaimSHA256: sha}
	if audio {
		_, _, contract, err := probeMediaMetadata(context.Background(), mediaPath)
		if err != nil {
			t.Fatal(err)
		}
		local.AudioContract = contract
	}
	return local
}

func TestBuildLargestPassingPrefixPreservesPacketsFramesAndAudio(t *testing.T) {
	dir := t.TempDir()
	first := makeMediaClip(t, dir, "one.mp4", 440, false)
	second := makeMediaClip(t, dir, "two.mp4", 880, false)
	first.ClipID, second.ClipID = 1, 2
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	built, err := BuildLargestPassingPrefix(ctx, []LocalSource{first, second}, dir, strings.Repeat("f", 64))
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(built.Path)
	if built.SourceCount != 2 || built.Verification.Status != "passed" || built.Verification.PacketPayloadOrderStatus != "passed" || built.Verification.DecodedFrameTotalsStatus != "passed" || built.Verification.DecodedAudioTotalsStatus != "passed" {
		t.Fatalf("verification=%+v", built.Verification)
	}
}

func TestVerifyJoinedMediaAllowsDecodedEquivalentTimebaseNormalization(t *testing.T) {
	dir := t.TempDir()
	first := makeMediaClip(t, dir, "one.mp4", 440, false)
	second := makeMediaClip(t, dir, "two.mp4", 880, false)
	manifestPath := filepath.Join(dir, "concat.txt")
	manifest := fmt.Sprintf("file '%s'\nfile '%s'\n", first.Path, second.Path)
	if err := os.WriteFile(manifestPath, []byte(manifest), 0600); err != nil {
		t.Fatal(err)
	}
	outputPath := filepath.Join(dir, "normalized-timebase.mp4")
	cmd := exec.Command(ffmpegBinary(), "-nostdin", "-v", "error", "-f", "concat", "-safe", "0", "-i", manifestPath, "-map", "0:v:0", "-c", "copy", "-video_track_timescale", "90000", outputPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("make normalized-timebase fixture: %v (%s)", err, output)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	verification, err := VerifyJoinedMedia(ctx, []LocalSource{first, second}, outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if verification.Status != "passed" || verification.AcceptanceMode != "decoded_frame_equivalent" || verification.DecodedFrameSequenceStatus != "passed" || !lowerHex64(verification.SourceFingerprint.DecodedVideoSHA256) || verification.SourceFingerprint.DecodedVideoSHA256 != verification.OutputFingerprint.DecodedVideoSHA256 || verification.StrictDecodeStatus != "passed" {
		t.Fatalf("normalized timebase was not accepted by decoded equivalence: %+v", verification)
	}
	for mediaType, want := range verification.SourceFingerprint.Tracks {
		got := verification.OutputFingerprint.Tracks[mediaType]
		if got == nil || want.PacketCount != got.PacketCount || want.PacketChainSHA256 != got.PacketChainSHA256 {
			t.Fatalf("decoded equivalence certified changed %s packet payloads", mediaType)
		}
	}
}

func TestVerifyJoinedMediaRejectsChangedDecodedFrames(t *testing.T) {
	dir := t.TempDir()
	source := makeMediaClip(t, dir, "source.mp4", 440, false)
	changedPath := filepath.Join(dir, "changed.mp4")
	cmd := exec.Command(ffmpegBinary(), "-nostdin", "-v", "error", "-i", source.Path, "-vf", "negate", "-c:v", "mpeg4", changedPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("make changed-frame fixture: %v (%s)", err, output)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := VerifyJoinedMedia(ctx, []LocalSource{source}, changedPath)
	if err == nil || !strings.Contains(err.Error(), "media_sequence_mismatch") || !strings.Contains(err.Error(), "packet payload, decoded totals, or timeline mismatch") {
		t.Fatalf("changed decoded frames were not rejected: %v", err)
	}
}

func TestBuildLosslessNativeTimelinePreservesEveryDecodedFrame(t *testing.T) {
	dir := t.TempDir()
	first := makeProgressiveLosslessSource(t, dir, "lossless-one.mp4", 440, false)
	second := makeProgressiveLosslessSource(t, dir, "lossless-two.mp4", 880, false)
	first.ClipID, second.ClipID = 1, 2
	sources := []LocalSource{first, second}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	wantFrames, wantSHA, err := decodedVideoSequenceIdentity(ctx, []string{first.Path, second.Path})
	if err != nil {
		t.Fatal(err)
	}
	built, err := buildLosslessNativeTimeline(ctx, sources, dir, testLosslessTrigger(t, sources))
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(built.Path)
	gotFrames, gotSHA, err := decodedVideoSequenceIdentity(ctx, []string{built.Path})
	if err != nil {
		t.Fatal(err)
	}
	if gotFrames != wantFrames || gotSHA != wantSHA {
		t.Fatalf("lossless fallback changed decoded frame order: frames=%d/%d sha=%s/%s", gotFrames, wantFrames, gotSHA, wantSHA)
	}
	evidence := built.Verification.LosslessNormalization
	if built.SourceCount != 2 || built.Verification.Status != "passed" || built.Verification.AcceptanceMode != "lossless_native_timeline_normalized" || evidence == nil {
		t.Fatalf("lossless fallback evidence=%+v", built.Verification)
	}
	if evidence.Codec != "libx264" || evidence.Quantizer != 0 || evidence.SourceDecodedFrames != wantFrames || evidence.OutputDecodedFrames != wantFrames || evidence.DecodedFrameSequenceSHA256 != wantSHA || evidence.DecodedFrameFieldStatus != explicitProgressiveFrameStatus || evidence.DecodedFrameFieldSHA256 != progressiveFrameFieldSequenceSHA256(wantFrames) || !lowerHex64(evidence.SourceTimelineSignatureSHA256) {
		t.Fatalf("lossless fallback evidence differs: %+v", evidence)
	}
	if built.Verification.PacketPayloadOrderStatus != "not_applicable_lossless_normalization" || validateLosslessNormalizationVerification(built.Verification) != nil {
		t.Fatalf("lossless fallback verification contract differs: %+v", built.Verification)
	}

	mutated := built.Verification
	copyEvidence := *mutated.LosslessNormalization
	mutated.LosslessNormalization = &copyEvidence
	mutated.LosslessNormalization.OutputDecodedFrames--
	if validateLosslessNormalizationVerification(mutated) == nil {
		t.Fatal("lossless normalization accepted mismatched frame accounting")
	}
}

func TestDecodedFrameFieldLineRequiresExplicitProgressiveDisplaySemantics(t *testing.T) {
	layout := losslessVideoLayout{Width: 64, Height: 64, PixelFormat: "yuv420p", SampleAspectRatio: "1:1", ChromaLocation: "left"}
	valid := "frame|width=64|height=64|pix_fmt=yuv420p|sample_aspect_ratio=1:1|interlaced_frame=0|top_field_first=0|repeat_pict=0|color_range=unknown|color_space=unknown|color_primaries=unknown|color_transfer=unknown|chroma_location=left|side_datum:side_data_type=H.26[45] User Data Unregistered SEI message"
	if err := validateDecodedFrameFieldLine(valid, 2, 7, layout); err != nil {
		t.Fatalf("explicit progressive frame rejected: %v", err)
	}
	for name, line := range map[string]string{
		"interlaced": strings.Replace(valid, "interlaced_frame=0", "interlaced_frame=1", 1),
		"top-field":  strings.Replace(valid, "top_field_first=0", "top_field_first=1", 1),
		"repeat":     strings.Replace(valid, "repeat_pict=0", "repeat_pict=1", 1),
		"sar":        strings.Replace(valid, "sample_aspect_ratio=1:1", "sample_aspect_ratio=2:1", 1),
		"color":      strings.Replace(valid, "color_range=unknown", "color_range=tv", 1),
		"missing":    strings.Replace(valid, "repeat_pict=0|", "", 1),
	} {
		t.Run(name, func(t *testing.T) {
			var unsupported *unsupportedDecodedFrameField
			err := validateDecodedFrameFieldLine(line, 2, 7, layout)
			if !errors.As(err, &unsupported) || unsupported.SourceIndex != 2 || unsupported.FrameOrdinal != 7 {
				t.Fatalf("frame semantics did not fail closed: %+v err=%v", unsupported, err)
			}
		})
	}
}

func TestBuildLosslessNativeTimelinePreservesDisplayMetadataAndRejectsChanges(t *testing.T) {
	dir := t.TempDir()
	makeSARClip := func(name, sar string, clipID int64) LocalSource {
		mediaPath := filepath.Join(dir, name)
		cmd := exec.Command(ffmpegBinary(), "-nostdin", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=64x64:rate=10:duration=1", "-vf", "setsar="+sar, "-c:v", "libx264", "-field_order", "progressive", mediaPath)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("make SAR fixture: %v (%s)", err, output)
		}
		size, sha, err := localIdentity(mediaPath)
		if err != nil {
			t.Fatal(err)
		}
		return LocalSource{ClipID: clipID, Path: mediaPath, SizeBytes: size, SHA256: sha, SourceClaimSHA256: sha}
	}
	first := makeSARClip("sar-one.mp4", "2/1", 1)
	second := makeSARClip("sar-two.mp4", "2/1", 2)
	built, err := buildLosslessNativeTimeline(context.Background(), []LocalSource{first, second}, dir, testLosslessTrigger(t, []LocalSource{first, second}))
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(built.Path)
	sourceLayout, _, err := probeLosslessVideoLayout(context.Background(), first.Path)
	if err != nil {
		t.Fatal(err)
	}
	outputLayout, _, err := probeLosslessVideoLayout(context.Background(), built.Path)
	if err != nil {
		t.Fatal(err)
	}
	if sourceLayout != outputLayout || built.Verification.LosslessNormalization.SampleAspectRatio != "2:1" {
		t.Fatalf("display metadata changed: source=%+v output=%+v evidence=%+v", sourceLayout, outputLayout, built.Verification.LosslessNormalization)
	}

	changed := makeSARClip("sar-changed.mp4", "1/1", 3)
	_, err = buildLosslessNativeTimeline(context.Background(), []LocalSource{first, changed}, dir, testLosslessTrigger(t, []LocalSource{first, changed}))
	var deterministic *deterministicMediaError
	if !errors.As(err, &deterministic) || deterministic.code != "lossless_normalization_layout_mismatch" {
		t.Fatalf("display metadata change did not fail closed: %v", err)
	}
}

func TestBuildLosslessNativeTimelineRejectsMissingFieldOrder(t *testing.T) {
	dir := t.TempDir()
	first := makeMediaClip(t, dir, "unknown-field-order-one.mp4", 440, false)
	second := makeMediaClip(t, dir, "unknown-field-order-two.mp4", 880, false)
	first.ClipID, second.ClipID = 1, 2
	sources := []LocalSource{first, second}
	layout, _, err := probeLosslessVideoLayout(context.Background(), first.Path)
	if err != nil {
		t.Fatal(err)
	}
	if layout.FieldOrder == "progressive" {
		t.Fatalf("fixture unexpectedly reports explicit progressive field order: %+v", layout)
	}
	_, err = buildLosslessNativeTimeline(context.Background(), sources, dir, testLosslessTrigger(t, sources))
	var deterministic *deterministicMediaError
	if !errors.As(err, &deterministic) || deterministic.code != "lossless_normalization_field_order_unsupported" {
		t.Fatalf("missing field order did not fail closed: %v", err)
	}
}

func TestBuildLosslessNativeTimelineRejectsInterlacedFieldOrder(t *testing.T) {
	dir := t.TempDir()
	makeInterlaced := func(name string, clipID int64) LocalSource {
		mediaPath := filepath.Join(dir, name)
		cmd := exec.Command(ffmpegBinary(), "-nostdin", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=64x64:rate=10:duration=1", "-c:v", "libx264", "-flags", "+ilme+ildct", "-x264-params", "tff=1", mediaPath)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("make interlaced fixture: %v (%s)", err, output)
		}
		size, sha, err := localIdentity(mediaPath)
		if err != nil {
			t.Fatal(err)
		}
		return LocalSource{ClipID: clipID, Path: mediaPath, SizeBytes: size, SHA256: sha, SourceClaimSHA256: sha}
	}
	first := makeInterlaced("interlaced-one.mp4", 1)
	second := makeInterlaced("interlaced-two.mp4", 2)
	sources := []LocalSource{first, second}
	layout, _, err := probeLosslessVideoLayout(context.Background(), first.Path)
	if err != nil {
		t.Fatal(err)
	}
	if layout.FieldOrder == "" || layout.FieldOrder == "unknown" || layout.FieldOrder == "progressive" {
		t.Fatalf("fixture did not expose an interlaced field order: %+v", layout)
	}
	_, err = buildLosslessNativeTimeline(context.Background(), sources, dir, testLosslessTrigger(t, sources))
	var deterministic *deterministicMediaError
	if !errors.As(err, &deterministic) || deterministic.code != "lossless_normalization_field_order_unsupported" {
		t.Fatalf("interlaced field order did not fail closed: %v", err)
	}
}

func TestBuildLosslessNativeTimelineRejectsInterlacedFramesHiddenByProgressiveStreamMetadata(t *testing.T) {
	dir := t.TempDir()
	makeSegment := func(name string, interlaced bool) string {
		mediaPath := filepath.Join(dir, name)
		args := []string{"-nostdin", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=64x64:rate=10:duration=1", "-c:v", "libx264", "-g", "10"}
		if interlaced {
			args = append(args, "-flags", "+ilme+ildct", "-x264-params", "tff=1")
		} else {
			args = append(args, "-field_order", "progressive")
		}
		args = append(args, mediaPath)
		if output, err := exec.Command(ffmpegBinary(), args...).CombinedOutput(); err != nil {
			t.Fatalf("make mixed-field fixture segment: %v (%s)", err, output)
		}
		return mediaPath
	}
	progressivePath := makeSegment("progressive.mp4", false)
	interlacedPath := makeSegment("interlaced.mp4", true)
	concatPath := filepath.Join(dir, "mixed.txt")
	if err := os.WriteFile(concatPath, []byte(fmt.Sprintf("file '%s'\nfile '%s'\n", progressivePath, interlacedPath)), 0o600); err != nil {
		t.Fatal(err)
	}
	mixedPath := filepath.Join(dir, "mixed-progressive-metadata.mp4")
	cmd := exec.Command(ffmpegBinary(), "-nostdin", "-v", "error", "-f", "concat", "-safe", "0", "-i", concatPath, "-map", "0:v:0", "-c", "copy", "-field_order", "progressive", mixedPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("make mixed-field fixture: %v (%s)", err, output)
	}
	makeSource := func(path string, clipID int64) LocalSource {
		size, sha, err := localIdentity(path)
		if err != nil {
			t.Fatal(err)
		}
		return LocalSource{ClipID: clipID, Path: path, SizeBytes: size, SHA256: sha, SourceClaimSHA256: sha}
	}
	mixed := makeSource(mixedPath, 1)
	second := makeSource(makeSegment("second-progressive.mp4", false), 2)
	sources := []LocalSource{mixed, second}
	layout, _, err := probeLosslessVideoLayout(context.Background(), mixed.Path)
	if err != nil {
		t.Fatal(err)
	}
	if layout.FieldOrder != "progressive" {
		t.Fatalf("fixture does not hide interlaced frames behind progressive stream metadata: %+v", layout)
	}
	_, err = buildLosslessNativeTimeline(context.Background(), sources, dir, testLosslessTrigger(t, sources))
	var deterministic *deterministicMediaError
	if !errors.As(err, &deterministic) || deterministic.code != "lossless_normalization_frame_fields_unsupported" {
		t.Fatalf("hidden interlaced frame did not fail closed: %v", err)
	}
}

func TestBuildSealedOutputReproducesFrozenLosslessModeWhenFlagIsOff(t *testing.T) {
	dir := t.TempDir()
	first := makeProgressiveLosslessSource(t, dir, "sealed-lossless-one.mp4", 440, false)
	second := makeProgressiveLosslessSource(t, dir, "sealed-lossless-two.mp4", 880, false)
	first.ClipID, second.ClipID = 1, 2
	sources := []LocalSource{first, second}
	originalDir := filepath.Join(dir, "original")
	rebuildDir := filepath.Join(dir, "rebuild")
	if err := os.MkdirAll(originalDir, 0o700); err != nil {
		t.Fatal(err)
	}
	original, err := buildLosslessNativeTimeline(context.Background(), sources, originalDir, testLosslessTrigger(t, sources))
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(original.Path)

	t.Setenv("JOINED_LOSSLESS_NORMALIZATION_ENABLED", "")
	rebuilt, err := BuildSealedOutputForVerification(context.Background(), sources, rebuildDir, original.Verification)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(rebuilt.Path)
	if rebuilt.SHA256 != original.SHA256 || rebuilt.SizeBytes != original.SizeBytes || !sameCanonical([]Verification{rebuilt.Verification}, []Verification{original.Verification}) {
		t.Fatalf("sealed lossless rebuild drifted: original=%+v rebuilt=%+v", original, rebuilt)
	}
}

func TestBuildWithLosslessFallbackRunsOnlyAfterSequenceMismatch(t *testing.T) {
	sources := []LocalSource{{ClipID: 1}, {ClipID: 2}}
	fastCalls, fallbackCalls := 0, 0
	fast := func(context.Context, []LocalSource, string) (BuiltOutput, error) {
		fastCalls++
		return BuiltOutput{}, deterministicFailure("media_sequence_mismatch", struct{}{}, errors.New("stream copy dropped a frame"))
	}
	fallback := func(_ context.Context, _ []LocalSource, _ string, trigger *deterministicMediaError) (BuiltOutput, error) {
		fallbackCalls++
		if trigger == nil || trigger.code != "media_sequence_mismatch" {
			t.Fatal("lossless fallback trigger was not bound")
		}
		return BuiltOutput{SourceCount: 2, Verification: Verification{Status: "passed", AcceptanceMode: "lossless_native_timeline_normalized"}}, nil
	}
	built, err := buildWithLosslessFallback(context.Background(), sources, t.TempDir(), fast, fallback)
	if err != nil {
		t.Fatal(err)
	}
	if fastCalls != 1 || fallbackCalls != 1 || built.Verification.AcceptanceMode != "lossless_native_timeline_normalized" {
		t.Fatalf("fallback policy differed: fast=%d fallback=%d built=%+v", fastCalls, fallbackCalls, built)
	}

	fast = func(context.Context, []LocalSource, string) (BuiltOutput, error) {
		return BuiltOutput{}, deterministicFailure("corrupt_source_media", struct{}{}, errors.New("bad source"))
	}
	_, err = buildWithLosslessFallback(context.Background(), sources, t.TempDir(), fast, fallback)
	if err == nil {
		t.Fatal("non-sequence failure unexpectedly reached fallback")
	}
	if fallbackCalls != 1 {
		t.Fatalf("fallback ran for unrelated failure: %d", fallbackCalls)
	}
}

func TestLosslessNativeTimelineKeepsAudioOnStrictPath(t *testing.T) {
	dir := t.TempDir()
	first := makeProgressiveLosslessSource(t, dir, "audio-one.mp4", 440, true)
	second := makeProgressiveLosslessSource(t, dir, "audio-two.mp4", 880, true)
	first.ClipID, second.ClipID = 1, 2
	_, err := buildLosslessNativeTimeline(context.Background(), []LocalSource{first, second}, dir, testLosslessTrigger(t, []LocalSource{first, second}))
	var deterministic *deterministicMediaError
	if !errors.As(err, &deterministic) || deterministic.code != "lossless_normalization_audio_unsupported" {
		t.Fatalf("audio-bearing candidate did not fail closed: %v", err)
	}
}

func testLosslessTrigger(t *testing.T, sources []LocalSource) *deterministicMediaError {
	t.Helper()
	if len(sources) < 2 {
		t.Fatal("lossless trigger fixture requires multiple sources")
	}
	_, err := VerifyJoinedMedia(context.Background(), sources, sources[0].Path)
	failure, ok := deterministicBuildFailure(err)
	if !ok || failure.code != "media_sequence_mismatch" {
		t.Fatalf("make rejected stream-copy evidence: %v", err)
	}
	return failure
}

func TestLosslessExpansionLimitUsesSingleExplicitProof(t *testing.T) {
	sources := makeSyntheticLocalSources(2)
	err := losslessOutputLimitFailure(899, 900, 700, 700)
	failure, ok := deterministicBuildFailure(err)
	if !ok || failure.code != "lossless_normalization_expansion_cap" {
		t.Fatalf("lossless bounded output did not retain its explicit expansion-cap reason: %v", err)
	}
	tool := strings.Repeat("f", 64)
	evidence := maximalityEvidence(sources, failure, 1, tool)
	if err := validateMaximalityEvidence(evidence, tool, evidence.SourceClaimSHA256); err != nil {
		t.Fatalf("single bounded output proof did not validate: %v", err)
	}
	evidence.RepeatCount = 2
	if err := validateMaximalityEvidence(evidence, tool, evidence.SourceClaimSHA256); err == nil {
		t.Fatal("output-cap proof accepted an unbound repeat count")
	}
}

func TestLosslessTruncatedOutputUsesExpansionCapBelowFFmpegFileLimit(t *testing.T) {
	err := losslessTruncatedOutputFailure(899, 900, 699, 700)
	failure, ok := deterministicBuildFailure(err)
	if !ok || failure.code != "lossless_normalization_expansion_cap" {
		t.Fatalf("ffmpeg -fs truncation below its byte limit was not routed to size partitioning: %v", err)
	}
	if err := losslessTruncatedOutputFailure(0, 900, 699, 700); err == nil {
		t.Fatal("zero-frame encoder output was not classified without probing the partial media")
	}
	if err := losslessTruncatedOutputFailure(899, 900, 699, 0); err != nil {
		t.Fatalf("invalid limit fabricated a bounded-output failure: %v", err)
	}
	if err := losslessTruncatedOutputFailure(900, 900, 699, 700); err != nil {
		t.Fatalf("complete output was misclassified as truncated: %v", err)
	}
}

func TestBuildLosslessNativeTimelineClassifiesPinnedFFmpegZeroFrameCapAndCleansOutput(t *testing.T) {
	dir := t.TempDir()
	first := makeProgressiveLosslessSource(t, dir, "capped-one.mp4", 440, false)
	second := makeProgressiveLosslessSource(t, dir, "capped-two.mp4", 880, false)
	first.ClipID, second.ClipID = 1, 2
	sources := []LocalSource{first, second}
	before, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = buildLosslessNativeTimelineWithOutputLimit(ctx, sources, dir, testLosslessTrigger(t, sources), 1)
	failure, ok := deterministicBuildFailure(err)
	if !ok || failure.code != "lossless_normalization_expansion_cap" {
		t.Fatalf("pinned ffmpeg zero-frame cap did not route to size partitioning: %v", err)
	}
	var evidence struct {
		EncodedFrames  int64 `json:"encoded_frames"`
		ExpectedFrames int64 `json:"expected_frames"`
	}
	if err := json.Unmarshal(failure.evidence, &evidence); err != nil {
		t.Fatal(err)
	}
	if evidence.EncodedFrames != 0 || evidence.ExpectedFrames <= 0 {
		t.Fatalf("cap was not proven independently of output probing: %+v", evidence)
	}
	after, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(after) != fmt.Sprint(before) {
		t.Fatalf("failed capped output was retained: before=%v after=%v", before, after)
	}
}

func TestLosslessNormalizationRequiresExplicitWorkerOptIn(t *testing.T) {
	t.Setenv("JOINED_LOSSLESS_NORMALIZATION_ENABLED", "")
	if losslessNormalizationEnabled() {
		t.Fatal("lossless normalization enabled by default")
	}
	t.Setenv("JOINED_LOSSLESS_NORMALIZATION_ENABLED", "true")
	if !losslessNormalizationEnabled() {
		t.Fatal("explicit lossless normalization opt-in was ignored")
	}
	t.Setenv("JOINED_LOSSLESS_NORMALIZATION_ENABLED", "TRUE")
	if losslessNormalizationEnabled() {
		t.Fatal("ambiguous lossless normalization opt-in was accepted")
	}
}

func TestDefaultMediaCandidateBudgetDoesNotRushExactPairProofs(t *testing.T) {
	for _, kind := range []string{"full", "full_repeat"} {
		if got := defaultMediaCandidateBudget(kind, 2); got != 60*time.Minute {
			t.Errorf("%s budget=%s want=60m", kind, got)
		}
	}
	if got := defaultMediaCandidateBudget("segment", 37); got != 60*time.Minute {
		t.Fatalf("37-source segment budget=%s want=60m", got)
	}
	if got := defaultMediaCandidateBudget("full_timeout_retry", 2); got != 60*time.Minute {
		t.Errorf("full timeout retry budget=%s want=60m", got)
	}
	for _, kind := range []string{"pair", "pair_repeat"} {
		if got := defaultMediaCandidateBudget(kind, 2); got != 5*time.Minute {
			t.Errorf("%s budget=%s want=5m", kind, got)
		}
	}
	if got := defaultMediaCandidateBudget("prefix", 2); got != 170*time.Second {
		t.Fatalf("two-source discovery budget=%s want=170s", got)
	}
}

func TestBuildLargestPassingPrefixPeelsRepeatableCorruptSource(t *testing.T) {
	dir := t.TempDir()
	first := makeMediaClip(t, dir, "one.mp4", 440, false)
	first.ClipID = 1
	badPath := filepath.Join(dir, "bad.mp4")
	if err := os.WriteFile(badPath, []byte("not media"), 0600); err != nil {
		t.Fatal(err)
	}
	size, sha, err := localIdentity(badPath)
	if err != nil {
		t.Fatal(err)
	}
	bad := LocalSource{ClipID: 2, Path: badPath, SizeBytes: size, SHA256: sha, SourceClaimSHA256: sha}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	built, err := BuildLargestPassingPrefix(ctx, []LocalSource{first, bad}, dir, strings.Repeat("f", 64))
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(built.Path)
	if built.SourceCount != 1 || len(built.SplitEvidence) == 0 || built.SplitEvidence[len(built.SplitEvidence)-1].ReasonCode != "corrupt_source_media" {
		t.Fatalf("repeatable corrupt source was not audibly peeled: %+v", built)
	}
}

func TestBuildAllPassingPartsHasLinearCandidateWork(t *testing.T) {
	const sourceCount = 60
	sources := make([]LocalSource, sourceCount)
	for i := range sources {
		sources[i] = LocalSource{ClipID: int64(i + 1), SourceClaimSHA256: strings.Repeat("a", 64)}
	}
	totalSourcesBuilt := 0
	attempts := 0
	attempt := func(_ context.Context, candidate []LocalSource, _ string) (BuiltOutput, error) {
		attempts++
		totalSourcesBuilt += len(candidate)
		if len(candidate) == 1 {
			return BuiltOutput{SourceCount: 1}, nil
		}
		return BuiltOutput{}, deterministicFailure("media_sequence_mismatch", struct {
			CandidateCount int `json:"candidate_count"`
		}{len(candidate)}, errors.New("repeatable adjacent seam failure"))
	}

	parts, quarantines, err := buildAllPassingPartsWithAttempt(context.Background(), sources, t.TempDir(), strings.Repeat("f", 64), attempt)
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != sourceCount || len(quarantines) != 0 {
		t.Fatalf("parts=%d quarantines=%d want=%d/0", len(parts), len(quarantines), sourceCount)
	}
	if totalSourcesBuilt > sourceCount*12 {
		t.Fatalf("candidate work is superlinear: attempts=%d total_sources_built=%d limit=%d", attempts, totalSourcesBuilt, sourceCount*12)
	}
}

func TestBuildAllPassingPartsReusesExactPairFailureProofForSingletonBoundaries(t *testing.T) {
	const sourceCount = 60
	sources := makeSyntheticLocalSourcesWithIdentity(t, sourceCount)
	attempt := func(_ context.Context, candidate []LocalSource, _ string) (BuiltOutput, error) {
		if len(candidate) == 1 {
			return BuiltOutput{SourceCount: 1}, nil
		}
		return BuiltOutput{}, deterministicFailure("media_sequence_mismatch", struct {
			CandidateClipIDs []int64 `json:"candidate_clip_ids"`
		}{clipIDs(candidate)}, errors.New("repeatable seam failure"))
	}

	legacyAttempts := 0
	legacyAttempt := func(ctx context.Context, candidate []LocalSource, scratch string) (BuiltOutput, error) {
		legacyAttempts++
		return attempt(ctx, candidate, scratch)
	}
	legacyParts, legacyQuarantines, err := buildAllPassingPartsWithPairProofReuse(context.Background(), sources, t.TempDir(), strings.Repeat("f", 64), legacyAttempt, defaultMediaCandidateBudget, false)
	if err != nil {
		t.Fatal(err)
	}

	reusedAttempts := 0
	successfulOutputAttempts := 0
	reuseEvents := 0
	ctx := WithStageTimingObserver(context.Background(), func(event StageTimingEvent) {
		if event.Stage == "media_candidate_extension_pair_proof_reused" && event.Outcome == "ok" {
			reuseEvents++
		}
	})
	reusedAttempt := func(ctx context.Context, candidate []LocalSource, scratch string) (BuiltOutput, error) {
		reusedAttempts++
		built, err := attempt(ctx, candidate, scratch)
		if err == nil {
			successfulOutputAttempts++
		}
		return built, err
	}
	reusedParts, reusedQuarantines, err := buildAllPassingPartsWithPairProofReuse(ctx, sources, t.TempDir(), strings.Repeat("f", 64), reusedAttempt, defaultMediaCandidateBudget, true)
	if err != nil {
		t.Fatal(err)
	}
	if legacyAttempts != 298 || reusedAttempts != 180 {
		t.Fatalf("attempts legacy=%d reused=%d want=298/180", legacyAttempts, reusedAttempts)
	}
	if successfulOutputAttempts != sourceCount {
		t.Fatalf("successful outputs attempted=%d want=%d", successfulOutputAttempts, sourceCount)
	}
	if reuseEvents != sourceCount-1 {
		t.Fatalf("reuse metric events=%d want=%d", reuseEvents, sourceCount-1)
	}
	legacyJSON, _ := json.Marshal(struct {
		Parts       []BuiltOutput
		Quarantines []QuarantinedBuild
	}{legacyParts, legacyQuarantines})
	reusedJSON, _ := json.Marshal(struct {
		Parts       []BuiltOutput
		Quarantines []QuarantinedBuild
	}{reusedParts, reusedQuarantines})
	if !bytes.Equal(legacyJSON, reusedJSON) {
		t.Fatalf("reused proof changed evidence\nlegacy=%s\nreused=%s", legacyJSON, reusedJSON)
	}
}

func TestExactPairFailureProofIdentityIsFailClosed(t *testing.T) {
	base := makeSyntheticLocalSources(2)
	base[0].SizeBytes, base[1].SizeBytes = 11, 12
	base[0].SHA256, base[1].SHA256 = strings.Repeat("1", 64), strings.Repeat("2", 64)
	base[0].AudioContract = &AudioSequenceContract{CodecName: "aac", SampleRate: 48000, Channels: 2, ChannelLayout: "stereo"}
	base[1].AudioContract = &AudioSequenceContract{CodecName: "aac", SampleRate: 48000, Channels: 2, ChannelLayout: "stereo"}
	baseKey, err := exactPairFailureProofKey(base, 0, PlanPolicyVersion, strings.Repeat("f", 64))
	if err != nil {
		t.Fatal(err)
	}
	mutations := map[string]func([]LocalSource) ([]LocalSource, int, string, string){
		"order": func(in []LocalSource) ([]LocalSource, int, string, string) {
			return []LocalSource{in[1], in[0]}, 0, PlanPolicyVersion, strings.Repeat("f", 64)
		},
		"locator position": func(in []LocalSource) ([]LocalSource, int, string, string) {
			return in, 1, PlanPolicyVersion, strings.Repeat("f", 64)
		},
		"clip id": func(in []LocalSource) ([]LocalSource, int, string, string) {
			in[0].ClipID++
			return in, 0, PlanPolicyVersion, strings.Repeat("f", 64)
		},
		"source claim": func(in []LocalSource) ([]LocalSource, int, string, string) {
			in[0].SourceClaimSHA256 = strings.Repeat("b", 64)
			return in, 0, PlanPolicyVersion, strings.Repeat("f", 64)
		},
		"local sha": func(in []LocalSource) ([]LocalSource, int, string, string) {
			in[0].SHA256 = strings.Repeat("3", 64)
			return in, 0, PlanPolicyVersion, strings.Repeat("f", 64)
		},
		"local size": func(in []LocalSource) ([]LocalSource, int, string, string) {
			in[0].SizeBytes++
			return in, 0, PlanPolicyVersion, strings.Repeat("f", 64)
		},
		"audio contract": func(in []LocalSource) ([]LocalSource, int, string, string) {
			copy := *in[0].AudioContract
			copy.SampleRate++
			in[0].AudioContract = &copy
			return in, 0, PlanPolicyVersion, strings.Repeat("f", 64)
		},
		"policy": func(in []LocalSource) ([]LocalSource, int, string, string) {
			return in, 0, PlanPolicyVersion + "-changed", strings.Repeat("f", 64)
		},
		"tool": func(in []LocalSource) ([]LocalSource, int, string, string) {
			return in, 0, PlanPolicyVersion, strings.Repeat("e", 64)
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			candidate := append([]LocalSource(nil), base...)
			candidate, position, policy, tool := mutate(candidate)
			got, err := exactPairFailureProofKey(candidate, position, policy, tool)
			if err != nil {
				t.Fatal(err)
			}
			if got == baseKey {
				t.Fatal("identity mutation reused an exact-pair proof")
			}
		})
	}
}

func TestBuildAllPassingPartsNeverReusesSourceFailure(t *testing.T) {
	sources := makeSyntheticLocalSourcesWithIdentity(t, 2)
	calls := 0
	attempt := func(_ context.Context, candidate []LocalSource, _ string) (BuiltOutput, error) {
		calls++
		return BuiltOutput{}, deterministicFailure("corrupt_source_media", struct {
			CandidateClipIDs []int64 `json:"candidate_clip_ids"`
		}{clipIDs(candidate)}, errors.New("source probe failed"))
	}
	_, _, _ = buildAllPassingPartsWithAttempt(context.Background(), sources, t.TempDir(), strings.Repeat("f", 64), attempt)
	if calls != 8 {
		t.Fatalf("source failure attempts=%d want=8", calls)
	}
}

func TestBuildAllPassingPartsExactPairProofFailsClosedOnSourceMutation(t *testing.T) {
	sources := makeSyntheticLocalSourcesWithIdentity(t, 3)
	calls12 := 0
	mutated := false
	attempt := func(_ context.Context, candidate []LocalSource, _ string) (BuiltOutput, error) {
		ids := clipIDs(candidate)
		if equalInt64s(ids, []int64{1}) && !mutated {
			mutated = true
			if err := os.WriteFile(sources[1].Path, []byte("changed exact bytes"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		if equalInt64s(ids, []int64{1, 2}) {
			calls12++
			return BuiltOutput{}, seamFailure(1, 2)
		}
		if len(candidate) > 1 {
			return BuiltOutput{}, seamFailure(candidate[0].ClipID, candidate[1].ClipID)
		}
		return BuiltOutput{SourceCount: 1}, nil
	}
	_, _, err := buildAllPassingPartsWithAttempt(context.Background(), sources, t.TempDir(), strings.Repeat("f", 64), attempt)
	if err != nil {
		t.Fatal(err)
	}
	if calls12 != 4 {
		t.Fatalf("mutated source reused stale pair proof: calls=%d want=4", calls12)
	}
}

func TestBuildAllPassingPartsExactPairProofHonorsCancellation(t *testing.T) {
	sources := makeSyntheticLocalSourcesWithIdentity(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	attempt := func(_ context.Context, candidate []LocalSource, _ string) (BuiltOutput, error) {
		if len(candidate) == 1 {
			cancel()
			return BuiltOutput{SourceCount: 1}, nil
		}
		return BuiltOutput{}, seamFailure(1, 2)
	}
	parts, quarantines, err := buildAllPassingPartsWithAttempt(ctx, sources, t.TempDir(), strings.Repeat("f", 64), attempt)
	if !errors.Is(err, context.Canceled) || len(parts) != 0 || len(quarantines) != 0 {
		t.Fatalf("parts=%v quarantines=%v err=%v", parts, quarantines, err)
	}
}

func TestCloneDeterministicMediaErrorDoesNotAliasFacts(t *testing.T) {
	original, ok := deterministicBuildFailure(deterministicFailure("media_sequence_mismatch", struct {
		Value string `json:"value"`
	}{"original"}, errors.New("mismatch")))
	if !ok {
		t.Fatal("fixture is not deterministic")
	}
	copy := cloneDeterministicMediaError(original)
	original.evidence[0] = 'x'
	if bytes.Equal(original.evidence, copy.evidence) || copy.evidenceSHA256 != original.evidenceSHA256 || copy.code != original.code {
		t.Fatalf("proof copy aliases or changes evidence: original=%q copy=%q", original.evidence, copy.evidence)
	}
}

func TestBuildAllPassingPartsUsesExactBoundaryProof(t *testing.T) {
	sources := makeSyntheticLocalSources(6)
	calls := make([][]int64, 0)
	attempt := func(_ context.Context, candidate []LocalSource, _ string) (BuiltOutput, error) {
		calls = append(calls, clipIDs(candidate))
		if containsAdjacentClipIDs(candidate, 3, 4) {
			return BuiltOutput{}, seamFailure(3, 4)
		}
		return BuiltOutput{SourceCount: len(candidate)}, nil
	}

	parts, quarantines, err := buildAllPassingPartsWithAttempt(context.Background(), sources, t.TempDir(), strings.Repeat("f", 64), attempt)
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 2 || parts[0].SourceCount != 3 || parts[1].SourceCount != 3 || len(quarantines) != 0 {
		t.Fatalf("parts=%+v quarantines=%+v", parts, quarantines)
	}
	if len(parts[0].SplitEvidence) != 1 || parts[0].SplitEvidence[0].RepeatCount != 2 || !equalInt64s(parts[0].SplitEvidence[0].CandidateClipIDs, []int64{1, 2, 3, 4}) || len(parts[1].SplitEvidence) != 0 {
		t.Fatalf("boundary evidence=%+v", parts)
	}
	if countSpan(calls, []int64{1, 2, 3, 4}) != 2 {
		t.Fatalf("exact boundary extension was not proved twice: calls=%v", calls)
	}
}

func TestBuildAllPassingPartsUsesMaximalPrefixesForThresholdFailure(t *testing.T) {
	sources := makeSyntheticLocalSources(5)
	attempt := func(_ context.Context, candidate []LocalSource, _ string) (BuiltOutput, error) {
		if len(candidate) >= 3 {
			return BuiltOutput{}, deterministicFailure("media_sequence_mismatch", struct {
				CandidateCount int `json:"candidate_count"`
			}{len(candidate)}, errors.New("three-way-only mismatch"))
		}
		return BuiltOutput{SourceCount: len(candidate)}, nil
	}

	parts, quarantines, err := buildAllPassingPartsWithAttempt(context.Background(), sources, t.TempDir(), strings.Repeat("f", 64), attempt)
	if err != nil || len(parts) != 3 || parts[0].SourceCount != 2 || parts[1].SourceCount != 2 || parts[2].SourceCount != 1 || len(quarantines) != 0 {
		t.Fatalf("parts=%v quarantines=%v err=%v", parts, quarantines, err)
	}
	if !equalInt64s(parts[0].SplitEvidence[0].CandidateClipIDs, []int64{1, 2, 3}) ||
		!equalInt64s(parts[1].SplitEvidence[0].CandidateClipIDs, []int64{3, 4, 5}) || len(parts[2].SplitEvidence) != 0 {
		t.Fatalf("threshold maximality evidence differs: %+v", parts)
	}
}

func TestBuildAllPassingPartsUsesMaximalPassingPrefixForNonlocalFailure(t *testing.T) {
	sources := makeSyntheticLocalSources(32)
	calls := make([][]int64, 0)
	attempt := func(_ context.Context, candidate []LocalSource, _ string) (BuiltOutput, error) {
		calls = append(calls, clipIDs(candidate))
		if len(candidate) == len(sources) {
			return BuiltOutput{}, deterministicFailure("media_sequence_mismatch", struct {
				CandidateCount int `json:"candidate_count"`
			}{len(candidate)}, errors.New("repeatable nonlocal mismatch"))
		}
		return BuiltOutput{SourceCount: len(candidate)}, nil
	}

	parts, quarantines, err := buildAllPassingPartsWithAttempt(context.Background(), sources, t.TempDir(), strings.Repeat("f", 64), attempt)
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 2 || parts[0].SourceCount != 31 || parts[1].SourceCount != 1 || len(quarantines) != 0 {
		t.Fatalf("parts=%+v quarantines=%+v", parts, quarantines)
	}
	if len(parts[0].SplitEvidence) != 1 || parts[0].SplitEvidence[0].RepeatCount != 2 ||
		parts[0].SplitEvidence[0].ReasonCode != "media_sequence_mismatch" ||
		!equalInt64s(parts[0].SplitEvidence[0].CandidateClipIDs, clipIDs(sources)) || len(parts[1].SplitEvidence) != 0 {
		t.Fatalf("maximal prefix evidence=%+v", parts)
	}
	if countSpan(calls, clipIDs(sources)) != 2 || countSpan(calls, clipIDs(sources[:31])) != 1 || countSpan(calls, clipIDs(sources[31:])) != 1 {
		t.Fatalf("candidates were not independently and minimally proved: %v", calls)
	}
}

func TestBuildAllPassingPartsDescendsAfterRepeatedPrefixFailure(t *testing.T) {
	sources := makeSyntheticLocalSources(32)
	calls := make([][]int64, 0)
	attempt := func(_ context.Context, candidate []LocalSource, _ string) (BuiltOutput, error) {
		calls = append(calls, clipIDs(candidate))
		if len(candidate) >= 31 {
			return BuiltOutput{}, deterministicFailure("media_sequence_mismatch", struct {
				CandidateCount int `json:"candidate_count"`
			}{len(candidate)}, errors.New("repeatable nonlocal mismatch"))
		}
		return BuiltOutput{SourceCount: len(candidate)}, nil
	}

	parts, quarantines, err := buildAllPassingPartsWithAttempt(context.Background(), sources, t.TempDir(), strings.Repeat("f", 64), attempt)
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 2 || parts[0].SourceCount != 30 || parts[1].SourceCount != 2 || len(quarantines) != 0 {
		t.Fatalf("parts=%+v quarantines=%+v", parts, quarantines)
	}
	if len(parts[0].SplitEvidence) != 1 || parts[0].SplitEvidence[0].RepeatCount != 2 ||
		parts[0].SplitEvidence[0].ReasonCode != "media_sequence_mismatch" ||
		!equalInt64s(parts[0].SplitEvidence[0].CandidateClipIDs, clipIDs(sources[:31])) || len(parts[1].SplitEvidence) != 0 {
		t.Fatalf("maximal prefix evidence=%+v", parts)
	}
	if countSpan(calls, clipIDs(sources)) != 2 || countSpan(calls, clipIDs(sources[:31])) != 2 ||
		countSpan(calls, clipIDs(sources[:30])) != 1 || countSpan(calls, clipIDs(sources[30:])) != 2 || len(calls) != 37 {
		t.Fatalf("descending candidates were not independently and minimally proved: %v", calls)
	}
}

func TestBuildAllPassingPartsLocalizesAfterOpaqueFullCandidateDeadline(t *testing.T) {
	sources := makeSyntheticLocalSources(4)
	parentCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	attempt := func(ctx context.Context, candidate []LocalSource, _ string) (BuiltOutput, error) {
		ids := clipIDs(candidate)
		if equalInt64s(ids, []int64{1, 2, 3, 4}) {
			<-ctx.Done()
			return BuiltOutput{}, errors.Join(fmt.Errorf("lossless fallback: %v", ctx.Err()), ctx.Err())
		}
		if containsAdjacentClipIDs(candidate, 2, 3) {
			return BuiltOutput{}, seamFailure(2, 3)
		}
		return BuiltOutput{SourceCount: len(candidate)}, nil
	}
	budget := func(kind string, _ int) time.Duration {
		if kind == "full" {
			return 10 * time.Millisecond
		}
		return 100 * time.Millisecond
	}

	parts, quarantines, err := buildAllPassingPartsWithPolicy(parentCtx, sources, t.TempDir(), strings.Repeat("f", 64), attempt, budget)
	if err != nil {
		t.Fatal(err)
	}
	if parentCtx.Err() != nil || len(parts) != 2 || parts[0].SourceCount != 2 || parts[1].SourceCount != 2 || len(parts[0].SplitEvidence) != 1 || len(quarantines) != 0 {
		t.Fatalf("parent_err=%v parts=%+v quarantines=%+v", parentCtx.Err(), parts, quarantines)
	}
}

func TestBuildAllPassingPartsRetriesUnisolatedFullDeadlineWithLongParent(t *testing.T) {
	sources := makeSyntheticLocalSources(4)
	parentCtx, cancel := context.WithTimeout(context.Background(), 140*time.Minute)
	defer cancel()
	parentDeadline, _ := parentCtx.Deadline()
	fullAttempts := 0
	pairAttempts := 0
	attempt := func(ctx context.Context, candidate []LocalSource, _ string) (BuiltOutput, error) {
		if len(candidate) == len(sources) {
			fullAttempts++
			deadline, ok := ctx.Deadline()
			if !ok {
				t.Fatal("full candidate lacked deadline")
			}
			if fullAttempts == 1 {
				if remaining := time.Until(deadline); remaining > 60*time.Minute || remaining < 59*time.Minute {
					t.Fatalf("initial full budget=%s want about 60m", remaining)
				}
				return BuiltOutput{}, context.DeadlineExceeded
			}
			if deadline.After(parentDeadline.Add(-75 * time.Minute)) {
				t.Fatalf("retry deadline=%s exceeds parent reserve cutoff=%s", deadline, parentDeadline.Add(-75*time.Minute))
			}
			if remaining := time.Until(deadline); remaining > 60*time.Minute || remaining < 59*time.Minute {
				t.Fatalf("retry budget=%s want about 60m", remaining)
			}
			return BuiltOutput{SourceCount: len(candidate)}, nil
		}
		pairAttempts++
		return BuiltOutput{SourceCount: len(candidate)}, nil
	}

	parts, quarantines, err := buildAllPassingPartsWithAttempt(parentCtx, sources, t.TempDir(), strings.Repeat("f", 64), attempt)
	if err != nil || len(parts) != 1 || parts[0].SourceCount != len(sources) || len(quarantines) != 0 {
		t.Fatalf("full_attempts=%d pair_attempts=%d parts=%+v quarantines=%+v err=%v", fullAttempts, pairAttempts, parts, quarantines, err)
	}
	if fullAttempts != 2 || pairAttempts != len(sources)-1 {
		t.Fatalf("full_attempts=%d pair_attempts=%d want=2/%d", fullAttempts, pairAttempts, len(sources)-1)
	}
}

func TestBuildAllPassingPartsDoesNotRetryFullDeadlineWithoutReserve(t *testing.T) {
	tests := []struct {
		name         string
		ctx          func() (context.Context, context.CancelFunc)
		wantCanceled bool
	}{
		{"no deadline", func() (context.Context, context.CancelFunc) { return context.Background(), func() {} }, false},
		{"short deadline", func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), 134*time.Minute)
		}, false},
		{"cancelled parent", func() (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return ctx, func() {}
		}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := tt.ctx()
			defer cancel()
			sources := makeSyntheticLocalSources(3)
			fullAttempts := 0
			attempt := func(_ context.Context, candidate []LocalSource, _ string) (BuiltOutput, error) {
				if len(candidate) == len(sources) {
					fullAttempts++
					return BuiltOutput{}, context.DeadlineExceeded
				}
				return BuiltOutput{SourceCount: len(candidate)}, nil
			}
			parts, quarantines, err := buildAllPassingPartsWithAttempt(ctx, sources, t.TempDir(), strings.Repeat("f", 64), attempt)
			if !errors.Is(err, context.DeadlineExceeded) || len(parts) != 0 || len(quarantines) != 0 || fullAttempts != 1 {
				t.Fatalf("full_attempts=%d parts=%+v quarantines=%+v err=%v", fullAttempts, parts, quarantines, err)
			}
			if tt.wantCanceled && !errors.Is(err, context.Canceled) {
				t.Fatalf("parent cancellation was lost: %v", err)
			}
		})
	}
}

func TestBuildAllPassingPartsPreservesCancellationAfterPairScan(t *testing.T) {
	sources := makeSyntheticLocalSources(3)
	ctx, cancel := context.WithCancel(context.Background())
	fullAttempts := 0
	pairAttempts := 0
	attempt := func(_ context.Context, candidate []LocalSource, _ string) (BuiltOutput, error) {
		if len(candidate) == len(sources) {
			fullAttempts++
			return BuiltOutput{}, context.DeadlineExceeded
		}
		pairAttempts++
		if pairAttempts == len(sources)-1 {
			cancel()
		}
		return BuiltOutput{SourceCount: len(candidate)}, nil
	}
	parts, quarantines, err := buildAllPassingPartsWithAttempt(ctx, sources, t.TempDir(), strings.Repeat("f", 64), attempt)
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, context.Canceled) || len(parts) != 0 || len(quarantines) != 0 || fullAttempts != 1 || pairAttempts != 2 {
		t.Fatalf("full_attempts=%d pair_attempts=%d parts=%+v quarantines=%+v err=%v", fullAttempts, pairAttempts, parts, quarantines, err)
	}
}

func TestBuildAllPassingPartsDoesNotLocalizeDeadlineJoinedWithENOSPC(t *testing.T) {
	sources := makeSyntheticLocalSources(3)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Hour)
	defer cancel()
	attempts := 0
	attempt := func(_ context.Context, _ []LocalSource, _ string) (BuiltOutput, error) {
		attempts++
		return BuiltOutput{}, errors.Join(context.DeadlineExceeded, syscall.ENOSPC)
	}
	parts, quarantines, err := buildAllPassingPartsWithAttempt(ctx, sources, t.TempDir(), strings.Repeat("f", 64), attempt)
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, syscall.ENOSPC) || len(parts) != 0 || len(quarantines) != 0 || attempts != 1 {
		t.Fatalf("attempts=%d parts=%+v quarantines=%+v err=%v", attempts, parts, quarantines, err)
	}
}

func TestBuildAllPassingPartsDoesNotRetryFullDeadlineAcrossBoundary(t *testing.T) {
	sources := makeSyntheticLocalSources(4)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Hour)
	defer cancel()
	fullAttempts := 0
	attempt := func(_ context.Context, candidate []LocalSource, _ string) (BuiltOutput, error) {
		if len(candidate) == len(sources) {
			fullAttempts++
			return BuiltOutput{}, context.DeadlineExceeded
		}
		if containsAdjacentClipIDs(candidate, 2, 3) {
			return BuiltOutput{}, seamFailure(2, 3)
		}
		return BuiltOutput{SourceCount: len(candidate)}, nil
	}
	parts, quarantines, err := buildAllPassingPartsWithAttempt(ctx, sources, t.TempDir(), strings.Repeat("f", 64), attempt)
	if err != nil || len(parts) != 2 || len(quarantines) != 0 || fullAttempts != 1 {
		t.Fatalf("full_attempts=%d parts=%+v quarantines=%+v err=%v", fullAttempts, parts, quarantines, err)
	}
}

func TestBuildAllPassingPartsFullTimeoutRetryFailureCleansOutput(t *testing.T) {
	for _, retryErr := range []error{context.DeadlineExceeded, syscall.ENOSPC} {
		t.Run(retryErr.Error(), func(t *testing.T) {
			sources := makeSyntheticLocalSources(3)
			scratch := t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Hour)
			defer cancel()
			initialCause := errors.New("initial full timeout")
			initialErr := errors.Join(initialCause, context.DeadlineExceeded)
			fullAttempts := 0
			attempt := func(_ context.Context, candidate []LocalSource, scratchDir string) (BuiltOutput, error) {
				if len(candidate) != len(sources) {
					return BuiltOutput{SourceCount: len(candidate)}, nil
				}
				fullAttempts++
				if fullAttempts == 1 {
					return BuiltOutput{}, initialErr
				}
				attemptDir, err := os.MkdirTemp(scratchDir, "attempt-")
				if err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(attemptDir, "joined.mp4")
				if err := os.WriteFile(path, []byte("partial"), 0600); err != nil {
					t.Fatal(err)
				}
				return BuiltOutput{Path: path, SourceCount: len(candidate)}, retryErr
			}
			parts, quarantines, err := buildAllPassingPartsWithAttempt(ctx, sources, scratch, strings.Repeat("f", 64), attempt)
			if !errors.Is(err, initialCause) || !errors.Is(err, retryErr) || len(parts) != 0 || len(quarantines) != 0 || fullAttempts != 2 {
				t.Fatalf("parts=%+v quarantines=%+v err=%v", parts, quarantines, err)
			}
			entries, readErr := os.ReadDir(scratch)
			if readErr != nil || len(entries) != 0 {
				t.Fatalf("retry scratch retained: entries=%v err=%v", entries, readErr)
			}
		})
	}
}

func TestBuildAllPassingPartsFullTimeoutRetryDeterministicFailureDoesNotRepeat(t *testing.T) {
	sources := makeSyntheticLocalSources(3)
	scratch := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Hour)
	defer cancel()
	initialCause := errors.New("initial full timeout")
	retryCause := errors.New("retry deterministic mismatch")
	fullAttempts := 0
	attempt := func(_ context.Context, candidate []LocalSource, _ string) (BuiltOutput, error) {
		if len(candidate) != len(sources) {
			return BuiltOutput{SourceCount: len(candidate)}, nil
		}
		fullAttempts++
		if fullAttempts == 1 {
			return BuiltOutput{}, errors.Join(initialCause, context.DeadlineExceeded)
		}
		return BuiltOutput{}, deterministicFailure("media_sequence_mismatch", struct{}{}, retryCause)
	}
	parts, quarantines, err := buildAllPassingPartsWithAttempt(ctx, sources, scratch, strings.Repeat("f", 64), attempt)
	if !errors.Is(err, initialCause) || !errors.Is(err, retryCause) || len(parts) != 0 || len(quarantines) != 0 || fullAttempts != 2 {
		t.Fatalf("full_attempts=%d parts=%+v quarantines=%+v err=%v", fullAttempts, parts, quarantines, err)
	}
	if entries, readErr := os.ReadDir(scratch); readErr != nil || len(entries) != 0 {
		t.Fatalf("deterministic retry retained output/evidence: entries=%v err=%v", entries, readErr)
	}
}

func TestBuildAllPassingPartsDeterministicFullRepeatKeeps60MinuteBudget(t *testing.T) {
	sources := makeSyntheticLocalSources(4)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Hour)
	defer cancel()
	fullAttempts := 0
	attempt := func(ctx context.Context, candidate []LocalSource, _ string) (BuiltOutput, error) {
		if len(candidate) == len(sources) {
			fullAttempts++
			deadline, ok := ctx.Deadline()
			if remaining := time.Until(deadline); !ok || remaining > 60*time.Minute || remaining < 59*time.Minute {
				t.Fatalf("deterministic full attempt %d exceeded 60m: deadline=%s ok=%v", fullAttempts, deadline, ok)
			}
			return BuiltOutput{}, seamFailure(2, 3)
		}
		if containsAdjacentClipIDs(candidate, 2, 3) {
			return BuiltOutput{}, seamFailure(2, 3)
		}
		return BuiltOutput{SourceCount: len(candidate)}, nil
	}
	parts, quarantines, err := buildAllPassingPartsWithAttempt(ctx, sources, t.TempDir(), strings.Repeat("f", 64), attempt)
	if err != nil || len(parts) != 2 || len(quarantines) != 0 || fullAttempts != 2 {
		t.Fatalf("full_attempts=%d parts=%+v quarantines=%+v err=%v", fullAttempts, parts, quarantines, err)
	}
}

func TestBuildAllPassingPartsRetriesSingletonFullDeadline(t *testing.T) {
	sources := makeSyntheticLocalSources(1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Hour)
	defer cancel()
	fullAttempts := 0
	attempt := func(_ context.Context, candidate []LocalSource, _ string) (BuiltOutput, error) {
		fullAttempts++
		if fullAttempts == 1 {
			return BuiltOutput{}, context.DeadlineExceeded
		}
		return BuiltOutput{SourceCount: len(candidate)}, nil
	}
	parts, quarantines, err := buildAllPassingPartsWithAttempt(ctx, sources, t.TempDir(), strings.Repeat("f", 64), attempt)
	if err != nil || len(parts) != 1 || parts[0].SourceCount != 1 || len(quarantines) != 0 || fullAttempts != 2 {
		t.Fatalf("full_attempts=%d parts=%+v quarantines=%+v err=%v", fullAttempts, parts, quarantines, err)
	}
}

func TestBuildAllPassingPartsPreservesOpaqueSizeAttemptDeadline(t *testing.T) {
	sources := makeSyntheticLocalSources(4)
	attempts := 0
	attempt := func(ctx context.Context, candidate []LocalSource, _ string) (BuiltOutput, error) {
		attempts++
		if attempts == 1 {
			return BuiltOutput{}, deterministicFailure("output_exceeds_put_cap", struct{}{}, errors.New("bounded output cap"))
		}
		<-ctx.Done()
		return BuiltOutput{}, errors.Join(fmt.Errorf("lossless fallback: %v", ctx.Err()), ctx.Err())
	}
	budget := func(kind string, _ int) time.Duration {
		if kind == "size" {
			return 10 * time.Millisecond
		}
		return 100 * time.Millisecond
	}

	parts, quarantines, err := buildAllPassingPartsWithPolicy(context.Background(), sources, t.TempDir(), strings.Repeat("f", 64), attempt, budget)
	if !errors.Is(err, context.DeadlineExceeded) || len(parts) != 0 || len(quarantines) != 0 {
		t.Fatalf("parts=%v quarantines=%v err=%v", parts, quarantines, err)
	}
}

func TestBuildAllPassingPartsLetsSlowPairReachStrictClassification(t *testing.T) {
	sources := makeSyntheticLocalSources(4)
	pairClassifications := 0
	attempt := func(ctx context.Context, candidate []LocalSource, _ string) (BuiltOutput, error) {
		ids := clipIDs(candidate)
		if equalInt64s(ids, []int64{2, 3}) {
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) < 4*time.Minute {
				return BuiltOutput{}, context.DeadlineExceeded
			}
			pairClassifications++
			return BuiltOutput{}, seamFailure(2, 3)
		}
		if len(candidate) == len(sources) || containsAdjacentClipIDs(candidate, 2, 3) {
			return BuiltOutput{}, seamFailure(2, 3)
		}
		return BuiltOutput{SourceCount: len(candidate)}, nil
	}

	parts, quarantines, err := buildAllPassingPartsWithAttempt(context.Background(), sources, t.TempDir(), strings.Repeat("f", 64), attempt)
	if err != nil {
		t.Fatal(err)
	}
	if pairClassifications != 2 {
		t.Fatalf("pair classifications=%d want=2", pairClassifications)
	}
	if len(parts) != 2 || parts[0].SourceCount != 2 || parts[1].SourceCount != 2 || len(quarantines) != 0 {
		t.Fatalf("strict seam split changed: parts=%+v quarantines=%+v", parts, quarantines)
	}
	if len(parts[0].SplitEvidence) != 1 || parts[0].SplitEvidence[0].ReasonCode != "media_sequence_mismatch" {
		t.Fatalf("strict seam evidence=%+v", parts[0].SplitEvidence)
	}
}

func TestBuildAllPassingPartsRejectsPairLocatorWithoutExactExtensionFailureAndCleansScratch(t *testing.T) {
	sources := makeSyntheticLocalSources(6)
	scratch := t.TempDir()
	attempt := func(_ context.Context, candidate []LocalSource, scratchDir string) (BuiltOutput, error) {
		ids := clipIDs(candidate)
		if equalInt64s(ids, []int64{1, 2, 3, 4, 5, 6}) || equalInt64s(ids, []int64{3, 4}) {
			return BuiltOutput{}, seamFailure(3, 4)
		}
		dir, err := os.MkdirTemp(scratchDir, "attempt-")
		if err != nil {
			return BuiltOutput{}, err
		}
		path := filepath.Join(dir, "joined.mp4")
		if err := os.WriteFile(path, []byte(fmt.Sprint(ids)), 0600); err != nil {
			return BuiltOutput{}, err
		}
		return BuiltOutput{Path: path, SourceCount: len(candidate)}, nil
	}

	parts, quarantines, err := buildAllPassingPartsWithAttempt(context.Background(), sources, scratch, strings.Repeat("f", 64), attempt)
	if !errors.Is(err, errMediaSplitNotIsolated) || len(parts) != 0 || len(quarantines) != 0 {
		t.Fatalf("parts=%v quarantines=%v err=%v", parts, quarantines, err)
	}
	entries, readErr := os.ReadDir(scratch)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		leaks := make([]string, 0, len(entries))
		for _, entry := range entries {
			payload, _ := os.ReadFile(filepath.Join(scratch, entry.Name(), "joined.mp4"))
			leaks = append(leaks, entry.Name()+":"+string(payload))
		}
		t.Fatalf("provisional scratch leaked: %v", leaks)
	}
}

func TestDiscardIsolatedBuildNeverRemovesScratchParent(t *testing.T) {
	parent, err := os.MkdirTemp(t.TempDir(), "attempt-parent-")
	if err != nil {
		t.Fatal(err)
	}
	scratch := filepath.Join(parent, "scratch")
	if err := os.Mkdir(scratch, 0700); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(parent, "joined.mp4")
	if err := os.WriteFile(outside, []byte("must survive"), 0600); err != nil {
		t.Fatal(err)
	}
	discardIsolatedBuild(BuiltOutput{Path: outside}, scratch)
	if payload, err := os.ReadFile(outside); err != nil || string(payload) != "must survive" {
		t.Fatalf("scratch parent was changed payload=%q err=%v", payload, err)
	}
}

func TestBuildAllPassingPartsPreservesRepeatedSingletonQuarantine(t *testing.T) {
	sources := makeSyntheticLocalSources(5)
	calls := make([][]int64, 0)
	attempt := func(_ context.Context, candidate []LocalSource, _ string) (BuiltOutput, error) {
		calls = append(calls, clipIDs(candidate))
		for _, source := range candidate {
			if source.ClipID == 3 {
				return BuiltOutput{}, deterministicFailure("corrupt_source_media", struct {
					ClipID int64 `json:"clip_id"`
				}{3}, errors.New("corrupt source"))
			}
		}
		return BuiltOutput{SourceCount: len(candidate)}, nil
	}

	parts, quarantines, err := buildAllPassingPartsWithAttempt(context.Background(), sources, t.TempDir(), strings.Repeat("f", 64), attempt)
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 2 || parts[0].SourceCount != 2 || parts[1].SourceCount != 2 || len(quarantines) != 1 || quarantines[0].Source.ClipID != 3 || quarantines[0].Evidence.RepeatCount != 2 {
		t.Fatalf("parts=%+v quarantines=%+v", parts, quarantines)
	}
	if countSpan(calls, []int64{3}) != 2 {
		t.Fatalf("singleton corruption was not proved twice: calls=%v", calls)
	}
}

func TestBuildAllPassingPartsRejectsChangingPairAndBoundaryProofs(t *testing.T) {
	for _, test := range []struct {
		name      string
		changing  []int64
		pairFails []int64
	}{
		{name: "pair repeat", changing: []int64{2, 3}, pairFails: []int64{2, 3}},
		{name: "boundary repeat", changing: []int64{1, 2, 3}, pairFails: []int64{2, 3}},
	} {
		t.Run(test.name, func(t *testing.T) {
			sources := makeSyntheticLocalSources(5)
			changingCalls := 0
			attempt := func(_ context.Context, candidate []LocalSource, _ string) (BuiltOutput, error) {
				ids := clipIDs(candidate)
				if equalInt64s(ids, []int64{1, 2, 3, 4, 5}) {
					return BuiltOutput{}, seamFailure(2, 3)
				}
				if equalInt64s(ids, test.changing) {
					changingCalls++
					return BuiltOutput{}, deterministicFailure("media_sequence_mismatch", struct {
						Version int `json:"version"`
					}{changingCalls}, errors.New("changing failure"))
				}
				if equalInt64s(ids, test.pairFails) {
					return BuiltOutput{}, seamFailure(2, 3)
				}
				return BuiltOutput{SourceCount: len(candidate)}, nil
			}

			parts, quarantines, err := buildAllPassingPartsWithAttempt(context.Background(), sources, t.TempDir(), strings.Repeat("f", 64), attempt)
			if !errors.Is(err, errMediaSplitNotIsolated) || len(parts) != 0 || len(quarantines) != 0 {
				t.Fatalf("parts=%v quarantines=%v err=%v", parts, quarantines, err)
			}
		})
	}
}

func TestBuildAllPassingPartsPropagatesInfrastructureFailureWithoutPartialPlan(t *testing.T) {
	sources := makeSyntheticLocalSources(4)
	attempt := func(_ context.Context, candidate []LocalSource, _ string) (BuiltOutput, error) {
		ids := clipIDs(candidate)
		if equalInt64s(ids, []int64{1, 2, 3, 4}) {
			return BuiltOutput{}, seamFailure(2, 3)
		}
		if equalInt64s(ids, []int64{1, 2}) {
			return BuiltOutput{}, syscall.ENOSPC
		}
		return BuiltOutput{SourceCount: len(candidate)}, nil
	}

	parts, quarantines, err := buildAllPassingPartsWithAttempt(context.Background(), sources, t.TempDir(), strings.Repeat("f", 64), attempt)
	if !errors.Is(err, syscall.ENOSPC) || len(parts) != 0 || len(quarantines) != 0 {
		t.Fatalf("parts=%v quarantines=%v err=%v", parts, quarantines, err)
	}
}

func TestBuildAllPassingPartsDoesNotLocalizeENOSPCAfterAttemptExpiry(t *testing.T) {
	sources := makeSyntheticLocalSources(4)
	attempt := func(ctx context.Context, candidate []LocalSource, _ string) (BuiltOutput, error) {
		if len(candidate) == len(sources) {
			<-ctx.Done()
			return BuiltOutput{}, syscall.ENOSPC
		}
		if containsAdjacentClipIDs(candidate, 2, 3) {
			return BuiltOutput{}, seamFailure(2, 3)
		}
		return BuiltOutput{SourceCount: len(candidate)}, nil
	}
	budget := func(kind string, _ int) time.Duration {
		if kind == "full" {
			return 10 * time.Millisecond
		}
		return 100 * time.Millisecond
	}

	parts, quarantines, err := buildAllPassingPartsWithPolicy(context.Background(), sources, t.TempDir(), strings.Repeat("f", 64), attempt, budget)
	if !errors.Is(err, syscall.ENOSPC) || len(parts) != 0 || len(quarantines) != 0 {
		t.Fatalf("parts=%v quarantines=%v err=%v", parts, quarantines, err)
	}
}

func TestBuildAllPassingPartsPreservesOutputCapPartition(t *testing.T) {
	for _, reason := range []string{"output_exceeds_put_cap", "lossless_normalization_expansion_cap"} {
		t.Run(reason, func(t *testing.T) {
			sources := makeSyntheticLocalSources(5)
			attempts := 0
			attempt := func(_ context.Context, candidate []LocalSource, _ string) (BuiltOutput, error) {
				attempts++
				if len(candidate) > 3 {
					return BuiltOutput{}, deterministicFailure(reason, struct {
						CandidateCount int `json:"candidate_count"`
					}{len(candidate)}, errors.New("bounded output cap"))
				}
				return BuiltOutput{SourceCount: len(candidate)}, nil
			}

			parts, quarantines, err := buildAllPassingPartsWithAttempt(context.Background(), sources, t.TempDir(), strings.Repeat("f", 64), attempt)
			if err != nil || len(quarantines) != 0 || len(parts) != 2 || parts[0].SourceCount != 3 || parts[1].SourceCount != 2 || attempts > 8 {
				t.Fatalf("parts=%+v quarantines=%+v attempts=%d err=%v", parts, quarantines, attempts, err)
			}
		})
	}
}

func TestSizeBoundPartitionDoesNotAssumePerPrefixExpansionIsMonotonic(t *testing.T) {
	sources := makeSyntheticLocalSources(5)
	attempt := func(_ context.Context, candidate []LocalSource, _ string) (BuiltOutput, error) {
		if candidate[0].ClipID == 1 && (len(candidate) == 5 || len(candidate) == 2) {
			return BuiltOutput{}, deterministicFailure("lossless_normalization_expansion_cap", struct {
				CandidateCount int `json:"candidate_count"`
			}{len(candidate)}, errors.New("candidate-specific expansion cap"))
		}
		return BuiltOutput{SourceCount: len(candidate)}, nil
	}
	parts, quarantines, err := buildAllPassingPartsWithAttempt(context.Background(), sources, t.TempDir(), strings.Repeat("f", 64), attempt)
	if err != nil || len(quarantines) != 0 || len(parts) != 2 || parts[0].SourceCount != 4 || parts[1].SourceCount != 1 {
		t.Fatalf("nonmonotonic expansion partition=%+v quarantines=%+v err=%v", parts, quarantines, err)
	}
}

func TestLosslessScratchReservationCoversRetainedPartAndBoundaryExtension(t *testing.T) {
	const sourceSize int64 = 100
	sources := makeSyntheticLocalSources(3)
	for i := range sources {
		sources[i].SizeBytes = sourceSize
		sources[i].SHA256 = strings.Repeat(string(rune('1'+i)), 64)
	}
	scratch := t.TempDir()
	for i := range sources {
		sources[i].Path = filepath.Join(scratch, fmt.Sprintf("source-%d.mp4", sources[i].ClipID))
		if err := os.WriteFile(sources[i].Path, make([]byte, sourceSize), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var peakScratchBytes int64
	attempt := func(_ context.Context, candidate []LocalSource, root string) (BuiltOutput, error) {
		attemptDir, err := os.MkdirTemp(root, "attempt-")
		if err != nil {
			return BuiltOutput{}, err
		}
		outputPath := filepath.Join(attemptDir, "joined.mp4")
		outputSize := int64(len(candidate)) * sourceSize * losslessNormalizationExpansionLimit
		if err := os.WriteFile(outputPath, make([]byte, outputSize), 0o600); err != nil {
			return BuiltOutput{}, err
		}
		var live int64
		if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil || entry.IsDir() {
				return walkErr
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			live += info.Size()
			return nil
		}); err != nil {
			return BuiltOutput{}, err
		}
		if live > peakScratchBytes {
			peakScratchBytes = live
		}
		if containsAdjacentClipIDs(candidate, 2, 3) {
			_ = os.RemoveAll(attemptDir)
			return BuiltOutput{}, seamFailure(2, 3)
		}
		return BuiltOutput{Path: outputPath, SizeBytes: outputSize, SourceCount: len(candidate)}, nil
	}
	parts, quarantines, err := buildAllPassingPartsWithAttempt(context.Background(), sources, scratch, strings.Repeat("f", 64), attempt)
	if err != nil || len(quarantines) != 0 || len(parts) != 2 {
		t.Fatalf("overlap fixture parts=%+v quarantines=%+v err=%v", parts, quarantines, err)
	}
	clips := make([]SourceClip, len(sources))
	for i := range clips {
		clips[i] = SourceClip{ClipID: sources[i].ClipID, Object: ObjectIdentity{SizeBytes: sourceSize}}
	}
	required, err := requiredScratchBytes(clips, true)
	if err != nil {
		t.Fatal(err)
	}
	reservedScratch := int64(required - ScratchSafetyMarginBytes)
	oldSingleSetReserve := int64(len(sources)) * sourceSize * (1 + losslessNormalizationExpansionLimit)
	if peakScratchBytes <= oldSingleSetReserve || reservedScratch < peakScratchBytes {
		t.Fatalf("peak scratch=%d old reserve=%d new reserve=%d", peakScratchBytes, oldSingleSetReserve, reservedScratch)
	}
}

func TestBuildAllPassingPartsOutputCapKeepsLargeWorkloadAndQuarantine(t *testing.T) {
	sources := makeSyntheticLocalSources(60)
	attempts := 0
	attempt := func(_ context.Context, candidate []LocalSource, _ string) (BuiltOutput, error) {
		attempts++
		if len(candidate) > 3 {
			return BuiltOutput{}, deterministicFailure("output_exceeds_put_cap", struct {
				CandidateCount int `json:"candidate_count"`
			}{len(candidate)}, errors.New("bounded output cap"))
		}
		for _, source := range candidate {
			if source.ClipID == 10 {
				return BuiltOutput{}, deterministicFailure("corrupt_source_media", struct {
					ClipID int64 `json:"clip_id"`
				}{10}, errors.New("corrupt source"))
			}
		}
		return BuiltOutput{SourceCount: len(candidate)}, nil
	}

	parts, quarantines, err := buildAllPassingPartsWithAttempt(context.Background(), sources, t.TempDir(), strings.Repeat("f", 64), attempt)
	if err != nil || len(parts) != 20 || len(quarantines) != 1 || quarantines[0].Source.ClipID != 10 {
		t.Fatalf("parts=%d quarantines=%+v attempts=%d err=%v", len(parts), quarantines, attempts, err)
	}
	if attempts <= 6*len(sources)+2 {
		t.Fatalf("fixture did not exercise exhaustive longest-prefix proof: attempts=%d", attempts)
	}
}

func makeSyntheticLocalSources(count int) []LocalSource {
	sources := make([]LocalSource, count)
	for i := range sources {
		sources[i] = LocalSource{ClipID: int64(i + 1), SourceClaimSHA256: strings.Repeat("a", 64)}
	}
	return sources
}

func makeSyntheticLocalSourcesWithIdentity(t *testing.T, count int) []LocalSource {
	t.Helper()
	dir := t.TempDir()
	sources := makeSyntheticLocalSources(count)
	for i := range sources {
		path := filepath.Join(dir, fmt.Sprintf("source-%03d.mp4", i))
		if err := os.WriteFile(path, []byte(fmt.Sprintf("source-%03d", i)), 0600); err != nil {
			t.Fatal(err)
		}
		size, sha, err := localIdentity(path)
		if err != nil {
			t.Fatal(err)
		}
		sources[i].Path = path
		sources[i].SizeBytes = size
		sources[i].SHA256 = sha
	}
	return sources
}

func clipIDs(sources []LocalSource) []int64 {
	ids := make([]int64, len(sources))
	for i := range sources {
		ids[i] = sources[i].ClipID
	}
	return ids
}

func containsAdjacentClipIDs(sources []LocalSource, left, right int64) bool {
	for i := 0; i+1 < len(sources); i++ {
		if sources[i].ClipID == left && sources[i+1].ClipID == right {
			return true
		}
	}
	return false
}

func seamFailure(left, right int64) error {
	return deterministicFailure("media_sequence_mismatch", struct {
		Left  int64 `json:"left"`
		Right int64 `json:"right"`
	}{left, right}, errors.New("repeatable adjacent seam failure"))
}

func equalInt64s(left, right []int64) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func countSpan(calls [][]int64, want []int64) int {
	count := 0
	for _, call := range calls {
		if equalInt64s(call, want) {
			count++
		}
	}
	return count
}

func TestVerifyJoinedMediaAttributesSourceProbeFailure(t *testing.T) {
	dir := t.TempDir()
	output := makeMediaClip(t, dir, "output.mp4", 440, false)
	output.ClipID = 1
	badPath := filepath.Join(dir, "bad.mp4")
	if err := os.WriteFile(badPath, []byte("not media"), 0600); err != nil {
		t.Fatal(err)
	}
	size, sha, err := localIdentity(badPath)
	if err != nil {
		t.Fatal(err)
	}
	bad := LocalSource{ClipID: 928, Path: badPath, SizeBytes: size, SHA256: sha, SourceClaimSHA256: sha}
	_, err = VerifyJoinedMedia(context.Background(), []LocalSource{bad}, output.Path)
	if err == nil || !strings.Contains(err.Error(), "probe source ordinal=1 clip_id=928") {
		t.Fatalf("source probe failure was not attributed: %v", err)
	}
	var deterministic *deterministicMediaError
	if !errors.As(err, &deterministic) || deterministic.code != "corrupt_source_media" {
		t.Fatalf("source attribution changed deterministic classification: %v", err)
	}
}

func TestFreezeDownloadedAudioAttributesCancellation(t *testing.T) {
	dir := t.TempDir()
	local := makeMediaClip(t, dir, "source.mp4", 440, false)
	local.ClipID = 928
	source := testSource(928, time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, _, _, err := freezeDownloadedAudioForPreflight(ctx, []SourceClip{source}, []LocalSource{local}, strings.Repeat("f", 64))
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "freeze source ordinal=1 clip_id=928") {
		t.Fatalf("freeze cancellation was not attributed: %v", err)
	}
}

func TestBuildLargestPassingPrefixRejectsAACPrimingTotalChange(t *testing.T) {
	dir := t.TempDir()
	first := makeMediaClip(t, dir, "one.mp4", 440, true)
	second := makeMediaClip(t, dir, "two.mp4", 880, true)
	first.ClipID, second.ClipID = 1, 2
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	built, err := BuildLargestPassingPrefix(ctx, []LocalSource{first, second}, dir, strings.Repeat("f", 64))
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(built.Path)
	if built.SourceCount != 1 || len(built.SplitEvidence) == 0 || built.SplitEvidence[0].RepeatCount != 2 || !lowerHex64(built.SplitEvidence[0].EvidenceSHA256) {
		t.Fatalf("AAC decoded-total mismatch was accepted across seam: %+v", built.Verification)
	}
}

func TestAACDiscardPaddingNormalizationIsDefaultOff(t *testing.T) {
	t.Setenv(audioPaddingFeatureEnv, "")
	if audioPaddingNormalizationEnabled() {
		t.Fatal("AAC padding normalization unexpectedly enabled by default")
	}
	t.Setenv(audioPaddingFeatureEnv, "1")
	if !audioPaddingNormalizationEnabled() {
		t.Fatal("explicit AAC padding normalization gate was ignored")
	}
}
func TestAACPaddingModeBuildsAndRebuildsExactRow1923WithCreationGateOff(t *testing.T) {
	fixtureDir := os.Getenv("STOARAMA_ROW1923_FIXTURE_DIR")
	if fixtureDir == "" {
		t.Skip("set STOARAMA_ROW1923_FIXTURE_DIR to the preserved exact row1923 fixture")
	}
	sources := make([]LocalSource, 2)
	for i, fixture := range []struct {
		name string
		id   int64
		sha  string
	}{
		{"clip-435622.mp4", 435622, "2c09397a529e12a6ac278240e953573a8d76f37c3be5437fbf0144aeb9b0b134"},
		{"clip-435667.mp4", 435667, "fce1715a807030246c060780d54c1027a2d457c0239eb7baae090bacb3df4efa"},
	} {
		mediaPath := filepath.Join(fixtureDir, fixture.name)
		size, sha, err := localIdentity(mediaPath)
		if err != nil || sha != fixture.sha {
			t.Fatalf("row1923 fixture identity differs: name=%s sha=%s err=%v", fixture.name, sha, err)
		}
		_, _, contract, err := probeMediaMetadata(context.Background(), mediaPath)
		if err != nil {
			t.Fatal(err)
		}
		sources[i] = LocalSource{ClipID: fixture.id, Path: mediaPath, SizeBytes: size, SHA256: sha, SourceClaimSHA256: sha, AudioContract: contract}
	}
	dir := t.TempDir()
	t.Setenv(audioPaddingFeatureEnv, "1")
	built, err := BuildSealedOutput(context.Background(), sources, filepath.Join(dir, "first-build"))
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(built.Path)
	if built.SHA256 != "148c9293c2dfc989881692e497432fce1e643cac5f1879322857393b370dbeb1" || built.Verification.AcceptanceMode != audioPaddingAcceptanceMode || built.Verification.AudioPaddingNormalization == nil || built.Verification.AudioPaddingNormalization.DecodedAudioSurplusSamples != 662 {
		t.Fatalf("exact row1923 fixture did not use bounded mode: sha=%s verification=%+v", built.SHA256, built.Verification)
	}
	if err := validateAudioPaddingNormalizationVerification(built.Verification); err != nil {
		t.Fatalf("fresh row1923 verification did not revalidate: %v", err)
	}
	t.Setenv(audioPaddingFeatureEnv, "")
	rebuilt, err := BuildSealedOutputForVerification(context.Background(), sources, filepath.Join(dir, "rebuild"), built.Verification)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(rebuilt.Path)
	if rebuilt.SizeBytes != built.SizeBytes || rebuilt.SHA256 != built.SHA256 || !sameCanonical([]Verification{rebuilt.Verification}, []Verification{built.Verification}) {
		t.Fatalf("gate-off sealed rebuild differed: first=%+v rebuilt=%+v", built, rebuilt)
	}
}

func TestProbeAACStreamProofBindsCodecFramesAndTrimEvidence(t *testing.T) {
	t.Setenv(audioPaddingFeatureEnv, "1")
	clip := makeMediaClip(t, t.TempDir(), "aac-proof.mp4", 440, true)
	fingerprint, err := probeMedia(context.Background(), clip.Path)
	if err != nil {
		t.Fatal(err)
	}
	if len(fingerprint.AudioContracts) != 1 {
		t.Fatal("AAC contract is missing")
	}
	contract := fingerprint.AudioContracts[0]
	proof, err := probeAACPaddingContract(context.Background(), clip.Path, contract)
	if err != nil {
		t.Fatal(err)
	}
	if proof.Profile != "LC" || !lowerHex64(proof.ExtradataSHA256) || proof.PacketCount <= 0 || proof.MaxFrameSamples != 1024 || proof.TerminalPacketDurationSamples <= 0 || proof.TerminalPacketDurationSamples > proof.MaxFrameSamples {
		t.Fatalf("AAC stream proof differs: contract=%+v proof=%+v", contract, proof)
	}
	var skip, discard, terminalDiscard, lastOrdinal int64
	for _, event := range proof.TrimEvents {
		if event.PacketOrdinal <= lastOrdinal || event.PacketOrdinal > proof.PacketCount || event.SkipSamples < 0 || event.DiscardPadding < 0 {
			t.Fatalf("AAC trim event is invalid: %+v", proof)
		}
		lastOrdinal = event.PacketOrdinal
		skip += event.SkipSamples
		discard += event.DiscardPadding
		if event.PacketOrdinal == proof.PacketCount {
			terminalDiscard = event.DiscardPadding
		}
	}
	if skip+discard <= 0 || skip != contract.SkipSamples || discard != contract.DiscardPadding {
		t.Fatalf("AAC trim did not match observed stream facts: contract=%+v proof=%+v", contract, proof)
	}
	if discard > 0 && (terminalDiscard != discard || proof.LastFrameSamples+discard != proof.MaxFrameSamples) {
		t.Fatalf("terminal AAC discard was not position/frame bounded: contract=%+v proof=%+v", contract, proof)
	}
}

func TestAACDiscardPaddingTimingTransformAdjustsOnlyTerminalSeamPacket(t *testing.T) {
	packet := func(pts, duration int64) aacPacketTiming {
		return aacPacketTiming{PTS: pts, DTS: pts, Duration: duration, TimeBaseNum: 1, TimeBaseDen: 44100}
	}
	first := AudioSequenceContract{SampleRate: 44100, DiscardPadding: 662, aacProof: &aacStreamProof{PacketTimings: []aacPacketTiming{packet(0, 1024), packet(1024, 1024)}}}
	second := AudioSequenceContract{SampleRate: 44100, aacProof: &aacStreamProof{PacketTimings: []aacPacketTiming{packet(0, 1024), packet(1024, 1024)}}}
	output := AudioSequenceContract{SampleRate: 44100, aacProof: &aacStreamProof{PacketTimings: []aacPacketTiming{packet(0, 1024), packet(1024, 1686), packet(2710, 1024), packet(3734, 1024)}}}
	want, err := aacPacketTimingSHA([]AudioSequenceContract{first, second}, aacDiscardPaddingPolicyVersion)
	if err != nil {
		t.Fatal(err)
	}
	got, err := aacPacketTimingSHA([]AudioSequenceContract{output}, "")
	if err != nil || got != want {
		t.Fatalf("exact seam timing transform differed: got=%s want=%s err=%v", got, want, err)
	}
	output.aacProof.PacketTimings[2].PTS++
	if changed, _ := aacPacketTimingSHA([]AudioSequenceContract{output}, ""); changed == want {
		t.Fatal("internal packet timing drift preserved the seam transform hash")
	}
}

func TestAACDiscardPaddingTimingTransformUsesNextSourceStartOffset(t *testing.T) {
	packetCounts := []int{2584, 2583, 2584}
	startAndDiscard := []int64{970, 1014, 0}
	contracts := make([]AudioSequenceContract, len(packetCounts))
	for sourceIndex, packetCount := range packetCounts {
		packets := make([]aacPacketTiming, packetCount)
		for packetIndex := range packets {
			pts := startAndDiscard[sourceIndex] + int64(packetIndex)*aacLCFrameSamples
			packets[packetIndex] = aacPacketTiming{PTS: pts, DTS: pts, Duration: aacLCFrameSamples, TimeBaseNum: 1, TimeBaseDen: 44100}
		}
		contracts[sourceIndex] = AudioSequenceContract{
			SampleRate:     44100,
			DiscardPadding: startAndDiscard[sourceIndex],
			aacProof:       &aacStreamProof{PacketTimings: packets},
		}
	}

	outputPackets := make([]aacPacketTiming, 0, 2584+2583+2584)
	var base int64
	for sourceIndex, contract := range contracts {
		firstDTS := contract.aacProof.PacketTimings[0].DTS
		for packetIndex, packet := range contract.aacProof.PacketTimings {
			duration := packet.Duration
			if sourceIndex+1 < len(contracts) && packetIndex+1 == len(contract.aacProof.PacketTimings) {
				duration += startAndDiscard[sourceIndex+1]
			}
			outputPackets = append(outputPackets, aacPacketTiming{
				PTS: packet.PTS - firstDTS + base, DTS: packet.DTS - firstDTS + base,
				Duration: duration, TimeBaseNum: 1, TimeBaseDen: 44100,
			})
		}
		base += int64(len(contract.aacProof.PacketTimings)) * aacLCFrameSamples
		if sourceIndex+1 < len(contracts) {
			base += startAndDiscard[sourceIndex+1]
		}
	}
	output := AudioSequenceContract{SampleRate: 44100, aacProof: &aacStreamProof{PacketTimings: outputPackets}}
	want, err := aacPacketTimingSHA(contracts, aacVariablePaddingPolicyVersion)
	if err != nil {
		t.Fatal(err)
	}
	got, err := aacPacketTimingSHA([]AudioSequenceContract{output}, "")
	if err != nil || got != want {
		t.Fatalf("row1924 variable-offset timing transform differs: got=%s want=%s err=%v", got, want, err)
	}
}

func TestAACDiscardWrapBoundariesMatchRow1924(t *testing.T) {
	sources := aacTestSources(row1924AACDiscardSamples())
	boundaries, wraps := aacDiscardWrapBoundaries(sources)
	if !slices.Equal(boundaries, []int{37}) || len(wraps) != 1 || wraps[0].PreviousDiscard != 1014 || wraps[0].NextDiscard != 0 || wraps[0].MaximumTailLossBound != 1014 {
		t.Fatalf("row1924 AAC partition differs: boundaries=%v wraps=%+v", boundaries, wraps)
	}
	t.Setenv(audioPaddingFeatureEnv, "1")
	err := aacDiscardPaddingWrapFailure(context.Background(), sources)
	var failure *deterministicMediaError
	if !errors.As(err, &failure) || failure.code != "aac_discard_padding_wrap" || !lowerHex64(failure.evidenceSHA256) {
		t.Fatalf("row1924 wrap was not deterministic: %v", err)
	}
	t.Setenv(audioPaddingFeatureEnv, "")
	if err := aacDiscardPaddingWrapFailure(context.Background(), sources); err != nil {
		t.Fatalf("default-off AAC policy changed: %v", err)
	}
}

func row1924AACDiscardSamples() []int64 {
	var discards []int64
	for _, group := range []struct {
		discard int64
		count   int
	}{
		{441, 3}, {485, 3}, {529, 3}, {573, 2}, {617, 3}, {662, 3}, {706, 3}, {750, 2},
		{794, 3}, {838, 3}, {882, 3}, {926, 3}, {970, 2}, {1014, 1}, {0, 3}, {44, 2},
		{88, 3}, {132, 3}, {176, 3}, {221, 2}, {265, 3}, {309, 3}, {353, 1},
	} {
		for range group.count {
			discards = append(discards, group.discard)
		}
	}
	return discards
}

func aacTestSources(discards []int64) []LocalSource {
	sources := make([]LocalSource, len(discards))
	for i, discard := range discards {
		sources[i] = LocalSource{ClipID: int64(i + 1), AudioContract: &AudioSequenceContract{
			CodecName: "aac", SampleRate: 44100, Channels: 2, ChannelLayout: "stereo",
			DiscardPadding: discard, EditListKind: "decoder_timeline_v1", EditListSHA256: strings.Repeat("a", 64),
		}}
	}
	return sources
}

func TestAACDiscardWrapPrepartitionBuildsRow1924As37And23WithoutPairSweep(t *testing.T) {
	t.Setenv(audioPaddingFeatureEnv, "1")
	sources := aacTestSources(row1924AACDiscardSamples())
	attempts := make([][]int64, 0)
	successes := make([][]int64, 0, 2)
	attempt := func(ctx context.Context, candidate []LocalSource, _ string) (BuiltOutput, error) {
		attempts = append(attempts, clipIDs(candidate))
		if err := aacDiscardPaddingWrapFailure(ctx, candidate); err != nil {
			return BuiltOutput{}, err
		}
		successes = append(successes, clipIDs(candidate))
		return BuiltOutput{SourceCount: len(candidate)}, nil
	}
	parts, quarantines, err := buildAllPassingPartsWithAttempt(context.Background(), sources, t.TempDir(), strings.Repeat("f", 64), attempt)
	if err != nil || len(quarantines) != 0 || len(parts) != 2 || parts[0].SourceCount != 37 || parts[1].SourceCount != 23 {
		t.Fatalf("row1924 prepartition differs: parts=%+v quarantines=%+v err=%v", parts, quarantines, err)
	}
	if len(attempts) != 6 || len(parts[0].SplitEvidence) != 1 || parts[0].SplitEvidence[0].RepeatCount != 2 || parts[0].SplitEvidence[0].ReasonCode != "aac_discard_padding_wrap" || len(parts[0].SplitEvidence[0].CandidateClipIDs) != 38 {
		t.Fatalf("row1924 prepartition evidence differs: attempts=%v evidence=%+v", attempts, parts[0].SplitEvidence)
	}
	wantFirst, wantSecond := clipIDs(sources[:37]), clipIDs(sources[37:])
	if len(successes) != 2 || !slices.Equal(successes[0], wantFirst) || !slices.Equal(successes[1], wantSecond) {
		t.Fatalf("row1924 successful candidates differ: got=%v want=%v,%v", successes, wantFirst, wantSecond)
	}
	wantAttempts := [][]int64{clipIDs(sources), clipIDs(sources), wantFirst, clipIDs(sources[:38]), clipIDs(sources[:38]), wantSecond}
	for i := range wantAttempts {
		if !slices.Equal(attempts[i], wantAttempts[i]) {
			t.Fatalf("row1924 attempt %d differs: got=%v want=%v", i, attempts[i], wantAttempts[i])
		}
	}
	if !slices.Equal(parts[0].SplitEvidence[0].CandidateClipIDs, clipIDs(sources[:38])) {
		t.Fatalf("row1924 boundary evidence candidate differs: %+v", parts[0].SplitEvidence[0])
	}
	for _, candidate := range attempts {
		if len(candidate) == 2 {
			t.Fatalf("row1924 used adjacent pair sweep: %v", attempts)
		}
	}
}

func TestAACDiscardWrapPrepartitionFallsBackWithinMonotoneSegment(t *testing.T) {
	t.Setenv(audioPaddingFeatureEnv, "1")
	sources := aacTestSources([]int64{100, 200, 0, 100})
	attempt := func(ctx context.Context, candidate []LocalSource, _ string) (BuiltOutput, error) {
		if err := aacDiscardPaddingWrapFailure(ctx, candidate); err != nil {
			return BuiltOutput{}, err
		}
		if len(candidate) > 1 && candidate[0].ClipID == 1 && candidate[len(candidate)-1].ClipID == 2 {
			return BuiltOutput{}, deterministicFailure("media_sequence_mismatch", struct {
				First int64 `json:"first"`
				Last  int64 `json:"last"`
			}{1, 2}, errors.New("repeatable nested incompatibility"))
		}
		return BuiltOutput{SourceCount: len(candidate)}, nil
	}
	parts, quarantines, err := buildAllPassingPartsWithAttempt(context.Background(), sources, t.TempDir(), strings.Repeat("f", 64), attempt)
	if err != nil || len(quarantines) != 0 || len(parts) != 3 || parts[0].SourceCount != 1 || parts[1].SourceCount != 1 || parts[2].SourceCount != 2 {
		t.Fatalf("nested AAC isolation did not continue: parts=%+v quarantines=%+v err=%v", parts, quarantines, err)
	}
}

func TestAACVariablePaddingVerificationSeparatesSeamAndDecodedDeltas(t *testing.T) {
	verification := rowVariableAACPaddingVerificationFixture(t)
	if err := validatePassedVerification(verification); err != nil {
		t.Fatalf("variable AAC padding evidence was rejected: %v", err)
	}
	verification.AudioPaddingNormalization.DecodeTimelineSpanDeltaSeconds = "1/147"
	if err := validatePassedVerification(verification); err == nil {
		t.Fatal("variable AAC timing used non-final decoded padding instead of next-source offsets")
	}
}

func TestAACVariablePaddingVerificationRejectsDecreasingOffsets(t *testing.T) {
	verification := rowVariableAACPaddingVerificationFixture(t)
	source := &verification.AudioPaddingNormalization.Sources[2]
	discard := int64(50)
	source.FirstPacketPTSSamples, source.FirstPacketDTSSamples = &discard, &discard
	source.LastDecodedFrameSamples = 1024 - discard
	source.TrimEvents[0].DiscardPadding = discard
	verification.SourceFingerprint.AudioContracts[2].DiscardPadding = discard
	orderedSHA, _, err := stitchcert.CanonicalSHA(verification.AudioPaddingNormalization.Sources)
	if err != nil {
		t.Fatal(err)
	}
	verification.AudioPaddingNormalization.OrderedSourceEvidenceSHA256 = orderedSHA
	if err := validateAudioPaddingNormalizationFacts(verification.SourceFingerprint, verification.OutputFingerprint, verification.AudioPaddingNormalization); err == nil || !strings.Contains(err.Error(), "source offsets decrease") {
		t.Fatalf("decreasing variable AAC offsets did not reach the monotonicity guard: %v", err)
	}
}

func TestAACVariablePaddingVideoHoldMustBeCoherentAndBounded(t *testing.T) {
	verification := rowVariableAACPaddingVerificationFixture(t)
	for name, mutate := range map[string]func(*Verification){
		"first timestamp": func(v *Verification) { v.OutputFingerprint.Tracks["video"].FirstPacketDTSSeconds = "1/30" },
		"negative hold": func(v *Verification) {
			v.OutputFingerprint.Tracks["video"].PacketDurationSeconds = "9"
		},
		"incoherent last timestamp": func(v *Verification) {
			v.OutputFingerprint.Tracks["video"].LastPacketPTSSeconds = "9"
		},
		"excessive hold": func(v *Verification) {
			video := v.OutputFingerprint.Tracks["video"]
			video.PacketDurationSeconds, video.DecodeTimelineSpanSeconds = "14", "14"
			video.LastPacketPTSSeconds, video.LastPacketDTSSeconds = "13", "13"
		},
	} {
		t.Run(name, func(t *testing.T) {
			raw, err := json.Marshal(verification)
			if err != nil {
				t.Fatal(err)
			}
			var changed Verification
			if err := json.Unmarshal(raw, &changed); err != nil {
				t.Fatal(err)
			}
			mutate(&changed)
			if err := validatePassedVerification(changed); err == nil {
				t.Fatal("invalid variable AAC video hold passed")
			}
		})
	}
}

func TestDecodedFramePixelIdentityExcludesDuration(t *testing.T) {
	firstSequence, secondSequence := sha256.New(), sha256.New()
	firstPixels, secondPixels := sha256.New(), sha256.New()
	frameSHA := strings.Repeat("a", 64)
	writeDecodedFrameIdentities(firstSequence, firstPixels, "1", "1382400", frameSHA)
	writeDecodedFrameIdentities(secondSequence, secondPixels, "2", "1382400", frameSHA)
	if bytes.Equal(firstSequence.Sum(nil), secondSequence.Sum(nil)) {
		t.Fatal("duration-inclusive decoded identities unexpectedly match")
	}
	if !bytes.Equal(firstPixels.Sum(nil), secondPixels.Sum(nil)) {
		t.Fatal("duration-only change altered decoded pixel identity")
	}
	changedSize, changedPixel := sha256.New(), sha256.New()
	writeDecodedFrameIdentities(sha256.New(), changedSize, "1", "1382401", frameSHA)
	writeDecodedFrameIdentities(sha256.New(), changedPixel, "1", "1382400", strings.Repeat("b", 64))
	if bytes.Equal(firstPixels.Sum(nil), changedSize.Sum(nil)) || bytes.Equal(firstPixels.Sum(nil), changedPixel.Sum(nil)) {
		t.Fatal("decoded size or pixel change reused the pixel identity")
	}
	firstOrder, secondOrder := sha256.New(), sha256.New()
	writeDecodedFrameIdentities(sha256.New(), firstOrder, "1", "1", strings.Repeat("a", 64))
	writeDecodedFrameIdentities(sha256.New(), firstOrder, "1", "1", strings.Repeat("b", 64))
	writeDecodedFrameIdentities(sha256.New(), secondOrder, "1", "1", strings.Repeat("b", 64))
	writeDecodedFrameIdentities(sha256.New(), secondOrder, "1", "1", strings.Repeat("a", 64))
	if bytes.Equal(firstOrder.Sum(nil), secondOrder.Sum(nil)) {
		t.Fatal("reordered decoded pixels reused the pixel identity")
	}
}

func TestDecodedVideoSHAForAACPolicyKeepsV1Strict(t *testing.T) {
	want := decodedIdentity{sha: strings.Repeat("a", 64), pixelSHA: strings.Repeat("c", 64)}
	got := decodedIdentity{sha: strings.Repeat("b", 64), pixelSHA: want.pixelSHA}
	if sha, err := decodedVideoSHAForAACPolicy(aacVariablePaddingPolicyVersion, want, got); err != nil || sha != want.pixelSHA {
		t.Fatalf("v2 duration-only transform rejected: sha=%s err=%v", sha, err)
	}
	if _, err := decodedVideoSHAForAACPolicy(aacDiscardPaddingPolicyVersion, want, got); err == nil {
		t.Fatal("v1 accepted duration-inclusive decoded video mismatch")
	}
	got.pixelSHA = strings.Repeat("d", 64)
	if _, err := decodedVideoSHAForAACPolicy(aacVariablePaddingPolicyVersion, want, got); err == nil {
		t.Fatal("v2 accepted decoded pixel mismatch")
	}
}

func TestAACVariablePaddingZeroOffsetSerializesEmptyTrimEvents(t *testing.T) {
	evidence := aacSourcePaddingEvidence(&aacStreamProof{
		PacketCount: 2, MaxFrameSamples: 1024, LastFrameSamples: 1024,
		TerminalPacketDurationSamples: 1024,
	})
	zero := int64(0)
	evidence.ClipID, evidence.SourceClaimSHA256 = 1, strings.Repeat("c", 64)
	evidence.FirstPacketPTSSamples, evidence.FirstPacketDTSSamples = &zero, &zero
	raw, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte(`"first_packet_pts_samples":0,"first_packet_dts_samples":0`)) || !bytes.Contains(raw, []byte(`"trim_events":[]`)) {
		t.Fatalf("v2 zero-offset source evidence is not canonical NAS JSON: %s", raw)
	}
	legacy, err := json.Marshal(aacSourcePaddingEvidence(&aacStreamProof{
		PacketCount: 2, MaxFrameSamples: 1024, LastFrameSamples: 1024,
		TerminalPacketDurationSamples: 1024,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(legacy, []byte(`"trim_events":[]`)) {
		t.Fatalf("zero-event serialization is not accepted NAS JSON: %s", legacy)
	}
}

func rowVariableAACPaddingVerificationFixture(t *testing.T) Verification {
	t.Helper()
	discards := []int64{100, 200, 300}
	sourceEvidence := make([]AACSourcePaddingEvidence, len(discards))
	contracts := make([]AudioSequenceContract, len(discards))
	for i, discard := range discards {
		pts, dts := discard, discard
		sourceEvidence[i] = AACSourcePaddingEvidence{
			ClipID: int64(i + 1), SourceClaimSHA256: strings.Repeat(string(rune('c'+i)), 64),
			FirstPacketPTSSamples: &pts, FirstPacketDTSSamples: &dts,
			PacketCount: 2, MaxDecodedFrameSamples: 1024, LastDecodedFrameSamples: 1024 - discard,
			TerminalPacketDurationSamples: 1024,
			TrimEvents:                    []AACTrimEventEvidence{{PacketOrdinal: 2, DiscardPadding: discard}},
		}
		contracts[i] = AudioSequenceContract{
			CodecName: "aac", SampleRate: 44100, Channels: 2, ChannelLayout: "stereo", DiscardPadding: discard,
			EditListKind: "decoder_timeline_v1", EditListSHA256: strings.Repeat("1", 64),
		}
	}
	orderedSourceSHA, _, err := stitchcert.CanonicalSHA(sourceEvidence)
	if err != nil {
		t.Fatal(err)
	}
	videoSource := &TrackFingerprint{MediaType: "video", PacketCount: 6, PacketChainSHA256: strings.Repeat("2", 64), PacketTimingSHA256: strings.Repeat("3", 64), PacketTimeBases: []string{"1/90000"}, FirstPacketPTSSeconds: "0", LastPacketPTSSeconds: "9", FirstPacketDTSSeconds: "0", LastPacketDTSSeconds: "9", PacketDurationSeconds: "10", DecodeTimelineSpanSeconds: "10", DecodedFrames: 6, TimestampStatus: "source_clips_independent"}
	videoOutput := *videoSource
	videoOutput.TimestampStatus = "monotonic"
	videoOutput.LastPacketPTSSeconds, videoOutput.LastPacketDTSSeconds = "55/6", "55/6"
	videoOutput.PacketDurationSeconds, videoOutput.DecodeTimelineSpanSeconds = "61/6", "61/6"
	audioSource := &TrackFingerprint{MediaType: "audio", PacketCount: 6, PacketChainSHA256: strings.Repeat("4", 64), PacketTimingSHA256: strings.Repeat("5", 64), PacketTimeBases: []string{"1/44100"}, FirstPacketPTSSeconds: "1/441", LastPacketPTSSeconds: "9", FirstPacketDTSSeconds: "1/441", LastPacketDTSSeconds: "9", PacketDurationSeconds: "10", DecodeTimelineSpanSeconds: "10", DecodedFrames: 6, DecodedSamples: 5544, CodecProfile: "LC", CodecExtradataSHA256: strings.Repeat("6", 64), AACPaddingNormalizedTimingSHA256: strings.Repeat("7", 64), TimestampStatus: "source_clips_independent"}
	audioOutput := *audioSource
	audioOutput.PacketTimingSHA256 = strings.Repeat("7", 64)
	audioOutput.AACPaddingNormalizedTimingSHA256 = ""
	audioOutput.LastPacketPTSSeconds, audioOutput.LastPacketDTSSeconds = "3974/441", "3974/441"
	audioOutput.PacketDurationSeconds, audioOutput.DecodeTimelineSpanSeconds = "4415/441", "4415/441"
	audioOutput.DecodedSamples, audioOutput.TimestampStatus = 5844, "monotonic"
	return Verification{
		Status: "passed", AcceptanceMode: audioPaddingAcceptanceMode,
		AudioPaddingNormalization: &AudioPaddingNormalizationEvidence{
			PolicyVersion: aacVariablePaddingPolicyVersion, Sources: sourceEvidence,
			Output:                      AACSourcePaddingEvidence{PacketCount: 6, MaxDecodedFrameSamples: 1024, LastDecodedFrameSamples: 724, TerminalPacketDurationSamples: 1024, TrimEvents: []AACTrimEventEvidence{{PacketOrdinal: 6, DiscardPadding: 300}}},
			OrderedSourceEvidenceSHA256: orderedSourceSHA, DecodedAudioSurplusSamples: 300,
			PacketDurationDeltaSeconds: "5/441", DecodeTimelineSpanDeltaSeconds: "5/441", AudioContentStatus: audioPaddingNotSampleExactStatus,
		},
		PacketPayloadOrderStatus: "passed", DecodedFrameSequenceStatus: "passed", DecodedFrameTotalsStatus: "passed", DecodedAudioTotalsStatus: audioPaddingDecodedTotalsStatus, OutputTimestampStatus: "passed", StrictDecodeStatus: "passed",
		SourceFingerprint: MediaFingerprint{DurationSeconds: 10, Tracks: map[string]*TrackFingerprint{"video": videoSource, "audio": audioSource}, DecodedVideoSHA256: strings.Repeat("a", 64), AudioContracts: contracts, EffectiveAudioBytes: 44352, EffectiveAudioFrames: 5544, EffectiveAudioSHA256: strings.Repeat("8", 64)},
		OutputFingerprint: MediaFingerprint{DurationSeconds: 10, Tracks: map[string]*TrackFingerprint{"video": &videoOutput, "audio": &audioOutput}, DecodedVideoSHA256: strings.Repeat("a", 64), AudioContracts: []AudioSequenceContract{contracts[2]}, EffectiveAudioBytes: 46752, EffectiveAudioFrames: 5844, EffectiveAudioSHA256: strings.Repeat("9", 64)},
	}
}

func TestAACDiscardPaddingMalformedEvidenceRejectsWithoutPanic(t *testing.T) {
	verification := row1923AACPaddingVerificationFixture(t)
	verification.OutputFingerprint.Tracks["audio"] = nil
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("malformed AAC evidence panicked: %v", recovered)
		}
	}()
	if err := validateAudioPaddingNormalizationFacts(verification.SourceFingerprint, verification.OutputFingerprint, verification.AudioPaddingNormalization); err == nil {
		t.Fatal("malformed AAC evidence passed")
	}
}
func TestAACPacketSideDataRejectsUnknownAndDuplicateEvidence(t *testing.T) {
	valid := map[string]string{
		"pts": "0", "dts": "0", "duration": "1024",
		"side_datum/skip_samples:side_data_type":  "Skip Samples",
		"side_datum/skip_samples:skip_samples":    "0",
		"side_datum/skip_samples:discard_padding": "662",
		"side_datum/skip_samples:skip_reason":     "0",
		"side_datum/skip_samples:discard_reason":  "0",
	}
	if skip, discard, err := parseAACPacketSideData(valid); err != nil || skip != 0 || discard != 662 {
		t.Fatalf("valid terminal trim rejected: skip=%d discard=%d err=%v", skip, discard, err)
	}
	unknown := make(map[string]string, len(valid)+1)
	for key, value := range valid {
		unknown[key] = value
	}
	unknown["side_datum/new_extradata:side_data_type"] = "New Extradata"
	if _, _, err := parseAACPacketSideData(unknown); err == nil {
		t.Fatal("decoder-affecting packet side data passed")
	}
	if _, err := parseUniqueCompactFields([]string{"pts=0", "pts=1"}); err == nil {
		t.Fatal("duplicate packet evidence passed")
	}
}

func TestAACPaddingSourceClaimsBindEveryEvidenceOrdinal(t *testing.T) {
	verification := row1923AACPaddingVerificationFixture(t)
	sources := []SourceClip{{ClipID: 435622}, {ClipID: 435667}}
	for i := range sources {
		claimSHA, _, err := sourceClaimSHA([]SourceClip{sources[i]})
		if err != nil {
			t.Fatal(err)
		}
		verification.AudioPaddingNormalization.Sources[i].ClipID = sources[i].ClipID
		verification.AudioPaddingNormalization.Sources[i].SourceClaimSHA256 = claimSHA
	}
	orderedSHA, _, err := stitchcert.CanonicalSHA(verification.AudioPaddingNormalization.Sources)
	if err != nil {
		t.Fatal(err)
	}
	verification.AudioPaddingNormalization.OrderedSourceEvidenceSHA256 = orderedSHA
	if err := validateAudioPaddingSourceClaims(verification, sources); err != nil {
		t.Fatal(err)
	}
	sources[0], sources[1] = sources[1], sources[0]
	if err := validateAudioPaddingSourceClaims(verification, sources); err == nil {
		t.Fatal("reordered AAC source claims passed")
	}
}

func TestRejectedLegacyStreamCopyEvidenceRejectsAACPaddingBlock(t *testing.T) {
	verification := row1923AACPaddingVerificationFixture(t)
	verification.Status, verification.AcceptanceMode = "failed", ""
	verification.PacketPayloadOrderStatus, verification.DecodedFrameTotalsStatus, verification.DecodedAudioTotalsStatus = "", "", ""
	verification.DecodedFrameSequenceStatus, verification.OutputTimestampStatus, verification.StrictDecodeStatus = "failed", "", ""
	for _, fingerprint := range []*MediaFingerprint{&verification.SourceFingerprint, &verification.OutputFingerprint} {
		for _, track := range fingerprint.Tracks {
			track.CodecProfile, track.CodecExtradataSHA256, track.AACPaddingNormalizedTimingSHA256 = "", "", ""
		}
	}
	raw, err := json.Marshal(verification)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateRejectedStreamCopyVerification(raw, verification.SourceFingerprint); err == nil {
		t.Fatal("legacy rejected stream-copy evidence accepted an AAC padding block")
	}
}

func TestAACDiscardPaddingNormalizationAcceptsExact662SampleSeamAndRejectsTamper(t *testing.T) {
	verification := row1923AACPaddingVerificationFixture(t)
	if err := validatePassedVerification(verification); err != nil {
		t.Fatalf("exact 662-sample AAC seam was rejected: %v", err)
	}

	for name, mutate := range map[string]func(*Verification){
		"video packet count":   func(v *Verification) { v.OutputFingerprint.Tracks["video"].PacketCount-- },
		"video packet order":   func(v *Verification) { v.OutputFingerprint.Tracks["video"].PacketChainSHA256 = strings.Repeat("9", 64) },
		"video decoded count":  func(v *Verification) { v.OutputFingerprint.Tracks["video"].DecodedFrames-- },
		"video decoded pixels": func(v *Verification) { v.OutputFingerprint.DecodedVideoSHA256 = strings.Repeat("8", 64) },
		"video packet timing":  func(v *Verification) { v.OutputFingerprint.Tracks["video"].TimestampStatus = "nonmonotonic" },
		"audio packet count":   func(v *Verification) { v.OutputFingerprint.Tracks["audio"].PacketCount-- },
		"audio packet order":   func(v *Verification) { v.OutputFingerprint.Tracks["audio"].PacketChainSHA256 = strings.Repeat("7", 64) },
		"audio timing evidence": func(v *Verification) {
			v.OutputFingerprint.Tracks["audio"].PacketTimingSHA256 = strings.Repeat("7", 64)
		},
		"audio decoded surplus":         func(v *Verification) { v.OutputFingerprint.EffectiveAudioFrames++ },
		"strict decode":                 func(v *Verification) { v.StrictDecodeStatus = "failed" },
		"audio packet timing":           func(v *Verification) { v.OutputFingerprint.Tracks["audio"].TimestampStatus = "nonmonotonic" },
		"padding evidence":              func(v *Verification) { v.AudioPaddingNormalization.DecodedAudioSurplusSamples++ },
		"padding location":              func(v *Verification) { v.AudioPaddingNormalization.Sources[0].TrimEvents[0].PacketOrdinal-- },
		"intermediate initial padding":  func(v *Verification) { v.SourceFingerprint.AudioContracts[1].InitialPadding = 1 },
		"intermediate codec delay":      func(v *Verification) { v.SourceFingerprint.AudioContracts[1].CodecDelay = 1 },
		"intermediate trailing padding": func(v *Verification) { v.SourceFingerprint.AudioContracts[1].TrailingPadding = 1 },
		"padding seam bound":            func(v *Verification) { v.AudioPaddingNormalization.Sources[0].TrimEvents[0].DiscardPadding = 1025 },
		"codec profile":                 func(v *Verification) { v.OutputFingerprint.Tracks["audio"].CodecProfile = "HE-AAC" },
		"codec extradata": func(v *Verification) {
			v.OutputFingerprint.Tracks["audio"].CodecExtradataSHA256 = strings.Repeat("9", 64)
		},
		"timing transform": func(v *Verification) {
			v.SourceFingerprint.Tracks["audio"].AACPaddingNormalizedTimingSHA256 = strings.Repeat("9", 64)
		},
		"legacy totals status": func(v *Verification) { v.DecodedAudioTotalsStatus = "passed" },
	} {
		t.Run(name, func(t *testing.T) {
			raw, err := json.Marshal(verification)
			if err != nil {
				t.Fatal(err)
			}
			var changed Verification
			if err := json.Unmarshal(raw, &changed); err != nil {
				t.Fatal(err)
			}
			mutate(&changed)
			if err := validatePassedVerification(changed); err == nil {
				t.Fatal("tampered AAC padding verification passed")
			}
		})
	}
}

func row1923AACPaddingVerificationFixture(t *testing.T) Verification {
	t.Helper()
	sourceEvidence := []AACSourcePaddingEvidence{
		{ClipID: 435622, SourceClaimSHA256: strings.Repeat("c", 64), PacketCount: 2584, MaxDecodedFrameSamples: 1024, LastDecodedFrameSamples: 362, TerminalPacketDurationSamples: 1024, TrimEvents: []AACTrimEventEvidence{{PacketOrdinal: 2584, DiscardPadding: 662}}},
		{ClipID: 435623, SourceClaimSHA256: strings.Repeat("d", 64), PacketCount: 2584, MaxDecodedFrameSamples: 1024, LastDecodedFrameSamples: 362, TerminalPacketDurationSamples: 1024, TrimEvents: []AACTrimEventEvidence{{PacketOrdinal: 2584, DiscardPadding: 662}}},
	}
	orderedSourceSHA, _, err := stitchcert.CanonicalSHA(sourceEvidence)
	if err != nil {
		t.Fatal(err)
	}
	firstContract := AudioSequenceContract{CodecName: "aac", SampleRate: 44100, Channels: 2, ChannelLayout: "stereo", DiscardPadding: 662, EditListKind: "decoder_timeline_v1", EditListSHA256: strings.Repeat("1", 64)}
	videoSource := &TrackFingerprint{MediaType: "video", PacketCount: 1800, PacketChainSHA256: strings.Repeat("2", 64), PacketTimingSHA256: strings.Repeat("3", 64), PacketTimeBases: []string{"1/90000"}, FirstPacketPTSSeconds: "0", LastPacketPTSSeconds: "60", FirstPacketDTSSeconds: "0", LastPacketDTSSeconds: "60", PacketDurationSeconds: "61", DecodeTimelineSpanSeconds: "61", DecodedFrames: 1800, TimestampStatus: "source_clips_independent"}
	videoOutput := *videoSource
	videoOutput.TimestampStatus = "monotonic"
	audioSource := &TrackFingerprint{MediaType: "audio", PacketCount: 5168, PacketChainSHA256: "3930be7a305e40819fb3b78e0752c5e48bb3584e956b12686830112c0fd9d42e", PacketTimingSHA256: strings.Repeat("4", 64), PacketTimeBases: []string{"1/44100"}, FirstPacketPTSSeconds: "0", LastPacketPTSSeconds: "5291008/44100", FirstPacketDTSSeconds: "0", LastPacketDTSSeconds: "5291008/44100", PacketDurationSeconds: "5292032/44100", DecodeTimelineSpanSeconds: "5292032/44100", DecodedFrames: 5168, DecodedSamples: 5290708, CodecProfile: "LC", CodecExtradataSHA256: strings.Repeat("6", 64), AACPaddingNormalizedTimingSHA256: strings.Repeat("5", 64), TimestampStatus: "source_clips_independent"}
	audioOutput := *audioSource
	audioOutput.PacketTimingSHA256 = strings.Repeat("5", 64)
	audioOutput.AACPaddingNormalizedTimingSHA256 = ""
	audioOutput.LastPacketPTSSeconds = "5291670/44100"
	audioOutput.LastPacketDTSSeconds = "5291670/44100"
	audioOutput.PacketDurationSeconds = "5292694/44100"
	audioOutput.DecodeTimelineSpanSeconds = "5292694/44100"
	audioOutput.DecodedSamples = 5291370
	audioOutput.TimestampStatus = "monotonic"
	return Verification{
		Status: "passed", AcceptanceMode: audioPaddingAcceptanceMode,
		AudioPaddingNormalization: &AudioPaddingNormalizationEvidence{
			PolicyVersion:               aacDiscardPaddingPolicyVersion,
			Sources:                     sourceEvidence,
			OrderedSourceEvidenceSHA256: orderedSourceSHA,
			Output:                      AACSourcePaddingEvidence{PacketCount: 5168, MaxDecodedFrameSamples: 1024, LastDecodedFrameSamples: 362, TerminalPacketDurationSamples: 1024, TrimEvents: []AACTrimEventEvidence{{PacketOrdinal: 5168, DiscardPadding: 662}}},
			DecodedAudioSurplusSamples:  662, PacketDurationDeltaSeconds: "331/22050", DecodeTimelineSpanDeltaSeconds: "331/22050", AudioContentStatus: audioPaddingNotSampleExactStatus,
		},
		PacketPayloadOrderStatus: "passed", DecodedFrameSequenceStatus: "passed", DecodedFrameTotalsStatus: "passed", DecodedAudioTotalsStatus: audioPaddingDecodedTotalsStatus, OutputTimestampStatus: "passed", StrictDecodeStatus: "passed",
		SourceFingerprint: MediaFingerprint{DurationSeconds: 120, Tracks: map[string]*TrackFingerprint{"video": videoSource, "audio": audioSource}, DecodedVideoSHA256: strings.Repeat("a", 64), AudioContracts: []AudioSequenceContract{firstContract, firstContract}, EffectiveAudioBytes: 42325664, EffectiveAudioFrames: 5290708, EffectiveAudioSHA256: "2edf342c" + strings.Repeat("a", 56)},
		OutputFingerprint: MediaFingerprint{DurationSeconds: 120.015, Tracks: map[string]*TrackFingerprint{"video": &videoOutput, "audio": &audioOutput}, DecodedVideoSHA256: strings.Repeat("a", 64), AudioContracts: []AudioSequenceContract{firstContract}, EffectiveAudioBytes: 42330960, EffectiveAudioFrames: 5291370, EffectiveAudioSHA256: "a0a08641" + strings.Repeat("b", 56)},
	}
}

func TestBuildAllPassingPartsContinuesAfterAACSeamSplit(t *testing.T) {
	dir := t.TempDir()
	first := makeMediaClip(t, dir, "one.mp4", 440, true)
	second := makeMediaClip(t, dir, "two.mp4", 880, true)
	first.ClipID, second.ClipID = 1, 2
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	parts, quarantines, err := buildAllPassingParts(ctx, []LocalSource{first, second}, dir, strings.Repeat("f", 64))
	if err != nil || len(quarantines) != 0 {
		t.Fatal(err)
	}
	for _, part := range parts {
		defer os.Remove(part.Path)
	}
	if len(parts) != 2 || parts[0].SourceCount != 1 || parts[1].SourceCount != 1 {
		t.Fatalf("AAC split did not account for every source: %+v", parts)
	}
}

func TestBuildAllPassingPartsVerifiesMultipleAACSeams(t *testing.T) {
	dir := t.TempDir()
	sources := make([]LocalSource, 4)
	for i := range sources {
		sources[i] = makeMediaClip(t, dir, fmt.Sprintf("audio-%d.mp4", i+1), 440+i*110, true)
		sources[i].ClipID = int64(i + 1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	parts, quarantines, err := buildAllPassingParts(ctx, sources, dir, strings.Repeat("f", 64))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, part := range parts {
			discardIsolatedBuild(part, dir)
		}
	}()
	if len(parts) != 4 || len(quarantines) != 0 {
		t.Fatalf("parts=%d quarantines=%d", len(parts), len(quarantines))
	}
	for i, part := range parts {
		if part.SourceCount != 1 || part.Verification.Status != "passed" {
			t.Fatalf("part[%d]=%+v", i, part)
		}
		if i+1 < len(parts) && (len(part.SplitEvidence) != 1 || part.SplitEvidence[0].RepeatCount != 2) {
			t.Fatalf("part[%d] boundary evidence=%+v", i, part.SplitEvidence)
		}
	}
}

func TestPreflightQuarantinesIrreducibleCorruptSourceAndContinues(t *testing.T) {
	dir := t.TempDir()
	first := makeMediaClip(t, dir, "one.mp4", 440, false)
	bad := makeMediaClip(t, dir, "bad.mp4", 660, false)
	third := makeMediaClip(t, dir, "three.mp4", 880, false)
	first.ClipID, bad.ClipID, third.ClipID = 1, 2, 3
	if err := os.Truncate(bad.Path, 128); err != nil {
		t.Fatal(err)
	}
	bad.SizeBytes, bad.SHA256, _ = localIdentity(bad.Path)
	start := time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC)
	clips := []SourceClip{testSource(1, start), testSource(2, start.Add(time.Minute)), testSource(3, start.Add(2*time.Minute))}
	draft := HourDraft{LocalDate: "2026-05-04", LocalHour: 1, Parts: []OutputPlan{{Hour: 1, Sources: clips}}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result, err := PreflightHour(ctx, draft, []LocalSource{first, bad, third}, dir, strings.Repeat("f", 64))
	if err != nil {
		t.Fatal(err)
	}
	for _, built := range result.Built {
		defer os.Remove(built.Path)
	}
	if len(result.Built) != 2 || len(result.Quarantined) != 1 || result.Quarantined[0].ClipID != 2 || len(result.Quarantines) != 1 || result.Quarantines[0].Evidence.RepeatCount != 2 || result.Sources[0].ClipID != 1 || result.Sources[1].ClipID != 3 || result.Sources[1].SeamToPrevious.Reason != "source_quarantined" {
		t.Fatalf("corrupt source accounting differs: %+v", result)
	}
}

func TestPreflightQuarantinesDeterministicallyCorruptSingletonPartAndContinues(t *testing.T) {
	dir := t.TempDir()
	first := makeMediaClip(t, dir, "one.mp4", 440, false)
	bad := makeMediaClip(t, dir, "bad.mp4", 660, false)
	third := makeMediaClip(t, dir, "three.mp4", 880, false)
	first.ClipID, bad.ClipID, third.ClipID = 1, 2, 3
	if err := os.Truncate(bad.Path, 128); err != nil {
		t.Fatal(err)
	}
	bad.SizeBytes, bad.SHA256, _ = localIdentity(bad.Path)
	start := time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC)
	clips := []SourceClip{testSource(1, start), testSource(2, start.Add(time.Minute)), testSource(3, start.Add(2*time.Minute+3*time.Second))}
	draft := HourDraft{LocalDate: "2026-05-04", LocalHour: 1, Parts: []OutputPlan{
		{Hour: 1, Sources: clips[0:1]},
		{Hour: 1, Sources: clips[1:2]},
		{Hour: 1, Sources: clips[2:3]},
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	mediaToolIdentity := testRequest(clips).MediaTool.IdentitySHA256
	result, err := PreflightHour(ctx, draft, []LocalSource{first, bad, third}, dir, mediaToolIdentity)
	if err != nil {
		t.Fatal(err)
	}
	for _, built := range result.Built {
		defer os.Remove(built.Path)
	}
	if len(result.Built) != 2 || result.Built[0].SourceCount != 1 || result.Built[1].SourceCount != 1 {
		t.Fatalf("built parts differ: %+v", result.Built)
	}
	if len(result.Quarantined) != 1 || result.Quarantined[0].ClipID != 2 || len(result.Quarantines) != 1 || result.Quarantines[0].Evidence.RepeatCount != 2 {
		t.Fatalf("quarantine accounting differs: %+v", result)
	}
	if len(result.Sources) != 2 || result.Sources[0].ClipID != 1 || result.Sources[1].ClipID != 3 || result.Sources[1].SeamToPrevious.Reason != "source_quarantined" || result.Sources[1].SeamToPrevious.SignedGapNanoseconds != int64(3*time.Second) {
		t.Fatalf("surviving source accounting differs: %+v", result.Sources)
	}
	req := testRequest(result.Sources)
	req.QuarantinedSources = result.Quarantined
	req.BuiltArtifacts = make([]BuiltArtifactIdentity, len(result.Built))
	for i, built := range result.Built {
		req.BuiltArtifacts[i] = BuiltArtifactIdentity{SizeBytes: built.SizeBytes, SHA256: built.SHA256, MediaToolIdentity: req.MediaTool.IdentitySHA256}
	}
	ledgerSources, err := mergeAccountedSources(req.Sources, req.QuarantinedSources)
	if err != nil {
		t.Fatal(err)
	}
	ledgerReq := req
	ledgerReq.Sources, ledgerReq.QuarantinedSources = ledgerSources, nil
	ledger, err := testLedger(ledgerReq, req.LocalDate)
	if err != nil {
		t.Fatal(err)
	}
	req.AllocationLedgerSHA = ledger.LedgerSHA256
	plan, err := BuildPlan(req)
	if err != nil {
		t.Fatal(err)
	}
	claim := PreflightHourClaim{HourID: plan.HourID}
	seal, err := sealHourRequest(claim, plan, result.Built, quarantineEvidenceFromBuilds(result.Quarantines))
	if err != nil {
		t.Fatal(err)
	}
	if err := seal.Validate(plan.RecordingID, plan.MediaTool.IdentitySHA256); err != nil {
		t.Fatalf("preflight quarantine must satisfy the seal contract: %v", err)
	}
	seal.Quarantine[0].NormalizedFacts = json.RawMessage(`{"category":"tampered"}`)
	if err := seal.Validate(plan.RecordingID, plan.MediaTool.IdentitySHA256); err == nil {
		t.Fatal("tampered quarantine facts passed strict digest validation")
	}
}

func TestSingletonIsExactByteCopy(t *testing.T) {
	dir := t.TempDir()
	source := makeMediaClip(t, dir, "one.mp4", 440, false)
	source.ClipID = 1
	built, err := BuildSealedOutput(context.Background(), []LocalSource{source}, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(built.Path)
	if built.SizeBytes != source.SizeBytes || built.SHA256 != source.SHA256 {
		t.Fatalf("singleton was remuxed: source=%d/%s output=%d/%s", source.SizeBytes, source.SHA256, built.SizeBytes, built.SHA256)
	}
}

func TestBuildSealedOutputNeverTailPeelsRuntimeFailure(t *testing.T) {
	dir := t.TempDir()
	first := makeMediaClip(t, dir, "one.mp4", 440, true)
	second := makeMediaClip(t, dir, "two.mp4", 880, true)
	first.ClipID, second.ClipID = 1, 2
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if built, err := BuildSealedOutput(ctx, []LocalSource{first, second}, dir); err == nil {
		_ = os.Remove(built.Path)
		t.Fatal("sealed runtime task silently tail-peeled")
	}
}

func TestPreflightCancellationNeverTailPeels(t *testing.T) {
	dir := t.TempDir()
	first := makeMediaClip(t, dir, "one.mp4", 440, false)
	second := makeMediaClip(t, dir, "two.mp4", 880, false)
	first.ClipID, second.ClipID = 1, 2
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if built, err := BuildLargestPassingPrefix(ctx, []LocalSource{first, second}, dir, strings.Repeat("f", 64)); err == nil {
		_ = os.Remove(built.Path)
		t.Fatal("cancelled preflight tail-peeled")
	}
}

func TestMissingMediaToolNeverTailPeels(t *testing.T) {
	dir := t.TempDir()
	first := makeMediaClip(t, dir, "one.mp4", 440, false)
	second := makeMediaClip(t, dir, "two.mp4", 880, false)
	first.ClipID, second.ClipID = 1, 2
	t.Setenv("FFMPEG_BIN", filepath.Join(dir, "missing-ffmpeg"))
	if built, err := BuildLargestPassingPrefix(context.Background(), []LocalSource{first, second}, dir, strings.Repeat("f", 64)); err == nil {
		_ = os.Remove(built.Path)
		t.Fatal("missing media tool tail-peeled")
	}
}

func TestRepeatedENOSPCNeverTailPeels(t *testing.T) {
	dir := t.TempDir()
	first := makeMediaClip(t, dir, "one.mp4", 440, false)
	second := makeMediaClip(t, dir, "two.mp4", 880, false)
	first.ClipID, second.ClipID = 1, 2
	fake := filepath.Join(dir, "ffmpeg-enospc")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\necho 'No space left on device' >&2\nexit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FFMPEG_BIN", fake)
	if built, err := BuildLargestPassingPrefix(context.Background(), []LocalSource{first, second}, dir, strings.Repeat("f", 64)); err == nil {
		_ = os.Remove(built.Path)
		t.Fatal("repeated ENOSPC tail-peeled")
	}
}

func TestMediaToolVersionRejectsOversizedOutputWithoutHanging(t *testing.T) {
	dir := t.TempDir()
	fake := filepath.Join(dir, "oversized-version")
	script := "#!/bin/sh\ni=0\nwhile [ \"$i\" -le 65536 ]; do printf x; i=$((i+1)); done\n"
	if err := os.WriteFile(fake, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := mediaToolVersion(ctx, fake); err == nil || !strings.Contains(err.Error(), "exceeds bounded output") {
		t.Fatalf("oversized media-tool version err=%v", err)
	}
}

func TestCompactEvidenceFingerprintIsBoundedForLargeHour(t *testing.T) {
	const frames = 200000
	accumulator := newMediaAccumulator()
	accumulator.tracks["video"] = &trackAccumulator{fingerprint: TrackFingerprint{MediaType: "video", PacketTimeBases: []string{"1/1000"}}, packetHash: sha256.New(), timingHash: sha256.New(), packetDuration: new(big.Rat)}
	if err := consumeCompactEvidence(&generatedEvidence{frames: frames}, map[int]probedStream{0: {MediaType: "video", TimeBase: "1/1000", TimeBaseNum: 1, TimeBaseDen: 1000}}, accumulator, "monotonic"); err != nil {
		t.Fatal(err)
	}
	got := accumulator.fingerprint().Tracks["video"]
	if got.PacketCount != frames || got.DecodedFrames != frames || !lowerHex64(got.PacketChainSHA256) {
		t.Fatalf("large streaming fingerprint=%+v", got)
	}
}

func TestCompactEvidenceRejectsNonmonotonicPacketTimestamps(t *testing.T) {
	evidence := strings.NewReader(
		"packet|stream_index=0|pts=2|dts=2|duration=1|data_hash=SHA256:" + strings.Repeat("a", 64) + "\n" +
			"frame|media_type=video|stream_index=0|best_effort_timestamp=1\n" +
			"packet|stream_index=0|pts=3|dts=1|duration=1|data_hash=SHA256:" + strings.Repeat("b", 64) + "\n" +
			"frame|media_type=video|stream_index=0|best_effort_timestamp=2\n")
	accumulator := newMediaAccumulator()
	accumulator.tracks["video"] = &trackAccumulator{fingerprint: TrackFingerprint{MediaType: "video", PacketTimeBases: []string{"1/1"}}, packetHash: sha256.New(), timingHash: sha256.New(), packetDuration: new(big.Rat)}
	if err := consumeCompactEvidence(evidence, map[int]probedStream{0: {MediaType: "video", TimeBase: "1/1", TimeBaseNum: 1, TimeBaseDen: 1}}, accumulator, "monotonic"); err == nil || !strings.Contains(err.Error(), "nonmonotonic packet") {
		t.Fatalf("expected nonmonotonic packet rejection, got %v", err)
	}
}

func TestFingerprintRejectsRetimedIdenticalPackets(t *testing.T) {
	packetA, packetB := strings.Repeat("a", 64), strings.Repeat("b", 64)
	buildFingerprint := func(timingStatus string, secondPTS int64) MediaFingerprint {
		t.Helper()
		evidence := strings.NewReader(fmt.Sprintf(
			"packet|stream_index=0|pts=0|dts=0|duration=1|data_hash=SHA256:%s\nframe|media_type=video|stream_index=0|best_effort_timestamp=0\npacket|stream_index=0|pts=%d|dts=1|duration=1|data_hash=SHA256:%s\nframe|media_type=video|stream_index=0|best_effort_timestamp=1\n",
			packetA, secondPTS, packetB))
		accumulator := newMediaAccumulator()
		accumulator.duration = 2
		accumulator.tracks["video"] = &trackAccumulator{fingerprint: TrackFingerprint{MediaType: "video", PacketTimeBases: []string{"1/1"}}, packetHash: sha256.New(), timingHash: sha256.New(), packetDuration: new(big.Rat)}
		if err := consumeCompactEvidence(evidence, map[int]probedStream{0: {MediaType: "video", TimeBase: "1/1", TimeBaseNum: 1, TimeBaseDen: 1}}, accumulator, timingStatus); err != nil {
			t.Fatal(err)
		}
		return accumulator.fingerprint()
	}
	expected := buildFingerprint("source_clips_independent", 1)
	retimed := buildFingerprint("monotonic", 2)
	if expected.Tracks["video"].PacketDurationSeconds != retimed.Tracks["video"].PacketDurationSeconds || expected.Tracks["video"].DecodeTimelineSpanSeconds != retimed.Tracks["video"].DecodeTimelineSpanSeconds || expected.Tracks["video"].PacketChainSHA256 != retimed.Tracks["video"].PacketChainSHA256 || expected.Tracks["video"].PacketTimingSHA256 == retimed.Tracks["video"].PacketTimingSHA256 {
		t.Fatal("retiming fixture did not isolate timing-chain evidence")
	}
	if err := compareFingerprints(expected, retimed); err == nil {
		t.Fatal("identical packet payloads with changed normalized timing accepted")
	}
}

func TestFingerprintRejectsChangedPacketTimeBaseWithSameTimingFacts(t *testing.T) {
	expected := passingVerification().SourceFingerprint
	actual := passingVerification().OutputFingerprint
	actual.Tracks["video"].PacketTimeBases = []string{"1/2"}
	if err := compareFingerprints(expected, actual); err == nil {
		t.Fatal("changed packet time-base sequence accepted with otherwise identical timing facts")
	}
}

func TestPacketEvidenceFailuresAreDeterministicMedia(t *testing.T) {
	for _, fact := range []string{"missing exact packet timestamp evidence", "nonmonotonic packet timestamp", "evidence has unknown stream", "packet duration overflow"} {
		var deterministic *deterministicMediaError
		if err := deterministicEvidenceFailure(context.Background(), "packet_timing_failed", errors.New(fact)); !errors.As(err, &deterministic) {
			t.Fatalf("repeatable packet fact %q was not classified", fact)
		}
	}
}

func TestDecodedVideoIdentityBindingClassifiesMediaEvidenceButNotInfrastructure(t *testing.T) {
	exitErr := exec.Command("sh", "-c", "exit 9").Run()
	malformed := fmt.Errorf("framemd5 decode: %w (Invalid data found when processing input)", exitErr)
	err := validateDecodedVideoIdentityBinding(context.Background(),
		decodedIdentity{err: malformed}, decodedIdentity{err: context.Canceled}, nil, nil)
	failure, ok := deterministicBuildFailure(err)
	if !ok || failure.code != "media_sequence_mismatch" || !errors.Is(err, malformed) || !errors.Is(err, context.Canceled) {
		t.Fatalf("malformed decoded evidence was not classified with its causes: %v", err)
	}
	var facts struct {
		SourceFailure                    json.RawMessage `json:"source_failure"`
		OutputCanceledAfterSourceFailure bool            `json:"output_canceled_after_source_failure"`
	}
	if err := json.Unmarshal(failure.evidence, &facts); err != nil || len(facts.SourceFailure) == 0 || !facts.OutputCanceledAfterSourceFailure {
		t.Fatalf("decoded evidence facts=%s err=%v", failure.evidence, err)
	}

	unknownExit := exec.Command("sh", "-c", "exit 8").Run()
	for name, testErr := range map[string]error{
		"missing_binary":  &os.PathError{Op: "fork/exec", Path: "/missing/ffmpeg", Err: os.ErrNotExist},
		"no_space":        syscall.ENOSPC,
		"io":              syscall.EIO,
		"unknown_exit":    unknownExit,
		"parser_contract": errors.New("invalid decoded frame duration"),
	} {
		t.Run(name, func(t *testing.T) {
			err := validateDecodedVideoIdentityBinding(context.Background(), decodedIdentity{err: testErr}, decodedIdentity{}, nil, nil)
			if _, ok := deterministicBuildFailure(err); ok || !errors.Is(err, testErr) {
				t.Fatalf("infrastructure failure was classified or lost: %v", err)
			}
		})
	}

	joinedInfra := errors.Join(context.Canceled, syscall.ENOSPC)
	err = validateDecodedVideoIdentityBinding(context.Background(), decodedIdentity{err: malformed}, decodedIdentity{err: joinedInfra}, nil, nil)
	if _, ok := deterministicBuildFailure(err); ok || !errors.Is(err, context.Canceled) || !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("joined peer infrastructure failure was classified or lost: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = validateDecodedVideoIdentityBinding(ctx, decodedIdentity{err: malformed}, decodedIdentity{}, nil, nil)
	if _, ok := deterministicBuildFailure(err); ok || !errors.Is(err, malformed) || !errors.Is(err, context.Canceled) {
		t.Fatalf("parent cancellation was classified or lost: %v", err)
	}
	err = validateDecodedVideoIdentityBinding(ctx,
		decodedIdentity{frames: 1, sha: strings.Repeat("a", 64), pixelSHA: strings.Repeat("c", 64)},
		decodedIdentity{frames: 1, sha: strings.Repeat("b", 64), pixelSHA: strings.Repeat("d", 64)},
		&TrackFingerprint{DecodedFrames: 2}, &TrackFingerprint{DecodedFrames: 1})
	if _, ok := deterministicBuildFailure(err); ok || !errors.Is(err, context.Canceled) {
		t.Fatalf("parent cancellation did not win structural mismatch: %v", err)
	}
}

func TestDecodedVideoIdentityBindingFactsDriveDeterminismAndQuarantine(t *testing.T) {
	wantTrack := &TrackFingerprint{DecodedFrames: 2}
	gotTrack := &TrackFingerprint{DecodedFrames: 1}
	want := decodedIdentity{frames: 1, sha: strings.Repeat("a", 64), pixelSHA: strings.Repeat("c", 64)}
	got := decodedIdentity{frames: 1, sha: strings.Repeat("b", 64), pixelSHA: strings.Repeat("d", 64)}
	err := validateDecodedVideoIdentityBinding(context.Background(), want, got, wantTrack, gotTrack)
	failure, ok := deterministicBuildFailure(err)
	if !ok || failure.code != "media_sequence_mismatch" {
		t.Fatalf("structural binding failure was not deterministic: %v", err)
	}
	err = validateDecodedVideoIdentityBinding(context.Background(),
		decodedIdentity{frames: 2, sha: "invalid", pixelSHA: strings.Repeat("c", 64)},
		decodedIdentity{frames: 1, sha: strings.Repeat("b", 64), pixelSHA: strings.Repeat("d", 64)},
		wantTrack, gotTrack)
	if _, ok := deterministicBuildFailure(err); ok {
		t.Fatalf("invalid generated SHA was treated as a media fact: %v", err)
	}

	want.frames = 0
	changed, ok := deterministicBuildFailure(validateDecodedVideoIdentityBinding(context.Background(), want, got, wantTrack, gotTrack))
	if !ok || changed.evidenceSHA256 == failure.evidenceSHA256 {
		t.Fatalf("changed decoded binding facts reused evidence: before=%s after=%s", failure.evidenceSHA256, changed.evidenceSHA256)
	}

	sources := makeSyntheticLocalSourcesWithIdentity(t, 3)
	attempt := func(_ context.Context, candidate []LocalSource, _ string) (BuiltOutput, error) {
		for _, source := range candidate {
			if source.ClipID == 2 {
				return BuiltOutput{}, validateDecodedVideoIdentityBinding(context.Background(),
					decodedIdentity{frames: 1, sha: strings.Repeat("a", 64), pixelSHA: strings.Repeat("c", 64)},
					decodedIdentity{frames: 1, sha: strings.Repeat("b", 64), pixelSHA: strings.Repeat("d", 64)},
					&TrackFingerprint{DecodedFrames: 2}, &TrackFingerprint{DecodedFrames: 1})
			}
		}
		return BuiltOutput{SourceCount: len(candidate)}, nil
	}
	parts, quarantines, err := buildAllPassingPartsWithAttempt(context.Background(), sources, t.TempDir(), strings.Repeat("f", 64), attempt)
	if err != nil || len(parts) != 2 || len(quarantines) != 1 || quarantines[0].Source.ClipID != 2 {
		t.Fatalf("parts=%v quarantines=%v err=%v", parts, quarantines, err)
	}
}
