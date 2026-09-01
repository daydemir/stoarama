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
	"strconv"
	"strings"
	"syscall"
	"testing"
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

func TestBuildAllPassingPartsLocalizesAfterFullCandidateDeadline(t *testing.T) {
	sources := makeSyntheticLocalSources(4)
	parentCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	attempt := func(ctx context.Context, candidate []LocalSource, _ string) (BuiltOutput, error) {
		ids := clipIDs(candidate)
		if equalInt64s(ids, []int64{1, 2, 3, 4}) {
			<-ctx.Done()
			return BuiltOutput{}, ctx.Err()
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
	seal := sealHourRequest(claim, plan, result.Built, quarantineEvidenceFromBuilds(result.Quarantines))
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
