package joinedrecording

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
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
	args = append(args, "-c:v", "mpeg4", "-g", "10", "-bf", "2", mediaPath)
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
