package joinedrecording

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
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
			r.active = strings.NewReader("packet|stream_index=0|data_hash=SHA256:" + strings.Repeat("a", 64) + "\n")
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
	return LocalSource{Path: mediaPath, SizeBytes: size, SHA256: sha}
}

func TestBuildLargestPassingPrefixPreservesPacketsFramesAndAudio(t *testing.T) {
	dir := t.TempDir()
	first := makeMediaClip(t, dir, "one.mp4", 440, false)
	second := makeMediaClip(t, dir, "two.mp4", 880, false)
	first.ClipID, second.ClipID = 1, 2
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	built, err := BuildLargestPassingPrefix(ctx, []LocalSource{first, second}, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(built.Path)
	if built.SourceCount != 2 || built.Verification.Status != "passed" || built.Verification.PacketPayloadOrderStatus != "passed" || built.Verification.DecodedFrameTotalsStatus != "passed" || built.Verification.DecodedAudioTotalsStatus != "passed" {
		t.Fatalf("verification=%+v", built.Verification)
	}
}

func TestBuildLargestPassingPrefixPeelsBadTail(t *testing.T) {
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
	bad := LocalSource{ClipID: 2, Path: badPath, SizeBytes: size, SHA256: sha}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	built, err := BuildLargestPassingPrefix(ctx, []LocalSource{first, bad}, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(built.Path)
	if built.SourceCount != 1 || built.Verification.Status != "passed" {
		t.Fatalf("tail was not peeled: %+v", built)
	}
}

func TestBuildLargestPassingPrefixRejectsAACPrimingTotalChange(t *testing.T) {
	dir := t.TempDir()
	first := makeMediaClip(t, dir, "one.mp4", 440, true)
	second := makeMediaClip(t, dir, "two.mp4", 880, true)
	first.ClipID, second.ClipID = 1, 2
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	built, err := BuildLargestPassingPrefix(ctx, []LocalSource{first, second}, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(built.Path)
	if built.SourceCount != 1 {
		t.Fatalf("AAC decoded-total mismatch was accepted across seam: %+v", built.Verification)
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

func TestCompactEvidenceFingerprintIsBoundedForLargeHour(t *testing.T) {
	const frames = 200000
	accumulator := newMediaAccumulator()
	accumulator.tracks["video"] = &trackAccumulator{fingerprint: TrackFingerprint{MediaType: "video"}, packetHash: sha256.New()}
	if err := consumeCompactEvidence(&generatedEvidence{frames: frames}, map[int]string{0: "video"}, accumulator); err != nil {
		t.Fatal(err)
	}
	got := accumulator.fingerprint("monotonic").Tracks["video"]
	if got.PacketCount != frames || got.DecodedFrames != frames || !lowerHex64(got.PacketChainSHA256) {
		t.Fatalf("large streaming fingerprint=%+v", got)
	}
}
