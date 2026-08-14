package recordability

import (
	"strings"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/capture"
)

func TestNativeSegmentSignatureBindsAllStoredMediaFacts(t *testing.T) {
	fps := 29.97
	base := capture.Segment{VideoCodec: "H264", AudioCodec: "AAC", AudioPresent: true, VideoWidth: 1920, VideoHeight: 1080, ActualFPS: &fps}
	want := nativeSegmentSignature(base)
	if want == "" || hashStrings([]string{want}) == hashStrings([]string{"", want}) {
		t.Fatal("signature or length-prefixed hash is ambiguous")
	}
	mutations := []capture.Segment{
		{VideoCodec: "hevc", AudioCodec: "AAC", AudioPresent: true, VideoWidth: 1920, VideoHeight: 1080, ActualFPS: &fps},
		{VideoCodec: "H264", AudioCodec: "", AudioPresent: false, VideoWidth: 1920, VideoHeight: 1080, ActualFPS: &fps},
		{VideoCodec: "H264", AudioCodec: "AAC", AudioPresent: true, VideoWidth: 1280, VideoHeight: 1080, ActualFPS: &fps},
	}
	for i, mutation := range mutations {
		if nativeSegmentSignature(mutation) == want {
			t.Fatalf("mutation %d did not change native signature", i)
		}
	}
}

func TestTargetedChallengeProofCannotReplayAcrossAttempts(t *testing.T) {
	fps := 30.0
	evidence := TargetedEvidence{
		MediaSHA256: strings.Repeat("a", 64), FrameSHA256: strings.Repeat("b", 64),
		VideoCodec: "h264", AudioCodec: "aac", AudioPresent: true,
		VideoWidth: 1920, VideoHeight: 1080, ActualFPS: &fps,
	}
	evidence.NativeSignatureSHA256 = TargetedNativeSignatureSHA256(evidence)
	first := TargetedChallengeProofSHA256(strings.Repeat("c", 64), evidence)
	second := TargetedChallengeProofSHA256(strings.Repeat("d", 64), evidence)
	if first == second || len(first) != 64 || len(second) != 64 {
		t.Fatalf("challenge proofs are not exact and attempt-specific: %q %q", first, second)
	}
	evidence.VideoWidth = 1280
	if TargetedNativeSignatureSHA256(evidence) == evidence.NativeSignatureSHA256 {
		t.Fatal("typed media mutation did not change recomputed native signature")
	}
}

func TestTargetedCaptureExitClassNeverPersistsRawSourceOrCredential(t *testing.T) {
	raw := "https://user:secret@private.example/live.m3u8?token=secret: connection reset by peer"
	got := targetedCaptureExitClass(raw)
	if got != "network_cut" || strings.Contains(got, "private.example") || strings.Contains(got, "secret") {
		t.Fatalf("unsafe targeted capture classification %q", got)
	}
}
