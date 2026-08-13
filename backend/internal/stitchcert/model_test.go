package stitchcert

import (
	"testing"
	"time"
)

func mc(id int64, start, end time.Time, generation string, sequence int64, size int64) ManifestClip {
	return ManifestClip{Ordinal: int(id), ClipID: id, RecordingID: 7, RecordingJobID: 9,
		RelativePath: "safe/" + time.Unix(id, 0).UTC().Format("150405") + ".mp4", SizeBytes: size,
		SHA256:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ClipStartAt: start, ClipEndAt: end, CaptureGeneration: generation, CaptureSequence: sequence}
}

func TestValidateManifestAndTimeline(t *testing.T) {
	start := time.Date(2026, 8, 10, 8, 0, 0, 0, time.UTC)
	clips := []ManifestClip{mc(1, start.Add(time.Minute), start.Add(2*time.Minute), "g1", 1, 10), mc(2, start.Add(3*time.Minute), start.Add(4*time.Minute), "g1", 2, 10)}
	if got, err := ValidateManifest(clips, 7, 9, start, start.Add(5*time.Minute)); err != nil || got != 20 {
		t.Fatalf("got=%d err=%v", got, err)
	}
	timeline := MeasureTimeline(clips, start, start.Add(5*time.Minute))
	if timeline.CoveredSeconds != 120 || timeline.LeadingGapSeconds != 60 || timeline.LargestInternalGapSecond != 60 || timeline.TrailingGapSeconds != 60 || timeline.GapCount != 3 || timeline.OverlapCount != 0 {
		t.Fatalf("timeline=%+v", timeline)
	}
}

func TestValidateManifestBoundsAndDuplicates(t *testing.T) {
	start := time.Now().UTC()
	clips := []ManifestClip{mc(1, start, start.Add(time.Minute), "g", 1, MaxSourceByte), mc(2, start.Add(time.Minute), start.Add(2*time.Minute), "g", 2, 1)}
	if _, err := ValidateManifest(clips, 7, 9, start, start.Add(2*time.Minute)); err == nil {
		t.Fatal("64GiB+1 must fail")
	}
	clips = []ManifestClip{mc(1, start, start.Add(time.Minute), "g", 1, 1), mc(2, start.Add(time.Minute), start.Add(2*time.Minute), "g", 1, 1)}
	if _, err := ValidateManifest(clips, 7, 9, start, start.Add(2*time.Minute)); err == nil {
		t.Fatal("duplicate provenance must fail")
	}
}

func TestValidateManifestRequiresCompleteGenerationSequence(t *testing.T) {
	start := time.Now().UTC()
	firstMissing := []ManifestClip{mc(1, start, start.Add(time.Minute), "g", 2, 1)}
	if _, err := ValidateManifest(firstMissing, 7, 9, start, start.Add(time.Hour)); err == nil {
		t.Fatal("generation beginning at sequence 2 must fail closed")
	}
	middleMissing := []ManifestClip{
		mc(1, start, start.Add(time.Minute), "g", 1, 1),
		mc(2, start.Add(time.Minute), start.Add(2*time.Minute), "g", 3, 1),
	}
	if _, err := ValidateManifest(middleMissing, 7, 9, start, start.Add(time.Hour)); err == nil {
		t.Fatal("generation with a missing middle sequence must fail closed")
	}
	reappears := []ManifestClip{
		mc(1, start, start.Add(time.Minute), "g1", 1, 1),
		mc(2, start.Add(time.Minute), start.Add(2*time.Minute), "g2", 1, 1),
		mc(3, start.Add(2*time.Minute), start.Add(3*time.Minute), "g1", 2, 1),
	}
	if _, err := ValidateManifest(reappears, 7, 9, start, start.Add(time.Hour)); err == nil {
		t.Fatal("generation reappearing after a boundary must fail closed")
	}
}

func TestCanonicalSHAMatchesPythonUTF8Contract(t *testing.T) {
	value := map[string]any{"text": "şehir <&>\u2028\u2029", "nested": map[string]any{"z": 1, "a": "雪"}}
	got, raw, err := CanonicalSHA(value)
	if err != nil {
		t.Fatal(err)
	}
	const wantRaw = `{"nested":{"a":"雪","z":1},"text":"şehir <&>\u2028\u2029"}`
	const wantSHA = "52aa500473f858f6ebeac4cf64d06940d591e1c9a866a6c6e81ae045d1655036"
	if string(raw) != wantRaw || got != wantSHA {
		t.Fatalf("raw=%q sha=%s", raw, got)
	}
}

func TestValidateRunsNeverRegroupsNoncontiguousSignature(t *testing.T) {
	start := time.Now().UTC()
	base := []ManifestClip{mc(1, start, start.Add(time.Minute), "g", 1, 1), mc(2, start.Add(time.Minute), start.Add(2*time.Minute), "g", 2, 1), mc(3, start.Add(2*time.Minute), start.Add(3*time.Minute), "g", 3, 1)}
	clips := []ClipFact{{ManifestClip: base[0], NativeSignatureSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, {ManifestClip: base[1], NativeSignatureSHA256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}, {ManifestClip: base[2], NativeSignatureSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}
	for i := range clips {
		clips[i].StrictDecode = "passed"
	}
	bad := []RunFact{{Ordinal: 1, FirstClipOrdinal: 1, LastClipOrdinal: 3, ClipCount: 3, NativeSignatureSHA256: clips[0].NativeSignatureSHA256, CaptureGeneration: "g", BoundaryReason: "window_start", ValidationStatus: "lossless_concat_decode_passed", SourceBytes: 3}}
	if err := ValidateRuns(clips, bad); err == nil {
		t.Fatal("noncontiguous equal signatures must not be regrouped")
	}
	good := []RunFact{
		{Ordinal: 1, FirstClipOrdinal: 1, LastClipOrdinal: 1, ClipCount: 1, NativeSignatureSHA256: clips[0].NativeSignatureSHA256, CaptureGeneration: "g", BoundaryReason: "window_start", ValidationStatus: "single_clip_decode_only", SourceBytes: 1},
		{Ordinal: 2, FirstClipOrdinal: 2, LastClipOrdinal: 2, ClipCount: 1, NativeSignatureSHA256: clips[1].NativeSignatureSHA256, CaptureGeneration: "g", BoundaryReason: "native_signature_change", ValidationStatus: "single_clip_decode_only", SourceBytes: 1},
		{Ordinal: 3, FirstClipOrdinal: 3, LastClipOrdinal: 3, ClipCount: 1, NativeSignatureSHA256: clips[2].NativeSignatureSHA256, CaptureGeneration: "g", BoundaryReason: "native_signature_change", ValidationStatus: "single_clip_decode_only", SourceBytes: 1},
	}
	if err := ValidateRuns(clips, good); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRunsRequiresCaptureGenerationBoundary(t *testing.T) {
	start := time.Now().UTC()
	sig := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	a, b := mc(1, start, start.Add(time.Minute), "g1", 1, 1), mc(2, start.Add(time.Minute), start.Add(2*time.Minute), "g2", 1, 1)
	clips := []ClipFact{{ManifestClip: a, NativeSignatureSHA256: sig, StrictDecode: "passed"}, {ManifestClip: b, NativeSignatureSHA256: sig, StrictDecode: "passed"}}
	runs := []RunFact{{Ordinal: 1, FirstClipOrdinal: 1, LastClipOrdinal: 2, ClipCount: 2, NativeSignatureSHA256: sig, CaptureGeneration: "g1", BoundaryReason: "window_start", ValidationStatus: "lossless_concat_decode_passed", SourceBytes: 2}}
	if err := ValidateRuns(clips, runs); err == nil {
		t.Fatal("capture generations must be separate runs")
	}
}

func TestValidateSeamsRequiresContinuousCaptureProvenance(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Second)
	sig := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	base := []ManifestClip{
		mc(1, start, start.Add(time.Second), "g", 1, 1),
		mc(2, start.Add(time.Second), start.Add(2*time.Second), "g", 2, 1),
	}
	for i := range base {
		base[i].CaptureAttemptID = "attempt-1"
		base[i].TimestampContractVersion = "continuous-source-pts-v1"
		base[i].TimestampContractStatus = "per_clip_probe_complete"
		base[i].TimestampContractSHA256 = sig
	}
	clips := []ClipFact{
		{ManifestClip: base[0], NativeSignatureSHA256: sig, StrictDecode: "passed"},
		{ManifestClip: base[1], NativeSignatureSHA256: sig, StrictDecode: "passed"},
	}
	clips[0].RecomputedTimestampContract = &TimestampContract{Version: 1, Mode: "muxed_source_copy", AudioSelection: "first_optional", Tracks: []TimestampContractTrack{{MediaType: "video", TimeBaseNum: 1, TimeBaseDen: 30, FirstTimestamp: 27, LastTimestamp: 29, LastDuration: 1, UnitCount: 3}}}
	clips[1].RecomputedTimestampContract = &TimestampContract{Version: 1, Mode: "muxed_source_copy", AudioSelection: "first_optional", Tracks: []TimestampContractTrack{{MediaType: "video", TimeBaseNum: 1, TimeBaseDen: 30, FirstTimestamp: 30, LastTimestamp: 32, LastDuration: 1, UnitCount: 3}}}
	runs := []RunFact{{Ordinal: 1, FirstClipOrdinal: 1, LastClipOrdinal: 2, ClipCount: 2, NativeSignatureSHA256: sig, CaptureGeneration: "g", BoundaryReason: "window_start", ValidationStatus: "lossless_concat_decode_passed", SourceBytes: 2}}
	frame := func(ts int64, hash string) SeamFrameEvidence {
		return SeamFrameEvidence{BestEffortTimestamp: ts, DurationTimestamp: 1, TimeBaseNumerator: 1, TimeBaseDenominator: 30, PictureType: "P", DecodedSHA256: hash, PacketSHA256: hash}
	}
	hashA := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	hashB := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	seam := SeamFact{
		Ordinal: 1, PreviousClipID: 1, NextClipID: 2, CaptureGeneration: "g", PreviousSequence: 1, NextSequence: 2,
		NativeSignatureSHA256: sig, CaptureAttemptID: "attempt-1", TimelineBasis: "continuous_source_pts_v1", CaptureContract: "continuous-source-pts-v1",
		PreviousFrames: []SeamFrameEvidence{frame(27, hashA), frame(28, hashA), frame(29, hashA)},
		NextFrames:     []SeamFrameEvidence{frame(30, hashA), frame(31, hashB), frame(32, hashB)},
		Confidence:     "high", Verdict: "exact", Reason: "frame_adjacency_proven",
	}
	if err := ValidateSeams(clips, runs, []SeamFact{seam}); err == nil {
		t.Fatal("v1 endpoint-only provenance authorized an exact seam")
	}
	if err := ValidatePartialSeams(clips, runs, []SeamFact{seam}); err == nil {
		t.Fatal("aggregate UNKNOWN persisted a client-authored exact v1 seam")
	}
	seam.TimelineBasis, seam.CaptureContract = "unavailable", ""
	seam.PreviousFrames, seam.NextFrames = nil, nil
	seam.Confidence, seam.Verdict, seam.Reason = "none", "ambiguous", "continuous_source_pts_unavailable"
	if err := ValidatePartialSeams(clips, runs, []SeamFact{seam}); err != nil {
		t.Fatalf("honest v1 ambiguity was rejected: %v", err)
	}
}

func TestValidatePartialSeamsRequiresEveryAdjacentPairWithoutSingletonEscape(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Second)
	sig := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	base := []ManifestClip{
		mc(1, start, start.Add(time.Second), "g", 1, 1),
		mc(2, start.Add(time.Second), start.Add(2*time.Second), "g", 2, 1),
		mc(3, start.Add(2*time.Second), start.Add(3*time.Second), "g", 3, 1),
	}
	clips := make([]ClipFact, len(base))
	for i := range base {
		clips[i] = ClipFact{ManifestClip: base[i], NativeSignatureSHA256: sig, StrictDecode: "passed"}
	}
	runs := []RunFact{{Ordinal: 1, FirstClipOrdinal: 1, LastClipOrdinal: 3, ClipCount: 3, NativeSignatureSHA256: sig, CaptureGeneration: "g", BoundaryReason: "window_start", ValidationStatus: "lossless_concat_decode_passed", SourceBytes: 3}}
	ambiguous := func(i int) SeamFact {
		return SeamFact{Ordinal: i, PreviousClipID: int64(i), NextClipID: int64(i + 1), CaptureGeneration: "g", PreviousSequence: int64(i), NextSequence: int64(i + 1), NativeSignatureSHA256: sig, TimelineBasis: "unavailable", Confidence: "none", Verdict: "ambiguous", Reason: "continuous_source_pts_unavailable"}
	}
	if err := ValidatePartialSeams(clips, runs, []SeamFact{ambiguous(1), ambiguous(2)}); err != nil {
		t.Fatal(err)
	}
	if err := ValidatePartialSeams(clips, runs, []SeamFact{ambiguous(1)}); err == nil {
		t.Fatal("partial evidence omitted an adjacent pair")
	}
	singletons := []RunFact{
		{Ordinal: 1, FirstClipOrdinal: 1, LastClipOrdinal: 1, ClipCount: 1, NativeSignatureSHA256: sig, CaptureGeneration: "g", BoundaryReason: "window_start", ValidationStatus: "single_clip_decode_only", SourceBytes: 1},
		{Ordinal: 2, FirstClipOrdinal: 2, LastClipOrdinal: 2, ClipCount: 1, NativeSignatureSHA256: sig, CaptureGeneration: "g", BoundaryReason: "unproven_frame_adjacency", ValidationStatus: "single_clip_decode_only", SourceBytes: 1},
		{Ordinal: 3, FirstClipOrdinal: 3, LastClipOrdinal: 3, ClipCount: 1, NativeSignatureSHA256: sig, CaptureGeneration: "g", BoundaryReason: "unproven_frame_adjacency", ValidationStatus: "single_clip_decode_only", SourceBytes: 1},
	}
	if err := ValidatePartialRuns(clips, singletons); err == nil {
		t.Fatal("ambiguous adjacency was used to manufacture singleton runs")
	}
}

func TestValidateAudioSeamsSeparatesAbsentAmbiguousAndExact(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Second)
	base := []ManifestClip{mc(1, start, start.Add(time.Second), "g", 1, 1), mc(2, start.Add(time.Second), start.Add(2*time.Second), "g", 2, 1)}
	clips := []ClipFact{{ManifestClip: base[0]}, {ManifestClip: base[1]}}
	absent := AudioSeamFact{Ordinal: 1, PreviousClipID: 1, NextClipID: 2, Verdict: "not_present", Reason: "audio_not_present"}
	if err := ValidateAudioSeams(clips, []AudioSeamFact{absent}, false); err != nil {
		t.Fatal(err)
	}
	clips[0].NativeSignatureSHA256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	clips[1].NativeSignatureSHA256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	boundary := AudioSeamFact{Ordinal: 1, PreviousClipID: 1, NextClipID: 2, Verdict: "not_applicable", Reason: "native_signature_change"}
	if err := ValidateAudioSeams(clips, []AudioSeamFact{boundary}, false); err != nil {
		t.Fatalf("audio-free objective run boundary was not preserved: %v", err)
	}
	clips[1].NativeSignatureSHA256 = clips[0].NativeSignatureSHA256
	clips[0].AudioTimeline = &AudioTimelineEvidence{SampleRate: 48000, FirstSample: 0, EndSample: 48000, SampleCount: 48000}
	clips[1].AudioTimeline = &AudioTimelineEvidence{SampleRate: 48000, FirstSample: 48000, EndSample: 96000, SampleCount: 48000}
	clips[0].AudioPresent, clips[1].AudioPresent = true, true
	ambiguous := AudioSeamFact{Ordinal: 1, PreviousClipID: 1, NextClipID: 2, Verdict: "ambiguous", Reason: "continuous_source_pts_unavailable"}
	if err := ValidateAudioSeams(clips, []AudioSeamFact{ambiguous}, false); err != nil {
		t.Fatal(err)
	}
	if err := ValidateAudioSeams(clips, []AudioSeamFact{ambiguous}, true); err == nil {
		t.Fatal("ambiguous audio seam passed the exact gate")
	}
	for i := range clips {
		clips[i].CaptureAttemptID = "attempt"
		clips[i].TimestampContractVersion = "continuous-source-pts-v1"
		clips[i].TimestampContractStatus = "per_clip_probe_complete"
		clips[i].TimestampContractSHA256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	}
	exact := AudioSeamFact{Ordinal: 1, PreviousClipID: 1, NextClipID: 2, SampleRate: 48000, PreviousEndSample: 48000, NextStartSample: 48000, PreviousSampleCount: 48000, NextSampleCount: 48000, CaptureAttemptID: "attempt", TimestampContract: "continuous-source-pts-v1", Verdict: "exact", Reason: "audio_sample_adjacency_proven"}
	if err := ValidateAudioSeams(clips, []AudioSeamFact{exact}, true); err != nil {
		t.Fatal(err)
	}
	exact.PreviousSampleCount--
	if err := ValidateAudioSeams(clips, []AudioSeamFact{exact}, true); err == nil {
		t.Fatal("client-authored audio seam count diverged from per-clip sample evidence")
	}
}

func TestValidateAxisStatusesAllowsTerminalHistoricalPartialOnly(t *testing.T) {
	report := Report{Status: "partial", NASByteDecodeStatus: "passed", NativeRunConcatStatus: "passed", WithinRunFrameAdjacencyStatus: "unknown", WithinRunAudioContinuityStatus: "unknown", WindowContinuityStatus: "unknown"}
	if err := ValidateAxisStatuses(report); err != nil {
		t.Fatal(err)
	}
	report.Status = "passed"
	if err := ValidateAxisStatuses(report); err == nil {
		t.Fatal("full pass accepted unknown frame/audio axes")
	}
	report = Report{Status: "unknown", NASByteDecodeStatus: "unknown", NativeRunConcatStatus: "unknown", WithinRunFrameAdjacencyStatus: "passed", WithinRunAudioContinuityStatus: "unknown", WindowContinuityStatus: "unknown"}
	if err := ValidateAxisStatuses(report); err == nil {
		t.Fatal("retryable UNKNOWN asserted a passing presentation axis")
	}
	report = Report{Status: "failed", NASByteDecodeStatus: "failed", NativeRunConcatStatus: "unknown", WithinRunFrameAdjacencyStatus: "unknown", WithinRunAudioContinuityStatus: "unknown", WindowContinuityStatus: "unknown"}
	if err := ValidateAxisStatuses(report); err != nil {
		t.Fatal(err)
	}
	report.NativeRunConcatStatus = "failed"
	if err := ValidateAxisStatuses(report); err == nil {
		t.Fatal("FAILED asserted two incompatible deterministic axes")
	}
	report = Report{Status: "failed", NASByteDecodeStatus: "failed", NativeRunConcatStatus: "unknown", WithinRunFrameAdjacencyStatus: "unknown", WithinRunAudioContinuityStatus: "unknown", WindowContinuityStatus: "unknown", Clips: []ClipFact{{StrictDecode: "failed"}}}
	if err := ValidateDeterministicFailureEvidence(report, []string{"clip_decode_failed"}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateDeterministicFailureEvidence(report, []string{"clip_decode_failed", "run_concat_failed"}); err == nil {
		t.Fatal("FAILED claimed multiple defects after verification stopped at the primary failure")
	}
}

func TestWithinRunAdjacencyPassAllowsObjectivePartitionButNotSeamlessWindow(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Second)
	sigA := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	sigB := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	base := []ManifestClip{
		mc(1, start, start.Add(time.Second), "g", 1, 1),
		mc(2, start.Add(time.Second), start.Add(2*time.Second), "g", 2, 1),
		mc(3, start.Add(2*time.Second), start.Add(3*time.Second), "g", 3, 1),
	}
	for i := range base {
		base[i].CaptureAttemptID = "attempt"
		base[i].TimestampContractVersion = "continuous-source-pts-v1"
		base[i].TimestampContractStatus = "per_clip_probe_complete"
		base[i].TimestampContractSHA256 = sigA
	}
	clips := []ClipFact{
		{ManifestClip: base[0], NativeSignatureSHA256: sigA, StrictDecode: "passed"},
		{ManifestClip: base[1], NativeSignatureSHA256: sigA, StrictDecode: "passed"},
		{ManifestClip: base[2], NativeSignatureSHA256: sigB, StrictDecode: "passed"},
	}
	for i := range clips {
		first := int64(27 + i*3)
		clips[i].RecomputedTimestampContract = &TimestampContract{Version: 1, Mode: "muxed_source_copy", AudioSelection: "first_optional", Tracks: []TimestampContractTrack{{MediaType: "video", TimeBaseNum: 1, TimeBaseDen: 30, FirstTimestamp: first, LastTimestamp: first + 2, LastDuration: 1, UnitCount: 3}}}
	}
	runs := []RunFact{
		{Ordinal: 1, FirstClipOrdinal: 1, LastClipOrdinal: 2, ClipCount: 2, NativeSignatureSHA256: sigA, CaptureGeneration: "g", CaptureAttemptID: "attempt", TimestampContract: "continuous-source-pts-v1", BoundaryReason: "window_start", ValidationStatus: "lossless_concat_decode_passed", SourceBytes: 2},
		{Ordinal: 2, FirstClipOrdinal: 3, LastClipOrdinal: 3, ClipCount: 1, NativeSignatureSHA256: sigB, CaptureGeneration: "g", CaptureAttemptID: "attempt", TimestampContract: "continuous-source-pts-v1", BoundaryReason: "native_signature_change", ValidationStatus: "single_clip_decode_only", SourceBytes: 1},
	}
	frame := func(ts int64) SeamFrameEvidence {
		return SeamFrameEvidence{BestEffortTimestamp: ts, DurationTimestamp: 1, TimeBaseNumerator: 1, TimeBaseDenominator: 30, PictureType: "P", DecodedSHA256: sigA, PacketSHA256: sigA}
	}
	seams := []SeamFact{
		{Ordinal: 1, PreviousClipID: 1, NextClipID: 2, CaptureGeneration: "g", PreviousSequence: 1, NextSequence: 2, NativeSignatureSHA256: sigA, CaptureAttemptID: "attempt", TimelineBasis: "continuous_source_pts_v1", CaptureContract: "continuous-source-pts-v1", PreviousFrames: []SeamFrameEvidence{frame(27), frame(28), frame(29)}, NextFrames: []SeamFrameEvidence{frame(30), frame(31), frame(32)}, Confidence: "high", Verdict: "exact", Reason: "frame_adjacency_proven"},
		{Ordinal: 2, PreviousClipID: 2, NextClipID: 3, PreviousSequence: 2, NextSequence: 3, Verdict: "not_applicable", Reason: "native_signature_change"},
	}
	if err := ValidateRuns(clips, runs); err != nil {
		t.Fatal(err)
	}
	if err := ValidateSeams(clips, runs, seams); err == nil {
		t.Fatal("v1 exact seam escaped server-owned edge provenance gate")
	}
	seams[0].Verdict = "ambiguous"
	seams[0].Reason = "continuous_source_pts_unavailable"
	seams[0].TimelineBasis = "unavailable"
	seams[0].Confidence = "none"
	seams[0].PreviousFrames = nil
	seams[0].NextFrames = nil
	if err := ValidatePartialSeams(clips, runs, seams); err != nil {
		t.Fatal(err)
	}
	report := Report{Status: "partial", NASByteDecodeStatus: "passed", NativeRunConcatStatus: "passed", WithinRunFrameAdjacencyStatus: "unknown", WithinRunAudioContinuityStatus: "not_present", WindowContinuityStatus: "partitioned", NativeRuns: runs}
	if err := ValidateAxisStatuses(report); err != nil {
		t.Fatal(err)
	}
	report.Status, report.WindowContinuityStatus = "passed", "passed"
	if err := ValidateAxisStatuses(report); err == nil {
		t.Fatal("multi-run proof was mislabeled as one seamless window")
	}
}

func TestValidateClipAndSeamEvidenceCannotDivergeFromRecomputedContract(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Second)
	sig := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	base := []ManifestClip{mc(1, start, start.Add(time.Second), "g", 1, 1), mc(2, start.Add(time.Second), start.Add(2*time.Second), "g", 2, 1)}
	for i := range base {
		base[i].CaptureAttemptID = "attempt"
		base[i].TimestampContractVersion = "continuous-source-pts-v1"
		base[i].TimestampContractStatus = "per_clip_probe_complete"
		base[i].TimestampContractSHA256 = sig
	}
	makeClip := func(c ManifestClip, first int64) ClipFact {
		contract := &TimestampContract{Version: 1, Mode: "muxed_source_copy", AudioSelection: "first_optional", Tracks: []TimestampContractTrack{{MediaType: "video", TimeBaseNum: 1, TimeBaseDen: 30, FirstTimestamp: first, LastTimestamp: first + 2, LastDuration: 1, UnitCount: 3}}}
		return ClipFact{ManifestClip: c, NativeSignatureSHA256: sig, StrictDecode: "passed", RecomputedTimestampContract: contract,
			VideoTimeline: &StreamTimelineEvidence{FrameCount: 3, FirstTimestamp: first, LastTimestamp: first + 2, LastDurationTimestamp: 1, TimeBaseNumerator: 1, TimeBaseDenominator: 30}}
	}
	clips := []ClipFact{makeClip(base[0], 27), makeClip(base[1], 30)}
	if err := ValidateClipTimelines(clips, false, false); err != nil {
		t.Fatal(err)
	}
	withAudioTrack := *clips[0].RecomputedTimestampContract
	withAudioTrack.Tracks = append(append([]TimestampContractTrack(nil), withAudioTrack.Tracks...), TimestampContractTrack{
		MediaType: "audio", TimeBaseNum: 1, TimeBaseDen: 48000, FirstTimestamp: 0,
		LastTimestamp: 47999, LastDuration: 1, UnitCount: 48000, SampleRate: 48000, LastSampleCount: 1,
	})
	clips[0].RecomputedTimestampContract = &withAudioTrack
	if err := ValidateClipTimelines(clips, false, false); err == nil {
		t.Fatal("audio-bearing exact-byte contract was forged as audio-free")
	}
	clips[0].AudioPresent = true
	clips[0].RecomputedTimestampContract = makeClip(base[0], 27).RecomputedTimestampContract
	if err := ValidateClipTimelines(clips, false, false); err == nil {
		t.Fatal("video-only exact-byte contract was forged as audio-bearing")
	}
	clips[0] = makeClip(base[0], 27)
	clips[0].VideoTimeline.LastTimestamp = 28
	if err := ValidateClipTimelines(clips, false, false); err == nil {
		t.Fatal("client-authored video timeline diverged from exact-byte contract")
	}
	clips[0] = makeClip(base[0], 27)
	frame := func(ts int64) SeamFrameEvidence {
		return SeamFrameEvidence{BestEffortTimestamp: ts, DurationTimestamp: 1, TimeBaseNumerator: 1, TimeBaseDenominator: 30, PictureType: "P", DecodedSHA256: sig, PacketSHA256: sig}
	}
	runs := []RunFact{{Ordinal: 1, FirstClipOrdinal: 1, LastClipOrdinal: 2, ClipCount: 2, NativeSignatureSHA256: sig, CaptureGeneration: "g", CaptureAttemptID: "attempt", TimestampContract: "continuous-source-pts-v1", BoundaryReason: "window_start", ValidationStatus: "lossless_concat_decode_passed", SourceBytes: 2}}
	seam := SeamFact{Ordinal: 1, PreviousClipID: 1, NextClipID: 2, CaptureGeneration: "g", PreviousSequence: 1, NextSequence: 2, NativeSignatureSHA256: sig, CaptureAttemptID: "attempt", TimelineBasis: "continuous_source_pts_v1", CaptureContract: "continuous-source-pts-v1", PreviousFrames: []SeamFrameEvidence{frame(27), frame(28), frame(29)}, NextFrames: []SeamFrameEvidence{frame(30), frame(31), frame(32)}, Confidence: "high", Verdict: "exact", Reason: "frame_adjacency_proven"}
	if err := ValidateSeams(clips, runs, []SeamFact{seam}); err == nil {
		t.Fatal("v1 exact seam escaped server-owned edge provenance gate")
	}
	seam.PreviousFrames[2].BestEffortTimestamp = 26
	seam.NextFrames[0].BestEffortTimestamp = 27
	if err := ValidateSeams(clips, runs, []SeamFact{seam}); err == nil {
		t.Fatal("fabricated but internally adjacent edge escaped exact-byte contract binding")
	}
}

func TestValidateSingleClipCannotBypassPresentationAxisProvenance(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Second)
	historical := ClipFact{ManifestClip: mc(1, start, start.Add(time.Second), "g", 1, 1), StrictDecode: "passed"}
	if err := ValidateClipTimelines([]ClipFact{historical}, true, false); err == nil {
		t.Fatal("historical single clip falsely passed video presentation axis")
	}
	audio := historical
	audio.AudioPresent = true
	if err := ValidateClipTimelines([]ClipFact{audio}, false, true); err == nil {
		t.Fatal("audio-present single clip falsely passed without sample evidence")
	}
	if err := ValidateAudioAxisStatus([]ClipFact{historical}, "not_present"); err != nil {
		t.Fatal(err)
	}
	if err := ValidateAudioAxisStatus([]ClipFact{audio}, "not_present"); err == nil {
		t.Fatal("audio-present clip was mislabeled not_present")
	}
	if err := ValidateAudioAxisStatus([]ClipFact{historical}, "unknown"); err == nil {
		t.Fatal("audio-free completed proof was mislabeled unknown")
	}
}

func TestIncompleteReportsCannotCarryExtraOrLaterFacts(t *testing.T) {
	unknown := Report{Status: "unknown", Clips: []ClipFact{{StrictDecode: "unknown"}}}
	if err := ValidateUnknownEvidenceEmpty(unknown); err == nil {
		t.Fatal("UNKNOWN report carried a clip fact")
	}
	unknown.Clips = nil
	if err := ValidateUnknownEvidenceEmpty(unknown); err != nil {
		t.Fatal(err)
	}
	clipFailure := Report{Status: "failed", NASByteDecodeStatus: "failed", NativeRunConcatStatus: "unknown", Clips: []ClipFact{{StrictDecode: "failed"}, {StrictDecode: "passed"}}}
	if err := ValidateDeterministicFailureEvidence(clipFailure, []string{"clip_decode_failed"}); err == nil {
		t.Fatal("clip failure carried a later clip fact")
	}
	clipFailure.Clips = clipFailure.Clips[:1]
	if err := ValidateDeterministicFailureEvidence(clipFailure, []string{"clip_decode_failed"}); err != nil {
		t.Fatal(err)
	}
	runFailure := Report{Status: "failed", NASByteDecodeStatus: "unknown", NativeRunConcatStatus: "failed", Clips: []ClipFact{{StrictDecode: "passed"}}, NativeRuns: []RunFact{{ValidationStatus: "failed"}, {ValidationStatus: "lossless_concat_decode_passed"}}}
	if err := ValidateDeterministicFailureEvidence(runFailure, []string{"run_concat_failed"}); err == nil {
		t.Fatal("run failure carried a later run fact")
	}
	runFailure.NativeRuns = runFailure.NativeRuns[:1]
	if err := ValidateDeterministicFailureEvidence(runFailure, []string{"run_concat_failed"}); err != nil {
		t.Fatal(err)
	}
}

func TestValidateManifestRejectsBoundaryAndUnsafePath(t *testing.T) {
	start := time.Now().UTC()
	c := mc(1, start.Add(-time.Minute), start, "g", 1, 1)
	if _, err := ValidateManifest([]ManifestClip{c}, 7, 9, start, start.Add(time.Hour)); err == nil {
		t.Fatal("zero-intersection boundary clip accepted")
	}
	c = mc(1, start, start.Add(time.Minute), "g", 1, 1)
	c.RelativePath = "safe/../escape.mp4"
	if _, err := ValidateManifest([]ManifestClip{c}, 7, 9, start, start.Add(time.Hour)); err == nil {
		t.Fatal("unsafe path accepted")
	}
}
