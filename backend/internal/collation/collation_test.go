package collation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/recordingnaming"
)

func baseClips() (Clip, Clip, ClipMedia, ClipMedia) {
	t0 := time.Date(2026, 8, 13, 15, 0, 0, 0, time.UTC)
	a := Clip{ClipID: 1, RecordingID: 7, JobID: 70, CaptureSequence: 5, StartUTC: t0, EndUTC: t0.Add(60 * time.Second), NominalSeconds: 60}
	b := Clip{ClipID: 2, RecordingID: 7, JobID: 70, CaptureSequence: 6, StartUTC: t0.Add(60 * time.Second), EndUTC: t0.Add(120 * time.Second)}
	m := ClipMedia{Playable: true, CodecSignature: "abc", VideoPackets: 1500, ContentSeconds: 60, FrameSeconds: 0.04}
	return a, b, m, m
}

func TestMetadataGateRequiresEveryCondition(t *testing.T) {
	policy := DefaultSeamPolicy()
	a, b, am, bm := baseClips()
	if r := MetadataGate(policy, a, b, am, bm); r != "" {
		t.Fatalf("clean seam rejected: %s", r)
	}
	cases := map[string]func(a, b *Clip, am, bm *ClipMedia){
		"different_recording":                func(a, b *Clip, _, _ *ClipMedia) { b.RecordingID = 8 },
		"different_job":                      func(a, b *Clip, _, _ *ClipMedia) { b.JobID = 71 },
		"missing_job":                        func(a, b *Clip, _, _ *ClipMedia) { a.JobID, b.JobID = 0, 0 },
		"different_capture_attempt":          func(a, b *Clip, _, _ *ClipMedia) { a.CaptureAttemptID, b.CaptureAttemptID = "x", "y" },
		"capture_attempt_indicator_changed":  func(a, b *Clip, _, _ *ClipMedia) { b.CaptureAttemptID = "y" },
		"different_capture_lease":            func(a, b *Clip, _, _ *ClipMedia) { a.CaptureLeaseToken, b.CaptureLeaseToken = "x", "y" },
		"capture_sequence_not_consecutive":   func(a, b *Clip, _, _ *ClipMedia) { b.CaptureSequence = 8 },
		"capture_sequence_indicator_changed": func(a, b *Clip, _, _ *ClipMedia) { b.CaptureSequence = 0 },
		"unplayable":                         func(_, _ *Clip, am, _ *ClipMedia) { am.Playable = false },
		"codec_params_differ":                func(_, _ *Clip, _, bm *ClipMedia) { bm.CodecSignature = "def" },
		"db_gap":                             func(_, b *Clip, _, _ *ClipMedia) { b.StartUTC = b.StartUTC.Add(50 * time.Millisecond) },
		"db_overlap":                         func(_, b *Clip, _, _ *ClipMedia) { b.StartUTC = b.StartUTC.Add(-50 * time.Millisecond) },
		"content_span_mismatch":              func(_, _ *Clip, am, _ *ClipMedia) { am.ContentSeconds = 59.8 },
		"prev_clip_cut_short": func(a, b *Clip, am, _ *ClipMedia) {
			a.EndUTC = a.EndUTC.Add(-10 * time.Second)
			am.ContentSeconds = 50
			b.StartUTC = a.EndUTC
		},
	}
	for want, mutate := range cases {
		a, b, am, bm := baseClips()
		mutate(&a, &b, &am, &bm)
		if got := MetadataGate(policy, a, b, am, bm); got != want {
			t.Errorf("%s: got %q", want, got)
		}
		ev := MatchEvidence{Verdict: MatchContinuous}
		if d := DecideSeam(policy, a, b, am, bm, &ev); d.Decision != DecisionSplit {
			t.Errorf("%s: joined despite failed gate", want)
		}
	}
	// Within one frame of DB gap is tolerated.
	a, b, am, bm = baseClips()
	b.StartUTC = b.StartUTC.Add(39 * time.Millisecond)
	if r := MetadataGate(policy, a, b, am, bm); r != "" {
		t.Fatalf("sub-frame gap rejected: %s", r)
	}
	// Frame-match verdict gates the join.
	a, b, am, bm = baseClips()
	for _, v := range []string{MatchJump, MatchOverlap, MatchLowMotion, MatchTooShort, MatchDecodeFail} {
		ev := MatchEvidence{Verdict: v}
		if d := DecideSeam(policy, a, b, am, bm, &ev); d.Decision != DecisionSplit || d.Reason != "frame_"+v {
			t.Errorf("verdict %s: %+v", v, d)
		}
	}
	if d := DecideSeam(policy, a, b, am, bm, nil); d.Decision != DecisionSplit {
		t.Error("joined without frame match")
	}
}

func curve(n int, f func(i int) float64) []float64 {
	out := make([]float64, n)
	for i := range out {
		out[i] = f(i)
	}
	return out
}

func TestEvaluateCurvesVerdicts(t *testing.T) {
	policy := DefaultSeamPolicy()
	n := 250
	steps := curve(2*n-2, func(int) float64 { return 0.4 })
	// Continuous: distance grows with temporal distance from the boundary.
	cont := Curves{TailToHead0: curve(n, func(k int) float64 { return 0.4 + 0.02*float64(n-1-k) }),
		TailLastToHead: curve(n, func(j int) float64 { return 0.4 + 0.02*float64(j) }), Steps: steps, BoundaryMAD: 0.45}
	if ev := EvaluateCurves(policy, cont, 0.04); ev.Verdict != MatchContinuous {
		t.Fatalf("continuous: %+v", ev)
	}
	// Boundary step much larger than an ordinary step: a small jump.
	small := cont
	small.BoundaryMAD = 1.5
	if ev := EvaluateCurves(policy, small, 0.04); ev.Verdict != MatchJump {
		t.Fatalf("small jump: %+v", ev)
	}
	// Overlap: B[0] sharply matches 100 frames before A's end.
	over := cont
	over.TailToHead0 = curve(n, func(k int) float64 { return 0.4 + 0.05*math.Abs(float64(n-1-100-k)) })
	if ev := EvaluateCurves(policy, over, 0.04); ev.Verdict != MatchOverlap || ev.OverlapSeconds != 4 {
		t.Fatalf("overlap: %+v", ev)
	}
	// Jump: no sharp minimum anywhere.
	jump := Curves{TailToHead0: curve(n, func(k int) float64 { return 5 + 0.1*math.Sin(float64(k)) }),
		TailLastToHead: curve(n, func(j int) float64 { return 5 + 0.1*math.Cos(float64(j)) }), Steps: steps, BoundaryMAD: 5}
	if ev := EvaluateCurves(policy, jump, 0.04); ev.Verdict != MatchJump {
		t.Fatalf("jump: %+v", ev)
	}
	// Static scene: nothing can be proven.
	static := Curves{TailToHead0: curve(n, func(int) float64 { return 0.3 }), TailLastToHead: curve(n, func(int) float64 { return 0.3 }), Steps: steps, BoundaryMAD: 0.3}
	if ev := EvaluateCurves(policy, static, 0.04); ev.Verdict != MatchLowMotion {
		t.Fatalf("static: %+v", ev)
	}
	short := Curves{TailToHead0: []float64{1}, TailLastToHead: []float64{1}}
	if ev := EvaluateCurves(policy, short, 0.04); ev.Verdict != MatchTooShort {
		t.Fatalf("short: %+v", ev)
	}
}

func testWork() HourWork {
	return HourWork{BatchID: "goodplus-20260821-generation-1", RecordingID: 418, Timezone: "Europe/Prague", LocalDate: "2026-08-13", DeliveryHour: 12,
		NamingProfile: string(recordingnaming.ProfilePlazaHourlyV1), FolderName: "100017_Europe_Czechia_Valask_Mezi_Valasske_Valask_Mezi_Valasske_-_Market_square",
		Metadata: recordingnaming.Metadata{PlazaID: "100017", Continent: "Europe", Country: "Czechia", City: "Valašské Meziříčí (Valasske)", PlazaName: "Valašské Meziříčí (Valasske) - Market square"},
	}
}

func TestDeliveryPathMirrorsRawFolderFormat(t *testing.T) {
	w := testWork()
	w.ScheduledStart = time.Date(2026, 8, 13, 17, 0, 0, 0, time.UTC)
	w.ScheduledEnd = w.ScheduledStart.Add(time.Hour)
	start := time.Date(2026, 8, 13, 17, 0, 8, 0, time.UTC)
	got, err := DeliveryPath(w, start, start.Add(20*time.Minute), 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	want := "100017_Europe_Czechia_Valask_Mezi_Valasske_Valask_Mezi_Valasske_-_Market_square/August/13-Thursday/100017_Valask_Mezi_Valasske_-_Market_square_2026_August_W2_Thursday_hour_19_part_02_190008-192008.mp4"
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	w.NamingProfile, w.FolderName, w.RecordingID = string(recordingnaming.ProfileStoaramaV1), "recordings", 339
	got, err = DeliveryPath(w, start, start.Add(time.Hour-time.Second), 1, 1)
	if err != nil || got != "recordings/339/joined/2026-08-13/339_2026-08-13_hour_19_190008-200007.mp4" {
		t.Fatalf("stoarama_v1 path %q %v", got, err)
	}
	if _, err := DeliveryPath(w, start, start, 1, 1); err == nil {
		t.Fatal("empty range accepted")
	}
}

func TestHourIDAndKeys(t *testing.T) {
	id, err := HourIDFor("goodplus-20260821-generation-1", 418, "2026-08-13", 12)
	if err != nil || id != "goodplus-20260821-generation-1__recording-418__date-2026-08-13__hour-12__generation-2" {
		t.Fatalf("%s %v", id, err)
	}
	if ManifestKey("b", id) != "joined/b/coverage/hours/"+id+".json" || ObjectKey("b", "ff") != "joined/b/objects/ff.mp4" {
		t.Fatal("layout differs from generation-1 joined layout")
	}
	for _, bad := range []int{0, 13} {
		if _, err := HourIDFor("b", 1, "2026-08-13", bad); err == nil {
			t.Fatalf("hour %d accepted", bad)
		}
	}
}

func TestConcatOffsetsRoundAgainstRunningTotal(t *testing.T) {
	// mk builds a clip whose video starts lead seconds after its audio.
	mk := func(num, den int64, n int, lead *big.Rat) LocalClip {
		sp := &StreamPackets{Kind: "video"}
		at := new(big.Rat).Set(lead)
		for i := 0; i < n; i++ {
			sp.Packets = append(sp.Packets, Packet{PTS: new(big.Rat).Set(at), Dur: big.NewRat(num, den)})
			at.Add(at, big.NewRat(num, den))
		}
		audio := &StreamPackets{Kind: "audio", Packets: []Packet{{PTS: new(big.Rat), Dur: big.NewRat(1, 50)}}}
		return LocalClip{Probe: Probe{Video: sp, Audio: audio}}
	}
	zero := new(big.Rat)
	clips := []LocalClip{mk(1, 30, 1, zero), mk(1, 30, 1, zero), mk(1, 30, 1, zero)}
	offsets, durs := concatOffsets(clips)
	if fmt.Sprint(durs) != "[33333 33334 33333]" {
		t.Fatalf("durations %v", durs)
	}
	if f, _ := offsets[2].Float64(); f != 0.066667 {
		t.Fatalf("offset %v", f)
	}
	// Video starting 0.1 s after audio in clip 1: its extent is 0.1 + 1/30.
	clips = []LocalClip{mk(1, 30, 1, big.NewRat(1, 10)), mk(1, 30, 1, zero)}
	offsets, durs = concatOffsets(clips)
	if f, _ := offsets[1].Float64(); f != 0.133333 || fmt.Sprint(durs) != "[133333 33334]" {
		t.Fatalf("lead offsets %v %v", f, durs)
	}
	// Audio outlasting video: the next clip starts after the audio, never
	// overlapping it.
	long := mk(1, 30, 1, zero)
	long.Probe.Audio.Packets[0].Dur = big.NewRat(1, 10)
	offsets, durs = concatOffsets([]LocalClip{long, mk(1, 30, 1, zero)})
	if f, _ := offsets[1].Float64(); f != 0.1 || fmt.Sprint(durs) != "[100000 33333]" {
		t.Fatalf("audio extent offsets %v %v", f, durs)
	}
}

func TestManifestValidateRejectsInconsistentAccounting(t *testing.T) {
	id, _ := HourIDFor("b", 1, "2026-08-13", 1)
	good := HourManifest{SchemaVersion: 1, PolicyVersion: PolicyVersion, Generation: Generation, Status: StatusCollated, BatchID: "b", HourID: id,
		RecordingID: 1, LocalDate: "2026-08-13", DeliveryHour: 1,
		Clips: []ClipDisposition{{Clip: Clip{ClipID: 1}, Disposition: "included", Part: 1}, {Clip: Clip{ClipID: 2}, Disposition: "included", Part: 1},
			{Clip: Clip{ClipID: 3}, Disposition: "quarantined", Reason: "unplayable"}},
		Seams: []SeamDecision{{PrevClipID: 1, NextClipID: 2, Decision: DecisionJoin, Match: &MatchEvidence{Verdict: MatchContinuous}}},
		Outputs: []Output{{Part: 1, Parts: 1, SHA256: strings.Repeat("a", 64), SizeBytes: 1, SourceClipIDs: []int64{1, 2},
			Verification: Verification{PayloadChainMatches: true, VideoTimingMatches: true, EdgeDecodeOK: true}}}}
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	bad := good
	bad.Seams = []SeamDecision{{PrevClipID: 1, NextClipID: 2, Decision: DecisionJoin, Match: &MatchEvidence{Verdict: MatchJump}}}
	if bad.Validate() == nil {
		t.Fatal("join without continuity proof accepted")
	}
	bad = good
	bad.Seams = []SeamDecision{{PrevClipID: 1, NextClipID: 2, Decision: DecisionSplit}}
	if bad.Validate() == nil {
		t.Fatal("split seam inside one part accepted")
	}
	bad = good
	bad.Clips = append(append([]ClipDisposition(nil), good.Clips...), ClipDisposition{Clip: Clip{ClipID: 1}, Disposition: "duplicate", Reason: "x"})
	if bad.Validate() == nil {
		t.Fatal("clip accounted twice accepted")
	}
	bad = good
	bad.Outputs = []Output{good.Outputs[0]}
	bad.Outputs[0].SourceClipIDs = []int64{1}
	if bad.Validate() == nil {
		t.Fatal("included clip missing from its output accepted")
	}
	bad.Outputs[0].SourceClipIDs = []int64{1, 2, 2}
	if bad.Validate() == nil {
		t.Fatal("repeated output source accepted")
	}
}

// --- end-to-end on synthetic media (skipped without ffmpeg) ---

func requireFFmpeg(t *testing.T) Tools {
	tools := ToolsFromEnv()
	if _, err := exec.LookPath(tools.FFmpeg); err != nil {
		t.Skip("ffmpeg not available")
	}
	if _, err := exec.LookPath(tools.FFprobe); err != nil {
		t.Skip("ffprobe not available")
	}
	return tools
}

// synth renders a moving test pattern and cuts keyframe-aligned pieces.
func synth(t *testing.T, tools Tools, dir string) string {
	src := filepath.Join(dir, "src.mp4")
	cmd := exec.Command(tools.FFmpeg, "-nostdin", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=320x240:rate=25:duration=40",
		"-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000:duration=40",
		"-c:v", "libx264", "-g", "25", "-keyint_min", "25", "-sc_threshold", "0", "-bf", "0", "-pix_fmt", "yuv420p", "-c:a", "aac", "-shortest", src)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("cannot synthesize test media: %v %s", err, out)
	}
	return src
}

func cut(t *testing.T, tools Tools, src, dst string, from, dur float64) {
	cmd := exec.Command(tools.FFmpeg, "-nostdin", "-v", "error", "-ss", fmt.Sprint(from), "-i", src, "-t", fmt.Sprint(dur), "-c", "copy", "-avoid_negative_ts", "make_zero", dst)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("cut: %v %s", err, out)
	}
}

type dirStore struct {
	src       string
	mu        sync.Mutex
	published map[string]string
}

func (s *dirStore) Download(_ context.Context, c Clip, dst string) (int64, string, error) {
	in, err := os.Open(filepath.Join(s.src, c.ObjectKey))
	if err != nil {
		return 0, "", err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return 0, "", err
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(out, h), in)
	out.Close()
	return n, hex.EncodeToString(h.Sum(nil)), err
}

func (s *dirStore) PublishVerified(_ context.Context, key, _, path string, size int64, sha string) error {
	n, got, err := fileIdentity(path)
	if err != nil || n != size || got != sha {
		return fmt.Errorf("publish identity differs")
	}
	s.mu.Lock()
	s.published[key] = sha
	s.mu.Unlock()
	return nil
}

func (s *dirStore) PutManifestIfAbsent(_ context.Context, key string, body []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.published[key]; ok {
		return fmt.Errorf("manifest exists")
	}
	s.published[key] = string(body)
	return nil
}

func TestProcessHourSyntheticContinuousJumpOverlap(t *testing.T) {
	tools := requireFFmpeg(t)
	dir := t.TempDir()
	src := synth(t, tools, dir)
	// A=[0,10) B=[10,20) continuous; C=[23,33) jumps 3 s; D=[20,30) would be
	// continuous after B but we give it to a later hour. E=[6,16) overlaps A.
	pieces := map[string][2]float64{"a.mp4": {0, 10}, "b.mp4": {10, 10}, "c.mp4": {23, 10}, "e.mp4": {6, 10}}
	for name, r := range pieces {
		cut(t, tools, src, filepath.Join(dir, name), r[0], r[1])
	}
	ctx := context.Background()
	policy := DefaultSeamPolicy()
	probe := func(name string) Probe {
		p, err := ProbeFile(ctx, tools, filepath.Join(dir, name))
		if err != nil || !p.Media.Playable {
			t.Fatalf("probe %s: %v %+v", name, err, p.Media)
		}
		return p
	}
	window := func(name string, tail bool) []Frame {
		f, err := ExtractWindow(ctx, tools, policy, filepath.Join(dir, name), tail)
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	pa := probe("a.mp4")
	eval := func(a, b string) MatchEvidence {
		tail, head := window(a, true), window(b, false)
		return EvaluateFrames(policy, tail, head, probe(a).Video.WindowKeys(len(tail), true), probe(b).Video.WindowKeys(len(head), false), pa.Media.FrameSeconds)
	}
	if ev := eval("a.mp4", "b.mp4"); ev.Verdict != MatchContinuous {
		t.Fatalf("A->B: %+v", ev)
	}
	if ev := eval("a.mp4", "c.mp4"); ev.Verdict == MatchContinuous {
		t.Fatalf("A->C joined a 13 s jump: %+v", ev)
	}
	if ev := eval("a.mp4", "e.mp4"); ev.Verdict != MatchOverlap || math.Abs(ev.OverlapSeconds-4) > 0.1 {
		t.Fatalf("A->E: %+v", ev)
	}

	// A single changed payload byte inside the joined output fails verification.
	ab := []LocalClip{{Clip: Clip{ClipID: 1}, Path: filepath.Join(dir, "a.mp4"), Probe: probe("a.mp4")}, {Clip: Clip{ClipID: 2}, Path: filepath.Join(dir, "b.mp4"), Probe: probe("b.mp4")}}
	built, err := BuildPart(ctx, tools, ab, filepath.Join(dir, "ab.mp4"))
	if err != nil {
		t.Fatalf("build a+b: %v", err)
	}
	raw, err := os.ReadFile(built.Path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)*3/4] ^= 0x5a // inside mdat (faststart puts moov first)
	corrupt := filepath.Join(dir, "corrupt.mp4")
	if err := os.WriteFile(corrupt, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyPart(ctx, tools, ab, corrupt); err == nil || !strings.Contains(err.Error(), "payload chain differs") {
		t.Fatalf("corrupted payload byte not caught: %v", err)
	}

	// Full hour: A,B continuous (join) then C with forged contiguous stamps
	// and consecutive sequence (must still split on pixels).
	t0 := time.Date(2026, 8, 13, 17, 0, 0, 0, time.UTC)
	w := testWork()
	w.HourID, _ = HourIDFor(w.BatchID, w.RecordingID, w.LocalDate, w.DeliveryHour)
	w.ScheduledStart, w.ScheduledEnd = t0, t0.Add(time.Hour)
	var at time.Time = t0
	for i, name := range []string{"a.mp4", "b.mp4", "c.mp4"} {
		p := probe(name)
		n, sha, err := fileIdentity(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		dur := time.Duration(p.Media.ContentSeconds * float64(time.Second))
		w.Clips = append(w.Clips, Clip{ClipID: int64(i + 1), RecordingID: w.RecordingID, JobID: 9, CaptureSequence: int64(i + 1),
			StartUTC: at, EndUTC: at.Add(dur), ObjectKey: name, SizeBytes: n, SHA256: sha})
		at = at.Add(dur)
	}
	// An exact duplicate is dropped, never joined.
	dup := w.Clips[1]
	dup.ClipID = 99
	w.Clips = append(w.Clips, dup)
	store := &dirStore{src: dir, published: map[string]string{}}
	env := Env{Tools: tools, Policy: policy, ScratchRoot: filepath.Join(dir, "scratch"), Store: store, MediaTool: "test",
		CPU: make(chan struct{}, 4), Net: make(chan struct{}, 4), Now: time.Now}
	m, err := ProcessHour(ctx, env, w)
	if err != nil {
		t.Fatal(err)
	}
	seamJSON, _ := json.MarshalIndent(m.Seams, "", " ")
	if len(m.Outputs) != 2 || fmt.Sprint(m.Outputs[0].SourceClipIDs) != "[1 2]" || fmt.Sprint(m.Outputs[1].SourceClipIDs) != "[3]" {
		t.Fatalf("parts %+v seams %s", m.Outputs, seamJSON)
	}
	if m.Clips[2].Disposition != "duplicate" && m.Clips[3].Disposition != "duplicate" {
		t.Fatalf("duplicate not dropped: %+v", m.Clips)
	}
	if m.Outputs[0].Verification.VideoPackets != 500 || !m.Outputs[0].Verification.PayloadChainMatches || m.Outputs[0].Verification.AudioPackets == 0 {
		t.Fatalf("verification %+v", m.Outputs[0].Verification)
	}
	if !strings.Contains(m.Outputs[0].NASRelativePath, "/August/13-Thursday/") || !strings.Contains(m.Outputs[0].NASRelativePath, "_hour_19_part_01_") {
		t.Fatalf("path %s", m.Outputs[0].NASRelativePath)
	}
	if _, ok := store.published[ManifestKey(w.BatchID, w.HourID)]; !ok {
		t.Fatal("manifest not published")
	}
}

func TestReplayOverlapDetectsRepeatedLeadingPackets(t *testing.T) {
	mk := func(hashes ...string) Probe {
		sp := &StreamPackets{Kind: "video"}
		for i, h := range hashes {
			sp.Packets = append(sp.Packets, Packet{PTS: big.NewRat(int64(i), 10), DTS: big.NewRat(int64(i), 10), Dur: big.NewRat(1, 10), Key: i%5 == 0, Hash: h})
		}
		return Probe{Video: sp}
	}
	a := mk("k0", "p1", "p2", "p3", "p4", "k5", "p6", "p7", "p8", "p9")
	// B starts by replaying A from its keyframe k5 (0.5 s of overlap).
	if sec, ok := ReplayOverlap(a, mk("k5", "p6", "p7", "p8", "p9", "kX")); !ok || sec != 0.5 {
		t.Fatalf("keyframe replay: %v %v", sec, ok)
	}
	// A run of three repeated packets further in is a replay too.
	if _, ok := ReplayOverlap(a, mk("kA", "p6", "p7", "p8")); !ok {
		t.Fatal("repeated run missed")
	}
	// Isolated identical P-frames (static scene) are not.
	if _, ok := ReplayOverlap(a, mk("kA", "p6", "pB", "p8", "pC")); ok {
		t.Fatal("isolated identical packets flagged")
	}
	if _, ok := ReplayOverlap(a, mk("kA", "pB", "pC")); ok {
		t.Fatal("fresh packets flagged")
	}
}
