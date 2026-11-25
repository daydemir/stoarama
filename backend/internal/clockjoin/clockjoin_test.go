package clockjoin

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/collation"
	"golang.org/x/image/font"
	"golang.org/x/image/font/gofont/gomonobold"
	"golang.org/x/image/font/opentype"
	"golang.org/x/image/math/fixed"
)

func TestParseClock(t *testing.T) {
	for text, want := range map[string]int{
		"10:00:02":            36002,
		"19092026 10:00:02":   36002, // date glued in front, as the 435 overlay reads
		"2610:00:14":          36014,
		"23:59:59\n":          86399,
		"01:02:03 then 04:05": 3723,
	} {
		if got, ok := ParseClock(text); !ok || got != want {
			t.Errorf("ParseClock(%q) = %d %v, want %d", text, got, ok, want)
		}
	}
	for _, text := range []string{"", "10:00", "24:00:00", "10:60:00", "10-00-02"} {
		if _, ok := ParseClock(text); ok {
			t.Errorf("ParseClock(%q) accepted", text)
		}
	}
	if ClockDiff(86399, 0) != 1 || ClockDiff(0, 86399) != -1 {
		t.Fatal("midnight wrap")
	}
}

func TestEvaluateEdge(t *testing.T) {
	cases := []struct {
		e    Edge
		raw  [2]string
		want ReadStatus
	}{
		{Head, [2]string{"10:00:00", "10:00:01"}, ReadOK},
		{Head, [2]string{"10:00:00", "10:00:02"}, ReadOK},
		{Head, [2]string{"10:00:00", "10:00:08"}, ReadInconsistent}, // a misread digit
		{Head, [2]string{"10:00:01", "10:00:00"}, ReadInconsistent}, // runs backwards
		{Tail, [2]string{"10:00:19", "10:00:18"}, ReadOK},
		{Tail, [2]string{"10:00:18", "10:00:19"}, ReadInconsistent},
		{Tail, [2]string{"", "10:00:18"}, ReadUnreadable},
	}
	for _, c := range cases {
		if got := EvaluateEdge(c.e, c.raw); got.Status != c.want {
			t.Errorf("%s %v: %s, want %s", c.e, c.raw, got.Status, c.want)
		}
	}
	if r := EvaluateEdge(Tail, [2]string{"10:00:19", "10:00:18"}); r.Clock != 36019 {
		t.Fatalf("tail clock %d", r.Clock)
	}
}

func ok(clock int) EdgeRead { return EdgeRead{Clock: clock, Status: ReadOK} }

func TestCheckPartAndDecideClock(t *testing.T) {
	// 20 s at 10 fps: 200 frames over a 19 s head-to-tail clock span.
	a := CheckPart(1, 1, 200, 0.1, ok(36000), ok(36019))
	if a.Reason != "" || a.SpanSeconds != 19 || a.RelativeFPSError > 0.06 {
		t.Fatalf("consistent part rejected: %+v", a)
	}
	if fast := CheckPart(2, 2, 200, 0.1, ok(36020), ok(36059)); fast.Reason != FrameCountMismatch {
		t.Fatalf("clock running 2x accepted: %+v", fast)
	}
	if short := CheckPart(2, 2, 5, 0.1, ok(36020), ok(36020)); short.Reason != ClockSpanTooShort {
		t.Fatalf("zero span accepted: %+v", short)
	}
	next := func(head int) PartCheck { return CheckPart(2, 2, 200, 0.1, ok(head), ok(head+19)) }
	for head, want := range map[int]Reason{36019: Joined, 36020: Joined, 36021: ClockGap, 36026: ClockGap, 36018: ClockOverlap} {
		v := DecideClock(SeamVerdict{}, a, next(head))
		if v.Reason != want || v.Join != (want == Joined) {
			t.Errorf("next head %d: %s join=%v, want %s", head, v.Reason, v.Join, want)
		}
	}
	bad := next(36020)
	bad.Reason = FrameCountMismatch
	if v := DecideClock(SeamVerdict{}, a, bad); v.Join || v.Reason != FrameCountMismatch {
		t.Fatalf("frame-count mismatch joined: %+v", v)
	}
	unread := next(36020)
	unread.Head.Status = ReadUnreadable
	if v := DecideClock(SeamVerdict{}, a, unread); v.Join || v.Reason != ClockUnreadable {
		t.Fatalf("unreadable seam joined: %+v", v)
	}
}

func TestAssemblyPath(t *testing.T) {
	stem := "435_Kromeriz/September/Saturday/435_Kromeriz_-_Hanacke_namesti_2026_September_W3_Saturday_hour_03"
	parts := []string{stem + "_part_02.mp4", stem + "_part_03.mp4"}
	if got, _ := AssemblyPath(parts, false); got != stem+"_part_02.mp4" {
		t.Fatalf("run name %s", got)
	}
	if got, _ := AssemblyPath(parts, true); got != stem+".mp4" {
		t.Fatalf("whole-hour name %s", got)
	}
	if _, err := AssemblyPath([]string{stem + "_part_02.mp4", strings.Replace(stem, "hour_03", "hour_04", 1) + "_part_01.mp4"}, false); err == nil {
		t.Fatal("parts of two hours named as one")
	}
	for _, p := range []string{"x.mp4", stem + "_part_02_reviewed_union.mp4"} {
		if _, err := AssemblyPath([]string{p, p}, false); err == nil {
			t.Fatalf("%s accepted as a part name", p)
		}
	}
}

func TestProfileValidate(t *testing.T) {
	if err := profile().Validate(); err != nil {
		t.Fatal(err)
	}
	for _, p := range []Profile{{RecordingID: 1, X: 0.5, W: 0.6, H: 0.1, Channel: Luma}, {RecordingID: 1, W: 0.5, H: 0.1}, {RecordingID: 1, W: 0.5, H: 0.1, Channel: "orange"}} {
		if p.Validate() == nil {
			t.Errorf("%+v accepted", p)
		}
	}
	if f := (Profile{RecordingID: 1, W: 0.5, H: 0.1, Channel: Blue}).filter(); !strings.HasSuffix(f, "format=gbrp,extractplanes=b") {
		t.Fatalf("blue filter %s", f)
	}
}

func TestPlaceCreateOnly(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	sha := func(body string) string { s := sha256.Sum256([]byte(body)); return hex.EncodeToString(s[:]) }
	dst := filepath.Join(dir, "out", "k.mp4")
	if err := placeCreateOnly(write("a", "one"), dst, sha("one")); err != nil {
		t.Fatal(err)
	}
	if err := placeCreateOnly(write("b", "one"), dst, sha("one")); err != nil {
		t.Fatalf("identical rerun refused: %v", err)
	}
	if err := placeCreateOnly(write("c", "two"), dst, sha("two")); err == nil {
		t.Fatal("overwrote an existing key")
	}
	if body, _ := os.ReadFile(dst); string(body) != "one" {
		t.Fatalf("existing bytes changed: %q", body)
	}
}

// --- hours built from manifests (synthetic) ---

const (
	testBatch    = "recovery-test"
	testRec      = 435
	testDate     = "2026-09-19"
	testHour     = 3
	testFPS      = 10
	clockBatch   = "clock-prestitch-20261126"
	partSeconds  = 20.0
	deliveryStem = "435_Kromeriz/September/Saturday/435_Kromeriz_-_Hanacke_namesti_2026_September_W3_Saturday_hour_03"
)

// spec is one synthetic part: its burned-in clock start and how fast it runs.
type spec struct {
	clock int     // seconds of day at the first frame
	rate  float64 // clock seconds per video second
	audio bool
}

type hourFixture struct {
	root, out string
	key       string
	env       Env
}

func profile() Profile {
	return Profile{RecordingID: testRec, X: 0.4, Y: 0, W: 0.6, H: 1, Channel: Luma}
}

// newHour writes parts and an hour manifest under a DirSource root. seamReason
// is every split's collation-v2 reason.
func newHour(t *testing.T, specs []spec, seamReason string) hourFixture {
	t.Helper()
	tools := requireTools(t)
	root, out := t.TempDir(), t.TempDir()
	hourID, err := collation.HourIDFor(testBatch, testRec, testDate, testHour)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 9, 19, 8, 0, 0, 0, time.UTC)
	m := collation.HourManifest{SchemaVersion: 1, PolicyVersion: collation.PolicyVersion, Generation: collation.Generation,
		Status: collation.StatusCollated, BatchID: testBatch, HourID: hourID, RecordingID: testRec, Timezone: "Europe/Prague",
		LocalDate: testDate, DeliveryHour: testHour, ScheduledStart: start, ScheduledEnd: start.Add(time.Hour)}
	delivered, paths := map[string]Delivered{}, map[string]bool{}
	for i, s := range specs {
		local := filepath.Join(t.TempDir(), fmt.Sprintf("p%d.mp4", i))
		renderClock(t, tools, local, s)
		size, sha, err := identity(local)
		if err != nil {
			t.Fatal(err)
		}
		probe, err := collation.ProbeFile(context.Background(), tools, local)
		if err != nil || !probe.Media.Playable {
			t.Fatalf("probe: %v %+v", err, probe.Media)
		}
		key := collation.ObjectKey(testBatch, sha)
		dst := filepath.Join(root, filepath.FromSlash(key))
		if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(local, dst); err != nil {
			t.Fatal(err)
		}
		clipID := int64(500 + i)
		cs := start.Add(time.Duration(i) * time.Duration(partSeconds*float64(time.Second)))
		clip := collation.Clip{ClipID: clipID, RecordingID: testRec, JobID: 7, StartUTC: cs, EndUTC: cs.Add(time.Duration(partSeconds * float64(time.Second)))}
		m.Clips = append(m.Clips, collation.ClipDisposition{Clip: clip, Disposition: "included", Part: i + 1, Media: probe.Media})
		m.Outputs = append(m.Outputs, collation.Output{Part: i + 1, Parts: len(specs), ObjectKey: key, SizeBytes: size, SHA256: sha,
			StartUTC: clip.StartUTC, EndUTC: clip.EndUTC, SourceClipIDs: []int64{clipID},
			Verification: collation.Verification{VideoPackets: probe.Media.VideoPackets, PayloadChainMatches: true, VideoTimingMatches: true, EdgeDecodeOK: true}})
		if i > 0 {
			m.Seams = append(m.Seams, collation.SeamDecision{PrevClipID: clipID - 1, NextClipID: clipID, Decision: collation.DecisionSplit, Reason: seamReason})
		}
		p := fmt.Sprintf("%s_part_%02d.mp4", deliveryStem, i+1)
		delivered[key] = Delivered{ID: int64(9000 + i), Path: p, SHA256: sha, SizeBytes: size, RecordingID: testRec}
		paths[p] = true
	}
	if err := m.Validate(); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(m)
	mk := collation.ManifestKey(testBatch, hourID)
	if err := os.MkdirAll(filepath.Dir(filepath.Join(root, mk)), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, mk), body, 0o600); err != nil {
		t.Fatal(err)
	}
	tess, _ := exec.LookPath("tesseract")
	env := Env{Tools: tools, Reader: Reader{Tools: tools, Tesseract: tess}, Source: DirSource{Root: root}, Out: out, Batch: clockBatch,
		Profiles: map[int64]Profile{testRec: profile()}, Delivered: delivered, Assembled: map[int64]bool{}, Paths: paths,
		VetoClips: map[int64]bool{}, VetoRecordings: map[int64]bool{}, MediaTool: "test-ffmpeg", ClockTool: "test-tesseract"}
	return hourFixture{root: root, out: out, key: mk, env: env}
}

func requireTools(t *testing.T) collation.Tools {
	t.Helper()
	tools := collation.ToolsFromEnv()
	for _, bin := range []string{tools.FFmpeg, tools.FFprobe, "tesseract"} {
		if _, err := exec.LookPath(bin); err != nil {
			t.Skipf("%s not available", bin)
		}
	}
	return tools
}

// renderClock encodes partSeconds of video at testFPS with a burned-in
// HH:MM:SS clock (and a tone when s.audio), like a camera overlay.
func renderClock(t *testing.T, tools collation.Tools, dst string, s spec) {
	t.Helper()
	const w, h = 320, 64
	ttf, err := opentype.Parse(gomonobold.TTF)
	if err != nil {
		t.Fatal(err)
	}
	face, err := opentype.NewFace(ttf, &opentype.FaceOptions{Size: 28, DPI: 72, Hinting: font.HintingFull})
	if err != nil {
		t.Fatal(err)
	}
	var frames bytes.Buffer
	n := int(partSeconds * testFPS)
	for i := 0; i < n; i++ {
		clock := s.clock + int(float64(i)/testFPS*s.rate)
		img := image.NewGray(image.Rect(0, 0, w, h))
		for p := range img.Pix {
			img.Pix[p] = 0xff
		}
		d := font.Drawer{Dst: img, Src: image.NewUniform(color.Gray{0}), Face: face, Dot: fixed.P(150, 42)}
		d.DrawString(fmt.Sprintf("%02d:%02d:%02d", clock/3600%24, clock/60%60, clock%60))
		frames.Write(img.Pix)
	}
	args := []string{"-nostdin", "-v", "error", "-f", "rawvideo", "-pix_fmt", "gray", "-s", fmt.Sprintf("%dx%d", w, h), "-r", fmt.Sprint(testFPS), "-i", "-"}
	if s.audio {
		args = append(args, "-f", "lavfi", "-i", fmt.Sprintf("sine=frequency=440:sample_rate=48000:duration=%g", partSeconds), "-c:a", "aac")
	}
	args = append(args, "-c:v", "libx264", "-g", fmt.Sprint(testFPS), "-bf", "0", "-pix_fmt", "yuv420p", "-t", fmt.Sprint(partSeconds), dst)
	cmd := exec.Command(tools.FFmpeg, args...)
	cmd.Stdin = &frames
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("cannot synthesize clock media: %v %s", err, out)
	}
}

func run(t *testing.T, f hourFixture) HourResult {
	t.Helper()
	res, err := ProcessHour(context.Background(), f.env, f.key)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func reasons(r HourResult) []Reason {
	var out []Reason
	for _, s := range r.Seams {
		out = append(out, s.Reason)
	}
	return out
}

func TestReaderReadsSyntheticClock(t *testing.T) {
	f := newHour(t, []spec{{clock: 36000, rate: 1}}, "")
	local := filepath.Join(f.root, filepath.FromSlash(mustManifest(t, f).Outputs[0].ObjectKey))
	f.env.Reader.Scratch = t.TempDir()
	head, err := f.env.Reader.ReadEdge(context.Background(), local, profile(), Head)
	if err != nil || head.Status != ReadOK || head.Clock != 36000 {
		t.Fatalf("head %+v %v", head, err)
	}
	tail, err := f.env.Reader.ReadEdge(context.Background(), local, profile(), Tail)
	if err != nil || tail.Status != ReadOK || tail.Clock != 36019 {
		t.Fatalf("tail %+v %v", tail, err)
	}
}

// Consecutive clocks join with audio kept; a non-consecutive clock stays split.
func TestConsecutiveJoinNonConsecutiveSplitAudioKept(t *testing.T) {
	f := newHour(t, []spec{{36000, 1, true}, {36020, 1, true}, {36046, 1, true}}, "frame_jump")
	res := run(t, f)
	if got := reasons(res); len(got) != 2 || got[0] != Joined || got[1] != ClockGap {
		t.Fatalf("seams %v", got)
	}
	if res.Status != HourJoined || len(res.Assemblies) != 1 {
		t.Fatalf("status %s assemblies %d", res.Status, len(res.Assemblies))
	}
	a := res.Assemblies[0]
	if a.Path != deliveryStem+"_part_01.mp4" || a.InputParts != 2 || a.PartsReduced != 1 || a.InputIDs[0] != 9000 || a.InputIDs[1] != 9001 {
		t.Fatalf("assembly %+v", a)
	}
	if a.ProductionVersion != "clock-prestitched" || len(a.ClockBoundaryAnnotations) != 1 || a.ClockBoundaryAnnotations[0].SourceInputIDs != [2]int64{9000, 9001} ||
		a.ClockBoundaryAnnotations[0].MaxRelativeError > MaxRelativeFPSError || a.ClockBoundaryAnnotations[0].ArtifactSHA256 != a.SHA256 {
		t.Fatalf("annotation %+v", a.ClockBoundaryAnnotations)
	}
	if !a.OriginalSourceAudioPresent || a.AudioDeliveryStatus != AudioCopied || a.AudioPackets == 0 {
		t.Fatalf("audio %+v", a)
	}
	// The assembly on disk is exactly both parts' video and audio packets.
	local := filepath.Join(f.out, filepath.FromSlash(a.Key))
	size, sha, err := identity(local)
	if err != nil || size != a.SizeBytes || sha != a.SHA256 || a.Key != AssemblyKey(clockBatch, sha) {
		t.Fatalf("assembly bytes: %v", err)
	}
	probe, err := collation.ProbeFile(context.Background(), f.env.Tools, local)
	if err != nil || probe.Audio == nil || len(probe.Audio.Packets) != a.AudioPackets {
		t.Fatalf("assembly audio missing: %v", err)
	}
	var parts int
	for _, o := range []int{0, 1} {
		parts += mustManifest(t, f).Outputs[o].Verification.VideoPackets
	}
	if len(probe.Video.Packets) != parts {
		t.Fatalf("video packets %d, parts %d", len(probe.Video.Packets), parts)
	}
	proof, err := os.ReadFile(filepath.Join(f.out, filepath.FromSlash(a.ProofKey)))
	if s := sha256.Sum256(proof); err != nil || hex.EncodeToString(s[:]) != a.ProofSHA256 {
		t.Fatalf("proof identity: %v", err)
	}
	for _, name := range []string{"test-ffmpeg", "test-tesseract", PassVersion} {
		if !bytes.Contains(proof, []byte(name)) {
			t.Fatalf("proof lacks tool metadata %q", name)
		}
	}
	if strings.Contains(a.Path, "clock") || strings.Contains(a.Path, PassVersion) {
		t.Fatalf("delivery name names the tool: %s", a.Path)
	}
}

func mustManifest(t *testing.T, f hourFixture) collation.HourManifest {
	body, err := DirSource{Root: f.root}.Read(context.Background(), f.key)
	if err != nil {
		t.Fatal(err)
	}
	var m collation.HourManifest
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestFrameCountMismatchStaysSplit(t *testing.T) {
	// The second part's clock runs 2x: 200 frames over a 39 s clock span.
	res := run(t, newHour(t, []spec{{36000, 1, false}, {36020, 2, false}}, "frame_low_motion"))
	if got := reasons(res); len(got) != 1 || got[0] != FrameCountMismatch || len(res.Assemblies) != 0 || res.Status != HourNoJoin {
		t.Fatalf("seams %v assemblies %d", got, len(res.Assemblies))
	}
	if n := res.Seams[0].Next; n == nil || n.RelativeFPSError < 0.4 {
		t.Fatalf("next part check %+v", n)
	}
}

func TestWholeHourTakesTheHourNameAndRerunIsCreateOnly(t *testing.T) {
	f := newHour(t, []spec{{36000, 1, false}, {36020, 1, false}}, "frame_insufficient_frames")
	res := run(t, f)
	if len(res.Assemblies) != 1 || res.Assemblies[0].Path != deliveryStem+".mp4" || res.Assemblies[0].AudioDeliveryStatus != AudioNoSource {
		t.Fatalf("assemblies %+v", res.Assemblies)
	}
	if err := WriteResult(f.out, res); err != nil {
		t.Fatal(err)
	}
	again := run(t, f) // same bytes at the same keys: accepted, nothing rewritten
	if again.Assemblies[0].SHA256 != res.Assemblies[0].SHA256 {
		t.Fatal("rebuild differs")
	}
	if err := WriteResult(f.out, again); err == nil {
		t.Fatal("hour result overwritten")
	}
}

func TestVetoesNeedNoMedia(t *testing.T) {
	base := newHour(t, []spec{{36000, 1, false}, {36020, 1, false}}, "frame_jump")
	cases := map[string]struct {
		edit func(*hourFixture)
		want Reason
	}{
		"metadata split": {func(f *hourFixture) { rewriteSeam(t, f, "different_job", 0) }, SeamNotEligible},
		"replay split":   {func(f *hourFixture) { rewriteSeam(t, f, "packet_replay", 0) }, SeamNotEligible},
		"large db gap":   {func(f *hourFixture) { rewriteSeam(t, f, "db_gap", 30) }, SeamNotEligible},
		"known bad":      {func(f *hourFixture) { f.env.VetoClips = map[int64]bool{501: true} }, KnownBadSource},
		"assembled":      {func(f *hourFixture) { f.env.Assembled = map[int64]bool{9000: true} }, AlreadyAssembled},
		"not delivered":  {func(f *hourFixture) { f.env.Delivered = map[string]Delivered{} }, PartNotDelivered},
	}
	for name, c := range cases {
		f := base
		f.env.Delivered = base.env.Delivered
		c.edit(&f)
		res := run(t, f)
		if got := reasons(res); len(got) != 1 || got[0] != c.want || res.Seams[0].Prev != nil {
			t.Errorf("%s: %v", name, got)
		}
		rewriteSeam(t, &f, "frame_jump", 0)
	}
	f := base
	f.env.VetoRecordings = map[int64]bool{testRec: true}
	if res := run(t, f); res.Status != HourVetoed || len(res.Seams) != 0 {
		t.Fatalf("vetoed recording: %s", res.Status)
	}
	f = base
	f.env.Profiles = map[int64]Profile{}
	if res := run(t, f); res.Status != HourNoProfile {
		t.Fatalf("no profile: %s", res.Status)
	}
}

func rewriteSeam(t *testing.T, f *hourFixture, reason string, gap float64) {
	m := mustManifest(t, *f)
	m.Seams[0].Reason, m.Seams[0].DBGapSeconds = reason, gap
	body, _ := json.Marshal(m)
	if err := os.WriteFile(filepath.Join(f.root, filepath.FromSlash(f.key)), body, 0o600); err != nil {
		t.Fatal(err)
	}
}
