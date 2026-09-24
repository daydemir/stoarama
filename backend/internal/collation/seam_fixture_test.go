package collation

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// seamCase is one seam from the 2026-09-24 seam proof (real hours C1-C4 and
// adversarial constructions A1-A6), with the proof's ground truth.
type seamCase struct {
	Case            string   `json:"case"`
	Seam            int      `json:"seam"`
	Prev            caseClip `json:"prev"`
	Next            caseClip `json:"next"`
	Expected        string   `json:"expected"`
	TruthContinuous *bool    `json:"truth_continuous"`
	TruthSource     string   `json:"truth_source"`
}

type caseClip struct {
	CID       string  `json:"cid"`
	Start     string  `json:"start"`
	End       string  `json:"end"`
	Job       string  `json:"job"`
	Seq       string  `json:"seq"`
	Rec       string  `json:"rec"`
	ContentS  float64 `json:"content_s"`
	SpanS     float64 `json:"span_s"`
	CodecSig  string  `json:"codec_sig"`
	HasAudio  bool    `json:"has_audio"`
	NumFrames int     `json:"n_frames"`
}

func (c caseClip) clip(t *testing.T) (Clip, ClipMedia) {
	parse := func(raw string) time.Time {
		v, err := time.Parse("2006-01-02 15:04:05.999999-07:00", raw)
		if err != nil {
			t.Fatalf("parse %q: %v", raw, err)
		}
		return v.UTC()
	}
	id, _ := strconv.ParseInt(c.CID, 10, 64)
	rec, _ := strconv.ParseInt(c.Rec, 10, 64)
	job, _ := strconv.ParseInt(c.Job, 10, 64)
	seq, _ := strconv.ParseInt(c.Seq, 10, 64)
	clip := Clip{ClipID: id, RecordingID: rec, JobID: job, CaptureSequence: seq, StartUTC: parse(c.Start), EndUTC: parse(c.End), NominalSeconds: 60}
	frame := c.ContentS / float64(max(c.NumFrames, 1))
	return clip, ClipMedia{Playable: true, CodecSignature: c.CodecSig, HasAudio: c.HasAudio, VideoPackets: c.NumFrames,
		ContentSeconds: c.ContentS, FrameSeconds: frame}
}

func (c seamCase) key() string {
	return fmt.Sprintf("%s/%d/%s-%s", c.Case, c.Seam, c.Prev.CID, c.Next.CID)
}

func loadSeamCases(t *testing.T) []seamCase {
	body, err := os.ReadFile("testdata/seam_cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []seamCase
	if err := json.Unmarshal(body, &cases); err != nil {
		t.Fatal(err)
	}
	return cases
}

// windowCurves holds a seam's curves at the short and the full window.
type windowCurves struct {
	Short Curves `json:"short"`
	Full  Curves `json:"full"`
}

func loadSeamCurves(t *testing.T) map[string]windowCurves {
	f, err := os.Open("testdata/seam_curves.json.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]windowCurves{}
	if err := json.NewDecoder(zr).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

// evaluateEscalating mirrors matchSeam: short window first, full on failure.
func evaluateEscalating(policy SeamPolicy, c windowCurves, frame float64) MatchEvidence {
	short := policy
	short.WindowSeconds = policy.ShortWindowSeconds
	ev := EvaluateCurves(short, c.Short, frame)
	ev.WindowSeconds = policy.ShortWindowSeconds
	if !needsFullWindow(policy, ev) {
		return ev
	}
	ev = EvaluateCurves(policy, c.Full, frame)
	ev.WindowSeconds = policy.WindowSeconds
	return ev
}

// TestSeamProofCases replays every proof seam through the full join rule.
// Hard requirement: no seam whose ground truth is "not continuous" may join.
func TestSeamProofCases(t *testing.T) {
	policy := DefaultSeamPolicy()
	curves := loadSeamCurves(t)
	joins := map[string]int{}
	trueSeams := map[string]int{}
	for _, c := range loadSeamCases(t) {
		cv, ok := curves[c.key()]
		if !ok {
			t.Fatalf("missing curves for %s", c.key())
		}
		prev, pm := c.Prev.clip(t)
		next, nm := c.Next.clip(t)
		ev := evaluateEscalating(policy, cv, pm.FrameSeconds)
		d := DecideSeam(policy, prev, next, pm, nm, &ev)
		truthFalse := c.TruthContinuous != nil && !*c.TruthContinuous
		if truthFalse && d.Decision == DecisionJoin {
			t.Errorf("WRONG JOIN %s (%s): %+v", c.key(), c.TruthSource, ev)
		}
		// Adversarial seams must also fail on pixels alone, independent of the
		// metadata gates (the attack can forge stamps and sequence numbers).
		if strings.HasPrefix(c.Case, "A") && truthFalse && (EvaluateCurves(policy, cv.Full, pm.FrameSeconds).Verdict == MatchContinuous || ev.Verdict == MatchContinuous) {
			t.Errorf("adversarial seam %s passes the frame match alone: %+v", c.key(), ev)
		}
		if c.TruthContinuous != nil && *c.TruthContinuous {
			trueSeams[c.Case]++
			if d.Decision == DecisionJoin {
				joins[c.Case]++
			}
		}
		truth := "unknown"
		if c.TruthContinuous != nil {
			truth = strconv.FormatBool(*c.TruthContinuous)
		}
		t.Logf("%-18s truth=%-7s decision=%-5s reason=%-28s tail=%d/%.2f/%.2f head=%d/%.2f/%.2f step=%.2f/%.2f",
			c.key(), truth, d.Decision, d.Reason, ev.TailMinOffset, ev.TailMinMAD, ev.TailMedianMAD,
			ev.HeadMinOffset, ev.HeadMinMAD, ev.HeadMedianMAD, ev.BoundaryMAD, ev.StepP95MAD)
	}
	// The continuous control hour (one capture, 60 clips) must mostly join;
	// extra splits are allowed but a rule that splits everything is useless.
	if trueSeams["C1"] == 0 || float64(joins["C1"]) < 0.8*float64(trueSeams["C1"]) {
		t.Errorf("C1 continuous hour joined %d of %d true seams", joins["C1"], trueSeams["C1"])
	}
	t.Logf("true-continuous seams joined: %v of %v", joins, trueSeams)
}

// TestGenerateSeamCurves regenerates testdata/seam_curves.json.gz from the
// proof clips (COLLATION_FIXTURE_CLIPS=<dir of <clip_id>.mp4>). Not run in CI.
func TestGenerateSeamCurves(t *testing.T) {
	dir := os.Getenv("COLLATION_FIXTURE_CLIPS")
	if dir == "" {
		t.Skip("COLLATION_FIXTURE_CLIPS not set")
	}
	policy := DefaultSeamPolicy()
	tools := ToolsFromEnv()
	ctx := context.Background()
	cases := loadSeamCases(t)
	results := make([]windowCurves, len(cases))
	errs := make([]error, len(cases))
	curvesAt := func(c seamCase, window float64) (Curves, error) {
		p := policy
		p.WindowSeconds = window
		tail, err := ExtractWindow(ctx, tools, p, filepath.Join(dir, c.Prev.CID+".mp4"), true)
		var head []Frame
		if err == nil {
			head, err = ExtractWindow(ctx, tools, p, filepath.Join(dir, c.Next.CID+".mp4"), false)
		}
		if err != nil {
			// A seam region that does not decode is itself a proof case.
			return Curves{DecodeError: trimStderr(err.Error())}, nil
		}
		pa, err := ProbeFile(ctx, tools, filepath.Join(dir, c.Prev.CID+".mp4"))
		if err != nil || !pa.Media.Playable {
			return Curves{}, fmt.Errorf("%s: probe prev: %v %+v", c.key(), err, pa.Media)
		}
		pb, err := ProbeFile(ctx, tools, filepath.Join(dir, c.Next.CID+".mp4"))
		if err != nil || !pb.Media.Playable {
			return Curves{}, fmt.Errorf("%s: probe next: %v %+v", c.key(), err, pb.Media)
		}
		return roundCurves(framesToCurves(p, tail, head, pa.Video.WindowKeys(len(tail), true), pb.Video.WindowKeys(len(head), false))), nil
	}
	var wg sync.WaitGroup
	sem := make(chan struct{}, 12)
	for i, c := range cases {
		wg.Add(1)
		go func(i int, c seamCase) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			short, err := curvesAt(c, policy.ShortWindowSeconds)
			if err != nil {
				errs[i] = err
				return
			}
			full, err := curvesAt(c, policy.WindowSeconds)
			errs[i] = err
			results[i] = windowCurves{Short: short, Full: full}
		}(i, c)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	out := map[string]windowCurves{}
	for i, c := range cases {
		out[c.key()] = results[i]
	}
	f, err := os.Create("testdata/seam_curves.json.gz")
	if err != nil {
		t.Fatal(err)
	}
	zw := gzip.NewWriter(f)
	if err := json.NewEncoder(zw).Encode(out); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func roundCurves(c Curves) Curves {
	r := func(v []float64) []float64 {
		out := make([]float64, len(v))
		for i := range v {
			out[i] = round4(v[i])
		}
		return out
	}
	return Curves{TailToHead0: r(c.TailToHead0), TailLastToHead: r(c.TailLastToHead), Steps: r(c.Steps), KeySteps: r(c.KeySteps), BoundaryMAD: round4(c.BoundaryMAD),
		TailKeys: c.TailKeys, HeadKeys: c.HeadKeys, DecodeError: c.DecodeError}
}

// replaySeam is a seam from the 2026-09-24 old-vs-new comparison where B's
// first packets are byte-identical repeats of A's tail (zero DB gap), or a
// continuous control seam that must not be flagged.
type replaySeam struct {
	Name   string        `json:"name"`
	Replay bool          `json:"replay"`
	A      []fixturePack `json:"a"`
	B      []fixturePack `json:"b"`
}

type fixturePack struct {
	PTS  string `json:"pts"`
	Dur  string `json:"dur"`
	Key  bool   `json:"key,omitempty"`
	Hash string `json:"hash"`
}

var replayFixtureSeams = []struct {
	name   string
	a, b   string
	replay bool
}{
	{"rec406/483362-483405", "483362", "483405", true}, {"rec406/483449-483488", "483449", "483488", true},
	{"rec409/528721-528761", "528721", "528761", true}, {"rec401/572194-572231", "572194", "572231", true},
	{"rec401/572531-572570", "572531", "572570", true}, {"rec406/488141-488145", "488141", "488145", true},
	{"rec406/488153-488157", "488153", "488157", true},
	{"C1/1", "1594350", "1594369", false}, {"C1/2", "1594369", "1594388", false}, {"C1/3", "1594388", "1594405", false},
	{"C1/4", "1594405", "1594422", false}, {"C3/2", "1590458", "1590471", false}, {"C3/10", "1590599", "1590610", false},
}

func toFixture(sp *StreamPackets) []fixturePack {
	out := make([]fixturePack, len(sp.Packets))
	for i, pk := range sp.Packets {
		out[i] = fixturePack{PTS: pk.PTS.RatString(), Dur: pk.Dur.RatString(), Key: pk.Key, Hash: pk.Hash[len(pk.Hash)-16:]}
	}
	return out
}

func fromFixture(t *testing.T, packs []fixturePack) Probe {
	sp := &StreamPackets{Kind: "video"}
	for _, p := range packs {
		pts, ok1 := new(big.Rat).SetString(p.PTS)
		dur, ok2 := new(big.Rat).SetString(p.Dur)
		if !ok1 || !ok2 {
			t.Fatalf("bad fixture packet %+v", p)
		}
		sp.Packets = append(sp.Packets, Packet{PTS: pts, DTS: pts, Dur: dur, Key: p.Key, Hash: p.Hash})
	}
	return Probe{Video: sp}
}

// TestReplaySeamsNeverJoin: every recorded replay seam is detected from packet
// hashes alone, and continuous controls are not.
func TestReplaySeamsNeverJoin(t *testing.T) {
	f, err := os.Open("testdata/replay_seams.json.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	var seams []replaySeam
	if err := json.NewDecoder(zr).Decode(&seams); err != nil {
		t.Fatal(err)
	}
	if len(seams) != len(replayFixtureSeams) {
		t.Fatalf("fixture has %d seams", len(seams))
	}
	for _, s := range seams {
		sec, found := ReplayOverlap(fromFixture(t, s.A), fromFixture(t, s.B))
		if found != s.Replay {
			t.Errorf("%s: replay=%v want %v", s.Name, found, s.Replay)
		}
		if s.Replay && (sec <= 0 || sec > 10) {
			t.Errorf("%s: overlap %.2f s is not a plausible replay", s.Name, sec)
		}
		t.Logf("%s replay=%v overlap=%.2fs", s.Name, found, sec)
	}
}

// TestGenerateReplaySeams regenerates testdata/replay_seams.json.gz from clips
// in COLLATION_FIXTURE_CLIPS. Not run in CI.
func TestGenerateReplaySeams(t *testing.T) {
	dir := os.Getenv("COLLATION_FIXTURE_CLIPS")
	if dir == "" {
		t.Skip("COLLATION_FIXTURE_CLIPS not set")
	}
	tools := ToolsFromEnv()
	var out []replaySeam
	for _, s := range replayFixtureSeams {
		pa, err := ProbeFile(context.Background(), tools, filepath.Join(dir, s.a+".mp4"))
		if err != nil || !pa.Media.Playable {
			t.Fatalf("%s: %v", s.name, err)
		}
		pb, err := ProbeFile(context.Background(), tools, filepath.Join(dir, s.b+".mp4"))
		if err != nil || !pb.Media.Playable {
			t.Fatalf("%s: %v", s.name, err)
		}
		out = append(out, replaySeam{Name: s.name, Replay: s.replay, A: toFixture(pa.Video), B: toFixture(pb.Video)})
	}
	fh, err := os.Create("testdata/replay_seams.json.gz")
	if err != nil {
		t.Fatal(err)
	}
	zw := gzip.NewWriter(fh)
	if err := json.NewEncoder(zw).Encode(out); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fh.Close(); err != nil {
		t.Fatal(err)
	}
}
