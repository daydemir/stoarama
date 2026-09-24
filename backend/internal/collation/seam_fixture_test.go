package collation

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
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

func loadSeamCurves(t *testing.T) map[string]Curves {
	f, err := os.Open("testdata/seam_curves.json.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]Curves{}
	if err := json.NewDecoder(zr).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
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
		ev := EvaluateCurves(policy, cv, pm.FrameSeconds)
		d := DecideSeam(policy, prev, next, pm, nm, &ev)
		truthFalse := c.TruthContinuous != nil && !*c.TruthContinuous
		if truthFalse && d.Decision == DecisionJoin {
			t.Errorf("WRONG JOIN %s (%s): %+v", c.key(), c.TruthSource, ev)
		}
		// Adversarial seams must also fail on pixels alone, independent of the
		// metadata gates (the attack can forge stamps and sequence numbers).
		if strings.HasPrefix(c.Case, "A") && truthFalse && ev.Verdict == MatchContinuous {
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
	out := map[string]Curves{}
	cases := loadSeamCases(t)
	results := make([]Curves, len(cases))
	errs := make([]error, len(cases))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 12)
	for i, c := range cases {
		wg.Add(1)
		go func(i int, c seamCase) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			tail, err := ExtractWindow(ctx, tools, policy, filepath.Join(dir, c.Prev.CID+".mp4"), true)
			var head []Frame
			if err == nil {
				head, err = ExtractWindow(ctx, tools, policy, filepath.Join(dir, c.Next.CID+".mp4"), false)
			}
			if err != nil {
				// A seam region that does not decode is itself a proof case.
				results[i] = Curves{DecodeError: trimStderr(err.Error())}
				return
			}
			pa, err := ProbeFile(ctx, tools, filepath.Join(dir, c.Prev.CID+".mp4"))
			if err != nil || !pa.Media.Playable {
				errs[i] = fmt.Errorf("%s: probe prev: %v %+v", c.key(), err, pa.Media)
				return
			}
			pb, err := ProbeFile(ctx, tools, filepath.Join(dir, c.Next.CID+".mp4"))
			if err != nil || !pb.Media.Playable {
				errs[i] = fmt.Errorf("%s: probe next: %v %+v", c.key(), err, pb.Media)
				return
			}
			results[i] = roundCurves(framesToCurves(policy, tail, head, pa.Video.WindowKeys(len(tail), true), pb.Video.WindowKeys(len(head), false)))
		}(i, c)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
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
