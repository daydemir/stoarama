package collation

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// canarySeam is one of the 40 seams Deniz flight-reviewed on 2026-09-30
// (collation-v2 canary), frozen from its generation-2 manifest, with the
// media facts v3 needs: the HRD view of both clip edges and the GOP lengths
// around the seam. Vote is about v2's claim: correct | wrong | unsure.
type canarySeam struct {
	ID        string       `json:"id"`
	Vote      string       `json:"vote"`
	Prev      Clip         `json:"prev"`
	Next      Clip         `json:"next"`
	PrevMedia ClipMedia    `json:"prev_media"`
	NextMedia ClipMedia    `json:"next_media"`
	V2        SeamDecision `json:"v2"`
	// HRD edges (nil when the stream carries no NAL HRD).
	PrevHRD *HRDTrack `json:"prev_hrd,omitempty"`
	NextHRD *HRDTrack `json:"next_hrd,omitempty"`
	// GOP lengths (decode-order packets): previous clip's complete GOPs (last
	// 8), its final GOP, next clip's complete GOPs (first 8).
	PrevGOPs []int `json:"prev_gops"`
	PrevLast int   `json:"prev_last_gop"`
	NextGOPs []int `json:"next_gops"`
}

// hrdFixtures holds the canary seams and one real continuous HRD stream (rec
// 337, three consecutive clips) the adversarial generator splices.
type hrdFixtures struct {
	Canary []canarySeam `json:"canary"`
	// Stream is the concatenated HRD units of the continuous clips.
	Stream HRDTrack `json:"stream"`
}

func loadHRDFixtures(t *testing.T) hrdFixtures {
	t.Helper()
	f, err := os.Open("testdata/hrd_fixtures.json.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	var fx hrdFixtures
	if err := json.NewDecoder(zr).Decode(&fx); err != nil {
		t.Fatal(err)
	}
	return fx
}

// trimHRD keeps what EvaluateHRDSeam reads: from the 10th-last buffering
// period on (it calibrates on at most the last 9).
func trimHRD(t HRDTrack) HRDTrack {
	var idx []int
	for i, u := range t.Units {
		if u.BP {
			idx = append(idx, i)
		}
	}
	if len(idx) > 10 {
		t.Units = t.Units[idx[len(idx)-10]:]
	}
	return t
}

// TestGenerateHRDFixtures regenerates testdata/hrd_fixtures.json.gz from
// local copies of the canary clips. COLLATION_CANARY_IN is the frozen canary
// seam list (canarySeam without media facts, plus prev_path/next_path);
// COLLATION_HRD_STREAM is an OS path list of consecutive clip paths.
// Not run in CI.
func TestGenerateHRDFixtures(t *testing.T) {
	in, stream := os.Getenv("COLLATION_CANARY_IN"), os.Getenv("COLLATION_HRD_STREAM")
	if in == "" || stream == "" {
		t.Skip("COLLATION_CANARY_IN/COLLATION_HRD_STREAM not set")
	}
	ctx := context.Background()
	tools := ToolsFromEnv()
	body, err := os.ReadFile(in)
	if err != nil {
		t.Fatal(err)
	}
	var raw []struct {
		canarySeam
		PrevPath string `json:"prev_path"`
		NextPath string `json:"next_path"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatal(err)
	}
	var fx hrdFixtures
	for _, r := range raw {
		s := r.canarySeam
		pa, err := ProbeFile(ctx, tools, r.PrevPath)
		if err != nil || !pa.Media.Playable {
			t.Fatalf("%s probe prev: %v", s.ID, err)
		}
		pb, err := ProbeFile(ctx, tools, r.NextPath)
		if err != nil || !pb.Media.Playable {
			t.Fatalf("%s probe next: %v", s.ID, err)
		}
		pc, last := gopLengths(pa.Video)
		nc, _ := gopLengths(pb.Video)
		if len(pc) > 8 {
			pc = pc[len(pc)-8:]
		}
		if len(nc) > 8 {
			nc = nc[:8]
		}
		s.PrevGOPs, s.PrevLast, s.NextGOPs = pc, last, nc
		prev, okA, errA := ReadHRDTrack(r.PrevPath, true, DefaultHRDPolicy().TailUnits)
		next, okB, errB := ReadHRDTrack(r.NextPath, false, 1)
		if errA != nil || errB != nil {
			t.Fatalf("%s hrd: %v %v", s.ID, errA, errB)
		}
		if okA && okB {
			prev = trimHRD(prev)
			s.PrevHRD, s.NextHRD = &prev, &next
		}
		fx.Canary = append(fx.Canary, s)
	}
	for i, p := range filepath.SplitList(stream) {
		tr, ok, err := ReadHRDTrack(p, true, math.MaxInt32)
		if err != nil || !ok {
			t.Fatalf("stream %s: %v %v", p, ok, err)
		}
		if i == 0 {
			fx.Stream.Params = tr.Params
		} else if tr.Params != fx.Stream.Params {
			t.Fatalf("stream %s: hrd params differ", p)
		}
		fx.Stream.Units = append(fx.Stream.Units, tr.Units...)
	}
	f, err := os.Create("testdata/hrd_fixtures.json.gz")
	if err != nil {
		t.Fatal(err)
	}
	zw, _ := gzip.NewWriterLevel(f, gzip.BestCompression)
	if err := json.NewEncoder(zw).Encode(fx); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// decideCanaryV3 replays one frozen canary seam through the v3 rule with the
// recorded v2 frame match (packet replay is decided before the rule, as in
// DecideLocalSeam).
func decideCanaryV3(s canarySeam) SeamDecision {
	policy := SeamPolicyFor(PolicyV3)
	gop := EvaluateGOPLengths(s.PrevGOPs, s.PrevLast, s.NextGOPs, 8, 3)
	var src *SourceEvidence
	if s.PrevHRD != nil && s.NextHRD != nil {
		ev := EvaluateHRDSeam(*policy.HRD, *s.PrevHRD, *s.NextHRD)
		src = &ev
	} else {
		src = &SourceEvidence{Channel: "h264_hrd", Status: SourceUnavailable, Reason: "no_nal_hrd"}
	}
	if s.V2.Reason == "packet_replay" {
		d := s.V2
		d.Source, d.GOP = src, &gop
		return d
	}
	return DecideSeamV3(policy, s.Prev, s.Next, s.PrevMedia, s.NextMedia, s.V2.Match, src, &gop)
}

type confusion struct{ CorrectJoin, FalseSplit, FalseJoin, CorrectSplit, Unsure int }

// truthContinuous maps a vote on v2's claim to ground truth.
func truthContinuous(v2 SeamDecision, vote string) (continuous, known bool) {
	switch vote {
	case "correct":
		return v2.Decision == DecisionJoin, true
	case "wrong":
		return v2.Decision != DecisionJoin, true
	}
	return false, false
}

func score(decision string, continuous, known bool, c *confusion) {
	switch {
	case !known:
		c.Unsure++
	case continuous && decision == DecisionJoin:
		c.CorrectJoin++
	case continuous:
		c.FalseSplit++
	case decision == DecisionJoin:
		c.FalseJoin++
	default:
		c.CorrectSplit++
	}
}

// TestCanaryV3AgainstFlightReview is the labeled gate for collation-v3: the
// 40 Sep-30 canary seams, Deniz's votes as ground truth (unsure = unlabeled).
func TestCanaryV3AgainstFlightReview(t *testing.T) {
	fx := loadHRDFixtures(t)
	if len(fx.Canary) != 40 {
		t.Fatalf("canary seams: %d", len(fx.Canary))
	}
	policy := DefaultSeamPolicy()
	var before, after confusion
	for _, s := range fx.Canary {
		// Fixture integrity: the frozen inputs reproduce the recorded v2 decision.
		if s.V2.Reason != "packet_replay" {
			if got := DecideSeam(policy, s.Prev, s.Next, s.PrevMedia, s.NextMedia, s.V2.Match); got.Decision != s.V2.Decision || got.Reason != s.V2.Reason {
				t.Fatalf("%s: v2 replay %s/%s, recorded %s/%s", s.ID, got.Decision, got.Reason, s.V2.Decision, s.V2.Reason)
			}
		}
		cont, known := truthContinuous(s.V2, s.Vote)
		score(s.V2.Decision, cont, known, &before)
		d := decideCanaryV3(s)
		score(d.Decision, cont, known, &after)
		if known && !cont && d.Decision == DecisionJoin {
			t.Errorf("FALSE JOIN %s: %s", s.ID, d.Reason)
		}
		if d.Decision != s.V2.Decision {
			t.Logf("%s flips %s/%s -> %s/%s (vote on v2: %s) source=%+v gop=%+v", s.ID, s.V2.Decision, s.V2.Reason, d.Decision, d.Reason, s.Vote, d.Source, d.GOP)
		}
	}
	t.Logf("v2 %+v", before)
	t.Logf("v3 %+v", after)
	if before != (confusion{CorrectJoin: 20, FalseSplit: 8, FalseJoin: 0, CorrectSplit: 8, Unsure: 4}) {
		t.Fatalf("baseline differs from the Sep-30 flight review: %+v", before)
	}
	if after != (confusion{CorrectJoin: 21, FalseSplit: 7, FalseJoin: 0, CorrectSplit: 8, Unsure: 4}) {
		t.Fatalf("v3 canary confusion %+v", after)
	}
}

// bpIndices returns the buffering-period unit indices of a stream.
func bpIndices(units []HRDUnit) []int {
	var idx []int
	for i, u := range units {
		if u.BP {
			idx = append(idx, i)
		}
	}
	return idx
}

// TestHRDAdversarialSplices splices REAL continuous HRD footage the ways a
// capture failure can: forward skips and rewinds of 1..75 frames and of whole
// GOPs (a reconnect with the same framing 2-60 s later), at every GOP
// boundary. Metadata is irrelevant to this channel (restamped forgeries look
// identical). No splice may ever prove adjacency; every true seam that is not
// saturated must.
func TestHRDAdversarialSplices(t *testing.T) {
	fx := loadHRDFixtures(t)
	pol := DefaultHRDPolicy()
	units := fx.Stream.Units
	bp := bpIndices(units)
	if len(bp) < 60 {
		t.Fatalf("stream has %d buffering periods", len(bp))
	}
	edge := func(from, to int) HRDTrack {
		from = max(from, to-pol.TailUnits)
		return HRDTrack{Params: fx.Stream.Params, Units: units[from:to]}
	}
	head := func(at int) HRDTrack { return HRDTrack{Params: fx.Stream.Params, Units: units[at : at+1]} }
	counts := map[string]map[string]int{}
	tally := func(class, status string) {
		if counts[class] == nil {
			counts[class] = map[string]int{}
		}
		counts[class][status]++
	}
	for k := 10; k+31 < len(bp); k++ {
		at := bp[k] // the true next buffering period
		ctl := EvaluateHRDSeam(pol, edge(0, at), head(at))
		tally("control", ctl.Status)
		if ctl.Status == SourceContradicts {
			t.Errorf("true seam at unit %d contradicts: %+v", at, ctl)
		}
		check := func(class string, prevEnd, nextAt int) {
			ev := EvaluateHRDSeam(pol, edge(0, prevEnd), head(nextAt))
			tally(class, ev.Status)
			if ev.Status == SourceAdjacent {
				t.Errorf("%s at unit %d proved adjacent: %+v", class, at, ev)
			}
		}
		for _, f := range []int{1, 2, 3, 5, 12, 25} {
			check(fmt.Sprintf("skip_%02df", f), at-f, at) // previous clip lost its last f frames
		}
		for _, f := range []int{1, 2, 5, 10, 25, 75} {
			check(fmt.Sprintf("rewind_%02df", f), at+f, at) // previous clip ran f frames past the next start
		}
		for _, g := range []int{1, 2, 5, 10, 15, 30} {
			check(fmt.Sprintf("skip_%02dg", g), at, bp[k+g])
		}
		for _, g := range []int{1, 2, 5} {
			check(fmt.Sprintf("rewind_%dg", g), at, bp[k-g])
		}
	}
	classes := make([]string, 0, len(counts))
	for c := range counts {
		classes = append(classes, c)
	}
	sort.Strings(classes)
	for _, c := range classes {
		t.Logf("%-10s %v", c, counts[c])
	}
	if counts["control"][SourceAdjacent] < 20 {
		t.Fatalf("too few unsaturated true seams proved adjacent: %v", counts["control"])
	}
	for _, c := range classes {
		if c != "control" && counts[c][SourceContradicts] == 0 {
			t.Errorf("%s never proved a discontinuity: %v", c, counts[c])
		}
	}
}
