package collation

import (
	"strings"
	"testing"
	"time"
)

func v3Pair() (Clip, Clip, ClipMedia, ClipMedia) {
	t0 := time.Date(2026, 8, 13, 17, 0, 0, 0, time.UTC)
	a := Clip{ClipID: 1, RecordingID: 7, JobID: 9, CaptureLeaseToken: "l", CaptureSequence: 4, StartUTC: t0, EndUTC: t0.Add(56 * time.Second), NominalSeconds: 60}
	b := Clip{ClipID: 2, RecordingID: 7, JobID: 9, CaptureLeaseToken: "l", CaptureSequence: 5, StartUTC: a.EndUTC, EndUTC: a.EndUTC.Add(60 * time.Second), NominalSeconds: 60}
	m := ClipMedia{Playable: true, CodecSignature: "sig", ContentSeconds: 56, FrameSeconds: 0.04}
	n := m
	n.ContentSeconds = 60
	return a, b, m, n
}

func TestDecideSeamV3(t *testing.T) {
	policy := SeamPolicyFor(PolicyV3)
	adjacent := &SourceEvidence{Status: SourceAdjacent}
	contradicts := &SourceEvidence{Status: SourceContradicts}
	unavailable := &SourceEvidence{Status: SourceUnavailable}
	complete := &GOPEvidence{Status: GOPComplete}
	truncated := &GOPEvidence{Status: GOPTruncated}
	jump := &MatchEvidence{Verdict: MatchJump}
	cont := &MatchEvidence{Verdict: MatchContinuous}
	overlap := &MatchEvidence{Verdict: MatchOverlap, OverlapSeconds: 1}
	a, b, am, bm := v3Pair()
	full := func(c Clip, m ClipMedia) (Clip, ClipMedia) { // not cut short, spans match
		c.EndUTC = c.StartUTC.Add(60 * time.Second)
		m.ContentSeconds = 60
		return c, m
	}
	fa, fam := full(a, am)
	fb := b
	fb.StartUTC, fb.EndUTC = fa.EndUTC, fa.EndUTC.Add(60*time.Second)
	gap := fb
	gap.StartUTC, gap.EndUTC = fb.StartUTC.Add(5*time.Second), fb.EndUTC.Add(5*time.Second)
	cases := []struct {
		name       string
		prev, next Clip
		pm         ClipMedia
		match      *MatchEvidence
		src        *SourceEvidence
		gop        *GOPEvidence
		want, why  string
	}{
		{"cut short, source proves adjacency, pixels jump", a, b, am, jump, adjacent, complete, DecisionJoin, "source_adjacent"},
		{"cut short, no source evidence", a, b, am, jump, unavailable, complete, DecisionSplit, "prev_clip_cut_short"},
		{"cut short, adjacency but footage repeats", a, b, am, overlap, adjacent, complete, DecisionSplit, "prev_clip_cut_short"},
		{"cut short, adjacency but no frame match", a, b, am, nil, adjacent, complete, DecisionSplit, "prev_clip_cut_short"},
		{"pixels jump, source proves adjacency", fa, fb, fam, jump, adjacent, complete, DecisionJoin, "source_adjacent"},
		{"pixels overlap, source proves adjacency", fa, fb, fam, overlap, adjacent, complete, DecisionSplit, "frame_overlap"},
		{"db gap is a hard gate", fa, gap, fam, jump, adjacent, complete, DecisionSplit, "db_gap"},
		{"v2 join, source contradicts", fa, fb, fam, cont, contradicts, complete, DecisionSplit, "source_discontinuity"},
		{"v2 join, source unavailable", fa, fb, fam, cont, unavailable, complete, DecisionJoin, "continuous"},
		{"v2 join, previous clip ends inside a GOP", fa, fb, fam, cont, unavailable, truncated, DecisionSplit, "gop_truncated"},
		{"truncated GOP beats source adjacency", fa, fb, fam, cont, adjacent, truncated, DecisionSplit, "gop_truncated"},
	}
	for _, c := range cases {
		d := DecideSeamV3(policy, c.prev, c.next, c.pm, bm, c.match, c.src, c.gop)
		if d.Decision != c.want || d.Reason != c.why {
			t.Errorf("%s: got %s/%s want %s/%s", c.name, d.Decision, d.Reason, c.want, c.why)
		}
	}
	// Different capture leases stay split whatever the source says.
	other := fb
	other.CaptureLeaseToken = "m"
	if d := DecideSeamV3(policy, fa, other, fam, bm, jump, adjacent, complete); d.Decision != DecisionSplit {
		t.Errorf("lease change joined: %+v", d)
	}
}

func TestGOPLengths(t *testing.T) {
	if ev := EvaluateGOPLengths([]int{60, 60, 60}, 60, []int{60}, 8, 3); ev.Status != GOPComplete || ev.GOPLength != 60 {
		t.Fatalf("complete: %+v", ev)
	}
	for _, last := range []int{59, 35, 61, 1} {
		if ev := EvaluateGOPLengths([]int{60, 60, 60}, last, []int{60, 60}, 8, 3); ev.Status != GOPTruncated {
			t.Fatalf("last %d: %+v", last, ev)
		}
	}
	// Variable GOPs (scene cuts) or too few observations: no lattice, no verdict.
	if ev := EvaluateGOPLengths([]int{22, 45, 45}, 30, []int{45}, 8, 3); ev.Status != GOPIrregular {
		t.Fatalf("irregular: %+v", ev)
	}
	if ev := EvaluateGOPLengths([]int{60}, 30, []int{60}, 8, 3); ev.Status != GOPIrregular {
		t.Fatalf("too few: %+v", ev)
	}
	// Only the GOPs near the seam count.
	if ev := EvaluateGOPLengths([]int{45, 50, 50, 50}, 50, []int{50, 50, 50, 50, 45}, 3, 3); ev.Status != GOPComplete {
		t.Fatalf("window: %+v", ev)
	}
}

func TestBitReaderExpGolomb(t *testing.T) {
	// ue: 1 -> 0, 010 -> 1, 011 -> 2, 00100 -> 3; se: 00101 -> -2 (ue 4)
	r := &bitReader{b: []byte{0b10100110, 0b01000010, 0b10000000}}
	for _, want := range []uint64{0, 1, 2, 3} {
		if got, err := r.ue(); err != nil || got != want {
			t.Fatalf("ue: got %d want %d (%v)", got, want, err)
		}
	}
	if got, err := r.se(); err != nil || got != -2 {
		t.Fatalf("se: got %d (%v)", got, err)
	}
	if _, err := (&bitReader{b: []byte{0}}).ue(); err == nil {
		t.Fatal("truncated exp-golomb accepted")
	}
	if got := rbsp([]byte{0, 0, 3, 1, 0, 0, 3, 0}); string(got) != string([]byte{0, 0, 1, 0, 0, 0}) {
		t.Fatalf("rbsp %v", got)
	}
}

func TestPolicyVersionsAndHourIdentity(t *testing.T) {
	if s, err := SpecFor(""); err != nil || s != PolicyV2 {
		t.Fatalf("empty policy: %v %v", s, err)
	}
	if s, err := SpecFor("collation-v3"); err != nil || s != PolicyV3 {
		t.Fatalf("v3: %v %v", s, err)
	}
	if _, err := SpecFor("collation-v9"); err == nil {
		t.Fatal("unknown policy accepted")
	}
	v2, _ := HourIDFor("b", 1, "2026-08-13", 1)
	v3, _ := HourIDForGeneration("b", 1, "2026-08-13", 1, GenerationV3)
	if !strings.HasSuffix(v2, "__generation-2") || !strings.HasSuffix(v3, "__generation-3") {
		t.Fatalf("hour ids %s %s", v2, v3)
	}
	if _, err := HourIDForGeneration("b", 1, "2026-08-13", 1, 4); err == nil {
		t.Fatal("generation 4 accepted")
	}
	if SeamPolicyFor(PolicyV2).HRD != nil || SeamPolicyFor(PolicyV3).HRD == nil {
		t.Fatal("seam policy versions")
	}
}

func TestJoinProvenByPolicy(t *testing.T) {
	jump := &MatchEvidence{Verdict: MatchJump}
	sourceJoin := SeamDecision{Decision: DecisionJoin, Match: jump, Source: &SourceEvidence{Status: SourceAdjacent}}
	if joinProven(PolicyV2, sourceJoin) {
		t.Fatal("v2 manifest accepted a source-proven join")
	}
	if !joinProven(PolicyV3, sourceJoin) {
		t.Fatal("v3 manifest rejected a source-proven join")
	}
	overlap := sourceJoin
	overlap.Match = &MatchEvidence{Verdict: MatchOverlap}
	if joinProven(PolicyV3, overlap) {
		t.Fatal("joined repeated footage")
	}
	contradicted := SeamDecision{Decision: DecisionJoin, Match: &MatchEvidence{Verdict: MatchContinuous}, Source: &SourceEvidence{Status: SourceContradicts}}
	if joinProven(PolicyV3, contradicted) {
		t.Fatal("joined against source evidence")
	}
	if !joinProven(PolicyV2, SeamDecision{Decision: DecisionJoin, Match: &MatchEvidence{Verdict: MatchContinuous}}) {
		t.Fatal("v2 continuous join rejected")
	}
}
