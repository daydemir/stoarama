package collation

import (
	"encoding/json"
	"math"
	"testing"
	"time"
)

func adversarialEndpointCurves(n int, boundary float64) Curves {
	return Curves{TailToHead0: curve(n, func(i int) float64 { return boundary + 0.1*float64(n-1-i) }),
		TailLastToHead: curve(n, func(i int) float64 { return boundary + 0.1*float64(i) }),
		BoundaryMAD:    boundary, Steps: []float64{0.4, 0.4, 0.4}, KeySteps: []float64{0.4, 0.4}}
}

// These are the five counterexamples in the independent review of bc092824.
// Each must reject despite clean DB stamps and freshly encoded source packets.
func TestAdversarialSeamRelaxationsNeverJoin(t *testing.T) {
	p := DefaultSeamPolicy()
	t.Run("equal_tail_minima_rewind_and_VFR_plateau", func(t *testing.T) {
		c := adversarialEndpointCurves(250, 0)
		for i := 175; i < 250; i++ {
			c.TailToHead0[i] = 0
		}
		for i := 0; i < 75; i++ {
			c.TailLastToHead[i] = 0
		}
		ev := EvaluateCurves(p, c, 0.04)
		if ev.Verdict != MatchOverlap || ev.TailMinOffset != 74 {
			t.Fatalf("replayed plateau lost its earliest equal minimum: %+v", ev)
		}
	})
	t.Run("lease_only_short_clip_reconnect", func(t *testing.T) {
		a, b, am, bm := baseClips()
		a.EndUTC = a.StartUTC.Add(56 * time.Second)
		b.StartUTC = a.EndUTC
		b.EndUTC = b.StartUTC.Add(60 * time.Second)
		am.ContentSeconds = 56
		a.CaptureLeaseToken, b.CaptureLeaseToken = "same-lease", "same-lease"
		ev := EvaluateCurves(p, adversarialEndpointCurves(250, 0.4), 0.04)
		if ev.Verdict != MatchContinuous {
			t.Fatalf("control pixels must pass to isolate the session gate: %+v", ev)
		}
		if d := DecideSeam(p, a, b, am, bm, &ev); d.Decision != DecisionSplit || d.Reason != "prev_clip_cut_short" {
			t.Fatalf("lease order is not capture-session continuity: %+v", d)
		}
	})
	t.Run("p95_hides_one_frame_forward_skip", func(t *testing.T) {
		c := adversarialEndpointCurves(100, 4)
		c.KeySteps = []float64{1, 1}
		c.Steps = []float64{2, 2, 3}
		if ev := EvaluateCurves(p, c, 0.2); ev.Verdict != MatchJump {
			t.Fatalf("200ms forward skip joined: %+v", ev)
		}
	})
	t.Run("endpoint_slack_bypasses_head_offset", func(t *testing.T) {
		c := adversarialEndpointCurves(100, 0.4)
		c.TailLastToHead[40] = 0.37
		if ev := EvaluateCurves(p, c, 0.04); ev.Verdict != MatchJump {
			t.Fatalf("same-framing reconnect joined through a remote minimum: %+v", ev)
		}
	})
	t.Run("keyframe_dominated_shallow_minima", func(t *testing.T) {
		c := adversarialEndpointCurves(100, 7)
		c.TailToHead0 = curve(100, func(int) float64 { return 10 })
		c.TailToHead0[99] = 7
		c.TailLastToHead = curve(100, func(int) float64 { return 9 })
		c.TailLastToHead[0] = 7
		c.KeySteps = []float64{7, 7}
		if ev := EvaluateCurves(p, c, 0.04); ev.Verdict != MatchJump {
			t.Fatalf("compression variation substituted for continuity: %+v", ev)
		}
	})
}

func TestAdversarialShortWindowJumpCannotBeRescued(t *testing.T) {
	p := DefaultSeamPolicy()
	short := adversarialEndpointCurves(100, 2)
	short.KeySteps = []float64{0.1}
	short.Steps = []float64{1}
	short.TailLastToHead[40] = 0.3 // short pixels do not prove an endpoint match
	full := adversarialEndpointCurves(250, 0.4)
	full.KeySteps = []float64{1}
	ev := evaluateEscalating(p, windowCurves{Short: short, Full: full}, 0.04)
	if ev.Verdict == MatchContinuous || ev.WindowSeconds != p.ShortWindowSeconds {
		t.Fatalf("clear short-window discontinuity rescued by full window: %+v", ev)
	}
}

func TestRestampedContractAdversarialSeamsNeverJoin(t *testing.T) {
	p := DefaultSeamPolicy()
	p.WindowSeconds = 5
	found := 0
	for _, r := range loadCanaryFixtures(t, "live_review_seams.json.gz") {
		if r.ID != "A7_1" && r.ID != "A8_1" {
			continue
		}
		found++
		r.Next.CaptureSequence = r.Prev.CaptureSequence + 1
		ev := evaluateEscalating(p, r.Curves, math.Max(r.PrevMedia.FrameSeconds, r.NextMedia.FrameSeconds))
		if d := DecideSeam(p, r.Prev, r.Next, r.PrevMedia, r.NextMedia, &ev); d.Decision != DecisionSplit {
			t.Errorf("%s restamped sequence hides forward skip: %+v", r.ID, d)
		}
		// Original cached probe: A ends at 2700360/90000 and B begins at
		// 5400630/90000. Altering DB/sequence labels cannot close this source gap.
		// JSON keeps this regression runnable on bc092824, which ignores timing.
		r.PrevMedia = withSourceTiming(t, r.PrevMedia, "0", "7501/250")
		r.NextMedia = withSourceTiming(t, r.NextMedia, "60007/1000", "90011/1000")
		r.Prev.CaptureAttemptID, r.Next.CaptureAttemptID = "proof-contract-attempt-a", "proof-contract-attempt-a"
		if d := DecideSeam(p, r.Prev, r.Next, r.PrevMedia, r.NextMedia, &ev); d.Decision != DecisionSplit || d.Reason != "source_pts_not_consecutive" {
			t.Errorf("%s same-attempt restamp hides original source PTS gap: %+v", r.ID, d)
		}
	}
	if found != 2 {
		t.Fatalf("got %d required contract attack fixtures", found)
	}
}

// JSON makes the attack runnable on bc092824, whose media summary ignored
// these original packet timestamps. Direct struct fields would not compile there.
func withSourceTiming(t *testing.T, media ClipMedia, start, end string) ClipMedia {
	t.Helper()
	body, err := json.Marshal(media)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(body, &fields); err != nil {
		t.Fatal(err)
	}
	fields["video_start_pts"], fields["video_end_pts"] = start, end
	body, err = json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(body, &media); err != nil {
		t.Fatal(err)
	}
	return media
}

func TestContractSourceTimingMustBeMeasuredAndExactlyAdjacent(t *testing.T) {
	p := DefaultSeamPolicy()
	a, b, am, bm := baseClips()
	a.CaptureAttemptID, b.CaptureAttemptID = "same-contract-process", "same-contract-process"
	am = withSourceTiming(t, am, "0", "60")
	bm = withSourceTiming(t, bm, "60", "120")
	ev := EvaluateCurves(p, adversarialEndpointCurves(250, 0.4), 0.04)
	if d := DecideSeam(p, a, b, am, bm, &ev); d.Decision != DecisionJoin {
		t.Fatalf("positive source contract rejected: %+v", d)
	}
	for name, pts := range map[string][2]string{
		"missing": {"", ""}, "malformed": {"bogus", "120"}, "reversed": {"120", "60"},
		"one_frame_skip": {"1501/25", "3001/25"}, "one_frame_rewind": {"1499/25", "2999/25"},
	} {
		t.Run(name, func(t *testing.T) {
			bad := withSourceTiming(t, bm, pts[0], pts[1])
			if d := DecideSeam(p, a, b, am, bad, &ev); d.Decision != DecisionSplit {
				t.Fatalf("DB restamp hid %s source timeline: %+v", name, d)
			}
		})
	}
}
