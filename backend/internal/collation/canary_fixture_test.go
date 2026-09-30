package collation

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type canarySeam struct {
	ID              string       `json:"id"`
	Prev            Clip         `json:"prev"`
	Next            Clip         `json:"next"`
	PrevMedia       ClipMedia    `json:"prev_media"`
	NextMedia       ClipMedia    `json:"next_media"`
	Before          string       `json:"before"`
	BeforeReason    string       `json:"before_reason"`
	TruthContinuous *bool        `json:"truth_continuous"`
	Replay          bool         `json:"replay"`
	Curves          windowCurves `json:"curves"`
}

// TestGenerateCanaryFixtures consumes original clips and a frozen worklist;
// it never downloads, publishes, registers, or accesses a database.
func TestGenerateCanaryFixtures(t *testing.T) {
	root := os.Getenv("COLLATION_CANARY_ROOT")
	if root == "" {
		t.Skip("COLLATION_CANARY_ROOT not set")
	}
	body, err := os.ReadFile(filepath.Join(root, "inputs.json"))
	if err != nil {
		t.Fatal(err)
	}
	var rows []canarySeam
	if err = json.Unmarshal(body, &rows); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	tools := ToolsFromEnv()
	policy := DefaultSeamPolicy()
	sanitize := func(c Clip) Clip {
		c.Bucket, c.ObjectKey, c.ETag, c.SHA256 = "", "", "", ""
		c.SizeBytes = 0
		if c.CaptureLeaseToken != "" {
			h := sha256.Sum256([]byte(c.CaptureLeaseToken))
			c.CaptureLeaseToken = hex.EncodeToString(h[:8])
		}
		if c.CaptureAttemptID != "" {
			h := sha256.Sum256([]byte(c.CaptureAttemptID))
			c.CaptureAttemptID = hex.EncodeToString(h[:8])
		}
		return c
	}
	for i := range rows {
		r := &rows[i]
		ap := filepath.Join(root, "clips", fmt.Sprintf("%d.mp4", r.Prev.ClipID))
		bp := filepath.Join(root, "clips", fmt.Sprintf("%d.mp4", r.Next.ClipID))
		a, err := ProbeFile(ctx, tools, ap)
		if err != nil {
			t.Fatal(err)
		}
		b, err := ProbeFile(ctx, tools, bp)
		if err != nil {
			t.Fatal(err)
		}
		r.PrevMedia, r.NextMedia = a.Media, b.Media
		_, r.Replay = ReplayOverlap(a, b)
		at := func(w float64) Curves {
			p := policy
			p.WindowSeconds = w
			tail, err := ExtractWindow(ctx, tools, p, ap, true)
			if err != nil {
				return Curves{DecodeError: trimStderr(err.Error())}
			}
			head, err := ExtractWindow(ctx, tools, p, bp, false)
			if err != nil {
				return Curves{DecodeError: trimStderr(err.Error())}
			}
			return roundCurves(framesToCurves(p, tail, head, a.Video.WindowKeys(len(tail), true), b.Video.WindowKeys(len(head), false)))
		}
		r.Curves = windowCurves{Short: at(policy.ShortWindowSeconds), Full: at(policy.WindowSeconds)}
		r.Prev = sanitize(r.Prev)
		r.Next = sanitize(r.Next)
		ev := evaluateEscalating(policy, r.Curves, r.PrevMedia.FrameSeconds)
		d := DecideSeam(policy, r.Prev, r.Next, r.PrevMedia, r.NextMedia, &ev)
		if r.Replay {
			d.Decision, d.Reason = DecisionSplit, "packet_replay"
		}
		t.Logf("%s truth=%v decision=%s reason=%s match=%+v span_delta=%.3f/%.3f", r.ID, r.TruthContinuous, d.Decision, d.Reason, ev, r.PrevMedia.ContentSeconds-r.Prev.EndUTC.Sub(r.Prev.StartUTC).Seconds(), r.NextMedia.ContentSeconds-r.Next.EndUTC.Sub(r.Next.StartUTC).Seconds())
	}
	body, err = json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "canary_fixtures.json"), body, 0600); err != nil {
		t.Fatal(err)
	}
}

// TestGenerateLiveReviewFixtures supplements the original-pixel proof set with
// the retained 5s review excerpts. Inputs are cropped to remove all review UI;
// these re-encoded windows are supplemental evidence, not original clip probes.
func TestGenerateLiveReviewFixtures(t *testing.T) {
	root := os.Getenv("COLLATION_LIVE_REVIEW_ROOT")
	if root == "" {
		t.Skip("COLLATION_LIVE_REVIEW_ROOT not set")
	}
	body, err := os.ReadFile(filepath.Join(root, "inputs.json"))
	if err != nil {
		t.Fatal(err)
	}
	var rows []struct {
		ID              string `json:"id"`
		Prev, Next      caseClip
		TruthContinuous *bool `json:"truth_continuous"`
	}
	if err = json.Unmarshal(body, &rows); err != nil {
		t.Fatal(err)
	}
	var out []canarySeam
	tools := ToolsFromEnv()
	ctx := context.Background()
	policy := DefaultSeamPolicy()
	for _, r := range rows {
		a, am := r.Prev.clip(t)
		b, bm := r.Next.clip(t)
		a.RecordingID, b.RecordingID, a.JobID, b.JobID = 1, 1, 1, 1
		a.NominalSeconds, b.NominalSeconds = 30, 30
		ap, bp := filepath.Join(root, r.ID+"_a.mp4"), filepath.Join(root, r.ID+"_b.mp4")
		at := func(w float64) Curves {
			p := policy
			p.WindowSeconds = w
			tail, err := ExtractWindow(ctx, tools, p, ap, true)
			if err != nil {
				t.Fatal(err)
			}
			head, err := ExtractWindow(ctx, tools, p, bp, false)
			if err != nil {
				t.Fatal(err)
			}
			// The review encode's keyframe positions are unrelated to source GOPs.
			return roundCurves(framesToCurves(p, tail, head, nil, nil))
		}
		out = append(out, canarySeam{ID: r.ID, Prev: a, Next: b, PrevMedia: am, NextMedia: bm, TruthContinuous: r.TruthContinuous, Curves: windowCurves{Short: at(4), Full: at(5)}})
	}
	body, err = json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "fixtures.json"), body, 0600); err != nil {
		t.Fatal(err)
	}
}

func loadCanaryFixtures(t *testing.T, name string) []canarySeam {
	t.Helper()
	f, err := os.Open(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer zr.Close()
	var rows []canarySeam
	if err = json.NewDecoder(zr).Decode(&rows); err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestLiveReviewProof(t *testing.T) {
	p := DefaultSeamPolicy()
	p.WindowSeconds = 5
	rows := loadCanaryFixtures(t, "live_review_seams.json.gz")
	if len(rows) != 14 {
		t.Fatalf("got %d supplemental proof seams", len(rows))
	}
	broken := 0
	for _, r := range rows {
		if r.TruthContinuous != nil && !*r.TruthContinuous {
			broken++
		}
		ev := evaluateEscalating(p, r.Curves, math.Max(r.PrevMedia.FrameSeconds, r.NextMedia.FrameSeconds))
		d := DecideSeam(p, r.Prev, r.Next, r.PrevMedia, r.NextMedia, &ev)
		if r.TruthContinuous != nil && !*r.TruthContinuous && d.Decision != DecisionSplit {
			t.Errorf("broken review seam %s joined: %+v", r.ID, ev)
		}
		t.Logf("%s truth=%v decision=%s reason=%s match=%+v", r.ID, *r.TruthContinuous, d.Decision, d.Reason, ev)
	}
	if broken != 8 {
		t.Fatalf("got %d supplemental broken seams", broken)
	}
}

// TestLabeledCanarySeams replays Deniz's 2026-09-30 votes on original clip
// curves. Unknowns remain outside the confusion matrix and are kept split.
func TestLabeledCanarySeams(t *testing.T) {
	p := DefaultSeamPolicy()
	counts := map[string]int{}
	fixed := map[string]bool{"canary-s27": true, "canary-s33": true, "canary-s35": true, "canary-s36": true, "canary-s38": true}
	rows := loadCanaryFixtures(t, "canary_seams.json.gz")
	if len(rows) != 40 {
		t.Fatalf("got %d canaries", len(rows))
	}
	for _, r := range rows {
		ev := evaluateEscalating(p, r.Curves, math.Max(r.PrevMedia.FrameSeconds, r.NextMedia.FrameSeconds))
		d := DecideSeam(p, r.Prev, r.Next, r.PrevMedia, r.NextMedia, &ev)
		if r.Replay {
			d.Decision, d.Reason = DecisionSplit, "packet_replay"
		}
		want := DecisionSplit
		if r.Before == DecisionJoin || fixed[r.ID] {
			want = DecisionJoin
		}
		if d.Decision != want {
			t.Errorf("%s decision=%s want=%s reason=%s match=%+v", r.ID, d.Decision, want, d.Reason, ev)
		}
		if r.TruthContinuous == nil {
			counts["unknown_"+d.Decision]++
		} else if *r.TruthContinuous {
			counts["continuous_"+d.Decision]++
		} else {
			counts["broken_"+d.Decision]++
		}
		t.Logf("%s %s -> %s (%s)", r.ID, r.Before, d.Decision, d.Reason)
	}
	t.Logf("confusion matrix: %v", counts)
	if counts["continuous_join"] != 25 || counts["continuous_split"] != 3 || counts["broken_join"] != 0 || counts["broken_split"] != 8 || counts["unknown_split"] != 4 {
		t.Errorf("unexpected matrix %v", counts)
	}
}

func TestShortClipRequiresIdentifiedConsecutiveCaptureAndPixelProof(t *testing.T) {
	p := DefaultSeamPolicy()
	r := loadCanaryFixtures(t, "canary_seams.json.gz")[33]
	ev := evaluateEscalating(p, r.Curves, r.PrevMedia.FrameSeconds)
	if MetadataGate(p, r.Prev, r.Next, r.PrevMedia, r.NextMedia) != "" {
		t.Fatal("identified short clip never reaches pixel proof")
	}
	for _, v := range []string{MatchJump, MatchOverlap, MatchLowMotion, MatchTooShort, MatchDecodeFail} {
		bad := ev
		bad.Verdict = v
		if d := DecideSeam(p, r.Prev, r.Next, r.PrevMedia, r.NextMedia, &bad); d.Decision != DecisionSplit {
			t.Errorf("joined short clip with %s", v)
		}
	}
	for name, mutate := range map[string]func(*MatchEvidence){
		"remote head minimum":    func(m *MatchEvidence) { m.HeadMinMAD = m.BoundaryMAD - 1 },
		"abnormal boundary step": func(m *MatchEvidence) { m.StepP95MAD, m.KeyStepMedianMAD = 0, 0 },
	} {
		t.Run(name, func(t *testing.T) {
			bad := ev
			mutate(&bad)
			if d := DecideSeam(p, r.Prev, r.Next, r.PrevMedia, r.NextMedia, &bad); d.Decision != DecisionSplit || d.Reason != "short_clip_continuity_unproven" {
				t.Fatalf("short-clip endpoint proof not enforced: %+v", d)
			}
		})
	}
	if d := DecideSeam(p, r.Prev, r.Next, r.PrevMedia, r.NextMedia, nil); d.Decision != DecisionSplit {
		t.Fatal("joined without pixels")
	}
	for name, mutate := range map[string]func(*Clip, *Clip){
		"missing sequence": func(a, b *Clip) { a.CaptureSequence, b.CaptureSequence = 0, 0 },
		"missing identity": func(a, b *Clip) {
			a.CaptureAttemptID, b.CaptureAttemptID, a.CaptureLeaseToken, b.CaptureLeaseToken = "", "", "", ""
		},
		"different identity": func(a, b *Clip) { b.CaptureLeaseToken = "other" },
		"nonconsecutive":     func(a, b *Clip) { b.CaptureSequence++ },
		"gap":                func(a, b *Clip) { b.StartUTC = b.StartUTC.Add(time.Second) },
		"overlap":            func(a, b *Clip) { b.StartUTC = b.StartUTC.Add(-time.Second) },
	} {
		t.Run(name, func(t *testing.T) {
			a, b := r.Prev, r.Next
			mutate(&a, &b)
			if d := DecideSeam(p, a, b, r.PrevMedia, r.NextMedia, &ev); d.Decision != DecisionSplit {
				t.Fatal("invalid short clip joined")
			}
		})
	}
}
