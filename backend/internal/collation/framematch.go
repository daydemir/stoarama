package collation

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"os/exec"
	"sort"
	"strconv"
	"strings"
)

// Frame is one downscaled, blurred grayscale frame (FrameWidth x FrameHeight).
type Frame []byte

// ExtractWindow strictly decodes the tail (tail=true) or head window of a clip
// into small grayscale frames. Any decode error fails the extraction: a seam
// whose region does not decode cleanly can never be joined.
func ExtractWindow(ctx context.Context, tools Tools, policy SeamPolicy, path string, tail bool) ([]Frame, error) {
	w, h := policy.FrameWidth, policy.FrameHeight
	window := strconv.FormatFloat(policy.WindowSeconds, 'f', -1, 64)
	args := []string{"-nostdin", "-v", "error", "-xerror", "-err_detect", "explode", "-threads", "1", "-filter_threads", "1", "-skip_loop_filter", "all"}
	// passthrough: exactly one output frame per decoded source frame (VFR
	// sources would otherwise be resampled to CFR with duplicated frames,
	// misaligning frames with their packets' keyframe flags).
	if tail {
		args = append(args, "-sseof", "-"+window, "-i", path)
	} else {
		args = append(args, "-i", path, "-t", window)
	}
	filter := fmt.Sprintf("scale=%d:%d:flags=area", w, h)
	if policy.BlurSigma > 0 {
		filter += fmt.Sprintf(",gblur=sigma=%g", policy.BlurSigma)
	}
	args = append(args, "-map", "0:v:0", "-an", "-vf", filter+",format=gray", "-fps_mode", "passthrough", "-f", "rawvideo", "-")
	cmd := exec.CommandContext(ctx, tools.FFmpeg, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("decode %s window: %w: %s", map[bool]string{true: "tail", false: "head"}[tail], err, trimStderr(stderr.String()))
	}
	if s := decoderComplaints(stderr.String()); s != "" {
		return nil, fmt.Errorf("decode window reported errors: %s", trimStderr(s))
	}
	raw := stdout.Bytes()
	size := w * h
	if len(raw)%size != 0 {
		return nil, fmt.Errorf("decoded window has a partial frame")
	}
	frames := make([]Frame, len(raw)/size)
	for i := range frames {
		frames[i] = Frame(raw[i*size : (i+1)*size])
	}
	return frames, nil
}

func trimStderr(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 300 {
		s = s[:300]
	}
	return s
}

func mad(a, b Frame) float64 {
	var sum int64
	for i := range a {
		d := int64(a[i]) - int64(b[i])
		if d < 0 {
			d = -d
		}
		sum += d
	}
	return float64(sum) / float64(len(a))
}

func median(v []float64) float64 {
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	n := len(s)
	if n == 0 {
		return 0
	}
	if n%2 == 1 {
		return s[n/2]
	}
	return (s[n/2-1] + s[n/2]) / 2
}

func percentile(v []float64, p float64) float64 {
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	if len(s) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(s)))) - 1
	if idx < 0 {
		idx = 0
	}
	return s[idx]
}

func argminExcluding(v []float64, exclude []bool, keep int) int {
	best := -1
	for i := range v {
		if len(exclude) == len(v) && exclude[i] && i != keep {
			continue
		}
		if best < 0 || v[i] < v[best] {
			best = i
		}
	}
	return best
}

func argmin(v []float64) int {
	best := 0
	for i := range v {
		if v[i] < v[best] {
			best = i
		}
	}
	return best
}

// EvaluateFrames decides whether B's head continues A's tail exactly at the
// boundary. tail is A's last window in presentation order, head is B's first;
// tailKeys/headKeys flag which of those frames are keyframes.
//
// Distances are measured only over the "moving" pixels (the MotionPixelShare
// of pixels with the highest temporal variance across both windows), so a
// static background cannot make unrelated moments look alike, and compression
// noise on still areas cannot hide a match.
//
//   - continuous: B[0] best matches A's LAST frames and A[-1] best matches B's
//     first frames, both minima are sharp against the window median, the scene
//     moves enough to discriminate, and the boundary step is no larger than a
//     normal step into a keyframe (B[0] is always a keyframe, and keyframes
//     "pop" against the preceding P-frame even in continuous footage).
//   - overlap: B[0] sharply matches a frame well before A's end (footage
//     repeats); the overlap length is recorded.
//   - everything else is a jump (or low motion / too short) and splits.
func EvaluateFrames(policy SeamPolicy, tail, head []Frame, tailKeys, headKeys []bool, frameSeconds float64) MatchEvidence {
	if len(tail) < policy.MinFrames || len(head) < policy.MinFrames {
		return MatchEvidence{Verdict: MatchTooShort, TailFrames: len(tail), HeadFrames: len(head)}
	}
	return EvaluateCurves(policy, framesToCurves(policy, tail, head, tailKeys, headKeys), frameSeconds)
}

// motionMask selects the share of pixels with the highest temporal variance.
func motionMask(frames []Frame, share float64) []int {
	n := len(frames[0])
	variance := make([]float64, n)
	for p := 0; p < n; p++ {
		var sum, sq float64
		for _, f := range frames {
			v := float64(f[p])
			sum += v
			sq += v * v
		}
		mean := sum / float64(len(frames))
		variance[p] = sq/float64(len(frames)) - mean*mean
	}
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool { return variance[order[i]] > variance[order[j]] })
	k := max(int(math.Ceil(share*float64(n))), 1)
	return order[:k]
}

func maskedMAD(a, b Frame, mask []int) float64 {
	var sum int64
	for _, p := range mask {
		d := int64(a[p]) - int64(b[p])
		if d < 0 {
			d = -d
		}
		sum += d
	}
	return float64(sum) / float64(len(mask))
}

func framesToCurves(policy SeamPolicy, tail, head []Frame, tailKeys, headKeys []bool) Curves {
	all := append(append([]Frame(nil), tail...), head...)
	keys := make([]bool, 0, len(all))
	if len(tailKeys) == len(tail) && len(headKeys) == len(head) {
		keys = append(append(keys, tailKeys...), headKeys...)
	}
	mask := motionMask(all, policy.MotionPixelShare)
	b0, aLast := head[0], tail[len(tail)-1]
	c := Curves{TailToHead0: make([]float64, len(tail)), TailLastToHead: make([]float64, len(head)), BoundaryMAD: maskedMAD(aLast, b0, mask)}
	if len(keys) == len(all) {
		c.TailKeys, c.HeadKeys = keys[:len(tail)], keys[len(tail):]
	}
	for k := range tail {
		c.TailToHead0[k] = maskedMAD(tail[k], b0, mask)
	}
	for j := range head {
		c.TailLastToHead[j] = maskedMAD(aLast, head[j], mask)
	}
	for i := 1; i < len(all); i++ {
		if i == len(tail) {
			continue // the seam itself
		}
		step := maskedMAD(all[i-1], all[i], mask)
		if len(keys) == len(all) && keys[i] {
			c.KeySteps = append(c.KeySteps, step)
		} else {
			c.Steps = append(c.Steps, step)
		}
	}
	return c
}

// Curves are the only frame-derived inputs of a seam verdict:
// TailToHead0[k] = MAD(A[k], B[0]), TailLastToHead[j] = MAD(A[-1], B[j]),
// Steps / KeySteps = consecutive-frame MADs inside both windows into a
// non-keyframe / keyframe.
type Curves struct {
	TailToHead0    []float64 `json:"tail_to_head0"`
	TailLastToHead []float64 `json:"tail_last_to_head"`
	Steps          []float64 `json:"steps"`
	KeySteps       []float64 `json:"key_steps"`
	BoundaryMAD    float64   `json:"boundary_mad"`
	// TailKeys/HeadKeys flag keyframes in each window (empty when unknown).
	TailKeys []bool `json:"tail_keys,omitempty"`
	HeadKeys []bool `json:"head_keys,omitempty"`
	// DecodeError is set (and the curves empty) when a window failed to decode.
	DecodeError string `json:"decode_error,omitempty"`
}

// EvaluateCurves is the seam verdict rule (see EvaluateFrames).
func EvaluateCurves(policy SeamPolicy, c Curves, frameSeconds float64) MatchEvidence {
	ev := MatchEvidence{TailFrames: len(c.TailToHead0), HeadFrames: len(c.TailLastToHead)}
	if c.DecodeError != "" {
		ev.Verdict, ev.DecodeError = MatchDecodeFail, c.DecodeError
		return ev
	}
	if ev.TailFrames < policy.MinFrames || ev.HeadFrames < policy.MinFrames {
		ev.Verdict = MatchTooShort
		return ev
	}
	dA, dB := c.TailToHead0, c.TailLastToHead
	// B[0] is a keyframe; other keyframes share its fresh-encode "pop" and can
	// look spuriously similar to it. Best-match candidates therefore exclude
	// keyframes other than the boundary frames themselves.
	kA := argminExcluding(dA, c.TailKeys, len(dA)-1)
	jB := argminExcluding(dB, c.HeadKeys, 0)
	ev.BoundaryMAD = round4(c.BoundaryMAD)
	ev.TailMinMAD, ev.TailMinOffset, ev.TailMedianMAD = round4(dA[kA]), len(dA)-1-kA, round4(median(dA))
	ev.HeadMinMAD, ev.HeadMinOffset, ev.HeadMedianMAD = round4(dB[jB]), jB, round4(median(dB))
	ev.StepP95MAD = round4(percentile(c.Steps, 0.95))
	// The expected boundary step: B[0] is a keyframe, so compare against steps
	// into keyframes when the windows contain any, else ordinary steps.
	ev.KeyStepMedianMAD = round4(median(c.KeySteps))
	expected := ev.KeyStepMedianMAD
	if len(c.KeySteps) == 0 {
		expected = ev.StepP95MAD
	}
	tailSharp := ev.TailMinMAD <= policy.TailSharpRatio*ev.TailMedianMAD
	headSharp := ev.HeadMinMAD <= policy.HeadSharpRatio*ev.HeadMedianMAD
	lowMotion := ev.TailMedianMAD < policy.MinSceneMedianMAD || ev.HeadMedianMAD < policy.MinSceneMedianMAD
	switch {
	case tailSharp && ev.TailMinOffset > policy.MaxTailOffsetFrames && !lowMotion:
		ev.Verdict = MatchOverlap
		ev.OverlapSeconds = round4(float64(ev.TailMinOffset) * frameSeconds)
	case lowMotion:
		ev.Verdict = MatchLowMotion
	case tailSharp && headSharp &&
		ev.TailMinOffset <= policy.MaxTailOffsetFrames && ev.HeadMinOffset <= policy.MaxHeadOffsetFrames &&
		ev.BoundaryMAD <= policy.BoundaryStepFactor*expected+policy.StepSlackMAD:
		ev.Verdict = MatchContinuous
	default:
		ev.Verdict = MatchJump
	}
	return ev
}

func round4(v float64) float64 { return math.Round(v*1e4) / 1e4 }
