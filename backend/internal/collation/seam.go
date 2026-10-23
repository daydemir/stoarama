// Package collation builds joined hours: generation 2 ("collation-v2") and,
// opt-in, generation 3 ("collation-v3").
//
// The one hard rule: never join non-sequential footage. Under v2 every seam
// between two time-adjacent clips must pass every metadata gate AND a
// frame-match proof of continuity; anything else is a split. v3 keeps every v2
// decision and adds independent source evidence (see h264hrd.go): it may join
// a seam v2 split on a heuristic only when the source's own encoder state
// proves adjacency, and it splits a v2 join that the same evidence proves
// discontinuous. Pixels alone never turn a split into a join. Extra splits are
// acceptable, a wrong join is not.
package collation

import (
	"fmt"
	"math"
	"time"
)

const (
	// PolicyVersion and Generation identify the default (v2) policy.
	PolicyVersion = "collation-v2"
	Generation    = 2

	PolicyVersionV3 = "collation-v3"
	GenerationV3    = 3

	DecisionJoin  = "join"
	DecisionSplit = "split"
)

// SeamPolicy holds every threshold that decides a join. It is written into each
// hour manifest so a decision can be re-audited against the rule that made it.
type SeamPolicy struct {
	// ShortWindowSeconds is tried first; a seam that does not prove continuous
	// there is re-examined on WindowSeconds.
	ShortWindowSeconds float64 `json:"short_window_seconds"`
	WindowSeconds      float64 `json:"window_seconds"`
	FrameWidth         int     `json:"frame_width"`
	FrameHeight        int     `json:"frame_height"`
	// BlurSigma is the gaussian blur applied after downscaling (0 = none).
	BlurSigma float64 `json:"blur_sigma"`
	// Share of pixels (highest temporal variance) the distances are measured on.
	MotionPixelShare float64 `json:"motion_pixel_share"`
	// A continuous seam has B[0]'s best match within this many frames of A's end
	// and A[-1]'s best match within this many frames of B's start...
	MaxTailOffsetFrames int `json:"max_tail_offset_frames"`
	MaxHeadOffsetFrames int `json:"max_head_offset_frames"`
	// ...and each best match is sharp: min MAD <= ratio x window median MAD.
	TailSharpRatio float64 `json:"tail_sharp_ratio"`
	HeadSharpRatio float64 `json:"head_sharp_ratio"`
	// Below this (masked) window median there is too little motion to prove anything.
	MinSceneMedianMAD float64 `json:"min_scene_median_mad"`
	// The boundary step must look like an ordinary step into a keyframe:
	// MAD(A[-1],B[0]) <= factor x median(key steps) + slack.
	BoundaryStepFactor float64 `json:"boundary_step_factor"`
	// A short-window jump whose boundary step exceeds this factor needs no
	// full-window second look (it can only split).
	ClearJumpStepFactor float64 `json:"clear_jump_step_factor"`
	StepSlackMAD        float64 `json:"step_slack_mad"`
	MinFrames           int     `json:"min_frames"`
	GapFrameSlack       float64 `json:"db_gap_tolerance_frames"`
	// Decoded content duration must match the stamped span within this many frames.
	SpanFrameSlack float64 `json:"content_span_tolerance_frames"`
	// A previous clip shorter than the recording's nominal clip length by more
	// than this was cut early: a capture restart indicator.
	ShortClipSlackSeconds float64 `json:"short_clip_slack_seconds"`
	// HRD is the v3 source-continuity proof (nil under v2).
	HRD *HRDPolicy `json:"hrd,omitempty"`
}

// PolicySpec is one versioned seam policy and the hour generation it writes.
type PolicySpec struct {
	Version    string
	Generation int
}

var (
	PolicyV2 = PolicySpec{Version: PolicyVersion, Generation: Generation}
	PolicyV3 = PolicySpec{Version: PolicyVersionV3, Generation: GenerationV3}
)

// SpecFor resolves a policy version ("" is v2, for worklists written before v3).
func SpecFor(version string) (PolicySpec, error) {
	switch version {
	case "", PolicyVersion:
		return PolicyV2, nil
	case PolicyVersionV3:
		return PolicyV3, nil
	}
	return PolicySpec{}, fmt.Errorf("unknown collation policy %q", version)
}

func (p SeamPolicy) windows() []float64 {
	if p.ShortWindowSeconds > 0 && p.ShortWindowSeconds < p.WindowSeconds {
		return []float64{p.ShortWindowSeconds, p.WindowSeconds}
	}
	return []float64{p.WindowSeconds}
}

func DefaultSeamPolicy() SeamPolicy {
	return SeamPolicy{
		ShortWindowSeconds:    4,
		WindowSeconds:         10,
		FrameWidth:            96,
		FrameHeight:           64,
		BlurSigma:             0,
		MotionPixelShare:      0.10,
		MaxTailOffsetFrames:   3,
		MaxHeadOffsetFrames:   10,
		TailSharpRatio:        0.60,
		HeadSharpRatio:        0.70,
		MinSceneMedianMAD:     0.8,
		BoundaryStepFactor:    2.0,
		ClearJumpStepFactor:   4.0,
		StepSlackMAD:          0.05,
		MinFrames:             10,
		GapFrameSlack:         1,
		SpanFrameSlack:        2,
		ShortClipSlackSeconds: 2,
	}
}

// SeamPolicyFor returns the thresholds of a policy version.
func SeamPolicyFor(spec PolicySpec) SeamPolicy {
	p := DefaultSeamPolicy()
	if spec.Version == PolicyVersionV3 {
		h := DefaultHRDPolicy()
		p.HRD = &h
	}
	return p
}

// Clip is one source clip exactly as the planner froze it from the database.
type Clip struct {
	ClipID            int64     `json:"clip_id"`
	RecordingID       int64     `json:"recording_id"`
	JobID             int64     `json:"recording_job_id"`
	CaptureAttemptID  string    `json:"capture_attempt_id,omitempty"`
	CaptureLeaseToken string    `json:"capture_lease_token,omitempty"`
	CaptureSequence   int64     `json:"capture_sequence,omitempty"`
	StartUTC          time.Time `json:"start_utc"`
	EndUTC            time.Time `json:"end_utc"`
	Bucket            string    `json:"bucket"`
	ObjectKey         string    `json:"object_key"`
	ETag              string    `json:"etag,omitempty"`
	SizeBytes         int64     `json:"size_bytes"`
	SHA256            string    `json:"sha256,omitempty"`
	// NominalSeconds is the recording's configured clip length (0 = unknown).
	NominalSeconds float64 `json:"nominal_seconds,omitempty"`
	Purged         bool    `json:"purged,omitempty"`
	KnownBroken    bool    `json:"known_broken,omitempty"`
}

// ClipMedia is the decoded-free media summary of one clip (see Probe).
type ClipMedia struct {
	Playable       bool    `json:"playable"`
	FailReason     string  `json:"fail_reason,omitempty"`
	CodecSignature string  `json:"codec_signature,omitempty"`
	HasAudio       bool    `json:"has_audio"`
	VideoPackets   int     `json:"video_packets"`
	ContentSeconds float64 `json:"content_seconds"`
	FrameSeconds   float64 `json:"frame_seconds"`
}

// MatchEvidence is the frame-match proof for one seam. A is the previous
// clip's tail window, B the next clip's head window.
type MatchEvidence struct {
	Verdict          string  `json:"verdict"`
	WindowSeconds    float64 `json:"window_seconds"`
	TailFrames       int     `json:"tail_frames"`
	HeadFrames       int     `json:"head_frames"`
	BoundaryMAD      float64 `json:"boundary_mad"`
	TailMinMAD       float64 `json:"tail_min_mad"`
	TailMinOffset    int     `json:"tail_min_offset_frames"`
	TailMedianMAD    float64 `json:"tail_median_mad"`
	HeadMinMAD       float64 `json:"head_min_mad"`
	HeadMinOffset    int     `json:"head_min_offset_frames"`
	HeadMedianMAD    float64 `json:"head_median_mad"`
	StepP95MAD       float64 `json:"step_p95_mad"`
	KeyStepMedianMAD float64 `json:"key_step_median_mad"`
	OverlapSeconds   float64 `json:"overlap_seconds,omitempty"`
	DecodeError      string  `json:"decode_error,omitempty"`
}

const (
	MatchContinuous = "continuous"
	MatchOverlap    = "overlap"
	MatchJump       = "jump"
	MatchLowMotion  = "low_motion"
	MatchTooShort   = "insufficient_frames"
	MatchDecodeFail = "seam_decode_failed"
)

type SeamDecision struct {
	PrevClipID     int64          `json:"prev_clip_id"`
	NextClipID     int64          `json:"next_clip_id"`
	Decision       string         `json:"decision"`
	Reason         string         `json:"reason"`
	DBGapSeconds   float64        `json:"db_gap_seconds"`
	FrameSeconds   float64        `json:"frame_seconds"`
	OverlapSeconds float64        `json:"overlap_seconds,omitempty"`
	Match          *MatchEvidence `json:"frame_match,omitempty"`
	// Source is the v3 independent source-continuity evidence.
	Source *SourceEvidence `json:"source_evidence,omitempty"`
	// GOP is the v3 keyframe-lattice check.
	GOP *GOPEvidence `json:"gop_lattice,omitempty"`
}

// MetadataGate evaluates every non-visual join condition. It returns "" when
// all hold (the seam then needs a frame-match proof) or the first failed
// condition as a stable reason code.
func MetadataGate(policy SeamPolicy, prev, next Clip, pm, nm ClipMedia) string {
	switch {
	case prev.RecordingID != next.RecordingID:
		return "different_recording"
	case prev.JobID <= 0 || next.JobID <= 0:
		return "missing_job"
	case prev.JobID != next.JobID:
		return "different_job"
	case (prev.CaptureAttemptID == "") != (next.CaptureAttemptID == ""):
		return "capture_attempt_indicator_changed"
	case prev.CaptureAttemptID != next.CaptureAttemptID:
		return "different_capture_attempt"
	case (prev.CaptureLeaseToken == "") != (next.CaptureLeaseToken == ""):
		return "capture_lease_indicator_changed"
	case prev.CaptureLeaseToken != next.CaptureLeaseToken:
		return "different_capture_lease"
	case (prev.CaptureSequence == 0) != (next.CaptureSequence == 0):
		return "capture_sequence_indicator_changed"
	case prev.CaptureSequence != 0 && next.CaptureSequence != prev.CaptureSequence+1:
		return "capture_sequence_not_consecutive"
	case !pm.Playable || !nm.Playable:
		return "unplayable"
	case pm.CodecSignature == "" || pm.CodecSignature != nm.CodecSignature:
		return "codec_params_differ"
	}
	if prev.NominalSeconds > 0 && prev.EndUTC.Sub(prev.StartUTC).Seconds() < prev.NominalSeconds-policy.ShortClipSlackSeconds {
		return "prev_clip_cut_short"
	}
	frame := math.Max(pm.FrameSeconds, nm.FrameSeconds)
	if frame <= 0 || frame > 2 {
		return "unknown_frame_duration"
	}
	gap := next.StartUTC.Sub(prev.EndUTC).Seconds()
	if gap > policy.GapFrameSlack*frame {
		return "db_gap"
	}
	if gap < -policy.GapFrameSlack*frame {
		return "db_overlap"
	}
	for _, c := range []struct {
		clip  Clip
		media ClipMedia
	}{{prev, pm}, {next, nm}} {
		span := c.clip.EndUTC.Sub(c.clip.StartUTC).Seconds()
		if math.Abs(c.media.ContentSeconds-span) > policy.SpanFrameSlack*frame {
			return "content_span_mismatch"
		}
	}
	return ""
}

// DecideSeam combines the metadata gate with the frame-match evidence. match
// may be nil only when the gate already failed.
func DecideSeam(policy SeamPolicy, prev, next Clip, pm, nm ClipMedia, match *MatchEvidence) SeamDecision {
	d := SeamDecision{PrevClipID: prev.ClipID, NextClipID: next.ClipID, Decision: DecisionSplit,
		DBGapSeconds: round6(next.StartUTC.Sub(prev.EndUTC).Seconds()),
		FrameSeconds: round6(math.Max(pm.FrameSeconds, nm.FrameSeconds)), Match: match}
	if reason := MetadataGate(policy, prev, next, pm, nm); reason != "" {
		d.Reason = reason
		return d
	}
	if match == nil {
		d.Reason = "frame_match_missing"
		return d
	}
	if match.Verdict != MatchContinuous {
		d.Reason = "frame_" + match.Verdict
		d.OverlapSeconds = match.OverlapSeconds
		return d
	}
	d.Decision, d.Reason = DecisionJoin, "continuous"
	return d
}

// softGates are v2 metadata reasons that are heuristics about capture
// restarts rather than facts about the media: a clip ending early, or DB
// stamps disagreeing with the decoded duration. Exact source adjacency
// overrides them; every other gate stays a hard split.
var softGates = map[string]bool{"prev_clip_cut_short": true, "content_span_mismatch": true}

// SoftGate reports whether a MetadataGate reason may be overridden by proof.
func SoftGate(reason string) bool { return softGates[reason] }

// sourceJoinVerdicts are the frame verdicts that show no duplicated footage.
// A frame "jump" or "low_motion" verdict is not evidence of a discontinuity
// once the source proves adjacency; an overlap is, and is never overridden.
var sourceJoinVerdicts = map[string]bool{MatchContinuous: true, MatchJump: true, MatchLowMotion: true}

// DecideSeamV3 is the collation-v3 rule. It starts from the v2 decision and
//   - splits any seam whose previous clip ends inside a fixed-length GOP;
//   - splits a v2 join whose source evidence proves a discontinuity;
//   - joins a v2 split only when the source proves adjacency, every hard
//     metadata gate passes, and the frame match shows no duplicated footage.
//
// match may be nil only when no frame match ran (the seam then never joins).
func DecideSeamV3(policy SeamPolicy, prev, next Clip, pm, nm ClipMedia, match *MatchEvidence, src *SourceEvidence, gop *GOPEvidence) SeamDecision {
	d := DecideSeam(policy, prev, next, pm, nm, match)
	d.Source, d.GOP = src, gop
	if gop != nil && gop.Status == GOPTruncated {
		if d.Decision == DecisionJoin || d.Reason == "" {
			d.Reason = "gop_truncated"
		}
		d.Decision = DecisionSplit
		return d
	}
	if src == nil {
		return d
	}
	switch {
	case d.Decision == DecisionJoin && src.Status == SourceContradicts:
		d.Decision, d.Reason = DecisionSplit, "source_discontinuity"
	case d.Decision == DecisionSplit && src.Status == SourceAdjacent && match != nil && sourceJoinVerdicts[match.Verdict]:
		gate := MetadataGate(policy, prev, next, pm, nm)
		if gate == "" || SoftGate(gate) {
			d.Decision, d.Reason, d.OverlapSeconds = DecisionJoin, "source_adjacent", 0
		}
	}
	return d
}

func round6(v float64) float64 { return math.Round(v*1e6) / 1e6 }
