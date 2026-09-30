// Package collation builds generation-2 joined hours ("collation v2").
//
// The one hard rule: never join non-sequential footage. Every seam between two
// time-adjacent clips must pass every metadata gate AND a frame-match proof of
// continuity; anything else is a split. Extra splits are acceptable, a wrong
// join is not.
package collation

import (
	"math"
	"time"
)

const (
	PolicyVersion = "collation-v2.1"
	Generation    = 2

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
	// A sharper tail match beyond this offset is an overlap. The default
	// requires the endpoint itself (exact equal minima resolve to the endpoint).
	// A[-1]'s head match must be within MaxHeadOffsetFrames or tied at B[0].
	MaxTailOffsetFrames int `json:"max_tail_offset_frames"`
	MaxHeadOffsetFrames int `json:"max_head_offset_frames"`
	// ...and each best match is sharp: min MAD <= ratio x window median MAD.
	TailSharpRatio float64 `json:"tail_sharp_ratio"`
	HeadSharpRatio float64 `json:"head_sharp_ratio"`
	// Below this (masked) window median there is too little motion to prove anything.
	MinSceneMedianMAD float64 `json:"min_scene_median_mad"`
	// The boundary step must look like an ordinary step into a keyframe:
	// MAD(A[-1],B[0]) <= factor x max(median(key steps), p95(non-key steps)) + slack.
	BoundaryStepFactor float64 `json:"boundary_step_factor"`
	// A short-window jump whose boundary step exceeds this factor needs no
	// full-window second look (it can only split).
	ClearJumpStepFactor float64 `json:"clear_jump_step_factor"`
	StepSlackMAD        float64 `json:"step_slack_mad"`
	MinFrames           int     `json:"min_frames"`
	GapFrameSlack       float64 `json:"db_gap_tolerance_frames"`
	// Decoded content duration must match the stamped span within this many frames.
	SpanFrameSlack float64 `json:"content_span_tolerance_frames"`
	// A previous clip this far below nominal length needs an identified,
	// consecutive capture chain in addition to every other gate and pixel proof.
	ShortClipSlackSeconds float64 `json:"short_clip_slack_seconds"`
	// Boundary endpoints may be less sharp only when their step is within
	// BoundaryBaselineFactor times the local baseline.
	BoundarySharpRatio float64 `json:"boundary_sharp_ratio"`
	// The relaxed endpoint route uses a tighter local-step limit.
	BoundaryBaselineFactor float64 `json:"boundary_baseline_factor"`
	// Endpoint distances within this MAD of a shallow minimum are tied.
	EndpointSlackMAD float64 `json:"endpoint_slack_mad"`
}

func (p SeamPolicy) windows() []float64 {
	if p.ShortWindowSeconds > 0 && p.ShortWindowSeconds < p.WindowSeconds {
		return []float64{p.ShortWindowSeconds, p.WindowSeconds}
	}
	return []float64{p.WindowSeconds}
}

func DefaultSeamPolicy() SeamPolicy {
	return SeamPolicy{
		ShortWindowSeconds:     4,
		WindowSeconds:          10,
		FrameWidth:             96,
		FrameHeight:            64,
		BlurSigma:              0,
		MotionPixelShare:       0.10,
		MaxTailOffsetFrames:    0,
		MaxHeadOffsetFrames:    10,
		TailSharpRatio:         0.60,
		HeadSharpRatio:         0.70,
		MinSceneMedianMAD:      0.8,
		BoundaryStepFactor:     2.0,
		ClearJumpStepFactor:    4.0,
		StepSlackMAD:           0.05,
		MinFrames:              10,
		GapFrameSlack:          1,
		SpanFrameSlack:         2,
		ShortClipSlackSeconds:  2,
		BoundarySharpRatio:     0.8,
		BoundaryBaselineFactor: 1.2,
		EndpointSlackMAD:       0.05,
	}
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
	if prev.NominalSeconds > 0 && prev.EndUTC.Sub(prev.StartUTC).Seconds() < prev.NominalSeconds-policy.ShortClipSlackSeconds &&
		(prev.CaptureSequence <= 0 || (prev.CaptureAttemptID == "" && prev.CaptureLeaseToken == "")) {
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
	// Short clips need positive continuity at B[0], rather than merely an
	// acceptable nearby head match, and an ordinary local boundary step.
	if prev.NominalSeconds > 0 && prev.EndUTC.Sub(prev.StartUTC).Seconds() < prev.NominalSeconds-policy.ShortClipSlackSeconds {
		baseline := math.Max(match.KeyStepMedianMAD, match.StepP95MAD)
		if match.BoundaryMAD > match.HeadMinMAD+policy.EndpointSlackMAD ||
			match.BoundaryMAD > policy.BoundaryBaselineFactor*baseline+policy.StepSlackMAD {
			d.Reason = "short_clip_continuity_unproven"
			return d
		}
	}
	d.Decision, d.Reason = DecisionJoin, "continuous"
	return d
}

func round6(v float64) float64 { return math.Round(v*1e6) / 1e6 }
