// Local read-only reproduction harness. No Store, DB, network or publishing callpoint.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	c "github.com/daydemir/stoarama/backend/internal/collation"
	"io"
	"os"
	"os/exec"
	"strconv"
	"time"
)

type Input struct {
	Prev       c.Clip                 `json:"prev"`
	Next       c.Clip                 `json:"next"`
	PrevPath   string                 `json:"prev_path"`
	NextPath   string                 `json:"next_path"`
	ActualSpan bool                   `json:"actual_span"`
	BadSources []string               `json:"bad_sources"`
	HumanPairs []c.ResearchSourcePair `json:"human_pairs"`
}

func identity(path string, clip c.Clip) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	h := sha256.New()
	n, e := io.Copy(h, f)
	if e != nil {
		return e
	}
	if n != clip.SizeBytes || hex.EncodeToString(h.Sum(nil)) != clip.SHA256 {
		return fmt.Errorf("source identity mismatch")
	}
	return nil
}
func source(ctx context.Context, tools c.Tools, path string, clip c.Clip) (c.SourceContext, map[string]any, error) {
	summary := map[string]any{"decode_validation": "ffprobe_err_detect_explode_EOF"}
	cmd := exec.CommandContext(ctx, tools.FFprobe, "-v", "error", "-threads", "1", "-err_detect", "explode", "-select_streams", "v:0", "-show_entries", "frame=best_effort_timestamp_time,pkt_duration_time,duration_time", "-of", "json", path)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	raw, e := cmd.Output()
	summary["strict_decode_ok"] = e == nil && stderr.Len() == 0
	if e != nil || stderr.Len() != 0 {
		return c.SourceContext{}, summary, fmt.Errorf("decoded PTS probe failed")
	}
	var d struct {
		Frames []struct {
			PTS         string `json:"best_effort_timestamp_time"`
			Duration    string `json:"duration_time"`
			PktDuration string `json:"pkt_duration_time"`
		}
	}
	if e = json.Unmarshal(raw, &d); e != nil {
		return c.SourceContext{}, summary, e
	}
	pts := make([]float64, len(d.Frames))
	last := 0.
	for i, f := range d.Frames {
		pts[i], e = strconv.ParseFloat(f.PTS, 64)
		if e != nil {
			return c.SourceContext{}, summary, e
		}
		dur := f.Duration
		if dur == "" {
			dur = f.PktDuration
		}
		if i == len(d.Frames)-1 {
			last, e = strconv.ParseFloat(dur, 64)
			if e != nil {
				return c.SourceContext{}, summary, e
			}
		}
	}
	h := sha256.Sum256(raw)
	proof := hex.EncodeToString(h[:])
	summary["decoded_frames"] = len(pts)
	summary["decoded_pts_proof_sha256"] = proof
	summary["last_frame_duration"] = last
	summary["registered_capture_span"] = clip.EndUTC.Sub(clip.StartUTC).Seconds()
	if len(pts) > 0 {
		summary["decoded_extent"] = pts[len(pts)-1] + last - pts[0]
	}
	return c.SourceContext{SourceSHA: clip.SHA256, SourceBytes: clip.SizeBytes, OwnerPinnedProofSHA: proof, VerifiedProofSHA: proof, StrictDecodePassed: true, FramePTS: pts, LastFrameDuration: last, RegisteredCaptureSpan: clip.EndUTC.Sub(clip.StartUTC).Seconds()}, summary, nil
}
func run() map[string]any {
	var in Input
	if e := json.NewDecoder(os.Stdin).Decode(&in); e != nil {
		return map[string]any{"error": e.Error()}
	}
	out := map[string]any{"local_test_only": true, "harness_revision": 4}
	remaining := 180 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), remaining)
	defer cancel()
	tools := c.Tools{FFmpeg: os.Getenv("V85R_FFMPEG"), FFprobe: os.Getenv("V85R_FFPROBE")}
	if e := identity(in.PrevPath, in.Prev); e != nil {
		out["error"] = e.Error()
		return out
	}
	if e := identity(in.NextPath, in.Next); e != nil {
		out["error"] = e.Error()
		return out
	}
	pa, e := c.ProbeFile(ctx, tools, in.PrevPath)
	if e != nil {
		out["error"] = "prev probe failed"
		return out
	}
	pb, e := c.ProbeFile(ctx, tools, in.NextPath)
	if e != nil {
		out["error"] = "next probe failed"
		return out
	}
	out["media"] = []c.ClipMedia{pa.Media, pb.Media}
	var ac, bc c.SourceContext
	var ae, be error
	if in.ActualSpan {
		var as, bs map[string]any
		ac, as, ae = source(ctx, tools, in.PrevPath, in.Prev)
		bc, bs, be = source(ctx, tools, in.NextPath, in.Next)
		out["source_audits"] = []map[string]any{as, bs}
		if ae != nil || be != nil {
			out["source_integrity_error"] = fmt.Sprintf("prev=%v next=%v", ae, be)
		}
	} else {
		out["source_validation_scope"] = "strict 20-second windows; no full-EOF certification requested"
	}

	policy := c.SeamPolicyFor(c.PolicyV3)
	policy.ShortWindowSeconds = 0
	policy.WindowSeconds = 20
	env := c.Env{Tools: tools, Policy: policy, CPU: make(chan struct{}, 1), Now: time.Now}
	a := c.LocalClip{Clip: in.Prev, Path: in.PrevPath, Probe: pa}
	b := c.LocalClip{Clip: in.Next, Path: in.NextPath, Probe: pb}
	out["original_metadata_gate"] = c.MetadataGate(policy, a.Clip, b.Clip, a.Probe.Media, b.Probe.Media)
	veto := c.ResearchPairVeto{SourceSHA: map[string]bool{}, OrderedPairs: map[c.ResearchSourcePair]bool{}}
	for _, sh := range in.BadSources {
		veto.SourceSHA[sh] = true
	}
	for _, hp := range in.HumanPairs {
		veto.OrderedPairs[hp] = true
	}
	if in.ActualSpan {
		if ae != nil || be != nil {
			out["error"] = "cannot apply actual-span: strict decode/PTS failed"
			return out
		}
		aa, bb, actx, bctx, e := c.PrepareResearchPair(a, b, ac, bc, veto)
		if e != nil {
			out["error"] = e.Error()
			return out
		}
		a, b = aa, bb
		out["actual_span_contexts"] = []c.Context{actx, bctx}
	}
	d := c.DecideLocalSeam(ctx, env, a, b)
	out["diagnostic_window20"] = d
	// Expose the raw 20-second measurements even if an earlier metadata gate abstains.
	if d.Match != nil {
		out["measured_window20"] = d.Match
	} else if pa.Video != nil && pb.Video != nil {
		tail, e := c.ExtractWindow(ctx, tools, policy, in.PrevPath, true)
		if e == nil {
			head, e := c.ExtractWindow(ctx, tools, policy, in.NextPath, false)
			if e == nil {
				m := c.EvaluateFrames(policy, tail, head, pa.Video.WindowKeys(len(tail), true), pb.Video.WindowKeys(len(head), false), d.FrameSeconds)
				m.WindowSeconds = 20
				out["measured_window20"] = m
			} else {
				out["measurement_error"] = "head extraction failed"
			}
		} else {
			out["measurement_error"] = "tail extraction failed"
		}
	}
	return out
}
func main() { json.NewEncoder(os.Stdout).Encode(run()) }
