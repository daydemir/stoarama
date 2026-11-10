package collation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// ReplayRepair is a standalone, opt-in canary proof. It is deliberately not a
// SeamDecision: a human Split on an untrimmed replay never approves this repair.
// Production/default seam policies and hour manifests do not consume it.
type ReplayRepair struct {
	Version                          string  `json:"version"`
	PrevClipID                       int64   `json:"prev_clip_id"`
	NextClipID                       int64   `json:"next_clip_id"`
	PrevSHA256                       string  `json:"prev_sha256"`
	NextSHA256                       string  `json:"next_sha256"`
	RetainedPrevPackets              int     `json:"retained_prev_video_packets"`
	DroppedPrevPackets               int     `json:"dropped_prev_duplicate_video_packets"`
	NextPackets                      int     `json:"retained_next_video_packets"`
	CutPrevDTS                       string  `json:"cut_prev_dts_seconds_rational"`
	CutPrevMicro                     int64   `json:"concat_outpoint_us"`
	DuplicateChainSHA256             string  `json:"duplicate_payload_chain_sha256"`
	MaxDuplicateTimingErrorMicro     int64   `json:"duplicate_timing_error_us"`
	TerminalDuplicatePaddingMicro    int64   `json:"redundant_terminal_padding_us"`
	DroppedSeconds                   float64 `json:"dropped_prev_seconds"`
	Audio                            string  `json:"audio"`
	RetainedCrossSourceReplayChecked bool    `json:"retained_cross_source_replay_checked"`
}

const replayRepairMinPackets = 16

// PlanReplayRepair proves every packet of A's suffix is exactly B's prefix,
// with a unique keyframe anchor, complete GOP boundaries, coherent source times
// and new footage following B's prefix. It trims A (not B), so B's original
// decode entry and unique motion are retained. At the new join, the last
// retained A packet followed by B[0] is exactly the packet transition already
// present in original A before its matched suffix; it is not inferred from
// similar pixels or stamped adjacency. Audio, B-frame reordering,
// ambiguous/static matches, interior-only replay and wholly duplicated clips
// fail closed in this first video-only candidate.
func PlanReplayRepair(policy SeamPolicy, a, b LocalClip) (ReplayRepair, error) {
	var proof ReplayRepair
	reject := func(why string) (ReplayRepair, error) { return proof, fmt.Errorf("overlap repair: %s", why) }
	if policy.HRD == nil {
		return reject("v3 opt-in required")
	}
	if a.Clip.RecordingID <= 0 || a.Clip.ClipID <= 0 || b.Clip.ClipID <= 0 || a.Clip.ClipID == b.Clip.ClipID {
		return reject("invalid source identities")
	}
	for _, c := range []LocalClip{a, b} {
		if c.Clip.Purged || c.Clip.KnownBroken || !c.Clip.EndUTC.After(c.Clip.StartUTC) {
			return reject("invalid or excluded source metadata")
		}
		if math.IsNaN(c.Probe.Media.FrameSeconds) || math.IsInf(c.Probe.Media.FrameSeconds, 0) || c.Probe.Media.FrameSeconds <= 0 || c.Probe.Media.FrameSeconds > 2 {
			return reject("unknown frame duration")
		}
	}
	if reason := HardMetadataGate(policy, a.Clip, b.Clip, a.Probe.Media, b.Probe.Media); reason != "" {
		return reject(reason)
	}
	if a.Clip.CaptureSequence <= 0 || b.Clip.CaptureSequence <= a.Clip.CaptureSequence || (a.Clip.CaptureAttemptID == "" && a.Clip.CaptureLeaseToken == "") {
		return reject("independent capture identity required")
	}
	if a.Probe.Audio != nil || b.Probe.Audio != nil || a.Probe.Media.HasAudio || b.Probe.Media.HasAudio {
		return reject("audio correspondence not proven")
	}
	av, bv := a.Probe.Video, b.Probe.Video
	if av == nil || bv == nil || len(av.Packets) < 2*replayRepairMinPackets || len(bv.Packets) < 2*replayRepairMinPackets {
		return reject("insufficient two-sided context")
	}
	tol := big.NewRat(1, 1000)
	for _, track := range []*StreamPackets{av, bv} {
		if !track.Packets[0].Key {
			return reject("source does not open on keyframe")
		}
		for i, p := range track.Packets {
			if p.PTS == nil || p.DTS == nil || p.Dur == nil || p.Dur.Sign() <= 0 || p.PTS.Cmp(p.DTS) != 0 {
				return reject("unproven packet timing or reordering")
			}
			raw, err := hex.DecodeString(strings.TrimPrefix(p.Hash, "SHA256:"))
			if !strings.HasPrefix(p.Hash, "SHA256:") || err != nil || len(raw) != 32 {
				return reject("cryptographic packet identities required")
			}
			if i > 0 {
				prev := track.Packets[i-1]
				if p.DTS.Cmp(prev.DTS) <= 0 || new(big.Rat).Abs(new(big.Rat).Sub(p.PTS, new(big.Rat).Add(prev.PTS, prev.Dur))).Cmp(tol) > 0 {
					return reject("source packet timeline discontinuous")
				}
			}
		}
	}
	start := -1
	for i, p := range av.Packets {
		if p.Hash == bv.Packets[0].Hash {
			if start >= 0 {
				return reject("ambiguous keyframe anchor")
			}
			start = i
		}
	}
	n := len(av.Packets) - start
	if start < replayRepairMinPackets || n < replayRepairMinPackets || len(bv.Packets)-n < replayRepairMinPackets {
		return reject("no complete suffix/prefix overlap with unique footage on both sides")
	}
	if !av.Packets[start].Key || !bv.Packets[n].Key {
		return reject("overlap is not a complete GOP")
	}
	if replayRepairCrossSourceReplay(av.Packets, bv.Packets, n) {
		return reject("retained next footage replays previous source outside the removable overlap")
	}
	shift := new(big.Rat).Sub(av.Packets[start].PTS, bv.Packets[0].PTS)
	chain := sha256.New()
	distinct := map[string]bool{}
	maxError := new(big.Rat)
	padding := new(big.Rat)
	for i := 0; i < n; i++ {
		x, y := av.Packets[start+i], bv.Packets[i]
		if x.Hash != y.Hash || x.Key != y.Key {
			return reject("entire suffix/prefix correspondence failed")
		}
		distinct[x.Hash] = true
		fmt.Fprintln(chain, x.Hash)
		for _, pair := range [][2]*big.Rat{{x.PTS, y.PTS}, {x.DTS, y.DTS}} {
			diff := new(big.Rat).Abs(new(big.Rat).Sub(new(big.Rat).Sub(pair[0], pair[1]), shift))
			if diff.Cmp(tol) > 0 {
				return reject("duplicate timestamps disagree")
			}
			if diff.Cmp(maxError) > 0 {
				maxError = diff
			}
		}
		delta := new(big.Rat).Sub(x.Dur, y.Dur)
		if new(big.Rat).Abs(new(big.Rat).Set(delta)).Cmp(tol) > 0 {
			// A's redundant final packet can have muxer terminal padding. It is
			// discarded, never applied to unique retained video; B is kept in full.
			frame, _ := new(big.Rat).SetString(fmt.Sprint(a.Probe.Media.FrameSeconds))
			if i != n-1 || delta.Sign() < 0 || frame == nil || delta.Cmp(frame) > 0 {
				return reject("duplicate packet durations disagree")
			}
			padding = delta
		}
	}
	if len(distinct) < 8 {
		return reject("insufficient distinct source content")
	}
	cut := av.Packets[start].DTS
	scaled := new(big.Rat).Mul(cut, big.NewRat(1_000_000, 1))
	wholeMicro := new(big.Int).Quo(scaled.Num(), scaled.Denom())
	if !wholeMicro.IsInt64() {
		return reject("concat cut overflows integer microseconds")
	}
	micro := wholeMicro.Int64()
	if cut.Sign() < 0 || new(big.Rat).SetFrac64(micro, 1_000_000).Cmp(av.Packets[start-1].DTS) <= 0 {
		return reject("unrepresentable safe concat cut")
	}
	dropped, _ := new(big.Rat).Sub(a.Probe.endTime(), cut).Float64()
	proof = ReplayRepair{Version: "replay-repair-v2", PrevClipID: a.Clip.ClipID, NextClipID: b.Clip.ClipID, PrevSHA256: a.Clip.SHA256, NextSHA256: b.Clip.SHA256, RetainedPrevPackets: start, DroppedPrevPackets: n, NextPackets: len(bv.Packets), CutPrevDTS: cut.RatString(), CutPrevMicro: micro, DuplicateChainSHA256: hex.EncodeToString(chain.Sum(nil)), MaxDuplicateTimingErrorMicro: roundRatMicro(maxError), TerminalDuplicatePaddingMicro: roundRatMicro(padding), DroppedSeconds: round6(dropped), Audio: "absent in both sources; audio-bearing repairs fail closed", RetainedCrossSourceReplayChecked: true}
	return proof, nil
}

// replayRepairCrossSourceReplay rejects a second cross-source rewind after the
// removable prefix. Local PTS can be restamped, so timing alone cannot detect
// it. An encoded keyframe repeated from A is ambiguous; matching runs of
// distinct payloads also catch replay beginning inside a GOP. Repetition
// wholly internal to B is preserved as original source content, not silently
// repaired or interpreted as a new seam introduced by this operation.
func replayRepairCrossSourceReplay(prev, next []Packet, overlap int) bool {
	anchors := make(map[string]bool, len(prev)+len(next))
	for _, p := range prev {
		anchors[p.Hash] = true
	}
	for i, p := range next {
		if i >= overlap && p.Key && anchors[p.Hash] {
			return true
		}
	}
	windows := make(map[string]bool, len(prev)+len(next))
	signature := func(packets []Packet) (string, bool) {
		hashes := make(map[string]bool)
		chain := sha256.New()
		for _, p := range packets {
			hashes[p.Hash] = true
			fmt.Fprintln(chain, p.Hash, p.Key)
		}
		return hex.EncodeToString(chain.Sum(nil)), len(hashes) >= 8
	}
	for i := 0; i+replayRepairMinPackets <= len(prev); i++ {
		key, distinct := signature(prev[i : i+replayRepairMinPackets])
		if distinct {
			windows[key] = true
		}
	}
	for i := 0; i+replayRepairMinPackets <= len(next); i++ {
		key, distinct := signature(next[i : i+replayRepairMinPackets])
		if !distinct {
			continue
		}
		if i >= overlap && windows[key] {
			return true
		}
	}
	return false
}

// BuildReplayRepair re-probes and hashes both originals, creates only a new
// derivative, and verifies exactly A's retained prefix plus all of B. There is
// no transcode, packet substitution, original mutation or production publish.
func BuildReplayRepair(ctx context.Context, tools Tools, policy SeamPolicy, a, b LocalClip, outPath string) (Built, ReplayRepair, error) {
	var proof ReplayRepair
	reject := func(err error) (Built, ReplayRepair, error) { return Built{}, proof, err }
	outAbs, err := filepath.Abs(outPath)
	if err != nil {
		return reject(err)
	}
	if _, err = os.Lstat(outAbs); err == nil || !os.IsNotExist(err) {
		return reject(fmt.Errorf("repair output must be a new file"))
	}
	pair := []LocalClip{a, b}
	for i := range pair {
		srcAbs, e := filepath.Abs(pair[i].Path)
		if e != nil || srcAbs == outAbs {
			return reject(fmt.Errorf("unsafe repair output path"))
		}
		size, hash, e := fileIdentity(srcAbs)
		if e != nil {
			return reject(e)
		}
		if pair[i].Clip.SHA256 == "" || pair[i].Clip.SHA256 != hash || pair[i].Clip.SizeBytes != size {
			return reject(fmt.Errorf("source %d identity differs", pair[i].Clip.ClipID))
		}
		p, e := probeFileWithHash(ctx, tools, srcAbs, "sha256")
		if e != nil {
			return reject(e)
		}
		pair[i].Path, pair[i].Probe = srcAbs, p
	}
	proof, err = PlanReplayRepair(policy, pair[0], pair[1])
	if err != nil {
		return reject(err)
	}
	expected := append([]LocalClip(nil), pair...)
	trimmed := *pair[0].Probe.Video
	trimmed.Packets = append([]Packet(nil), trimmed.Packets[:proof.RetainedPrevPackets]...)
	expected[0].Probe.Video = &trimmed
	_, durations := concatOffsets(expected)
	for _, c := range pair {
		if strings.ContainsAny(c.Path, "\r\n'") {
			return reject(fmt.Errorf("unsafe source path"))
		}
	}
	list, err := os.CreateTemp(filepath.Dir(outAbs), ".replay-repair-*.concat")
	if err != nil {
		return reject(err)
	}
	listPath := list.Name()
	defer os.Remove(listPath)
	_, err = fmt.Fprintf(list, "file '%s'\noutpoint %dus\nduration %dus\nfile '%s'\nduration %dus\n", pair[0].Path, proof.CutPrevMicro, durations[0], pair[1].Path, durations[1])
	closeErr := list.Close()
	if err != nil {
		return reject(err)
	}
	if closeErr != nil {
		return reject(closeErr)
	}
	cmd := exec.CommandContext(ctx, tools.FFmpeg, "-nostdin", "-v", "error", "-f", "concat", "-safe", "0", "-auto_convert", "0", "-i", listPath, "-copyts", "-map", "0:v:0", "-c", "copy", "-movflags", "+faststart", outAbs)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err = cmd.Run(); err != nil {
		return reject(fmt.Errorf("repair concat: %w: %s", err, trimStderr(stderr.String())))
	}
	verification, err := verifyPartWithHash(ctx, tools, expected, outAbs, "sha256")
	if err != nil {
		return reject(err)
	}
	// Unlike ordinary joins, this new operation strictly decodes the whole
	// derivative, including the trimmed interior seam, with one decoder thread.
	if err = strictDecode(ctx, tools, outAbs, "-threads", "1"); err != nil {
		return reject(fmt.Errorf("full repaired output decode: %w", err))
	}
	size, hash, err := fileIdentity(outAbs)
	if err != nil {
		return reject(err)
	}
	for _, c := range pair {
		_, again, e := fileIdentity(c.Path)
		if e != nil || again != c.Clip.SHA256 {
			return reject(fmt.Errorf("original source changed during repair"))
		}
	}
	verification.Notes = append(verification.Notes, "replay-repair-v2: exactly retained A prefix plus all B; retained cross-source replay checks; full output strict decode; revision-specific human approval required")
	return Built{Path: outAbs, SizeBytes: size, SHA256: hash, Verification: verification}, proof, nil
}
