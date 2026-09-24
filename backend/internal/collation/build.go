package collation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// LocalClip is one downloaded, probed source ready to be built into a part.
type LocalClip struct {
	Clip  Clip
	Path  string
	Probe Probe
}

// Verification records how an output was proven to be exactly its sources.
type Verification struct {
	VideoPackets        int      `json:"video_packets"`
	AudioPackets        int      `json:"audio_packets"`
	PayloadChainMatches bool     `json:"payload_chain_matches"`
	VideoTimingMatches  bool     `json:"video_timing_matches"`
	MaxTimingErrorMicro int64    `json:"max_timing_error_us"`
	EdgeDecodeOK        bool     `json:"edge_decode_ok"`
	OutputSeconds       float64  `json:"output_seconds"`
	Notes               []string `json:"notes,omitempty"`
}

// Built is one verified joined part on local disk.
type Built struct {
	Path         string
	SizeBytes    int64
	SHA256       string
	Verification Verification
}

// concatOffsets returns each clip's start offset in the output and the
// per-entry concat durations. The concat demuxer places file i at offset_i
// minus its start time (earliest pts over all tracks). Each file's duration is
// its full extent, max(video end, audio end) - start, from exact packet
// timestamps (VFR-safe), so no track of the next clip can overlap this one;
// offsets are rounded to microseconds against the running total so rounding
// never accumulates.
func concatOffsets(clips []LocalClip) (offsets []*big.Rat, durationsMicro []int64) {
	total := new(big.Rat)
	micros := make([]int64, len(clips)+1)
	for i, c := range clips {
		micros[i] = roundRatMicro(total)
		total.Add(total, new(big.Rat).Sub(c.Probe.endTime(), c.Probe.startTime()))
	}
	micros[len(clips)] = roundRatMicro(total)
	for i := range clips {
		offsets = append(offsets, new(big.Rat).SetFrac64(micros[i], 1_000_000))
		durationsMicro = append(durationsMicro, micros[i+1]-micros[i])
	}
	return offsets, durationsMicro
}

func roundRatMicro(r *big.Rat) int64 {
	scaled := new(big.Rat).Mul(r, big.NewRat(1_000_000, 1))
	num, den := scaled.Num(), scaled.Denom()
	q, m := new(big.Int).QuoRem(num, den, new(big.Int))
	if new(big.Int).Mul(m, big.NewInt(2)).Cmp(den) >= 0 {
		q.Add(q, big.NewInt(1))
	}
	return q.Int64()
}

// BuildPart stream-copies clips into one MP4 and verifies it. The concat demuxer
// runs with auto_convert disabled and exact per-file durations so VFR sources
// keep their exact packet timing.
func BuildPart(ctx context.Context, tools Tools, clips []LocalClip, outPath string) (Built, error) {
	if len(clips) == 0 {
		return Built{}, fmt.Errorf("empty part")
	}
	_, durations := concatOffsets(clips)
	var list strings.Builder
	for i, c := range clips {
		if strings.ContainsAny(c.Path, "\r\n'") {
			return Built{}, fmt.Errorf("unsafe source path")
		}
		fmt.Fprintf(&list, "file '%s'\nduration %dus\n", c.Path, durations[i])
	}
	listPath := outPath + ".concat.txt"
	if err := os.WriteFile(listPath, []byte(list.String()), 0o600); err != nil {
		return Built{}, err
	}
	defer os.Remove(listPath)
	_ = os.Remove(outPath)
	cmd := exec.CommandContext(ctx, tools.FFmpeg, "-nostdin", "-v", "error", "-f", "concat", "-safe", "0", "-auto_convert", "0",
		"-i", listPath, "-copyts", "-map", "0:v:0", "-map", "0:a?", "-c", "copy", "-movflags", "+faststart", outPath)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return Built{}, fmt.Errorf("concat: %w: %s", err, trimStderr(stderr.String()))
	}
	v, err := VerifyPart(ctx, tools, clips, outPath)
	if err != nil {
		return Built{}, err
	}
	size, sha, err := fileIdentity(outPath)
	if err != nil {
		return Built{}, err
	}
	return Built{Path: outPath, SizeBytes: size, SHA256: sha, Verification: v}, nil
}

// VerifyPart proves the output is exactly the sources' packets in order:
//  1. per-track payload hash chain and packet counts are identical;
//  2. every output packet (video and audio) sits at its source timing shifted
//     by the clip's concat offset (within 1 ms), and dts never goes back;
//  3. the output's first and last seconds decode strictly.
func VerifyPart(ctx context.Context, tools Tools, clips []LocalClip, outPath string) (Verification, error) {
	out, err := ProbeFile(ctx, tools, outPath)
	if err != nil {
		return Verification{}, err
	}
	if !out.Media.Playable {
		return Verification{}, fmt.Errorf("output unplayable: %s", out.Media.FailReason)
	}
	v := Verification{VideoPackets: len(out.Video.Packets)}
	if out.Audio != nil {
		v.AudioPackets = len(out.Audio.Packets)
	}
	for _, kind := range []string{"video", "audio"} {
		want, got := sha256.New(), sha256.New()
		n := 0
		for _, c := range clips {
			sp := c.Probe.track(kind)
			if sp == nil {
				continue
			}
			for _, pk := range sp.Packets {
				io.WriteString(want, pk.Hash+"\n")
				n++
			}
		}
		if sp := out.track(kind); sp != nil {
			if len(sp.Packets) != n {
				return v, fmt.Errorf("%s packet count differs: out %d src %d", kind, len(sp.Packets), n)
			}
			for _, pk := range sp.Packets {
				io.WriteString(got, pk.Hash+"\n")
			}
		} else if n != 0 {
			return v, fmt.Errorf("%s track missing from output", kind)
		}
		if !bytes.Equal(want.Sum(nil), got.Sum(nil)) {
			return v, fmt.Errorf("%s payload chain differs", kind)
		}
	}
	v.PayloadChainMatches = true

	// The concat demuxer places each file at its offset minus the file's start
	// time; every output packet of every track must sit exactly there.
	offsets, _ := concatOffsets(clips)
	outBase := out.startTime()
	tol := big.NewRat(1, 1000)
	maxErr := new(big.Rat)
	for _, kind := range []string{"video", "audio"} {
		outTrack := out.track(kind)
		if outTrack == nil {
			continue
		}
		i := 0
		var prevDTS *big.Rat
		for ci, c := range clips {
			src := c.Probe.track(kind)
			if src == nil {
				continue
			}
			srcBase := c.Probe.startTime()
			for _, pk := range src.Packets {
				o := outTrack.Packets[i]
				i++
				for _, pair := range [][2]*big.Rat{{pk.PTS, o.PTS}, {pk.DTS, o.DTS}} {
					want := new(big.Rat).Add(offsets[ci], new(big.Rat).Sub(pair[0], srcBase))
					got := new(big.Rat).Sub(pair[1], outBase)
					diff := new(big.Rat).Abs(new(big.Rat).Sub(want, got))
					if diff.Cmp(maxErr) > 0 {
						maxErr = diff
					}
					if diff.Cmp(tol) > 0 {
						return v, fmt.Errorf("%s timing differs at clip %d", kind, c.Clip.ClipID)
					}
				}
				if prevDTS != nil && (o.DTS.Cmp(prevDTS) < 0 || (kind == "video" && o.DTS.Cmp(prevDTS) == 0)) {
					return v, fmt.Errorf("output %s dts not increasing", kind)
				}
				prevDTS = o.DTS
			}
		}
	}
	v.VideoTimingMatches = true
	v.MaxTimingErrorMicro = roundRatMicro(maxErr)
	total, _ := new(big.Rat).Sub(out.endTime(), outBase).Float64()
	v.OutputSeconds = round6(total)

	// Every source window around a joined seam was already strictly decoded by
	// the frame match (B[0] is a keyframe, so decoding the joined stream from
	// the seam equals decoding B's head). Here: the output opens and its first
	// and last seconds decode strictly.
	if err := strictDecode(ctx, tools, outPath, "-t", "2"); err != nil {
		return v, fmt.Errorf("head decode: %w", err)
	}
	if err := strictDecode(ctx, tools, outPath, "-sseof", "-2"); err != nil {
		return v, fmt.Errorf("tail decode: %w", err)
	}
	v.EdgeDecodeOK = true
	return v, nil
}

func strictDecode(ctx context.Context, tools Tools, path string, pre ...string) error {
	args := append([]string{"-nostdin", "-v", "error", "-xerror", "-err_detect", "explode"}, pre...)
	args = append(args, "-i", path, "-map", "0:v:0", "-f", "null", "-")
	cmd := exec.CommandContext(ctx, tools.FFmpeg, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%w: %s", err, trimStderr(stderr.String()))
	}
	if s := decoderComplaints(stderr.String()); s != "" {
		return fmt.Errorf("decoder reported: %s", trimStderr(s))
	}
	return nil
}

// decoderComplaints drops the null muxer's notes about repeated source
// timestamps (a timing property, verified from packets, not a decode error).
func decoderComplaints(stderr string) string {
	var kept []string
	for _, line := range strings.Split(stderr, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.Contains(line, "non monotonically increasing dts to muxer") || strings.HasPrefix(line, "Last message repeated") {
			continue
		}
		kept = append(kept, line)
	}
	return strings.Join(kept, "\n")
}

// endTime is the latest presentation end (pts + duration) over the file's tracks.
func (p Probe) endTime() *big.Rat {
	var end *big.Rat
	for _, sp := range []*StreamPackets{p.Video, p.Audio} {
		if sp == nil {
			continue
		}
		for _, pk := range sp.Packets {
			e := new(big.Rat).Add(pk.PTS, pk.Dur)
			if end == nil || e.Cmp(end) > 0 {
				end = e
			}
		}
	}
	return end
}

// startTime is the earliest presentation time over the file's tracks.
func (p Probe) startTime() *big.Rat {
	var start *big.Rat
	for _, sp := range []*StreamPackets{p.Video, p.Audio} {
		if sp == nil {
			continue
		}
		for _, pk := range sp.Packets {
			if start == nil || pk.PTS.Cmp(start) < 0 {
				start = pk.PTS
			}
		}
	}
	return start
}

func (p Probe) track(kind string) *StreamPackets {
	if kind == "video" {
		return p.Video
	}
	return p.Audio
}

func fileIdentity(path string) (int64, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return 0, "", err
	}
	return n, hex.EncodeToString(h.Sum(nil)), nil
}

func partPath(dir string, part int) string {
	return filepath.Join(dir, fmt.Sprintf("part-%02d.mp4", part))
}
