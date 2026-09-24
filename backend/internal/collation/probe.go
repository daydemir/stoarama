package collation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
)

type Tools struct {
	FFmpeg  string
	FFprobe string
}

func ToolsFromEnv() Tools {
	t := Tools{FFmpeg: os.Getenv("FFMPEG_BIN"), FFprobe: os.Getenv("FFPROBE_BIN")}
	if t.FFmpeg == "" {
		t.FFmpeg = "ffmpeg"
	}
	if t.FFprobe == "" {
		t.FFprobe = "ffprobe"
	}
	return t
}

// Version returns the first line of `ffmpeg -version` for the manifest.
func (t Tools) Version(ctx context.Context) (string, error) {
	out, err := exec.CommandContext(ctx, t.FFmpeg, "-version").Output()
	if err != nil {
		return "", fmt.Errorf("ffmpeg -version: %w", err)
	}
	line, _, _ := strings.Cut(string(out), "\n")
	return strings.TrimSpace(line), nil
}

// Packet is one demuxed packet; times are exact rationals in seconds.
type Packet struct {
	PTS, DTS, Dur *big.Rat
	Key           bool
	Hash          string
}

// StreamPackets holds one track's packets in demux order.
type StreamPackets struct {
	Kind     string
	TimeBase *big.Rat
	Packets  []Packet
}

// Probe is the full packet-level view of one MP4: enough to prove a joined
// output carries exactly the source packets, without decoding.
type Probe struct {
	Media  ClipMedia
	Video  *StreamPackets
	Audio  *StreamPackets
	format string
}

type ffStream struct {
	Index         int    `json:"index"`
	CodecType     string `json:"codec_type"`
	CodecName     string `json:"codec_name"`
	Profile       string `json:"profile"`
	Level         int    `json:"level"`
	Width         int    `json:"width"`
	Height        int    `json:"height"`
	PixFmt        string `json:"pix_fmt"`
	SampleRate    string `json:"sample_rate"`
	Channels      int    `json:"channels"`
	ChannelLayout string `json:"channel_layout"`
	TimeBase      string `json:"time_base"`
	ExtradataHash string `json:"extradata_hash"`
}

type ffPacket struct {
	StreamIndex int    `json:"stream_index"`
	PTS         *int64 `json:"pts"`
	DTS         *int64 `json:"dts"`
	Duration    *int64 `json:"duration"`
	Flags       string `json:"flags"`
	DataHash    string `json:"data_hash"`
}

// ProbeFile reads stream parameters and every packet (with a SHA-256 of each
// payload). A file without a moov atom or with unusable timing is reported as
// unplayable rather than as an error: the caller quarantines it.
func ProbeFile(ctx context.Context, tools Tools, path string) (Probe, error) {
	cmd := exec.CommandContext(ctx, tools.FFprobe, "-v", "error", "-show_data_hash", "sha256",
		"-show_entries", "stream=index,codec_type,codec_name,profile,level,width,height,pix_fmt,sample_rate,channels,channel_layout,time_base,extradata_hash:packet=stream_index,pts,dts,duration,flags,data_hash",
		"-of", "json", path)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return Probe{}, ctx.Err()
		}
		return unplayable("probe_failed: " + trimStderr(stderr.String())), nil
	}
	var doc struct {
		Streams []ffStream `json:"streams"`
		Packets []ffPacket `json:"packets"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &doc); err != nil {
		return unplayable("probe_json_invalid"), nil
	}
	if strings.TrimSpace(stderr.String()) != "" {
		return unplayable("probe_reported_errors: " + trimStderr(stderr.String())), nil
	}
	p := Probe{}
	byIndex := map[int]*StreamPackets{}
	sigParts := []string{}
	for _, s := range doc.Streams {
		tb, ok := new(big.Rat).SetString(s.TimeBase)
		if !ok || tb.Sign() <= 0 {
			return unplayable("invalid_time_base"), nil
		}
		switch {
		case s.CodecType == "video" && p.Video == nil:
			p.Video = &StreamPackets{Kind: "video", TimeBase: tb}
			byIndex[s.Index] = p.Video
		case s.CodecType == "audio" && p.Audio == nil:
			p.Audio = &StreamPackets{Kind: "audio", TimeBase: tb}
			byIndex[s.Index] = p.Audio
		default:
			continue
		}
		sigParts = append(sigParts, strings.Join([]string{s.CodecType, s.CodecName, s.Profile, strconv.Itoa(s.Level),
			strconv.Itoa(s.Width), strconv.Itoa(s.Height), s.PixFmt, s.SampleRate, strconv.Itoa(s.Channels),
			s.ChannelLayout, s.ExtradataHash}, "|"))
	}
	if p.Video == nil {
		return unplayable("no_video_stream"), nil
	}
	for _, pk := range doc.Packets {
		sp := byIndex[pk.StreamIndex]
		if sp == nil {
			continue
		}
		if pk.PTS == nil || pk.DTS == nil || pk.Duration == nil || pk.DataHash == "" {
			return unplayable("packet_timing_missing"), nil
		}
		sp.Packets = append(sp.Packets, Packet{
			PTS: new(big.Rat).Mul(big.NewRat(*pk.PTS, 1), sp.TimeBase), DTS: new(big.Rat).Mul(big.NewRat(*pk.DTS, 1), sp.TimeBase),
			Dur: new(big.Rat).Mul(big.NewRat(*pk.Duration, 1), sp.TimeBase), Key: strings.Contains(pk.Flags, "K"), Hash: pk.DataHash})
	}
	if len(p.Video.Packets) < 2 {
		return unplayable("too_few_video_packets"), nil
	}
	if !p.Video.Packets[0].Key {
		return unplayable("first_video_packet_not_keyframe"), nil
	}
	sum := sha256.Sum256([]byte(strings.Join(sigParts, "\n")))
	content := p.Video.Duration()
	contentF, _ := content.Float64()
	p.Media = ClipMedia{Playable: true, CodecSignature: hex.EncodeToString(sum[:8]), HasAudio: p.Audio != nil,
		VideoPackets: len(p.Video.Packets), ContentSeconds: round6(contentF), FrameSeconds: round6(p.Video.medianDur())}
	return p, nil
}

func unplayable(reason string) Probe {
	return Probe{Media: ClipMedia{Playable: false, FailReason: reason}}
}

// Duration is the exact sum of packet durations.
func (s *StreamPackets) Duration() *big.Rat {
	total := new(big.Rat)
	for _, pk := range s.Packets {
		total.Add(total, pk.Dur)
	}
	return total
}

func (s *StreamPackets) medianDur() float64 {
	v := make([]float64, 0, len(s.Packets))
	for _, pk := range s.Packets {
		f, _ := pk.Dur.Float64()
		v = append(v, f)
	}
	sort.Float64s(v)
	return v[len(v)/2]
}

// KeyFlagsByPTS returns the keyframe flag of every packet in presentation order,
// which is the order decoded frames come out in.
func (s *StreamPackets) KeyFlagsByPTS() []bool {
	order := make([]int, len(s.Packets))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(i, j int) bool { return s.Packets[order[i]].PTS.Cmp(s.Packets[order[j]].PTS) < 0 })
	out := make([]bool, len(order))
	for i, idx := range order {
		out[i] = s.Packets[idx].Key
	}
	return out
}

// WindowKeys returns the keyframe flags of the first (head) or last (tail) n
// presented frames, or nil when the packet count cannot cover them.
func (s *StreamPackets) WindowKeys(n int, tail bool) []bool {
	flags := s.KeyFlagsByPTS()
	if n > len(flags) {
		return nil
	}
	if tail {
		return flags[len(flags)-n:]
	}
	return flags[:n]
}
