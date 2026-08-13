package presentationprobe

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
)

type Rational struct {
	Num int64 `json:"num"`
	Den int64 `json:"den"`
}

func (r Rational) Validate() error {
	if r.Den <= 0 || r.Num <= 0 {
		return errors.New("invalid rational")
	}
	a := new(big.Int).Abs(big.NewInt(r.Num))
	b := big.NewInt(r.Den)
	if new(big.Int).GCD(nil, nil, a, b).Cmp(big.NewInt(1)) != 0 {
		return errors.New("rational is not reduced")
	}
	return nil
}

type PacketEdge struct {
	Side           string   `json:"side"`
	Rank           int32    `json:"rank"`
	Ordinal        int64    `json:"ordinal"`
	PTS            int64    `json:"pts"`
	DTS            int64    `json:"dts"`
	Duration       int64    `json:"duration"`
	TimeBase       Rational `json:"time_base"`
	Flags          string   `json:"flags"`
	SideDataSHA256 string   `json:"side_data_sha256"`
	PayloadSHA256  string   `json:"payload_sha256"`
}
type RawExtent struct {
	Side     string `json:"side"`
	Rank     int32  `json:"rank"`
	Ordinal  int64  `json:"ordinal"`
	Position int64  `json:"position"`
	Size     int64  `json:"size"`
	SHA256   string `json:"sha256"`
}
type VideoEdge struct {
	Side        string   `json:"side"`
	Rank        int32    `json:"rank"`
	Ordinal     int64    `json:"ordinal"`
	PTS         int64    `json:"pts"`
	Duration    int64    `json:"duration"`
	TimeBase    Rational `json:"time_base"`
	PixelSHA256 string   `json:"pixel_sha256,omitempty"`
}
type AudioEdge struct {
	Side       string   `json:"side"`
	Rank       int32    `json:"rank"`
	Ordinal    int64    `json:"ordinal"`
	PTS        int64    `json:"pts"`
	Samples    int64    `json:"samples"`
	TimeBase   Rational `json:"time_base"`
	SampleRate int64    `json:"sample_rate"`
	Channels   int32    `json:"channels"`
	Layout     string   `json:"layout"`
	PCMSHA256  string   `json:"pcm_sha256,omitempty"`
}

type AxisReport struct {
	Axis                 AxisName     `json:"axis"`
	Status               AxisStatus   `json:"status"`
	Reason               AxisReason   `json:"reason,omitempty"`
	StreamIndex          *int32       `json:"stream_index,omitempty"`
	UnitCount            *int64       `json:"unit_count,omitempty"`
	CanonicalSHA256      string       `json:"canonical_sha256,omitempty"`
	TimeBase             *Rational    `json:"time_base,omitempty"`
	FirstOrdinal         *int64       `json:"first_ordinal,omitempty"`
	FirstTimestamp       *int64       `json:"first_timestamp,omitempty"`
	EndOrdinal           *int64       `json:"end_ordinal,omitempty"`
	EndTimestamp         *int64       `json:"end_timestamp,omitempty"`
	NonmonotonicCount    int64        `json:"nonmonotonic_count"`
	DuplicateCount       int64        `json:"duplicate_count"`
	HoleCount            int64        `json:"hole_count"`
	OverlapCount         int64        `json:"overlap_count"`
	SampleRate           *int64       `json:"sample_rate,omitempty"`
	ChannelCount         *int32       `json:"channel_count,omitempty"`
	ChannelLayout        string       `json:"channel_layout,omitempty"`
	NormalizationProfile string       `json:"normalization_profile,omitempty"`
	EditListSHA256       string       `json:"edit_list_sha256,omitempty"`
	EditListKind         string       `json:"edit_list_kind,omitempty"`
	SkipSamples          *int64       `json:"skip_samples,omitempty"`
	DiscardPadding       *int64       `json:"discard_padding,omitempty"`
	CodecDelay           *int64       `json:"codec_delay,omitempty"`
	InitialPadding       *int64       `json:"initial_padding,omitempty"`
	TrailingPadding      *int64       `json:"trailing_padding,omitempty"`
	Packets              []PacketEdge `json:"packets,omitempty"`
	RawExtents           []RawExtent  `json:"raw_extents,omitempty"`
	VideoFrames          []VideoEdge  `json:"video_frames,omitempty"`
	AudioBlocks          []AudioEdge  `json:"audio_blocks,omitempty"`
}

type SemanticTool struct{ FFmpeg, FFprobe, AVFormat, AVCodec, AVUtil, BuildSHA, Demuxer, VideoDecoder, AudioDecoder, Parser string }

type Report struct {
	InvocationID        [32]byte     `json:"-"`
	ConfigSHA256        string       `json:"config_sha256"`
	InputSHA256         string       `json:"input_sha256"`
	InputSize           int64        `json:"input_size"`
	ManifestSHA256      string       `json:"manifest_sha256"`
	ExecutableSHA256    string       `json:"executable_sha256"`
	DerivedTool         SemanticTool `json:"derived_tool"`
	DerivedToolSHA256   string       `json:"derived_tool_sha256"`
	DerivedNativeSHA256 string       `json:"derived_native_sha256"`
	Axes                []AxisReport `json:"axes"`
}

func SemanticToolIdentity(t SemanticTool) (string, error) {
	values := []string{t.FFmpeg, t.FFprobe, t.AVFormat, t.AVCodec, t.AVUtil, t.BuildSHA, t.Demuxer, t.VideoDecoder, t.AudioDecoder, t.Parser}
	for i, v := range values {
		if err := ValidateASCII(v, fmt.Sprintf("tool[%d]", i), 256); err != nil {
			return "", err
		}
	}
	var b bytes.Buffer
	writeField(&b, "presentation-semantic-tool-v2")
	for _, v := range values {
		writeField(&b, v)
	}
	return SHA256(b.Bytes()), nil
}

func (r Report) Validate(config Config) error {
	configBytes, err := EncodeConfig(config)
	if err != nil {
		return err
	}
	if r.InvocationID != config.InvocationID || r.ConfigSHA256 != SHA256(configBytes) || r.InputSHA256 != config.InputSHA256 || r.InputSize != config.InputSize {
		return errors.New("report is not bound to config/input")
	}
	if !ValidSHA256(r.ManifestSHA256) || !ValidSHA256(r.ExecutableSHA256) {
		return errors.New("report artifact SHA invalid")
	}
	toolSHA, err := SemanticToolIdentity(r.DerivedTool)
	if err != nil {
		return err
	}
	if toolSHA != r.DerivedToolSHA256 || toolSHA != config.ExpectedToolSHA256 {
		return errors.New("derived semantic tool mismatch")
	}
	if !ValidSHA256(r.DerivedNativeSHA256) || r.DerivedNativeSHA256 != config.ExpectedNativeSHA256 {
		return errors.New("derived native signature mismatch")
	}
	if len(r.Axes) != len(AxisOrder) {
		return errors.New("report must contain six axes")
	}
	for i, axis := range r.Axes {
		if axis.Axis != AxisOrder[i] {
			return errors.New("axes are not in canonical order")
		}
		if err := axis.Validate(config); err != nil {
			return fmt.Errorf("%s: %w", axis.Axis, err)
		}
	}
	return validateReportAxes(r.Axes, config)
}

func validateReportAxes(axes []AxisReport, config Config) error {
	videoTB := Rational{Num: config.Video.TimeBaseNum, Den: config.Video.TimeBaseDen}
	if err := validateAxisTriplet(axes[0:3], config.Video.Index, videoTB); err != nil {
		return fmt.Errorf("video triplet: %w", err)
	}
	if config.Audio == nil {
		for _, axis := range axes[3:6] {
			if axis.Status != AxisNotPresent || axis.Reason != ReasonAudioNotPresent {
				return errors.New("audio-free config requires three not_present audio axes")
			}
		}
		return nil
	}
	for _, axis := range axes[3:6] {
		if axis.Status == AxisNotPresent {
			return errors.New("configured audio cannot be not_present")
		}
	}
	audioTB := Rational{Num: config.Audio.TimeBaseNum, Den: config.Audio.TimeBaseDen}
	return validateAxisTriplet(axes[3:6], config.Audio.Index, audioTB)
}

func validateAxisTriplet(axes []AxisReport, streamIndex int32, timeBase Rational) error {
	for _, axis := range axes {
		if axis.Status != AxisComplete {
			continue
		}
		if axis.StreamIndex == nil || *axis.StreamIndex != streamIndex || axis.TimeBase == nil || *axis.TimeBase != timeBase {
			return errors.New("complete axis does not match configured stream/timebase")
		}
	}
	demux, raw, presentation := axes[0], axes[1], axes[2]
	if (raw.Status == AxisComplete || presentation.Status == AxisComplete) && demux.Status != AxisComplete {
		return errors.New("raw/presentation completion requires demux completion")
	}
	if raw.Status == AxisComplete {
		if *raw.UnitCount != *demux.UnitCount || *raw.FirstOrdinal != *demux.FirstOrdinal || *raw.EndOrdinal != *demux.EndOrdinal {
			return errors.New("raw/demux unit identity mismatch")
		}
		packets := make(map[string]int64, len(demux.Packets))
		for _, edge := range demux.Packets {
			packets[fmt.Sprintf("%s:%d", edge.Side, edge.Rank)] = edge.Ordinal
		}
		for _, edge := range raw.RawExtents {
			if packets[fmt.Sprintf("%s:%d", edge.Side, edge.Rank)] != edge.Ordinal {
				return errors.New("raw edge ordinal does not match demux edge")
			}
		}
		if *raw.FirstTimestamp != *demux.FirstTimestamp || *raw.EndTimestamp != *demux.EndTimestamp {
			return errors.New("raw summary timing does not match demux summary")
		}
	}
	return nil
}

func (a AxisReport) Validate(config Config) error {
	if err := ValidateAxisReason(a.Axis, a.Status, a.Reason); err != nil {
		return err
	}
	children := len(a.Packets) + len(a.RawExtents) + len(a.VideoFrames) + len(a.AudioBlocks)
	if a.Status != AxisComplete {
		if a.StreamIndex != nil || a.UnitCount != nil || a.CanonicalSHA256 != "" || a.TimeBase != nil || a.FirstOrdinal != nil || a.FirstTimestamp != nil || a.EndOrdinal != nil || a.EndTimestamp != nil || children != 0 || a.NonmonotonicCount != 0 || a.DuplicateCount != 0 || a.HoleCount != 0 || a.OverlapCount != 0 || !audioSummaryFieldsZero(a) {
			return errors.New("noncomplete axis carries authoritative evidence")
		}
		return nil
	}
	if a.StreamIndex == nil || a.UnitCount == nil || *a.UnitCount <= 0 || !ValidSHA256(a.CanonicalSHA256) || a.TimeBase == nil || a.FirstOrdinal == nil || a.FirstTimestamp == nil || a.EndOrdinal == nil || a.EndTimestamp == nil {
		return errors.New("complete axis summary incomplete")
	}
	if uint64(*a.UnitCount) > config.Limits.Units {
		return errors.New("complete axis exceeds configured unit limit")
	}
	if err := a.TimeBase.Validate(); err != nil {
		return err
	}
	if *a.FirstOrdinal < 0 || *a.EndOrdinal < *a.FirstOrdinal || *a.EndOrdinal-*a.FirstOrdinal != *a.UnitCount-1 || *a.FirstTimestamp == AVNoPTSValue || *a.EndTimestamp == AVNoPTSValue {
		return errors.New("axis ordinal range inconsistent")
	}
	if a.NonmonotonicCount != 0 || a.DuplicateCount != 0 || a.HoleCount != 0 || a.OverlapCount != 0 {
		return errors.New("complete axis has defects")
	}
	want := int(*a.UnitCount)
	if want > 4 {
		want = 4
	}
	want *= 2
	switch a.Axis {
	case AxisDemuxVideo, AxisDemuxAudio:
		if !audioSummaryFieldsZero(a) {
			return errors.New("demux axis carries audio-sample summary fields")
		}
		if len(a.Packets) != want || len(a.RawExtents)+len(a.VideoFrames)+len(a.AudioBlocks) != 0 {
			return errors.New("demux edge cardinality invalid")
		}
		return validatePacketEdges(a.Packets, a)
	case AxisRawVideo, AxisRawAudio:
		if !audioSummaryFieldsZero(a) {
			return errors.New("raw axis carries audio-sample summary fields")
		}
		if len(a.RawExtents) != want || len(a.Packets)+len(a.VideoFrames)+len(a.AudioBlocks) != 0 {
			return errors.New("raw extent cardinality invalid")
		}
		return validateRawExtents(a.RawExtents, a, config.InputSize)
	case AxisVideoPresentation:
		if !audioSummaryFieldsZero(a) {
			return errors.New("video axis carries audio-sample summary fields")
		}
		if len(a.VideoFrames) != want || len(a.Packets)+len(a.RawExtents)+len(a.AudioBlocks) != 0 {
			return errors.New("video edge cardinality invalid")
		}
		return validateVideoEdges(a.VideoFrames, a)
	case AxisAudioSample:
		if len(a.AudioBlocks) != want || len(a.Packets)+len(a.RawExtents)+len(a.VideoFrames) != 0 {
			return errors.New("audio edge cardinality invalid")
		}
		return validateAudioEdges(a.AudioBlocks, a)
	default:
		return errors.New("unknown axis")
	}
}

func audioSummaryFieldsZero(a AxisReport) bool {
	return a.SampleRate == nil && a.ChannelCount == nil && a.ChannelLayout == "" && a.NormalizationProfile == "" && a.EditListSHA256 == "" && a.EditListKind == "" && a.SkipSamples == nil && a.DiscardPadding == nil && a.CodecDelay == nil && a.InitialPadding == nil && a.TrailingPadding == nil
}

func validateAudioSummary(a AxisReport) error {
	if a.SampleRate == nil || a.ChannelCount == nil || *a.SampleRate <= 0 || *a.ChannelCount <= 0 || a.NormalizationProfile != "decoder-output-effective-samples-v2.0" {
		return errors.New("invalid audio summary")
	}
	if err := ValidateASCII(a.ChannelLayout, "channel layout", 128); err != nil {
		return err
	}
	if (a.EditListSHA256 == "") != (a.EditListKind == "") {
		return errors.New("audio edit-list identity is partial")
	}
	if a.EditListSHA256 != "" {
		if !ValidSHA256(a.EditListSHA256) || (a.EditListKind != "none" && a.EditListKind != "identity") {
			return errors.New("audio edit-list identity invalid")
		}
	}
	for _, value := range []*int64{a.SkipSamples, a.DiscardPadding, a.CodecDelay, a.InitialPadding, a.TrailingPadding} {
		if value != nil && *value < 0 {
			return errors.New("audio trim metadata is negative")
		}
	}
	return nil
}

func validateSides(side string, rank int32, want int) error {
	if side != "leading" && side != "trailing" {
		return errors.New("invalid edge side")
	}
	if rank < 1 || rank > int32(want) {
		return errors.New("invalid edge rank")
	}
	return nil
}
func validatePacketEdges(edges []PacketEdge, a AxisReport) error {
	want := len(edges) / 2
	seen := map[string]bool{}
	for _, e := range edges {
		if err := validateSides(e.Side, e.Rank, want); err != nil {
			return err
		}
		key := fmt.Sprintf("%s:%d", e.Side, e.Rank)
		if seen[key] {
			return errors.New("duplicate packet edge")
		}
		seen[key] = true
		if e.Duration <= 0 || e.PTS == AVNoPTSValue || e.DTS == AVNoPTSValue || e.TimeBase != *a.TimeBase || !ValidSHA256(e.PayloadSHA256) || !ValidSHA256(e.SideDataSHA256) {
			return errors.New("invalid packet edge")
		}
		if !ValidPacketFlags(e.Flags) {
			return errors.New("packet flags are not canonical")
		}
		if e.Ordinal != canonicalEdgeOrdinal(a, e.Side, e.Rank, want) {
			return errors.New("packet edge ordinal is not canonical")
		}
	}
	first, last := packetEdgeAt(edges, "leading", 1), packetEdgeAt(edges, "trailing", int32(want))
	if first == nil || last == nil {
		return errors.New("packet endpoint edge missing")
	}
	end, ok := checkedAdd(last.PTS, last.Duration)
	if !ok || first.PTS != *a.FirstTimestamp || end != *a.EndTimestamp {
		return errors.New("packet summary endpoints do not match edges")
	}
	return nil
}
func validateRawExtents(edges []RawExtent, a AxisReport, size int64) error {
	want := len(edges) / 2
	seen := map[string]bool{}
	for _, e := range edges {
		if err := validateSides(e.Side, e.Rank, want); err != nil {
			return err
		}
		key := fmt.Sprintf("%s:%d", e.Side, e.Rank)
		if seen[key] {
			return errors.New("duplicate raw edge")
		}
		seen[key] = true
		if e.Position < 0 || e.Size <= 0 || e.Position > size-e.Size || !ValidSHA256(e.SHA256) {
			return errors.New("invalid raw extent")
		}
		if e.Ordinal != canonicalEdgeOrdinal(a, e.Side, e.Rank, want) {
			return errors.New("raw edge ordinal is not canonical")
		}
	}
	return nil
}
func validateVideoEdges(edges []VideoEdge, a AxisReport) error {
	want := len(edges) / 2
	seen := map[string]bool{}
	for _, e := range edges {
		if err := validateSides(e.Side, e.Rank, want); err != nil {
			return err
		}
		if e.Duration <= 0 || e.PTS == AVNoPTSValue || e.TimeBase != *a.TimeBase {
			return errors.New("invalid video edge")
		}
		if e.Ordinal != canonicalEdgeOrdinal(a, e.Side, e.Rank, want) {
			return errors.New("video edge ordinal is not canonical")
		}
		key := fmt.Sprintf("%s:%d", e.Side, e.Rank)
		if seen[key] {
			return errors.New("duplicate video edge")
		}
		seen[key] = true
		if e.PixelSHA256 != "" && !ValidSHA256(e.PixelSHA256) {
			return errors.New("invalid diagnostic pixel SHA")
		}
	}
	first, last := videoEdgeAt(edges, "leading", 1), videoEdgeAt(edges, "trailing", int32(want))
	if first == nil || last == nil {
		return errors.New("video endpoint edge missing")
	}
	end, ok := checkedAdd(last.PTS, last.Duration)
	if !ok || first.PTS != *a.FirstTimestamp || end != *a.EndTimestamp {
		return errors.New("video summary endpoints do not match edges")
	}
	return nil
}
func validateAudioEdges(edges []AudioEdge, a AxisReport) error {
	want := len(edges) / 2
	seen := map[string]bool{}
	if err := validateAudioSummary(a); err != nil {
		return err
	}
	for _, e := range edges {
		if err := validateSides(e.Side, e.Rank, want); err != nil {
			return err
		}
		if e.Samples <= 0 || e.PTS == AVNoPTSValue || e.TimeBase != *a.TimeBase || e.SampleRate != *a.SampleRate || e.Channels != *a.ChannelCount || e.Layout != a.ChannelLayout {
			return errors.New("invalid audio edge")
		}
		if e.Ordinal != canonicalEdgeOrdinal(a, e.Side, e.Rank, want) {
			return errors.New("audio edge ordinal is not canonical")
		}
		key := fmt.Sprintf("%s:%d", e.Side, e.Rank)
		if seen[key] {
			return errors.New("duplicate audio edge")
		}
		seen[key] = true
		if e.PCMSHA256 != "" && !ValidSHA256(e.PCMSHA256) {
			return errors.New("invalid diagnostic PCM SHA")
		}
	}
	first, last := audioEdgeAt(edges, "leading", 1), audioEdgeAt(edges, "trailing", int32(want))
	if first == nil || last == nil {
		return errors.New("audio endpoint edge missing")
	}
	end, ok := audioEffectiveEnd(*last)
	if !ok || first.PTS != *a.FirstTimestamp || end != *a.EndTimestamp {
		return errors.New("audio summary endpoints do not match edges")
	}
	return nil
}

func canonicalEdgeOrdinal(a AxisReport, side string, rank int32, width int) int64 {
	if side == "leading" {
		return *a.FirstOrdinal + int64(rank) - 1
	}
	return *a.EndOrdinal - int64(width) + int64(rank)
}

func checkedAdd(left, right int64) (int64, bool) {
	result := new(big.Int).Add(big.NewInt(left), big.NewInt(right))
	return result.Int64(), result.IsInt64()
}

func packetEdgeAt(edges []PacketEdge, side string, rank int32) *PacketEdge {
	for i := range edges {
		if edges[i].Side == side && edges[i].Rank == rank {
			return &edges[i]
		}
	}
	return nil
}
func videoEdgeAt(edges []VideoEdge, side string, rank int32) *VideoEdge {
	for i := range edges {
		if edges[i].Side == side && edges[i].Rank == rank {
			return &edges[i]
		}
	}
	return nil
}
func audioEdgeAt(edges []AudioEdge, side string, rank int32) *AudioEdge {
	for i := range edges {
		if edges[i].Side == side && edges[i].Rank == rank {
			return &edges[i]
		}
	}
	return nil
}

func audioEffectiveEnd(edge AudioEdge) (int64, bool) {
	numerator := new(big.Int).Mul(big.NewInt(edge.Samples), big.NewInt(edge.TimeBase.Den))
	denominator := new(big.Int).Mul(big.NewInt(edge.SampleRate), big.NewInt(edge.TimeBase.Num))
	if denominator.Sign() <= 0 {
		return 0, false
	}
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(numerator, denominator, remainder)
	if remainder.Sign() != 0 || !quotient.IsInt64() {
		return 0, false
	}
	return checkedAdd(edge.PTS, quotient.Int64())
}

func writeField(b *bytes.Buffer, value string) {
	_ = binary.Write(b, binary.BigEndian, uint32(len(value)))
	_, _ = b.WriteString(value)
}
