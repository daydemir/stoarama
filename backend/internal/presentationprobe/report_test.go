package presentationprobe

import "testing"

func ptr[T any](v T) *T { return &v }
func toolFixture() SemanticTool {
	return SemanticTool{FFmpeg: "8.1.2", FFprobe: "8.1.2", AVFormat: "62.3.100", AVCodec: "62.6.100", AVUtil: "60.8.100", BuildSHA: SHA256([]byte("build")), Demuxer: "mov,mp4,m4a,3gp,3g2,mj2", VideoDecoder: "h264", AudioDecoder: "aac", Parser: ParserSchema}
}
func completeAxis(axis AxisName, index int32) AxisReport {
	count := int64(1)
	first := int64(0)
	end := int64(0)
	ts0 := int64(0)
	ts1 := int64(1)
	tb := Rational{1, 30}
	base := AxisReport{Axis: axis, Status: AxisComplete, StreamIndex: &index, UnitCount: &count, CanonicalSHA256: SHA256([]byte(axis)), TimeBase: &tb, FirstOrdinal: &first, FirstTimestamp: &ts0, EndOrdinal: &end, EndTimestamp: &ts1}
	switch axis {
	case AxisDemuxVideo, AxisDemuxAudio:
		base.Packets = []PacketEdge{{Side: "leading", Rank: 1, Ordinal: 0, PTS: 0, DTS: 0, Duration: 1, TimeBase: tb, Flags: "key", SideDataSHA256: SHA256(nil), PayloadSHA256: SHA256([]byte("p"))}, {Side: "trailing", Rank: 1, Ordinal: 0, PTS: 0, DTS: 0, Duration: 1, TimeBase: tb, Flags: "key", SideDataSHA256: SHA256(nil), PayloadSHA256: SHA256([]byte("p"))}}
	case AxisRawVideo, AxisRawAudio:
		base.RawExtents = []RawExtent{{Side: "leading", Rank: 1, Ordinal: 0, Position: 0, Size: 1, SHA256: SHA256([]byte("p"))}, {Side: "trailing", Rank: 1, Ordinal: 0, Position: 0, Size: 1, SHA256: SHA256([]byte("p"))}}
	case AxisVideoPresentation:
		base.VideoFrames = []VideoEdge{{Side: "leading", Rank: 1, Ordinal: 0, PTS: 0, Duration: 1, TimeBase: tb, PixelSHA256: SHA256([]byte("pixel-a"))}, {Side: "trailing", Rank: 1, Ordinal: 0, PTS: 0, Duration: 1, TimeBase: tb, PixelSHA256: SHA256([]byte("pixel-b"))}}
	case AxisAudioSample:
		rate := int64(48000)
		channels := int32(2)
		base.TimeBase = &Rational{1, 48000}
		base.SampleRate = &rate
		base.ChannelCount = &channels
		base.ChannelLayout = "stereo"
		base.NormalizationProfile = "decoder-output-effective-samples-v2.0"
		base.AudioBlocks = []AudioEdge{{Side: "leading", Rank: 1, Ordinal: 0, PTS: 0, Samples: 1, TimeBase: *base.TimeBase, SampleRate: rate, Channels: channels, Layout: "stereo", PCMSHA256: SHA256([]byte("pcm-a"))}, {Side: "trailing", Rank: 1, Ordinal: 0, PTS: 0, Samples: 1, TimeBase: *base.TimeBase, SampleRate: rate, Channels: channels, Layout: "stereo", PCMSHA256: SHA256([]byte("pcm-b"))}}
	}
	return base
}
func validReport(t *testing.T) (Config, Report) {
	t.Helper()
	c := validConfig()
	tool := toolFixture()
	sha, err := SemanticToolIdentity(tool)
	if err != nil {
		t.Fatal(err)
	}
	c.ExpectedToolSHA256 = sha
	raw, err := EncodeConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	axes := make([]AxisReport, 0, 6)
	for i, a := range AxisOrder {
		idx := int32(0)
		tb := Rational{c.Video.TimeBaseNum, c.Video.TimeBaseDen}
		if i >= 3 {
			idx = 1
			tb = Rational{c.Audio.TimeBaseNum, c.Audio.TimeBaseDen}
		}
		axis := completeAxis(a, idx)
		axis.TimeBase = &tb
		for j := range axis.Packets {
			axis.Packets[j].TimeBase = tb
		}
		for j := range axis.VideoFrames {
			axis.VideoFrames[j].TimeBase = tb
		}
		for j := range axis.AudioBlocks {
			axis.AudioBlocks[j].TimeBase = tb
		}
		axes = append(axes, axis)
	}
	return c, Report{InvocationID: c.InvocationID, ConfigSHA256: SHA256(raw), InputSHA256: c.InputSHA256, InputSize: c.InputSize, ManifestSHA256: SHA256([]byte("manifest")), ExecutableSHA256: SHA256([]byte("exe")), DerivedTool: tool, DerivedToolSHA256: sha, DerivedNativeSHA256: c.ExpectedNativeSHA256, Axes: axes}
}
func TestReportValidatesExactC1Shape(t *testing.T) {
	c, r := validReport(t)
	if err := r.Validate(c); err != nil {
		t.Fatal(err)
	}
	r.Axes[1].Status = AxisUnknown
	r.Axes[1].Reason = ReasonRawExtentUnavailable
	if err := r.Validate(c); err == nil {
		t.Fatal("unknown axis carrying complete evidence accepted")
	}
}
func TestAtomicFailureLoweringHasNoChildren(t *testing.T) {
	for _, reason := range []AxisReason{ReasonProbeTimeout, ReasonProbeResourceLimit, ReasonToolIncompatible, ReasonProbeUnavailable} {
		for _, axis := range AxisOrder {
			got := AxisReport{Axis: axis, Status: AxisUnknown, Reason: reason}
			if err := got.Validate(validConfig()); err != nil {
				t.Fatalf("%s/%s: %v", axis, reason, err)
			}
		}
	}
}

func TestReportRejectsCrossAxisAndConfigForgeries(t *testing.T) {
	tests := map[string]func(*Config, *Report){
		"wrong stream":           func(_ *Config, report *Report) { *report.Axes[1].StreamIndex = 9 },
		"wrong native signature": func(_ *Config, report *Report) { report.DerivedNativeSHA256 = SHA256([]byte("other native")) },
		"raw ordinal range":      func(_ *Config, report *Report) { *report.Axes[1].FirstOrdinal = 1; *report.Axes[1].EndOrdinal = 1 },
		"raw edge ordinal":       func(_ *Config, report *Report) { report.Axes[1].RawExtents[0].Ordinal = 7 },
		"presentation without demux": func(_ *Config, report *Report) {
			report.Axes[0] = AxisReport{Axis: AxisDemuxVideo, Status: AxisUnknown, Reason: ReasonPresentationAmbiguous}
		},
		"duplicate video edge rank": func(_ *Config, report *Report) { report.Axes[2].VideoFrames[1].Side = "leading" },
		"duplicate audio edge rank": func(_ *Config, report *Report) { report.Axes[5].AudioBlocks[1].Side = "leading" },
		"configured audio not present": func(_ *Config, report *Report) {
			for i := 3; i < 6; i++ {
				report.Axes[i] = AxisReport{Axis: AxisOrder[i], Status: AxisNotPresent, Reason: ReasonAudioNotPresent}
			}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			config, report := validReport(t)
			mutate(&config, &report)
			if err := report.Validate(config); err == nil {
				t.Fatal("forged report accepted")
			}
		})
	}
}

func TestAudioFreeConfigRequiresExactNotPresentTriplet(t *testing.T) {
	config, report := validReport(t)
	config.Audio = nil
	raw, err := EncodeConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	report.ConfigSHA256 = SHA256(raw)
	if err := report.Validate(config); err == nil {
		t.Fatal("audio facts accepted for audio-free config")
	}
	for i := 3; i < 6; i++ {
		report.Axes[i] = AxisReport{Axis: AxisOrder[i], Status: AxisNotPresent, Reason: ReasonAudioNotPresent}
	}
	if err := report.Validate(config); err != nil {
		t.Fatal(err)
	}
}

func TestNoncompleteAndNonAudioAxesRejectEveryAudioSummaryFieldClass(t *testing.T) {
	mutations := map[string]func(*AxisReport){
		"sample rate":      func(axis *AxisReport) { axis.SampleRate = ptr[int64](48000) },
		"channel count":    func(axis *AxisReport) { axis.ChannelCount = ptr[int32](2) },
		"channel layout":   func(axis *AxisReport) { axis.ChannelLayout = "stereo" },
		"normalization":    func(axis *AxisReport) { axis.NormalizationProfile = "decoder-output-effective-samples-v2.0" },
		"edit SHA":         func(axis *AxisReport) { axis.EditListSHA256 = SHA256([]byte("edit")) },
		"edit kind":        func(axis *AxisReport) { axis.EditListKind = "identity" },
		"skip samples":     func(axis *AxisReport) { axis.SkipSamples = ptr[int64](0) },
		"discard padding":  func(axis *AxisReport) { axis.DiscardPadding = ptr[int64](0) },
		"codec delay":      func(axis *AxisReport) { axis.CodecDelay = ptr[int64](0) },
		"initial padding":  func(axis *AxisReport) { axis.InitialPadding = ptr[int64](0) },
		"trailing padding": func(axis *AxisReport) { axis.TrailingPadding = ptr[int64](0) },
	}
	for name, mutate := range mutations {
		t.Run("unknown "+name, func(t *testing.T) {
			axis := AxisReport{Axis: AxisAudioSample, Status: AxisUnknown, Reason: ReasonPresentationAmbiguous}
			mutate(&axis)
			if err := axis.Validate(validConfig()); err == nil {
				t.Fatal("UNKNOWN axis accepted audio summary field")
			}
		})
		t.Run("not present "+name, func(t *testing.T) {
			axis := AxisReport{Axis: AxisAudioSample, Status: AxisNotPresent, Reason: ReasonAudioNotPresent}
			mutate(&axis)
			if err := axis.Validate(validConfig()); err == nil {
				t.Fatal("not_present axis accepted audio summary field")
			}
		})
		t.Run("video complete "+name, func(t *testing.T) {
			axis := completeAxis(AxisVideoPresentation, 0)
			mutate(&axis)
			if err := axis.Validate(validConfig()); err == nil {
				t.Fatal("complete non-audio axis accepted audio summary field")
			}
		})
	}
}

func TestCompleteAxisSummariesAreBoundToCanonicalEdges(t *testing.T) {
	mutations := map[string]func(*Report){
		"packet ordinal":         func(report *Report) { report.Axes[0].Packets[0].Ordinal++ },
		"raw ordinal":            func(report *Report) { report.Axes[1].RawExtents[0].Ordinal++ },
		"video ordinal":          func(report *Report) { report.Axes[2].VideoFrames[0].Ordinal++ },
		"audio ordinal":          func(report *Report) { report.Axes[5].AudioBlocks[0].Ordinal++ },
		"packet first timestamp": func(report *Report) { *report.Axes[0].FirstTimestamp++ },
		"packet end timestamp":   func(report *Report) { *report.Axes[0].EndTimestamp++ },
		"raw timestamp":          func(report *Report) { *report.Axes[1].EndTimestamp++ },
		"video first timestamp":  func(report *Report) { *report.Axes[2].FirstTimestamp++ },
		"video end timestamp":    func(report *Report) { *report.Axes[2].EndTimestamp++ },
		"audio first timestamp":  func(report *Report) { *report.Axes[5].FirstTimestamp++ },
		"audio end timestamp":    func(report *Report) { *report.Axes[5].EndTimestamp++ },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			config, report := validReport(t)
			mutate(&report)
			if err := report.Validate(config); err == nil {
				t.Fatal("forged summary/edge endpoint accepted")
			}
		})
	}
}

func TestPacketFlagsAreBoundedSafeASCII(t *testing.T) {
	for _, flags := range []string{"", "K", "K\n", "K flag", "Ké", "key+key", "corrupt+key", string(make([]byte, 65))} {
		config, report := validReport(t)
		report.Axes[0].Packets[0].Flags = flags
		if err := report.Validate(config); err == nil {
			t.Fatalf("unsafe packet flags accepted: %q", flags)
		}
	}
}

func TestTimebasesAndAuthoritativeTimestampsRejectNOPTS(t *testing.T) {
	configMutations := map[string]func(*Config){
		"negative video timebase": func(config *Config) { config.Video.TimeBaseNum = -1 },
		"negative audio timebase": func(config *Config) { config.Audio.TimeBaseNum = -1 },
	}
	for name, mutate := range configMutations {
		t.Run(name, func(t *testing.T) {
			config := validConfig()
			mutate(&config)
			if _, err := EncodeConfig(config); err == nil {
				t.Fatal("negative timebase accepted")
			}
		})
	}
	reportMutations := map[string]func(*Report){
		"summary first": func(report *Report) { *report.Axes[2].FirstTimestamp = AVNoPTSValue },
		"summary end":   func(report *Report) { *report.Axes[2].EndTimestamp = AVNoPTSValue },
		"packet PTS":    func(report *Report) { report.Axes[0].Packets[0].PTS = AVNoPTSValue },
		"packet DTS":    func(report *Report) { report.Axes[0].Packets[0].DTS = AVNoPTSValue },
		"video PTS":     func(report *Report) { report.Axes[2].VideoFrames[0].PTS = AVNoPTSValue },
		"audio PTS":     func(report *Report) { report.Axes[5].AudioBlocks[0].PTS = AVNoPTSValue },
	}
	for name, mutate := range reportMutations {
		t.Run(name, func(t *testing.T) {
			config, report := validReport(t)
			mutate(&report)
			if err := report.Validate(config); err == nil {
				t.Fatal("AV_NOPTS_VALUE accepted")
			}
		})
	}
}
