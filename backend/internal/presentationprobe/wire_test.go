package presentationprobe

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"testing"
)

func encodeReportFixture(t *testing.T, report Report) []byte {
	t.Helper()
	digest, err := reportSHA256(report)
	if err != nil {
		t.Fatal(err)
	}
	invocation := hex.EncodeToString(report.InvocationID[:])
	records := []wireRecord{{Type: "header", InvocationID: invocation, ConfigSHA256: report.ConfigSHA256, InputSHA256: report.InputSHA256, InputSize: report.InputSize, ManifestSHA256: report.ManifestSHA256, ExecutableSHA256: report.ExecutableSHA256, DerivedTool: &report.DerivedTool, DerivedToolSHA256: report.DerivedToolSHA256, DerivedNativeSHA256: report.DerivedNativeSHA256}}
	for i := range report.Axes {
		axis := report.Axes[i]
		records = append(records, wireRecord{Type: "axis", InvocationID: invocation, ConfigSHA256: report.ConfigSHA256, Axis: &axis})
	}
	records = append(records, wireRecord{Type: "final", InvocationID: invocation, ConfigSHA256: report.ConfigSHA256, ReportSHA256: digest})
	var out bytes.Buffer
	encoder := json.NewEncoder(&out)
	encoder.SetEscapeHTML(false)
	for _, record := range records {
		if err := encoder.Encode(record); err != nil {
			t.Fatal(err)
		}
	}
	return out.Bytes()
}

func TestStrictNDJSONRoundTripAndAtomicDiscard(t *testing.T) {
	config, report := validReport(t)
	raw := encodeReportFixture(t, report)
	got, err := ParseReportNDJSON(bytes.NewReader(raw), int64(len(raw)), config)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Axes) != 6 {
		t.Fatal("axis facts lost")
	}
	cases := [][]byte{append(append([]byte(nil), raw...), []byte("{}\n")...), raw[:len(raw)-2], bytes.Replace(raw, []byte(`"type":"axis"`), []byte(`"type":"unknown"`), 1), bytes.Replace(raw, []byte(`"config_sha256"`), []byte(`"extra"`), 1)}
	for i, bad := range cases {
		if partial, err := ParseReportNDJSON(bytes.NewReader(bad), int64(len(bad))+10, config); err == nil || len(partial.Axes) != 0 {
			t.Fatalf("bad report %d retained evidence: err=%v axes=%d", i, err, len(partial.Axes))
		}
	}
}
func TestStrictNDJSONLiveByteLimit(t *testing.T) {
	config, report := validReport(t)
	raw := encodeReportFixture(t, report)
	if _, err := ParseReportNDJSON(bytes.NewReader(raw), int64(len(raw)-1), config); err == nil {
		t.Fatal("over-limit report accepted")
	}
}

func TestStrictNDJSONRejectsAggregateForgeriesAndDiscardsFacts(t *testing.T) {
	tests := map[string]func(*Report){
		"wrong stream": func(report *Report) { *report.Axes[2].StreamIndex = 8 },
		"wrong native": func(report *Report) { report.DerivedNativeSHA256 = SHA256([]byte("wrong native")) },
		"mixed audio": func(report *Report) {
			report.Axes[4] = AxisReport{Axis: AxisRawAudio, Status: AxisNotPresent, Reason: ReasonAudioNotPresent}
		},
		"raw demux ordinal":    func(report *Report) { report.Axes[1].RawExtents[1].Ordinal = 44 },
		"duplicate video rank": func(report *Report) { report.Axes[2].VideoFrames[1].Side = "leading" },
		"duplicate audio rank": func(report *Report) { report.Axes[5].AudioBlocks[1].Side = "leading" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			config, report := validReport(t)
			mutate(&report)
			raw := encodeReportFixture(t, report)
			partial, err := ParseReportNDJSON(bytes.NewReader(raw), int64(len(raw))+1, config)
			if err == nil || len(partial.Axes) != 0 {
				t.Fatalf("forgery retained facts: err=%v axes=%d", err, len(partial.Axes))
			}
		})
	}
}

func TestStrictNDJSONRejectsFieldMatrixAndEndpointForgeriesWithoutFacts(t *testing.T) {
	mutations := map[string]func(*Report){
		"unknown sample summary": func(report *Report) {
			report.Axes[5] = AxisReport{Axis: AxisAudioSample, Status: AxisUnknown, Reason: ReasonPresentationAmbiguous, SampleRate: ptr[int64](48000)}
		},
		"not-present trim summary": func(report *Report) {
			report.Axes[5] = AxisReport{Axis: AxisAudioSample, Status: AxisNotPresent, Reason: ReasonAudioNotPresent, SkipSamples: ptr[int64](0)}
		},
		"video audio-only summary": func(report *Report) { report.Axes[2].ChannelLayout = "stereo" },
		"packet summary endpoint":  func(report *Report) { *report.Axes[0].EndTimestamp++ },
		"raw summary endpoint":     func(report *Report) { *report.Axes[1].FirstTimestamp++ },
		"video summary endpoint":   func(report *Report) { *report.Axes[2].EndTimestamp++ },
		"audio summary endpoint":   func(report *Report) { *report.Axes[5].EndTimestamp++ },
		"packet canonical ordinal": func(report *Report) { report.Axes[0].Packets[0].Ordinal++ },
		"raw canonical ordinal":    func(report *Report) { report.Axes[1].RawExtents[0].Ordinal++ },
		"video canonical ordinal":  func(report *Report) { report.Axes[2].VideoFrames[0].Ordinal++ },
		"audio canonical ordinal":  func(report *Report) { report.Axes[5].AudioBlocks[0].Ordinal++ },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			config, report := validReport(t)
			mutate(&report)
			raw := encodeReportFixture(t, report)
			partial, err := ParseReportNDJSON(bytes.NewReader(raw), int64(len(raw))+1, config)
			if err == nil || len(partial.Axes) != 0 {
				t.Fatalf("forgery retained facts: err=%v axes=%d", err, len(partial.Axes))
			}
		})
	}
}

func mutateRecordLine(t *testing.T, raw []byte, lineIndex int, key string, value any) []byte {
	t.Helper()
	lines := bytes.Split(bytes.TrimSuffix(raw, []byte{'\n'}), []byte{'\n'})
	var object map[string]any
	if err := json.Unmarshal(lines[lineIndex], &object); err != nil {
		t.Fatal(err)
	}
	object[key] = value
	line, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	lines[lineIndex] = line
	return append(bytes.Join(lines, []byte{'\n'}), '\n')
}

func TestStrictNDJSONRejectsKnownFieldsOnWrongRecordTypeWithoutFacts(t *testing.T) {
	illegal := map[int]map[string]any{
		0: {"axis": map[string]any{}, "report_sha256": SHA256([]byte("report"))},
		1: {"input_sha256": SHA256([]byte("input")), "input_size": float64(1), "manifest_sha256": SHA256([]byte("manifest")), "executable_sha256": SHA256([]byte("exe")), "derived_tool": map[string]any{}, "derived_tool_sha256": SHA256([]byte("tool")), "derived_native_sha256": SHA256([]byte("native")), "report_sha256": SHA256([]byte("report"))},
		7: {"input_sha256": SHA256([]byte("input")), "input_size": float64(1), "manifest_sha256": SHA256([]byte("manifest")), "executable_sha256": SHA256([]byte("exe")), "derived_tool": map[string]any{}, "derived_tool_sha256": SHA256([]byte("tool")), "derived_native_sha256": SHA256([]byte("native")), "axis": map[string]any{}},
	}
	for line, fields := range illegal {
		for key, value := range fields {
			t.Run(string(rune('0'+line))+"-"+key, func(t *testing.T) {
				config, report := validReport(t)
				raw := mutateRecordLine(t, encodeReportFixture(t, report), line, key, value)
				partial, err := ParseReportNDJSON(bytes.NewReader(raw), int64(len(raw))+1, config)
				if err == nil || len(partial.Axes) != 0 {
					t.Fatalf("wrong-type field retained facts: err=%v axes=%d", err, len(partial.Axes))
				}
			})
		}
	}
}

func duplicateTopLevelKey(t *testing.T, line []byte, key, encodedValue string) []byte {
	t.Helper()
	if len(line) == 0 || line[0] != '{' {
		t.Fatal("fixture line is not an object")
	}
	return append([]byte(`{"`+key+`":`+encodedValue+`,`), line[1:]...)
}

func TestStrictNDJSONRejectsDuplicateKeysAtEveryLevelWithoutFacts(t *testing.T) {
	config, report := validReport(t)
	base := encodeReportFixture(t, report)
	configBytes, err := EncodeConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	configSHA := SHA256(configBytes)
	mutations := map[string]func([][]byte){
		"duplicate type":    func(lines [][]byte) { lines[0] = duplicateTopLevelKey(t, lines[0], "type", `"header"`) },
		"duplicate binding": func(lines [][]byte) { lines[1] = duplicateTopLevelKey(t, lines[1], "config_sha256", `"`+configSHA+`"`) },
		"duplicate nested axis": func(lines [][]byte) {
			lines[1] = bytes.Replace(lines[1], []byte(`"axis":{"axis":"demux_video"`), []byte(`"axis":{"axis":"demux_video","axis":"demux_video"`), 1)
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			lines := bytes.Split(bytes.TrimSuffix(base, []byte{'\n'}), []byte{'\n'})
			mutate(lines)
			raw := append(bytes.Join(lines, []byte{'\n'}), '\n')
			partial, err := ParseReportNDJSON(bytes.NewReader(raw), int64(len(raw))+1, config)
			if err == nil || len(partial.Axes) != 0 {
				t.Fatalf("duplicate key retained facts: err=%v axes=%d", err, len(partial.Axes))
			}
		})
	}
}

func TestStrictNDJSONRejectsUnsafePacketFlagsWithoutFacts(t *testing.T) {
	for _, flags := range []string{"", "K", "K\n", "K flag", "Ké", "key+key", "corrupt+key", string(bytes.Repeat([]byte{'K'}, 65))} {
		config, report := validReport(t)
		report.Axes[0].Packets[0].Flags = flags
		raw := encodeReportFixture(t, report)
		partial, err := ParseReportNDJSON(bytes.NewReader(raw), int64(len(raw))+1, config)
		if err == nil || len(partial.Axes) != 0 {
			t.Fatalf("unsafe flags retained facts: flags=%q err=%v axes=%d", flags, err, len(partial.Axes))
		}
	}
}

func TestStrictNDJSONRejectsNOPTSAndNegativeTimebaseWithoutFacts(t *testing.T) {
	mutations := map[string]func(*Report){
		"summary first":          func(report *Report) { *report.Axes[0].FirstTimestamp = AVNoPTSValue },
		"summary end":            func(report *Report) { *report.Axes[0].EndTimestamp = AVNoPTSValue },
		"packet PTS":             func(report *Report) { report.Axes[0].Packets[0].PTS = AVNoPTSValue },
		"packet DTS":             func(report *Report) { report.Axes[0].Packets[0].DTS = AVNoPTSValue },
		"video PTS":              func(report *Report) { report.Axes[2].VideoFrames[0].PTS = AVNoPTSValue },
		"audio PTS":              func(report *Report) { report.Axes[5].AudioBlocks[0].PTS = AVNoPTSValue },
		"negative edge timebase": func(report *Report) { report.Axes[0].Packets[0].TimeBase.Num = -1 },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			config, report := validReport(t)
			mutate(&report)
			raw := encodeReportFixture(t, report)
			partial, err := ParseReportNDJSON(bytes.NewReader(raw), int64(len(raw))+1, config)
			if err == nil || len(partial.Axes) != 0 {
				t.Fatalf("invalid timestamp retained facts: err=%v axes=%d", err, len(partial.Axes))
			}
		})
	}
}
