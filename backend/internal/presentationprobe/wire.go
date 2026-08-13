package presentationprobe

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

type wireRecord struct {
	Type                string        `json:"type"`
	InvocationID        string        `json:"invocation_id"`
	ConfigSHA256        string        `json:"config_sha256"`
	InputSHA256         string        `json:"input_sha256,omitempty"`
	InputSize           int64         `json:"input_size,omitempty"`
	ManifestSHA256      string        `json:"manifest_sha256,omitempty"`
	ExecutableSHA256    string        `json:"executable_sha256,omitempty"`
	DerivedTool         *SemanticTool `json:"derived_tool,omitempty"`
	DerivedToolSHA256   string        `json:"derived_tool_sha256,omitempty"`
	DerivedNativeSHA256 string        `json:"derived_native_sha256,omitempty"`
	Axis                *AxisReport   `json:"axis,omitempty"`
	ReportSHA256        string        `json:"report_sha256,omitempty"`
}

func ParseReportNDJSON(reader io.Reader, maximum int64, config Config) (Report, error) {
	var report Report
	if maximum <= 0 || maximum > MaximumStdoutBytes {
		return report, errors.New("invalid output bound")
	}
	limited := &hardLimitReader{reader: reader, remaining: maximum}
	scanner := bufio.NewScanner(limited)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	records := make([]wireRecord, 0, 8)
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		if len(line) == 0 {
			return report, errors.New("empty report line")
		}
		if err := rejectDuplicateJSONKeys(line); err != nil {
			return report, err
		}
		var record wireRecord
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&record); err != nil {
			return report, fmt.Errorf("decode report record: %w", err)
		}
		if err := requireJSONEOF(decoder); err != nil {
			return report, err
		}
		if err := validateWireRecordKeys(line, record.Type); err != nil {
			return report, err
		}
		records = append(records, record)
		if len(records) > 8 {
			return report, errors.New("too many report records")
		}
	}
	if err := scanner.Err(); err != nil {
		return report, err
	}
	if limited.exceeded {
		return report, errors.New("report exceeds byte bound")
	}
	if len(records) != 8 || records[0].Type != "header" || records[7].Type != "final" {
		return report, errors.New("report record cardinality/order invalid")
	}
	configBytes, err := EncodeConfig(config)
	if err != nil {
		return report, err
	}
	configSHA := SHA256(configBytes)
	invocation := hex.EncodeToString(config.InvocationID[:])
	header := records[0]
	if header.InvocationID != invocation || header.ConfigSHA256 != configSHA || header.InputSHA256 != config.InputSHA256 || header.InputSize != config.InputSize || !ValidSHA256(header.ManifestSHA256) || !ValidSHA256(header.ExecutableSHA256) || !ValidSHA256(header.DerivedNativeSHA256) || header.DerivedTool == nil {
		return report, errors.New("report header binding invalid")
	}
	toolSHA, err := SemanticToolIdentity(*header.DerivedTool)
	if err != nil || toolSHA != header.DerivedToolSHA256 {
		return report, errors.New("report header tool identity invalid")
	}
	axes := make([]AxisReport, 0, 6)
	for i := 1; i <= 6; i++ {
		record := records[i]
		if record.Type != "axis" || record.InvocationID != invocation || record.ConfigSHA256 != configSHA || record.Axis == nil || record.Axis.Axis != AxisOrder[i-1] {
			return report, errors.New("axis record order/binding invalid")
		}
		axes = append(axes, *record.Axis)
	}
	final := records[7]
	if final.InvocationID != invocation || final.ConfigSHA256 != configSHA || !ValidSHA256(final.ReportSHA256) {
		return report, errors.New("final record binding invalid")
	}
	report = Report{InvocationID: config.InvocationID, ConfigSHA256: configSHA, InputSHA256: header.InputSHA256, InputSize: header.InputSize, ManifestSHA256: header.ManifestSHA256, ExecutableSHA256: header.ExecutableSHA256, DerivedTool: *header.DerivedTool, DerivedToolSHA256: header.DerivedToolSHA256, DerivedNativeSHA256: header.DerivedNativeSHA256, Axes: axes}
	digest, err := reportSHA256(report)
	if err != nil {
		return Report{}, err
	}
	if digest != final.ReportSHA256 {
		return Report{}, errors.New("final report digest mismatch")
	}
	if err := report.Validate(config); err != nil {
		return Report{}, err
	}
	return report, nil
}

func rejectDuplicateJSONKeys(line []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(line))
	if err := scanJSONValue(decoder, 0); err != nil {
		return err
	}
	return requireJSONEOF(decoder)
}

func scanJSONValue(decoder *json.Decoder, depth int) error {
	if depth > 32 {
		return errors.New("JSON nesting exceeds limit")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok || seen[key] {
				return errors.New("JSON object contains duplicate or invalid key")
			}
			seen[key] = true
			if err := scanJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		closeToken, err := decoder.Token()
		if err != nil || closeToken != json.Delim('}') {
			return errors.New("JSON object terminator invalid")
		}
	case '[':
		for decoder.More() {
			if err := scanJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		closeToken, err := decoder.Token()
		if err != nil || closeToken != json.Delim(']') {
			return errors.New("JSON array terminator invalid")
		}
	default:
		return errors.New("unexpected JSON delimiter")
	}
	return nil
}

func validateWireRecordKeys(line []byte, recordType string) error {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(line, &object); err != nil {
		return err
	}
	allowed := map[string]bool{}
	switch recordType {
	case "header":
		for _, key := range []string{"type", "invocation_id", "config_sha256", "input_sha256", "input_size", "manifest_sha256", "executable_sha256", "derived_tool", "derived_tool_sha256", "derived_native_sha256"} {
			allowed[key] = true
		}
	case "axis":
		for _, key := range []string{"type", "invocation_id", "config_sha256", "axis"} {
			allowed[key] = true
		}
	case "final":
		for _, key := range []string{"type", "invocation_id", "config_sha256", "report_sha256"} {
			allowed[key] = true
		}
	default:
		return errors.New("unknown report record type")
	}
	if len(object) != len(allowed) {
		return errors.New("report record does not have exact field set")
	}
	for key := range object {
		if !allowed[key] {
			return errors.New("report field is illegal for record type")
		}
	}
	return nil
}

func reportSHA256(report Report) (string, error) {
	var b bytes.Buffer
	writeField(&b, ReportDomain)
	writeField(&b, hex.EncodeToString(report.InvocationID[:]))
	canonical, err := canonicalJSON(report)
	if err != nil {
		return "", err
	}
	writeField(&b, string(canonical))
	return SHA256(b.Bytes()), nil
}

type hardLimitReader struct {
	reader    io.Reader
	remaining int64
	exceeded  bool
}

func (r *hardLimitReader) Read(p []byte) (int, error) {
	if r.remaining < 0 {
		return 0, errors.New("reader exceeded")
	}
	limit := int64(len(p))
	if limit > r.remaining+1 {
		limit = r.remaining + 1
	}
	n, err := r.reader.Read(p[:limit])
	r.remaining -= int64(n)
	if r.remaining < 0 {
		r.exceeded = true
		return n, errors.New("output limit exceeded")
	}
	return n, err
}
