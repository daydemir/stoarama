package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestParseNASBenchmarkArgs(t *testing.T) {
	opts, err := parseNASBenchmarkArgs([]string{"report", "--connection-id", "13"})
	if err != nil || opts.command != "report" || opts.connectionID != 13 || opts.limit != 5 {
		t.Fatalf("report opts=%+v err=%v", opts, err)
	}
	opts, err = parseNASBenchmarkArgs([]string{"request", "--connection-id", "13", "--recording-id", "339", "--window-start", "2026-09-22T14:00:00Z"})
	if err != nil || opts.recordingID != 339 || opts.windowStart == nil || !opts.windowStart.Equal(time.Date(2026, 9, 22, 14, 0, 0, 0, time.UTC)) {
		t.Fatalf("request opts=%+v err=%v", opts, err)
	}
	for _, args := range [][]string{
		{}, {"run"}, {"report"}, {"report", "--connection-id", "13", "--recording-id", "1"},
		{"request", "--connection-id", "13", "--recording-id", "339"},
		{"request", "--connection-id", "13", "--window-start", "nope", "--recording-id", "1"},
		{"report", "--connection-id", "13", "extra"},
	} {
		if _, err := parseNASBenchmarkArgs(args); err == nil {
			t.Fatalf("args %v accepted", args)
		}
	}
}

func TestWriteNASBenchmarkReportShowsPerStageCost(t *testing.T) {
	reported := time.Date(2026, 9, 24, 13, 0, 0, 0, time.UTC)
	report := nasBenchmarkReport{
		ConnectionID: 13, Label: "MIT NAS", Host: json.RawMessage(`{"machine":"x86_64","cpu_count":8}`), HostReportedAt: &reported,
		Benchmarks: []nasBenchmarkRow{{
			ID: 2, State: "reported", RequestedBy: "auto", RequestedAt: reported, ClaimCount: 1, Status: "ok",
			ClientVersion: "abc12345", PlanClips: 60, PlanBytes: 750e6,
			Result: json.RawMessage(`{"media_seconds":3600,"input_bytes":750000000,"threads":2,"nice":15,"total_wall_s":600,
				"temp_removed":true,"stages":{"remux":{"ok":true,"wall_s":20,"cpu_s":12,"max_rss_kb":40960,"output_bytes":749000000},
				"packet_chain":{"ok":true,"match":true,"wall_s":30,"cpu_s":25},
				"seam_decode":{"ok":true,"seams":59,"failures":0,"wall_s":60,"cpu_s":40},
				"full_decode":{"ok":true,"wall_s":200,"cpu_s":300}}}`),
		}, {ID: 1, State: "abandoned", RequestedBy: "operator", RequestedAt: reported, Error: "no complete hour"}},
	}
	var out bytes.Buffer
	if err := writeNASBenchmarkReport(&out, report, false); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{
		`Host (2026-09-24T13:00:00Z): {"machine":"x86_64","cpu_count":8}`,
		"#2 reported by auto", "hour: 60 clips, 0.75 GB", "temp_removed=true",
		"full_decode   true      200.0     300.0         300.0",
		"match=true", "seams=59 failures=0", "out=0.75GB", `error="no complete hour"`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("report lacks %q:\n%s", want, text)
		}
	}
	out.Reset()
	if err := writeNASBenchmarkReport(&out, report, true); err != nil || !strings.Contains(out.String(), `"connection_id": 13`) {
		t.Fatalf("json report err=%v out=%s", err, out.String())
	}
}

func TestNASBenchmarkRequestPoolIsWritable(t *testing.T) {
	if got := nasBenchmarkReadOnly("request"); got != "off" {
		t.Fatalf("request pool read-only=%q, want off", got)
	}
	if got := nasBenchmarkReadOnly("report"); got != "on" {
		t.Fatalf("report pool read-only=%q, want on", got)
	}
}
