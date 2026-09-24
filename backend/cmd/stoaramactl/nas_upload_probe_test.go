package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestParseNASUploadProbeArgs(t *testing.T) {
	opts, err := parseNASUploadProbeArgs([]string{"report", "--connection-id", "13", "--limit", "5", "--json"})
	if err != nil || opts.connectionID != 13 || opts.limit != 5 || !opts.asJSON {
		t.Fatalf("opts=%+v err=%v", opts, err)
	}
	for _, args := range [][]string{nil, {"show"}, {"report"}, {"report", "--connection-id", "13", "--limit", "0"}, {"report", "--connection-id", "13", "extra"}} {
		if _, err := parseNASUploadProbeArgs(args); err == nil {
			t.Errorf("args %v accepted", args)
		}
	}
}

func TestNASUploadProbeStatusAndSummary(t *testing.T) {
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	reported := now.Add(-time.Hour)
	if got := nasUploadProbeStatus(nasUploadProbeRow{}, now.Add(time.Minute), now); got != "pending" {
		t.Fatalf("pending status=%s", got)
	}
	if got := nasUploadProbeStatus(nasUploadProbeRow{}, now.Add(-time.Minute), now); got != "abandoned" {
		t.Fatalf("abandoned status=%s", got)
	}
	if got := nasUploadProbeStatus(nasUploadProbeRow{ReportedAt: &reported, Error: "timeout"}, now, now); got != "error" {
		t.Fatalf("error status=%s", got)
	}
	mbps := func(v float64) *float64 { return &v }
	report := nasUploadProbeReport{Probes: []nasUploadProbeRow{
		{Status: "ok", Mbps: mbps(300)}, {Status: "error", Mbps: mbps(1)}, {Status: "ok", Mbps: mbps(100)},
		{Status: "ok", Mbps: mbps(200)}, {Status: "ok", Mbps: mbps(400)}, {Status: "abandoned"},
	}}
	summarizeNASUploadProbes(&report)
	if report.OKCount != 4 || *report.MedianMbps != 250 || *report.MinMbps != 100 || *report.MaxMbps != 400 {
		t.Fatalf("summary=%+v", report)
	}
}

func TestWriteNASUploadProbeReport(t *testing.T) {
	var empty bytes.Buffer
	if err := writeNASUploadProbeReport(&empty, nasUploadProbeReport{ConnectionID: 13, Label: "MIT", Probes: []nasUploadProbeRow{}}, false); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(empty.String(), "No upload probes recorded yet") {
		t.Fatalf("empty report=%q", empty.String())
	}
	uploaded, duration, rate := int64(256<<20), int64(21_474), 100.0
	reported := time.Date(2026, 9, 24, 12, 0, 30, 0, time.UTC)
	report := nasUploadProbeReport{ConnectionID: 13, Label: "MIT", Probes: []nasUploadProbeRow{{
		ID: 7, Status: "ok", CreatedAt: reported.Add(-30 * time.Second), ReportedAt: &reported, SizeBytes: uploaded, Streams: 4,
		BytesUploaded: &uploaded, DurationMS: &duration, Mbps: &rate, ClientPhaseStart: "draining", ClientPhaseEnd: "idle",
	}}}
	summarizeNASUploadProbes(&report)
	var text bytes.Buffer
	if err := writeNASUploadProbeReport(&text, report, false); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Latest: #7 ok at 2026-09-24T12:00:00Z 100.0 Mbps", "median 100.0 Mbps", "256.0", "draining>idle"} {
		if !strings.Contains(text.String(), want) {
			t.Errorf("report missing %q:\n%s", want, text.String())
		}
	}
	var encoded bytes.Buffer
	if err := writeNASUploadProbeReport(&encoded, report, true); err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded.Bytes(), &decoded); err != nil || decoded["median_mbps"] != 100.0 {
		t.Fatalf("json report=%s err=%v", encoded.String(), err)
	}
}
