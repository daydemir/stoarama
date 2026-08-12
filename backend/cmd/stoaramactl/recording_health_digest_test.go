package main

import (
	"strings"
	"testing"
	"time"
)

func TestHealthDigestBucket(t *testing.T) {
	got := healthDigestBucket(time.Date(2026, 8, 12, 15, 59, 0, 0, time.FixedZone("west", -4*60*60)))
	want := time.Date(2026, 8, 12, 16, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("bucket = %s, want %s", got, want)
	}
}

func TestComposeHealthDigestSeparatesCurrentAndHistorical(t *testing.T) {
	now := time.Date(2026, 8, 12, 16, 0, 0, 0, time.UTC)
	items := []digestRecording{
		{ID: 1, Name: "healthy now", Scheduled: true, Bucket: "stable", CurrentCause: "current capture progressing", HistoryBucket: "failing", HistoryNote: "latest completed 50%"},
		{ID: 2, Name: "off window", Scheduled: false, Bucket: "unknown", CurrentCause: "off-window / not currently assessed", HistoryBucket: "stable", HistoryNote: "latest completed 99.9%"},
		{ID: 3, Name: "live failure", Scheduled: true, Bucket: "failing", CurrentCause: "no fresh ingest", HistoryBucket: "degraded", HistoryNote: "latest completed 96%"},
	}
	body := composeHealthDigest("https://stoarama.com", now, items, digestNAS{})
	for _, want := range []string{
		"Active fleet: 3 total; 2 currently scheduled/live; 1 off-window/not assessed.",
		"Current operational health: 1/2 current healthy, 1 failing, 0 unknown.",
		"Latest completed-window quality (historical, not current status): 1 stable, 1 degraded, 1 failing, 0 unknown.",
		"CURRENT FAILING (1)",
		"#3 live failure",
		"CURRENT STABLE (1)",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("body missing %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "#1 healthy now — latest completed 50%") {
		t.Fatalf("historical failure was rendered as a current failure:\n%s", body)
	}
}

func TestComposeHealthDigestNASCurrentAndRecoveredAreDistinct(t *testing.T) {
	now := time.Date(2026, 8, 12, 16, 0, 0, 0, time.UTC)
	reported := now.Add(-time.Minute)
	recovered := now.Add(-time.Hour)
	free, total := int64(2e12), int64(8e12)
	body := composeHealthDigest("", now, nil, digestNAS{
		Label: "NAS", Phase: "idle", AlertState: "critical", Free: &free, Total: &total,
		ReportedAt: &reported, Blocked: true, LastOutageClass: "timeout", OutageRecoveredAt: &recovered,
	})
	if !strings.Contains(body, "Current: alert=critical") || !strings.Contains(body, "Historical transient: timeout recovered=") || !strings.Contains(body, "not a current outage") {
		t.Fatalf("NAS current/recovered wording missing:\n%s", body)
	}
}
