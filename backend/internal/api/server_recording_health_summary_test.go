package api

import "testing"

func float64Ptr(v float64) *float64 { return &v }

func TestClassifyRecordingTimelineHealth(t *testing.T) {
	tests := []struct {
		name          string
		health        recordingTimelineHealth
		grade         string
		stitchability string
	}{
		{name: "awaiting window", health: recordingTimelineHealth{}, grade: "unknown", stitchability: "unknown"},
		{name: "low union coverage", health: recordingTimelineHealth{RecentWindowCount: 2, RecentCoveragePct: float64Ptr(53.79)}, grade: "critical", stitchability: "gapped"},
		{name: "layout changed", health: recordingTimelineHealth{RecentWindowCount: 2, RecentCoveragePct: float64Ptr(99.95), RecentLayoutChangeCount: 1}, grade: "warning", stitchability: "layout_changed"},
		{name: "overlap", health: recordingTimelineHealth{RecentWindowCount: 2, RecentCoveragePct: float64Ptr(100), RecentOverlapCount: 1}, grade: "warning", stitchability: "overlap"},
		{name: "continuous", health: recordingTimelineHealth{RecentWindowCount: 2, RecentCoveragePct: float64Ptr(100)}, grade: "healthy", stitchability: "continuous"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			classifyRecordingTimelineHealth(&tc.health)
			if tc.health.Grade != tc.grade || tc.health.Stitchability != tc.stitchability {
				t.Fatalf("grade=%q stitchability=%q want %q/%q", tc.health.Grade, tc.health.Stitchability, tc.grade, tc.stitchability)
			}
		})
	}
}

func TestClassifyRecordingDailyGrade(t *testing.T) {
	metric := func(coverage, gap float64, over30, over5, overlaps, version int) qualificationWindowMetrics {
		expected := int64(43200)
		return qualificationWindowMetrics{
			CoveragePct: &coverage, LargestGap: &gap, GapsOver30s: &over30, GapsOver5m: &over5,
			OverlapCount: &overlaps, MetricVersion: &version, ExpectedSeconds: expected,
			MeasuredExpected: &expected, JobCount: 1, HealthCount: 1,
		}
	}
	tests := []struct {
		name  string
		m     qualificationWindowMetrics
		clips int
		want  string
	}{
		{"great", metric(99, 120, 1, 0, 0, 2), 10, "A"},
		{"good", metric(95, 900, 6, 1, 0, 2), 10, "B"},
		{"acceptable", metric(90, 1800, 9, 2, 0, 2), 10, "C"},
		{"degraded", metric(80, 2000, 9, 3, 0, 2), 10, "D"},
		{"poor", metric(79.99, 2000, 9, 3, 0, 2), 10, "E"},
		{"blackout", metric(0, 43200, 1, 1, 0, 2), 0, "F"},
		{"unknown metric", metric(100, 0, 0, 0, 0, 1), 10, "UNKNOWN"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got, _ := classifyRecordingDailyGrade(tc.m, tc.clips); got != tc.want {
				t.Fatalf("grade=%s want=%s", got, tc.want)
			}
		})
	}
}
