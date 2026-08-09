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
