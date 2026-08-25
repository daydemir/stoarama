package api

import (
	"reflect"
	"testing"
	"time"
)

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

func dailyGrades(s string) []recordingDailyGrade {
	out := make([]recordingDailyGrade, 0, len(s))
	for i, grade := range s {
		value := string(grade)
		if grade == '?' {
			value = "UNKNOWN"
		}
		out = append(out, recordingDailyGrade{WindowStartAt: time.Date(2026, 1, i+1, 0, 0, 0, 0, time.UTC), Grade: value})
	}
	return out
}

func TestClassifyBest14(t *testing.T) {
	tests := []struct {
		name, grades, status, rating, qualifier string
		runway                                  int
		completed                               int
	}{
		{"great", "AAABBBCCCAAABB", "completed", "GREAT", "", 0, 14},
		{"very good", "AAABBBCCCAAABD", "completed", "VERY_GOOD", "", 0, 14},
		{"good two e", "AAABBBCCCAAEEB", "completed", "GOOD", "", 0, 14},
		{"fine three e", "AAABBBCCCAEEEB", "completed", "FINE", "", 0, 14},
		{"one f wins precedence", "AAABBBCCCEEEDF", "completed", "QUESTIONABLE", "", 0, 14},
		{"multiple f", "AAABBBCCCAEEFF", "completed", "BAD", "", 0, 14},
		{"exactly thirteen ended", "AAABBBCCCAAAB", "completed", "INSUFFICIENT", "ENDED", 0, 13},
		{"live great potential", "ABCC", "active", "INSUFFICIENT", "GREAT_POTENTIAL", 10, 4},
		{"live questionable potential", "ABCF", "active", "INSUFFICIENT", "QUESTIONABLE_POTENTIAL", 10, 4},
		{"live bad potential", "ABCFF", "active", "INSUFFICIENT", "BAD_POTENTIAL", 9, 5},
		{"short runway", "ABCC", "active", "INSUFFICIENT", "SHORT_RUNWAY", 9, 4},
		{"paused is ended", "ABCC", "paused", "INSUFFICIENT", "ENDED", 20, 4},
		{"unknown breaks streak", "ABC?D", "active", "INSUFFICIENT", "VERY_GOOD_POTENTIAL", 13, 1},
		{"no completed days", "?", "active", "INSUFFICIENT", "UNKNOWN_POTENTIAL", 14, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyBest14(dailyGrades(tc.grades), tc.status, tc.runway)
			if got.Rating != tc.rating || got.Qualifier != tc.qualifier || got.Completed != tc.completed {
				t.Fatalf("got %+v want rating=%s qualifier=%s completed=%d", got, tc.rating, tc.qualifier, tc.completed)
			}
		})
	}
}

func TestBest14ChoosesFewestFThenEThenDThenC(t *testing.T) {
	got := best14Grades(dailyGrades("FEEEEEEEEEEEEEABBBBBBBBBBBBB"))
	if score := gradeRunScore(got); score != [5]int{0, 0, 0, 0, -1} {
		t.Fatalf("best score=%v grades=%v", score, got)
	}
}

func TestBest14CannotBridgeUnknownOrMissingCalendarDay(t *testing.T) {
	unknown := dailyGrades("AAAAAAA?AAAAAAA")
	if got := best14Grades(unknown); len(got) != 7 {
		t.Fatalf("unknown bridged into %d-day run, want 7", len(got))
	}

	missing := dailyGrades("AAAAAAAAAAAAAA")
	missing[7].WindowStartAt = missing[7].WindowStartAt.AddDate(0, 0, 1)
	for i := 8; i < len(missing); i++ {
		missing[i].WindowStartAt = missing[i].WindowStartAt.AddDate(0, 0, 1)
	}
	if got := best14Grades(missing); len(got) != 7 {
		t.Fatalf("calendar gap bridged into %d-day run, want 7", len(got))
	}
}

func TestRollOffPotentialCannotBridgeUnknownDay(t *testing.T) {
	grades := dailyGrades("AAAAFCCCCCC?CCCCCC")
	got := classifyBest14(grades, "active", 1)
	if got.Completed >= 14 || got.PotentialKind == "FUTURE_ROLL_OFF" {
		t.Fatalf("unknown day produced false completed roll-off path: %+v", got)
	}
}

func TestScheduledPotentialCannotBridgeFutureCalendarGap(t *testing.T) {
	grades := dailyGrades("CCCCCCCCCCCCC")
	last := grades[len(grades)-1].WindowStartAt
	got := classifyBest14Scheduled(grades, "active", []time.Time{last.AddDate(0, 0, 2)})
	if got.Qualifier != "SHORT_RUNWAY" || got.PotentialRating != "" {
		t.Fatalf("future calendar gap produced false potential: %+v", got)
	}
}

func TestCompletedQuestionableExposesCleanDayRollOffToGreat(t *testing.T) {
	// Recording 417's shape: every existing 14-day window still contains the F;
	// one more clean day creates a clean trailing 14.
	got := classifyBest14(dailyGrades("AAABFBBBAAAAAAAABB"), "active", 1)
	if got.Rating != "QUESTIONABLE" || got.Completed != 14 {
		t.Fatalf("current rating=%+v, want completed Questionable", got)
	}
	if got.PotentialRating != "GREAT" || got.PotentialKind != "FUTURE_ROLL_OFF" || got.PotentialDays != 1 {
		t.Fatalf("roll-off potential=%+v, want Great in one clean day", got)
	}
	want := []string{"questionable", "great_potential", "good_potential", "fine_potential"}
	if !reflect.DeepEqual(got.FilterKeys, want) {
		t.Fatalf("filter keys=%v want=%v", got.FilterKeys, want)
	}
}

func TestCompletedPotentialRequiresActiveScheduledRunway(t *testing.T) {
	grades := dailyGrades("AAABFBBBAAAAAAAABB")
	for _, tc := range []struct {
		name, status string
		runway       int
	}{
		{name: "completed", status: "completed", runway: 5},
		{name: "paused", status: "paused", runway: 5},
		{name: "no runway", status: "active", runway: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyBest14(grades, tc.status, tc.runway)
			if got.PotentialRating != "" || got.PotentialKind != "" || got.PotentialDays != 0 {
				t.Fatalf("unexpected potential %+v", got)
			}
		})
	}
}

func TestBest14SortOrder(t *testing.T) {
	want := []string{"GREAT", "VERY_GOOD", "GOOD", "FINE", "QUESTIONABLE", "BAD"}
	for i, rating := range want {
		if got := ratingRank(rating); got != i {
			t.Fatalf("rank(%s)=%d want=%d", rating, got, i)
		}
	}
	if greatPotential := classifyBest14(dailyGrades("ABC"), "active", 11); greatPotential.SortRank != 10 {
		t.Fatalf("unexpected potential sort rank %+v", greatPotential)
	}
	if classifyBest14(dailyGrades("ABC"), "active", 1).SortRank >= classifyBest14(dailyGrades("ABC"), "completed", 0).SortRank {
		t.Fatal("short runway must sort before ended")
	}
}
