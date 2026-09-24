package qualitygrade

import (
	"testing"
	"time"
)

func f(v float64) *float64 { return &v }
func i(v int) *int         { return &v }

func metrics(coverage, gap float64, over30, over5, overlaps, clips int) WindowMetrics {
	return WindowMetrics{Measured: true, CoveragePct: f(coverage), LargestGapSeconds: f(gap),
		GapsOver30s: i(over30), GapsOver5m: i(over5), Overlaps: i(overlaps), MetricVersion: i(MetricVersion), ClipCount: clips}
}

func TestClassifyMatchesDocumentedRules(t *testing.T) {
	cases := []struct {
		name string
		m    WindowMetrics
		want Grade
	}{
		{"A", metrics(99.5, 120, 0, 0, 0, 700), GradeA},
		{"A needs zero overlap", metrics(99.5, 60, 0, 0, 1, 700), GradeD},
		{"B", metrics(96, 800, 6, 1, 0, 700), GradeB},
		{"B gap budget exceeded is C", metrics(96, 800, 7, 1, 0, 700), GradeC},
		{"C", metrics(91, 1800, 20, 2, 0, 700), GradeC},
		{"D", metrics(85, 3000, 20, 5, 0, 700), GradeD},
		{"E seoul tls day", metrics(72, 2763, 30, 8, 0, 500), GradeE},
		{"F no clips", metrics(0, 43200, 0, 0, 0, 0), GradeF},
		{"F zero coverage with clips row", metrics(0, 43200, 0, 0, 0, 3), GradeF},
		{"unknown unmeasured", WindowMetrics{}, GradeUnknown},
		{"unknown old metric version", func() WindowMetrics { m := metrics(99.9, 10, 0, 0, 0, 700); m.MetricVersion = i(1); return m }(), GradeUnknown},
		{"unknown missing gap count", func() WindowMetrics { m := metrics(99.9, 10, 0, 0, 0, 700); m.GapsOver5m = nil; return m }(), GradeUnknown},
	}
	for _, tc := range cases {
		if got := Classify(tc.m); got != tc.want {
			t.Errorf("%s: got %s want %s", tc.name, got, tc.want)
		}
	}
}

func series(grades ...Grade) []Day {
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	out := make([]Day, len(grades))
	for k, g := range grades {
		out[k] = Day{Date: start.AddDate(0, 0, k), Grade: g}
	}
	return out
}

func repeat(g Grade, n int) []Grade {
	out := make([]Grade, n)
	for k := range out {
		out[k] = g
	}
	return out
}

func progress(t *testing.T, days []Day, tier Tier) Progress {
	t.Helper()
	for _, p := range Evaluate(days) {
		if p.Tier == tier {
			return p
		}
	}
	t.Fatalf("tier %s missing", tier)
	return Progress{}
}

func TestEvaluateGoodPlusAllowsTwoE(t *testing.T) {
	grades := append(repeat(GradeA, 5), GradeE, GradeB, GradeE)
	grades = append(grades, repeat(GradeC, 6)...)
	p := progress(t, series(grades...), TierGood)
	if !p.Completed || p.RunDays != 14 || p.DaysToGo != 0 || p.ECount != 2 || p.FCount != 0 {
		t.Fatalf("good+ with two E = %+v", p)
	}
	great := progress(t, series(grades...), TierGreat)
	if great.Completed || great.RunDays != 6 {
		t.Fatalf("great+ must restart after the last E: %+v", great)
	}
}

func TestEvaluateThirdEShortensGoodRun(t *testing.T) {
	grades := append([]Grade{GradeE}, repeat(GradeA, 4)...)
	grades = append(grades, GradeE, GradeA, GradeE, GradeA)
	p := progress(t, series(grades...), TierGood)
	// The run may keep at most two E days, so it begins after the first E.
	if p.Completed || p.RunDays != 8 || p.ECount != 2 || p.DaysToGo != 6 {
		t.Fatalf("good+ with third E = %+v", p)
	}
}

func TestEvaluateFAndUnknownBreakGoodButNotFine(t *testing.T) {
	grades := append(repeat(GradeB, 10), GradeF, GradeA, GradeUnknown, GradeA)
	days := series(grades...)
	good := progress(t, days, TierGood)
	if good.RunDays != 1 || good.DaysToGo != 13 {
		t.Fatalf("good+ after unknown = %+v", good)
	}
	fine := progress(t, days, TierFine)
	if !fine.Completed || fine.RunDays != 14 || fine.FCount != 2 {
		t.Fatalf("fine+ accepts any grade = %+v", fine)
	}
}

func TestFillMissingBreaksContiguity(t *testing.T) {
	days := series(repeat(GradeA, 14)...)
	gapped := append(append([]Day{}, days[:7]...), days[8:]...)
	filled := FillMissing(gapped)
	if len(filled) != 14 || filled[7].Grade != GradeMissing {
		t.Fatalf("filled=%+v", filled)
	}
	for _, p := range Evaluate(filled) {
		if p.Completed || p.RunDays != 6 {
			t.Fatalf("missing day must break %s: %+v", p.Tier, p)
		}
	}
}

func TestPoorGrades(t *testing.T) {
	for _, g := range []Grade{GradeE, GradeF, GradeUnknown} {
		if !g.Poor() {
			t.Fatalf("%s must be poor", g)
		}
	}
	for _, g := range []Grade{GradeA, GradeB, GradeC, GradeD} {
		if g.Poor() {
			t.Fatalf("%s must not be poor", g)
		}
	}
}
