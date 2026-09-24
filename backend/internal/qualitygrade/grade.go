// Package qualitygrade is the single definition of the daily continuous-window
// quality grades (A-F) and the 14-day tier progress built on them. The API,
// the stoaramactl report, the hourly health alert, and the operator digest all
// read grades through this package so they can never disagree.
package qualitygrade

import "time"

// Grade is one completed continuous window's timeline grade.
type Grade string

const (
	GradeA       Grade = "A"
	GradeB       Grade = "B"
	GradeC       Grade = "C"
	GradeD       Grade = "D"
	GradeE       Grade = "E"
	GradeF       Grade = "F"
	GradeUnknown Grade = "unknown"
	// GradeMissing marks a local calendar day with no scheduled window between
	// two graded days. It breaks every tier's contiguity.
	GradeMissing Grade = "missing"
)

// MetricVersion is the recording_window_health metric version these rules are
// calibrated against. Rows from any other version grade as unknown.
const MetricVersion = 2

// WindowMetrics are the recording_window_health facts for one window. Nil
// pointers mean the metric is absent. Measured is false when no health row
// exists or the row predates the window close (outdated).
type WindowMetrics struct {
	Measured          bool
	CoveragePct       *float64
	LargestGapSeconds *float64
	GapsOver30s       *int
	GapsOver5m        *int
	Overlaps          *int
	MetricVersion     *int
	ClipCount         int
}

// Classify grades one window:
//
//	A: zero overlap, coverage >= 99%, largest gap <= 120s.
//	B: zero overlap, coverage >= 95%, largest gap <= 900s, <= 1 gap over 5m, <= 6 gaps over 30s.
//	C: zero overlap, coverage >= 90%, largest gap <= 1800s, <= 2 gaps over 5m.
//	F: no clips or zero coverage.
//	E: coverage below 80%.
//	D: everything else.
//	unknown: missing or outdated metrics.
func Classify(m WindowMetrics) Grade {
	if !m.Measured || m.MetricVersion == nil || *m.MetricVersion != MetricVersion ||
		m.CoveragePct == nil || m.LargestGapSeconds == nil || m.GapsOver30s == nil ||
		m.GapsOver5m == nil || m.Overlaps == nil {
		return GradeUnknown
	}
	coverage, gap := *m.CoveragePct, *m.LargestGapSeconds
	noOverlap := *m.Overlaps == 0
	switch {
	case noOverlap && coverage >= 99 && gap <= 120:
		return GradeA
	case noOverlap && coverage >= 95 && gap <= 900 && *m.GapsOver5m <= 1 && *m.GapsOver30s <= 6:
		return GradeB
	case noOverlap && coverage >= 90 && gap <= 1800 && *m.GapsOver5m <= 2:
		return GradeC
	case m.ClipCount == 0 || coverage == 0:
		return GradeF
	case coverage < 80:
		return GradeE
	default:
		return GradeD
	}
}

// Poor reports whether a grade needs operator attention: it breaks Good+
// progress outright (F, unknown) or consumes its small E allowance.
func (g Grade) Poor() bool {
	return g == GradeE || g == GradeF || g == GradeUnknown
}

// Tier is a 14-contiguous-day quality tier.
type Tier string

const (
	TierFine  Tier = "fine+"
	TierGood  Tier = "good+"
	TierGreat Tier = "great+"
)

// Tiers lists the tiers from weakest to strongest.
var Tiers = []Tier{TierFine, TierGood, TierGreat}

// RunLength is the number of contiguous local-calendar days a tier requires.
const RunLength = 14

// maxGoodE is how many E days one Good+ window may contain.
const maxGoodE = 2

// admits reports whether a single day can belong to a qualifying window of
// the tier, ignoring the Good+ E allowance, which is a window-level count.
func (t Tier) admits(g Grade) bool {
	switch t {
	case TierFine:
		return g != GradeMissing
	case TierGood:
		return g != GradeMissing && g != GradeF && g != GradeUnknown
	case TierGreat:
		return g == GradeA || g == GradeB || g == GradeC
	}
	return false
}

// qualifies reports whether a contiguous slice of days satisfies the tier.
func (t Tier) qualifies(days []Grade) bool {
	e := 0
	for _, g := range days {
		if !t.admits(g) {
			return false
		}
		if g == GradeE {
			e++
		}
	}
	return t != TierGood || e <= maxGoodE
}

// Day is one local calendar day's grade.
type Day struct {
	Date  time.Time `json:"date"` // midnight of the local calendar day, UTC-normalized
	Grade Grade     `json:"grade"`
}

// Progress is one tier's standing for a recording.
type Progress struct {
	Tier Tier `json:"tier"`
	// Completed is true when any 14 contiguous days in the history qualify.
	Completed bool `json:"completed"`
	// RunDays is the current candidate run: the most recent days (up to 14)
	// that still qualify together. Fourteen means the tier is met right now.
	RunDays int `json:"run_days"`
	// DaysToGo is how many more qualifying days complete the current run.
	DaysToGo int `json:"days_to_go"`
	// ECount and FCount count E days and F/unknown days inside the run.
	ECount int `json:"e_count"`
	FCount int `json:"f_count"`
}

// FillMissing sorts nothing; it expects ascending, de-duplicated days and
// inserts GradeMissing for any calendar day skipped between two entries.
func FillMissing(days []Day) []Day {
	if len(days) == 0 {
		return nil
	}
	out := make([]Day, 0, len(days))
	for i, d := range days {
		if i > 0 {
			for gap := days[i-1].Date.AddDate(0, 0, 1); gap.Before(d.Date); gap = gap.AddDate(0, 0, 1) {
				out = append(out, Day{Date: gap, Grade: GradeMissing})
			}
		}
		out = append(out, d)
	}
	return out
}

// Evaluate computes every tier's progress over contiguous, ascending days
// (use FillMissing first).
func Evaluate(days []Day) []Progress {
	grades := make([]Grade, len(days))
	for i, d := range days {
		grades[i] = d.Grade
	}
	out := make([]Progress, 0, len(Tiers))
	for _, tier := range Tiers {
		p := Progress{Tier: tier}
		for start := 0; start+RunLength <= len(grades); start++ {
			if tier.qualifies(grades[start : start+RunLength]) {
				p.Completed = true
				break
			}
		}
		for k := min(RunLength, len(grades)); k > 0; k-- {
			run := grades[len(grades)-k:]
			if tier.qualifies(run) {
				p.RunDays = k
				for _, g := range run {
					switch g {
					case GradeE:
						p.ECount++
					case GradeF, GradeUnknown:
						p.FCount++
					}
				}
				break
			}
		}
		p.DaysToGo = RunLength - p.RunDays
		out = append(out, p)
	}
	return out
}
