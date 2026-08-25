package api

import (
	"context"
	"fmt"
	"time"
)

type recordingTimelineHealth struct {
	RecentCoveragePct         *float64              `json:"recent_coverage_pct"`
	RecentLargestGapSeconds   float64               `json:"recent_largest_gap_seconds"`
	RecentGapCount            int64                 `json:"recent_gap_count"`
	RecentOverlapCount        int64                 `json:"recent_overlap_count"`
	RecentOverlapSeconds      float64               `json:"recent_overlap_seconds"`
	RecentLayoutChangeCount   int64                 `json:"recent_layout_change_count"`
	RecentWindowCount         int                   `json:"recent_window_count"`
	LifetimeCoveragePct       *float64              `json:"lifetime_coverage_pct"`
	LifetimeLargestGapSeconds float64               `json:"lifetime_largest_gap_seconds"`
	LifetimeGapCount          int64                 `json:"lifetime_gap_count"`
	LifetimeOverlapCount      int64                 `json:"lifetime_overlap_count"`
	LifetimeOverlapSeconds    float64               `json:"lifetime_overlap_seconds"`
	LifetimeLayoutChangeCount int64                 `json:"lifetime_layout_change_count"`
	LifetimeWindowCount       int                   `json:"lifetime_window_count"`
	LifetimeComplete          bool                  `json:"lifetime_complete"`
	CalculatedAt              time.Time             `json:"calculated_at"`
	Grade                     string                `json:"grade"`
	Stitchability             string                `json:"stitchability"`
	Diagnosis                 string                `json:"diagnosis"`
	DailyGrades               []recordingDailyGrade `json:"daily_grades"`
	Best14Rating              recordingBest14Rating `json:"best_14_rating"`
}

type recordingBest14Rating struct {
	Rating       string   `json:"rating"`
	Qualifier    string   `json:"qualifier,omitempty"`
	Completed    int      `json:"completed_days"`
	SortRank     int      `json:"sort_rank"`
	FilterKeys   []string `json:"filter_keys"`
	TierSortRank int      `json:"tier_sort_rank"`
}

type recordingDailyGrade struct {
	WindowStartAt     time.Time `json:"window_start_at"`
	WindowEndAt       time.Time `json:"window_end_at"`
	Grade             string    `json:"grade"`
	CoveragePct       float64   `json:"coverage_pct"`
	LargestGapSeconds float64   `json:"largest_gap_seconds"`
	Reason            string    `json:"reason"`
}

func classifyRecordingDailyGrade(m qualificationWindowMetrics, clipCount int) (string, string) {
	grade, reason := classifyQualificationTimeline(m)
	switch grade {
	case "GREAT_CANDIDATE":
		return "A", reason
	case "GOOD_CANDIDATE":
		return "B", reason
	case "ACCEPTABLE_CANDIDATE":
		return "C", reason
	case "UNKNOWN":
		return "UNKNOWN", reason
	}
	if m.CoveragePct == nil || clipCount == 0 || *m.CoveragePct == 0 {
		return "F", "no usable completed media"
	}
	if *m.CoveragePct < 80 {
		return "E", reason
	}
	return "D", reason
}

func gradeRunRating(grades []recordingDailyGrade) string {
	counts := map[string]int{}
	known := 0
	for _, day := range grades {
		if day.Grade != "UNKNOWN" {
			counts[day.Grade]++
			known++
		}
	}
	switch {
	case known == 0:
		return "UNKNOWN"
	case counts["F"] > 1:
		return "BAD"
	case counts["F"] == 1:
		return "QUESTIONABLE"
	case counts["E"] > 2:
		return "FINE"
	case counts["E"] > 0:
		return "GOOD"
	case counts["D"] > 0:
		return "VERY_GOOD"
	default:
		return "GREAT"
	}
}

func gradeRunScore(grades []recordingDailyGrade) [5]int {
	counts := map[string]int{}
	for _, day := range grades {
		counts[day.Grade]++
	}
	return [5]int{counts["F"], counts["E"], counts["D"], counts["C"], -counts["A"]}
}

func best14Grades(days []recordingDailyGrade) []recordingDailyGrade {
	known := make([]recordingDailyGrade, 0, len(days))
	for _, day := range days {
		if day.Grade != "UNKNOWN" {
			known = append(known, day)
		}
	}
	if len(known) < 14 {
		return known
	}
	best := known[:14]
	for i := 1; i+14 <= len(known); i++ {
		candidate := known[i : i+14]
		if scoreLess(gradeRunScore(candidate), gradeRunScore(best)) {
			best = candidate
		}
	}
	return best
}

func scoreLess(left, right [5]int) bool {
	for i := range left {
		if left[i] != right[i] {
			return left[i] < right[i]
		}
	}
	return false
}

func ratingRank(rating string) int {
	return map[string]int{"GREAT": 0, "VERY_GOOD": 1, "GOOD": 2, "FINE": 3, "QUESTIONABLE": 4, "BAD": 5, "UNKNOWN": 6}[rating]
}

func best14FilterKeys(rating recordingBest14Rating) []string {
	if rating.Rating != "INSUFFICIENT" && rating.Completed >= 14 && rating.Qualifier == "" {
		switch rating.Rating {
		case "GREAT":
			return []string{"great_plus", "good_plus", "fine_plus"}
		case "VERY_GOOD", "GOOD":
			return []string{"good_plus", "fine_plus"}
		case "FINE":
			return []string{"fine_plus"}
		case "QUESTIONABLE":
			return []string{"questionable"}
		case "BAD":
			return []string{"bad"}
		}
	}
	if rating.Rating == "INSUFFICIENT" && rating.Completed < 14 {
		switch rating.Qualifier {
		case "GREAT_POTENTIAL":
			return []string{"great_potential", "good_potential", "fine_potential"}
		case "VERY_GOOD_POTENTIAL", "GOOD_POTENTIAL":
			return []string{"good_potential", "fine_potential"}
		case "FINE_POTENTIAL":
			return []string{"fine_potential"}
		case "QUESTIONABLE_POTENTIAL":
			return []string{"questionable_potential"}
		case "BAD_POTENTIAL":
			return []string{"bad_potential"}
		}
	}
	return []string{"insufficient"}
}

func best14TierSortRank(rating recordingBest14Rating) int {
	if rating.Rating == "INSUFFICIENT" || rating.Completed < 14 || rating.Qualifier != "" {
		return 5
	}
	switch rating.Rating {
	case "GREAT":
		return 0
	case "VERY_GOOD", "GOOD":
		return 1
	case "FINE":
		return 2
	case "QUESTIONABLE":
		return 3
	case "BAD":
		return 4
	default:
		return 5
	}
}

func finalizeBest14Rating(rating recordingBest14Rating) recordingBest14Rating {
	rating.FilterKeys = best14FilterKeys(rating)
	rating.TierSortRank = best14TierSortRank(rating)
	return rating
}

func classifyBest14(days []recordingDailyGrade, status string, remainingWindows int) recordingBest14Rating {
	best := best14Grades(days)
	completed := len(best)
	rating := gradeRunRating(best)
	if completed >= 14 {
		return finalizeBest14Rating(recordingBest14Rating{Rating: rating, Completed: completed, SortRank: ratingRank(rating)})
	}
	out := recordingBest14Rating{Rating: "INSUFFICIENT", Completed: completed, SortRank: 60}
	if status != "active" {
		out.Qualifier = "ENDED"
		out.SortRank = 80
	} else if completed+remainingWindows < 14 {
		out.Qualifier = "SHORT_RUNWAY"
		out.SortRank = 70
	} else {
		out.Qualifier = rating + "_POTENTIAL"
		out.SortRank = 10 + ratingRank(rating)
	}
	return finalizeBest14Rating(out)
}

func classifyRecordingTimelineHealth(h *recordingTimelineHealth) {
	h.Grade, h.Stitchability, h.Diagnosis = "unknown", "unknown", "Awaiting a completed recording window"
	if h.RecentWindowCount == 0 || h.RecentCoveragePct == nil {
		return
	}
	pct := *h.RecentCoveragePct
	switch {
	case pct < 95:
		h.Grade, h.Diagnosis = "critical", fmt.Sprintf("%.2f%% coverage in recent completed windows", pct)
	case h.RecentOverlapCount > 0:
		h.Grade, h.Diagnosis = "warning", fmt.Sprintf("%d overlapping clip seam(s)", h.RecentOverlapCount)
	case h.RecentLargestGapSeconds > 300:
		h.Grade, h.Diagnosis = "warning", fmt.Sprintf("largest gap %s", compactHealthDuration(h.RecentLargestGapSeconds))
	case h.RecentGapCount > 10:
		h.Grade, h.Diagnosis = "warning", fmt.Sprintf("fragmented across %d gap(s)", h.RecentGapCount)
	case h.RecentLayoutChangeCount > 0:
		h.Grade, h.Diagnosis = "warning", fmt.Sprintf("%d native media layout change(s)", h.RecentLayoutChangeCount)
	default:
		h.Grade, h.Diagnosis = "healthy", "recent completed windows are continuous"
	}
	switch {
	case h.RecentLayoutChangeCount > 0:
		h.Stitchability = "layout_changed"
	case h.RecentOverlapCount > 0:
		h.Stitchability = "overlap"
	case h.RecentGapCount > 0 || pct < 99.9:
		h.Stitchability = "gapped"
	default:
		h.Stitchability = "continuous"
	}
}

func compactHealthDuration(seconds float64) string {
	return (time.Duration(seconds * float64(time.Second))).Round(time.Second).String()
}

func (s *Server) recordingTimelineHealthForAccount(ctx context.Context, accountID int64, recordingIDs []int64) (map[int64]recordingTimelineHealth, error) {
	out := make(map[int64]recordingTimelineHealth, len(recordingIDs))
	if accountID <= 0 || len(recordingIDs) == 0 {
		return out, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT h.recording_id,h.recent_coverage_pct,h.recent_largest_gap_seconds,h.recent_gap_count,
		       h.recent_overlap_count,h.recent_overlap_seconds,h.recent_layout_change_count,h.recent_window_count,
		       h.lifetime_coverage_pct,h.lifetime_largest_gap_seconds,h.lifetime_gap_count,
		       h.lifetime_overlap_count,h.lifetime_overlap_seconds,h.lifetime_layout_change_count,
		       h.lifetime_window_count,h.lifetime_complete,h.calculated_at
		FROM recording_health_summaries h
		JOIN recordings r ON r.id=h.recording_id
		WHERE r.account_id=$1 AND h.recording_id=ANY($2::bigint[])
	`, accountID, recordingIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var h recordingTimelineHealth
		if err := rows.Scan(&id, &h.RecentCoveragePct, &h.RecentLargestGapSeconds, &h.RecentGapCount,
			&h.RecentOverlapCount, &h.RecentOverlapSeconds, &h.RecentLayoutChangeCount, &h.RecentWindowCount,
			&h.LifetimeCoveragePct, &h.LifetimeLargestGapSeconds, &h.LifetimeGapCount,
			&h.LifetimeOverlapCount, &h.LifetimeOverlapSeconds, &h.LifetimeLayoutChangeCount,
			&h.LifetimeWindowCount, &h.LifetimeComplete, &h.CalculatedAt); err != nil {
			return nil, err
		}
		classifyRecordingTimelineHealth(&h)
		out[id] = h
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	remaining := map[int64]int{}
	status := map[int64]string{}
	metaRows, err := s.pool.Query(ctx, `
		SELECT r.id,r.status,
		CASE WHEN r.status <> 'active' THEN 0 WHEN r.end_at IS NULL THEN 14 ELSE (
			SELECT count(*) FROM generate_series(
				(now() AT TIME ZONE r.cron_timezone)::date - 1,
				(r.end_at AT TIME ZONE r.cron_timezone)::date,
				interval '1 day') d
			WHERE (r.active_weekdays & (1 << (extract(isodow FROM d)::int-1))) <> 0
			  AND ((d::date + r.daily_window_end) AT TIME ZONE r.cron_timezone) > now()
			  AND ((d::date + r.daily_window_end) AT TIME ZONE r.cron_timezone) <= r.end_at
		) END
		FROM recordings r WHERE r.account_id=$1 AND r.id=ANY($2::bigint[])
	`, accountID, recordingIDs)
	if err != nil {
		return nil, err
	}
	defer metaRows.Close()
	for metaRows.Next() {
		var id int64
		var state string
		var runway int
		if err := metaRows.Scan(&id, &state, &runway); err != nil {
			return nil, err
		}
		status[id], remaining[id] = state, runway
		if _, ok := out[id]; !ok {
			out[id] = recordingTimelineHealth{}
		}
	}
	if err := metaRows.Err(); err != nil {
		return nil, err
	}
	gradeRows, err := s.pool.Query(ctx, `
		SELECT h.recording_id,h.window_start_at,h.window_end_at,h.expected_seconds,h.coverage_pct,
		       h.largest_gap_seconds,h.gap_over_30s_count,h.gap_over_5m_count,h.overlap_count,
		       h.metric_version,h.clip_count
		FROM recording_window_health h
		JOIN recordings r ON r.id=h.recording_id
		WHERE r.account_id=$1 AND h.recording_id=ANY($2::bigint[])
		ORDER BY h.recording_id,h.window_start_at
	`, accountID, recordingIDs)
	if err != nil {
		return nil, err
	}
	defer gradeRows.Close()
	for gradeRows.Next() {
		var id, expected int64
		var start, end time.Time
		var coverage, largest float64
		var over30, over5, overlaps *int
		var version *int
		var clips int
		if err := gradeRows.Scan(&id, &start, &end, &expected, &coverage, &largest, &over30, &over5, &overlaps, &version, &clips); err != nil {
			return nil, err
		}
		grade, reason := classifyRecordingDailyGrade(qualificationWindowMetrics{
			CoveragePct: &coverage, LargestGap: &largest, GapsOver30s: over30, GapsOver5m: over5,
			OverlapCount: overlaps, MetricVersion: version, ExpectedSeconds: expected,
			MeasuredExpected: &expected, JobCount: 1, HealthCount: 1,
		}, clips)
		h := out[id]
		h.DailyGrades = append(h.DailyGrades, recordingDailyGrade{
			WindowStartAt: start, WindowEndAt: end, Grade: grade, CoveragePct: coverage,
			LargestGapSeconds: largest, Reason: reason,
		})
		out[id] = h
	}
	if err := gradeRows.Err(); err != nil {
		return nil, err
	}
	for id, h := range out {
		h.Best14Rating = classifyBest14(h.DailyGrades, status[id], remaining[id])
		out[id] = h
	}
	return out, nil
}
