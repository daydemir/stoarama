package api

import (
	"errors"
	"net/http"
	"sort"
	"time"

	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

type streakWindow struct {
	End    time.Time `json:"window_end_at"`
	Grade  string    `json:"grade"`
	Reason string    `json:"reason"`
}

type streakPriorityRecording struct {
	RecordingID      int64          `json:"recording_id"`
	Name             string         `json:"name"`
	Lifecycle        string         `json:"lifecycle"`
	CurrentStreak    int            `json:"current_streak"`
	WindowsTo14      int            `json:"windows_to_14"`
	AcceptableInRun  int            `json:"acceptable_in_run"`
	Qualified14      bool           `json:"qualified_14"`
	Protection       string         `json:"protection"`
	Action           string         `json:"action"`
	CompletedWindows int            `json:"completed_windows_examined"`
	RecentWindows    []streakWindow `json:"recent_windows"`
}

type streakPriorityResponse struct {
	GeneratedAt time.Time                 `json:"generated_at"`
	Scope       string                    `json:"scope"`
	Items       []streakPriorityRecording `json:"items"`
}

func gradePassesStreak(grade string) bool {
	return grade == "GREAT_CANDIDATE" || grade == "GOOD_CANDIDATE" || grade == "ACCEPTABLE_CANDIDATE"
}

func finishStreakPriority(item *streakPriorityRecording, recordingStatus string) {
	acceptable := 0
	for _, window := range item.RecentWindows {
		if !gradePassesStreak(window.Grade) || (window.Grade == "ACCEPTABLE_CANDIDATE" && acceptable == 1) {
			break
		}
		if window.Grade == "ACCEPTABLE_CANDIDATE" {
			acceptable++
		}
		item.CurrentStreak++
		if item.CurrentStreak == 14 {
			break
		}
	}
	item.AcceptableInRun = acceptable
	item.Qualified14 = item.CurrentStreak >= 14
	item.WindowsTo14 = 14 - item.CurrentStreak
	if item.WindowsTo14 < 0 {
		item.WindowsTo14 = 0
	}
	switch {
	case recordingStatus == "paused":
		item.Lifecycle = "candidate"
	case len(item.RecentWindows) < 2:
		item.Lifecycle = "probation"
	default:
		item.Lifecycle = "active"
	}
	switch {
	case item.Qualified14:
		item.Protection = "qualified_hold"
	case item.CurrentStreak >= 13:
		item.Protection = "critical_13_of_14"
	case item.CurrentStreak >= 10:
		item.Protection = "high"
	case item.CurrentStreak >= 5:
		item.Protection = "medium"
	default:
		item.Protection = "normal"
	}
	failures := 0
	for i, window := range item.RecentWindows {
		if i == 3 {
			break
		}
		if window.Grade == "FAILED" || window.Grade == "UNKNOWN" {
			failures++
		}
	}
	switch {
	case len(item.RecentWindows) <= 2 && failures > 0:
		item.Action = "early_failure_review_replace_or_reprobe"
	case failures >= 2:
		item.Action = "repeated_failure_replace_or_source_repair"
	case item.CurrentStreak >= 13:
		item.Action = "protect_no_nonessential_change"
	default:
		item.Action = "continue_monitoring"
	}
}

func (s *Server) handleAccountRecordingStreakPriority(w http.ResponseWriter, r *http.Request) {
	p, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	rows, err := s.pool.Query(r.Context(), `
		SELECT r.id,r.name,r.status,j.window_end_at,h.coverage_pct,h.largest_gap_seconds,
		       h.gap_over_30s_count,h.gap_over_5m_count,h.overlap_count,h.metric_version,h.expected_seconds,
		       CASE WHEN j.fire_at IS NULL THEN 0 ELSE extract(epoch FROM(j.window_end_at-j.fire_at))::bigint END,
		       COALESCE((SELECT count(*)::int FROM recording_jobs peer WHERE peer.recording_id=r.id AND peer.kind='continuous_window'
		          AND peer.fire_at=j.fire_at AND peer.window_end_at=j.window_end_at),0) job_count,
		       COALESCE(h.health_count,0),
		       EXISTS(SELECT 1 FROM recording_clips c WHERE c.recording_id=r.id AND c.clip_end_at>j.fire_at
		          AND c.clip_start_at<j.window_end_at AND c.created_at>h.calculated_at) late_clip
		FROM recordings r
		LEFT JOIN LATERAL (SELECT * FROM recording_jobs x WHERE x.recording_id=r.id AND x.kind='continuous_window' AND x.window_end_at<=now()
		              ORDER BY x.window_end_at DESC,x.id DESC LIMIT 30) j ON true
		LEFT JOIN LATERAL (SELECT count(*)::int health_count,max(coverage_pct) coverage_pct,max(largest_gap_seconds) largest_gap_seconds,
		              max(gap_over_30s_count) gap_over_30s_count,max(gap_over_5m_count) gap_over_5m_count,max(overlap_count) overlap_count,
		              max(metric_version) metric_version,max(expected_seconds) expected_seconds,max(calculated_at) calculated_at
		              FROM recording_window_health x WHERE x.recording_id=r.id AND x.job_id=j.id) h ON true
		WHERE r.account_id=$1 AND r.status IN('active','paused')
		ORDER BY r.id,j.window_end_at DESC,j.id DESC`, p.AccountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "read streak priority")
		return
	}
	defer rows.Close()
	items := map[int64]*streakPriorityRecording{}
	statuses := map[int64]string{}
	for rows.Next() {
		var id int64
		var name, status string
		var end *time.Time
		var coverage, gap *float64
		var over30, over5, overlaps, version *int
		var measuredExpected *int64
		var expected int64
		var jobs, health int
		var late bool
		if err := rows.Scan(&id, &name, &status, &end, &coverage, &gap, &over30, &over5, &overlaps, &version, &measuredExpected, &expected, &jobs, &health, &late); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "scan streak priority")
			return
		}
		item := items[id]
		if item == nil {
			item = &streakPriorityRecording{RecordingID: id, Name: name, RecentWindows: []streakWindow{}}
			items[id] = item
			statuses[id] = status
		}
		grade, reason := classifyQualificationTimeline(qualificationWindowMetrics{CoveragePct: coverage, LargestGap: gap, GapsOver30s: over30, GapsOver5m: over5, OverlapCount: overlaps, MetricVersion: version, ExpectedSeconds: expected, MeasuredExpected: measuredExpected, JobCount: jobs, HealthCount: health, LateClip: late})
		windowEnd := time.Time{}
		if end != nil {
			windowEnd = *end
		}
		item.RecentWindows = append(item.RecentWindows, streakWindow{End: windowEnd, Grade: grade, Reason: reason})
	}
	if err := rows.Err(); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusInternalServerError, "read streak priority")
		return
	}
	out := streakPriorityResponse{GeneratedAt: time.Now().UTC(), Scope: "dynamic active/probation/candidate recordings; timeline grades only; NAS and stitch certification remain separate", Items: []streakPriorityRecording{}}
	for id, item := range items {
		item.CompletedWindows = len(item.RecentWindows)
		finishStreakPriority(item, statuses[id])
		out.Items = append(out.Items, *item)
	}
	sort.Slice(out.Items, func(i, j int) bool {
		if out.Items[i].CurrentStreak != out.Items[j].CurrentStreak {
			return out.Items[i].CurrentStreak > out.Items[j].CurrentStreak
		}
		return out.Items[i].RecordingID < out.Items[j].RecordingID
	})
	util.WriteJSON(w, http.StatusOK, out)
}
