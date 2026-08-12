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
	GeneratedAt               time.Time                 `json:"generated_at"`
	Scope                     string                    `json:"scope"`
	PausedCandidateLimit      int                       `json:"paused_candidate_limit"`
	PausedCandidatesTruncated bool                      `json:"paused_candidates_truncated"`
	Items                     []streakPriorityRecording `json:"items"`
}

var streakPriorityNow = func() time.Time { return time.Now().UTC() }

const streakPriorityFactsSQL = `
		WITH eligible AS (
		  SELECT * FROM recordings WHERE account_id=$1 AND mode='continuous' AND status='active' AND daily_window_start='08:00' AND daily_window_end='20:00'
		  UNION ALL
		  SELECT * FROM (SELECT * FROM recordings WHERE account_id=$1 AND mode='continuous' AND status='paused' AND paused_at>=$2::timestamptz-interval '7 days' AND daily_window_start='08:00' AND daily_window_end='20:00' ORDER BY paused_at DESC,id LIMIT 100) p
		), expected AS (
		  SELECT r.id,r.name,r.status,(d.day::date+'08:00:00'::time) AT TIME ZONE r.cron_timezone fire_at,
		         (d.day::date+'20:00:00'::time) AT TIME ZONE r.cron_timezone window_end_at,
		         count(j.id)::int job_count,min(j.id) job_id
		  FROM eligible r CROSS JOIN LATERAL generate_series(($2::timestamptz AT TIME ZONE r.cron_timezone)::date-60,($2::timestamptz AT TIME ZONE r.cron_timezone)::date,interval '1 day') d(day)
		  LEFT JOIN recording_jobs j ON j.recording_id=r.id AND j.kind='continuous_window' AND j.fire_at=(d.day::date+'08:00:00'::time) AT TIME ZONE r.cron_timezone AND j.window_end_at=(d.day::date+'20:00:00'::time) AT TIME ZONE r.cron_timezone
		  WHERE ((d.day::date+'20:00:00'::time) AT TIME ZONE r.cron_timezone)<=$2::timestamptz AND ((d.day::date+'08:00:00'::time) AT TIME ZONE r.cron_timezone)>=r.start_at
		    AND (r.end_at IS NULL OR ((d.day::date+'20:00:00'::time) AT TIME ZONE r.cron_timezone)<=r.end_at) AND (r.active_weekdays & (1 << (extract(isodow FROM d.day::date)::int-1)))<>0
		  GROUP BY r.id,r.name,r.status,d.day,r.cron_timezone
		), recent AS (SELECT *,row_number() OVER(PARTITION BY id ORDER BY window_end_at DESC) rn FROM expected)
		SELECT q.id,q.name,q.status,q.window_end_at,h.coverage_pct,h.largest_gap_seconds,h.gap_over_30s_count,h.gap_over_5m_count,h.overlap_count,h.metric_version,h.expected_seconds,
		       extract(epoch FROM(q.window_end_at-q.fire_at))::bigint,q.job_count,COALESCE(h.health_count,0),
		       EXISTS(SELECT 1 FROM recording_clips c WHERE c.recording_id=q.id AND c.clip_end_at>q.fire_at AND c.clip_start_at<q.window_end_at AND c.created_at>h.calculated_at) late_clip
		FROM recent q LEFT JOIN LATERAL (SELECT count(*)::int health_count,max(coverage_pct) coverage_pct,max(largest_gap_seconds) largest_gap_seconds,max(gap_over_30s_count) gap_over_30s_count,
		 max(gap_over_5m_count) gap_over_5m_count,max(overlap_count) overlap_count,max(metric_version) metric_version,max(expected_seconds) expected_seconds,max(calculated_at) calculated_at
		 FROM recording_window_health x WHERE x.recording_id=q.id AND x.job_id=q.job_id AND x.calculated_at>=q.window_end_at AND x.calculated_at<=$2::timestamptz+interval '5 minutes') h ON true
		WHERE q.rn<=30 ORDER BY q.id,q.window_end_at DESC`

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
	tx, err := s.pool.BeginTx(r.Context(), pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "start streak snapshot")
		return
	}
	defer tx.Rollback(r.Context())
	asOf := streakPriorityNow()
	items := map[int64]*streakPriorityRecording{}
	statuses := map[int64]string{}
	eligibleRows, err := tx.Query(r.Context(), `WITH active AS (SELECT id,name,status FROM recordings WHERE account_id=$1 AND mode='continuous' AND status='active' AND daily_window_start='08:00' AND daily_window_end='20:00'), paused AS (SELECT id,name,status FROM recordings WHERE account_id=$1 AND mode='continuous' AND status='paused' AND paused_at>=$2-interval '7 days' AND daily_window_start='08:00' AND daily_window_end='20:00' ORDER BY paused_at DESC,id LIMIT 100) SELECT * FROM active UNION ALL SELECT * FROM paused ORDER BY id`, p.AccountID, asOf)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "read streak candidates")
		return
	}
	for eligibleRows.Next() {
		var id int64
		var name, status string
		if err := eligibleRows.Scan(&id, &name, &status); err != nil {
			eligibleRows.Close()
			util.WriteError(w, 500, "scan streak candidates")
			return
		}
		items[id] = &streakPriorityRecording{RecordingID: id, Name: name, RecentWindows: []streakWindow{}}
		statuses[id] = status
	}
	if err := eligibleRows.Err(); err != nil {
		eligibleRows.Close()
		util.WriteError(w, 500, "read streak candidates")
		return
	}
	eligibleRows.Close()
	rows, err := tx.Query(r.Context(), streakPriorityFactsSQL, p.AccountID, asOf)

	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "read streak priority")
		return
	}
	defer rows.Close()
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
			continue
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
	var pausedTotal int
	if err := tx.QueryRow(r.Context(), `SELECT count(*) FROM recordings WHERE account_id=$1 AND mode='continuous' AND status='paused' AND paused_at>=$2-interval '7 days' AND daily_window_start='08:00' AND daily_window_end='20:00'`, p.AccountID, asOf).Scan(&pausedTotal); err != nil {
		util.WriteError(w, 500, "count streak candidates")
		return
	}
	out := streakPriorityResponse{GeneratedAt: time.Now().UTC(), Scope: "dynamic active/probation/candidate recordings; timeline grades only; NAS and stitch certification remain separate", PausedCandidateLimit: 100, PausedCandidatesTruncated: pausedTotal > 100, Items: []streakPriorityRecording{}}
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
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, 500, "finish streak snapshot")
		return
	}
	util.WriteJSON(w, http.StatusOK, out)
}
