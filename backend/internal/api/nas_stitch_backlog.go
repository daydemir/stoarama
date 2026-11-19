package api

import (
	"context"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
)

const nasStitchMaxClips = 200

// These predicates are shared by the lease planner and the read-only report.
const nasStitchExactNASInventorySQL = `n2.clip_id=c2.id AND n2.connection_id=13 AND n2.state='present'
 AND n2.relative_path=c2.display_path AND n2.sha256=c2.sha256 AND n2.size_bytes=c2.size_bytes`
const nasStitchHourClipsSQL = `c2.recording_id=r.id AND c2.clip_start_at>=date_trunc('hour',c.clip_start_at)
 AND c2.clip_start_at<date_trunc('hour',c.clip_start_at)+interval '1 hour'
 AND c2.clip_start_at>=s.start_at AND c2.clip_end_at<=s.end_at`
const nasStitchHourPresentSQL = `NOT EXISTS(SELECT 1 FROM recording_clips c2
 LEFT JOIN nas_inventory_files n2 ON ` + nasStitchExactNASInventorySQL + `
 WHERE ` + nasStitchHourClipsSQL + ` AND n2.clip_id IS NULL)`
const nasStitchHourBoundsSQL = `count(*) BETWEEN 1 AND $1
 AND bool_and(c2.size_bytes>0 AND c2.clip_end_at>c2.clip_start_at) AND sum(c2.size_bytes)<=$2`

type nasStitchBacklogQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// NASStitchBacklogStream is one stream's backlog. WindowFrom and WindowTo
// enclose the selected scopes. Clips between disjoint scopes are excluded.
type NASStitchBacklogStream struct {
	StreamID                 int64          `json:"stream_id"`
	WindowFrom               time.Time      `json:"window_start"`
	WindowTo                 time.Time      `json:"window_end"`
	WindowSource             string         `json:"window_source"`
	ScopeIDs                 []int64        `json:"scope_ids,omitempty"`
	RecordingHours           int            `json:"recording_hours"`
	ReadyHours               int            `json:"ready_hours"`
	MissingClipsHours        int            `json:"missing_clips_hours"`
	TaskHoursByState         map[string]int `json:"task_hours_by_state"`
	RemainingReadyHours      int            `json:"remaining_ready_hours"`
	RemainingReadyInputBytes int64          `json:"remaining_ready_input_bytes"`
}

type NASStitchBacklogReport struct {
	Policy              string                   `json:"policy"`
	Source              string                   `json:"window_source"`
	Observe             time.Time                `json:"observed_at"`
	CleanupFreezeActive bool                     `json:"cleanup_freeze_active"`
	Streams             []NASStitchBacklogStream `json:"streams"`
	Total               NASStitchBacklogStream   `json:"total"`
}

type nasStitchBacklogWindow struct {
	scopeID, streamID int64
	start, end        time.Time
}

// NASStitchBacklog counts recording/hour units with exact NAS inventory. The
// keep28 view, used without a live scope, is a capacity estimate; it grants no
// lease authority. Remaining units exclude existing tasks and per-hour bounds.
func NASStitchBacklog(ctx context.Context, q nasStitchBacklogQuerier) (NASStitchBacklogReport, error) {
	report := NASStitchBacklogReport{
		Policy: nasStitchPolicy, Observe: time.Now().UTC(),
		Streams: []NASStitchBacklogStream{},
		Total:   NASStitchBacklogStream{TaskHoursByState: map[string]int{}},
	}
	windows, source, err := nasStitchBacklogWindows(ctx, q)
	if err != nil {
		return report, err
	}
	report.Source = source
	if len(windows) == 0 {
		return report, nil
	}
	entries := make(map[int64]*NASStitchBacklogStream)
	ids := make([]int64, 0, len(windows))
	starts, ends := make([]time.Time, 0, len(windows)), make([]time.Time, 0, len(windows))
	for _, w := range windows {
		ids, starts, ends = append(ids, w.streamID), append(starts, w.start), append(ends, w.end)
		e := entries[w.streamID]
		if e == nil {
			e = &NASStitchBacklogStream{StreamID: w.streamID, WindowSource: source, WindowFrom: w.start.UTC(), WindowTo: w.end.UTC()}
			entries[w.streamID] = e
		}
		if w.start.Before(e.WindowFrom) {
			e.WindowFrom = w.start.UTC()
		}
		if w.end.After(e.WindowTo) {
			e.WindowTo = w.end.UTC()
		}
		if w.scopeID != 0 {
			e.ScopeIDs = append(e.ScopeIDs, w.scopeID)
		}
	}

	rows, err := q.Query(ctx, `
WITH w AS (
 SELECT DISTINCT stream_id,start_at,end_at
 FROM unnest($3::bigint[],$4::timestamptz[],$5::timestamptz[]) AS windows(stream_id,start_at,end_at)
), scoped AS (
 SELECT s.stream_id,r.id AS recording_id,date_trunc('hour',c2.clip_start_at) AS hour_at,
        bool_and(n2.clip_id IS NOT NULL) AS nas_present,
        (`+nasStitchHourBoundsSQL+`) AS bounded,
        sum(c2.size_bytes) AS bytes
 FROM w s JOIN recordings r ON r.stream_id=s.stream_id AND r.account_id=47
 JOIN recording_clips c2 ON c2.recording_id=r.id AND c2.clip_start_at>=s.start_at AND c2.clip_end_at<=s.end_at
 LEFT JOIN nas_inventory_files n2 ON `+nasStitchExactNASInventorySQL+`
 GROUP BY s.stream_id,s.start_at,s.end_at,r.id,date_trunc('hour',c2.clip_start_at)
), hours AS (
 SELECT stream_id,recording_id,hour_at,bool_or(nas_present) AS nas_present,
        max(bytes) FILTER(WHERE nas_present AND bounded) AS ready_bytes
 FROM scoped GROUP BY stream_id,recording_id,hour_at
), by_state AS (
 SELECT h.stream_id,t.state,count(*) AS hours
 FROM hours h JOIN nas_stitch_tasks t USING(recording_id,hour_at) GROUP BY h.stream_id,t.state
)
SELECT s.stream_id,
 (SELECT count(*) FROM hours h WHERE h.stream_id=s.stream_id),
 (SELECT count(*) FROM hours h WHERE h.stream_id=s.stream_id AND h.nas_present),
 coalesce((SELECT jsonb_object_agg(state,hours) FROM by_state b WHERE b.stream_id=s.stream_id),'{}'),
 count(h.ready_bytes) FILTER(WHERE t.id IS NULL),
 coalesce(sum(h.ready_bytes) FILTER(WHERE t.id IS NULL),0)::bigint,
 EXISTS(SELECT 1 FROM nas_cleanup_frozen_streams)
FROM (SELECT DISTINCT stream_id FROM w) s
LEFT JOIN hours h ON h.stream_id=s.stream_id
 AND NOT EXISTS(SELECT 1 FROM nas_cleanup_frozen_streams f WHERE f.stream_id=h.stream_id)
LEFT JOIN nas_stitch_tasks t USING(recording_id,hour_at)
GROUP BY s.stream_id ORDER BY s.stream_id`, nasStitchMaxClips, nasStitchMaxBytes, ids, starts, ends)
	if err != nil {
		return report, err
	}
	defer rows.Close()
	for rows.Next() {
		var streamID int64
		var recording, ready, remaining int
		var states map[string]int
		var bytes int64
		if err := rows.Scan(&streamID, &recording, &ready, &states, &remaining, &bytes, &report.CleanupFreezeActive); err != nil {
			return report, err
		}
		e := entries[streamID]
		e.RecordingHours, e.ReadyHours, e.MissingClipsHours = recording, ready, recording-ready
		e.TaskHoursByState, e.RemainingReadyHours, e.RemainingReadyInputBytes = states, remaining, bytes
		report.Streams = append(report.Streams, *e)
		report.Total.RecordingHours += recording
		report.Total.ReadyHours += ready
		report.Total.MissingClipsHours += recording - ready
		report.Total.RemainingReadyHours += remaining
		report.Total.RemainingReadyInputBytes += bytes
		for state, n := range states {
			report.Total.TaskHoursByState[state] += n
		}
	}
	return report, rows.Err()
}

func nasStitchBacklogWindows(ctx context.Context, q nasStitchBacklogQuerier) ([]nasStitchBacklogWindow, string, error) {
	rows, err := q.Query(ctx, `SELECT id,stream_id,start_at,end_at FROM nas_stitch_scopes
 WHERE NOT paused AND approved_until>now() ORDER BY stream_id,id`)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var windows []nasStitchBacklogWindow
	for rows.Next() {
		var w nasStitchBacklogWindow
		if err := rows.Scan(&w.scopeID, &w.streamID, &w.start, &w.end); err != nil {
			return nil, "", err
		}
		windows = append(windows, w)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	if len(windows) != 0 {
		return windows, "scope", nil
	}
	_, policy, err := Keep28Policy()
	if err != nil {
		return nil, "", err
	}
	for sid, w := range policy {
		windows = append(windows, nasStitchBacklogWindow{streamID: sid, start: w.Start, end: w.End})
	}
	sort.Slice(windows, func(a, b int) bool { return windows[a].streamID < windows[b].streamID })
	return windows, "keep28_policy", nil
}
