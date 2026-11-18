package api

import (
	"context"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
)

// nasStitchHourMissingClipsSQL is the exact per-hour NAS presence predicate
// stitchPlanNext uses. It requires the outer query to alias the scope window as
// s, the recording as r and one clip of that hour as c, and it is true when at
// least one clip of that hour (inside the scope window) has no present NAS
// inventory row bound to its display path, size and SHA-256.
//
// The read-only backlog report below evaluates the same predicate; it must never
// be copied, because the two answers would silently diverge.
const nasStitchHourMissingClipsSQL = `SELECT 1 FROM recording_clips c2 LEFT JOIN nas_inventory_files n2 ON n2.clip_id=c2.id AND n2.connection_id=13 AND n2.state='present' AND n2.relative_path=c2.display_path AND n2.sha256=c2.sha256 AND n2.size_bytes=c2.size_bytes
 WHERE c2.recording_id=r.id AND c2.clip_start_at>=date_trunc('hour',c.clip_start_at) AND c2.clip_start_at<date_trunc('hour',c.clip_start_at)+interval '1 hour' AND c2.clip_start_at>=s.start_at AND c2.clip_end_at<=s.end_at AND n2.clip_id IS NULL`

// nasStitchHourPresentSQL is the same "every clip is exactly on the NAS" test,
// written positively so a report can count ready hours in one pass.
const nasStitchHourPresentSQL = `NOT EXISTS(` + nasStitchHourMissingClipsSQL + `)`

type nasStitchBacklogQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// NASStitchBacklogStream is one stream's stitching backlog.
type NASStitchBacklogStream struct {
	StreamID   int64     `json:"stream_id"`
	WindowFrom time.Time `json:"window_start"`
	WindowTo   time.Time `json:"window_end"`
	// WindowSource is "scope" for an approved nas_stitch_scopes window and
	// "keep28_policy" for the pinned policy fallback.
	WindowSource      string         `json:"window_source"`
	ScopeIDs          []int64        `json:"scope_ids,omitempty"`
	RecordingHours    int            `json:"recording_hours"`
	ReadyHours        int            `json:"ready_hours"`
	MissingClipsHours int            `json:"missing_clips_hours"`
	TaskHoursByState  map[string]int `json:"task_hours_by_state"`
	// RemainingReadyHours is ready hours with no task yet: what the lane can
	// actually plan next.
	RemainingReadyHours int `json:"remaining_ready_hours"`
}

// NASStitchBacklogReport is the whole read-only answer.
type NASStitchBacklogReport struct {
	Policy  string                   `json:"policy"`
	Source  string                   `json:"window_source"`
	Observe time.Time                `json:"observed_at"`
	Streams []NASStitchBacklogStream `json:"streams"`
	Total   NASStitchBacklogStream   `json:"total"`
}

type nasStitchBacklogWindow struct {
	streamID   int64
	start, end time.Time
	source     string
	scopeIDs   []int64
}

// NASStitchBacklog counts stitchable hours without writing anything. It uses
// the current approved nas_stitch_scopes windows, and falls back to the pinned
// keep28 policy windows when no scope is approved. Only reads.
func NASStitchBacklog(ctx context.Context, q nasStitchBacklogQuerier) (NASStitchBacklogReport, error) {
	report := NASStitchBacklogReport{Policy: nasStitchPolicy, Streams: []NASStitchBacklogStream{}}
	report.Observe = time.Now().UTC()
	windows, err := nasStitchBacklogWindows(ctx, q)
	if err != nil {
		return report, err
	}
	if len(windows) == 0 {
		return report, nil
	}
	report.Source = windows[0].source
	ids := make([]int64, len(windows))
	starts := make([]time.Time, len(windows))
	ends := make([]time.Time, len(windows))
	for n, w := range windows {
		ids[n], starts[n], ends[n] = w.streamID, w.start, w.end
	}
	// One pass over every window. c is one clip per hour (the planner's own
	// shape), so nasStitchHourPresentSQL is evaluated exactly as the planner
	// evaluates it.
	rows, err := q.Query(ctx, `
WITH w AS (SELECT * FROM unnest($1::bigint[],$2::timestamptz[],$3::timestamptz[]) AS w(stream_id,start_at,end_at)),
hours AS (
 SELECT s.stream_id,r.id AS recording_id,date_trunc('hour',c.clip_start_at) AS hour_at,count(*) AS clips
 FROM w s JOIN recordings r ON r.stream_id=s.stream_id AND r.account_id=47
 JOIN recording_clips c ON c.recording_id=r.id AND c.clip_start_at>=s.start_at AND c.clip_end_at<=s.end_at
 GROUP BY 1,2,3),
ready AS (
 SELECT DISTINCT s.stream_id,r.id AS recording_id,date_trunc('hour',c.clip_start_at) AS hour_at
 FROM w s JOIN recordings r ON r.stream_id=s.stream_id AND r.account_id=47
 JOIN recording_clips c ON c.recording_id=r.id AND c.clip_start_at>=s.start_at AND c.clip_end_at<=s.end_at
 JOIN nas_inventory_files n ON n.clip_id=c.id AND n.connection_id=13 AND n.state='present' AND n.sha256=c.sha256 AND n.size_bytes=c.size_bytes
 WHERE `+nasStitchHourPresentSQL+`)
SELECT s.stream_id,
 (SELECT count(*) FROM hours h WHERE h.stream_id=s.stream_id),
 (SELECT count(*) FROM ready d WHERE d.stream_id=s.stream_id)
FROM w s ORDER BY s.stream_id`, ids, starts, ends)
	if err != nil {
		return report, err
	}
	type counts struct{ recording, ready int }
	byStream := map[int64]*counts{}
	var order []int64
	for rows.Next() {
		var sid int64
		var c counts
		if err = rows.Scan(&sid, &c.recording, &c.ready); err != nil {
			rows.Close()
			return report, err
		}
		byStream[sid] = &c
		order = append(order, sid)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return report, err
	}
	rows.Close()
	// Existing tasks for these windows, by state. Tasks are keyed to their
	// recording's stream, so a window change cannot hide an existing hour.
	tasks := map[int64]map[string]int{}
	taskRows, err := q.Query(ctx, `
WITH w AS (SELECT * FROM unnest($1::bigint[],$2::timestamptz[],$3::timestamptz[]) AS w(stream_id,start_at,end_at)),
task AS (
 SELECT DISTINCT s.stream_id,t.state,date_trunc('hour',t.hour_at) AS hour_at
 FROM w s JOIN recordings r ON r.stream_id=s.stream_id AND r.account_id=47
 JOIN nas_stitch_tasks t ON t.recording_id=r.id AND t.hour_at>=s.start_at AND t.hour_at<s.end_at)
SELECT stream_id,state,count(*) FROM task GROUP BY 1,2 ORDER BY 1,2`, ids, starts, ends)
	if err != nil {
		return report, err
	}
	for taskRows.Next() {
		var sid int64
		var state string
		var n int
		if err = taskRows.Scan(&sid, &state, &n); err != nil {
			taskRows.Close()
			return report, err
		}
		if tasks[sid] == nil {
			tasks[sid] = map[string]int{}
		}
		tasks[sid][state] += n
	}
	if err = taskRows.Err(); err != nil {
		taskRows.Close()
		return report, err
	}
	taskRows.Close()

	report.Total = NASStitchBacklogStream{TaskHoursByState: map[string]int{}}
	for _, w := range windows {
		c := byStream[w.streamID]
		if c == nil {
			c = &counts{}
		}
		byState := tasks[w.streamID]
		if byState == nil {
			byState = map[string]int{}
		}
		tasked := 0
		for _, n := range byState {
			tasked += n
		}
		entry := NASStitchBacklogStream{
			StreamID: w.streamID, WindowFrom: w.start.UTC(), WindowTo: w.end.UTC(), WindowSource: w.source,
			ScopeIDs: w.scopeIDs, RecordingHours: c.recording, ReadyHours: c.ready,
			MissingClipsHours: c.recording - c.ready, TaskHoursByState: byState,
		}
		if remaining := c.ready - tasked; remaining > 0 {
			entry.RemainingReadyHours = remaining
		}
		report.Streams = append(report.Streams, entry)
		report.Total.RecordingHours += entry.RecordingHours
		report.Total.ReadyHours += entry.ReadyHours
		report.Total.MissingClipsHours += entry.MissingClipsHours
		report.Total.RemainingReadyHours += entry.RemainingReadyHours
		for state, n := range byState {
			report.Total.TaskHoursByState[state] += n
		}
	}
	sort.Slice(report.Streams, func(a, b int) bool { return report.Streams[a].StreamID < report.Streams[b].StreamID })
	return report, nil
}

// nasStitchBacklogWindows prefers live approved scopes and otherwise returns the
// pinned keep28 policy windows. Both are read-only views of existing authority.
func nasStitchBacklogWindows(ctx context.Context, q nasStitchBacklogQuerier) ([]nasStitchBacklogWindow, error) {
	rows, err := q.Query(ctx, `SELECT id,stream_id,start_at,end_at FROM nas_stitch_scopes
 WHERE NOT paused AND approved_until>now() ORDER BY stream_id,id`)
	if err != nil {
		return nil, err
	}
	byStream := map[int64]*nasStitchBacklogWindow{}
	var order []int64
	for rows.Next() {
		var id, sid int64
		var start, end time.Time
		if err = rows.Scan(&id, &sid, &start, &end); err != nil {
			rows.Close()
			return nil, err
		}
		w := byStream[sid]
		if w == nil {
			w = &nasStitchBacklogWindow{streamID: sid, start: start, end: end, source: "scope"}
			byStream[sid] = w
			order = append(order, sid)
		}
		// Overlapping approved scopes for one stream widen the reported window
		// only; the planner still emits one task per (recording,hour).
		if start.Before(w.start) {
			w.start = start
		}
		if end.After(w.end) {
			w.end = end
		}
		w.scopeIDs = append(w.scopeIDs, id)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	if len(order) != 0 {
		windows := make([]nasStitchBacklogWindow, 0, len(order))
		for _, sid := range order {
			windows = append(windows, *byStream[sid])
		}
		return windows, nil
	}
	_, policy, err := Keep28Policy()
	if err != nil {
		return nil, err
	}
	windows := make([]nasStitchBacklogWindow, 0, len(policy))
	for sid, w := range policy {
		windows = append(windows, nasStitchBacklogWindow{streamID: sid, start: w.Start, end: w.End, source: "keep28_policy"})
	}
	sort.Slice(windows, func(a, b int) bool { return windows[a].streamID < windows[b].streamID })
	return windows, nil
}
