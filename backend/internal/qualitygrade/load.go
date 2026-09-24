package qualitygrade

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
)

// Querier is the read surface Load needs (a pool, a conn, or a transaction).
type Querier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

// Window is one graded completed continuous window.
type Window struct {
	JobID       int64     `json:"job_id"`
	LocalDate   time.Time `json:"local_date"`
	WindowEndAt time.Time `json:"window_end_at"`
	Grade       Grade     `json:"grade"`
	CoveragePct *float64  `json:"coverage_pct"`
}

// Recording is one active continuous recording's grade history and tiers.
type Recording struct {
	RecordingID int64      `json:"recording_id"`
	AccountID   int64      `json:"account_id"`
	Name        string     `json:"name"`
	Windows     []Window   `json:"windows"`
	Tiers       []Progress `json:"tiers"`
}

// Latest returns the most recent graded window, if any.
func (r Recording) Latest() (Window, bool) {
	if len(r.Windows) == 0 {
		return Window{}, false
	}
	return r.Windows[len(r.Windows)-1], true
}

// TierProgress returns one tier's progress.
func (r Recording) TierProgress(t Tier) Progress {
	for _, p := range r.Tiers {
		if p.Tier == t {
			return p
		}
	}
	return Progress{Tier: t, DaysToGo: RunLength}
}

// UnmeasuredGrace is how long after a window closes its health row may still
// be pending materialization. Until then a missing row is not yet evidence of
// an unknown grade, so Load omits such a window rather than grading it unknown.
const UnmeasuredGrace = 2 * time.Hour

// loadSQL reads completed continuous windows for active continuous recordings.
// A health row calculated before the window closed is outdated and grades as
// unknown. When a local day holds more than one window the latest wins.
const loadSQL = `
	SELECT r.id, r.account_id, r.name, j.id,
	       (j.fire_at AT TIME ZONE r.cron_timezone)::date,
	       j.window_end_at,
	       h.job_id IS NOT NULL AND h.calculated_at >= j.window_end_at,
	       h.coverage_pct, h.largest_gap_seconds, h.gap_over_30s_count, h.gap_over_5m_count,
	       h.overlap_count, h.metric_version, COALESCE(h.clip_count, 0)
	FROM recordings r
	JOIN recording_jobs j ON j.recording_id = r.id
	  AND j.kind = 'continuous_window'
	  AND j.window_end_at <= $2
	  AND j.window_end_at > $2 - make_interval(days => $3)
	LEFT JOIN recording_window_health h ON h.recording_id = r.id AND h.job_id = j.id
	WHERE r.status = 'active' AND r.mode = 'continuous'
	  AND ($1::bigint = 0 OR r.account_id = $1)
	ORDER BY r.id, j.fire_at, j.id`

// Load grades the last `days` of completed windows for every active continuous
// recording of accountID (0 means every account) as of now.
func Load(ctx context.Context, q Querier, accountID int64, now time.Time, days int) ([]Recording, error) {
	if days <= 0 {
		return nil, fmt.Errorf("days must be positive")
	}
	rows, err := q.Query(ctx, loadSQL, accountID, now, days)
	if err != nil {
		return nil, fmt.Errorf("load window grades: %w", err)
	}
	defer rows.Close()
	byID := map[int64]*Recording{}
	order := []int64{}
	for rows.Next() {
		var (
			rec       Recording
			w         Window
			localDate time.Time
			m         WindowMetrics
		)
		if err := rows.Scan(&rec.RecordingID, &rec.AccountID, &rec.Name, &w.JobID, &localDate, &w.WindowEndAt,
			&m.Measured, &m.CoveragePct, &m.LargestGapSeconds, &m.GapsOver30s, &m.GapsOver5m,
			&m.Overlaps, &m.MetricVersion, &m.ClipCount); err != nil {
			return nil, fmt.Errorf("scan window grade: %w", err)
		}
		if !m.Measured && now.Sub(w.WindowEndAt) < UnmeasuredGrace {
			continue
		}
		w.LocalDate = time.Date(localDate.Year(), localDate.Month(), localDate.Day(), 0, 0, 0, 0, time.UTC)
		w.Grade = Classify(m)
		w.CoveragePct = m.CoveragePct
		r := byID[rec.RecordingID]
		if r == nil {
			rec.Windows = []Window{}
			r = &rec
			byID[rec.RecordingID] = r
			order = append(order, rec.RecordingID)
		}
		if n := len(r.Windows); n > 0 && r.Windows[n-1].LocalDate.Equal(w.LocalDate) {
			r.Windows[n-1] = w
			continue
		}
		r.Windows = append(r.Windows, w)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate window grades: %w", err)
	}
	out := make([]Recording, 0, len(order))
	for _, id := range order {
		r := byID[id]
		days := make([]Day, 0, len(r.Windows))
		for _, w := range r.Windows {
			days = append(days, Day{Date: w.LocalDate, Grade: w.Grade})
		}
		r.Tiers = Evaluate(FillMissing(days))
		out = append(out, *r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RecordingID < out[j].RecordingID })
	return out, nil
}
