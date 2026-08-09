package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type healthClipLayout struct {
	videoCodec string
	audioCodec string
	audio      bool
	width      int
	height     int
}

func healthLayoutChanged(previous, current healthClipLayout) bool {
	if previous.videoCodec != current.videoCodec || previous.audio != current.audio {
		return true
	}
	if current.audio && previous.audioCodec != current.audioCodec {
		return true
	}
	return previous.width > 0 && previous.height > 0 && current.width > 0 && current.height > 0 &&
		(previous.width != current.width || previous.height != current.height)
}

// materializeRecordingWindowHealth persists exact timeline measurements for
// completed continuous windows. Missing historical windows are backfilled once;
// the last 48 hours are recalculated so late clip delivery repairs the summary.
// The recordings list reads only recording_health_summaries and never scans clips.
func materializeRecordingWindowHealth(ctx context.Context, pool *pgxpool.Pool, now time.Time) error {
	rows, err := pool.Query(ctx, `
		WITH recent_jobs AS (
		  SELECT j.id,j.recording_id,j.fire_at,j.window_end_at
		  FROM recording_jobs j
		  LEFT JOIN recording_window_health h ON h.recording_id=j.recording_id AND h.job_id=j.id
		  WHERE j.kind='continuous_window' AND j.window_end_at<=$1
		    AND j.window_end_at>$1::timestamptz-interval '48 hours'
		    AND (h.job_id IS NULL OR EXISTS (
		      SELECT 1 FROM recording_clips late
		      WHERE late.recording_id=j.recording_id AND late.clip_end_at>j.fire_at
		        AND late.clip_start_at<j.window_end_at AND late.created_at>h.calculated_at
		    ))
		), backfill_jobs AS (
		  SELECT j.id,j.recording_id,j.fire_at,j.window_end_at
		  FROM recording_jobs j
		  LEFT JOIN recording_window_health h ON h.recording_id=j.recording_id AND h.job_id=j.id
		  WHERE j.kind='continuous_window' AND j.window_end_at<=$1::timestamptz-interval '48 hours'
		    AND h.job_id IS NULL
		  ORDER BY j.window_end_at DESC,j.id DESC
		  LIMIT 32
		), candidate_jobs AS (
		  SELECT * FROM recent_jobs UNION ALL SELECT * FROM backfill_jobs
		)
		SELECT j.id,j.recording_id,j.fire_at,j.window_end_at,
		       c.clip_start_at,c.clip_end_at,
		       COALESCE(c.video_codec,''),COALESCE(c.audio_codec,''),COALESCE(c.audio_present,false),
		       COALESCE(c.video_width,0),COALESCE(c.video_height,0)
		FROM candidate_jobs j
		JOIN recordings r ON r.id=j.recording_id
		LEFT JOIN recording_clips c ON c.recording_id=j.recording_id
		  AND c.clip_end_at>j.fire_at AND c.clip_start_at<j.window_end_at
		ORDER BY j.recording_id,j.id,c.clip_start_at,c.id
	`, now.UTC())
	if err != nil {
		return fmt.Errorf("query recording windows for health materialization: %w", err)
	}
	defer rows.Close()

	type pendingWindow struct {
		jobID, recordingID int64
		start, end         time.Time
		clips              [][2]time.Time
		layoutChanges      int
		clipCount          int
		previousLayout     *healthClipLayout
	}
	var current *pendingWindow
	flush := func() error {
		if current == nil {
			return nil
		}
		metrics := measureStitchWindow(current.start, current.end, current.clips)
		_, err := pool.Exec(ctx, `
			INSERT INTO recording_window_health (
			  recording_id,job_id,window_start_at,window_end_at,expected_seconds,
			  covered_seconds,coverage_pct,largest_gap_seconds,gap_count,
			  overlap_count,overlap_seconds,longest_run_seconds,layout_change_count,clip_count,calculated_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
			ON CONFLICT (recording_id,job_id) DO UPDATE SET
			  window_start_at=EXCLUDED.window_start_at,window_end_at=EXCLUDED.window_end_at,
			  expected_seconds=EXCLUDED.expected_seconds,covered_seconds=EXCLUDED.covered_seconds,
			  coverage_pct=EXCLUDED.coverage_pct,largest_gap_seconds=EXCLUDED.largest_gap_seconds,
			  gap_count=EXCLUDED.gap_count,overlap_count=EXCLUDED.overlap_count,
			  overlap_seconds=EXCLUDED.overlap_seconds,longest_run_seconds=EXCLUDED.longest_run_seconds,
			  layout_change_count=EXCLUDED.layout_change_count,clip_count=EXCLUDED.clip_count,
			  calculated_at=EXCLUDED.calculated_at
		`, current.recordingID, current.jobID, current.start, current.end,
			int64(current.end.Sub(current.start)/time.Second), metrics.covered.Seconds(),
			metrics.coveragePct, metrics.maxGap.Seconds(), metrics.gapClips, metrics.overlapClips,
			metrics.overlapSeconds, metrics.longestRun.Seconds(), current.layoutChanges, current.clipCount, now.UTC())
		return err
	}

	for rows.Next() {
		var jobID, recordingID int64
		var start, end time.Time
		var clipStart, clipEnd *time.Time
		var layout healthClipLayout
		if err := rows.Scan(&jobID, &recordingID, &start, &end, &clipStart, &clipEnd,
			&layout.videoCodec, &layout.audioCodec, &layout.audio, &layout.width, &layout.height); err != nil {
			return fmt.Errorf("scan recording window health input: %w", err)
		}
		if current == nil || current.jobID != jobID || current.recordingID != recordingID {
			if err := flush(); err != nil {
				return fmt.Errorf("persist recording window health: %w", err)
			}
			current = &pendingWindow{jobID: jobID, recordingID: recordingID, start: start.UTC(), end: end.UTC()}
		}
		if clipStart == nil || clipEnd == nil {
			continue
		}
		current.clips = append(current.clips, [2]time.Time{clipStart.UTC(), clipEnd.UTC()})
		current.clipCount++
		if current.previousLayout != nil && healthLayoutChanged(*current.previousLayout, layout) {
			current.layoutChanges++
		}
		copy := layout
		current.previousLayout = &copy
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate recording window health input: %w", err)
	}
	rows.Close()
	if err := flush(); err != nil {
		return fmt.Errorf("persist final recording window health: %w", err)
	}
	return refreshRecordingHealthSummaries(ctx, pool, now.UTC())
}

func refreshRecordingHealthSummaries(ctx context.Context, pool *pgxpool.Pool, now time.Time) error {
	_, err := pool.Exec(ctx, `
		WITH aggregates AS (
		  SELECT recording_id,
		    COALESCE(SUM(expected_seconds) FILTER (WHERE window_end_at>$1::timestamptz-interval '48 hours'),0)::bigint AS recent_expected,
		    COALESCE(SUM(covered_seconds) FILTER (WHERE window_end_at>$1::timestamptz-interval '48 hours'),0)::double precision AS recent_covered,
		    COALESCE(MAX(largest_gap_seconds) FILTER (WHERE window_end_at>$1::timestamptz-interval '48 hours'),0)::double precision AS recent_largest_gap,
		    COALESCE(SUM(gap_count) FILTER (WHERE window_end_at>$1::timestamptz-interval '48 hours'),0)::bigint AS recent_gaps,
		    COALESCE(SUM(overlap_count) FILTER (WHERE window_end_at>$1::timestamptz-interval '48 hours'),0)::bigint AS recent_overlaps,
		    COALESCE(SUM(overlap_seconds) FILTER (WHERE window_end_at>$1::timestamptz-interval '48 hours'),0)::double precision AS recent_overlap_seconds,
		    COALESCE(SUM(layout_change_count) FILTER (WHERE window_end_at>$1::timestamptz-interval '48 hours'),0)::bigint AS recent_layout_changes,
		    COUNT(*) FILTER (WHERE window_end_at>$1::timestamptz-interval '48 hours')::int AS recent_windows,
		    SUM(expected_seconds)::bigint AS lifetime_expected,SUM(covered_seconds)::double precision AS lifetime_covered,
		    MAX(largest_gap_seconds)::double precision AS lifetime_largest_gap,SUM(gap_count)::bigint AS lifetime_gaps,
		    SUM(overlap_count)::bigint AS lifetime_overlaps,SUM(overlap_seconds)::double precision AS lifetime_overlap_seconds,
		    SUM(layout_change_count)::bigint AS lifetime_layout_changes,COUNT(*)::int AS lifetime_windows
		  FROM recording_window_health GROUP BY recording_id
		), durable AS (
		  SELECT aggregates.*,
		    GREATEST(
		      COALESCE((SELECT prior.lifetime_expected_window_count FROM recording_health_summaries prior WHERE prior.recording_id=aggregates.recording_id),0),
		      lifetime_windows + (
		        SELECT COUNT(*)::int FROM recording_jobs j
		        LEFT JOIN recording_window_health missing ON missing.recording_id=j.recording_id AND missing.job_id=j.id
		        WHERE j.recording_id=aggregates.recording_id AND j.kind='continuous_window'
		          AND j.window_end_at<=$1 AND missing.job_id IS NULL
		      )
		    ) AS expected_lifetime_windows
		  FROM aggregates
		)
		INSERT INTO recording_health_summaries (
		  recording_id,recent_expected_seconds,recent_covered_seconds,recent_coverage_pct,
		  recent_largest_gap_seconds,recent_gap_count,recent_overlap_count,recent_overlap_seconds,
		  recent_layout_change_count,recent_window_count,lifetime_expected_seconds,lifetime_covered_seconds,
		  lifetime_coverage_pct,lifetime_largest_gap_seconds,lifetime_gap_count,lifetime_overlap_count,
		  lifetime_overlap_seconds,lifetime_layout_change_count,lifetime_window_count,
		  lifetime_expected_window_count,lifetime_complete,calculated_at
		)
		SELECT recording_id,recent_expected,recent_covered,
		  CASE WHEN recent_expected>0 THEN LEAST(100,100*recent_covered/recent_expected) END,
		  recent_largest_gap,recent_gaps,recent_overlaps,recent_overlap_seconds,recent_layout_changes,recent_windows,
		  lifetime_expected,lifetime_covered,
		  CASE WHEN lifetime_expected>0 THEN LEAST(100,100*lifetime_covered/lifetime_expected) END,
		  lifetime_largest_gap,lifetime_gaps,lifetime_overlaps,lifetime_overlap_seconds,lifetime_layout_changes,lifetime_windows,
		  expected_lifetime_windows,lifetime_windows>=expected_lifetime_windows,$1
		FROM durable
		ON CONFLICT (recording_id) DO UPDATE SET
		  recent_expected_seconds=EXCLUDED.recent_expected_seconds,recent_covered_seconds=EXCLUDED.recent_covered_seconds,
		  recent_coverage_pct=EXCLUDED.recent_coverage_pct,recent_largest_gap_seconds=EXCLUDED.recent_largest_gap_seconds,
		  recent_gap_count=EXCLUDED.recent_gap_count,recent_overlap_count=EXCLUDED.recent_overlap_count,
		  recent_overlap_seconds=EXCLUDED.recent_overlap_seconds,recent_layout_change_count=EXCLUDED.recent_layout_change_count,
		  recent_window_count=EXCLUDED.recent_window_count,lifetime_expected_seconds=EXCLUDED.lifetime_expected_seconds,
		  lifetime_covered_seconds=EXCLUDED.lifetime_covered_seconds,lifetime_coverage_pct=EXCLUDED.lifetime_coverage_pct,
		  lifetime_largest_gap_seconds=EXCLUDED.lifetime_largest_gap_seconds,lifetime_gap_count=EXCLUDED.lifetime_gap_count,
		  lifetime_overlap_count=EXCLUDED.lifetime_overlap_count,lifetime_overlap_seconds=EXCLUDED.lifetime_overlap_seconds,
		  lifetime_layout_change_count=EXCLUDED.lifetime_layout_change_count,lifetime_window_count=EXCLUDED.lifetime_window_count,
		  lifetime_expected_window_count=GREATEST(recording_health_summaries.lifetime_expected_window_count,EXCLUDED.lifetime_expected_window_count),
		  lifetime_complete=EXCLUDED.lifetime_complete,calculated_at=EXCLUDED.calculated_at
	`, now)
	if err != nil {
		return fmt.Errorf("refresh recording health summaries: %w", err)
	}
	return nil
}
