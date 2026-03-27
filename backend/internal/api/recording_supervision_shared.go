package api

import (
	"context"
	"fmt"
	"time"
)

type recordingSupervisionMetrics struct {
	LossRate10m      float64
	LossRate2h       float64
	ProcessIssues2h  int64
	OutageEpisodes2h int64
}

type recordingSupervisionInput struct {
	RecordingState  string
	ModeClass       string
	ServerID        string
	RuntimeStatus   string
	AssignedAt      *time.Time
	LastFrameAt     *time.Time
	StreamUpdatedAt time.Time
	Metrics         recordingSupervisionMetrics
}

func classifyRecordingSupervision(now time.Time, input recordingSupervisionInput) (state, reason string, unhealthySince *time.Time) {
	state = "healthy"
	reason = "fresh_captures"
	if input.RecordingState != "on" {
		return "off", "recording_disabled", nil
	}
	downThreshold := 10 * time.Minute
	switch {
	case input.ServerID == "":
		if now.Sub(input.StreamUpdatedAt.UTC()) >= downThreshold {
			return "down_10m", "recording_on_but_unassigned", &input.StreamUpdatedAt
		}
		return "healthy", "waiting_for_assignment", nil
	case input.LastFrameAt == nil:
		if input.AssignedAt != nil && now.Sub(input.AssignedAt.UTC()) >= downThreshold {
			return "down_10m", "no_successful_frames", input.AssignedAt
		}
		return "healthy", "warmup", nil
	default:
		lastFrameAt := input.LastFrameAt.UTC()
		age := now.Sub(lastFrameAt)
		if age < 0 {
			age = 0
		}
		if age >= downThreshold {
			return "down_10m", "stale_frames_10m", input.LastFrameAt
		}
		if input.RuntimeStatus == "stopped" || input.RuntimeStatus == "error" {
			return "down_10m", "capture_runtime_" + input.RuntimeStatus, input.LastFrameAt
		}
		if input.Metrics.OutageEpisodes2h >= 3 {
			unhealthySince = input.AssignedAt
			if unhealthySince == nil {
				unhealthySince = input.LastFrameAt
			}
			return "spotty_2h", "outage_episodes_2h", unhealthySince
		}
		if input.Metrics.ProcessIssues2h >= 3 {
			unhealthySince = input.AssignedAt
			if unhealthySince == nil {
				unhealthySince = input.LastFrameAt
			}
			return "spotty_2h", "process_restarts_2h", unhealthySince
		}
		if input.Metrics.LossRate2h > 20 {
			unhealthySince = input.AssignedAt
			if unhealthySince == nil {
				unhealthySince = input.LastFrameAt
			}
			return "spotty_2h", "loss_rate_2h", unhealthySince
		}
		return state, reason, nil
	}
}

func dashboardHealthFromSupervision(state string) string {
	switch state {
	case "down_10m":
		return "stale"
	case "spotty_2h":
		return "degraded"
	case "healthy":
		return "healthy"
	default:
		return "off"
	}
}

func expectedCapturesForWindow(modeClass string, recordingIntervalSec int, window time.Duration) int64 {
	if window <= 0 {
		return 0
	}
	if isClipNativeExecutionClass(modeClass) {
		if window < time.Minute {
			return expectedCapturesPer60s(modeClass, recordingIntervalSec)
		}
		return int64(window / (30 * time.Second))
	}
	return expectedFramesPer60s(recordingIntervalSec) * int64(window/time.Minute)
}

func lossRateForWindow(expected, success int64) float64 {
	if expected <= 0 {
		return 0
	}
	loss := 100.0 * float64(maxInt64(expected-success, 0)) / float64(expected)
	if loss < 0 {
		return 0
	}
	return loss
}

func (s *Server) outageEpisodeCountsSince(ctx context.Context, frameStreamIDs, clipStreamIDs []int64, window, gap time.Duration) (map[int64]int64, error) {
	out := map[int64]int64{}
	if gap <= 0 {
		gap = 2 * time.Minute
	}
	if window <= 0 {
		window = 2 * time.Hour
	}
	seconds := int64(window / time.Second)
	if seconds <= 0 {
		seconds = int64((2 * time.Hour) / time.Second)
	}
	windowStart := time.Now().UTC().Add(-window)
	rows, err := s.pool.Query(ctx, `
		WITH success_events AS (
			SELECT f.stream_id, f.captured_at AS captured_at
			FROM frames f
			WHERE f.stream_id = ANY($1::bigint[])
			  AND f.capture_status='success'
			  AND f.captured_at >= now() - make_interval(secs => $3)
			UNION ALL
			SELECT cs.stream_id, cs.segment_end_at AS captured_at
			FROM capture_segments cs
			WHERE cs.stream_id = ANY($2::bigint[])
			  AND cs.capture_status='success'
			  AND cs.segment_end_at >= now() - make_interval(secs => $3)
		)
		SELECT stream_id, captured_at
		FROM success_events
		ORDER BY stream_id ASC, captured_at ASC
	`, frameStreamIDs, clipStreamIDs, seconds)
	if err != nil {
		return nil, fmt.Errorf("query outage episode timestamps: %w", err)
	}
	defer rows.Close()
	type streamWindow struct {
		SeenFirst bool
		Prev      time.Time
	}
	state := map[int64]streamWindow{}
	for rows.Next() {
		var streamID int64
		var capturedAt time.Time
		if err := rows.Scan(&streamID, &capturedAt); err != nil {
			return nil, fmt.Errorf("scan outage episode timestamp: %w", err)
		}
		cur := state[streamID]
		if !cur.SeenFirst {
			if capturedAt.Sub(windowStart) >= gap {
				out[streamID]++
			}
			cur.SeenFirst = true
			cur.Prev = capturedAt.UTC()
			state[streamID] = cur
			continue
		}
		if capturedAt.UTC().Sub(cur.Prev) >= gap {
			out[streamID]++
		}
		cur.Prev = capturedAt.UTC()
		state[streamID] = cur
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate outage episode timestamps: %w", rows.Err())
	}
	return out, nil
}
