package api

import (
	"context"
	"time"
)

func (s *Server) recordingProcessIssueCountsSince(ctx context.Context, window time.Duration) (map[int64]int64, error) {
	out := map[int64]int64{}
	rows, err := s.pool.Query(ctx, `
		SELECT stream_id, COUNT(*)::bigint
		FROM recording_process_runs
		WHERE COALESCE(stopped_at, updated_at, started_at) >= now() - ($1 * interval '1 second')
		  AND status IN ('stopped', 'failed', 'crashed')
		GROUP BY stream_id
	`, int64(window/time.Second))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var streamID, count int64
		if err := rows.Scan(&streamID, &count); err != nil {
			return nil, err
		}
		out[streamID] = count
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return out, nil
}
