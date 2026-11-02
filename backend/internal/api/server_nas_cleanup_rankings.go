package api

import (
	"context"
	"github.com/jackc/pgx/v5"
	"time"
)

// Advisory preflight only. Enabled observations always replay live rankings.
type nasCleanupRankingsSnapshot struct {
	Raw []byte
	At  time.Time
}

func (s *Server) cleanupRankings(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, stream int64, dryRun bool) ([]byte, time.Time, error) {
	started := time.Now().UTC()
	if dryRun {
		s.nasCleanupMu.Lock()
		entry, ok := s.nasCleanupRankingCache[stream]
		s.nasCleanupMu.Unlock()
		age := started.Sub(entry.At)
		if ok && age >= 0 && age < time.Minute {
			return append([]byte(nil), entry.Raw...), entry.At, nil
		}
	}
	var raw []byte
	if err := q.QueryRow(ctx, cleanupCurrentRankingsSQL, stream).Scan(&raw); err != nil {
		return nil, time.Time{}, err
	}
	if dryRun {
		s.nasCleanupMu.Lock()
		if s.nasCleanupRankingCache == nil {
			s.nasCleanupRankingCache = map[int64]nasCleanupRankingsSnapshot{}
		}
		if old, ok := s.nasCleanupRankingCache[stream]; !ok || !old.At.After(started) {
			s.nasCleanupRankingCache[stream] = nasCleanupRankingsSnapshot{Raw: append([]byte(nil), raw...), At: started}
		}
		s.nasCleanupMu.Unlock()
	}
	return raw, started, nil
}
