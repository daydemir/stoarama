package api

import (
	"context"
	"github.com/jackc/pgx/v5"
	"time"
)

// Advisory preflight only. Enabled observations use a frozen replay verdict.
// Keep expensive read-only sensitivity selection bounded to five minutes;
// exact source metadata and protected-window checks still run per file.
const cleanupDryRunRankingTTL = 5 * time.Minute

type nasCleanupRankingsSnapshot struct {
	Raw []byte
	At  time.Time
}
type nasCleanupRankingsFlight struct {
	done chan struct{}
	raw  []byte
	at   time.Time
	err  error
}

func (s *Server) cleanupRankings(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, stream int64, dryRun bool) ([]byte, time.Time, error) {
	started := time.Now().UTC()
	if !dryRun {
		var raw []byte
		err := q.QueryRow(ctx, cleanupCurrentRankingsSQL, stream).Scan(&raw)
		return raw, started, err
	}
	s.nasCleanupMu.Lock()
	entry, ok := s.nasCleanupRankingCache[stream]
	age := started.Sub(entry.At)
	if ok && age >= 0 && age < cleanupDryRunRankingTTL {
		s.nasCleanupMu.Unlock()
		return append([]byte(nil), entry.Raw...), entry.At, nil
	}
	if flight := s.nasCleanupRankingFlights[stream]; flight != nil {
		s.nasCleanupMu.Unlock()
		select {
		case <-ctx.Done():
			return nil, time.Time{}, ctx.Err()
		case <-flight.done:
			return append([]byte(nil), flight.raw...), flight.at, flight.err
		}
	}
	flight := &nasCleanupRankingsFlight{done: make(chan struct{}), at: started}
	if s.nasCleanupRankingFlights == nil {
		s.nasCleanupRankingFlights = map[int64]*nasCleanupRankingsFlight{}
	}
	s.nasCleanupRankingFlights[stream] = flight
	s.nasCleanupMu.Unlock()

	var raw []byte
	err := q.QueryRow(ctx, cleanupCurrentRankingsSQL, stream).Scan(&raw)
	s.nasCleanupMu.Lock()
	if err == nil {
		if s.nasCleanupRankingCache == nil {
			s.nasCleanupRankingCache = map[int64]nasCleanupRankingsSnapshot{}
		}
		s.nasCleanupRankingCache[stream] = nasCleanupRankingsSnapshot{Raw: append([]byte(nil), raw...), At: started}
	}
	flight.raw, flight.err = append([]byte(nil), raw...), err
	delete(s.nasCleanupRankingFlights, stream)
	close(flight.done)
	s.nasCleanupMu.Unlock()
	return raw, started, err
}
