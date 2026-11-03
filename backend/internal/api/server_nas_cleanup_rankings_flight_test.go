package api

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"sync"
	"testing"
	"time"
)

type cleanupRankingQuery struct {
	mu      sync.Mutex
	calls   int
	started chan struct{}
	release chan struct{}
	err     error
}
type cleanupRankingRow struct{ q *cleanupRankingQuery }

func (q *cleanupRankingQuery) QueryRow(ctx context.Context, _ string, _ ...any) pgx.Row {
	q.mu.Lock()
	q.calls++
	q.mu.Unlock()
	select {
	case q.started <- struct{}{}:
	default:
	}
	return cleanupRankingRow{q}
}
func (r cleanupRankingRow) Scan(dest ...any) error {
	<-r.q.release
	if r.q.err != nil {
		return r.q.err
	}
	*dest[0].(*[]byte) = []byte("{\"registered_seconds\":\"2026-09-01\"}")
	return nil
}
func TestNASCleanupRankingFlightSharesOnlyAdvisoryReads(t *testing.T) {
	s := &Server{}
	q := &cleanupRankingQuery{started: make(chan struct{}, 16), release: make(chan struct{})}
	ctx := context.Background()
	type result struct {
		raw []byte
		at  time.Time
		err error
	}
	results := make(chan result, 8)
	for n := 0; n < 8; n++ {
		go func() { raw, at, err := s.cleanupRankings(ctx, q, 3, true); results <- result{raw, at, err} }()
	}
	<-q.started
	close(q.release)
	var at time.Time
	for n := 0; n < 8; n++ {
		r := <-results
		if r.err != nil || string(r.raw) != "{\"registered_seconds\":\"2026-09-01\"}" {
			t.Fatal(r.err, string(r.raw))
		}
		if n == 0 {
			at = r.at
		} else if !at.Equal(r.at) {
			t.Fatal("followers changed snapshot origin")
		}
	}
	q.mu.Lock()
	calls := q.calls
	q.mu.Unlock()
	if calls != 1 {
		t.Fatalf("stampede: %d queries", calls)
	}
	for n := 0; n < 2; n++ {
		if _, _, err := s.cleanupRankings(ctx, q, 3, false); err != nil {
			t.Fatal(err)
		}
	}
	q.mu.Lock()
	calls = q.calls
	q.mu.Unlock()
	if calls != 3 {
		t.Fatal("enabled path reused advisory query", calls)
	}
}
func TestNASCleanupRankingFlightCancellationAndFailureRelease(t *testing.T) {
	s := &Server{}
	q := &cleanupRankingQuery{started: make(chan struct{}, 16), release: make(chan struct{}), err: errors.New("leader failed")}
	leader := make(chan error, 1)
	go func() { _, _, err := s.cleanupRankings(context.Background(), q, 3, true); leader <- err }()
	<-q.started
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := s.cleanupRankings(cancelled, q, 3, true); !errors.Is(err, context.Canceled) {
		t.Fatal("follower cancellation lost", err)
	}
	// An enabled read does not wait on the advisory flight.
	enabled := make(chan error, 1)
	go func() { _, _, err := s.cleanupRankings(context.Background(), q, 3, false); enabled <- err }()
	<-q.started
	close(q.release)
	if err := <-leader; err == nil {
		t.Fatal("leader failure hidden")
	}
	if err := <-enabled; err == nil {
		t.Fatal("enabled query failure hidden")
	}
	s.nasCleanupMu.Lock()
	if len(s.nasCleanupRankingFlights) != 0 || len(s.nasCleanupRankingCache) != 0 {
		s.nasCleanupMu.Unlock()
		t.Fatal("failed flight retained")
	}
	s.nasCleanupMu.Unlock()
	q.err = nil
	if _, _, err := s.cleanupRankings(context.Background(), q, 3, true); err != nil {
		t.Fatal("failed flight blocked retry", err)
	}
	q.mu.Lock()
	calls := q.calls
	q.mu.Unlock()
	if calls != 3 {
		t.Fatal("failure did not permit new read", calls)
	}
}
