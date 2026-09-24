package dropletpool

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeSpendGuard struct {
	burn, size, critical float64
	err                  error
	quotes               int
}

func (f *fakeSpendGuard) Quote(context.Context, string) (float64, float64, error) {
	f.quotes++
	return f.burn, f.size, f.err
}

func (f *fakeSpendGuard) Critical() float64 { return f.critical }

func TestScaleUpSpendGuardBlocksPastCriticalAndOpensEpisode(t *testing.T) {
	tests := []struct {
		name        string
		guard       *fakeSpendGuard
		wantCreated int
		wantBlocked bool
	}{
		// $8 + 2 x $0.857 stays under $10; the third would reach $10.57.
		{name: "batch accumulates to the ceiling", guard: &fakeSpendGuard{burn: 8, size: 0.857, critical: 10}, wantCreated: 2, wantBlocked: true},
		{name: "headroom allows the whole batch", guard: &fakeSpendGuard{burn: 1, size: 0.857, critical: 10}, wantCreated: 3},
		{name: "runaway account blocks the first", guard: &fakeSpendGuard{burn: 133, size: 0.857, critical: 10}, wantCreated: 0, wantBlocked: true},
		{name: "quote failure falls back to the hard cap", guard: &fakeSpendGuard{err: errors.New("billing api down"), critical: 10}, wantCreated: 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pool, cleanup := testDropletPoolDB(t)
			defer cleanup()
			ctx := context.Background()
			provider := &fakeDOClient{}
			c := NewController(pool, provider, Config{
				OperatorAccountID: insertForecastAccount(t, pool),
				Region:            "nyc1", Size: "s-2vcpu-4gb", Image: "123", Capacity: 5, Max: 20,
				BackendAPIURL: "https://example.invalid", SpendGuard: tc.guard,
			})
			now := time.Now().UTC()
			c.spend = nil // as at the top of tick
			var blockedErr error
			for i := 0; i < 3; i++ {
				if err := c.scaleUp(ctx, now, i); err != nil {
					blockedErr = err
					break
				}
			}
			if len(provider.created) != tc.wantCreated {
				t.Fatalf("created=%d want %d", len(provider.created), tc.wantCreated)
			}
			if tc.guard.quotes != 1 {
				t.Fatalf("guard quoted %d times in one tick, want 1", tc.guard.quotes)
			}
			if tc.wantBlocked != errors.Is(blockedErr, ErrSpendBudget) {
				t.Fatalf("blocked err=%v want blocked=%t", blockedErr, tc.wantBlocked)
			}
			var open int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM ops_alert_episodes WHERE alert_key=$1 AND signal=$1 AND resolved_at IS NULL`, SpendBlockedSignal).Scan(&open); err != nil {
				t.Fatal(err)
			}
			if (open == 1) != tc.wantBlocked {
				t.Fatalf("open blocked episodes=%d want blocked=%t", open, tc.wantBlocked)
			}
		})
	}
}

func TestNoteOpsAlertReopensResolvedEpisode(t *testing.T) {
	pool, cleanup := testDropletPoolDB(t)
	defer cleanup()
	ctx := context.Background()
	s := NewStore(pool)
	t0 := time.Date(2026, 9, 24, 7, 0, 0, 0, time.UTC)
	if err := s.NoteOpsAlert(ctx, SpendBlockedSignal, SpendBlockedSignal, t0); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE ops_alert_episodes SET last_alerted_at=$1, resolved_at=$2`, t0, t0.Add(3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	t1 := t0.Add(5 * time.Hour)
	if err := s.NoteOpsAlert(ctx, SpendBlockedSignal, SpendBlockedSignal, t1); err != nil {
		t.Fatal(err)
	}
	var first, last time.Time
	var alerted, resolved *time.Time
	if err := pool.QueryRow(ctx, `SELECT first_detected_at, last_detected_at, last_alerted_at, resolved_at FROM ops_alert_episodes WHERE alert_key=$1`, SpendBlockedSignal).Scan(&first, &last, &alerted, &resolved); err != nil {
		t.Fatal(err)
	}
	if !first.Equal(t1) || !last.Equal(t1) || alerted != nil || resolved != nil {
		t.Fatalf("reopened episode first=%s last=%s alerted=%v resolved=%v", first, last, alerted, resolved)
	}
}
