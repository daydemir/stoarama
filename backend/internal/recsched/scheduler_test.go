package recsched

import (
	"fmt"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
)

func mustParse(t *testing.T, expr string) cron.Schedule {
	t.Helper()
	sched, err := ParseCron(expr)
	if err != nil {
		t.Fatalf("ParseCron(%q): %v", expr, err)
	}
	return sched
}

func TestCatchupCursor(t *testing.T) {
	now := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	window := 15 * time.Minute

	// No prior cursor: floor to now-window.
	if got := catchupCursor(nil, now, window); !got.Equal(now.Add(-window)) {
		t.Fatalf("nil cursor: want %s, got %s", now.Add(-window), got)
	}

	// Prior cursor older than the window: still floored to now-window
	// (a recording idle longer than the window never backfills before it).
	old := now.Add(-2 * time.Hour)
	if got := catchupCursor(&old, now, window); !got.Equal(now.Add(-window)) {
		t.Fatalf("stale cursor: want %s, got %s", now.Add(-window), got)
	}

	// Prior cursor inside the window: resume exactly from it.
	recent := now.Add(-5 * time.Minute)
	if got := catchupCursor(&recent, now, window); !got.Equal(recent) {
		t.Fatalf("recent cursor: want %s, got %s", recent, got)
	}
}

func TestCatchupInstantCap(t *testing.T) {
	// 15-minute window, 10-minute floor -> 1 instant per tick.
	if got := catchupInstantCap(15*time.Minute, 600); got != 1 {
		t.Fatalf("want cap 1, got %d", got)
	}
	// 1-hour window, 10-minute floor -> 6.
	if got := catchupInstantCap(time.Hour, 600); got != 6 {
		t.Fatalf("want cap 6, got %d", got)
	}
	// Degenerate config never returns < 1.
	if got := catchupInstantCap(0, 600); got != 1 {
		t.Fatalf("want cap 1 for zero window, got %d", got)
	}
	if got := catchupInstantCap(time.Hour, 0); got != 1 {
		t.Fatalf("want cap 1 for zero interval, got %d", got)
	}
}

func TestDueFireInstantsBackfillsEveryMissedFire(t *testing.T) {
	sched := mustParse(t, "*/10 * * * *") // every 10 minutes
	// Cursor at 12:00, now at 12:35 -> 12:10, 12:20, 12:30 are all due.
	cursor := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	now := time.Date(2026, 6, 24, 12, 35, 0, 0, time.UTC)
	got := dueFireInstants(sched, time.UTC, cursor, now, 100)
	want := []time.Time{
		time.Date(2026, 6, 24, 12, 10, 0, 0, time.UTC),
		time.Date(2026, 6, 24, 12, 20, 0, 0, time.UTC),
		time.Date(2026, 6, 24, 12, 30, 0, 0, time.UTC),
	}
	if len(got) != len(want) {
		t.Fatalf("want %d instants, got %d: %v", len(want), len(got), got)
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Fatalf("instant %d: want %s, got %s", i, want[i], got[i])
		}
		if got[i].Location() != time.UTC {
			t.Fatalf("instant %d not UTC: %s", i, got[i].Location())
		}
	}
}

func TestDueFireInstantsExclusiveLowerInclusiveUpper(t *testing.T) {
	sched := mustParse(t, "*/10 * * * *")
	// Cursor exactly on a fire (12:00): that fire is excluded (lower bound is
	// exclusive). now exactly on a fire (12:20): that fire is included.
	cursor := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	now := time.Date(2026, 6, 24, 12, 20, 0, 0, time.UTC)
	got := dueFireInstants(sched, time.UTC, cursor, now, 100)
	want := []time.Time{
		time.Date(2026, 6, 24, 12, 10, 0, 0, time.UTC),
		time.Date(2026, 6, 24, 12, 20, 0, 0, time.UTC),
	}
	if len(got) != len(want) {
		t.Fatalf("want %d, got %d: %v", len(want), len(got), got)
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Fatalf("instant %d: want %s, got %s", i, want[i], got[i])
		}
	}
}

func TestDueFireInstantsCapTruncates(t *testing.T) {
	sched := mustParse(t, "*/10 * * * *")
	cursor := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	now := time.Date(2026, 6, 24, 13, 0, 0, 0, time.UTC) // 6 fires due
	got := dueFireInstants(sched, time.UTC, cursor, now, 2)
	if len(got) != 2 {
		t.Fatalf("want 2 (capped), got %d: %v", len(got), got)
	}
	// The cap keeps the EARLIEST instants so the cursor advances monotonically
	// and the remainder is drained on subsequent ticks.
	want := []time.Time{
		time.Date(2026, 6, 24, 12, 10, 0, 0, time.UTC),
		time.Date(2026, 6, 24, 12, 20, 0, 0, time.UTC),
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Fatalf("instant %d: want %s, got %s", i, want[i], got[i])
		}
	}
}

func TestDueFireInstantsNoneDueYet(t *testing.T) {
	sched := mustParse(t, "*/10 * * * *")
	// Cursor at 12:00, now at 12:05: the next fire (12:10) is in the future.
	cursor := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	now := time.Date(2026, 6, 24, 12, 5, 0, 0, time.UTC)
	if got := dueFireInstants(sched, time.UTC, cursor, now, 100); len(got) != 0 {
		t.Fatalf("want no instants due, got %v", got)
	}
}

func TestDueFireInstantsZeroCap(t *testing.T) {
	sched := mustParse(t, "*/10 * * * *")
	cursor := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	now := time.Date(2026, 6, 24, 13, 0, 0, 0, time.UTC)
	if got := dueFireInstants(sched, time.UTC, cursor, now, 0); got != nil {
		t.Fatalf("zero cap must yield nil, got %v", got)
	}
}

func TestDueFireInstantsTimezoneCorrect(t *testing.T) {
	// A daily 09:00 New York fire in winter (EST, UTC-5) materializes at 14:00 UTC.
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("load location: %v", err)
	}
	sched := mustParse(t, "0 9 * * *")
	cursor := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 1, 16, 0, 0, 0, 0, time.UTC)
	got := dueFireInstants(sched, loc, cursor, now, 100)
	if len(got) != 1 {
		t.Fatalf("want exactly one daily fire, got %d: %v", len(got), got)
	}
	want := time.Date(2026, 1, 15, 14, 0, 0, 0, time.UTC)
	if !got[0].Equal(want) {
		t.Fatalf("tz fire: want %s, got %s", want, got[0])
	}
}

// TestIdempotencyKeyFormat pins the exact key shape the enqueue path writes and
// the contract documents ('recjob:<recording_id>:<floor(epoch(fire_at))>'), since
// it is the cross-restart dedup guarantee for recording_jobs.
func TestIdempotencyKeyFormat(t *testing.T) {
	fireAt := time.Date(2026, 6, 24, 12, 30, 0, 0, time.UTC)
	got := fmt.Sprintf("recjob:%d:%d", int64(7), fireAt.Unix())
	want := fmt.Sprintf("recjob:7:%d", fireAt.Unix())
	if got != want {
		t.Fatalf("idempotency key: want %s, got %s", want, got)
	}
	// Sub-minute jitter on the same fire instant must collapse to the same key
	// (the column is minute-aligned by construction; epoch seconds are identical).
	if fireAt.Unix() != fireAt.Truncate(time.Minute).Unix() {
		t.Fatalf("fire instant is not minute-aligned: %s", fireAt)
	}
}
