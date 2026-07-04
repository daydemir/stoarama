package recsched

import (
	"testing"
	"time"
)

func tod(h, m int) TimeOfDay { return TimeOfDay{Hour: h, Minute: m} }

func TestValidateContinuousWindowForCreate(t *testing.T) {
	if err := ValidateContinuousWindowForCreate(tod(9, 0), tod(21, 0), 60); err != nil {
		t.Fatalf("valid window rejected: %v", err)
	}
	if err := ValidateContinuousWindowForCreate(tod(21, 0), tod(9, 0), 60); err != nil {
		t.Fatalf("overnight window (start>end) must be accepted: %v", err)
	}
	if err := ValidateContinuousWindowForCreate(tod(9, 0), tod(9, 0), 60); err != nil {
		t.Fatalf("end==start (24h window) must be accepted: %v", err)
	}
	if err := ValidateContinuousWindowForCreate(tod(9, 0), tod(21, 0), 1); err == nil {
		t.Fatalf("expected rejection of sub-5s segment length")
	}
	if err := ValidateContinuousWindowForCreate(tod(9, 0), tod(21, 0), 1000); err == nil {
		t.Fatalf("expected rejection of >900s segment length")
	}
}

func TestCurrentOpenContinuousWindow(t *testing.T) {
	env := time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC)
	// 10:00 UTC is inside a 09:00-21:00 UTC window.
	now := time.Date(2026, 6, 30, 10, 0, 0, 0, time.UTC)
	open, openUTC, closeUTC, err := currentOpenContinuousWindow("UTC", tod(9, 0), tod(21, 0), env, time.Time{}, now)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !open {
		t.Fatalf("expected window open at 10:00")
	}
	if !openUTC.Equal(time.Date(2026, 6, 30, 9, 0, 0, 0, time.UTC)) {
		t.Fatalf("openUTC=%s", openUTC)
	}
	if !closeUTC.Equal(time.Date(2026, 6, 30, 21, 0, 0, 0, time.UTC)) {
		t.Fatalf("closeUTC=%s", closeUTC)
	}
	// 22:00 UTC is outside.
	nowClosed := time.Date(2026, 6, 30, 22, 0, 0, 0, time.UTC)
	open, _, _, err = currentOpenContinuousWindow("UTC", tod(9, 0), tod(21, 0), env, time.Time{}, nowClosed)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if open {
		t.Fatalf("expected window closed at 22:00")
	}
}

func TestCurrentOpenContinuousWindowOvernight(t *testing.T) {
	// Overnight window 22:00 -> 06:00 in UTC. Occurrence opening on day D covers
	// [22:00 D, 06:00 D+1).
	env := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	openD := func(y int, mo time.Month, d int) time.Time { return time.Date(y, mo, d, 22, 0, 0, 0, time.UTC) }
	closeD := func(y int, mo time.Month, d int) time.Time { return time.Date(y, mo, d, 6, 0, 0, 0, time.UTC) }
	cases := []struct {
		name     string
		now      time.Time
		want     bool
		wantOpen time.Time
		wantClz  time.Time
	}{
		{"before open (12:00)", time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC), false, time.Time{}, time.Time{}},
		{"just after open (22:00)", time.Date(2026, 6, 30, 22, 0, 0, 0, time.UTC), true, openD(2026, 6, 30), closeD(2026, 7, 1)},
		{"around midnight (00:30)", time.Date(2026, 7, 1, 0, 30, 0, 0, time.UTC), true, openD(2026, 6, 30), closeD(2026, 7, 1)},
		{"just before close (05:59)", time.Date(2026, 7, 1, 5, 59, 0, 0, time.UTC), true, openD(2026, 6, 30), closeD(2026, 7, 1)},
		{"at close (06:00)", time.Date(2026, 7, 1, 6, 0, 0, 0, time.UTC), false, time.Time{}, time.Time{}},
		{"after close (12:00)", time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC), false, time.Time{}, time.Time{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			open, openUTC, closeUTC, err := currentOpenContinuousWindow("UTC", tod(22, 0), tod(6, 0), env, time.Time{}, c.now)
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if open != c.want {
				t.Fatalf("open=%v want %v", open, c.want)
			}
			if c.want {
				if !openUTC.Equal(c.wantOpen) {
					t.Fatalf("openUTC=%s want %s", openUTC, c.wantOpen)
				}
				if !closeUTC.Equal(c.wantClz) {
					t.Fatalf("closeUTC=%s want %s", closeUTC, c.wantClz)
				}
			}
		})
	}
}

func TestCurrentOpenContinuousWindow24h(t *testing.T) {
	// end == start is a 24h window: always open, distinct daily occurrences.
	env := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 6, 30, 3, 0, 0, 0, time.UTC)
	open, openUTC, closeUTC, err := currentOpenContinuousWindow("UTC", tod(9, 0), tod(9, 0), env, time.Time{}, now)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !open {
		t.Fatalf("24h window must be open at 03:00")
	}
	// 03:00 is before 09:00, so the active occurrence opened yesterday at 09:00.
	if !openUTC.Equal(time.Date(2026, 6, 29, 9, 0, 0, 0, time.UTC)) {
		t.Fatalf("openUTC=%s", openUTC)
	}
	if !closeUTC.Equal(time.Date(2026, 6, 30, 9, 0, 0, 0, time.UTC)) {
		t.Fatalf("closeUTC=%s", closeUTC)
	}
}

func TestNextWindowOpenUTC(t *testing.T) {
	env := time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC)
	// At 22:00 the next 09:00 open is the next day.
	now := time.Date(2026, 6, 30, 22, 0, 0, 0, time.UTC)
	next, err := NextWindowOpenUTC("UTC", tod(9, 0), env, time.Time{}, now)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	want := time.Date(2026, 7, 1, 9, 0, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Fatalf("next=%s want %s", next, want)
	}
}
