package recsched

import (
	"testing"
	"time"
)

func TestContinuousWeekdaysOwnOvernightByOpeningDay(t *testing.T) {
	monday, err := NewWeekdaySet([]int{1})
	if err != nil {
		t.Fatal(err)
	}
	start, _ := ParseTimeOfDay("22:00")
	end, _ := ParseTimeOfDay("02:00")
	envStart := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	// Tuesday 01:00 UTC belongs to the window opened Monday at 22:00.
	now := time.Date(2026, 7, 7, 1, 0, 0, 0, time.UTC)
	open, opened, closes, err := currentOpenContinuousWindowOn("UTC", start, end, monday, envStart, time.Time{}, now)
	if err != nil {
		t.Fatal(err)
	}
	if !open || opened.Weekday() != time.Monday || !closes.Equal(time.Date(2026, 7, 7, 2, 0, 0, 0, time.UTC)) {
		t.Fatalf("open=%v opened=%s closes=%s", open, opened, closes)
	}
	// The same clock time Wednesday is closed because Tuesday is not active.
	open, _, _, err = currentOpenContinuousWindowOn("UTC", start, end, monday, envStart, time.Time{}, now.AddDate(0, 0, 1))
	if err != nil || open {
		t.Fatalf("open=%v err=%v, want closed", open, err)
	}
}

func TestNextWindowOpenUTCOnSkipsInactiveDays(t *testing.T) {
	friday, _ := NewWeekdaySet([]int{5})
	start, _ := ParseTimeOfDay("09:00")
	after := time.Date(2026, 7, 6, 10, 0, 0, 0, time.UTC) // Monday
	got, err := NextWindowOpenUTCOn("UTC", start, friday, time.Time{}, time.Time{}, after)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 7, 10, 9, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("got %s want %s", got, want)
	}
}
