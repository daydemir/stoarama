package recsched

import (
	"testing"
	"time"
)

func TestNextFullContinuousWindowsOnUsesGoDSTRules(t *testing.T) {
	start := TimeOfDay{Hour: 1, Minute: 30}
	end := TimeOfDay{Hour: 3, Minute: 30}

	fall, err := NextFullContinuousWindowsOn(
		"America/New_York", start, end, AllWeekdays,
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Time{},
		time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC), 1,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := fall[0].OpenAt, time.Date(2026, 11, 1, 5, 30, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("fall open=%s want %s", got, want)
	}
	if got, want := fall[0].EndAt, time.Date(2026, 11, 1, 8, 30, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("fall end=%s want %s", got, want)
	}
	if got := fall[0].EndAt.Sub(fall[0].OpenAt); got != 3*time.Hour {
		t.Fatalf("fall duration=%s want 3h", got)
	}

	spring, err := NextFullContinuousWindowsOn(
		"America/New_York", start, end, AllWeekdays,
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Time{},
		time.Date(2026, 3, 8, 0, 0, 0, 0, time.UTC), 1,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := spring[0].OpenAt, time.Date(2026, 3, 8, 6, 30, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("spring open=%s want %s", got, want)
	}
	if got, want := spring[0].EndAt, time.Date(2026, 3, 8, 7, 30, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("spring end=%s want %s", got, want)
	}
	if got := spring[0].EndAt.Sub(spring[0].OpenAt); got != time.Hour {
		t.Fatalf("spring duration=%s want 1h", got)
	}
}

func TestNextFullContinuousWindowsOnHonorsWeekdaysOvernightAndEnvelope(t *testing.T) {
	monday, err := NewWeekdaySet([]int{1})
	if err != nil {
		t.Fatal(err)
	}
	start := TimeOfDay{Hour: 20}
	end := TimeOfDay{Hour: 8}
	envStart := time.Date(2026, 8, 3, 21, 0, 0, 0, time.UTC) // first Monday is partial
	envEnd := time.Date(2026, 8, 25, 7, 0, 0, 0, time.UTC)   // last Monday is truncated
	windows, err := NextFullContinuousWindowsOn("UTC", start, end, monday, envStart, envEnd, envStart, 2)
	if err != nil {
		t.Fatal(err)
	}
	wants := []struct{ open, end time.Time }{
		{time.Date(2026, 8, 10, 20, 0, 0, 0, time.UTC), time.Date(2026, 8, 11, 8, 0, 0, 0, time.UTC)},
		{time.Date(2026, 8, 17, 20, 0, 0, 0, time.UTC), time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)},
	}
	for i, want := range wants {
		if !windows[i].OpenAt.Equal(want.open) || !windows[i].EndAt.Equal(want.end) || windows[i].Ordinal != i+1 {
			t.Fatalf("window[%d]=%+v want open=%s end=%s", i, windows[i], want.open, want.end)
		}
	}
}

func TestNextFullContinuousWindowsOnRefusesInsufficientRunway(t *testing.T) {
	_, err := NextFullContinuousWindowsOn(
		"UTC", TimeOfDay{Hour: 8}, TimeOfDay{Hour: 20}, AllWeekdays,
		time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), 14,
	)
	if err == nil {
		t.Fatal("expected insufficient-runway error")
	}
}

func TestNextFullContinuousWindowsOnFindsFourteenSparseWeekdays(t *testing.T) {
	monday, err := NewWeekdaySet([]int{1})
	if err != nil {
		t.Fatal(err)
	}
	windows, err := NextFullContinuousWindowsOn(
		"UTC", TimeOfDay{Hour: 8}, TimeOfDay{Hour: 20}, monday,
		time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), time.Time{},
		time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), 14,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := windows[13].OpenAt; !got.Equal(time.Date(2026, 11, 2, 8, 0, 0, 0, time.UTC)) {
		t.Fatalf("fourteenth Monday=%s", got)
	}
	if _, err := NextFullContinuousWindowsOn(
		"UTC", TimeOfDay{Hour: 8}, TimeOfDay{Hour: 20}, monday,
		time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), time.Time{},
		time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), 15,
	); err == nil {
		t.Fatal("qualification helper accepted more than fourteen windows")
	}
}

func TestNextFullContinuousWindowsOnRejectsInvalidInputs(t *testing.T) {
	start := TimeOfDay{Hour: 8}
	end := TimeOfDay{Hour: 20}
	envStart := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		weekdays WeekdaySet
		limit    int
	}{
		{name: "zero limit", weekdays: AllWeekdays, limit: 0},
		{name: "negative limit", weekdays: AllWeekdays, limit: -1},
		{name: "over contract limit", weekdays: AllWeekdays, limit: 15},
		{name: "empty weekdays", weekdays: 0, limit: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NextFullContinuousWindowsOn(
				"UTC", start, end, test.weekdays, envStart, time.Time{}, envStart, test.limit,
			); err == nil {
				t.Fatal("expected invalid-input error")
			}
		})
	}
}
