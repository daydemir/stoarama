package recsched

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestExpectedSampledClipCountCap(t *testing.T) {
	start := time.Date(2016, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err := ExpectedClipCount("sampled", "*/10 * * * *", "UTC", nil, nil, AllWeekdays, 60, start, start, start.Add(time.Duration(maxSampledClips+1)*10*time.Minute))
	if !errors.Is(err, ErrCoverageTooLarge) || !strings.Contains(err.Error(), "exceeds 500000 sampled clips") {
		t.Fatalf("got %v, want sampled clip cap error", err)
	}
}

func TestExpectedSampledClipCountExcludesIncompleteClip(t *testing.T) {
	start := time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC)
	end := start.Add(31 * time.Minute)
	got, err := ExpectedClipCount("sampled", "*/10 * * * *", "UTC", nil, nil, AllWeekdays, 120, start, start, end)
	if err != nil {
		t.Fatal(err)
	}
	if got != 3 {
		t.Fatalf("got %d expected 3", got)
	}
}

func TestExpectedContinuousClipCountUsesWindowOverlap(t *testing.T) {
	startTOD := TimeOfDay{Hour: 9}
	endTOD := TimeOfDay{Hour: 10}
	start := time.Date(2026, 7, 22, 8, 30, 0, 0, time.UTC)
	end := time.Date(2026, 7, 22, 9, 45, 0, 0, time.UTC)
	got, err := ExpectedClipCount("continuous", "", "UTC", &startTOD, &endTOD, AllWeekdays, 300, start, start, end)
	if err != nil {
		t.Fatal(err)
	}
	if got != 9 {
		t.Fatalf("got %d expected 9", got)
	}
}

func TestExpectedContinuousClipCountHonorsWeekdays(t *testing.T) {
	startTOD := TimeOfDay{Hour: 9}
	endTOD := TimeOfDay{Hour: 10}
	mondayOnly, err := NewWeekdaySet([]int{1})
	if err != nil {
		t.Fatal(err)
	}
	// 2026-07-22 is Wednesday.
	start := time.Date(2026, 7, 22, 8, 0, 0, 0, time.UTC)
	end := time.Date(2026, 7, 22, 11, 0, 0, 0, time.UTC)
	got, err := ExpectedClipCount("continuous", "", "UTC", &startTOD, &endTOD, mondayOnly, 300, start, start, end)
	if err != nil {
		t.Fatal(err)
	}
	if got != 0 {
		t.Fatalf("got %d expected 0", got)
	}
}

func TestVisitExpectedClipStartsMatchesContinuousCount(t *testing.T) {
	start := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	windowStart, windowEnd := TimeOfDay{Hour: 0}, TimeOfDay{Hour: 23}
	count, err := ExpectedClipCount("continuous", "", "America/New_York", &windowStart, &windowEnd, AllWeekdays, 60, start, start, start.Add(48*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	var visited int64
	err = VisitExpectedClipStarts("continuous", "", "America/New_York", &windowStart, &windowEnd, AllWeekdays, 60, start, start, start.Add(48*time.Hour), func(time.Time) {
		visited++
	})
	if err != nil {
		t.Fatal(err)
	}
	if visited != count {
		t.Fatalf("visited=%d count=%d", visited, count)
	}
}

func TestExpectedContinuousClipCountIsNotLimitedByStartEnumerationCap(t *testing.T) {
	window := TimeOfDay{}
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 0, 400)
	got, err := ExpectedClipCount("continuous", "", "UTC", &window, &window, AllWeekdays, 60, start, start, end)
	if err != nil {
		t.Fatal(err)
	}
	const want = int64(400 * 24 * 60)
	if got != want {
		t.Fatalf("got %d expected %d", got, want)
	}
}
