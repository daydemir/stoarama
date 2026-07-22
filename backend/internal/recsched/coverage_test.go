package recsched

import (
	"strings"
	"testing"
	"time"
)

func TestExpectedSampledClipCountCap(t *testing.T) {
	start := time.Date(2016, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err := ExpectedClipCount("sampled", "*/10 * * * *", "UTC", nil, nil, AllWeekdays, 60, start, start, start.Add(time.Duration(maxSampledClips+1)*10*time.Minute))
	if err == nil || !strings.Contains(err.Error(), "exceeds 500000 sampled clips") {
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
