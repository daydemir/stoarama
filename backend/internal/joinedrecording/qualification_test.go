package joinedrecording

import (
	"testing"
	"time"
)

func qualificationFixture(t *testing.T, timezone string, first time.Time) QualificationWindow {
	t.Helper()
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		t.Fatal(err)
	}
	days := make([]QualifiedDay, 14)
	for i := range days {
		date := first.In(loc).AddDate(0, 0, i)
		start := time.Date(date.Year(), date.Month(), date.Day(), 8, 0, 0, 0, loc)
		days[i] = QualifiedDay{LocalDate: start.Format("2006-01-02"), JobID: int64(i + 1), WindowStart: start.UTC(), WindowEnd: start.Add(12 * time.Hour).UTC(), CompletedAt: start.Add(12 * time.Hour).UTC(), QualityTier: "good+"}
	}
	return QualificationWindow{RecordingID: 1, Timezone: timezone, Days: days, FrozenAt: days[13].WindowEnd.Add(time.Hour)}
}

func TestQualificationRejectsWrongLocalClock(t *testing.T) {
	window := qualificationFixture(t, "UTC", time.Date(2026, time.May, 1, 8, 0, 0, 0, time.UTC))
	window.Days[0].WindowStart = window.Days[0].WindowStart.Add(-time.Hour)
	if _, err := SealQualificationWindow(window); err == nil {
		t.Fatal("07:00 local qualification start was accepted")
	}
}

func TestQualificationCalendarEnvelopeSurvivesDST(t *testing.T) {
	window := qualificationFixture(t, "America/New_York", time.Date(2026, time.March, 1, 8, 0, 0, 0, time.UTC))
	sealed, err := SealQualificationWindow(window)
	if err != nil || ValidateQualificationWindow(sealed) != nil {
		t.Fatalf("DST-spanning local calendar rejected: %v", err)
	}
}
