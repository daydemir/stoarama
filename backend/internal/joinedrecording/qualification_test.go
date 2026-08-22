package joinedrecording

import (
	"encoding/json"
	"strings"
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
		days[i] = QualifiedDay{LocalDate: start.Format("2006-01-02"), QualificationWindowOrdinal: i + 1, JobID: int64(i + 1), WindowStart: start.UTC(), WindowEnd: start.Add(12 * time.Hour).UTC(), CompletedAt: start.Add(12 * time.Hour).UTC()}
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

func TestQualificationJSONRejectsDayLevelQualityTier(t *testing.T) {
	window, err := SealQualificationWindow(qualificationFixture(t, "UTC", time.Date(2026, time.May, 1, 8, 0, 0, 0, time.UTC)))
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := json.Marshal(window)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(canonical), "quality_tier") {
		t.Fatal("completion-day evidence still claims a quality tier")
	}
	stale := strings.Replace(string(canonical), `"completed_at":`, `"quality_tier":"good+","completed_at":`, 1)
	var decoded QualificationWindow
	if err := json.Unmarshal([]byte(stale), &decoded); err == nil {
		t.Fatal("stale per-day quality claim was accepted")
	}
}

func TestQualificationWindowJSONRejectsUnknownTopLevelField(t *testing.T) {
	window, err := SealQualificationWindow(qualificationFixture(t, "UTC", time.Date(2026, time.May, 1, 8, 0, 0, 0, time.UTC)))
	if err != nil {
		t.Fatal(err)
	}
	canonical, _ := json.Marshal(window)
	stale := strings.Replace(string(canonical), `"evidence_sha256":`, `"unknown":true,"evidence_sha256":`, 1)
	var decoded QualificationWindow
	if err := json.Unmarshal([]byte(stale), &decoded); err == nil {
		t.Fatal("unknown qualification window field was accepted")
	}
}

func TestQualificationWindowOrdinalIsExactAndDigestBound(t *testing.T) {
	window := qualificationFixture(t, "UTC", time.Date(2026, time.May, 1, 8, 0, 0, 0, time.UTC))
	sealed, err := SealQualificationWindow(window)
	if err != nil {
		t.Fatal(err)
	}
	mutated := window
	mutated.Days = append([]QualifiedDay(nil), window.Days...)
	mutated.Days[0].QualificationWindowOrdinal = 2
	if _, err := SealQualificationWindow(mutated); err == nil {
		t.Fatal("qualification window accepted a substituted ordinal")
	}
	mutated = sealed
	mutated.Days = append([]QualifiedDay(nil), sealed.Days...)
	mutated.Days[0].QualificationWindowOrdinal = 2
	if ValidateQualificationWindow(mutated) == nil {
		t.Fatal("sealed qualification digest ignored its window ordinal")
	}
}

func TestHistoricalQualificationPreservesDriftAndExactErrorDay(t *testing.T) {
	window := qualificationFixture(t, "UTC", time.Date(2026, time.July, 27, 8, 0, 0, 0, time.UTC))
	window.RecordingID = 348
	window.AuthorityKind = Tier1HistoricalAuthorityKind
	for i := range window.Days {
		scheduled := window.Days[i].WindowStart
		window.Days[i].ScheduledFor = &scheduled
		window.Days[i].JobStatus = "done"
	}
	window.Days[0].ScheduledFor = timePointer(window.Days[0].WindowStart.Add(time.Minute))
	window.Days[0].ReasonCodes = []string{"scheduled_for_drift"}
	window.Days[2].JobStatus = "error"
	window.Days[2].CompletedAt = window.Days[2].WindowStart.Add(time.Hour)
	window.Days[2].ReasonCodes = []string{"terminal_job_error"}
	sealed, err := SealQualificationWindow(window)
	if err != nil || ValidateQualificationWindow(sealed) != nil {
		t.Fatalf("exact historical evidence rejected: %v", err)
	}

	wrongDate := window
	wrongDate.Days = append([]QualifiedDay(nil), window.Days...)
	wrongDate.Days[2].JobStatus = "done"
	wrongDate.Days[2].CompletedAt = wrongDate.Days[2].WindowEnd
	wrongDate.Days[2].ReasonCodes = nil
	wrongDate.Days[3].JobStatus = "error"
	wrongDate.Days[3].CompletedAt = wrongDate.Days[3].WindowStart.Add(time.Hour)
	wrongDate.Days[3].ReasonCodes = []string{"terminal_job_error"}
	if _, err := SealQualificationWindow(wrongDate); err == nil {
		t.Fatal("historical error was accepted on an unapproved recording/date")
	}
	reversedReasons := window
	reversedReasons.Days = append([]QualifiedDay(nil), window.Days...)
	reversedReasons.Days[2].ScheduledFor = timePointer(reversedReasons.Days[2].WindowStart.Add(time.Minute))
	reversedReasons.Days[2].ReasonCodes = []string{"terminal_job_error", "scheduled_for_drift"}
	if _, err := SealQualificationWindow(reversedReasons); err == nil {
		t.Fatal("historical reasons were accepted out of canonical order")
	}
	earlyDone := window
	earlyDone.Days = append([]QualifiedDay(nil), window.Days...)
	earlyDone.Days[2].JobStatus = "done"
	earlyDone.Days[2].ReasonCodes = nil
	if _, err := SealQualificationWindow(earlyDone); err == nil {
		t.Fatal("historical done job completed before its window end")
	}

	prospective := qualificationFixture(t, "UTC", time.Date(2026, time.July, 27, 8, 0, 0, 0, time.UTC))
	prospective.Days[0].ScheduledFor = timePointer(prospective.Days[0].WindowStart)
	prospective.Days[0].JobStatus = "done"
	if _, err := SealQualificationWindow(prospective); err == nil {
		t.Fatal("prospective authority accepted historical job fields")
	}
}
