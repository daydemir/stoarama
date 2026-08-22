package api

import (
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
)

func finalFreezeQualificationFixture(t *testing.T, historical bool) (joinedrecording.SelectionAuthority,
	[]joinedrecording.FrozenRecording, []joinedrecording.QualificationWindow) {
	t.Helper()
	recordingID := int64(377)
	first := time.Date(2026, time.August, 1, 8, 0, 0, 0, time.UTC)
	cutoff := time.Date(2026, time.August, 21, 6, 59, 7, 534131000, time.UTC)
	rule, authorityKind := "recording-qualification-v1", ""
	runFrozen := time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC)
	if historical {
		recordingID = 348
		first = time.Date(2026, time.July, 29, 8, 0, 0, 0, time.UTC)
		rule, authorityKind = joinedrecording.Tier1HistoricalQualificationVersion, joinedrecording.Tier1HistoricalAuthorityKind
		runFrozen = cutoff.Add(time.Hour)
	}
	days := make([]joinedrecording.QualifiedDay, 14)
	for i := range days {
		start := first.AddDate(0, 0, i)
		days[i] = joinedrecording.QualifiedDay{LocalDate: start.Format("2006-01-02"),
			QualificationWindowOrdinal: i + 1, JobID: int64(i + 1), WindowStart: start,
			WindowEnd: start.Add(12 * time.Hour), CompletedAt: start.Add(12 * time.Hour)}
		if historical {
			scheduled := start
			days[i].ScheduledFor, days[i].JobStatus = &scheduled, "done"
		}
	}
	if historical {
		days[0].JobStatus, days[0].CompletedAt = "error", days[0].WindowStart.Add(time.Hour)
		days[0].ReasonCodes = []string{"terminal_job_error"}
		drifted := days[1].WindowStart.Add(time.Minute)
		days[1].ScheduledFor, days[1].ReasonCodes = &drifted, []string{"scheduled_for_drift"}
	}
	window, err := joinedrecording.SealQualificationWindow(joinedrecording.QualificationWindow{
		RecordingID: recordingID, Timezone: "UTC", Days: days, FrozenAt: cutoff, AuthorityKind: authorityKind,
	})
	if err != nil {
		t.Fatal(err)
	}
	recordings := []joinedrecording.FrozenRecording{{RecordingID: recordingID, PriorityOrdinal: 1,
		SelectionTier: "good+", QualificationSHA256: window.EvidenceSHA, CompletedAt: days[13].CompletedAt}}
	selectedSHA, err := joinedrecording.SelectedQualificationWindowsSHA256(recordings)
	if err != nil {
		t.Fatal(err)
	}
	orderedSHA, err := joinedrecording.RecordingIDsSHA256([]int64{recordingID})
	if err != nil {
		t.Fatal(err)
	}
	authority := joinedrecording.SelectionAuthority{SelectionBasis: joinedrecording.OperatorApprovedSelectionBasis,
		OrderedRecordingIDSHA256: orderedSHA, Cutoff: cutoff, QualificationRunID: 9,
		QualificationRunFrozenAt: runFrozen, QualificationRuleVersion: rule,
		QualificationCohortSHA256: strings.Repeat("a", 64), QualificationWindowsSHA256: strings.Repeat("b", 64),
		SelectedQualificationWindowsSHA256: selectedSHA}
	return authority, recordings, []joinedrecording.QualificationWindow{window}
}

func TestFinalFreezeRevalidatesProspectiveAndHistoricalQualificationEvidence(t *testing.T) {
	for _, historical := range []bool{false, true} {
		t.Run(map[bool]string{false: "prospective", true: "historical"}[historical], func(t *testing.T) {
			authority, recordings, windows := finalFreezeQualificationFixture(t, historical)
			if err := validateJoinedFinalFreezeQualificationAuthority(authority, recordings, windows); err != nil {
				t.Fatal(err)
			}
			recordings[0].QualificationSHA256 = strings.Repeat("c", 64)
			if err := validateJoinedFinalFreezeQualificationAuthority(authority, recordings, windows); err == nil {
				t.Fatal("qualification label substitution was accepted")
			}
		})
	}
	authority, recordings, windows := finalFreezeQualificationFixture(t, true)
	authority.SelectedQualificationWindowsSHA256 = strings.Repeat("d", 64)
	if err := validateJoinedFinalFreezeQualificationAuthority(authority, recordings, windows); err == nil {
		t.Fatal("selected qualification window hash substitution was accepted")
	}
	prospectiveAuthority, prospectiveRecordings, prospectiveWindows := finalFreezeQualificationFixture(t, false)
	prospectiveAuthority.QualificationRuleVersion = joinedrecording.Tier1HistoricalQualificationVersion
	prospectiveAuthority.QualificationRunFrozenAt = prospectiveAuthority.Cutoff.Add(time.Hour)
	if err := validateJoinedFinalFreezeQualificationAuthority(prospectiveAuthority, prospectiveRecordings, prospectiveWindows); err == nil {
		t.Fatal("historical rule accepted prospective qualification authority kind")
	}
	historicalAuthority, historicalRecordings, historicalWindows := finalFreezeQualificationFixture(t, true)
	historicalAuthority.QualificationRuleVersion = "recording-qualification-v1"
	historicalAuthority.QualificationRunFrozenAt = historicalAuthority.Cutoff.Add(-time.Hour)
	if err := validateJoinedFinalFreezeQualificationAuthority(historicalAuthority, historicalRecordings, historicalWindows); err == nil {
		t.Fatal("prospective rule accepted historical qualification authority kind")
	}
}
