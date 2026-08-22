package joinedrecording

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"slices"
	"time"

	"github.com/daydemir/stoarama/backend/internal/stitchcert"
)

var reasonCode = regexp.MustCompile(`^[a-z][a-z0-9_]{0,79}$`)

type QualifiedDay struct {
	LocalDate                  string     `json:"local_date"`
	QualificationWindowOrdinal int        `json:"qualification_window_ordinal"`
	JobID                      int64      `json:"job_id,omitempty"`
	ScheduledFor               *time.Time `json:"scheduled_for,omitempty"`
	JobStatus                  string     `json:"job_status,omitempty"`
	ReasonCodes                []string   `json:"reason_codes,omitempty"`
	WindowStart                time.Time  `json:"window_start"`
	WindowEnd                  time.Time  `json:"window_end"`
	CompletedAt                time.Time  `json:"completed_at,omitempty"`
}

func (day QualifiedDay) MarshalJSON() ([]byte, error) {
	if day.ScheduledFor != nil || day.JobStatus != "" {
		type historicalWire struct {
			LocalDate                  string     `json:"local_date"`
			QualificationWindowOrdinal int        `json:"qualification_window_ordinal"`
			JobID                      int64      `json:"job_id"`
			ScheduledFor               *time.Time `json:"scheduled_for"`
			JobStatus                  string     `json:"job_status"`
			ReasonCodes                []string   `json:"reason_codes"`
			WindowStart                time.Time  `json:"window_start"`
			WindowEnd                  time.Time  `json:"window_end"`
			CompletedAt                time.Time  `json:"completed_at"`
		}
		reasons := day.ReasonCodes
		if reasons == nil {
			reasons = []string{}
		}
		return json.Marshal(historicalWire{day.LocalDate, day.QualificationWindowOrdinal, day.JobID,
			day.ScheduledFor, day.JobStatus, reasons, day.WindowStart, day.WindowEnd, day.CompletedAt})
	}
	type prospectiveWire QualifiedDay
	return json.Marshal(prospectiveWire(day))
}

func (day *QualifiedDay) UnmarshalJSON(data []byte) error {
	type wire QualifiedDay
	var decoded wire
	if err := decodeStrictJSON(data, &decoded); err != nil {
		return fmt.Errorf("invalid qualified day: %w", err)
	}
	if decoded.ScheduledFor != nil || decoded.JobStatus != "" {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(data, &fields); err != nil || fields["reason_codes"] == nil {
			return fmt.Errorf("invalid qualified day: historical reason_codes are required")
		}
	}
	*day = QualifiedDay(decoded)
	return nil
}

func decodeStrictJSON(data []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("invalid trailing JSON")
	}
	return nil
}

// QualificationWindow freezes exactly 14 completed scheduled local days.
type QualificationWindow struct {
	RecordingID   int64          `json:"recording_id"`
	Timezone      string         `json:"timezone"`
	Days          []QualifiedDay `json:"days"`
	FrozenAt      time.Time      `json:"frozen_at"`
	AuthorityKind string         `json:"authority_kind,omitempty"`
	EvidenceSHA   string         `json:"evidence_sha256"`
}

func (window *QualificationWindow) UnmarshalJSON(data []byte) error {
	type wire QualificationWindow
	var decoded wire
	if err := decodeStrictJSON(data, &decoded); err != nil {
		return fmt.Errorf("invalid qualification window: %w", err)
	}
	*window = QualificationWindow(decoded)
	return nil
}

func SealQualificationWindow(window QualificationWindow) (QualificationWindow, error) {
	window.EvidenceSHA = ""
	loc, err := time.LoadLocation(window.Timezone)
	if err != nil || window.RecordingID <= 0 || len(window.Days) != 14 {
		return QualificationWindow{}, fmt.Errorf("invalid frozen 14-day qualification window")
	}
	seenJobs := map[int64]bool{}
	var previousDate time.Time
	for i, day := range window.Days {
		date, parseErr := time.ParseInLocation("2006-01-02", day.LocalDate, loc)
		startLocal, endLocal := day.WindowStart.In(loc), day.WindowEnd.In(loc)
		if parseErr != nil || (i > 0 && !date.Equal(previousDate.AddDate(0, 0, 1))) || !day.WindowEnd.After(day.WindowStart) || day.WindowEnd.Sub(day.WindowStart) != 12*time.Hour || startLocal.Format("2006-01-02") != day.LocalDate || endLocal.Format("2006-01-02") != day.LocalDate || startLocal.Hour() != 8 || startLocal.Minute() != 0 || startLocal.Second() != 0 || startLocal.Nanosecond() != 0 || endLocal.Hour() != 20 || endLocal.Minute() != 0 || endLocal.Second() != 0 || endLocal.Nanosecond() != 0 {
			return QualificationWindow{}, fmt.Errorf("qualification days must be 14 consecutive local 12-hour windows")
		}
		previousDate = date
		if day.QualificationWindowOrdinal != i+1 || day.JobID <= 0 || seenJobs[day.JobID] || day.CompletedAt.After(window.FrozenAt) ||
			!validQualifiedDayAuthority(window, day) {
			return QualificationWindow{}, fmt.Errorf("invalid completed qualification day")
		}
		seenJobs[day.JobID] = true
	}
	digest, _, err := stitchcert.CanonicalSHA(window)
	if err != nil {
		return QualificationWindow{}, err
	}
	window.EvidenceSHA = digest
	return window, nil
}

func validQualifiedDayAuthority(window QualificationWindow, day QualifiedDay) bool {
	if window.AuthorityKind == "" {
		return day.ScheduledFor == nil && day.JobStatus == "" && len(day.ReasonCodes) == 0 && !day.CompletedAt.Before(day.WindowEnd)
	}
	if window.AuthorityKind != Tier1HistoricalAuthorityKind || day.ScheduledFor == nil ||
		(day.JobStatus != "done" && day.JobStatus != "error") || day.CompletedAt.Before(day.WindowStart) {
		return false
	}
	reasons := make([]string, 0, 2)
	if !day.ScheduledFor.Equal(day.WindowStart) {
		reasons = append(reasons, "scheduled_for_drift")
	}
	if day.JobStatus == "error" {
		reasons = append(reasons, "terminal_job_error")
		if !tier1HistoricalErrorDay(window.RecordingID, day.LocalDate) {
			return false
		}
	} else if day.CompletedAt.Before(day.WindowEnd) {
		return false
	}
	return slices.Equal(day.ReasonCodes, reasons)
}

func tier1HistoricalErrorDay(recordingID int64, localDate string) bool {
	return (recordingID == 348 && localDate == "2026-07-29") ||
		((recordingID == 408 || recordingID == 406 || recordingID == 409) && localDate == "2026-08-11")
}

func ValidateQualificationWindow(window QualificationWindow) error {
	want := window.EvidenceSHA
	sealed, err := SealQualificationWindow(window)
	if err != nil || sealed.EvidenceSHA != want || !lowerHex64(want) {
		return fmt.Errorf("qualification window is not sealed")
	}
	return nil
}

func (w QualificationWindow) permits(source SourceClip) bool {
	if source.RecordingID != w.RecordingID {
		return false
	}
	for _, day := range w.Days {
		if day.JobID == source.RecordingJobID && source.EndUTC.After(day.WindowStart) && source.StartUTC.Before(day.WindowEnd) {
			return true
		}
	}
	return false
}

func qualificationContainsDate(window QualificationWindow, localDate string) bool {
	_, ok := qualifiedDay(window, localDate)
	return ok
}

func qualifiedDay(window QualificationWindow, localDate string) (QualifiedDay, bool) {
	for _, day := range window.Days {
		if day.LocalDate == localDate {
			return day, true
		}
	}
	return QualifiedDay{}, false
}
