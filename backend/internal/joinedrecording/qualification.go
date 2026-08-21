package joinedrecording

import (
	"fmt"
	"regexp"
	"time"

	"github.com/daydemir/stoarama/backend/internal/stitchcert"
)

var reasonCode = regexp.MustCompile(`^[a-z][a-z0-9_]{0,79}$`)

type QualifiedDay struct {
	LocalDate   string    `json:"local_date"`
	JobID       int64     `json:"job_id,omitempty"`
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	CompletedAt time.Time `json:"completed_at,omitempty"`
	QualityTier string    `json:"quality_tier,omitempty"`
}

// QualificationWindow freezes exactly 14 completed scheduled local days.
type QualificationWindow struct {
	RecordingID int64          `json:"recording_id"`
	Timezone    string         `json:"timezone"`
	Days        []QualifiedDay `json:"days"`
	FrozenAt    time.Time      `json:"frozen_at"`
	EvidenceSHA string         `json:"evidence_sha256"`
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
		if day.JobID <= 0 || seenJobs[day.JobID] || day.CompletedAt.Before(day.WindowEnd) || day.CompletedAt.After(window.FrozenAt) || day.QualityTier != "good+" {
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
