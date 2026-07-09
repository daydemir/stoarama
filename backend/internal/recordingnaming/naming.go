package recordingnaming

import (
	"encoding/json"
	"fmt"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
)

type Profile string

const (
	ProfileStoaramaV1    Profile = "stoarama_v1"
	ProfilePlazaHourlyV1 Profile = "plaza_hourly_v1"
)

type JobKind string

const (
	JobKindClip             JobKind = "clip"
	JobKindContinuousWindow JobKind = "continuous_window"
)

func ParseProfile(raw string) (Profile, error) {
	switch Profile(strings.TrimSpace(raw)) {
	case ProfileStoaramaV1:
		return ProfileStoaramaV1, nil
	case ProfilePlazaHourlyV1:
		return ProfilePlazaHourlyV1, nil
	default:
		return "", fmt.Errorf("naming_profile must be stoarama_v1 or plaza_hourly_v1")
	}
}

func (p Profile) String() string {
	return string(p)
}

type Metadata struct {
	PlazaID   string `json:"plaza_id"`
	Continent string `json:"continent"`
	Country   string `json:"country"`
	City      string `json:"city"`
	PlazaName string `json:"plaza_name"`
}

func ParseMetadata(raw []byte) (Metadata, error) {
	if len(raw) == 0 {
		return Metadata{}, nil
	}
	var out Metadata
	if err := json.Unmarshal(raw, &out); err != nil {
		return Metadata{}, fmt.Errorf("parse naming metadata: %w", err)
	}
	return out, nil
}

func MarshalMetadata(in Metadata) ([]byte, error) {
	return json.Marshal(in)
}

type Policy struct {
	Profile       Profile
	JobKind       JobKind
	FolderName    string
	Metadata      Metadata
	RecordingID   int64
	JobID         int64
	CronTimezone  string
	ClipStartedAt time.Time
}

func BuildDisplayPath(p Policy) (string, error) {
	switch p.Profile {
	case ProfileStoaramaV1:
		return buildStoaramaPath(p)
	case ProfilePlazaHourlyV1:
		return buildPlazaHourlyPath(p)
	default:
		return "", fmt.Errorf("unknown naming profile %q", p.Profile)
	}
}

func BuildFolderName(profile Profile, recordingID int64, metadata Metadata, rawFolder string) (string, error) {
	rawFolder = strings.TrimSpace(rawFolder)
	switch profile {
	case ProfileStoaramaV1:
		if rawFolder != "" {
			return sanitizePath(rawFolder)
		}
		return "recordings", nil
	case ProfilePlazaHourlyV1:
		if err := metadata.ValidatePlazaHourly(); err != nil {
			return "", err
		}
		if rawFolder != "" {
			return sanitizePath(rawFolder)
		}
		return joinClean(
			twoDigitID(metadata.PlazaID, recordingID),
			sanitizeToken(metadata.Continent),
			sanitizeToken(metadata.Country),
			sanitizeToken(metadata.City),
			sanitizeToken(metadata.PlazaName),
		), nil
	default:
		return "", fmt.Errorf("unknown naming profile %q", profile)
	}
}

func (m Metadata) ValidatePlazaHourly() error {
	required := []struct {
		name  string
		value string
	}{
		{"plaza_id", m.PlazaID},
		{"continent", m.Continent},
		{"country", m.Country},
		{"city", m.City},
		{"plaza_name", m.PlazaName},
	}
	for _, field := range required {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("naming.%s is required for plaza_hourly_v1", field.name)
		}
	}
	if _, err := strconv.Atoi(m.PlazaID); err != nil {
		return fmt.Errorf("naming.plaza_id must be numeric")
	}
	return nil
}

func IsAllowedClipDuration(sec int) bool {
	return sec >= 5 && sec <= 900
}

func ValidateSchedule(profile Profile, mode, cronExpr string, clipDurationSec int, dailyWindowStart, dailyWindowEnd string) error {
	if profile != ProfilePlazaHourlyV1 {
		return nil
	}
	switch strings.TrimSpace(mode) {
	case "sampled":
		if strings.TrimSpace(cronExpr) != "0 8-19 * * *" {
			return fmt.Errorf("plaza_hourly_v1 sampled naming requires cron_expr 0 8-19 * * *")
		}
	case "continuous":
		if clipDurationSec != 60 && clipDurationSec != 300 && clipDurationSec != 600 {
			return fmt.Errorf("plaza_hourly_v1 continuous naming requires 60, 300, or 600 second clips")
		}
		if _, _, _, ok := parseClock(dailyWindowStart); !ok {
			return fmt.Errorf("plaza_hourly_v1 continuous naming requires a valid daily_window_start")
		}
		if _, _, _, ok := parseClock(dailyWindowEnd); !ok {
			return fmt.Errorf("plaza_hourly_v1 continuous naming requires a valid daily_window_end")
		}
	default:
		return fmt.Errorf("mode must be sampled or continuous")
	}
	return nil
}

func parseClock(raw string) (int, int, int, bool) {
	parts := strings.Split(strings.TrimSpace(raw), ":")
	if len(parts) != 2 && len(parts) != 3 {
		return 0, 0, 0, false
	}
	h, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, 0, false
	}
	m, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, 0, false
	}
	s := 0
	if len(parts) == 3 {
		s, err = strconv.Atoi(parts[2])
		if err != nil {
			return 0, 0, 0, false
		}
	}
	return h, m, s, h >= 0 && h <= 23 && m >= 0 && m <= 59 && s >= 0 && s <= 59
}

func buildStoaramaPath(p Policy) (string, error) {
	folder, err := sanitizePath(p.FolderName)
	if err != nil {
		return "", err
	}
	if p.JobKind == JobKindContinuousWindow {
		file := fmt.Sprintf("%d/continuous/%d.mp4", p.RecordingID, p.ClipStartedAt.UTC().Unix())
		return path.Join(folder, file), nil
	}
	file := fmt.Sprintf("%d/%d/%d.mp4", p.RecordingID, p.JobID, p.ClipStartedAt.UTC().UnixMilli())
	return path.Join(folder, file), nil
}

func buildPlazaHourlyPath(p Policy) (string, error) {
	if err := p.Metadata.ValidatePlazaHourly(); err != nil {
		return "", err
	}
	folder, err := sanitizePath(p.FolderName)
	if err != nil {
		return "", err
	}
	loc, err := time.LoadLocation(strings.TrimSpace(p.CronTimezone))
	if err != nil {
		return "", fmt.Errorf("load naming timezone: %w", err)
	}
	local := p.ClipStartedAt.In(loc)
	hour := plazaHourlyHour(local)
	if p.JobKind != JobKindContinuousWindow && (hour < 1 || hour > 12) {
		return "", fmt.Errorf("plaza_hourly_v1 clip hour must be between 08:00 and 19:00 local time")
	}
	plazaID := twoDigitID(p.Metadata.PlazaID, p.RecordingID)
	plazaName := sanitizeToken(p.Metadata.PlazaName)
	hourToken := fmt.Sprintf("%02d", hour)
	if p.JobKind == JobKindContinuousWindow {
		hourToken = fmt.Sprintf("%02d%02d%02d", local.Hour(), local.Minute(), local.Second())
	}
	file := fmt.Sprintf(
		"%s_%s_%04d_%s_W%d_%s_hour_%s.mp4",
		plazaID,
		plazaName,
		local.Year(),
		local.Month().String(),
		((local.Day()-1)/7)+1,
		local.Weekday().String(),
		hourToken,
	)
	return path.Join(folder, local.Month().String(), local.Weekday().String(), file), nil
}

func plazaHourlyHour(t time.Time) int {
	return t.Hour() - 7
}

func twoDigitID(raw string, recordingID int64) string {
	id, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		id = int(recordingID)
	}
	return fmt.Sprintf("%02d", id)
}

func sanitizePath(raw string) (string, error) {
	trimmed := strings.Trim(strings.TrimSpace(raw), "/")
	if trimmed == "" {
		return "", fmt.Errorf("folder_name is required")
	}
	parts := strings.Split(trimmed, "/")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		clean := sanitizeToken(part)
		if clean == "" || clean == "." || clean == ".." {
			return "", fmt.Errorf("invalid folder_name segment %q", part)
		}
		out = append(out, clean)
	}
	return strings.Join(out, "/"), nil
}

var underscoreRuns = regexp.MustCompile(`_+`)

func sanitizeToken(raw string) string {
	var b strings.Builder
	for _, r := range strings.TrimSpace(raw) {
		switch {
		case r == '-' || r == '.':
			b.WriteRune(r)
		case r == '_' || unicode.IsSpace(r):
			b.WriteByte('_')
		case r <= unicode.MaxASCII && (unicode.IsLetter(r) || unicode.IsDigit(r)):
			b.WriteRune(r)
		}
	}
	return strings.Trim(underscoreRuns.ReplaceAllString(b.String(), "_"), "_.")
}

func joinClean(parts ...string) string {
	return strings.Join(parts, "_")
}
