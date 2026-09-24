package collation

import (
	"encoding/json"
	"fmt"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/recordingnaming"
)

// HourWork is one planned local delivery hour, frozen by the planner.
type HourWork struct {
	BatchID        string                   `json:"batch_id"`
	HourID         string                   `json:"hour_id"`
	RecordingID    int64                    `json:"recording_id"`
	Timezone       string                   `json:"timezone"`
	LocalDate      string                   `json:"local_date"`
	DeliveryHour   int                      `json:"delivery_hour"`
	ScheduledStart time.Time                `json:"scheduled_start_utc"`
	ScheduledEnd   time.Time                `json:"scheduled_end_utc"`
	NamingProfile  string                   `json:"naming_profile"`
	FolderName     string                   `json:"folder_name"`
	Metadata       recordingnaming.Metadata `json:"naming_metadata"`
	// SupersedesHourID is the generation-1 hour this one replaces ("" if none).
	SupersedesHourID       string `json:"supersedes_hour_id,omitempty"`
	SupersedesHourRecordID int64  `json:"supersedes_hour_record_id,omitempty"`
	Clips                  []Clip `json:"clips"`
}

var safeBatch = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)

// HourIDFor matches the generation-1 canonical hour identity, with generation 2.
func HourIDFor(batchID string, recordingID int64, localDate string, deliveryHour int) (string, error) {
	if !safeBatch.MatchString(batchID) || recordingID <= 0 || deliveryHour < 1 || deliveryHour > 12 {
		return "", fmt.Errorf("invalid hour identity")
	}
	if _, err := time.Parse("2006-01-02", localDate); err != nil {
		return "", fmt.Errorf("invalid local date")
	}
	return fmt.Sprintf("%s__recording-%d__date-%s__hour-%02d__generation-%d", batchID, recordingID, localDate, deliveryHour, Generation), nil
}

// ManifestKey is today's joined coverage layout.
func ManifestKey(batchID, hourID string) string {
	return path.Join("joined", batchID, "coverage", "hours", hourID+".json")
}

// ObjectKey is today's content-addressed joined media layout.
func ObjectKey(batchID, sha string) string {
	return path.Join("joined", batchID, "objects", sha+".mp4")
}

// DeliveryPath is the NAS relative path of one joined part (relative to the
// NAS joined root), per the collated path contract
// (recordingnaming.BuildCollatedPath, docs/NAS_JOINED_DELIVERY.md): it mirrors
// the raw clip tree <folder>/<Month>/<DD-Weekday>/ and names the local clock
// hour, part and local HHMMSS range. The R2 prefix is never part of it.
// Recordings without plaza naming (stoarama_v1) use
// <folder>/<recording>/joined/<date>/<recording>_<date>_hour_<HH>[_part_NN]_<range>.mp4.
func DeliveryPath(w HourWork, start, end time.Time, part, parts int) (string, error) {
	switch w.NamingProfile {
	case string(recordingnaming.ProfilePlazaHourlyV1):
		return recordingnaming.BuildCollatedPath(recordingnaming.CollatedPolicy{FolderName: w.FolderName, Metadata: w.Metadata,
			Timezone: w.Timezone, HourStart: w.ScheduledStart, ActualStart: start, ActualEnd: end, Part: part, Parts: parts})
	case string(recordingnaming.ProfileStoaramaV1):
		loc, err := time.LoadLocation(w.Timezone)
		if err != nil {
			return "", err
		}
		h, s, e := w.ScheduledStart.In(loc), start.In(loc), end.In(loc)
		if !end.After(start) || start.Before(w.ScheduledStart) || !start.Before(w.ScheduledEnd) || part < 1 || part > parts ||
			strings.ContainsAny(w.FolderName, "/\\") || strings.Contains(w.FolderName, "..") || w.FolderName == "" {
			return "", fmt.Errorf("invalid joined delivery range")
		}
		name := fmt.Sprintf("%d_%s_hour_%02d", w.RecordingID, h.Format("2006-01-02"), h.Hour())
		if parts > 1 {
			name += fmt.Sprintf("_part_%02d", part)
		}
		name += fmt.Sprintf("_%s-%s.mp4", s.Format("150405"), e.Format("150405"))
		return path.Join(w.FolderName, fmt.Sprint(w.RecordingID), "joined", h.Format("2006-01-02"), name), nil
	default:
		return "", fmt.Errorf("unsupported naming profile %q", w.NamingProfile)
	}
}

// ClipDisposition accounts for every planned clip exactly once.
type ClipDisposition struct {
	Clip        Clip      `json:"clip"`
	Disposition string    `json:"disposition"` // included | quarantined | duplicate
	Reason      string    `json:"reason,omitempty"`
	Part        int       `json:"part,omitempty"`
	Media       ClipMedia `json:"media"`
}

type Output struct {
	Part            int          `json:"part"`
	Parts           int          `json:"parts"`
	ObjectKey       string       `json:"object_key"`
	NASRelativePath string       `json:"nas_relative_path"`
	SizeBytes       int64        `json:"size_bytes"`
	SHA256          string       `json:"sha256"`
	StartUTC        time.Time    `json:"start_utc"`
	EndUTC          time.Time    `json:"end_utc"`
	SourceClipIDs   []int64      `json:"source_clip_ids"`
	Verification    Verification `json:"verification"`
	R2VerifiedAt    time.Time    `json:"r2_verified_at"`
}

const (
	StatusCollated       = "collated"
	StatusGapOnly        = "gap_only"
	StatusQuarantineOnly = "quarantine_only"
)

type HourManifest struct {
	SchemaVersion    int               `json:"schema_version"`
	PolicyVersion    string            `json:"policy_version"`
	Generation       int               `json:"generation"`
	Status           string            `json:"status"`
	BatchID          string            `json:"batch_id"`
	HourID           string            `json:"hour_id"`
	SupersedesHourID string            `json:"supersedes_hour_id,omitempty"`
	RecordingID      int64             `json:"recording_id"`
	Timezone         string            `json:"timezone"`
	LocalDate        string            `json:"local_date"`
	DeliveryHour     int               `json:"delivery_hour"`
	ScheduledStart   time.Time         `json:"scheduled_start_utc"`
	ScheduledEnd     time.Time         `json:"scheduled_end_utc"`
	SeamPolicy       SeamPolicy        `json:"seam_policy"`
	MediaTool        string            `json:"media_tool"`
	Clips            []ClipDisposition `json:"clips"`
	Seams            []SeamDecision    `json:"seams"`
	Outputs          []Output          `json:"outputs"`
	CreatedAt        time.Time         `json:"created_at"`
}

// Validate checks the manifest's internal accounting before it is published or
// registered: every clip once, parts contiguous, every seam inside a part a
// join and every seam between parts a split.
func (m HourManifest) Validate() error {
	if m.SchemaVersion != 1 || m.PolicyVersion != PolicyVersion || m.Generation != Generation {
		return fmt.Errorf("manifest version differs")
	}
	want, err := HourIDFor(m.BatchID, m.RecordingID, m.LocalDate, m.DeliveryHour)
	if err != nil || want != m.HourID {
		return fmt.Errorf("manifest hour identity differs")
	}
	seen := map[int64]bool{}
	partOf := map[int64]int{}
	for _, c := range m.Clips {
		if seen[c.Clip.ClipID] {
			return fmt.Errorf("clip %d accounted twice", c.Clip.ClipID)
		}
		seen[c.Clip.ClipID] = true
		switch c.Disposition {
		case "included":
			if c.Part < 1 || c.Part > len(m.Outputs) {
				return fmt.Errorf("clip %d has no part", c.Clip.ClipID)
			}
			partOf[c.Clip.ClipID] = c.Part
		case "quarantined", "duplicate":
			if c.Reason == "" || c.Part != 0 {
				return fmt.Errorf("clip %d exclusion lacks reason", c.Clip.ClipID)
			}
		default:
			return fmt.Errorf("clip %d disposition invalid", c.Clip.ClipID)
		}
	}
	covered := map[int64]bool{}
	for i, o := range m.Outputs {
		if o.Part != i+1 || o.Parts != len(m.Outputs) || len(o.SourceClipIDs) == 0 || len(o.SHA256) != 64 || o.SizeBytes <= 0 {
			return fmt.Errorf("output %d invalid", i+1)
		}
		for _, id := range o.SourceClipIDs {
			if partOf[id] != o.Part || covered[id] {
				return fmt.Errorf("output %d source %d differs", o.Part, id)
			}
			covered[id] = true
		}
		if !o.Verification.PayloadChainMatches || !o.Verification.VideoTimingMatches || !o.Verification.KeyframeDecodeOK {
			return fmt.Errorf("output %d not verified", o.Part)
		}
	}
	if len(covered) != len(partOf) {
		return fmt.Errorf("an included clip is missing from its output")
	}
	for _, s := range m.Seams {
		pp, np := partOf[s.PrevClipID], partOf[s.NextClipID]
		joined := pp != 0 && pp == np
		if joined != (s.Decision == DecisionJoin) {
			return fmt.Errorf("seam %d->%d decision differs from parts", s.PrevClipID, s.NextClipID)
		}
		if s.Decision == DecisionJoin && (s.Match == nil || s.Match.Verdict != MatchContinuous) {
			return fmt.Errorf("seam %d->%d joined without continuity proof", s.PrevClipID, s.NextClipID)
		}
	}
	switch m.Status {
	case StatusCollated:
		if len(m.Outputs) == 0 {
			return fmt.Errorf("collated hour without outputs")
		}
	case StatusGapOnly, StatusQuarantineOnly:
		if len(m.Outputs) != 0 {
			return fmt.Errorf("%s hour with outputs", m.Status)
		}
	default:
		return fmt.Errorf("manifest status invalid")
	}
	return nil
}

// MarshalManifest is the one serialization of a published manifest.
func MarshalManifest(m HourManifest) ([]byte, error) {
	return json.Marshal(m)
}
