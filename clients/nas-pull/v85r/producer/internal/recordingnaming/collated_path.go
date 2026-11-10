package recordingnaming

import (
	"fmt"
	"path"
	"strings"
	"time"
)

// CollatedPolicy identifies one collated (joined) part of one local clock hour.
//
// NAS path contract (docs/NAS_JOINED_DELIVERY.md): the returned path is relative
// to the NAS joined root (/clips/joined) and mirrors the raw clip tree
// <folder>/<Month>/<DD-Weekday>/ exactly, so a collated hour sits under the same
// folder and day as the raw clips it replaces. The R2 key prefix
// (managed/acct-N) is never part of a NAS path.
type CollatedPolicy struct {
	FolderName string
	Metadata   Metadata
	Timezone   string
	// HourStart is the instant the local clock hour begins (minute and second 0).
	HourStart time.Time
	// ActualStart/ActualEnd bound the part's presentation. The start lies inside
	// the hour; the end may run past the hour boundary by at most
	// collatedHourOverrun (the last source clip's tail).
	ActualStart time.Time
	ActualEnd   time.Time
	Part        int
	Parts       int
}

const (
	collatedHourOverrun = 15 * time.Minute
	collatedMaxParts    = 99
)

// BuildCollatedPath returns
//
//	<folder>/<Month>/<DD-Weekday>/<id>_<Plaza>_<YYYY>_<Month>_W<n>_<Weekday>_hour_<HH>[_part_NN]_<HHMMSS>-<HHMMSS>.mp4
//
// HH is the local clock hour 00-23. _part_NN appears only when the hour is split
// into more than one part. The HHMMSS range is local wall-clock time of the
// part's first and last presented frame (the end wraps past midnight for hour
// 23). When a DST fall-back repeats a local hour, the second occurrence carries
// hour_<HH>_dst2 so the two hours never collide.
func BuildCollatedPath(p CollatedPolicy) (string, error) {
	dir, base, loc, err := collatedHourBase(p)
	if err != nil {
		return "", err
	}
	start, end := p.ActualStart.In(loc), p.ActualEnd.In(loc)
	hourEnd := p.HourStart.Add(time.Hour)
	if p.ActualStart.Before(p.HourStart) || !p.ActualStart.Before(hourEnd) || !p.ActualEnd.After(p.ActualStart) ||
		p.ActualEnd.After(hourEnd.Add(collatedHourOverrun)) {
		return "", fmt.Errorf("collated part range must start inside its hour and end within %s of the hour boundary", collatedHourOverrun)
	}
	if p.Parts < 1 || p.Parts > collatedMaxParts || p.Part < 1 || p.Part > p.Parts {
		return "", fmt.Errorf("invalid collated part ordinal %d of %d", p.Part, p.Parts)
	}
	name := base
	if p.Parts > 1 {
		name += fmt.Sprintf("_part_%02d", p.Part)
	}
	name += fmt.Sprintf("_%s-%s.mp4", start.Format("150405"), end.Format("150405"))
	return path.Join(dir, name), nil
}

// BuildCollatedManifestPath returns the hour's manifest path beside its media:
// <folder>/<Month>/<DD-Weekday>/<hour base>.manifest.json. Part and range fields
// of the policy are ignored.
func BuildCollatedManifestPath(p CollatedPolicy) (string, error) {
	dir, base, _, err := collatedHourBase(p)
	if err != nil {
		return "", err
	}
	return path.Join(dir, base+".manifest.json"), nil
}

func collatedHourBase(p CollatedPolicy) (string, string, *time.Location, error) {
	if err := p.Metadata.ValidatePlazaHourly(); err != nil {
		return "", "", nil, err
	}
	folder, err := sanitizePath(p.FolderName)
	if err != nil {
		return "", "", nil, err
	}
	loc, err := time.LoadLocation(strings.TrimSpace(p.Timezone))
	if err != nil {
		return "", "", nil, fmt.Errorf("load naming timezone: %w", err)
	}
	hour := p.HourStart.In(loc)
	if hour.Minute() != 0 || hour.Second() != 0 || hour.Nanosecond() != 0 {
		return "", "", nil, fmt.Errorf("collated hour must start on a local clock hour")
	}
	token := fmt.Sprintf("%02d", hour.Hour())
	if prev := p.HourStart.Add(-time.Hour).In(loc); prev.Hour() == hour.Hour() {
		token += "_dst2"
	}
	base := fmt.Sprintf("%s_%s_%04d_%s_W%d_%s_hour_%s",
		twoDigitID(p.Metadata.PlazaID, 0), sanitizeToken(p.Metadata.PlazaName), hour.Year(), hour.Month().String(),
		((hour.Day()-1)/7)+1, hour.Weekday().String(), token)
	dir := path.Join(folder, hour.Month().String(), fmt.Sprintf("%02d-%s", hour.Day(), hour.Weekday()))
	return dir, base, loc, nil
}
