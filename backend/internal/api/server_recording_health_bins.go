package api

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/daydemir/stoarama/backend/internal/recsched"
)

const (
	recentHealthBinSize      = 2 * time.Hour
	recentHealthBinCount     = 12
	recentHealthLookback     = 366 * 24 * time.Hour
	recentContinuousLookback = 90 * 24 * time.Hour
)

type recordingHealthBin struct {
	Start    time.Time                   `json:"start"`
	End      time.Time                   `json:"end"`
	Captured int64                       `json:"captured"`
	Expected int64                       `json:"expected"`
	Health   recordingCaptureHealthState `json:"health"`
}

type recordingHealthSpec struct {
	ID               int64
	Mode             string
	CronExpr         string
	Timezone         string
	DailyWindowStart string
	DailyWindowEnd   string
	ActiveWeekdays   recsched.WeekdaySet
	ClipDurationSec  int
	Status           string
	StartAt          time.Time
	EndAt            *time.Time
	PausedAt         *time.Time
}

func alignedHealthBinStart(at time.Time, size time.Duration) time.Time {
	return time.Unix(0, at.UTC().UnixNano()/int64(size)*int64(size)).UTC()
}

func expectedHealthBinsInRanges(spec recordingHealthSpec, ranges []captureHealthRange, coverageEnd time.Time) ([]recordingHealthBin, error) {
	if len(ranges) == 0 {
		return []recordingHealthBin{}, nil
	}
	expectedEnd := ranges[len(ranges)-1].end.Add(time.Duration(spec.ClipDurationSec)*time.Second - time.Nanosecond)
	if expectedEnd.After(coverageEnd) {
		expectedEnd = coverageEnd
	}
	startTOD, endTOD, err := recordingHealthWindow(spec)
	if err != nil {
		return nil, err
	}
	bins := make([]recordingHealthBin, len(ranges))
	for i, hour := range ranges {
		bins[i] = recordingHealthBin{Start: hour.start, End: hour.end}
	}
	if spec.Mode == "continuous" {
		for i, hour := range ranges {
			binEnd := hour.end.Add(time.Duration(spec.ClipDurationSec)*time.Second - time.Nanosecond)
			if binEnd.After(coverageEnd) {
				binEnd = coverageEnd
			}
			bins[i].Expected, err = recsched.ExpectedClipCount(spec.Mode, spec.CronExpr, spec.Timezone, startTOD, endTOD, spec.ActiveWeekdays, spec.ClipDurationSec, spec.StartAt, hour.start, binEnd)
			if err != nil {
				return nil, err
			}
		}
		return populatedHealthBins(bins), nil
	}
	index := 0
	err = recsched.VisitExpectedClipStarts(spec.Mode, spec.CronExpr, spec.Timezone, startTOD, endTOD, spec.ActiveWeekdays, spec.ClipDurationSec, spec.StartAt, ranges[0].start, expectedEnd, func(start time.Time) {
		for index < len(ranges) && !start.Before(ranges[index].end) {
			index++
		}
		if index == len(ranges) {
			return
		}
		if !start.Before(ranges[index].start) {
			bins[index].Expected++
		}
	})
	if err != nil {
		return nil, err
	}
	return populatedHealthBins(bins), nil
}

func populatedHealthBins(bins []recordingHealthBin) []recordingHealthBin {
	expected := bins[:0]
	for _, bin := range bins {
		if bin.Expected > 0 {
			expected = append(expected, bin)
		}
	}
	return expected
}

func recordingHealthWindow(spec recordingHealthSpec) (*recsched.TimeOfDay, *recsched.TimeOfDay, error) {
	if spec.Mode != "continuous" {
		return nil, nil, nil
	}
	start, err := recsched.ParseTimeOfDay(spec.DailyWindowStart)
	if err != nil {
		return nil, nil, err
	}
	end, err := recsched.ParseTimeOfDay(spec.DailyWindowEnd)
	if err != nil {
		return nil, nil, err
	}
	return &start, &end, nil
}

func expectedHealthBins(spec recordingHealthSpec, now time.Time) ([]recordingHealthBin, error) {
	_, coverageEnd := recordingCoverageWindow(spec.Status, spec.StartAt, spec.EndAt, spec.PausedAt, now)
	coverageStart := spec.StartAt.UTC()
	if !coverageStart.Before(coverageEnd) {
		return []recordingHealthBin{}, nil
	}
	lookback := recentHealthLookback
	if spec.Mode == "continuous" {
		// A continuous schedule repeats weekly at its sparsest, so 90 days
		// contains at least twelve eligible weekday windows.
		lookback = recentContinuousLookback
	}
	if boundedStart := coverageEnd.Add(-lookback); coverageStart.Before(boundedStart) {
		coverageStart = boundedStart
	}
	cursor := alignedHealthBinStart(coverageEnd.Add(-time.Nanosecond), recentHealthBinSize)
	newest := make([]recordingHealthBin, 0, recentHealthBinCount)
	for cursor.Add(recentHealthBinSize).After(coverageStart) && len(newest) < recentHealthBinCount {
		ranges := make([]captureHealthRange, 0, recentHealthBinCount)
		for len(ranges) < recentHealthBinCount && cursor.Add(recentHealthBinSize).After(coverageStart) {
			binStart, binEnd := cursor, cursor.Add(recentHealthBinSize)
			if binStart.Before(coverageStart) {
				binStart = coverageStart
			}
			if binEnd.After(coverageEnd) {
				binEnd = coverageEnd
			}
			ranges = append(ranges, captureHealthRange{start: binStart, end: binEnd})
			cursor = cursor.Add(-recentHealthBinSize)
		}
		slices.Reverse(ranges)
		bins, err := expectedHealthBinsInRanges(spec, ranges, coverageEnd)
		if err != nil {
			return nil, err
		}
		for i := len(bins) - 1; i >= 0 && len(newest) < recentHealthBinCount; i-- {
			newest = append(newest, bins[i])
		}
	}
	slices.Reverse(newest)
	return newest, nil
}

func (s *Server) recordingHealthBinsForAccount(ctx context.Context, accountID int64, recordingIDs []int64) (map[int64][]recordingHealthBin, error) {
	out := make(map[int64][]recordingHealthBin, len(recordingIDs))
	if accountID <= 0 || len(recordingIDs) == 0 {
		return out, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, mode, COALESCE(cron_expr,''), cron_timezone,
		       COALESCE(to_char(daily_window_start,'HH24:MI'),''), COALESCE(to_char(daily_window_end,'HH24:MI'),''), active_weekdays,
		       clip_duration_sec, status, start_at, end_at, paused_at
		FROM recordings
		WHERE account_id=$1 AND id=ANY($2::bigint[]) AND status <> 'canceled'
	`, accountID, recordingIDs)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	for rows.Next() {
		var spec recordingHealthSpec
		if err := rows.Scan(&spec.ID, &spec.Mode, &spec.CronExpr, &spec.Timezone, &spec.DailyWindowStart, &spec.DailyWindowEnd, &spec.ActiveWeekdays, &spec.ClipDurationSec, &spec.Status, &spec.StartAt, &spec.EndAt, &spec.PausedAt); err != nil {
			rows.Close()
			return nil, err
		}
		bins, err := expectedHealthBins(spec, now)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("compute recording %d health bins: %w", spec.ID, err)
		}
		out[spec.ID] = bins
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	type binRef struct {
		recordingID int64
		index       int
	}
	refs := make([]binRef, 0, len(recordingIDs)*recentHealthBinCount)
	binRecordingIDs := make([]int64, 0, cap(refs))
	binStarts := make([]time.Time, 0, cap(refs))
	binEnds := make([]time.Time, 0, cap(refs))
	for id, bins := range out {
		for i, bin := range bins {
			refs = append(refs, binRef{recordingID: id, index: i})
			binRecordingIDs = append(binRecordingIDs, id)
			binStarts = append(binStarts, bin.Start)
			binEnds = append(binEnds, bin.End)
		}
	}
	if len(refs) == 0 {
		return out, nil
	}
	countRows, err := s.pool.Query(ctx, `
		SELECT b.ordinality, COUNT(c.id)::bigint
		FROM unnest($1::bigint[], $2::timestamptz[], $3::timestamptz[])
		     WITH ORDINALITY AS b(recording_id, bin_start, bin_end, ordinality)
		LEFT JOIN recording_clips c
		  ON c.recording_id=b.recording_id
		 AND c.clip_start_at >= b.bin_start
		 AND c.clip_start_at < b.bin_end
		GROUP BY b.ordinality
		ORDER BY b.ordinality
	`, binRecordingIDs, binStarts, binEnds)
	if err != nil {
		return nil, err
	}
	for countRows.Next() {
		var ordinal, count int64
		if err := countRows.Scan(&ordinal, &count); err != nil {
			countRows.Close()
			return nil, err
		}
		if ordinal <= 0 || ordinal > int64(len(refs)) {
			countRows.Close()
			return nil, fmt.Errorf("invalid recording health bin ordinal %d", ordinal)
		}
		ref := refs[ordinal-1]
		out[ref.recordingID][ref.index].Captured = count
	}
	if err := countRows.Err(); err != nil {
		countRows.Close()
		return nil, err
	}
	countRows.Close()

	for id, bins := range out {
		for i := range bins {
			bins[i].Health = recordingCaptureHealth("active", bins[i].Captured, bins[i].Expected)
		}
		out[id] = bins
	}
	return out, nil
}
