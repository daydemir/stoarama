package api

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/daydemir/stoarama/backend/internal/recsched"
)

const (
	recentHealthBinSize  = 2 * time.Hour
	recentHealthBinCount = 12
	recentHealthLookback = 366 * 24 * time.Hour
	maxDetailHealthBins  = 160
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

func recordingHealthBinSize(start, end time.Time, detailed bool) time.Duration {
	if !detailed {
		return recentHealthBinSize
	}
	span := end.Sub(start)
	for _, size := range []time.Duration{2 * time.Hour, 6 * time.Hour, 12 * time.Hour, 24 * time.Hour, 7 * 24 * time.Hour, 30 * 24 * time.Hour} {
		if span <= time.Duration(maxDetailHealthBins)*size {
			return size
		}
	}
	days := int64(span/(24*time.Hour))/maxDetailHealthBins + 1
	return time.Duration(days) * 24 * time.Hour
}

func alignedHealthBinStart(at time.Time, size time.Duration) time.Time {
	return time.Unix(0, at.UTC().UnixNano()/int64(size)*int64(size)).UTC()
}

func expectedClipsStartingInBin(spec recordingHealthSpec, binStart, binEnd, coverageEnd time.Time) (int64, error) {
	expectedEnd := binEnd.Add(time.Duration(spec.ClipDurationSec)*time.Second - time.Nanosecond)
	if expectedEnd.After(coverageEnd) {
		expectedEnd = coverageEnd
	}
	return expectedRecordingClips(spec.Mode, spec.CronExpr, spec.Timezone, spec.DailyWindowStart, spec.DailyWindowEnd, spec.ActiveWeekdays, spec.ClipDurationSec, spec.StartAt, binStart, expectedEnd)
}

func expectedRecentSampledHealthBins(spec recordingHealthSpec, coverageEnd time.Time) ([]recordingHealthBin, error) {
	schedule, err := recsched.ParseCron(spec.CronExpr)
	if err != nil {
		return nil, err
	}
	location, err := recsched.LoadLocation(spec.Timezone)
	if err != nil {
		return nil, err
	}
	lookback := 24 * time.Hour
	for {
		rangeStart := coverageEnd.Add(-lookback)
		if rangeStart.Before(spec.StartAt) {
			rangeStart = spec.StartAt.UTC()
		}
		latestFire := coverageEnd.Add(-time.Duration(spec.ClipDurationSec) * time.Second)
		counts := map[int64]int64{}
		for fire := schedule.Next(rangeStart.Add(-time.Nanosecond).In(location)); !fire.IsZero() && !fire.After(latestFire); fire = schedule.Next(fire) {
			counts[alignedHealthBinStart(fire, recentHealthBinSize).Unix()]++
		}
		starts := make([]int64, 0, len(counts))
		for start := range counts {
			starts = append(starts, start)
		}
		sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
		if len(starts) >= recentHealthBinCount || rangeStart.Equal(spec.StartAt.UTC()) || lookback >= recentHealthLookback {
			if len(starts) > recentHealthBinCount {
				starts = starts[len(starts)-recentHealthBinCount:]
			}
			bins := make([]recordingHealthBin, 0, len(starts))
			for _, unixStart := range starts {
				start := time.Unix(unixStart, 0).UTC()
				end := start.Add(recentHealthBinSize)
				if start.Before(spec.StartAt) {
					start = spec.StartAt.UTC()
				}
				if end.After(coverageEnd) {
					end = coverageEnd
				}
				bins = append(bins, recordingHealthBin{Start: start, End: end, Expected: counts[unixStart]})
			}
			return bins, nil
		}
		lookback *= 2
		if lookback > recentHealthLookback {
			lookback = recentHealthLookback
		}
	}
}

func expectedHealthBins(spec recordingHealthSpec, now time.Time, detailed bool) ([]recordingHealthBin, error) {
	_, coverageEnd := recordingCoverageWindow(spec.Status, spec.StartAt, spec.EndAt, spec.PausedAt, now)
	coverageStart := spec.StartAt.UTC()
	if !coverageStart.Before(coverageEnd) {
		return []recordingHealthBin{}, nil
	}
	size := recordingHealthBinSize(coverageStart, coverageEnd, detailed)
	if !detailed {
		if spec.Mode == "sampled" {
			return expectedRecentSampledHealthBins(spec, coverageEnd)
		}
		if boundedStart := coverageEnd.Add(-recentHealthLookback); coverageStart.Before(boundedStart) {
			coverageStart = boundedStart
		}
		bins := make([]recordingHealthBin, 0, recentHealthBinCount)
		for cursor := alignedHealthBinStart(coverageEnd.Add(-time.Nanosecond), size); !cursor.Add(size).Before(coverageStart) && len(bins) < recentHealthBinCount; cursor = cursor.Add(-size) {
			binStart, binEnd := cursor, cursor.Add(size)
			if binStart.Before(coverageStart) {
				binStart = coverageStart
			}
			if binEnd.After(coverageEnd) {
				binEnd = coverageEnd
			}
			expected, err := expectedClipsStartingInBin(spec, binStart, binEnd, coverageEnd)
			if err != nil {
				return nil, err
			}
			if expected > 0 {
				bins = append(bins, recordingHealthBin{Start: binStart, End: binEnd, Expected: expected})
			}
		}
		for left, right := 0, len(bins)-1; left < right; left, right = left+1, right-1 {
			bins[left], bins[right] = bins[right], bins[left]
		}
		return bins, nil
	}
	start := alignedHealthBinStart(coverageStart, size)
	bins := make([]recordingHealthBin, 0, recentHealthBinCount)
	for cursor := start; cursor.Before(coverageEnd); cursor = cursor.Add(size) {
		binStart := cursor
		if binStart.Before(coverageStart) {
			binStart = coverageStart
		}
		binEnd := cursor.Add(size)
		if binEnd.After(coverageEnd) {
			binEnd = coverageEnd
		}
		expected, err := expectedClipsStartingInBin(spec, binStart, binEnd, coverageEnd)
		if err != nil {
			return nil, err
		}
		if expected == 0 {
			continue
		}
		bins = append(bins, recordingHealthBin{Start: binStart, End: binEnd, Expected: expected})
	}
	return bins, nil
}

func (s *Server) recordingHealthBinsForAccount(ctx context.Context, accountID int64, recordingIDs []int64, detailed bool) (map[int64][]recordingHealthBin, error) {
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
		bins, err := expectedHealthBins(spec, now, detailed)
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
