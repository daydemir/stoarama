package api

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/recsched"
	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

const (
	captureHealthPageDays    = 90
	captureHealthPageTimeout = 8 * time.Second
)

type captureHealthRequestError struct {
	message string
}

func (e captureHealthRequestError) Error() string { return e.message }

type recordingCaptureHealthPage struct {
	RecordingID int64                `json:"recording_id"`
	Timezone    string               `json:"timezone"`
	From        string               `json:"from"`
	To          string               `json:"to"`
	HasOlder    bool                 `json:"has_older"`
	HasNewer    bool                 `json:"has_newer"`
	Bins        []recordingHealthBin `json:"bins"`
}

type captureHealthRange struct {
	start time.Time
	end   time.Time
}

func localCaptureHealthRanges(start, end time.Time, location *time.Location) []captureHealthRange {
	ranges := []captureHealthRange{}
	for cursor := start; cursor.Before(end); {
		local := cursor.In(location)
		next := time.Date(local.Year(), local.Month(), local.Day(), local.Hour()+1, 0, 0, 0, location).UTC()
		if next.Sub(cursor) > time.Hour {
			next = nextTimezoneTransition(cursor, next, location)
		}
		if !next.After(cursor) {
			next = cursor.Add(time.Hour)
		}
		if next.After(end) {
			next = end
		}
		ranges = append(ranges, captureHealthRange{start: cursor, end: next})
		cursor = next
	}
	return ranges
}

func nextTimezoneTransition(start, end time.Time, location *time.Location) time.Time {
	_, startOffset := start.In(location).Zone()
	low, high := start.Unix(), end.Unix()
	for low+1 < high {
		mid := low + (high-low)/2
		_, offset := time.Unix(mid, 0).In(location).Zone()
		if offset == startOffset {
			low = mid
		} else {
			high = mid
		}
	}
	return time.Unix(high, 0).UTC()
}

func (s *Server) handleAccountRecordingCaptureHealth(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	s.writeRecordingCaptureHealth(w, r, principal.AccountID, id, false)
}

func recordingCaptureHealthCacheKey(r *http.Request, accountID, recordingID int64, shared bool) recordingMetricCacheKey {
	return recordingMetricCacheKey{
		AccountID: accountID, RecordingID: recordingID, Shared: shared,
		Scope: strings.TrimSpace(r.URL.Query().Get("from")) + "|" + strings.TrimSpace(r.URL.Query().Get("to")),
	}
}

func (s *Server) writeRecordingCaptureHealth(w http.ResponseWriter, r *http.Request, accountID, recordingID int64, shared bool) {
	ctx, cancel := context.WithTimeout(r.Context(), captureHealthPageTimeout)
	defer cancel()
	key := recordingCaptureHealthCacheKey(r, accountID, recordingID, shared)
	page, err := loadRecordingMetricCached(ctx, &s.recordingHealthPageCache, key, captureHealthPageTimeout, recordingHealthPageCacheTTL, recordingMetricFailureTTL, s.recordingMetricWorkSlots(), func(loadCtx context.Context) (recordingCaptureHealthPage, error) {
		return s.recordingCaptureHealthPage(r.Clone(loadCtx), accountID, recordingID)
	})
	if err != nil {
		var requestError captureHealthRequestError
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			util.WriteError(w, http.StatusNotFound, "recording not found")
		case errors.As(err, &requestError):
			util.WriteError(w, http.StatusBadRequest, err.Error())
		case errors.Is(err, context.DeadlineExceeded):
			w.Header().Set("Retry-After", "10")
			util.WriteError(w, http.StatusServiceUnavailable, "capture health temporarily unavailable")
		default:
			log.Printf("recording capture health failed account_id=%d recording_id=%d: %v", accountID, recordingID, err)
			util.WriteError(w, http.StatusInternalServerError, "compute recording capture health")
		}
		return
	}
	w.Header().Set("Cache-Control", "private, no-store")
	util.WriteJSON(w, http.StatusOK, page)
}

func (s *Server) recordingCaptureHealthPage(r *http.Request, accountID, recordingID int64) (recordingCaptureHealthPage, error) {
	var spec recordingHealthSpec
	err := s.pool.QueryRow(r.Context(), `
		SELECT id, mode, COALESCE(cron_expr,''), cron_timezone,
		       COALESCE(to_char(daily_window_start,'HH24:MI'),''), COALESCE(to_char(daily_window_end,'HH24:MI'),''),
		       active_weekdays, clip_duration_sec, status, start_at, end_at, paused_at
		FROM recordings
		WHERE account_id=$1 AND id=$2 AND status <> 'canceled'
	`, accountID, recordingID).Scan(
		&spec.ID, &spec.Mode, &spec.CronExpr, &spec.Timezone,
		&spec.DailyWindowStart, &spec.DailyWindowEnd, &spec.ActiveWeekdays,
		&spec.ClipDurationSec, &spec.Status, &spec.StartAt, &spec.EndAt, &spec.PausedAt,
	)
	if err != nil {
		return recordingCaptureHealthPage{}, err
	}
	location, err := recsched.LoadLocation(spec.Timezone)
	if err != nil {
		return recordingCaptureHealthPage{}, fmt.Errorf("invalid recording timezone: %w", err)
	}
	coverageStart, coverageEnd := recordingHistoryWindow(spec.Status, spec.StartAt, spec.EndAt, spec.PausedAt, time.Now().UTC())
	defaultTo := coverageEnd.In(location)
	toDate, err := parseCaptureHealthDate(r.URL.Query().Get("to"), defaultTo, location)
	if err != nil {
		return recordingCaptureHealthPage{}, captureHealthRequestError{message: "invalid to date; expected YYYY-MM-DD"}
	}
	lastHistoryDay := captureHealthLastDay(coverageStart, coverageEnd, location)
	if toDate.After(lastHistoryDay) {
		toDate = lastHistoryDay
	}
	defaultFrom := toDate.AddDate(0, 0, -(captureHealthPageDays - 1))
	if coverageStart.In(location).After(defaultFrom) {
		defaultFrom = coverageStart.In(location)
	}
	from, err := parseCaptureHealthDate(r.URL.Query().Get("from"), defaultFrom, location)
	if err != nil {
		return recordingCaptureHealthPage{}, captureHealthRequestError{message: "invalid from date; expected YYYY-MM-DD"}
	}
	to := toDate.AddDate(0, 0, 1)
	if !from.Before(to) {
		return recordingCaptureHealthPage{}, captureHealthRequestError{message: "from must be on or before to"}
	}
	if from.Before(toDate.AddDate(0, 0, -(captureHealthPageDays - 1))) {
		return recordingCaptureHealthPage{}, captureHealthRequestError{message: fmt.Sprintf("capture health range cannot exceed %d local days", captureHealthPageDays)}
	}
	if !captureHealthRangeOverlaps(from.UTC(), to.UTC(), coverageStart, coverageEnd) {
		return recordingCaptureHealthPage{}, captureHealthRequestError{message: "requested range does not overlap recording coverage"}
	}
	rangeStart := from.UTC()
	if rangeStart.Before(coverageStart) {
		rangeStart = coverageStart
	}
	rangeEnd := to.UTC()
	if rangeEnd.After(coverageEnd) {
		rangeEnd = coverageEnd
	}
	bins, err := expectedHealthBinsInRanges(spec, localCaptureHealthRanges(rangeStart, rangeEnd, location), coverageEnd)
	if err != nil {
		return recordingCaptureHealthPage{}, fmt.Errorf("compute capture health: %w", err)
	}
	starts := make([]time.Time, len(bins))
	ends := make([]time.Time, len(bins))
	for i := range bins {
		starts[i], ends[i] = bins[i].Start, bins[i].End
	}
	if len(bins) > 0 {
		rows, err := s.pool.Query(r.Context(), `
			SELECT b.ordinality, COUNT(c.id)::bigint
			FROM unnest($2::timestamptz[], $3::timestamptz[]) WITH ORDINALITY AS b(bin_start, bin_end, ordinality)
			LEFT JOIN recording_clips c
			  ON c.recording_id=$1 AND c.clip_start_at >= b.bin_start AND c.clip_start_at < b.bin_end
			GROUP BY b.ordinality
			ORDER BY b.ordinality
		`, recordingID, starts, ends)
		if err != nil {
			return recordingCaptureHealthPage{}, fmt.Errorf("count captured clips: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var ordinal, captured int64
			if err := rows.Scan(&ordinal, &captured); err != nil {
				return recordingCaptureHealthPage{}, fmt.Errorf("scan captured clips: %w", err)
			}
			if err := setCapturedHealthBin(bins, ordinal, captured); err != nil {
				return recordingCaptureHealthPage{}, err
			}
		}
		if err := rows.Err(); err != nil {
			return recordingCaptureHealthPage{}, fmt.Errorf("count captured clips: %w", err)
		}
		if err := s.populateRecordingJoinedProgressBins(r.Context(), accountID, recordingID, starts, ends, bins); err != nil {
			return recordingCaptureHealthPage{}, err
		}
	}
	for i := range bins {
		bins[i].Health = recordingCaptureHealth("active", bins[i].Captured, bins[i].Expected)
	}
	return recordingCaptureHealthPage{
		RecordingID: recordingID,
		Timezone:    spec.Timezone,
		From:        from.Format("2006-01-02"),
		To:          toDate.Format("2006-01-02"),
		HasOlder:    from.After(time.Date(coverageStart.In(location).Year(), coverageStart.In(location).Month(), coverageStart.In(location).Day(), 0, 0, 0, 0, location)),
		HasNewer:    to.Before(lastHistoryDay.AddDate(0, 0, 1)),
		Bins:        bins,
	}, nil
}

func captureHealthRangeOverlaps(start, end, coverageStart, coverageEnd time.Time) bool {
	return start.Before(coverageEnd) && end.After(coverageStart)
}

func captureHealthLastDay(start, end time.Time, location *time.Location) time.Time {
	last := end
	if end.After(start) {
		last = end.Add(-time.Nanosecond)
	}
	local := last.In(location)
	return time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, location)
}

func setCapturedHealthBin(bins []recordingHealthBin, ordinal, captured int64) error {
	if ordinal < 1 || ordinal > int64(len(bins)) {
		return fmt.Errorf("capture health bin ordinal %d out of range", ordinal)
	}
	bins[ordinal-1].Captured = captured
	return nil
}

func recordingHistoryWindow(status string, startAt time.Time, endAt, pausedAt *time.Time, now time.Time) (time.Time, time.Time) {
	end := now.UTC()
	if status == "paused" && pausedAt != nil {
		end = pausedAt.UTC()
	}
	if endAt != nil && end.After(endAt.UTC()) {
		end = endAt.UTC()
	}
	start := startAt.UTC()
	if end.Before(start) {
		end = start
	}
	return start, end
}

func parseCaptureHealthDate(raw string, fallback time.Time, location *time.Location) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Date(fallback.Year(), fallback.Month(), fallback.Day(), 0, 0, 0, 0, location), nil
	}
	return time.ParseInLocation("2006-01-02", raw, location)
}
