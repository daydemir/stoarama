package api

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/model"
	"github.com/daydemir/stoarama/backend/internal/settings"
	"github.com/daydemir/stoarama/backend/internal/util"
)

type dashboardStreamRecordingHealthStream struct {
	ID                           int64                `json:"id"`
	Provider                     string               `json:"provider"`
	Name                         string               `json:"name"`
	Slug                         string               `json:"slug"`
	SourceURL                    string               `json:"source_url"`
	SourcePageURL                string               `json:"source_page_url"`
	LocationText                 string               `json:"location_text"`
	LocationCountry              string               `json:"location_country"`
	LocationCountryCode          string               `json:"location_country_code"`
	LocationCity                 string               `json:"location_city"`
	RecordingState               model.RecordingState `json:"recording_state"`
	CaptureType                  string               `json:"capture_type"`
	ExecutionClass               string               `json:"execution_class"`
	EffectiveExecutionClass      string               `json:"effective_execution_class"`
	CaptureUnit                  string               `json:"capture_unit"`
	CaptureRuntimeStatus         *string              `json:"capture_runtime_status,omitempty"`
	CaptureRuntimeExecutionClass *string              `json:"capture_runtime_execution_class,omitempty"`
	CaptureRuntimeLastFrameAt    *time.Time           `json:"capture_runtime_last_frame_at,omitempty"`
	AssignmentExecutionClass     *string              `json:"assignment_execution_class,omitempty"`
}

type dashboardStreamRecordingHealthBucket struct {
	HourStartUTC     time.Time `json:"hour_start_utc"`
	HourEndUTC       time.Time `json:"hour_end_utc"`
	ExpectedCaptures int64     `json:"expected_captures"`
	SuccessCaptures  int64     `json:"success_captures"`
	ErrorCaptures    int64     `json:"error_captures"`
	MissingCaptures  int64     `json:"missing_captures"`
	LossRatePct      float64   `json:"loss_rate_pct"`
}

type dashboardStreamRecordingHealthSummary struct {
	Buckets               int     `json:"buckets"`
	HoursWithLoss         int     `json:"hours_with_loss"`
	HoursWithErrors       int     `json:"hours_with_errors"`
	TotalExpectedCaptures int64   `json:"total_expected_captures"`
	TotalSuccessCaptures  int64   `json:"total_success_captures"`
	TotalErrorCaptures    int64   `json:"total_error_captures"`
	TotalMissingCaptures  int64   `json:"total_missing_captures"`
	AvgLossRatePct        float64 `json:"avg_loss_rate_pct"`
	MaxLossRatePct        float64 `json:"max_loss_rate_pct"`
}

type dashboardStreamRecordingHealthResponse struct {
	Stream                  dashboardStreamRecordingHealthStream   `json:"stream"`
	Timezone                string                                 `json:"timezone"`
	Hours                   int                                    `json:"hours"`
	WindowStartUTC          time.Time                              `json:"window_start_utc"`
	WindowEndUTCExclusive   time.Time                              `json:"window_end_utc_exclusive"`
	RecordingIntervalSec    int                                    `json:"recording_interval_sec"`
	ExpectedCapturesPerHour int64                                  `json:"expected_captures_per_hour"`
	Buckets                 []dashboardStreamRecordingHealthBucket `json:"buckets"`
	Summary                 dashboardStreamRecordingHealthSummary  `json:"summary"`
}

type recordingHealthHourlyCounts struct {
	Success int64
	Error   int64
}

func (s *Server) handleDashboardStreamRecordingHealth(w http.ResponseWriter, r *http.Request) {
	streamID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	hours := parseIntQuery(r, "hours", 72, 24, 336)

	recordingSettings, err := settings.GetRecordingSettings(r.Context(), s.pool)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if recordingSettings.CaptureIntervalSec <= 0 {
		recordingSettings.CaptureIntervalSec = settings.DefaultRecordingIntervalSec
	}

	stream, err := s.getStreamByID(r.Context(), streamID)
	if err != nil {
		util.WriteError(w, http.StatusNotFound, err.Error())
		return
	}

	var assignedExecutionClass *string
	if err := s.pool.QueryRow(r.Context(), `
		SELECT execution_class
		FROM recording_assignments
		WHERE stream_id=$1
	`, streamID).Scan(&assignedExecutionClass); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query stream assignment: %v", err))
		return
	}

	effectiveExecutionClass := firstNonEmpty(
		strings.TrimSpace(defaultStringPtr(assignedExecutionClass)),
		defaultStringPtr(stream.CaptureRuntimeClass),
		stream.ExecutionClass,
	)
	captureUnit := captureUnitLabelForExecutionClass(effectiveExecutionClass)
	expectedPerHour := expectedCapturesPerHour(effectiveExecutionClass, recordingSettings.CaptureIntervalSec)

	windowEnd := time.Now().UTC().Truncate(time.Hour)
	windowStart := windowEnd.Add(-time.Duration(hours) * time.Hour)

	counts, err := s.queryHourlyRecordingHealthCounts(r.Context(), streamID, isClipNativeExecutionClass(effectiveExecutionClass), windowStart, windowEnd)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query stream recording health buckets: %v", err))
		return
	}

	buckets, summary := buildRecordingHealthBuckets(windowStart, windowEnd, expectedPerHour, counts)

	streamInfo := dashboardStreamRecordingHealthStream{
		ID:                           stream.ID,
		Provider:                     stream.Provider,
		Name:                         stream.Name,
		Slug:                         stream.Slug,
		SourceURL:                    stream.SourceURL,
		SourcePageURL:                stream.SourcePageURL,
		LocationText:                 stream.LocationText,
		LocationCountry:              stream.LocationCountry,
		LocationCountryCode:          stream.LocationCountryCode,
		LocationCity:                 stream.LocationCity,
		RecordingState:               stream.RecordingState,
		CaptureType:                  stream.CaptureType,
		ExecutionClass:               stream.ExecutionClass,
		EffectiveExecutionClass:      effectiveExecutionClass,
		CaptureUnit:                  captureUnit,
		CaptureRuntimeStatus:         stream.CaptureRuntimeStatus,
		CaptureRuntimeExecutionClass: stream.CaptureRuntimeClass,
		CaptureRuntimeLastFrameAt:    stream.CaptureRuntimeLastSeen,
		AssignmentExecutionClass:     assignedExecutionClass,
	}

	util.WriteJSON(w, http.StatusOK, dashboardStreamRecordingHealthResponse{
		Stream:                  streamInfo,
		Timezone:                "UTC",
		Hours:                   hours,
		WindowStartUTC:          windowStart,
		WindowEndUTCExclusive:   windowEnd,
		RecordingIntervalSec:    recordingSettings.CaptureIntervalSec,
		ExpectedCapturesPerHour: expectedPerHour,
		Buckets:                 buckets,
		Summary:                 summary,
	})
}

func (s *Server) queryHourlyRecordingHealthCounts(ctx context.Context, streamID int64, clipNative bool, windowStart, windowEnd time.Time) (map[time.Time]recordingHealthHourlyCounts, error) {
	counts := make(map[time.Time]recordingHealthHourlyCounts)
	tsColumn := "f.captured_at"
	table := "frames f"
	if clipNative {
		tsColumn = "cs.segment_end_at"
		table = "capture_segments cs"
	}

	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT
			(date_trunc('hour', %s AT TIME ZONE 'UTC') AT TIME ZONE 'UTC') AS hour_start,
			COUNT(*) FILTER (WHERE capture_status='success')::bigint AS success_captures,
			COUNT(*) FILTER (WHERE capture_status='error')::bigint AS error_captures
		FROM %s
		WHERE stream_id=$1
		  AND %s >= $2
		  AND %s < $3
		GROUP BY 1
		ORDER BY 1 ASC
	`, tsColumn, table, tsColumn, tsColumn), streamID, windowStart, windowEnd)
	if err != nil {
		return nil, fmt.Errorf("query hourly recording health counts: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var hourStart time.Time
		var successCaptures, errorCaptures int64
		if err := rows.Scan(&hourStart, &successCaptures, &errorCaptures); err != nil {
			return nil, fmt.Errorf("scan hourly recording health bucket: %w", err)
		}
		counts[hourStart.UTC().Truncate(time.Hour)] = recordingHealthHourlyCounts{
			Success: successCaptures,
			Error:   errorCaptures,
		}
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate hourly recording health counts: %w", rows.Err())
	}
	return counts, nil
}

func buildRecordingHealthBuckets(windowStart, windowEnd time.Time, expectedPerHour int64, counts map[time.Time]recordingHealthHourlyCounts) ([]dashboardStreamRecordingHealthBucket, dashboardStreamRecordingHealthSummary) {
	windowStart = windowStart.UTC().Truncate(time.Hour)
	windowEnd = windowEnd.UTC().Truncate(time.Hour)
	if windowEnd.Before(windowStart) {
		windowEnd = windowStart
	}
	if expectedPerHour <= 0 {
		expectedPerHour = 1
	}

	buckets := make([]dashboardStreamRecordingHealthBucket, 0, int(windowEnd.Sub(windowStart)/time.Hour))
	summary := dashboardStreamRecordingHealthSummary{}

	for hourStart := windowStart; hourStart.Before(windowEnd); hourStart = hourStart.Add(time.Hour) {
		hourCounts := counts[hourStart]
		missing := expectedPerHour - hourCounts.Success
		if missing < 0 {
			missing = 0
		}
		lossRate := 100.0 * float64(missing) / float64(expectedPerHour)
		lossRate = math.Round(lossRate*100) / 100
		bucket := dashboardStreamRecordingHealthBucket{
			HourStartUTC:     hourStart,
			HourEndUTC:       hourStart.Add(time.Hour),
			ExpectedCaptures: expectedPerHour,
			SuccessCaptures:  hourCounts.Success,
			ErrorCaptures:    hourCounts.Error,
			MissingCaptures:  missing,
			LossRatePct:      lossRate,
		}
		buckets = append(buckets, bucket)

		summary.Buckets++
		summary.TotalExpectedCaptures += expectedPerHour
		summary.TotalSuccessCaptures += hourCounts.Success
		summary.TotalErrorCaptures += hourCounts.Error
		summary.TotalMissingCaptures += missing
		if missing > 0 {
			summary.HoursWithLoss++
		}
		if hourCounts.Error > 0 {
			summary.HoursWithErrors++
		}
		if lossRate > summary.MaxLossRatePct {
			summary.MaxLossRatePct = lossRate
		}
	}

	if summary.TotalExpectedCaptures > 0 {
		summary.AvgLossRatePct = math.Round((100.0*float64(summary.TotalMissingCaptures)/float64(summary.TotalExpectedCaptures))*100) / 100
	}

	return buckets, summary
}

func expectedCapturesPerHour(raw string, recordingIntervalSec int) int64 {
	return expectedCapturesPer60s(raw, recordingIntervalSec) * 60
}

func defaultStringPtr(v *string) string {
	if v == nil {
		return ""
	}
	return strings.TrimSpace(*v)
}
