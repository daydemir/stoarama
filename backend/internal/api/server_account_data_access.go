package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/util"
)

const accountClipBatchLimit = 120
const clipRangeMaxDuration = 4 * time.Hour
const clipRangeMaxItems = 5000
const clipAvailabilityDays = 45

type clipTimeRange struct {
	From time.Time `json:"captured_from"`
	To   time.Time `json:"captured_to"`
}

type clipAvailabilityDay struct {
	Day       string `json:"day"`
	ClipCount int64  `json:"clip_count"`
}

type clipAvailabilityHour struct {
	HourStart       time.Time `json:"hour_start"`
	ClipCount       int64     `json:"clip_count"`
	TotalDurationMs int64     `json:"total_duration_ms"`
	TotalSizeBytes  int64     `json:"total_size_bytes"`
}

type dataAccessSpecEndpoint struct {
	Key         string            `json:"key"`
	Method      string            `json:"method"`
	Path        string            `json:"path"`
	Auth        string            `json:"auth"`
	Description string            `json:"description"`
	Query       map[string]string `json:"query,omitempty"`
	Limit       int               `json:"limit,omitempty"`
}

type accountClipDownloadPrepareRequest struct {
	StreamID   int64   `json:"stream_id"`
	SegmentIDs []int64 `json:"segment_ids"`
}

type accountClipDownloadItem struct {
	ID                   int64     `json:"id"`
	StreamID             int64     `json:"stream_id"`
	SegmentStart         time.Time `json:"segment_start_at"`
	SegmentEnd           time.Time `json:"segment_end_at"`
	DownloadURL          string    `json:"download_url"`
	ThumbnailDownloadURL string    `json:"thumbnail_download_url,omitempty"`
	Filename             string    `json:"filename"`
}

func (s *Server) handleDataAccessSpec(w http.ResponseWriter, r *http.Request) {
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"auth_model": map[string]any{
			"public_reads":    "Stream metadata and public previews are available without auth.",
			"account_session": "A signed-in browser session is used for recording changes and account clip browsing.",
			"api_key":         "API keys act as account credentials for CLI/script access to authenticated data APIs.",
		},
		"batch_limits": map[string]any{
			"clip_download_prepare_max_segments": accountClipBatchLimit,
		},
		"endpoints": []dataAccessSpecEndpoint{
			{
				Key:         "stream_search",
				Method:      http.MethodGet,
				Path:        "/api/v1/dashboard/streams",
				Auth:        "public",
				Description: "Search the public stream catalog.",
				Query: map[string]string{
					"q":                  "free-text stream search",
					"recording_state":    "use on for recorded streams only",
					"limit":              "page size",
					"offset":             "page offset",
					"include_image_urls": "0 to skip preview URLs",
				},
				Limit: 2000,
			},
			{
				Key:         "stream_detail",
				Method:      http.MethodGet,
				Path:        "/api/v1/dashboard/streams/{id}",
				Auth:        "public",
				Description: "Load public stream detail, including preview-oriented metadata.",
			},
			{
				Key:         "clip_list",
				Method:      http.MethodGet,
				Path:        "/api/v1/streams/{id}/clips",
				Auth:        "public",
				Description: "Browse recorded clips for one stream without signing in.",
				Query: map[string]string{
					"limit":  "page size",
					"offset": "page offset",
				},
				Limit: 200,
			},
			{
				Key:         "clip_download_prepare",
				Method:      http.MethodPost,
				Path:        "/api/v1/clips/download-prepare",
				Auth:        "public",
				Description: "Prepare up to 120 clip downloads for a selected stream without signing in.",
				Limit:       accountClipBatchLimit,
			},
			{
				Key:         "account_clip_list",
				Method:      http.MethodGet,
				Path:        "/api/v1/account/streams/{id}/clips",
				Auth:        "account",
				Description: "Browse recorded clips for one stream with session or API key auth.",
				Query: map[string]string{
					"limit":  "page size",
					"offset": "page offset",
				},
				Limit: 200,
			},
			{
				Key:         "account_clip_download_prepare",
				Method:      http.MethodPost,
				Path:        "/api/v1/account/clips/download-prepare",
				Auth:        "account",
				Description: "Prepare up to 120 clip downloads for a selected stream with session or API key auth.",
				Limit:       accountClipBatchLimit,
			},
		},
	})
}

func (s *Server) handleAccountStreamClipsList(w http.ResponseWriter, r *http.Request) {
	s.handleStreamClipsList(w, r)
}

func (s *Server) handleAccountStreamClipsAvailability(w http.ResponseWriter, r *http.Request) {
	s.handleStreamClipsAvailability(w, r)
}

func (s *Server) handleAccountStreamClipsRange(w http.ResponseWriter, r *http.Request) {
	s.handleStreamClipsRange(w, r)
}

func (s *Server) handlePublicStreamClipsList(w http.ResponseWriter, r *http.Request) {
	s.handleStreamClipsList(w, r)
}

func (s *Server) handlePublicStreamClipsAvailability(w http.ResponseWriter, r *http.Request) {
	s.handleStreamClipsAvailability(w, r)
}

func (s *Server) handlePublicStreamClipsRange(w http.ResponseWriter, r *http.Request) {
	s.handleStreamClipsRange(w, r)
}

func (s *Server) handleStreamClipsList(w http.ResponseWriter, r *http.Request) {
	streamID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	if _, err := s.getStreamByID(r.Context(), streamID); err != nil {
		util.WriteError(w, http.StatusNotFound, err.Error())
		return
	}
	limit := parseIntQuery(r, "limit", 60, 1, 200)
	offset := parseIntQuery(r, "offset", 0, 0, 1_000_000)
	clipRange, err := parseOptionalClipTimeRange(r)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	opts := captureSegmentQueryOptions{
		StreamID:                    streamID,
		TimeRange:                   clipRange,
		CaptureStatus:               "success",
		RequireDownloadable:         true,
		Limit:                       limit,
		Offset:                      offset,
		IncludeDownloadURL:          true,
		IncludeThumbnailDownloadURL: true,
	}
	total, err := s.countCaptureSegments(r.Context(), opts)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	items, err := s.queryCaptureSegments(r.Context(), opts)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	payload := map[string]any{
		"stream_id": streamID,
		"limit":     limit,
		"offset":    offset,
		"total":     total,
		"items":     items,
	}
	if clipRange != nil {
		payload["captured_from"] = clipRange.From
		payload["captured_to"] = clipRange.To
	}
	util.WriteJSON(w, http.StatusOK, payload)
}

func (s *Server) handleStreamClipsAvailability(w http.ResponseWriter, r *http.Request) {
	streamID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	if _, err := s.getStreamByID(r.Context(), streamID); err != nil {
		util.WriteError(w, http.StatusNotFound, err.Error())
		return
	}
	days, err := s.queryClipAvailabilityDays(r.Context(), streamID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	day := strings.TrimSpace(r.URL.Query().Get("day"))
	if day == "" && len(days) > 0 {
		day = days[0].Day
	}
	hours := []clipAvailabilityHour{}
	if day != "" {
		var err error
		hours, err = s.queryClipAvailabilityHours(r.Context(), streamID, day)
		if err != nil {
			util.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"stream_id":     streamID,
		"selected_day":  day,
		"days":          days,
		"hour_buckets":  hours,
		"max_range_sec": int64(clipRangeMaxDuration.Seconds()),
	})
}

func (s *Server) handleStreamClipsRange(w http.ResponseWriter, r *http.Request) {
	streamID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	stream, err := s.getStreamByID(r.Context(), streamID)
	if err != nil {
		util.WriteError(w, http.StatusNotFound, err.Error())
		return
	}
	clipRange, err := parseRequiredClipTimeRange(r)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	opts := captureSegmentQueryOptions{
		StreamID:            streamID,
		TimeRange:           clipRange,
		CaptureStatus:       "success",
		RequireDownloadable: true,
		Limit:               clipRangeMaxItems,
		Offset:              0,
		IncludeDownloadURL:  true,
	}
	total, err := s.countCaptureSegments(r.Context(), opts)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if total > clipRangeMaxItems {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("clip range has %d clips; max=%d", total, clipRangeMaxItems))
		return
	}
	items, err := s.queryCaptureSegments(r.Context(), opts)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	totalDurationMs := int64(0)
	totalSizeBytes := int64(0)
	for _, item := range items {
		totalDurationMs += item.DurationMs
		if item.SizeBytes != nil {
			totalSizeBytes += *item.SizeBytes
		}
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"stream_id":         streamID,
		"stream_slug":       stream.Slug,
		"captured_from":     clipRange.From,
		"captured_to":       clipRange.To,
		"count":             len(items),
		"total_duration_ms": totalDurationMs,
		"total_size_bytes":  totalSizeBytes,
		"items":             items,
		"max_range_sec":     int64(clipRangeMaxDuration.Seconds()),
	})
}

func parseOptionalClipTimeRange(r *http.Request) (*clipTimeRange, error) {
	fromRaw := strings.TrimSpace(r.URL.Query().Get("captured_from"))
	toRaw := strings.TrimSpace(r.URL.Query().Get("captured_to"))
	if fromRaw == "" && toRaw == "" {
		return nil, nil
	}
	if fromRaw == "" || toRaw == "" {
		return nil, fmt.Errorf("captured_from and captured_to are required together")
	}
	return parseClipTimeRange(fromRaw, toRaw)
}

func parseRequiredClipTimeRange(r *http.Request) (*clipTimeRange, error) {
	fromRaw := strings.TrimSpace(r.URL.Query().Get("captured_from"))
	toRaw := strings.TrimSpace(r.URL.Query().Get("captured_to"))
	if fromRaw == "" || toRaw == "" {
		return nil, fmt.Errorf("captured_from and captured_to are required")
	}
	return parseClipTimeRange(fromRaw, toRaw)
}

func parseClipTimeRange(fromRaw, toRaw string) (*clipTimeRange, error) {
	from, err := time.Parse(time.RFC3339Nano, fromRaw)
	if err != nil {
		return nil, fmt.Errorf("invalid captured_from; expected RFC3339")
	}
	to, err := time.Parse(time.RFC3339Nano, toRaw)
	if err != nil {
		return nil, fmt.Errorf("invalid captured_to; expected RFC3339")
	}
	from = from.UTC()
	to = to.UTC()
	if !to.After(from) {
		return nil, fmt.Errorf("captured_to must be after captured_from")
	}
	if to.Sub(from) > clipRangeMaxDuration {
		return nil, fmt.Errorf("clip range cannot exceed 4 hours")
	}
	return &clipTimeRange{From: from, To: to}, nil
}

func (s *Server) queryClipAvailabilityDays(ctx context.Context, streamID int64) ([]clipAvailabilityDay, error) {
	var latestDay time.Time
	if err := s.pool.QueryRow(ctx, `
		SELECT (cs.segment_start_at AT TIME ZONE 'UTC')::date AS day
		FROM capture_segments cs
		WHERE cs.stream_id=$1
		  AND cs.capture_status='success'
		  AND cs.media_object_id IS NOT NULL
		ORDER BY cs.segment_start_at DESC, cs.id DESC
		LIMIT 1
	`, streamID).Scan(&latestDay); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return []clipAvailabilityDay{}, nil
		}
		return nil, fmt.Errorf("query latest clip day: %w", err)
	}
	windowStart := latestDay.UTC().AddDate(0, 0, -(clipAvailabilityDays - 1))
	windowEnd := latestDay.UTC().AddDate(0, 0, 1)
	rows, err := s.pool.Query(ctx, `
		SELECT
			(cs.segment_start_at AT TIME ZONE 'UTC')::date AS day,
			COUNT(*)::bigint AS clip_count
		FROM capture_segments cs
		WHERE cs.stream_id=$1
		  AND cs.capture_status='success'
		  AND cs.media_object_id IS NOT NULL
		  AND cs.segment_start_at >= $2
		  AND cs.segment_start_at < $3
		GROUP BY 1
		ORDER BY 1 DESC
	`, streamID, windowStart, windowEnd)
	if err != nil {
		return nil, fmt.Errorf("query clip availability days: %w", err)
	}
	defer rows.Close()

	days := []clipAvailabilityDay{}
	for rows.Next() {
		var day time.Time
		var item clipAvailabilityDay
		if err := rows.Scan(&day, &item.ClipCount); err != nil {
			return nil, fmt.Errorf("scan clip availability day: %w", err)
		}
		item.Day = day.UTC().Format("2006-01-02")
		days = append(days, item)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate clip availability days: %w", rows.Err())
	}
	return days, nil
}

func (s *Server) queryClipAvailabilityHours(ctx context.Context, streamID int64, dayRaw string) ([]clipAvailabilityHour, error) {
	day, err := time.Parse("2006-01-02", strings.TrimSpace(dayRaw))
	if err != nil {
		return nil, fmt.Errorf("invalid day; expected YYYY-MM-DD")
	}
	day = day.UTC()
	dayEnd := day.AddDate(0, 0, 1)
	rows, err := s.pool.Query(ctx, `
		WITH hours AS (
			SELECT
				gs.hour_start,
				gs.hour_start + interval '1 hour' AS hour_end
			FROM generate_series($2::timestamptz, $2::timestamptz + interval '23 hours', interval '1 hour') AS gs(hour_start)
		)
		SELECT
			h.hour_start,
			COUNT(cs.id)::bigint,
			COALESCE(SUM(cs.duration_ms)::bigint, 0),
			0::bigint
		FROM hours h
		LEFT JOIN capture_segments cs
		  ON cs.stream_id=$1
		 AND cs.capture_status='success'
		 AND cs.media_object_id IS NOT NULL
		 AND cs.segment_end_at > h.hour_start
		 AND cs.segment_start_at < h.hour_end
		 AND cs.segment_end_at > $2
		 AND cs.segment_start_at < $3
		GROUP BY h.hour_start
		ORDER BY h.hour_start ASC
	`, streamID, day, dayEnd)
	if err != nil {
		return nil, fmt.Errorf("query clip availability hours: %w", err)
	}
	defer rows.Close()

	hours := make([]clipAvailabilityHour, 0, 24)
	for rows.Next() {
		var item clipAvailabilityHour
		if err := rows.Scan(&item.HourStart, &item.ClipCount, &item.TotalDurationMs, &item.TotalSizeBytes); err != nil {
			return nil, fmt.Errorf("scan clip availability hour: %w", err)
		}
		item.HourStart = item.HourStart.UTC()
		hours = append(hours, item)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate clip availability hours: %w", rows.Err())
	}
	return hours, nil
}

func (s *Server) handleAccountClipDownloadPrepare(w http.ResponseWriter, r *http.Request) {
	s.handleClipDownloadPrepare(w, r)
}

func (s *Server) handlePublicClipDownloadPrepare(w http.ResponseWriter, r *http.Request) {
	s.handleClipDownloadPrepare(w, r)
}

func (s *Server) handleClipDownloadPrepare(w http.ResponseWriter, r *http.Request) {
	var req accountClipDownloadPrepareRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.StreamID <= 0 {
		util.WriteError(w, http.StatusBadRequest, "stream_id is required")
		return
	}
	segmentIDs := uniquePositiveInt64s(req.SegmentIDs)
	if len(segmentIDs) == 0 {
		util.WriteError(w, http.StatusBadRequest, "segment_ids is required")
		return
	}
	if len(segmentIDs) > accountClipBatchLimit {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("segment_ids limit exceeded; max=%d", accountClipBatchLimit))
		return
	}
	stream, err := s.getStreamByID(r.Context(), req.StreamID)
	if err != nil {
		util.WriteError(w, http.StatusNotFound, err.Error())
		return
	}
	items, err := s.queryCaptureSegments(r.Context(), captureSegmentQueryOptions{
		StreamID:                    req.StreamID,
		SegmentIDs:                  segmentIDs,
		CaptureStatus:               "success",
		RequireDownloadable:         true,
		Limit:                       len(segmentIDs),
		Offset:                      0,
		IncludeDownloadURL:          true,
		IncludeThumbnailDownloadURL: true,
	})
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	byID := make(map[int64]captureSegmentListItem, len(items))
	for _, item := range items {
		byID[item.ID] = item
	}
	out := make([]accountClipDownloadItem, 0, len(segmentIDs))
	for _, id := range segmentIDs {
		item, ok := byID[id]
		if !ok {
			util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("segment %d not found for stream %d", id, req.StreamID))
			return
		}
		if strings.TrimSpace(item.DownloadURL) == "" {
			util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("segment %d is not downloadable", id))
			return
		}
		out = append(out, accountClipDownloadItem{
			ID:                   item.ID,
			StreamID:             item.StreamID,
			SegmentStart:         item.SegmentStartAt,
			SegmentEnd:           item.SegmentEndAt,
			DownloadURL:          item.DownloadURL,
			ThumbnailDownloadURL: item.ThumbnailDownloadURL,
			Filename:             buildAccountClipFilename(stream.Slug, item),
		})
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"stream_id":       req.StreamID,
		"requested_count": len(segmentIDs),
		"items":           out,
		"max_segments":    accountClipBatchLimit,
	})
}

func buildAccountClipFilename(streamSlug string, item captureSegmentListItem) string {
	slug := strings.TrimSpace(streamSlug)
	if slug == "" {
		slug = fmt.Sprintf("stream-%d", item.StreamID)
	}
	ext := fileExtensionFromMIME(derefString(item.MIMEType))
	if ext == "" {
		ext = ".mp4"
	}
	return fmt.Sprintf(
		"%s-%s%s",
		slug,
		item.SegmentStartAt.UTC().Format("20060102T150405Z"),
		ext,
	)
}

func uniquePositiveInt64s(in []int64) []int64 {
	out := make([]int64, 0, len(in))
	seen := make(map[int64]struct{}, len(in))
	for _, v := range in {
		if v <= 0 {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}
