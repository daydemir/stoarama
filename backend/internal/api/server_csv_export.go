package api

import (
	"encoding/csv"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/util"
)

// writeCSVHeaders sets the response headers for a streamed CSV attachment
// download. It must be called before the first body byte is written so the
// browser saves the file rather than rendering it inline.
func writeCSVHeaders(w http.ResponseWriter, filename string) {
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
}

// csvTime formats a UTC RFC3339 timestamp for a CSV cell, or "" when nil.
func csvTime(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// handleAccountRecordingsCSV streams the signed-in account's recordings as a
// plain metadata CSV (no media). It reuses the exact list SELECT + scanner from
// handleAccountRecordingsList so columns and ownership scope are identical, and
// writes one CSV record per row directly to w (no slice buffering).
func (s *Server) handleAccountRecordingsCSV(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	rows, err := s.pool.Query(r.Context(), recordingListSelectSQL+`
		WHERE rec.account_id=$1 AND rec.status <> 'canceled'
		ORDER BY rec.created_at DESC, rec.id DESC
	`, principal.AccountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list recordings: %v", err))
		return
	}
	defer rows.Close()

	writeCSVHeaders(w, fmt.Sprintf("stoarama-recordings-%s.csv", time.Now().UTC().Format("2006-01-02")))
	cw := csv.NewWriter(w)
	_ = cw.Write([]string{
		"id", "name", "stream_name", "stream_url", "source_kind",
		"cron_expr", "cron_timezone", "clip_duration_sec", "status", "health",
		"start_at", "end_at", "next_fire_at", "last_clip_at", "created_at",
		"recent_clip_count_24h", "storage_destination_id", "storage_destination_name",
		"storage_managed", "stream_id", "stream_location",
	})
	for rows.Next() {
		item, err := scanRecordingListRow(rows, s.billing != nil)
		if err != nil {
			// Headers are already sent; terminate the body. The truncated CSV
			// surfaces the failure rather than silently returning a short file.
			cw.Flush()
			return
		}
		_ = cw.Write([]string{
			csvAny(item["id"]),
			csvAny(item["name"]),
			csvAny(item["stream_name"]),
			csvAny(item["stream_url"]),
			csvAny(item["source_kind"]),
			csvAny(item["cron_expr"]),
			csvAny(item["cron_timezone"]),
			csvAny(item["clip_duration_sec"]),
			csvAny(item["status"]),
			csvAny(item["health"]),
			csvAnyTime(item["start_at"]),
			csvAnyTime(item["end_at"]),
			csvAnyTime(item["next_fire_at"]),
			csvAnyTime(item["last_clip_at"]),
			csvAnyTime(item["created_at"]),
			csvAny(item["recent_clip_count"]),
			csvAny(item["storage_destination_id"]),
			csvAny(item["storage_destination_name"]),
			csvAny(item["storage_managed"]),
			csvAny(item["stream_id"]),
			csvAny(item["stream_location"]),
		})
	}
	cw.Flush()
}

// handleAccountRecordingClipsCSV streams ALL clips for one recording owned by
// the session account as a plain metadata CSV (no media). Ownership is enforced
// with the same EXISTS guard as handleAccountRecordingClips. The clip query has
// no LIMIT/OFFSET: pgx streams rows so a recording with thousands of clips never
// materializes in memory; each row is written straight to w and flushed.
func (s *Server) handleAccountRecordingClipsCSV(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	recordingID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}

	// Ownership + recording name in one lookup (404 when not the caller's).
	var recordingName string
	err := s.pool.QueryRow(r.Context(), `
		SELECT name FROM recordings WHERE id=$1 AND account_id=$2 AND status <> 'canceled'
	`, recordingID, principal.AccountID).Scan(&recordingName)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "recording not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load recording: %v", err))
		return
	}

	rows, err := s.pool.Query(r.Context(), `
		SELECT id, fire_at, clip_start_at, clip_end_at, duration_ms, actual_fps, size_bytes, object_key, purged_at
		FROM recording_clips
		WHERE recording_id=$1
		ORDER BY fire_at DESC
	`, recordingID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list clips: %v", err))
		return
	}
	defer rows.Close()

	slug := slugifyName(recordingName)
	if slug == "" {
		slug = fmt.Sprintf("recording-%d", recordingID)
	}
	writeCSVHeaders(w, fmt.Sprintf("%s-clips-%s.csv", slug, time.Now().UTC().Format("2006-01-02")))
	cw := csv.NewWriter(w)
	_ = cw.Write([]string{
		"id", "filename", "fire_at", "start", "end", "duration_ms",
		"actual_fps", "size_bytes", "object_key", "status",
	})
	for rows.Next() {
		var (
			clipID      int64
			fireAt      time.Time
			clipStartAt time.Time
			clipEndAt   time.Time
			durationMs  int64
			actualFPS   *float64
			sizeBytes   int64
			objectKey   string
			purgedAt    *time.Time
		)
		if err := rows.Scan(&clipID, &fireAt, &clipStartAt, &clipEndAt, &durationMs, &actualFPS, &sizeBytes, &objectKey, &purgedAt); err != nil {
			// Headers are already sent; flush what we have and stop.
			cw.Flush()
			return
		}
		status := "live"
		if purgedAt != nil {
			status = "purged"
		}
		fps := ""
		if actualFPS != nil {
			fps = strconv.FormatFloat(*actualFPS, 'f', -1, 64)
		}
		_ = cw.Write([]string{
			strconv.FormatInt(clipID, 10),
			buildClipDownloadFilename(recordingName, recordingID, clipStartAt, objectKey),
			fireAt.UTC().Format(time.RFC3339Nano),
			clipStartAt.UTC().Format(time.RFC3339Nano),
			clipEndAt.UTC().Format(time.RFC3339Nano),
			strconv.FormatInt(durationMs, 10),
			fps,
			strconv.FormatInt(sizeBytes, 10),
			strings.TrimSpace(objectKey),
			status,
		})
		// Flush periodically so rows reach the client incrementally and peak
		// memory stays O(1) for large clip sets.
		cw.Flush()
	}
	cw.Flush()
}

// handleDashboardStreamsCSV streams the dashboard streams catalog as a plain CSV
// for labelers to do stream selection in a spreadsheet. It honors the SAME
// filters as the streams list (so a filtered catalog exports filtered) via
// dashboardBuildStreamWhereFromRequest, and embeds two DURABLE links per stream:
// a stable detail page and a refresh/resolve link that re-resolves the playable
// URL on demand (never an expiring one-time token). It deliberately omits the
// heavy per-stream aggregate/preview passes the JSON dashboard computes.
func (s *Server) handleDashboardStreamsCSV(w http.ResponseWriter, r *http.Request) {
	where, args, err := dashboardBuildStreamWhereFromRequest(r, dashboardStreamWhereConfig{
		IncludeSearch:         true,
		IncludeProvider:       true,
		IncludeSource:         true,
		IncludeYouTubeChannel: true,
		IncludeCaptureMode:    true,
	})
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	base := strings.TrimRight(strings.TrimSpace(s.cfg.AppBaseURL), "/")
	if base == "" {
		util.WriteError(w, http.StatusServiceUnavailable, "app base url is not configured")
		return
	}
	whereSQL := strings.Join(where, " AND ")
	// One streamed pass over the filtered catalog. rt.resolved_url is the
	// runtime-resolved playable manifest already persisted for the stream (same
	// column the JSON dashboard exposes as capture_runtime_resolved_url); it is
	// read from the row, never re-resolved per row, so the export stays O(1) memory
	// and adds no network calls. For indirect '!hls' sources it can be a rolling
	// tokenized URL that expires, so source_url (the durable reference) and the
	// per-row refresh_url are always emitted alongside it. sis.* carry the
	// survey/detection rollups present on the catalog row.
	rows, err := s.pool.Query(r.Context(), fmt.Sprintf(`
		SELECT
			s.id, s.name, s.provider, %s AS source_kind,
			s.source_url, s.source_page_url, s.capture_type, s.execution_class,
			(s.capture_type IN ('hls','http_video')) AS recordable,
			rt.resolved_url,
			%s AS city, %s AS country, s.location_text, s.lat, s.lon,
			s.tags, s.recording_state, s.created_at, s.updated_at,
			COALESCE(sis.inferenced_captures, 0)::bigint,
			COALESCE(sis.person_detections_total, 0)::bigint,
			COALESCE(sis.avg_people_per_inferenced_capture, 0)::double precision
		FROM streams s
		LEFT JOIN stream_capture_runtime rt ON rt.stream_id = s.id
		LEFT JOIN stream_inference_stats sis ON sis.stream_id = s.id
		WHERE %s
		ORDER BY s.id ASC
	`, dashboardSourceExprSQL(), dashboardCityExprSQL(), dashboardCountryExprSQL(), whereSQL), args...)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("dashboard streams csv query: %v", err))
		return
	}
	defer rows.Close()

	writeCSVHeaders(w, fmt.Sprintf("stoarama-streams-%s.csv", time.Now().UTC().Format("2006-01-02")))
	cw := csv.NewWriter(w)
	_ = cw.Write([]string{
		"id", "name", "provider", "source", "source_url", "source_page_url",
		"capture_type", "execution_class", "recordable", "resolved_live_url",
		"city", "country", "location_text", "lat", "lon", "tags",
		"recording_state", "created_at", "updated_at",
		"inferenced_captures", "person_detections_total", "avg_people_per_inferenced_capture",
		"detail_url", "refresh_url",
	})
	for rows.Next() {
		var (
			id                    int64
			name                  string
			provider              string
			source                *string
			sourceURL             string
			sourcePageURL         string
			captureType           string
			executionClass        string
			recordable            bool
			resolvedURL           *string
			city                  *string
			country               *string
			locationText          string
			lat                   *float64
			lon                   *float64
			tags                  []string
			recordingState        string
			createdAt             time.Time
			updatedAt             time.Time
			inferencedCaptures    int64
			personDetectionsTotal int64
			avgPeoplePerCapture   float64
		)
		if err := rows.Scan(&id, &name, &provider, &source, &sourceURL, &sourcePageURL, &captureType, &executionClass, &recordable, &resolvedURL, &city, &country, &locationText, &lat, &lon, &tags, &recordingState, &createdAt, &updatedAt, &inferencedCaptures, &personDetectionsTotal, &avgPeoplePerCapture); err != nil {
			cw.Flush()
			return
		}
		_ = cw.Write([]string{
			strconv.FormatInt(id, 10),
			name,
			provider,
			derefString(source),
			strings.TrimSpace(sourceURL),
			strings.TrimSpace(sourcePageURL),
			captureType,
			executionClass,
			strconv.FormatBool(recordable),
			strings.TrimSpace(derefString(resolvedURL)),
			derefString(city),
			derefString(country),
			strings.TrimSpace(locationText),
			csvFloat(lat),
			csvFloat(lon),
			strings.Join(tags, ";"),
			recordingState,
			createdAt.UTC().Format(time.RFC3339),
			updatedAt.UTC().Format(time.RFC3339),
			strconv.FormatInt(inferencedCaptures, 10),
			strconv.FormatInt(personDetectionsTotal, 10),
			strconv.FormatFloat(avgPeoplePerCapture, 'f', -1, 64),
			fmt.Sprintf("%s/streams/%d", base, id),
			fmt.Sprintf("%s/api/v1/dashboard/streams/%d/resolve", base, id),
		})
		cw.Flush()
	}
	cw.Flush()
}

// csvAny renders a scalar from the recordings list map for a CSV cell. nil ->
// "", *string/*int64 are dereferenced; everything else uses %v.
func csvAny(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case *string:
		if t == nil {
			return ""
		}
		return *t
	case int64:
		return strconv.FormatInt(t, 10)
	case *int64:
		if t == nil {
			return ""
		}
		return strconv.FormatInt(*t, 10)
	case int:
		return strconv.Itoa(t)
	case bool:
		return strconv.FormatBool(t)
	default:
		return fmt.Sprintf("%v", t)
	}
}

// csvAnyTime renders a time value (time.Time or *time.Time) from the recordings
// list map as UTC RFC3339, or "" when nil.
func csvAnyTime(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case time.Time:
		return t.UTC().Format(time.RFC3339)
	case *time.Time:
		return csvTime(t)
	default:
		return ""
	}
}

// csvFloat renders a nullable float for a CSV cell, or "" when nil.
func csvFloat(f *float64) string {
	if f == nil {
		return ""
	}
	return strconv.FormatFloat(*f, 'f', -1, 64)
}
