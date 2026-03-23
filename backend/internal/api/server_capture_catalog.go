package api

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/util"
)

type captureCatalogCandidateItem struct {
	StreamID          int64      `json:"stream_id"`
	Provider          string     `json:"provider"`
	Name              string     `json:"name"`
	CaptureType       string     `json:"capture_type"`
	ExecutionClass    string     `json:"execution_class"`
	CapturesSuccess   int64      `json:"captures_success"`
	CapturesError     int64      `json:"captures_error"`
	LastCapturedAt    *time.Time `json:"last_captured_at,omitempty"`
	RuntimeStatus     *string    `json:"runtime_status,omitempty"`
	RuntimeError      *string    `json:"runtime_error,omitempty"`
	LastFrameAt       *time.Time `json:"last_frame_at,omitempty"`
	ConsecutiveErrors *int       `json:"consecutive_errors,omitempty"`
}

func (s *Server) handleServiceCaptureCatalogCandidates(w http.ResponseWriter, r *http.Request) {
	limit := parseIntQuery(r, "limit", 200, 1, 500)
	offset := parseIntQuery(r, "offset", 0, 0, 1000000)

	executionClass, err := normalizeExecutionClassInput(strings.TrimSpace(r.URL.Query().Get("execution_class")))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	switch executionClass {
	case capture.ExecutionClassVideoLive:
	default:
		util.WriteError(w, http.StatusBadRequest, "execution_class must be video_live")
		return
	}

	var total int64
	if err := s.pool.QueryRow(r.Context(), `
		SELECT COUNT(*)::bigint
		FROM streams s
		WHERE s.recording_state='off'
		  AND s.execution_class=$1
		  AND s.capture_type <> 'youtube_watch'
	`, executionClass).Scan(&total); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("count capture catalog candidates: %v", err))
		return
	}

	rows, err := s.pool.Query(r.Context(), `
		SELECT
			s.id,
			s.provider,
			s.name,
			s.capture_type,
			s.execution_class,
			COALESCE(sh.captures_success, 0)::bigint,
			COALESCE(sh.captures_error, 0)::bigint,
			sh.last_capture_at,
			rt.status,
			rt.last_error_text,
			rt.last_frame_at,
			rt.consecutive_errors
		FROM streams s
		LEFT JOIN stream_health sh ON sh.stream_id=s.id
		LEFT JOIN stream_capture_runtime rt ON rt.stream_id=s.id
		WHERE s.recording_state='off'
		  AND s.execution_class=$1
		  AND s.capture_type <> 'youtube_watch'
		ORDER BY
			CASE WHEN COALESCE(sh.captures_success, 0) = 0 THEN 0 ELSE 1 END ASC,
			COALESCE(sh.captures_error, 0) ASC,
			COALESCE(sh.last_capture_at, s.created_at) ASC,
			s.id ASC
		LIMIT $2 OFFSET $3
	`, executionClass, limit, offset)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query capture catalog candidates: %v", err))
		return
	}
	defer rows.Close()

	items := make([]captureCatalogCandidateItem, 0, limit)
	for rows.Next() {
		var item captureCatalogCandidateItem
		if err := rows.Scan(
			&item.StreamID,
			&item.Provider,
			&item.Name,
			&item.CaptureType,
			&item.ExecutionClass,
			&item.CapturesSuccess,
			&item.CapturesError,
			&item.LastCapturedAt,
			&item.RuntimeStatus,
			&item.RuntimeError,
			&item.LastFrameAt,
			&item.ConsecutiveErrors,
		); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan capture catalog candidate: %v", err))
			return
		}
		items = append(items, item)
	}
	if rows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate capture catalog candidates: %v", rows.Err()))
		return
	}

	util.WriteJSON(w, http.StatusOK, map[string]any{
		"items":           items,
		"limit":           limit,
		"offset":          offset,
		"total":           total,
		"execution_class": executionClass,
	})
}
