package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/settings"
	"github.com/daydemir/stoarama/backend/internal/util"
)

type recordingSupervisionRow struct {
	StreamID             int64
	Name                 string
	Slug                 string
	Provider             string
	CaptureType          string
	StreamExecutionClass string
	RecordingState       string
	StreamUpdatedAt      time.Time
	ServerID             string
	AssignmentClass      string
	AssignmentRevision   *int64
	AssignedAt           *time.Time
	RuntimeClass         *string
	RuntimeStatus        string
	LastFrameAt          *time.Time
	LastErrorText        string
	ConsecutiveErrors    int
	RelayStatus          string
	RelayErrorText       string
	IncidentType         *string
	IncidentFirstSeenAt  *time.Time
	IncidentLastSeenAt   *time.Time
	IncidentLastNotifyAt *time.Time
	IncidentNotifyCount  *int
	IncidentDetailsRaw   []byte
}

func (s *Server) handleRecordingSupervisionStatus(w http.ResponseWriter, r *http.Request) {
	limit := parseIntQuery(r, "limit", 500, 1, 2000)
	offset := parseIntQuery(r, "offset", 0, 0, 1000000)
	streamID := int64(0)
	if streamIDPtr := parseInt64QueryPtr(r, "stream_id"); streamIDPtr != nil && *streamIDPtr > 0 {
		streamID = *streamIDPtr
	}
	recordingSettings, err := settings.GetRecordingSettings(r.Context(), s.pool)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}

	where := "s.recording_state='on'"
	args := []any{limit, offset}
	if streamID > 0 {
		where += " AND s.id=$3"
		args = append(args, streamID)
	}

	rows, err := s.pool.Query(r.Context(), fmt.Sprintf(`
		SELECT
			s.id,
			s.name,
			s.slug,
			s.provider,
			s.capture_type,
			s.execution_class,
			s.recording_state::text,
			s.updated_at,
			COALESCE(ra.server_id, ''),
			COALESCE(ra.execution_class, ''),
			ra.assignment_revision,
			ra.assigned_at,
			rt.execution_class,
			COALESCE(rt.status, ''),
			rt.last_frame_at,
			COALESCE(rt.last_error_text, ''),
			COALESCE(rt.consecutive_errors, 0),
			COALESCE(yr.status, ''),
			COALESCE(yr.error_text, ''),
			inc.incident_type,
			inc.first_observed_at,
			inc.last_observed_at,
			inc.last_notified_at,
			inc.notify_count,
			inc.details_jsonb
		FROM streams s
		LEFT JOIN recording_assignments ra ON ra.stream_id=s.id
		LEFT JOIN stream_capture_runtime rt ON rt.stream_id=s.id
		LEFT JOIN youtube_relay_routes yr ON yr.stream_id=s.id
		LEFT JOIN LATERAL (
			SELECT incident_type, first_observed_at, last_observed_at, last_notified_at, notify_count, details_jsonb
			FROM stream_recording_incidents
			WHERE stream_id=s.id
			  AND status='open'
			ORDER BY updated_at DESC, id DESC
			LIMIT 1
		) inc ON true
		WHERE %s
		ORDER BY s.id ASC
		LIMIT $1 OFFSET $2
	`, where), args...)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query recording supervision rows: %v", err))
		return
	}
	defer rows.Close()

	items := make([]recordingSupervisionRow, 0, limit)
	frameStreamIDs := make([]int64, 0, limit)
	clipStreamIDs := make([]int64, 0, limit)
	for rows.Next() {
		var row recordingSupervisionRow
		if err := rows.Scan(
			&row.StreamID,
			&row.Name,
			&row.Slug,
			&row.Provider,
			&row.CaptureType,
			&row.StreamExecutionClass,
			&row.RecordingState,
			&row.StreamUpdatedAt,
			&row.ServerID,
			&row.AssignmentClass,
			&row.AssignmentRevision,
			&row.AssignedAt,
			&row.RuntimeClass,
			&row.RuntimeStatus,
			&row.LastFrameAt,
			&row.LastErrorText,
			&row.ConsecutiveErrors,
			&row.RelayStatus,
			&row.RelayErrorText,
			&row.IncidentType,
			&row.IncidentFirstSeenAt,
			&row.IncidentLastSeenAt,
			&row.IncidentLastNotifyAt,
			&row.IncidentNotifyCount,
			&row.IncidentDetailsRaw,
		); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan recording supervision row: %v", err))
			return
		}
		items = append(items, row)
		modeClass := firstNonEmpty(row.AssignmentClass, derefString(row.RuntimeClass), row.StreamExecutionClass)
		if isClipNativeExecutionClass(modeClass) {
			clipStreamIDs = append(clipStreamIDs, row.StreamID)
		} else {
			frameStreamIDs = append(frameStreamIDs, row.StreamID)
		}
	}
	if rows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate recording supervision rows: %v", rows.Err()))
		return
	}

	success2h, err := s.successCaptureCountsSince(r.Context(), frameStreamIDs, clipStreamIDs, 2*time.Hour)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query recording supervision success counters: %v", err))
		return
	}
	processIssueCounts2h, err := s.recordingProcessIssueCountsSince(r.Context(), 2*time.Hour)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query recording supervision process issues: %v", err))
		return
	}

	now := time.Now().UTC()
	type outputItem map[string]any
	out := make([]outputItem, 0, len(items))
	var healthyTotal, down10mTotal, spotty2hTotal, incidentsOpenTotal int64
	for _, row := range items {
		modeClass := firstNonEmpty(row.AssignmentClass, derefString(row.RuntimeClass), row.StreamExecutionClass)
		expected2h := expectedCapturesPer60s(modeClass, recordingSettings.CaptureIntervalSec) * 120
		successCount2h := success2h[row.StreamID]
		lossRate2h := 0.0
		if expected2h > 0 {
			lossRate2h = 100.0 * float64(maxInt64(expected2h-successCount2h, 0)) / float64(expected2h)
		}
		processIssues2h := processIssueCounts2h[row.StreamID]
		state := "healthy"
		reason := "fresh_captures"
		unhealthySince := (*time.Time)(nil)
		downThreshold := 10 * time.Minute
		spottyEligible := row.AssignedAt != nil && now.Sub(row.AssignedAt.UTC()) >= 2*time.Hour
		if row.IncidentType != nil && strings.TrimSpace(*row.IncidentType) != "" {
			incidentsOpenTotal++
		}

		switch {
		case strings.TrimSpace(row.ServerID) == "":
			if now.Sub(row.StreamUpdatedAt.UTC()) >= downThreshold {
				state = "down_10m"
				reason = "recording_on_but_unassigned"
				unhealthySince = &row.StreamUpdatedAt
			} else {
				reason = "waiting_for_assignment"
			}
		case row.LastFrameAt == nil:
			if row.AssignedAt != nil && now.Sub(row.AssignedAt.UTC()) >= downThreshold {
				state = "down_10m"
				reason = "no_successful_frames"
				unhealthySince = row.AssignedAt
			} else {
				reason = "warmup"
			}
		default:
			lastFrameAt := row.LastFrameAt.UTC()
			age := now.Sub(lastFrameAt)
			if age < 0 {
				age = 0
			}
			if age >= downThreshold {
				state = "down_10m"
				reason = "stale_frames_10m"
				unhealthySince = row.LastFrameAt
			} else if strings.TrimSpace(row.RuntimeStatus) == "stopped" || strings.TrimSpace(row.RuntimeStatus) == "error" {
				if age >= downThreshold {
					state = "down_10m"
					reason = "capture_runtime_" + strings.TrimSpace(row.RuntimeStatus)
					unhealthySince = row.LastFrameAt
				}
			} else if spottyEligible && (lossRate2h > 20 || processIssues2h >= 3) {
				state = "spotty_2h"
				if processIssues2h >= 3 {
					reason = "process_restarts_2h"
				} else {
					reason = "loss_rate_2h"
				}
				assignedAt := row.AssignedAt.UTC()
				unhealthySince = &assignedAt
			}
		}

		switch state {
		case "healthy":
			healthyTotal++
		case "down_10m":
			down10mTotal++
		case "spotty_2h":
			spotty2hTotal++
		}

		incidentDetails := map[string]any{}
		if len(row.IncidentDetailsRaw) > 0 {
			_ = json.Unmarshal(row.IncidentDetailsRaw, &incidentDetails)
		}
		item := outputItem{
			"stream_id":                  row.StreamID,
			"name":                       row.Name,
			"slug":                       row.Slug,
			"provider":                   row.Provider,
			"capture_type":               row.CaptureType,
			"execution_class":            modeClass,
			"recording_state":            row.RecordingState,
			"server_id":                  row.ServerID,
			"assignment_execution_class": row.AssignmentClass,
			"assignment_revision":        row.AssignmentRevision,
			"assigned_at":                row.AssignedAt,
			"runtime_status":             row.RuntimeStatus,
			"runtime_execution_class":    row.RuntimeClass,
			"last_frame_at":              row.LastFrameAt,
			"last_error_text":            row.LastErrorText,
			"consecutive_errors":         row.ConsecutiveErrors,
			"relay_status":               row.RelayStatus,
			"relay_error_text":           row.RelayErrorText,
			"success_captures_2h":        successCount2h,
			"expected_captures_2h":       expected2h,
			"loss_rate_2h":               lossRate2h,
			"process_issues_2h":          processIssues2h,
			"supervision_state":          state,
			"supervision_reason":         reason,
			"unhealthy_since":            unhealthySince,
			"incident_open":              row.IncidentType != nil && strings.TrimSpace(*row.IncidentType) != "",
			"incident_type":              row.IncidentType,
			"incident_first_seen_at":     row.IncidentFirstSeenAt,
			"incident_last_seen_at":      row.IncidentLastSeenAt,
			"incident_last_notify_at":    row.IncidentLastNotifyAt,
			"incident_notify_count":      row.IncidentNotifyCount,
			"incident_details":           incidentDetails,
		}
		out = append(out, item)
	}

	util.WriteJSON(w, http.StatusOK, map[string]any{
		"items":           out,
		"limit":           limit,
		"offset":          offset,
		"total":           len(out),
		"healthy_total":   healthyTotal,
		"down_10m_total":  down10mTotal,
		"spotty_2h_total": spotty2hTotal,
		"incidents_open":  incidentsOpenTotal,
		"window_2h":       "2h",
		"down_threshold":  "10m",
	})
}

func (s *Server) recordingProcessIssueCountsSince(ctx context.Context, window time.Duration) (map[int64]int64, error) {
	out := map[int64]int64{}
	rows, err := s.pool.Query(ctx, `
		SELECT stream_id, COUNT(*)::bigint
		FROM recording_process_runs
		WHERE COALESCE(stopped_at, updated_at, started_at) >= now() - ($1 * interval '1 second')
		  AND status IN ('stopped', 'failed', 'crashed')
		GROUP BY stream_id
	`, int64(window/time.Second))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var streamID, count int64
		if err := rows.Scan(&streamID, &count); err != nil {
			return nil, err
		}
		out[streamID] = count
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return out, nil
}

func (s *Server) handleRecordingIncidentsList(w http.ResponseWriter, r *http.Request) {
	status := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("status")))
	limit := parseIntQuery(r, "limit", 200, 1, 1000)
	offset := parseIntQuery(r, "offset", 0, 0, 1000000)
	if status == "" {
		status = "open"
	}
	if status != "open" && status != "resolved" {
		util.WriteError(w, http.StatusBadRequest, "status must be open|resolved")
		return
	}
	rows, err := s.pool.Query(r.Context(), `
		SELECT
			i.id,
			i.stream_id,
			s.name,
			s.slug,
			i.incident_type,
			i.status,
			i.first_observed_at,
			i.last_observed_at,
			i.opened_at,
			i.resolved_at,
			i.last_notified_at,
			i.notify_count,
			i.details_jsonb,
			i.updated_at
		FROM stream_recording_incidents i
		JOIN streams s ON s.id=i.stream_id
		WHERE i.status=$1
		ORDER BY i.last_observed_at DESC, i.id DESC
		LIMIT $2 OFFSET $3
	`, status, limit, offset)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query recording incidents: %v", err))
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0, limit)
	for rows.Next() {
		var (
			id, streamID                                 int64
			name, slug                                   string
			incidentType                                 string
			rowStatus                                    string
			firstSeenAt, lastSeenAt, openedAt, updatedAt time.Time
			resolvedAt, lastNotifiedAt                   *time.Time
			notifyCount                                  int
			detailsRaw                                   []byte
		)
		if err := rows.Scan(
			&id, &streamID, &name, &slug, &incidentType, &rowStatus,
			&firstSeenAt, &lastSeenAt, &openedAt, &resolvedAt,
			&lastNotifiedAt, &notifyCount, &detailsRaw, &updatedAt,
		); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan recording incident: %v", err))
			return
		}
		details := map[string]any{}
		if len(detailsRaw) > 0 {
			_ = json.Unmarshal(detailsRaw, &details)
		}
		items = append(items, map[string]any{
			"id":                id,
			"stream_id":         streamID,
			"stream_name":       name,
			"stream_slug":       slug,
			"incident_type":     incidentType,
			"status":            rowStatus,
			"first_observed_at": firstSeenAt,
			"last_observed_at":  lastSeenAt,
			"opened_at":         openedAt,
			"resolved_at":       resolvedAt,
			"last_notified_at":  lastNotifiedAt,
			"notify_count":      notifyCount,
			"details":           details,
			"updated_at":        updatedAt,
		})
	}
	if rows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate recording incidents: %v", rows.Err()))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"items":  items,
		"limit":  limit,
		"offset": offset,
		"total":  len(items),
		"status": status,
	})
}
