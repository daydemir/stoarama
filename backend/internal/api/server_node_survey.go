package api

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/survey"
	"github.com/daydemir/stoarama/backend/internal/util"
)

type nodeSurveyLeaseRequest struct {
	Limit int `json:"limit"`
}

type nodeSurveyCompleteRequest struct {
	StreamID  int64  `json:"stream_id"`
	Day       string `json:"day"`
	FrameB64  string `json:"frame_base64"`
	MIMEType  string `json:"mime_type"`
	Width     int    `json:"width"`
	Height    int    `json:"height"`
	SHA256    string `json:"sha256"`
	SizeBytes int64  `json:"size_bytes"`
	Detection *struct {
		PipelineVersion string  `json:"pipeline_version"`
		ConfThreshold   float64 `json:"conf_threshold"`
		Imgsz           int     `json:"imgsz"`
		DetectMs        int     `json:"detect_ms"`
		Person          int     `json:"person"`
		Bicycle         int     `json:"bicycle"`
		Car             int     `json:"car"`
		Motorcycle      int     `json:"motorcycle"`
		Bus             int     `json:"bus"`
		Truck           int     `json:"truck"`
	} `json:"detection,omitempty"`
}

type nodeSurveyFailRequest struct {
	StreamID int64  `json:"stream_id"`
	Error    string `json:"error"`
}

var errSurveyStreamUnsupported = errors.New("survey stream not found or unsupported")

func (s *Server) requireSurveyRelay(w http.ResponseWriter, r *http.Request) (nodePrincipal, bool) {
	principal, ok := nodePrincipalFromContext(r.Context())
	if !ok || principal.NodeType != nodeTypeRelay {
		util.WriteError(w, http.StatusUnauthorized, "relay node required")
		return nodePrincipal{}, false
	}
	requireHeartbeat := strings.HasSuffix(r.URL.Path, "/survey/lease")
	var enabled bool
	query := `
		SELECT survey_enabled AND COALESCE((capabilities_jsonb->>'survey_enabled')::boolean, false)
		FROM nodes
		WHERE id=$1 AND status='active'
	`
	if requireHeartbeat {
		query = `
			SELECT survey_enabled AND COALESCE((capabilities_jsonb->>'survey_enabled')::boolean, false)
			FROM nodes
			WHERE id=$1
			  AND status='active'
			  AND last_heartbeat_at >= now() - interval '120 seconds'
		`
	}
	if err := s.pool.QueryRow(r.Context(), query, principal.NodeID).Scan(&enabled); err != nil {
		if err == pgx.ErrNoRows {
			if !requireHeartbeat {
				util.WriteError(w, http.StatusUnauthorized, "relay node is not active")
				return nodePrincipal{}, false
			}
			util.WriteJSON(w, http.StatusOK, map[string]any{"targets": []survey.Target{}, "day": time.Now().UTC().Format("2006-01-02")})
			return nodePrincipal{}, false
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("check relay survey capability: %v", err))
		return nodePrincipal{}, false
	}
	if !enabled {
		if !requireHeartbeat {
			util.WriteError(w, http.StatusForbidden, "relay survey is not enabled")
			return nodePrincipal{}, false
		}
		util.WriteJSON(w, http.StatusOK, map[string]any{"targets": []survey.Target{}, "day": time.Now().UTC().Format("2006-01-02")})
		return nodePrincipal{}, false
	}
	return principal, true
}

func (s *Server) requireSurveyStream(ctx context.Context, streamID int64) error {
	var exists bool
	err := s.pool.QueryRow(ctx, `
		SELECT true
		FROM streams
		WHERE id=$1
		  AND deleted_at IS NULL
		  AND COALESCE(capture_type, '') IN ('youtube_watch', 'hls', 'http_video')
	`, streamID).Scan(&exists)
	if err == pgx.ErrNoRows {
		return errSurveyStreamUnsupported
	}
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) handleNodeSurveyLease(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireSurveyRelay(w, r); !ok {
		return
	}
	var req nodeSurveyLeaseRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 1
	}
	if limit > 20 {
		limit = 20
	}
	now := time.Now().UTC()
	rows, err := s.pool.Query(r.Context(), `
		SELECT s.id,
		       COALESCE(s.provider, ''),
		       COALESCE(s.source_url, ''),
		       COALESCE(s.source_page_url, ''),
		       COALESCE(s.capture_type, ''),
		       COALESCE(s.source_family, ''),
		       COALESCE(s.execution_class, '')
		FROM streams s
		LEFT JOIN survey_stream_state st ON st.stream_id = s.id
		WHERE s.deleted_at IS NULL
		  AND COALESCE(s.capture_type, '') IN ('youtube_watch', 'hls', 'http_video')
		  AND NOT EXISTS (
		      SELECT 1 FROM frames f
		      WHERE f.stream_id = s.id
		        AND f.source_kind = 'survey'
		        AND (f.captured_at AT TIME ZONE 'UTC')::date = ($1 AT TIME ZONE 'UTC')::date
		  )
		  AND NOT (
		      COALESCE(st.consecutive_failures, 0) >= 3
		      AND st.last_attempt_at IS NOT NULL
		      AND st.last_attempt_at + interval '6 hours' > $1
		  )
		ORDER BY
		  (COALESCE(s.capture_type, '') = 'youtube_watch') DESC,
		  st.last_attempt_at ASC NULLS FIRST,
		  s.id ASC
		LIMIT $2
	`, now, limit)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("lease survey targets: %v", err))
		return
	}
	defer rows.Close()
	targets := make([]survey.Target, 0, limit)
	for rows.Next() {
		var t survey.Target
		if err := rows.Scan(&t.ID, &t.Provider, &t.SourceURL, &t.SourcePageURL, &t.CaptureType, &t.SourceFamily, &t.ExecClass); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan survey target: %v", err))
			return
		}
		targets = append(targets, t)
	}
	if rows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate survey targets: %v", rows.Err()))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"targets": targets, "day": now.Format("2006-01-02")})
}

func (s *Server) handleNodeSurveyComplete(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireSurveyRelay(w, r); !ok {
		return
	}
	var req nodeSurveyCompleteRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.StreamID <= 0 {
		util.WriteError(w, http.StatusBadRequest, "stream_id is required")
		return
	}
	if err := s.requireSurveyStream(r.Context(), req.StreamID); err != nil {
		if errors.Is(err, errSurveyStreamUnsupported) {
			util.WriteError(w, http.StatusNotFound, errSurveyStreamUnsupported.Error())
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("check survey stream: %v", err))
		return
	}
	day, err := time.Parse("2006-01-02", strings.TrimSpace(req.Day))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, "day must be YYYY-MM-DD")
		return
	}
	exists, err := survey.HasFrameForDay(r.Context(), s.pool, req.StreamID, day)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("check existing survey frame: %v", err))
		return
	}
	if exists {
		util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true, "stream_id": req.StreamID, "skipped": true})
		return
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(req.FrameB64))
	if err != nil || len(raw) == 0 {
		util.WriteError(w, http.StatusBadRequest, "frame_base64 is invalid")
		return
	}
	frame := capture.Frame{
		Bytes:      raw,
		MIMEType:   strings.TrimSpace(req.MIMEType),
		Width:      req.Width,
		Height:     req.Height,
		SHA256:     strings.TrimSpace(req.SHA256),
		SizeBytes:  req.SizeBytes,
		SourceKind: "survey",
	}
	var det *survey.DetectionResult
	if req.Detection != nil {
		det = &survey.DetectionResult{
			PipelineVersion: strings.TrimSpace(req.Detection.PipelineVersion),
			ConfThreshold:   req.Detection.ConfThreshold,
			Imgsz:           req.Detection.Imgsz,
			DetectMs:        req.Detection.DetectMs,
			Counts: survey.DetectionCounts{
				Person:     req.Detection.Person,
				Bicycle:    req.Detection.Bicycle,
				Car:        req.Detection.Car,
				Motorcycle: req.Detection.Motorcycle,
				Bus:        req.Detection.Bus,
				Truck:      req.Detection.Truck,
			},
		}
	}
	if err := survey.Persist(r.Context(), s.pool, s.r2, req.StreamID, day, time.Now().UTC(), frame, det); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("persist survey result: %v", err))
		return
	}
	if err := survey.MarkHealthy(r.Context(), s.pool, req.StreamID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("mark survey healthy: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true, "stream_id": req.StreamID})
}

func (s *Server) handleNodeSurveyFail(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireSurveyRelay(w, r); !ok {
		return
	}
	var req nodeSurveyFailRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.StreamID <= 0 {
		util.WriteError(w, http.StatusBadRequest, "stream_id is required")
		return
	}
	if err := s.requireSurveyStream(r.Context(), req.StreamID); err != nil {
		if errors.Is(err, errSurveyStreamUnsupported) {
			util.WriteError(w, http.StatusNotFound, errSurveyStreamUnsupported.Error())
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("check survey stream: %v", err))
		return
	}
	if err := survey.RecordFailure(r.Context(), s.pool, req.StreamID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("record survey failure: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true, "stream_id": req.StreamID})
}
