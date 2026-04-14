package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/model"
	"github.com/daydemir/stoarama/backend/internal/storage"
	"github.com/daydemir/stoarama/backend/internal/util"
)

type serviceStreamImportRequest struct {
	Provider            string         `json:"provider"`
	ExternalID          string         `json:"external_id"`
	Name                string         `json:"name"`
	Slug                string         `json:"slug"`
	SourceURL           string         `json:"source_url"`
	SourcePageURL       string         `json:"source_page_url"`
	SourceFamily        string         `json:"source_family"`
	Lat                 *float64       `json:"lat"`
	Lon                 *float64       `json:"lon"`
	CaptureType         string         `json:"capture_type"`
	ExecutionClass      string         `json:"execution_class"`
	ExecutionConfigJSON map[string]any `json:"execution_config_json"`
	Tags                []string       `json:"tags"`
	LocationText        string         `json:"location_text"`
	LocationCountry     string         `json:"location_country"`
	LocationCountryCode string         `json:"location_country_code"`
	LocationRegion      string         `json:"location_region"`
	LocationCity        string         `json:"location_city"`
	LocationLocality    string         `json:"location_locality"`
	LocationSource      string         `json:"location_source"`
	MetadataJSON        map[string]any `json:"metadata_json"`
}

type serviceFrameImportRequest struct {
	StreamID    int64  `json:"stream_id"`
	FrameURL    string `json:"frame_url"`
	CapturedAt  string `json:"captured_at"`
	SourceKind  string `json:"source_kind"`
	SourceLabel string `json:"source_label"`
}

type serviceStreamImageCaptureRepairRequest struct {
	StreamID        int64  `json:"stream_id"`
	SourceURLLike   string `json:"source_url_like"`
	Provider        string `json:"provider"`
	Limit           int    `json:"limit"`
	Apply           bool   `json:"apply"`
	OnlyChanged     bool   `json:"only_changed"`
	RecordingActor  string `json:"recording_actor"`
	RecordingReason string `json:"recording_reason"`
}

type serviceStreamImageCaptureRepairItem struct {
	ID                     int64      `json:"id"`
	Provider               string     `json:"provider"`
	Name                   string     `json:"name"`
	Slug                   string     `json:"slug"`
	SourceURL              string     `json:"source_url"`
	SourcePageURL          string     `json:"source_page_url"`
	RecordingState         string     `json:"recording_state"`
	CurrentSourceFamily    string     `json:"current_source_family"`
	CurrentCaptureType     string     `json:"current_capture_type"`
	CurrentExecutionClass  string     `json:"current_execution_class"`
	ProposedSourceFamily   string     `json:"proposed_source_family"`
	ProposedCaptureType    string     `json:"proposed_capture_type"`
	ProposedExecutionClass string     `json:"proposed_execution_class"`
	WouldChange            bool       `json:"would_change"`
	Applied                bool       `json:"applied,omitempty"`
	PreviousServerID       *string    `json:"previous_server_id,omitempty"`
	NewServerID            *string    `json:"new_server_id,omitempty"`
	LastFrameAt            *time.Time `json:"last_frame_at,omitempty"`
}

type serviceStreamCanonicalCaptureRepairRequest struct {
	StreamID           int64  `json:"stream_id"`
	SourceURLLike      string `json:"source_url_like"`
	Provider           string `json:"provider"`
	Limit              int    `json:"limit"`
	Apply              bool   `json:"apply"`
	OnlyChanged        bool   `json:"only_changed"`
	OnlyReview         bool   `json:"only_review"`
	LegacyImportedOnly bool   `json:"legacy_imported_only"`
	NonYouTubeOnly     bool   `json:"non_youtube_only"`
	RecordingActor     string `json:"recording_actor"`
	RecordingReason    string `json:"recording_reason"`
}

type serviceStreamCanonicalCaptureRepairItem struct {
	ID                     int64      `json:"id"`
	Provider               string     `json:"provider"`
	Name                   string     `json:"name"`
	Slug                   string     `json:"slug"`
	SourceURL              string     `json:"source_url"`
	SourcePageURL          string     `json:"source_page_url"`
	RecordingState         string     `json:"recording_state"`
	CurrentSourceFamily    string     `json:"current_source_family"`
	CurrentCaptureType     string     `json:"current_capture_type"`
	CurrentExecutionClass  string     `json:"current_execution_class"`
	ResolvedCaptureType    string     `json:"resolved_capture_type,omitempty"`
	ProposedSourceFamily   string     `json:"proposed_source_family"`
	ProposedCaptureType    string     `json:"proposed_capture_type"`
	ProposedExecutionClass string     `json:"proposed_execution_class"`
	WouldChange            bool       `json:"would_change"`
	ReviewRequired         bool       `json:"review_required"`
	Reasons                []string   `json:"reasons,omitempty"`
	Applied                bool       `json:"applied,omitempty"`
	PreviousServerID       *string    `json:"previous_server_id,omitempty"`
	NewServerID            *string    `json:"new_server_id,omitempty"`
	LastFrameAt            *time.Time `json:"last_frame_at,omitempty"`
}

type serviceStreamRecordingStateRequest struct {
	StreamID        int64  `json:"stream_id"`
	RecordingState  string `json:"recording_state"`
	RecordingActor  string `json:"recording_actor"`
	RecordingReason string `json:"recording_reason"`
}

func (s *Server) handleServiceStreamImport(w http.ResponseWriter, r *http.Request) {
	var req serviceStreamImportRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid json: %v", err))
		return
	}
	stream, created, err := s.upsertImportedStream(r, req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"created": created,
		"stream":  stream,
	})
}

func (s *Server) handleServiceFrameImport(w http.ResponseWriter, r *http.Request) {
	var req serviceFrameImportRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid json: %v", err))
		return
	}
	if req.StreamID <= 0 || strings.TrimSpace(req.FrameURL) == "" {
		util.WriteError(w, http.StatusBadRequest, "stream_id and frame_url are required")
		return
	}
	capturedAt := time.Now().UTC()
	if raw := strings.TrimSpace(req.CapturedAt); raw != "" {
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid captured_at: %v", err))
			return
		}
		capturedAt = parsed.UTC()
	}
	sourceKind := strings.TrimSpace(req.SourceKind)
	if sourceKind == "" {
		sourceKind = "snapshot_url"
	}
	frame, err := capture.CaptureFrame(r.Context(), strings.TrimSpace(req.FrameURL))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("capture frame: %v", err))
		return
	}
	objectKey := fmt.Sprintf("raw/stream/%d/%04d/%02d/%02d/import-%s-%d%s",
		req.StreamID, capturedAt.Year(), int(capturedAt.Month()), capturedAt.Day(),
		sanitizePathToken(firstNonEmptyImport(req.SourceLabel, "legacy-latest-frame")),
		capturedAt.UnixNano(), fileExtensionFromMIME(frame.MIMEType))

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin import frame tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	etag, err := s.r2.PutBytes(r.Context(), objectKey, frame.MIMEType, frame.Bytes)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("upload imported frame: %v", err))
		return
	}
	mediaID, err := storage.UpsertMediaObject(r.Context(), tx, storage.MediaObjectInput{
		StorageProvider: "r2",
		Bucket:          s.r2.Bucket(),
		ObjectKey:       objectKey,
		MIMEType:        frame.MIMEType,
		SizeBytes:       frame.SizeBytes,
		ETag:            etag,
		SHA256:          frame.SHA256,
		Width:           frame.Width,
		Height:          frame.Height,
	})
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("upsert media object: %v", err))
		return
	}
	ct, err := tx.Exec(r.Context(), `
		INSERT INTO frames (stream_id, capture_job_id, captured_at, raw_media_object_id, capture_status, capture_error, source_kind)
		VALUES ($1, NULL, $2, $3, 'success', NULL, $4)
		ON CONFLICT (stream_id, captured_at, raw_media_object_id) DO NOTHING
	`, req.StreamID, capturedAt, mediaID, sourceKind)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("insert imported frame: %v", err))
		return
	}
	inserted := ct.RowsAffected() == 1
	if inserted {
		if _, err := tx.Exec(r.Context(), `
			INSERT INTO stream_health (stream_id, captures_total, captures_success, captures_error, last_capture_at)
			VALUES ($1, 1, 1, 0, $2)
			ON CONFLICT (stream_id)
			DO UPDATE SET
				captures_total=stream_health.captures_total+1,
				captures_success=stream_health.captures_success+1,
				last_capture_at=GREATEST(stream_health.last_capture_at, EXCLUDED.last_capture_at),
				updated_at=now()
		`, req.StreamID, capturedAt); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("update stream_health: %v", err))
			return
		}
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit imported frame tx: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"ok":         true,
		"inserted":   inserted,
		"media_id":   mediaID,
		"object_key": objectKey,
	})
}

func (s *Server) handleServiceStreamImageCaptureRepair(w http.ResponseWriter, r *http.Request) {
	var req serviceStreamImageCaptureRepairRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid json: %v", err))
		return
	}
	if req.StreamID <= 0 && strings.TrimSpace(req.SourceURLLike) == "" && strings.TrimSpace(req.Provider) == "" {
		util.WriteError(w, http.StatusBadRequest, "stream_id, source_url_like, or provider is required")
		return
	}
	if req.Limit < 0 {
		util.WriteError(w, http.StatusBadRequest, "limit must be >= 0")
		return
	}
	items, err := s.loadStreamImageCaptureRepairItems(r.Context(), req)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load image capture repair items: %v", err))
		return
	}
	selected := items
	if req.OnlyChanged {
		selected = make([]serviceStreamImageCaptureRepairItem, 0, len(items))
		for _, item := range items {
			if item.WouldChange {
				selected = append(selected, item)
			}
		}
	}
	applied := 0
	if req.Apply {
		actor := strings.TrimSpace(req.RecordingActor)
		if actor == "" {
			actor = "service.stream_image_capture_repair"
		}
		reason := strings.TrimSpace(req.RecordingReason)
		if reason == "" {
			reason = "repair image capture classification"
		}
		for i := range selected {
			if !selected[i].WouldChange {
				continue
			}
			appliedItem, err := s.applyStreamImageCaptureRepair(r.Context(), selected[i], actor, reason)
			if err != nil {
				util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("apply image capture repair stream %d: %v", selected[i].ID, err))
				return
			}
			selected[i] = appliedItem
			applied++
		}
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"ok":       true,
		"total":    len(items),
		"selected": len(selected),
		"changed":  countStreamImageRepairItems(items, func(it serviceStreamImageCaptureRepairItem) bool { return it.WouldChange }),
		"applied":  applied,
		"items":    selected,
	})
}

func (s *Server) handleServiceStreamCanonicalCaptureRepair(w http.ResponseWriter, r *http.Request) {
	var req serviceStreamCanonicalCaptureRepairRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid json: %v", err))
		return
	}
	if req.StreamID <= 0 && strings.TrimSpace(req.SourceURLLike) == "" && strings.TrimSpace(req.Provider) == "" && !req.LegacyImportedOnly {
		util.WriteError(w, http.StatusBadRequest, "stream_id, source_url_like, provider, or legacy_imported_only is required")
		return
	}
	if req.Limit < 0 {
		util.WriteError(w, http.StatusBadRequest, "limit must be >= 0")
		return
	}
	items, err := s.loadStreamCanonicalCaptureRepairItems(r.Context(), req)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load canonical capture repair items: %v", err))
		return
	}
	selected := items
	if req.OnlyChanged || req.OnlyReview {
		selected = make([]serviceStreamCanonicalCaptureRepairItem, 0, len(items))
		for _, item := range items {
			if req.OnlyChanged && !item.WouldChange {
				continue
			}
			if req.OnlyReview && !item.ReviewRequired {
				continue
			}
			selected = append(selected, item)
		}
	}
	applied := 0
	if req.Apply {
		actor := strings.TrimSpace(req.RecordingActor)
		if actor == "" {
			actor = "service.stream_canonical_capture_repair"
		}
		reason := strings.TrimSpace(req.RecordingReason)
		if reason == "" {
			reason = "repair canonical capture classification"
		}
		for i := range selected {
			if !selected[i].WouldChange || selected[i].ReviewRequired {
				continue
			}
			appliedItem, err := s.applyStreamCanonicalCaptureRepair(r.Context(), selected[i], actor, reason)
			if err != nil {
				util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("apply canonical capture repair stream %d: %v", selected[i].ID, err))
				return
			}
			selected[i] = appliedItem
			applied++
		}
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"ok":               true,
		"total":            len(items),
		"selected":         len(selected),
		"changed":          countStreamCanonicalRepairItems(items, func(it serviceStreamCanonicalCaptureRepairItem) bool { return it.WouldChange }),
		"review_required":  countStreamCanonicalRepairItems(items, func(it serviceStreamCanonicalCaptureRepairItem) bool { return it.ReviewRequired }),
		"safe_to_apply":    countStreamCanonicalRepairItems(items, func(it serviceStreamCanonicalCaptureRepairItem) bool { return it.WouldChange && !it.ReviewRequired }),
		"applied":          applied,
		"proposed_types":   summarizeStreamCanonicalRepairItems(items, func(it serviceStreamCanonicalCaptureRepairItem) string { return it.ProposedCaptureType }),
		"proposed_classes": summarizeStreamCanonicalRepairItems(items, func(it serviceStreamCanonicalCaptureRepairItem) string { return it.ProposedExecutionClass }),
		"providers":        summarizeStreamCanonicalRepairItems(items, func(it serviceStreamCanonicalCaptureRepairItem) string { return it.Provider }),
		"items":            selected,
	})
}

func (s *Server) handleServiceStreamRecordingState(w http.ResponseWriter, r *http.Request) {
	var req serviceStreamRecordingStateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid json: %v", err))
		return
	}
	if req.StreamID <= 0 {
		util.WriteError(w, http.StatusBadRequest, "stream_id is required")
		return
	}
	state, ok := parseRecordingState(strings.TrimSpace(req.RecordingState))
	if !ok {
		util.WriteError(w, http.StatusBadRequest, "invalid recording_state; expected off|on")
		return
	}
	actor := strings.TrimSpace(req.RecordingActor)
	if actor == "" {
		actor = "service.stream_recording_state"
	}
	reason := strings.TrimSpace(req.RecordingReason)
	if reason == "" {
		reason = "service recording state update"
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	result, status, err := s.setStreamRecordingStateTx(r.Context(), tx, req.StreamID, state, "", actor, reason)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if status > 0 {
		util.WriteJSON(w, status, result)
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit stream recording_state: %v", err))
		return
	}
	updated, err := s.getStreamByID(r.Context(), req.StreamID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("reload stream: %v", err))
		return
	}
	result["stream"] = updated
	util.WriteJSON(w, http.StatusOK, result)
}

type serviceStreamTagsAddRequest struct {
	Tags []string `json:"tags"`
}

func (s *Server) handleServiceStreamTagsAdd(w http.ResponseWriter, r *http.Request) {
	streamID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req serviceStreamTagsAddRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	tagsToAdd := dedupeStrings(req.Tags)
	if len(tagsToAdd) == 0 {
		util.WriteError(w, http.StatusBadRequest, "tags must contain at least one tag")
		return
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin tag update tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	current, err := s.loadStreamForAssignmentTx(r.Context(), tx, streamID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusNotFound, "stream not found")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load stream: %v", err))
		return
	}
	updatedTags := dedupeStrings(append(current.Tags, tagsToAdd...))
	if _, err := tx.Exec(r.Context(), `
		UPDATE streams
		SET tags=$2, updated_at=now()
		WHERE id=$1
	`, streamID, updatedTags); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("update stream tags: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit tag update tx: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"ok":        true,
		"stream_id": streamID,
		"tags":      updatedTags,
	})
}

func (s *Server) loadStreamImageCaptureRepairItems(ctx context.Context, req serviceStreamImageCaptureRepairRequest) ([]serviceStreamImageCaptureRepairItem, error) {
	where := []string{"source_url <> ''"}
	args := make([]any, 0, 4)
	if req.StreamID > 0 {
		args = append(args, req.StreamID)
		where = append(where, fmt.Sprintf("id=$%d", len(args)))
	}
	if raw := strings.TrimSpace(req.SourceURLLike); raw != "" {
		args = append(args, raw)
		where = append(where, fmt.Sprintf("source_url ILIKE $%d", len(args)))
	}
	if raw := strings.TrimSpace(req.Provider); raw != "" {
		args = append(args, raw)
		where = append(where, fmt.Sprintf("LOWER(provider)=LOWER($%d)", len(args)))
	}
	query := fmt.Sprintf(`
		SELECT id, provider, name, slug, source_url, source_page_url, recording_state, source_family, capture_type, execution_class
		FROM streams
		WHERE %s
		ORDER BY id ASC
	`, strings.Join(where, " AND "))
	if req.Limit > 0 {
		args = append(args, req.Limit)
		query += fmt.Sprintf(" LIMIT $%d", len(args))
	}
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]serviceStreamImageCaptureRepairItem, 0, 128)
	for rows.Next() {
		var item serviceStreamImageCaptureRepairItem
		if err := rows.Scan(
			&item.ID,
			&item.Provider,
			&item.Name,
			&item.Slug,
			&item.SourceURL,
			&item.SourcePageURL,
			&item.RecordingState,
			&item.CurrentSourceFamily,
			&item.CurrentCaptureType,
			&item.CurrentExecutionClass,
		); err != nil {
			return nil, err
		}
		proposed, err := capture.DeriveCanonicalStreamFields(item.SourceURL, item.SourcePageURL, "", "", "")
		if err != nil || proposed.CaptureType != capture.CaptureTypeStillImage {
			continue
		}
		item.CurrentSourceFamily = strings.TrimSpace(item.CurrentSourceFamily)
		item.CurrentCaptureType = strings.TrimSpace(item.CurrentCaptureType)
		item.CurrentExecutionClass = strings.TrimSpace(item.CurrentExecutionClass)
		item.ProposedSourceFamily = proposed.SourceFamily
		item.ProposedCaptureType = proposed.CaptureType
		item.ProposedExecutionClass = proposed.ExecutionClass
		item.WouldChange = item.CurrentSourceFamily != item.ProposedSourceFamily ||
			item.CurrentCaptureType != item.ProposedCaptureType ||
			item.CurrentExecutionClass != item.ProposedExecutionClass
		items = append(items, item)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return items, nil
}

func (s *Server) applyStreamImageCaptureRepair(ctx context.Context, item serviceStreamImageCaptureRepairItem, actor string, reason string) (serviceStreamImageCaptureRepairItem, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return item, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	current, err := s.loadStreamForAssignmentTx(ctx, tx, item.ID)
	if err != nil {
		return item, err
	}
	profile, err := capture.DeriveCaptureProfile(current.Provider, current.SourceURL, current.SourcePageURL, item.ProposedCaptureType, item.ProposedSourceFamily, item.ProposedExecutionClass, current.ExecutionConfigJSON, nil, nil)
	if err != nil {
		return item, err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE streams
		SET source_family=$2, capture_type=$3, execution_class=$4, capture_family=$5, expected_fps=$6, expected_image_interval_sec=$7, updated_at=now()
		WHERE id=$1
	`, item.ID, profile.SourceFamily, profile.CaptureType, profile.ExecutionClass, profile.CaptureFamily, profile.ExpectedFPS, profile.ExpectedImageIntervalSec); err != nil {
		return item, err
	}
	updated, err := s.loadStreamForAssignmentTx(ctx, tx, item.ID)
	if err != nil {
		return item, err
	}
	assignment, existed, err := loadRecordingAssignmentTx(ctx, tx, item.ID)
	if err != nil {
		return item, err
	}
	if existed {
		item.PreviousServerID = &assignment.ServerID
		issues := buildRecordingAssignmentAuditIssues(updated, assignment, nil)
		if len(issues) > 0 {
			if _, _, err := s.unassignRecordingStreamTx(ctx, tx, item.ID, actor, reason); err != nil {
				return item, err
			}
			existed = false
		}
	}
	if updated.RecordingState == model.RecordingStateOn && !existed {
		result, status, err := s.assignRecordingStreamTx(ctx, tx, updated, "", "", actor, reason)
		if err != nil {
			return item, err
		}
		if status != 0 {
			return item, fmt.Errorf("assign repaired stream: %v", result["error"])
		}
		if serverID := strings.TrimSpace(fmt.Sprint(result["server_id"])); serverID != "" {
			item.NewServerID = &serverID
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return item, err
	}
	stream, err := s.getStreamByID(ctx, item.ID)
	if err == nil {
		item.CurrentSourceFamily = stream.SourceFamily
		item.CurrentCaptureType = stream.CaptureType
		item.CurrentExecutionClass = stream.ExecutionClass
	}
	var serverID string
	if err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(server_id, '')
		FROM recording_assignments
		WHERE stream_id=$1
	`, item.ID).Scan(&serverID); err == nil && strings.TrimSpace(serverID) != "" {
		serverID = strings.TrimSpace(serverID)
		item.NewServerID = &serverID
	}
	var lastFrame pgtype.Timestamptz
	if err := s.pool.QueryRow(ctx, `
		SELECT last_frame_at
		FROM stream_capture_runtime
		WHERE stream_id=$1
	`, item.ID).Scan(&lastFrame); err != nil && err != pgx.ErrNoRows {
		return item, err
	}
	if lastFrame.Valid {
		ts := lastFrame.Time.UTC()
		item.LastFrameAt = &ts
	}
	item.Applied = true
	return item, nil
}

func countStreamImageRepairItems(items []serviceStreamImageCaptureRepairItem, keep func(serviceStreamImageCaptureRepairItem) bool) int {
	count := 0
	for _, item := range items {
		if keep(item) {
			count++
		}
	}
	return count
}

func (s *Server) loadStreamCanonicalCaptureRepairItems(ctx context.Context, req serviceStreamCanonicalCaptureRepairRequest) ([]serviceStreamCanonicalCaptureRepairItem, error) {
	where := []string{"s.source_url <> ''"}
	args := make([]any, 0, 5)
	if req.LegacyImportedOnly {
		where = append(where, "s.tags @> ARRAY['imported:legacy-social-isolation']::text[]")
	}
	if req.StreamID > 0 {
		args = append(args, req.StreamID)
		where = append(where, fmt.Sprintf("s.id=$%d", len(args)))
	}
	if raw := strings.TrimSpace(req.SourceURLLike); raw != "" {
		args = append(args, raw)
		where = append(where, fmt.Sprintf("s.source_url ILIKE $%d", len(args)))
	}
	if raw := strings.TrimSpace(req.Provider); raw != "" {
		args = append(args, raw)
		where = append(where, fmt.Sprintf("LOWER(s.provider)=LOWER($%d)", len(args)))
	}
	query := fmt.Sprintf(`
		SELECT
			s.id,
			s.provider,
			s.name,
			s.slug,
			s.source_url,
			s.source_page_url,
			s.recording_state::text,
			s.source_family,
			s.capture_type,
			s.execution_class,
			COALESCE(rt.resolved_capture_type, ''),
			rt.last_frame_at
		FROM streams s
		LEFT JOIN stream_capture_runtime rt ON rt.stream_id=s.id
		WHERE %s
		ORDER BY s.id ASC
	`, strings.Join(where, " AND "))
	if req.Limit > 0 {
		args = append(args, req.Limit)
		query += fmt.Sprintf(" LIMIT $%d", len(args))
	}
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]serviceStreamCanonicalCaptureRepairItem, 0, 256)
	for rows.Next() {
		var item serviceStreamCanonicalCaptureRepairItem
		var lastFrame pgtype.Timestamptz
		if err := rows.Scan(
			&item.ID,
			&item.Provider,
			&item.Name,
			&item.Slug,
			&item.SourceURL,
			&item.SourcePageURL,
			&item.RecordingState,
			&item.CurrentSourceFamily,
			&item.CurrentCaptureType,
			&item.CurrentExecutionClass,
			&item.ResolvedCaptureType,
			&lastFrame,
		); err != nil {
			return nil, err
		}
		item.CurrentSourceFamily = strings.TrimSpace(item.CurrentSourceFamily)
		item.CurrentCaptureType = strings.TrimSpace(item.CurrentCaptureType)
		item.CurrentExecutionClass = strings.TrimSpace(item.CurrentExecutionClass)
		item.ResolvedCaptureType = strings.TrimSpace(item.ResolvedCaptureType)
		proposal := capture.ProposeCanonicalStreamRepair(capture.CanonicalRepairInput{
			Provider:              item.Provider,
			SourceURL:             item.SourceURL,
			SourcePageURL:         item.SourcePageURL,
			CurrentSourceFamily:   item.CurrentSourceFamily,
			CurrentCaptureType:    item.CurrentCaptureType,
			CurrentExecutionClass: item.CurrentExecutionClass,
			ResolvedCaptureType:   item.ResolvedCaptureType,
		})
		if req.NonYouTubeOnly && (item.CurrentCaptureType == capture.CaptureTypeYouTubeWatch || proposal.ProposedCaptureType == capture.CaptureTypeYouTubeWatch) {
			continue
		}
		item.ProposedSourceFamily = proposal.ProposedSourceFamily
		item.ProposedCaptureType = proposal.ProposedCaptureType
		item.ProposedExecutionClass = proposal.ProposedExecutionClass
		item.WouldChange = proposal.WouldChange
		item.ReviewRequired = proposal.ReviewRequired
		item.Reasons = proposal.Reasons
		if lastFrame.Valid {
			ts := lastFrame.Time.UTC()
			item.LastFrameAt = &ts
		}
		items = append(items, item)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return items, nil
}

func (s *Server) applyStreamCanonicalCaptureRepair(ctx context.Context, item serviceStreamCanonicalCaptureRepairItem, actor string, reason string) (serviceStreamCanonicalCaptureRepairItem, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return item, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	current, err := s.loadStreamForAssignmentTx(ctx, tx, item.ID)
	if err != nil {
		return item, err
	}
	profile, err := capture.DeriveCaptureProfile(current.Provider, current.SourceURL, current.SourcePageURL, item.ProposedCaptureType, item.ProposedSourceFamily, item.ProposedExecutionClass, current.ExecutionConfigJSON, nil, nil)
	if err != nil {
		return item, err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE streams
		SET source_family=$2, capture_type=$3, execution_class=$4, capture_family=$5, expected_fps=$6, expected_image_interval_sec=$7, updated_at=now()
		WHERE id=$1
	`, item.ID, profile.SourceFamily, profile.CaptureType, profile.ExecutionClass, profile.CaptureFamily, profile.ExpectedFPS, profile.ExpectedImageIntervalSec); err != nil {
		return item, err
	}
	updated, err := s.loadStreamForAssignmentTx(ctx, tx, item.ID)
	if err != nil {
		return item, err
	}
	assignment, existed, err := loadRecordingAssignmentTx(ctx, tx, item.ID)
	if err != nil {
		return item, err
	}
	if existed {
		item.PreviousServerID = &assignment.ServerID
		issues := buildRecordingAssignmentAuditIssues(updated, assignment, nil)
		if len(issues) > 0 {
			if _, _, err := s.unassignRecordingStreamTx(ctx, tx, item.ID, actor, reason); err != nil {
				return item, err
			}
			existed = false
		}
	}
	if updated.RecordingState == model.RecordingStateOn && !existed {
		result, status, err := s.assignRecordingStreamTx(ctx, tx, updated, "", "", actor, reason)
		if err != nil {
			return item, err
		}
		if status != 0 {
			return item, fmt.Errorf("assign repaired stream: %v", result["error"])
		}
		if serverID := strings.TrimSpace(fmt.Sprint(result["server_id"])); serverID != "" {
			item.NewServerID = &serverID
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return item, err
	}
	stream, err := s.getStreamByID(ctx, item.ID)
	if err == nil {
		item.CurrentSourceFamily = stream.SourceFamily
		item.CurrentCaptureType = stream.CaptureType
		item.CurrentExecutionClass = stream.ExecutionClass
	}
	var serverID string
	if err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(server_id, '')
		FROM recording_assignments
		WHERE stream_id=$1
	`, item.ID).Scan(&serverID); err == nil && strings.TrimSpace(serverID) != "" {
		serverID = strings.TrimSpace(serverID)
		item.NewServerID = &serverID
	}
	var lastFrame pgtype.Timestamptz
	if err := s.pool.QueryRow(ctx, `
		SELECT last_frame_at
		FROM stream_capture_runtime
		WHERE stream_id=$1
	`, item.ID).Scan(&lastFrame); err != nil && err != pgx.ErrNoRows {
		return item, err
	}
	if lastFrame.Valid {
		ts := lastFrame.Time.UTC()
		item.LastFrameAt = &ts
	}
	item.Applied = true
	return item, nil
}

func countStreamCanonicalRepairItems(items []serviceStreamCanonicalCaptureRepairItem, keep func(serviceStreamCanonicalCaptureRepairItem) bool) int {
	count := 0
	for _, item := range items {
		if keep(item) {
			count++
		}
	}
	return count
}

func summarizeStreamCanonicalRepairItems(items []serviceStreamCanonicalCaptureRepairItem, field func(serviceStreamCanonicalCaptureRepairItem) string) map[string]int {
	out := map[string]int{}
	for _, item := range items {
		key := strings.TrimSpace(field(item))
		if key == "" {
			key = "<empty>"
		}
		out[key]++
	}
	return out
}

func (s *Server) upsertImportedStream(r *http.Request, req serviceStreamImportRequest) (model.Stream, bool, error) {
	provider := strings.TrimSpace(req.Provider)
	externalID := strings.TrimSpace(req.ExternalID)
	name := strings.TrimSpace(req.Name)
	if provider == "" || externalID == "" || name == "" {
		return model.Stream{}, false, newAPIStatusError(http.StatusBadRequest, "provider, external_id, and name are required")
	}

	profile, err := capture.DeriveCaptureProfile(provider, req.SourceURL, req.SourcePageURL, req.CaptureType, req.SourceFamily, req.ExecutionClass, nonNilMap(req.ExecutionConfigJSON), nil, nil)
	if err != nil {
		return model.Stream{}, false, newAPIStatusError(http.StatusBadRequest, "%s", err.Error())
	}
	metaBytes, err := json.Marshal(nonNilMap(req.MetadataJSON))
	if err != nil {
		return model.Stream{}, false, newAPIStatusError(http.StatusBadRequest, "invalid metadata_json: %v", err)
	}
	cfgBytes, err := json.Marshal(nonNilMap(req.ExecutionConfigJSON))
	if err != nil {
		return model.Stream{}, false, newAPIStatusError(http.StatusBadRequest, "invalid execution_config_json: %v", err)
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		return model.Stream{}, false, fmt.Errorf("begin import tx: %w", err)
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	var existingID int64
	err = tx.QueryRow(r.Context(), `
		SELECT id
		FROM streams
		WHERE provider=$1 AND external_id=$2
	`, provider, externalID).Scan(&existingID)
	if err == nil {
		current, err := s.loadStreamForAssignmentTx(r.Context(), tx, existingID)
		if err != nil {
			return model.Stream{}, false, fmt.Errorf("load existing imported stream: %w", err)
		}
		if err := ensureImportedSourceURLProviderConflictFree(r.Context(), tx, profile.SourceURL, provider, existingID); err != nil {
			return model.Stream{}, false, err
		}
		if _, err := tx.Exec(r.Context(), `
			UPDATE streams
			SET
				name=$2,
				source_url=$3,
				source_page_url=$4,
				lat=$5,
				lon=$6,
				location_text=$7,
				location_country=$8,
				location_country_code=$9,
				location_region=$10,
				location_city=$11,
				location_locality=$12,
				location_source=$13,
				metadata_jsonb=$14,
				source_family=$15,
				capture_type=$16,
				execution_class=$17,
				capture_family=$18,
				expected_fps=$19,
				expected_image_interval_sec=$20,
				execution_config_jsonb=$21,
				tags=$22,
				updated_at=now()
			WHERE id=$1
		`, existingID, name, profile.SourceURL, profile.SourcePageURL,
			req.Lat, req.Lon, strings.TrimSpace(req.LocationText), strings.TrimSpace(req.LocationCountry), strings.ToUpper(strings.TrimSpace(req.LocationCountryCode)),
			strings.TrimSpace(req.LocationRegion), strings.TrimSpace(req.LocationCity), strings.TrimSpace(req.LocationLocality), strings.TrimSpace(req.LocationSource),
			metaBytes, profile.SourceFamily, profile.CaptureType, profile.ExecutionClass, profile.CaptureFamily, profile.ExpectedFPS, profile.ExpectedImageIntervalSec, cfgBytes, dedupeStrings(req.Tags),
		); err != nil {
			return model.Stream{}, false, fmt.Errorf("update imported stream: %w", err)
		}
		updated, err := s.loadStreamForAssignmentTx(r.Context(), tx, existingID)
		if err != nil {
			return model.Stream{}, false, fmt.Errorf("reload imported stream: %w", err)
		}
		result, status, err := s.reconcileStreamRecordingAssignments(
			r.Context(),
			tx,
			existingID,
			"service.stream_import",
			"stream import updated recording assignment",
			"stream import updated source",
			current,
			updated,
			updated.RecordingState == model.RecordingStateOn,
		)
		if err != nil {
			return model.Stream{}, false, err
		}
		if status > 0 {
			return model.Stream{}, false, newAPIStatusError(status, "reconcile imported stream assignment: %v", result["error"])
		}
		if err := tx.Commit(r.Context()); err != nil {
			return model.Stream{}, false, fmt.Errorf("commit imported stream update: %w", err)
		}
		stream, err := s.getStreamByID(r.Context(), existingID)
		if err != nil {
			return model.Stream{}, false, fmt.Errorf("load imported stream %d: %w", existingID, err)
		}
		return stream, false, nil
	}
	if err != nil && err != pgx.ErrNoRows {
		return model.Stream{}, false, fmt.Errorf("query imported stream existence: %w", err)
	}
	if err := ensureImportedSourceURLProviderConflictFree(r.Context(), tx, profile.SourceURL, provider, 0); err != nil {
		return model.Stream{}, false, err
	}

	slug := strings.TrimSpace(req.Slug)
	if slug == "" {
		slug = slugify(provider + "-" + externalID)
	}
	slug, err = ensureAvailableSlugTx(r.Context(), tx, slug, 0)
	if err != nil {
		return model.Stream{}, false, err
	}

	var id int64
	if err := tx.QueryRow(r.Context(), `
		INSERT INTO streams (
			provider, external_id, name, slug, source_url, source_page_url,
			lat, lon, location_text, location_country, location_country_code, location_region, location_city, location_locality, location_source, metadata_jsonb,
			recording_state, source_family, capture_type, execution_class, capture_family, expected_fps, expected_image_interval_sec, execution_config_jsonb, tags
		)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)
		RETURNING id
	`, provider, externalID, name, slug, profile.SourceURL, profile.SourcePageURL,
		req.Lat, req.Lon, strings.TrimSpace(req.LocationText), strings.TrimSpace(req.LocationCountry), strings.ToUpper(strings.TrimSpace(req.LocationCountryCode)),
		strings.TrimSpace(req.LocationRegion), strings.TrimSpace(req.LocationCity), strings.TrimSpace(req.LocationLocality), strings.TrimSpace(req.LocationSource), metaBytes,
		string(model.RecordingStateOff), profile.SourceFamily, profile.CaptureType, profile.ExecutionClass, profile.CaptureFamily, profile.ExpectedFPS, profile.ExpectedImageIntervalSec, cfgBytes, dedupeStrings(req.Tags),
	).Scan(&id); err != nil {
		return model.Stream{}, false, newAPIStatusError(http.StatusConflict, "create imported stream: %v", err)
	}
	if err := tx.Commit(r.Context()); err != nil {
		return model.Stream{}, false, fmt.Errorf("commit imported stream insert: %w", err)
	}
	stream, err := s.getStreamByID(r.Context(), id)
	if err != nil {
		return model.Stream{}, false, fmt.Errorf("load imported stream %d: %w", id, err)
	}
	return stream, true, nil
}

func ensureImportedSourceURLProviderConflictFree(ctx context.Context, tx pgx.Tx, sourceURL string, provider string, keepID int64) error {
	sourceURL = strings.TrimSpace(sourceURL)
	provider = strings.TrimSpace(provider)
	if sourceURL == "" || provider == "" {
		return nil
	}
	var existingID int64
	var existingProvider string
	err := tx.QueryRow(ctx, `
		SELECT id, provider
		FROM streams
		WHERE source_url=$1 AND provider<>$2
		ORDER BY id ASC
		LIMIT 1
	`, sourceURL, provider).Scan(&existingID, &existingProvider)
	if err == pgx.ErrNoRows {
		return nil
	}
	if err != nil {
		return fmt.Errorf("query imported stream source_url conflict: %w", err)
	}
	if keepID > 0 && existingID == keepID {
		return nil
	}
	return newAPIStatusError(http.StatusConflict, "source_url already exists on stream %d under provider %s", existingID, strings.TrimSpace(existingProvider))
}

type slugQueryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func ensureAvailableSlugTx(ctx context.Context, q slugQueryRower, raw string, keepID int64) (string, error) {
	base := slugify(raw)
	if base == "" {
		base = "stream"
	}
	slug := base
	for attempt := 0; attempt < 1000; attempt++ {
		var existingID int64
		err := q.QueryRow(ctx, `SELECT id FROM streams WHERE slug=$1`, slug).Scan(&existingID)
		if err != nil {
			if err == pgx.ErrNoRows {
				return slug, nil
			}
			return "", fmt.Errorf("query slug availability: %w", err)
		}
		if keepID > 0 && existingID == keepID {
			return slug, nil
		}
		slug = fmt.Sprintf("%s-%d", base, attempt+2)
	}
	return "", newAPIStatusError(http.StatusConflict, "could not find available slug for %q", raw)
}

func firstNonEmptyImport(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
