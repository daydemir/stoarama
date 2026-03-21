package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

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

func (s *Server) upsertImportedStream(r *http.Request, req serviceStreamImportRequest) (model.Stream, bool, error) {
	provider := strings.TrimSpace(req.Provider)
	externalID := strings.TrimSpace(req.ExternalID)
	name := strings.TrimSpace(req.Name)
	if provider == "" || externalID == "" || name == "" {
		return model.Stream{}, false, newAPIStatusError(http.StatusBadRequest, "provider, external_id, and name are required")
	}

	fields, err := capture.DeriveCanonicalStreamFields(req.SourceURL, req.SourcePageURL, req.CaptureType, req.SourceFamily, req.ExecutionClass)
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
				execution_config_jsonb=$18,
				tags=$19,
				updated_at=now()
			WHERE id=$1
		`, existingID, name, fields.SourceURL, fields.SourcePageURL,
			nil, nil, strings.TrimSpace(req.LocationText), strings.TrimSpace(req.LocationCountry), strings.ToUpper(strings.TrimSpace(req.LocationCountryCode)),
			strings.TrimSpace(req.LocationRegion), strings.TrimSpace(req.LocationCity), strings.TrimSpace(req.LocationLocality), strings.TrimSpace(req.LocationSource),
			metaBytes, fields.SourceFamily, fields.CaptureType, fields.ExecutionClass, cfgBytes, dedupeStrings(req.Tags),
		); err != nil {
			return model.Stream{}, false, fmt.Errorf("update imported stream: %w", err)
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
			recording_state, source_family, capture_type, execution_class, execution_config_jsonb, tags
		)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)
		RETURNING id
	`, provider, externalID, name, slug, fields.SourceURL, fields.SourcePageURL,
		nil, nil, strings.TrimSpace(req.LocationText), strings.TrimSpace(req.LocationCountry), strings.ToUpper(strings.TrimSpace(req.LocationCountryCode)),
		strings.TrimSpace(req.LocationRegion), strings.TrimSpace(req.LocationCity), strings.TrimSpace(req.LocationLocality), strings.TrimSpace(req.LocationSource), metaBytes,
		string(model.RecordingStateOff), fields.SourceFamily, fields.CaptureType, fields.ExecutionClass, cfgBytes, dedupeStrings(req.Tags),
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
