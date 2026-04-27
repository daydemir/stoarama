package api

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/storage"
	"github.com/daydemir/stoarama/backend/internal/util"
)

type captureSegmentFinalize struct {
	IntentID           string
	ObjectKey          string
	MIMEType           string
	SizeBytes          int64
	ETag               string
	SHA256             string
	SegmentStartAt     time.Time
	SegmentEndAt       time.Time
	DurationMs         int64
	TargetFPS          int
	ActualFPS          *float64
	VideoCodec         string
	AudioCodec         string
	Container          string
	AudioPresent       bool
	SourceKind         string
	ExecutionClass     string
	ResolvedURL        string
	CaptureType        string
	ThumbnailIntentID  string
	ThumbnailObjectKey string
	ThumbnailMIME      string
	ThumbnailSizeBytes int64
	ThumbnailSHA256    string
}

func buildCaptureSegmentObjectKey(streamID int64, startAt time.Time, mimeType string) string {
	ext := fileExtensionFromMIME(mimeType)
	if ext == "" {
		ext = ".mp4"
	}
	startAt = startAt.UTC()
	return fmt.Sprintf("raw/stream/%d/%04d/%02d/%02d/segment-%d%s",
		streamID, startAt.Year(), int(startAt.Month()), startAt.Day(), startAt.UnixMilli(), ext)
}

func buildCaptureSegmentThumbnailObjectKey(streamID int64, startAt time.Time) string {
	startAt = startAt.UTC()
	return fmt.Sprintf("raw/stream/%d/%04d/%02d/%02d/segment-%d.jpg",
		streamID, startAt.Year(), int(startAt.Month()), startAt.Day(), startAt.UnixMilli())
}

func (s *Server) finalizeCaptureSegmentUpload(ctx context.Context, tx pgx.Tx, streamID int64, payload captureSegmentFinalize) (int64, error) {
	var intentObjectKey string
	var expectedSize sql.NullInt64
	var expectedETag string
	if strings.TrimSpace(payload.IntentID) != "" {
		if err := tx.QueryRow(ctx, `
			SELECT object_key, expected_size_bytes, expected_etag
			FROM upload_intents
			WHERE id=$1::uuid AND kind='capture_segment' AND status='pending'
		`, payload.IntentID).Scan(&intentObjectKey, &expectedSize, &expectedETag); err != nil {
			return 0, fmt.Errorf("load capture upload intent: %w", err)
		}
	}

	objectKey := strings.TrimSpace(payload.ObjectKey)
	if objectKey == "" {
		objectKey = strings.TrimSpace(intentObjectKey)
	}
	if objectKey == "" {
		return 0, fmt.Errorf("object_key is required")
	}
	if strings.TrimSpace(payload.MIMEType) == "" {
		payload.MIMEType = "video/mp4"
	}
	if payload.SizeBytes <= 0 {
		head, err := s.r2.Head(ctx, objectKey)
		if err != nil {
			return 0, fmt.Errorf("head uploaded segment: %w", err)
		}
		payload.SizeBytes = head.SizeBytes
		if strings.TrimSpace(payload.ETag) == "" {
			payload.ETag = head.ETag
		}
	}
	if expectedSize.Valid && expectedSize.Int64 > 0 && payload.SizeBytes > 0 && payload.SizeBytes != expectedSize.Int64 {
		return 0, fmt.Errorf("uploaded segment size mismatch: got=%d expected=%d", payload.SizeBytes, expectedSize.Int64)
	}
	if strings.TrimSpace(expectedETag) != "" && strings.TrimSpace(payload.ETag) != "" && strings.TrimSpace(payload.ETag) != strings.TrimSpace(expectedETag) {
		return 0, fmt.Errorf("uploaded segment etag mismatch")
	}

	mediaID, err := storage.UpsertMediaObject(ctx, tx, storage.MediaObjectInput{
		StorageProvider: "r2",
		Bucket:          s.r2.Bucket(),
		ObjectKey:       objectKey,
		MIMEType:        payload.MIMEType,
		SizeBytes:       payload.SizeBytes,
		ETag:            strings.TrimSpace(payload.ETag),
		SHA256:          strings.TrimSpace(payload.SHA256),
	})
	if err != nil {
		return 0, fmt.Errorf("upsert segment media object: %w", err)
	}
	if strings.TrimSpace(payload.IntentID) != "" {
		if _, err := tx.Exec(ctx, `
			UPDATE upload_intents
			SET status='consumed'
			WHERE id=$1::uuid
		`, payload.IntentID); err != nil {
			return 0, fmt.Errorf("complete capture upload intent: %w", err)
		}
	}
	return mediaID, nil
}

func (s *Server) finalizeCaptureSegmentThumbnailUpload(ctx context.Context, tx pgx.Tx, payload captureSegmentFinalize) (int64, error) {
	var intentObjectKey string
	var expectedSize sql.NullInt64
	if strings.TrimSpace(payload.ThumbnailIntentID) != "" {
		if err := tx.QueryRow(ctx, `
			SELECT object_key, expected_size_bytes
			FROM upload_intents
			WHERE id=$1::uuid AND kind='capture_segment_thumbnail' AND status='pending'
		`, payload.ThumbnailIntentID).Scan(&intentObjectKey, &expectedSize); err != nil {
			return 0, fmt.Errorf("load thumbnail upload intent: %w", err)
		}
	}

	objectKey := strings.TrimSpace(payload.ThumbnailObjectKey)
	if objectKey == "" {
		objectKey = strings.TrimSpace(intentObjectKey)
	}
	if objectKey == "" {
		return 0, fmt.Errorf("thumbnail_object_key is required")
	}
	mimeType := strings.TrimSpace(payload.ThumbnailMIME)
	if mimeType == "" {
		mimeType = "image/jpeg"
	}
	sizeBytes := payload.ThumbnailSizeBytes
	if sizeBytes <= 0 {
		head, err := s.r2.Head(ctx, objectKey)
		if err != nil {
			return 0, fmt.Errorf("head uploaded thumbnail: %w", err)
		}
		sizeBytes = head.SizeBytes
	}
	if expectedSize.Valid && expectedSize.Int64 > 0 && sizeBytes > 0 && sizeBytes != expectedSize.Int64 {
		return 0, fmt.Errorf("uploaded thumbnail size mismatch: got=%d expected=%d", sizeBytes, expectedSize.Int64)
	}
	mediaID, err := storage.UpsertMediaObject(ctx, tx, storage.MediaObjectInput{
		StorageProvider: "r2",
		Bucket:          s.r2.Bucket(),
		ObjectKey:       objectKey,
		MIMEType:        mimeType,
		SizeBytes:       sizeBytes,
		SHA256:          strings.TrimSpace(payload.ThumbnailSHA256),
	})
	if err != nil {
		return 0, fmt.Errorf("upsert thumbnail media object: %w", err)
	}
	if strings.TrimSpace(payload.ThumbnailIntentID) != "" {
		if _, err := tx.Exec(ctx, `
			UPDATE upload_intents
			SET status='consumed'
			WHERE id=$1::uuid
		`, payload.ThumbnailIntentID); err != nil {
			return 0, fmt.Errorf("complete thumbnail upload intent: %w", err)
		}
	}
	return mediaID, nil
}

func parseUUIDString(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	if _, err := uuid.Parse(raw); err != nil {
		return "", err
	}
	return raw, nil
}

func (s *Server) persistCaptureSegmentSuccess(ctx context.Context, streamID int64, payload captureSegmentFinalize) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin segment success tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	mediaID, err := s.finalizeCaptureSegmentUpload(ctx, tx, streamID, payload)
	if err != nil {
		return err
	}
	var thumbnailMediaID any
	if strings.TrimSpace(payload.ThumbnailIntentID) != "" || strings.TrimSpace(payload.ThumbnailObjectKey) != "" {
		if id, thumbErr := s.finalizeCaptureSegmentThumbnailUpload(ctx, tx, payload); thumbErr != nil {
			log.Printf("capture-segment thumbnail upload stream_id=%d start=%s error=%v", streamID, payload.SegmentStartAt.UTC().Format(time.RFC3339), thumbErr)
		} else if id > 0 {
			thumbnailMediaID = id
		}
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO capture_segments (
			stream_id, capture_job_id, media_object_id, execution_class, resolved_capture_type, resolved_url,
			thumbnail_media_object_id,
			segment_start_at, segment_end_at, duration_ms, target_fps, actual_fps,
			video_codec, audio_codec, container, audio_present,
			capture_status, capture_error, source_kind
		)
		VALUES ($1, NULL, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, 'success', NULL, $16)
		ON CONFLICT (stream_id, segment_start_at, media_object_id) DO NOTHING
	`, streamID, mediaID, payload.ExecutionClass, nullableTrimmed(payload.CaptureType), nullableTrimmed(payload.ResolvedURL), thumbnailMediaID,
		payload.SegmentStartAt, payload.SegmentEndAt, payload.DurationMs, payload.TargetFPS, payload.ActualFPS,
		nullableTrimmed(payload.VideoCodec), nullableTrimmed(payload.AudioCodec), payload.Container, payload.AudioPresent, payload.SourceKind); err != nil {
		return fmt.Errorf("insert capture segment success: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO stream_health (stream_id, captures_total, captures_success, captures_error, last_capture_at, last_error_at, last_error_text)
		VALUES ($1, 1, 1, 0, $2, NULL, NULL)
		ON CONFLICT (stream_id)
		DO UPDATE SET
			captures_total=stream_health.captures_total+1,
			captures_success=stream_health.captures_success+1,
			last_capture_at=EXCLUDED.last_capture_at,
			last_error_at=NULL,
			last_error_text=NULL,
			updated_at=now()
	`, streamID, payload.SegmentEndAt); err != nil {
		return fmt.Errorf("update stream_health success: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO stream_capture_runtime (stream_id, execution_class, resolved_capture_type, resolved_url, status, last_resolved_at, last_frame_at, consecutive_errors, last_error_text)
		VALUES ($1, $2, $3, $4, 'running', now(), $5, 0, NULL)
		ON CONFLICT (stream_id)
		DO UPDATE SET
			execution_class=EXCLUDED.execution_class,
			resolved_capture_type=COALESCE(EXCLUDED.resolved_capture_type, stream_capture_runtime.resolved_capture_type),
			resolved_url=EXCLUDED.resolved_url,
			status='running',
			last_frame_at=EXCLUDED.last_frame_at,
			consecutive_errors=0,
			last_error_text=NULL,
			updated_at=now()
	`, streamID, payload.ExecutionClass, nullableTrimmed(payload.CaptureType), payload.ResolvedURL, payload.SegmentEndAt); err != nil {
		return fmt.Errorf("update stream_capture_runtime success: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit segment success tx: %w", err)
	}
	return nil
}

func (s *Server) persistCaptureSegmentError(ctx context.Context, streamID int64, executionClass string, resolvedURL string, sourceKind string, captureErr string) (int, error) {
	errText := strings.TrimSpace(captureErr)
	if errText == "" {
		errText = "capture failed"
	}
	if sourceKind == "" {
		sourceKind = "live"
	}
	now := time.Now().UTC()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin segment error tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
		INSERT INTO capture_segments (
			stream_id, capture_job_id, media_object_id, execution_class, resolved_capture_type, resolved_url,
			segment_start_at, segment_end_at, duration_ms, target_fps, actual_fps,
			video_codec, audio_codec, container, audio_present,
			capture_status, capture_error, source_kind
		)
		VALUES ($1, NULL, NULL, $2, $3, $4, $5, $5, 0, $6, NULL, NULL, NULL, 'mp4', false, 'error', $7, $8)
	`, streamID, executionClass, nullableTrimmed(capture.ResolvedCaptureTypeFromURL(resolvedURL)), nullableTrimmed(resolvedURL), now, capture.SegmentTargetFPS, errText, sourceKind); err != nil {
		return 0, fmt.Errorf("insert capture segment error: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO stream_health (stream_id, captures_total, captures_success, captures_error, last_capture_at, last_error_at, last_error_text)
		VALUES ($1, 1, 0, 1, $2, $2, $3)
		ON CONFLICT (stream_id)
		DO UPDATE SET
			captures_total=stream_health.captures_total+1,
			captures_error=stream_health.captures_error+1,
			last_capture_at=EXCLUDED.last_capture_at,
			last_error_at=EXCLUDED.last_error_at,
			last_error_text=EXCLUDED.last_error_text,
			updated_at=now()
	`, streamID, now, errText); err != nil {
		return 0, fmt.Errorf("update stream_health error: %w", err)
	}

	var consecutive int
	if err := tx.QueryRow(ctx, `
		INSERT INTO stream_capture_runtime (stream_id, execution_class, resolved_capture_type, resolved_url, status, last_resolved_at, last_frame_at, consecutive_errors, last_error_text)
		VALUES ($1, $2, $3, $4, 'error', now(), NULL, 1, $5)
		ON CONFLICT (stream_id)
		DO UPDATE SET
			execution_class=EXCLUDED.execution_class,
			resolved_capture_type=COALESCE(EXCLUDED.resolved_capture_type, stream_capture_runtime.resolved_capture_type),
			resolved_url=COALESCE(NULLIF(EXCLUDED.resolved_url,''), stream_capture_runtime.resolved_url),
			status='error',
			consecutive_errors=stream_capture_runtime.consecutive_errors+1,
			last_error_text=EXCLUDED.last_error_text,
			updated_at=now()
		RETURNING consecutive_errors
	`, streamID, executionClass, nullableTrimmed(capture.ResolvedCaptureTypeFromURL(resolvedURL)), nullableTrimmed(resolvedURL), errText).Scan(&consecutive); err != nil {
		return 0, fmt.Errorf("update stream_capture_runtime error: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit segment error tx: %w", err)
	}
	return consecutive, nil
}

type captureSegmentListItem struct {
	ID                   int64     `json:"id"`
	StreamID             int64     `json:"stream_id"`
	CaptureJobID         *int64    `json:"capture_job_id,omitempty"`
	ExecutionClass       string    `json:"execution_class"`
	ResolvedCaptureType  *string   `json:"resolved_capture_type,omitempty"`
	ResolvedURL          *string   `json:"resolved_url,omitempty"`
	SegmentStartAt       time.Time `json:"segment_start_at"`
	SegmentEndAt         time.Time `json:"segment_end_at"`
	DurationMs           int64     `json:"duration_ms"`
	TargetFPS            int       `json:"target_fps"`
	ActualFPS            *float64  `json:"actual_fps,omitempty"`
	VideoCodec           *string   `json:"video_codec,omitempty"`
	AudioCodec           *string   `json:"audio_codec,omitempty"`
	Container            *string   `json:"container,omitempty"`
	AudioPresent         bool      `json:"audio_present"`
	CaptureStatus        string    `json:"capture_status"`
	CaptureError         *string   `json:"capture_error,omitempty"`
	SourceKind           string    `json:"source_kind"`
	ObjectKey            *string   `json:"object_key,omitempty"`
	MIMEType             *string   `json:"mime_type,omitempty"`
	SizeBytes            *int64    `json:"size_bytes,omitempty"`
	ThumbnailObjectKey   *string   `json:"thumbnail_object_key,omitempty"`
	ThumbnailMIMEType    *string   `json:"thumbnail_mime_type,omitempty"`
	ThumbnailSizeBytes   *int64    `json:"thumbnail_size_bytes,omitempty"`
	CreatedAt            time.Time `json:"created_at"`
	DownloadURL          string    `json:"download_url,omitempty"`
	ThumbnailDownloadURL string    `json:"thumbnail_download_url,omitempty"`
}

type captureSegmentQueryOptions struct {
	StreamID                    int64
	SegmentIDs                  []int64
	TimeRange                   *clipTimeRange
	CaptureStatus               string
	RequireDownloadable         bool
	Limit                       int
	Offset                      int
	IncludeDownloadURL          bool
	IncludeThumbnailDownloadURL bool
}

func captureSegmentWhere(opts captureSegmentQueryOptions) ([]string, []any) {
	where := []string{"cs.stream_id = $1"}
	args := []any{opts.StreamID}
	if len(opts.SegmentIDs) > 0 {
		args = append(args, opts.SegmentIDs)
		where = append(where, fmt.Sprintf("cs.id = ANY($%d::bigint[])", len(args)))
	}
	if opts.TimeRange != nil {
		args = append(args, opts.TimeRange.From)
		where = append(where, fmt.Sprintf("cs.segment_end_at > $%d", len(args)))
		args = append(args, opts.TimeRange.To)
		where = append(where, fmt.Sprintf("cs.segment_start_at < $%d", len(args)))
	}
	if status := strings.TrimSpace(opts.CaptureStatus); status != "" {
		args = append(args, status)
		where = append(where, fmt.Sprintf("cs.capture_status = $%d", len(args)))
	}
	if opts.RequireDownloadable {
		where = append(where, "cs.media_object_id IS NOT NULL")
		where = append(where, "NULLIF(TRIM(mo.object_key), '') IS NOT NULL")
	}
	return where, args
}

func (s *Server) countCaptureSegments(ctx context.Context, opts captureSegmentQueryOptions) (int64, error) {
	where, args := captureSegmentWhere(opts)
	var total int64
	if err := s.pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT COUNT(*)::bigint
		FROM capture_segments cs
		LEFT JOIN media_objects mo ON mo.id = cs.media_object_id
		WHERE %s
	`, strings.Join(where, " AND ")), args...).Scan(&total); err != nil {
		return 0, fmt.Errorf("count capture segments: %w", err)
	}
	return total, nil
}

func (s *Server) queryCaptureSegments(ctx context.Context, opts captureSegmentQueryOptions) ([]captureSegmentListItem, error) {
	where, args := captureSegmentWhere(opts)
	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}
	offset := opts.Offset
	if offset < 0 {
		offset = 0
	}
	args = append(args, limit, offset)
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT
			cs.id,
			cs.stream_id,
			cs.capture_job_id,
			cs.execution_class,
			cs.resolved_capture_type,
			cs.resolved_url,
			cs.segment_start_at,
			cs.segment_end_at,
			cs.duration_ms,
			cs.target_fps,
			cs.actual_fps,
			cs.video_codec,
			cs.audio_codec,
			cs.container,
			cs.audio_present,
			cs.capture_status,
			cs.capture_error,
			cs.source_kind,
			mo.object_key,
			mo.mime_type,
			mo.size_bytes,
			tmo.object_key,
			tmo.mime_type,
			tmo.size_bytes,
			cs.created_at
		FROM capture_segments cs
		LEFT JOIN media_objects mo ON mo.id = cs.media_object_id
		LEFT JOIN media_objects tmo ON tmo.id = cs.thumbnail_media_object_id
		WHERE %s
		ORDER BY cs.segment_start_at DESC, cs.id DESC
		LIMIT $%d OFFSET $%d
	`, strings.Join(where, " AND "), len(args)-1, len(args)), args...)
	if err != nil {
		return nil, fmt.Errorf("query capture segments: %w", err)
	}
	defer rows.Close()

	items := make([]captureSegmentListItem, 0, limit)
	for rows.Next() {
		var it captureSegmentListItem
		if err := rows.Scan(
			&it.ID,
			&it.StreamID,
			&it.CaptureJobID,
			&it.ExecutionClass,
			&it.ResolvedCaptureType,
			&it.ResolvedURL,
			&it.SegmentStartAt,
			&it.SegmentEndAt,
			&it.DurationMs,
			&it.TargetFPS,
			&it.ActualFPS,
			&it.VideoCodec,
			&it.AudioCodec,
			&it.Container,
			&it.AudioPresent,
			&it.CaptureStatus,
			&it.CaptureError,
			&it.SourceKind,
			&it.ObjectKey,
			&it.MIMEType,
			&it.SizeBytes,
			&it.ThumbnailObjectKey,
			&it.ThumbnailMIMEType,
			&it.ThumbnailSizeBytes,
			&it.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan capture segment: %w", err)
		}
		if opts.IncludeDownloadURL && it.ObjectKey != nil && strings.TrimSpace(*it.ObjectKey) != "" {
			if url, err := s.r2.PresignGet(ctx, *it.ObjectKey, s.cfg.R2SignGetTTL); err == nil {
				it.DownloadURL = url
			}
		}
		if opts.IncludeThumbnailDownloadURL && it.ThumbnailObjectKey != nil && strings.TrimSpace(*it.ThumbnailObjectKey) != "" {
			if url, err := s.r2.PresignGet(ctx, *it.ThumbnailObjectKey, s.cfg.R2SignGetTTL); err == nil {
				it.ThumbnailDownloadURL = url
			}
		}
		items = append(items, it)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate capture segments: %w", rows.Err())
	}
	return items, nil
}

func (s *Server) handleCaptureStreamSegmentsList(w http.ResponseWriter, r *http.Request) {
	streamID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	limit := parseIntQuery(r, "limit", 100, 1, 1000)
	offset := parseIntQuery(r, "offset", 0, 0, 1_000_000)
	includeDownloadURLs := true
	if v := parseBoolQueryPtr(r, "include_download_urls"); v != nil {
		includeDownloadURLs = *v
	}
	includeThumbnailDownloadURLs := true
	if v := parseBoolQueryPtr(r, "include_thumbnail_download_urls"); v != nil {
		includeThumbnailDownloadURLs = *v
	}
	items, err := s.queryCaptureSegments(r.Context(), captureSegmentQueryOptions{
		StreamID:                    streamID,
		Limit:                       limit,
		Offset:                      offset,
		IncludeDownloadURL:          includeDownloadURLs,
		IncludeThumbnailDownloadURL: includeThumbnailDownloadURLs,
	})
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"items":  items,
		"limit":  limit,
		"offset": offset,
	})
}

func (s *Server) handleCaptureStreamSegmentLatest(w http.ResponseWriter, r *http.Request) {
	streamID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	items, err := s.queryCaptureSegments(r.Context(), captureSegmentQueryOptions{
		StreamID:                    streamID,
		Limit:                       1,
		Offset:                      0,
		IncludeDownloadURL:          true,
		IncludeThumbnailDownloadURL: true,
	})
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	var item any
	if len(items) > 0 {
		item = items[0]
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"item": item})
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
