package captureapi

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/apihttp"
	"github.com/daydemir/stoarama/backend/internal/capture"
)

type ClientConfig struct {
	BaseURL    string
	APIToken   string
	HTTPClient *http.Client
}

type Client struct {
	baseURL  string
	apiToken string
	httpc    *http.Client
	api      *apihttp.Client
}

type IngestSuccessRequest struct {
	StreamID           int64
	CapturedAt         time.Time
	SourceKind         string
	EffectiveMode      capture.Mode
	ResolvedURL        string
	MIMEType           string
	FrameBytes         []byte
	RecordingHeartbeat bool
}

type SegmentUploadIntentRequest struct {
	StreamID  int64
	MimeType  string
	SizeBytes int64
	StartAt   time.Time
}

type SegmentUploadIntent struct {
	IntentID    string
	Bucket      string
	ObjectKey   string
	UploadURL   string
	ExpiresAt   time.Time
	ContentType string
}

type IngestSegmentSuccessRequest struct {
	StreamID           int64
	SourceKind         string
	EffectiveMode      capture.Mode
	ResolvedURL        string
	UploadIntentID     string
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
	ThumbnailIntent    *SegmentUploadIntent
	ThumbnailSizeBytes int64
	ThumbnailSHA256    string
	RecordingHeartbeat bool
}

type IngestErrorRequest struct {
	StreamID      int64
	CapturedAt    time.Time
	SourceKind    string
	EffectiveMode capture.Mode
	ResolvedURL   string
	ErrorText     string
}

type ingestResponse struct {
	ConsecutiveErrors int  `json:"consecutive_errors"`
	Unsupported       bool `json:"unsupported"`
}

func NewClient(cfg ClientConfig) (*Client, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if baseURL == "" {
		return nil, fmt.Errorf("missing BaseURL")
	}
	if strings.TrimSpace(cfg.APIToken) == "" {
		return nil, fmt.Errorf("missing APIToken")
	}
	httpc := cfg.HTTPClient
	if httpc == nil {
		httpc = &http.Client{Timeout: 20 * time.Second}
	}
	api, err := apihttp.New(baseURL, cfg.APIToken, httpc, 20*time.Second)
	if err != nil {
		return nil, err
	}
	return &Client{
		baseURL:  baseURL,
		apiToken: strings.TrimSpace(cfg.APIToken),
		httpc:    httpc,
		api:      api,
	}, nil
}

func (c *Client) IngestSuccess(ctx context.Context, req IngestSuccessRequest) error {
	if req.StreamID <= 0 {
		return fmt.Errorf("stream_id must be > 0")
	}
	if len(req.FrameBytes) == 0 {
		return fmt.Errorf("frame bytes are empty")
	}
	if req.CapturedAt.IsZero() {
		req.CapturedAt = time.Now().UTC()
	}
	sourceKind := strings.TrimSpace(req.SourceKind)
	if sourceKind == "" {
		sourceKind = "live"
	}
	mimeType := strings.TrimSpace(req.MIMEType)
	if mimeType == "" {
		mimeType = "image/jpeg"
	}

	payload := map[string]any{
		"stream_id":           req.StreamID,
		"status":              "success",
		"captured_at":         req.CapturedAt.UTC().Format(time.RFC3339Nano),
		"source_kind":         sourceKind,
		"execution_class":     capture.ModeToExecutionClass(req.EffectiveMode),
		"resolved_url":        strings.TrimSpace(req.ResolvedURL),
		"mime_type":           mimeType,
		"frame_base64":        base64.StdEncoding.EncodeToString(req.FrameBytes),
		"recording_heartbeat": req.RecordingHeartbeat,
	}
	var out ingestResponse
	if err := c.postJSONWithRetry(ctx, "/api/v1/capture/ingest", payload, &out, ingestMaxAttempts()); err != nil {
		return err
	}
	return nil
}

func (c *Client) ReserveSegmentUpload(ctx context.Context, req SegmentUploadIntentRequest) (SegmentUploadIntent, error) {
	return c.reserveSegmentUpload(ctx, "capture_segment", "capture-segment", req)
}

func (c *Client) ReserveSegmentThumbnailUpload(ctx context.Context, req SegmentUploadIntentRequest) (SegmentUploadIntent, error) {
	if req.StartAt.IsZero() {
		return SegmentUploadIntent{}, fmt.Errorf("start_at is required")
	}
	return c.reserveSegmentUpload(ctx, "capture_segment_thumbnail", "capture-segment-thumbnail", req)
}

func (c *Client) reserveSegmentUpload(ctx context.Context, kind string, idempotencyPrefix string, req SegmentUploadIntentRequest) (SegmentUploadIntent, error) {
	if req.StreamID <= 0 {
		return SegmentUploadIntent{}, fmt.Errorf("stream_id must be > 0")
	}
	mimeType := strings.TrimSpace(req.MimeType)
	if mimeType == "" {
		mimeType = "video/mp4"
	}
	payload := map[string]any{
		"kind":      kind,
		"stream_id": req.StreamID,
		"mime_type": mimeType,
		"size_bytes": func() any {
			if req.SizeBytes > 0 {
				return req.SizeBytes
			}
			return nil
		}(),
	}
	if !req.StartAt.IsZero() {
		payload["segment_start_at"] = req.StartAt.UTC().Format(time.RFC3339Nano)
	}
	var out struct {
		IntentID    string    `json:"intent_id"`
		Bucket      string    `json:"bucket"`
		ObjectKey   string    `json:"object_key"`
		UploadURL   string    `json:"upload_url"`
		ExpiresAt   time.Time `json:"expires_at"`
		ContentType string    `json:"content_type"`
	}
	headers := map[string]string{
		"Idempotency-Key": buildIdempotencyKey(idempotencyPrefix, req.StreamID),
	}
	if err := c.postJSONWithHeaders(ctx, "/api/v1/media/upload-intents", payload, headers, &out); err != nil {
		return SegmentUploadIntent{}, err
	}
	return SegmentUploadIntent{
		IntentID:    strings.TrimSpace(out.IntentID),
		Bucket:      strings.TrimSpace(out.Bucket),
		ObjectKey:   strings.TrimSpace(out.ObjectKey),
		UploadURL:   strings.TrimSpace(out.UploadURL),
		ExpiresAt:   out.ExpiresAt,
		ContentType: strings.TrimSpace(out.ContentType),
	}, nil
}

func (c *Client) UploadFile(ctx context.Context, uploadURL string, path string, mimeType string) error {
	return c.api.PutFile(ctx, uploadURL, path, mimeType)
}

func (c *Client) UploadSegment(ctx context.Context, uploadURL string, body []byte, mimeType string) error {
	return c.api.PutBytes(ctx, uploadURL, body, mimeType)
}

func (c *Client) IngestSegmentSuccess(ctx context.Context, req IngestSegmentSuccessRequest) error {
	if req.StreamID <= 0 {
		return fmt.Errorf("stream_id must be > 0")
	}
	if req.SegmentStartAt.IsZero() || req.SegmentEndAt.IsZero() {
		return fmt.Errorf("segment_start_at and segment_end_at are required")
	}
	sourceKind := strings.TrimSpace(req.SourceKind)
	if sourceKind == "" {
		sourceKind = "live"
	}
	mimeType := strings.TrimSpace(req.MIMEType)
	if mimeType == "" {
		mimeType = "video/mp4"
	}
	payload := map[string]any{
		"stream_id":           req.StreamID,
		"status":              "success",
		"source_kind":         sourceKind,
		"execution_class":     capture.ModeToExecutionClass(req.EffectiveMode),
		"resolved_url":        strings.TrimSpace(req.ResolvedURL),
		"upload_intent_id":    strings.TrimSpace(req.UploadIntentID),
		"object_key":          strings.TrimSpace(req.ObjectKey),
		"mime_type":           mimeType,
		"size_bytes":          req.SizeBytes,
		"etag":                strings.TrimSpace(req.ETag),
		"sha256":              strings.TrimSpace(req.SHA256),
		"segment_start_at":    req.SegmentStartAt.UTC().Format(time.RFC3339Nano),
		"segment_end_at":      req.SegmentEndAt.UTC().Format(time.RFC3339Nano),
		"duration_ms":         req.DurationMs,
		"target_fps":          req.TargetFPS,
		"actual_fps":          req.ActualFPS,
		"video_codec":         strings.TrimSpace(req.VideoCodec),
		"audio_codec":         strings.TrimSpace(req.AudioCodec),
		"container":           strings.TrimSpace(req.Container),
		"audio_present":       req.AudioPresent,
		"recording_heartbeat": req.RecordingHeartbeat,
	}
	if req.ThumbnailIntent != nil {
		payload["thumbnail_upload_intent_id"] = strings.TrimSpace(req.ThumbnailIntent.IntentID)
		payload["thumbnail_object_key"] = strings.TrimSpace(req.ThumbnailIntent.ObjectKey)
		payload["thumbnail_mime_type"] = strings.TrimSpace(req.ThumbnailIntent.ContentType)
		payload["thumbnail_size_bytes"] = req.ThumbnailSizeBytes
		payload["thumbnail_sha256"] = strings.TrimSpace(req.ThumbnailSHA256)
	}
	var out ingestResponse
	if err := c.postJSONWithRetry(ctx, "/api/v1/capture/ingest", payload, &out, ingestMaxAttempts()); err != nil {
		return err
	}
	return nil
}

func (c *Client) IngestError(ctx context.Context, req IngestErrorRequest) (int, error) {
	if req.StreamID <= 0 {
		return 0, fmt.Errorf("stream_id must be > 0")
	}
	errText := strings.TrimSpace(req.ErrorText)
	if errText == "" {
		return 0, fmt.Errorf("error_text is required")
	}
	if req.CapturedAt.IsZero() {
		req.CapturedAt = time.Now().UTC()
	}
	sourceKind := strings.TrimSpace(req.SourceKind)
	if sourceKind == "" {
		sourceKind = "live"
	}

	payload := map[string]any{
		"stream_id":       req.StreamID,
		"status":          "error",
		"captured_at":     req.CapturedAt.UTC().Format(time.RFC3339Nano),
		"source_kind":     sourceKind,
		"execution_class": capture.ModeToExecutionClass(req.EffectiveMode),
		"resolved_url":    strings.TrimSpace(req.ResolvedURL),
		"error_text":      errText,
	}
	var out ingestResponse
	if err := c.postJSONWithRetry(ctx, "/api/v1/capture/ingest", payload, &out, ingestMaxAttempts()); err != nil {
		return 0, err
	}
	return out.ConsecutiveErrors, nil
}

func (c *Client) MarkUnsupported(ctx context.Context, streamID int64, effective capture.Mode, resolvedURL, reason string) error {
	if streamID <= 0 {
		return fmt.Errorf("stream_id must be > 0")
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return fmt.Errorf("reason is required")
	}
	payload := map[string]any{
		"stream_id":       streamID,
		"execution_class": capture.ModeToExecutionClass(effective),
		"resolved_url":    strings.TrimSpace(resolvedURL),
		"reason":          reason,
	}
	return c.postJSON(ctx, "/api/v1/capture/mark-unsupported", payload, nil)
}

func (c *Client) postJSON(ctx context.Context, path string, payload any, out any) error {
	return c.api.PostJSON(ctx, path, payload, out)
}

func (c *Client) postJSONWithHeaders(ctx context.Context, path string, payload any, headers map[string]string, out any) error {
	return c.api.PostJSONWithHeaders(ctx, path, payload, headers, out)
}

func buildIdempotencyKey(prefix string, streamID int64) string {
	if strings.TrimSpace(prefix) == "" {
		prefix = "capture-segment"
	}
	return apihttp.IdempotencyKey(prefix, streamID)
}

func (c *Client) postJSONWithRetry(ctx context.Context, path string, payload any, out any, attempts int) error {
	if attempts <= 1 {
		return c.postJSON(ctx, path, payload, out)
	}
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if attempt > 1 {
			delay := time.Duration(attempt-1) * time.Second
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}

		err := c.postJSON(ctx, path, payload, out)
		if err == nil {
			return nil
		}
		lastErr = err
		if !isRetryablePostError(err) {
			return err
		}
	}
	return lastErr
}

func ingestMaxAttempts() int {
	const fallback = 4
	raw := strings.TrimSpace(os.Getenv("CAPTURE_API_INGEST_MAX_ATTEMPTS"))
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 1 {
		return fallback
	}
	return v
}

func isRetryablePostError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "context canceled") {
		return false
	}
	if strings.Contains(msg, "status=429") ||
		strings.Contains(msg, "status=500") ||
		strings.Contains(msg, "status=502") ||
		strings.Contains(msg, "status=503") ||
		strings.Contains(msg, "status=504") ||
		strings.Contains(msg, "status=522") ||
		strings.Contains(msg, "status=524") {
		return true
	}
	if strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "context deadline exceeded") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "unexpected eof") ||
		strings.Contains(msg, "eof") {
		return true
	}
	return false
}
