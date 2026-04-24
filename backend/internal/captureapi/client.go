package captureapi

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/model"
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

type RecordingStateUpdateRequest struct {
	StreamID       int64
	State          model.RecordingState
	ExecutionClass string
	Actor          string
	Reason         string
}

type RecordingAssignRequest struct {
	StreamID       int64
	ServerID       string
	ExecutionClass string
	Actor          string
	Reason         string
}

type RecordingAssignResponse struct {
	StreamID           int64  `json:"stream_id"`
	ServerID           string `json:"server_id"`
	ExecutionClass     string `json:"execution_class"`
	AssignmentRevision int64  `json:"assignment_revision"`
	EventType          string `json:"event_type"`
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

type WorkerHeartbeatRequest struct {
	WorkerID       string
	ExecutionClass string
	Capacity       int
	LeaseSec       int
	MetadataJSON   map[string]any
}

type RecordingServerHeartbeatClass struct {
	ExecutionClass string
	MaxActive      int
	Draining       bool
}

type RecordingServerHeartbeatRequest struct {
	ServerID         string
	LeaseSec         int
	ExecutionClasses []RecordingServerHeartbeatClass
	MetadataJSON     map[string]any
}

type RecordingProcessHeartbeatRequest struct {
	StreamID       int64
	ExecutionClass string
	ServerID       string
	AssignmentRev  int64
	ProcessID      string
	WorkerID       string
	Status         string
	LeaseSec       int
	LastFrameAt    *time.Time
	ErrorText      string
	StartReason    string
	RestartCount   int
	LastHeartbeat  *time.Time
}

type RecordingProcessStoppedRequest struct {
	StreamID       int64
	ProcessID      string
	WorkerID       string
	ServerID       string
	ExecutionClass string
	AssignmentRev  int64
	FinalStatus    string
	StopReason     string
	ErrorText      string
	StoppedAt      *time.Time
}

type ingestResponse struct {
	ConsecutiveErrors int  `json:"consecutive_errors"`
	Unsupported       bool `json:"unsupported"`
}

type RecordingSettings struct {
	IntervalSec int       `json:"interval_sec"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type RecordingAssignment struct {
	StreamID           int64          `json:"stream_id"`
	ServerID           string         `json:"server_id"`
	ExecutionClass     string         `json:"execution_class"`
	AssignmentRevision int64          `json:"assignment_revision"`
	Provider           string         `json:"provider"`
	StreamURL          string         `json:"source_url"`
	SourcePageURL      string         `json:"source_page_url"`
	CaptureType        string         `json:"capture_type"`
	CaptureConfigJSON  map[string]any `json:"execution_config_json"`
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
	return &Client{
		baseURL:  baseURL,
		apiToken: strings.TrimSpace(cfg.APIToken),
		httpc:    httpc,
	}, nil
}

func (c *Client) ListRecordedStreams(ctx context.Context, limit int) ([]model.Stream, error) {
	if limit <= 0 {
		limit = 500
	}
	out := make([]model.Stream, 0, limit)
	total := 1
	offset := 0
	for offset < total {
		batch, t, err := c.listRecordedStreamsPage(ctx, model.RecordingStateOn, limit, offset)
		if err != nil {
			return nil, err
		}
		total = t
		out = append(out, batch...)
		if len(batch) == 0 {
			break
		}
		offset += len(batch)
	}
	return out, nil
}

func (c *Client) listRecordedStreamsPage(ctx context.Context, state model.RecordingState, limit, offset int) ([]model.Stream, int, error) {
	if state != model.RecordingStateOn && state != model.RecordingStateOff {
		return nil, 0, fmt.Errorf("unsupported recording_state: %s", state)
	}
	u, err := url.Parse(c.baseURL + "/api/v1/capture/streams")
	if err != nil {
		return nil, 0, fmt.Errorf("parse capture streams URL: %w", err)
	}
	q := u.Query()
	q.Set("recording_state", string(state))
	q.Set("limit", fmt.Sprintf("%d", limit))
	q.Set("offset", fmt.Sprintf("%d", offset))
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, 0, fmt.Errorf("build capture streams request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiToken)

	resp, err := c.httpc.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("capture streams request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 16*1024))
		return nil, 0, fmt.Errorf("capture streams status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b)))
	}

	var payload struct {
		Total int `json:"total"`
		Items []struct {
			Stream model.Stream `json:"stream"`
		} `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, 0, fmt.Errorf("decode capture streams response: %w", err)
	}

	batch := make([]model.Stream, 0, len(payload.Items))
	for _, item := range payload.Items {
		stream := item.Stream
		normalizeStreamPayload(&stream)
		batch = append(batch, stream)
	}
	return batch, payload.Total, nil
}

func (c *Client) GetStream(ctx context.Context, streamID int64) (model.Stream, error) {
	if streamID <= 0 {
		return model.Stream{}, fmt.Errorf("stream_id must be > 0")
	}
	u := fmt.Sprintf("%s/api/v1/capture/streams/%d", c.baseURL, streamID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return model.Stream{}, fmt.Errorf("build stream detail request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiToken)

	resp, err := c.httpc.Do(req)
	if err != nil {
		return model.Stream{}, fmt.Errorf("stream detail request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 16*1024))
		return model.Stream{}, fmt.Errorf("stream detail status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b)))
	}

	var payload struct {
		Stream model.Stream `json:"stream"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return model.Stream{}, fmt.Errorf("decode stream detail response: %w", err)
	}
	if payload.Stream.ID <= 0 {
		return model.Stream{}, fmt.Errorf("stream detail missing stream payload for id=%d", streamID)
	}
	normalizeStreamPayload(&payload.Stream)
	return payload.Stream, nil
}

func normalizeStreamPayload(stream *model.Stream) {
	if stream == nil {
		return
	}
	if stream.ExecutionConfigJSON == nil {
		stream.ExecutionConfigJSON = map[string]any{}
	}
}

func (c *Client) SetRecordingState(ctx context.Context, req RecordingStateUpdateRequest) error {
	if req.StreamID <= 0 {
		return fmt.Errorf("stream_id must be > 0")
	}
	if req.State != model.RecordingStateOn && req.State != model.RecordingStateOff {
		return fmt.Errorf("recording state must be off|on")
	}
	payload := map[string]any{
		"recording_state": string(req.State),
		"execution_class": strings.TrimSpace(req.ExecutionClass),
		"actor":           strings.TrimSpace(req.Actor),
		"reason":          strings.TrimSpace(req.Reason),
	}
	return c.postJSON(ctx, fmt.Sprintf("/api/v1/recording/streams/%d/state", req.StreamID), payload, nil)
}

func (c *Client) AssignRecordingStream(ctx context.Context, req RecordingAssignRequest) (RecordingAssignResponse, error) {
	if req.StreamID <= 0 {
		return RecordingAssignResponse{}, fmt.Errorf("stream_id must be > 0")
	}
	serverID := strings.TrimSpace(req.ServerID)
	if serverID == "" {
		return RecordingAssignResponse{}, fmt.Errorf("server_id is required")
	}
	payload := map[string]any{
		"server_id":       serverID,
		"execution_class": strings.TrimSpace(req.ExecutionClass),
		"actor":           strings.TrimSpace(req.Actor),
		"reason":          strings.TrimSpace(req.Reason),
	}
	var out RecordingAssignResponse
	if err := c.postJSON(ctx, fmt.Sprintf("/api/v1/recording/streams/%d/assign", req.StreamID), payload, &out); err != nil {
		return RecordingAssignResponse{}, err
	}
	return out, nil
}

func normalizeExecutionClassValue(raw string) (string, error) {
	if executionClass, ok := capture.NormalizeExecutionClass(raw); ok {
		return executionClass, nil
	}
	return "", fmt.Errorf("invalid execution_class %q", raw)
}

func (c *Client) GetRecordingSettings(ctx context.Context) (RecordingSettings, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/v1/recording/settings", nil)
	if err != nil {
		return RecordingSettings{}, fmt.Errorf("build recording settings request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiToken)

	resp, err := c.httpc.Do(req)
	if err != nil {
		return RecordingSettings{}, fmt.Errorf("recording settings request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 16*1024))
		return RecordingSettings{}, fmt.Errorf("recording settings status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b)))
	}

	var payload RecordingSettings
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return RecordingSettings{}, fmt.Errorf("decode recording settings response: %w", err)
	}
	if payload.IntervalSec <= 0 {
		return RecordingSettings{}, fmt.Errorf("invalid recording interval from API: %d", payload.IntervalSec)
	}
	return payload, nil
}

func (c *Client) listRecordingAssignmentsPage(ctx context.Context, serverID string, executionClass string, limit int, offset int) ([]RecordingAssignment, error) {
	serverID = strings.TrimSpace(serverID)
	if serverID == "" {
		return nil, fmt.Errorf("server_id is required")
	}
	if limit <= 0 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}
	u, err := url.Parse(c.baseURL + "/api/v1/service/recording/assignments")
	if err != nil {
		return nil, fmt.Errorf("parse recording assignments URL: %w", err)
	}
	q := u.Query()
	q.Set("server_id", serverID)
	if strings.TrimSpace(executionClass) != "" {
		q.Set("execution_class", strings.TrimSpace(executionClass))
	}
	q.Set("limit", fmt.Sprintf("%d", limit))
	q.Set("offset", fmt.Sprintf("%d", offset))
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build recording assignments request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiToken)

	resp, err := c.httpc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("recording assignments request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 16*1024))
		return nil, fmt.Errorf("recording assignments status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b)))
	}

	var payload struct {
		Items []RecordingAssignment `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode recording assignments response: %w", err)
	}
	for i := range payload.Items {
		if payload.Items[i].CaptureConfigJSON == nil {
			payload.Items[i].CaptureConfigJSON = map[string]any{}
		}
	}
	return payload.Items, nil
}

func (c *Client) ListRecordingAssignments(ctx context.Context, serverID string, executionClass string, limit int, offset int) ([]RecordingAssignment, error) {
	if limit <= 0 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}
	out := make([]RecordingAssignment, 0, limit)
	for {
		items, err := c.listRecordingAssignmentsPage(ctx, serverID, executionClass, limit, offset)
		if err != nil {
			return nil, err
		}
		out = append(out, items...)
		if len(items) < limit {
			return out, nil
		}
		offset += len(items)
	}
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
	if strings.TrimSpace(uploadURL) == "" {
		return fmt.Errorf("upload_url is required")
	}
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open upload file: %w", err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat upload file: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, uploadURL, f)
	if err != nil {
		return fmt.Errorf("build upload request: %w", err)
	}
	req.ContentLength = st.Size()
	if strings.TrimSpace(mimeType) != "" {
		req.Header.Set("Content-Type", strings.TrimSpace(mimeType))
	}
	resp, err := c.httpc.Do(req)
	if err != nil {
		return fmt.Errorf("upload file failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 16*1024))
		return fmt.Errorf("upload file status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return nil
}

func (c *Client) UploadSegment(ctx context.Context, uploadURL string, body []byte, mimeType string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, uploadURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build segment upload request: %w", err)
	}
	if strings.TrimSpace(mimeType) != "" {
		req.Header.Set("Content-Type", strings.TrimSpace(mimeType))
	}
	resp, err := c.httpc.Do(req)
	if err != nil {
		return fmt.Errorf("upload segment failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 16*1024))
		return fmt.Errorf("upload segment status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return nil
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

func (c *Client) SetRuntimeStopped(ctx context.Context, streamID int64) error {
	if streamID <= 0 {
		return fmt.Errorf("stream_id must be > 0")
	}
	payload := map[string]any{"stream_id": streamID}
	return c.postJSON(ctx, "/api/v1/capture/runtime/stopped", payload, nil)
}

func (c *Client) WorkerHeartbeat(ctx context.Context, req WorkerHeartbeatRequest) error {
	workerID := strings.TrimSpace(req.WorkerID)
	if workerID == "" {
		return fmt.Errorf("worker_id is required")
	}
	executionClass, err := normalizeExecutionClassValue(req.ExecutionClass)
	if err != nil {
		return err
	}
	if req.Capacity <= 0 {
		return fmt.Errorf("capacity must be > 0")
	}
	leaseSec := req.LeaseSec
	if leaseSec <= 0 {
		leaseSec = 45
	}
	payload := map[string]any{
		"worker_id":       workerID,
		"execution_class": executionClass,
		"capacity":        req.Capacity,
		"lease_sec":       leaseSec,
		"metadata_json":   nonNilMap(req.MetadataJSON),
	}
	return c.postJSON(ctx, "/api/v1/capture/worker-heartbeat", payload, nil)
}

func nonNilMap(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return m
}

func (c *Client) WorkerStopped(ctx context.Context, workerID string, executionClass string) error {
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return fmt.Errorf("worker_id is required")
	}
	executionClassValue, err := normalizeExecutionClassValue(executionClass)
	if err != nil {
		return err
	}
	payload := map[string]any{
		"worker_id":       workerID,
		"execution_class": executionClassValue,
	}
	return c.postJSON(ctx, "/api/v1/capture/worker-stopped", payload, nil)
}

func (c *Client) RecordingServerHeartbeat(ctx context.Context, req RecordingServerHeartbeatRequest) error {
	serverID := strings.TrimSpace(req.ServerID)
	if serverID == "" {
		return fmt.Errorf("server_id is required")
	}
	leaseSec := req.LeaseSec
	if leaseSec <= 0 {
		leaseSec = 45
	}
	if leaseSec > 3600 {
		return fmt.Errorf("lease_sec must be <= 3600")
	}
	if len(req.ExecutionClasses) == 0 {
		return fmt.Errorf("execution_classes is required")
	}
	items := make([]map[string]any, 0, len(req.ExecutionClasses))
	seen := map[string]struct{}{}
	for _, modeItem := range req.ExecutionClasses {
		executionClass, err := normalizeExecutionClassValue(modeItem.ExecutionClass)
		if err != nil {
			return err
		}
		if _, ok := seen[executionClass]; ok {
			return fmt.Errorf("duplicate execution_class %q", executionClass)
		}
		seen[executionClass] = struct{}{}
		if modeItem.MaxActive < 0 {
			return fmt.Errorf("max_active must be >= 0 for execution_class %q", executionClass)
		}
		items = append(items, map[string]any{
			"execution_class": executionClass,
			"max_active":      modeItem.MaxActive,
			"draining":        modeItem.Draining,
		})
	}
	payload := map[string]any{
		"server_id":         serverID,
		"lease_sec":         leaseSec,
		"execution_classes": items,
		"metadata_json":     nonNilMap(req.MetadataJSON),
	}
	return c.postJSON(ctx, "/api/v1/recording/servers/heartbeat", payload, nil)
}

func (c *Client) RecordingServerStopped(ctx context.Context, serverID string) error {
	serverID = strings.TrimSpace(serverID)
	if serverID == "" {
		return fmt.Errorf("server_id is required")
	}
	return c.postJSON(ctx, "/api/v1/recording/servers/stopped", map[string]any{
		"server_id": serverID,
	}, nil)
}

func (c *Client) RecordingProcessHeartbeat(ctx context.Context, req RecordingProcessHeartbeatRequest) error {
	if req.StreamID <= 0 {
		return fmt.Errorf("stream_id must be > 0")
	}
	executionClass, err := normalizeExecutionClassValue(req.ExecutionClass)
	if err != nil {
		return err
	}
	serverID := strings.TrimSpace(req.ServerID)
	if serverID == "" {
		return fmt.Errorf("server_id is required")
	}
	processID := strings.TrimSpace(req.ProcessID)
	if processID == "" {
		return fmt.Errorf("process_id is required")
	}
	workerID := strings.TrimSpace(req.WorkerID)
	if workerID == "" {
		return fmt.Errorf("worker_id is required")
	}
	status := strings.TrimSpace(strings.ToLower(req.Status))
	if status != "starting" && status != "running" {
		return fmt.Errorf("status must be starting|running")
	}
	leaseSec := req.LeaseSec
	if leaseSec <= 0 {
		leaseSec = 20
	}
	if leaseSec > 3600 {
		return fmt.Errorf("lease_sec must be <= 3600")
	}
	restartCount := req.RestartCount
	if restartCount < 0 {
		restartCount = 0
	}
	payload := map[string]any{
		"stream_id":           req.StreamID,
		"execution_class":     executionClass,
		"server_id":           serverID,
		"assignment_revision": req.AssignmentRev,
		"process_id":          processID,
		"worker_id":           workerID,
		"status":              status,
		"lease_sec":           leaseSec,
		"last_frame_at":       req.LastFrameAt,
		"error_text":          strings.TrimSpace(req.ErrorText),
		"start_reason":        strings.TrimSpace(req.StartReason),
		"restart_count":       restartCount,
		"last_heartbeat_at":   req.LastHeartbeat,
	}
	return c.postJSON(ctx, "/api/v1/recording/process/heartbeat", payload, nil)
}

func (c *Client) RecordingProcessStopped(ctx context.Context, req RecordingProcessStoppedRequest) error {
	if req.StreamID <= 0 {
		return fmt.Errorf("stream_id must be > 0")
	}
	processID := strings.TrimSpace(req.ProcessID)
	if processID == "" {
		return fmt.Errorf("process_id is required")
	}
	workerID := strings.TrimSpace(req.WorkerID)
	if workerID == "" {
		return fmt.Errorf("worker_id is required")
	}
	finalStatus := strings.TrimSpace(strings.ToLower(req.FinalStatus))
	if finalStatus == "" {
		finalStatus = "stopped"
	}
	serverID := strings.TrimSpace(req.ServerID)
	executionClass := ""
	if strings.TrimSpace(req.ExecutionClass) != "" {
		var err error
		executionClass, err = normalizeExecutionClassValue(req.ExecutionClass)
		if err != nil {
			return err
		}
	}
	payload := map[string]any{
		"stream_id":           req.StreamID,
		"process_id":          processID,
		"worker_id":           workerID,
		"server_id":           serverID,
		"execution_class":     executionClass,
		"assignment_revision": req.AssignmentRev,
		"final_status":        finalStatus,
		"stop_reason":         strings.TrimSpace(req.StopReason),
		"error_text":          strings.TrimSpace(req.ErrorText),
	}
	if req.StoppedAt != nil && !req.StoppedAt.IsZero() {
		payload["stopped_at"] = req.StoppedAt.UTC().Format(time.RFC3339Nano)
	}
	return c.postJSON(ctx, "/api/v1/recording/process/stopped", payload, nil)
}

func (c *Client) postJSON(ctx context.Context, path string, payload any, out any) error {
	return c.postJSONWithHeaders(ctx, path, payload, nil, out)
}

func (c *Client) postJSONWithHeaders(ctx context.Context, path string, payload any, headers map[string]string, out any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal request payload: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("build request %s: %w", path, err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	for k, v := range headers {
		if strings.TrimSpace(k) == "" || strings.TrimSpace(v) == "" {
			continue
		}
		req.Header.Set(k, v)
	}

	resp, err := c.httpc.Do(req)
	if err != nil {
		return fmt.Errorf("request %s failed: %w", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 16*1024))
		return fmt.Errorf("request %s status=%d body=%s", path, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil && !errorsIsEOF(err) {
		return fmt.Errorf("decode %s response: %w", path, err)
	}
	return nil
}

func buildIdempotencyKey(prefix string, streamID int64) string {
	const fallback = "capture-segment"
	p := strings.TrimSpace(prefix)
	if p == "" {
		p = fallback
	}
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return fmt.Sprintf("%s-%d-%d", p, streamID, time.Now().UTC().UnixNano())
	}
	return fmt.Sprintf("%s-%d-%x", p, streamID, buf[:])
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

func errorsIsEOF(err error) bool {
	return err == io.EOF
}
