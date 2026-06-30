// Package recordingapi is the recorder worker's HTTP client for the six
// recording endpoints. It authenticates with a per-droplet local_recorder node
// token (Bearer), never the shared service token. It is modeled on
// internal/captureapi but is independent of it.
package recordingapi

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

type ClientConfig struct {
	BaseURL    string
	NodeToken  string
	HTTPClient *http.Client
}

type Client struct {
	baseURL   string
	nodeToken string
	httpc     *http.Client
}

func NewClient(cfg ClientConfig) (*Client, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if baseURL == "" {
		return nil, fmt.Errorf("missing BaseURL")
	}
	if strings.TrimSpace(cfg.NodeToken) == "" {
		return nil, fmt.Errorf("missing NodeToken")
	}
	httpc := cfg.HTTPClient
	if httpc == nil {
		httpc = &http.Client{Timeout: 60 * time.Second}
	}
	return &Client{
		baseURL:   baseURL,
		nodeToken: strings.TrimSpace(cfg.NodeToken),
		httpc:     httpc,
	}, nil
}

// RecordingJob is a leased clip-capture unit.
type RecordingJob struct {
	JobID                int64     `json:"job_id"`
	RecordingID          int64     `json:"recording_id"`
	SourceURL            string    `json:"source_url"`
	ClipDurationSec      int       `json:"clip_duration_sec"`
	StorageDestinationID int64     `json:"storage_destination_id"`
	FireAt               time.Time `json:"fire_at"`
	AttemptCount         int       `json:"attempt_count"`
	LeaseExpiresAt       time.Time `json:"lease_expires_at"`
	// TargetFPS, when non-nil, normalizes each captured clip to that exact frame
	// rate (re-encode). nil = Source/native (stream-copy, preserve source fps).
	TargetFPS *int `json:"target_fps"`
	// Kind is 'clip' (default, per-cron-fire) or 'continuous_window' (one window-
	// long lease driving back-to-back segment capture). WindowEndAt is the
	// continuous window's close instant (zero/nil for a clip job).
	Kind        string     `json:"kind"`
	WindowEndAt *time.Time `json:"window_end_at"`
}

// ClipUploadIntent is a presigned PUT against the user's bucket.
type ClipUploadIntent struct {
	IntentID     string    `json:"intent_id"`
	UploadURL    string    `json:"upload_url"`
	ObjectKey    string    `json:"object_key"`
	Bucket       string    `json:"bucket"`
	Endpoint     string    `json:"endpoint"`
	ContentType  string    `json:"content_type"`
	MaxSizeBytes int64     `json:"max_size_bytes"`
	ExpiresAt    time.Time `json:"expires_at"`
}

// IngestClipRequest carries the captured clip's metadata to the ingest endpoint.
type IngestClipRequest struct {
	IntentID     string
	JobID        int64
	SizeBytes    int64
	ETag         string
	SHA256       string
	DurationMs   int64
	VideoCodec   string
	AudioCodec   string
	AudioPresent bool
	ActualFPS    *float64
	Container    string
	ResolvedURL  string
	ClipStartAt  time.Time
	ClipEndAt    time.Time
}

// LeaseRecordingJob leases one due job, or returns (nil, nil) when none is due.
func (c *Client) LeaseRecordingJob(ctx context.Context) (*RecordingJob, error) {
	var out struct {
		Job *RecordingJob `json:"job"`
	}
	if err := c.postJSON(ctx, "/api/v1/recording/jobs/lease", map[string]any{}, &out); err != nil {
		return nil, err
	}
	return out.Job, nil
}

// ReserveClipUpload presigns a PUT for the given leased job. segmentStartMs is 0
// for an ordinary clip job (the intent is keyed by the job alone), or the
// segment's UTC start in Unix millis for a continuous_window job, where one lease
// raises many per-segment intents: the discriminator both forwards the
// per-segment object-key derivation to the server and makes each segment's
// Idempotency-Key distinct so they are not deduped against each other.
func (c *Client) ReserveClipUpload(ctx context.Context, jobID int64, mimeType string, segmentStartMs int64) (ClipUploadIntent, error) {
	payload := map[string]any{"job_id": jobID, "mime_type": strings.TrimSpace(mimeType)}
	idemKey := buildIdempotencyKey("recording-clip", jobID)
	if segmentStartMs > 0 {
		payload["segment_start_ms"] = segmentStartMs
		idemKey = fmt.Sprintf("recording-seg-%d-%d", jobID, segmentStartMs)
	}
	headers := map[string]string{"Idempotency-Key": idemKey}
	var out ClipUploadIntent
	if err := c.postJSONWithHeaders(ctx, "/api/v1/recording/upload-intents", payload, headers, &out); err != nil {
		return ClipUploadIntent{}, err
	}
	return out, nil
}

// UploadFile streams a local file to a presigned PUT URL with an explicit
// ContentLength and Content-Type (matching the captureapi upload shape).
func (c *Client) UploadFile(ctx context.Context, uploadURL, path, mimeType string) error {
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

// IngestClip records the uploaded clip and returns the new clip id.
func (c *Client) IngestClip(ctx context.Context, req IngestClipRequest) (int64, error) {
	payload := map[string]any{
		"intent_id":     strings.TrimSpace(req.IntentID),
		"job_id":        req.JobID,
		"size_bytes":    req.SizeBytes,
		"etag":          strings.TrimSpace(req.ETag),
		"sha256":        strings.TrimSpace(req.SHA256),
		"duration_ms":   req.DurationMs,
		"video_codec":   strings.TrimSpace(req.VideoCodec),
		"audio_codec":   strings.TrimSpace(req.AudioCodec),
		"audio_present": req.AudioPresent,
		"actual_fps":    req.ActualFPS,
		"container":     strings.TrimSpace(req.Container),
		"resolved_url":  strings.TrimSpace(req.ResolvedURL),
		"clip_start_at": req.ClipStartAt.UTC().Format(time.RFC3339Nano),
		"clip_end_at":   req.ClipEndAt.UTC().Format(time.RFC3339Nano),
	}
	var out struct {
		ClipID int64 `json:"clip_id"`
	}
	if err := c.postJSON(ctx, "/api/v1/recording/clips/ingest", payload, &out); err != nil {
		return 0, err
	}
	return out.ClipID, nil
}

// HeartbeatRecordingJob extends the lease. It returns cancel=true when the
// server signals (409) that the job was canceled or is no longer owned.
func (c *Client) HeartbeatRecordingJob(ctx context.Context, jobID int64) (cancel bool, err error) {
	path := fmt.Sprintf("/api/v1/recording/jobs/%d/heartbeat", jobID)
	status, body, err := c.postRaw(ctx, path, map[string]any{})
	if err != nil {
		return false, err
	}
	if status == http.StatusConflict {
		return true, nil
	}
	if status < 200 || status >= 300 {
		return false, fmt.Errorf("heartbeat status=%d body=%s", status, strings.TrimSpace(string(body)))
	}
	return false, nil
}

// CompleteRecordingJob marks the job done (no reschedule).
func (c *Client) CompleteRecordingJob(ctx context.Context, jobID int64) error {
	return c.postJSON(ctx, fmt.Sprintf("/api/v1/recording/jobs/%d/complete", jobID), map[string]any{}, nil)
}

// FailRecordingJob requeues or fails the job and records the error.
func (c *Client) FailRecordingJob(ctx context.Context, jobID int64, errText string) error {
	return c.postJSON(ctx, fmt.Sprintf("/api/v1/recording/jobs/%d/fail", jobID), map[string]any{"error_text": strings.TrimSpace(errText)}, nil)
}

// TouchDroplet records droplet liveness independent of any held job by touching
// recorder_droplets.last_seen_at. The worker calls this on an independent ticker
// so an idle managed droplet (no leased job, so no per-job heartbeat) is still
// seen as worker-alive by the autoscaler, which gates promotion-to-active and
// failed-node detection on last_seen_at rather than on DO power-on. For a manual
// node with no managed droplet row the server update is a harmless no-op.
func (c *Client) TouchDroplet(ctx context.Context) error {
	return c.postJSON(ctx, "/api/v1/recording/droplets/heartbeat", map[string]any{}, nil)
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
	req.Header.Set("Authorization", "Bearer "+c.nodeToken)
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
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil && err != io.EOF {
		return fmt.Errorf("decode %s response: %w", path, err)
	}
	return nil
}

// postRaw posts and returns the raw status + body so callers can branch on a
// non-2xx (e.g. the 409 cancel signal) without treating it as a hard error.
func (c *Client) postRaw(ctx context.Context, path string, payload any) (int, []byte, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return 0, nil, fmt.Errorf("marshal request payload: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(b))
	if err != nil {
		return 0, nil, fmt.Errorf("build request %s: %w", path, err)
	}
	req.Header.Set("Authorization", "Bearer "+c.nodeToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := c.httpc.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("request %s failed: %w", path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 16*1024))
	return resp.StatusCode, body, nil
}

func buildIdempotencyKey(prefix string, jobID int64) string {
	p := strings.TrimSpace(prefix)
	if p == "" {
		p = "recording-clip"
	}
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return fmt.Sprintf("%s-%d-%d", p, jobID, time.Now().UTC().UnixNano())
	}
	return fmt.Sprintf("%s-%d-%x", p, jobID, buf[:])
}
