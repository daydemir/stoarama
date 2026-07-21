// Package recordingapi is the recorder worker's HTTP client for the six
// recording endpoints. It authenticates with a per-droplet local_recorder node
// token (Bearer), never the shared service token. It is modeled on
// internal/captureapi but is independent of it.
package recordingapi

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/apihttp"
	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/survey"
)

const uploadTimeout = 5 * time.Minute

type ClientConfig struct {
	BaseURL    string
	NodeToken  string
	HTTPClient *http.Client
}

type Client struct {
	baseURL   string
	nodeToken string
	httpc     *http.Client
	api       *apihttp.Client
	uploads   *apihttp.Client
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
	api, err := apihttp.New(baseURL, cfg.NodeToken, httpc, 60*time.Second)
	if err != nil {
		return nil, err
	}
	uploadHTTP := *httpc
	uploadHTTP.Timeout = uploadTimeout
	uploads, err := apihttp.New(baseURL, cfg.NodeToken, &uploadHTTP, uploadTimeout)
	if err != nil {
		return nil, err
	}
	return &Client{
		baseURL:   baseURL,
		nodeToken: strings.TrimSpace(cfg.NodeToken),
		httpc:     httpc,
		api:       api,
		uploads:   uploads,
	}, nil
}

// RecordingJob is a leased clip-capture unit.
type RecordingJob struct {
	JobID                int64     `json:"job_id"`
	RecordingID          int64     `json:"recording_id"`
	SourceURL            string    `json:"source_url"`
	StreamID             int64     `json:"stream_id,omitempty"`
	StreamProvider       string    `json:"stream_provider,omitempty"`
	SourcePageURL        string    `json:"source_page_url,omitempty"`
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

type SurveyLease struct {
	Targets []survey.Target `json:"targets"`
	Day     string          `json:"day"`
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
	return c.uploads.PutFile(ctx, uploadURL, path, mimeType)
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

// NodeHeartbeat refreshes this node's last_heartbeat_at and merges the reported
// capability keys into nodes.capabilities_jsonb via POST /api/v1/node/heartbeat.
// The relay binary calls this on its own 30s ticker; cloud droplet workers use
// TouchDroplet instead and never call this.
func (c *Client) NodeHeartbeat(ctx context.Context, capabilities map[string]any) error {
	return c.postJSON(ctx, "/api/v1/node/heartbeat", map[string]any{"capabilities_json": capabilities}, nil)
}

func (c *Client) LeaseSurveyTargets(ctx context.Context, limit int) (SurveyLease, error) {
	var out SurveyLease
	if err := c.postJSON(ctx, "/api/v1/node/survey/lease", map[string]any{"limit": limit}, &out); err != nil {
		return SurveyLease{}, err
	}
	return out, nil
}

func (c *Client) CompleteSurveyTarget(ctx context.Context, target survey.Target, day string, frame capture.Frame, det *survey.DetectionResult) error {
	payload := map[string]any{
		"stream_id":    target.ID,
		"day":          day,
		"frame_base64": base64.StdEncoding.EncodeToString(frame.Bytes),
		"mime_type":    frame.MIMEType,
		"width":        frame.Width,
		"height":       frame.Height,
		"sha256":       frame.SHA256,
		"size_bytes":   frame.SizeBytes,
	}
	if det != nil {
		payload["detection"] = map[string]any{
			"pipeline_version": det.PipelineVersion,
			"conf_threshold":   det.ConfThreshold,
			"imgsz":            det.Imgsz,
			"detect_ms":        det.DetectMs,
			"person":           det.Counts.Person,
			"bicycle":          det.Counts.Bicycle,
			"car":              det.Counts.Car,
			"motorcycle":       det.Counts.Motorcycle,
			"bus":              det.Counts.Bus,
			"truck":            det.Counts.Truck,
		}
	}
	return c.postJSON(ctx, "/api/v1/node/survey/complete", payload, nil)
}

func (c *Client) FailSurveyTarget(ctx context.Context, target survey.Target, captureErr error) error {
	msg := ""
	if captureErr != nil {
		msg = captureErr.Error()
	}
	return c.postJSON(ctx, "/api/v1/node/survey/fail", map[string]any{
		"stream_id": target.ID,
		"error":     msg,
	}, nil)
}

func (c *Client) postJSON(ctx context.Context, path string, payload any, out any) error {
	return c.api.PostJSON(ctx, path, payload, out)
}

func (c *Client) postJSONWithHeaders(ctx context.Context, path string, payload any, headers map[string]string, out any) error {
	return c.api.PostJSONWithHeaders(ctx, path, payload, headers, out)
}

// postRaw posts and returns the raw status + body so callers can branch on a
// non-2xx (e.g. the 409 cancel signal) without treating it as a hard error.
func (c *Client) postRaw(ctx context.Context, path string, payload any) (int, []byte, error) {
	return c.api.PostRaw(ctx, path, payload)
}

func buildIdempotencyKey(prefix string, jobID int64) string {
	if strings.TrimSpace(prefix) == "" {
		prefix = "recording-clip"
	}
	return apihttp.IdempotencyKey(prefix, jobID)
}
