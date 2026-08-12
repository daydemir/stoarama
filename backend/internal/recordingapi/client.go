// Package recordingapi is the recorder worker's HTTP client for the six
// recording endpoints. It authenticates with a per-droplet local_recorder node
// token (Bearer), never the shared service token. It is modeled on
// internal/captureapi but is independent of it.
package recordingapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/apihttp"
	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/survey"
)

// UploadTimeout bounds the complete upload of one finalized recording segment.
const UploadTimeout = 5 * time.Minute

const (
	leaseTokenHeader          = "X-Stoarama-Recording-Lease-Token"
	leaseTokenSupportedHeader = "X-Stoarama-Recording-Lease-Token-Supported"
)

// ErrorCode identifies a stable recording API failure independent of its message.
type ErrorCode string

const (
	ErrorCodeClipAlreadyIngested     ErrorCode = "recording_clip_already_ingested"
	ErrorCodeUploadIntentUnavailable ErrorCode = "recording_upload_intent_unavailable"
)

// ErrorCodeFrom returns a structured recording API error code, when present.
func ErrorCodeFrom(err error) ErrorCode {
	var statusErr *apihttp.StatusError
	if !errors.As(err, &statusErr) {
		return ""
	}
	var response struct {
		Code ErrorCode `json:"code"`
	}
	if json.Unmarshal([]byte(statusErr.Body), &response) != nil {
		return ""
	}
	return response.Code
}

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
	uploadHTTP.Timeout = UploadTimeout
	uploads, err := apihttp.New(baseURL, cfg.NodeToken, &uploadHTTP, UploadTimeout)
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
	LeaseToken           string    `json:"lease_token,omitempty"`
	// TargetFPS may be populated by an older API response. Workers must ignore it:
	// recording capture always preserves the source without video re-encoding.
	TargetFPS *int `json:"target_fps"`
	// Kind is 'clip' (default, per-cron-fire) or 'continuous_window' (one window-
	// long lease driving back-to-back segment capture). WindowEndAt is the
	// continuous window's close instant (zero/nil for a clip job).
	Kind        string     `json:"kind"`
	WindowEndAt *time.Time `json:"window_end_at"`
}

// RecordingCanarySpec is the canonical, account-scoped source returned to an
// authenticated relay for a short local canary. Fetching it never leases or
// changes production recording state.
type RecordingCanarySpec struct {
	ReservationID string    `json:"reservation_id"`
	RecordingID   int64     `json:"recording_id"`
	NodeID        int64     `json:"node_id"`
	StreamID      int64     `json:"stream_id"`
	Provider      string    `json:"provider"`
	SourceURL     string    `json:"source_url"`
	SourcePageURL string    `json:"source_page_url,omitempty"`
	SafeUntil     time.Time `json:"safe_until"`
}

type SurrenderReason string

const (
	SurrenderNoProgress      SurrenderReason = "no_progress"
	SurrenderDiskPressure    SurrenderReason = "disk_pressure"
	SurrenderSelfUpdate      SurrenderReason = "self_update"
	SurrenderOperatorHandoff SurrenderReason = "operator_handoff"
)

// ClipUploadIntent is a presigned PUT against the user's bucket.
type ClipUploadIntent struct {
	IntentID        string    `json:"intent_id"`
	UploadURL       string    `json:"upload_url"`
	ObjectKey       string    `json:"object_key"`
	Bucket          string    `json:"bucket"`
	Endpoint        string    `json:"endpoint"`
	ContentType     string    `json:"content_type"`
	MaxSizeBytes    int64     `json:"max_size_bytes"`
	ExpiresAt       time.Time `json:"expires_at"`
	AlreadyIngested bool      `json:"already_ingested"`
}

// IngestClipRequest carries the captured clip's metadata to the ingest endpoint.
type IngestClipRequest struct {
	IntentID        string
	JobID           int64
	SizeBytes       int64
	ETag            string
	SHA256          string
	DurationMs      int64
	VideoCodec      string
	AudioCodec      string
	AudioPresent    bool
	ActualFPS       *float64
	VideoWidth      int
	VideoHeight     int
	Container       string
	ResolvedURL     string
	ClipStartAt     time.Time
	ClipEndAt       time.Time
	LeaseToken      string
	CaptureSequence int64
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
	if err := c.postJSONWithHeaders(ctx, "/api/v1/recording/jobs/lease", map[string]any{}, map[string]string{
		leaseTokenSupportedHeader: "true",
	}, &out); err != nil {
		return nil, err
	}
	return out.Job, nil
}

// ReserveClipUpload presigns a PUT for the given leased job. segmentStartMs is 0
// for an ordinary clip job (the intent is keyed by the job alone), or the
// segment's UTC start in Unix millis for a continuous_window job, where one lease
// raises many per-segment intents. The discriminator forwards the per-segment
// object-key derivation to the server and keeps the request compatible with an
// older API during a rollback.
func (c *Client) ReserveClipUpload(ctx context.Context, jobID int64, leaseToken, mimeType, sha256 string, segmentStartMs int64) (ClipUploadIntent, error) {
	payload := map[string]any{"job_id": jobID, "mime_type": strings.TrimSpace(mimeType)}
	// Only generation-aware servers understand the hash preflight field. If a
	// freshly rolled worker briefly reaches the previous API during deployment,
	// its lease response has no token and the request remains byte-compatible.
	if sha256 = strings.TrimSpace(sha256); strings.TrimSpace(leaseToken) != "" && sha256 != "" {
		payload["sha256"] = sha256
	}
	idemKey := buildIdempotencyKey("recording-clip", jobID)
	if segmentStartMs > 0 {
		payload["segment_start_ms"] = segmentStartMs
		idemKey = fmt.Sprintf("recording-seg-%d-%d", jobID, segmentStartMs)
	}
	headers := leaseTokenHeaders(leaseToken)
	headers["Idempotency-Key"] = idemKey
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
		"video_width":   req.VideoWidth,
		"video_height":  req.VideoHeight,
		"container":     strings.TrimSpace(req.Container),
		"resolved_url":  strings.TrimSpace(req.ResolvedURL),
		"clip_start_at": req.ClipStartAt.UTC().Format(time.RFC3339Nano),
		"clip_end_at":   req.ClipEndAt.UTC().Format(time.RFC3339Nano),
	}
	if strings.TrimSpace(req.LeaseToken) != "" && req.CaptureSequence > 0 {
		payload["capture_sequence"] = req.CaptureSequence
	}
	var out struct {
		ClipID int64 `json:"clip_id"`
	}
	if err := c.postJSONWithHeaders(ctx, "/api/v1/recording/clips/ingest", payload, leaseTokenHeaders(req.LeaseToken), &out); err != nil {
		return 0, err
	}
	return out.ClipID, nil
}

// HeartbeatRecordingJob extends the lease. It returns cancel=true when the
// server signals (409) that the job was canceled or is no longer owned.
func (c *Client) HeartbeatRecordingJob(ctx context.Context, jobID int64, leaseToken string) (bool, time.Time, error) {
	cancel, expires, _, err := c.HeartbeatRecordingJobWithControl(ctx, jobID, leaseToken)
	return cancel, expires, err
}

func (c *Client) HeartbeatRecordingJobWithControl(ctx context.Context, jobID int64, leaseToken string) (cancel bool, leaseExpiresAt time.Time, gracefulHandoffRequestID string, err error) {
	path := fmt.Sprintf("/api/v1/recording/jobs/%d/heartbeat", jobID)
	status, body, err := c.postRawWithHeaders(ctx, path, map[string]any{}, leaseTokenHeaders(leaseToken))
	if err != nil {
		return false, time.Time{}, "", err
	}
	if status == http.StatusConflict {
		return true, time.Time{}, "", nil
	}
	if status < 200 || status >= 300 {
		return false, time.Time{}, "", fmt.Errorf("heartbeat status=%d body=%s", status, strings.TrimSpace(string(body)))
	}
	var out struct {
		LeaseExpiresAt           time.Time `json:"lease_expires_at"`
		GracefulHandoffRequestID string    `json:"graceful_handoff_request_id"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return false, time.Time{}, "", fmt.Errorf("decode heartbeat: %w", err)
	}
	if out.LeaseExpiresAt.IsZero() {
		return false, time.Time{}, "", fmt.Errorf("heartbeat response missing lease_expires_at")
	}
	return false, out.LeaseExpiresAt, strings.TrimSpace(out.GracefulHandoffRequestID), nil
}

// CompleteRecordingJob marks the job done (no reschedule).
func (c *Client) CompleteRecordingJob(ctx context.Context, jobID int64, leaseToken string) error {
	return c.postJSONWithHeaders(ctx, fmt.Sprintf("/api/v1/recording/jobs/%d/complete", jobID), map[string]any{}, leaseTokenHeaders(leaseToken), nil)
}

// FailRecordingJob requeues or fails the job and records the error.
func (c *Client) FailRecordingJob(ctx context.Context, jobID int64, leaseToken, errText string) error {
	return c.postJSONWithHeaders(ctx, fmt.Sprintf("/api/v1/recording/jobs/%d/fail", jobID), map[string]any{"error_text": strings.TrimSpace(errText)}, leaseTokenHeaders(leaseToken), nil)
}

func (c *Client) SurrenderRecordingJob(ctx context.Context, jobID int64, leaseToken string, reason SurrenderReason, errorText string, gracefulHandoffRequestID ...string) error {
	payload := map[string]any{
		"reason": reason, "error_text": strings.TrimSpace(errorText),
	}
	if len(gracefulHandoffRequestID) > 0 && strings.TrimSpace(gracefulHandoffRequestID[0]) != "" {
		payload["graceful_handoff_request_id"] = strings.TrimSpace(gracefulHandoffRequestID[0])
	}
	return c.postJSONWithHeaders(ctx, fmt.Sprintf("/api/v1/recording/jobs/%d/surrender", jobID), map[string]any{
		"reason": payload["reason"], "error_text": payload["error_text"], "graceful_handoff_request_id": payload["graceful_handoff_request_id"],
	}, leaseTokenHeaders(leaseToken), nil)
}

func leaseTokenHeaders(token string) map[string]string {
	headers := map[string]string{}
	if token = strings.TrimSpace(token); token != "" {
		headers[leaseTokenHeader] = token
	}
	return headers
}

// TouchDroplet records droplet liveness independent of any held job by touching
// recorder_droplets.last_seen_at. The worker calls this on an independent ticker
// so an idle managed droplet (no leased job, so no per-job heartbeat) is still
// seen as worker-alive by the autoscaler, which gates promotion-to-active and
// failed-node detection on last_seen_at rather than on DO power-on. For a manual
// node with no managed droplet row the server update is a harmless no-op.
func (c *Client) TouchDroplet(ctx context.Context, buildSHA string) error {
	return c.postJSON(ctx, "/api/v1/recording/droplets/heartbeat", map[string]any{
		"build_sha": strings.ToLower(strings.TrimSpace(buildSHA)),
	}, nil)
}

// NodeHeartbeat refreshes this node's last_heartbeat_at and merges the reported
// capability keys into nodes.capabilities_jsonb via POST /api/v1/node/heartbeat.
// The relay binary calls this on its own 30s ticker; cloud droplet workers use
// TouchDroplet instead and never call this.
func (c *Client) NodeHeartbeat(ctx context.Context, capabilities map[string]any) error {
	status, body, err := c.postRaw(ctx, "/api/v1/node/heartbeat", map[string]any{"capabilities_json": capabilities})
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return &apihttp.StatusError{Label: "node heartbeat", Code: status, Body: strings.TrimSpace(string(body))}
	}
	return nil
}

// StartRecordingCanary creates a short metadata-only reservation honored by
// the production lease query, then returns the canonical source.
func (c *Client) StartRecordingCanary(ctx context.Context, recordingID int64) (RecordingCanarySpec, error) {
	var out RecordingCanarySpec
	if recordingID <= 0 {
		return out, fmt.Errorf("recording id must be positive")
	}
	if err := c.postJSON(ctx, fmt.Sprintf("/api/v1/node/recordings/%d/canary-reservations", recordingID), map[string]any{}, &out); err != nil {
		return RecordingCanarySpec{}, err
	}
	return out, nil
}

// CheckRecordingCanary confirms that this relay still owns a live reservation.
func (c *Client) CheckRecordingCanary(ctx context.Context, recordingID int64, reservationID string) (RecordingCanarySpec, error) {
	var out RecordingCanarySpec
	if recordingID <= 0 || strings.TrimSpace(reservationID) == "" {
		return out, fmt.Errorf("recording id and reservation id are required")
	}
	reservation := url.PathEscape(strings.TrimSpace(reservationID))
	if err := c.postJSON(ctx, fmt.Sprintf("/api/v1/node/recordings/%d/canary-reservations/%s/check", recordingID, reservation), map[string]any{}, &out); err != nil {
		return RecordingCanarySpec{}, err
	}
	return out, nil
}

// FinishRecordingCanary releases this relay's reservation early.
func (c *Client) FinishRecordingCanary(ctx context.Context, recordingID int64, reservationID string) error {
	if recordingID <= 0 || strings.TrimSpace(reservationID) == "" {
		return fmt.Errorf("recording id and reservation id are required")
	}
	return c.postJSON(ctx, fmt.Sprintf("/api/v1/node/recordings/%d/canary-reservations/%s/finish", recordingID, url.PathEscape(strings.TrimSpace(reservationID))), map[string]any{}, nil)
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

func (c *Client) postRawWithHeaders(ctx context.Context, path string, payload any, headers map[string]string) (int, []byte, error) {
	return c.api.PostRawWithHeaders(ctx, path, payload, headers)
}

func buildIdempotencyKey(prefix string, jobID int64) string {
	if strings.TrimSpace(prefix) == "" {
		prefix = "recording-clip"
	}
	return apihttp.IdempotencyKey(prefix, jobID)
}
