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
	Kind                       string     `json:"kind"`
	WindowEndAt                *time.Time `json:"window_end_at"`
	TimestampContractSupported bool       `json:"timestamp_contract_supported"`
	SurrenderTransportVersion  int        `json:"surrender_transport_version,omitempty"`
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

// RecordingCanaryResult is the bounded native-media proof produced by the
// reservation owner. The server accepts it only while that exact reservation
// is live and atomically records the matching pre-open PASS.
type RecordingCanaryResult struct {
	DurationMS     int64  `json:"duration_ms"`
	SizeBytes      int64  `json:"size_bytes"`
	SHA256         string `json:"sha256"`
	VideoCodec     string `json:"video_codec"`
	ProbeOK        bool   `json:"probe_ok"`
	DecodeOK       bool   `json:"decode_ok"`
	NativeCopy     bool   `json:"native_copy"`
	Uploaded       bool   `json:"uploaded"`
	RelayVersion   string `json:"relay_version"`
	SourceRevision string `json:"source_revision"`
}

type SurrenderReason string

const (
	SurrenderNoProgress   SurrenderReason = "no_progress"
	SurrenderDiskPressure SurrenderReason = "disk_pressure"
	SurrenderSelfUpdate   SurrenderReason = "self_update"
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

// CaptureProducer is the server-sealed identity of one ffmpeg producer. It is
// reserved before ffmpeg can create bytes; every artifact produced by that
// invocation is subsequently sealed against ProducerID.
type CaptureProducer struct {
	ProducerID     string `json:"producer_id"`
	CaptureOrdinal int64  `json:"capture_ordinal"`
}

type CaptureProducerStatus struct {
	ProducerID   string `json:"producer_id"`
	Found        bool   `json:"found"`
	CurrentLease bool   `json:"current_lease"`
	IntentCount  int    `json:"intent_count"`
	Result       string `json:"result,omitempty"`
}

type ClipIngestResult struct {
	ClipID         int64  `json:"clip_id"`
	HeadVersion    int64  `json:"head_version,omitempty"`
	UploadIntentID string `json:"head_upload_intent_id,omitempty"`
}

type SurrenderRequest struct {
	AttemptID            string
	Reason               SurrenderReason
	ErrorText            string
	ExpectedHeadVersion  int64
	ExpectedUploadIntent string
	ExpectedClipID       int64
	SpoolCount           int
	SpoolBytes           int64
	InFlightCount        int
}

type SurrenderResult struct {
	Result                string    `json:"result"`
	HandoffUntil          time.Time `json:"handoff_until,omitempty"`
	NextRetryAt           time.Time `json:"next_retry_at,omitempty"`
	HadClips              bool      `json:"had_clips,omitempty"`
	AlternateAvailable    bool      `json:"alternate_available,omitempty"`
	CurrentHeadVersion    int64     `json:"current_head_version"`
	CurrentUploadIntentID string    `json:"current_upload_intent_id,omitempty"`
	CurrentClipID         int64     `json:"current_clip_id,omitempty"`
}

type SurrenderTransportObservation struct {
	ID            string    `json:"id"`
	LeaseToken    string    `json:"lease_token"`
	AttemptID     string    `json:"attempt_id,omitempty"`
	Type          string    `json:"type"`
	ErrorClass    string    `json:"error_class,omitempty"`
	ObservedAt    time.Time `json:"observed_at"`
	RequestSHA256 string    `json:"request_sha256"`
}

type RecoveryArtifact struct {
	IntentID        string `json:"intent_id"`
	CaptureSequence int64  `json:"capture_sequence"`
	SegmentStartMs  int64  `json:"segment_start_ms"`
	SizeBytes       int64  `json:"size_bytes"`
	SHA256          string `json:"sha256"`
	Result          string `json:"result,omitempty"`
}

type RecoveryStatus struct {
	IntentID       string             `json:"intent_id"`
	ProducerID     string             `json:"producer_id"`
	JobID          int64              `json:"job_id"`
	LeaseToken     string             `json:"lease_token"`
	ExpiresAt      time.Time          `json:"expires_at"`
	Authority      string             `json:"authority"`
	ProducerResult string             `json:"producer_result,omitempty"`
	Artifacts      []RecoveryArtifact `json:"artifacts"`
}

type CaptureArtifactReservationInput struct {
	IntentID             string `json:"intent_id"`
	RecoverySecretSHA256 string `json:"recovery_secret_sha256"`
	CaptureSequence      int64  `json:"capture_sequence"`
}

type CaptureSetPlan struct {
	PlanID                  string    `json:"plan_id"`
	SetID                   string    `json:"set_id"`
	ProducerID              string    `json:"producer_id"`
	CaptureOrdinal          int64     `json:"capture_ordinal"`
	FirstCaptureSequence    int64     `json:"first_capture_sequence"`
	AccountID               int64     `json:"account_id"`
	RecordingID             int64     `json:"recording_id"`
	JobID                   int64     `json:"job_id"`
	LeaseToken              string    `json:"lease_token"`
	OriginClaimGeneration   int64     `json:"origin_claim_generation"`
	SnapshotGeneration      int64     `json:"snapshot_generation"`
	SourceSnapshotSHA256    string    `json:"source_snapshot_sha256"`
	DestinationNamingSHA256 string    `json:"destination_naming_sha256"`
	PlanAt                  time.Time `json:"plan_at"`
	WindowEndAt             time.Time `json:"window_end_at"`
	DurationMicroseconds    int64     `json:"duration_microseconds"`
	ClipDurationSeconds     int       `json:"clip_duration_seconds"`
	ArtifactCount           int       `json:"artifact_count"`
	SegmentTimesArgument    string    `json:"segment_times_argument"`
	MaxArtifactBytes        int64     `json:"max_artifact_bytes"`
	ExpiresAt               time.Time `json:"expires_at"`
}

func (c *Client) PlanCaptureSet(ctx context.Context, jobID int64, leaseToken, planID, setID, producerID string, captureOrdinal, firstCaptureSequence int64) (CaptureSetPlan, error) {
	var out CaptureSetPlan
	err := c.postJSONWithHeaders(ctx, fmt.Sprintf("/api/v1/recording/jobs/%d/capture-set-plans", jobID), map[string]any{
		"plan_id": planID, "set_id": setID, "producer_id": producerID, "capture_ordinal": captureOrdinal,
		"first_capture_sequence": firstCaptureSequence,
	}, leaseTokenHeaders(leaseToken), &out)
	return out, err
}

func (c *Client) CommitCaptureSet(ctx context.Context, jobID int64, leaseToken, planID, merkleRootSHA256 string) error {
	return c.postJSONWithHeaders(ctx, fmt.Sprintf("/api/v1/recording/jobs/%d/capture-set-plans/%s/commit", jobID, url.PathEscape(planID)), map[string]any{
		"merkle_root_sha256": strings.TrimSpace(merkleRootSHA256),
	}, leaseTokenHeaders(leaseToken), nil)
}

type CaptureArtifactMaterialization struct {
	ArtifactID           string   `json:"artifact_id"`
	CaptureSequence      int64    `json:"capture_sequence"`
	RecoverySecretSHA256 string   `json:"recovery_secret_sha256"`
	Proof                []string `json:"proof"`
}

func (c *Client) MaterializeCaptureArtifact(ctx context.Context, jobID int64, leaseToken, setID string, ordinal int, artifact CaptureArtifactMaterialization) error {
	return c.postJSONWithHeaders(ctx, fmt.Sprintf("/api/v1/recording/jobs/%d/capture-sets/%s/artifacts/%d/materialize", jobID, url.PathEscape(setID), ordinal), artifact, leaseTokenHeaders(leaseToken), nil)
}

func (c *Client) FinishCaptureSet(ctx context.Context, jobID int64, leaseToken, setID string) error {
	return c.postJSONWithHeaders(ctx, fmt.Sprintf("/api/v1/recording/jobs/%d/capture-sets/%s/finish", jobID, url.PathEscape(setID)), map[string]any{}, leaseTokenHeaders(leaseToken), nil)
}

// IngestClipRequest carries the captured clip's metadata to the ingest endpoint.
type IngestClipRequest struct {
	IntentID                string
	JobID                   int64
	SizeBytes               int64
	ETag                    string
	SHA256                  string
	DurationMs              int64
	VideoCodec              string
	AudioCodec              string
	AudioPresent            bool
	ActualFPS               *float64
	VideoWidth              int
	VideoHeight             int
	Container               string
	ResolvedURL             string
	ClipStartAt             time.Time
	ClipEndAt               time.Time
	LeaseToken              string
	CaptureSequence         int64
	CaptureAttemptID        string
	TimestampContract       *capture.TimestampContract
	TimestampContractStatus string
	TimestampContractReason string
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

func recoveryHeaders(intentID, secret string) map[string]string {
	return map[string]string{
		"X-Stoarama-Recording-Recovery-Intent": strings.TrimSpace(intentID),
		"X-Stoarama-Recording-Recovery-Secret": strings.TrimSpace(secret),
	}
}

func (c *Client) ReserveCaptureArtifacts(ctx context.Context, jobID int64, leaseToken, producerID string, artifacts []CaptureArtifactReservationInput) error {
	return c.postJSONWithHeaders(ctx, fmt.Sprintf("/api/v1/recording/jobs/%d/capture-producers/%s/artifacts/reserve", jobID, url.PathEscape(producerID)), map[string]any{"artifacts": artifacts}, leaseTokenHeaders(leaseToken), nil)
}

func (c *Client) SealCaptureArtifact(ctx context.Context, jobID int64, leaseToken, intentID, producerID string, captureSequence, segmentStartMs, sizeBytes int64, sha string) (ClipUploadIntent, error) {
	var out ClipUploadIntent
	err := c.postJSONWithHeaders(ctx, fmt.Sprintf("/api/v1/recording/upload-intents/%s/seal", url.PathEscape(intentID)), map[string]any{
		"job_id": jobID, "producer_id": producerID, "capture_sequence": captureSequence,
		"segment_start_ms": segmentStartMs, "size_bytes": sizeBytes, "sha256": sha,
	}, leaseTokenHeaders(leaseToken), &out)
	return out, err
}

func (c *Client) SealCaptureSetArtifact(ctx context.Context, jobID int64, leaseToken, setID string, ordinal int, artifactID, producerID string, captureSequence, segmentStartMicroseconds, sizeBytes int64, sha string) (ClipUploadIntent, error) {
	var out ClipUploadIntent
	err := c.postJSONWithHeaders(ctx, fmt.Sprintf("/api/v1/recording/upload-intents/%s/seal", url.PathEscape(artifactID)), map[string]any{
		"job_id": jobID, "producer_id": producerID, "set_id": setID, "ordinal": ordinal,
		"capture_sequence": captureSequence, "segment_start_microseconds": segmentStartMicroseconds,
		"size_bytes": sizeBytes, "sha256": sha,
	}, leaseTokenHeaders(leaseToken), &out)
	return out, err
}

func (c *Client) SealCaptureArtifactRecovery(ctx context.Context, jobID int64, leaseToken, intentID, recoverySecret, producerID string, captureSequence, segmentStartMs, sizeBytes int64, sha string) (ClipUploadIntent, error) {
	var out ClipUploadIntent
	err := c.postJSONWithHeaders(ctx, fmt.Sprintf("/api/v1/recording/upload-intents/%s/seal", url.PathEscape(intentID)), map[string]any{
		"job_id": jobID, "producer_id": producerID, "capture_sequence": captureSequence,
		"segment_start_ms": segmentStartMs, "size_bytes": sizeBytes, "sha256": sha,
	}, recoveryHeaders(intentID, recoverySecret), &out)
	return out, err
}

func (c *Client) ReserveCaptureProducer(ctx context.Context, jobID int64, leaseToken, producerID string, captureOrdinal int64, sealedIntentLimit int) (CaptureProducer, error) {
	var out CaptureProducer
	err := c.postJSONWithHeaders(ctx, fmt.Sprintf("/api/v1/recording/jobs/%d/capture-producers", jobID), map[string]any{
		"producer_id": producerID, "capture_ordinal": captureOrdinal,
		"sealed_intent_limit": sealedIntentLimit,
	}, leaseTokenHeaders(leaseToken), &out)
	return out, err
}

func (c *Client) FinishCaptureProducer(ctx context.Context, jobID int64, leaseToken, producerID, result, detailClass string) error {
	return c.postJSONWithHeaders(ctx, fmt.Sprintf("/api/v1/recording/jobs/%d/capture-producers/%s/finish", jobID, url.PathEscape(producerID)), map[string]any{
		"result": result, "detail_class": detailClass,
	}, leaseTokenHeaders(leaseToken), nil)
}

func (c *Client) CaptureProducerStatus(ctx context.Context, jobID int64, producerID string) (CaptureProducerStatus, error) {
	var out CaptureProducerStatus
	err := c.postJSON(ctx, fmt.Sprintf("/api/v1/recording/jobs/%d/capture-producers/%s/status", jobID, url.PathEscape(producerID)), map[string]any{}, &out)
	return out, err
}

// UploadFile streams a local file to a presigned PUT URL with an explicit
// ContentLength and Content-Type (matching the captureapi upload shape).
func (c *Client) UploadFile(ctx context.Context, uploadURL, path, mimeType string) error {
	return c.uploads.PutFile(ctx, uploadURL, path, mimeType)
}

// IngestClip records the uploaded clip and returns the new clip id.
func (c *Client) IngestClip(ctx context.Context, req IngestClipRequest) (int64, error) {
	result, err := c.IngestClipWithResult(ctx, req)
	return result.ClipID, err
}

func (c *Client) IngestClipWithResult(ctx context.Context, req IngestClipRequest) (ClipIngestResult, error) {
	return c.ingestClipWithHeaders(ctx, req, nil)
}

func (c *Client) ingestClipWithHeaders(ctx context.Context, req IngestClipRequest, extraHeaders map[string]string) (ClipIngestResult, error) {
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
		if strings.TrimSpace(req.CaptureAttemptID) != "" {
			payload["capture_attempt_id"] = req.CaptureAttemptID
			if req.TimestampContractStatus == capture.TimestampProbeComplete {
				payload["timestamp_contract_version"] = capture.TimestampVersionContinuousSourcePTSV1
			}
			payload["timestamp_contract"] = req.TimestampContract
			payload["timestamp_contract_status"] = req.TimestampContractStatus
			payload["timestamp_contract_reason"] = req.TimestampContractReason
		}
	}
	var out ClipIngestResult
	headers := leaseTokenHeaders(req.LeaseToken)
	for key, value := range extraHeaders {
		headers[key] = value
	}
	if err := c.postJSONWithHeaders(ctx, "/api/v1/recording/clips/ingest", payload, headers, &out); err != nil {
		return ClipIngestResult{}, err
	}
	return out, nil
}

func (c *Client) IngestClipRecovery(ctx context.Context, req IngestClipRequest, intentID, recoverySecret string) (ClipIngestResult, error) {
	return c.ingestClipWithHeaders(ctx, req, recoveryHeaders(intentID, recoverySecret))
}

func (c *Client) RecordingRecoveryStatus(ctx context.Context, intentID, recoverySecret string) (RecoveryStatus, error) {
	var out RecoveryStatus
	err := c.postJSONWithHeaders(ctx, fmt.Sprintf("/api/v1/recording/recovery/intents/%s/status", url.PathEscape(intentID)), map[string]any{}, recoveryHeaders(intentID, recoverySecret), &out)
	return out, err
}

func (c *Client) FinishRecordingRecovery(ctx context.Context, intentID, recoverySecret, result string) error {
	return c.postJSONWithHeaders(ctx, fmt.Sprintf("/api/v1/recording/recovery/intents/%s/finish", url.PathEscape(intentID)), map[string]any{"result": result}, recoveryHeaders(intentID, recoverySecret), nil)
}

// HeartbeatRecordingJob extends the lease. It returns cancel=true when the
// server signals (409) that the job was canceled or is no longer owned.
func (c *Client) HeartbeatRecordingJob(ctx context.Context, jobID int64, leaseToken string) (cancel bool, leaseExpiresAt time.Time, err error) {
	state, err := c.HeartbeatRecordingJobState(ctx, jobID, leaseToken)
	return state.Cancel, state.LeaseExpiresAt, err
}

type RecordingHeartbeatState struct {
	Cancel         bool
	LeaseExpiresAt time.Time
	StopRequired   bool
}

func (c *Client) HeartbeatRecordingJobState(ctx context.Context, jobID int64, leaseToken string) (RecordingHeartbeatState, error) {
	path := fmt.Sprintf("/api/v1/recording/jobs/%d/heartbeat", jobID)
	status, body, err := c.postRawWithHeaders(ctx, path, map[string]any{}, leaseTokenHeaders(leaseToken))
	if err != nil {
		return RecordingHeartbeatState{}, err
	}
	if status == http.StatusConflict {
		return RecordingHeartbeatState{Cancel: true}, nil
	}
	if status < 200 || status >= 300 {
		return RecordingHeartbeatState{}, fmt.Errorf("heartbeat status=%d body=%s", status, strings.TrimSpace(string(body)))
	}
	var out struct {
		LeaseExpiresAt time.Time `json:"lease_expires_at"`
		StopRequired   bool      `json:"stop_required"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return RecordingHeartbeatState{}, fmt.Errorf("decode heartbeat: %w", err)
	}
	if out.LeaseExpiresAt.IsZero() {
		return RecordingHeartbeatState{}, fmt.Errorf("heartbeat response missing lease_expires_at")
	}
	return RecordingHeartbeatState{LeaseExpiresAt: out.LeaseExpiresAt, StopRequired: out.StopRequired}, nil
}

// CompleteRecordingJob marks the job done (no reschedule).
func (c *Client) CompleteRecordingJob(ctx context.Context, jobID int64, leaseToken string) error {
	return c.postJSONWithHeaders(ctx, fmt.Sprintf("/api/v1/recording/jobs/%d/complete", jobID), map[string]any{}, leaseTokenHeaders(leaseToken), nil)
}

// FailRecordingJob requeues or fails the job and records the error.
func (c *Client) FailRecordingJob(ctx context.Context, jobID int64, leaseToken, errText string) error {
	return c.postJSONWithHeaders(ctx, fmt.Sprintf("/api/v1/recording/jobs/%d/fail", jobID), map[string]any{"error_text": strings.TrimSpace(errText)}, leaseTokenHeaders(leaseToken), nil)
}

func (c *Client) SurrenderRecordingJob(ctx context.Context, jobID int64, leaseToken string, reason SurrenderReason, errorText string) error {
	return c.postJSONWithHeaders(ctx, fmt.Sprintf("/api/v1/recording/jobs/%d/surrender", jobID), map[string]any{
		"reason":     reason,
		"error_text": strings.TrimSpace(errorText),
	}, leaseTokenHeaders(leaseToken), nil)
}

func (c *Client) SurrenderRecordingJobV1(ctx context.Context, jobID int64, leaseToken string, req SurrenderRequest) (SurrenderResult, error) {
	var out SurrenderResult
	err := c.postJSONWithHeaders(ctx, fmt.Sprintf("/api/v1/recording/jobs/%d/surrender", jobID), map[string]any{
		"transport_version": 1, "attempt_id": strings.TrimSpace(req.AttemptID),
		"reason": req.Reason, "error_text": strings.TrimSpace(req.ErrorText),
		"expected_head_version":     req.ExpectedHeadVersion,
		"expected_upload_intent_id": strings.TrimSpace(req.ExpectedUploadIntent),
		"expected_clip_id":          req.ExpectedClipID,
		"spool_count":               req.SpoolCount, "spool_bytes": req.SpoolBytes, "in_flight_count": req.InFlightCount,
	}, leaseTokenHeaders(leaseToken), &out)
	return out, err
}

func (c *Client) RecordSurrenderTransportObservations(ctx context.Context, jobID int64, observations []SurrenderTransportObservation) error {
	return c.postJSON(ctx, fmt.Sprintf("/api/v1/recording/jobs/%d/surrender/observations", jobID), map[string]any{"observations": observations}, nil)
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

// CompleteRecordingCanary atomically persists a reservation-backed pre-open
// PASS. Finish releases the reservation on both success and failure.
func (c *Client) CompleteRecordingCanary(ctx context.Context, recordingID int64, reservationID string, result RecordingCanaryResult) error {
	if recordingID <= 0 || strings.TrimSpace(reservationID) == "" {
		return fmt.Errorf("recording id and reservation id are required")
	}
	return c.postJSON(ctx, fmt.Sprintf("/api/v1/node/recordings/%d/canary-reservations/%s/complete", recordingID, url.PathEscape(strings.TrimSpace(reservationID))), result, nil)
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
