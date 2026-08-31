package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
)

const (
	joinedAPITimeout       = 55 * time.Second
	joinedWorkerIdlePoll   = 2 * time.Second
	// A strict 60-source hour can legitimately spend more than two hours in
	// deterministic media isolation. Each media subprocess keeps its narrower
	// deadline; this is only the outer bound for one renewable, fenced task.
	joinedWorkerTaskLimit  = 4 * time.Hour
	joinedAPIResponseLimit = 1 << 20
	// Sealed-hour claims repeat canonical source, plan, manifest, and proof data.
	// Thirty-part hours can legitimately exceed the ordinary API response cap.
	joinedLargeHourAPIResponseLimit = 2 << 20
	joinedAPIErrorLimit             = 4 << 10
)

type joinedAPIClient struct {
	baseURL        string
	bootstrapToken string
	httpClient     *http.Client
}

type joinedAPIResponseError struct {
	path    string
	status  int
	message string
}

type joinedAPITransportError struct{ cause error }

func (*joinedAPITransportError) Error() string   { return "joined API transport failed" }
func (e *joinedAPITransportError) Unwrap() error { return e.cause }

func (e *joinedAPIResponseError) Error() string {
	if strings.TrimSpace(e.message) == "" {
		return fmt.Sprintf("joined API %s returned status %d", e.path, e.status)
	}
	return fmt.Sprintf("joined API %s returned status %d error=%q", e.path, e.status, e.message)
}

type remoteJoinedOperatorService struct {
	cfg              config.Config
	api              *joinedAPIClient
	operatorToken    string
	capabilityClient joinedrecording.CapabilityHTTPClient
	idlePoll         time.Duration
	processClaim     func(context.Context, joinedrecording.PublicationClaimResponse, string) error
	processPreflight func(context.Context, joinedrecording.PreflightHourClaim, string) error
}

type joinedOperationTracker struct {
	mu      sync.RWMutex
	leaseID string
	token   string
	expires time.Time
}

func newJoinedOperationTracker(leaseID, token string, expires time.Time) *joinedOperationTracker {
	return &joinedOperationTracker{leaseID: leaseID, token: token, expires: expires}
}

func (t *joinedOperationTracker) replace(leaseID, token string, expires time.Time) {
	t.mu.Lock()
	t.leaseID, t.token, t.expires = leaseID, token, expires
	t.mu.Unlock()
}

func (t *joinedOperationTracker) accept(credentials joinedrecording.OperationCredentials) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if credentials.LeaseID == t.leaseID && !credentials.ExpiresAt.Before(t.expires) {
		t.token, t.expires = credentials.OperationToken, credentials.ExpiresAt
	}
}

func (t *joinedOperationTracker) get() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.token
}

type joinedOperationTrackerContextKey struct{}

type joinedAdminBatchStatusStreamDay struct {
	RecordingID       int64  `json:"recording_id"`
	LocalDate         string `json:"local_date"`
	State             string `json:"state"`
	SourceCount       int    `json:"source_count"`
	SourceBytes       int64  `json:"source_bytes"`
	SealRequestSHA256 string `json:"seal_request_sha256"`
}

type joinedAdminBatchStatus struct {
	ProtocolVersion         int                               `json:"protocol_version"`
	BatchID                 string                            `json:"batch_id"`
	State                   string                            `json:"state"`
	FrozenDenominatorSHA256 string                            `json:"frozen_denominator_sha256"`
	FreezeStartedAt         *time.Time                        `json:"freeze_started_at,omitempty"`
	FrozenAt                *time.Time                        `json:"frozen_at,omitempty"`
	ExpectedStreamDays      int                               `json:"expected_stream_days"`
	ExpectedScheduledHours  int                               `json:"expected_scheduled_hours"`
	StreamDays              []joinedAdminBatchStatusStreamDay `json:"stream_days"`
}

type joinedWorkerStatus struct {
	ProtocolVersion int              `json:"protocol_version"`
	Enabled         bool             `json:"enabled"`
	BatchID         string           `json:"batch_id"`
	WorkScope       string           `json:"work_scope"`
	CanaryHourIDs   []string         `json:"canary_hour_ids"`
	Hours           map[string]int64 `json:"hours"`
}

type joinedDeliveryStatus struct {
	BatchID                  string                  `json:"batch_id"`
	ArtifactID               int64                   `json:"artifact_id"`
	ArtifactKind             string                  `json:"artifact_kind"`
	HourID                   string                  `json:"hour_id"`
	RelativePath             string                  `json:"relative_path"`
	ExpectedSizeBytes        int64                   `json:"expected_size_bytes"`
	ExpectedSHA256           string                  `json:"expected_sha256"`
	PublicationState         string                  `json:"publication_state"`
	PublishedAt              *time.Time              `json:"published_at,omitempty"`
	Acknowledged             bool                    `json:"acknowledged"`
	VerifiedAt               *time.Time              `json:"verified_at,omitempty"`
	AcknowledgedPath         string                  `json:"acknowledged_relative_path,omitempty"`
	AcknowledgedSize         *int64                  `json:"acknowledged_size_bytes,omitempty"`
	AcknowledgedSHA256       string                  `json:"acknowledged_sha256,omitempty"`
	IdentityMatches          bool                    `json:"identity_matches"`
	ConnectionID             int64                   `json:"connection_id"`
	ConnectionProtocol       int                     `json:"connection_protocol_version"`
	ObservedAt               time.Time               `json:"observed_at"`
	FeedHead                 *joinedFeedHeadStatus   `json:"feed_head,omitempty"`
	LastAttemptArtifactID    *int64                  `json:"last_attempt_artifact_id,omitempty"`
	LastAttemptBlockerClass  string                  `json:"last_attempt_blocker_class,omitempty"`
	LastAttemptBlockerSHA256 string                  `json:"last_attempt_blocker_sha256,omitempty"`
	LastAttemptAt            *time.Time              `json:"last_attempt_at,omitempty"`
	RetryAt                  *time.Time              `json:"retry_at,omitempty"`
	TelemetryMatchesHead     bool                    `json:"telemetry_matches_head"`
	RawDelivery              joinedRawDeliveryStatus `json:"raw_delivery"`
}

type joinedFeedHeadStatus struct {
	ArtifactID        int64   `json:"artifact_id"`
	BatchID           string  `json:"batch_id"`
	HourID            *string `json:"hour_id,omitempty"`
	Kind              string  `json:"kind"`
	Ordinal           int     `json:"ordinal"`
	ExpectedSizeBytes int64   `json:"expected_size_bytes"`
	ExpectedSHA256    string  `json:"expected_sha256"`
}

type joinedRawDeliveryStatus struct {
	LastCursorID        int64      `json:"last_cursor_id"`
	ClipsPulled         int64      `json:"clips_pulled"`
	BytesPulled         int64      `json:"bytes_pulled"`
	ClientLastSuccessAt *time.Time `json:"client_last_success_at,omitempty"`
	NASBatchCompletedAt *time.Time `json:"nas_batch_completed_at,omitempty"`
	NASBatchClips       int        `json:"nas_batch_clips"`
	NASBatchBytes       int64      `json:"nas_batch_bytes"`
	NASBatchFailures    int        `json:"nas_batch_failures"`
	PendingClips        int64      `json:"pending_clips"`
	PendingBytes        int64      `json:"pending_bytes"`
	OldestPendingAt     *time.Time `json:"oldest_pending_at,omitempty"`
	JoinedFilesPulled   int64      `json:"joined_files_pulled"`
	JoinedBytesPulled   int64      `json:"joined_bytes_pulled"`
}

type joinedSealStreamDayReceipt struct {
	ProtocolVersion   int    `json:"protocol_version"`
	BatchID           string `json:"batch_id"`
	RecordingID       int64  `json:"recording_id"`
	LocalDate         string `json:"local_date"`
	SourceSnapshotSHA string `json:"source_snapshot_sha256"`
	HeadManifestSHA   string `json:"head_manifest_sha256"`
	LedgerSHA         string `json:"ledger_sha256"`
	LedgerArtifactSHA string `json:"ledger_artifact_sha256"`
	SealRequestSHA    string `json:"seal_request_sha256"`
	LedgerArtifactID  int64  `json:"ledger_artifact_id"`
	SourceCount       int    `json:"source_count"`
	SourceBytes       int64  `json:"source_bytes"`
	AlreadySealed     bool   `json:"already_sealed"`
}

func newRemoteJoinedOperatorService(cfg config.Config) (*remoteJoinedOperatorService, error) {
	if err := cfg.ValidateJoined(); err != nil {
		return nil, err
	}
	api, err := newJoinedAPIClient(cfg.AppBaseURL, cfg.JoinedRecordingWorkerToken, nil)
	if err != nil {
		return nil, err
	}
	service := &remoteJoinedOperatorService{
		cfg:              cfg,
		api:              api,
		operatorToken:    cfg.JoinedOperatorToken,
		capabilityClient: joinedrecording.NewCapabilityHTTPClient(),
		idlePoll:         joinedWorkerIdlePoll,
	}
	service.processClaim = service.publishClaim
	service.processPreflight = service.preflightAndPublish
	return service, nil
}

func newJoinedAPIClient(baseURL, bootstrapToken string, httpClient *http.Client) (*joinedAPIClient, error) {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	parsed, err := url.ParseRequestURI(baseURL)
	if err != nil {
		return nil, fmt.Errorf("APP_BASE_URL must be one HTTP(S) origin")
	}
	localTestHTTP := parsed.Scheme == "http" && (parsed.Hostname() == "127.0.0.1" || parsed.Hostname() == "localhost" || parsed.Hostname() == "::1")
	if (parsed.Scheme != "https" && !localTestHTTP) || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "" {
		return nil, fmt.Errorf("APP_BASE_URL must be one HTTP(S) origin")
	}
	bootstrapToken = strings.TrimSpace(bootstrapToken)
	if httpClient == nil {
		httpClient = &http.Client{Timeout: joinedAPITimeout}
	} else {
		clone := *httpClient
		httpClient = &clone
	}
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &joinedAPIClient{baseURL: baseURL, bootstrapToken: bootstrapToken, httpClient: httpClient}, nil
}

func (s *remoteJoinedOperatorService) FreezeTier1(ctx context.Context, req joinedFreezeTier1Request) (any, error) {
	token, err := s.validOperatorToken()
	if err != nil {
		return nil, err
	}
	cutoff, err := time.Parse(time.RFC3339Nano, joinedrecording.Tier1FrozenAt)
	if err != nil {
		return nil, fmt.Errorf("parse frozen Tier-1 cutoff: %w", err)
	}
	payload := struct {
		ProtocolVersion          int       `json:"protocol_version"`
		ConnectionID             int64     `json:"connection_id"`
		BatchID                  string    `json:"batch_id"`
		Generation               int       `json:"generation"`
		SourceEndpoint           string    `json:"source_endpoint"`
		QualificationRunID       int64     `json:"qualification_run_id"`
		RecordingIDs             []int64   `json:"recording_ids"`
		OrderedRecordingIDSHA256 string    `json:"ordered_recording_ids_sha256"`
		EligibilityCutoff        time.Time `json:"eligibility_cutoff"`
		Apply                    bool      `json:"apply"`
		ExpectedRequestSHA256    string    `json:"expected_request_sha256,omitempty"`
	}{joinedrecording.JoinedProtocolVersion, req.ConnectionID, req.BatchID, req.Generation, req.SourceEndpoint,
		req.QualificationRunID, append([]int64(nil), joinedrecording.Tier1RecordingIDs...),
		joinedrecording.Tier1RecordingIDSHA, cutoff, req.Apply, req.ExpectedRequestSHA256}
	var response map[string]any
	if err := s.api.postJSON(ctx, "/api/v1/recording/joined/freeze-tier1", token, payload, &response); err != nil {
		return nil, err
	}
	return response, nil
}

// FreezeTier1Checkpointed builds the immutable denominator through the
// server-owned dry-run checkpoints. It deliberately never calls the applying
// freeze endpoint. After every step, including an error, status is the only
// source of truth for selecting the next ordinal, so a client timeout cannot
// duplicate or skip a recording.
func (s *remoteJoinedOperatorService) FreezeTier1Checkpointed(ctx context.Context, req joinedFreezeTier1Request) (any, error) {
	token, err := s.validOperatorToken()
	if err != nil {
		return nil, err
	}
	if req.Apply || strings.TrimSpace(req.ExpectedRequestSHA256) != "" {
		return nil, errors.New("checkpointed Tier-1 command cannot apply")
	}
	payload, err := joinedTier1FreezePayload(req)
	if err != nil {
		return nil, err
	}
	var progress joinedTier1CheckpointedProgress
	if err := s.api.postJSON(ctx, "/api/v1/recording/joined/freeze-tier1/dry-run/start", token, payload, &progress); err != nil {
		return nil, err
	}
	for {
		if err := validateJoinedTier1CheckpointedProgress(progress); err != nil {
			return nil, err
		}
		if progress.State == "ready" {
			if progress.RequestSHA256 == nil || validateExpectedHash("request_sha256", *progress.RequestSHA256, true) != nil {
				return nil, errors.New("checkpointed Tier-1 dry run is ready without a valid request hash")
			}
			return progress, nil
		}
		if progress.NextPriorityOrdinal == nil || *progress.NextPriorityOrdinal != progress.CompletedRecordings+1 {
			return nil, errors.New("checkpointed Tier-1 dry-run cursor is not the next serial ordinal")
		}
		step := struct {
			RunID           string `json:"run_id"`
			PriorityOrdinal int    `json:"priority_ordinal"`
		}{progress.RunID, *progress.NextPriorityOrdinal}
		var stepProgress joinedTier1CheckpointedProgress
		stepErr := s.api.postJSON(ctx, "/api/v1/recording/joined/freeze-tier1/dry-run/step", token, step, &stepProgress)
		// A step may have committed before the HTTP request timed out. Always
		// reconcile through status before deciding whether to retry/resume.
		statusProgress, statusErr := s.joinedTier1CheckpointedStatus(ctx, token, progress.RunID)
		if statusErr != nil {
			if stepErr != nil {
				return nil, fmt.Errorf("checkpointed step %d failed and status reconciliation failed: %w", step.PriorityOrdinal, statusErr)
			}
			return nil, fmt.Errorf("checkpointed step %d status reconciliation failed: %w", step.PriorityOrdinal, statusErr)
		}
		if stepErr != nil {
			// A deterministic API rejection must stop. Only continue when the
			// status proves that an ambiguous transport failure committed the
			// step; otherwise a bad request/409 would spin forever.
			if statusProgress.CompletedRecordings <= progress.CompletedRecordings {
				return nil, fmt.Errorf("checkpointed step %d did not advance; reconcile status before rerunning: %w", step.PriorityOrdinal, stepErr)
			}
			progress = statusProgress
			continue
		}
		if err := validateJoinedTier1CheckpointedProgress(stepProgress); err != nil {
			return nil, err
		}
		// Prefer the freshly reread server status over the POST response. This
		// keeps the resume cursor authoritative even if a proxy returned stale
		// content after the transaction committed.
		if statusProgress.CompletedRecordings <= progress.CompletedRecordings {
			return nil, fmt.Errorf("checkpointed step %d response advanced but status did not", step.PriorityOrdinal)
		}
		progress = statusProgress
	}
}

func joinedTier1FreezePayload(req joinedFreezeTier1Request) (any, error) {
	cutoff, err := time.Parse(time.RFC3339Nano, joinedrecording.Tier1FrozenAt)
	if err != nil {
		return nil, fmt.Errorf("parse frozen Tier-1 cutoff: %w", err)
	}
	return struct {
		ProtocolVersion          int       `json:"protocol_version"`
		ConnectionID             int64     `json:"connection_id"`
		BatchID                  string    `json:"batch_id"`
		Generation               int       `json:"generation"`
		SourceEndpoint           string    `json:"source_endpoint"`
		QualificationRunID       int64     `json:"qualification_run_id"`
		RecordingIDs             []int64   `json:"recording_ids"`
		OrderedRecordingIDSHA256 string    `json:"ordered_recording_ids_sha256"`
		EligibilityCutoff        time.Time `json:"eligibility_cutoff"`
		Apply                    bool      `json:"apply"`
	}{joinedrecording.JoinedProtocolVersion, req.ConnectionID, req.BatchID, req.Generation, req.SourceEndpoint,
		req.QualificationRunID, append([]int64(nil), joinedrecording.Tier1RecordingIDs...),
		joinedrecording.Tier1RecordingIDSHA, cutoff, false}, nil
}

func (s *remoteJoinedOperatorService) joinedTier1CheckpointedStatus(ctx context.Context, token, runID string) (joinedTier1CheckpointedProgress, error) {
	var progress joinedTier1CheckpointedProgress
	path := "/api/v1/recording/joined/freeze-tier1/dry-run/status?run_id=" + url.QueryEscape(runID)
	if err := s.api.getJSON(ctx, path, token, &progress); err != nil {
		return progress, err
	}
	return progress, nil
}

func validateJoinedTier1CheckpointedProgress(progress joinedTier1CheckpointedProgress) error {
	if _, err := uuid.Parse(progress.RunID); err != nil || progress.ExpectedRecordings != len(joinedrecording.Tier1RecordingIDs) ||
		progress.CompletedRecordings < 0 || progress.CompletedRecordings > progress.ExpectedRecordings {
		return errors.New("checkpointed Tier-1 dry-run progress differs")
	}
	if progress.State != "building" && progress.State != "ready" {
		return fmt.Errorf("checkpointed Tier-1 dry-run state %q is not resumable", progress.State)
	}
	if progress.State == "building" && progress.NextPriorityOrdinal == nil {
		return errors.New("checkpointed Tier-1 building progress has no next ordinal")
	}
	if progress.State == "ready" && progress.CompletedRecordings != progress.ExpectedRecordings {
		return errors.New("checkpointed Tier-1 ready progress is incomplete")
	}
	return nil
}

func (s *remoteJoinedOperatorService) ImportHistoricalQualification(ctx context.Context,
	req joinedImportHistoricalQualificationRequest) (any, error) {
	token, err := s.validOperatorToken()
	if err != nil {
		return nil, err
	}
	payload := struct {
		ProtocolVersion       int                                 `json:"protocol_version"`
		ConnectionID          int64                               `json:"connection_id"`
		BatchID               string                              `json:"batch_id"`
		Generation            int                                 `json:"generation"`
		RecordingJobs         []joinedHistoricalQualificationJobs `json:"recording_jobs"`
		Apply                 bool                                `json:"apply"`
		ExpectedRequestSHA256 string                              `json:"expected_request_sha256,omitempty"`
	}{joinedrecording.JoinedProtocolVersion, req.ConnectionID, req.BatchID, req.Generation,
		req.RecordingJobs, req.Apply, req.ExpectedRequestSHA256}
	var response map[string]any
	if err := s.api.postJSON(ctx, "/api/v1/recording/joined/qualification/import-tier1-historical", token,
		payload, &response); err != nil {
		return nil, err
	}
	return response, nil
}

func (s *remoteJoinedOperatorService) SealStreamDay(ctx context.Context, req joinedSealStreamDayRequest) (any, error) {
	token, err := s.validOperatorToken()
	if err != nil {
		return nil, err
	}
	payload := struct {
		ProtocolVersion int    `json:"protocol_version"`
		BatchID         string `json:"batch_id"`
		RecordingID     int64  `json:"recording_id"`
		LocalDate       string `json:"local_date"`
	}{joinedrecording.JoinedProtocolVersion, req.BatchID, req.RecordingID, req.LocalDate}
	var response joinedSealStreamDayReceipt
	if err := s.api.postJSON(ctx, "/api/v1/recording/joined/stream-days/seal", token, payload, &response); err != nil {
		return nil, err
	}
	if response.ProtocolVersion != joinedrecording.JoinedProtocolVersion || response.BatchID != req.BatchID ||
		response.RecordingID != req.RecordingID || response.LocalDate != req.LocalDate || response.LedgerArtifactID <= 0 ||
		response.SourceCount < 0 || response.SourceBytes < 0 || (response.SourceCount == 0) != (response.SourceBytes == 0) ||
		validateExpectedHash("source_snapshot_sha256", response.SourceSnapshotSHA, true) != nil ||
		validateExpectedHash("head_manifest_sha256", response.HeadManifestSHA, true) != nil ||
		validateExpectedHash("ledger_sha256", response.LedgerSHA, true) != nil ||
		validateExpectedHash("ledger_artifact_sha256", response.LedgerArtifactSHA, true) != nil ||
		validateExpectedHash("seal_request_sha256", response.SealRequestSHA, true) != nil {
		return nil, errors.New("joined stream-day seal receipt differs")
	}
	return response, nil
}

func (s *remoteJoinedOperatorService) SealRemainingDays(ctx context.Context, req joinedSealRemainingDaysRequest) (any, error) {
	token, err := s.validOperatorToken()
	if err != nil {
		return nil, err
	}
	status, err := s.adminBatchStatus(ctx, token, req.BatchID)
	if err != nil {
		return nil, err
	}
	if err := validateJoinedAdminBatchStatus(status, req.BatchID); err != nil {
		return nil, err
	}
	canaryFound := false
	pending := make([]joinedSealStreamDayRequest, 0, status.ExpectedStreamDays-1)
	sealed := 0
	for _, day := range status.StreamDays {
		isCanary := day.RecordingID == req.CanaryRecordingID && day.LocalDate == req.CanaryLocalDate
		if isCanary {
			if canaryFound || day.State != "sealed" || day.SealRequestSHA256 != req.ExpectedCanarySealRequestSHA256 {
				return nil, errors.New("joined canary receipt differs")
			}
			canaryFound = true
			sealed++
			continue
		}
		switch day.State {
		case "sealed":
			sealed++
		case "pending":
			if day.SealRequestSHA256 != "" {
				return nil, errors.New("pending joined stream day carries a seal receipt")
			}
			pending = append(pending, joinedSealStreamDayRequest{BatchID: req.BatchID,
				RecordingID: day.RecordingID, LocalDate: day.LocalDate, Apply: true})
		default:
			return nil, errors.New("joined stream-day state is not resumable")
		}
	}
	if !canaryFound {
		return nil, errors.New("approved joined canary is absent")
	}
	if !req.Apply {
		return map[string]any{"dry_run": true, "batch_id": req.BatchID, "already_sealed": sealed,
			"remaining": len(pending)}, nil
	}
	completed := 0
	for _, day := range pending {
		if _, err := s.SealStreamDay(ctx, day); err != nil {
			return nil, fmt.Errorf("seal joined stream day %d/%s after %d completions: %w",
				day.RecordingID, day.LocalDate, completed, err)
		}
		completed++
	}
	return map[string]any{"dry_run": false, "batch_id": req.BatchID, "already_sealed": sealed,
		"sealed_now": completed, "remaining": 0}, nil
}

func validateJoinedAdminBatchStatus(status joinedAdminBatchStatus, batchID string) error {
	if status.ProtocolVersion != joinedrecording.JoinedProtocolVersion || status.BatchID != batchID ||
		status.State != "building" || status.FreezeStartedAt != nil || status.FrozenAt != nil ||
		status.ExpectedStreamDays != len(joinedrecording.Tier1RecordingIDs)*14 ||
		status.ExpectedScheduledHours != len(joinedrecording.Tier1RecordingIDs)*14*12 ||
		len(status.StreamDays) != status.ExpectedStreamDays ||
		validateExpectedHash("frozen_denominator_sha256", status.FrozenDenominatorSHA256, true) != nil {
		return errors.New("joined batch is not ready for serial stream-day sealing")
	}
	for index, day := range status.StreamDays {
		if day.RecordingID != joinedrecording.Tier1RecordingIDs[index/14] || day.SourceCount < 0 || day.SourceBytes < 0 ||
			(day.SourceCount == 0) != (day.SourceBytes == 0) || (day.State != "pending" && day.State != "sealed") ||
			(day.State == "pending" && day.SealRequestSHA256 != "") ||
			(day.State == "sealed" && validateExpectedHash("seal_request_sha256", day.SealRequestSHA256, true) != nil) {
			return errors.New("joined stream-day status evidence differs")
		}
		date, err := time.Parse("2006-01-02", day.LocalDate)
		if err != nil || date.Format("2006-01-02") != day.LocalDate {
			return errors.New("joined stream-day status date differs")
		}
		if index%14 != 0 {
			previous, _ := time.Parse("2006-01-02", status.StreamDays[index-1].LocalDate)
			if !date.Equal(previous.AddDate(0, 0, 1)) {
				return errors.New("joined stream-day status dates are not consecutive")
			}
		}
	}
	return nil
}

func (s *remoteJoinedOperatorService) adminBatchStatus(ctx context.Context, token, batchID string) (joinedAdminBatchStatus, error) {
	var status joinedAdminBatchStatus
	path := "/api/v1/recording/joined/batches/status?batch_id=" + url.QueryEscape(batchID)
	err := s.api.getJSON(ctx, path, token, &status)
	return status, err
}

func (s *remoteJoinedOperatorService) FinalFreeze(ctx context.Context, req joinedFinalFreezeRequest) (any, error) {
	token, err := s.validOperatorToken()
	if err != nil {
		return nil, err
	}
	payload := struct {
		ProtocolVersion                 int    `json:"protocol_version"`
		BatchID                         string `json:"batch_id"`
		ExpectedFrozenDenominatorSHA256 string `json:"expected_frozen_denominator_sha256"`
	}{joinedrecording.JoinedProtocolVersion, req.BatchID, req.ExpectedFrozenDenominatorSHA256}
	var response map[string]any
	if err := s.api.postJSON(ctx, "/api/v1/recording/joined/batches/final-freeze", token, payload, &response); err != nil {
		return nil, err
	}
	return response, nil
}

// FinalValidation runs the server-owned stream-day certificate checkpoint
// serially. Status is reconciled after every step, including ambiguous
// transport failures, so the operator never guesses whether a receipt landed.
func (s *remoteJoinedOperatorService) FinalValidation(ctx context.Context, req joinedFinalFreezeRequest) (any, error) {
	token, err := s.validOperatorToken()
	if err != nil {
		return nil, err
	}
	start := struct {
		ProtocolVersion                 int    `json:"protocol_version"`
		BatchID                         string `json:"batch_id"`
		ExpectedFrozenDenominatorSHA256 string `json:"expected_frozen_denominator_sha256"`
	}{joinedrecording.JoinedProtocolVersion, req.BatchID, req.ExpectedFrozenDenominatorSHA256}
	var progress joinedFinalValidationProgress
	if err := s.api.postJSON(ctx, "/api/v1/recording/joined/batches/final-freeze/validation/start", token, start, &progress); err != nil {
		return nil, err
	}
	for {
		if progress.State == "ready" {
			return progress, nil
		}
		if progress.NextOrdinal == nil || *progress.NextOrdinal != progress.CompletedScopes+1 {
			return nil, errors.New("joined final-validation cursor is not the next serial ordinal")
		}
		step := struct {
			ProtocolVersion int    `json:"protocol_version"`
			RunID           string `json:"run_id"`
			Ordinal         int    `json:"ordinal"`
		}{joinedrecording.JoinedProtocolVersion, progress.RunID, *progress.NextOrdinal}
		var stepProgress joinedFinalValidationProgress
		stepErr := s.api.postJSON(ctx, "/api/v1/recording/joined/batches/final-freeze/validation/step", token, step, &stepProgress)
		status, statusErr := s.finalValidationStatus(ctx, token, progress.RunID)
		if statusErr != nil {
			if stepErr != nil {
				return nil, fmt.Errorf("joined final-validation step %d failed and status reconciliation failed: %w", step.Ordinal, statusErr)
			}
			return nil, fmt.Errorf("joined final-validation step %d status reconciliation failed: %w", step.Ordinal, statusErr)
		}
		if stepErr != nil {
			if status.CompletedScopes <= progress.CompletedScopes {
				return nil, fmt.Errorf("joined final-validation step %d did not advance; reconcile before retry: %w", step.Ordinal, stepErr)
			}
			progress = status
			continue
		}
		if status.CompletedScopes <= progress.CompletedScopes {
			return nil, fmt.Errorf("joined final-validation step %d response advanced but status did not", step.Ordinal)
		}
		progress = status
	}
}

func (s *remoteJoinedOperatorService) finalValidationStatus(ctx context.Context, token, runID string) (joinedFinalValidationProgress, error) {
	var progress joinedFinalValidationProgress
	path := "/api/v1/recording/joined/batches/final-freeze/validation/status?run_id=" + url.QueryEscape(runID)
	if err := s.api.getJSON(ctx, path, token, &progress); err != nil {
		return progress, err
	}
	return progress, nil
}

func (s *remoteJoinedOperatorService) SealBatchIndex(ctx context.Context, req joinedSealBatchIndexRequest) (any, error) {
	token, err := s.validOperatorToken()
	if err != nil {
		return nil, err
	}
	payload := struct {
		ProtocolVersion int    `json:"protocol_version"`
		BatchID         string `json:"batch_id"`
		Apply           bool   `json:"apply"`
		ExpectedSHA256  string `json:"expected_sha256,omitempty"`
	}{joinedrecording.JoinedProtocolVersion, req.BatchID, req.Apply, req.ExpectedSHA256}
	var response map[string]any
	if err := s.api.postJSON(ctx, "/api/v1/recording/joined/batches/index/seal", token, payload, &response); err != nil {
		return nil, err
	}
	return response, nil
}

func (s *remoteJoinedOperatorService) validOperatorToken() (string, error) {
	token := strings.TrimSpace(s.operatorToken)
	if len(token) < 32 || token == strings.TrimSpace(s.api.bootstrapToken) {
		return "", errors.New("a distinct STOARAMA_JOINED_OPERATOR_TOKEN is required for operator actions")
	}
	return token, nil
}

func (s *remoteJoinedOperatorService) Status(ctx context.Context, req joinedStatusRequest) (any, error) {
	if strings.TrimSpace(s.api.bootstrapToken) == "" {
		return nil, errors.New("joined worker bootstrap token is required for worker status")
	}
	var status joinedWorkerStatus
	path := "/api/v1/recording/joined/status?batch_id=" + url.QueryEscape(req.BatchID)
	if err := s.api.getJSON(ctx, path, s.api.bootstrapToken, &status); err != nil {
		return nil, err
	}
	return status, nil
}

func (s *remoteJoinedOperatorService) DeliveryStatus(ctx context.Context, req joinedDeliveryStatusRequest) (any, error) {
	token, err := s.validOperatorToken()
	if err != nil {
		return nil, err
	}
	var status joinedDeliveryStatus
	path := "/api/v1/recording/joined/delivery-status?batch_id=" + url.QueryEscape(req.BatchID) +
		"&artifact_id=" + url.QueryEscape(fmt.Sprint(req.ArtifactID))
	if err := s.api.getJSON(ctx, path, token, &status); err != nil {
		return nil, err
	}
	if status.BatchID != req.BatchID || status.ArtifactID != req.ArtifactID {
		return nil, errors.New("joined delivery status identity differs")
	}
	return status, nil
}

func (s *remoteJoinedOperatorService) ClaimAdmissionStatus(ctx context.Context, batchID string) (joinedrecording.ClaimAdmissionStatus, error) {
	token, err := s.validOperatorToken()
	if err != nil {
		return joinedrecording.ClaimAdmissionStatus{}, err
	}
	var status joinedrecording.ClaimAdmissionStatus
	path := "/api/v1/recording/joined/admission?batch_id=" + url.QueryEscape(batchID)
	if err := s.api.getJSON(ctx, path, token, &status); err != nil {
		return status, err
	}
	if err := status.Validate(); err != nil || status.BatchID != batchID {
		return status, errors.New("joined claim admission status differs")
	}
	return status, nil
}

func (s *remoteJoinedOperatorService) SetClaimAdmission(ctx context.Context, req joinedrecording.ClaimAdmissionRequest) (joinedrecording.ClaimAdmissionStatus, error) {
	token, err := s.validOperatorToken()
	if err != nil {
		return joinedrecording.ClaimAdmissionStatus{}, err
	}
	if err := req.Validate(); err != nil {
		return joinedrecording.ClaimAdmissionStatus{}, err
	}
	var status joinedrecording.ClaimAdmissionStatus
	if err := s.api.putJSON(ctx, "/api/v1/recording/joined/admission", token, req, &status); err != nil {
		return status, err
	}
	if err := status.Validate(); err != nil || status.BatchID != req.BatchID || status.ClaimsPaused != req.ClaimsPaused {
		return status, errors.New("joined claim admission update differs")
	}
	return status, nil
}

func (s *remoteJoinedOperatorService) CheckWorkerStartup(ctx context.Context, req joinedWorkerRequest) error {
	if err := os.MkdirAll(req.ScratchRoot, 0o700); err != nil {
		return fmt.Errorf("create joined scratch root: %w", err)
	}
	scratch, err := os.Lstat(req.ScratchRoot)
	if err != nil || !scratch.IsDir() || scratch.Mode()&0o077 != 0 {
		return fmt.Errorf("joined scratch root must be a private directory")
	}
	tool, err := joinedrecording.InspectMediaToolEvidence(ctx)
	if err != nil {
		return fmt.Errorf("inspect installed media tools: %w", err)
	}
	if tool.FFmpegSHA256 != s.cfg.JoinedRecordingFFmpegSHA256 || tool.FFprobeSHA256 != s.cfg.JoinedRecordingFFprobeSHA256 {
		return fmt.Errorf("installed media tool hashes differ from configured pins")
	}
	status, err := s.Status(ctx, joinedStatusRequest{BatchID: req.BatchID})
	if err != nil {
		return fmt.Errorf("read joined backend status: %w", err)
	}
	values, ok := status.(joinedWorkerStatus)
	if !ok || validateJoinedWorkerStatus(s.cfg, req.BatchID, values) != nil {
		return fmt.Errorf("joined backend is not enabled")
	}
	removed, err := s.cleanupInactiveScratch(ctx, req)
	if err != nil {
		return fmt.Errorf("cleanup inactive joined scratch: %w", err)
	}
	if len(removed) > 0 {
		log.Printf("joined worker removed inactive scratch directories count=%d", len(removed))
	}
	return nil
}

func (s *remoteJoinedOperatorService) cleanupInactiveScratch(ctx context.Context, req joinedWorkerRequest) ([]string, error) {
	return joinedrecording.CleanupInactiveLeaseScratch(ctx, req.ScratchRoot, func(callCtx context.Context, leaseIDs []string) (map[string]bool, error) {
		response, err := s.api.leaseStatus(callCtx, s.api.bootstrapToken, joinedrecording.LeaseStatusRequest{
			ProtocolVersion: joinedrecording.JoinedProtocolVersion,
			BatchID:         req.BatchID,
			LeaseIDs:        leaseIDs,
		})
		if err != nil {
			return nil, err
		}
		proof := make(map[string]bool, len(response.Leases))
		for _, lease := range response.Leases {
			proof[lease.LeaseID] = !lease.Active
		}
		return proof, nil
	})
}

func validateJoinedWorkerStatus(cfg config.Config, batchID string, status joinedWorkerStatus) error {
	workScope, scopeErr := cfg.JoinedWorkScope()
	var localHours []string
	var err error
	if config.IsJoinedCanaryWorkScope(workScope) {
		localHours, err = cfg.JoinedCanaryHourIDs()
	}
	if err != nil || scopeErr != nil || !status.Enabled || status.ProtocolVersion != joinedrecording.JoinedProtocolVersion ||
		status.BatchID != batchID || batchID != cfg.JoinedRecordingBatchID ||
		status.WorkScope != workScope ||
		strings.Join(status.CanaryHourIDs, ",") != strings.Join(localHours, ",") {
		return errors.New("joined backend batch or work scope differs")
	}
	return nil
}

func joinedConfiguredWorkScope(cfg config.Config) (joinedrecording.WorkScopeIdentity, error) {
	scope, err := cfg.JoinedWorkScope()
	if err != nil {
		return joinedrecording.WorkScopeIdentity{}, err
	}
	var hours []string
	if config.IsJoinedCanaryWorkScope(scope) {
		hours, err = cfg.JoinedCanaryHourIDs()
		if err != nil {
			return joinedrecording.WorkScopeIdentity{}, err
		}
	}
	return joinedrecording.NewWorkScopeIdentity(cfg.JoinedRecordingBatchID, scope, hours)
}

func (s *remoteJoinedOperatorService) RunWorker(ctx context.Context, req joinedWorkerRequest) error {
	return runJoinedWorkerLoop(ctx, s.idlePoll, func(admissionCtx, taskCtx context.Context) (bool, error) {
		return s.runWorkerOnceWithTaskContext(admissionCtx, taskCtx, req)
	})
}

// runJoinedWorkerLoop separates claim admission from work already admitted.
// Canceling admission stops the next bootstrap or claim immediately, while a
// claimed task keeps its lease heartbeat and gets its existing hard deadline.
func runJoinedWorkerLoop(ctx context.Context, idlePoll time.Duration, runOnce func(context.Context, context.Context) (bool, error)) error {
	if idlePoll <= 0 || runOnce == nil {
		return errors.New("joined worker loop configuration is required")
	}
	taskBase := context.WithoutCancel(ctx)
	for {
		if ctx.Err() != nil {
			return nil
		}
		worked, err := runOnce(ctx, taskBase)
		if err != nil {
			if ctx.Err() != nil && !worked {
				return nil
			}
			return err
		}
		if worked {
			continue
		}
		timer := time.NewTimer(idlePoll)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		case <-timer.C:
		}
	}
}

func (s *remoteJoinedOperatorService) runWorkerOnce(ctx context.Context, req joinedWorkerRequest) (bool, error) {
	return s.runWorkerOnceWithTaskContext(ctx, ctx, req)
}

func (s *remoteJoinedOperatorService) runWorkerOnceWithTaskContext(admissionCtx, taskCtx context.Context, req joinedWorkerRequest) (bool, error) {
	workScope, err := joinedConfiguredWorkScope(s.cfg)
	if err != nil {
		return false, err
	}
	bootstrapRequest := joinedrecording.WorkerBootstrapRequest{ProtocolVersion: joinedrecording.JoinedProtocolVersion,
		BatchID: req.BatchID, WorkScopeIdentity: workScope}
	bootstrap, ok, err := s.api.bootstrap(admissionCtx, bootstrapRequest)
	if err != nil || !ok {
		return false, err
	}
	claimRequest := joinedrecording.WorkClaimRequest{ProtocolVersion: joinedrecording.JoinedProtocolVersion, BatchID: req.BatchID, WorkerID: req.WorkerID}
	if workScope.WorkScope == joinedrecording.WorkScopeFrozenBatch {
		budget, err := joinedrecording.AvailableScratchBudget(req.ScratchRoot)
		if err != nil {
			return false, fmt.Errorf("measure joined scratch admission budget: %w", err)
		}
		taskBudget, err := joinedrecording.WorkerTaskBudgetBytes(budget)
		if err != nil {
			return false, fmt.Errorf("derive joined scratch admission budget: %w", err)
		}
		claimRequest.ScratchAvailableBytes = budget
		claimRequest.TaskBudgetBytes = taskBudget
	}
	// Cancellation can race after either claim API commits a lease but before
	// this client receives it. No local task starts in that case. The unseen
	// fenced lease expires and becomes reclaimable through the normal path.
	publication, ok, err := s.api.claimPublication(admissionCtx, bootstrap.ClaimToken, claimRequest)
	if err != nil {
		return false, err
	}
	if ok {
		if err := joinedPublicationWithinScope(s.cfg, publication); err != nil {
			return false, err
		}
		kind, id, token := joinedPublicationFailureIdentity(publication)
		leaseID, expires := joinedPublicationLeaseIdentity(publication)
		tracker := newJoinedOperationTracker(leaseID, token, expires)
		taskErr := runJoinedWorkerTask(taskCtx, joinedWorkerTaskLimit, "publish_claim", func(workCtx context.Context) error {
			workCtx = context.WithValue(workCtx, joinedOperationTrackerContextKey{}, tracker)
			return s.processClaim(workCtx, publication, req.ScratchRoot)
		})
		return true, s.reportJoinedTaskFailure(taskCtx, tracker.get(), kind, id, taskErr)
	}
	preflight, ok, err := s.api.claimPreflight(admissionCtx, bootstrap.ClaimToken, claimRequest)
	if err != nil || !ok {
		return false, err
	}
	if !joinedHourWithinScope(s.cfg, preflight.HourID) {
		return false, errors.New("joined preflight claim is outside configured work scope")
	}
	tracker := newJoinedOperationTracker(preflight.LeaseID, preflight.OperationToken, preflight.LeaseExpires)
	taskErr := runJoinedWorkerTask(taskCtx, joinedWorkerTaskLimit, "preflight_and_publish", func(workCtx context.Context) error {
		workCtx = context.WithValue(workCtx, joinedOperationTrackerContextKey{}, tracker)
		return s.processPreflight(workCtx, preflight, req.ScratchRoot)
	})
	return true, s.reportJoinedTaskFailure(taskCtx, tracker.get(), "hour", preflight.HourID, taskErr)
}

func joinedPublicationFailureIdentity(response joinedrecording.PublicationClaimResponse) (kind, id, token string) {
	switch response.Kind {
	case "ledger":
		return "ledger", response.Ledger.ScopeID, response.Ledger.OperationToken
	case "hour":
		return "hour", response.Hour.HourID, response.Hour.OperationToken
	case "batch_index":
		return "batch_index", response.BatchIndex.ScopeID, response.BatchIndex.OperationToken
	default:
		return "", "", ""
	}
}

func joinedPublicationLeaseIdentity(response joinedrecording.PublicationClaimResponse) (string, time.Time) {
	switch response.Kind {
	case "ledger":
		return response.Ledger.LeaseID, response.Ledger.LeaseExpires
	case "hour":
		return response.Hour.LeaseID, response.Hour.LeaseExpires
	case "batch_index":
		return response.BatchIndex.LeaseID, response.BatchIndex.LeaseExpires
	default:
		return "", time.Time{}
	}
}

func joinedFailureClassification(err error) (class, reason string) {
	if errors.Is(err, syscall.ENOSPC) || strings.Contains(strings.ToLower(err.Error()), "scratch") {
		return "resource", "scratch_resource_exhausted"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "transient", "worker_task_deadline"
	}
	return "transient", "worker_task_failed"
}

func (s *remoteJoinedOperatorService) reportJoinedTaskFailure(taskCtx context.Context, token, kind, id string, taskErr error) error {
	if taskErr == nil {
		return nil
	}
	class, reason := joinedFailureClassification(taskErr)
	reportCtx, cancel := context.WithTimeout(context.WithoutCancel(taskCtx), 30*time.Second)
	defer cancel()
	_, reportErr := s.api.reportFailure(reportCtx, token, joinedrecording.WorkFailureRequest{
		ProtocolVersion: joinedrecording.JoinedProtocolVersion, ScopeKind: kind, ScopeID: id,
		FailureClass: class, ReasonCode: reason,
	})
	if reportErr != nil {
		return errors.Join(taskErr, fmt.Errorf("report joined task failure: %w", reportErr))
	}
	if diagnostic := joinedTaskFailureDiagnostic(taskErr); diagnostic != "" {
		log.Printf("joined worker task failure recorded scope_kind=%s scope_id=%s class=%s reason=%s diagnostic=%q", kind, id, class, reason, diagnostic)
	} else {
		log.Printf("joined worker task failure recorded scope_kind=%s scope_id=%s class=%s reason=%s", kind, id, class, reason)
	}
	return nil
}

func joinedTaskFailureDiagnostic(err error) string {
	var storageErr *joinedrecording.StorageCapabilityError
	if errors.As(err, &storageErr) {
		diagnostic := storageErr.Error()
		var responseErr *joinedAPIResponseError
		if errors.As(err, &responseErr) {
			diagnostic += fmt.Sprintf(" api_status=%d", responseErr.status)
		}
		return diagnostic
	}
	var responseErr *joinedAPIResponseError
	if errors.As(err, &responseErr) {
		return responseErr.Error()
	}
	return ""
}

func joinedHourWithinScope(cfg config.Config, hourID string) bool {
	scope, scopeErr := cfg.JoinedWorkScope()
	if scopeErr != nil {
		return false
	}
	if scope == config.JoinedWorkScopeFrozenBatch {
		return strings.HasPrefix(hourID, cfg.JoinedRecordingBatchID+"__recording-")
	}
	hours, err := cfg.JoinedCanaryHourIDs()
	if err != nil {
		return false
	}
	for _, allowed := range hours {
		if hourID == allowed {
			return true
		}
	}
	return false
}

func joinedPublicationWithinScope(cfg config.Config, response joinedrecording.PublicationClaimResponse) error {
	scope, err := cfg.JoinedWorkScope()
	if err != nil {
		return err
	}
	if publicationBatchID(response) != cfg.JoinedRecordingBatchID {
		return errors.New("joined publication claim has a foreign batch")
	}
	if scope == config.JoinedWorkScopeFrozenBatch {
		return nil
	}
	switch response.Kind {
	case "hour":
		if response.Hour != nil && joinedHourWithinScope(cfg, response.Hour.HourID) {
			return nil
		}
	case "ledger":
		if response.Ledger != nil {
			ledger := response.Ledger.Ledger
			for _, hour := range ledger.Hours {
				hourID, err := joinedrecording.CanonicalHourID(ledger.BatchID, ledger.RecordingID, ledger.LocalDate,
					hour.DeliveryHour, ledger.Generation)
				if err == nil && joinedHourWithinScope(cfg, hourID) {
					return nil
				}
			}
		}
	case "batch_index":
		return errors.New("joined batch index is not eligible during the exact-hour canary")
	}
	return errors.New("joined publication claim is outside configured canary scope")
}

func runJoinedWorkerTask(ctx context.Context, limit time.Duration, stage string, work func(context.Context) error) error {
	if limit <= 0 || strings.TrimSpace(stage) == "" || work == nil {
		return errors.New("joined worker task limit is required")
	}
	startedAt := time.Now()
	taskCtx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	err := work(taskCtx)
	if err != nil && errors.Is(taskCtx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("joined worker task deadline exceeded stage=%s elapsed=%s limit=%s: %w", stage, time.Since(startedAt).Round(time.Millisecond), limit, context.DeadlineExceeded)
	}
	// A nil result means the task completed its finalize call. Preserve that
	// committed success if the cooperative deadline becomes visible at the
	// same boundary; restarting it would create a false retry.
	return err
}

func (s *remoteJoinedOperatorService) preflightAndPublish(ctx context.Context, claim joinedrecording.PreflightHourClaim, scratchRoot string) error {
	ctx = withJoinedStageTiming(ctx, claim.HourID)
	heartbeat := s.hourHeartbeat(claim.HourID)
	resolveSource := func(callCtx context.Context, current joinedrecording.PreflightHourClaim, source joinedrecording.SourceClip, operation string) (joinedrecording.SourceReadCapability, error) {
		return s.api.sourceCapability(callCtx, current.OperationToken, joinedrecording.SourceCapabilityRequest{ProtocolVersion: joinedrecording.JoinedProtocolVersion, HourID: current.HourID, ClipID: source.ClipID, Operation: operation})
	}
	seal := func(callCtx context.Context, current joinedrecording.PreflightHourClaim, request joinedrecording.SealHourRequest) (joinedrecording.WorkerClaim, error) {
		if err := request.Validate(current.RecordingID, current.MediaTool.IdentitySHA256); err != nil {
			return joinedrecording.WorkerClaim{}, err
		}
		sealed, err := s.api.sealHour(callCtx, current.OperationToken, request)
		if err == nil && sealed.HourID != current.HourID {
			err = fmt.Errorf("sealed joined hour differs from preflight claim")
		}
		if err == nil {
			if tracker, ok := callCtx.Value(joinedOperationTrackerContextKey{}).(*joinedOperationTracker); ok {
				tracker.replace(sealed.LeaseID, sealed.OperationToken, sealed.LeaseExpires)
			}
		}
		return sealed, err
	}
	sealed, scratch, err := joinedrecording.RunPreflightHourRenewing(ctx, claim, scratchRoot, s.capabilityClient, s.cfg.JoinedRecordingStorageAuthority, heartbeat, resolveSource, seal)
	if err != nil {
		return fmt.Errorf("preflight joined hour: %w", err)
	}
	_, err = joinedrecording.PublishClaimedHourRenewing(ctx, s.capabilityClient, s.cfg.JoinedRecordingStorageAuthority, sealed, scratch, s.hourHeartbeat(sealed.HourID), s.hourCreateCapability, s.hourReadCapability, s.finalizeHour)
	if err != nil {
		return fmt.Errorf("publish freshly sealed joined hour: %w", err)
	}
	return nil
}

func joinedStageTimingLog(hourID string, event joinedrecording.StageTimingEvent) string {
	base := fmt.Sprintf("joined worker stage timing hour_id=%s stage=%s elapsed_ms=%d outcome=%s", hourID, event.Stage, event.ElapsedMS, event.Outcome)
	if event.Stage != "upload_verify" || event.Outcome != "error" || !event.FailureStage.Valid() {
		return base
	}
	artifactID, ordinal := event.ArtifactID, event.ArtifactOrdinal
	if artifactID < 0 {
		artifactID = 0
	}
	if ordinal < 0 {
		ordinal = 0
	}
	return fmt.Sprintf("%s failure_stage=%s artifact_id=%d artifact_ordinal=%d", base, event.FailureStage, artifactID, ordinal)
}

func withJoinedStageTiming(ctx context.Context, hourID string) context.Context {
	return joinedrecording.WithStageTimingObserver(ctx, func(event joinedrecording.StageTimingEvent) {
		log.Print(joinedStageTimingLog(hourID, event))
	})
}

func (s *remoteJoinedOperatorService) publishClaim(ctx context.Context, response joinedrecording.PublicationClaimResponse, scratchRoot string) error {
	switch response.Kind {
	case "ledger":
		claim := *response.Ledger
		_, err := joinedrecording.PublishAllocationLedgerRenewing(ctx, s.capabilityClient, s.cfg.JoinedRecordingStorageAuthority, claim, s.scopeHeartbeat("ledger", claim.ScopeID),
			func(callCtx context.Context, current joinedrecording.LedgerPublicationClaim) (joinedrecording.ObjectCreateCapability, error) {
				return s.artifactCreateCapability(callCtx, "ledger", current.ScopeID, current.ArtifactID, current.OperationToken)
			},
			func(callCtx context.Context, current joinedrecording.LedgerPublicationClaim) (joinedrecording.ObjectReadCapability, error) {
				return s.artifactReadCapability(callCtx, "ledger", current.ScopeID, current.ArtifactID, current.OperationToken)
			},
			func(callCtx context.Context, current joinedrecording.LedgerPublicationClaim, published joinedrecording.PublishedLedger) error {
				return s.api.finalizeLedger(callCtx, current.OperationToken, joinedrecording.FinalizeLedgerRequest{ProtocolVersion: joinedrecording.JoinedProtocolVersion, Published: published})
			})
		return err
	case "hour":
		claim := *response.Hour
		ctx = withJoinedStageTiming(ctx, claim.HourID)
		var scratch joinedrecording.SealedHourScratch
		var err error
		if claim.Plan.GapOnly {
			scratch, err = joinedrecording.BindReclaimedGapOnlyHourScratch(claim, scratchRoot)
		} else {
			resolveSource := func(callCtx context.Context, current joinedrecording.WorkerClaim, source joinedrecording.SourceClip, operation string) (joinedrecording.SourceReadCapability, error) {
				return s.api.sourceCapability(callCtx, current.OperationToken, joinedrecording.SourceCapabilityRequest{
					ProtocolVersion: joinedrecording.JoinedProtocolVersion, HourID: current.HourID,
					ClipID: source.ClipID, Operation: operation,
				})
			}
			claim, scratch, err = joinedrecording.RebuildSealedHourRenewing(ctx, claim, scratchRoot, s.capabilityClient,
				s.cfg.JoinedRecordingStorageAuthority, s.hourHeartbeat(claim.HourID), resolveSource)
		}
		if err != nil {
			return fmt.Errorf("rebuild reclaimed joined hour: %w", err)
		}
		_, err = joinedrecording.PublishClaimedHourRenewing(ctx, s.capabilityClient, s.cfg.JoinedRecordingStorageAuthority, claim, scratch, s.hourHeartbeat(claim.HourID), s.hourCreateCapability, s.hourReadCapability, s.finalizeHour)
		if err != nil {
			return fmt.Errorf("publish reclaimed joined hour: %w", err)
		}
		return nil
	case "batch_index":
		claim := *response.BatchIndex
		_, err := joinedrecording.PublishBatchIndexRenewing(ctx, s.capabilityClient, s.cfg.JoinedRecordingStorageAuthority, claim, s.scopeHeartbeat("batch_index", claim.ScopeID),
			func(callCtx context.Context, current joinedrecording.BatchIndexPublicationClaim) (joinedrecording.ObjectCreateCapability, error) {
				return s.artifactCreateCapability(callCtx, "batch_index", current.ScopeID, current.ArtifactID, current.OperationToken)
			},
			func(callCtx context.Context, current joinedrecording.BatchIndexPublicationClaim) (joinedrecording.ObjectReadCapability, error) {
				return s.artifactReadCapability(callCtx, "batch_index", current.ScopeID, current.ArtifactID, current.OperationToken)
			},
			func(callCtx context.Context, current joinedrecording.BatchIndexPublicationClaim, published joinedrecording.PublishedBatchIndex) error {
				return s.api.finalizeBatchIndex(callCtx, current.OperationToken, joinedrecording.FinalizeBatchIndexRequest{ProtocolVersion: joinedrecording.JoinedProtocolVersion, Published: published})
			})
		return err
	default:
		return fmt.Errorf("unsupported joined publication claim kind")
	}
}

func (s *remoteJoinedOperatorService) scopeHeartbeat(kind, id string) joinedrecording.HeartbeatOperation {
	return func(ctx context.Context, operationToken string) (joinedrecording.OperationCredentials, error) {
		response, err := s.api.heartbeat(ctx, operationToken, joinedrecording.HeartbeatRequest{ProtocolVersion: joinedrecording.JoinedProtocolVersion, ScopeKind: kind, ScopeID: id})
		if err != nil {
			return joinedrecording.OperationCredentials{}, err
		}
		credentials := joinedrecording.OperationCredentials{LeaseID: response.LeaseID, OperationToken: response.OperationToken, ExpiresAt: response.ExpiresAt}
		if tracker, ok := ctx.Value(joinedOperationTrackerContextKey{}).(*joinedOperationTracker); ok {
			tracker.accept(credentials)
		}
		return credentials, nil
	}
}

func (s *remoteJoinedOperatorService) hourHeartbeat(hourID string) joinedrecording.HeartbeatOperation {
	return s.scopeHeartbeat("hour", hourID)
}

func (s *remoteJoinedOperatorService) artifactCreateCapability(ctx context.Context, kind, id string, artifactID int64, token string) (joinedrecording.ObjectCreateCapability, error) {
	capability, err := s.api.artifactCreateCapability(ctx, token, joinedrecording.ArtifactCapabilityRequest{ProtocolVersion: joinedrecording.JoinedProtocolVersion, ScopeKind: kind, ScopeID: id, ArtifactID: artifactID, Operation: "put"})
	if err == nil {
		return capability, nil
	}
	return joinedrecording.ObjectCreateCapability{}, joinedCreateCapabilityError(err, artifactID)
}

func joinedCreateCapabilityError(err error, artifactID int64) error {
	diagnostic := &joinedrecording.StorageCapabilityError{Operation: "create_capability", Reason: "capability",
		ArtifactID: artifactID, Cause: err}
	var responseErr *joinedAPIResponseError
	var transportErr *joinedAPITransportError
	if errors.As(err, &responseErr) {
		diagnostic.Reason, diagnostic.StatusCode = "status", responseErr.status
	} else if errors.As(err, &transportErr) {
		diagnostic.Reason = "transport"
	} else if !errors.Is(err, context.Canceled) {
		// A capability failure without an HTTP status is ambiguous transport.
		// The bounded retry loop resolves a fresh capability before retrying.
		diagnostic.Reason = "transport"
	}
	return diagnostic
}

func (s *remoteJoinedOperatorService) artifactReadCapability(ctx context.Context, kind, id string, artifactID int64, token string) (joinedrecording.ObjectReadCapability, error) {
	capability, err := s.api.artifactReadCapability(ctx, token, joinedrecording.ArtifactCapabilityRequest{ProtocolVersion: joinedrecording.JoinedProtocolVersion, ScopeKind: kind, ScopeID: id, ArtifactID: artifactID, Operation: "read"})
	if err == nil {
		return capability, nil
	}
	return joinedrecording.ObjectReadCapability{}, joinedReadCapabilityError(err, artifactID)
}

func joinedReadCapabilityError(err error, artifactID int64) error {
	diagnostic := &joinedrecording.StorageCapabilityError{Operation: "reread_capability", Reason: "capability",
		ArtifactID: artifactID, Cause: err}
	var responseErr *joinedAPIResponseError
	var transportErr *joinedAPITransportError
	if errors.As(err, &responseErr) {
		diagnostic.Reason, diagnostic.StatusCode = "status", responseErr.status
	} else if errors.As(err, &transportErr) {
		diagnostic.Reason = "transport"
	} else if !errors.Is(err, context.Canceled) {
		diagnostic.Reason = "transport"
	}
	return diagnostic
}

func (s *remoteJoinedOperatorService) hourCreateCapability(ctx context.Context, claim joinedrecording.WorkerClaim, artifactID int64) (joinedrecording.ObjectCreateCapability, error) {
	return s.artifactCreateCapability(ctx, "hour", claim.HourID, artifactID, claim.OperationToken)
}

func (s *remoteJoinedOperatorService) hourReadCapability(ctx context.Context, claim joinedrecording.WorkerClaim, artifactID int64) (joinedrecording.ObjectReadCapability, error) {
	return s.artifactReadCapability(ctx, "hour", claim.HourID, artifactID, claim.OperationToken)
}

func (s *remoteJoinedOperatorService) finalizeHour(ctx context.Context, claim joinedrecording.WorkerClaim, published joinedrecording.PublishedHour) error {
	return s.api.finalizeHour(ctx, claim.OperationToken, joinedrecording.FinalizeHourRequest{ProtocolVersion: joinedrecording.JoinedProtocolVersion, Published: published})
}

func (c *joinedAPIClient) bootstrap(ctx context.Context, request joinedrecording.WorkerBootstrapRequest) (joinedrecording.WorkerBootstrapResponse, bool, error) {
	if strings.TrimSpace(c.bootstrapToken) == "" {
		return joinedrecording.WorkerBootstrapResponse{}, false, errors.New("joined worker bootstrap token is required")
	}
	var response joinedrecording.WorkerBootstrapResponse
	if err := request.Validate(); err != nil {
		return response, false, err
	}
	ok, err := c.postOptionalJSON(ctx, "/api/v1/recording/joined/token", c.bootstrapToken, request, &response)
	if err == nil && ok {
		err = response.Validate(time.Now().UTC())
		if err == nil && (response.BatchID != request.BatchID || !response.WorkScopeIdentity.Equal(request.WorkScopeIdentity)) {
			err = fmt.Errorf("joined bootstrap batch or work scope differs")
		}
	}
	return response, ok, err
}

func (c *joinedAPIClient) claimPublication(ctx context.Context, token string, request joinedrecording.WorkClaimRequest) (joinedrecording.PublicationClaimResponse, bool, error) {
	var response joinedrecording.PublicationClaimResponse
	if err := request.Validate(); err != nil {
		return response, false, err
	}
	ok, err := c.postOptionalJSON(ctx, "/api/v1/recording/joined/publication/claim", token, request, &response)
	if err == nil && ok {
		err = response.Validate(time.Now().UTC())
		if err == nil && publicationBatchID(response) != request.BatchID {
			err = fmt.Errorf("joined publication batch differs")
		}
	}
	return response, ok, err
}

func (c *joinedAPIClient) claimPreflight(ctx context.Context, token string, request joinedrecording.WorkClaimRequest) (joinedrecording.PreflightHourClaim, bool, error) {
	var response joinedrecording.PreflightHourClaim
	if err := request.Validate(); err != nil {
		return response, false, err
	}
	ok, err := c.postOptionalJSON(ctx, "/api/v1/recording/joined/claim", token, request, &response)
	if err == nil && ok {
		err = response.Validate(time.Now().UTC())
		if err == nil && response.BatchID != request.BatchID {
			err = fmt.Errorf("joined preflight batch differs")
		}
	}
	return response, ok, err
}

func (c *joinedAPIClient) reportFailure(ctx context.Context, token string, request joinedrecording.WorkFailureRequest) (joinedrecording.WorkFailureResponse, error) {
	var response joinedrecording.WorkFailureResponse
	if err := request.Validate(); err != nil {
		return response, err
	}
	if err := c.postJSON(ctx, "/api/v1/recording/joined/failure", token, request, &response); err != nil {
		return response, err
	}
	if response.ProtocolVersion != joinedrecording.JoinedProtocolVersion || response.AttemptCount < 1 ||
		(response.State != "retry" && response.State != "terminal") ||
		(response.State == "retry") != (response.NextAttemptAt != nil) {
		return response, errors.New("invalid joined failure response")
	}
	return response, nil
}

func (c *joinedAPIClient) leaseStatus(ctx context.Context, token string, request joinedrecording.LeaseStatusRequest) (joinedrecording.LeaseStatusResponse, error) {
	var response joinedrecording.LeaseStatusResponse
	if err := request.Validate(); err != nil {
		return response, err
	}
	if err := c.postJSON(ctx, "/api/v1/recording/joined/leases/status", token, request, &response); err != nil {
		return response, err
	}
	if response.ProtocolVersion != joinedrecording.JoinedProtocolVersion || len(response.Leases) != len(request.LeaseIDs) {
		return response, errors.New("invalid joined lease status response")
	}
	for i := range response.Leases {
		if response.Leases[i].LeaseID != request.LeaseIDs[i] || (response.Leases[i].Active && response.Leases[i].ExpiresAt == nil) {
			return response, errors.New("joined lease status response differs")
		}
	}
	return response, nil
}

func (c *joinedAPIClient) heartbeat(ctx context.Context, token string, request joinedrecording.HeartbeatRequest) (joinedrecording.HeartbeatResponse, error) {
	var response joinedrecording.HeartbeatResponse
	if err := request.Validate(); err != nil {
		return response, err
	}
	err := c.postJSON(ctx, "/api/v1/recording/joined/heartbeat", token, request, &response)
	if err == nil {
		err = response.Validate(time.Now().UTC())
		if err == nil && (response.ScopeKind != request.ScopeKind || response.ScopeID != request.ScopeID) {
			err = fmt.Errorf("joined heartbeat scope differs")
		}
	}
	return response, err
}

func (c *joinedAPIClient) sourceCapability(ctx context.Context, token string, request joinedrecording.SourceCapabilityRequest) (joinedrecording.SourceReadCapability, error) {
	var response joinedrecording.SourceReadCapability
	if err := request.Validate(); err != nil {
		return response, err
	}
	err := c.postJSON(ctx, "/api/v1/recording/joined/capabilities/source", token, request, &response)
	return response, err
}

func (c *joinedAPIClient) sealHour(ctx context.Context, token string, request joinedrecording.SealHourRequest) (joinedrecording.WorkerClaim, error) {
	var response joinedrecording.WorkerClaim
	err := c.postJSON(ctx, "/api/v1/recording/joined/hour/seal", token, request, &response)
	if err == nil {
		err = response.Validate(time.Now().UTC())
	}
	return response, err
}

func (c *joinedAPIClient) artifactCreateCapability(ctx context.Context, token string, request joinedrecording.ArtifactCapabilityRequest) (joinedrecording.ObjectCreateCapability, error) {
	var response joinedrecording.ObjectCreateCapability
	if err := request.Validate(); err != nil {
		return response, err
	}
	err := c.postJSON(ctx, "/api/v1/recording/joined/capabilities/artifact", token, request, &response)
	return response, err
}

func (c *joinedAPIClient) artifactReadCapability(ctx context.Context, token string, request joinedrecording.ArtifactCapabilityRequest) (joinedrecording.ObjectReadCapability, error) {
	var response joinedrecording.ObjectReadCapability
	if err := request.Validate(); err != nil {
		return response, err
	}
	err := c.postJSON(ctx, "/api/v1/recording/joined/capabilities/artifact", token, request, &response)
	return response, err
}

func (c *joinedAPIClient) finalizeLedger(ctx context.Context, token string, request joinedrecording.FinalizeLedgerRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	return c.postNoContent(ctx, "/api/v1/recording/joined/publication/ledger/finalize", token, request)
}

func (c *joinedAPIClient) finalizeHour(ctx context.Context, token string, request joinedrecording.FinalizeHourRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	return c.postNoContent(ctx, "/api/v1/recording/joined/publication/hour/finalize", token, request)
}

func (c *joinedAPIClient) finalizeBatchIndex(ctx context.Context, token string, request joinedrecording.FinalizeBatchIndexRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	return c.postNoContent(ctx, "/api/v1/recording/joined/publication/index/finalize", token, request)
}

func publicationBatchID(response joinedrecording.PublicationClaimResponse) string {
	switch response.Kind {
	case "ledger":
		return response.Ledger.BatchID
	case "hour":
		return response.Hour.Plan.BatchID
	case "batch_index":
		return response.BatchIndex.Index.BatchID
	default:
		return ""
	}
}

func (c *joinedAPIClient) postJSON(ctx context.Context, path, token string, request, response any) error {
	ok, err := c.postOptionalJSON(ctx, path, token, request, response)
	if err == nil && !ok {
		return fmt.Errorf("joined API %s unexpectedly returned no content", path)
	}
	return err
}

func (c *joinedAPIClient) putJSON(ctx context.Context, path, token string, payload, response any) error {
	body, err := marshalJoinedAPIRequest(payload)
	if err != nil {
		return fmt.Errorf("encode joined API request")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("construct joined API request")
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	httpResponse, err := c.httpClient.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("execute joined API %s", path)
	}
	defer httpResponse.Body.Close()
	if httpResponse.StatusCode < 200 || httpResponse.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(httpResponse.Body, joinedAPIResponseLimit))
		return fmt.Errorf("joined API %s returned status %d", path, httpResponse.StatusCode)
	}
	return decodeJoinedAPIResponse(httpResponse.Body, response)
}

func (c *joinedAPIClient) postNoContent(ctx context.Context, path, token string, request any) error {
	hasContent, err := c.postOptionalJSON(ctx, path, token, request, nil)
	if err != nil {
		return err
	}
	if hasContent {
		return fmt.Errorf("joined API %s unexpectedly returned content", path)
	}
	return nil
}

func (c *joinedAPIClient) postOptionalJSON(ctx context.Context, path, token string, payload, response any) (bool, error) {
	body, err := marshalJoinedAPIRequest(payload)
	if err != nil {
		return false, fmt.Errorf("encode joined API request")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return false, fmt.Errorf("construct joined API request")
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	httpResponse, err := c.httpClient.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		return false, &joinedAPITransportError{cause: err}
	}
	defer httpResponse.Body.Close()
	if httpResponse.StatusCode == http.StatusNoContent {
		return false, nil
	}
	if httpResponse.StatusCode < 200 || httpResponse.StatusCode >= 300 {
		return false, joinedAPIStatusError(path, httpResponse.StatusCode, httpResponse.Body)
	}
	if response == nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(httpResponse.Body, joinedAPIResponseLimit))
		return true, nil
	}
	if err := decodeJoinedAPIResponseWithLimit(httpResponse.Body, response, joinedAPIResponseLimitForPath(path)); err != nil {
		return false, fmt.Errorf("decode joined API %s response: %w", path, err)
	}
	return true, nil
}

func marshalJoinedAPIRequest(payload any) ([]byte, error) {
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(payload); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(body.Bytes(), []byte("\n")), nil
}

func joinedAPIStatusError(path string, status int, body io.Reader) error {
	raw, err := io.ReadAll(io.LimitReader(body, joinedAPIErrorLimit+1))
	if err != nil || len(raw) > joinedAPIErrorLimit {
		return &joinedAPIResponseError{path: path, status: status}
	}
	var payload struct {
		Error string `json:"error"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&payload) != nil || decoder.Decode(&struct{}{}) != io.EOF || strings.TrimSpace(payload.Error) == "" {
		return &joinedAPIResponseError{path: path, status: status}
	}
	return &joinedAPIResponseError{path: path, status: status, message: payload.Error}
}

func (c *joinedAPIClient) getJSON(ctx context.Context, path, token string, response any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return fmt.Errorf("construct joined API request")
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Accept", "application/json")
	httpResponse, err := c.httpClient.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("execute joined API status")
	}
	defer httpResponse.Body.Close()
	if httpResponse.StatusCode < 200 || httpResponse.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(httpResponse.Body, joinedAPIResponseLimit))
		return fmt.Errorf("joined API status returned status %d", httpResponse.StatusCode)
	}
	return decodeJoinedAPIResponse(httpResponse.Body, response)
}

func decodeJoinedAPIResponse(body io.Reader, response any) error {
	return decodeJoinedAPIResponseWithLimit(body, response, joinedAPIResponseLimit)
}

func joinedAPIResponseLimitForPath(path string) int64 {
	switch path {
	case "/api/v1/recording/joined/hour/seal", "/api/v1/recording/joined/publication/claim":
		return joinedLargeHourAPIResponseLimit
	default:
		return joinedAPIResponseLimit
	}
}

func decodeJoinedAPIResponseWithLimit(body io.Reader, response any, limit int64) error {
	raw, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil || int64(len(raw)) > limit {
		return fmt.Errorf("joined API response exceeds limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(response); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("joined API response has trailing data")
	}
	return nil
}
