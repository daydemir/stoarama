package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
)

const (
	joinedAPITimeout       = 60 * time.Second
	joinedWorkerIdlePoll   = 2 * time.Second
	joinedWorkerTaskLimit  = 2 * time.Hour
	joinedAPIResponseLimit = 1 << 20
)

type joinedAPIClient struct {
	baseURL        string
	bootstrapToken string
	httpClient     *http.Client
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
	CanaryHourIDs   []string         `json:"canary_hour_ids"`
	Hours           map[string]int64 `json:"hours"`
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
	localTestHTTP := parsed.Scheme == "http" && (parsed.Hostname() == "127.0.0.1" || parsed.Hostname() == "localhost" || parsed.Hostname() == "::1")
	if err != nil || (parsed.Scheme != "https" && !localTestHTTP) || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "" {
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
	return nil
}

func validateJoinedWorkerStatus(cfg config.Config, batchID string, status joinedWorkerStatus) error {
	localHours, err := cfg.JoinedCanaryHourIDs()
	if err != nil || !status.Enabled || status.ProtocolVersion != joinedrecording.JoinedProtocolVersion ||
		status.BatchID != batchID || batchID != cfg.JoinedRecordingBatchID ||
		strings.Join(status.CanaryHourIDs, ",") != strings.Join(localHours, ",") {
		return errors.New("joined backend batch or canary scope differs")
	}
	return nil
}

func (s *remoteJoinedOperatorService) RunWorker(ctx context.Context, req joinedWorkerRequest) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		worked, err := s.runWorkerOnce(ctx, req)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if worked {
			continue
		}
		timer := time.NewTimer(s.idlePoll)
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
	bootstrap, ok, err := s.api.bootstrap(ctx, req.BatchID)
	if err != nil || !ok {
		return false, err
	}
	claimRequest := joinedrecording.WorkClaimRequest{ProtocolVersion: joinedrecording.JoinedProtocolVersion, BatchID: req.BatchID, WorkerID: req.WorkerID}
	publication, ok, err := s.api.claimPublication(ctx, bootstrap.ClaimToken, claimRequest)
	if err != nil {
		return false, err
	}
	if ok {
		if err := joinedPublicationWithinCanary(s.cfg, publication); err != nil {
			return false, err
		}
		return true, runJoinedWorkerTask(ctx, joinedWorkerTaskLimit, func(taskCtx context.Context) error {
			return s.processClaim(taskCtx, publication, req.ScratchRoot)
		})
	}
	preflight, ok, err := s.api.claimPreflight(ctx, bootstrap.ClaimToken, claimRequest)
	if err != nil || !ok {
		return false, err
	}
	if !joinedHourWithinCanary(s.cfg, preflight.HourID) {
		return false, errors.New("joined preflight claim is outside configured canary scope")
	}
	return true, runJoinedWorkerTask(ctx, joinedWorkerTaskLimit, func(taskCtx context.Context) error {
		return s.processPreflight(taskCtx, preflight, req.ScratchRoot)
	})
}

func joinedHourWithinCanary(cfg config.Config, hourID string) bool {
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

func joinedPublicationWithinCanary(cfg config.Config, response joinedrecording.PublicationClaimResponse) error {
	switch response.Kind {
	case "hour":
		if response.Hour != nil && joinedHourWithinCanary(cfg, response.Hour.HourID) {
			return nil
		}
	case "ledger":
		if response.Ledger != nil {
			ledger := response.Ledger.Ledger
			for _, hour := range ledger.Hours {
				hourID, err := joinedrecording.CanonicalHourID(ledger.BatchID, ledger.RecordingID, ledger.LocalDate,
					hour.DeliveryHour, ledger.Generation)
				if err == nil && joinedHourWithinCanary(cfg, hourID) {
					return nil
				}
			}
		}
	case "batch_index":
		return errors.New("joined batch index is not eligible during the exact-hour canary")
	}
	return errors.New("joined publication claim is outside configured canary scope")
}

func runJoinedWorkerTask(ctx context.Context, limit time.Duration, work func(context.Context) error) error {
	if limit <= 0 || work == nil {
		return errors.New("joined worker task limit is required")
	}
	taskCtx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	return work(taskCtx)
}

func (s *remoteJoinedOperatorService) preflightAndPublish(ctx context.Context, claim joinedrecording.PreflightHourClaim, scratchRoot string) error {
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
		return joinedrecording.OperationCredentials{LeaseID: response.LeaseID, OperationToken: response.OperationToken, ExpiresAt: response.ExpiresAt}, nil
	}
}

func (s *remoteJoinedOperatorService) hourHeartbeat(hourID string) joinedrecording.HeartbeatOperation {
	return s.scopeHeartbeat("hour", hourID)
}

func (s *remoteJoinedOperatorService) artifactCreateCapability(ctx context.Context, kind, id string, artifactID int64, token string) (joinedrecording.ObjectCreateCapability, error) {
	return s.api.artifactCreateCapability(ctx, token, joinedrecording.ArtifactCapabilityRequest{ProtocolVersion: joinedrecording.JoinedProtocolVersion, ScopeKind: kind, ScopeID: id, ArtifactID: artifactID, Operation: "put"})
}

func (s *remoteJoinedOperatorService) artifactReadCapability(ctx context.Context, kind, id string, artifactID int64, token string) (joinedrecording.ObjectReadCapability, error) {
	return s.api.artifactReadCapability(ctx, token, joinedrecording.ArtifactCapabilityRequest{ProtocolVersion: joinedrecording.JoinedProtocolVersion, ScopeKind: kind, ScopeID: id, ArtifactID: artifactID, Operation: "read"})
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

func (c *joinedAPIClient) bootstrap(ctx context.Context, batchID string) (joinedrecording.WorkerBootstrapResponse, bool, error) {
	if strings.TrimSpace(c.bootstrapToken) == "" {
		return joinedrecording.WorkerBootstrapResponse{}, false, errors.New("joined worker bootstrap token is required")
	}
	request := joinedrecording.WorkerBootstrapRequest{ProtocolVersion: joinedrecording.JoinedProtocolVersion, BatchID: batchID}
	var response joinedrecording.WorkerBootstrapResponse
	if err := request.Validate(); err != nil {
		return response, false, err
	}
	ok, err := c.postOptionalJSON(ctx, "/api/v1/recording/joined/token", c.bootstrapToken, request, &response)
	if err == nil && ok {
		err = response.Validate(time.Now().UTC())
		if err == nil && response.BatchID != batchID {
			err = fmt.Errorf("joined bootstrap batch differs")
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
	body, err := json.Marshal(payload)
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
		return false, fmt.Errorf("execute joined API %s", path)
	}
	defer httpResponse.Body.Close()
	if httpResponse.StatusCode == http.StatusNoContent {
		return false, nil
	}
	if httpResponse.StatusCode < 200 || httpResponse.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(httpResponse.Body, joinedAPIResponseLimit))
		return false, fmt.Errorf("joined API %s returned status %d", path, httpResponse.StatusCode)
	}
	if response == nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(httpResponse.Body, joinedAPIResponseLimit))
		return true, nil
	}
	if err := decodeJoinedAPIResponse(httpResponse.Body, response); err != nil {
		return false, fmt.Errorf("decode joined API %s response: %w", path, err)
	}
	return true, nil
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
	raw, err := io.ReadAll(io.LimitReader(body, joinedAPIResponseLimit+1))
	if err != nil || len(raw) > joinedAPIResponseLimit {
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
