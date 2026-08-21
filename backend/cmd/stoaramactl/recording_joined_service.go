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
	capabilityClient joinedrecording.CapabilityHTTPClient
	idlePoll         time.Duration
	processClaim     func(context.Context, joinedrecording.PublicationClaimResponse) error
	processPreflight func(context.Context, joinedrecording.PreflightHourClaim, string) error
}

func newRemoteJoinedOperatorService(cfg config.Config) (*remoteJoinedOperatorService, error) {
	api, err := newJoinedAPIClient(cfg.AppBaseURL, cfg.JoinedRecordingWorkerToken, nil)
	if err != nil {
		return nil, err
	}
	service := &remoteJoinedOperatorService{
		cfg:              cfg,
		api:              api,
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
	if bootstrapToken == "" {
		return nil, fmt.Errorf("joined worker bootstrap token is required")
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: joinedAPITimeout}
	} else {
		clone := *httpClient
		httpClient = &clone
	}
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &joinedAPIClient{baseURL: baseURL, bootstrapToken: bootstrapToken, httpClient: httpClient}, nil
}

func (s *remoteJoinedOperatorService) FreezeTier1(context.Context, joinedFreezeTier1Request) (any, error) {
	return nil, errors.New("joined Tier-1 freeze is an API operator action, not a media-worker action")
}

func (s *remoteJoinedOperatorService) FinalizeIndex(context.Context, joinedFinalizeIndexRequest) (any, error) {
	return nil, errors.New("joined final index freeze is an API operator action, not a media-worker action")
}

func (s *remoteJoinedOperatorService) Status(ctx context.Context, req joinedStatusRequest) (any, error) {
	var status map[string]any
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
	values, ok := status.(map[string]any)
	enabled, present := values["enabled"].(bool)
	if !ok || !present || !enabled {
		return fmt.Errorf("joined backend is not enabled")
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
		return true, s.processClaim(ctx, publication)
	}
	preflight, ok, err := s.api.claimPreflight(ctx, bootstrap.ClaimToken, claimRequest)
	if err != nil || !ok {
		return false, err
	}
	return true, s.processPreflight(ctx, preflight, req.ScratchRoot)
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

func (s *remoteJoinedOperatorService) publishClaim(ctx context.Context, response joinedrecording.PublicationClaimResponse) error {
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
		return fmt.Errorf("reclaimed sealed hour %q cannot be rebuilt from the current publication claim; refusing unverified publication", response.Hour.HourID)
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
	return c.postJSON(ctx, "/api/v1/recording/joined/publication/ledger/finalize", token, request, nil)
}

func (c *joinedAPIClient) finalizeHour(ctx context.Context, token string, request joinedrecording.FinalizeHourRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	return c.postJSON(ctx, "/api/v1/recording/joined/publication/hour/finalize", token, request, nil)
}

func (c *joinedAPIClient) finalizeBatchIndex(ctx context.Context, token string, request joinedrecording.FinalizeBatchIndexRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	return c.postJSON(ctx, "/api/v1/recording/joined/publication/index/finalize", token, request, nil)
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
