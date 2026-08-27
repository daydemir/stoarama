package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
)

type joinedFreezeTier1Request struct {
	ConnectionID          int64  `json:"connection_id"`
	BatchID               string `json:"batch_id"`
	Generation            int    `json:"generation"`
	SourceEndpoint        string `json:"source_endpoint"`
	QualificationRunID    int64  `json:"qualification_run_id"`
	ExpectedRequestSHA256 string `json:"expected_request_sha256,omitempty"`
	Apply                 bool   `json:"apply"`
}

// joinedTier1CheckpointedProgress is the server-owned resume cursor for the
// operator-only denominator dry run. The CLI never treats a failed step as
// committed; it rereads this record before choosing the next ordinal.
type joinedTier1CheckpointedProgress struct {
	RunID               string  `json:"run_id"`
	State               string  `json:"state"`
	CompletedRecordings int     `json:"completed_recordings"`
	ExpectedRecordings  int     `json:"expected_recordings"`
	NextPriorityOrdinal *int    `json:"next_priority_ordinal,omitempty"`
	RequestSHA256       *string `json:"request_sha256,omitempty"`
}

type joinedHistoricalQualificationJobs struct {
	RecordingID int64   `json:"recording_id"`
	JobIDs      []int64 `json:"job_ids"`
}

type joinedImportHistoricalQualificationRequest struct {
	ConnectionID          int64                               `json:"connection_id"`
	BatchID               string                              `json:"batch_id"`
	Generation            int                                 `json:"generation"`
	RecordingJobs         []joinedHistoricalQualificationJobs `json:"recording_jobs"`
	ExpectedRequestSHA256 string                              `json:"expected_request_sha256,omitempty"`
	Apply                 bool                                `json:"apply"`
}

type joinedSealStreamDayRequest struct {
	BatchID     string `json:"batch_id"`
	RecordingID int64  `json:"recording_id"`
	LocalDate   string `json:"local_date"`
	Apply       bool   `json:"-"`
}

type joinedFinalFreezeRequest struct {
	BatchID                         string `json:"batch_id"`
	ExpectedFrozenDenominatorSHA256 string `json:"expected_frozen_denominator_sha256"`
	Apply                           bool   `json:"-"`
}

type joinedFinalValidationProgress struct {
	RunID                     string  `json:"run_id"`
	BatchID                   string  `json:"batch_id"`
	State                     string  `json:"state"`
	CompletedScopes           int     `json:"completed_scopes"`
	ExpectedScopes            int     `json:"expected_scopes"`
	NextOrdinal               *int    `json:"next_ordinal,omitempty"`
	ReceiptSetSHA256          *string `json:"receipt_set_sha256,omitempty"`
	ExpectedDenominatorSHA256 string  `json:"expected_frozen_denominator_sha256"`
}

type joinedSealBatchIndexRequest struct {
	BatchID        string `json:"batch_id"`
	ExpectedSHA256 string `json:"expected_sha256"`
	Apply          bool   `json:"apply"`
}

type joinedSealRemainingDaysRequest struct {
	BatchID                         string `json:"batch_id"`
	CanaryRecordingID               int64  `json:"canary_recording_id"`
	CanaryLocalDate                 string `json:"canary_local_date"`
	ExpectedCanarySealRequestSHA256 string `json:"expected_canary_seal_request_sha256"`
	Apply                           bool   `json:"-"`
}

type joinedWorkerRequest struct {
	BatchID     string `json:"batch_id"`
	WorkerID    string `json:"worker_id"`
	ScratchRoot string `json:"scratch_root"`
}

type joinedStatusRequest struct {
	BatchID string `json:"batch_id"`
}

type joinedDeliveryStatusRequest struct {
	BatchID    string `json:"batch_id"`
	ArtifactID int64  `json:"artifact_id"`
}

type joinedAdmissionRequest struct {
	BatchID string
	Action  string
	Timeout time.Duration
}

// joinedOperatorService is the narrow boundary between operator parsing and
// the joined-recording lifecycle. The integration supplies its DB/media/API
// implementation without teaching the CLI about those clients.
type joinedOperatorService interface {
	ImportHistoricalQualification(context.Context, joinedImportHistoricalQualificationRequest) (any, error)
	FreezeTier1(context.Context, joinedFreezeTier1Request) (any, error)
	FreezeTier1Checkpointed(context.Context, joinedFreezeTier1Request) (any, error)
	SealStreamDay(context.Context, joinedSealStreamDayRequest) (any, error)
	SealRemainingDays(context.Context, joinedSealRemainingDaysRequest) (any, error)
	FinalValidation(context.Context, joinedFinalFreezeRequest) (any, error)
	FinalFreeze(context.Context, joinedFinalFreezeRequest) (any, error)
	SealBatchIndex(context.Context, joinedSealBatchIndexRequest) (any, error)
	ClaimAdmissionStatus(context.Context, string) (joinedrecording.ClaimAdmissionStatus, error)
	SetClaimAdmission(context.Context, joinedrecording.ClaimAdmissionRequest) (joinedrecording.ClaimAdmissionStatus, error)
	CheckWorkerStartup(context.Context, joinedWorkerRequest) error
	RunWorker(context.Context, joinedWorkerRequest) error
	Status(context.Context, joinedStatusRequest) (any, error)
	DeliveryStatus(context.Context, joinedDeliveryStatusRequest) (any, error)
}

type joinedOperatorFactory func(context.Context, config.Config) (joinedOperatorService, error)

func runRecordingJoined(ctx context.Context, cfg config.Config, args []string) {
	result, err := runRecordingJoinedCommand(ctx, cfg, args, newJoinedOperatorService)
	if err != nil {
		log.Fatalf("recording-joined: %v", err)
	}
	if result != nil {
		printJSON(result)
	}
}

func runRecordingJoinedCommand(ctx context.Context, cfg config.Config, args []string, factory joinedOperatorFactory) (any, error) {
	signalAware := len(args) >= 2 && ((args[0] == "worker" && args[1] == "run") || (args[0] == "admission" && args[1] == "drain"))
	if signalAware {
		var stop context.CancelFunc
		ctx, stop = signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
		defer stop()
	}
	result, err := runRecordingJoinedWith(ctx, cfg, args, factory)
	if err != nil {
		return nil, err
	}
	if joinedWorkerIsDisabled(result) {
		waitForJoinedWorkerShutdown(ctx)
	}
	return result, nil
}

func newJoinedOperatorService(_ context.Context, cfg config.Config) (joinedOperatorService, error) {
	return newRemoteJoinedOperatorService(cfg)
}

func joinedWorkerIsDisabled(result any) bool {
	status, ok := result.(map[string]any)
	return ok && status["enabled"] == false && status["status"] == "disabled"
}

func waitForJoinedWorkerShutdown(ctx context.Context) {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
}

func runRecordingJoinedWith(ctx context.Context, cfg config.Config, args []string, factory joinedOperatorFactory) (any, error) {
	if len(args) == 0 {
		return nil, errors.New("expected import-tier1-historical, freeze-tier1, seal-stream-day, seal-remaining-days, final-freeze, seal-batch-index, worker run, status, or delivery-status")
	}

	switch args[0] {
	case "admission":
		req, err := parseJoinedAdmission(cfg, args[1:])
		if err != nil {
			return nil, err
		}
		service, err := factory(ctx, cfg)
		if err != nil {
			return nil, err
		}
		switch req.Action {
		case "status":
			return service.ClaimAdmissionStatus(ctx, req.BatchID)
		case "refill-one":
			status, err := service.ClaimAdmissionStatus(ctx, req.BatchID)
			if err != nil {
				return nil, err
			}
			if !status.ClaimsPaused || status.OneShotClaimsRemaining != 0 || status.ActiveClaimsSHA256 == "" {
				return nil, errors.New("joined one-shot refill requires paused admission and an exact active-claim digest")
			}
			return service.SetClaimAdmission(ctx, joinedrecording.ClaimAdmissionRequest{
				ProtocolVersion: joinedrecording.JoinedProtocolVersion, BatchID: req.BatchID, ClaimsPaused: true,
				ExpectedActiveClaimsSHA256: status.ActiveClaimsSHA256, MaxNewClaims: 1,
			})
		case "pause", "resume":
			return service.SetClaimAdmission(ctx, joinedrecording.ClaimAdmissionRequest{ProtocolVersion: joinedrecording.JoinedProtocolVersion,
				BatchID: req.BatchID, ClaimsPaused: req.Action == "pause"})
		case "drain":
			status, err := service.SetClaimAdmission(ctx, joinedrecording.ClaimAdmissionRequest{ProtocolVersion: joinedrecording.JoinedProtocolVersion,
				BatchID: req.BatchID, ClaimsPaused: true})
			if err != nil || status.ActiveLeaseCount == 0 {
				return status, err
			}
			deadlineCtx, cancel := context.WithTimeout(ctx, req.Timeout)
			defer cancel()
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-deadlineCtx.Done():
					return nil, fmt.Errorf("joined admission remains paused with %d active leases: %w", status.ActiveLeaseCount, deadlineCtx.Err())
				case <-ticker.C:
					status, err = service.ClaimAdmissionStatus(deadlineCtx, req.BatchID)
					if err != nil {
						return nil, err
					}
					if !status.ClaimsPaused {
						return nil, errors.New("joined claim admission resumed before drain completed")
					}
					if status.ActiveLeaseCount == 0 {
						return status, nil
					}
				}
			}
		}
		return nil, errors.New("invalid joined admission action")
	case "import-tier1-historical":
		if err := requireJoinedActiveProtocol(cfg); err != nil {
			return nil, err
		}
		req, err := parseJoinedImportHistoricalQualification(cfg, args[1:])
		if err != nil {
			return nil, err
		}
		service, err := factory(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return service.ImportHistoricalQualification(ctx, req)
	case "freeze-tier1":
		if err := requireJoinedActiveProtocol(cfg); err != nil {
			return nil, err
		}
		req, err := parseJoinedFreezeTier1(cfg, args[1:])
		if err != nil {
			return nil, err
		}
		service, err := factory(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return service.FreezeTier1(ctx, req)
	case "freeze-tier1-checkpointed":
		if err := requireJoinedCheckpointProtocol(cfg); err != nil {
			return nil, err
		}
		req, err := parseJoinedFreezeTier1Checkpointed(cfg, args[1:])
		if err != nil {
			return nil, err
		}
		service, err := factory(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return service.FreezeTier1Checkpointed(ctx, req)
	case "seal-stream-day":
		if err := requireJoinedActiveProtocol(cfg); err != nil {
			return nil, err
		}
		req, err := parseJoinedSealStreamDay(cfg, args[1:])
		if err != nil {
			return nil, err
		}
		if !req.Apply {
			return map[string]any{"dry_run": true, "request": req}, nil
		}
		service, err := factory(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return service.SealStreamDay(ctx, req)
	case "seal-remaining-days":
		if err := requireJoinedActiveProtocol(cfg); err != nil {
			return nil, err
		}
		req, err := parseJoinedSealRemainingDays(cfg, args[1:])
		if err != nil {
			return nil, err
		}
		service, err := factory(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return service.SealRemainingDays(ctx, req)
	case "final-freeze":
		if err := requireJoinedActiveProtocol(cfg); err != nil {
			return nil, err
		}
		req, err := parseJoinedFinalFreeze(cfg, args[1:])
		if err != nil {
			return nil, err
		}
		if !req.Apply {
			return map[string]any{"dry_run": true, "request": req}, nil
		}
		service, err := factory(ctx, cfg)
		if err != nil {
			return nil, err
		}
		validation, err := service.FinalValidation(ctx, req)
		if err != nil {
			return nil, err
		}
		frozen, err := service.FinalFreeze(ctx, req)
		if err != nil {
			return nil, err
		}
		return map[string]any{"validation": validation, "final_freeze": frozen}, nil
	case "seal-batch-index":
		if err := requireJoinedActiveProtocol(cfg); err != nil {
			return nil, err
		}
		req, err := parseJoinedSealBatchIndex(cfg, args[1:])
		if err != nil {
			return nil, err
		}
		service, err := factory(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return service.SealBatchIndex(ctx, req)
	case "worker":
		if len(args) < 2 || args[1] != "run" {
			return nil, errors.New("expected worker run")
		}
		if cfg.JoinedRecordingRollingEnabled && !cfg.JoinedRecordingEnabled {
			return nil, errors.New("JOINED_RECORDING_ROLLING_ENABLED requires JOINED_RECORDING_ENABLED")
		}
		if !cfg.JoinedRecordingEnabled {
			return map[string]any{"enabled": false, "status": "disabled"}, nil
		}
		req, err := parseJoinedWorker(cfg, args[2:])
		if err != nil {
			return nil, err
		}
		workerCfg := cfg
		workerCfg.JoinedRecordingBatchID = req.BatchID
		workerCfg.JoinedRecordingScratchRoot = req.ScratchRoot
		if err := workerCfg.ValidateJoinedRecording(); err != nil {
			return nil, err
		}
		service, err := factory(ctx, workerCfg)
		if err != nil {
			return nil, err
		}
		if err := service.CheckWorkerStartup(ctx, req); err != nil {
			return nil, fmt.Errorf("startup check: %w", err)
		}
		if err := service.RunWorker(ctx, req); err != nil {
			return nil, err
		}
		return map[string]any{"batch_id": req.BatchID, "status": "stopped"}, nil
	case "status":
		req, err := parseJoinedStatus(cfg, args[1:])
		if err != nil {
			return nil, err
		}
		service, err := factory(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return service.Status(ctx, req)
	case "delivery-status":
		req, err := parseJoinedDeliveryStatus(cfg, args[1:])
		if err != nil {
			return nil, err
		}
		service, err := factory(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return service.DeliveryStatus(ctx, req)
	default:
		return nil, fmt.Errorf("unknown subcommand %q", args[0])
	}
}

func parseJoinedAdmission(cfg config.Config, args []string) (joinedAdmissionRequest, error) {
	req := joinedAdmissionRequest{BatchID: cfg.JoinedRecordingBatchID, Timeout: joinedWorkerTaskLimit}
	if len(args) == 0 {
		return req, errors.New("expected admission status, pause, resume, refill-one, or drain")
	}
	req.Action = args[0]
	if req.Action != "status" && req.Action != "pause" && req.Action != "resume" && req.Action != "refill-one" && req.Action != "drain" {
		return req, fmt.Errorf("unknown admission action %q", req.Action)
	}
	flags := newJoinedFlagSet("recording-joined admission " + req.Action)
	flags.StringVar(&req.BatchID, "batch-id", req.BatchID, "immutable batch identifier")
	timeoutSec := int(req.Timeout / time.Second)
	if req.Action == "drain" {
		flags.IntVar(&timeoutSec, "timeout-sec", timeoutSec, "maximum seconds to wait for zero active leases")
	}
	if err := parseJoinedFlags(flags, args[1:]); err != nil {
		return req, err
	}
	if err := validateJoinedBatchID(req.BatchID); err != nil {
		return req, err
	}
	if timeoutSec < 1 || timeoutSec > int(joinedWorkerTaskLimit/time.Second) {
		return req, fmt.Errorf("--timeout-sec must be between 1 and %d", int(joinedWorkerTaskLimit/time.Second))
	}
	req.Timeout = time.Duration(timeoutSec) * time.Second
	return req, nil
}

func parseJoinedImportHistoricalQualification(cfg config.Config, args []string) (joinedImportHistoricalQualificationRequest, error) {
	req := joinedImportHistoricalQualificationRequest{BatchID: joinedrecording.Tier1BatchID, Generation: 1}
	flags := newJoinedFlagSet("recording-joined import-tier1-historical")
	flags.Int64Var(&req.ConnectionID, "connection-id", 0, "NAS connection identifier")
	evidenceFile := flags.String("evidence-file", "", "strict JSON file containing the exact ordered 33x14 job-ID map")
	flags.StringVar(&req.ExpectedRequestSHA256, "expected-request-sha256", "", "request hash returned by dry-run")
	flags.BoolVar(&req.Apply, "apply", false, "create and freeze the historical authority")
	if err := parseJoinedFlags(flags, args); err != nil {
		return req, err
	}
	if req.ConnectionID <= 0 {
		return req, errors.New("exact Tier-1 connection is required")
	}
	var evidence struct {
		RecordingJobs []joinedHistoricalQualificationJobs `json:"recording_jobs"`
	}
	if err := decodeStrictJSONFile(strings.TrimSpace(*evidenceFile), &evidence); err != nil {
		return req, fmt.Errorf("read --evidence-file: %w", err)
	}
	req.RecordingJobs = evidence.RecordingJobs
	if len(req.RecordingJobs) != len(joinedrecording.Tier1RecordingIDs) {
		return req, errors.New("--evidence-file must contain exactly 33 recordings")
	}
	seenJobs := make(map[int64]bool, len(req.RecordingJobs)*14)
	for i, recording := range req.RecordingJobs {
		if recording.RecordingID != joinedrecording.Tier1RecordingIDs[i] || len(recording.JobIDs) != 14 {
			return req, errors.New("--evidence-file recording order or day cardinality differs")
		}
		for _, jobID := range recording.JobIDs {
			if jobID <= 0 || seenJobs[jobID] {
				return req, errors.New("--evidence-file job IDs must be unique positive integers")
			}
			seenJobs[jobID] = true
		}
	}
	if err := validateExpectedHash("--expected-request-sha256", req.ExpectedRequestSHA256, req.Apply); err != nil {
		return req, err
	}
	return req, nil
}

func requireJoinedActiveProtocol(cfg config.Config) error {
	if !cfg.JoinedRecordingEnabled || cfg.JoinedRecordingProtocolVersion != 1 {
		return errors.New("joined recording mutations require JOINED_RECORDING_ENABLED=true and JOINED_RECORDING_PROTOCOL_VERSION=1")
	}
	return nil
}

// The checkpoint is an operator-only control-plane operation. It must remain
// usable while the joined media worker is disabled; applying or claiming work
// continues to require the stronger active-worker guard above.
func requireJoinedCheckpointProtocol(cfg config.Config) error {
	if !cfg.JoinedRecordingControlPlaneEnabled || cfg.JoinedRecordingProtocolVersion != 1 {
		return errors.New("checkpointed Tier-1 dry-run requires JOINED_RECORDING_CONTROL_PLANE_ENABLED=true and JOINED_RECORDING_PROTOCOL_VERSION=1")
	}
	return nil
}

func validateJoinedFreezeTier1Fields(req joinedFreezeTier1Request) error {
	if req.ConnectionID <= 0 {
		return errors.New("--connection-id must be positive")
	}
	if err := validateJoinedBatchID(req.BatchID); err != nil {
		return err
	}
	if req.Generation <= 0 || !strings.HasSuffix(req.BatchID, fmt.Sprintf("-generation-%d", req.Generation)) {
		return errors.New("--batch-id must end with the exact --generation")
	}
	if _, err := joinedrecording.CanonicalSourceEndpointAuthority(req.SourceEndpoint); err != nil {
		return errors.New("--source-endpoint must be the exact supported R2 endpoint")
	}
	if req.QualificationRunID <= 0 {
		return errors.New("--qualification-run-id must be positive")
	}
	return nil
}

func parseJoinedFreezeTier1(cfg config.Config, args []string) (joinedFreezeTier1Request, error) {
	req := joinedFreezeTier1Request{BatchID: cfg.JoinedRecordingBatchID, Generation: 1}
	flags := newJoinedFlagSet("recording-joined freeze-tier1")
	flags.Int64Var(&req.ConnectionID, "connection-id", 0, "NAS connection identifier")
	flags.StringVar(&req.BatchID, "batch-id", req.BatchID, "immutable batch identifier")
	flags.IntVar(&req.Generation, "generation", req.Generation, "immutable batch generation")
	flags.StringVar(&req.SourceEndpoint, "source-endpoint", "", "exact frozen R2 endpoint")
	flags.Int64Var(&req.QualificationRunID, "qualification-run-id", 0, "immutable qualification run identifier")
	flags.StringVar(&req.ExpectedRequestSHA256, "expected-request-sha256", "", "request hash returned by dry-run")
	flags.BoolVar(&req.Apply, "apply", false, "freeze the cohort")
	if err := parseJoinedFlags(flags, args); err != nil {
		return req, err
	}
	if err := validateJoinedFreezeTier1Fields(req); err != nil {
		return req, err
	}
	if err := validateExpectedHash("--expected-request-sha256", req.ExpectedRequestSHA256, req.Apply); err != nil {
		return req, err
	}
	return req, nil
}

func parseJoinedFreezeTier1Checkpointed(cfg config.Config, args []string) (joinedFreezeTier1Request, error) {
	req := joinedFreezeTier1Request{BatchID: cfg.JoinedRecordingBatchID, Generation: 1}
	flags := newJoinedFlagSet("recording-joined freeze-tier1-checkpointed")
	flags.Int64Var(&req.ConnectionID, "connection-id", 0, "NAS connection identifier")
	flags.StringVar(&req.BatchID, "batch-id", req.BatchID, "immutable batch identifier")
	flags.IntVar(&req.Generation, "generation", req.Generation, "immutable batch generation")
	flags.StringVar(&req.SourceEndpoint, "source-endpoint", "", "exact frozen R2 endpoint")
	flags.Int64Var(&req.QualificationRunID, "qualification-run-id", 0, "immutable qualification run identifier")
	if err := parseJoinedFlags(flags, args); err != nil {
		return req, err
	}
	if err := validateJoinedFreezeTier1Fields(req); err != nil {
		return req, err
	}
	return req, nil
}

func parseJoinedSealStreamDay(cfg config.Config, args []string) (joinedSealStreamDayRequest, error) {
	req := joinedSealStreamDayRequest{BatchID: cfg.JoinedRecordingBatchID}
	flags := newJoinedFlagSet("recording-joined seal-stream-day")
	flags.StringVar(&req.BatchID, "batch-id", req.BatchID, "immutable batch identifier")
	flags.Int64Var(&req.RecordingID, "recording-id", 0, "frozen Tier-1 recording identifier")
	flags.StringVar(&req.LocalDate, "local-date", "", "frozen local date (YYYY-MM-DD)")
	flags.BoolVar(&req.Apply, "apply", false, "HEAD and seal this exact stream day")
	if err := parseJoinedFlags(flags, args); err != nil {
		return req, err
	}
	if err := validateJoinedBatchID(req.BatchID); err != nil {
		return req, err
	}
	if !slices.Contains(joinedrecording.Tier1RecordingIDs, req.RecordingID) {
		return req, errors.New("--recording-id must be in the frozen Tier-1 cohort")
	}
	date, err := time.Parse("2006-01-02", req.LocalDate)
	if err != nil || date.Format("2006-01-02") != req.LocalDate {
		return req, errors.New("--local-date must be YYYY-MM-DD")
	}
	return req, nil
}

func parseJoinedFinalFreeze(cfg config.Config, args []string) (joinedFinalFreezeRequest, error) {
	req := joinedFinalFreezeRequest{BatchID: cfg.JoinedRecordingBatchID}
	flags := newJoinedFlagSet("recording-joined final-freeze")
	flags.StringVar(&req.BatchID, "batch-id", req.BatchID, "immutable batch identifier")
	flags.StringVar(&req.ExpectedFrozenDenominatorSHA256, "expected-frozen-denominator-sha256", "", "apply-time frozen denominator hash")
	flags.BoolVar(&req.Apply, "apply", false, "freeze the fully sealed batch")
	if err := parseJoinedFlags(flags, args); err != nil {
		return req, err
	}
	if err := validateJoinedBatchID(req.BatchID); err != nil {
		return req, err
	}
	if err := validateExpectedHash("--expected-frozen-denominator-sha256", req.ExpectedFrozenDenominatorSHA256, req.Apply); err != nil {
		return req, err
	}
	return req, nil
}

func parseJoinedSealBatchIndex(cfg config.Config, args []string) (joinedSealBatchIndexRequest, error) {
	req := joinedSealBatchIndexRequest{BatchID: cfg.JoinedRecordingBatchID}
	flags := newJoinedFlagSet("recording-joined seal-batch-index")
	flags.StringVar(&req.BatchID, "batch-id", req.BatchID, "immutable batch identifier")
	flags.StringVar(&req.ExpectedSHA256, "expected-sha256", "", "canonical index hash returned by preview")
	flags.BoolVar(&req.Apply, "apply", false, "seal the canonical final batch index")
	if err := parseJoinedFlags(flags, args); err != nil {
		return req, err
	}
	if err := validateJoinedBatchID(req.BatchID); err != nil {
		return req, err
	}
	if err := validateExpectedHash("--expected-sha256", req.ExpectedSHA256, req.Apply); err != nil {
		return req, err
	}
	return req, nil
}

func parseJoinedSealRemainingDays(cfg config.Config, args []string) (joinedSealRemainingDaysRequest, error) {
	req := joinedSealRemainingDaysRequest{BatchID: cfg.JoinedRecordingBatchID}
	flags := newJoinedFlagSet("recording-joined seal-remaining-days")
	flags.StringVar(&req.BatchID, "batch-id", req.BatchID, "immutable batch identifier")
	flags.Int64Var(&req.CanaryRecordingID, "canary-recording-id", 0, "approved canary recording identifier")
	flags.StringVar(&req.CanaryLocalDate, "canary-local-date", "", "approved canary local date")
	flags.StringVar(&req.ExpectedCanarySealRequestSHA256, "expected-canary-seal-request-sha256", "", "approved canary receipt hash")
	flags.BoolVar(&req.Apply, "apply", false, "seal the other 461 stream days serially")
	if err := parseJoinedFlags(flags, args); err != nil {
		return req, err
	}
	if err := validateJoinedBatchID(req.BatchID); err != nil {
		return req, err
	}
	if !slices.Contains(joinedrecording.Tier1RecordingIDs, req.CanaryRecordingID) {
		return req, errors.New("--canary-recording-id must be in the frozen Tier-1 cohort")
	}
	date, err := time.Parse("2006-01-02", req.CanaryLocalDate)
	if err != nil || date.Format("2006-01-02") != req.CanaryLocalDate {
		return req, errors.New("--canary-local-date must be YYYY-MM-DD")
	}
	if err := validateExpectedHash("--expected-canary-seal-request-sha256", req.ExpectedCanarySealRequestSHA256, true); err != nil {
		return req, err
	}
	return req, nil
}

func parseJoinedWorker(cfg config.Config, args []string) (joinedWorkerRequest, error) {
	req := joinedWorkerRequest{
		BatchID:     cfg.JoinedRecordingBatchID,
		WorkerID:    defaultJoinedWorkerID(),
		ScratchRoot: cfg.JoinedRecordingScratchRoot,
	}
	flags := newJoinedFlagSet("recording-joined worker run")
	flags.StringVar(&req.BatchID, "batch-id", req.BatchID, "immutable batch identifier")
	flags.StringVar(&req.WorkerID, "worker-id", req.WorkerID, "stable worker identifier")
	flags.StringVar(&req.ScratchRoot, "scratch-root", req.ScratchRoot, "absolute scratch directory")
	if err := parseJoinedFlags(flags, args); err != nil {
		return req, err
	}
	if err := validateJoinedBatchID(req.BatchID); err != nil {
		return req, err
	}
	req.WorkerID = strings.TrimSpace(req.WorkerID)
	if req.WorkerID == "" || len(req.WorkerID) > 128 || strings.IndexFunc(req.WorkerID, func(r rune) bool { return r <= ' ' }) >= 0 {
		return req, errors.New("--worker-id must be 1-128 non-whitespace characters")
	}
	req.ScratchRoot = strings.TrimSpace(req.ScratchRoot)
	if !filepath.IsAbs(req.ScratchRoot) || filepath.Clean(req.ScratchRoot) != req.ScratchRoot || req.ScratchRoot == string(filepath.Separator) {
		return req, errors.New("--scratch-root must be a clean absolute non-root path")
	}
	return req, nil
}

func parseJoinedStatus(cfg config.Config, args []string) (joinedStatusRequest, error) {
	req := joinedStatusRequest{BatchID: cfg.JoinedRecordingBatchID}
	flags := newJoinedFlagSet("recording-joined status")
	flags.StringVar(&req.BatchID, "batch-id", req.BatchID, "immutable batch identifier")
	if err := parseJoinedFlags(flags, args); err != nil {
		return req, err
	}
	return req, validateJoinedBatchID(req.BatchID)
}

func parseJoinedDeliveryStatus(cfg config.Config, args []string) (joinedDeliveryStatusRequest, error) {
	req := joinedDeliveryStatusRequest{BatchID: cfg.JoinedRecordingBatchID}
	flags := newJoinedFlagSet("recording-joined delivery-status")
	flags.StringVar(&req.BatchID, "batch-id", req.BatchID, "immutable batch identifier")
	flags.Int64Var(&req.ArtifactID, "artifact-id", 0, "joined artifact identifier")
	if err := parseJoinedFlags(flags, args); err != nil {
		return req, err
	}
	if err := validateJoinedBatchID(req.BatchID); err != nil {
		return req, err
	}
	if req.ArtifactID <= 0 {
		return req, errors.New("--artifact-id must be a positive integer")
	}
	return req, nil
}

func newJoinedFlagSet(name string) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	return flags
}

func parseJoinedFlags(flags *flag.FlagSet, args []string) error {
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected argument %q", flags.Arg(0))
	}
	return nil
}

func validateJoinedBatchID(value string) error {
	if !joinedrecording.ValidBatchID(value) {
		return errors.New("--batch-id must use 1-63 lowercase letters, numbers, or hyphens")
	}
	return nil
}

func validateExpectedHash(flagName, value string, required bool) error {
	if value == "" && !required {
		return nil
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size || value != strings.ToLower(value) {
		return fmt.Errorf("%s must be a lowercase SHA-256 hex digest", flagName)
	}
	return nil
}

func defaultJoinedWorkerID() string {
	if value := strings.TrimSpace(os.Getenv("RENDER_INSTANCE_ID")); value != "" {
		return value
	}
	if value, err := os.Hostname(); err == nil && strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	return "joined-worker"
}
