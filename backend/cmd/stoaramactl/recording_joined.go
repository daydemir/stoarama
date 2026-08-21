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
	"strings"
	"syscall"

	"github.com/daydemir/stoarama/backend/internal/config"
)

type joinedFreezeTier1Request struct {
	ConnectionID           int64  `json:"connection_id"`
	BatchID                string `json:"batch_id"`
	ExpectedManifestSHA256 string `json:"expected_manifest_sha256,omitempty"`
	Apply                  bool   `json:"apply"`
}

type joinedWorkerRequest struct {
	BatchID     string `json:"batch_id"`
	WorkerID    string `json:"worker_id"`
	ScratchRoot string `json:"scratch_root"`
}

type joinedStatusRequest struct {
	BatchID string `json:"batch_id"`
}

type joinedFinalizeIndexRequest struct {
	BatchID                string `json:"batch_id"`
	ExpectedManifestSHA256 string `json:"expected_manifest_sha256,omitempty"`
	Apply                  bool   `json:"apply"`
}

// joinedOperatorService is the narrow boundary between operator parsing and
// the joined-recording lifecycle. The integration supplies its DB/media/API
// implementation without teaching the CLI about those clients.
type joinedOperatorService interface {
	FreezeTier1(context.Context, joinedFreezeTier1Request) (any, error)
	CheckWorkerStartup(context.Context, joinedWorkerRequest) error
	RunWorker(context.Context, joinedWorkerRequest) error
	Status(context.Context, joinedStatusRequest) (any, error)
	FinalizeIndex(context.Context, joinedFinalizeIndexRequest) (any, error)
}

type joinedOperatorFactory func(context.Context, config.Config) (joinedOperatorService, error)

func runRecordingJoined(ctx context.Context, cfg config.Config, args []string) {
	result, err := runRecordingJoinedWith(ctx, cfg, args, newJoinedOperatorService)
	if err != nil {
		log.Fatalf("recording-joined: %v", err)
	}
	if result != nil {
		printJSON(result)
	}
	if joinedWorkerIsDisabled(result) {
		waitForJoinedWorkerShutdown(ctx)
	}
}

// newJoinedOperatorService is intentionally an integration seam. Release B's
// cloud/DB slice replaces this body once its contracts land.
func newJoinedOperatorService(context.Context, config.Config) (joinedOperatorService, error) {
	return nil, errors.New("joined recording service is not integrated")
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
		return nil, errors.New("expected freeze-tier1, worker run, status, or finalize-index")
	}

	switch args[0] {
	case "freeze-tier1":
		req, err := parseJoinedFreezeTier1(cfg, args[1:])
		if err != nil {
			return nil, err
		}
		service, err := factory(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return service.FreezeTier1(ctx, req)
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
		service, err := factory(ctx, cfg)
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
	case "finalize-index":
		req, err := parseJoinedFinalizeIndex(cfg, args[1:])
		if err != nil {
			return nil, err
		}
		service, err := factory(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return service.FinalizeIndex(ctx, req)
	default:
		return nil, fmt.Errorf("unknown subcommand %q", args[0])
	}
}

func parseJoinedFreezeTier1(cfg config.Config, args []string) (joinedFreezeTier1Request, error) {
	var req joinedFreezeTier1Request
	flags := newJoinedFlagSet("recording-joined freeze-tier1")
	flags.Int64Var(&req.ConnectionID, "connection-id", 0, "NAS connection identifier")
	flags.StringVar(&req.BatchID, "batch-id", cfg.JoinedRecordingBatchID, "immutable batch identifier")
	flags.StringVar(&req.ExpectedManifestSHA256, "expected-manifest-sha256", "", "expected cohort manifest hash")
	flags.BoolVar(&req.Apply, "apply", false, "freeze the cohort")
	if err := parseJoinedFlags(flags, args); err != nil {
		return req, err
	}
	if req.ConnectionID <= 0 {
		return req, errors.New("--connection-id must be positive")
	}
	if err := validateJoinedBatchID(req.BatchID); err != nil {
		return req, err
	}
	if err := validateExpectedHash(req.ExpectedManifestSHA256, req.Apply); err != nil {
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

func parseJoinedFinalizeIndex(cfg config.Config, args []string) (joinedFinalizeIndexRequest, error) {
	req := joinedFinalizeIndexRequest{BatchID: cfg.JoinedRecordingBatchID}
	flags := newJoinedFlagSet("recording-joined finalize-index")
	flags.StringVar(&req.BatchID, "batch-id", req.BatchID, "immutable batch identifier")
	flags.StringVar(&req.ExpectedManifestSHA256, "expected-manifest-sha256", "", "expected batch-index hash")
	flags.BoolVar(&req.Apply, "apply", false, "publish the batch index")
	if err := parseJoinedFlags(flags, args); err != nil {
		return req, err
	}
	if err := validateJoinedBatchID(req.BatchID); err != nil {
		return req, err
	}
	if err := validateExpectedHash(req.ExpectedManifestSHA256, req.Apply); err != nil {
		return req, err
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
	if len(value) < 1 || len(value) > 63 || value[0] == '-' || value[len(value)-1] == '-' {
		return errors.New("--batch-id must use 1-63 lowercase letters, numbers, or hyphens")
	}
	for _, char := range value {
		if char != '-' && (char < 'a' || char > 'z') && (char < '0' || char > '9') {
			return errors.New("--batch-id must use 1-63 lowercase letters, numbers, or hyphens")
		}
	}
	return nil
}

func validateExpectedHash(value string, required bool) error {
	if value == "" && !required {
		return nil
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size || value != strings.ToLower(value) {
		return errors.New("--expected-manifest-sha256 must be a lowercase SHA-256 hex digest")
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
