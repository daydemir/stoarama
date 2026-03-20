package main

import (
	"context"
	"errors"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/db"
	"github.com/daydemir/stoarama/backend/internal/inferencebox"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	if err := cfg.ValidateWorker(); err != nil {
		log.Fatalf("invalid worker config: %v", err)
	}
	if cfg.InferenceBoxPollSec <= 0 {
		log.Fatalf("invalid BOX_WORKER_POLL_SEC: must be > 0")
	}
	if cfg.InferenceBoxConcurrency <= 0 {
		log.Fatalf("invalid BOX_WORKER_CONCURRENCY: must be > 0")
	}
	if cfg.InferenceBoxLeaseSec <= 0 {
		log.Fatalf("invalid BOX_WORKER_LEASE_SEC: must be > 0")
	}
	if cfg.InferenceBoxMaxAttempts <= 0 {
		log.Fatalf("invalid BOX_WORKER_MAX_ATTEMPTS: must be > 0")
	}
	if cfg.InferenceBoxRetryBaseSec <= 0 {
		log.Fatalf("invalid BOX_WORKER_RETRY_BASE_SEC: must be > 0")
	}
	if cfg.InferenceBoxRetryMaxSec <= 0 {
		log.Fatalf("invalid BOX_WORKER_RETRY_MAX_SEC: must be > 0")
	}

	pool, err := db.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer pool.Close()

	if cfg.AutoMigrate {
		if err := db.MigrateUp(ctx, pool, cfg.MigrationDir); err != nil {
			log.Fatalf("migrate up: %v", err)
		}
	}

	r2c, err := r2.New(ctx, r2.Config{
		AccountID: cfg.R2AccountID,
		AccessKey: cfg.R2AccessKeyID,
		SecretKey: cfg.R2SecretAccessKey,
		Region:    cfg.R2Region,
		Bucket:    cfg.R2Bucket,
		Endpoint:  cfg.R2Endpoint,
	})
	if err != nil {
		log.Fatalf("init r2 client: %v", err)
	}

	workerID := cfg.WorkerID
	if workerID == "" {
		workerID = "inference-box-worker-1"
	}
	mgr := inferencebox.NewManager(pool, r2c, inferencebox.ManagerConfig{
		WorkerID:      workerID,
		PollInterval:  time.Duration(cfg.InferenceBoxPollSec) * time.Second,
		MaxWorkers:    cfg.InferenceBoxConcurrency,
		LeaseDuration: time.Duration(cfg.InferenceBoxLeaseSec) * time.Second,
		MaxAttempts:   cfg.InferenceBoxMaxAttempts,
		RetryBase:     time.Duration(cfg.InferenceBoxRetryBaseSec) * time.Second,
		RetryMax:      time.Duration(cfg.InferenceBoxRetryMaxSec) * time.Second,
	})

	log.Printf(
		"inference-box-worker start worker_id=%s poll=%ds concurrency=%d lease=%ds max_attempts=%d retry_base=%ds retry_max=%ds",
		workerID,
		cfg.InferenceBoxPollSec,
		cfg.InferenceBoxConcurrency,
		cfg.InferenceBoxLeaseSec,
		cfg.InferenceBoxMaxAttempts,
		cfg.InferenceBoxRetryBaseSec,
		cfg.InferenceBoxRetryMaxSec,
	)
	if err := mgr.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("inference-box-worker run: %v", err)
	}
	log.Printf("inference-box-worker shutdown complete")
}
