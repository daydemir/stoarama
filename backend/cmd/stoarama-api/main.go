package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/db"
	"github.com/daydemir/stoarama/backend/internal/email"
	"github.com/daydemir/stoarama/backend/internal/inferencebox"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

func main() {
	ctx, cancelRoot := context.WithCancel(context.Background())
	defer cancelRoot()
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	if err := cfg.ValidateAPI(); err != nil {
		log.Fatalf("invalid API config: %v", err)
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

	mailer, err := email.NewSender(email.Config{
		Provider:  cfg.ResearchEmailProvider,
		From:      cfg.ResearchEmailFrom,
		ReplyTo:   cfg.ResearchEmailReplyTo,
		ResendKey: cfg.ResearchEmailResendAPIKey,
	})
	if err != nil {
		log.Fatalf("init email sender: %v", err)
	}

	router, err := api.NewRouter(cfg, pool, r2c, mailer)
	if err != nil {
		log.Fatalf("build router: %v", err)
	}

	if cfg.BoxWorkerEmbedded {
		if cfg.InferenceBoxPollSec <= 0 || cfg.InferenceBoxConcurrency <= 0 || cfg.InferenceBoxLeaseSec <= 0 || cfg.InferenceBoxMaxAttempts <= 0 || cfg.InferenceBoxRetryBaseSec <= 0 || cfg.InferenceBoxRetryMaxSec <= 0 {
			log.Fatalf("invalid embedded box-worker config")
		}
		boxWorkerID := cfg.WorkerID + "-api-box"
		go func() {
			log.Printf(
				"stoarama-api embedded inference-box worker enabled worker_id=%s poll=%ds concurrency=%d",
				boxWorkerID, cfg.InferenceBoxPollSec, cfg.InferenceBoxConcurrency,
			)
			const restartDelay = 3 * time.Second
			for {
				if ctx.Err() != nil {
					return
				}
				mgr := inferencebox.NewManager(pool, r2c, inferencebox.ManagerConfig{
					WorkerID:      boxWorkerID,
					PollInterval:  time.Duration(cfg.InferenceBoxPollSec) * time.Second,
					MaxWorkers:    cfg.InferenceBoxConcurrency,
					LeaseDuration: time.Duration(cfg.InferenceBoxLeaseSec) * time.Second,
					MaxAttempts:   cfg.InferenceBoxMaxAttempts,
					RetryBase:     time.Duration(cfg.InferenceBoxRetryBaseSec) * time.Second,
					RetryMax:      time.Duration(cfg.InferenceBoxRetryMaxSec) * time.Second,
				})
				if err := mgr.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
					log.Printf("embedded inference-box worker exited: %v; restarting in %s", err, restartDelay)
					select {
					case <-ctx.Done():
						return
					case <-time.After(restartDelay):
					}
					continue
				}
				return
			}
		}()
	}

	httpSrv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           router,
		ReadTimeout:       20 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		log.Printf("stoarama-api listening on %s", httpSrv.Addr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	cancelRoot()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
}
