package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/db"
	"github.com/daydemir/stoarama/backend/internal/email"
	"github.com/daydemir/stoarama/backend/internal/inferencebox"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/survey"
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
		Provider:  cfg.EmailProvider,
		From:      cfg.EmailFrom,
		ReplyTo:   cfg.EmailReplyTo,
		ResendKey: cfg.EmailResendAPIKey,
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

	if cfg.SurveyEnabled {
		if cfg.SurveyConcurrency <= 0 || cfg.SurveyResolveTimeoutSec <= 0 || cfg.SurveyCaptureTimeoutSec <= 0 {
			log.Fatalf("invalid embedded survey config")
		}
		registry, err := capture.NewDefaultRegistry()
		if err != nil {
			log.Fatalf("init survey capture registry: %v", err)
		}
		resolveTimeout := time.Duration(cfg.SurveyResolveTimeoutSec) * time.Second
		captureTimeout := time.Duration(cfg.SurveyCaptureTimeoutSec) * time.Second
		go func() {
			log.Printf(
				"stoarama-api embedded survey scheduler enabled (one sweep/day at a randomized time) concurrency=%d",
				cfg.SurveyConcurrency,
			)
			for {
				now := time.Now().UTC()
				startOfDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
				next := startOfDay.Add(time.Duration(rand.Intn(86400)) * time.Second)
				if !next.After(now) {
					// today's randomized slot already passed; pick one tomorrow
					next = startOfDay.Add(24*time.Hour + time.Duration(rand.Intn(86400))*time.Second)
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Until(next)):
				}
				day := time.Now().UTC()
				targets, err := survey.SelectTargets(ctx, pool)
				if err != nil {
					log.Printf("survey scheduler: select targets failed: %v", err)
					continue
				}
				// Detection is disabled on the embedded API scheduler (nil detector):
				// yolo11x detection runs only on the dedicated survey+detection droplet
				// via `stoaramactl survey run-once --detect`. Here it is capture-only.
				res := survey.RunOnce(ctx, pool, r2c, registry, targets, day, cfg.SurveyConcurrency, resolveTimeout, captureTimeout, nil, 0, func(streamID int64, err error) {
					log.Printf("survey scheduler: stream %d capture failed: %v", streamID, err)
				})
				log.Printf("survey scheduler: day=%s total=%d success=%d skipped=%d failed=%d",
					day.Format("2006-01-02"), res.Total, res.Success, res.Skipped, res.Failed)
			}
		}()
	}

	if cfg.StreamAlertsEnabled {
		log.Printf("STREAM_ALERTS_ENABLED is deprecated in stoarama-api; run `stoaramactl recording supervisor run` for recording supervision and alert delivery")
	}

	// The standalone stream-recorder cron scheduler and droplet autoscaler do NOT
	// run on this public web dyno. They run on the dedicated single-instance
	// control service via `stoaramactl recorder-control run` (see render.yaml).
	if cfg.RecSchedEnabled {
		log.Printf("REC_SCHED_ENABLED is set but the recorder scheduler runs on the stoarama-recorder-control service, not stoarama-api; ignoring here")
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
