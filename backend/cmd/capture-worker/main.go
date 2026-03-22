package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/capturepersistent"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/db"
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

	refresh := time.Duration(cfg.CaptureTickSec) * time.Second
	if refresh <= 0 {
		refresh = 5 * time.Second
	}
	modeAllowlist := parseModeAllowlist(cfg.CaptureModeAllowlist)

	log.Printf(
		"capture-worker starting persistent mode worker_id=%s refresh=%s max_sessions=%d frame_queue_size=%d frame_enqueue_timeout=%ds frame_writers=%d",
		cfg.WorkerID, refresh, cfg.CaptureConcurrency, cfg.CaptureFrameQueueSize, cfg.CaptureFrameEnqueueTimeout, cfg.CaptureFrameWriters,
	)
	stopHeartbeat, err := startWorkerHeartbeatLoop(ctx, pool, cfg.WorkerID, modeAllowlist, cfg.CaptureConcurrency, cfg.CaptureLeaseSec)
	if err != nil {
		log.Fatalf("start worker heartbeat loop: %v", err)
	}
	defer stopHeartbeat()
	newManager := func() *capturepersistent.Manager {
		return capturepersistent.NewManager(pool, r2c, capturepersistent.ManagerConfig{
			WorkerID:                  cfg.WorkerID,
			RefreshInterval:           refresh,
			MaxSessions:               cfg.CaptureConcurrency,
			MaxFrameBytes:             25 << 20,
			FrameQueueSize:            cfg.CaptureFrameQueueSize,
			FrameEnqueueTimeout:       time.Duration(cfg.CaptureFrameEnqueueTimeout) * time.Second,
			FrameWriterWorkers:        cfg.CaptureFrameWriters,
			SegmentDuration:           time.Duration(cfg.CaptureSegmentDurationSec) * time.Second,
			SegmentTargetFPS:          cfg.CaptureSegmentTargetFPS,
			ModeAllowlist:             parseModeAllowlist(cfg.CaptureModeAllowlist),
			LeaseDuration:             time.Duration(cfg.CaptureLeaseSec) * time.Second,
			LeaseRenewInterval:        time.Duration(maxInt(5, cfg.CaptureLeaseSec/3)) * time.Second,
			UnsupportedErrorThreshold: cfg.CaptureUnsupportedThreshold,
		})
	}
	const restartDelay = 3 * time.Second
	for {
		if ctx.Err() != nil {
			break
		}
		if err := newManager().Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("capture manager run error: %v; restarting in %s", err, restartDelay)
			select {
			case <-ctx.Done():
				break
			case <-time.After(restartDelay):
			}
			continue
		}
		break
	}
	log.Printf("capture-worker shutdown complete")
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func parseModeAllowlist(raw string) []capture.Mode {
	parts := strings.Split(raw, ",")
	out := make([]capture.Mode, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		mode := capture.NormalizeMode(p)
		if mode == capture.ModeUnsupported {
			log.Printf("capture-worker ignoring invalid mode in CAPTURE_MODE_ALLOWLIST: %q", p)
			continue
		}
		out = append(out, mode)
	}
	return out
}

func defaultHeartbeatModes(modeAllowlist []capture.Mode) []capture.Mode {
	if len(modeAllowlist) > 0 {
		return modeAllowlist
	}
	return []capture.Mode{
		capture.ModeFFmpegDirect,
		capture.ModeHLSLive,
		capture.ModeImagePoll,
	}
}

func defaultServerID(workerID string) string {
	v := strings.TrimSpace(workerID)
	if v != "" {
		return strings.ToLower(v)
	}
	host, err := os.Hostname()
	if err == nil {
		host = strings.TrimSpace(host)
		if host != "" {
			if i := strings.IndexByte(host, '.'); i > 0 {
				host = host[:i]
			}
			return strings.ToLower(host)
		}
	}
	return "capture-worker"
}

func startWorkerHeartbeatLoop(ctx context.Context, pool dbExecer, workerID string, modeAllowlist []capture.Mode, capacity int, leaseSec int) (func(), error) {
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return nil, errors.New("worker_id is required")
	}
	if capacity <= 0 {
		return nil, errors.New("capacity must be > 0")
	}
	if leaseSec <= 0 {
		leaseSec = 45
	}
	modes := defaultHeartbeatModes(modeAllowlist)
	serverID := defaultServerID(workerID)
	heartbeatInterval := time.Duration(maxInt(5, leaseSec/3)) * time.Second

	stopCtx, stopCancel := context.WithCancel(context.Background())
	sendHeartbeat := func() error {
		callCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, mode := range modes {
			if mode == capture.ModeAuto || mode == capture.ModeUnsupported {
				continue
			}
			if err := upsertWorkerHeartbeat(callCtx, pool, workerID, serverID, mode, capacity, leaseSec); err != nil {
				return err
			}
		}
		return nil
	}
	if err := sendHeartbeat(); err != nil {
		stopCancel()
		return nil, err
	}
	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stopCtx.Done():
				return
			case <-ticker.C:
				if err := sendHeartbeat(); err != nil {
					log.Printf("capture-worker heartbeat error: %v", err)
				}
			}
		}
	}()
	return func() {
		stopCancel()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		for _, mode := range modes {
			if mode == capture.ModeAuto || mode == capture.ModeUnsupported {
				continue
			}
			if err := deleteWorkerHeartbeat(cleanupCtx, pool, workerID, mode); err != nil {
				log.Printf("capture-worker heartbeat cleanup error mode=%s: %v", mode, err)
			}
		}
	}, nil
}

type dbExecer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

func upsertWorkerHeartbeat(ctx context.Context, pool dbExecer, workerID, serverID string, mode capture.Mode, capacity int, leaseSec int) error {
	if _, err := pool.Exec(ctx, `
		INSERT INTO capture_worker_heartbeats (worker_id, execution_class, capacity, heartbeat_at, lease_expires_at, updated_at)
		VALUES ($1, $2, $3, now(), now() + make_interval(secs => $4), now())
		ON CONFLICT (worker_id, execution_class)
		DO UPDATE SET
			capacity=EXCLUDED.capacity,
			heartbeat_at=EXCLUDED.heartbeat_at,
			lease_expires_at=EXCLUDED.lease_expires_at,
			updated_at=now()
	`, workerID, string(mode), capacity, leaseSec); err != nil {
		return err
	}
	meta := map[string]any{
		"capacity":     capacity,
		"server_id":    serverID,
		"process_id":   workerID,
		"process_name": "capture-worker",
	}
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO processing_worker_heartbeats (
			worker_id, worker_kind, execution_class, pipeline_id, metadata_jsonb,
			heartbeat_at, lease_expires_at, updated_at
		)
		VALUES ($1, 'capture', $2, '', $3::jsonb, now(), now() + make_interval(secs => $4), now())
		ON CONFLICT (worker_id, worker_kind, execution_class, pipeline_id)
		DO UPDATE SET
			metadata_jsonb=EXCLUDED.metadata_jsonb,
			heartbeat_at=EXCLUDED.heartbeat_at,
			lease_expires_at=EXCLUDED.lease_expires_at,
			updated_at=now()
	`, workerID, string(mode), string(metaJSON), leaseSec); err != nil {
		return err
	}
	return nil
}

func deleteWorkerHeartbeat(ctx context.Context, pool dbExecer, workerID string, mode capture.Mode) error {
	if _, err := pool.Exec(ctx, `
		DELETE FROM capture_worker_heartbeats
		WHERE worker_id=$1 AND execution_class=$2
	`, workerID, string(mode)); err != nil {
		return err
	}
	if _, err := pool.Exec(ctx, `
		DELETE FROM processing_worker_heartbeats
		WHERE worker_id=$1 AND worker_kind='capture' AND execution_class=$2 AND pipeline_id=''
	`, workerID, string(mode)); err != nil {
		return err
	}
	return nil
}
