package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/recordingapi"
	"github.com/daydemir/stoarama/backend/internal/recordingworker"
)

const (
	relayMinLeaseFreeBytes  = 2 << 30
	relayMinActiveFreeBytes = 512 << 20
	relayLogMaxBytes        = 8 << 20
	relayLogTailBytes       = 64 << 10
)

// runRelay is the launchd/systemd service entrypoint. It runs the shared
// recordingworker loop with the relay-specific config (node:{id} lease owner,
// droplet heartbeat skipped, cookie-error classification on), points the shared
// capture/resolve.go at the installed yt-dlp/platform ffmpeg, and runs the node
// heartbeat and YouTube probe on separate goroutines.
func runRelay(ctx context.Context) error {
	startedAt := time.Now().UTC()
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	lock, err := acquireRelayRunLock()
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := refreshSystemdUnit(); err != nil {
		log.Printf("relay systemd unit refresh error: %v", err)
	}
	relayLog, err := openBoundedRelayLog()
	if err != nil {
		return err
	}
	previousLogWriter := log.Writer()
	defer relayLog.Close()
	defer log.SetOutput(previousLogWriter)
	log.SetOutput(relayLogOutput(previousLogWriter, relayLog))
	bd, err := binDir()
	if err != nil {
		return err
	}
	ytdlp := filepath.Join(bd, "yt-dlp")
	tempRoot, err := relayCaptureTempRoot()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(tempRoot, 0o700); err != nil {
		return fmt.Errorf("create relay capture temp root: %w", err)
	}
	removed, err := cleanupRelayCaptureTemp(tempRoot)
	if err != nil {
		log.Printf("clean relay capture temp root error: %v", err)
	}
	removed += cleanupLegacyCaptureTempBestEffort(os.TempDir(), time.Now(), 15*time.Minute)
	if removed > 0 {
		log.Printf("stoarama-relay removed %d stale capture directories", removed)
	}

	// Force UTC for this process AND every ffmpeg child it spawns. The capture path
	// names segments with a strftime pattern that ffmpeg expands in the local zone,
	// and the ingest handler parses those names as clip_start_at; without this a relay
	// on a non-UTC machine would emit clips whose timestamps are offset by the local
	// UTC offset, landing them outside the job window. Setting TZ in the process env
	// (inherited by exec'd ffmpeg) plus resetting time.Local keeps both sides in UTC.
	os.Setenv("TZ", "UTC")
	time.Local = time.UTC

	// Point the shared capture path at the installed yt-dlp and platform ffmpeg.
	// YouTube resolves COOKIELESS by default (applyCookieEnv leaves the cookie env unset unless the
	// experimental with-cookies opt-in is on); both cookie env vars are cleared here so
	// a stale value from the environment (or a leftover ~/.stoarama/cookies.txt) can
	// never leak in. --cookies-from-browser is never used in this headless path.
	os.Setenv("YT_DLP_BIN", ytdlp)
	os.Unsetenv("YT_DLP_COOKIES_FROM_BROWSER")
	os.Unsetenv("YT_DLP_COOKIES_FILE")
	os.Setenv("FFMPEG_BIN", relayFFmpegBin(bd))
	prependPath(bd) // ffprobe is resolved from PATH by the capture path

	client, err := recordingapi.NewClient(recordingapi.ClientConfig{
		BaseURL:   cfg.APIURL,
		NodeToken: cfg.NodeToken,
	})
	if err != nil {
		return fmt.Errorf("init recording api client: %w", err)
	}

	var activeJobs atomic.Int64
	relayDiag := &recordingworker.RelayDiagnostics{}
	worker, err := recordingworker.NewWorker(recordingworker.Config{
		Client:                      client,
		WorkerID:                    fmt.Sprintf("node:%d", cfg.NodeID),
		Concurrency:                 relayWorkerCeiling,
		HeartbeatSec:                15,
		PollInterval:                5 * time.Second,
		SkipDropletHeartbeat:        true,
		ClassifyYouTubeCookieErrors: true,
		ActiveJobs:                  &activeJobs,
		RelayDiagnostics:            relayDiag,
		ContinuousNoProgressTimeout: 5 * time.Minute,
		CaptureTempDir:              tempRoot,
		UploadWorkers:               config.RelayUploadWorkersFromEnv(),
		DiskFreeBytes: func() (uint64, error) {
			return diskFreeBytes(tempRoot)
		},
		MinLeaseFreeBytes:  relayMinLeaseFreeBytes,
		MinActiveFreeBytes: relayMinActiveFreeBytes,
	})
	if err != nil {
		return fmt.Errorf("init relay worker: %w", err)
	}

	// Startup probe (hard-timeout bounded) so the resolve env reflects reality before
	// the first job can be leased. The mode (cookieless default vs experimental
	// with-cookies) is decided HERE, ONCE, and set via applyCookieEnv before the worker
	// starts. It is never mutated again for the process lifetime: later probes only
	// update heartbeat visibility and do not touch the resolve env. A mode change takes
	// effect only across a process restart.
	pr := newProbe(ytdlp)
	selfUpdatesEnabled, err := prepareSelfUpdates(cfg)
	if err != nil {
		return fmt.Errorf("establish rollback baseline: %w", err)
	}
	firstHeartbeat := make(chan struct{})
	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		relayHeartbeatLoop(heartbeatCtx, client, pr, &activeJobs, relayDiag, startedAt, tempRoot, cfg.APIURL, firstHeartbeat)
	}()
	select {
	case <-ctx.Done():
		stopHeartbeat()
		<-heartbeatDone
		return ctx.Err()
	case <-firstHeartbeat:
	}
	pr.runOnce(ctx)
	pr.applyCookieEnv()
	log.Printf("stoarama-relay run node=%d worker_ceiling=%d api=%s youtube_ready=%t youtube_error=%q",
		cfg.NodeID, relayWorkerCeiling, cfg.APIURL, pr.ok(), pr.errorClass())

	go pr.runLoop(ctx)
	if selfUpdatesEnabled {
		go selfUpdateLoop(ctx, cfg)
	}

	err = worker.Run(ctx)
	stopHeartbeat()
	<-heartbeatDone
	if err == nil || ctx.Err() != nil {
		if path := recoveryStatePath(); path != "" {
			markRelayExit(relayExitClean)
		}
	} else {
		markRelayExit(relayExitProcessError)
	}
	return err
}

func prepareSelfUpdates(cfg relayConfig) (bool, error) {
	if strings.TrimSpace(releasePublicKeyBase64) == "" {
		log.Printf("relay self-update disabled: release public key unavailable")
		return false, nil
	}
	if err := ensureRollbackBaseline(cfg); err != nil {
		return false, err
	}
	return true, nil
}

type boundedRelayLog struct {
	mu   sync.Mutex
	file *os.File
}

type dualRelayLogWriter struct {
	primary  io.Writer
	recovery io.Writer
}

func relayLogOutput(primary io.Writer, recovery *boundedRelayLog) io.Writer {
	primaryFile, ok := primary.(*os.File)
	if ok {
		primaryInfo, primaryErr := primaryFile.Stat()
		recoveryInfo, recoveryErr := recovery.file.Stat()
		if primaryErr == nil && recoveryErr == nil && os.SameFile(primaryInfo, recoveryInfo) {
			return recovery
		}
	}
	return dualRelayLogWriter{primary: primary, recovery: recovery}
}

func (w dualRelayLogWriter) Write(p []byte) (int, error) {
	_, primaryErr := w.primary.Write(p)
	_, recoveryErr := w.recovery.Write(p)
	if err := errors.Join(primaryErr, recoveryErr); err != nil {
		return 0, err
	}
	return len(p), nil
}

func openBoundedRelayLog() (*boundedRelayLog, error) {
	home, err := stoaramaHome()
	if err != nil {
		return nil, err
	}
	path := relayLogPath(home)
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create relay log directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open relay log: %w", err)
	}
	return &boundedRelayLog{file: file}, nil
}

func relayLogPath(home string) string {
	if home == "" {
		return ""
	}
	return filepath.Join(home, "logs", "relay.log")
}

func (w *boundedRelayLog) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	info, err := w.file.Stat()
	if err != nil {
		return 0, err
	}
	if info.Size()+int64(len(p)) > relayLogMaxBytes {
		if len(p) >= relayLogMaxBytes {
			if err := w.file.Truncate(0); err != nil {
				return 0, err
			}
			if _, err := w.file.Write(p[len(p)-relayLogMaxBytes:]); err != nil {
				return 0, err
			}
			return len(p), nil
		}
		tailBytes := min(int64(relayLogTailBytes), info.Size(), int64(relayLogMaxBytes-len(p)))
		tail := make([]byte, tailBytes)
		if tailBytes > 0 {
			if _, err := w.file.ReadAt(tail, info.Size()-tailBytes); err != nil {
				return 0, err
			}
		}
		if err := w.file.Truncate(0); err != nil {
			return 0, err
		}
		if _, err := w.file.Seek(0, 0); err != nil {
			return 0, err
		}
		if _, err := w.file.Write(tail); err != nil {
			return 0, err
		}
	}
	return w.file.Write(p)
}

func (w *boundedRelayLog) Close() error {
	return w.file.Close()
}

func relayCaptureTempRoot() (string, error) {
	home, err := stoaramaHome()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, "tmp"), nil
}

func diskFreeBytes(path string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, err
	}
	return stat.Bavail * uint64(stat.Bsize), nil
}

func cleanupRelayCaptureTemp(root string) (int, error) {
	return cleanupRelayCaptureTempWith(root, os.RemoveAll)
}

func cleanupRelayCaptureTempWith(root string, remove func(string) error) (int, error) {
	// runRelay acquires the single-process lock before calling this function, so
	// every capture directory in the app-owned temp root belongs to an exited run.
	entries, err := os.ReadDir(root)
	if err != nil {
		return 0, err
	}
	removed := 0
	var errs []error
	for _, entry := range entries {
		if !entry.IsDir() || (!strings.HasPrefix(entry.Name(), "capture-continuous-") && !strings.HasPrefix(entry.Name(), "capture-segment-")) {
			continue
		}
		if err := remove(filepath.Join(root, entry.Name())); err != nil {
			errs = append(errs, fmt.Errorf("remove %s: %w", entry.Name(), err))
			continue
		}
		removed++
	}
	return removed, errors.Join(errs...)
}

func cleanupLegacyCaptureTemp(root string, now time.Time, staleAfter time.Duration) (int, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, entry := range entries {
		if !entry.IsDir() || (!strings.HasPrefix(entry.Name(), "capture-continuous-") && !strings.HasPrefix(entry.Name(), "capture-segment-")) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return removed, err
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Uid != uint32(os.Getuid()) || now.Sub(info.ModTime()) < staleAfter {
			continue
		}
		if err := os.RemoveAll(filepath.Join(root, entry.Name())); err != nil {
			return removed, err
		}
		removed++
	}
	return removed, nil
}

func cleanupLegacyCaptureTempBestEffort(root string, now time.Time, staleAfter time.Duration) int {
	removed, err := cleanupLegacyCaptureTemp(root, now, staleAfter)
	if err != nil {
		log.Printf("relay legacy temp cleanup skipped: %v", err)
		return 0
	}
	return removed
}

func acquireRelayRunLock() (*os.File, error) {
	home, err := stoaramaHome()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(home, 0o700); err != nil {
		return nil, err
	}
	path := filepath.Join(home, "relay.lock")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		file.Close()
		return nil, fmt.Errorf("acquire relay process lock: %w", err)
	}
	return file, nil
}

func relayFFmpegBin(binDir string) string {
	bundled := filepath.Join(binDir, "ffmpeg")
	system := "/usr/bin/ffmpeg"
	if runtime.GOOS == "linux" && runtime.GOARCH == "arm64" && executable(system) {
		return system
	}
	return bundled
}

func executable(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode()&0111 != 0
}

// prependPath puts dir at the front of PATH so the bundled ffprobe (and any other
// bundled tool resolved by name) is preferred over a system install.
func prependPath(dir string) {
	if cur := os.Getenv("PATH"); cur != "" {
		os.Setenv("PATH", dir+string(os.PathListSeparator)+cur)
		return
	}
	os.Setenv("PATH", dir)
}
