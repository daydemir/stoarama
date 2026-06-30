// Package recordingworker is the recorder droplet's job loop: lease a clip job,
// SSRF-re-check the URL, capture it with ffmpeg, upload it to the user's bucket
// via an API-presigned PUT, and ingest the metadata. It owns no S3 credentials.
package recordingworker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/netguard"
	"github.com/daydemir/stoarama/backend/internal/recordingapi"
)

type Config struct {
	Client       *recordingapi.Client
	WorkerID     string
	Concurrency  int
	HeartbeatSec int
	PollInterval time.Duration
}

type Worker struct {
	cfg          Config
	heartbeatInt time.Duration
}

func NewWorker(cfg Config) (*Worker, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("client is required")
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.HeartbeatSec <= 0 {
		cfg.HeartbeatSec = 15
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 5 * time.Second
	}
	return &Worker{cfg: cfg, heartbeatInt: time.Duration(cfg.HeartbeatSec) * time.Second}, nil
}

// Run polls for due jobs and processes up to Concurrency at a time until ctx is
// canceled.
func (w *Worker) Run(ctx context.Context) error {
	log.Printf("recording worker start worker_id=%s concurrency=%d poll=%s heartbeat=%s",
		w.cfg.WorkerID, w.cfg.Concurrency, w.cfg.PollInterval, w.heartbeatInt)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, w.cfg.Concurrency)
	var wg sync.WaitGroup

	// Independent droplet-heartbeat ticker (SRE-drain-liveness): touch droplet
	// liveness every HeartbeatSec regardless of whether a job is held, so an idle
	// worker is still seen as live by the autoscaler's failed-node detection.
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.dropletHeartbeatLoop(ctx)
	}()

	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()

	w.drain(ctx, sem, &wg)
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		case <-ticker.C:
			w.drain(ctx, sem, &wg)
		}
	}
}

// dropletHeartbeatLoop touches droplet liveness on a ticker until ctx is
// canceled. It runs whether or not a job is held.
func (w *Worker) dropletHeartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(w.heartbeatInt)
	defer ticker.Stop()
	if err := w.cfg.Client.TouchDroplet(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("recording worker droplet heartbeat error: %v", err)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.cfg.Client.TouchDroplet(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("recording worker droplet heartbeat error: %v", err)
			}
		}
	}
}

// drain leases and dispatches jobs until either no job is due or the worker is
// at capacity.
func (w *Worker) drain(ctx context.Context, sem chan struct{}, wg *sync.WaitGroup) {
	for {
		select {
		case sem <- struct{}{}:
		default:
			return
		}
		if ctx.Err() != nil {
			<-sem
			return
		}
		job, err := w.cfg.Client.LeaseRecordingJob(ctx)
		if err != nil {
			<-sem
			if !errors.Is(err, context.Canceled) {
				log.Printf("recording worker lease error: %v", err)
			}
			return
		}
		if job == nil {
			<-sem
			return
		}
		wg.Add(1)
		go func(j recordingapi.RecordingJob) {
			defer wg.Done()
			defer func() { <-sem }()
			if j.Kind == "continuous_window" {
				w.processContinuousJob(ctx, j)
				return
			}
			w.processJob(ctx, j)
		}(*job)
	}
}

// processJob runs the full capture pipeline for one job. It re-validates the URL
// against the SSRF guard immediately before ffmpeg (defeating DNS rebinding),
// runs a per-job heartbeat that can cancel the capture, and fails the job on any
// error so it is retried or surfaced.
func (w *Worker) processJob(ctx context.Context, job recordingapi.RecordingJob) {
	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	canceled := w.startHeartbeat(jobCtx, cancel, job.JobID)

	// Resolve the stored reference (e.g. a KBS '!hls' indirect URL) to a live
	// playable URL fresh on every capture, so an expiring token (the KBS Wowza
	// m3u8 token rolls every 24h) never breaks a schedule. A direct .m3u8 passes
	// through unchanged. The resolve fetch is SSRF-guarded inside ResolveCaptureInput.
	resolveCtx, resolveCancel := context.WithTimeout(jobCtx, 30*time.Second)
	sourceURL, isImage, err := capture.ResolveCaptureInput(resolveCtx, "", job.SourceURL, "")
	resolveCancel()
	if err != nil {
		w.fail(ctx, job.JobID, fmt.Errorf("resolve source url: %w", err))
		return
	}
	if isImage {
		w.fail(ctx, job.JobID, fmt.Errorf("image sources are not supported by the recorder"))
		return
	}

	// S-1: re-check the resolved URL right before ffmpeg (DNS-rebinding gate).
	// ValidatePublicURL rejects any host that resolves to a private/metadata
	// address. We then hand ffmpeg the original hostname URL (no host->IP
	// rewrite) so TLS SNI + Host routing work for SNI/Host-routed CDNs. The
	// TOCTOU window between this resolution and ffmpeg's own resolution is
	// covered by the droplet egress firewall, which REJECTs all traffic to
	// private/metadata ranges.
	if _, err := netguard.ValidatePublicURL(sourceURL); err != nil {
		w.fail(ctx, job.JobID, fmt.Errorf("ssrf guard rejected source url: %w", err))
		return
	}

	clipDuration := time.Duration(job.ClipDurationSec) * time.Second
	captureCtx, captureCancel := context.WithTimeout(jobCtx, capture.SegmentCaptureTimeout(clipDuration))
	seg, err := capture.CaptureSegment(captureCtx, sourceURL, clipDuration, "", job.TargetFPS)
	captureCancel()
	if err != nil {
		if canceled() {
			log.Printf("recording worker job=%d canceled during capture", job.JobID)
			return
		}
		w.fail(ctx, job.JobID, fmt.Errorf("capture clip: %w", err))
		return
	}
	defer capture.CleanupSegment(seg)

	if canceled() {
		log.Printf("recording worker job=%d canceled before upload", job.JobID)
		return
	}

	intent, err := w.cfg.Client.ReserveClipUpload(jobCtx, job.JobID, seg.MIMEType, 0)
	if err != nil {
		w.fail(ctx, job.JobID, fmt.Errorf("reserve clip upload: %w", err))
		return
	}
	if err := w.cfg.Client.UploadFile(jobCtx, intent.UploadURL, seg.Path, seg.MIMEType); err != nil {
		w.fail(ctx, job.JobID, fmt.Errorf("upload clip: %w", err))
		return
	}
	if _, err := w.cfg.Client.IngestClip(jobCtx, recordingapi.IngestClipRequest{
		IntentID:     intent.IntentID,
		JobID:        job.JobID,
		SizeBytes:    seg.SizeBytes,
		SHA256:       seg.SHA256,
		DurationMs:   seg.DurationMs,
		VideoCodec:   seg.VideoCodec,
		AudioCodec:   seg.AudioCodec,
		AudioPresent: seg.AudioPresent,
		ActualFPS:    seg.ActualFPS,
		Container:    seg.Container,
		ResolvedURL:  sourceURL,
		ClipStartAt:  seg.StartAt,
		ClipEndAt:    seg.EndAt,
	}); err != nil {
		w.fail(ctx, job.JobID, fmt.Errorf("ingest clip: %w", err))
		return
	}
	if err := w.cfg.Client.CompleteRecordingJob(ctx, job.JobID); err != nil {
		log.Printf("recording worker job=%d complete failed: %v", job.JobID, err)
		return
	}
	log.Printf("recording worker job=%d recording=%d clip captured size=%d", job.JobID, job.RecordingID, seg.SizeBytes)
}

// processContinuousJob runs ONE window-long lease: it resolves+SSRF-checks the
// URL once, holds one persistent ffmpeg open via CaptureContinuous for the whole
// window, and runs the EXISTING per-clip ingest path unchanged for each finalized
// segment. The same per-job heartbeat extends the lease for the whole window and
// can cancel (SIGINT) ffmpeg at window close. Each segment becomes one ordinary
// recording_clips row keyed on the segment start, so a re-leased window overwrites
// the same per-second keys (idempotent).
func (w *Worker) processContinuousJob(ctx context.Context, job recordingapi.RecordingJob) {
	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	canceled := w.startHeartbeat(jobCtx, cancel, job.JobID)

	// Resolve + SSRF-check ONCE for the whole window (identical to processJob).
	resolveCtx, resolveCancel := context.WithTimeout(jobCtx, 30*time.Second)
	sourceURL, isImage, err := capture.ResolveCaptureInput(resolveCtx, "", job.SourceURL, "")
	resolveCancel()
	if err != nil {
		w.fail(ctx, job.JobID, fmt.Errorf("resolve source url: %w", err))
		return
	}
	if isImage {
		w.fail(ctx, job.JobID, fmt.Errorf("image sources are not supported by the recorder"))
		return
	}
	if _, err := netguard.ValidatePublicURL(sourceURL); err != nil {
		w.fail(ctx, job.JobID, fmt.Errorf("ssrf guard rejected source url: %w", err))
		return
	}

	clipDuration := time.Duration(job.ClipDurationSec) * time.Second

	// Bound ffmpeg to the window close. The heartbeat/cancel path (window auto-stop
	// at end_at) also cancels jobCtx, which CaptureContinuous treats as a clean
	// shutdown (SIGINT + final sweep).
	windowCtx := jobCtx
	if job.WindowEndAt != nil && !job.WindowEndAt.IsZero() {
		var windowCancel context.CancelFunc
		windowCtx, windowCancel = context.WithDeadline(jobCtx, job.WindowEndAt.UTC())
		defer windowCancel()
	}

	outDir, err := os.MkdirTemp("", "capture-continuous-*")
	if err != nil {
		w.fail(ctx, job.JobID, fmt.Errorf("mktemp continuous outdir: %w", err))
		return
	}
	defer os.RemoveAll(outDir)

	onSegment := func(seg capture.Segment) error {
		if canceled() {
			return nil
		}
		segStartMs := seg.StartAt.UTC().UnixMilli()
		intent, err := w.cfg.Client.ReserveClipUpload(jobCtx, job.JobID, seg.MIMEType, segStartMs)
		if err != nil {
			return fmt.Errorf("reserve segment upload: %w", err)
		}
		if err := w.cfg.Client.UploadFile(jobCtx, intent.UploadURL, seg.Path, seg.MIMEType); err != nil {
			return fmt.Errorf("upload segment: %w", err)
		}
		if _, err := w.cfg.Client.IngestClip(jobCtx, recordingapi.IngestClipRequest{
			IntentID:     intent.IntentID,
			JobID:        job.JobID,
			SizeBytes:    seg.SizeBytes,
			SHA256:       seg.SHA256,
			DurationMs:   seg.DurationMs,
			VideoCodec:   seg.VideoCodec,
			AudioCodec:   seg.AudioCodec,
			AudioPresent: seg.AudioPresent,
			ActualFPS:    seg.ActualFPS,
			Container:    seg.Container,
			ResolvedURL:  sourceURL,
			ClipStartAt:  seg.StartAt,
			ClipEndAt:    seg.EndAt,
		}); err != nil {
			// A 409 means this exact segment key already ingested (a re-leased window
			// re-capturing the same wall-clock second). Treat as already-done so a
			// re-lease is idempotent rather than failing the whole window.
			if isAlreadyIngested(err) {
				capture.RemoveSegmentFile(seg)
				return nil
			}
			return fmt.Errorf("ingest segment: %w", err)
		}
		capture.RemoveSegmentFile(seg)
		log.Printf("recording worker job=%d recording=%d continuous segment ingested start=%s size=%d",
			job.JobID, job.RecordingID, seg.StartAt.UTC().Format(time.RFC3339), seg.SizeBytes)
		return nil
	}

	err = capture.CaptureContinuous(windowCtx, sourceURL, clipDuration, "", job.TargetFPS, outDir, onSegment)
	if canceled() {
		log.Printf("recording worker job=%d continuous canceled", job.JobID)
		return
	}
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		w.fail(ctx, job.JobID, fmt.Errorf("continuous capture: %w", err))
		return
	}
	if err := w.cfg.Client.CompleteRecordingJob(ctx, job.JobID); err != nil {
		log.Printf("recording worker job=%d continuous complete failed: %v", job.JobID, err)
		return
	}
	log.Printf("recording worker job=%d recording=%d continuous window complete", job.JobID, job.RecordingID)
}

// isAlreadyIngested reports whether an ingest error is the server's 409 dedup
// signal (a clip already exists for this object key), which for a re-leased
// continuous window means the segment is already stored and must not fail the job.
func isAlreadyIngested(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "status=409") || strings.Contains(msg, "already exists for this object key")
}

// startHeartbeat extends the lease on a ticker; on a cancel signal it cancels the
// job context (aborting ffmpeg). The returned func reports whether a cancel was
// observed, so the caller skips ingest for a canceled job.
func (w *Worker) startHeartbeat(ctx context.Context, cancel context.CancelFunc, jobID int64) func() bool {
	var mu sync.Mutex
	wasCanceled := false
	markCanceled := func() {
		mu.Lock()
		wasCanceled = true
		mu.Unlock()
		cancel()
	}
	go func() {
		ticker := time.NewTicker(w.heartbeatInt)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				cancelSignal, err := w.cfg.Client.HeartbeatRecordingJob(ctx, jobID)
				if err != nil {
					if !errors.Is(err, context.Canceled) {
						log.Printf("recording worker job=%d heartbeat error: %v", jobID, err)
					}
					continue
				}
				if cancelSignal {
					log.Printf("recording worker job=%d received cancel signal", jobID)
					markCanceled()
					return
				}
			}
		}
	}()
	return func() bool {
		mu.Lock()
		defer mu.Unlock()
		return wasCanceled
	}
}

func (w *Worker) fail(ctx context.Context, jobID int64, runErr error) {
	errText := "recording capture failed"
	if runErr != nil {
		errText = runErr.Error()
	}
	log.Printf("recording worker job=%d failed: %s", jobID, errText)
	// Use a fresh short-lived context so a canceled parent does not block the
	// fail report.
	failCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = ctx
	if err := w.cfg.Client.FailRecordingJob(failCtx, jobID, errText); err != nil {
		log.Printf("recording worker job=%d fail report failed: %v", jobID, err)
	}
}
