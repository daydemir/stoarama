// Package recordingworker is the recorder droplet's job loop: lease a clip job,
// SSRF-re-check the URL, capture it with ffmpeg, upload it to the user's bucket
// via an API-presigned PUT, and ingest the metadata. It owns no S3 credentials.
package recordingworker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/daydemir/stoarama/backend/internal/apihttp"
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

	// SkipDropletHeartbeat disables the recorder_droplets liveness touch loop.
	// Relay workers have no recorder_droplets row and report liveness through the
	// node heartbeat instead, so they set this true. The zero value (false) keeps
	// the cloud droplet heartbeat loop byte-identical.
	SkipDropletHeartbeat bool
	// ClassifyYouTubeCookieErrors, when true, rewrites a job-fail error_text to the
	// "youtube_cookie_expired" sentinel when the underlying failure is a genuine
	// YouTube sign-in / cookie-expiry failure (never a cookie-DB lock or a stale
	// extractor), so the relay UI can prompt a re-login. The zero value (false)
	// leaves the reported error_text byte-identical for cloud droplet workers.
	ClassifyYouTubeCookieErrors bool
	// ActiveJobs, when non-nil, is incremented while a job goroutine is in flight
	// and decremented when it returns, so the relay can report its live lease count
	// in the node heartbeat. The zero value (nil) is a no-op for cloud droplet
	// workers.
	ActiveJobs *atomic.Int64
	// RelayDiagnostics, when non-nil, is updated with non-secret job progress for
	// relay node heartbeats. Cloud droplet workers leave it nil.
	RelayDiagnostics *RelayDiagnostics
	// ContinuousNoProgressTimeout makes a relay surrender a continuous job after
	// this long without a successfully ingested segment. Zero keeps cloud workers
	// on their existing window-long retry behavior.
	ContinuousNoProgressTimeout time.Duration
}

type Worker struct {
	cfg               Config
	heartbeatInt      time.Duration
	leaseSafetyMargin time.Duration
}

var errSegmentDelivery = errors.New("segment delivery failed")

func NewWorker(cfg Config) (*Worker, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("client is required")
	}
	if cfg.ContinuousNoProgressTimeout > 0 && cfg.RelayDiagnostics == nil {
		return nil, fmt.Errorf("continuous no-progress timeout requires relay diagnostics")
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
	return &Worker{
		cfg:               cfg,
		heartbeatInt:      time.Duration(cfg.HeartbeatSec) * time.Second,
		leaseSafetyMargin: 5 * time.Second,
	}, nil
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
	if w.cfg.SkipDropletHeartbeat {
		return
	}
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
		if w.cfg.ActiveJobs != nil {
			w.cfg.ActiveJobs.Add(1)
		}
		go func(j recordingapi.RecordingJob) {
			defer wg.Done()
			defer func() { <-sem }()
			if w.cfg.ActiveJobs != nil {
				defer w.cfg.ActiveJobs.Add(-1)
			}
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
	w.cfg.RelayDiagnostics.Start(job)
	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	canceled := w.startHeartbeat(jobCtx, cancel, job.JobID, job.LeaseExpiresAt)

	// Resolve the stored reference (e.g. a KBS '!hls' indirect URL) to a live
	// playable URL fresh on every capture, so an expiring token (the KBS Wowza
	// m3u8 token rolls every 24h, and Skyline page tokens roll frequently) never
	// breaks a schedule. A direct .m3u8 passes through unchanged. The resolve fetch
	// is SSRF-guarded inside ResolveCaptureInput.
	w.cfg.RelayDiagnostics.Stage(job.JobID, "resolving")
	resolveCtx, resolveCancel := context.WithTimeout(jobCtx, 30*time.Second)
	sourceURL, isImage, inputHeaders, err := capture.ResolveCaptureInputWithHeaders(resolveCtx, job.StreamProvider, job.SourceURL, job.SourcePageURL)
	resolveCancel()
	if err != nil {
		w.cfg.RelayDiagnostics.Finish(job.JobID, "failed", fmt.Errorf("resolve source url: %w", err))
		w.fail(ctx, job.JobID, fmt.Errorf("resolve source url: %w", err))
		return
	}
	if isImage {
		err := fmt.Errorf("image sources are not supported by the recorder")
		w.cfg.RelayDiagnostics.Finish(job.JobID, "failed", err)
		w.fail(ctx, job.JobID, err)
		return
	}

	// S-1: re-check the resolved URL right before ffmpeg (DNS-rebinding gate).
	// ValidatePublicURL rejects any host that resolves to a private/metadata
	// address. We then hand ffmpeg the original hostname URL (no host->IP
	// rewrite) so TLS SNI + Host routing work for SNI/Host-routed CDNs. The
	// TOCTOU window between this resolution and ffmpeg's own resolution is
	// covered by the droplet egress firewall, which REJECTs all traffic to
	// private/metadata ranges.
	w.cfg.RelayDiagnostics.Stage(job.JobID, "ssrf_check")
	if _, err := netguard.ValidatePublicURL(sourceURL); err != nil {
		w.cfg.RelayDiagnostics.Finish(job.JobID, "failed", fmt.Errorf("ssrf guard rejected source url: %w", err))
		w.fail(ctx, job.JobID, fmt.Errorf("ssrf guard rejected source url: %w", err))
		return
	}

	clipDuration := time.Duration(job.ClipDurationSec) * time.Second
	w.cfg.RelayDiagnostics.Stage(job.JobID, "capturing")
	captureCtx, captureCancel := context.WithTimeout(jobCtx, capture.SegmentCaptureTimeout(clipDuration))
	seg, err := capture.CaptureSegmentWithHeaders(captureCtx, sourceURL, clipDuration, "", job.TargetFPS, inputHeaders)
	captureCancel()
	if err != nil {
		if canceled() {
			log.Printf("recording worker job=%d canceled during capture", job.JobID)
			w.cfg.RelayDiagnostics.Finish(job.JobID, "canceled", nil)
			return
		}
		w.cfg.RelayDiagnostics.Finish(job.JobID, "failed", fmt.Errorf("capture clip: %w", err))
		w.fail(ctx, job.JobID, fmt.Errorf("capture clip: %w", err))
		return
	}
	defer capture.CleanupSegment(seg)

	if canceled() {
		log.Printf("recording worker job=%d canceled before upload", job.JobID)
		w.cfg.RelayDiagnostics.Finish(job.JobID, "canceled", nil)
		return
	}

	w.cfg.RelayDiagnostics.Stage(job.JobID, "reserve_upload")
	intent, err := w.cfg.Client.ReserveClipUpload(jobCtx, job.JobID, seg.MIMEType, 0)
	if err != nil {
		w.cfg.RelayDiagnostics.Finish(job.JobID, "failed", fmt.Errorf("reserve clip upload: %w", err))
		w.fail(ctx, job.JobID, fmt.Errorf("reserve clip upload: %w", err))
		return
	}
	w.cfg.RelayDiagnostics.Stage(job.JobID, "uploading")
	if err := w.cfg.Client.UploadFile(jobCtx, intent.UploadURL, seg.Path, seg.MIMEType); err != nil {
		w.cfg.RelayDiagnostics.Finish(job.JobID, "failed", fmt.Errorf("upload clip: %w", err))
		w.fail(ctx, job.JobID, fmt.Errorf("upload clip: %w", err))
		return
	}
	w.cfg.RelayDiagnostics.Stage(job.JobID, "ingesting")
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
		w.cfg.RelayDiagnostics.Finish(job.JobID, "failed", fmt.Errorf("ingest clip: %w", err))
		w.fail(ctx, job.JobID, fmt.Errorf("ingest clip: %w", err))
		return
	}
	w.cfg.RelayDiagnostics.Segment(job.JobID, seg.StartAt)
	w.cfg.RelayDiagnostics.Stage(job.JobID, "completing")
	if err := w.cfg.Client.CompleteRecordingJob(ctx, job.JobID); err != nil {
		log.Printf("recording worker job=%d complete failed: %v", job.JobID, err)
		w.cfg.RelayDiagnostics.Finish(job.JobID, "complete_failed", err)
		return
	}
	w.cfg.RelayDiagnostics.Finish(job.JobID, "done", nil)
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
	w.cfg.RelayDiagnostics.Start(job)
	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	canceled := w.startHeartbeat(jobCtx, cancel, job.JobID, job.LeaseExpiresAt)

	clipDuration := time.Duration(job.ClipDurationSec) * time.Second

	// Bound ffmpeg to the window close. The heartbeat/cancel path (window auto-stop
	// at end_at) also cancels jobCtx, which CaptureContinuous treats as a clean
	// shutdown (SIGINT + final sweep). Created ONCE for the whole window (a nil/zero
	// WindowEndAt leaves windowCtx == jobCtx, so the job only ends on cancel).
	windowCtx := jobCtx
	if job.WindowEndAt != nil && !job.WindowEndAt.IsZero() {
		var windowCancel context.CancelFunc
		windowCtx, windowCancel = context.WithDeadline(jobCtx, job.WindowEndAt.UTC())
		defer windowCancel()
	}

	// sourceURL is re-resolved every supervisor attempt; onSegment records the URL
	// that produced the segment it is ingesting. segmentIngested flips true when a
	// segment is delivered in the current attempt, which the supervisor uses to reset
	// the reconnect backoff (a healthy attempt that later drops must not inherit a
	// grown delay).
	var sourceURL string
	var segmentIngested bool
	lastProgressAt := time.Now()
	onSegment := func(seg capture.Segment) error {
		if canceled() {
			return nil
		}
		segStartMs := seg.StartAt.UTC().UnixMilli()
		w.cfg.RelayDiagnostics.Stage(job.JobID, "segment_reserve_upload")
		intent, err := w.cfg.Client.ReserveClipUpload(jobCtx, job.JobID, seg.MIMEType, segStartMs)
		if err != nil {
			w.cfg.RelayDiagnostics.Error(job.JobID, "segment_reserve_upload_failed", err)
			return fmt.Errorf("%w: reserve segment upload: %v", errSegmentDelivery, err)
		}
		w.cfg.RelayDiagnostics.Stage(job.JobID, "segment_uploading")
		uploadCtx, uploadCancel := context.WithTimeout(jobCtx, recordingapi.UploadTimeout)
		err = uploadWithRetry(uploadCtx, job.JobID, func(attemptCtx context.Context) error {
			return w.cfg.Client.UploadFile(attemptCtx, intent.UploadURL, seg.Path, seg.MIMEType)
		}, func(err error, delay time.Duration) {
			w.cfg.RelayDiagnostics.Error(job.JobID, "segment_upload_retry", err)
			log.Printf("recording worker job=%d recording=%d segment upload failed: %v; retrying in %s",
				job.JobID, job.RecordingID, err, delay)
		})
		uploadCancel()
		if err != nil {
			w.cfg.RelayDiagnostics.Error(job.JobID, "segment_upload_failed", err)
			return fmt.Errorf("%w: upload segment: %v", errSegmentDelivery, err)
		}
		w.cfg.RelayDiagnostics.Stage(job.JobID, "segment_ingesting")
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
				segmentIngested = true
				lastProgressAt = time.Now()
				capture.RemoveSegmentFile(seg)
				w.cfg.RelayDiagnostics.Segment(job.JobID, seg.StartAt)
				return nil
			}
			w.cfg.RelayDiagnostics.Error(job.JobID, "segment_ingest_failed", err)
			return fmt.Errorf("%w: ingest segment: %v", errSegmentDelivery, err)
		}
		segmentIngested = true
		lastProgressAt = time.Now()
		capture.RemoveSegmentFile(seg)
		w.cfg.RelayDiagnostics.Segment(job.JobID, seg.StartAt)
		log.Printf("recording worker job=%d recording=%d continuous segment ingested start=%s size=%d",
			job.JobID, job.RecordingID, seg.StartAt.UTC().Format(time.RFC3339), seg.SizeBytes)
		return nil
	}

	// Supervisor loop: a live source can drop mid-window (an HLS end-of-stream
	// makes the persistent ffmpeg exit CLEANLY with hours left in the window). The
	// scheduler cannot re-enqueue (idempotency key is per window open), so we
	// resolve + capture in a loop and reconnect until the window closes. In-window
	// restarts must NOT consume attempt_count, so fail() is never called for a
	// resolve/capture drop here; the job only fails on a permanent misconfiguration.
	// Jittered exponential reconnect backoff gives transient drops a fast retry and
	// grows to a five-minute cap so a persistently dead source is not hammered,
	// while an attempt that ingested at least one clip resets failures to zero. The
	// sleep stays interruptible by windowCtx.Done() (window close / job cancel) just
	// like a fixed delay.
	failures := 0
	backoff := func(delay time.Duration) {
		select {
		case <-windowCtx.Done():
		case <-time.After(delay):
		}
	}
	for attempt := 1; ; attempt++ {
		if continuousShouldStop(canceled(), windowCtx.Err() != nil) {
			break
		}
		segmentIngested = false

		// Re-resolve EVERY attempt so expiring tokens are refreshed on reconnect.
		// A transient resolve error backs off and retries rather than failing the
		// job mid-window.
		w.cfg.RelayDiagnostics.Stage(job.JobID, "continuous_resolving")
		resolveCtx, resolveCancel := context.WithTimeout(windowCtx, 30*time.Second)
		resolved, isImage, inputHeaders, err := capture.ResolveCaptureInputWithHeaders(resolveCtx, job.StreamProvider, job.SourceURL, job.SourcePageURL)
		resolveCancel()
		if err != nil {
			if continuousShouldStop(canceled(), windowCtx.Err() != nil) {
				break
			}
			w.cfg.RelayDiagnostics.Error(job.JobID, "resolve_retry", err)
			failures++
			delay := reconnectBackoff(job.JobID, failures)
			log.Printf("recording worker job=%d recording=%d continuous resolve failed (attempt %d): %v; retrying in %s",
				job.JobID, job.RecordingID, attempt, err, delay)
			if w.surrenderContinuousJob(ctx, cancel, job, lastProgressAt) {
				return
			}
			backoff(continuousReconnectDelay(lastProgressAt, time.Now(), w.cfg.ContinuousNoProgressTimeout, delay))
			continue
		}
		if isImage {
			err := fmt.Errorf("image sources are not supported by the recorder")
			w.cfg.RelayDiagnostics.Finish(job.JobID, "failed", err)
			w.fail(ctx, job.JobID, err)
			return
		}
		// S-1: re-check the resolved URL right before ffmpeg (DNS-rebinding gate),
		// same call and same transient treatment as a resolve error.
		w.cfg.RelayDiagnostics.Stage(job.JobID, "continuous_ssrf_check")
		if _, err := netguard.ValidatePublicURL(resolved); err != nil {
			if continuousShouldStop(canceled(), windowCtx.Err() != nil) {
				break
			}
			w.cfg.RelayDiagnostics.Error(job.JobID, "ssrf_retry", err)
			failures++
			delay := reconnectBackoff(job.JobID, failures)
			log.Printf("recording worker job=%d recording=%d continuous ssrf guard rejected url (attempt %d): %v; retrying in %s",
				job.JobID, job.RecordingID, attempt, err, delay)
			if w.surrenderContinuousJob(ctx, cancel, job, lastProgressAt) {
				return
			}
			backoff(continuousReconnectDelay(lastProgressAt, time.Now(), w.cfg.ContinuousNoProgressTimeout, delay))
			continue
		}
		sourceURL = resolved

		// Fresh outDir per attempt, removed immediately after the attempt returns: a
		// previous attempt's leftover seg-*.mp4 would otherwise be re-finalized and
		// re-ingested by the next CaptureContinuous call.
		outDir, err := os.MkdirTemp("", "capture-continuous-*")
		if err != nil {
			w.cfg.RelayDiagnostics.Finish(job.JobID, "failed", fmt.Errorf("mktemp continuous outdir: %w", err))
			w.fail(ctx, job.JobID, fmt.Errorf("mktemp continuous outdir: %w", err))
			return
		}
		w.cfg.RelayDiagnostics.Stage(job.JobID, "continuous_capturing")
		captureErr := capture.CaptureContinuousWithHeaders(windowCtx, sourceURL, clipDuration, "", job.TargetFPS, outDir, onSegment, inputHeaders)
		os.RemoveAll(outDir)

		// Window close vs premature drop: CaptureContinuous returns nil on ctx.Done,
		// so windowCtx.Err() (NOT captureErr) is what distinguishes a real window
		// close/cancel from a premature clean ffmpeg exit (HLS end-of-stream).
		if continuousShouldStop(canceled(), windowCtx.Err() != nil) {
			break
		}
		// Premature exit (clean end-of-stream or a hard ffmpeg error) with the window
		// still open: back off and reconnect. An attempt that ingested at least one
		// clip was a healthy connection that later dropped, so reset the backoff.
		if segmentIngested {
			failures = 0
		}
		failures++
		delay := reconnectBackoff(job.JobID, failures)
		if captureErr != nil {
			w.cfg.RelayDiagnostics.Error(job.JobID, "capture_retry", captureErr)
		} else {
			w.cfg.RelayDiagnostics.Stage(job.JobID, "capture_retry")
		}
		log.Printf("recording worker job=%d recording=%d continuous source dropped (attempt %d): %v; reconnecting in %s",
			job.JobID, job.RecordingID, attempt, captureErr, delay)
		if !errors.Is(captureErr, errSegmentDelivery) && w.surrenderContinuousJob(ctx, cancel, job, lastProgressAt) {
			return
		}
		backoff(continuousReconnectDelay(lastProgressAt, time.Now(), w.cfg.ContinuousNoProgressTimeout, delay))
	}

	if canceled() {
		log.Printf("recording worker job=%d continuous canceled", job.JobID)
		w.cfg.RelayDiagnostics.Finish(job.JobID, "canceled", nil)
		return
	}
	w.cfg.RelayDiagnostics.Stage(job.JobID, "completing")
	if err := w.cfg.Client.CompleteRecordingJob(ctx, job.JobID); err != nil {
		log.Printf("recording worker job=%d continuous complete failed: %v", job.JobID, err)
		w.cfg.RelayDiagnostics.Finish(job.JobID, "complete_failed", err)
		return
	}
	w.cfg.RelayDiagnostics.Finish(job.JobID, "done", nil)
	log.Printf("recording worker job=%d recording=%d continuous window complete", job.JobID, job.RecordingID)
}

func (w *Worker) surrenderContinuousJob(ctx context.Context, cancel context.CancelFunc, job recordingapi.RecordingJob, lastProgressAt time.Time) bool {
	if !continuousNoProgressExpired(lastProgressAt, time.Now(), w.cfg.ContinuousNoProgressTimeout) {
		return false
	}
	err := fmt.Errorf("continuous relay made no progress for %s", w.cfg.ContinuousNoProgressTimeout)
	cancel()
	surrenderCtx, surrenderCancel := context.WithTimeout(ctx, 15*time.Second)
	defer surrenderCancel()
	if surrenderErr := w.cfg.Client.SurrenderRecordingJob(surrenderCtx, job.JobID, recordingapi.SurrenderNoProgress); surrenderErr != nil {
		w.cfg.RelayDiagnostics.Finish(job.JobID, "surrender_failed", surrenderErr)
		log.Printf("recording worker job=%d surrender failed: %v", job.JobID, surrenderErr)
		return true
	}
	w.cfg.RelayDiagnostics.Finish(job.JobID, "surrendered", err)
	return true
}

func continuousNoProgressExpired(lastProgressAt, now time.Time, timeout time.Duration) bool {
	return timeout > 0 && !now.Before(lastProgressAt.Add(timeout))
}

func continuousReconnectDelay(lastProgressAt, now time.Time, timeout, delay time.Duration) time.Duration {
	if timeout <= 0 {
		return delay
	}
	return min(delay, max(0, lastProgressAt.Add(timeout).Sub(now)))
}

// continuousShouldStop decides whether the continuous supervisor loop must stop
// (versus reconnect) after an attempt or a mid-window resolve/SSRF failure. It
// stops only when the job was canceled (window auto-stop at end_at) or the window
// context has closed; every other outcome is a mid-window drop that reconnects.
// It never signals "fail": in-window restarts must not consume attempt_count.
func continuousShouldStop(canceled, windowClosed bool) bool {
	return canceled || windowClosed
}

// reconnectBackoff returns a deterministic per-job jittered delay. It starts at
// 1-2s for fast recovery after a healthy source drop and grows to 2.5-5m for a
// persistently dead source without synchronizing every job against one origin.
func reconnectBackoff(jobID int64, failures int) time.Duration {
	const base = 2 * time.Second
	const maxDelay = 5 * time.Minute
	nominal := maxDelay
	if failures-1 < 8 {
		nominal = base << (failures - 1)
	}
	return jitteredDelay(jobID, failures, nominal)
}

func jitteredDelay(jobID int64, attempt int, nominal time.Duration) time.Duration {
	hash := uint64(jobID)*0x9e3779b97f4a7c15 ^ uint64(attempt)*0xbf58476d1ce4e5b9
	return nominal * time.Duration(50+hash%51) / 100
}

func uploadWithRetry(ctx context.Context, jobID int64, upload func(context.Context) error, onRetry func(error, time.Duration)) error {
	const attempts = 3
	const attemptTimeout = 90 * time.Second
	for attempt := 1; ; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
		err := upload(attemptCtx)
		cancel()
		if err == nil || attempt == attempts || !retryableUploadError(ctx, err) {
			return err
		}
		delay := jitteredDelay(jobID, attempt, time.Duration(1<<(attempt-1))*time.Second)
		onRetry(err, delay)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func retryableUploadError(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() != nil || errors.Is(err, context.Canceled) {
		return false
	}
	var statusErr *apihttp.StatusError
	if errors.As(err, &statusErr) {
		return statusErr.Code == 408 || statusErr.Code == 425 || statusErr.Code == 429 || statusErr.Code >= 500
	}
	var networkErr net.Error
	return (errors.As(err, &networkErr) && (networkErr.Timeout() || networkErr.Temporary())) || errors.Is(err, context.DeadlineExceeded)
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
func (w *Worker) startHeartbeat(ctx context.Context, cancel context.CancelFunc, jobID int64, leaseExpiresAt time.Time) func() bool {
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
		leaseTimer := time.NewTimer(time.Until(leaseExpiresAt.Add(-w.leaseSafetyMargin)))
		defer leaseTimer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-leaseTimer.C:
				log.Printf("recording worker job=%d lease expired without confirmed renewal; stopping", jobID)
				markCanceled()
				return
			case <-ticker.C:
				heartbeatCtx, heartbeatCancel := context.WithDeadline(ctx, leaseExpiresAt)
				cancelSignal, renewedUntil, err := w.cfg.Client.HeartbeatRecordingJob(heartbeatCtx, jobID)
				heartbeatCancel()
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
				leaseExpiresAt = renewedUntil
				if !leaseTimer.Stop() {
					select {
					case <-leaseTimer.C:
					default:
					}
				}
				leaseTimer.Reset(time.Until(leaseExpiresAt.Add(-w.leaseSafetyMargin)))
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
	// Map a genuine YouTube sign-in / cookie-expiry failure to a stable sentinel so
	// the relay UI can distinguish "log into YouTube again" from a generic capture
	// failure. Gated off by default (cloud droplet error_text is unchanged); a
	// cookie-DB lock or a stale-extractor failure is never mapped here.
	if w.cfg.ClassifyYouTubeCookieErrors && capture.IsYouTubeSignInError(errText) {
		errText = "youtube_cookie_expired"
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
