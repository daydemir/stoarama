// Package recordingworker is the recorder droplet's job loop: lease a clip job,
// SSRF-re-check the URL, capture it with ffmpeg, upload it to the user's bucket
// via an API-presigned PUT, and ingest the metadata. It owns no S3 credentials.
package recordingworker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
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
	BuildSHA     string

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
	// LeaseGate, when non-nil, prevents a relay self-update restart from racing
	// with lease admission. The worker holds a read lock from immediately before
	// requesting a lease until ActiveJobs reflects the admitted job. Cloud
	// workers leave it nil.
	LeaseGate *sync.RWMutex
	// DrainForUpdate stops new lease admission and asks continuous captures to
	// surrender at the next fully accepted segment boundary. It lets a relay load
	// a verified binary during a 12-hour window without dropping its local spool.
	DrainForUpdate *atomic.Bool
	// RelayDiagnostics, when non-nil, is updated with non-secret job progress for
	// relay node heartbeats. Cloud droplet workers leave it nil.
	RelayDiagnostics *RelayDiagnostics
	// ContinuousNoProgressTimeout makes a worker surrender a continuous job after
	// this long without a successfully ingested segment. The server then hands the
	// job to another worker; zero disables the safeguard.
	ContinuousNoProgressTimeout time.Duration
	// ContinuousMaxMediaLag makes a worker surrender a continuous job when a
	// finalized segment's media timestamp trails wall time by this much. This
	// catches a delivery queue that is making slow progress but can never return to
	// real time; zero disables the safeguard.
	ContinuousMaxMediaLag time.Duration
	// FrozenHLSQuiescenceAllowlist is a comma-separated list of exact
	// "worker-id/recording-id" pairs. Empty is structurally disabled. The policy
	// only suppresses repeated FFmpeg launches after a bounded frozen-playlist
	// proof; it never suppresses or acknowledges media.
	FrozenHLSQuiescenceAllowlist string
	// CaptureTempDir owns relay capture attempts outside the OS-wide temporary
	// directory. Empty preserves the cloud worker's existing behavior.
	CaptureTempDir string
	// UploadWorkers bounds how many segment uploads ONE continuous job keeps in
	// flight. Delivery used to be strictly serial per job, which capped a single
	// job at one reserve+upload+ingest round trip at a time. Zero or negative
	// uses defaultUploadWorkers.
	UploadWorkers int
	// DiskFreeBytes and the thresholds are recorder safety gates. The server
	// remains authoritative for stream capacity; these prevent a full local disk
	// from making a relay, cloud recorder, or host unhealthy.
	DiskFreeBytes      func() (uint64, error)
	MinLeaseFreeBytes  uint64
	MinActiveFreeBytes uint64
}

type Worker struct {
	cfg                      Config
	heartbeatInt             time.Duration
	leaseSafetyMargin        time.Duration
	reconnectDelay           func(int64, int) time.Duration
	lastDiskPauseLog         time.Time
	lastDiskErrorLog         atomic.Int64
	frozenHLSAllowlist       frozenHLSAllowlist
	frozenHLSObserve         frozenHLSObserveFunc
	frozenHLSObserveCurrent  frozenHLSObserveCurrentFunc
	frozenHLSPollMax         time.Duration
	frozenHLSForceCapture    time.Duration
	frozenHLSSafetyInterval  time.Duration
	frozenHLSWait            func(context.Context, time.Duration) error
	frozenHLSProofSpan       func(time.Duration) time.Duration
	continuousCapture        continuousCaptureFunc
	recoverContinuousSegment func(context.Context, string, time.Duration) (capture.Segment, error)
	surrenderJobs            sync.Map // job id -> *surrenderJobState
	surrenderObservationMu   sync.Mutex
}

var (
	errSegmentDelivery           = errors.New("segment delivery failed")
	errPermanentSegmentDelivery  = errors.New("permanent segment delivery failure")
	errSegmentDeliveryExhausted  = errors.New("segment delivery retry budget exhausted")
	errReplaySegmentDelivery     = errors.New("segment delivery must reserve again")
	errSegmentReplayAcknowledged = errors.New("segment replay already ingested")
)

// defaultUploadWorkers is the per-job segment upload concurrency used when
// Config.UploadWorkers is unset. Keep in sync with the RELAY_UPLOAD_WORKERS
// default in internal/config.
const defaultUploadWorkers = 2

const segmentDeliveryRetryDelay = 5 * time.Second

// Keep finalized source-quality segments on the recorder long enough to ride
// through a shared object-storage incident. On 2026-07-29 R2 PUT/HEAD failures
// lasted about 30 minutes across many streams; the former two-minute budget
// canceled ffmpeg and guaranteed a hole even though the recorder still had
// healthy source access and ample local disk. The delivery pool remains bounded
// by the recorder's minimum-free-space fence and the delivery deadline.
const segmentDeliveryRetryBudget = 45 * time.Minute
const postWindowDeliveryGrace = segmentDeliveryRetryBudget
const uploadIntentRefreshMargin = recordingapi.UploadTimeout + 30*time.Second
const continuousDiskMonitorInterval = 5 * time.Second
const diskPauseLogInterval = 5 * time.Minute
const diskErrorLogInterval = 5 * time.Minute
const continuousTimelineLeadAllowance = 5 * time.Second

var continuousUpdateDrainPollInterval = time.Second

func NewWorker(cfg Config) (*Worker, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("client is required")
	}
	if cfg.LeaseGate != nil && cfg.ActiveJobs == nil {
		return nil, fmt.Errorf("lease gate requires active jobs counter")
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
	if cfg.UploadWorkers <= 0 {
		cfg.UploadWorkers = defaultUploadWorkers
	}
	frozenHLSAllowlist, err := parseFrozenHLSAllowlist(cfg.FrozenHLSQuiescenceAllowlist)
	if err != nil {
		return nil, err
	}
	return &Worker{
		cfg:                      cfg,
		heartbeatInt:             time.Duration(cfg.HeartbeatSec) * time.Second,
		leaseSafetyMargin:        5 * time.Second,
		reconnectDelay:           reconnectBackoff,
		frozenHLSAllowlist:       frozenHLSAllowlist,
		frozenHLSObserve:         observeFrozenHLS,
		frozenHLSPollMax:         30 * time.Second,
		frozenHLSForceCapture:    frozenHLSForcedCaptureMax,
		frozenHLSSafetyInterval:  time.Second,
		recoverContinuousSegment: capture.RecoverContinuousSegment,
	}, nil
}

// Run polls for due jobs and processes up to Concurrency at a time until ctx is
// canceled.
func (w *Worker) Run(ctx context.Context) error {
	log.Printf("recording worker start worker_id=%s concurrency=%d poll=%s heartbeat=%s",
		w.cfg.WorkerID, w.cfg.Concurrency, w.cfg.PollInterval, w.heartbeatInt)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Register every durable producer journal before the first lease poll. The
	// recovery attempt may remain pending (for example while the API is down), but
	// beginActiveSurrenderJob must already see that authority and refuse to start a
	// second capture generation over bytes from the first one.
	w.recoverProducerJournals(ctx)

	sem := make(chan struct{}, w.cfg.Concurrency)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.recoveryLoop(ctx)
	}()

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
	if err := w.cfg.Client.TouchDroplet(ctx, w.cfg.BuildSHA); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("recording worker droplet heartbeat error: %v", err)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.cfg.Client.TouchDroplet(ctx, w.cfg.BuildSHA); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("recording worker droplet heartbeat error: %v", err)
			}
		}
	}
}

// drain leases and dispatches jobs until either no job is due or the worker is
// at capacity.
func (w *Worker) drain(ctx context.Context, sem chan struct{}, wg *sync.WaitGroup) {
	for {
		if w.cfg.DrainForUpdate != nil && w.cfg.DrainForUpdate.Load() {
			return
		}
		if !w.diskHasSpace(w.cfg.MinLeaseFreeBytes) {
			w.logDiskPause(time.Now())
			return
		}
		select {
		case sem <- struct{}{}:
		default:
			return
		}
		if ctx.Err() != nil {
			<-sem
			return
		}
		if w.cfg.LeaseGate != nil {
			w.cfg.LeaseGate.RLock()
		}
		if w.cfg.DrainForUpdate != nil && w.cfg.DrainForUpdate.Load() {
			if w.cfg.LeaseGate != nil {
				w.cfg.LeaseGate.RUnlock()
			}
			<-sem
			return
		}
		job, err := w.cfg.Client.LeaseRecordingJob(ctx)
		if err != nil {
			if w.cfg.LeaseGate != nil {
				w.cfg.LeaseGate.RUnlock()
			}
			<-sem
			if !errors.Is(err, context.Canceled) {
				log.Printf("recording worker lease error: %v", err)
			}
			return
		}
		if job == nil {
			if w.cfg.LeaseGate != nil {
				w.cfg.LeaseGate.RUnlock()
			}
			<-sem
			return
		}
		wg.Add(1)
		if w.cfg.ActiveJobs != nil {
			w.cfg.ActiveJobs.Add(1)
		}
		if w.cfg.LeaseGate != nil {
			w.cfg.LeaseGate.RUnlock()
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

func (w *Worker) logDiskPause(now time.Time) {
	if !w.lastDiskPauseLog.IsZero() && now.Sub(w.lastDiskPauseLog) < diskPauseLogInterval {
		return
	}
	w.lastDiskPauseLog = now
	log.Printf("recording worker lease paused: disk free space is below %d bytes", w.cfg.MinLeaseFreeBytes)
}

// processJob runs the full capture pipeline for one job. It re-validates the URL
// against the SSRF guard immediately before ffmpeg (defeating DNS rebinding),
// runs a per-job heartbeat that can cancel the capture, and fails the job on any
// error so it is retried or surfaced.
func (w *Worker) processJob(ctx context.Context, job recordingapi.RecordingJob) {
	w.cfg.RelayDiagnostics.Start(job)
	if err := w.ensureCaptureTempDir(); err != nil {
		w.cfg.RelayDiagnostics.Finish(job.JobID, "failed", err)
		w.fail(ctx, job.JobID, job.LeaseToken, err)
		return
	}
	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	canceled := w.startHeartbeat(jobCtx, cancel, job.JobID, job.LeaseToken, job.LeaseExpiresAt)

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
		w.fail(ctx, job.JobID, job.LeaseToken, fmt.Errorf("resolve source url: %w", err))
		return
	}
	if isImage {
		err := fmt.Errorf("image sources are not supported by the recorder")
		w.cfg.RelayDiagnostics.Finish(job.JobID, "failed", err)
		w.fail(ctx, job.JobID, job.LeaseToken, err)
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
		w.fail(ctx, job.JobID, job.LeaseToken, fmt.Errorf("ssrf guard rejected source url: %w", err))
		return
	}

	clipDuration := time.Duration(job.ClipDurationSec) * time.Second
	w.cfg.RelayDiagnostics.Stage(job.JobID, "capturing")
	captureCtx, captureCancel := context.WithTimeout(jobCtx, capture.SegmentCaptureTimeout(clipDuration))
	// Recording footage is always captured source-native. TargetFPS is retained in
	// the wire shape only for compatibility with older API/relay versions; never
	// allow it to select FFmpeg's re-encode branch.
	seg, err := capture.CaptureSegmentInDirWithHeaders(captureCtx, sourceURL, clipDuration, "", recordingCaptureTargetFPS(job.TargetFPS), w.cfg.CaptureTempDir, inputHeaders)
	captureCancel()
	if err != nil {
		if canceled() {
			log.Printf("recording worker job=%d canceled during capture", job.JobID)
			w.cfg.RelayDiagnostics.Finish(job.JobID, "canceled", nil)
			return
		}
		w.cfg.RelayDiagnostics.Finish(job.JobID, "failed", fmt.Errorf("capture clip: %w", err))
		w.fail(ctx, job.JobID, job.LeaseToken, fmt.Errorf("capture clip: %w", err))
		return
	}
	defer capture.CleanupSegment(seg)

	if canceled() {
		log.Printf("recording worker job=%d canceled before upload", job.JobID)
		w.cfg.RelayDiagnostics.Finish(job.JobID, "canceled", nil)
		return
	}

	w.cfg.RelayDiagnostics.Stage(job.JobID, "reserve_upload")
	intent, err := w.cfg.Client.ReserveClipUpload(jobCtx, job.JobID, job.LeaseToken, seg.MIMEType, seg.SHA256, 0)
	if err != nil {
		w.cfg.RelayDiagnostics.Finish(job.JobID, "failed", fmt.Errorf("reserve clip upload: %w", err))
		w.fail(ctx, job.JobID, job.LeaseToken, fmt.Errorf("reserve clip upload: %w", err))
		return
	}
	if err := deliverReservedClip(intent,
		func() error {
			w.cfg.RelayDiagnostics.Stage(job.JobID, "uploading")
			return w.cfg.Client.UploadFile(jobCtx, intent.UploadURL, seg.Path, seg.MIMEType)
		},
		func() error {
			w.cfg.RelayDiagnostics.Stage(job.JobID, "ingesting")
			_, err := w.cfg.Client.IngestClip(jobCtx, recordingapi.IngestClipRequest{
				IntentID:        intent.IntentID,
				JobID:           job.JobID,
				SizeBytes:       seg.SizeBytes,
				SHA256:          seg.SHA256,
				DurationMs:      seg.DurationMs,
				VideoCodec:      seg.VideoCodec,
				AudioCodec:      seg.AudioCodec,
				AudioPresent:    seg.AudioPresent,
				ActualFPS:       seg.ActualFPS,
				VideoWidth:      seg.VideoWidth,
				VideoHeight:     seg.VideoHeight,
				Container:       seg.Container,
				ResolvedURL:     sourceURL,
				ClipStartAt:     seg.StartAt,
				ClipEndAt:       seg.EndAt,
				LeaseToken:      job.LeaseToken,
				CaptureSequence: seg.CaptureSequence,
			})
			return err
		},
	); err != nil {
		w.cfg.RelayDiagnostics.Finish(job.JobID, "failed", err)
		w.fail(ctx, job.JobID, job.LeaseToken, err)
		return
	}
	w.cfg.RelayDiagnostics.Segment(job.JobID, seg.StartAt)
	w.cfg.RelayDiagnostics.Stage(job.JobID, "completing")
	if err := w.cfg.Client.CompleteRecordingJob(ctx, job.JobID, job.LeaseToken); err != nil {
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
	jobState, stateErr := w.beginActiveSurrenderJob(job)
	if stateErr != nil {
		w.cfg.RelayDiagnostics.Finish(job.JobID, "surrender_lifecycle_busy", stateErr)
		return
	}
	defer w.endActiveSurrenderJob(jobState)
	if err := w.ensureCaptureTempDir(); err != nil {
		w.cfg.RelayDiagnostics.Finish(job.JobID, "failed", err)
		w.fail(ctx, job.JobID, job.LeaseToken, err)
		return
	}
	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	canceled := w.startHeartbeat(jobCtx, cancel, job.JobID, job.LeaseToken, job.LeaseExpiresAt)

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

	// The source URL is re-resolved every supervisor attempt; deliverSegment records
	// the URL that produced the segment it is ingesting, and takes it as a parameter
	// so each attempt's delivery pool closes over an immutable copy rather than a
	// variable the job goroutine rewrites on reconnect.
	//
	// CONCURRENCY: CaptureContinuous no longer performs delivery inline. It still
	// calls the pool's Submit synchronously and in order, but reserve -> PUT ->
	// ingest runs on UploadWorkers goroutines, so the delivery state this loop reads
	// is NOT owned by the job goroutine any more:
	//   - progress (the old lastProgressAt) is mutex-guarded in continuousProgress;
	//   - the per-attempt first error, in-flight count and "ingested anything" flag
	//     (the old segmentDeliveryPending / segmentIngested) live inside
	//     segmentDeliveryPool behind its own mutex, and this loop reads them only
	//     from the segmentDeliveryResult that pool.close() returns after joining
	//     every worker;
	//   - RelayDiagnostics and the canceled() probe were already mutex-guarded, and
	//     the disk gate is atomic.
	// segmentDeliveryPending stays job-scoped (an attempt that delivers nothing
	// leaves the previous attempt's value untouched, as the inline path did).
	segmentDeliveryPending := false
	var captureSequence int64
	var captureOrdinal int64
	// CaptureContinuous owns one media chain per ffmpeg attempt. This job-scoped
	// high-water mark preserves genuine reconnect downtime while ensuring a
	// restarted resolver or DVR tail can never move behind accepted footage.
	// Only timeline metadata changes; source MP4 bytes are never trimmed or
	// transcoded here.
	var timelineEnd time.Time
	progress := newContinuousProgress(time.Now())
	deliverSegment := func(sourceURL string, producer *captureProducerJournal, seg capture.Segment) error {
		w.cfg.RelayDiagnostics.DeliveryPhase(job.JobID, "finalize_read", seg.FinalizeReadDuration)
		w.cfg.RelayDiagnostics.DeliveryPhase(job.JobID, "finalize_hash", seg.FinalizeHashDuration)
		w.cfg.RelayDiagnostics.DeliveryPhase(job.JobID, "finalize_probe", seg.FinalizeProbeDuration)
		if !seg.DeliveryQueuedAt.IsZero() {
			w.cfg.RelayDiagnostics.DeliveryPhase(job.JobID, "queue_wait", time.Since(seg.DeliveryQueuedAt))
		}
		if !w.diskHasSpace(w.cfg.MinActiveFreeBytes) {
			return errDiskPressure
		}
		if canceled() {
			return context.Canceled
		}
		segStartMs := seg.StartAt.UTC().UnixMilli()
		segmentCtx, segmentCancel := continuousSegmentDeliveryContext(jobCtx, job.WindowEndAt, time.Now())
		defer segmentCancel()
		alreadyIngested := false
		artifact, artifactErr := captureArtifactForSequence(producer, seg.CaptureSequence)
		if artifactErr != nil {
			return artifactErr
		}
		err := deliverSegmentWithRetry(segmentCtx, segmentDeliveryRetryDelay, func() bool {
			return w.diskHasSpace(w.cfg.MinActiveFreeBytes)
		}, segmentDeliveryOps{
			Reserve: func() (recordingapi.ClipUploadIntent, error) {
				started := time.Now()
				defer func() { w.cfg.RelayDiagnostics.DeliveryPhase(job.JobID, "reserve", time.Since(started)) }()
				w.cfg.RelayDiagnostics.Stage(job.JobID, "segment_reserve_upload")
				var reserved recordingapi.ClipUploadIntent
				var err error
				if producer != nil {
					if journalErr := w.recordProducerArtifact(job.JobID, producer, seg, artifact.IntentID); journalErr != nil {
						return recordingapi.ClipUploadIntent{}, journalErr
					}
					reserved, err = w.cfg.Client.SealCaptureArtifact(segmentCtx, job.JobID, job.LeaseToken, artifact.IntentID, producer.ProducerID, seg.CaptureSequence, segStartMs, seg.SizeBytes, seg.SHA256)
				} else {
					reserved, err = w.cfg.Client.ReserveClipUpload(segmentCtx, job.JobID, job.LeaseToken, seg.MIMEType, seg.SHA256, segStartMs)
				}
				if err != nil {
					w.cfg.RelayDiagnostics.Error(job.JobID, "segment_reserve_upload_failed", err)
				}
				return reserved, err
			},
			Upload: func(intent recordingapi.ClipUploadIntent) error {
				started := time.Now()
				defer func() { w.cfg.RelayDiagnostics.DeliveryPhase(job.JobID, "put", time.Since(started)) }()
				w.cfg.RelayDiagnostics.Stage(job.JobID, "segment_uploading")
				uploadCtx, uploadCancel := context.WithTimeout(segmentCtx, recordingapi.UploadTimeout)
				err := w.cfg.Client.UploadFile(uploadCtx, intent.UploadURL, seg.Path, seg.MIMEType)
				uploadCancel()
				if err != nil {
					w.cfg.RelayDiagnostics.Error(job.JobID, "segment_upload_failed", err)
				}
				return err
			},
			Ingest: func(intent recordingapi.ClipUploadIntent) error {
				started := time.Now()
				defer func() { w.cfg.RelayDiagnostics.DeliveryPhase(job.JobID, "ingest", time.Since(started)) }()
				w.cfg.RelayDiagnostics.Stage(job.JobID, "segment_ingesting")
				result, err := w.cfg.Client.IngestClipWithResult(segmentCtx, recordingapi.IngestClipRequest{
					IntentID:                intent.IntentID,
					JobID:                   job.JobID,
					SizeBytes:               seg.SizeBytes,
					SHA256:                  seg.SHA256,
					DurationMs:              seg.DurationMs,
					VideoCodec:              seg.VideoCodec,
					AudioCodec:              seg.AudioCodec,
					AudioPresent:            seg.AudioPresent,
					ActualFPS:               seg.ActualFPS,
					VideoWidth:              seg.VideoWidth,
					VideoHeight:             seg.VideoHeight,
					Container:               seg.Container,
					ResolvedURL:             sourceURL,
					ClipStartAt:             seg.StartAt,
					ClipEndAt:               seg.EndAt,
					LeaseToken:              job.LeaseToken,
					CaptureSequence:         seg.CaptureSequence,
					CaptureAttemptID:        seg.CaptureAttemptID,
					TimestampContract:       seg.TimestampContract,
					TimestampContractStatus: seg.TimestampContractStatus,
					TimestampContractReason: seg.TimestampContractReason,
				})
				if err != nil {
					w.cfg.RelayDiagnostics.Error(job.JobID, "segment_ingest_failed", err)
					return err
				}
				w.surrenderState(job.JobID).markHead(result)
				return nil
			},
			AlreadyIngested: func() { alreadyIngested = true },
		}, func(err error) {
			w.cfg.RelayDiagnostics.DeliveryRetry(job.JobID)
			w.cfg.RelayDiagnostics.Error(job.JobID, "segment_delivery_retry", err)
			log.Printf("recording worker job=%d recording=%d segment delivery failed: %v; retrying in %s",
				job.JobID, job.RecordingID, err, segmentDeliveryRetryDelay)
		})
		if alreadyIngested {
			// A different lease generation may have ingested this exact byte run.
			// Acknowledge and remove only this local replay, but do not move the
			// no-progress clock or claim a new unique ingest.
			if removeErr := os.Remove(seg.Path); removeErr != nil && !os.IsNotExist(removeErr) {
				return removeErr
			}
			if journalErr := w.acknowledgeProducerArtifact(job.JobID, producer, seg.Path); journalErr != nil {
				w.cfg.RelayDiagnostics.Error(job.JobID, "segment_journal_ack_failed", journalErr)
			}
			return errSegmentReplayAcknowledged
		}
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && jobCtx.Err() == nil {
				return fmt.Errorf("%w: %v", errSegmentDeliveryExhausted, err)
			}
			return err
		}
		progress.mark(time.Now())
		if removeErr := os.Remove(seg.Path); removeErr != nil && !os.IsNotExist(removeErr) {
			return removeErr
		}
		if journalErr := w.acknowledgeProducerArtifact(job.JobID, producer, seg.Path); journalErr != nil {
			w.cfg.RelayDiagnostics.Error(job.JobID, "segment_journal_ack_failed", journalErr)
		}
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
	// grows to a 30-second cap so a persistently dead source is not hammered,
	// while an attempt that ingested at least one clip resets failures to zero. The
	// separate five-minute no-progress safeguard still hands a dead source to a new
	// worker. The sleep stays interruptible by windowCtx.Done() (window close / job
	// cancel) just like a fixed delay.
	failures := 0
	var frozenHLSState frozenHLSJobState
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
			delay := w.nextReconnectDelay(job.JobID, failures)
			log.Printf("recording worker job=%d recording=%d continuous resolve failed (attempt %d): %v; retrying in %s",
				job.JobID, job.RecordingID, attempt, err, delay)
			if w.surrenderContinuousJob(ctx, cancel, job, progress.last(), err) {
				return
			}
			backoff(continuousReconnectDelay(progress.last(), time.Now(), w.cfg.ContinuousNoProgressTimeout, delay))
			continue
		}
		if isImage {
			err := fmt.Errorf("image sources are not supported by the recorder")
			w.cfg.RelayDiagnostics.Finish(job.JobID, "failed", err)
			w.fail(ctx, job.JobID, job.LeaseToken, err)
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
			delay := w.nextReconnectDelay(job.JobID, failures)
			log.Printf("recording worker job=%d recording=%d continuous ssrf guard rejected url (attempt %d): %v; retrying in %s",
				job.JobID, job.RecordingID, attempt, err, delay)
			if w.surrenderContinuousJob(ctx, cancel, job, progress.last(), err) {
				return
			}
			backoff(continuousReconnectDelay(progress.last(), time.Now(), w.cfg.ContinuousNoProgressTimeout, delay))
			continue
		}

		// Fresh outDir per attempt, removed immediately after the attempt returns: a
		// previous attempt's leftover seg-*.mp4 would otherwise be re-finalized and
		// re-ingested by the next CaptureContinuous call.
		outDir, err := os.MkdirTemp(w.cfg.CaptureTempDir, "capture-continuous-*")
		if err != nil {
			// Disk pressure is transient, so treat it like a resolve/ssrf failure and
			// reconnect rather than failing the job. Failing here burned an attempt in
			// microseconds and immediately re-leased the next job, so a single full
			// host poisoned every job it touched (15 jobs in 134s on 2026-07-24).
			if continuousShouldStop(canceled(), windowCtx.Err() != nil) {
				break
			}
			w.cfg.RelayDiagnostics.Error(job.JobID, "mktemp_retry", err)
			failures++
			delay := w.nextReconnectDelay(job.JobID, failures)
			log.Printf("recording worker job=%d recording=%d continuous mktemp failed (attempt %d): %v; retrying in %s",
				job.JobID, job.RecordingID, attempt, err, delay)
			if w.surrenderContinuousJob(ctx, cancel, job, progress.last(), err) {
				return
			}
			backoff(continuousReconnectDelay(progress.last(), time.Now(), w.cfg.ContinuousNoProgressTimeout, delay))
			continue
		}
		w.cfg.RelayDiagnostics.Stage(job.JobID, "continuous_capturing")
		producerOrdinal := captureOrdinal + 1
		jobState.mu.Lock()
		if jobState.producer != nil {
			producerOrdinal = jobState.producer.CaptureOrdinal
		}
		jobState.mu.Unlock()
		producer, producerErr := w.reserveCaptureProducer(jobCtx, job, producerOrdinal, outDir)
		if producerErr != nil {
			w.cfg.RelayDiagnostics.Error(job.JobID, "capture_producer_reserve_failed", producerErr)
			// Reservation response loss is ambiguous: the server may already have
			// frozen this producer's source/config identity. Do not re-resolve and
			// reparent that identity to a later ffmpeg attempt. The recovery loop
			// proves absent/current/expired authority before cleanup or capture.
			cancel()
			w.cfg.RelayDiagnostics.Finish(job.JobID, "producer_recovery_pending", producerErr)
			return
		}
		if producer != nil && len(producer.Artifacts) == 0 {
			reservationCount, countErr := captureArtifactReservationCount(job, time.Now())
			if countErr == nil {
				countErr = w.reserveCaptureArtifactSlots(jobCtx, job, producer, captureSequence+1, reservationCount)
			}
			if countErr != nil {
				w.cfg.RelayDiagnostics.Error(job.JobID, "capture_artifact_prereserve_failed", countErr)
				cancel()
				w.cfg.RelayDiagnostics.Finish(job.JobID, "producer_recovery_pending", countErr)
				return
			}
		}
		if producer != nil && producer.CaptureOrdinal > captureOrdinal {
			captureOrdinal = producer.CaptureOrdinal
		}
		// One delivery pool per attempt. attemptCtx lets the first delivery failure
		// stop ffmpeg as promptly as the old inline upload did; it is a child of
		// windowCtx, so aborting an attempt never looks like a window close (the
		// supervisor still reconnects). The per-segment delivery budget stays on
		// jobCtx, so uploads already in flight keep their full retry budget (and the
		// post-window grace) while the pool is drained.
		attemptCtx, abortAttempt := context.WithCancel(windowCtx)
		var mediaLagged atomic.Bool
		pool := startSegmentDeliveryPool(w.cfg.UploadWorkers, abortAttempt, func(seg capture.Segment) error {
			// Measure age when an upload worker actually reaches the descriptor, not
			// when capture enqueues it. Submit may pause at the producer's hard
			// outstanding bound, so only
			// this point can detect a queue that is falling behind real time. Stopping
			// capture is control flow only: this segment and the rest of the accepted
			// spool still drain normally on jobCtx.
			observeContinuousMediaLag(seg.EndAt, time.Now(), w.cfg.ContinuousMaxMediaLag, &mediaLagged, abortAttempt)
			return deliverSegment(resolved, producer, seg)
		}, func(depth int) { w.cfg.RelayDiagnostics.DeliveryQueue(job.JobID, depth) })
		var diskPressure atomic.Bool
		stopDiskMonitor := make(chan struct{})
		go w.monitorContinuousDisk(stopDiskMonitor, &diskPressure, abortAttempt)
		stopUpdateMonitor := make(chan struct{})
		if w.cfg.DrainForUpdate != nil {
			go monitorContinuousUpdateDrain(stopUpdateMonitor, w.cfg.DrainForUpdate, abortAttempt)
		}
		submitInCaptureOrder := func(seg capture.Segment) error {
			seg = clampContinuousSegmentTimeline(seg, timelineEnd)
			// Some live HLS origins briefly drain an already-published DVR tail
			// faster than wall time after a reconnect. Keep every source-quality
			// segment, but do not publish its logical timeline in the future: wait
			// for wall time to catch up before handing it to the upload pool. The
			// capture directory remains protected by the disk-pressure monitor.
			if err := waitForContinuousTimeline(attemptCtx, seg.EndAt, time.Now); err != nil {
				return err
			}
			seg.CaptureSequence = captureSequence + 1
			if !job.TimestampContractSupported {
				seg.CaptureAttemptID = ""
				seg.TimestampContract = nil
				seg.TimestampContractStatus = ""
				seg.TimestampContractReason = ""
			}
			if err := pool.Submit(seg); err != nil {
				return err
			}
			captureSequence = seg.CaptureSequence
			timelineEnd = seg.EndAt
			if w.cfg.DrainForUpdate != nil && w.cfg.DrainForUpdate.Load() {
				abortAttempt()
			}
			return nil
		}
		captureContinuous := w.continuousCapture
		if captureContinuous == nil {
			captureContinuous = continuousCaptureForJob(job)
		}
		captureErr := captureContinuous(attemptCtx, resolved, clipDuration, "", recordingCaptureTargetFPS(job.TargetFPS), outDir, submitInCaptureOrder, inputHeaders)
		close(stopDiskMonitor)
		close(stopUpdateMonitor)
		// Join every outstanding upload BEFORE the attempt is judged, so the window
		// never closes (and outDir is never removed) with an upload still running.
		delivery := pool.close()
		remainingProducerFiles, spoolErr := filepath.Glob(filepath.Join(outDir, "seg-*.mp4"))
		durableProducerSpool := producer != nil && (delivery.pending > 0 || spoolErr != nil || len(remainingProducerFiles) > 0)
		var producerFinishErr error
		if durableProducerSpool {
			producerFinishErr = fmt.Errorf("capture producer retains durable local spool")
			if spoolErr != nil {
				producerFinishErr = errors.Join(producerFinishErr, spoolErr)
			}
			captureErr = errors.Join(captureErr, producerFinishErr)
		}
		abortAttempt()
		captureErr = joinSegmentDeliveryError(captureErr, delivery.err)
		if diskPressure.Load() {
			captureErr = errors.Join(captureErr, errDiskPressure)
		}
		if delivery.submitted > 0 || durableProducerSpool {
			segmentDeliveryPending = delivery.pending > 0 || durableProducerSpool
		}
		if shouldCleanupCaptureProducerAttempt(producer != nil, durableProducerSpool, segmentDeliveryPending, captureErr, mediaLagged.Load()) {
			if removeErr := os.RemoveAll(outDir); removeErr != nil {
				w.cfg.RelayDiagnostics.Error(job.JobID, "temp_cleanup_failed", removeErr)
			}
		}
		if producerFinishErr != nil {
			// The producer reservation and any local/artifact bytes remain durably
			// journaled. Stop heartbeating so the server can issue the exact upload-
			// only recovery grant (or expose an already-committed terminal result)
			// instead of looping forever behind a nonterminal producer ordinal.
			cancel()
			w.cfg.RelayDiagnostics.Finish(job.JobID, "producer_recovery_pending", producerFinishErr)
			return
		}
		if producer != nil {
			if finishErr := w.finishCaptureProducer(jobCtx, job, producer, captureProducerTerminalResult(producer), ""); finishErr != nil {
				cancel()
				w.cfg.RelayDiagnostics.Finish(job.JobID, "producer_recovery_pending", finishErr)
				return
			}
			if removeErr := os.RemoveAll(outDir); removeErr != nil {
				w.cfg.RelayDiagnostics.Error(job.JobID, "temp_cleanup_failed", removeErr)
			}
		}

		if errors.Is(captureErr, errDiskPressure) {
			if w.surrenderContinuousJobForReason(ctx, cancel, job, recordingapi.SurrenderDiskPressure, errDiskPressure) {
				return
			}
			backoff(segmentDeliveryRetryDelay)
			continue
		}
		windowClosed := continuousShouldStop(canceled(), windowCtx.Err() != nil)
		if continuousSelfUpdateCanSurrender(w.cfg.DrainForUpdate != nil && w.cfg.DrainForUpdate.Load(), delivery.err, windowClosed) {
			err := fmt.Errorf("relay is draining for a verified self-update")
			if w.surrenderContinuousJobForReason(ctx, cancel, job, recordingapi.SurrenderSelfUpdate, err) {
				return
			}
			continue
		}
		if continuousMediaLagCanSurrender(mediaLagged.Load(), delivery.err, windowClosed) {
			err := fmt.Errorf("continuous media timeline fell behind by at least %s", w.cfg.ContinuousMaxMediaLag)
			if w.surrenderContinuousJobForReason(ctx, cancel, job, recordingapi.SurrenderNoProgress, err) {
				return
			}
			continue
		}
		if continuousDeliveryFailureShouldFail(captureErr, windowClosed) {
			if finishErr := w.finishActiveCaptureProducer(jobCtx, job); finishErr != nil {
				cancel()
				w.cfg.RelayDiagnostics.Finish(job.JobID, "producer_recovery_pending", finishErr)
				return
			}
			w.cfg.RelayDiagnostics.Finish(job.JobID, "failed", captureErr)
			w.fail(ctx, job.JobID, job.LeaseToken, captureErr)
			return
		}
		if delivery.ingested {
			// Any new unique media invalidates the frozen-live-edge baseline. A later
			// no-output episode must earn a fresh three-observation classification.
			frozenHLSState = frozenHLSJobState{}
		}
		if frozenHLSCanObserve(windowClosed, delivery.err, segmentDeliveryPending, captureErr) {
			ffmpegExitAt := time.Now()
			forcedLaunchDeadline := ffmpegExitAt.Add(min(w.frozenHLSForceCapture, frozenHLSForcedCaptureMax))
			if !frozenHLSState.classified {
				// The first classification may not extend the existing no-unique-
				// ingest handoff boundary. A late FFmpeg output-growth stall must
				// fail open to the ordinary surrender path when three separated
				// observations no longer fit. Already-classified cycles use the
				// fresh per-cycle forced-launch ceiling while retaining the lease.
				forcedLaunchDeadline = frozenHLSInitialDeadline(
					ffmpegExitAt, progress.last(), w.frozenHLSForceCapture, w.cfg.ContinuousNoProgressTimeout,
				)
			}
			cycleResult, observeErr := w.handleFrozenHLSCycle(
				windowCtx, job, resolved, inputHeaders, &frozenHLSState, forcedLaunchDeadline,
			)
			switch {
			case errors.Is(observeErr, errDiskPressure):
				if w.surrenderContinuousJobForReason(ctx, cancel, job, recordingapi.SurrenderDiskPressure, errDiskPressure) {
					return
				}
				continue
			case errors.Is(observeErr, errFrozenHLSSelfUpdate):
				if w.surrenderContinuousJobForReason(ctx, cancel, job, recordingapi.SurrenderSelfUpdate, observeErr) {
					return
				}
				continue
			case observeErr != nil && windowCtx.Err() != nil:
				windowClosed = true
			case cycleResult == frozenHLSCycleResumeCapture:
				// A changed/ambiguous observation or the absolute five-minute
				// forced-launch ceiling resumes at the top of the ordinary loop.
				// Persisted classified state prevents the stale unique-progress
				// clock from causing an immediate lease surrender on the next
				// unchanged no-output cycle.
				failures = 0
				continue
			}
		}
		// Window close vs premature drop: CaptureContinuous returns nil on ctx.Done,
		// so windowCtx.Err() (NOT captureErr) is what distinguishes a real window
		// close/cancel from a premature clean ffmpeg exit (HLS end-of-stream).
		if windowClosed {
			if segmentDeliveryPending {
				err := fmt.Errorf("continuous window closed with an unacknowledged segment")
				w.cfg.RelayDiagnostics.Finish(job.JobID, "delivery_incomplete", err)
				log.Printf("recording worker job=%d recording=%d %v", job.JobID, job.RecordingID, err)
				return
			}
			break
		}
		// Premature exit (clean end-of-stream or a hard ffmpeg error) with the window
		// still open: back off and reconnect. An attempt that ingested at least one
		// clip was a healthy connection that later dropped, so reset the backoff.
		if delivery.ingested {
			failures = 0
		}
		failures++
		delay := w.nextReconnectDelay(job.JobID, failures)
		if captureErr != nil {
			w.cfg.RelayDiagnostics.Error(job.JobID, "capture_retry", captureErr)
		} else {
			w.cfg.RelayDiagnostics.Stage(job.JobID, "capture_retry")
		}
		log.Printf("recording worker job=%d recording=%d continuous source dropped (attempt %d): %v; reconnecting in %s",
			job.JobID, job.RecordingID, attempt, captureErr, delay)
		if !errors.Is(captureErr, errSegmentDelivery) && w.surrenderContinuousJob(ctx, cancel, job, progress.last(), captureErr) {
			return
		}
		backoff(continuousReconnectDelay(progress.last(), time.Now(), w.cfg.ContinuousNoProgressTimeout, delay))
	}

	if canceled() {
		log.Printf("recording worker job=%d continuous canceled", job.JobID)
		w.cfg.RelayDiagnostics.Finish(job.JobID, "canceled", nil)
		return
	}
	if err := w.finishActiveCaptureProducer(ctx, job); err != nil {
		cancel()
		w.cfg.RelayDiagnostics.Finish(job.JobID, "producer_recovery_pending", err)
		return
	}
	w.cfg.RelayDiagnostics.Stage(job.JobID, "completing")
	if err := w.cfg.Client.CompleteRecordingJob(ctx, job.JobID, job.LeaseToken); err != nil {
		log.Printf("recording worker job=%d continuous complete failed: %v", job.JobID, err)
		w.cfg.RelayDiagnostics.Finish(job.JobID, "complete_failed", err)
		return
	}
	w.cfg.RelayDiagnostics.Finish(job.JobID, "done", nil)
	log.Printf("recording worker job=%d recording=%d continuous window complete", job.JobID, job.RecordingID)
}

type continuousCaptureFunc func(context.Context, string, time.Duration, string, *int, string, func(capture.Segment) error, string) error

func continuousCaptureForJob(job recordingapi.RecordingJob) continuousCaptureFunc {
	if job.TimestampContractSupported {
		return capture.CaptureContinuousWithTimestampContract
	}
	return capture.CaptureContinuousWithHeaders
}

func continuousSelfUpdateCanSurrender(draining bool, deliveryErr error, windowClosed bool) bool {
	return draining && deliveryErr == nil && !windowClosed
}

func monitorContinuousUpdateDrain(stop <-chan struct{}, drain *atomic.Bool, abort func()) {
	ticker := time.NewTicker(continuousUpdateDrainPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			if drain != nil && drain.Load() {
				abort()
				return
			}
		}
	}
}

func (w *Worker) monitorContinuousDisk(stop <-chan struct{}, pressured *atomic.Bool, abort context.CancelFunc) {
	w.monitorContinuousDiskAtInterval(stop, pressured, abort, continuousDiskMonitorInterval)
}

func (w *Worker) monitorContinuousDiskAtInterval(stop <-chan struct{}, pressured *atomic.Bool, abort context.CancelFunc, interval time.Duration) {
	if w.cfg.MinActiveFreeBytes == 0 || w.cfg.DiskFreeBytes == nil {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			if !w.diskHasSpace(w.cfg.MinActiveFreeBytes) {
				pressured.Store(true)
				abort()
				return
			}
		}
	}
}

// clampContinuousSegmentTimeline preserves a real reconnect gap when the new
// attempt starts after prior media, but forbids a backward seam when it starts
// behind the job's accepted capture sequence.
func clampContinuousSegmentTimeline(seg capture.Segment, timelineEnd time.Time) capture.Segment {
	if timelineEnd.IsZero() || !seg.StartAt.Before(timelineEnd) {
		return seg
	}
	seg.StartAt = timelineEnd
	if seg.DurationMs > 0 {
		seg.EndAt = seg.StartAt.Add(time.Duration(seg.DurationMs) * time.Millisecond)
	} else {
		// Match CaptureContinuous's unknown-duration fallback so two clamped
		// segments cannot reuse the same millisecond idempotency key.
		seg.EndAt = seg.StartAt.Add(time.Millisecond)
	}
	return seg
}

func continuousTimelineDelay(endAt, now time.Time) time.Duration {
	if endAt.IsZero() {
		return 0
	}
	delay := endAt.Sub(now.Add(continuousTimelineLeadAllowance))
	if delay < 0 {
		return 0
	}
	return delay
}

func waitForContinuousTimeline(ctx context.Context, endAt time.Time, now func() time.Time) error {
	delay := continuousTimelineDelay(endAt, now())
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type segmentDeliveryOps struct {
	Reserve         func() (recordingapi.ClipUploadIntent, error)
	Upload          func(recordingapi.ClipUploadIntent) error
	Ingest          func(recordingapi.ClipUploadIntent) error
	AlreadyIngested func()
}

func deliverReservedClip(intent recordingapi.ClipUploadIntent, upload, ingest func() error) error {
	if intent.AlreadyIngested {
		return nil
	}
	if err := upload(); err != nil {
		return fmt.Errorf("upload clip: %w", err)
	}
	if err := ingest(); err != nil {
		return fmt.Errorf("ingest clip: %w", err)
	}
	return nil
}

func deliverSegmentWithRetry(ctx context.Context, retryDelay time.Duration, diskOK func() bool, ops segmentDeliveryOps, onRetry func(error)) error {
	var intent *recordingapi.ClipUploadIntent
	uploaded := false
	return retrySegmentDelivery(ctx, retryDelay, diskOK, func() error {
		if intent == nil {
			reserved, err := ops.Reserve()
			if err != nil {
				return fmt.Errorf("%w: reserve segment upload: %w", errSegmentDelivery, err)
			}
			intent = &reserved
		}
		if intent.AlreadyIngested {
			if ops.AlreadyIngested != nil {
				ops.AlreadyIngested()
			}
			return nil
		}
		// Presigned PUT URLs are shorter-lived than the storage-outage retry
		// budget. Refresh before starting an upload that could outlive its URL;
		// reserving the same idempotent segment renews the existing intent and
		// never changes the object key or media bytes.
		if !intent.ExpiresAt.IsZero() && !time.Now().Add(uploadIntentRefreshMargin).Before(intent.ExpiresAt) {
			intent = nil
			uploaded = false
			return fmt.Errorf("%w: upload intent expires too soon", errReplaySegmentDelivery)
		}
		if !uploaded {
			if err := ops.Upload(*intent); err != nil {
				return fmt.Errorf("%w: upload segment: %w", errSegmentDelivery, err)
			}
			uploaded = true
		}
		if err := ops.Ingest(*intent); err != nil {
			if isAlreadyIngested(err) {
				if ops.AlreadyIngested != nil {
					ops.AlreadyIngested()
				}
				return nil
			}
			if retryableTransportError(ctx, err) || isUploadIntentStateConflict(err) {
				intent = nil
				uploaded = false
				if isUploadIntentStateConflict(err) {
					return fmt.Errorf("%w: %v", errReplaySegmentDelivery, err)
				}
			}
			return fmt.Errorf("%w: ingest segment: %w", errSegmentDelivery, err)
		}
		return nil
	}, onRetry)
}

var errDiskPressure = errors.New("relay disk pressure")

func (w *Worker) diskHasSpace(min uint64) bool {
	if min == 0 || w.cfg.DiskFreeBytes == nil {
		return true
	}
	free, err := w.cfg.DiskFreeBytes()
	if err != nil {
		if w.shouldLogDiskError(time.Now()) {
			log.Printf("recording worker disk check failed: %v", err)
		}
		return false
	}
	w.lastDiskErrorLog.Store(0)
	return free >= min
}

func (w *Worker) shouldLogDiskError(now time.Time) bool {
	current := now.UnixNano()
	for {
		previous := w.lastDiskErrorLog.Load()
		if previous != 0 && now.Sub(time.Unix(0, previous)) < diskErrorLogInterval {
			return false
		}
		if w.lastDiskErrorLog.CompareAndSwap(previous, current) {
			return true
		}
	}
}

func (w *Worker) surrenderContinuousJob(ctx context.Context, cancel context.CancelFunc, job recordingapi.RecordingJob, lastProgressAt time.Time, cause error) bool {
	if !continuousNoProgressExpired(lastProgressAt, time.Now(), w.cfg.ContinuousNoProgressTimeout) {
		return false
	}
	err := fmt.Errorf("continuous capture made no progress for %s", w.cfg.ContinuousNoProgressTimeout)
	if cause != nil {
		err = fmt.Errorf("%w: %v", err, cause)
	}
	return w.surrenderContinuousJobForReason(ctx, cancel, job, recordingapi.SurrenderNoProgress, err)
}

func (w *Worker) surrenderContinuousJobForReason(ctx context.Context, cancel context.CancelFunc, job recordingapi.RecordingJob, reason recordingapi.SurrenderReason, cause error) bool {
	errorText := SanitizeDiagnosticError(cause)
	if strings.TrimSpace(job.LeaseToken) != "" && job.SurrenderTransportVersion == 1 {
		result, surrenderErr := w.surrenderRecordingJobV1(ctx, job, reason, errorText)
		if surrenderErr != nil {
			// The bounded v1 transport budget elapsed without an authoritative
			// result. Stop heartbeating deliberately so the server's typed expiry
			// recovery can take over; never leave an apparently live silent lease.
			cancel()
			w.cfg.RelayDiagnostics.Finish(job.JobID, "surrender_transport_exhausted", surrenderErr)
			log.Printf("recording worker job=%d surrender transport exhausted: %v", job.JobID, surrenderErr)
			return true
		}
		switch result.Result {
		case "committed", "stale_fence":
			cancel()
			w.cfg.RelayDiagnostics.Finish(job.JobID, "surrendered", cause)
			return true
		case "window_closed":
			completeCtx, completeCancel := context.WithTimeout(context.Background(), 15*time.Second)
			completeErr := w.cfg.Client.CompleteRecordingJob(completeCtx, job.JobID, job.LeaseToken)
			completeCancel()
			cancel()
			if completeErr == nil {
				w.cfg.RelayDiagnostics.Finish(job.JobID, "done", nil)
				return true
			}
			w.cfg.RelayDiagnostics.Finish(job.JobID, "complete_failed", completeErr)
			return true
		case "stale_progress", "ineligible_spool":
			// The lease remains valid and its heartbeat has never stopped. The
			// caller resumes capture/delivery rather than expiring useful work.
			return false
		default:
			return false
		}
	}
	cancel()
	surrenderCtx, surrenderCancel := context.WithTimeout(ctx, 15*time.Second)
	defer surrenderCancel()
	if surrenderErr := w.cfg.Client.SurrenderRecordingJob(surrenderCtx, job.JobID, job.LeaseToken, reason, errorText); surrenderErr != nil {
		if job.WindowEndAt != nil && !time.Now().Before(*job.WindowEndAt) {
			completeCtx, completeCancel := context.WithTimeout(context.Background(), 15*time.Second)
			completeErr := w.cfg.Client.CompleteRecordingJob(completeCtx, job.JobID, job.LeaseToken)
			completeCancel()
			if completeErr == nil {
				w.cfg.RelayDiagnostics.Finish(job.JobID, "done", nil)
				return true
			}
			log.Printf("recording worker job=%d close-time complete after surrender failure failed: %v", job.JobID, completeErr)
		}
		w.cfg.RelayDiagnostics.Finish(job.JobID, "surrender_failed", surrenderErr)
		log.Printf("recording worker job=%d surrender failed: %v", job.JobID, surrenderErr)
		return true
	}
	w.cfg.RelayDiagnostics.Finish(job.JobID, "surrendered", cause)
	return true
}

func continuousNoProgressExpired(lastProgressAt, now time.Time, timeout time.Duration) bool {
	return timeout > 0 && !now.Before(lastProgressAt.Add(timeout))
}

func continuousMediaLagExpired(mediaEnd, now time.Time, timeout time.Duration) bool {
	return timeout > 0 && !mediaEnd.IsZero() && !now.Before(mediaEnd.Add(timeout))
}

// recordingCaptureTargetFPS is deliberately nil even for jobs created by an
// older API that carried target_fps. A non-nil value selects FFmpeg's x264
// branch; recording footage must always use source/native stream-copy.
func recordingCaptureTargetFPS(_ *int) *int { return nil }

func observeContinuousMediaLag(mediaEnd, now time.Time, timeout time.Duration, lagged *atomic.Bool, abort context.CancelFunc) {
	if continuousMediaLagExpired(mediaEnd, now, timeout) && lagged.CompareAndSwap(false, true) {
		abort()
	}
}

func continuousMediaLagCanSurrender(lagged bool, deliveryErr error, windowClosed bool) bool {
	return lagged && deliveryErr == nil && !windowClosed
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

// joinSegmentDeliveryError folds an asynchronous delivery failure into the
// attempt's capture error so it is classified by the same path an inline delivery
// failure was. A failure seen while ffmpeg is still running normally also surfaces
// through the next Submit (and therefore through captureErr, which is why the
// already-carried case is dropped rather than duplicated); a failure that lands
// after the last sweep would otherwise be invisible to the supervisor.
func joinSegmentDeliveryError(captureErr, deliveryErr error) error {
	if deliveryErr == nil || errors.Is(captureErr, deliveryErr) {
		return captureErr
	}
	if captureErr == nil {
		return deliveryErr
	}
	return errors.Join(captureErr, deliveryErr)
}

func continuousDeliveryFailureShouldFail(captureErr error, windowClosed bool) bool {
	return errors.Is(captureErr, errPermanentSegmentDelivery) ||
		(!windowClosed && errors.Is(captureErr, errSegmentDeliveryExhausted))
}

const continuousReconnectMaxDelay = 30 * time.Second

// reconnectBackoff returns a deterministic per-job jittered delay. It starts at
// 1-2s for fast recovery after a healthy source drop and grows to 15-30s for a
// persistently dead source without synchronizing every job against one origin.
// Keeping this well below the no-progress handoff timeout bounds how long an
// active window can miss a source that recovered between attempts. FFmpeg exit
// diagnostics are strings, not typed HTTP errors, so applying this only to
// 404/5xx would be unreliable and could strand an equally transient transport
// failure on the old multi-minute backoff.
func reconnectBackoff(jobID int64, failures int) time.Duration {
	return reconnectBackoffFor(jobID, failures, 2*time.Second, continuousReconnectMaxDelay)
}

func (w *Worker) nextReconnectDelay(jobID int64, failures int) time.Duration {
	if w.reconnectDelay == nil {
		return reconnectBackoff(jobID, failures)
	}
	return w.reconnectDelay(jobID, failures)
}

func reconnectBackoffFor(jobID int64, failures int, base, maxDelay time.Duration) time.Duration {
	nominal := base
	for attempt := 1; attempt < failures && nominal < maxDelay; attempt++ {
		if nominal > maxDelay/2 {
			nominal = maxDelay
			break
		}
		nominal *= 2
	}
	nominal = min(nominal, maxDelay)
	return jitteredDelay(jobID, failures, nominal)
}

func jitteredDelay(jobID int64, attempt int, nominal time.Duration) time.Duration {
	hash := uint64(jobID)*0x9e3779b97f4a7c15 ^ uint64(attempt)*0xbf58476d1ce4e5b9
	return nominal * time.Duration(50+hash%51) / 100
}

func retrySegmentDelivery(ctx context.Context, delay time.Duration, hasSpace func() bool, deliver func() error, onRetry func(error)) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !hasSpace() {
			return errDiskPressure
		}
		err := deliver()
		if err == nil {
			return nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if !retryableTransportError(ctx, err) {
			return fmt.Errorf("%w: %v", errPermanentSegmentDelivery, err)
		}
		onRetry(err)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func continuousSegmentDeliveryContext(jobCtx context.Context, windowEnd *time.Time, now time.Time) (context.Context, context.CancelFunc) {
	deadline := now.Add(segmentDeliveryRetryBudget)
	if windowEnd != nil && !windowEnd.IsZero() {
		postWindowDeadline := windowEnd.UTC().Add(postWindowDeliveryGrace)
		if postWindowDeadline.Before(deadline) {
			deadline = postWindowDeadline
		}
	}
	return context.WithDeadline(jobCtx, deadline)
}

func shouldCleanupContinuousAttempt(deliveryPending bool, captureErr error, mediaLagged bool) bool {
	// A lag-triggered stop deliberately accepted its entire finalized spool before
	// stopping capture. If that drain then failed, retain the files for operator
	// recovery instead of applying the ordinary terminal-error cleanup policy.
	if deliveryPending && mediaLagged {
		return false
	}
	return !deliveryPending ||
		errors.Is(captureErr, context.Canceled) ||
		errors.Is(captureErr, errDiskPressure) ||
		errors.Is(captureErr, errPermanentSegmentDelivery) ||
		errors.Is(captureErr, errSegmentDeliveryExhausted)
}

func shouldCleanupCaptureProducerAttempt(hasDurableProducer, durableProducerSpool, deliveryPending bool, captureErr error, mediaLagged bool) bool {
	// A v1 producer is the server-side promise that this exact local spool will
	// either ingest or receive an honest terminal result. Pending producer bytes
	// therefore survive every ordinary capture/error/window-close classification;
	// expiry recovery, not local cleanup, owns their final disposition.
	if hasDurableProducer && durableProducerSpool {
		return false
	}
	return shouldCleanupContinuousAttempt(deliveryPending, captureErr, mediaLagged)
}

func frozenHLSCanObserve(windowClosed bool, deliveryErr error, deliveryPending bool, captureErr error) bool {
	return !windowClosed && deliveryErr == nil && !deliveryPending &&
		capture.IsCleanContinuousNoOutput(captureErr)
}

func (w *Worker) ensureCaptureTempDir() error {
	root := strings.TrimSpace(w.cfg.CaptureTempDir)
	if root == "" {
		return nil
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return fmt.Errorf("create capture temp directory: %w", err)
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("capture temp directory is not a private real directory")
	}
	if err := os.Chmod(root, 0o700); err != nil {
		return fmt.Errorf("make capture temp directory private: %w", err)
	}
	info, err = os.Lstat(root)
	if err != nil || info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("capture temp directory permissions are not private")
	}
	return nil
}

func retryableTransportError(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() != nil || errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, errReplaySegmentDelivery) {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
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
	return recordingapi.ErrorCodeFrom(err) == recordingapi.ErrorCodeClipAlreadyIngested
}

func isUploadIntentStateConflict(err error) bool {
	return recordingapi.ErrorCodeFrom(err) == recordingapi.ErrorCodeUploadIntentUnavailable
}

// startHeartbeat extends the lease on a ticker; on a cancel signal it cancels the
// job context (aborting ffmpeg). The returned func reports whether a cancel was
// observed, so the caller skips ingest for a canceled job.
func (w *Worker) startHeartbeat(ctx context.Context, cancel context.CancelFunc, jobID int64, leaseToken string, leaseExpiresAt time.Time) func() bool {
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
				cancelSignal, renewedUntil, err := w.cfg.Client.HeartbeatRecordingJob(heartbeatCtx, jobID, leaseToken)
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

func (w *Worker) fail(ctx context.Context, jobID int64, leaseToken string, runErr error) {
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
	if err := w.cfg.Client.FailRecordingJob(failCtx, jobID, leaseToken, errText); err != nil {
		log.Printf("recording worker job=%d fail report failed: %v", jobID, err)
	}
}
