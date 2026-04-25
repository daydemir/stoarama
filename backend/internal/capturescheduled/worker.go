package capturescheduled

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/captureapi"
	"github.com/daydemir/stoarama/backend/internal/model"
	"github.com/daydemir/stoarama/backend/internal/queue"
	"github.com/daydemir/stoarama/backend/internal/settings"
)

type Config struct {
	Pool              *pgxpool.Pool
	Client            *captureapi.Client
	Registry          *capture.Registry
	WorkerID          string
	ServerID          string
	Concurrency       int
	LeaseSec          int
	PollInterval      time.Duration
	HeartbeatInterval time.Duration
	MetadataJSON      map[string]any
	StreamIDs         []int64
}

type Worker struct {
	cfg          Config
	streamFilter map[int64]struct{}
}

func NewWorker(cfg Config) (*Worker, error) {
	if cfg.Pool == nil {
		return nil, fmt.Errorf("pool is required")
	}
	if cfg.Client == nil {
		return nil, fmt.Errorf("client is required")
	}
	if cfg.Registry == nil {
		return nil, fmt.Errorf("registry is required")
	}
	if strings.TrimSpace(cfg.WorkerID) == "" {
		return nil, fmt.Errorf("worker_id is required")
	}
	if cfg.Concurrency <= 0 {
		return nil, fmt.Errorf("concurrency must be > 0")
	}
	if cfg.LeaseSec <= 0 {
		cfg.LeaseSec = 45
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 5 * time.Second
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 15 * time.Second
	}
	filter := make(map[int64]struct{}, len(cfg.StreamIDs))
	for _, id := range cfg.StreamIDs {
		if id > 0 {
			filter[id] = struct{}{}
		}
	}
	return &Worker{cfg: cfg, streamFilter: filter}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	log.Printf("sampled capture worker start worker_id=%s concurrency=%d stream_filter=%d", w.cfg.WorkerID, w.cfg.Concurrency, len(w.streamFilter))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := w.heartbeatLoop(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()

	sem := make(chan struct{}, w.cfg.Concurrency)
	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()

	if err := w.tick(ctx, sem); err != nil {
		cancel()
		wg.Wait()
		return err
	}
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		case err := <-errCh:
			cancel()
			wg.Wait()
			return err
		case <-ticker.C:
			if err := w.tick(ctx, sem); err != nil {
				cancel()
				wg.Wait()
				return err
			}
		}
	}
}

func (w *Worker) heartbeatLoop(ctx context.Context) error {
	ticker := time.NewTicker(w.cfg.HeartbeatInterval)
	defer ticker.Stop()
	if err := w.heartbeat(ctx); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := w.heartbeat(ctx); err != nil {
				return err
			}
		}
	}
}

func (w *Worker) heartbeat(ctx context.Context) error {
	meta := map[string]any{}
	for k, v := range w.cfg.MetadataJSON {
		meta[k] = v
	}
	meta["server_id"] = strings.TrimSpace(w.cfg.ServerID)
	meta["process_name"] = "sampled-capture-worker"
	meta["sample_interval_min_sec"] = settings.DefaultSampleIntervalMinSec
	meta["sample_interval_max_sec"] = settings.DefaultSampleIntervalMaxSec
	return w.cfg.Client.WorkerHeartbeat(ctx, captureapi.WorkerHeartbeatRequest{
		WorkerID:       w.cfg.WorkerID,
		ExecutionClass: capture.ExecutionClassVideoLive,
		Capacity:       w.cfg.Concurrency,
		LeaseSec:       w.cfg.LeaseSec,
		MetadataJSON:   meta,
	})
}

func (w *Worker) tick(ctx context.Context, sem chan struct{}) error {
	rs, err := settings.GetRecordingSettings(ctx, w.cfg.Pool)
	if err != nil {
		return err
	}
	policy := queue.CaptureSamplingPolicy{
		MinIntervalSec: rs.SampleIntervalMinSec,
		MaxIntervalSec: rs.SampleIntervalMaxSec,
	}
	if err := queue.EnqueueDueCaptureJobs(ctx, w.cfg.Pool, policy); err != nil {
		return err
	}
	for {
		select {
		case sem <- struct{}{}:
		default:
			return nil
		}
		job, err := queue.LeaseOneCaptureJob(ctx, w.cfg.Pool, w.cfg.WorkerID, w.cfg.LeaseSec)
		if err != nil {
			<-sem
			return err
		}
		if job == nil {
			<-sem
			return nil
		}
		go func() {
			defer func() { <-sem }()
			w.processJob(ctx, *job, policy)
		}()
	}
}

func (w *Worker) processJob(ctx context.Context, job queue.CaptureJob, policy queue.CaptureSamplingPolicy) {
	nextDelaySec := randomDelaySec(policy)
	stream, err := w.cfg.Client.GetStream(ctx, job.StreamID)
	if err != nil {
		w.failJob(ctx, job, capture.ModeUnsupported, "", err, nextDelaySec)
		return
	}
	if !w.shouldCapture(stream) {
		if err := queue.CompleteCaptureJobWithoutNext(ctx, w.cfg.Pool, job.ID); err != nil {
			log.Printf("sampled capture stream_id=%d complete without next failed: %v", job.StreamID, err)
		}
		return
	}
	spec := buildSpec(stream)
	effective := capture.EffectiveMode(spec)
	adapter, ok := w.cfg.Registry.Get(effective)
	if !ok || !adapter.Supports(spec) {
		w.failJob(ctx, job, effective, "", fmt.Errorf("unsupported capture mode %s", effective), nextDelaySec)
		return
	}
	resolved, err := adapter.Resolve(ctx, spec)
	if err != nil {
		w.failJob(ctx, job, effective, "", err, nextDelaySec)
		return
	}
	segmentCtx, cancel := context.WithTimeout(ctx, capture.SegmentCaptureTimeout())
	seg, err := capture.CaptureSegment(segmentCtx, resolved.URL)
	cancel()
	if err != nil {
		w.failJob(ctx, job, effective, resolved.URL, err, nextDelaySec)
		return
	}
	defer capture.CleanupSegment(seg)
	if err := w.persistSegmentSuccess(ctx, stream.ID, effective, resolved.URL, seg); err != nil {
		w.failJob(ctx, job, effective, resolved.URL, err, nextDelaySec)
		return
	}
	if err := queue.CompleteCaptureJob(ctx, w.cfg.Pool, job.ID, nextDelaySec); err != nil {
		log.Printf("sampled capture stream_id=%d complete failed: %v", stream.ID, err)
		return
	}
	log.Printf("sampled capture stream_id=%d mode=%s next_delay_sec=%d", stream.ID, effective, nextDelaySec)
}

func (w *Worker) shouldCapture(stream model.Stream) bool {
	if stream.RecordingState != model.RecordingStateOn {
		return false
	}
	if len(w.streamFilter) > 0 {
		if _, ok := w.streamFilter[stream.ID]; !ok {
			return false
		}
	}
	family := strings.TrimSpace(stream.CaptureFamily)
	return family == "" || family == capture.CaptureFamilyContinuousVideo
}

func buildSpec(stream model.Stream) capture.StreamSpec {
	return capture.StreamSpec{
		ID:                 stream.ID,
		Provider:           stream.Provider,
		StreamURL:          stream.SourceURL,
		SourcePageURL:      stream.SourcePageURL,
		CaptureMode:        capture.LegacyModeForStream(stream.CaptureType, stream.ExecutionClass),
		CaptureConfig:      stream.ExecutionConfigJSON,
		CaptureIntervalSec: settings.DefaultRecordingIntervalSec,
		TargetFPS:          capture.SegmentTargetFPS,
		MaxFrameBytes:      25 << 20,
	}
}

func (w *Worker) persistSegmentSuccess(ctx context.Context, streamID int64, effective capture.Mode, resolvedURL string, seg capture.Segment) error {
	intent, err := w.cfg.Client.ReserveSegmentUpload(ctx, captureapi.SegmentUploadIntentRequest{
		StreamID:  streamID,
		MimeType:  seg.MIMEType,
		SizeBytes: seg.SizeBytes,
		StartAt:   seg.StartAt,
	})
	if err != nil {
		return fmt.Errorf("reserve segment upload: %w", err)
	}
	if err := w.cfg.Client.UploadFile(ctx, intent.UploadURL, seg.Path, seg.MIMEType); err != nil {
		return fmt.Errorf("upload segment: %w", err)
	}
	var thumbnailIntent *captureapi.SegmentUploadIntent
	if seg.Thumbnail != nil && strings.TrimSpace(seg.Thumbnail.Path) != "" {
		intent, err := w.cfg.Client.ReserveSegmentThumbnailUpload(ctx, captureapi.SegmentUploadIntentRequest{
			StreamID:  streamID,
			MimeType:  seg.Thumbnail.MIMEType,
			SizeBytes: seg.Thumbnail.SizeBytes,
			StartAt:   seg.StartAt,
		})
		if err != nil {
			log.Printf("sampled capture thumbnail skipped stream_id=%d error=%v", streamID, err)
		} else if err := w.cfg.Client.UploadFile(ctx, intent.UploadURL, seg.Thumbnail.Path, seg.Thumbnail.MIMEType); err != nil {
			log.Printf("sampled capture thumbnail upload skipped stream_id=%d error=%v", streamID, err)
		} else {
			thumbnailIntent = &intent
		}
	}
	return w.cfg.Client.IngestSegmentSuccess(ctx, captureapi.IngestSegmentSuccessRequest{
		StreamID:           streamID,
		SourceKind:         seg.SourceKind,
		EffectiveMode:      effective,
		ResolvedURL:        resolvedURL,
		UploadIntentID:     intent.IntentID,
		ObjectKey:          intent.ObjectKey,
		MIMEType:           seg.MIMEType,
		SizeBytes:          seg.SizeBytes,
		SHA256:             seg.SHA256,
		SegmentStartAt:     seg.StartAt,
		SegmentEndAt:       seg.EndAt,
		DurationMs:         seg.DurationMs,
		TargetFPS:          capture.SegmentTargetFPS,
		ActualFPS:          seg.ActualFPS,
		VideoCodec:         seg.VideoCodec,
		AudioCodec:         seg.AudioCodec,
		Container:          seg.Container,
		AudioPresent:       seg.AudioPresent,
		ThumbnailIntent:    thumbnailIntent,
		ThumbnailSizeBytes: thumbnailSizeBytes(seg.Thumbnail),
		ThumbnailSHA256:    thumbnailSHA256(seg.Thumbnail),
	})
}

func (w *Worker) failJob(ctx context.Context, job queue.CaptureJob, effective capture.Mode, resolvedURL string, runErr error, nextDelaySec int) {
	errText := strings.TrimSpace(runErr.Error())
	if errText == "" {
		errText = "capture failed"
	}
	if effective == capture.ModeUnsupported {
		effective = capture.ModeFFmpegDirect
	}
	consecutive, ingestErr := w.cfg.Client.IngestError(ctx, captureapi.IngestErrorRequest{
		StreamID:      job.StreamID,
		CapturedAt:    time.Now().UTC(),
		SourceKind:    sourceKindForMode(effective),
		EffectiveMode: effective,
		ResolvedURL:   resolvedURL,
		ErrorText:     errText,
	})
	if ingestErr != nil {
		log.Printf("sampled capture stream_id=%d ingest error failed: %v", job.StreamID, ingestErr)
	}
	if consecutive >= 2 {
		log.Printf("sampled capture stream_id=%d consecutive_errors=%d error=%s", job.StreamID, consecutive, errText)
	}
	if err := queue.FailCaptureJob(ctx, w.cfg.Pool, job.ID, errText, nextDelaySec); err != nil {
		log.Printf("sampled capture stream_id=%d fail job failed: %v", job.StreamID, err)
	}
}

func randomDelaySec(policy queue.CaptureSamplingPolicy) int {
	if policy.MaxIntervalSec <= policy.MinIntervalSec {
		return policy.MinIntervalSec
	}
	return policy.MinIntervalSec + rand.Intn(policy.MaxIntervalSec-policy.MinIntervalSec+1)
}

func sourceKindForMode(mode capture.Mode) string {
	if mode == capture.ModeImagePoll {
		return "snapshot_url"
	}
	return "live"
}

func thumbnailSizeBytes(thumb *capture.SegmentThumbnail) int64 {
	if thumb == nil {
		return 0
	}
	return thumb.SizeBytes
}

func thumbnailSHA256(thumb *capture.SegmentThumbnail) string {
	if thumb == nil {
		return ""
	}
	return strings.TrimSpace(thumb.SHA256)
}
