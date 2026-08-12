package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/netguard"
	"github.com/daydemir/stoarama/backend/internal/recordingapi"
	"github.com/daydemir/stoarama/backend/internal/recordingworker"
)

const recordingCanaryDuration = 15 * time.Second
const recordingCanarySafetyPoll = 250 * time.Millisecond

func runRecordingCanary(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("canary", flag.ContinueOnError)
	recordingID := fs.Int64("recording-id", 0, "recording id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *recordingID <= 0 {
		return fmt.Errorf("--recording-id is required")
	}
	if err := configureCanaryCaptureRuntime(); err != nil {
		return err
	}

	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{
		BaseURL:   cfg.APIURL,
		NodeToken: cfg.NodeToken,
	})
	if err != nil {
		return fmt.Errorf("init recording API client: %w", err)
	}
	spec, err := client.StartRecordingCanary(ctx, *recordingID)
	if err != nil {
		return fmt.Errorf("canary safety check: %s", recordingworker.SanitizeDiagnosticError(err))
	}
	reservationCtx, cancelReservation := context.WithDeadline(ctx, spec.SafeUntil)
	defer cancelReservation()
	defer func() {
		finishCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = client.FinishRecordingCanary(finishCtx, spec.RecordingID, spec.ReservationID)
	}()
	if spec.NodeID != cfg.NodeID || spec.RecordingID != *recordingID {
		return fmt.Errorf("canary safety response did not match this relay")
	}
	if spec.ReservationID == "" || time.Until(spec.SafeUntil) < recordingCanaryDuration+30*time.Second {
		return fmt.Errorf("canary safety window is too short")
	}

	resolveCtx, cancelResolve := context.WithTimeout(reservationCtx, 60*time.Second)
	resolvedURL, isImage, inputHeaders, err := capture.ResolveCaptureInputWithHeaders(
		resolveCtx, spec.Provider, spec.SourceURL, spec.SourcePageURL,
	)
	cancelResolve()
	if err != nil {
		return fmt.Errorf("resolve canary source: %s", recordingworker.SanitizeDiagnosticError(err))
	}
	if isImage {
		return fmt.Errorf("recording canary requires a video source")
	}
	if _, err := netguard.ValidatePublicURL(resolvedURL); err != nil {
		return fmt.Errorf("resolved canary source rejected: %s", recordingworker.SanitizeDiagnosticError(err))
	}

	// Recheck immediately before starting FFmpeg. The server-side reservation is
	// atomic with the production lease lock and remains checked four times per second.
	confirmedSpec, err := client.CheckRecordingCanary(reservationCtx, *recordingID, spec.ReservationID)
	if err != nil {
		return fmt.Errorf("final canary safety check: %s", recordingworker.SanitizeDiagnosticError(err))
	}
	if !sameCanarySource(spec, confirmedSpec) {
		return fmt.Errorf("canary source changed during resolution")
	}
	canaryCtx, cancelCanary := context.WithCancel(reservationCtx)
	defer cancelCanary()
	watchDone := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(recordingCanarySafetyPoll)
		defer ticker.Stop()
		err := watchRecordingCanarySafety(canaryCtx, ticker.C, spec, func(checkCtx context.Context) (recordingapi.RecordingCanarySpec, error) {
			return client.CheckRecordingCanary(checkCtx, *recordingID, spec.ReservationID)
		})
		if err != nil {
			cancelCanary()
		}
		watchDone <- err
	}()

	root, err := os.MkdirTemp("", "stoarama-relay-canary-*")
	if err != nil {
		cancelCanary()
		<-watchDone
		return fmt.Errorf("create private canary directory: %w", err)
	}
	defer os.RemoveAll(root)
	started := time.Now()
	seg, captureErr := capture.CaptureSegmentInDirWithHeadersNoThumbnail(
		canaryCtx, resolvedURL, recordingCanaryDuration, "", root, inputHeaders,
	)
	var validationErr error
	if captureErr == nil {
		defer capture.CleanupSegment(seg)
		if err := capture.ValidateSegmentFile(canaryCtx, seg.Path); err != nil {
			validationErr = fmt.Errorf("canary media probe failed: %s", recordingworker.SanitizeDiagnosticError(err))
		} else if err := capture.ValidateSegmentDecode(canaryCtx, seg.Path); err != nil {
			validationErr = fmt.Errorf("canary media decode failed: %s", recordingworker.SanitizeDiagnosticError(err))
		}
	}
	cancelCanary()
	observedSafetyErr := <-watchDone
	if observedSafetyErr != nil {
		return fmt.Errorf("canary stopped because production safety could not be confirmed: %s",
			recordingworker.SanitizeDiagnosticError(observedSafetyErr))
	}
	if reservationCtx.Err() == context.DeadlineExceeded && !time.Now().Before(spec.SafeUntil) {
		return fmt.Errorf("canary reservation expired before media validation completed")
	}
	if captureErr != nil {
		return fmt.Errorf("canary capture failed: %s", recordingworker.SanitizeDiagnosticError(captureErr))
	}
	if validationErr != nil {
		return validationErr
	}
	if err := client.CompleteRecordingCanary(ctx, spec.RecordingID, spec.ReservationID, recordingapi.RecordingCanaryResult{
		DurationMS: seg.DurationMs, SizeBytes: seg.SizeBytes, SHA256: seg.SHA256,
		VideoCodec: seg.VideoCodec, ProbeOK: true, DecodeOK: true, NativeCopy: true,
		Uploaded: false, RelayVersion: version, SourceRevision: sourceRevision,
	}); err != nil {
		return fmt.Errorf("persist canary validation: %s", recordingworker.SanitizeDiagnosticError(err))
	}

	hostname, _ := os.Hostname()
	fps := "unknown"
	if seg.ActualFPS != nil {
		fps = fmt.Sprintf("%.3f", *seg.ActualFPS)
	}
	fmt.Printf("canary_ok recording_id=%d stream_id=%d node_id=%d host=%s relay_version=%s source_revision=%s elapsed=%s duration_ms=%d size_bytes=%d sha256=%s video_codec=%s audio_codec=%s audio_present=%t fps=%s dimensions=%dx%d probe_ok=true decode_ok=true native_copy=true uploaded=false\n",
		spec.RecordingID, spec.StreamID, spec.NodeID, hostname, version, sourceRevision,
		time.Since(started).Round(time.Millisecond), seg.DurationMs, seg.SizeBytes, seg.SHA256,
		seg.VideoCodec, seg.AudioCodec, seg.AudioPresent, fps, seg.VideoWidth, seg.VideoHeight)
	return nil
}

func watchRecordingCanarySafety(
	ctx context.Context,
	ticks <-chan time.Time,
	expected recordingapi.RecordingCanarySpec,
	check func(context.Context) (recordingapi.RecordingCanarySpec, error),
) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case _, ok := <-ticks:
			if !ok {
				return fmt.Errorf("canary safety ticker stopped")
			}
			checkCtx, cancel := context.WithTimeout(ctx, time.Second)
			actual, err := check(checkCtx)
			cancel()
			// Normal completion cancels ctx to stop this watcher. An in-flight
			// check then returns context.Canceled; that is not a safety failure.
			if ctx.Err() != nil {
				return nil
			}
			if err != nil {
				return err
			}
			if !sameCanarySource(expected, actual) {
				return fmt.Errorf("canary source changed")
			}
		}
	}
}

func sameCanarySource(a, b recordingapi.RecordingCanarySpec) bool {
	return a.ReservationID == b.ReservationID && a.RecordingID == b.RecordingID &&
		a.NodeID == b.NodeID && a.StreamID == b.StreamID && a.Provider == b.Provider &&
		a.SourceURL == b.SourceURL && a.SourcePageURL == b.SourcePageURL
}

func configureCanaryCaptureRuntime() error {
	bd, err := binDir()
	if err != nil {
		return err
	}
	ytdlp := filepath.Join(bd, "yt-dlp")
	if err := os.Setenv("TZ", "UTC"); err != nil {
		return fmt.Errorf("set TZ: %w", err)
	}
	if err := os.Setenv("YT_DLP_BIN", ytdlp); err != nil {
		return fmt.Errorf("set YT_DLP_BIN: %w", err)
	}
	configureYTDLPJSRuntime(bd, ytdlp)
	if err := os.Unsetenv("YT_DLP_COOKIES_FROM_BROWSER"); err != nil {
		return fmt.Errorf("unset YT_DLP_COOKIES_FROM_BROWSER: %w", err)
	}
	if err := os.Unsetenv("YT_DLP_COOKIES_FILE"); err != nil {
		return fmt.Errorf("unset YT_DLP_COOKIES_FILE: %w", err)
	}
	if err := configureRelayTLSRuntime(); err != nil {
		return fmt.Errorf("configure canary TLS runtime: %w", err)
	}
	if err := os.Setenv("FFMPEG_BIN", relayFFmpegBin(bd)); err != nil {
		return fmt.Errorf("set FFMPEG_BIN: %w", err)
	}
	prependPath(bd)
	return nil
}
