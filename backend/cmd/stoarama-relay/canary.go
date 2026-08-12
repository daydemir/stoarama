package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
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

	if err := configureCanaryCaptureRuntime(); err != nil {
		return err
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
	var safetyErr error
	var safetyMu sync.Mutex
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		ticker := time.NewTicker(recordingCanarySafetyPoll)
		defer ticker.Stop()
		for {
			select {
			case <-canaryCtx.Done():
				return
			case <-ticker.C:
				checkCtx, cancel := context.WithTimeout(canaryCtx, time.Second)
				checkedSpec, checkErr := client.CheckRecordingCanary(checkCtx, *recordingID, spec.ReservationID)
				cancel()
				if checkErr != nil || !sameCanarySource(spec, checkedSpec) {
					if checkErr == nil {
						checkErr = fmt.Errorf("canary source changed")
					}
					safetyMu.Lock()
					safetyErr = checkErr
					safetyMu.Unlock()
					cancelCanary()
					return
				}
			}
		}
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
	<-watchDone
	safetyMu.Lock()
	observedSafetyErr := safetyErr
	safetyMu.Unlock()
	if observedSafetyErr != nil {
		return fmt.Errorf("canary stopped because production safety could not be confirmed: %s",
			recordingworker.SanitizeDiagnosticError(observedSafetyErr))
	}
	if captureErr != nil {
		return fmt.Errorf("canary capture failed: %s", recordingworker.SanitizeDiagnosticError(captureErr))
	}
	if validationErr != nil {
		return validationErr
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
	os.Setenv("TZ", "UTC")
	os.Setenv("YT_DLP_BIN", ytdlp)
	configureYTDLPJSRuntime(bd, ytdlp)
	os.Unsetenv("YT_DLP_COOKIES_FROM_BROWSER")
	os.Unsetenv("YT_DLP_COOKIES_FILE")
	os.Setenv("FFMPEG_BIN", relayFFmpegBin(bd))
	prependPath(bd)
	return nil
}
