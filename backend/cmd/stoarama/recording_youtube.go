package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/captureapi"
	"github.com/daydemir/stoarama/backend/internal/capturescheduled"
	"github.com/daydemir/stoarama/backend/internal/model"
)

const nodeTypeLocalRecorderCLI = "local_recorder"

func runRecording(args []string) {
	if len(args) < 1 {
		usage()
		os.Exit(2)
	}
	switch args[0] {
	case "youtube":
		runRecordingYouTube(args[1:])
	default:
		usage()
		os.Exit(2)
	}
}

func runRecordingYouTube(args []string) {
	if len(args) < 1 || args[0] != "run" {
		fatalf("usage: stoarama recording youtube run --stream-id N [--api-base-url URL --node-token TOKEN --server-id ID --worker-id ID --cookies-file FILE|--cookies-from-browser BROWSER --yt-dlp-bin PATH --yt-dlp-format FORMAT --yt-dlp-format-sort SORT --duration 0]")
	}

	cfg, _ := loadCLIConfig()
	fs := flag.NewFlagSet("recording youtube run", flag.ExitOnError)
	apiBaseURL := fs.String("api-base-url", nodeAPIBaseURLForType(cfg, nodeTypeLocalRecorderCLI), "Stoarama API base URL")
	nodeToken := fs.String("node-token", "", "local recorder node bearer token")
	streamID := fs.Int64("stream-id", 0, "YouTube watch stream id")
	serverID := fs.String("server-id", defaultLocalRecorderServerID(), "stable recorder server id")
	workerID := fs.String("worker-id", defaultLocalRecorderWorkerID(), "stable recorder worker id")
	heartbeatSec := fs.Int("heartbeat-sec", 15, "recording heartbeat interval seconds")
	leaseSec := fs.Int("lease-sec", 45, "recording heartbeat lease seconds")
	refreshSec := fs.Int("refresh-sec", 5, "capture queue refresh interval seconds")
	duration := fs.Duration("duration", 0, "optional run duration")
	cookiesFile := fs.String("cookies-file", strings.TrimSpace(os.Getenv("YT_DLP_COOKIES_FILE")), "yt-dlp cookies file")
	cookiesFromBrowser := fs.String("cookies-from-browser", strings.TrimSpace(os.Getenv("YT_DLP_COOKIES_FROM_BROWSER")), "yt-dlp cookies-from-browser value")
	ytDlpBin := fs.String("yt-dlp-bin", strings.TrimSpace(os.Getenv("YT_DLP_BIN")), "optional yt-dlp binary path")
	ytDlpFormat := fs.String("yt-dlp-format", strings.TrimSpace(os.Getenv("YT_DLP_FORMAT")), "optional yt-dlp format selector")
	ytDlpFormatSort := fs.String("yt-dlp-format-sort", strings.TrimSpace(os.Getenv("YT_DLP_FORMAT_SORT")), "optional yt-dlp format sort selector")
	_ = fs.Parse(args[1:])

	if *streamID <= 0 {
		fatalf("--stream-id is required")
	}
	if strings.TrimSpace(*serverID) == "" {
		fatalf("--server-id is required")
	}
	if strings.TrimSpace(*workerID) == "" {
		fatalf("--worker-id is required")
	}
	if *heartbeatSec <= 0 {
		fatalf("--heartbeat-sec must be > 0")
	}
	if *leaseSec <= *heartbeatSec || *leaseSec > 3600 {
		fatalf("--lease-sec must be greater than --heartbeat-sec and <= 3600")
	}
	if *refreshSec <= 0 {
		fatalf("--refresh-sec must be > 0")
	}

	requireBinary("ffmpeg")
	requireBinary("ffprobe")
	if strings.TrimSpace(*ytDlpBin) == "" {
		requireBinary("yt-dlp")
	} else if _, err := os.Stat(strings.TrimSpace(*ytDlpBin)); err != nil {
		fatalf("yt-dlp binary not found at %s: %v", strings.TrimSpace(*ytDlpBin), err)
	}
	configureRecorderYouTubeEnv(*cookiesFile, *cookiesFromBrowser, *ytDlpBin, *ytDlpFormat, *ytDlpFormatSort)

	baseURL, token := mustResolveNodeAuthForType(*apiBaseURL, *nodeToken, nodeTypeLocalRecorderCLI)
	nodeInfo := mustLoadNodeInfo(baseURL, token)
	if strings.TrimSpace(nodeInfo.Node.NodeType) != nodeTypeLocalRecorderCLI {
		fatalf("node type %q cannot run local YouTube recording; enroll a local_recorder node", nodeInfo.Node.NodeType)
	}

	client, err := captureapi.NewClient(captureapi.ClientConfig{BaseURL: baseURL, APIToken: token})
	if err != nil {
		fatalf("init capture api client: %v", err)
	}
	registry, err := capture.NewDefaultRegistry()
	if err != nil {
		fatalf("init capture registry: %v", err)
	}

	rootCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	runCtx := rootCtx
	cancel := func() {}
	if *duration > 0 {
		runCtx, cancel = context.WithTimeout(rootCtx, *duration)
	}
	defer cancel()

	stream, err := client.GetStream(runCtx, *streamID)
	if err != nil {
		fatalf("load stream %d: %v", *streamID, err)
	}
	if strings.TrimSpace(stream.CaptureType) != capture.CaptureTypeYouTubeWatch {
		fatalf("stream %d capture_type=%q; expected youtube_watch", *streamID, stream.CaptureType)
	}

	meta := recorderMetadata(nodeInfo, *serverID, *workerID)
	hbReq := captureapi.RecordingServerHeartbeatRequest{
		ServerID: strings.TrimSpace(*serverID),
		LeaseSec: *leaseSec,
		ExecutionClasses: []captureapi.RecordingServerHeartbeatClass{{
			ExecutionClass: capture.ExecutionClassYouTubeDirect,
			MaxActive:      1,
		}},
		MetadataJSON: meta,
	}
	if err := client.RecordingServerHeartbeat(runCtx, hbReq); err != nil {
		fatalf("recording server heartbeat: %v", err)
	}
	if !stream.IsRecordingOn() {
		if err := client.SetRecordingState(runCtx, captureapi.RecordingStateUpdateRequest{
			StreamID:       stream.ID,
			State:          model.RecordingStateOn,
			ExecutionClass: capture.ExecutionClassYouTubeDirect,
			Actor:          "stoarama.local_recorder",
			Reason:         "local recorder start",
		}); err != nil {
			fatalf("turn recording on: %v", err)
		}
	}
	assignment, err := client.AssignRecordingStream(runCtx, captureapi.RecordingAssignRequest{
		StreamID:       stream.ID,
		ServerID:       strings.TrimSpace(*serverID),
		ExecutionClass: capture.ExecutionClassYouTubeDirect,
		Actor:          "stoarama.local_recorder",
		Reason:         "local YouTube recorder start",
	})
	if err != nil {
		fatalf("assign stream to local recorder: %v", err)
	}
	log.Printf("recording youtube sampled stream_id=%d server_id=%s assignment_revision=%d", stream.ID, assignment.ServerID, assignment.AssignmentRevision)

	managedCtx, managedCancel := context.WithCancel(runCtx)
	defer managedCancel()
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		if err := client.RecordingServerStopped(stopCtx, strings.TrimSpace(*serverID)); err != nil {
			log.Printf("recording server stopped signal failed server_id=%s: %v", strings.TrimSpace(*serverID), err)
		}
	}()

	errCh := make(chan error, 2)
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		if err := runLocalRecorderServerHeartbeatLoop(managedCtx, client, hbReq, time.Duration(*heartbeatSec)*time.Second); err != nil {
			errCh <- err
		}
	}()

	worker, err := capturescheduled.NewWorker(capturescheduled.Config{
		Client:            client,
		Registry:          registry,
		WorkerID:          strings.TrimSpace(*workerID),
		ServerID:          strings.TrimSpace(*serverID),
		Concurrency:       1,
		LeaseSec:          *leaseSec,
		PollInterval:      time.Duration(*refreshSec) * time.Second,
		HeartbeatInterval: time.Duration(*heartbeatSec) * time.Second,
		MetadataJSON:      meta,
		StreamIDs:         []int64{stream.ID},
		ExecutionClass:    capture.ExecutionClassYouTubeDirect,
	})
	if err != nil {
		fatalf("init sampled youtube worker: %v", err)
	}

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- worker.Run(managedCtx)
	}()

	select {
	case <-runCtx.Done():
		managedCancel()
		err := <-runErrCh
		<-heartbeatDone
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			fatalf("recording stopped with error: %v", err)
		}
	case err := <-errCh:
		managedCancel()
		<-runErrCh
		<-heartbeatDone
		fatalf("recording heartbeat failed: %v", err)
	case err := <-runErrCh:
		managedCancel()
		<-heartbeatDone
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			fatalf("recording failed: %v", err)
		}
	}
}

func runLocalRecorderServerHeartbeatLoop(ctx context.Context, client *captureapi.Client, req captureapi.RecordingServerHeartbeatRequest, interval time.Duration) error {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			hbCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := client.RecordingServerHeartbeat(hbCtx, req)
			cancel()
			if err != nil {
				return err
			}
		}
	}
}

func configureRecorderYouTubeEnv(cookiesFile, cookiesFromBrowser, ytDlpBin, ytDlpFormat, ytDlpFormatSort string) {
	setOrUnsetEnv("YT_DLP_COOKIES_FILE", cookiesFile)
	setOrUnsetEnv("YT_DLP_COOKIES_FROM_BROWSER", cookiesFromBrowser)
	setOrUnsetEnv("YT_DLP_BIN", ytDlpBin)
	setOrUnsetEnv("YT_DLP_FORMAT", ytDlpFormat)
	setOrUnsetEnv("YT_DLP_FORMAT_SORT", ytDlpFormatSort)
	_ = os.Setenv("CAPTURE_FFMPEG_RECONNECT", "true")
	_ = os.Setenv("CAPTURE_FFMPEG_RECONNECT_DELAY_MAX_SEC", "2")
}

func setOrUnsetEnv(key, value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		_ = os.Unsetenv(key)
		return
	}
	_ = os.Setenv(key, value)
}

func requireBinary(name string) {
	if _, err := exec.LookPath(name); err != nil {
		fatalf("missing required binary %q in PATH", name)
	}
}

func defaultLocalRecorderServerID() string {
	return "local-recorder-" + sanitizeRecorderToken(defaultHostname())
}

func defaultLocalRecorderWorkerID() string {
	return defaultLocalRecorderServerID() + "-youtube"
}

func sanitizeRecorderToken(raw string) string {
	v := strings.ToLower(strings.TrimSpace(raw))
	if v == "" {
		return "unknown"
	}
	var b strings.Builder
	for _, r := range v {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('-')
	}
	out := strings.Trim(b.String(), "-_")
	if out == "" {
		return "unknown"
	}
	return out
}

func recorderMetadata(nodeInfo nodeMeResponse, serverID, workerID string) map[string]any {
	return map[string]any{
		"server_id":         strings.TrimSpace(serverID),
		"worker_id":         strings.TrimSpace(workerID),
		"process_name":      "stoarama-recording-youtube",
		"process_id":        strings.TrimSpace(workerID),
		"host":              defaultHostname(),
		"platform":          defaultPlatform(),
		"node_id":           nodeInfo.Node.ID,
		"node_type":         nodeInfo.Node.NodeType,
		"node_display_name": nodeInfo.Node.DisplayName,
		"cli_binary":        "stoarama",
	}
}
