package main

import (
	"context"
	"flag"
	"net/http"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/config"
)

func runStreamsRecordingStateService(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("streams recording-state-service", flag.ExitOnError)
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	apiToken := fs.String("api-token", cfg.APIToken, "service token")
	streamID := fs.Int64("id", 0, "stream id")
	recordingState := fs.String("recording-state", "", "recording state off|on")
	asJSON := fs.Bool("json", false, "print JSON")
	_ = fs.Parse(args)

	if *streamID <= 0 {
		fs.Usage()
		return
	}
	out := mustAPIRequest(ctx, http.MethodPost, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), "/api/v1/imports/streams/recording-state", map[string]any{
		"stream_id":        *streamID,
		"recording_state":  strings.TrimSpace(*recordingState),
		"recording_actor":  "stoaramactl.streams_recording_state_service",
		"recording_reason": "service recording state update",
	})
	if *asJSON {
		printJSON(out)
		return
	}
	printJSON(out)
}
