package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/model"
)

const recordingStateBulkActor = "stoaramactl.streams_recording_state_bulk"

type recordingStateBulkStream struct {
	ID             int64                `json:"id"`
	Provider       string               `json:"provider"`
	Name           string               `json:"name"`
	RecordingState model.RecordingState `json:"recording_state"`
}

type recordingStateBulkResult struct {
	DryRun                 bool                       `json:"dry_run"`
	RecordingState         model.RecordingState       `json:"recording_state"`
	PreserveKoreaProviders bool                       `json:"preserve_korea_providers"`
	Candidates             []recordingStateBulkStream `json:"candidates"`
	UpdatedCount           int                        `json:"updated_count"`
}

func runStreamsRecordingStateBulk(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("streams recording-state-bulk", flag.ExitOnError)
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	apiToken := fs.String("api-token", cfg.APIToken, "service token")
	recordingStateRaw := fs.String("recording-state", "", "recording state off")
	preserveKoreaProviders := fs.Bool("preserve-korea-providers", false, "preserve KBS/SPATIC/TOPIS/GIGAEYES streams")
	dryRunFlag := fs.Bool("dry-run", false, "print candidates without updating")
	apply := fs.Bool("apply", false, "apply updates")
	asJSON := fs.Bool("json", false, "print JSON")
	_ = fs.Parse(args)

	recordingState, ok := parseRecordingStateCLI(*recordingStateRaw)
	if !ok || recordingState != model.RecordingStateOff {
		log.Fatalf("--recording-state must be off")
	}
	dryRun := !*apply || *dryRunFlag
	candidates := fetchRecordingStateBulkCandidates(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), *preserveKoreaProviders)
	result := recordingStateBulkResult{
		DryRun:                 dryRun,
		RecordingState:         recordingState,
		PreserveKoreaProviders: *preserveKoreaProviders,
		Candidates:             candidates,
	}
	if !dryRun {
		for _, candidate := range candidates {
			mustAPIRequest(ctx, http.MethodPost, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), "/api/v1/imports/streams/recording-state", map[string]any{
				"stream_id":        candidate.ID,
				"recording_state":  string(recordingState),
				"recording_actor":  recordingStateBulkActor,
				"recording_reason": "bulk non-korea recording cost cleanup",
			})
			result.UpdatedCount++
		}
	}
	if *asJSON {
		printJSON(result)
		return
	}
	fmt.Printf("dry_run=%t recording_state=%s preserve_korea_providers=%t candidates=%d updated=%d\n",
		result.DryRun, result.RecordingState, result.PreserveKoreaProviders, len(result.Candidates), result.UpdatedCount)
	for _, candidate := range result.Candidates {
		fmt.Printf("stream_id=%d provider=%s recording_state=%s name=%q\n", candidate.ID, candidate.Provider, candidate.RecordingState, candidate.Name)
	}
}

func parseRecordingStateCLI(raw string) (model.RecordingState, bool) {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case string(model.RecordingStateOff):
		return model.RecordingStateOff, true
	case string(model.RecordingStateOn):
		return model.RecordingStateOn, true
	default:
		return "", false
	}
}

func fetchRecordingStateBulkCandidates(ctx context.Context, backendAPIURL string, apiToken string, preserveKoreaProviders bool) []recordingStateBulkStream {
	const pageSize = 2000
	candidates := []recordingStateBulkStream{}
	for offset := 0; ; offset += pageSize {
		q := url.Values{}
		q.Set("limit", strconv.Itoa(pageSize))
		q.Set("offset", strconv.Itoa(offset))
		q.Set("include_image_urls", "false")
		q.Set("recording_state", string(model.RecordingStateOn))
		q.Set("sort_by", "id")
		q.Set("sort_dir", "asc")
		payload := mustAPIGet(ctx, backendAPIURL, apiToken, "/api/v1/dashboard/streams?"+q.Encode())
		items, ok := payload["items"].([]any)
		if !ok {
			log.Fatalf("dashboard streams response missing items")
		}
		for _, raw := range items {
			candidate, ok := recordingStateBulkCandidateFromAPIItem(raw)
			if !ok {
				continue
			}
			if preserveKoreaProviders && model.IsKoreaRecordingProvider(candidate.Provider) {
				continue
			}
			candidates = append(candidates, candidate)
		}
		total := int(int64FromAny(payload["total"]))
		if len(items) == 0 || offset+len(items) >= total {
			break
		}
	}
	return candidates
}

func recordingStateBulkCandidateFromAPIItem(raw any) (recordingStateBulkStream, bool) {
	item := asMap(raw)
	stream := asMap(item["stream"])
	id := int64FromAny(stream["id"])
	if id <= 0 {
		return recordingStateBulkStream{}, false
	}
	state, ok := parseRecordingStateCLI(fmt.Sprint(stream["recording_state"]))
	if !ok || state != model.RecordingStateOn {
		return recordingStateBulkStream{}, false
	}
	return recordingStateBulkStream{
		ID:             id,
		Provider:       strings.TrimSpace(fmt.Sprint(stream["provider"])),
		Name:           strings.TrimSpace(fmt.Sprint(stream["name"])),
		RecordingState: state,
	}, true
}
