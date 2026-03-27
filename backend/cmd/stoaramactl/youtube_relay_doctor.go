package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/captureapi"
	"github.com/daydemir/stoarama/backend/internal/config"
)

func runYouTubeRelayDoctor(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("youtube-relay doctor", flag.ExitOnError)
	streamID := fs.Int64("stream-id", 0, "exact stream id to inspect")
	sourceServerID := fs.String("source-server-id", "", "source server id filter")
	sinkServerID := fs.String("sink-server-id", "", "optional sink server filter")
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
	timeoutSec := fs.Int("timeout-sec", 20, "relay probe timeout seconds")
	eventsLimit := fs.Int("events-limit", 5, "recent relay events to include per stream")
	asJSON := fs.Bool("json", false, "print JSON")
	_ = fs.Parse(args)

	if *streamID <= 0 && strings.TrimSpace(*sourceServerID) == "" {
		log.Fatalf("use --stream-id or --source-server-id")
	}
	if *timeoutSec <= 0 {
		log.Fatalf("--timeout-sec must be > 0")
	}
	if *eventsLimit < 0 {
		log.Fatalf("--events-limit must be >= 0")
	}

	client, err := captureapi.NewClient(captureapi.ClientConfig{
		BaseURL:  strings.TrimSpace(*backendAPIURL),
		APIToken: strings.TrimSpace(*apiToken),
	})
	if err != nil {
		log.Fatalf("init capture api client: %v", err)
	}

	limit := 200
	if *streamID > 0 {
		limit = 2000
	}
	routes, err := client.ListYouTubeRelayRoutes(ctx, strings.TrimSpace(*sourceServerID), strings.TrimSpace(*sinkServerID), "", limit, 0)
	if err != nil {
		log.Fatalf("list youtube relay routes: %v", err)
	}
	filtered := make([]captureapi.YouTubeRelayRoute, 0, len(routes))
	for _, route := range routes {
		if *streamID > 0 && route.StreamID != *streamID {
			continue
		}
		filtered = append(filtered, route)
	}
	if len(filtered) == 0 {
		log.Fatalf("no youtube relay routes matched the requested filters")
	}

	httpc := &http.Client{Timeout: time.Duration(*timeoutSec) * time.Second}
	items := make([]map[string]any, 0, len(filtered))
	for _, route := range filtered {
		item := map[string]any{
			"stream_id":           route.StreamID,
			"source_server_id":    route.SourceServerID,
			"sink_server_id":      route.SinkServerID,
			"assignment_revision": route.AssignmentRevision,
			"route_status":        route.Status,
			"relay_pull_url":      route.RelayPullURL,
			"route_error_text":    route.ErrorText,
			"route_updated_at":    route.UpdatedAt,
			"route_metadata_json": route.MetadataJSON,
			"source_url":          route.StreamURL,
			"source_page_url":     route.SourcePageURL,
		}
		if route.StreamID > 0 {
			supervisionItem, supErr := fetchRecordingSupervisionItem(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), route.StreamID)
			if supErr != nil {
				item["supervision_error"] = supErr.Error()
			} else if supervisionItem != nil {
				item["supervision"] = supervisionItem
			}
			if *eventsLimit > 0 {
				events, eventsErr := client.ListYouTubeRelayRouteEvents(ctx, route.StreamID, *eventsLimit, 0)
				if eventsErr != nil {
					item["route_events_error"] = eventsErr.Error()
				} else {
					item["route_events"] = events
				}
			}
		}
		if strings.TrimSpace(route.RelayPullURL) != "" {
			probeCtx, cancel := context.WithTimeout(ctx, time.Duration(*timeoutSec)*time.Second)
			item["relay_probe"] = inspectRelayPlaylist(probeCtx, httpc, strings.TrimSpace(route.RelayPullURL))
			cancel()
		}
		items = append(items, item)
	}

	if *asJSON {
		printJSON(map[string]any{
			"items":         items,
			"count":         len(items),
			"stream_id":     *streamID,
			"source_server": strings.TrimSpace(*sourceServerID),
			"sink_server":   strings.TrimSpace(*sinkServerID),
			"probe_timeout": *timeoutSec,
		})
		return
	}

	fmt.Printf("routes=%d probe_timeout=%ds\n", len(items), *timeoutSec)
	for _, item := range items {
		supervision, _ := item["supervision"].(map[string]any)
		relayProbe, _ := item["relay_probe"].(map[string]any)
		fmt.Printf(
			"stream_id=%v route_status=%v source=%v sink=%v relay_ok=%v supervision=%v reason=%v last_frame=%v\n",
			item["stream_id"],
			item["route_status"],
			item["source_server_id"],
			item["sink_server_id"],
			relayProbe["ok"],
			supervision["supervision_state"],
			supervision["supervision_reason"],
			supervision["last_frame_at"],
		)
		if relayProbe != nil {
			fmt.Printf("  relay status=%v playlist_hls=%v segment_url=%v segment_status=%v error=%v\n",
				relayProbe["status_code"], relayProbe["playlist_hls"], relayProbe["segment_url"], relayProbe["segment_status_code"], relayProbe["error"])
		}
		if errText := strings.TrimSpace(fmt.Sprint(item["route_error_text"])); errText != "" {
			fmt.Printf("  route_error=%s\n", errText)
		}
		if supervision != nil {
			if errText := strings.TrimSpace(fmt.Sprint(supervision["relay_error_text"])); errText != "" {
				fmt.Printf("  supervision relay_error=%s\n", errText)
			}
			if errText := strings.TrimSpace(fmt.Sprint(supervision["last_error_text"])); errText != "" {
				fmt.Printf("  supervision runtime_error=%s\n", errText)
			}
		}
		if events, ok := item["route_events"].([]captureapi.YouTubeRelayEvent); ok && len(events) > 0 {
			fmt.Printf("  recent_events:\n")
			for _, event := range events {
				eventAt := "<unknown>"
				if event.CreatedAt != nil {
					eventAt = event.CreatedAt.UTC().Format(time.RFC3339)
				}
				fmt.Printf("    %s %s actor=%s reason=%s err=%s\n",
					eventAt,
					event.Status,
					event.Actor,
					event.Reason,
					strings.TrimSpace(event.ErrorText),
				)
			}
		}
	}
}

func inspectRelayPlaylist(ctx context.Context, httpc *http.Client, relayPullURL string) map[string]any {
	out := map[string]any{
		"url": strings.TrimSpace(relayPullURL),
		"ok":  false,
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSpace(relayPullURL), nil)
	if err != nil {
		out["error"] = fmt.Sprintf("build request: %v", err)
		return out
	}
	resp, err := httpc.Do(req)
	if err != nil {
		out["error"] = fmt.Sprintf("fetch playlist: %v", err)
		return out
	}
	defer resp.Body.Close()
	out["status_code"] = resp.StatusCode
	body, err := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
	if err != nil {
		out["error"] = fmt.Sprintf("read playlist: %v", err)
		return out
	}
	trimmed := bytes.TrimSpace(body)
	playlistHLS := bytes.HasPrefix(trimmed, []byte("#EXTM3U"))
	out["playlist_hls"] = playlistHLS
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		out["error"] = fmt.Sprintf("playlist status=%d", resp.StatusCode)
		return out
	}
	segmentURL := ""
	for _, rawLine := range strings.Split(strings.ReplaceAll(string(trimmed), "\r\n", "\n"), "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		segmentURL = line
		break
	}
	out["segment_url"] = segmentURL
	if !playlistHLS {
		out["error"] = "playlist is not valid hls"
		return out
	}
	if segmentURL == "" {
		out["error"] = "playlist has no segment url"
		return out
	}
	segReq, err := http.NewRequestWithContext(ctx, http.MethodGet, segmentURL, nil)
	if err != nil {
		out["error"] = fmt.Sprintf("build segment request: %v", err)
		return out
	}
	segResp, err := httpc.Do(segReq)
	if err != nil {
		out["error"] = fmt.Sprintf("fetch segment: %v", err)
		return out
	}
	defer segResp.Body.Close()
	out["segment_status_code"] = segResp.StatusCode
	if _, err := io.Copy(io.Discard, io.LimitReader(segResp.Body, 1024)); err != nil {
		out["error"] = fmt.Sprintf("read segment: %v", err)
		return out
	}
	if segResp.StatusCode < 200 || segResp.StatusCode >= 300 {
		out["error"] = fmt.Sprintf("segment status=%d", segResp.StatusCode)
		return out
	}
	out["ok"] = true
	return out
}
