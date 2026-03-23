package youtuberelay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/captureapi"
)

type SourceAPI interface {
	YouTubeRelaySourceHeartbeat(ctx context.Context, req captureapi.YouTubeRelaySourceHeartbeatRequest) error
	YouTubeRelaySourceStopped(ctx context.Context, serverID string) error
	ListYouTubeRelayRoutes(ctx context.Context, sourceServerID, sinkServerID, status string, limit, offset int) ([]captureapi.YouTubeRelayRoute, error)
	UpdateYouTubeRelayRouteStatus(ctx context.Context, req captureapi.YouTubeRelayRouteStatusRequest) error
}

type NodeSourceAPI struct {
	Client *captureapi.Client
}

func (a NodeSourceAPI) YouTubeRelaySourceHeartbeat(ctx context.Context, req captureapi.YouTubeRelaySourceHeartbeatRequest) error {
	return a.Client.NodeYouTubeRelaySourceHeartbeat(ctx, req)
}

func (a NodeSourceAPI) YouTubeRelaySourceStopped(ctx context.Context, _ string) error {
	return a.Client.NodeYouTubeRelaySourceStopped(ctx)
}

func (a NodeSourceAPI) ListYouTubeRelayRoutes(ctx context.Context, _ string, _ string, status string, limit, offset int) ([]captureapi.YouTubeRelayRoute, error) {
	return a.Client.NodeListYouTubeRelayRoutes(ctx, status, limit, offset)
}

func (a NodeSourceAPI) UpdateYouTubeRelayRouteStatus(ctx context.Context, req captureapi.YouTubeRelayRouteStatusRequest) error {
	return a.Client.NodeUpdateYouTubeRelayRouteStatus(ctx, req)
}

type SourceRunnerOptions struct {
	ServerID                string
	ShardID                 string
	Capacity                int
	HeartbeatSec            int
	LeaseSec                int
	RefreshSec              int
	ResolveTimeoutSec       int
	ResolveFailureThreshold int
	BindAddr                string
	PublicBaseURL           string
	SharedToken             string
	CacheFile               string
	NetworkTransport        string
	TopologyID              string
	TopologyRole            string
	HubServerID             string
	WGInterface             string
	WGIP                    string
	SourceEndpoint          string
	MetadataJSON            map[string]any
	CookiesFile             string
	CookiesFromBrowser      string
	YTDLPBin                string
	YTDLPFormat             string
	YTDLPFormatSort         string
	FFMPEGJPEGQuality       int
	FFMPEGThreads           int
	FFMPEGHWAccel           string
	FFMPEGReconnect         bool
	FFMPEGReconnectDelayMax int
}

type relayRouteState struct {
	UpstreamURL string
}

type relaySourceRouteCacheEntry struct {
	UpstreamURL string    `json:"upstream_url"`
	UpdatedAt   time.Time `json:"updated_at"`
}

const (
	defaultRelayRouteRefreshDelay = 10 * time.Minute
	relayResolveRetryBackoff      = 2 * time.Minute
)

func RunSource(ctx context.Context, api SourceAPI, opts SourceRunnerOptions) error {
	if api == nil {
		return fmt.Errorf("source api is required")
	}
	if strings.TrimSpace(opts.ServerID) == "" {
		return fmt.Errorf("server id is required")
	}
	if strings.TrimSpace(opts.ShardID) == "" {
		return fmt.Errorf("shard id is required")
	}
	if opts.Capacity <= 0 {
		return fmt.Errorf("capacity must be > 0")
	}
	if opts.HeartbeatSec <= 0 {
		return fmt.Errorf("heartbeat_sec must be > 0")
	}
	if opts.LeaseSec <= opts.HeartbeatSec || opts.LeaseSec > 3600 {
		return fmt.Errorf("lease_sec must be > heartbeat_sec and <= 3600")
	}
	if opts.RefreshSec <= 0 {
		return fmt.Errorf("refresh_sec must be > 0")
	}
	if opts.ResolveTimeoutSec <= 0 {
		return fmt.Errorf("resolve_timeout_sec must be > 0")
	}
	if opts.ResolveFailureThreshold <= 0 {
		return fmt.Errorf("resolve_failure_threshold must be > 0")
	}
	if strings.TrimSpace(opts.BindAddr) == "" {
		return fmt.Errorf("bind_addr is required")
	}
	relayBaseURL := strings.TrimRight(strings.TrimSpace(opts.PublicBaseURL), "/")
	if relayBaseURL == "" {
		return fmt.Errorf("public_base_url is required")
	}
	relayBaseParsed, err := url.Parse(relayBaseURL)
	if err != nil {
		return fmt.Errorf("invalid public_base_url: %w", err)
	}
	if relayBaseParsed.Scheme != "http" && relayBaseParsed.Scheme != "https" {
		return fmt.Errorf("public_base_url must use http or https scheme")
	}
	if strings.TrimSpace(relayBaseParsed.Host) == "" {
		return fmt.Errorf("public_base_url must include a host")
	}
	if strings.TrimSpace(opts.SharedToken) == "" {
		return fmt.Errorf("shared_token is required")
	}
	if strings.TrimSpace(opts.NetworkTransport) == "" {
		return fmt.Errorf("network_transport is required")
	}
	if strings.TrimSpace(opts.TopologyID) == "" {
		return fmt.Errorf("topology_id is required")
	}
	if strings.TrimSpace(opts.TopologyRole) == "" {
		return fmt.Errorf("topology_role is required")
	}
	if strings.TrimSpace(opts.HubServerID) == "" {
		return fmt.Errorf("hub_server_id is required")
	}

	configureYouTubeCaptureEnv(
		strings.TrimSpace(opts.CookiesFile),
		strings.TrimSpace(opts.CookiesFromBrowser),
		strings.TrimSpace(opts.YTDLPBin),
		strings.TrimSpace(opts.YTDLPFormat),
		strings.TrimSpace(opts.YTDLPFormatSort),
		opts.FFMPEGJPEGQuality,
		opts.FFMPEGThreads,
		strings.TrimSpace(opts.FFMPEGHWAccel),
		opts.FFMPEGReconnect,
		opts.FFMPEGReconnectDelayMax,
	)

	meta := cloneMap(nonNilMap(opts.MetadataJSON))
	hostName := ""
	if h, err := os.Hostname(); err == nil {
		hostName = strings.TrimSpace(h)
	}
	meta["host"] = hostName
	meta["server_id"] = strings.TrimSpace(opts.ServerID)
	meta["shard_id"] = strings.TrimSpace(opts.ShardID)
	meta["process_name"] = "youtube-relay-source"
	meta["process_id"] = strings.TrimSpace(opts.ServerID)
	meta["relay_bind_addr"] = strings.TrimSpace(opts.BindAddr)
	meta["relay_public_base_url"] = relayBaseURL
	meta["network_transport"] = strings.TrimSpace(opts.NetworkTransport)
	meta["topology_id"] = strings.TrimSpace(opts.TopologyID)
	meta["topology_role"] = strings.TrimSpace(opts.TopologyRole)
	meta["hub_server_id"] = strings.TrimSpace(opts.HubServerID)
	if v := strings.TrimSpace(opts.WGInterface); v != "" {
		meta["wg_interface"] = v
	}
	if v := strings.TrimSpace(opts.WGIP); v != "" {
		meta["wg_ip"] = v
	}
	if v := strings.TrimSpace(opts.SourceEndpoint); v != "" {
		meta["source_endpoint"] = v
	}

	registry, err := capture.NewDefaultRegistry()
	if err != nil {
		return fmt.Errorf("init capture registry: %w", err)
	}
	ytAdapter, ok := registry.Get(capture.ModeYouTubeLive)
	if !ok {
		return fmt.Errorf("youtube_live adapter not registered")
	}

	relayState := struct {
		mu     sync.RWMutex
		routes map[int64]relayRouteState
	}{
		routes: map[int64]relayRouteState{},
	}
	relayHTTPClient := &http.Client{Timeout: 15 * time.Second}
	isHopByHopHeader := func(k string) bool {
		switch strings.ToLower(strings.TrimSpace(k)) {
		case "connection", "proxy-connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade":
			return true
		default:
			return false
		}
	}

	relayMux := http.NewServeMux()
	relayMux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	relayMux.HandleFunc("/relay/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if strings.TrimSpace(r.URL.Query().Get("token")) != strings.TrimSpace(opts.SharedToken) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		pathPart := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, "/relay/"))
		if pathPart == "" {
			http.Error(w, "missing relay stream id", http.StatusNotFound)
			return
		}
		relaySuffix := ""
		if idx := strings.IndexByte(pathPart, '/'); idx >= 0 {
			relaySuffix = strings.Trim(strings.TrimSpace(pathPart[idx+1:]), "/")
			pathPart = pathPart[:idx]
		}
		if idx := strings.IndexByte(pathPart, '.'); idx >= 0 {
			pathPart = pathPart[:idx]
		}
		streamID, parseErr := strconv.ParseInt(pathPart, 10, 64)
		if parseErr != nil || streamID <= 0 {
			http.Error(w, "invalid relay stream id", http.StatusNotFound)
			return
		}
		relayState.mu.RLock()
		state, ok := relayState.routes[streamID]
		relayState.mu.RUnlock()
		if !ok || strings.TrimSpace(state.UpstreamURL) == "" {
			http.Error(w, "relay route not ready", http.StatusNotFound)
			return
		}
		upstreamURL := strings.TrimSpace(state.UpstreamURL)
		if relaySuffix == "" {
			w.Header().Set("Cache-Control", "no-cache")
			w.Header().Set("X-Youtube-Relay-Server", strings.TrimSpace(opts.ServerID))
			w.Header().Set("X-Youtube-Relay-Stream", strconv.FormatInt(streamID, 10))
			http.Redirect(w, r, upstreamURL, http.StatusTemporaryRedirect)
			return
		}
		if relaySuffix != "" {
			if relaySuffix != "segment" {
				http.Error(w, "unknown relay path", http.StatusNotFound)
				return
			}
			upstreamURL = strings.TrimSpace(r.URL.Query().Get("u"))
			if upstreamURL == "" {
				http.Error(w, "missing upstream segment url", http.StatusBadRequest)
				return
			}
		}
		upstreamReq, err := http.NewRequestWithContext(r.Context(), r.Method, upstreamURL, nil)
		if err != nil {
			http.Error(w, "build upstream request failed", http.StatusBadGateway)
			return
		}
		for k, vals := range r.Header {
			if isHopByHopHeader(k) || strings.EqualFold(k, "host") {
				continue
			}
			for _, v := range vals {
				upstreamReq.Header.Add(k, v)
			}
		}
		resp, err := relayHTTPClient.Do(upstreamReq)
		if err != nil {
			http.Error(w, fmt.Sprintf("upstream request failed: %v", err), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		for k, vals := range resp.Header {
			if isHopByHopHeader(k) {
				continue
			}
			for _, v := range vals {
				w.Header().Add(k, v)
			}
		}
		w.Header().Set("X-Youtube-Relay-Server", strings.TrimSpace(opts.ServerID))
		w.Header().Set("X-Youtube-Relay-Stream", strconv.FormatInt(streamID, 10))
		if relaySuffix == "" && r.Method == http.MethodGet && resp.StatusCode >= 200 && resp.StatusCode < 300 {
			body, readErr := io.ReadAll(resp.Body)
			if readErr != nil {
				http.Error(w, fmt.Sprintf("read upstream response failed: %v", readErr), http.StatusBadGateway)
				return
			}
			if rewritten, ok := rewriteRelayPlaylist(relayBaseURL, streamID, strings.TrimSpace(opts.SharedToken), upstreamURL, body); ok {
				w.Header().Del("Content-Length")
				w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
				w.WriteHeader(resp.StatusCode)
				_, _ = w.Write(rewritten)
				return
			}
			w.WriteHeader(resp.StatusCode)
			_, _ = w.Write(body)
			return
		}
		w.WriteHeader(resp.StatusCode)
		if r.Method == http.MethodHead {
			return
		}
		_, _ = io.Copy(w, resp.Body)
	})

	relayServer := &http.Server{
		Addr:              strings.TrimSpace(opts.BindAddr),
		Handler:           relayMux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer shutdownCancel()
		if err := relayServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("youtube-relay source shutdown relay server failed: %v", err)
		}
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		if stopErr := api.YouTubeRelaySourceStopped(stopCtx, strings.TrimSpace(opts.ServerID)); stopErr != nil {
			log.Printf("youtube-relay source stop signal failed server_id=%s: %v", strings.TrimSpace(opts.ServerID), stopErr)
		}
	}()

	errCh := make(chan error, 4)
	sendErr := func(err error) {
		if err == nil {
			return
		}
		select {
		case errCh <- err:
		default:
			log.Printf("youtube-relay source dropped async error due full channel: %v", err)
		}
	}
	go func() {
		log.Printf("youtube-relay source relay server listen bind=%s public_base=%s", strings.TrimSpace(opts.BindAddr), relayBaseURL)
		err := relayServer.ListenAndServe()
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return
		}
		sendErr(fmt.Errorf("youtube relay source http server: %w", err))
	}()

	var hbWG sync.WaitGroup
	hbWG.Add(1)
	go func() {
		defer hbWG.Done()
		ticker := time.NewTicker(time.Duration(opts.HeartbeatSec) * time.Second)
		defer ticker.Stop()
		heartbeatFailures := 0
		for {
			hbCtx, hbCancel := context.WithTimeout(ctx, 10*time.Second)
			hbErr := api.YouTubeRelaySourceHeartbeat(hbCtx, captureapi.YouTubeRelaySourceHeartbeatRequest{
				ServerID:     strings.TrimSpace(opts.ServerID),
				ShardID:      strings.TrimSpace(opts.ShardID),
				MaxActive:    opts.Capacity,
				Draining:     false,
				LeaseSec:     opts.LeaseSec,
				MetadataJSON: meta,
			})
			hbCancel()
			if hbErr != nil {
				heartbeatFailures++
				log.Printf("youtube-relay source heartbeat error consecutive=%d: %v", heartbeatFailures, hbErr)
			} else {
				heartbeatFailures = 0
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	refreshTicker := time.NewTicker(time.Duration(opts.RefreshSec) * time.Second)
	defer refreshTicker.Stop()
	lastUpstreamURL := map[int64]string{}
	lastPublishedPullURL := map[int64]string{}
	lastStatus := map[int64]string{}
	nextResolveAt := map[int64]time.Time{}
	routeResolveFailures := map[int64]int{}
	routeCache, err := loadRelaySourceRouteCache(strings.TrimSpace(opts.CacheFile))
	if err != nil {
		log.Printf("youtube-relay source cache load failed path=%s: %v", strings.TrimSpace(opts.CacheFile), err)
		routeCache = map[int64]relaySourceRouteCacheEntry{}
	}
	saveRouteCache := func() {
		if strings.TrimSpace(opts.CacheFile) == "" {
			return
		}
		if err := saveRelaySourceRouteCache(strings.TrimSpace(opts.CacheFile), routeCache); err != nil {
			log.Printf("youtube-relay source cache save failed path=%s: %v", strings.TrimSpace(opts.CacheFile), err)
		}
	}
	for streamID, cached := range routeCache {
		cachedURL := strings.TrimSpace(cached.UpstreamURL)
		if cachedURL == "" {
			continue
		}
		relayState.routes[streamID] = relayRouteState{UpstreamURL: cachedURL}
		lastUpstreamURL[streamID] = cachedURL
		lastPublishedPullURL[streamID] = fmt.Sprintf("%s/relay/%d?token=%s", relayBaseURL, streamID, url.QueryEscape(strings.TrimSpace(opts.SharedToken)))
		lastStatus[streamID] = "source_ready"
		nextResolveAt[streamID] = cached.UpdatedAt.Add(defaultRelayRouteRefreshDelay)
		log.Printf("youtube-relay source preloaded cached route stream_id=%d age=%s", streamID, time.Since(cached.UpdatedAt).Round(time.Second))
	}

	resolveOnce := func() error {
		routes, err := api.ListYouTubeRelayRoutes(ctx, strings.TrimSpace(opts.ServerID), "", "", 1000, 0)
		if err != nil {
			return fmt.Errorf("list youtube relay routes: %w", err)
		}
		activeRouteIDs := map[int64]struct{}{}
		activeRoutes := make([]captureapi.YouTubeRelayRoute, 0, len(routes))
		routesToResolve := make([]captureapi.YouTubeRelayRoute, 0, len(routes))
		for _, route := range routes {
			status := strings.TrimSpace(strings.ToLower(route.Status))
			if status != "assigned" && status != "source_ready" && status != "running" && status != "failed" && status != "stopped" {
				continue
			}
			activeRouteIDs[route.StreamID] = struct{}{}
			activeRoutes = append(activeRoutes, route)
		}
		for _, route := range activeRoutes {
			relayPullURL := fmt.Sprintf("%s/relay/%d?token=%s", relayBaseURL, route.StreamID, url.QueryEscape(strings.TrimSpace(opts.SharedToken)))
			status := strings.TrimSpace(strings.ToLower(route.Status))
			if lastUpstreamURL[route.StreamID] == "" {
				if cached, ok := routeCache[route.StreamID]; ok && strings.TrimSpace(cached.UpstreamURL) != "" {
					cachedURL := strings.TrimSpace(cached.UpstreamURL)
					relayState.mu.Lock()
					relayState.routes[route.StreamID] = relayRouteState{UpstreamURL: cachedURL}
					relayState.mu.Unlock()
					lastUpstreamURL[route.StreamID] = cachedURL
					lastPublishedPullURL[route.StreamID] = relayPullURL
					if lastStatus[route.StreamID] == "" {
						lastStatus[route.StreamID] = "source_ready"
					}
					if nextResolveAt[route.StreamID].IsZero() {
						nextResolveAt[route.StreamID] = cached.UpdatedAt.Add(defaultRelayRouteRefreshDelay)
					}
					log.Printf("youtube-relay source restored cached route stream_id=%d age=%s", route.StreamID, time.Since(cached.UpdatedAt).Round(time.Second))
					_ = api.UpdateYouTubeRelayRouteStatus(ctx, captureapi.YouTubeRelayRouteStatusRequest{
						StreamID:     route.StreamID,
						Actor:        "youtube_relay_source",
						Status:       "source_ready",
						Reason:       "restored_from_cache",
						RelayPullURL: relayPullURL,
						ErrorText:    "",
						MetadataJSON: map[string]any{
							"source_server_id":  strings.TrimSpace(opts.ServerID),
							"relay_public_url":  relayPullURL,
							"relay_bind_addr":   strings.TrimSpace(opts.BindAddr),
							"network_transport": strings.TrimSpace(opts.NetworkTransport),
							"topology_id":       strings.TrimSpace(opts.TopologyID),
							"topology_role":     strings.TrimSpace(opts.TopologyRole),
							"hub_server_id":     strings.TrimSpace(opts.HubServerID),
							"cache_restored":    true,
						},
					})
				}
			}
			relayState.mu.RLock()
			currentRoute, currentRouteOK := relayState.routes[route.StreamID]
			relayState.mu.RUnlock()
			if (status == "source_ready" || status == "running") &&
				currentRouteOK &&
				strings.TrimSpace(currentRoute.UpstreamURL) != "" &&
				lastPublishedPullURL[route.StreamID] == relayPullURL {
				if dueAt := nextResolveAt[route.StreamID]; !dueAt.IsZero() && time.Now().UTC().Before(dueAt) {
					continue
				}
			}
			routesToResolve = append(routesToResolve, route)
		}

		type resolveResult struct {
			route        captureapi.YouTubeRelayRoute
			relayPullURL string
			upstreamURL  string
			err          error
		}

		// Keep source-side yt-dlp resolution serialized. On the Mac relay source,
		// concurrent browser-cookie resolves can get killed under refresh load,
		// which leaves new routes stuck at assigned even though they resolve fine
		// individually.
		resolveWorkers := 1

		resultsCh := make(chan resolveResult, len(routesToResolve))
		var resolveWG sync.WaitGroup
		resolveSem := make(chan struct{}, resolveWorkers)
		for _, route := range routesToResolve {
			resolveWG.Add(1)
			go func(route captureapi.YouTubeRelayRoute) {
				defer resolveWG.Done()
				resolveSem <- struct{}{}
				defer func() { <-resolveSem }()

				spec := capture.StreamSpec{
					ID:            route.StreamID,
					Provider:      "youtube",
					StreamURL:     strings.TrimSpace(route.StreamURL),
					SourcePageURL: strings.TrimSpace(route.SourcePageURL),
					CaptureMode:   capture.ModeYouTubeLive,
				}
				resolveCtx, resolveCancel := context.WithTimeout(ctx, time.Duration(opts.ResolveTimeoutSec)*time.Second)
				resolved, err := ytAdapter.Resolve(resolveCtx, spec)
				resolveCancel()

				upstreamURL := ""
				if err == nil {
					upstreamURL = strings.TrimSpace(resolved.URL)
				}
				resultsCh <- resolveResult{
					route:        route,
					relayPullURL: fmt.Sprintf("%s/relay/%d?token=%s", relayBaseURL, route.StreamID, url.QueryEscape(strings.TrimSpace(opts.SharedToken))),
					upstreamURL:  upstreamURL,
					err:          err,
				}
			}(route)
		}
		resolveWG.Wait()
		close(resultsCh)

		for result := range resultsCh {
			route := result.route
			status := strings.TrimSpace(strings.ToLower(route.Status))
			relayPullURL := result.relayPullURL
			if result.err != nil {
				routeResolveFailures[route.StreamID]++
				if lastUpstreamURL[route.StreamID] != "" &&
					lastPublishedPullURL[route.StreamID] == relayPullURL &&
					(lastStatus[route.StreamID] == "source_ready" || lastStatus[route.StreamID] == "running") {
					nextResolveAt[route.StreamID] = time.Now().UTC().Add(relayResolveRetryBackoff)
					log.Printf("youtube-relay source keeping previous route stream_id=%d after resolve failure consecutive=%d/%d: %v", route.StreamID, routeResolveFailures[route.StreamID], opts.ResolveFailureThreshold, result.err)
					continue
				}
				relayState.mu.Lock()
				delete(relayState.routes, route.StreamID)
				relayState.mu.Unlock()
				_ = api.UpdateYouTubeRelayRouteStatus(ctx, captureapi.YouTubeRelayRouteStatusRequest{
					StreamID:  route.StreamID,
					Actor:     "youtube_relay_source",
					Status:    "failed",
					Reason:    "resolve_failed",
					ErrorText: strings.TrimSpace(result.err.Error()),
					MetadataJSON: map[string]any{
						"source_server_id":  strings.TrimSpace(opts.ServerID),
						"relay_public_url":  relayPullURL,
						"network_transport": strings.TrimSpace(opts.NetworkTransport),
						"topology_id":       strings.TrimSpace(opts.TopologyID),
						"topology_role":     strings.TrimSpace(opts.TopologyRole),
						"hub_server_id":     strings.TrimSpace(opts.HubServerID),
					},
				})
				continue
			}
			upstreamURL := result.upstreamURL
			if upstreamURL == "" {
				continue
			}
			now := time.Now().UTC()
			routeResolveFailures[route.StreamID] = 0
			relayState.mu.RLock()
			currentRoute, currentRouteOK := relayState.routes[route.StreamID]
			relayState.mu.RUnlock()
			if status != "failed" &&
				lastUpstreamURL[route.StreamID] == upstreamURL &&
				lastPublishedPullURL[route.StreamID] == relayPullURL &&
				(lastStatus[route.StreamID] == "source_ready" || lastStatus[route.StreamID] == "running") &&
				currentRouteOK &&
				strings.TrimSpace(currentRoute.UpstreamURL) == upstreamURL {
				continue
			}
			relayState.mu.Lock()
			relayState.routes[route.StreamID] = relayRouteState{UpstreamURL: upstreamURL}
			relayState.mu.Unlock()
			if err := api.UpdateYouTubeRelayRouteStatus(ctx, captureapi.YouTubeRelayRouteStatusRequest{
				StreamID:     route.StreamID,
				Actor:        "youtube_relay_source",
				Status:       "source_ready",
				Reason:       "resolved_and_relaying",
				RelayPullURL: relayPullURL,
				ErrorText:    "",
				MetadataJSON: map[string]any{
					"source_server_id":  strings.TrimSpace(opts.ServerID),
					"relay_public_url":  relayPullURL,
					"relay_bind_addr":   strings.TrimSpace(opts.BindAddr),
					"network_transport": strings.TrimSpace(opts.NetworkTransport),
					"topology_id":       strings.TrimSpace(opts.TopologyID),
					"topology_role":     strings.TrimSpace(opts.TopologyRole),
					"hub_server_id":     strings.TrimSpace(opts.HubServerID),
				},
			}); err != nil {
				return fmt.Errorf("update youtube relay route status stream_id=%d: %w", route.StreamID, err)
			}
			lastUpstreamURL[route.StreamID] = upstreamURL
			lastPublishedPullURL[route.StreamID] = relayPullURL
			lastStatus[route.StreamID] = "source_ready"
			refreshAfter := defaultRelayRouteRefreshDelay
			nextResolveAt[route.StreamID] = now.Add(refreshAfter)
			routeCache[route.StreamID] = relaySourceRouteCacheEntry{UpstreamURL: upstreamURL, UpdatedAt: now}
			saveRouteCache()
		}

		relayState.mu.Lock()
		cacheChanged := false
		for streamID := range relayState.routes {
			if _, ok := activeRouteIDs[streamID]; ok {
				continue
			}
			delete(relayState.routes, streamID)
			delete(lastUpstreamURL, streamID)
			delete(lastPublishedPullURL, streamID)
			delete(lastStatus, streamID)
			delete(nextResolveAt, streamID)
			delete(routeResolveFailures, streamID)
			if _, ok := routeCache[streamID]; ok {
				delete(routeCache, streamID)
				cacheChanged = true
			}
		}
		relayState.mu.Unlock()
		if cacheChanged {
			saveRouteCache()
		}
		return nil
	}

	resolveFailures := 0
	if err := resolveOnce(); err != nil {
		resolveFailures++
		log.Printf("youtube-relay source resolve error consecutive=%d/%d: %v", resolveFailures, opts.ResolveFailureThreshold, err)
	}
	for {
		select {
		case <-ctx.Done():
			hbWG.Wait()
			return nil
		case err := <-errCh:
			hbWG.Wait()
			return err
		case <-refreshTicker.C:
			if err := resolveOnce(); err != nil {
				resolveFailures++
				log.Printf("youtube-relay source resolve error consecutive=%d/%d: %v", resolveFailures, opts.ResolveFailureThreshold, err)
				continue
			}
			resolveFailures = 0
		}
	}
}

const relayPlaylistTailSegments = 12

func rewriteRelayPlaylist(relayBaseURL string, streamID int64, token string, upstreamPlaylistURL string, body []byte) ([]byte, bool) {
	trimmedBody := bytes.TrimSpace(body)
	if !bytes.HasPrefix(trimmedBody, []byte("#EXTM3U")) {
		return nil, false
	}
	baseURL, err := url.Parse(strings.TrimSpace(upstreamPlaylistURL))
	if err != nil {
		return nil, false
	}
	lines := strings.Split(strings.ReplaceAll(string(trimmedBody), "\r\n", "\n"), "\n")
	header := []string{}
	blocks := make([][]string, 0, 32)
	current := []string{}
	mediaSequence := -1
	for _, rawLine := range lines {
		line := strings.TrimSpace(rawLine)
		if line == "" || strings.EqualFold(line, "#EXT-X-ENDLIST") {
			continue
		}
		if strings.HasPrefix(line, "#EXT-X-MEDIA-SEQUENCE:") {
			if v, parseErr := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "#EXT-X-MEDIA-SEQUENCE:"))); parseErr == nil {
				mediaSequence = v
			}
			continue
		}
		if !strings.HasPrefix(line, "#") {
			segmentURL := line
			if ref, parseErr := url.Parse(line); parseErr == nil {
				segmentURL = baseURL.ResolveReference(ref).String()
			}
			current = append(current, segmentURL)
			blocks = append(blocks, append([]string(nil), current...))
			current = current[:0]
			continue
		}
		if len(blocks) == 0 && len(current) == 0 && isGlobalPlaylistTag(line) {
			header = append(header, line)
			continue
		}
		current = append(current, line)
	}
	if len(blocks) == 0 {
		return nil, false
	}
	dropped := 0
	if len(blocks) > relayPlaylistTailSegments {
		dropped = len(blocks) - relayPlaylistTailSegments
		blocks = blocks[dropped:]
	}
	out := make([]string, 0, len(header)+1+(len(blocks)*4))
	if len(header) == 0 || header[0] != "#EXTM3U" {
		out = append(out, "#EXTM3U")
	}
	for _, line := range header {
		if line == "#EXTM3U" {
			if len(out) == 0 || out[0] != "#EXTM3U" {
				out = append(out, line)
			}
			continue
		}
		out = append(out, line)
	}
	if mediaSequence >= 0 {
		out = append(out, fmt.Sprintf("#EXT-X-MEDIA-SEQUENCE:%d", mediaSequence+dropped))
	}
	for _, block := range blocks {
		out = append(out, block...)
	}
	return []byte(strings.Join(out, "\n") + "\n"), true
}

func isGlobalPlaylistTag(line string) bool {
	switch {
	case line == "#EXTM3U":
		return true
	case strings.HasPrefix(line, "#EXT-X-VERSION:"):
		return true
	case strings.HasPrefix(line, "#EXT-X-TARGETDURATION:"):
		return true
	case strings.HasPrefix(line, "#EXT-X-DISCONTINUITY-SEQUENCE:"):
		return true
	case strings.HasPrefix(line, "#EXT-X-PLAYLIST-TYPE:"):
		return true
	case strings.HasPrefix(line, "#EXT-X-INDEPENDENT-SEGMENTS"):
		return true
	case strings.HasPrefix(line, "#EXT-X-START:"):
		return true
	case strings.HasPrefix(line, "#EXT-X-ALLOW-CACHE:"):
		return true
	default:
		return false
	}
}

func loadRelaySourceRouteCache(path string) (map[int64]relaySourceRouteCacheEntry, error) {
	if strings.TrimSpace(path) == "" {
		return map[int64]relaySourceRouteCacheEntry{}, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return map[int64]relaySourceRouteCacheEntry{}, nil
		}
		return nil, err
	}
	cache := map[int64]relaySourceRouteCacheEntry{}
	if len(bytes.TrimSpace(data)) == 0 {
		return cache, nil
	}
	if err := json.Unmarshal(data, &cache); err != nil {
		return nil, err
	}
	return cache, nil
}

func saveRelaySourceRouteCache(path string, cache map[int64]relaySourceRouteCacheEntry) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cache, "", "  ")
	if err != nil {
		return err
	}
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, append(data, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func relayRouteFailureReason(statusCode int, errText string) string {
	switch statusCode {
	case http.StatusNotFound:
		return "relay_404"
	case http.StatusUnauthorized, http.StatusForbidden:
		return "relay_auth_failed"
	}
	lower := strings.ToLower(strings.TrimSpace(errText))
	switch {
	case strings.Contains(lower, "404"):
		return "relay_404"
	case strings.Contains(lower, "401"), strings.Contains(lower, "403"), strings.Contains(lower, "unauthorized"), strings.Contains(lower, "forbidden"):
		return "relay_auth_failed"
	case strings.Contains(lower, "connection refused"), strings.Contains(lower, "no route to host"), strings.Contains(lower, "dial tcp"), strings.Contains(lower, "i/o timeout"):
		return "relay_source_unreachable"
	default:
		return "relay_preflight_failed"
	}
}

func configureYouTubeCaptureEnv(
	cookiesFile string,
	cookiesBrowser string,
	ytDlpBin string,
	ytDlpFormat string,
	ytDlpFormatSort string,
	ffmpegJpegQuality int,
	ffmpegThreads int,
	ffmpegHWAccel string,
	ffmpegReconnect bool,
	ffmpegReconnectDelayMaxSec int,
) {
	if cookiesFile == "" && cookiesBrowser == "" {
		log.Fatalf("set yt-dlp cookies file or cookies-from-browser")
	}
	if cookiesFile != "" {
		st, err := os.Stat(cookiesFile)
		if err != nil {
			log.Fatalf("yt-dlp cookies file: %v", err)
		}
		if st.IsDir() {
			log.Fatalf("yt-dlp cookies file must be a file, got directory: %s", cookiesFile)
		}
		_ = os.Setenv("YT_DLP_COOKIES_FILE", cookiesFile)
	} else {
		_ = os.Unsetenv("YT_DLP_COOKIES_FILE")
	}
	if cookiesBrowser != "" {
		_ = os.Setenv("YT_DLP_COOKIES_FROM_BROWSER", cookiesBrowser)
	} else {
		_ = os.Unsetenv("YT_DLP_COOKIES_FROM_BROWSER")
	}
	if ytDlpBin != "" {
		_ = os.Setenv("YT_DLP_BIN", ytDlpBin)
	}
	if ytDlpFormat != "" {
		_ = os.Setenv("YT_DLP_FORMAT", ytDlpFormat)
	}
	if ytDlpFormatSort != "" {
		_ = os.Setenv("YT_DLP_FORMAT_SORT", ytDlpFormatSort)
	}
	if ffmpegJpegQuality <= 0 {
		ffmpegJpegQuality = 2
	}
	if ffmpegThreads <= 0 {
		ffmpegThreads = 1
	}
	if ffmpegReconnectDelayMaxSec <= 0 {
		ffmpegReconnectDelayMaxSec = 2
	}
	_ = os.Setenv("CAPTURE_FFMPEG_JPEG_Q", strconv.Itoa(ffmpegJpegQuality))
	_ = os.Setenv("CAPTURE_FFMPEG_THREADS", strconv.Itoa(ffmpegThreads))
	if strings.TrimSpace(ffmpegHWAccel) != "" {
		_ = os.Setenv("CAPTURE_FFMPEG_HWACCEL", strings.TrimSpace(ffmpegHWAccel))
	}
	if ffmpegReconnect {
		_ = os.Setenv("CAPTURE_FFMPEG_RECONNECT", "true")
	} else {
		_ = os.Setenv("CAPTURE_FFMPEG_RECONNECT", "false")
	}
	_ = os.Setenv("CAPTURE_FFMPEG_RECONNECT_DELAY_MAX_SEC", strconv.Itoa(ffmpegReconnectDelayMaxSec))
}

func nonNilMap(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return m
}

func cloneMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
