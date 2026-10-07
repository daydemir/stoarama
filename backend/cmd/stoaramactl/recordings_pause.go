package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
)

type recordingPauseOptions struct {
	accountID      int64
	ids            []int64
	apply          bool
	youtubeOnly    bool
	baseURL, token string
}

type recordingPauseItem struct {
	ID         int64   `json:"id"`
	Status     string  `json:"status"`
	StreamURL  string  `json:"stream_url"`
	NextFireAt *string `json:"next_fire_at"`
}

type recordingPauseReport struct {
	AccountID              int64   `json:"account_id"`
	Apply                  bool    `json:"apply"`
	RecordingIDs           []int64 `json:"recording_ids"`
	PausedIDs              []int64 `json:"paused_ids"`
	AlreadyPausedIDs       []int64 `json:"already_paused_ids"`
	Verified               bool    `json:"verified"`
	PreservesExistingMedia bool    `json:"preserves_existing_media"`
}

func runRecordingPause(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("recordings pause", flag.ExitOnError)
	accountID := fs.Int64("account-id", 0, "required authenticated account id")
	idsRaw := fs.String("recording-ids", "", "required exact comma-separated recording ids (maximum 50)")
	apply := fs.Bool("apply", false, "pause the reviewed recordings; default is read-only")
	youtubeOnly := fs.Bool("youtube-only", false, "reject any selected recording whose binding is not a YouTube URL")
	baseURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	token := fs.String("api-token", "", "existing account API token (defaults to API_TOKEN)")
	_ = fs.Bool("json", true, "emit a JSON report")
	_ = fs.Parse(args)
	if len(fs.Args()) != 0 {
		log.Fatal("unexpected positional arguments")
	}
	if strings.TrimSpace(*token) == "" {
		*token = cfg.APIToken
	}
	ids, err := parseRecordingPauseIDs(*idsRaw)
	if err != nil {
		log.Fatal(err)
	}
	options := recordingPauseOptions{*accountID, ids, *apply, *youtubeOnly, *baseURL, *token}
	client := recordingPauseHTTPClient()
	report, err := executeRecordingPause(ctx, client, options)
	printJSON(report)
	if err != nil {
		log.Fatal(err)
	}
}

func recordingPauseHTTPClient() *http.Client {
	return &http.Client{Timeout: 45 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

func parseRecordingPauseIDs(raw string) ([]int64, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("--recording-ids is required")
	}
	seen := map[int64]bool{}
	var ids []int64
	for _, part := range strings.Split(raw, ",") {
		id, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
		if err != nil || id <= 0 || seen[id] {
			return nil, fmt.Errorf("recording ids must be distinct positive integers")
		}
		seen[id] = true
		ids = append(ids, id)
	}
	if len(ids) > 50 {
		return nil, fmt.Errorf("maximum 50 recording ids")
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

func recordingPauseRequest(ctx context.Context, client *http.Client, options recordingPauseOptions, method, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(options.baseURL, "/")+path, nil)
	if err != nil {
		return fmt.Errorf("build account API request")
	}
	req.Header.Set("Authorization", "Bearer "+options.token)
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: account API transport failed", method, path)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		kind := "non-JSON response"
		if strings.HasPrefix(strings.ToLower(resp.Header.Get("Content-Type")), "application/json") {
			kind = "JSON response"
		}
		return fmt.Errorf("%s %s returned HTTP %d (%s); stopped without retry", method, path, resp.StatusCode, kind)
	}
	if err := json.NewDecoder(http.MaxBytesReader(nil, resp.Body, 8<<20)).Decode(out); err != nil {
		return fmt.Errorf("%s %s: invalid account API JSON", method, path)
	}
	return nil
}

func isRecordingPauseYouTubeURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") {
		return false
	}
	host := strings.ToLower(u.Hostname())
	return host == "youtu.be" || host == "youtube.com" || strings.HasSuffix(host, ".youtube.com") || host == "youtube-nocookie.com" || strings.HasSuffix(host, ".youtube-nocookie.com")
}

func executeRecordingPause(ctx context.Context, client *http.Client, options recordingPauseOptions) (recordingPauseReport, error) {
	report := recordingPauseReport{AccountID: options.accountID, Apply: options.apply, RecordingIDs: options.ids, PausedIDs: []int64{}, AlreadyPausedIDs: []int64{}, PreservesExistingMedia: true}
	u, err := url.Parse(options.baseURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return report, fmt.Errorf("a plain HTTP(S) --backend-api-url is required")
	}
	if options.accountID <= 0 || len(options.ids) == 0 || len(options.ids) > 50 || strings.TrimSpace(options.token) == "" {
		return report, fmt.Errorf("--account-id, exact recording ids, and account API authentication are required")
	}
	seen := map[int64]bool{}
	for _, id := range options.ids {
		if id <= 0 || seen[id] {
			return report, fmt.Errorf("recording ids must be distinct positive integers")
		}
		seen[id] = true
	}
	var me struct {
		Account struct {
			ID int64 `json:"id"`
		} `json:"account"`
	}
	if err := recordingPauseRequest(ctx, client, options, http.MethodGet, "/api/v1/account/me", &me); err != nil {
		return report, err
	}
	if me.Account.ID != options.accountID {
		return report, fmt.Errorf("authenticated account %d does not match required account %d", me.Account.ID, options.accountID)
	}
	load := func() (map[int64]recordingPauseItem, error) {
		var list struct {
			Items []recordingPauseItem `json:"items"`
		}
		if err := recordingPauseRequest(ctx, client, options, http.MethodGet, "/api/v1/account/recordings", &list); err != nil {
			return nil, err
		}
		items := map[int64]recordingPauseItem{}
		for _, item := range list.Items {
			items[item.ID] = item
		}
		return items, nil
	}
	items, err := load()
	if err != nil {
		return report, err
	}
	// Validate every target before the first mutation. Foreign, missing, finished,
	// or unexpected-source recordings cannot produce a partial preflight apply.
	for _, id := range options.ids {
		item, ok := items[id]
		if !ok {
			return report, fmt.Errorf("recording %d is absent from authenticated account listing", id)
		}
		if item.Status != "active" && item.Status != "paused" {
			return report, fmt.Errorf("recording %d has status %s; expected active or paused", id, item.Status)
		}
		if options.youtubeOnly && !isRecordingPauseYouTubeURL(item.StreamURL) {
			return report, fmt.Errorf("recording %d is not bound to a YouTube URL", id)
		}
		if item.Status == "paused" {
			report.AlreadyPausedIDs = append(report.AlreadyPausedIDs, id)
		}
	}
	if !options.apply {
		return report, nil
	}
	for _, id := range options.ids {
		if items[id].Status == "paused" {
			continue
		}
		var result struct {
			ID     int64  `json:"id"`
			Status string `json:"status"`
		}
		path := fmt.Sprintf("/api/v1/account/recordings/%d/pause", id)
		if err := recordingPauseRequest(ctx, client, options, http.MethodPost, path, &result); err != nil {
			return report, err
		}
		if result.ID != id || result.Status != "paused" {
			return report, fmt.Errorf("unexpected pause response for recording %d", id)
		}
		report.PausedIDs = append(report.PausedIDs, id)
	}
	final, err := load()
	if err != nil {
		return report, err
	}
	for _, id := range options.ids {
		item, ok := final[id]
		if !ok || item.Status != "paused" || item.NextFireAt != nil {
			return report, fmt.Errorf("recording %d did not verify paused with no future admission", id)
		}
	}
	report.Verified = true
	return report, nil
}
