package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/recordingnaming"
)

type campaignRecording struct {
	ID                int64                   `json:"id"`
	StreamID          *int64                  `json:"stream_id"`
	Status            string                  `json:"status"`
	CaptureVia        string                  `json:"capture_via"`
	RelayNodeName     *string                 `json:"relay_node_name"`
	HasRelayOnline    bool                    `json:"has_relay_online"`
	HasRelayAssigned  bool                    `json:"has_relay_assigned"`
	CaptureHealth     string                  `json:"capture_health"`
	CapturedClipCount int64                   `json:"captured_clip_count"`
	ExpectedClipCount int64                   `json:"expected_clip_count"`
	Delivery          recordingDeliveryMode   `json:"delivery"`
	Naming            campaignRecordingNaming `json:"naming"`
}

type campaignRecordingNaming struct {
	Profile recordingnaming.Profile `json:"profile"`
}

type campaignRecordingsResponse struct {
	Items                    []campaignRecording `json:"items"`
	FleetRelayOnline         int                 `json:"fleet_relay_online"`
	FleetRelayAvailableSlots int                 `json:"fleet_relay_available_slots"`
	FleetRelayWarning        bool                `json:"fleet_relay_warning"`
}

type campaignNASConnection struct {
	ID              int64      `json:"id"`
	Label           string     `json:"label"`
	Health          string     `json:"health"`
	ClientPhase     string     `json:"client_phase"`
	LastSeenAt      *time.Time `json:"last_seen_at"`
	LastSuccessAt   *time.Time `json:"client_last_success_at"`
	PendingClips    int64      `json:"pending_clips"`
	PendingBytes    int64      `json:"pending_bytes"`
	OldestPendingAt *time.Time `json:"oldest_pending_at"`
}

type campaignConnectionsResponse struct {
	Items []campaignNASConnection `json:"items"`
}

type campaignRecordingCheck struct {
	RecordingID   int64    `json:"recording_id"`
	StreamID      int64    `json:"stream_id"`
	Status        string   `json:"status"`
	CaptureVia    string   `json:"capture_via"`
	RelayNodeName *string  `json:"relay_node_name,omitempty"`
	RelayOnline   bool     `json:"relay_online"`
	RelayAssigned bool     `json:"relay_assigned"`
	CaptureHealth string   `json:"capture_health"`
	CapturedClips int64    `json:"captured_clips"`
	ExpectedClips int64    `json:"expected_clips"`
	Delivery      string   `json:"delivery"`
	NamingProfile string   `json:"naming_profile"`
	Healthy       bool     `json:"healthy"`
	Issues        []string `json:"issues,omitempty"`
}

type campaignNASCheck struct {
	ID              int64      `json:"id"`
	Label           string     `json:"label"`
	Health          string     `json:"health"`
	Phase           string     `json:"phase"`
	LastSeenAt      *time.Time `json:"last_seen_at,omitempty"`
	LastSuccessAt   *time.Time `json:"last_success_at,omitempty"`
	PendingClips    int64      `json:"pending_clips"`
	PendingBytes    int64      `json:"pending_bytes"`
	OldestPendingAt *time.Time `json:"oldest_pending_at,omitempty"`
	Healthy         bool       `json:"healthy"`
	Issues          []string   `json:"issues,omitempty"`
}

type campaignPostflightReport struct {
	Healthy                  bool                     `json:"healthy"`
	CheckedAt                time.Time                `json:"checked_at"`
	RequestedRecordingIDs    []int64                  `json:"requested_recording_ids"`
	RequestedStreamIDs       []int64                  `json:"requested_stream_ids"`
	MissingRecordingIDs      []int64                  `json:"missing_recording_ids,omitempty"`
	MissingStreamIDs         []int64                  `json:"missing_stream_ids,omitempty"`
	Recordings               []campaignRecordingCheck `json:"recordings"`
	NAS                      []campaignNASCheck       `json:"nas"`
	FleetRelayOnline         int                      `json:"fleet_relay_online"`
	FleetRelayAvailableSlots int                      `json:"fleet_relay_available_slots"`
	Issues                   []string                 `json:"issues,omitempty"`
}

func runRecordingCampaignPostflight(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("recordings campaign-postflight", flag.ExitOnError)
	recordingIDsRaw := fs.String("recording-ids", "", "comma-separated recording ids")
	batchResponsePath := fs.String("batch-response", "", "schedule-batch JSON response")
	specPath := fs.String("spec", "", "strict JSON batch schedule spec; resolves its streams to recordings")
	sessionCookiePath := fs.String("session-cookie-file", "", "file containing the stoarama_session cookie or complete Cookie header")
	maxNASPendingClips := fs.Int64("max-nas-pending-clips", 0, "largest acceptable NAS backlog")
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	apiToken := fs.String("api-token", cfg.APIToken, "account API token")
	_ = fs.Parse(args)
	if *maxNASPendingClips < 0 {
		log.Fatal("--max-nas-pending-clips cannot be negative")
	}
	if strings.TrimSpace(*backendAPIURL) == "" || strings.TrimSpace(*apiToken) == "" {
		log.Fatal("--backend-api-url and --api-token are required")
	}
	recordingIDs, streamIDs, err := campaignPostflightTargets(*recordingIDsRaw, *batchResponsePath, *specPath)
	if err != nil {
		log.Fatal(err)
	}
	cookie, err := readCampaignSessionCookie(*sessionCookiePath)
	if err != nil {
		log.Fatal(err)
	}
	var recordings campaignRecordingsResponse
	if err := getRecordingJSON(ctx, *backendAPIURL, *apiToken, "", "/api/v1/account/recordings", &recordings); err != nil {
		log.Fatalf("load recordings: %v", err)
	}
	var connections campaignConnectionsResponse
	if err := getRecordingJSON(ctx, *backendAPIURL, "", cookie, "/api/v1/account/connections", &connections); err != nil {
		log.Fatalf("load NAS connections: %v", err)
	}
	report := evaluateCampaignPostflight(time.Now().UTC(), recordingIDs, streamIDs, recordings, connections, *maxNASPendingClips)
	out, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		log.Fatalf("encode campaign postflight: %v", err)
	}
	fmt.Println(string(out))
	if !report.Healthy {
		os.Exit(1)
	}
}

func campaignPostflightTargets(recordingIDsRaw, batchResponsePath, specPath string) ([]int64, []int64, error) {
	set := 0
	if strings.TrimSpace(recordingIDsRaw) != "" {
		set++
	}
	if strings.TrimSpace(batchResponsePath) != "" {
		set++
	}
	if strings.TrimSpace(specPath) != "" {
		set++
	}
	if set != 1 {
		return nil, nil, fmt.Errorf("exactly one of --recording-ids, --batch-response, or --spec is required")
	}
	if strings.TrimSpace(recordingIDsRaw) != "" {
		ids, err := parseInt64CSV(recordingIDsRaw)
		if err != nil {
			return nil, nil, err
		}
		if err := validateCampaignIDs(ids); err != nil {
			return nil, nil, err
		}
		return ids, nil, nil
	}
	if strings.TrimSpace(batchResponsePath) != "" {
		var response recordingBatchResult
		if err := decodeStrictJSONFile(batchResponsePath, &response); err != nil {
			return nil, nil, fmt.Errorf("decode --batch-response: %w", err)
		}
		ids := make([]int64, 0, len(response.Items))
		for _, item := range response.Items {
			if item.RecordingID <= 0 {
				return nil, nil, fmt.Errorf("batch response has no persisted recording for stream %d", item.StreamID)
			}
			ids = append(ids, item.RecordingID)
		}
		if len(ids) == 0 {
			return nil, nil, fmt.Errorf("batch response contains no recordings")
		}
		if err := validateCampaignIDs(ids); err != nil {
			return nil, nil, err
		}
		return ids, nil, nil
	}
	f, err := os.Open(specPath)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	spec, err := decodeRecordingBatchSpec(f)
	if err != nil {
		return nil, nil, err
	}
	return nil, spec.StreamIDs, nil
}

func validateCampaignIDs(ids []int64) error {
	seen := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		if id <= 0 {
			return fmt.Errorf("campaign ids must be positive")
		}
		if _, exists := seen[id]; exists {
			return fmt.Errorf("duplicate campaign id %d", id)
		}
		seen[id] = struct{}{}
	}
	return nil
}

func decodeStrictJSONFile(path string, out any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("file must contain one JSON object")
		}
		return err
	}
	return nil
}

func readCampaignSessionCookie(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("--session-cookie-file is required for NAS readiness")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read --session-cookie-file: %w", err)
	}
	cookie := strings.TrimSpace(string(raw))
	if cookie == "" {
		return "", fmt.Errorf("--session-cookie-file is empty")
	}
	if !strings.Contains(cookie, "=") {
		cookie = "stoarama_session=" + cookie
	}
	return cookie, nil
}

func getRecordingJSON(ctx context.Context, baseURL, token, cookie, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(strings.TrimSpace(baseURL), "/")+path, nil)
	if err != nil {
		return err
	}
	if strings.TrimSpace(token) != "" {
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(token))
	}
	if strings.TrimSpace(cookie) != "" {
		req.Header.Set("Cookie", strings.TrimSpace(cookie))
	}
	req.Header.Set("Accept", "application/json")
	resp, err := (&http.Client{Timeout: 45 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("GET %s: status=%d body=%q", path, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func evaluateCampaignPostflight(
	now time.Time,
	recordingIDs, streamIDs []int64,
	recordings campaignRecordingsResponse,
	connections campaignConnectionsResponse,
	maxNASPendingClips int64,
) campaignPostflightReport {
	report := campaignPostflightReport{
		Healthy:                  true,
		CheckedAt:                now.UTC(),
		RequestedRecordingIDs:    recordingIDs,
		RequestedStreamIDs:       streamIDs,
		Recordings:               []campaignRecordingCheck{},
		NAS:                      []campaignNASCheck{},
		FleetRelayOnline:         recordings.FleetRelayOnline,
		FleetRelayAvailableSlots: recordings.FleetRelayAvailableSlots,
	}
	byRecording := make(map[int64]campaignRecording, len(recordings.Items))
	byStream := make(map[int64]campaignRecording, len(recordings.Items))
	for _, recording := range recordings.Items {
		byRecording[recording.ID] = recording
		if recording.StreamID != nil {
			if _, exists := byStream[*recording.StreamID]; !exists {
				byStream[*recording.StreamID] = recording
			}
		}
	}
	selected := make([]campaignRecording, 0, len(recordingIDs)+len(streamIDs))
	for _, id := range recordingIDs {
		recording, ok := byRecording[id]
		if !ok {
			report.MissingRecordingIDs = append(report.MissingRecordingIDs, id)
			continue
		}
		selected = append(selected, recording)
	}
	for _, id := range streamIDs {
		recording, ok := byStream[id]
		if !ok {
			report.MissingStreamIDs = append(report.MissingStreamIDs, id)
			continue
		}
		selected = append(selected, recording)
	}
	for _, recording := range selected {
		report.Recordings = append(report.Recordings, evaluateCampaignRecording(recording))
	}
	for _, connection := range connections.Items {
		report.NAS = append(report.NAS, evaluateCampaignNAS(connection, maxNASPendingClips))
	}
	if len(report.MissingRecordingIDs) > 0 || len(report.MissingStreamIDs) > 0 {
		report.Issues = append(report.Issues, "one or more requested recordings are missing")
	}
	if len(report.NAS) == 0 {
		report.Issues = append(report.Issues, "no NAS connection exists")
	} else {
		nasHealthy := slices.ContainsFunc(report.NAS, func(item campaignNASCheck) bool { return item.Healthy })
		if !nasHealthy {
			report.Issues = append(report.Issues, "no healthy NAS connection is ready")
		}
	}
	for _, recording := range report.Recordings {
		if !recording.Healthy {
			report.Healthy = false
		}
	}
	if recordings.FleetRelayWarning {
		report.Issues = append(report.Issues, "relay fleet warning is active")
	}
	if len(report.Issues) > 0 {
		report.Healthy = false
	}
	return report
}

func evaluateCampaignRecording(recording campaignRecording) campaignRecordingCheck {
	streamID := int64(0)
	if recording.StreamID != nil {
		streamID = *recording.StreamID
	}
	check := campaignRecordingCheck{
		RecordingID:   recording.ID,
		StreamID:      streamID,
		Status:        recording.Status,
		CaptureVia:    recording.CaptureVia,
		RelayNodeName: recording.RelayNodeName,
		RelayOnline:   recording.HasRelayOnline,
		RelayAssigned: recording.HasRelayAssigned,
		CaptureHealth: recording.CaptureHealth,
		CapturedClips: recording.CapturedClipCount,
		ExpectedClips: recording.ExpectedClipCount,
		Delivery:      string(recording.Delivery),
		NamingProfile: recording.Naming.Profile.String(),
		Healthy:       true,
	}
	if recording.Status != "active" {
		check.Issues = append(check.Issues, "recording is not active")
	}
	if recording.Delivery != recordingDeliveryNASPull {
		check.Issues = append(check.Issues, "recording is not configured for NAS pull")
	}
	if recording.Naming.Profile != recordingnaming.ProfilePlazaHourlyV1 {
		check.Issues = append(check.Issues, "recording does not use plaza_hourly_v1")
	}
	if recording.CaptureHealth != "healthy" && recording.CaptureHealth != "not_expected" {
		check.Issues = append(check.Issues, "capture health is "+recording.CaptureHealth)
	}
	if recording.CaptureVia == "relay" {
		if !recording.HasRelayOnline {
			check.Issues = append(check.Issues, "no relay is online")
		}
		if !recording.HasRelayAssigned {
			check.Issues = append(check.Issues, "no relay is assigned")
		}
	}
	check.Healthy = len(check.Issues) == 0
	return check
}

func evaluateCampaignNAS(connection campaignNASConnection, maxPendingClips int64) campaignNASCheck {
	check := campaignNASCheck{
		ID:              connection.ID,
		Label:           connection.Label,
		Health:          connection.Health,
		Phase:           connection.ClientPhase,
		LastSeenAt:      connection.LastSeenAt,
		LastSuccessAt:   connection.LastSuccessAt,
		PendingClips:    connection.PendingClips,
		PendingBytes:    connection.PendingBytes,
		OldestPendingAt: connection.OldestPendingAt,
		Healthy:         true,
	}
	if connection.Health != "healthy" {
		check.Issues = append(check.Issues, "connection health is "+connection.Health)
	}
	if connection.ClientPhase != "idle" && connection.ClientPhase != "draining" {
		check.Issues = append(check.Issues, "client phase is "+connection.ClientPhase)
	}
	if connection.LastSuccessAt == nil {
		check.Issues = append(check.Issues, "client has no successful batch")
	}
	if connection.PendingClips > maxPendingClips {
		check.Issues = append(check.Issues, fmt.Sprintf("pending clips %d exceed limit %d", connection.PendingClips, maxPendingClips))
	}
	check.Healthy = len(check.Issues) == 0
	return check
}
