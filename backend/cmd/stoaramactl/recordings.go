package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/recordingnaming"
)

const recordingsUsage = "usage: stoaramactl recordings naming allocate|get|set|preview | schedule-batch | campaign-postflight | capture-health | repair-source | scene-attest | qualification build|freeze|report | streak-priority report"

func runRecordings(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 {
		log.Fatal(recordingsUsage)
	}
	if args[0] == "schedule-batch" {
		runRecordingScheduleBatch(ctx, cfg, args[1:])
		return
	}
	if args[0] == "campaign-postflight" {
		runRecordingCampaignPostflight(ctx, cfg, args[1:])
		return
	}
	if args[0] == "capture-health" {
		runRecordingCaptureHealth(ctx, cfg, args[1:])
		return
	}
	if args[0] == "repair-source" {
		runRecordingSourceRepair(ctx, cfg, args[1:])
		return
	}
	if args[0] == "scene-attest" {
		runRecordingSceneAttest(ctx, args[1:])
		return
	}
	if args[0] == "qualification" {
		runRecordingQualification(ctx, cfg, args[1:])
		return
	}
	if args[0] == "streak-priority" {
		runRecordingStreakPriority(ctx, cfg, args[1:])
		return
	}
	if len(args) < 2 || args[0] != "naming" {
		log.Fatal(recordingsUsage)
	}
	switch args[1] {
	case "allocate":
		runRecordingNamingAllocate(ctx, cfg, args[2:])
	case "preview":
		runRecordingNamingPreview(args[2:])
	case "get":
		runRecordingNamingGet(ctx, cfg, args[2:])
	case "set":
		runRecordingNamingSet(ctx, cfg, args[2:])
	default:
		log.Fatalf("unknown recordings naming subcommand: %s", args[1])
	}
}

func runRecordingStreakPriority(ctx context.Context, cfg config.Config, args []string) {
	if len(args) != 1 || args[0] != "report" {
		log.Fatal("streak-priority requires report")
	}
	fs := flag.NewFlagSet("recordings streak-priority report", flag.ExitOnError)
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	apiToken := fs.String("api-token", cfg.APIToken, "account API token")
	_ = fs.Parse(args[1:])
	printJSON(mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), "/api/v1/account/recordings/streak-priority"))
}

func postRecordingSessionJSON(ctx context.Context, baseURL, cookie, path string, payload any) map[string]any {
	body, err := json.Marshal(payload)
	if err != nil {
		log.Fatalf("encode request: %v", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+path, strings.NewReader(string(body)))
	if err != nil {
		log.Fatalf("create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Cookie", cookie)
	resp, err := (&http.Client{Timeout: 90 * time.Second}).Do(req)
	if err != nil {
		log.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		log.Fatalf("read response: %v", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Fatalf("request failed status=%d body=%q", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	if len(raw) == 0 {
		return map[string]any{}
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		log.Fatalf("decode response status=%d: %v", resp.StatusCode, err)
	}
	return out
}

func runRecordingSceneAttest(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("recordings scene-attest", flag.ExitOnError)
	recordingID := fs.Int64("recording-id", 0, "recording id")
	frameID := fs.Int64("frame-id", 0, "authoritative successful frame id")
	identity := fs.String("scene-identity", "", "operator-confirmed canonical scene identity")
	cookieFile := fs.String("session-cookie-file", "", "file containing member session cookie")
	base := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	_ = fs.Parse(args)
	cookie, err := readCampaignSessionCookie(*cookieFile)
	if err != nil {
		log.Fatal(err)
	}
	if *recordingID <= 0 || *frameID <= 0 || strings.TrimSpace(*identity) == "" {
		log.Fatal("--recording-id, --frame-id, and --scene-identity are required")
	}
	printJSON(postRecordingSessionJSON(ctx, *base, cookie, "/api/v1/account/recordings/qualification/scene-attest", map[string]any{"recording_id": *recordingID, "frame_id": *frameID, "scene_identity": strings.TrimSpace(*identity)}))
}

func parseQualificationIDs(raw string) []int64 {
	var ids []int64
	for _, part := range strings.Split(raw, ",") {
		id, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
		if err != nil || id <= 0 {
			log.Fatalf("invalid recording id %q", part)
		}
		ids = append(ids, id)
	}
	return ids
}

func runRecordingQualification(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 {
		log.Fatal("qualification requires build, freeze, or report")
	}
	if args[0] == "report" {
		runRecordingQualificationReport(ctx, cfg, args[1:])
		return
	}
	if args[0] != "build" && args[0] != "freeze" {
		log.Fatal("qualification requires build, freeze, or report")
	}
	fs := flag.NewFlagSet("recordings qualification "+args[0], flag.ExitOnError)
	idsRaw := fs.String("recording-ids", "", "comma-separated explicit recording ids")
	startRaw := fs.String("sequence-start", "", "RFC3339 earliest full-window instant")
	expected := fs.String("expected-plan-sha256", "", "required exact dry-run hash for freeze")
	cookieFile := fs.String("session-cookie-file", "", "file containing member session cookie")
	base := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	_ = fs.Parse(args[1:])
	start, err := time.Parse(time.RFC3339, strings.TrimSpace(*startRaw))
	if err != nil {
		log.Fatal("--sequence-start must be RFC3339")
	}
	cookie, err := readCampaignSessionCookie(*cookieFile)
	if err != nil {
		log.Fatal(err)
	}
	apply := args[0] == "freeze"
	if apply && len(strings.TrimSpace(*expected)) != 64 {
		log.Fatal("freeze requires --expected-plan-sha256 from the immediately preceding build")
	}
	printJSON(postRecordingSessionJSON(ctx, *base, cookie, "/api/v1/account/recordings/qualification/build", map[string]any{"recording_ids": parseQualificationIDs(*idsRaw), "sequence_start_at": start.UTC(), "apply": apply, "expected_plan_sha256": strings.ToLower(strings.TrimSpace(*expected))}))
}

func runRecordingQualificationReport(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("recordings qualification report", flag.ExitOnError)
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	apiToken := fs.String("api-token", cfg.APIToken, "account API token")
	_ = fs.Parse(args)
	payload := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), "/api/v1/account/recordings/qualification")
	printJSON(payload)
}

func runRecordingSourceRepair(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("recordings repair-source", flag.ExitOnError)
	id := fs.Int64("id", 0, "recording id")
	accountID := fs.Int64("account-id", 0, "expected account id")
	streamID := fs.Int64("stream-id", 0, "expected catalog stream id")
	jobID := fs.Int64("job-id", 0, "expected pending job id")
	expectedHash := fs.String("expected-source-sha256", "", "SHA-256 of expected current source URL")
	replacement := fs.String("replacement-source-url", "", "validated replacement source URL")
	reason := fs.String("reason", "", "audited repair reason")
	apply := fs.Bool("apply", false, "perform the fenced repair")
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	apiToken := fs.String("api-token", cfg.APIToken, "admin API token")
	_ = fs.Parse(args)
	if *id <= 0 || *accountID <= 0 || *streamID <= 0 || *jobID <= 0 || len(strings.TrimSpace(*expectedHash)) != 64 || strings.TrimSpace(*replacement) == "" || strings.TrimSpace(*reason) == "" {
		log.Fatal("all repair-source identifiers, hash, replacement URL, and reason are required")
	}
	if !*apply {
		log.Fatal("repair-source is mutation-only; pass --apply after verifying every fence")
	}
	payload := mustAPIRequest(ctx, "POST", strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), fmt.Sprintf("/api/v1/recordings/%d/repair-source", *id), map[string]any{
		"account_id": *accountID, "stream_id": *streamID, "job_id": *jobID,
		"expected_current_source_sha256": strings.ToLower(strings.TrimSpace(*expectedHash)),
		"replacement_source_url":         strings.TrimSpace(*replacement), "reason": strings.TrimSpace(*reason),
	})
	printJSON(payload)
}

func runRecordingCaptureHealth(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("recordings capture-health", flag.ExitOnError)
	id := fs.Int64("id", 0, "recording id")
	from := fs.String("from", "", "first local date, YYYY-MM-DD")
	to := fs.String("to", "", "last local date, YYYY-MM-DD")
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	apiToken := fs.String("api-token", cfg.APIToken, "account API token")
	_ = fs.Parse(args)
	if *id <= 0 {
		log.Fatal("--id is required")
	}
	if strings.TrimSpace(*backendAPIURL) == "" || strings.TrimSpace(*apiToken) == "" {
		log.Fatal("--backend-api-url and --api-token are required")
	}
	query := url.Values{}
	if strings.TrimSpace(*from) != "" {
		query.Set("from", strings.TrimSpace(*from))
	}
	if strings.TrimSpace(*to) != "" {
		query.Set("to", strings.TrimSpace(*to))
	}
	path := fmt.Sprintf("/api/v1/account/recordings/%d/capture-health", *id)
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}
	payload := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), path)
	encoded, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		log.Fatalf("encode capture health: %v", err)
	}
	fmt.Println(string(encoded))
}

func runRecordingNamingAllocate(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("recordings naming allocate", flag.ExitOnError)
	accountID := fs.Int64("account-id", 0, "organization account id")
	streamID := fs.Int64("stream-id", 0, "catalog stream id")
	_ = fs.Parse(args)
	if *accountID <= 0 || *streamID <= 0 {
		log.Fatalf("--account-id and --stream-id are required")
	}
	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	tx, err := pool.Begin(ctx)
	if err != nil {
		log.Fatalf("begin plaza id allocation: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := tx.QueryRow(ctx, `
		SELECT id
		FROM streams
		WHERE id=$1 AND deleted_at IS NULL
		FOR UPDATE
	`, *streamID).Scan(streamID); err != nil {
		log.Fatalf("lock catalog stream: %v", err)
	}
	plazaID, err := recordingnaming.EnsureStreamPlazaID(ctx, tx, *accountID, *streamID)
	if err != nil {
		log.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		log.Fatalf("commit plaza id allocation: %v", err)
	}
	fmt.Printf("account_id=%d stream_id=%d plaza_id=%s\n", *accountID, *streamID, plazaID)
}

type recordingScheduleMode string

const (
	recordingScheduleSampled    recordingScheduleMode = "sampled"
	recordingScheduleContinuous recordingScheduleMode = "continuous"
)

func (m *recordingScheduleMode) UnmarshalJSON(data []byte) error {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	switch recordingScheduleMode(raw) {
	case recordingScheduleSampled, recordingScheduleContinuous:
		*m = recordingScheduleMode(raw)
		return nil
	default:
		return fmt.Errorf("mode must be %q or %q", recordingScheduleSampled, recordingScheduleContinuous)
	}
}

type recordingDeliveryMode string

const (
	recordingDeliveryManaged recordingDeliveryMode = "managed"
	recordingDeliveryNASPull recordingDeliveryMode = "nas_pull"
)

func (m *recordingDeliveryMode) UnmarshalJSON(data []byte) error {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	switch recordingDeliveryMode(raw) {
	case recordingDeliveryManaged, recordingDeliveryNASPull:
		*m = recordingDeliveryMode(raw)
		return nil
	default:
		return fmt.Errorf("delivery must be %q or %q", recordingDeliveryManaged, recordingDeliveryNASPull)
	}
}

type recordingBatchTimezone struct {
	StreamID int64  `json:"stream_id"`
	Timezone string `json:"timezone"`
}

type recordingBatchSpec struct {
	TargetAccountID              int64                    `json:"target_account_id"`
	StreamIDs                    []int64                  `json:"stream_ids"`
	StreamTimezones              []recordingBatchTimezone `json:"stream_timezones"`
	NamingProfile                recordingnaming.Profile  `json:"naming_profile"`
	Mode                         recordingScheduleMode    `json:"mode"`
	CronExpr                     string                   `json:"cron_expr"`
	ClipDurationSec              int                      `json:"clip_duration_sec"`
	DailyWindowStart             string                   `json:"daily_window_start"`
	DailyWindowEnd               string                   `json:"daily_window_end"`
	ActiveWeekdays               []int                    `json:"active_weekdays"`
	TargetFPS                    *int                     `json:"target_fps"`
	StartAt                      *time.Time               `json:"start_at"`
	EndAt                        *time.Time               `json:"end_at"`
	StorageDestinationID         int64                    `json:"storage_destination_id"`
	DeliveryStorageDestinationID int64                    `json:"delivery_storage_destination_id"`
	Delivery                     recordingDeliveryMode    `json:"delivery"`
	DryRun                       bool                     `json:"dry_run"`
	RequiredRelaySlots           int                      `json:"required_relay_slots"`
}

type recordingBatchResult struct {
	Items []struct {
		StreamID    int64  `json:"stream_id"`
		RecordingID int64  `json:"recording_id"`
		Action      string `json:"action"`
		Timezone    string `json:"timezone"`
	} `json:"items"`
	Created            int  `json:"created"`
	Updated            int  `json:"updated"`
	DryRun             bool `json:"dry_run"`
	RelayStreams       int  `json:"relay_streams"`
	OnlineRelaySlots   int  `json:"online_relay_slots"`
	RequiredRelaySlots int  `json:"required_relay_slots"`
}

func decodeRecordingBatchSpec(r io.Reader) (recordingBatchSpec, error) {
	var spec recordingBatchSpec
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&spec); err != nil {
		return spec, err
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return spec, fmt.Errorf("spec must contain one JSON object")
		}
		return spec, err
	}
	if len(spec.StreamIDs) == 0 || len(spec.StreamIDs) > 200 {
		return spec, fmt.Errorf("stream_ids must contain 1 to 200 ids")
	}
	if spec.TargetAccountID < 0 {
		return spec, fmt.Errorf("target_account_id must be non-negative")
	}
	if spec.Mode != recordingScheduleSampled && spec.Mode != recordingScheduleContinuous {
		return spec, fmt.Errorf("mode is required")
	}
	namingProfile, err := recordingnaming.ParseProfile(spec.NamingProfile.String())
	if err != nil {
		return spec, err
	}
	spec.NamingProfile = namingProfile
	if spec.Delivery != recordingDeliveryManaged && spec.Delivery != recordingDeliveryNASPull {
		return spec, fmt.Errorf("delivery is required")
	}
	if (spec.StorageDestinationID > 0) == (spec.DeliveryStorageDestinationID > 0) {
		return spec, fmt.Errorf("exactly one storage destination is required")
	}
	if spec.Delivery == recordingDeliveryNASPull && spec.DeliveryStorageDestinationID > 0 {
		return spec, fmt.Errorf("nas_pull cannot use delivery_storage_destination_id")
	}
	if spec.RequiredRelaySlots < 0 {
		return spec, fmt.Errorf("required_relay_slots cannot be negative")
	}
	selected := make(map[int64]struct{}, len(spec.StreamIDs))
	for _, id := range spec.StreamIDs {
		if id <= 0 {
			return spec, fmt.Errorf("stream_ids must be positive")
		}
		if _, exists := selected[id]; exists {
			return spec, fmt.Errorf("duplicate stream_id %d", id)
		}
		selected[id] = struct{}{}
	}
	zoned := make(map[int64]struct{}, len(spec.StreamTimezones))
	for _, item := range spec.StreamTimezones {
		if _, exists := selected[item.StreamID]; !exists {
			return spec, fmt.Errorf("timezone stream_id %d is not selected", item.StreamID)
		}
		if _, exists := zoned[item.StreamID]; exists {
			return spec, fmt.Errorf("duplicate timezone stream_id %d", item.StreamID)
		}
		if _, err := time.LoadLocation(item.Timezone); err != nil {
			return spec, fmt.Errorf("invalid timezone for stream_id %d: %w", item.StreamID, err)
		}
		zoned[item.StreamID] = struct{}{}
	}
	weekdays := make(map[int]struct{}, len(spec.ActiveWeekdays))
	for _, day := range spec.ActiveWeekdays {
		if day < 1 || day > 7 {
			return spec, fmt.Errorf("active_weekdays must use ISO days 1 through 7")
		}
		if _, exists := weekdays[day]; exists {
			return spec, fmt.Errorf("duplicate active weekday %d", day)
		}
		weekdays[day] = struct{}{}
	}
	return spec, nil
}

func runRecordingScheduleBatch(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("recordings schedule-batch", flag.ExitOnError)
	specPath := fs.String("spec", "", "strict JSON batch schedule spec")
	dryRun := optionalBoolFlag(fs, "dry-run")
	jsonOutput := fs.Bool("json", false, "print the complete JSON response for campaign postflight")
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	apiToken := fs.String("api-token", cfg.APIToken, "account API token")
	_ = fs.Parse(args)
	if strings.TrimSpace(*specPath) == "" {
		log.Fatal("--spec is required")
	}
	if strings.TrimSpace(*backendAPIURL) == "" {
		log.Fatal("--backend-api-url is required")
	}
	if strings.TrimSpace(*apiToken) == "" {
		log.Fatal("--api-token is required")
	}
	f, err := os.Open(*specPath)
	if err != nil {
		log.Fatalf("open --spec: %v", err)
	}
	defer f.Close()
	spec, err := decodeRecordingBatchSpec(f)
	if err != nil {
		log.Fatalf("decode --spec: %v", err)
	}
	if dryRun.set {
		spec.DryRun = dryRun.value
	}
	var result recordingBatchResult
	if err := postJSONWithToken(ctx, *backendAPIURL, *apiToken, "/api/v1/account/recordings/batch-schedule", spec, &result); err != nil {
		log.Fatalf("schedule recordings: %v", err)
	}
	if *jsonOutput {
		out, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			log.Fatalf("encode schedule response: %v", err)
		}
		fmt.Println(string(out))
		return
	}
	fmt.Printf("dry_run=%t created=%d updated=%d relay_streams=%d online_relay_slots=%d required_relay_slots=%d\n",
		result.DryRun, result.Created, result.Updated, result.RelayStreams, result.OnlineRelaySlots, result.RequiredRelaySlots)
	for _, item := range result.Items {
		fmt.Printf("stream_id=%d recording_id=%d action=%s timezone=%s\n", item.StreamID, item.RecordingID, item.Action, item.Timezone)
	}
}

func runRecordingNamingPreview(args []string) {
	fs := flag.NewFlagSet("recordings naming preview", flag.ExitOnError)
	profileRaw := fs.String("profile", recordingnaming.ProfileStoaramaV1.String(), "stoarama_v1 or plaza_hourly_v1")
	folderNameRaw := fs.String("folder-name", "", "root folder name")
	recordingID := fs.Int64("recording-id", 1, "recording id")
	jobID := fs.Int64("job-id", 1, "recording job id")
	clipStartRaw := fs.String("clip-start", time.Now().UTC().Format(time.RFC3339), "clip start RFC3339")
	cronTimezone := fs.String("cron-timezone", "UTC", "IANA timezone")
	metadata := namingMetadataFlags(fs)
	_ = fs.Parse(args)

	profile := mustNamingProfile(*profileRaw)
	folderName, metadataBytes := mustBuildNaming(profile, *folderNameRaw, *recordingID, metadata)
	clipStart, err := time.Parse(time.RFC3339, strings.TrimSpace(*clipStartRaw))
	if err != nil {
		log.Fatalf("parse --clip-start: %v", err)
	}
	parsedMetadata, err := recordingnaming.ParseMetadata(metadataBytes)
	if err != nil {
		log.Fatalf("parse metadata: %v", err)
	}
	displayPath, err := recordingnaming.BuildDisplayPath(recordingnaming.Policy{
		Profile:       profile,
		FolderName:    folderName,
		Metadata:      parsedMetadata,
		RecordingID:   *recordingID,
		JobID:         *jobID,
		CronTimezone:  *cronTimezone,
		ClipStartedAt: clipStart,
	})
	if err != nil {
		log.Fatalf("build display path: %v", err)
	}
	fmt.Println(displayPath)
}

func runRecordingNamingGet(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("recordings naming get", flag.ExitOnError)
	id := fs.Int64("id", 0, "recording id")
	_ = fs.Parse(args)
	if *id <= 0 {
		log.Fatalf("--id is required")
	}
	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	var profile, folderName string
	var metadataBytes []byte
	if err := pool.QueryRow(ctx, `
		SELECT naming_profile, folder_name, naming_metadata_jsonb FROM recordings WHERE id=$1
	`, *id).Scan(&profile, &folderName, &metadataBytes); err != nil {
		log.Fatalf("load recording naming: %v", err)
	}
	var metadata recordingnaming.Metadata
	if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
		log.Fatalf("parse metadata: %v", err)
	}
	out, _ := json.MarshalIndent(map[string]any{
		"profile":     profile,
		"folder_name": folderName,
		"metadata":    metadata,
	}, "", "  ")
	fmt.Println(string(out))
}

func runRecordingNamingSet(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("recordings naming set", flag.ExitOnError)
	id := fs.Int64("id", 0, "recording id")
	profileRaw := fs.String("profile", "", "stoarama_v1 or plaza_hourly_v1")
	folderNameRaw := fs.String("folder-name", "", "root folder name")
	metadata := namingMetadataFlags(fs)
	_ = fs.Parse(args)
	if *id <= 0 {
		log.Fatalf("--id is required")
	}
	profile := mustNamingProfile(*profileRaw)
	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	tx, err := pool.Begin(ctx)
	if err != nil {
		log.Fatalf("begin naming update: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var accountID, streamID int64
	if err := tx.QueryRow(ctx, `SELECT account_id, COALESCE(stream_id, 0) FROM recordings WHERE id=$1`, *id).Scan(&accountID, &streamID); err != nil {
		log.Fatalf("load recording owner: %v", err)
	}
	if streamID > 0 {
		if err := tx.QueryRow(ctx, `SELECT id FROM streams WHERE id=$1 AND deleted_at IS NULL FOR UPDATE`, streamID).Scan(&streamID); err != nil {
			log.Fatalf("lock catalog stream: %v", err)
		}
	}
	var mode, cronExpr, dailyWindowStart, dailyWindowEnd string
	var clipDuration int
	var lockedAccountID, lockedStreamID int64
	if err := tx.QueryRow(ctx, `
		SELECT account_id, COALESCE(stream_id, 0), mode, COALESCE(cron_expr, ''), clip_duration_sec,
		       COALESCE(to_char(daily_window_start, 'HH24:MI:SS'), ''),
		       COALESCE(to_char(daily_window_end, 'HH24:MI:SS'), '')
		FROM recordings WHERE id=$1
		FOR UPDATE
	`, *id).Scan(&lockedAccountID, &lockedStreamID, &mode, &cronExpr, &clipDuration, &dailyWindowStart, &dailyWindowEnd); err != nil {
		log.Fatalf("load recording schedule: %v", err)
	}
	if lockedAccountID != accountID || lockedStreamID != streamID {
		log.Fatalf("recording owner or stream changed; retry")
	}
	if profile == recordingnaming.ProfilePlazaHourlyV1 {
		if streamID > 0 {
			plazaID, err := recordingnaming.EnsureStreamPlazaID(ctx, tx, accountID, streamID)
			if err != nil {
				log.Fatal(err)
			}
			metadata.PlazaID = plazaID
		} else if err := recordingnaming.ValidateManualPlazaID(ctx, tx, accountID, *id, metadata.PlazaID); err != nil {
			log.Fatal(err)
		}
	}
	folderName, metadataBytes := mustBuildNaming(profile, *folderNameRaw, *id, metadata)
	if err := recordingnaming.ValidateSchedule(profile, mode, cronExpr, clipDuration, dailyWindowStart, dailyWindowEnd); err != nil {
		log.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE recordings SET naming_profile=$2, folder_name=$3, naming_metadata_jsonb=$4, updated_at=now()
		WHERE id=$1
	`, *id, profile.String(), folderName, metadataBytes); err != nil {
		log.Fatalf("update recording naming: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		log.Fatalf("commit naming update: %v", err)
	}
	fmt.Printf("recording_id=%d naming_profile=%s folder_name=%s\n", *id, profile.String(), folderName)
}

func namingMetadataFlags(fs *flag.FlagSet) *recordingnaming.Metadata {
	out := &recordingnaming.Metadata{}
	fs.StringVar(&out.PlazaID, "plaza-id", "", "plaza id")
	fs.StringVar(&out.Continent, "continent", "", "continent")
	fs.StringVar(&out.Country, "country", "", "country")
	fs.StringVar(&out.City, "city", "", "city")
	fs.StringVar(&out.PlazaName, "plaza-name", "", "plaza name")
	return out
}

func mustBuildNaming(profile recordingnaming.Profile, folderNameRaw string, recordingID int64, metadata *recordingnaming.Metadata) (string, []byte) {
	folderName, err := recordingnaming.BuildFolderName(profile, recordingID, *metadata, folderNameRaw)
	if err != nil {
		log.Fatalf("build folder name: %v", err)
	}
	metadataBytes, err := recordingnaming.MarshalMetadata(*metadata)
	if err != nil {
		log.Fatalf("marshal metadata: %v", err)
	}
	return folderName, metadataBytes
}

func mustNamingProfile(raw string) recordingnaming.Profile {
	if strings.TrimSpace(raw) == "" {
		return recordingnaming.ProfileStoaramaV1
	}
	profile, err := recordingnaming.ParseProfile(raw)
	if err != nil {
		log.Fatalf("parse --profile: %v", err)
	}
	return profile
}
