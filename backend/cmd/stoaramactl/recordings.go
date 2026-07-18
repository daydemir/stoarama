package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/recordingnaming"
)

func runRecordings(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 {
		log.Fatalf("usage: stoaramactl recordings naming get|set|preview | schedule-batch --spec FILE")
	}
	if args[0] == "schedule-batch" {
		runRecordingScheduleBatch(ctx, cfg, args[1:])
		return
	}
	if len(args) < 2 || args[0] != "naming" {
		log.Fatalf("usage: stoaramactl recordings naming get|set|preview | schedule-batch --spec FILE")
	}
	switch args[1] {
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

type recordingBatchTimezone struct {
	StreamID int64  `json:"stream_id"`
	Timezone string `json:"timezone"`
}

type recordingBatchSpec struct {
	StreamIDs                    []int64                  `json:"stream_ids"`
	StreamTimezones              []recordingBatchTimezone `json:"stream_timezones"`
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
}

type recordingBatchResult struct {
	Items []struct {
		StreamID    int64  `json:"stream_id"`
		RecordingID int64  `json:"recording_id"`
		Action      string `json:"action"`
		Timezone    string `json:"timezone"`
	} `json:"items"`
	Created int `json:"created"`
	Updated int `json:"updated"`
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
	if len(spec.StreamIDs) == 0 || len(spec.StreamIDs) > 50 {
		return spec, fmt.Errorf("stream_ids must contain 1 to 50 ids")
	}
	if spec.Mode != recordingScheduleSampled && spec.Mode != recordingScheduleContinuous {
		return spec, fmt.Errorf("mode is required")
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
	var result recordingBatchResult
	if err := postJSONWithToken(ctx, *backendAPIURL, *apiToken, "/api/v1/account/recordings/batch-schedule", spec, &result); err != nil {
		log.Fatalf("schedule recordings: %v", err)
	}
	fmt.Printf("created=%d updated=%d\n", result.Created, result.Updated)
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

	profile, folderName, metadataBytes := mustNamingInputs(*profileRaw, *folderNameRaw, *recordingID, metadata)
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
	profile, folderName, metadataBytes := mustNamingInputs(*profileRaw, *folderNameRaw, *id, metadata)
	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	var mode, cronExpr, dailyWindowStart, dailyWindowEnd string
	var clipDuration int
	if err := pool.QueryRow(ctx, `
		SELECT mode, COALESCE(cron_expr, ''), clip_duration_sec,
		       COALESCE(to_char(daily_window_start, 'HH24:MI:SS'), ''),
		       COALESCE(to_char(daily_window_end, 'HH24:MI:SS'), '')
		FROM recordings WHERE id=$1
	`, *id).Scan(&mode, &cronExpr, &clipDuration, &dailyWindowStart, &dailyWindowEnd); err != nil {
		log.Fatalf("load recording schedule: %v", err)
	}
	if err := recordingnaming.ValidateSchedule(profile, mode, cronExpr, clipDuration, dailyWindowStart, dailyWindowEnd); err != nil {
		log.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE recordings SET naming_profile=$2, folder_name=$3, naming_metadata_jsonb=$4, updated_at=now()
		WHERE id=$1
	`, *id, profile.String(), folderName, metadataBytes); err != nil {
		log.Fatalf("update recording naming: %v", err)
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

func mustNamingInputs(profileRaw, folderNameRaw string, recordingID int64, metadata *recordingnaming.Metadata) (recordingnaming.Profile, string, []byte) {
	profile := recordingnaming.ProfileStoaramaV1
	if strings.TrimSpace(profileRaw) != "" {
		parsed, err := recordingnaming.ParseProfile(profileRaw)
		if err != nil {
			log.Fatalf("parse --profile: %v", err)
		}
		profile = parsed
	}
	folderName, err := recordingnaming.BuildFolderName(profile, recordingID, *metadata, folderNameRaw)
	if err != nil {
		log.Fatalf("build folder name: %v", err)
	}
	metadataBytes, err := recordingnaming.MarshalMetadata(*metadata)
	if err != nil {
		log.Fatalf("marshal metadata: %v", err)
	}
	return profile, folderName, metadataBytes
}
