package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/recordingnaming"
)

func runRecordings(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 2 || args[0] != "naming" {
		log.Fatalf("usage: stoaramactl recordings naming get|set|preview")
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
	var mode, cronExpr string
	if err := pool.QueryRow(ctx, `
		SELECT mode, COALESCE(cron_expr, '') FROM recordings WHERE id=$1
	`, *id).Scan(&mode, &cronExpr); err != nil {
		log.Fatalf("load recording schedule: %v", err)
	}
	if err := recordingnaming.ValidateSchedule(profile, mode, cronExpr); err != nil {
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
