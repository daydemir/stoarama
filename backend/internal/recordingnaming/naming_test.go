package recordingnaming

import (
	"testing"
	"time"
)

func TestBuildPlazaHourlyPath(t *testing.T) {
	start := time.Date(2025, time.May, 12, 10, 0, 0, 0, time.UTC)
	folder, err := BuildFolderName(ProfilePlazaHourlyV1, 8, Metadata{
		PlazaID:   "8",
		Continent: "Europe",
		Country:   "Finland",
		City:      "Kuopio",
		PlazaName: "Market Square",
	}, "")
	if err != nil {
		t.Fatalf("folder: %v", err)
	}
	got, err := BuildDisplayPath(Policy{
		Profile:       ProfilePlazaHourlyV1,
		FolderName:    folder,
		RecordingID:   8,
		CronTimezone:  "UTC",
		ClipStartedAt: start,
		Metadata: Metadata{
			PlazaID:   "8",
			Continent: "Europe",
			Country:   "Finland",
			City:      "Kuopio",
			PlazaName: "Kuopio Market Square",
		},
	})
	if err != nil {
		t.Fatalf("path: %v", err)
	}
	want := "08_Europe_Finland_Kuopio_Market_Square/May/Monday/08_Kuopio_Market_Square_2025_May_W2_Monday_hour_03.mp4"
	if got != want {
		t.Fatalf("path=%q want %q", got, want)
	}
}

func TestBuildDisplayPathRejectsBadFolder(t *testing.T) {
	_, err := BuildDisplayPath(Policy{
		Profile:       ProfileStoaramaV1,
		FolderName:    "../x",
		RecordingID:   1,
		JobID:         2,
		ClipStartedAt: time.Unix(0, 0),
	})
	if err == nil {
		t.Fatal("expected traversal folder to fail")
	}
}

func TestBuildFolderNameValidatesPlazaMetadataWithCustomFolder(t *testing.T) {
	_, err := BuildFolderName(ProfilePlazaHourlyV1, 8, Metadata{}, "custom")
	if err == nil {
		t.Fatal("expected plaza hourly metadata validation to fail")
	}
}

func TestAllowedClipDuration(t *testing.T) {
	for _, sec := range []int{5, 60, 300, 600, 900} {
		if !IsAllowedClipDuration(sec) {
			t.Fatalf("%d should be allowed", sec)
		}
	}
	for _, sec := range []int{4, 901} {
		if IsAllowedClipDuration(sec) {
			t.Fatalf("%d should be rejected", sec)
		}
	}
}

func TestBuildStoaramaContinuousPathPreservesLegacyKey(t *testing.T) {
	start := time.Date(2026, time.July, 6, 12, 34, 56, 789000000, time.UTC)
	got, err := BuildDisplayPath(Policy{
		Profile:       ProfileStoaramaV1,
		JobKind:       JobKindContinuousWindow,
		FolderName:    "recordings",
		RecordingID:   8,
		JobID:         99,
		ClipStartedAt: start,
	})
	if err != nil {
		t.Fatalf("path: %v", err)
	}
	want := "recordings/8/continuous/1783341296.mp4"
	if got != want {
		t.Fatalf("path=%q want %q", got, want)
	}
}
