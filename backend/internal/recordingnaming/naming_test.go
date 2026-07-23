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
	want := "08_Europe_Finland_Kuopio_Market_Square/May/12-Monday/08_Kuopio_Market_Square_2025_May_W2_Monday_hour_03.mp4"
	if got != want {
		t.Fatalf("path=%q want %q", got, want)
	}
}

func TestBuildPlazaHourlyContinuousPathIncludesMinuteSecond(t *testing.T) {
	start := time.Date(2025, time.May, 12, 10, 4, 1, 0, time.UTC)
	got, err := BuildDisplayPath(Policy{
		Profile:       ProfilePlazaHourlyV1,
		JobKind:       JobKindContinuousWindow,
		FolderName:    "01_na_usa_losangeles_venicebeach",
		RecordingID:   1,
		CronTimezone:  "UTC",
		ClipStartedAt: start,
		Metadata: Metadata{
			PlazaID:   "1",
			Continent: "NA",
			Country:   "USA",
			City:      "Los Angeles",
			PlazaName: "Venice Beach",
		},
	})
	if err != nil {
		t.Fatalf("path: %v", err)
	}
	want := "01_na_usa_losangeles_venicebeach/May/12-Monday/01_Venice_Beach_2025_May_W2_Monday_hour_100401.mp4"
	if got != want {
		t.Fatalf("path=%q want %q", got, want)
	}
}

func TestBuildPlazaHourlyPathUsesLocalCalendarDate(t *testing.T) {
	got, err := BuildDisplayPath(Policy{
		Profile:       ProfilePlazaHourlyV1,
		JobKind:       JobKindContinuousWindow,
		FolderName:    "01_North_America_US_Seattle_Test_Plaza",
		RecordingID:   1,
		CronTimezone:  "America/Los_Angeles",
		ClipStartedAt: time.Date(2025, time.May, 13, 6, 30, 0, 0, time.UTC),
		Metadata: Metadata{
			PlazaID:   "1",
			Continent: "North America",
			Country:   "US",
			City:      "Seattle",
			PlazaName: "Test Plaza",
		},
	})
	if err != nil {
		t.Fatalf("path: %v", err)
	}
	want := "01_North_America_US_Seattle_Test_Plaza/May/12-Monday/01_Test_Plaza_2025_May_W2_Monday_hour_233000.mp4"
	if got != want {
		t.Fatalf("path=%q want %q", got, want)
	}
}

func TestValidatePlazaHourlyScheduleAllowsContinuousAnyWindow(t *testing.T) {
	for _, sec := range []int{60, 300, 600} {
		if err := ValidateSchedule(ProfilePlazaHourlyV1, "continuous", "", sec, "06:30", "23:45:00"); err != nil {
			t.Fatalf("continuous plaza hourly should allow %ds clips in any valid window: %v", sec, err)
		}
	}
	if err := ValidateSchedule(ProfilePlazaHourlyV1, "continuous", "", 120, "08:00", "20:00"); err == nil {
		t.Fatal("expected continuous plaza hourly to require 60, 300, or 600 second clips")
	}
	if err := ValidateSchedule(ProfilePlazaHourlyV1, "continuous", "", 60, "", "21:00"); err == nil {
		t.Fatal("expected continuous plaza hourly to require a valid daily window")
	}
	if err := ValidateSchedule(ProfilePlazaHourlyV1, "sampled", "0 8-19 * * *", 300, "", ""); err != nil {
		t.Fatalf("sampled plaza hourly should still be valid: %v", err)
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

func TestPlazaIDMustBePositive(t *testing.T) {
	metadata := Metadata{PlazaID: "0", Continent: "Europe", Country: "Italy", City: "Assisi", PlazaName: "Town Square"}
	if err := metadata.ValidatePlazaHourly(); err == nil {
		t.Fatal("expected zero plaza ID to be rejected")
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
