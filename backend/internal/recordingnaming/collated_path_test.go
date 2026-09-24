package recordingnaming

import (
	"strings"
	"testing"
	"time"
)

func collatedTestPolicy(t *testing.T, tz string, hourLocal string) CollatedPolicy {
	t.Helper()
	loc, err := time.LoadLocation(tz)
	if err != nil {
		t.Fatal(err)
	}
	start, err := time.ParseInLocation("2006-01-02 15:04:05", hourLocal, loc)
	if err != nil {
		t.Fatal(err)
	}
	return CollatedPolicy{
		FolderName: "100000_North_America_US_Key_West_Duval_Street",
		Metadata:   Metadata{PlazaID: "100000", PlazaName: "Duval Street", Continent: "North America", Country: "US", City: "Key West"},
		Timezone:   tz, HourStart: start, ActualStart: start.Add(27 * time.Second), ActualEnd: start.Add(time.Hour + 16*time.Second),
		Part: 1, Parts: 1,
	}
}

func TestBuildCollatedPathMirrorsRawDayTreeForEveryClockHour(t *testing.T) {
	for hour := 0; hour < 24; hour++ {
		p := collatedTestPolicy(t, "America/New_York", "2026-07-26 00:00:00")
		p.HourStart = p.HourStart.Add(time.Duration(hour) * time.Hour)
		p.ActualStart, p.ActualEnd = p.HourStart.Add(time.Second), p.HourStart.Add(59*time.Minute)
		got, err := BuildCollatedPath(p)
		if err != nil {
			t.Fatalf("hour %02d: %v", hour, err)
		}
		wantPrefix := "100000_North_America_US_Key_West_Duval_Street/July/26-Sunday/100000_Duval_Street_2026_July_W4_Sunday_hour_"
		if !strings.HasPrefix(got, wantPrefix) || !strings.Contains(got, "_hour_"+twoDigits(hour)+"_") {
			t.Fatalf("hour %02d path %s", hour, got)
		}
	}
	p := collatedTestPolicy(t, "America/New_York", "2026-07-26 13:00:00")
	p.Part, p.Parts = 4, 60
	got, err := BuildCollatedPath(p)
	if err != nil {
		t.Fatal(err)
	}
	want := "100000_North_America_US_Key_West_Duval_Street/July/26-Sunday/100000_Duval_Street_2026_July_W4_Sunday_hour_13_part_04_130027-140016.mp4"
	if got != want {
		t.Fatalf("path\n got %s\nwant %s", got, want)
	}
	manifest, err := BuildCollatedManifestPath(p)
	if err != nil || manifest != "100000_North_America_US_Key_West_Duval_Street/July/26-Sunday/100000_Duval_Street_2026_July_W4_Sunday_hour_13.manifest.json" {
		t.Fatalf("manifest %s err=%v", manifest, err)
	}
	// Hour 23 may run past midnight; the day folder stays the hour's day.
	p = collatedTestPolicy(t, "America/New_York", "2026-07-26 23:00:00")
	got, err = BuildCollatedPath(p)
	if err != nil || got != "100000_North_America_US_Key_West_Duval_Street/July/26-Sunday/100000_Duval_Street_2026_July_W4_Sunday_hour_23_230027-000016.mp4" {
		t.Fatalf("midnight path %s err=%v", got, err)
	}
}

func TestBuildCollatedPathRejectsInvalidRangesAndDisambiguatesDST(t *testing.T) {
	base := collatedTestPolicy(t, "America/New_York", "2026-07-26 13:00:00")
	for name, mutate := range map[string]func(*CollatedPolicy){
		"off-hour start": func(p *CollatedPolicy) { p.HourStart = p.HourStart.Add(time.Minute) },
		"before hour":    func(p *CollatedPolicy) { p.ActualStart = p.HourStart.Add(-time.Second) },
		"after hour":     func(p *CollatedPolicy) { p.ActualStart = p.HourStart.Add(time.Hour) },
		"empty":          func(p *CollatedPolicy) { p.ActualEnd = p.ActualStart },
		"overrun":        func(p *CollatedPolicy) { p.ActualEnd = p.HourStart.Add(time.Hour + 16*time.Minute) },
		"part zero":      func(p *CollatedPolicy) { p.Part = 0 },
		"part > parts":   func(p *CollatedPolicy) { p.Part, p.Parts = 3, 2 },
		"too many parts": func(p *CollatedPolicy) { p.Part, p.Parts = 1, 100 },
		"missing folder": func(p *CollatedPolicy) { p.FolderName = " " },
		"bad timezone":   func(p *CollatedPolicy) { p.Timezone = "Mars/Base" },
		"missing plaza":  func(p *CollatedPolicy) { p.Metadata.PlazaName = "" },
	} {
		p := base
		mutate(&p)
		if got, err := BuildCollatedPath(p); err == nil {
			t.Fatalf("%s accepted: %s", name, got)
		}
	}
	// 2026-11-01 01:00 repeats in New York; both hours must get distinct names.
	loc, _ := time.LoadLocation("America/New_York")
	first := time.Date(2026, 11, 1, 5, 0, 0, 0, time.UTC) // 01:00 EDT
	second := first.Add(time.Hour)                        // 01:00 EST
	if first.In(loc).Hour() != 1 || second.In(loc).Hour() != 1 {
		t.Fatal("fixture is not the repeated hour")
	}
	names := map[string]bool{}
	for _, h := range []time.Time{first, second} {
		p := base
		p.HourStart, p.ActualStart, p.ActualEnd = h, h.Add(time.Second), h.Add(time.Minute)
		got, err := BuildCollatedManifestPath(p)
		if err != nil {
			t.Fatal(err)
		}
		names[got] = true
	}
	if len(names) != 2 {
		t.Fatalf("DST repeated hour collided: %v", names)
	}
}

func twoDigits(v int) string { return string([]byte{byte('0' + v/10), byte('0' + v%10)}) }
