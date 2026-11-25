package collation

import (
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/recordingnaming"
)

// A replacement is generation 3 in its identity and manifest, but its delivery
// path is exactly the tool-agnostic path of the hour it supersedes.
func TestReplacementHourIdentityManifestAndPath(t *testing.T) {
	w := testWork()
	w.NamingProfile, w.FolderName, w.RecordingID = string(recordingnaming.ProfileStoaramaV1), "recordings", 339
	w.ScheduledStart = time.Date(2026, 8, 13, 17, 0, 0, 0, time.UTC)
	w.ScheduledEnd = w.ScheduledStart.Add(time.Hour)
	start := time.Date(2026, 8, 13, 17, 0, 8, 0, time.UTC)
	old, err := DeliveryPath(w, start, start.Add(time.Minute), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	w.Replaces = &ReplacedHour{HourRecordID: 7, HourID: "goodplus__recording-339__date-2026-08-13__hour-01__generation-2"}
	if w.Generation() != ReplacementGeneration {
		t.Fatal("replacement is not generation 3")
	}
	replacement, err := DeliveryPath(w, start, start.Add(time.Minute), 1, 1)
	if err != nil || replacement != old {
		t.Fatalf("replacement path %q, superseded %q: %v", replacement, old, err)
	}
	// The plaza naming profile too: no tool, batch or replacement label.
	plaza := testWork()
	plaza.ScheduledStart, plaza.ScheduledEnd = w.ScheduledStart, w.ScheduledEnd
	plazaOld, err := DeliveryPath(plaza, start, start.Add(20*time.Minute), 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	plaza.Replaces, plaza.BatchID = w.Replaces, "recovery-best14-20261125"
	plazaNew, err := DeliveryPath(plaza, start, start.Add(20*time.Minute), 2, 3)
	if err != nil || plazaNew != plazaOld || strings.Contains(plazaNew, "recovery") || strings.Contains(strings.ToLower(plazaNew), "ffmpeg") {
		t.Fatalf("plaza replacement path %q, superseded %q: %v", plazaNew, plazaOld, err)
	}
	id, err := HourIDForGeneration("recovery-x", 339, "2026-08-13", 1, ReplacementGeneration)
	if err != nil || !strings.HasSuffix(id, "__generation-3") {
		t.Fatalf("replacement identity %q %v", id, err)
	}
	if _, err := HourIDForGeneration("recovery-x", 339, "2026-08-13", 1, 4); err == nil {
		t.Fatal("generation 4 accepted")
	}
	m := HourManifest{SchemaVersion: 1, PolicyVersion: PolicyVersion, Generation: ReplacementGeneration, BatchID: "recovery-x", HourID: id,
		RecordingID: 339, LocalDate: "2026-08-13", DeliveryHour: 1, ReplacesHourID: w.Replaces.HourID, Status: StatusGapOnly}
	if err := m.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*HourManifest){
		"generation 3 replacing nothing": func(m *HourManifest) { m.ReplacesHourID = "" },
		"generation 2 replacing":         func(m *HourManifest) { m.Generation = Generation },
		"identity at another generation": func(m *HourManifest) { m.HourID = strings.TrimSuffix(m.HourID, "3") + "2" },
	} {
		bad := m
		mutate(&bad)
		if bad.Validate() == nil {
			t.Fatalf("%s validated", name)
		}
	}
}
