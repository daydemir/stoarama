package joinedrecording

import (
	"slices"
	"testing"
)

func TestApprovedSeptemberCohortIsDisjointAndPinned(t *testing.T) {
	cohort := CohortForBatch("goodplus-20260911-generation-1")
	want := []int64{339, 407, 417, 424, 427, 430, 441, 444, 445}
	if !slices.Equal(cohort.RecordingIDs, want) || cohort.FrozenAt != "2026-09-11T05:15:24.544Z" {
		t.Fatalf("unexpected approved September cohort: %+v", cohort)
	}
	if len(cohort.RecordingIDs)*14*12 != 1512 {
		t.Fatal("additive cohort must contain exactly 1512 scheduled hours")
	}
	for _, id := range cohort.RecordingIDs {
		if slices.Contains(Tier1RecordingIDs, id) {
			t.Fatalf("recording %d would duplicate existing public progress", id)
		}
	}
	cohort.RecordingIDs[0] = 377
	if CohortForBatch("goodplus-20260911-generation-1").RecordingIDs[0] != 339 {
		t.Fatal("caller can mutate approved cohort")
	}
}

func TestLegacyCohortPinsRemainUnchanged(t *testing.T) {
	for _, batchID := range []string{Tier1BatchID, "tier1-2026-08-generation-1"} {
		cohort := CohortForBatch(batchID)
		if !slices.Equal(cohort.RecordingIDs, Tier1RecordingIDs) || cohort.FrozenAt != Tier1FrozenAt ||
			cohort.RecordingIDSHA != Tier1RecordingIDSHA {
			t.Fatalf("legacy authority changed for %s", batchID)
		}
	}
}
