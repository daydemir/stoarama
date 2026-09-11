package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
)

func TestSeptemberImportAndFreezeCLIPreserveExactBatchScope(t *testing.T) {
	cohort := joinedrecording.CohortForBatch(joinedrecording.SeptemberBatchID)
	evidence := struct {
		RecordingJobs []joinedHistoricalQualificationJobs `json:"recording_jobs"`
	}{}
	for i, id := range cohort.RecordingIDs {
		jobs := make([]int64, 14)
		for day := range jobs {
			jobs[day] = int64(i*14 + day + 1)
		}
		evidence.RecordingJobs = append(evidence.RecordingJobs, joinedHistoricalQualificationJobs{RecordingID: id, JobIDs: jobs})
	}
	data, _ := json.Marshal(evidence)
	path := filepath.Join(t.TempDir(), "nine-jobs.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{"--connection-id", "9", "--batch-id", joinedrecording.SeptemberBatchID, "--evidence-file", path}
	request, err := parseJoinedImportHistoricalQualification(config.Config{}, args)
	if err != nil || request.BatchID != joinedrecording.SeptemberBatchID || len(request.RecordingJobs) != 9 {
		t.Fatalf("approved additive import rejected: request=%+v err=%v", request, err)
	}
	payload, err := joinedTier1FreezePayload(joinedFreezeTier1Request{BatchID: joinedrecording.SeptemberBatchID})
	if err != nil {
		t.Fatal(err)
	}
	data, _ = json.Marshal(payload)
	var wire struct {
		RecordingIDs []int64 `json:"recording_ids"`
		Cutoff       string  `json:"eligibility_cutoff"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		t.Fatal(err)
	}
	if len(wire.RecordingIDs) != 9 || wire.RecordingIDs[0] != 339 || wire.Cutoff != "2026-09-11T05:15:24.544Z" {
		t.Fatalf("additive CLI silently used original batch authority: %s", data)
	}
	if _, err := parseJoinedSealStreamDay(config.Config{}, []string{"--batch-id", joinedrecording.SeptemberBatchID, "--recording-id", "339", "--local-date", "2026-08-26"}); err != nil {
		t.Fatal(err)
	}
	if _, err := parseJoinedSealStreamDay(config.Config{}, []string{"--batch-id", joinedrecording.SeptemberBatchID, "--recording-id", "377", "--local-date", "2026-08-26"}); err == nil {
		t.Fatal("additive day command accepted old recording")
	}
}
