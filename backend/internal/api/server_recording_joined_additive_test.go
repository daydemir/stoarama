package api

import (
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
)

func TestSeptemberFrozenNamingOverridesOnlyApprovedRecording339(t *testing.T) {
	folder, metadata, err := joinedFrozenRecordingNaming(joinedrecording.SeptemberBatchID, 339, "stoarama_v1", "recordings", []byte(`{}`))
	if err != nil || folder != "52_Europe_Germany_Schwbisch_Gmnd_Marktplatz" || metadata.PlazaID != "052" || metadata.City != "Schwäbisch Gmünd" {
		t.Fatalf("approved joined-only naming not applied: %s %+v %v", folder, metadata, err)
	}
	for _, scope := range []struct {
		batch string
		id    int64
	}{{joinedrecording.Tier1BatchID, 339}, {joinedrecording.SeptemberBatchID, 407}} {
		folder, metadata, err := joinedFrozenRecordingNaming(scope.batch, scope.id, "stoarama_v1", "recordings", []byte(`{}`))
		if err != nil || folder != "recordings" || metadata.PlazaID != "" {
			t.Fatalf("unrelated raw naming changed: %s %+v %v", folder, metadata, err)
		}
	}
}

func TestSeptemberQualificationAndFreezeAcceptOnlyApprovedDisjointCohort(t *testing.T) {
	cohort := joinedrecording.CohortForBatch(joinedrecording.SeptemberBatchID)
	request := exactHistoricalQualificationRequestFixture()
	request.BatchID = joinedrecording.SeptemberBatchID
	request.RecordingJobs = request.RecordingJobs[:9]
	for i, id := range cohort.RecordingIDs {
		request.RecordingJobs[i].RecordingID = id
	}
	if err := request.validate(); err != nil {
		t.Fatalf("approved nine-recording historical import rejected: %v", err)
	}
	freeze := validJoinedTier1FreezeRequest(t)
	freeze.BatchID = joinedrecording.SeptemberBatchID
	freeze.RecordingIDs = cohort.RecordingIDs
	freeze.OrderedRecordingIDSHA256 = cohort.RecordingIDSHA
	freeze.EligibilityCutoff, _ = time.Parse(time.RFC3339Nano, cohort.FrozenAt)
	if err := freeze.validate(); err != nil {
		t.Fatalf("approved additive freeze rejected: %v", err)
	}
	request.RecordingJobs[0].RecordingID = 377
	freeze.RecordingIDs[0] = 377
	if request.validate() == nil || freeze.validate() == nil {
		t.Fatal("additive batch accepted an existing recording and would duplicate public progress")
	}
}
