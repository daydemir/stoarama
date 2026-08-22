package api

import (
	"testing"

	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
)

func exactHistoricalQualificationRequestFixture() joinedHistoricalQualificationRequest {
	request := joinedHistoricalQualificationRequest{
		ProtocolVersion: joinedrecording.JoinedProtocolVersion,
		ConnectionID:    7,
		BatchID:         joinedrecording.Tier1BatchID,
		Generation:      1,
		RecordingJobs:   make([]joinedHistoricalQualificationJobs, len(joinedrecording.Tier1RecordingIDs)),
	}
	var jobID int64 = 1
	for i, recordingID := range joinedrecording.Tier1RecordingIDs {
		request.RecordingJobs[i].RecordingID = recordingID
		request.RecordingJobs[i].JobIDs = make([]int64, 14)
		for day := range request.RecordingJobs[i].JobIDs {
			request.RecordingJobs[i].JobIDs[day] = jobID
			jobID++
		}
	}
	return request
}

func TestHistoricalQualificationRequestIsExactAndApplyIsHashGated(t *testing.T) {
	request := exactHistoricalQualificationRequestFixture()
	if err := request.validate(); err != nil {
		t.Fatal(err)
	}
	request.Apply = true
	if err := request.validate(); err == nil {
		t.Fatal("apply without an approved preview hash was accepted")
	}
	request.ExpectedRequestSHA256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if err := request.validate(); err != nil {
		t.Fatal(err)
	}

	for name, mutate := range map[string]func(*joinedHistoricalQualificationRequest){
		"recording order": func(value *joinedHistoricalQualificationRequest) {
			value.RecordingJobs[0], value.RecordingJobs[1] = value.RecordingJobs[1], value.RecordingJobs[0]
		},
		"missing day": func(value *joinedHistoricalQualificationRequest) {
			value.RecordingJobs[0].JobIDs = value.RecordingJobs[0].JobIDs[:13]
		},
		"duplicate job": func(value *joinedHistoricalQualificationRequest) {
			value.RecordingJobs[1].JobIDs[0] = value.RecordingJobs[0].JobIDs[0]
		},
		"different batch": func(value *joinedHistoricalQualificationRequest) { value.BatchID += "-other" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := exactHistoricalQualificationRequestFixture()
			mutate(&changed)
			if err := changed.validate(); err == nil {
				t.Fatal("non-exact historical authority request was accepted")
			}
		})
	}
}
