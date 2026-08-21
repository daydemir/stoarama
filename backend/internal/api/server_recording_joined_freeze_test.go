package api

import (
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
)

func validJoinedTier1FreezeRequest(t *testing.T) joinedTier1FreezeRequest {
	t.Helper()
	cutoff, err := time.Parse(time.RFC3339Nano, joinedrecording.Tier1FrozenAt)
	if err != nil {
		t.Fatal(err)
	}
	return joinedTier1FreezeRequest{ProtocolVersion: joinedrecording.JoinedProtocolVersion, ConnectionID: 1,
		BatchID: "goodplus-20260821-generation-1", Generation: 1,
		SourceEndpoint:     "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com",
		QualificationRunID: 2, RecordingIDs: append([]int64(nil), joinedrecording.Tier1RecordingIDs...),
		OrderedRecordingIDSHA256: joinedrecording.Tier1RecordingIDSHA, EligibilityCutoff: cutoff}
}

func TestJoinedTier1FreezeRequestRequiresExactFrozenAuthority(t *testing.T) {
	valid := validJoinedTier1FreezeRequest(t)
	if err := valid.validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*joinedTier1FreezeRequest){
		"order": func(r *joinedTier1FreezeRequest) {
			r.RecordingIDs[0], r.RecordingIDs[1] = r.RecordingIDs[1], r.RecordingIDs[0]
		},
		"hash":       func(r *joinedTier1FreezeRequest) { r.OrderedRecordingIDSHA256 = strings.Repeat("0", 64) },
		"cutoff":     func(r *joinedTier1FreezeRequest) { r.EligibilityCutoff = r.EligibilityCutoff.Add(time.Nanosecond) },
		"endpoint":   func(r *joinedTier1FreezeRequest) { r.SourceEndpoint += "/" },
		"generation": func(r *joinedTier1FreezeRequest) { r.Generation = 2 },
	} {
		t.Run(name, func(t *testing.T) {
			request := validJoinedTier1FreezeRequest(t)
			mutate(&request)
			if err := request.validate(); err == nil {
				t.Fatal("changed Tier-1 authority was accepted")
			}
		})
	}
}

func TestJoinedTier1FreezeApplyRequiresLowerHexRequestSHA(t *testing.T) {
	request := validJoinedTier1FreezeRequest(t)
	request.Apply = true
	for _, value := range []string{"", strings.Repeat("A", 64), strings.Repeat("g", 64)} {
		request.ExpectedRequestSHA256 = value
		if err := request.validate(); err == nil {
			t.Fatalf("expected SHA %q was accepted", value)
		}
	}
	request.ExpectedRequestSHA256 = strings.Repeat("a", 64)
	if err := request.validate(); err != nil {
		t.Fatal(err)
	}
}

func TestJoinedTier1FreezeExistingBatchRequiresExactExplicitRequest(t *testing.T) {
	request := validJoinedTier1FreezeRequest(t)
	plan := joinedTier1FreezePlan{BatchID: request.BatchID, Generation: request.Generation,
		ConnectionID: request.ConnectionID, SourceEndpoint: request.SourceEndpoint,
		RecordingIDs: append([]int64(nil), request.RecordingIDs...), SelectionAuthority: joinedrecording.SelectionAuthority{
			QualificationRunID: request.QualificationRunID, OrderedRecordingIDSHA256: request.OrderedRecordingIDSHA256,
			Cutoff: request.EligibilityCutoff,
		}}
	if !joinedTier1FreezeRequestMatchesPlan(request, plan) {
		t.Fatal("exact request did not match stored plan")
	}
	for name, mutate := range map[string]func(*joinedTier1FreezeRequest){
		"connection":        func(r *joinedTier1FreezeRequest) { r.ConnectionID++ },
		"qualification run": func(r *joinedTier1FreezeRequest) { r.QualificationRunID++ },
		"source endpoint": func(r *joinedTier1FreezeRequest) {
			r.SourceEndpoint = "https://abcdef0123456789abcdef0123456789.r2.cloudflarestorage.com"
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := request
			mutate(&changed)
			if joinedTier1FreezeRequestMatchesPlan(changed, plan) {
				t.Fatal("changed request matched stored plan")
			}
		})
	}
}

func TestJoinedFinalFreezeReplayStates(t *testing.T) {
	for _, state := range []string{"frozen", "index_sealed", "published"} {
		if !joinedBatchHasFinalFreeze(state) {
			t.Fatalf("state %q lost final-freeze replay", state)
		}
	}
	for _, state := range []string{"snapshotting", "building", "terminal_failed", ""} {
		if joinedBatchHasFinalFreeze(state) {
			t.Fatalf("state %q incorrectly accepted as final-freeze replay", state)
		}
	}
}
