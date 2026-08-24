package api

import (
	"encoding/json"
	"testing"
)

func TestJoinedContainmentResponseCarriesInScopeMediaHourIDs(t *testing.T) {
	want := joinedContainmentResponse{
		InScopeMediaCount: 2,
		InScopeMediaSample: []joinedContainmentArtifactSample{
			{ID: 101, ArtifactKind: "media", ScopeID: "hour-413", HourID: "hour-413"},
			{ID: 102, ArtifactKind: "media", ScopeID: "hour-421", HourID: "hour-421"},
		},
	}
	wire, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var got joinedContainmentResponse
	if err := json.Unmarshal(wire, &got); err != nil {
		t.Fatal(err)
	}
	if got.InScopeMediaCount != 2 || len(got.InScopeMediaSample) != 2 {
		t.Fatalf("in-scope media summary=%+v", got)
	}
	if got.InScopeMediaSample[0].HourID != "hour-413" || got.InScopeMediaSample[1].HourID != "hour-421" {
		t.Fatalf("hour IDs=%q,%q", got.InScopeMediaSample[0].HourID, got.InScopeMediaSample[1].HourID)
	}
}
