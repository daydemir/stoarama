package main

import (
	"reflect"
	"testing"
)

func TestCleanupLocationTagsForStreamRemovesRedundantStructuredGeoTags(t *testing.T) {
	got := cleanupLocationTagsForStream(locationTagCleanupRow{
		Tags:                []string{"shortlist", "city:seattle", "country:us", "state:wa", "scene:road"},
		LocationCountryCode: "US",
		LocationRegion:      "WA",
		LocationCity:        "Seattle",
	})
	wantTags := []string{"shortlist", "scene:road"}
	wantRemoved := []string{"city:seattle", "country:us", "state:wa"}
	if !reflect.DeepEqual(got.UpdatedTags, wantTags) {
		t.Fatalf("updated=%v want %v", got.UpdatedTags, wantTags)
	}
	if !reflect.DeepEqual(got.RemovedTags, wantRemoved) {
		t.Fatalf("removed=%v want %v", got.RemovedTags, wantRemoved)
	}
}

func TestCleanupLocationTagsForStreamReturnsEmptyTagsWhenAllTagsAreRemoved(t *testing.T) {
	got := cleanupLocationTagsForStream(locationTagCleanupRow{
		Tags:                []string{"city:seattle", "country:us", "state:wa"},
		LocationCountryCode: "US",
		LocationRegion:      "WA",
		LocationCity:        "Seattle",
	})
	wantTags := []string{}
	wantRemoved := []string{"city:seattle", "country:us", "state:wa"}
	if got.UpdatedTags == nil {
		t.Fatal("updated=nil want empty slice")
	}
	if !reflect.DeepEqual(got.UpdatedTags, wantTags) {
		t.Fatalf("updated=%v want %v", got.UpdatedTags, wantTags)
	}
	if !reflect.DeepEqual(got.RemovedTags, wantRemoved) {
		t.Fatalf("removed=%v want %v", got.RemovedTags, wantRemoved)
	}
}

func TestCleanupLocationTagsForStreamKeepsOnlyCopyOfGeo(t *testing.T) {
	got := cleanupLocationTagsForStream(locationTagCleanupRow{
		Tags: []string{"city:seattle", "country:us", "state:wa", "shortlist"},
	})
	wantTags := []string{"city:seattle", "country:us", "state:wa", "shortlist"}
	if !reflect.DeepEqual(got.UpdatedTags, wantTags) {
		t.Fatalf("updated=%v want %v", got.UpdatedTags, wantTags)
	}
	if len(got.RemovedTags) != 0 {
		t.Fatalf("removed=%v want empty", got.RemovedTags)
	}
}
