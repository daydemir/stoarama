package main

import "testing"

func TestPrepareBellevueStreamAcceptsBellevueRows(t *testing.T) {
	feature := bellevueFeature{
		Geometry: bellevueGeometry{Coordinates: []float64{-122.1234, 47.6101}},
		Properties: bellevueProperties{
			ID:             " cctv217 ",
			DisplayAddress: " 108th Ave NE   &  NE 38th Pl ",
			OwnedBy:        "COB",
			Media:          "Stream",
			Channel:        360,
			CameraType:     "360/PTZ",
		},
	}

	item, skipReason := prepareBellevueStream(feature, "https://example.test/query", bellevueSourcePageURL)
	if skipReason != "" {
		t.Fatalf("skipReason=%q want empty", skipReason)
	}
	if item.ExternalID != "CCTV217" {
		t.Fatalf("external_id=%q want CCTV217", item.ExternalID)
	}
	if item.Name != "108th Ave NE & NE 38th Pl" {
		t.Fatalf("name=%q", item.Name)
	}
	if item.SourceURL != "https://trafficcams.bellevuewa.gov:443/traffic-edge/CCTV217L.stream/playlist.m3u8" {
		t.Fatalf("source_url=%q", item.SourceURL)
	}
	if item.OwnerTag != "owner:cob" {
		t.Fatalf("owner_tag=%q want owner:cob", item.OwnerTag)
	}
	if item.Lat == nil || item.Lon == nil {
		t.Fatalf("expected lat/lon")
	}
	if *item.Lat != 47.6101 || *item.Lon != -122.1234 {
		t.Fatalf("lat/lon=(%v,%v)", *item.Lat, *item.Lon)
	}
	if item.LocationText != "108th Ave NE & NE 38th Pl, Bellevue, Washington, United States" {
		t.Fatalf("location_text=%q", item.LocationText)
	}
}

func TestPrepareBellevueStreamRejectsExcludedRows(t *testing.T) {
	tests := []struct {
		name       string
		feature    bellevueFeature
		wantReason string
	}{
		{
			name: "image row",
			feature: bellevueFeature{
				Geometry: bellevueGeometry{Coordinates: []float64{-122.1, 47.6}},
				Properties: bellevueProperties{
					ID:             "405VC01675",
					DisplayAddress: "NE 59th St & I-405",
					OwnedBy:        "WSDOT",
					Media:          "Image",
				},
			},
			wantReason: "media_not_stream",
		},
		{
			name: "owner excluded",
			feature: bellevueFeature{
				Geometry: bellevueGeometry{Coordinates: []float64{-122.1, 47.6}},
				Properties: bellevueProperties{
					ID:             "ABC123",
					DisplayAddress: "Somewhere",
					OwnedBy:        "Redmond",
					Media:          "Stream",
				},
			},
			wantReason: "owner_excluded",
		},
		{
			name: "missing geometry",
			feature: bellevueFeature{
				Properties: bellevueProperties{
					ID:             "CCTV999",
					DisplayAddress: "Nowhere",
					OwnedBy:        "Bellevue",
					Media:          "Stream",
				},
			},
			wantReason: "missing_geometry",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, got := prepareBellevueStream(tt.feature, "https://example.test/query", bellevueSourcePageURL)
			if got != tt.wantReason {
				t.Fatalf("skipReason=%q want=%q", got, tt.wantReason)
			}
		})
	}
}

func TestBellevueOwnerTag(t *testing.T) {
	if got := bellevueOwnerTag("Bellevue"); got != "owner:bellevue" {
		t.Fatalf("owner tag=%q", got)
	}
	if got := bellevueOwnerTag("COB"); got != "owner:cob" {
		t.Fatalf("owner tag=%q", got)
	}
	if got := bellevueOwnerTag(""); got != "owner:unknown" {
		t.Fatalf("owner tag=%q", got)
	}
}
