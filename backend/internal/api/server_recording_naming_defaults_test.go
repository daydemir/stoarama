package api

import (
	"strings"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/recordingnaming"
)

func TestCatalogNamingDefaultsFillOnlyMissingFields(t *testing.T) {
	req := &recordingNamingRequest{
		Profile:  recordingnaming.ProfilePlazaHourlyV1.String(),
		Metadata: recordingnaming.Metadata{Country: "User country"},
	}
	applyCatalogNamingDefaults(req, catalogNamingDefaults{
		Continent: "Europe",
		Country:   "Italy",
		City:      "Assisi",
		PlazaName: "Town Square",
	})
	if req.Metadata.Continent != "Europe" || req.Metadata.Country != "User country" || req.Metadata.City != "Assisi" || req.Metadata.PlazaName != "Town Square" {
		t.Fatalf("unexpected catalog defaults: %+v", req.Metadata)
	}
}

func TestLinkedPlazaNamingDefersIDAllocation(t *testing.T) {
	req := &recordingNamingRequest{
		Profile: recordingnaming.ProfilePlazaHourlyV1.String(),
		Metadata: recordingnaming.Metadata{
			Continent: "Europe",
			Country:   "Italy",
			City:      "Assisi",
			PlazaName: "Town Square",
		},
	}
	if _, _, _, err := resolveRecordingNamingForValidation(req, 17233); err != nil {
		t.Fatalf("linked stream should allocate plaza ID transactionally: %v", err)
	}
	if _, _, _, err := resolveRecordingNamingForValidation(req, 0); err == nil || !strings.Contains(err.Error(), "plaza_id") {
		t.Fatalf("unlinked stream should still require an explicit plaza ID, got %v", err)
	}
}
