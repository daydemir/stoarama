package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/korea"
)

func TestHandleKoreaInventory(t *testing.T) {
	oldLoader := loadKoreaInventory
	t.Cleanup(func() { loadKoreaInventory = oldLoader })

	inventory := korea.Inventory{
		RetrievedAt: time.Date(2026, 5, 7, 0, 0, 0, 0, time.UTC),
		Summary: korea.Summary{
			TotalFamilies:   5,
			CoveredFamilies: 5,
			TotalStreams:    1,
			ResolvedStreams: 1,
			Complete:        true,
		},
	}
	loadKoreaInventory = func(_ context.Context, _ koreaInventoryQueryer) (korea.Inventory, error) {
		return inventory, nil
	}

	srv := &Server{}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/korea/inventory", nil)
	srv.handleKoreaInventory(rec, req)

	if got, want := rec.Code, http.StatusOK; got != want {
		t.Fatalf("status=%d want=%d", got, want)
	}
	var got korea.Inventory
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if !got.Summary.Complete {
		t.Fatalf("summary complete=false want true: %#v", got.Summary)
	}
}
