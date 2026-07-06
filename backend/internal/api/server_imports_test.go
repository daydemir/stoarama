package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestServiceStreamCreateRejectsMissingRequiredFields(t *testing.T) {
	s := &Server{}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/service/streams", strings.NewReader(`{"provider":"global-street-scores"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	s.handleServiceStreamCreate(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "provider, external_id, name, source_url are required") {
		t.Fatalf("unexpected body: %s", rec.Body.String())
	}
}

func TestServiceStreamByExternalIDRejectsMissingParams(t *testing.T) {
	s := &Server{}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/service/streams/by-external-id?provider=global-street-scores", nil)
	rec := httptest.NewRecorder()

	s.handleServiceStreamByExternalID(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "provider and external_id are required") {
		t.Fatalf("unexpected body: %s", rec.Body.String())
	}
}

func TestServiceStreamTagsRemoveRejectsEmptyTags(t *testing.T) {
	s := &Server{}
	rec := httptest.NewRecorder()

	s.handleServiceStreamTagsRemove(rec, requestWithID(http.MethodDelete, "7", `{"tags":[]}`))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "tags must contain at least one tag") {
		t.Fatalf("unexpected body: %s", rec.Body.String())
	}
}
