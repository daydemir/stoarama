package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
)

// requestWithID builds an *http.Request carrying chi's {id} route param and the
// given JSON body, so the tag handlers can be exercised without a real router.
func requestWithID(method, id, body string) *http.Request {
	req := httptest.NewRequest(method, "/api/v1/dashboard/streams/"+id+"/tags", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", id)
	return req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
}

func TestDashboardStreamTagsAddRejectsEmptyTags(t *testing.T) {
	s := &Server{}
	rec := httptest.NewRecorder()
	s.handleDashboardStreamTagsAdd(rec, requestWithID(http.MethodPost, "7", `{"tags":[]}`))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty tags, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestDashboardStreamTagsAddRejectsBlankTagValues(t *testing.T) {
	s := &Server{}
	rec := httptest.NewRecorder()
	s.handleDashboardStreamTagsAdd(rec, requestWithID(http.MethodPost, "7", `{"tags":["   ",""]}`))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for blank tag values, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestDashboardStreamTagsRemoveRejectsEmptyTag(t *testing.T) {
	s := &Server{}
	rec := httptest.NewRecorder()
	s.handleDashboardStreamTagsRemove(rec, requestWithID(http.MethodDelete, "7", `{"tag":"  "}`))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty tag, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestDedupeStringsDropsProviderTags(t *testing.T) {
	got := dedupeStrings([]string{"traffic", " provider:youtube ", "Provider:sdot", "capture_type:hls", "traffic", "source:camera"})
	want := []string{"traffic", "source:camera"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("dedupeStrings()=%v want %v", got, want)
	}
}

func TestDashboardStreamTagsRejectInvalidStreamID(t *testing.T) {
	s := &Server{}
	rec := httptest.NewRecorder()
	s.handleDashboardStreamTagsAdd(rec, requestWithID(http.MethodPost, "0", `{"tags":["x"]}`))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid stream id, got %d body=%s", rec.Code, rec.Body.String())
	}
}
