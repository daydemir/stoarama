package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestParseSortQueryDefaults(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/dashboard/streams", nil)
	rr := httptest.NewRecorder()

	orderExpr, sortBy, sortDir, ok := parseSortQuery(rr, req, map[string]string{
		"id":   "s.id",
		"name": "s.name",
	}, "name", "desc")
	if !ok {
		t.Fatalf("expected ok")
	}
	if got, want := orderExpr, "s.name"; got != want {
		t.Fatalf("orderExpr=%q want %q", got, want)
	}
	if got, want := sortBy, "name"; got != want {
		t.Fatalf("sortBy=%q want %q", got, want)
	}
	if got, want := sortDir, "desc"; got != want {
		t.Fatalf("sortDir=%q want %q", got, want)
	}
}

func TestParseSortQueryInvalidSortBy(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/dashboard/streams?sort_by=unknown", nil)
	rr := httptest.NewRecorder()

	_, _, _, ok := parseSortQuery(rr, req, map[string]string{
		"id": "s.id",
	}, "id", "desc")
	if ok {
		t.Fatalf("expected !ok")
	}
	if got, want := rr.Code, http.StatusBadRequest; got != want {
		t.Fatalf("status=%d want %d", got, want)
	}
}

func TestParseSortQueryInvalidSortDir(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/dashboard/streams?sort_by=id&sort_dir=sideways", nil)
	rr := httptest.NewRecorder()

	_, _, _, ok := parseSortQuery(rr, req, map[string]string{
		"id": "s.id",
	}, "id", "desc")
	if ok {
		t.Fatalf("expected !ok")
	}
	if got, want := rr.Code, http.StatusBadRequest; got != want {
		t.Fatalf("status=%d want %d", got, want)
	}
}
