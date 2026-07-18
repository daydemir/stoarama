package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDashboardStreamNoteRequiresPrincipal(t *testing.T) {
	s := &Server{}
	rec := httptest.NewRecorder()
	s.handleDashboardStreamNotePut(rec, requestWithID(http.MethodPut, "7", `{"note":"hello"}`))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("code=%d want 401", rec.Code)
	}
}

func TestDashboardStreamNoteRejectsOversize(t *testing.T) {
	s := &Server{}
	rec := httptest.NewRecorder()
	req := requestWithID(http.MethodPut, "7", `{"note":"`+strings.Repeat("x", maxStreamNoteLength+1)+`"}`)
	req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: 9}))
	s.handleDashboardStreamNotePut(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code=%d want 400", rec.Code)
	}
}
