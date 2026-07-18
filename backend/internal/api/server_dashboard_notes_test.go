package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
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

func TestDashboardStreamNotesStayAccountScoped(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()

	_, accountA := seedUserOrg(t, pool, "notes-a@example.com", false)
	_, accountB := seedUserOrg(t, pool, "notes-b@example.com", false)
	var streamID int64
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO streams (provider, external_id, name, slug, stream_url)
		VALUES ('test', 'org-notes', 'Org notes', 'org-notes', 'https://example.com/stream')
		RETURNING id`).Scan(&streamID); err != nil {
		t.Fatal(err)
	}

	put := func(accountID int64, note string) {
		rec := httptest.NewRecorder()
		req := requestWithID(http.MethodPut, strconv.FormatInt(streamID, 10), `{"note":"`+note+`"}`)
		req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: accountID}))
		s.handleDashboardStreamNotePut(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("put account %d: status=%d body=%s", accountID, rec.Code, rec.Body.String())
		}
	}
	put(accountA, "A only")
	put(accountB, "B only")

	var noteA, noteB string
	if err := pool.QueryRow(context.Background(), `SELECT note FROM account_stream_notes WHERE account_id=$1 AND stream_id=$2`, accountA, streamID).Scan(&noteA); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT note FROM account_stream_notes WHERE account_id=$1 AND stream_id=$2`, accountB, streamID).Scan(&noteB); err != nil {
		t.Fatal(err)
	}
	if noteA != "A only" || noteB != "B only" {
		t.Fatalf("notes crossed accounts: A=%q B=%q", noteA, noteB)
	}

	rec := httptest.NewRecorder()
	req := requestWithID(http.MethodDelete, strconv.FormatInt(streamID, 10), "")
	req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: accountA}))
	s.handleDashboardStreamNoteDelete(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("delete account A: status=%d body=%s", rec.Code, rec.Body.String())
	}
	var deleted, remaining int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*) FILTER (WHERE account_id=$1), count(*) FILTER (WHERE account_id=$2 AND note='B only')
		FROM account_stream_notes WHERE stream_id=$3`, accountA, accountB, streamID).Scan(&deleted, &remaining); err != nil {
		t.Fatal(err)
	}
	if deleted != 0 || remaining != 1 {
		t.Fatalf("delete crossed accounts: A rows=%d B rows=%d", deleted, remaining)
	}
}
