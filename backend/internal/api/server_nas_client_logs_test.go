package api

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
)

func TestNASClientLogsUploadRedactRetainAndRead(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	s := &Server{pool: pool}
	upload := func(lines []map[string]any) int {
		w := cleanupCall(t, s.handleAccountNASClientLogs, 47, 5, "", "", map[string]any{"client_version": "b9f41fd0", "boot_id": "boot-1", "journal_entries": 64, "quarantined": 1, "dropped_lines": 0, "lines": lines})
		return w.Code
	}
	if _, err := pool.Exec(ctx, `INSERT INTO nas_client_logs(connection_id,received_at,client_version,boot_id,lines) VALUES(13,now()-interval '8 days','old','b','[]')`); err != nil {
		t.Fatal(err)
	}
	if code := upload([]map[string]any{
		// Assembled at runtime so secret scanners never see a credential literal.
		{"seq": 1, "at": "2026-11-10T00:10:00Z", "level": "WARN", "message": "NAS cleanup deferred: Authorization: " + "Bearer " + "abcdefghijklmnop https://r2.example/obj?" + "Sig" + "nature=deadbeef"},
		{"seq": 2, "at": "2026-11-10T00:10:01Z", "level": "ERROR", "message": "cleanup item=5 journal entry quarantined"},
	}); code != 200 {
		t.Fatalf("upload %d", code)
	}
	var stored string
	var old int
	if err := pool.QueryRow(ctx, `SELECT lines::text,(SELECT count(*) FROM nas_client_logs WHERE client_version='old') FROM nas_client_logs WHERE connection_id=13 AND client_version='b9f41fd0'`).Scan(&stored, &old); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(stored, "abcdefghijklmnop") || strings.Contains(stored, "deadbeef") || !strings.Contains(stored, "[REDACTED]") {
		t.Fatalf("server did not redact: %s", stored)
	}
	if old != 0 {
		t.Fatal("rows older than 7 days were kept")
	}
	if _, err := pool.Exec(ctx, `UPDATE nas_client_logs SET lines='[]'`); err == nil {
		t.Fatal("client logs were editable")
	}
	many := make([]map[string]any, 501)
	for n := range many {
		many[n] = map[string]any{"seq": n, "at": "x", "level": "WARN", "message": "m"}
	}
	if code := upload(many); code != 400 {
		t.Fatalf("501 lines accepted: %d", code)
	}
	if code := upload([]map[string]any{{"seq": 1, "at": "x", "level": "WARN", "message": strings.Repeat("a", 5000)}}); code != 400 {
		t.Fatalf("oversized line accepted: %d", code)
	}
	read := func(query string) map[string]any {
		req := httptest.NewRequest("GET", "/x?"+query, nil)
		route := chi.NewRouteContext()
		route.URLParams.Add("id", "13")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, route))
		w := httptest.NewRecorder()
		s.handleAdminNASClientLogs(w, req)
		var out map[string]any
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &out) != nil {
			t.Fatalf("read %s: %d %s", query, w.Code, w.Body.String())
		}
		return out
	}
	if lines := read("since=1h&grep=QUARANTINED")["lines"].([]any); len(lines) != 1 {
		t.Fatalf("grep %v", lines)
	}
	got := read("since=1h")
	if len(got["lines"].([]any)) != 2 || got["latest_upload"].(map[string]any)["journal_entries"].(float64) != 64 {
		t.Fatalf("read %v", got)
	}
}
