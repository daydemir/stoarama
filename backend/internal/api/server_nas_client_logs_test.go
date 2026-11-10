package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
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
	if _, err := pool.Exec(ctx, `INSERT INTO nas_client_logs(connection_id,received_at,client_version,boot_id,lines) VALUES(13,now()-interval '8 days','old','b','[]'),(14,now()-interval '8 days','old','b','[]'),(14,now()-interval '6 days','retained-offline','b','[]')`); err != nil {
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
	var retained int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM nas_client_logs WHERE client_version='retained-offline'`).Scan(&retained); err != nil || retained != 1 {
		t.Fatalf("disconnected logs within retention count=%d: %v", retained, err)
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

func nasDiagnosticPatterns() [][2]string {
	v := "fixture-sensitive-value"
	out := [][2]string{
		{`{"api_key": "` + v + `"}`, v},
		{`{'token': '` + v + `'}`, v},
		{"secret=" + v, v},
		{`{"api_key": "prefix\"` + v + `"}`, v},
		{"Authorization: Bearer " + v, v},
		{`{"Authorization": "Bearer ` + v + `"}`, v},
		{"https://user:" + v + "@host/path", v},
		{"eyJ" + strings.Repeat("a", 20) + "." + strings.Repeat("b", 20) + "." + strings.Repeat("c", 20), strings.Repeat("a", 20)},
		{"tskey-" + "auth-" + v, v},
		{"AKIA" + strings.Repeat("Q", 16), strings.Repeat("Q", 16)},
		{`{"R2_ACCESS_KEY_ID": "` + strings.Repeat("a", 32) + `"}`, strings.Repeat("a", 32)},
		{"https://host/path?X-Amz-Signature=" + v + "&other=x", v},
		{"https://nas.example.ts.net/path", "nas.example.ts.net"},
		{"100.64.0.1", "100.64.0.1"},
		{"100.127.255.255", "100.127.255.255"},
	}
	out = append(out,
		[2]string{`{"Authorization":["Bearer ` + v + `"]}`, v},
		[2]string{`{"Cookie":{"session":"` + v + `"}}`, v},
		[2]string{`Authorization: Digest username="` + v + `", nonce="` + v + `", cnonce="` + v + `", response="` + v + `"`, v},
		[2]string{`{\"api_key\":\"` + v + `\"}`, v},
		[2]string{`{\\\"password\\\":\\\"` + v + `\\\"}`, v},
		[2]string{"fd7a:115c:a1e0::1", "fd7a:115c:a1e0::1"},
		[2]string{"[FD7A:115C:A1E0:abcd:1234:5678:90ab:cdef]:443", "FD7A:115C:A1E0"},
	)

	for _, scheme := range []string{"postgres", "postgresql", "mysql", "redis", "rediss"} {
		out = append(out, [2]string{scheme + "://user:" + v + "@host/db", v})
	}
	return out
}

func TestNASDiagnosticsRedactEveryPattern(t *testing.T) {
	for _, c := range nasDiagnosticPatterns() {
		if got := redactNASClientLogLine(c[0]); strings.Contains(got, c[1]) || !strings.Contains(got, "[REDACTED]") {
			t.Fatalf("pattern %q leaked: %q", c[0], got)
		}
	}
	for _, safe := range []string{"100.63.255.255", "100.128.0.0", "sha256=" + strings.Repeat("a", 64), "POST /account/connections/cleanup/lease"} {
		if got := redactNASClientLogLine(safe); got != safe {
			t.Fatalf("overredacted %q: %q", safe, got)
		}
	}
}

func TestNASClientLogStormKeepsSevenDaysAndSQLFiltersNewest(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	s := &Server{pool: pool}
	if _, err := pool.Exec(ctx, `INSERT INTO nas_client_logs(connection_id,received_at,client_version,boot_id,lines)
      SELECT 13,now()-interval '6 days','storm','b','[{"at":"first","level":"WARN","message":"keep seven days"}]'::jsonb FROM generate_series(1,2100)`); err != nil {
		t.Fatal(err)
	}
	lines := []map[string]any{
		{"seq": 1, "at": "one", "level": "WARN", "message": "needle older"},
		{"seq": 2, "at": "two", "level": "WARN", "message": "irrelevant"},
		{"seq": 3, "at": "three", "level": "ERROR", "message": "needle newer"},
	}
	w := cleanupCall(t, s.handleAccountNASClientLogs, 47, 5, "", "", map[string]any{"client_version": "test", "lines": lines})
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM nas_client_logs WHERE client_version='storm'`).Scan(&count); err != nil || count != 2100 {
		t.Fatalf("retention count=%d: %v", count, err)
	}
	req := httptest.NewRequest("GET", "/x?since=1h&grep=NEEDLE&limit=1", nil)
	route := chi.NewRouteContext()
	route.URLParams.Add("id", "13")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, route))
	w = httptest.NewRecorder()
	s.handleAdminNASClientLogs(w, req)
	var out struct {
		Uploads int                `json:"uploads"`
		Lines   []nasClientLogLine `json:"lines"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &out) != nil || out.Uploads != 1 || len(out.Lines) != 1 || out.Lines[0].Message != "needle newer" {
		t.Fatalf("read %d %s", w.Code, w.Body.String())
	}
}

func TestNASCleanupReleaseHeartbeatAndDiagnosticRedaction(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()
	ctx := context.Background()
	var cid int64
	if err := pool.QueryRow(ctx, `INSERT INTO connections(account_id,kind,api_key_id) VALUES(47,'nas_pull',123) RETURNING id`).Scan(&cid); err != nil {
		t.Fatal(err)
	}
	s := &Server{pool: pool}
	release := cleanupCall(t, s.handleAdminNASReleaseQuarantine, 47, 123, "id", fmt.Sprint(cid), map[string]any{})
	if release.Code != 200 {
		t.Fatal(release.Body.String())
	}
	for _, c := range nasDiagnosticPatterns() {
		status, _ := json.Marshal(map[string]any{"last_error": c[0], "nested": []any{c[0]}, "quarantined": 2, "quarantine_item_ids": []int{5, 6}})
		request := connectionHeartbeatRequest{ClientVersion: "test", ClientPhase: "idle", ClientPreviousExit: "clean", ClientLastError: c[0], Cleanup: status}
		w := cleanupCall(t, s.handleAccountConnectionHeartbeat, 47, 123, "", "", request)
		var response connectionHeartbeatResponse
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &response) != nil || response.CleanupReleaseGeneration != 1 {
			t.Fatalf("heartbeat %d %s", w.Code, w.Body.String())
		}
		var stored, last string
		if err := pool.QueryRow(ctx, `SELECT nas_cleanup_status::text,client_last_error FROM connections WHERE id=$1`, cid).Scan(&stored, &last); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(stored, c[1]) || strings.Contains(last, c[1]) {
			t.Fatalf("diagnostic leak: %s %s", stored, last)
		}
	}
	// A lost response or an old client cannot consume the release request.
	w := cleanupCall(t, s.handleAccountConnectionHeartbeat, 47, 123, "", "", map[string]any{})
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"cleanup_release_generation":1`) {
		t.Fatal(w.Body.String())
	}
	release = cleanupCall(t, s.handleAdminNASReleaseQuarantine, 47, 123, "id", fmt.Sprint(cid), map[string]any{})
	if release.Code != 200 || !strings.Contains(release.Body.String(), `"cleanup_release_generation":2`) {
		t.Fatal(release.Body.String())
	}
}

func TestNASCleanupLeastLeasedItemDoesNotStarveFreshWork(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	_, err := pool.Exec(ctx, `INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256,mode) VALUES(1,47,13,'fair',$1,'[{"kind":"raw","source_id":1},{"kind":"raw","source_id":2}]',repeat('b',64),'dry_run')`, cleanupPolicySHA)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `INSERT INTO nas_cleanup_items(id,plan_id,ordinal,attempt) VALUES(1,1,1,2),(2,1,2,0)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256,mode,priority) VALUES(2,47,13,'high-priority-blocked',$1,'[{"kind":"raw","source_id":3}]',repeat('c',64),'dry_run',100)`, cleanupPolicySHA)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `INSERT INTO nas_cleanup_items(id,plan_id,ordinal,attempt) VALUES(3,2,1,1)`)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{pool: pool}
	w := cleanupCall(t, s.handleAccountNASCleanupLease, 47, 5, "", "", map[string]any{"client_version": "old-client", "protocol": 1})
	var out struct {
		Tasks []cleanupTask `json:"tasks"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &out) != nil || len(out.Tasks) != 1 || out.Tasks[0].ItemID != 2 || len(out.Tasks[0].ApprovedProof) != 0 {
		t.Fatalf("fair lease %d %s", w.Code, w.Body.String())
	}
}

func TestNASClientLogsPullScopedCredential(t *testing.T) {
	pool := cleanupTestPool(t)
	s := &Server{pool: pool}
	keyID := int64(5)
	principal := accountPrincipal{AccountID: 47, AuthType: "api_key", APIKeyID: &keyID, KeyScopes: []string{accountScopePull}}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/account/connections/client-logs", bytes.NewBufferString(`{"client_version":"test","lines":[{"level":"WARN","message":"cleanup deferred"}]}`))
	req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, principal))
	w := httptest.NewRecorder()
	s.confineAccountScope(http.HandlerFunc(s.handleAccountNASClientLogs)).ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("pull log upload: %d %s", w.Code, w.Body.String())
	}
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM nas_client_logs WHERE connection_id=13`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("stored %d: %v", count, err)
	}
}
