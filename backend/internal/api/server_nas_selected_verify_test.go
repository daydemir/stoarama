package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestValidateNASSelectedVerifyResult(t *testing.T) {
	valid := nasSelectedVerifyResultRequest{Attempt: 1, PlanSHA256: strings.Repeat("a", 64), Outcome: "match", DurationMS: 2}
	if err := validateNASSelectedVerifyResult(valid); err != nil {
		t.Fatalf("valid: %v", err)
	}
	for name, mutate := range map[string]func(*nasSelectedVerifyResultRequest){
		"attempt":  func(r *nasSelectedVerifyResultRequest) { r.Attempt = 0 },
		"plan":     func(r *nasSelectedVerifyResultRequest) { r.PlanSHA256 = "bad" },
		"outcome":  func(r *nasSelectedVerifyResultRequest) { r.Outcome = "copied" },
		"duration": func(r *nasSelectedVerifyResultRequest) { r.DurationMS = -1 },
		"error":    func(r *nasSelectedVerifyResultRequest) { r.Error = strings.Repeat("x", 501) },
	} {
		req := valid
		mutate(&req)
		if err := validateNASSelectedVerifyResult(req); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestNASSelectedVerifyLifecycle(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if dsn == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("selected_verify_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `CREATE TABLE accounts(id bigint PRIMARY KEY);CREATE TABLE connections(id bigint PRIMARY KEY,account_id bigint,kind text,api_key_id bigint);
 INSERT INTO accounts VALUES(47),(48);INSERT INTO connections VALUES(13,47,'nas_pull',5),(14,48,'nas_pull',6);`); err != nil {
		t.Fatal(err)
	}
	migration, err := os.ReadFile("../../../infra/sql/migrations/0165_nas_selected_verify.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO nas_selected_verify_plans(id,account_id,connection_id,label,manifest,plan_sha256) VALUES(1,47,13,'plan','[]',repeat('a',64));
 INSERT INTO nas_selected_verify_items(id,plan_id,ordinal,kind,source_id,relative_path,size_bytes,sha256) VALUES(1,1,1,'raw',1,'f/a.mp4',3,repeat('b',64));`); err != nil {
		t.Fatal(err)
	}
	s := &Server{pool: pool}
	call := func(h http.HandlerFunc, account, key int64, itemID string, payload any) (int, map[string]any) {
		t.Helper()
		body, _ := json.Marshal(payload)
		req := httptest.NewRequest("POST", "/x", bytes.NewReader(body))
		route := chi.NewRouteContext()
		route.URLParams.Add("itemId", itemID)
		req = req.WithContext(context.WithValue(context.WithValue(req.Context(), chi.RouteCtxKey, route), accountPrincipalContextKey, accountPrincipal{AccountID: account, APIKeyID: &key}))
		rec := httptest.NewRecorder()
		h(rec, req)
		out := map[string]any{}
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
		return rec.Code, out
	}
	lease := func() nasSelectedVerifyTask {
		t.Helper()
		code, out := call(s.handleAccountConnectionSelectedVerifyLease, 47, 5, "", nasSelectedVerifyLeaseRequest{MaxTasks: 1, ClientVersion: "test"})
		if code != 200 {
			t.Fatalf("lease %d %v", code, out)
		}
		data, _ := json.Marshal(out)
		var response nasSelectedVerifyLeaseResponse
		_ = json.Unmarshal(data, &response)
		if len(response.Tasks) != 1 {
			t.Fatalf("tasks=%+v", response.Tasks)
		}
		return response.Tasks[0]
	}
	first := lease()
	result := nasSelectedVerifyResultRequest{Attempt: first.Attempt, PlanSHA256: first.PlanSHA256, Outcome: "match", DurationMS: 1}
	if code, _ := call(s.handleAccountConnectionSelectedVerifyResult, 48, 6, "1", result); code != 409 {
		t.Fatalf("foreign result %d", code)
	}
	if _, err := pool.Exec(ctx, `UPDATE nas_selected_verify_items SET lease_expires_at=now()-interval '1 minute' WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	if code, _ := call(s.handleAccountConnectionSelectedVerifyResult, 47, 5, "1", result); code != 409 {
		t.Fatalf("expired accepted: %d", code)
	}
	second := lease()
	if second.Attempt != 2 {
		t.Fatalf("attempt=%d", second.Attempt)
	}
	if code, _ := call(s.handleAccountConnectionSelectedVerifyResult, 47, 5, "1", result); code != 409 {
		t.Fatalf("old attempt accepted: %d", code)
	}
	result.Attempt = second.Attempt
	if code, out := call(s.handleAccountConnectionSelectedVerifyResult, 47, 5, "1", result); code != 200 {
		t.Fatalf("result=%d %v", code, out)
	}
	if code, _ := call(s.handleAccountConnectionSelectedVerifyResult, 47, 5, "1", result); code != 200 {
		t.Fatalf("ACK retry=%d", code)
	}
	result.Outcome = "missing"
	if code, _ := call(s.handleAccountConnectionSelectedVerifyResult, 47, 5, "1", result); code != 409 {
		t.Fatalf("rewrote terminal result: %d", code)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO nas_selected_verify_items(id,plan_id,ordinal,kind,source_id,relative_path,size_bytes,sha256,state,attempts,lease_expires_at) VALUES(2,1,2,'collated',2,'joined/f/b.mp4',3,repeat('b',64),'leased',3,now()-interval '1 minute')`); err != nil {
		t.Fatal(err)
	}
	if code, _ := call(s.handleAccountConnectionSelectedVerifyLease, 47, 5, "", nasSelectedVerifyLeaseRequest{MaxTasks: 1}); code != 200 {
		t.Fatalf("expiry sweep=%d", code)
	}
	var state string
	_ = pool.QueryRow(ctx, `SELECT state FROM nas_selected_verify_items WHERE id=2`).Scan(&state)
	if state != "unverified" {
		t.Fatalf("exhausted=%s", state)
	}

	if _, err := pool.Exec(ctx, `INSERT INTO nas_selected_verify_items(id,plan_id,ordinal,kind,source_id,relative_path,size_bytes,sha256) VALUES(3,1,3,'collated',3,'joined/f/large.mp4',34359738368,repeat('b',64))`); err != nil {
		t.Fatal(err)
	}
	large := lease()
	remaining := time.Until(large.ExpiresAt)
	if remaining < 135*time.Minute || remaining > 140*time.Minute {
		t.Fatalf("large-file lease=%s", remaining)
	}
}
