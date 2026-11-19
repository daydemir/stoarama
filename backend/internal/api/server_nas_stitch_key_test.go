package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
)

func TestNASStitchDedicatedKeyThroughRouter(t *testing.T) {
	f, plan, result := newNASStitchTaskFixture(t)
	f.s.cfg.NASStitchEnabled = true
	_, err := f.pool.Exec(f.ctx, `
ALTER TABLE accounts ADD COLUMN email text DEFAULT 'fixture@example.test',
 ADD COLUMN name text DEFAULT 'fixture', ADD COLUMN role text DEFAULT 'admin',
 ADD COLUMN status text DEFAULT 'active';
CREATE TABLE users (id bigint PRIMARY KEY,email text,name text,is_operator boolean);
CREATE TABLE memberships (user_id bigint,org_id bigint,role text);
CREATE TABLE account_sessions (id bigint PRIMARY KEY,user_id bigint,current_org_id bigint,member_email text,
 session_hash text,revoked_at timestamptz,expires_at timestamptz,last_used_at timestamptz);
CREATE TABLE account_auth_events (account_id bigint,api_key_id bigint,event_type text,
 actor_type text,actor_ref text,detail_jsonb jsonb);
INSERT INTO users VALUES(1,'operator@example.test','operator',true);
INSERT INTO memberships VALUES(1,48,'owner');
ALTER SEQUENCE account_api_keys_id_seq RESTART WITH 100;
`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.pool.Exec(f.ctx, `INSERT INTO account_sessions(id,user_id,current_org_id,session_hash,expires_at)
VALUES(1,1,48,$1,clock_timestamp()+interval '1 hour')`, hashSecret("operator-fixture")); err != nil {
		t.Fatal(err)
	}
	_, _, admin, err := mintAccountAPIKey(f.ctx, f.pool, 48, "admin fixture", accountScopeRead, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler := f.s.router()
	call := func(method, path, key string, body any) *httptest.ResponseRecorder {
		t.Helper()
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(method, path, bytes.NewReader(data))
		req.Header.Set("Content-Type", "application/json")
		if key == "" {
			req.AddCookie(&http.Cookie{Name: accountSessionCookie, Value: "operator-fixture"})
		} else {
			req.Header.Set("Authorization", "Bearer "+key)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		return w
	}
	const mint = "/api/v1/admin/nas-stitch/key"
	const stitch = "/api/v1/account/connections/stitch/"
	post := func(path, key string, body any, want int) *httptest.ResponseRecorder {
		t.Helper()
		w := call(http.MethodPost, path, key, body)
		if w.Code != want {
			t.Fatalf("%s status %d, want %d", path, w.Code, want)
		}
		return w
	}
	post(mint, admin, nil, http.StatusForbidden)
	issue := func() (id int64, token string) {
		t.Helper()
		w := post(mint, "", nil, http.StatusCreated)
		var body struct {
			ID     int64                `json:"id"`
			Token  string               `json:"token"`
			Scopes []accountAPIKeyScope `json:"scopes"`
		}
		if w.Header().Get("Cache-Control") != "no-store" || json.Unmarshal(w.Body.Bytes(), &body) != nil ||
			body.ID <= 0 || body.Token == "" || len(body.Scopes) != 1 || body.Scopes[0] != accountScopeNASStitch {
			t.Fatal("missing isolated key issuance metadata")
		}
		return body.ID, body.Token
	}
	id, worker := issue()
	otherID, other := issue()
	var legacyMatch bool
	if err := f.pool.QueryRow(f.ctx, "SELECT EXISTS(SELECT 1 FROM account_api_keys WHERE secret_hash=$1)",
		hashSecret(worker)).Scan(&legacyMatch); err != nil || legacyMatch {
		t.Fatal("previous binaries could authenticate a dedicated key")
	}
	badID, _, wrongAccount, err := mintAccountAPIKey(f.ctx, f.pool, 48, "wrong account", accountScopeNASStitch, nil)
	if err != nil {
		t.Fatal(err)
	}
	post(stitch+"lease", wrongAccount, nil, http.StatusUnauthorized)
	for _, scopes := range [][]string{
		{}, {"unrecognized"}, {accountScopeRead},
		{string(accountScopeNASStitch), accountScopeRead},
		{string(accountScopeNASStitch), string(accountScopeNASStitch)},
	} {
		if _, err := f.pool.Exec(f.ctx, "UPDATE account_api_keys SET account_id=47,scopes=$2 WHERE id=$1", badID, scopes); err != nil {
			t.Fatal(err)
		}
		post(stitch+"lease", wrongAccount, nil, http.StatusUnauthorized)
	}
	if _, err := f.pool.Exec(f.ctx, "UPDATE account_api_keys SET scopes=$2,secret_hash=$3 WHERE id=$1",
		badID, []string{string(accountScopeNASStitch)}, hashSecret(wrongAccount)); err != nil {
		t.Fatal(err)
	}
	post(stitch+"lease", wrongAccount, nil, http.StatusUnauthorized)
	for _, path := range []string{
		"/api/v1/account/me", stitch + "lease", stitch + "tools/ffmpeg", stitch + "corpus",
		"/api/v1/admin/accounts",
	} {
		if w := call(http.MethodGet, path, worker, nil); w.Code != http.StatusUnauthorized {
			t.Fatalf("dedicated key reached GET %s: %d", path, w.Code)
		}
	}
	for _, path := range []string{mint, "/api/v1/account/connections/heartbeat",
		"/api/v1/account/connections/cleanup/lease", "/api/v1/account/connections/stitch-certifications/claim"} {
		post(path, worker, nil, http.StatusUnauthorized)
	}
	identity := map[string]any{"lease_token": result.Token, "plan_sha256": result.PlanSHA}
	post(stitch+"7/checkpoint", worker, identity, http.StatusConflict)
	post(stitch+"7/checkpoint", "fixture-key", identity, http.StatusOK)
	identity["release"] = true
	post(stitch+"7/checkpoint", "fixture-key", identity, http.StatusOK)
	w := post(stitch+"lease", worker, nil, http.StatusOK)
	var lease struct{ Task *nasStitchTask }
	if json.Unmarshal(w.Body.Bytes(), &lease) != nil || lease.Task == nil || lease.Task.ID != 7 ||
		lease.Task.Token == result.Token || lease.Task.PlanSHA != result.PlanSHA {
		t.Fatal("dedicated key received no fresh lease")
	}
	var owner int64
	if err := f.pool.QueryRow(f.ctx, `SELECT lease_api_key_id FROM nas_stitch_tasks WHERE id=7`).Scan(&owner); err != nil || owner != id {
		t.Fatalf("lease owner differs: %d %v", owner, err)
	}
	post(stitch+"7/checkpoint", worker, identity, http.StatusConflict) // stale token
	identity["lease_token"] = lease.Task.Token
	post(stitch+"7/checkpoint", other, identity, http.StatusConflict) // cannot release another key's lease
	delete(identity, "release")
	post(stitch+"7/checkpoint", worker, identity, http.StatusOK)
	result.Token = lease.Task.Token
	post(stitch+"7/result", other, result, http.StatusConflict)
	post(stitch+"7/result", worker, result, http.StatusOK)
	post(stitch+"7/result", "fixture-key", result, http.StatusConflict)
	post(stitch+"7/result", worker, result, http.StatusOK)
	identity["outputs_ready"] = true
	post(stitch+"7/ready", other, identity, http.StatusConflict)
	post(stitch+"7/ready", worker, identity, http.StatusOK)
	post(stitch+"7/ready", other, identity, http.StatusConflict)
	post(stitch+"7/ready", worker, identity, http.StatusOK)
	post(fmt.Sprintf("/api/v1/admin/api-keys/%d/revoke", id), "", nil, http.StatusOK)
	post(stitch+"7/result", worker, result, http.StatusUnauthorized)
	post(stitch+"7/ready", worker, identity, http.StatusUnauthorized)
	if _, err := f.pool.Exec(f.ctx, "DELETE FROM account_api_keys WHERE id=$1", id); err == nil {
		t.Fatal("deleting a referenced key would erase lease ownership")
	}
	plan.OutputFolder = "stream_V85r_keep28/2/2026-09-20T01-00-00Z"
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	result.PlanSHA = cleanupSHA(raw)
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO nas_stitch_tasks(id,scope_id,recording_id,hour_at,plan,plan_sha256)
 VALUES(8,1,2,'2026-09-20T01:00Z',$1,$2),(9,1,2,'2026-09-20T02:00Z',$1,$2)`, raw, result.PlanSHA); err != nil {
		t.Fatal(err)
	}
	claim := func(key string, expected int64) *nasStitchTask {
		t.Helper()
		w := post(stitch+"lease", key, nil, http.StatusOK)
		var response struct{ Task *nasStitchTask }
		if json.Unmarshal(w.Body.Bytes(), &response) != nil ||
			(expected == 0 && response.Task != nil) ||
			(expected != 0 && (response.Task == nil || response.Task.ID != expected)) {
			t.Fatalf("lease did not match expected task %d", expected)
		}
		return response.Task
	}
	next := claim(other, 8)
	result.Token = next.Token
	result.Outputs[0].Path = plan.OutputFolder + "/run-000.mp4"
	post(stitch+"8/result", other, result, http.StatusOK)
	const abandon = "/api/v1/admin/nas-stitch/8/abandon-registration"
	abandonBody := map[string]string{"plan_sha256": next.PlanSHA, "reason": "fixture key expired before receipt"}
	post(abandon, other, abandonBody, http.StatusUnauthorized)
	post(abandon, admin, abandonBody, http.StatusForbidden)
	post(abandon, "", abandonBody, http.StatusConflict)
	if _, err := f.pool.Exec(f.ctx, `UPDATE account_api_keys SET expires_at=clock_timestamp() WHERE id=$1`, otherID); err != nil {
		t.Fatal(err)
	}
	ready := map[string]any{"lease_token": next.Token, "plan_sha256": next.PlanSHA, "outputs_ready": true}
	post(stitch+"8/ready", other, ready, http.StatusUnauthorized)
	post(stitch+"8/ready", "fixture-key", ready, http.StatusConflict)
	claim("fixture-key", 0)
	post(abandon, "", abandonBody, http.StatusOK)
	post(abandon, "", abandonBody, http.StatusOK)
	abandonBody["reason"] = "new reason"
	post(abandon, "", abandonBody, http.StatusConflict)
	claim("fixture-key", 9)
	var state string
	var abandoned, legacyUnbound bool
	var outputs, creationAudits, abandonAudits int
	err = f.pool.QueryRow(f.ctx, `SELECT t.state,t.registration_abandoned_at IS NOT NULL,
 (SELECT lease_api_key_id IS NULL FROM nas_stitch_tasks WHERE id=9),
 (SELECT count(*) FROM nas_stitch_outputs WHERE task_id=t.id),
 (SELECT count(*) FROM account_auth_events WHERE account_id=47 AND event_type='api_key_created'),
 (SELECT count(*) FROM account_auth_events WHERE account_id=47 AND event_type='nas_stitch_registration_abandoned')
 FROM nas_stitch_tasks t WHERE t.id=8`).Scan(&state, &abandoned, &legacyUnbound, &outputs, &creationAudits, &abandonAudits)
	if err != nil || state != "verified" || !abandoned || !legacyUnbound || outputs != 1 || creationAudits != 2 || abandonAudits != 1 {
		t.Fatalf("lost or duplicated abandonment evidence: %s %t %t %d %d %d %v", state, abandoned, legacyUnbound, outputs, creationAudits, abandonAudits, err)
	}
}

func TestNASStitchDedicatedKeyRechecksAfterRowLock(t *testing.T) {
	f, _, _ := newNASStitchTaskFixture(t)
	id, _, _, err := mintAccountAPIKey(f.ctx, f.pool, 47, "race fixture", accountScopeNASStitch, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	revoker, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer revoker.Rollback(f.ctx)
	if _, err := revoker.Exec(ctx, "UPDATE account_api_keys SET revoked_at=clock_timestamp() WHERE id=$1", id); err != nil {
		t.Fatal(err)
	}
	worker, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan bool, 1)
	done := make(chan struct{})
	defer func() {
		cancel()
		<-done
		worker.Rollback(f.ctx)
	}()
	workerPID := worker.Conn().PgConn().PID()
	revokerPID := revoker.Conn().PgConn().PID()
	go func() {
		defer close(done)
		caller := nasStitchCaller{keyID: id, accountID: 47, dedicated: true}
		result <- caller.lockActive(ctx, worker)
	}()
	for {
		var blocked bool
		err := f.pool.QueryRow(ctx, `SELECT $1=ANY(pg_blocking_pids($2))`, int32(revokerPID), int32(workerPID)).Scan(&blocked)
		if err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		select {
		case <-result:
			t.Fatal("stitch mutation skipped the revocation row lock")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if err := revoker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if <-result {
		t.Fatal("stitch mutation accepted a key revoked during its lock wait")
	}
}

func TestNASStitchDedicatedCheckpointRechecksLeaseAfterKeyWait(t *testing.T) {
	f, _, lease := newNASStitchTaskFixture(t)
	f.s.cfg.NASStitchEnabled = true
	keyID, _, _, err := mintAccountAPIKey(f.ctx, f.pool, 47, "checkpoint fixture", accountScopeNASStitch, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 6*time.Second)
	defer cancel()
	blocker, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(f.ctx)
	if _, err := blocker.Exec(ctx, "SELECT id FROM account_api_keys WHERE id=$1 FOR NO KEY UPDATE", keyID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `UPDATE nas_stitch_tasks SET lease_api_key_id=$1,
 lease_expires_at=clock_timestamp()+interval '1 second' WHERE id=7`, keyID); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(map[string]string{"lease_token": lease.Token, "plan_sha256": lease.PlanSHA})
	if err != nil {
		t.Fatal(err)
	}
	route := chi.NewRouteContext()
	route.URLParams.Add("id", "7")
	principal := accountPrincipal{AccountID: 47, APIKeyID: &keyID, KeyScopes: []string{string(accountScopeNASStitch)}}
	ctx = context.WithValue(context.WithValue(ctx, chi.RouteCtxKey, route), accountPrincipalContextKey, principal)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/account/connections/stitch/7/checkpoint", bytes.NewReader(body)).WithContext(ctx)
	response := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		w := httptest.NewRecorder()
		f.s.handleAccountNASStitchCheckpoint(w, req)
		response <- w
	}()
	pid := int32(blocker.Conn().PgConn().PID())
	for {
		var blocked bool
		err := f.pool.QueryRow(ctx, `SELECT EXISTS(
 SELECT FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))`, pid).Scan(&blocked)
		if err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		select {
		case <-response:
			t.Fatal("checkpoint bypassed the active key lock")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if _, err := f.pool.Exec(ctx, `SELECT pg_sleep(greatest(0,extract(epoch FROM
 lease_expires_at-clock_timestamp()))+0.02) FROM nas_stitch_tasks WHERE id=7`); err != nil {
		t.Fatal(err)
	}
	if err := blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	w := <-response
	var result map[string]bool
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &result) != nil || len(result) != 1 || result["continue"] {
		t.Fatalf("checkpoint continued an expired lease: %d %s", w.Code, w.Body.String())
	}
}
