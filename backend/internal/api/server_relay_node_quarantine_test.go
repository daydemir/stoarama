package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type quarantineTraceMarker struct{}

type quarantineQueryTrace struct {
	mu      sync.Mutex
	waiting map[string]chan struct{}
}

func (q *quarantineQueryTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	marker, _ := ctx.Value(quarantineTraceMarker{}).(string)
	if marker == "" || !strings.Contains(data.SQL, "SELECT id FROM accounts WHERE id=$1 FOR UPDATE") {
		return ctx
	}
	q.mu.Lock()
	ch := q.waiting[marker]
	q.mu.Unlock()
	if ch != nil {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	return ctx
}

func (*quarantineQueryTrace) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

type quarantineFixture struct {
	pool               *pgxpool.Pool
	server             *Server
	cleanup            func()
	started, heartbeat time.Time
}

func newQuarantineFixture(t *testing.T) quarantineFixture {
	return newQuarantineFixtureWithTrace(t, nil)
}

func newQuarantineFixtureWithTrace(t *testing.T, trace *quarantineQueryTrace) quarantineFixture {
	t.Helper()
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("relay_quarantine_api_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams = map[string]string{"search_path": schema}
	if trace != nil {
		cfg.ConnConfig.Tracer = trace
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	cleanup := func() { pool.Close(); _, _ = admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); admin.Close() }
	started := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	heartbeat := time.Now().UTC().Truncate(time.Microsecond)
	ddl := `CREATE TABLE accounts(id BIGINT PRIMARY KEY,email TEXT NOT NULL,name TEXT NOT NULL DEFAULT '',role TEXT NOT NULL,status TEXT NOT NULL); CREATE TABLE users(id BIGINT PRIMARY KEY,email TEXT NOT NULL,is_operator BOOLEAN NOT NULL); CREATE TABLE account_api_keys(id BIGINT PRIMARY KEY,account_id BIGINT NOT NULL,key_prefix TEXT NOT NULL,secret_hash TEXT NOT NULL,scopes TEXT[] NOT NULL DEFAULT '{}',expires_at TIMESTAMPTZ,last_used_at TIMESTAMPTZ,revoked_at TIMESTAMPTZ); CREATE TABLE relay_groups(id BIGINT PRIMARY KEY,account_id BIGINT NOT NULL,max_streams INT NOT NULL,bandwidth_capacity_bps BIGINT); CREATE TABLE nodes(id BIGINT PRIMARY KEY,account_id BIGINT NOT NULL,node_type TEXT NOT NULL,display_name TEXT NOT NULL,hostname TEXT NOT NULL DEFAULT '',platform TEXT NOT NULL DEFAULT '',status TEXT NOT NULL,enrolled_at TIMESTAMPTZ NOT NULL DEFAULT now(),last_heartbeat_at TIMESTAMPTZ,capabilities_jsonb JSONB NOT NULL DEFAULT '{}',metadata_jsonb JSONB NOT NULL DEFAULT '{}',created_at TIMESTAMPTZ NOT NULL DEFAULT now(),updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),relay_max_streams INT NOT NULL,survey_enabled BOOLEAN NOT NULL DEFAULT false,relay_group_id BIGINT); CREATE TABLE node_tokens(id BIGSERIAL PRIMARY KEY,node_id BIGINT NOT NULL,secret_hash TEXT,revoked_at TIMESTAMPTZ); CREATE TABLE streams(id BIGINT PRIMARY KEY,provider TEXT,source_page_url TEXT,execution_class TEXT); CREATE TABLE storage_destinations(id BIGINT PRIMARY KEY); CREATE TABLE account_billing(account_id BIGINT PRIMARY KEY,has_payment_method BOOLEAN NOT NULL); CREATE TABLE recordings(id BIGINT PRIMARY KEY,account_id BIGINT NOT NULL,status TEXT NOT NULL,start_at TIMESTAMPTZ NOT NULL,end_at TIMESTAMPTZ,capture_via TEXT NOT NULL,stream_url TEXT NOT NULL,stream_id BIGINT,storage_destination_id BIGINT NOT NULL,target_fps INT,preferred_relay_group_id BIGINT,updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),mode TEXT,next_fire_at TIMESTAMPTZ); CREATE TABLE recording_bandwidth_observations(recording_id BIGINT PRIMARY KEY,observed_bandwidth_bps BIGINT NOT NULL,observed_at TIMESTAMPTZ NOT NULL DEFAULT now()); CREATE TABLE recording_jobs(id BIGINT PRIMARY KEY,recording_id BIGINT NOT NULL,status TEXT NOT NULL,scheduled_for TIMESTAMPTZ NOT NULL,kind TEXT NOT NULL,fire_at TIMESTAMPTZ NOT NULL,clip_duration_sec INT NOT NULL,lease_owner TEXT,lease_expires_at TIMESTAMPTZ,lease_token UUID,attempt_count INT NOT NULL DEFAULT 0,updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),window_end_at TIMESTAMPTZ,handoff_owner TEXT,handoff_until TIMESTAMPTZ,relay_fairness_started_at TIMESTAMPTZ); CREATE TABLE recording_canary_reservations(id UUID PRIMARY KEY DEFAULT gen_random_uuid(),recording_id BIGINT NOT NULL,node_id BIGINT NOT NULL,expires_at TIMESTAMPTZ NOT NULL,window_start_at TIMESTAMPTZ,preopen_stage TEXT,created_at TIMESTAMPTZ NOT NULL DEFAULT now()); CREATE TABLE account_auth_events(id BIGSERIAL PRIMARY KEY,account_id BIGINT NOT NULL,api_key_id BIGINT,event_type TEXT,actor_type TEXT,actor_ref TEXT,detail_jsonb JSONB,created_at TIMESTAMPTZ NOT NULL DEFAULT now());`
	if _, err = pool.Exec(ctx, ddl); err != nil {
		cleanup()
		t.Fatal(err)
	}
	raw, err := os.ReadFile("../../../infra/sql/migrations/0138_relay_node_quarantine.sql")
	if err != nil {
		cleanup()
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(raw)); err != nil {
		cleanup()
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `INSERT INTO accounts VALUES(47,'target@example.test','target','member','active'),(1,'operator-account@example.test','operator','admin','active'); INSERT INTO users VALUES(9,'operator@example.test',true),(10,'member@example.test',false); INSERT INTO account_api_keys VALUES(91,1,'admin-prefix','unused','{}',NULL,NULL,NULL),(92,47,'member-prefix','unused','{}',NULL,NULL,NULL),(93,1,'revoked-prefix','unused','{}',NULL,NULL,now()); INSERT INTO relay_groups VALUES(2,47,16,60000000); INSERT INTO nodes(id,account_id,node_type,display_name,status,last_heartbeat_at,relay_max_streams,relay_group_id,capabilities_jsonb) VALUES(66,47,'relay','node66','active',$1,16,2,jsonb_build_object('relay_version','b428cf9c','relay_source_revision',$2,'relay_started_at',$3,'active_jobs',0,'ffmpeg_runtime',jsonb_build_object('qualified',true,'network_probe','host_reached','observed_at',$4))),(93,47,'relay','node93','active',$1,16,2,jsonb_build_object('active_jobs',0)); INSERT INTO node_tokens(node_id,secret_hash) VALUES(66,'keep-token'); INSERT INTO storage_destinations VALUES(1); INSERT INTO account_billing VALUES(47,true)`, heartbeat, strings.Repeat("b", 40), started.Format(time.RFC3339Nano), heartbeat.Format(time.RFC3339Nano))
	if err != nil {
		cleanup()
		t.Fatal(err)
	}
	return quarantineFixture{pool, &Server{pool: pool}, cleanup, started, heartbeat}
}

func callQuarantineWithContext(f quarantineFixture, ctx context.Context, p accountPrincipal, body relayNodeQuarantineRequest) *httptest.ResponseRecorder {
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/relay-nodes/66/quarantine", bytes.NewReader(raw))
	rc := chi.NewRouteContext()
	rc.URLParams.Add("id", "66")
	ctx = context.WithValue(ctx, chi.RouteCtxKey, rc)
	ctx = context.WithValue(ctx, accountPrincipalContextKey, p)
	out := httptest.NewRecorder()
	f.server.handleAdminRelayNodeQuarantine(out, req.WithContext(ctx))
	return out
}

func quarantineRequestBody(f quarantineFixture, key uuid.UUID) relayNodeQuarantineRequest {
	return relayNodeQuarantineRequest{key.String(), 47, 2, 16, "active", "b428cf9c", strings.Repeat("b", 40), f.started.Format(time.RFC3339Nano), f.heartbeat.Format(time.RFC3339Nano), "tls_input_incompatibility", strings.Repeat("a", 64)}
}

func callQuarantine(t *testing.T, f quarantineFixture, p accountPrincipal, body relayNodeQuarantineRequest) *httptest.ResponseRecorder {
	t.Helper()
	return callQuarantineWithContext(f, context.Background(), p, body)
}

func callQuarantinedAccountHandler(t *testing.T, f quarantineFixture, method, path, body string, handler http.HandlerFunc, params map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rc := chi.NewRouteContext()
	for key, value := range params {
		rc.URLParams.Add(key, value)
	}
	ctx := context.WithValue(req.Context(), chi.RouteCtxKey, rc)
	ctx = context.WithValue(ctx, accountPrincipalContextKey, accountPrincipal{AccountID: 47, UserID: 10, Email: "member@example.test", Role: accountRoleMember})
	out := httptest.NewRecorder()
	handler(out, req.WithContext(ctx))
	return out
}

func TestRelayNodeQuarantinePersistsExactReplayAndPermanentlyFencesNode(t *testing.T) {
	f := newQuarantineFixture(t)
	defer f.cleanup()
	operator := accountPrincipal{AccountID: 1, UserID: 9, Email: "operator@example.test", Role: accountRoleAdmin, AuthType: "session"}
	body := quarantineRequestBody(f, uuid.New())
	out := callQuarantine(t, f, operator, body)
	if out.Code != 200 {
		t.Fatalf("quarantine status=%d body=%s", out.Code, out.Body.String())
	}
	first := append([]byte(nil), out.Body.Bytes()...)
	replay := callQuarantine(t, f, operator, body)
	if replay.Code != 200 || !bytes.Equal(first, replay.Body.Bytes()) {
		t.Fatalf("replay status=%d exact=%v body=%s", replay.Code, bytes.Equal(first, replay.Body.Bytes()), replay.Body.String())
	}
	otherActor := accountPrincipal{AccountID: 1, Email: "operator-account@example.test", Role: accountRoleAdmin, AuthType: "api_key", APIKeyID: quarantineInt64(91)}
	if got := callQuarantine(t, f, otherActor, body); got.Code != http.StatusConflict {
		t.Fatalf("cross-actor replay status=%d body=%s", got.Code, got.Body.String())
	}
	changed := body
	changed.EvidenceSHA256 = strings.Repeat("c", 64)
	if got := callQuarantine(t, f, operator, changed); got.Code != 409 {
		t.Fatalf("changed replay status=%d", got.Code)
	}
	var status string
	var group, cap, events, audits, tokens, canceled int
	if err := f.pool.QueryRow(context.Background(), `SELECT status,relay_group_id,relay_max_streams,(SELECT count(*) FROM relay_node_quarantine_events),(SELECT count(*) FROM account_auth_events WHERE event_type='relay_node_quarantined'),(SELECT count(*) FROM node_tokens WHERE node_id=66 AND revoked_at IS NULL),(SELECT count(*) FROM recording_jobs WHERE status='canceled') FROM nodes WHERE id=66`).Scan(&status, &group, &cap, &events, &audits, &tokens, &canceled); err != nil {
		t.Fatal(err)
	}
	if status != "disabled" || group != 2 || cap != 16 || events != 1 || audits != 1 || tokens != 1 || canceled != 0 {
		t.Fatalf("state=%s group=%d cap=%d events=%d audits=%d tokens=%d canceled=%d", status, group, cap, events, audits, tokens, canceled)
	}
	if _, err := f.pool.Exec(context.Background(), `UPDATE nodes SET last_heartbeat_at=now(),capabilities_jsonb=capabilities_jsonb||'{"active_jobs":0}',updated_at=now() WHERE id=66`); err != nil {
		t.Fatalf("heartbeat blocked: %v", err)
	}
	for _, q := range []string{`UPDATE nodes SET status='active' WHERE id=66`, `UPDATE nodes SET relay_group_id=NULL WHERE id=66`, `DELETE FROM nodes WHERE id=66`} {
		if _, err := f.pool.Exec(context.Background(), q); err == nil {
			t.Fatalf("direct bypass succeeded: %s", q)
		}
	}
	if _, err := f.pool.Exec(context.Background(), `INSERT INTO relay_groups VALUES(3,47,16,60000000)`); err != nil {
		t.Fatal(err)
	}
	accountBypasses := []struct {
		method, path, body string
		handler            http.HandlerFunc
		params             map[string]string
	}{
		{http.MethodPatch, "/api/v1/account/nodes/66", `{"status":"active"}`, f.server.handleAccountNodePatch, map[string]string{"id": "66"}},
		{http.MethodDelete, "/api/v1/account/nodes/66", "", f.server.handleAccountNodeDelete, map[string]string{"id": "66"}},
		{http.MethodDelete, "/api/v1/account/nodes/66/relay-group", "", f.server.handleAccountNodeRelayGroupDelete, map[string]string{"id": "66"}},
		{http.MethodPut, "/api/v1/account/nodes/66/relay-group/3", "", f.server.handleAccountNodeRelayGroupPut, map[string]string{"id": "66", "group_id": "3"}},
	}
	for _, bypass := range accountBypasses {
		got := callQuarantinedAccountHandler(t, f, bypass.method, bypass.path, bypass.body, bypass.handler, bypass.params)
		if got.Code != http.StatusConflict {
			t.Fatalf("generic bypass %s %s status=%d body=%s", bypass.method, bypass.path, got.Code, got.Body.String())
		}
	}
}

func TestRelayNodeQuarantineAuthWorkAndCapacityFailClosed(t *testing.T) {
	f := newQuarantineFixture(t)
	defer f.cleanup()
	body := quarantineRequestBody(f, uuid.New())
	if got := callQuarantine(t, f, accountPrincipal{AccountID: 47, UserID: 10, Role: accountRoleMember}, body); got.Code != 403 {
		t.Fatalf("member status=%d", got.Code)
	}
	if got := callQuarantine(t, f, accountPrincipal{AccountID: 47, Role: accountRoleAdmin}, body); got.Code != 403 {
		t.Fatalf("operator-like API key status=%d", got.Code)
	}
	if _, err := f.pool.Exec(context.Background(), `INSERT INTO recordings(id,account_id,status,start_at,capture_via,stream_url,storage_destination_id,preferred_relay_group_id,mode,next_fire_at) VALUES(1,47,'active',now()-interval '1 hour','relay','source',1,2,'continuous',now()+interval '1 hour'); INSERT INTO recording_jobs(id,recording_id,status,scheduled_for,kind,fire_at,clip_duration_sec,lease_owner,lease_expires_at,lease_token,attempt_count,window_end_at) VALUES(1,1,'leased',now(),'continuous_window',now(),60,'node:66',now()+interval '3 minutes',gen_random_uuid(),1,now()+interval '1 hour')`); err != nil {
		t.Fatal(err)
	}
	if got := callQuarantine(t, f, accountPrincipal{UserID: 9, Email: "operator@example.test", Role: accountRoleAdmin, AuthType: "session"}, body); got.Code != 409 {
		t.Fatalf("live work status=%d body=%s", got.Code, got.Body.String())
	}
	var state string
	if err := f.pool.QueryRow(context.Background(), `SELECT status FROM recording_jobs WHERE id=1`).Scan(&state); err != nil || state != "leased" {
		t.Fatalf("job mutated state=%s err=%v", state, err)
	}
}

func TestRelayNodeQuarantineAPIKeyActorIsTypedAndCrossTenant(t *testing.T) {
	for _, tc := range []struct {
		name string
		p    accountPrincipal
		want int
	}{
		{"platform_admin", accountPrincipal{AccountID: 1, Email: "operator-account@example.test", Role: accountRoleAdmin, AuthType: "api_key", APIKeyID: quarantineInt64(91)}, http.StatusOK},
		{"member_account", accountPrincipal{AccountID: 47, Email: "target@example.test", Role: accountRoleMember, AuthType: "api_key", APIKeyID: quarantineInt64(92)}, http.StatusForbidden},
		{"revoked_admin", accountPrincipal{AccountID: 1, Email: "operator-account@example.test", Role: accountRoleAdmin, AuthType: "api_key", APIKeyID: quarantineInt64(93)}, http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newQuarantineFixture(t)
			defer f.cleanup()
			got := callQuarantine(t, f, tc.p, quarantineRequestBody(f, uuid.New()))
			if got.Code != tc.want {
				t.Fatalf("status=%d want=%d body=%s", got.Code, tc.want, got.Body.String())
			}
			if tc.want == http.StatusOK {
				var kind, email, prefix string
				var userID *int64
				var keyID int64
				if err := f.pool.QueryRow(context.Background(), `SELECT actor_kind,operator_user_id,operator_api_key_id,actor_email,actor_key_prefix FROM relay_node_quarantine_events`).Scan(&kind, &userID, &keyID, &email, &prefix); err != nil {
					t.Fatal(err)
				}
				if kind != "operator_api_key" || userID != nil || keyID != 91 || email != "operator-account@example.test" || prefix != "admin-prefix" {
					t.Fatalf("actor kind=%s user=%v key=%d email=%s prefix=%s", kind, userID, keyID, email, prefix)
				}
				var auditKeyID *int64
				if err := f.pool.QueryRow(context.Background(), `SELECT api_key_id FROM account_auth_events WHERE event_type='relay_node_quarantined'`).Scan(&auditKeyID); err != nil || auditKeyID == nil || *auditKeyID != 91 {
					t.Fatalf("audit key=%v err=%v", auditKeyID, err)
				}
			}
		})
	}
}

func quarantineInt64(v int64) *int64 { return &v }

func TestRelayNodeQuarantineRequestIsSingleAndCanonical(t *testing.T) {
	f := newQuarantineFixture(t)
	defer f.cleanup()
	body := quarantineRequestBody(f, uuid.New())
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/relay-nodes/66/quarantine", bytes.NewReader(append(raw, []byte(` {}`)...)))
	rc := chi.NewRouteContext()
	rc.URLParams.Add("id", "66")
	ctx := context.WithValue(req.Context(), chi.RouteCtxKey, rc)
	ctx = context.WithValue(ctx, accountPrincipalContextKey, accountPrincipal{UserID: 9, Email: "operator@example.test", Role: accountRoleAdmin, AuthType: "session"})
	out := httptest.NewRecorder()
	f.server.handleAdminRelayNodeQuarantine(out, req.WithContext(ctx))
	if out.Code != http.StatusBadRequest {
		t.Fatalf("trailing JSON status=%d body=%s", out.Code, out.Body.String())
	}
	body.ExpectedRelayVersion = "  b428cf9c  "
	body.ExpectedRelayStartedAt = f.started.Format("2006-01-02T15:04:05.000000Z07:00")
	body.ExpectedHeartbeatAt = f.heartbeat.Format("2006-01-02T15:04:05.000000Z07:00")
	body.ReasonCode = " tls_input_incompatibility "
	got := callQuarantine(t, f, accountPrincipal{UserID: 9, Email: "operator@example.test", Role: accountRoleAdmin, AuthType: "session"}, body)
	if got.Code != http.StatusOK {
		t.Fatalf("canonical request status=%d body=%s", got.Code, got.Body.String())
	}
}

func TestRelayNodeQuarantineRejectsHandoffReservationAndInsufficientCapacity(t *testing.T) {
	operator := accountPrincipal{UserID: 9, Email: "operator@example.test", Role: accountRoleAdmin, AuthType: "session"}
	for _, tc := range []struct {
		name  string
		setup string
	}{
		{"live_handoff", `INSERT INTO recordings(id,account_id,status,start_at,end_at,capture_via,stream_url,storage_destination_id,preferred_relay_group_id,mode,next_fire_at) VALUES(1,47,'active',now()-interval '1 hour',now()+interval '1 hour','relay','source',1,2,'continuous',now()+interval '1 hour'); INSERT INTO recording_jobs(id,recording_id,status,scheduled_for,kind,fire_at,clip_duration_sec,attempt_count,window_end_at,handoff_owner,handoff_until) VALUES(1,1,'pending',now()+interval '1 hour','continuous_window',now(),60,0,now()+interval '2 hours','node:66',now()+interval '2 minutes')`},
		{"active_reservation", `INSERT INTO recordings(id,account_id,status,start_at,end_at,capture_via,stream_url,storage_destination_id,preferred_relay_group_id,mode,next_fire_at) VALUES(1,47,'active',now()-interval '1 hour',now()+interval '1 hour','relay','source',1,2,'continuous',now()+interval '1 hour'); INSERT INTO recording_canary_reservations(recording_id,node_id,expires_at) VALUES(1,93,now()+interval '2 minutes')`},
		{"remaining_capacity", `UPDATE nodes SET relay_max_streams=1 WHERE id=93; INSERT INTO recordings(id,account_id,status,start_at,end_at,capture_via,stream_url,storage_destination_id,preferred_relay_group_id,mode,next_fire_at) VALUES(1,47,'active',now()-interval '1 hour',now()+interval '1 hour','relay','source1',1,2,'continuous',now()+interval '1 hour'),(2,47,'active',now()-interval '1 hour',now()+interval '1 hour','relay','source2',1,2,'continuous',now()+interval '1 hour'); INSERT INTO recording_jobs(id,recording_id,status,scheduled_for,kind,fire_at,clip_duration_sec,attempt_count,window_end_at) VALUES(1,1,'pending',now()+interval '1 minute','continuous_window',now(),60,0,now()+interval '1 hour'),(2,2,'pending',now()+interval '2 minutes','continuous_window',now(),60,0,now()+interval '1 hour')`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newQuarantineFixture(t)
			defer f.cleanup()
			if _, err := f.pool.Exec(context.Background(), tc.setup); err != nil {
				t.Fatal(err)
			}
			got := callQuarantine(t, f, operator, quarantineRequestBody(f, uuid.New()))
			if got.Code != http.StatusConflict {
				t.Fatalf("status=%d body=%s", got.Code, got.Body.String())
			}
			var status string
			if err := f.pool.QueryRow(context.Background(), `SELECT status FROM nodes WHERE id=66`).Scan(&status); err != nil || status != "active" {
				t.Fatalf("node status=%s err=%v", status, err)
			}
		})
	}
}

func waitQuarantineTrace(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s account lock", label)
	}
}

func waitAccountLockWaiters(t *testing.T, pool *pgxpool.Pool, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var got int
		err := pool.QueryRow(context.Background(), `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%SELECT id FROM accounts WHERE id=$1 FOR UPDATE%'`).Scan(&got)
		if err == nil && got >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d queued account locks", want)
}

func prepareRelayClaimFixture(t *testing.T, f quarantineFixture) {
	t.Helper()
	_, err := f.pool.Exec(context.Background(), `INSERT INTO streams(id,provider,source_page_url,execution_class) VALUES(1,'native','https://example.test/source','native_hls'); INSERT INTO recordings(id,account_id,status,start_at,end_at,capture_via,stream_url,stream_id,storage_destination_id,preferred_relay_group_id,mode,next_fire_at) VALUES(1,47,'active',now()-interval '1 minute',now()+interval '1 hour','relay','https://example.test/live.m3u8',1,1,2,'continuous',now()+interval '1 hour'); INSERT INTO recording_jobs(id,recording_id,status,scheduled_for,kind,fire_at,clip_duration_sec,attempt_count,window_end_at) VALUES(1,1,'pending',now()-interval '1 second','continuous_window',now(),60,0,now()+interval '1 hour')`)
	if err != nil {
		t.Fatal(err)
	}
}

type quarantineRaceResult struct {
	response *httptest.ResponseRecorder
	lease    recordingLeaseResponse
	err      error
}

func TestRelayNodeQuarantineSerializesWithClaimInBothCommitOrders(t *testing.T) {
	for _, first := range []string{"claim", "quarantine"} {
		t.Run(first+"_first", func(t *testing.T) {
			trace := &quarantineQueryTrace{waiting: map[string]chan struct{}{"claim": make(chan struct{}, 1), "quarantine": make(chan struct{}, 1)}}
			f := newQuarantineFixtureWithTrace(t, trace)
			defer f.cleanup()
			prepareRelayClaimFixture(t, f)
			blocker, err := f.pool.Begin(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer blocker.Rollback(context.Background())
			if _, err = blocker.Exec(context.Background(), `SELECT id FROM accounts WHERE id=47 FOR UPDATE`); err != nil {
				t.Fatal(err)
			}
			results := make(chan quarantineRaceResult, 2)
			startClaim := func() {
				ctx := context.WithValue(context.Background(), quarantineTraceMarker{}, "claim")
				go func() {
					lease, leaseErr := f.server.leaseRelayRecordingJob(ctx, nodePrincipal{NodeID: 66, AccountID: 47}, true, 30, true)
					results <- quarantineRaceResult{lease: lease, err: leaseErr}
				}()
			}
			startQuarantine := func() {
				ctx := context.WithValue(context.Background(), quarantineTraceMarker{}, "quarantine")
				go func() {
					out := callQuarantineWithContext(f, ctx, accountPrincipal{UserID: 9, Email: "operator@example.test", Role: accountRoleAdmin, AuthType: "session"}, quarantineRequestBody(f, uuid.New()))
					results <- quarantineRaceResult{response: out}
				}()
			}
			if first == "claim" {
				startClaim()
				waitQuarantineTrace(t, trace.waiting["claim"], "claim")
				waitAccountLockWaiters(t, f.pool, 1)
				startQuarantine()
				waitQuarantineTrace(t, trace.waiting["quarantine"], "quarantine")
			} else {
				startQuarantine()
				waitQuarantineTrace(t, trace.waiting["quarantine"], "quarantine")
				waitAccountLockWaiters(t, f.pool, 1)
				startClaim()
				waitQuarantineTrace(t, trace.waiting["claim"], "claim")
			}
			waitAccountLockWaiters(t, f.pool, 2)
			if err = blocker.Commit(context.Background()); err != nil {
				t.Fatal(err)
			}
			var claimResult, quarantineResult quarantineRaceResult
			for range 2 {
				result := <-results
				if result.response == nil {
					claimResult = result
				} else {
					quarantineResult = result
				}
			}
			if first == "claim" {
				if claimResult.err != nil || claimResult.lease.JobID != 1 || quarantineResult.response.Code != http.StatusConflict {
					t.Fatalf("claim-first lease=%+v err=%v quarantine=%d body=%s", claimResult.lease, claimResult.err, quarantineResult.response.Code, quarantineResult.response.Body.String())
				}
			} else if !errors.Is(claimResult.err, pgx.ErrNoRows) || quarantineResult.response.Code != http.StatusOK {
				t.Fatalf("quarantine-first lease=%+v err=%v quarantine=%d body=%s", claimResult.lease, claimResult.err, quarantineResult.response.Code, quarantineResult.response.Body.String())
			}
		})
	}
}

func callCanaryStartWithContext(f quarantineFixture, ctx context.Context) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/node/recordings/1/canary/start", nil)
	rc := chi.NewRouteContext()
	rc.URLParams.Add("recording_id", "1")
	ctx = context.WithValue(ctx, chi.RouteCtxKey, rc)
	ctx = context.WithValue(ctx, nodePrincipalContextKey, nodePrincipal{NodeID: 66, AccountID: 47, NodeType: nodeTypeRelay})
	out := httptest.NewRecorder()
	f.server.handleNodeRecordingCanaryStart(out, req.WithContext(ctx))
	return out
}

func TestRelayNodeQuarantineSerializesWithCanaryInBothCommitOrders(t *testing.T) {
	for _, first := range []string{"canary", "quarantine"} {
		t.Run(first+"_first", func(t *testing.T) {
			trace := &quarantineQueryTrace{waiting: map[string]chan struct{}{"canary": make(chan struct{}, 1), "quarantine": make(chan struct{}, 1)}}
			f := newQuarantineFixtureWithTrace(t, trace)
			defer f.cleanup()
			if _, err := f.pool.Exec(context.Background(), `INSERT INTO streams(id,provider,source_page_url,execution_class) VALUES(1,'native','https://example.test/source','native_hls'); INSERT INTO recordings(id,account_id,status,start_at,end_at,capture_via,stream_url,stream_id,storage_destination_id,preferred_relay_group_id,mode,next_fire_at) VALUES(1,47,'active',now()-interval '1 minute',now()+interval '2 hours','relay','https://example.test/live.m3u8',1,1,2,'continuous',now()+interval '1 hour')`); err != nil {
				t.Fatal(err)
			}
			blocker, err := f.pool.Begin(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer blocker.Rollback(context.Background())
			if _, err = blocker.Exec(context.Background(), `SELECT id FROM accounts WHERE id=47 FOR UPDATE`); err != nil {
				t.Fatal(err)
			}
			type result struct {
				kind string
				out  *httptest.ResponseRecorder
			}
			results := make(chan result, 2)
			start := func(kind string) {
				ctx := context.WithValue(context.Background(), quarantineTraceMarker{}, kind)
				go func() {
					if kind == "canary" {
						results <- result{kind, callCanaryStartWithContext(f, ctx)}
						return
					}
					results <- result{kind, callQuarantineWithContext(f, ctx, accountPrincipal{UserID: 9, Email: "operator@example.test", Role: accountRoleAdmin, AuthType: "session"}, quarantineRequestBody(f, uuid.New()))}
				}()
			}
			start(first)
			waitQuarantineTrace(t, trace.waiting[first], first)
			waitAccountLockWaiters(t, f.pool, 1)
			second := "quarantine"
			if first == "quarantine" {
				second = "canary"
			}
			start(second)
			waitQuarantineTrace(t, trace.waiting[second], second)
			waitAccountLockWaiters(t, f.pool, 2)
			if err = blocker.Commit(context.Background()); err != nil {
				t.Fatal(err)
			}
			codes := map[string]int{}
			bodies := map[string]string{}
			for range 2 {
				got := <-results
				codes[got.kind], bodies[got.kind] = got.out.Code, got.out.Body.String()
			}
			if first == "canary" {
				if codes["canary"] != http.StatusOK || codes["quarantine"] != http.StatusConflict {
					t.Fatalf("canary-first codes=%v bodies=%v", codes, bodies)
				}
			} else if codes["quarantine"] != http.StatusOK || codes["canary"] != http.StatusNotFound {
				t.Fatalf("quarantine-first codes=%v bodies=%v", codes, bodies)
			}
		})
	}
}
