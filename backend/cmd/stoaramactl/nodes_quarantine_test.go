package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/config"
)

func quarantineTestHash(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func TestNodesQuarantineCLIUsesTypedAdminAPIKeyAcrossTenants(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("relay_quarantine_cli_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams = map[string]string{"search_path": schema}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ddl := `CREATE TABLE accounts(id BIGINT PRIMARY KEY,email TEXT NOT NULL,name TEXT NOT NULL,role TEXT NOT NULL,status TEXT NOT NULL); CREATE TABLE users(id BIGINT PRIMARY KEY,email TEXT NOT NULL,is_operator BOOLEAN NOT NULL); CREATE TABLE account_api_keys(id BIGINT PRIMARY KEY,account_id BIGINT NOT NULL,key_prefix TEXT NOT NULL,secret_hash TEXT NOT NULL,scopes TEXT[] NOT NULL DEFAULT '{}',expires_at TIMESTAMPTZ,last_used_at TIMESTAMPTZ,revoked_at TIMESTAMPTZ); CREATE TABLE relay_groups(id BIGINT PRIMARY KEY,account_id BIGINT NOT NULL,max_streams INT NOT NULL,bandwidth_capacity_bps BIGINT); CREATE TABLE nodes(id BIGINT PRIMARY KEY,account_id BIGINT NOT NULL,node_type TEXT NOT NULL,display_name TEXT NOT NULL,status TEXT NOT NULL,last_heartbeat_at TIMESTAMPTZ,capabilities_jsonb JSONB NOT NULL DEFAULT '{}',metadata_jsonb JSONB NOT NULL DEFAULT '{}',updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),relay_max_streams INT NOT NULL,relay_group_id BIGINT); CREATE TABLE recordings(id BIGINT PRIMARY KEY,account_id BIGINT NOT NULL,status TEXT NOT NULL,capture_via TEXT NOT NULL,preferred_relay_group_id BIGINT); CREATE TABLE recording_jobs(id BIGINT PRIMARY KEY,recording_id BIGINT NOT NULL,status TEXT NOT NULL,scheduled_for TIMESTAMPTZ NOT NULL,kind TEXT NOT NULL,window_end_at TIMESTAMPTZ,lease_owner TEXT,lease_expires_at TIMESTAMPTZ,handoff_owner TEXT,handoff_until TIMESTAMPTZ); CREATE TABLE recording_canary_reservations(id UUID PRIMARY KEY,recording_id BIGINT NOT NULL,node_id BIGINT NOT NULL,expires_at TIMESTAMPTZ NOT NULL); CREATE TABLE account_auth_events(id BIGSERIAL PRIMARY KEY,account_id BIGINT,api_key_id BIGINT,event_type TEXT NOT NULL,actor_type TEXT NOT NULL,actor_ref TEXT NOT NULL,detail_jsonb JSONB NOT NULL,created_at TIMESTAMPTZ NOT NULL DEFAULT now());`
	if _, err = pool.Exec(ctx, ddl); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "infra", "sql", "migrations", "0138_relay_node_quarantine.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(raw)); err != nil {
		t.Fatal(err)
	}
	started := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	heartbeat := time.Now().UTC().Truncate(time.Microsecond)
	adminToken, memberToken, revokedToken := "admin-token", "member-token", "revoked-token"
	_, err = pool.Exec(ctx, `INSERT INTO accounts VALUES(1,'operator@example.test','operator','admin','active'),(47,'target@example.test','target','member','active'); INSERT INTO users VALUES(9,'operator@example.test',true); INSERT INTO account_api_keys VALUES(91,1,'admin-prefix',$1,'{}',NULL,NULL,NULL),(92,47,'member-prefix',$2,'{}',NULL,NULL,NULL),(93,1,'revoked-prefix',$3,'{}',NULL,NULL,now()); INSERT INTO relay_groups VALUES(2,47,16,60000000); INSERT INTO nodes VALUES(66,47,'relay','node66','active',$4,jsonb_build_object('relay_version','b428cf9c','relay_source_revision',$5,'relay_started_at',$6),'{}',now(),16,2),(93,47,'relay','node93','active',$4,'{}','{}',now(),16,2)`, quarantineTestHash(adminToken), quarantineTestHash(memberToken), quarantineTestHash(revokedToken), heartbeat, strings.Repeat("b", 40), started.Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	router, err := api.NewRouter(config.Config{}, pool, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(router)
	defer server.Close()
	requestBody := map[string]any{"idempotency_key": uuid.New().String(), "expected_account_id": 47, "expected_relay_group_id": 2, "expected_relay_max_streams": 16, "expected_status": "active", "expected_relay_version": "b428cf9c", "expected_source_revision": strings.Repeat("b", 40), "expected_relay_started_at": started.Format(time.RFC3339Nano), "expected_heartbeat_at": heartbeat.Format(time.RFC3339Nano), "reason_code": "tls_input_incompatibility", "evidence_sha256": strings.Repeat("a", 64)}
	for _, tc := range []struct {
		name, token string
		want        int
	}{{"member", memberToken, http.StatusForbidden}, {"revoked", revokedToken, http.StatusUnauthorized}} {
		t.Run(tc.name, func(t *testing.T) {
			body, _ := json.Marshal(requestBody)
			req, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/admin/relay-nodes/66/quarantine", bytes.NewReader(body))
			req.Header.Set("Authorization", "Bearer "+tc.token)
			req.Header.Set("Content-Type", "application/json")
			resp, requestErr := http.DefaultClient.Do(req)
			if requestErr != nil {
				t.Fatal(requestErr)
			}
			resp.Body.Close()
			if resp.StatusCode != tc.want {
				t.Fatalf("status=%d want=%d", resp.StatusCode, tc.want)
			}
		})
	}
	runNodesQuarantine(ctx, config.Config{}, []string{"--backend-api-url", server.URL, "--api-token", adminToken, "--node-id", "66", "--expected-account-id", "47", "--expected-relay-group-id", "2", "--expected-relay-max-streams", "16", "--expected-relay-version", "b428cf9c", "--expected-source-revision", strings.Repeat("b", 40), "--expected-relay-started-at", started.Format(time.RFC3339Nano), "--expected-heartbeat-at", heartbeat.Format(time.RFC3339Nano), "--reason-code", "tls_input_incompatibility", "--evidence-sha256", strings.Repeat("a", 64), "--idempotency-key", uuid.New().String()})
	var status, actor string
	var keyID int64
	if err := pool.QueryRow(ctx, `SELECT n.status,q.actor_kind,q.operator_api_key_id FROM nodes n JOIN relay_node_quarantine_events q ON q.node_id=n.id WHERE n.id=66`).Scan(&status, &actor, &keyID); err != nil || status != "disabled" || actor != "operator_api_key" || keyID != 91 {
		t.Fatalf("status=%s actor=%s key=%d err=%v", status, actor, keyID, err)
	}
}
