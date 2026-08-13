package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRelayNodeQuarantineMigrationIsPermanentAndAppendOnly(t *testing.T) {
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
	schema := fmt.Sprintf("relay_quarantine_%d", time.Now().UnixNano())
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
	_, err = pool.Exec(ctx, `CREATE TABLE accounts(id BIGINT PRIMARY KEY); CREATE TABLE users(id BIGINT PRIMARY KEY,email TEXT NOT NULL,is_operator BOOLEAN NOT NULL); CREATE TABLE relay_groups(id BIGINT PRIMARY KEY,account_id BIGINT NOT NULL,max_streams INT NOT NULL); CREATE TABLE nodes(id BIGINT PRIMARY KEY,account_id BIGINT NOT NULL,node_type TEXT NOT NULL,display_name TEXT NOT NULL,hostname TEXT NOT NULL DEFAULT '',platform TEXT NOT NULL DEFAULT '',status TEXT NOT NULL,enrolled_at TIMESTAMPTZ NOT NULL DEFAULT now(),last_heartbeat_at TIMESTAMPTZ,capabilities_jsonb JSONB NOT NULL DEFAULT '{}',metadata_jsonb JSONB NOT NULL DEFAULT '{}',created_at TIMESTAMPTZ NOT NULL DEFAULT now(),updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),relay_max_streams INT NOT NULL,survey_enabled BOOLEAN NOT NULL DEFAULT false,relay_group_id BIGINT); INSERT INTO accounts VALUES(47),(48); INSERT INTO users VALUES(9,'operator@example.test',true),(10,'member@example.test',false); INSERT INTO relay_groups VALUES(2,47,16),(3,47,16); INSERT INTO nodes(id,account_id,node_type,display_name,status,last_heartbeat_at,relay_max_streams,relay_group_id,capabilities_jsonb) VALUES(66,47,'relay','node66','disabled',now(),16,2,jsonb_build_object('relay_version','v1','relay_source_revision',$$bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb$$,'relay_started_at',now()::text)),(67,47,'cloud_recorder','cloud67','disabled',now(),16,2,jsonb_build_object('relay_version','v1','relay_source_revision',$$bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb$$,'relay_started_at',now()::text))`)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile("../../../infra/sql/migrations/0138_relay_node_quarantine.sql")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err = os.WriteFile(filepath.Join(dir, "0138_relay_node_quarantine.sql"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err = MigrateUp(ctx, pool, dir); err != nil {
		t.Fatal(err)
	}
	for name, sql := range map[string]string{
		"0136_reserved_probe.sql": `CREATE TABLE reserved_probe_0136(id int)`,
		"0137_reserved_probe.sql": `CREATE TABLE reserved_probe_0137(id int)`,
	} {
		if err = os.WriteFile(filepath.Join(dir, name), []byte(sql), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err = MigrateUp(ctx, pool, dir); err != nil {
		t.Fatalf("lower reserved migrations arriving after 0138: %v", err)
	}
	var applied int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM schema_migrations WHERE version LIKE '0136_%' OR version LIKE '0137_%' OR version LIKE '0138_%'`).Scan(&applied); err != nil || applied != 3 {
		t.Fatalf("reserved sequence applied=%d err=%v", applied, err)
	}
	baseInsert := `INSERT INTO relay_node_quarantine_events(id,idempotency_key,request_digest,account_id,node_id,relay_group_id,relay_max_streams,display_name,relay_version,relay_source_revision,relay_started_at,heartbeat_at,reason_code,evidence_sha256,operator_user_id,operator_email,response_bytes) SELECT gen_random_uuid(),gen_random_uuid(),$1,$3,id,$4,16,'node66','v1',$2,(capabilities_jsonb->>'relay_started_at')::timestamptz,last_heartbeat_at,'tls_input_incompatibility',$1,$5,'operator@example.test','{}' FROM nodes WHERE id=$6`
	for _, tc := range []struct {
		name                  string
		account, group, actor int64
		node                  int64
	}{
		{"wrong account", 48, 2, 9, 66},
		{"wrong group", 47, 3, 9, 66},
		{"nonoperator", 47, 2, 10, 66},
		{"nonrelay node", 47, 2, 9, 67},
	} {
		if _, err = pool.Exec(ctx, baseInsert, strings.Repeat("a", 64), strings.Repeat("b", 40), tc.account, tc.group, tc.actor, tc.node); err == nil {
			t.Fatalf("%s event accepted", tc.name)
		}
	}
	if _, err = pool.Exec(ctx, strings.Replace(baseInsert, "'operator@example.test'", "'forged@example.test'", 1), strings.Repeat("a", 64), strings.Repeat("b", 40), 47, 2, 9, 66); err == nil {
		t.Fatal("forged operator email accepted")
	}
	_, err = pool.Exec(ctx, `INSERT INTO relay_node_quarantine_events(id,idempotency_key,request_digest,account_id,node_id,relay_group_id,relay_max_streams,display_name,relay_version,relay_source_revision,relay_started_at,heartbeat_at,reason_code,evidence_sha256,operator_user_id,operator_email,response_bytes) SELECT gen_random_uuid(),gen_random_uuid(),$1,47,id,2,16,'node66','v1',$2,(capabilities_jsonb->>'relay_started_at')::timestamptz,last_heartbeat_at,'tls_input_incompatibility',$1,9,'operator@example.test','{}' FROM nodes WHERE id=66`, strings.Repeat("a", 64), strings.Repeat("b", 40))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE nodes SET last_heartbeat_at=now(),capabilities_jsonb='{"ok":true}',metadata_jsonb='{"m":1}',updated_at=now() WHERE id=66`); err != nil {
		t.Fatalf("heartbeat update rejected: %v", err)
	}
	for _, q := range []string{`UPDATE nodes SET status='active' WHERE id=66`, `UPDATE nodes SET relay_group_id=NULL WHERE id=66`, `UPDATE nodes SET relay_max_streams=15 WHERE id=66`, `UPDATE nodes SET display_name='renamed' WHERE id=66`, `DELETE FROM nodes WHERE id=66`, `TRUNCATE nodes`, `UPDATE relay_node_quarantine_events SET reason_code='repeated_no_progress'`, `DELETE FROM relay_node_quarantine_events`, `TRUNCATE relay_node_quarantine_events`} {
		if _, err = pool.Exec(ctx, q); err == nil {
			t.Fatalf("mutation succeeded: %s", q)
		}
	}
}

func TestMigrationNumbersAreUniqueIncludingReservedRelaySequence(t *testing.T) {
	entries, err := os.ReadDir("../../../infra/sql/migrations")
	if err != nil {
		t.Fatal(err)
	}
	var allocations []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		if strings.HasPrefix(e.Name(), "0138_") {
			allocations = append(allocations, e.Name())
		}
	}
	if len(allocations) != 1 || allocations[0] != "0138_relay_node_quarantine.sql" {
		t.Fatalf("0138 allocations=%v", allocations)
	}
}
