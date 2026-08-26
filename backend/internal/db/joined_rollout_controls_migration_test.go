package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestJoinedRolloutMigrationTerminalizesExpiredExhaustedLeases(t *testing.T) {
	dsn := os.Getenv("STOARAMA_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run joined rollout migration regression")
	}
	ctx := context.Background()
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	schema := fmt.Sprintf("joined_rollout_%d", time.Now().UnixNano())
	if _, err = conn.Exec(ctx, `CREATE SCHEMA `+schema+`; SET search_path TO `+schema); err != nil {
		t.Fatal(err)
	}
	defer conn.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`)
	setup := `
CREATE TABLE recording_joined_batches(id BIGINT PRIMARY KEY);
CREATE TABLE recording_joined_hours(id BIGINT PRIMARY KEY,batch_record_id BIGINT REFERENCES recording_joined_batches(id),batch_id TEXT,hour_id TEXT,
 state TEXT,attempt_count INTEGER,claim_token UUID,claimed_by TEXT,lease_expires_at TIMESTAMPTZ,heartbeat_at TIMESTAMPTZ,
 failure_reason_code TEXT DEFAULT '',updated_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE recording_joined_artifacts(id BIGINT PRIMARY KEY,batch_record_id BIGINT REFERENCES recording_joined_batches(id),batch_id TEXT,
 scope_kind TEXT,scope_id TEXT,artifact_kind TEXT,publication_state TEXT,publication_attempt_count INTEGER,publication_token UUID,
 publication_claimed_by TEXT,publication_lease_expires_at TIMESTAMPTZ,publication_heartbeat_at TIMESTAMPTZ,
 failure_reason_code TEXT DEFAULT '',updated_at TIMESTAMPTZ DEFAULT now());
CREATE FUNCTION reject_recording_joined_mutation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'immutable'; END $$;
CREATE FUNCTION guard_recording_joined_hour_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE FUNCTION guard_recording_joined_artifact_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE TRIGGER recording_joined_hour_update_guard BEFORE UPDATE ON recording_joined_hours FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_hour_update();
CREATE TRIGGER recording_joined_artifact_update_guard BEFORE UPDATE ON recording_joined_artifacts FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_artifact_update();`
	if _, err = conn.Exec(ctx, setup); err != nil {
		t.Fatal(err)
	}
	migration, err := os.ReadFile(filepath.Join("..", "..", "..", "infra", "sql", "migrations", "0144_joined_rollout_admission_failures.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(ctx, string(migration)); err != nil {
		t.Fatalf("apply rollout migration: %v", err)
	}
	hourToken, artifactToken := uuid.New(), uuid.New()
	if _, err = conn.Exec(ctx, `INSERT INTO recording_joined_batches VALUES(1);
INSERT INTO recording_joined_hours(id,batch_record_id,batch_id,hour_id,state,attempt_count,claim_token,claimed_by,lease_expires_at,heartbeat_at)
 VALUES(1,1,'batch-1','hour-1','leased',8,$1,'worker',now()-interval '1 minute',now()-interval '2 minutes');
INSERT INTO recording_joined_artifacts(id,batch_record_id,batch_id,scope_kind,scope_id,artifact_kind,publication_state,
 publication_attempt_count,publication_token,publication_claimed_by,publication_lease_expires_at,publication_heartbeat_at)
 VALUES(2,1,'batch-1','hour','hour-1','hour_manifest','publishing',8,$2,'worker',now()-interval '1 minute',now()-interval '2 minutes')`, hourToken, artifactToken); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(ctx, `INSERT INTO recording_joined_worker_failures(batch_record_id,hour_record_id,batch_id,scope_kind,scope_id,
 claim_token,attempt_count,failure_class,reason_code,disposition) VALUES(1,1,'batch-1','hour','hour-1',$1,8,'transient','worker_lease_expired','terminal');
UPDATE recording_joined_hours SET state='terminal_failed',claim_token=NULL,claimed_by=NULL,lease_expires_at=NULL,heartbeat_at=NULL,
 failure_reason_code='worker_lease_expired' WHERE id=1;
INSERT INTO recording_joined_worker_failures(batch_record_id,artifact_id,batch_id,scope_kind,scope_id,
 claim_token,attempt_count,failure_class,reason_code,disposition) VALUES(1,2,'batch-1','hour','hour-1',$2,8,'transient','worker_lease_expired','terminal');
UPDATE recording_joined_artifacts SET publication_state='terminal_failed',publication_token=NULL,publication_claimed_by=NULL,
 publication_lease_expires_at=NULL,publication_heartbeat_at=NULL,failure_reason_code='worker_lease_expired' WHERE id=2`, hourToken, artifactToken); err != nil {
		t.Fatalf("terminalize exhausted leases: %v", err)
	}
	var hourState, artifactState, hourReason, artifactReason string
	var hourClaim, artifactClaim *uuid.UUID
	if err = conn.QueryRow(ctx, `SELECT h.state,h.failure_reason_code,h.claim_token,a.publication_state,a.failure_reason_code,a.publication_token
 FROM recording_joined_hours h CROSS JOIN recording_joined_artifacts a WHERE h.id=1 AND a.id=2`).Scan(
		&hourState, &hourReason, &hourClaim, &artifactState, &artifactReason, &artifactClaim); err != nil {
		t.Fatal(err)
	}
	if hourState != "terminal_failed" || artifactState != "terminal_failed" || hourReason != "worker_lease_expired" ||
		artifactReason != "worker_lease_expired" || hourClaim != nil || artifactClaim != nil {
		t.Fatalf("hour=%s/%s/%v artifact=%s/%s/%v", hourState, hourReason, hourClaim, artifactState, artifactReason, artifactClaim)
	}
}
