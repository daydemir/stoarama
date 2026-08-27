package db

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestJoinedGapOnlyScopeAuthorizationMigrationLifecycle(t *testing.T) {
	dsn := os.Getenv("STOARAMA_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run joined gap authorization migration regression")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	schema := fmt.Sprintf("joined_gap_auth_%d", time.Now().UnixNano())
	if _, err = conn.Exec(ctx, `CREATE SCHEMA `+schema+`; SET search_path TO `+schema); err != nil {
		t.Fatal(err)
	}
	defer conn.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`)
	if _, err = conn.Exec(ctx, `
		CREATE TABLE recording_joined_batches(id BIGINT PRIMARY KEY,batch_id TEXT NOT NULL UNIQUE);
		CREATE TABLE recording_joined_hours(id BIGINT PRIMARY KEY,batch_record_id BIGINT NOT NULL,batch_id TEXT NOT NULL,
		 hour_id TEXT NOT NULL,source_clip_count INTEGER NOT NULL,state TEXT NOT NULL);
		CREATE TABLE recording_joined_artifacts(id BIGINT PRIMARY KEY,batch_record_id BIGINT NOT NULL,batch_id TEXT NOT NULL,
		 hour_record_id BIGINT,scope_kind TEXT NOT NULL,scope_id TEXT NOT NULL,artifact_kind TEXT NOT NULL,
		 relative_path TEXT NOT NULL,object_key TEXT NOT NULL,expected_size_bytes BIGINT NOT NULL,expected_sha256 TEXT NOT NULL,
		 canonical_bytes BYTEA,publication_state TEXT NOT NULL DEFAULT 'sealed',publication_token UUID,etag TEXT,version_id TEXT);
		CREATE TABLE recording_joined_batch_index_refs(index_artifact_id BIGINT NOT NULL,referenced_artifact_id BIGINT NOT NULL,
		 reference_kind TEXT NOT NULL);
		INSERT INTO recording_joined_batches VALUES(1,'batch-generation-1');
		INSERT INTO recording_joined_hours VALUES(10,1,'batch-generation-1','hour-1',0,'sealed'),
		 (11,1,'batch-generation-1','hour-2',1,'sealed');
		INSERT INTO recording_joined_artifacts VALUES
		 (20,1,'batch-generation-1',10,'hour','hour-1','hour_manifest','hours/1.json','joined/hours/1.json',21,
		  '1a5f3ed7f0b7cc8d9f7f28f50f94a2e33ec8c5d45441b0ec8868889b3dc08225',convert_to('{"status":"gap_only"}','UTF8'),'sealed',NULL,NULL,NULL),
		 (21,1,'batch-generation-1',11,'hour','hour-2','hour_manifest','hours/2.json','joined/hours/2.json',18,
		  '559386dc215c8f883e632200e17eff009029761264ba109b7ea01d939e094a79',convert_to('{"status":"media"}','UTF8'),'sealed',NULL,NULL,NULL)`); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "infra", "sql", "migrations", "0146_joined_gap_only_scope_authorizations.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(ctx, string(raw)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}
	var count int
	if err = conn.QueryRow(ctx, `SELECT count(*) FROM recording_joined_gap_only_scope_authorizations`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("migration backfilled legacy authorization count=%d err=%v", count, err)
	}
	scopeBytes := []byte(`{"work_scope":"frozen_batch"}`)
	sha := "97561f738609af573500e390a602ac5a4312362f63909682b4300ca514ebc2e1"
	if _, err = conn.Exec(ctx, `UPDATE recording_joined_artifacts SET publication_state='publishing',publication_token=gen_random_uuid()
		WHERE id=20`); err == nil {
		t.Fatal("unauthorized gap-only publication transition succeeded")
	}
	if _, err = conn.Exec(ctx, `INSERT INTO recording_joined_gap_only_scope_authorizations
		(artifact_id,batch_record_id,batch_id,hour_record_id,hour_id,work_scope,work_scope_identity_sha256,work_scope_identity_bytes,
		 authorization_source,request_sha256,relative_path,object_key,expected_size_bytes,expected_sha256,review_evidence_sha256,incident_id,
		 verification_policy_version,verified_publication_state)
		 VALUES(20,1,'batch-generation-1',10,'hour-1','frozen_batch',$1,$2,'operator_frozen',$1,'hours/1.json','joined/hours/1.json',21,
		 '1a5f3ed7f0b7cc8d9f7f28f50f94a2e33ec8c5d45441b0ec8868889b3dc08225',$1,'migration-test','joined-gap-authorization-v1','sealed')`,
		strings.Repeat("c", 64), scopeBytes); err == nil {
		t.Fatal("forged scope digest succeeded")
	}
	canaryBytes := []byte(`{"work_scope":"canary_single","canary_hour_ids":["some-other-hour"],"canary_hour_ids_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`)
	canaryDigest := fmt.Sprintf("%x", sha256.Sum256(canaryBytes))
	if _, err = conn.Exec(ctx, `INSERT INTO recording_joined_gap_only_scope_authorizations
		(artifact_id,batch_record_id,batch_id,hour_record_id,hour_id,work_scope,work_scope_identity_sha256,work_scope_identity_bytes,
		 canary_hour_ids_sha256,authorization_source,request_sha256,relative_path,object_key,expected_size_bytes,expected_sha256,
		 review_evidence_sha256,verification_policy_version,verified_publication_state)
		 VALUES(20,1,'batch-generation-1',10,'hour-1','canary_single',$1,$2,
		 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','server_seal',$1,'hours/1.json','joined/hours/1.json',21,
		 '1a5f3ed7f0b7cc8d9f7f28f50f94a2e33ec8c5d45441b0ec8868889b3dc08225',$1,'joined-gap-authorization-v1','sealed')`, canaryDigest, canaryBytes); err == nil {
		t.Fatal("out-of-scope canary hour authorization succeeded")
	}
	if _, err = conn.Exec(ctx, `INSERT INTO recording_joined_gap_only_scope_authorizations
		(artifact_id,batch_record_id,batch_id,hour_record_id,hour_id,work_scope,work_scope_identity_sha256,work_scope_identity_bytes,
		 authorization_source,request_sha256,relative_path,object_key,expected_size_bytes,expected_sha256,review_evidence_sha256,incident_id,
		 verification_policy_version,verified_publication_state)
		 VALUES(20,1,'batch-generation-1',10,'hour-1','frozen_batch',$1,$2,'operator_frozen',$1,'hours/1.json','joined/hours/1.json',21,
		 '1a5f3ed7f0b7cc8d9f7f28f50f94a2e33ec8c5d45441b0ec8868889b3dc08225',$1,'migration-test','joined-gap-authorization-v1','sealed')`, sha, scopeBytes); err != nil {
		t.Fatalf("exact authorization rejected: %v", err)
	}
	if _, err = conn.Exec(ctx, `UPDATE recording_joined_artifacts SET publication_state='publishing',publication_token=gen_random_uuid()
		WHERE id=20`); err != nil {
		t.Fatalf("authorized gap-only publication transition rejected: %v", err)
	}
	if _, err = conn.Exec(ctx, `INSERT INTO recording_joined_batch_index_refs VALUES(99,20,'hour_manifest')`); err != nil {
		t.Fatalf("authorized frozen gap-only index reference rejected: %v", err)
	}
	if _, err = conn.Exec(ctx, `INSERT INTO recording_joined_gap_only_scope_authorizations
		(artifact_id,batch_record_id,batch_id,hour_record_id,hour_id,work_scope,work_scope_identity_sha256,work_scope_identity_bytes,
		 authorization_source,request_sha256,relative_path,object_key,expected_size_bytes,expected_sha256,review_evidence_sha256,incident_id,
		 verification_policy_version,verified_publication_state)
		 VALUES(21,1,'batch-generation-1',11,'hour-2','frozen_batch',$1,$2,'operator_frozen',$1,'hours/2.json','joined/hours/2.json',18,
		 '559386dc215c8f883e632200e17eff009029761264ba109b7ea01d939e094a79',$1,'migration-test','joined-gap-authorization-v1','sealed')`, sha, scopeBytes); err == nil {
		t.Fatal("non-gap authorization succeeded")
	}
	if _, err = conn.Exec(ctx, `UPDATE recording_joined_gap_only_scope_authorizations SET request_sha256=$1`, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"); err == nil {
		t.Fatal("authorization update succeeded")
	}
	if _, err = conn.Exec(ctx, `DELETE FROM recording_joined_gap_only_scope_authorizations`); err == nil {
		t.Fatal("authorization delete succeeded")
	}
	if _, err = conn.Exec(ctx, `TRUNCATE recording_joined_gap_only_scope_authorizations`); err == nil {
		t.Fatal("authorization truncate succeeded")
	}
}
