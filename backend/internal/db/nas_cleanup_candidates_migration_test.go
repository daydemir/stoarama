package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestNASCleanupCandidateMigrationFreezesQualificationEvidence(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	schema := fmt.Sprintf("nas_cleanup_%d", time.Now().UnixNano())
	if _, err = conn.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), `SET search_path TO public`)
		_, _ = conn.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
	}()
	if _, err = conn.Exec(ctx, `SET search_path TO `+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(ctx, `CREATE TABLE accounts(id BIGINT PRIMARY KEY); CREATE TABLE connections(id BIGINT PRIMARY KEY); CREATE TABLE users(id BIGINT PRIMARY KEY); CREATE TABLE storage_destinations(id BIGINT PRIMARY KEY); CREATE TABLE recordings(id BIGINT PRIMARY KEY); CREATE TABLE recording_clips(id BIGINT PRIMARY KEY); INSERT INTO accounts VALUES(1); INSERT INTO connections VALUES(2); INSERT INTO users VALUES(3); INSERT INTO storage_destinations VALUES(4); INSERT INTO recordings VALUES(5); INSERT INTO recording_clips VALUES(6);`); err != nil {
		t.Fatal(err)
	}
	migration, err := os.ReadFile(filepath.Join("..", "..", "..", "infra", "sql", "migrations", "0128_nas_cleanup_candidates.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(ctx, string(migration)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}
	const runID = "11111111-1111-1111-1111-111111111111"
	if _, err = conn.Exec(ctx, `INSERT INTO nas_cleanup_candidate_runs(id,account_id,connection_id,recording_ids,inventory_generation,inventory_digest,inventory_started_at,inventory_completed_at,item_count,total_bytes,unknown_count,request_digest,created_by_user_id) VALUES($1,1,2,ARRAY[5],'g',repeat('a',64),now()-interval '2 minutes',now()-interval '1 minute',1,7,1,repeat('b',64),3)`, runID); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(ctx, `INSERT INTO r2_content_verifications(storage_destination_id,endpoint_snapshot,bucket,object_key,expected_size_bytes,expected_sha256) VALUES(4,'https://example.invalid','bucket','key',7,repeat('c',64))`); err != nil {
		t.Fatal(err)
	}
	var verificationID int64
	if err = conn.QueryRow(ctx, `SELECT id FROM r2_content_verifications`).Scan(&verificationID); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(ctx, `INSERT INTO nas_cleanup_candidate_items(run_id,ordinal,clip_id,recording_id,window_start,window_end,relative_path,size_bytes,content_sha256,inventory_verified_at,file_mtime_ns,file_ctime_ns,file_inode,file_device,sidecar_relative_path,sidecar_size_bytes,sidecar_sha256,storage_destination_id,recovery_bucket,recovery_object_key,recovery_etag,verification_id) VALUES($1,1,6,5,now()-interval '2 minutes',now()-interval '1 minute','r/c.mp4',7,repeat('c',64),now(),1,2,3,4,'r/c.json',5,repeat('d',64),4,'bucket','key','etag',$2)`, runID, verificationID); err != nil {
		t.Fatal(err)
	}
	reject := func(name, sql string, args ...any) {
		t.Helper()
		if _, e := conn.Exec(ctx, sql, args...); e == nil {
			t.Fatalf("%s unexpectedly succeeded", name)
		}
	}
	reject("mutate expected identity", `UPDATE r2_content_verifications SET object_key='other' WHERE id=$1`, verificationID)
	reject("delete verification", `DELETE FROM r2_content_verifications WHERE id=$1`, verificationID)
	reject("delete item", `DELETE FROM nas_cleanup_candidate_items WHERE run_id=$1`, runID)
	reject("digest before ready", `UPDATE nas_cleanup_candidate_runs SET final_digest=repeat('e',64) WHERE id=$1`, runID)
	if _, err = conn.Exec(ctx, `UPDATE nas_cleanup_candidate_runs SET state='ready',final_digest=repeat('e',64) WHERE id=$1`, runID); err != nil {
		t.Fatalf("finalize ready: %v", err)
	}
	reject("mutate final digest", `UPDATE nas_cleanup_candidate_runs SET final_digest=repeat('f',64) WHERE id=$1`, runID)
	reject("delete run", `DELETE FROM nas_cleanup_candidate_runs WHERE id=$1`, runID)
}
