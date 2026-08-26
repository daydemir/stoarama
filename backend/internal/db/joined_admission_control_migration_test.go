package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestJoinedAdmissionControlMigrationLifecycle(t *testing.T) {
	dsn := os.Getenv("STOARAMA_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run joined admission migration regression")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	schema := fmt.Sprintf("joined_admission_%d", time.Now().UnixNano())
	if _, err = conn.Exec(ctx, `CREATE SCHEMA `+schema+`; SET search_path TO `+schema); err != nil {
		t.Fatal(err)
	}
	defer conn.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`)
	if _, err = conn.Exec(ctx, `CREATE TABLE recording_joined_batches(id BIGINT PRIMARY KEY,batch_id TEXT NOT NULL UNIQUE,
		UNIQUE(id,batch_id)); INSERT INTO recording_joined_batches VALUES(1,'batch-1')`); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "infra", "sql", "migrations", "0145_joined_claim_admission_control.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(ctx, string(raw)); err != nil {
		t.Fatalf("apply admission migration: %v", err)
	}
	var paused bool
	if err = conn.QueryRow(ctx, `SELECT claims_paused FROM recording_joined_admission_controls WHERE batch_id='batch-1'`).Scan(&paused); err != nil || !paused {
		t.Fatalf("existing batch did not fail closed: paused=%v err=%v", paused, err)
	}
	if _, err = conn.Exec(ctx, `UPDATE recording_joined_admission_controls SET claims_paused=FALSE,updated_at=clock_timestamp()
		WHERE batch_id='batch-1'`); err != nil {
		t.Fatalf("explicit resume rejected: %v", err)
	}
	if _, err = conn.Exec(ctx, `UPDATE recording_joined_admission_controls SET batch_id='batch-2',updated_at=clock_timestamp()
		WHERE batch_id='batch-1'`); err == nil {
		t.Fatal("admission identity rewrite succeeded")
	}
	if _, err = conn.Exec(ctx, `DELETE FROM recording_joined_admission_controls WHERE batch_id='batch-1'`); err == nil {
		t.Fatal("admission control deletion succeeded")
	}
	if _, err = conn.Exec(ctx, `TRUNCATE recording_joined_admission_controls`); err == nil {
		t.Fatal("admission control truncate succeeded")
	}
	if _, err = conn.Exec(ctx, `INSERT INTO recording_joined_batches VALUES(2,'batch-2')`); err != nil {
		t.Fatal(err)
	}
	if err = conn.QueryRow(ctx, `SELECT claims_paused FROM recording_joined_admission_controls WHERE batch_id='batch-2'`).Scan(&paused); err != nil || !paused {
		t.Fatalf("new batch did not fail closed: paused=%v err=%v", paused, err)
	}
}
