package api

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// prepaidTestPool spins up an isolated Postgres schema and hand-creates just the
// tables the prepay ledger tests touch (accounts + prepaid_storage_batches, mirroring
// migration 0074), matching the self-contained-schema convention used by the account
// clips regression test (which avoids replaying all migrations). Skips unless
// STOARAMA_TEST_DATABASE_URL is set.
func prepaidTestPool(t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()

	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run DB-backed prepaid ledger tests")
	}

	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open admin pool: %v", err)
	}

	schema := fmt.Sprintf("api_prepaid_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schema)); err != nil {
		admin.Close()
		t.Fatalf("create schema: %v", err)
	}

	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		_, _ = admin.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
		admin.Close()
		t.Fatalf("parse db url: %v", err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		_, _ = admin.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
		admin.Close()
		t.Fatalf("open test pool: %v", err)
	}

	// The prepaid_storage_batches DDL is copied verbatim from migration 0074 (the
	// set_updated_at trigger function is created locally so the trigger attaches).
	for _, stmt := range []string{
		`CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
		 BEGIN NEW.updated_at = now(); RETURN NEW; END; $$ LANGUAGE plpgsql`,
		`CREATE TABLE accounts (
			id BIGSERIAL PRIMARY KEY,
			email TEXT NOT NULL UNIQUE
		)`,
		`CREATE TABLE prepaid_storage_batches (
			id                     BIGSERIAL PRIMARY KEY,
			batch_key              TEXT NOT NULL UNIQUE,
			account_id             BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
			recording_id           BIGINT,
			stream_hours           DOUBLE PRECISION NOT NULL,
			charged_cents          BIGINT NOT NULL,
			stripe_invoice_id      TEXT,
			stripe_invoice_item_id TEXT,
			stripe_credit_grant_id TEXT,
			status                 TEXT NOT NULL DEFAULT 'pending'
			                         CHECK (status IN ('pending', 'charged', 'granted', 'failed')),
			charged_at             TIMESTAMPTZ,
			granted_at             TIMESTAMPTZ,
			expires_at             TIMESTAMPTZ,
			created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at             TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE TRIGGER trg_prepaid_storage_batches_updated_at BEFORE UPDATE ON prepaid_storage_batches
		 FOR EACH ROW EXECUTE FUNCTION set_updated_at()`,
	} {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			pool.Close()
			_, _ = admin.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
			admin.Close()
			t.Fatalf("create test table: %v", err)
		}
	}

	cleanup := func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
		admin.Close()
	}
	return pool, cleanup
}

// TestPrepaidBatchKeyNoDoubleCharge locks the no-double-charge DB discipline: the
// prepaid_storage_batches.batch_key UNIQUE constraint plus INSERT ... ON CONFLICT
// (batch_key) DO NOTHING means a second charge attempt under the same key inserts no
// row (RowsAffected==0), so the charge helper returns without calling Stripe again.
// This is the DB half of the #26 no-double-bill guarantee (the Stripe idempotency key
// is the other half).
func TestPrepaidBatchKeyNoDoubleCharge(t *testing.T) {
	pool, cleanup := prepaidTestPool(t)
	defer cleanup()
	ctx := context.Background()

	acctID := seedPrepaidAccount(t, pool)

	const batchKey = "prepay:acct-1:2026-07"
	insert := func() int64 {
		ct, err := pool.Exec(ctx, `
			INSERT INTO prepaid_storage_batches
				(batch_key, account_id, recording_id, stream_hours, charged_cents, status)
			VALUES ($1,$2,NULL,$3,$4,'pending')
			ON CONFLICT (batch_key) DO NOTHING
		`, batchKey, acctID, 10.0, int64(600))
		if err != nil {
			t.Fatalf("insert batch: %v", err)
		}
		return ct.RowsAffected()
	}

	if n := insert(); n != 1 {
		t.Fatalf("first insert affected %d rows, want 1", n)
	}
	if n := insert(); n != 0 {
		t.Fatalf("second insert under same batch_key affected %d rows, want 0 (no double-charge)", n)
	}

	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM prepaid_storage_batches WHERE batch_key=$1`, batchKey).Scan(&count); err != nil {
		t.Fatalf("count batches: %v", err)
	}
	if count != 1 {
		t.Fatalf("found %d ledger rows for batch_key, want exactly 1", count)
	}
}

// TestPrepaidLedgerStatusTransition exercises the ledger lifecycle the same way the
// metering pass + invoice.paid webhook do: pending -> charged -> granted, and the
// redelivery no-op (UPDATE ... WHERE status='charged' affects 0 rows once the row is
// already 'granted'). No Stripe: this pins the SQL gates the code relies on.
func TestPrepaidLedgerStatusTransition(t *testing.T) {
	pool, cleanup := prepaidTestPool(t)
	defer cleanup()
	ctx := context.Background()

	acctID := seedPrepaidAccount(t, pool)
	const batchKey = "prepay:acct-1:2026-08"

	if _, err := pool.Exec(ctx, `
		INSERT INTO prepaid_storage_batches (batch_key, account_id, stream_hours, charged_cents, status)
		VALUES ($1,$2,$3,$4,'pending')
	`, batchKey, acctID, 5.0, int64(300)); err != nil {
		t.Fatalf("insert pending: %v", err)
	}

	// pending -> charged (metering pass after ChargePrepaidBatch).
	if _, err := pool.Exec(ctx, `
		UPDATE prepaid_storage_batches
		SET status='charged', stripe_invoice_id='in_test', stripe_invoice_item_id='ii_test', charged_at=now()
		WHERE batch_key=$1
	`, batchKey); err != nil {
		t.Fatalf("charge transition: %v", err)
	}

	// charged -> granted (invoice.paid webhook after CreatePrepaidCreditGrant). The
	// guard WHERE status='charged' is what makes a redelivery a no-op.
	grantOnce := func() int64 {
		ct, err := pool.Exec(ctx, `
			UPDATE prepaid_storage_batches
			SET status='granted', stripe_credit_grant_id='credgr_test', granted_at=now(), expires_at=now() + interval '12 months'
			WHERE batch_key=$1 AND status='charged'
		`, batchKey)
		if err != nil {
			t.Fatalf("grant transition: %v", err)
		}
		return ct.RowsAffected()
	}

	if n := grantOnce(); n != 1 {
		t.Fatalf("first grant transition affected %d rows, want 1", n)
	}
	// Redelivered invoice.paid: already granted -> 0 rows -> no second credit grant.
	if n := grantOnce(); n != 0 {
		t.Fatalf("redelivered grant transition affected %d rows, want 0 (redelivery no-op)", n)
	}

	var status, grantID string
	if err := pool.QueryRow(ctx, `SELECT status, stripe_credit_grant_id FROM prepaid_storage_batches WHERE batch_key=$1`, batchKey).Scan(&status, &grantID); err != nil {
		t.Fatalf("read final: %v", err)
	}
	if status != "granted" || grantID != "credgr_test" {
		t.Fatalf("final state status=%q grant=%q, want granted/credgr_test", status, grantID)
	}
}

// seedPrepaidAccount inserts a minimal accounts row and returns its id.
func seedPrepaidAccount(t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO accounts (email) VALUES ($1) RETURNING id
	`, "prepay@example.com").Scan(&id); err != nil {
		t.Fatalf("seed account: %v", err)
	}
	return id
}
