package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	transientLedgerRetention  = 30 * 24 * time.Hour
	transientLedgerBatchSize  = 5000
	transientLedgerMaxBatches = 5
)

type transientLedgerMaintenanceResult struct {
	UploadIntentsDeleted   int64
	IdempotencyKeysDeleted int64
}

const deleteExpiredUploadIntentsSQL = `
WITH doomed AS (
  SELECT ctid
  FROM upload_intents
  WHERE created_at < $1
    AND (status IN ('consumed', 'expired') OR expires_at < $1)
  ORDER BY created_at
  LIMIT $2
)
DELETE FROM upload_intents u
USING doomed d
WHERE u.ctid = d.ctid
`

const deleteExpiredIdempotencyKeysSQL = `
WITH doomed AS (
  SELECT ctid
  FROM api_idempotency
  WHERE created_at < $1
  ORDER BY created_at
  LIMIT $2
)
DELETE FROM api_idempotency i
USING doomed d
WHERE i.ctid = d.ctid
`

// maintainTransientLedgers removes only bounded batches of retry bookkeeping.
// It deliberately leaves recording clips, capture segments, media objects, and
// every other historical record untouched. Each batch has short lock/statement
// timeouts so maintenance yields to clip delivery instead of affecting capture.
func maintainTransientLedgers(ctx context.Context, pool *pgxpool.Pool, now time.Time) (transientLedgerMaintenanceResult, error) {
	cutoff := now.UTC().Add(-transientLedgerRetention)
	var out transientLedgerMaintenanceResult
	for _, ledger := range []struct {
		name  string
		query string
		total *int64
	}{
		{name: "upload_intents", query: deleteExpiredUploadIntentsSQL, total: &out.UploadIntentsDeleted},
		{name: "api_idempotency", query: deleteExpiredIdempotencyKeysSQL, total: &out.IdempotencyKeysDeleted},
	} {
		for batch := 0; batch < transientLedgerMaxBatches; batch++ {
			tx, err := pool.Begin(ctx)
			if err != nil {
				return out, fmt.Errorf("begin %s retention batch: %w", ledger.name, err)
			}
			if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout = '1s'; SET LOCAL statement_timeout = '15s'`); err != nil {
				_ = tx.Rollback(ctx)
				return out, fmt.Errorf("bound %s retention batch: %w", ledger.name, err)
			}
			ct, err := tx.Exec(ctx, ledger.query, cutoff, transientLedgerBatchSize)
			if err != nil {
				_ = tx.Rollback(ctx)
				return out, fmt.Errorf("delete %s retention batch: %w", ledger.name, err)
			}
			if err := tx.Commit(ctx); err != nil {
				return out, fmt.Errorf("commit %s retention batch: %w", ledger.name, err)
			}
			deleted := ct.RowsAffected()
			*ledger.total += deleted
			if deleted < transientLedgerBatchSize {
				break
			}
		}
	}
	return out, nil
}
