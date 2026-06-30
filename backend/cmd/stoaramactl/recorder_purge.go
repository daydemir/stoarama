package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

// managedPurgeGraceDays is how long an account's managed recordings are retained
// after it stops paying (subscription canceled, or sustained dunning failure)
// before the purge job deletes the R2 objects. Active payers keep their data
// indefinitely (has_payment_method stays true, so they are never eligible).
const managedPurgeGraceDays = 14

// purgeTickInterval matches the metering loop: the job acts at most once per UTC
// day; the hourly tick just bounds how soon after eligibility a purge runs.
const purgeTickInterval = time.Hour

// r2DeleteBatchSize is the S3 DeleteObjects per-call cap (1000 keys); object keys
// are deleted in batches of at most this many.
const r2DeleteBatchSize = 1000

// objectDeleter is the thin seam over the operator R2 client the purge job needs:
// delete a batch of object keys. The production path passes the real *r2.Client;
// unit tests pass a fake so the batching + mark-purged MATH is exercised without
// R2 or a database.
type objectDeleter interface {
	DeleteObjects(ctx context.Context, keys []string) error
}

// clipObject is one managed, non-purged clip the purge job must delete: its row id
// (to mark purged_at) and its R2 object key (to delete).
type clipObject struct {
	id        int64
	objectKey string
}

// runManagedPurge is the daily retention job: it deletes the managed R2 objects of
// accounts that have stopped paying past the grace period, then marks those clips
// purged so they drop out of the stream_hour_month snapshot. It runs under
// runWithBackoff in recorder-control, gated on billingEnabled AND a valid operator
// R2 config. It NEVER touches BYO objects (the queries are restricted to managed
// destinations and the keys live under the managed/acct-<id>/ prefix).
func runManagedPurge(ctx context.Context, pool *pgxpool.Pool, deleter objectDeleter) error {
	ticker := time.NewTicker(purgeTickInterval)
	defer ticker.Stop()

	var lastRunDay string // YYYY-MM-DD of the last day a purge ran
	runOnce := func() {
		today := time.Now().UTC().Format("2006-01-02")
		if today == lastRunDay {
			return
		}
		if err := purgeEligibleAccounts(ctx, pool, deleter); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("managed purge sweep error: %v", err)
			return
		}
		lastRunDay = today
	}

	runOnce()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			runOnce()
		}
	}
}

// purgeEligibleAccounts deletes managed objects for every account past the
// retention grace, purging each independently so one account's R2/DB error cannot
// stall the rest. Eligible = has managed, non-purged clips AND has_payment_method
// is false AND (no subscription on file OR last_payment_failed_at older than the
// grace period). Active payers are excluded because has_payment_method stays true.
func purgeEligibleAccounts(ctx context.Context, pool *pgxpool.Pool, deleter objectDeleter) error {
	rows, err := pool.Query(ctx, `
		SELECT DISTINCT r.account_id
		FROM recording_clips c
		JOIN recordings r            ON r.id = c.recording_id
		JOIN storage_destinations sd ON sd.id = c.storage_destination_id
		JOIN account_billing b       ON b.account_id = r.account_id
		WHERE sd.managed
		  AND c.purged_at IS NULL
		  AND b.has_payment_method = false
		  AND (b.stripe_subscription_id IS NULL
		       OR (b.last_payment_failed_at IS NOT NULL
		           AND b.last_payment_failed_at < now() - make_interval(days => $1)))
		ORDER BY r.account_id ASC
	`, managedPurgeGraceDays)
	if err != nil {
		return fmt.Errorf("purge: select eligible accounts: %w", err)
	}
	defer rows.Close()
	accountIDs := make([]int64, 0, 16)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return fmt.Errorf("purge: scan account: %w", err)
		}
		accountIDs = append(accountIDs, id)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("purge: iterate accounts: %w", err)
	}

	for _, accountID := range accountIDs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := purgeAccount(ctx, pool, deleter, accountID); err != nil {
			log.Printf("managed purge: account %d skipped: %v", accountID, err)
		}
	}
	return nil
}

// purgeAccount loads an account's managed, non-purged clips and purges them.
func purgeAccount(ctx context.Context, pool *pgxpool.Pool, deleter objectDeleter, accountID int64) error {
	rows, err := pool.Query(ctx, `
		SELECT c.id, c.object_key
		FROM recording_clips c
		JOIN storage_destinations sd ON sd.id = c.storage_destination_id
		JOIN recordings r            ON r.id = c.recording_id
		WHERE r.account_id = $1 AND sd.managed AND c.purged_at IS NULL
	`, accountID)
	if err != nil {
		return fmt.Errorf("select managed clips: %w", err)
	}
	defer rows.Close()
	clips := make([]clipObject, 0, 64)
	for rows.Next() {
		var co clipObject
		if err := rows.Scan(&co.id, &co.objectKey); err != nil {
			return fmt.Errorf("scan clip: %w", err)
		}
		clips = append(clips, co)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate clips: %w", err)
	}
	return purgeClips(ctx, deleter, clips, func(ctx context.Context, ids []int64) error {
		_, err := pool.Exec(ctx, `UPDATE recording_clips SET purged_at = now() WHERE id = ANY($1)`, ids)
		return err
	})
}

// purgeClips deletes the given clips' R2 objects in batches of at most
// r2DeleteBatchSize, marking each batch's clip rows purged via markPurged only
// AFTER its DeleteObjects succeeds. A failed batch returns immediately, leaving
// the remaining (and that batch's) rows un-purged for the next pass; a re-run
// re-fetches only purged_at IS NULL rows, so the job is idempotent and a partial
// failure self-heals. It is the unit-testable core (fake deleter + fake
// markPurged) of the per-account purge.
func purgeClips(ctx context.Context, deleter objectDeleter, clips []clipObject, markPurged func(ctx context.Context, ids []int64) error) error {
	for start := 0; start < len(clips); start += r2DeleteBatchSize {
		end := start + r2DeleteBatchSize
		if end > len(clips) {
			end = len(clips)
		}
		batch := clips[start:end]
		keys := make([]string, len(batch))
		ids := make([]int64, len(batch))
		for i, co := range batch {
			keys[i] = co.objectKey
			ids[i] = co.id
		}
		if err := deleter.DeleteObjects(ctx, keys); err != nil {
			return fmt.Errorf("delete objects: %w", err)
		}
		if err := markPurged(ctx, ids); err != nil {
			return fmt.Errorf("mark purged: %w", err)
		}
	}
	return nil
}

// mustOperatorR2Client builds the operator R2 client used by the purge job from
// the process config, exiting on a missing/invalid R2 configuration. Mirrors
// mustArchiveR2Client (r2client.go) so managed-storage purge and the survey
// commands share one operator-credential path.
func mustOperatorR2Client(ctx context.Context, cfg config.Config) *r2.Client {
	if err := cfg.ValidateR2(); err != nil {
		log.Fatalf("managed purge: R2 config required: %v", err)
	}
	r2c, err := r2.New(ctx, r2.Config{
		AccountID: cfg.R2AccountID,
		AccessKey: cfg.R2AccessKeyID,
		SecretKey: cfg.R2SecretAccessKey,
		Region:    cfg.R2Region,
		Bucket:    cfg.R2Bucket,
		Endpoint:  cfg.R2Endpoint,
	})
	if err != nil {
		log.Fatalf("managed purge: open R2 client: %v", err)
	}
	return r2c
}
