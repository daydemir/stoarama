package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// managedReleaseGraceDays is how long an account's managed recordings stay billed
// after it stops paying (subscription canceled, or sustained dunning failure)
// before the retention job RELEASES them (billing stops + org can no longer see
// them). Active payers keep their data org-visible indefinitely (has_payment_method
// stays true, so they are never eligible). A released clip's R2 object + row +
// association are all KEPT (DENIZ policy: recorded content is never hard-deleted);
// release only detaches it from the org.
const managedReleaseGraceDays = 14

// releaseTickInterval matches the metering loop: the job acts at most once per UTC
// day; the hourly tick just bounds how soon after eligibility a release runs.
const releaseTickInterval = time.Hour

// runManagedRelease is the daily retention job: it RELEASES the managed clips of
// accounts that have stopped paying past the grace period (released_at=now()), so
// they drop out of the stream_hour_month snapshot and the org clip surfaces while
// their R2 objects are retained. It runs under runWithBackoff in recorder-control,
// gated on billingEnabled. It never touches BYO clips (the query is restricted to
// managed destinations) and NEVER deletes any R2 object.
func runManagedRelease(ctx context.Context, pool *pgxpool.Pool) error {
	ticker := time.NewTicker(releaseTickInterval)
	defer ticker.Stop()

	var lastRunDay string // YYYY-MM-DD of the last day a release ran
	runOnce := func() {
		today := time.Now().UTC().Format("2006-01-02")
		if today == lastRunDay {
			return
		}
		if err := releaseEligibleAccounts(ctx, pool); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("managed release sweep error: %v", err)
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

// eligibleReleaseAccountsSQL selects each account past the retention grace that
// still has managed, org-visible clips. Guarded by purged_at IS NULL AND
// released_at IS NULL so already-detached clips never re-trigger eligibility.
const eligibleReleaseAccountsSQL = `
	SELECT DISTINCT r.account_id
	FROM recording_clips c
	JOIN recordings r            ON r.id = c.recording_id
	JOIN storage_destinations sd ON sd.id = c.storage_destination_id
	JOIN account_billing b       ON b.account_id = r.account_id
	WHERE sd.managed
	  AND c.purged_at IS NULL
	  AND c.released_at IS NULL
	  AND b.has_payment_method = false
	  AND (b.stripe_subscription_id IS NULL
	       OR (b.last_payment_failed_at IS NOT NULL
	           AND b.last_payment_failed_at < now() - make_interval(days => $1)))
	ORDER BY r.account_id ASC
`

// releaseAccountClipsSQL marks every still-org-visible managed clip of one account
// released_at=now() in a single UPDATE. It NEVER references any R2/delete op: the
// row + object_key + association are retained; only the released_at stamp changes.
const releaseAccountClipsSQL = `
	UPDATE recording_clips c
	SET released_at = now()
	FROM recordings r, storage_destinations sd
	WHERE c.recording_id = r.id
	  AND c.storage_destination_id = sd.id
	  AND r.account_id = $1
	  AND sd.managed
	  AND c.purged_at IS NULL
	  AND c.released_at IS NULL
`

// releaseEligibleAccounts releases managed clips for every account past the
// retention grace in ONE account-scoped UPDATE per account, so one account's DB
// error cannot stall the rest. Eligible = has managed, still-org-visible clips AND
// has_payment_method is false AND (no subscription on file OR last_payment_failed_at
// older than the grace period). Active payers are excluded because has_payment_method
// stays true. Idempotent: only clips that are neither purged nor already released
// are touched.
func releaseEligibleAccounts(ctx context.Context, pool *pgxpool.Pool) error {
	rows, err := pool.Query(ctx, eligibleReleaseAccountsSQL, managedReleaseGraceDays)
	if err != nil {
		return fmt.Errorf("release: select eligible accounts: %w", err)
	}
	defer rows.Close()
	accountIDs := make([]int64, 0, 16)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return fmt.Errorf("release: scan account: %w", err)
		}
		accountIDs = append(accountIDs, id)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("release: iterate accounts: %w", err)
	}

	for _, accountID := range accountIDs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := releaseAccount(ctx, pool, accountID); err != nil {
			log.Printf("managed release: account %d skipped: %v", accountID, err)
		}
	}
	return nil
}

// releaseAccount marks every still-org-visible managed clip of one account
// released_at=now() in a single UPDATE. No R2 object is deleted (release keeps the
// bytes); the row + object_key + association are retained.
func releaseAccount(ctx context.Context, pool *pgxpool.Pool, accountID int64) error {
	if _, err := pool.Exec(ctx, releaseAccountClipsSQL, accountID); err != nil {
		return fmt.Errorf("release managed clips: %w", err)
	}
	return nil
}
