package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// meteringTickInterval is the metering loop's wakeup cadence. The job acts at most
// once per UTC day (see runRecordingMetering); the hourly tick just bounds how
// soon after a period close the meter event is pushed.
const meteringTickInterval = time.Hour

// meteringStripe is the thin seam over the Stripe client that the metering job
// needs: read a subscription's current period bounds and push one meter event. The
// production path passes the real *billing.Client; unit tests pass a fake so the
// day-count + idempotency-guard MATH is exercised without Stripe.
type meteringStripe interface {
	GetSubscriptionPeriod(ctx context.Context, subID string) (start, end time.Time, err error)
	ReportRecordingDays(ctx context.Context, customerID string, accountID int64, periodKey string, days int) error
}

// meterableAccount is one account the metering job may bill: it has a Stripe
// customer + subscription and the cursor that makes re-runs idempotent.
type meterableAccount struct {
	accountID            int64
	customerID           string
	subscriptionID       string
	lastMeteredPeriodEnd *time.Time
}

// runRecordingMetering is the nightly usage-reporting loop. It is the ONLY place
// that charges: for each account whose Stripe billing period has advanced past the
// last metered one, it counts that period's billable recording-days and pushes a
// single idempotent meter event. It runs under runWithBackoff alongside the
// scheduler, gated on billingEnabled. It acts at most once per UTC day.
func runRecordingMetering(ctx context.Context, pool *pgxpool.Pool, reporter meteringStripe) error {
	ticker := time.NewTicker(meteringTickInterval)
	defer ticker.Stop()

	var lastRunDay string // YYYY-MM-DD of the last day a sweep ran
	runOnce := func() {
		today := time.Now().UTC().Format("2006-01-02")
		if today == lastRunDay {
			return
		}
		if err := meterAllAccounts(ctx, pool, reporter); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("recording metering sweep error: %v", err)
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

// meterAllAccounts processes every account that has a Stripe subscription on file,
// metering each independently so one account's Stripe error cannot stall the rest.
func meterAllAccounts(ctx context.Context, pool *pgxpool.Pool, reporter meteringStripe) error {
	rows, err := pool.Query(ctx, `
		SELECT account_id, stripe_customer_id, stripe_subscription_id, last_metered_period_end
		FROM account_billing
		WHERE stripe_subscription_id IS NOT NULL
		  AND stripe_customer_id IS NOT NULL
		ORDER BY account_id ASC
	`)
	if err != nil {
		return fmt.Errorf("metering: select billable accounts: %w", err)
	}
	defer rows.Close()
	accts := make([]meterableAccount, 0, 16)
	for rows.Next() {
		var a meterableAccount
		if err := rows.Scan(&a.accountID, &a.customerID, &a.subscriptionID, &a.lastMeteredPeriodEnd); err != nil {
			return fmt.Errorf("metering: scan account: %w", err)
		}
		accts = append(accts, a)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("metering: iterate accounts: %w", err)
	}
	for _, a := range accts {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := meterAccount(ctx, pool, reporter, a); err != nil {
			log.Printf("recording metering: account %d skipped: %v", a.accountID, err)
		}
	}
	return nil
}

// meterAccount fetches the account's current Stripe period, skips it if already
// metered (last_metered_period_end guard), counts the period's billable
// recording-days from our own ledger, and on a non-empty period pushes one meter
// event keyed by period so a re-run is a no-op. It then advances the cursor. A
// zero-day period reports nothing (Stripe suppresses the empty invoice) but the
// cursor still advances so the empty period is not re-examined.
func meterAccount(ctx context.Context, pool *pgxpool.Pool, reporter meteringStripe, a meterableAccount) error {
	start, end, err := reporter.GetSubscriptionPeriod(ctx, a.subscriptionID)
	if err != nil {
		return fmt.Errorf("get subscription period: %w", err)
	}
	if start.IsZero() || end.IsZero() {
		return fmt.Errorf("subscription %s returned empty period bounds", a.subscriptionID)
	}
	if periodAlreadyMetered(end, a.lastMeteredPeriodEnd) {
		return nil // idempotent skip: this period (or a later one) is already metered.
	}

	var days int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM recording_billing_days
		WHERE account_id=$1
		  AND rec_day >= $2::date
		  AND rec_day <  $3::date
	`, a.accountID, start, end).Scan(&days); err != nil {
		return fmt.Errorf("count recording days: %w", err)
	}

	// A zero-day period reports nothing (Stripe suppresses the empty invoice) but
	// the cursor still advances so the empty period is not re-examined.
	if shouldReportDays(days) {
		if err := reporter.ReportRecordingDays(ctx, a.customerID, a.accountID, meterPeriodKey(end), days); err != nil {
			return fmt.Errorf("report recording days: %w", err)
		}
	}

	if _, err := pool.Exec(ctx, `
		UPDATE account_billing SET last_metered_period_end=$2::date, updated_at=now()
		WHERE account_id=$1
	`, a.accountID, end); err != nil {
		return fmt.Errorf("advance metering cursor: %w", err)
	}
	return nil
}

// periodAlreadyMetered is the idempotency guard: a period is skipped when its end
// date is not strictly after the last metered period end (we already billed this
// period, or a later one). Comparison is on the UTC calendar date, matching the
// DATE column the cursor is stored in.
func periodAlreadyMetered(periodEnd time.Time, lastMeteredEnd *time.Time) bool {
	if lastMeteredEnd == nil {
		return false
	}
	end := dateOnlyUTC(periodEnd)
	last := dateOnlyUTC(*lastMeteredEnd)
	return !end.After(last)
}

// shouldReportDays gates the meter event on a non-empty period: zero billable
// recording-days push nothing (Stripe suppresses the empty invoice).
func shouldReportDays(days int) bool { return days > 0 }

// meterPeriodKey is the per-period component of the meter-event identifier
// ("<accountID>-<periodKey>" is built inside the Stripe client). It is the
// period-end year-month, so re-sends within a period collapse to one meter event.
func meterPeriodKey(periodEnd time.Time) string {
	return periodEnd.UTC().Format("2006-01")
}

// dateOnlyUTC truncates a timestamp to its UTC calendar date.
func dateOnlyUTC(t time.Time) time.Time {
	u := t.UTC()
	return time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)
}
