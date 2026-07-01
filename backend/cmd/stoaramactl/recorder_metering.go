package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/billing"
)

// meteringTickInterval is the metering loop's wakeup cadence. The job acts at most
// once per UTC day (see runRecordingMetering); the hourly tick just bounds how
// soon after a period close the meter event is pushed.
const meteringTickInterval = time.Hour

// meteringStripe is the thin seam over the Stripe client that the metering job
// needs: read a subscription's current period bounds and push one meter event. The
// production path passes the real *billing.Client; unit tests pass a fake so the
// hour-count + idempotency-guard MATH is exercised without Stripe.
//
// ChargePrepaidBatch is the yearly-prepaid seam: it creates the standalone prepay
// invoice for one aggregated per-account month batch. The credit grant is NOT made
// here (it is made on the invoice.paid webhook once the card is actually charged);
// this pass only creates the charge and records the batch as 'charged'.
type meteringStripe interface {
	GetSubscriptionPeriod(ctx context.Context, subID string) (start, end time.Time, err error)
	ReportRecordingHours(ctx context.Context, customerID string, accountID int64, periodKey string, hours int) error
	ReportStreamHourMonth(ctx context.Context, customerID string, accountID int64, periodKey, hoursDecimal string) error
	ChargePrepaidBatch(ctx context.Context, customerID, batchKey string, cents int64, metadata map[string]string) (billing.PrepaidBatch, error)
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
// last metered one, it counts that period's billable recording-hours and pushes a
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
		// Snapshot first: record today's managed-storage byte + stream-hour totals per
		// account so the stream_hour_month period-average (computed in meterAccount)
		// includes today.
		if err := snapshotManagedStorage(ctx, pool); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("managed storage snapshot error: %v", err)
			return
		}
		if err := meterAllAccounts(ctx, pool, reporter, time.Now().UTC()); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("recording metering sweep error: %v", err)
			return
		}
		// Yearly-prepaid: after the snapshot + metered pass, charge each account with
		// yearly_prepaid recordings once per calendar month for that month's new
		// not-yet-charged managed footage. The metered stream_hour_month meter above
		// is reported UNCHANGED; the credit grant (made on invoice.paid) nets the
		// monthly line to $0 while it lasts. Log-and-continue: a prepay failure never
		// stalls the metered path (which already advanced its cursor).
		if err := prepayYearlyBatches(ctx, pool, reporter, time.Now().UTC()); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("yearly prepay sweep error: %v", err)
			// Do not return: metering already succeeded; retry prepay next tick/day.
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
// now is the sweep instant (UTC); it is threaded through so the closed-period guard
// in meterAccount is deterministically testable.
func meterAllAccounts(ctx context.Context, pool *pgxpool.Pool, reporter meteringStripe, now time.Time) error {
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
		if err := meterAccount(ctx, pool, reporter, a, now); err != nil {
			log.Printf("recording metering: account %d skipped: %v", a.accountID, err)
		}
	}
	return nil
}

// meterAccount fetches the account's current Stripe period and meters it exactly
// once, on the period's final UTC day, while it is still open. The order of guards
// is load-bearing for not-losing and not-double-billing real money:
//
//   - periodAlreadyMetered: skip a period (or a later one) already on the cursor.
//   - periodReadyToMeter: skip a period whose end UTC date has NOT yet arrived. The
//     billable-hour count (rec_hour < end) only becomes COMPLETE at 00:00 UTC of
//     the period-end day, and a metered subscription bills usage in arrears by
//     summing the meter events whose timestamp falls inside the closing period: an
//     event reported BEFORE the period-end instant lands on that period's invoice,
//     one reported after the period closes does not (it rolls to the next period).
//     So the only correct moment to report is on the period-end UTC day, before the
//     close instant, with the now-final hour count. Crucially we therefore NEVER
//     advance the cursor to a still-open period's end (the cursor-jump bug that
//     silently $0-billed real usage): a future-dated period end is left untouched
//     until its day arrives.
//
// On a non-empty period it pushes one meter event keyed by the period-end date, so
// a same-day re-run is a Stripe-dedup no-op; it then advances the cursor. A
// zero-hour period reports nothing (Stripe suppresses the empty invoice) but the
// cursor still advances so the closed empty period is not re-examined.
func meterAccount(ctx context.Context, pool *pgxpool.Pool, reporter meteringStripe, a meterableAccount, now time.Time) error {
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
	if !periodReadyToMeter(end, now) {
		return nil // period still open and its day count not yet final; do not advance.
	}

	var hours int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM recording_billing_hours
		WHERE account_id=$1
		  AND rec_hour >= $2
		  AND rec_hour <  $3
	`, a.accountID, start, end).Scan(&hours); err != nil {
		return fmt.Errorf("count recording hours: %w", err)
	}

	// A zero-hour period reports nothing (Stripe suppresses the empty invoice) but
	// the cursor still advances so the empty period is not re-examined.
	if shouldReportHours(hours) {
		if err := reporter.ReportRecordingHours(ctx, a.customerID, a.accountID, meterPeriodKey(end), hours); err != nil {
			return fmt.Errorf("report recording hours: %w", err)
		}
	}

	// stream_hour_month: average stored stream-hours of managed footage across the
	// same closing period, from the daily snapshots in [start, end). BYO / zero-hour
	// accounts have no snapshots and report nothing (mirrors the zero-hour
	// suppression). Reported BEFORE the cursor advance so a re-run is a no-op for both
	// meters.
	var sumHours float64
	var snapDays int
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(stream_hours_stored), 0), COUNT(*)
		FROM account_storage_snapshots
		WHERE account_id=$1 AND snapshot_date >= $2::date AND snapshot_date < $3::date
	`, a.accountID, start, end).Scan(&sumHours, &snapDays); err != nil {
		return fmt.Errorf("read storage snapshots: %w", err)
	}
	if hoursDecimal, ok := streamHourMonthMeterValue(sumHours, snapDays); ok {
		if err := reporter.ReportStreamHourMonth(ctx, a.customerID, a.accountID, meterPeriodKey(end), hoursDecimal); err != nil {
			return fmt.Errorf("report stream-hour-month: %w", err)
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

// snapshotManagedStorageSQL records today's managed-storage totals per account: the
// byte total SUM(recording_clips.size_bytes) AND the stored stream-hours
// SUM(clip_end_at - clip_start_at in hours), both over each managed account's
// still-org-visible clips, keyed (account_id, CURRENT_DATE). Idempotent within a
// day via ON CONFLICT (a same-day re-run overwrites both columns). Only managed
// accounts get rows, so BYO accounts never accrue snapshots and never report
// stream_hour_month. Both purged AND released clips are excluded (billing-critical
// WHERE, pinned by a shape test): once a clip is released (NAS-pulled, delivered, or
// retention-released) or purged, it drops out of the next snapshot and the account
// stops being billed for its storage. Clip duration uses the wall-clock span
// (clip_end_at - clip_start_at), not duration_ms (unreliable / 0 on many rows).
const snapshotManagedStorageSQL = `
	INSERT INTO account_storage_snapshots (account_id, snapshot_date, bytes_stored, stream_hours_stored)
	SELECT r.account_id, CURRENT_DATE,
	       COALESCE(SUM(c.size_bytes), 0),
	       COALESCE(SUM(EXTRACT(EPOCH FROM (c.clip_end_at - c.clip_start_at)) / 3600.0), 0)
	FROM recording_clips c
	JOIN recordings r            ON r.id = c.recording_id
	JOIN storage_destinations sd ON sd.id = c.storage_destination_id
	WHERE sd.managed AND c.purged_at IS NULL AND c.released_at IS NULL
	GROUP BY r.account_id
	ON CONFLICT (account_id, snapshot_date)
	DO UPDATE SET bytes_stored = EXCLUDED.bytes_stored,
	              stream_hours_stored = EXCLUDED.stream_hours_stored
`

// snapshotManagedStorage runs the nightly managed-storage rollup (see
// snapshotManagedStorageSQL).
func snapshotManagedStorage(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, snapshotManagedStorageSQL); err != nil {
		return fmt.Errorf("snapshot managed storage: %w", err)
	}
	return nil
}

// streamHourMonthMeterValue computes the stream_hour_month meter value from a
// period's snapshot rows: the time-average of stored stream-hours =
// SUM(stream_hours_stored) / numSnapshotDays, formatted to 3 decimals as a decimal
// string (the v1 Meter Events API accepts a decimal value; the price is $0.10 per 1
// stream-hour-month unit so the value IS the billable stream-hour-months). The
// values are ALREADY in hours, so there is NO /1e9 byte->GB conversion here. It
// reports (value, true) only when there is at least one snapshot day AND non-zero
// stored hours; otherwise ("", false) so the caller sends nothing (matching the
// zero-hour suppression). The denominator is the snapshot-row count, so a mid-period
// opt-in averages only over the days the data existed.
func streamHourMonthMeterValue(sumHours float64, snapDays int) (string, bool) {
	if snapDays <= 0 || sumHours <= 0 {
		return "", false
	}
	avgHours := sumHours / float64(snapDays)
	return strconv.FormatFloat(avgHours, 'f', 3, 64), true
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

// periodReadyToMeter reports whether the period ending periodEnd has reached its
// final UTC day as of now, i.e. its billable-day count (rec_day < end-date) is
// complete and it is the period-end day on which the closing meter event must be
// pushed (before the close instant). It is true exactly when now's UTC date is on
// or after periodEnd's UTC date. A still-open period whose end date is in the
// future is NOT ready: returning false here is what stops the cursor from ever
// jumping past an open period (the bug that silently $0-billed real usage).
func periodReadyToMeter(periodEnd, now time.Time) bool {
	return !dateOnlyUTC(periodEnd).After(dateOnlyUTC(now))
}

// shouldReportHours gates the meter event on a non-empty period: zero billable
// recording-hours push nothing (Stripe suppresses the empty invoice).
func shouldReportHours(hours int) bool { return hours > 0 }

// meterPeriodKey is the per-period component of the meter-event identifier
// ("<accountID>-<periodKey>" is built inside the Stripe client). It is the
// period-end UTC date (YYYY-MM-DD): a same-period re-send collapses to one meter
// event (same end date), while two DISTINCT periods get distinct keys. Keying on
// the full date, not just the year-month, is required because an out-of-cycle
// re-anchor (e.g. a manual charge that resets billing_cycle_anchor) can produce two
// separate closing periods inside the same calendar month; a month-only key would
// collide their identifiers and Stripe would reject the second period's usage as a
// duplicate, silently under-billing it.
func meterPeriodKey(periodEnd time.Time) string {
	return periodEnd.UTC().Format("2006-01-02")
}

// dateOnlyUTC truncates a timestamp to its UTC calendar date.
func dateOnlyUTC(t time.Time) time.Time {
	u := t.UTC()
	return time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)
}

// prepayAccount is one account with at least one yearly_prepaid recording plus the
// Stripe customer to bill. subscription id is not needed: a prepay is a STANDALONE
// invoice, not a metered-cycle line.
type prepayAccount struct {
	accountID  int64
	customerID string
}

// prepayYearlyBatches is the monthly per-account yearly-prepaid charge pass. For each
// account that has yearly_prepaid recordings and a Stripe customer, it aggregates
// that account's yearly-tier managed stream-hours of footage NOT yet covered by a
// prepay batch, and (once per calendar month, keyed by batch_key
// "prepay:acct-<id>:<YYYY-MM>") creates a standalone prepay invoice for
// round(stream_hours * 12 * $0.05) and records the ledger batch as 'charged'. The
// credit grant is created later, on invoice.paid.
//
// Idempotency is layered: the batch_key is UNIQUE in prepaid_storage_batches (a
// second run in the same month no-ops on the INSERT), and ChargePrepaidBatch sets
// the same key as the Stripe idempotency key on the invoice item + invoice, so even
// a torn run (ledger insert committed, charge not yet made, then re-run) cannot
// double-charge. 0-stream-hour accounts are skipped entirely (no ledger row, no
// charge). Each account is isolated: one account's Stripe error is logged and the
// sweep continues.
func prepayYearlyBatches(ctx context.Context, pool *pgxpool.Pool, reporter meteringStripe, now time.Time) error {
	rows, err := pool.Query(ctx, `
		SELECT DISTINCT ab.account_id, ab.stripe_customer_id
		FROM account_billing ab
		WHERE ab.stripe_customer_id IS NOT NULL
		  AND EXISTS (
		    SELECT 1 FROM recordings r
		    WHERE r.account_id = ab.account_id
		      AND r.storage_retention_tier = 'yearly_prepaid'
		  )
		ORDER BY ab.account_id ASC
	`)
	if err != nil {
		return fmt.Errorf("prepay: select yearly accounts: %w", err)
	}
	defer rows.Close()
	accts := make([]prepayAccount, 0, 8)
	for rows.Next() {
		var a prepayAccount
		if err := rows.Scan(&a.accountID, &a.customerID); err != nil {
			return fmt.Errorf("prepay: scan account: %w", err)
		}
		accts = append(accts, a)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("prepay: iterate accounts: %w", err)
	}
	for _, a := range accts {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := prepayAccountMonth(ctx, pool, reporter, a, now); err != nil {
			log.Printf("yearly prepay: account %d skipped: %v", a.accountID, err)
		}
	}
	return nil
}

// prepayAccountMonth runs one account's monthly prepay batch. batch_key is
// "prepay:acct-<id>:<YYYY-MM>" so each account is charged at most once per calendar
// month. stream-hours are the account's yearly-tier managed footage NOT already
// covered by ANY prior prepay batch for this account, computed directly from
// recording_clips (account_storage_snapshots has no recording_id). The
// already-charged set is the SUM of stream_hours across this account's prior
// non-failed batches, subtracted from the current yearly footage total, so each
// clip's storage is prepaid exactly once across successive months.
func prepayAccountMonth(ctx context.Context, pool *pgxpool.Pool, reporter meteringStripe, a prepayAccount, now time.Time) error {
	batchKey := fmt.Sprintf("prepay:acct-%d:%s", a.accountID, now.UTC().Format("2006-01"))

	// Short-circuit: if this month's batch already exists, nothing to do (idempotent).
	var exists bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM prepaid_storage_batches WHERE batch_key=$1)
	`, batchKey).Scan(&exists); err != nil {
		return fmt.Errorf("check batch exists: %w", err)
	}
	if exists {
		return nil
	}

	// Total yearly-tier managed stream-hours currently stored for this account
	// (purged/released clips excluded, matching the snapshot's billing WHERE), minus
	// what prior non-failed batches already prepaid. clip duration is the wall-clock
	// span; identical to the snapshot's stream-hour math.
	var newStreamHours float64
	if err := pool.QueryRow(ctx, `
		WITH current AS (
			SELECT COALESCE(SUM(EXTRACT(EPOCH FROM (c.clip_end_at - c.clip_start_at)) / 3600.0), 0) AS hours
			FROM recording_clips c
			JOIN recordings r            ON r.id = c.recording_id
			JOIN storage_destinations sd ON sd.id = c.storage_destination_id
			WHERE r.account_id = $1
			  AND r.storage_retention_tier = 'yearly_prepaid'
			  AND sd.managed
			  AND c.purged_at IS NULL
			  AND c.released_at IS NULL
		), prepaid AS (
			SELECT COALESCE(SUM(stream_hours), 0) AS hours
			FROM prepaid_storage_batches
			WHERE account_id = $1 AND status <> 'failed'
		)
		SELECT GREATEST(current.hours - prepaid.hours, 0) FROM current, prepaid
	`, a.accountID).Scan(&newStreamHours); err != nil {
		return fmt.Errorf("compute new yearly stream-hours: %w", err)
	}
	if newStreamHours <= 0 {
		return nil // no new footage to prepay this month.
	}

	cents := billing.PrepaidBatchCents(newStreamHours)
	if cents <= 0 {
		return nil // rounds to $0 (a few seconds of footage); wait for more.
	}

	if err := chargeAndRecordBatch(ctx, pool, reporter, chargeBatch{
		batchKey:    batchKey,
		accountID:   a.accountID,
		customerID:  a.customerID,
		recordingID: nil,
		streamHours: newStreamHours,
		cents:       cents,
	}); err != nil {
		return err
	}
	return nil
}

// chargeBatch is the fully-resolved inputs for one prepay charge, shared by the
// monthly per-account pass and the retroactive per-recording tier switch.
type chargeBatch struct {
	batchKey    string
	accountID   int64
	customerID  string
	recordingID *int64
	streamHours float64
	cents       int64
}

// chargeAndRecordBatch inserts the pending ledger row, charges the standalone prepay
// invoice, and transitions the row to 'charged'. The ledger insert is a
// no-double-charge gate: batch_key is UNIQUE, so a concurrent/retried run that finds
// the row already present returns without charging. On a Stripe error the row is
// marked 'failed' (so the next pass does not silently re-attempt under a key that
// already burned its Stripe idempotency key) and the error is returned.
func chargeAndRecordBatch(ctx context.Context, pool *pgxpool.Pool, reporter meteringStripe, b chargeBatch) error {
	var recArg any
	if b.recordingID != nil {
		recArg = *b.recordingID
	}
	ct, err := pool.Exec(ctx, `
		INSERT INTO prepaid_storage_batches
			(batch_key, account_id, recording_id, stream_hours, charged_cents, status)
		VALUES ($1,$2,$3,$4,$5,'pending')
		ON CONFLICT (batch_key) DO NOTHING
	`, b.batchKey, b.accountID, recArg, b.streamHours, b.cents)
	if err != nil {
		return fmt.Errorf("insert prepay batch: %w", err)
	}
	if ct.RowsAffected() == 0 {
		// Batch already exists (idempotent): do not charge again.
		return nil
	}

	meta := map[string]string{
		"account_id":   strconv.FormatInt(b.accountID, 10),
		"stream_hours": strconv.FormatFloat(b.streamHours, 'f', 4, 64),
		"kind":         "yearly_prepaid_storage",
	}
	if b.recordingID != nil {
		meta["recording_id"] = strconv.FormatInt(*b.recordingID, 10)
	}
	res, err := reporter.ChargePrepaidBatch(ctx, b.customerID, b.batchKey, b.cents, meta)
	if err != nil {
		if _, uerr := pool.Exec(ctx, `
			UPDATE prepaid_storage_batches SET status='failed', updated_at=now() WHERE batch_key=$1
		`, b.batchKey); uerr != nil {
			log.Printf("yearly prepay: mark batch %s failed: %v", b.batchKey, uerr)
		}
		return fmt.Errorf("charge prepay batch %s: %w", b.batchKey, err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE prepaid_storage_batches
		SET status='charged', stripe_invoice_id=$2, stripe_invoice_item_id=$3, charged_at=now(), updated_at=now()
		WHERE batch_key=$1
	`, b.batchKey, res.InvoiceID, res.InvoiceItemID); err != nil {
		return fmt.Errorf("record charged batch %s: %w", b.batchKey, err)
	}
	return nil
}
