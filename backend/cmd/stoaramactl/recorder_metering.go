package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/billing"
	"github.com/stripe/stripe-go/v82"
)

// Period close checks run every five minutes. Stripe period discovery runs hourly;
// storage snapshots and yearly prepay run once per UTC day.
const meteringTickInterval = 5 * time.Minute

// nasStagingGrace is the free window a nas_pull clip may sit in managed staging
// before it accrues managed-storage charges. Within the grace it is FREE (the NAS
// is expected to pull it); only clips still staged past the grace count toward
// stream_hour_month, i.e. only when the NAS is down or falling behind.
const nasStagingGrace = 24 * time.Hour

var errAmbiguousMeterReport = errors.New("ambiguous pending meter report")
var errMeteringPending = errors.New("billing verification pending")

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
	ReportRecordingHours(ctx context.Context, customerID string, accountID int64, periodKey string, eventAt time.Time, hours int) error
	ReportStreamHourMonth(ctx context.Context, customerID string, accountID int64, periodKey string, eventAt time.Time, hoursDecimal string) error
	MeterUsage(ctx context.Context, customerID, meterKind string, start, end time.Time) (float64, error)
	PeriodInvoice(ctx context.Context, customerID, subscriptionID string, start, end time.Time) (billing.PeriodInvoice, error)
	ChargePrepaidBatch(ctx context.Context, customerID, batchKey string, cents int64, metadata map[string]string) (billing.PrepaidBatch, error)
}

// meterableAccount is one account with a Stripe customer and subscription.
type meterableAccount struct {
	accountID      int64
	customerID     string
	subscriptionID string
}

// runRecordingMetering is the nightly usage-reporting loop. It is the ONLY place
// that charges: for each account whose Stripe billing period has advanced past the
// last metered one, it counts that period's billable recording-hours and pushes a
// single idempotent meter event. It runs under runWithBackoff alongside the
// scheduler, gated on billingEnabled. It acts at most once per UTC day.
func runRecordingMetering(ctx context.Context, pool *pgxpool.Pool, reporter meteringStripe) error {
	ticker := time.NewTicker(meteringTickInterval)
	defer ticker.Stop()

	var lastSnapshotDay, lastPrepayDay string
	var lastDiscovery time.Time
	runOnce := func() {
		now := time.Now().UTC()
		if lastDiscovery.IsZero() || now.Sub(lastDiscovery) >= time.Hour {
			if err := discoverBillingPeriods(ctx, pool, reporter); err != nil && ctx.Err() == nil {
				log.Printf("billing period discovery error: %v", err)
			} else {
				lastDiscovery = now
			}
		}
		if err := meterDuePeriods(ctx, pool, reporter, now); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("recording metering sweep error: %v", err)
		}
		today := now.Format("2006-01-02")
		// Daily storage/prepay work is deliberately independent: failure here can
		// never suppress a closed-period report.
		if today != lastSnapshotDay {
			if err := snapshotManagedStorage(ctx, pool); err != nil {
				if ctx.Err() == nil {
					log.Printf("managed storage snapshot error: %v", err)
				}
			} else {
				lastSnapshotDay = today
			}
		}
		// Yearly-prepaid: after the snapshot + metered pass, charge each account with
		// yearly_prepaid recordings once per calendar month for that month's new
		// not-yet-charged managed footage. The metered stream_hour_month meter above
		// is reported UNCHANGED; the credit grant (made on invoice.paid) nets the
		// monthly line to $0 while it lasts. Log-and-continue: a prepay failure never
		// stalls the metered path (which already advanced its cursor).
		if today != lastPrepayDay {
			if err := prepayYearlyBatches(ctx, pool, reporter, time.Now().UTC()); err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("yearly prepay sweep error: %v", err)
			} else {
				lastPrepayDay = today
			}
		}
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

// discoverBillingPeriods persists Stripe's current exact bounds while the period
// is open. Once stored, a restart or missed close sweep cannot make it disappear
// when Stripe advances to the next period.
func discoverBillingPeriods(ctx context.Context, pool *pgxpool.Pool, reporter meteringStripe) error {
	accts, err := selectMeterableAccounts(ctx, pool)
	if err != nil {
		return err
	}
	hadError := false
	for _, a := range accts {
		start, end, err := reporter.GetSubscriptionPeriod(ctx, a.subscriptionID)
		if err != nil {
			log.Printf("billing period discovery: account %d: %v", a.accountID, err)
			hadError = true
			continue
		}
		if start.IsZero() || !end.After(start) {
			log.Printf("billing period discovery: account %d invalid bounds", a.accountID)
			hadError = true
			continue
		}
		result, err := pool.Exec(ctx, `
			INSERT INTO billing_meter_periods(account_id,stripe_customer_id,stripe_subscription_id,period_start,period_end)
			VALUES($1,$2,$3,$4,$5)
			ON CONFLICT(account_id,period_end) DO UPDATE SET last_seen_at=now()
			WHERE billing_meter_periods.period_start=EXCLUDED.period_start
			  AND billing_meter_periods.stripe_customer_id=EXCLUDED.stripe_customer_id
			  AND billing_meter_periods.stripe_subscription_id=EXCLUDED.stripe_subscription_id
		`, a.accountID, a.customerID, a.subscriptionID, start.UTC(), end.UTC())
		if err != nil {
			log.Printf("billing period discovery: account %d: %v", a.accountID, err)
			hadError = true
			continue
		}
		if result.RowsAffected() != 1 {
			log.Printf("billing period discovery: account %d conflicting bounds for %s", a.accountID, end.UTC())
			hadError = true
		}
	}
	if hadError {
		return fmt.Errorf("one or more billing periods were not persisted")
	}
	return nil
}

func selectMeterableAccounts(ctx context.Context, pool *pgxpool.Pool) ([]meterableAccount, error) {
	rows, err := pool.Query(ctx, `SELECT account_id,stripe_customer_id,stripe_subscription_id FROM account_billing WHERE stripe_subscription_id IS NOT NULL AND stripe_customer_id IS NOT NULL ORDER BY account_id`)
	if err != nil {
		return nil, fmt.Errorf("metering: select billable accounts: %w", err)
	}
	defer rows.Close()
	var accts []meterableAccount
	for rows.Next() {
		var a meterableAccount
		if err := rows.Scan(&a.accountID, &a.customerID, &a.subscriptionID); err != nil {
			return nil, err
		}
		accts = append(accts, a)
	}
	return accts, rows.Err()
}

func meterDuePeriods(ctx context.Context, pool *pgxpool.Pool, reporter meteringStripe, now time.Time) error {
	rows, err := pool.Query(ctx, `
		SELECT p.account_id,p.stripe_customer_id,p.stripe_subscription_id,p.period_start,p.period_end
		FROM billing_meter_periods p
		WHERE p.metered_at IS NULL AND p.period_end <= $1::timestamptz - interval '70 minutes'
		ORDER BY p.period_end,p.account_id`, now.UTC())
	if err != nil {
		return err
	}
	defer rows.Close()
	type due struct {
		a          meterableAccount
		start, end time.Time
	}
	var periods []due
	for rows.Next() {
		var d due
		if err := rows.Scan(&d.a.accountID, &d.a.customerID, &d.a.subscriptionID, &d.start, &d.end); err != nil {
			return err
		}
		periods = append(periods, d)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, d := range periods {
		if now.Sub(d.end) > 34*24*time.Hour {
			log.Printf("URGENT billing period account %d ended %s and is too old for automatic Stripe catch-up", d.a.accountID, d.end.UTC())
			continue
		}
		if err := meterClosedPeriod(ctx, pool, reporter, d.a, d.start, d.end); err != nil {
			if !errors.Is(err, errMeteringPending) {
				log.Printf("recording metering: account %d period %s skipped: %v", d.a.accountID, d.end.UTC(), err)
			}
		}
	}
	return nil
}

// meterClosedPeriod reports a period only after its exact end. Its event timestamp
// remains inside that period, so a delayed sweep is attributed correctly.
func meterClosedPeriod(ctx context.Context, pool *pgxpool.Pool, reporter meteringStripe, a meterableAccount, start, end time.Time) error {
	if start.IsZero() || !end.After(start) {
		return fmt.Errorf("invalid closed-period bounds")
	}
	eventAt := end.UTC().Add(-time.Minute)
	if eventAt.Before(start.UTC()) {
		eventAt = start.UTC()
	}
	awaitingVerification := false

	var hours int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM recording_billing_hours
		WHERE account_id=$1
		  AND rec_hour >= $2
		  AND rec_hour <  $3
	`, a.accountID, start, end).Scan(&hours); err != nil {
		return fmt.Errorf("count recording hours: %w", err)
	}
	var storageEligibleFrom time.Time
	if err := pool.QueryRow(ctx, `SELECT eligible_from FROM billing_storage_fact_config WHERE singleton`).Scan(&storageEligibleFrom); err != nil {
		return fmt.Errorf("read storage billing epoch: %w", err)
	}
	if start.UTC().Before(storageEligibleFrom.UTC()) {
		return fmt.Errorf("period predates trustworthy frozen storage classification; manual audited billing required")
	}
	var hasUntrustedLegacy bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS(
		  SELECT 1 FROM recording_clips c
		  JOIN recordings r ON r.id=c.recording_id
		  LEFT JOIN clip_storage_billing_contracts bc ON bc.clip_id=c.id
		  WHERE r.account_id=$1 AND (bc.clip_id IS NULL OR NOT bc.authoritative)
		    AND c.created_at < $3
		    AND (c.released_at IS NULL OR c.released_at >= $2)
		    AND (c.purged_at IS NULL OR c.purged_at >= $2)
		)`, a.accountID, start, end).Scan(&hasUntrustedLegacy); err != nil {
		return fmt.Errorf("check legacy storage contracts: %w", err)
	}
	if hasUntrustedLegacy {
		return fmt.Errorf("period contains legacy inventory without an authoritative storage contract; manual audit required")
	}
	if err := reconstructStorageDailyFacts(ctx, pool, a.accountID, start, end); err != nil {
		return err
	}
	var sumHours float64
	var snapDays int
	if err := pool.QueryRow(ctx, `SELECT COALESCE(SUM(stream_hours_stored),0),COUNT(*) FROM billing_storage_daily_facts WHERE account_id=$1 AND usage_date >= ($2 AT TIME ZONE 'UTC')::date AND usage_date < ($3 AT TIME ZONE 'UTC')::date`, a.accountID, start, end).Scan(&sumHours, &snapDays); err != nil {
		return fmt.Errorf("read storage daily facts: %w", err)
	}
	storageDecimal, hasStorage := streamHourMonthMeterValue(sumHours, snapDays)
	inv, err := reporter.PeriodInvoice(ctx, a.customerID, a.subscriptionID, start, end)
	if err != nil {
		return fmt.Errorf("resolve period invoice: %w", err)
	}
	if err := bindPeriodInvoice(ctx, pool, a.accountID, end, inv.ID); err != nil {
		return err
	}
	if inv.Status != stripe.InvoiceStatusDraft {
		return verifyFinalizedPeriod(ctx, pool, reporter, a, start, end, hours, storageDecimal, hasStorage, inv)
	}
	if inv.FinalizesAt.IsZero() || inv.FinalizesAt.Before(time.Now().UTC().Add(30*time.Minute)) {
		return fmt.Errorf("period invoice has less than 30 minutes of verified draft runway; refusing meter submission")
	}

	// A zero-hour period reports nothing (Stripe suppresses the empty invoice) but
	// the cursor still advances so the empty period is not re-examined.
	if shouldReportHours(hours) {
		periodKey := meterPeriodKey(end)
		send, err := reserveMeterReport(ctx, pool, a.accountID, end, "recording_hour", strconv.Itoa(hours), fmt.Sprintf("%d-%s", a.accountID, periodKey), eventAt)
		if err != nil {
			if errors.Is(err, errAmbiguousMeterReport) {
				if err := reconcilePendingMeterReport(ctx, pool, reporter, a, start, end, "recording_hour", float64(hours)); err != nil {
					return err
				}
				send = false
			} else {
				return fmt.Errorf("reserve recording-hours report: %w", err)
			}
		}
		if send {
			if err := reporter.ReportRecordingHours(ctx, a.customerID, a.accountID, periodKey, eventAt, hours); err != nil {
				return fmt.Errorf("report recording hours (left pending for reconciliation): %w", err)
			}
			awaitingVerification = true
		}
	}

	// stream_hour_month: average stored stream-hours of managed footage across the
	// same closing period, from the daily snapshots in [start, end). BYO / zero-hour
	// accounts have no snapshots and report nothing (mirrors the zero-hour
	// suppression). Reported BEFORE the cursor advance so a re-run is a no-op for both
	// meters.
	if hoursDecimal, ok := storageDecimal, hasStorage; ok {
		periodKey := meterPeriodKey(end)
		identifier := fmt.Sprintf("%d-shm-%s", a.accountID, periodKey)
		send, err := reserveMeterReport(ctx, pool, a.accountID, end, "stream_hour_month", hoursDecimal, identifier, eventAt)
		if err != nil {
			if errors.Is(err, errAmbiguousMeterReport) {
				expected, parseErr := strconv.ParseFloat(hoursDecimal, 64)
				if parseErr != nil {
					return parseErr
				}
				if err := reconcilePendingMeterReport(ctx, pool, reporter, a, start, end, "stream_hour_month", expected); err != nil {
					return err
				}
				send = false
			} else {
				return fmt.Errorf("reserve stream-hour-month report: %w", err)
			}
		}
		if send {
			if err := reporter.ReportStreamHourMonth(ctx, a.customerID, a.accountID, periodKey, eventAt, hoursDecimal); err != nil {
				return fmt.Errorf("report stream-hour-month (left pending for reconciliation): %w", err)
			}
			awaitingVerification = true
		}
	}
	if awaitingVerification {
		return fmt.Errorf("%w: meter events submitted; awaiting Stripe aggregate and invoice verification", errMeteringPending)
	}
	return fmt.Errorf("%w: meter aggregates verified; awaiting invoice finalization", errMeteringPending)
}

func reconcilePendingMeterReport(ctx context.Context, pool *pgxpool.Pool, reporter meteringStripe, a meterableAccount, periodStart, periodEnd time.Time, meterKind string, expected float64) error {
	actual, err := reporter.MeterUsage(ctx, a.customerID, meterKind, periodStart, periodEnd)
	if err != nil {
		return fmt.Errorf("reconcile pending %s report: %w", meterKind, err)
	}
	if math.Abs(actual-expected) > 0.0005 {
		return fmt.Errorf("pending %s report remains unresolved: Stripe aggregate %.3f, expected %.3f; refusing automatic resend", meterKind, actual, expected)
	}
	if err := markMeterReportReported(ctx, pool, a.accountID, periodEnd, meterKind); err != nil {
		return fmt.Errorf("reconcile pending %s report: %w", meterKind, err)
	}
	return nil
}

func bindPeriodInvoice(ctx context.Context, pool *pgxpool.Pool, accountID int64, periodEnd time.Time, invoiceID string) error {
	result, err := pool.Exec(ctx, `UPDATE billing_meter_periods SET stripe_invoice_id=$3 WHERE account_id=$1 AND period_end=$2 AND (stripe_invoice_id IS NULL OR stripe_invoice_id=$3)`, accountID, periodEnd, invoiceID)
	if err != nil {
		return fmt.Errorf("bind period invoice: %w", err)
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("period invoice identity changed; refusing billing")
	}
	return nil
}

func verifyFinalizedPeriod(ctx context.Context, pool *pgxpool.Pool, reporter meteringStripe, a meterableAccount, start, end time.Time, hours int, storageDecimal string, hasStorage bool, inv billing.PeriodInvoice) error {
	if inv.Status != stripe.InvoiceStatusOpen && inv.Status != stripe.InvoiceStatusPaid {
		return fmt.Errorf("period invoice %s status %s requires manual adjustment", inv.ID, inv.Status)
	}
	if hours > 0 {
		if err := requireVerifiedMeterReport(ctx, pool, reporter, a, start, end, "recording_hour", float64(hours)); err != nil {
			return err
		}
	}
	storageExpected := 0.0
	if hasStorage {
		var err error
		storageExpected, err = strconv.ParseFloat(storageDecimal, 64)
		if err != nil {
			return err
		}
		if err := requireVerifiedMeterReport(ctx, pool, reporter, a, start, end, "stream_hour_month", storageExpected); err != nil {
			return err
		}
	}
	wantRecording := int64(hours) * billing.RecordingHourUnitAmountCents
	wantStorage := int64(math.Round(storageExpected * float64(billing.StreamHourMonthUnitAmountCents)))
	if inv.RecordingCents != wantRecording || inv.StorageCents != wantStorage {
		return fmt.Errorf("finalized invoice %s usage mismatch: recording %d/%d cents storage %d/%d cents; manual adjustment required", inv.ID, inv.RecordingCents, wantRecording, inv.StorageCents, wantStorage)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `UPDATE account_billing SET last_metered_period_end=GREATEST(COALESCE(last_metered_period_end,($2 AT TIME ZONE 'UTC')::date),($2 AT TIME ZONE 'UTC')::date),updated_at=now() WHERE account_id=$1`, a.accountID, end); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `UPDATE billing_meter_periods SET metered_at=now(),invoice_verified_at=now(),recording_amount_cents=$3,storage_amount_cents=$4 WHERE account_id=$1 AND period_end=$2 AND metered_at IS NULL`, a.accountID, end, wantRecording, wantStorage)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("billing period already completed concurrently")
	}
	return tx.Commit(ctx)
}

func requireVerifiedMeterReport(ctx context.Context, pool *pgxpool.Pool, reporter meteringStripe, a meterableAccount, start, end time.Time, kind string, expected float64) error {
	var status string
	err := pool.QueryRow(ctx, `SELECT status FROM billing_meter_reports WHERE account_id=$1 AND period_end=$2 AND meter_kind=$3`, a.accountID, end, kind).Scan(&status)
	if err != nil {
		return fmt.Errorf("%s report absent; finalized invoice requires manual adjustment: %w", kind, err)
	}
	if status == "reported" {
		return nil
	}
	return reconcilePendingMeterReport(ctx, pool, reporter, a, start, end, kind, expected)
}

// reserveMeterReport is a durable outbox barrier in front of a Stripe side
// effect. A pre-existing reported row makes a retry a no-op. A pre-existing
// pending row is ambiguous (Stripe may have accepted the prior request before
// the process lost its DB acknowledgement), so it fails closed and is never sent
// again automatically.
func reserveMeterReport(ctx context.Context, pool *pgxpool.Pool, accountID int64, periodEnd time.Time, meterKind, expectedValue, identifier string, eventAt time.Time) (bool, error) {
	result, err := pool.Exec(ctx, `
		INSERT INTO billing_meter_reports(account_id,period_end,meter_kind,expected_value,identifier,event_timestamp)
		VALUES($1,$2,$3,$4,$5,$6)
		ON CONFLICT (meter_kind,identifier) DO NOTHING
	`, accountID, periodEnd, meterKind, expectedValue, identifier, eventAt)
	if err != nil {
		return false, err
	}
	if result.RowsAffected() == 1 {
		return true, nil
	}
	var status, storedValue string
	var storedTimestamp *time.Time
	if err := pool.QueryRow(ctx, `
		SELECT status,expected_value,event_timestamp FROM billing_meter_reports
		WHERE meter_kind=$1 AND identifier=$2
	`, meterKind, identifier).Scan(&status, &storedValue, &storedTimestamp); err != nil {
		return false, err
	}
	if status == "reported" {
		return false, nil
	}
	if storedValue != expectedValue {
		return false, fmt.Errorf("existing pending report value %q differs from recomputed value %q", storedValue, expectedValue)
	}
	if storedTimestamp == nil || !storedTimestamp.Equal(eventAt) {
		return false, fmt.Errorf("existing report timestamp differs from recomputed timestamp")
	}
	return false, fmt.Errorf("%w requires Stripe reconciliation", errAmbiguousMeterReport)
}

func markMeterReportReported(ctx context.Context, pool *pgxpool.Pool, accountID int64, periodEnd time.Time, meterKind string) error {
	result, err := pool.Exec(ctx, `
		UPDATE billing_meter_reports
		SET status='reported',reported_at=now()
		WHERE account_id=$1 AND period_end=$2 AND meter_kind=$3 AND status='pending'
	`, accountID, periodEnd, meterKind)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("pending report row not found")
	}
	return nil
}

func reconstructStorageDailyFacts(ctx context.Context, pool *pgxpool.Pool, accountID int64, start, end time.Time) error {
	_, err := pool.Exec(ctx, `
		WITH days AS (
		  SELECT (($2::timestamptz AT TIME ZONE 'UTC')::date + n)::date AS day_date
		  FROM generate_series(0, (($3::timestamptz AT TIME ZONE 'UTC')::date - ($2::timestamptz AT TIME ZONE 'UTC')::date) - 1) n
		)
		INSERT INTO billing_storage_daily_facts(account_id,usage_date,stream_hours_stored)
		SELECT $1, day_date,
		       COALESCE((
		         SELECT SUM(GREATEST(EXTRACT(EPOCH FROM (c.clip_end_at-c.clip_start_at)),0)/3600.0)
		         FROM recording_clips c
		         JOIN recordings r ON r.id=c.recording_id
		         JOIN clip_storage_billing_contracts bc ON bc.clip_id=c.id
		         WHERE r.account_id=$1 AND bc.mode <> 'excluded'
		           AND c.created_at < ((day_date + 1)::timestamp AT TIME ZONE 'UTC')
		           AND (c.released_at IS NULL OR c.released_at >= ((day_date + 1)::timestamp AT TIME ZONE 'UTC'))
		           AND (c.purged_at IS NULL OR c.purged_at >= ((day_date + 1)::timestamp AT TIME ZONE 'UTC'))
		           AND (bc.mode <> 'nas_pull_monthly' OR c.created_at < ((day_date + 1)::timestamp AT TIME ZONE 'UTC') - $4::interval)
		       ),0)
		FROM days
		WHERE NOT EXISTS (SELECT 1 FROM billing_storage_daily_facts f WHERE f.account_id=$1 AND f.usage_date=day_date)
		ON CONFLICT(account_id,usage_date) DO NOTHING
	`, accountID, start.UTC(), end.UTC(), nasStagingGrace)
	if err != nil {
		return fmt.Errorf("reconstruct storage daily facts: %w", err)
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
//
// yearly_prepaid recordings are EXCLUDED: their storage is prepaid up front (a
// standalone half-rate batch), so metering them here too would double-charge. Only
// monthly-tier managed footage is metered.
//
// nas_pull clips only STAGE in managed storage while the NAS pulls them, so they get
// a nasStagingGrace free window: a nas_pull clip is counted only once it is still
// staged past the grace (NAS down or falling behind). Managed clips are counted from
// creation, unchanged.
const snapshotManagedStorageSQL = `
	WITH totals AS (
		SELECT r.account_id,
		       COALESCE(SUM(c.size_bytes), 0) AS bytes_stored,
		       COALESCE(SUM(GREATEST(EXTRACT(EPOCH FROM (c.clip_end_at - c.clip_start_at)), 0) / 3600.0), 0) AS stream_hours_stored
		FROM recording_clips c
		JOIN recordings r            ON r.id = c.recording_id
		JOIN storage_destinations sd ON sd.id = c.storage_destination_id
		WHERE sd.managed AND c.purged_at IS NULL AND c.released_at IS NULL
		  AND r.storage_retention_tier <> 'yearly_prepaid'
		  AND (r.delivery <> 'nas_pull' OR c.created_at < now() - ($1::interval))
		GROUP BY r.account_id
	)
	INSERT INTO account_storage_snapshots (account_id, snapshot_date, bytes_stored, stream_hours_stored)
	SELECT ab.account_id, CURRENT_DATE,
	       COALESCE(t.bytes_stored, 0), COALESCE(t.stream_hours_stored, 0)
	FROM account_billing ab
	LEFT JOIN totals t USING(account_id)
	WHERE ab.stripe_subscription_id IS NOT NULL
	ON CONFLICT (account_id, snapshot_date)
	DO UPDATE SET bytes_stored = EXCLUDED.bytes_stored,
	              stream_hours_stored = EXCLUDED.stream_hours_stored
`

// snapshotManagedStorage runs the nightly managed-storage rollup (see
// snapshotManagedStorageSQL).
func snapshotManagedStorage(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, snapshotManagedStorageSQL, nasStagingGrace); err != nil {
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
	return periodEnd.UTC().Format("20060102T150405Z")
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
			SELECT COALESCE(SUM(GREATEST(EXTRACT(EPOCH FROM (c.clip_end_at - c.clip_start_at)), 0) / 3600.0), 0) AS hours
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
