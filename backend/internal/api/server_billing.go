package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	stripe "github.com/stripe/stripe-go/v82"

	"github.com/daydemir/stoarama/backend/internal/billing"
	"github.com/daydemir/stoarama/backend/internal/util"
)

// stripeWebhookMaxBody bounds the webhook request body (S-DoS); Stripe events
// are far smaller than this.
const stripeWebhookMaxBody = 64 * 1024

// recordingHourRateCents is the per-recording-hour usage price (5 cents); the live
// UI estimate is hours * this. The authoritative charge is Stripe's meter sum.
const recordingHourRateCents = 5

// streamHourMonthRateCents is the managed-storage price (10 cents per
// stream-hour-month); the live UI estimate is avg_stream_hours * this. The
// authoritative charge is Stripe's stream_hour_month meter.
const streamHourMonthRateCents = 10

// handleAccountBillingMe returns the account's usage-billing summary: whether a
// card is on file, a live (DB-derived, never Stripe) measurement of the current
// billing period's recording-hours and dollar amount TO DATE, and a forward
// PROJECTION of the total through period end (measured recording-hours plus the
// expected additional record-hours of active/scheduled recordings). Stripe meter
// aggregation is async, so every figure reads our own ledger, never a Stripe
// summary. The projection is display-only: it never changes what is charged.
func (s *Server) handleAccountBillingMe(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var (
		customerID       *string
		hasPaymentMethod bool
	)
	err := s.pool.QueryRow(r.Context(), `
		SELECT stripe_customer_id, has_payment_method
		FROM account_billing
		WHERE account_id=$1
	`, principal.AccountID).Scan(&customerID, &hasPaymentMethod)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load billing: %v", err))
		return
	}

	// Resolve the billing window ONCE: the Stripe subscription period when
	// available (so to-date measurement and forward projection align with the real
	// invoice), else the current UTC calendar month. A Stripe error never
	// hard-fails the endpoint; it falls back to the calendar month, same as the
	// best-effort period display always has.
	now := time.Now().UTC()
	winStart := now.Truncate(time.Hour)
	winStart = time.Date(winStart.Year(), winStart.Month(), 1, 0, 0, 0, 0, time.UTC)
	winEnd := winStart.AddDate(0, 1, 0)
	var periodStart, periodEnd *string
	if s.billing != nil {
		var subID *string
		_ = s.pool.QueryRow(r.Context(), `
			SELECT stripe_subscription_id FROM account_billing WHERE account_id=$1
		`, principal.AccountID).Scan(&subID)
		if subID != nil && strings.TrimSpace(*subID) != "" {
			if start, end, err := s.billing.GetSubscriptionPeriod(r.Context(), strings.TrimSpace(*subID)); err == nil {
				if !start.IsZero() && !end.IsZero() && end.After(start) {
					winStart = start.UTC()
					winEnd = end.UTC()
				}
				if !start.IsZero() {
					v := start.UTC().Format(time.RFC3339)
					periodStart = &v
				}
				if !end.IsZero() {
					v := end.UTC().Format(time.RFC3339)
					periodEnd = &v
				}
			}
		}
	}

	// Recording-hours measured TO DATE, over the resolved window [winStart, winEnd),
	// from our own ledger (never Stripe summaries). Same [start,end) bind meterAccount
	// uses when it reports the period.
	var hours int
	if err := s.pool.QueryRow(r.Context(), `
		SELECT count(*) FROM recording_billing_hours
		WHERE account_id=$1 AND rec_hour >= $2 AND rec_hour < $3
	`, principal.AccountID, winStart, winEnd).Scan(&hours); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("count recording hours: %v", err))
		return
	}

	// Average stored stream-hours of managed footage over the window, from our own
	// daily snapshots. avg = SUM(stream_hours_stored)/count(snapshot days) across
	// the window; 0 when there are no snapshots. Mirrors the period-average the
	// stream_hour_month meter reports at period close. Storage stays TO DATE
	// measured (v1 does not forward-project storage).
	var snapHours float64
	var snapDays int
	if err := s.pool.QueryRow(r.Context(), `
		SELECT COALESCE(SUM(stream_hours_stored),0), COUNT(*)
		FROM account_storage_snapshots
		WHERE account_id=$1 AND snapshot_date >= $2::date AND snapshot_date < $3::date
	`, principal.AccountID, winStart, winEnd).Scan(&snapHours, &snapDays); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load storage snapshots: %v", err))
		return
	}
	avgStreamHours := 0.0
	if snapDays > 0 {
		avgStreamHours = snapHours / float64(snapDays)
	}

	// Forward-project the expected ADDITIONAL recording-hours of active recordings
	// from now to winEnd, mirroring the composer's estimate math (v1: recording
	// hours only; storage stays to-date). Only status='active' recordings project;
	// paused/canceled/completed contribute nothing.
	projectedHours, err := s.projectAccountRecordingHours(r.Context(), principal.AccountID, now, winEnd)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("project recording hours: %v", err))
		return
	}
	projectedHoursInt := int(math.Round(projectedHours))
	projectedRecordingCents := projectedHoursInt * recordingHourRateCents
	toDateRecordingCents := hours * recordingHourRateCents
	toDateStorageCents := int(math.Round(avgStreamHours * float64(streamHourMonthRateCents)))
	projectedTotalCents := toDateRecordingCents + projectedRecordingCents + toDateStorageCents

	// Managed storage is offered only when billing is on AND the operator R2 is
	// configured; otherwise the UI shows BYO only and the provision endpoint 503s.
	managedAvailable := s.billing != nil && s.r2 != nil && s.cfg.ValidateR2() == nil

	util.WriteJSON(w, http.StatusOK, map[string]any{
		"billing_enabled":                  s.billing != nil,
		"has_customer":                     customerID != nil && strings.TrimSpace(*customerID) != "",
		"has_payment_method":               hasPaymentMethod,
		"recording_hours_this_month":       hours,
		"estimated_amount_cents":           toDateRecordingCents,
		"managed_available":                managedAvailable,
		"storage_stream_hour_month_avg":    strconv.FormatFloat(avgStreamHours, 'f', 3, 64),
		"estimated_storage_amount_cents":   int64(toDateStorageCents),
		"projected_recording_hours":        projectedHoursInt,
		"projected_recording_amount_cents": int64(projectedRecordingCents),
		"projected_total_amount_cents":     int64(projectedTotalCents),
		"is_projection":                    true,
		"period_start":                     periodStart,
		"period_end":                       periodEnd,
		"recording_hour_rate_cents":        recordingHourRateCents,
		"stream_hour_month_rate_cents":     streamHourMonthRateCents,
	})
}

// projectAccountRecordingHours sums the forward-projected additional
// record-hours across the account's status='active' recordings for the window
// [now, winEnd). Each recording's projection uses its own stored schedule
// (mode/cron/timezone/daily-window/start/stop) via projectRecordingHours;
// paused/canceled/completed recordings are excluded by the WHERE clause. This is
// a display-only forecast and never touches metering or Stripe.
func (s *Server) projectAccountRecordingHours(ctx context.Context, accountID int64, now, winEnd time.Time) (float64, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT mode, COALESCE(cron_expr,''), COALESCE(cron_timezone,''),
		       COALESCE(to_char(daily_window_start,'HH24:MI:SS'),''),
		       COALESCE(to_char(daily_window_end,'HH24:MI:SS'),''),
		       start_at, end_at, active_weekdays
		FROM recordings
		WHERE account_id=$1 AND status='active'
	`, accountID)
	if err != nil {
		return 0, fmt.Errorf("load active recordings: %w", err)
	}
	defer rows.Close()

	var total float64
	for rows.Next() {
		var (
			rec     projectedRecording
			startAt time.Time
			endAt   *time.Time
		)
		if err := rows.Scan(&rec.Mode, &rec.CronExpr, &rec.CronTimezone, &rec.DailyStart, &rec.DailyEnd, &startAt, &endAt, &rec.ActiveWeekdays); err != nil {
			return 0, fmt.Errorf("scan active recording: %w", err)
		}
		rec.StartAt = startAt.UTC()
		if endAt != nil {
			e := endAt.UTC()
			rec.EndAt = &e
		}
		total += projectRecordingHours(rec, now, winEnd)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate active recordings: %w", err)
	}
	return total, nil
}

// handleAccountBillingInvoices returns the account's past charges (Stripe
// invoices, newest first) for the billing-history panel. The service bills
// monthly in arrears and is new, so an account legitimately has zero invoices:
// this returns an empty items array (never mock data) so the UI can render an
// honest empty state. has_customer=false means no Stripe customer exists yet.
func (s *Server) handleAccountBillingInvoices(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if s.billing == nil {
		util.WriteJSON(w, http.StatusOK, map[string]any{"billing_enabled": false, "has_customer": false, "items": []any{}})
		return
	}
	var customerID *string
	err := s.pool.QueryRow(r.Context(), `
		SELECT stripe_customer_id FROM account_billing WHERE account_id=$1
	`, principal.AccountID).Scan(&customerID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load billing: %v", err))
		return
	}
	custID := ""
	if customerID != nil {
		custID = strings.TrimSpace(*customerID)
	}
	if custID == "" {
		// No Stripe customer yet (no card ever added): honestly no charges.
		util.WriteJSON(w, http.StatusOK, map[string]any{"billing_enabled": true, "has_customer": false, "items": []any{}})
		return
	}
	invoices, err := s.billing.ListInvoices(r.Context(), custID, 12)
	if err != nil {
		util.WriteError(w, http.StatusBadGateway, fmt.Sprintf("list invoices: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"billing_enabled": true,
		"has_customer":    true,
		"items":           invoices,
	})
}

// handleAccountBillingCard returns the URL the account should be sent to manage
// its card on file, serializing per-account via a FOR UPDATE lock so concurrent
// clicks cannot mint two Stripe customers. If a card is already on file it returns
// the Stripe customer portal; otherwise it opens a $0 card-on-file Checkout (which
// creates the metered subscription and saves the card). Starting a recording never
// charges; this endpoint is only ever about the card.
func (s *Server) handleAccountBillingCard(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if !principalCanManageBilling(principal) {
		util.WriteError(w, http.StatusForbidden, "only an org owner or billing admin can manage billing")
		return
	}
	if s.billing == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "billing is not enabled")
		return
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin card tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	if _, err := tx.Exec(r.Context(), `
		INSERT INTO account_billing (account_id) VALUES ($1) ON CONFLICT (account_id) DO NOTHING
	`, principal.AccountID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("ensure billing row: %v", err))
		return
	}

	var (
		customerID       *string
		hasPaymentMethod bool
	)
	if err := tx.QueryRow(r.Context(), `
		SELECT stripe_customer_id, has_payment_method
		FROM account_billing WHERE account_id=$1 FOR UPDATE
	`, principal.AccountID).Scan(&customerID, &hasPaymentMethod); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("lock billing row: %v", err))
		return
	}

	custID := ""
	if customerID != nil {
		custID = strings.TrimSpace(*customerID)
	}
	if custID == "" {
		custID, err = s.billing.EnsureCustomer(r.Context(), principal.AccountID, principal.Email)
		if err != nil {
			util.WriteError(w, http.StatusBadGateway, fmt.Sprintf("ensure stripe customer: %v", err))
			return
		}
		if _, err := tx.Exec(r.Context(), `
			UPDATE account_billing SET stripe_customer_id=$2, updated_at=now() WHERE account_id=$1
		`, principal.AccountID, custID); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("store stripe customer: %v", err))
			return
		}
	}

	if hasPaymentMethod {
		// Card already on file: send them to the portal to update/remove it.
		portalURL, err := s.billing.CreatePortalSession(r.Context(), custID, s.cfg.AppBaseURL+"/recordings")
		if err != nil {
			util.WriteError(w, http.StatusBadGateway, fmt.Sprintf("create portal session: %v", err))
			return
		}
		if err := tx.Commit(r.Context()); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit card tx: %v", err))
			return
		}
		util.WriteJSON(w, http.StatusOK, map[string]any{"portal_url": portalURL})
		return
	}

	checkoutURL, err := s.billing.CreateCardOnFileCheckoutSession(r.Context(), custID, principal.AccountID)
	if err != nil {
		util.WriteError(w, http.StatusBadGateway, fmt.Sprintf("create card checkout session: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit card tx: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"checkout_url": checkoutURL})
}

// handleAccountBillingPortal opens the Stripe customer portal for the account.
func (s *Server) handleAccountBillingPortal(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if !principalCanManageBilling(principal) {
		util.WriteError(w, http.StatusForbidden, "only an org owner or billing admin can manage billing")
		return
	}
	if s.billing == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "billing is not enabled")
		return
	}
	var customerID *string
	err := s.pool.QueryRow(r.Context(), `
		SELECT stripe_customer_id FROM account_billing WHERE account_id=$1
	`, principal.AccountID).Scan(&customerID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load billing: %v", err))
		return
	}
	if customerID == nil || strings.TrimSpace(*customerID) == "" {
		util.WriteError(w, http.StatusConflict, "no Stripe customer for this account yet")
		return
	}
	url, err := s.billing.CreatePortalSession(r.Context(), strings.TrimSpace(*customerID), s.cfg.AppBaseURL+"/recordings")
	if err != nil {
		util.WriteError(w, http.StatusBadGateway, fmt.Sprintf("create portal session: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"url": url})
}

// handleStripeWebhook verifies the Stripe signature, dedups the event, and
// upserts account_billing in a single all-or-nothing transaction. It never
// trusts an unsigned payload (S-5) and never touches recordings.status.
func (s *Server) handleStripeWebhook(w http.ResponseWriter, r *http.Request) {
	if s.billing == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "billing is not enabled")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, stripeWebhookMaxBody))
	if err != nil {
		util.WriteError(w, http.StatusRequestEntityTooLarge, "webhook body too large")
		return
	}
	event, err := s.billing.ConstructEvent(body, r.Header.Get("Stripe-Signature"), s.cfg.StripeWebhookSecret)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid stripe signature")
		return
	}
	if event.Livemode != s.cfg.StripeLivemode {
		util.WriteError(w, http.StatusBadRequest, "stripe livemode mismatch")
		return
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin webhook tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	// Dedup: insert the event id; a no-op insert means we already handled it.
	var eventRowID int64
	err = tx.QueryRow(r.Context(), `
		INSERT INTO stripe_webhook_events (provider_event_id, event_type, payload_jsonb)
		VALUES ($1,$2,$3)
		ON CONFLICT (provider_event_id) DO NOTHING
		RETURNING id
	`, event.ID, string(event.Type), string(body)).Scan(&eventRowID)
	if errors.Is(err, pgx.ErrNoRows) {
		// Duplicate event: nothing to do, succeed so Stripe stops retrying.
		util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true, "duplicate": true})
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("record webhook event: %v", err))
		return
	}

	if err := s.applyStripeEvent(r.Context(), tx, event); err != nil {
		// Roll back including the event row so Stripe retries.
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("apply webhook event: %v", err))
		return
	}

	if _, err := tx.Exec(r.Context(), `
		UPDATE stripe_webhook_events SET processed_at=now() WHERE id=$1
	`, eventRowID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("mark webhook processed: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit webhook tx: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// applyStripeEvent maps the usage-billing webhook events onto account_billing,
// resolving the account only via the locally-stored customer id (and the checkout
// client_reference_id), never minting a billing row from an arbitrary customer.
//
//   - checkout.session.completed (mode=subscription): the card was saved; flip
//     has_payment_method=true and store the subscription id + default payment
//     method id so the nightly metering job and the portal have them.
//   - invoice.paid / invoice.payment_failed: track last_payment_failed_at only
//     (set on failure, cleared on payment). No quantity/period writes; Stripe owns
//     the usage math.
//   - payment_method.detached / customer.subscription.deleted: the card is gone;
//     flip has_payment_method=false so capture is gated again.
func (s *Server) applyStripeEvent(ctx context.Context, tx pgx.Tx, event stripe.Event) error {
	switch event.Type {
	case "checkout.session.completed":
		var sess stripe.CheckoutSession
		if err := json.Unmarshal(event.Data.Raw, &sess); err != nil {
			return fmt.Errorf("decode checkout session: %w", err)
		}
		if sess.Mode != stripe.CheckoutSessionModeSubscription {
			return nil
		}
		accountID, err := s.resolveBillingAccount(ctx, tx, customerIDOf(sess.Customer), clientRefAccountID(sess.ClientReferenceID))
		if err != nil || accountID == 0 {
			return err
		}
		subID := ""
		if sess.Subscription != nil {
			subID = strings.TrimSpace(sess.Subscription.ID)
		}
		var subIDArg any
		if subID != "" {
			subIDArg = subID
		}
		pmID := checkoutDefaultPaymentMethodID(&sess)
		var pmIDArg any
		if pmID != "" {
			pmIDArg = pmID
		}
		if _, err := tx.Exec(ctx, `
			UPDATE account_billing
			SET has_payment_method=true,
			    stripe_subscription_id=COALESCE($2, stripe_subscription_id),
			    stripe_default_payment_method_id=COALESCE($3, stripe_default_payment_method_id),
			    last_event_at=GREATEST(COALESCE(last_event_at, 'epoch'::timestamptz), COALESCE($4::timestamptz, now())),
			    updated_at=now()
			WHERE account_id=$1
		`, accountID, subIDArg, pmIDArg, eventCreatedArg(event)); err != nil {
			return fmt.Errorf("apply checkout completed: %w", err)
		}
		return nil

	case "invoice.paid", "invoice.payment_failed":
		var inv stripe.Invoice
		if err := json.Unmarshal(event.Data.Raw, &inv); err != nil {
			return fmt.Errorf("decode invoice: %w", err)
		}
		accountID, err := s.resolveBillingAccount(ctx, tx, customerIDOf(inv.Customer), 0)
		if err != nil || accountID == 0 {
			return err
		}
		var failedAtExpr string
		if event.Type == "invoice.payment_failed" {
			failedAtExpr = "now()"
		} else {
			failedAtExpr = "NULL"
		}
		// Gate on event ordering so a redelivered/out-of-order invoice event cannot
		// overwrite newer state.
		if _, err := tx.Exec(ctx, `
			UPDATE account_billing
			SET last_payment_failed_at=`+failedAtExpr+`,
			    last_event_at=GREATEST(COALESCE(last_event_at, 'epoch'::timestamptz), COALESCE($2::timestamptz, now())),
			    updated_at=now()
			WHERE account_id=$1
			  AND ($2::timestamptz IS NULL OR last_event_at IS NULL OR $2::timestamptz >= last_event_at)
		`, accountID, eventCreatedArg(event)); err != nil {
			return fmt.Errorf("apply invoice event: %w", err)
		}
		// Yearly-prepaid: a paid standalone prepay invoice is the trigger to grant the
		// prepaid storage credit. This is atomic with the webhook dedup: it runs inside
		// the same tx, so either the credit grant is created AND the ledger row moves
		// charged->granted, or the whole webhook rolls back and Stripe retries.
		if event.Type == "invoice.paid" {
			if err := s.grantPrepaidCreditForInvoice(ctx, tx, accountID, customerIDOf(inv.Customer), &inv); err != nil {
				return fmt.Errorf("grant prepaid credit: %w", err)
			}
		}
		return nil

	case "payment_method.detached", "customer.subscription.deleted":
		var customerID string
		switch event.Type {
		case "payment_method.detached":
			var pm stripe.PaymentMethod
			if err := json.Unmarshal(event.Data.Raw, &pm); err != nil {
				return fmt.Errorf("decode payment method: %w", err)
			}
			customerID = customerIDOf(pm.Customer)
		default:
			var sub stripe.Subscription
			if err := json.Unmarshal(event.Data.Raw, &sub); err != nil {
				return fmt.Errorf("decode subscription: %w", err)
			}
			customerID = customerIDOf(sub.Customer)
		}
		accountID, err := s.resolveBillingAccount(ctx, tx, customerID, 0)
		if err != nil || accountID == 0 {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE account_billing
			SET has_payment_method=false,
			    last_event_at=GREATEST(COALESCE(last_event_at, 'epoch'::timestamptz), COALESCE($2::timestamptz, now())),
			    updated_at=now()
			WHERE account_id=$1
			  AND ($2::timestamptz IS NULL OR last_event_at IS NULL OR $2::timestamptz >= last_event_at)
		`, accountID, eventCreatedArg(event)); err != nil {
			return fmt.Errorf("clear payment method: %w", err)
		}
		return nil

	default:
		// Unhandled event types are acknowledged (already deduped/stored).
		return nil
	}
}

// invoiceBatchKey pulls the prepay batch_key off a paid invoice, preferring the
// invoice metadata (set by ChargePrepaidBatch) so a batch is resolvable even if the
// invoice id was never persisted. Returns "" for a non-prepay invoice.
func invoiceBatchKey(inv *stripe.Invoice) string {
	if inv == nil {
		return ""
	}
	if inv.Metadata != nil {
		if bk := strings.TrimSpace(inv.Metadata["batch_key"]); bk != "" {
			return bk
		}
	}
	return ""
}

// grantPrepaidCreditForInvoice closes the yearly-prepaid ledger row on the
// invoice.paid webhook. It resolves the paid invoice to its prepaid_storage_batches
// row (by stripe_invoice_id, else by the metadata batch_key), and ONLY when that row
// is in status='charged' transitions it charged->granted (granted_at, expires_at).
// It does NOT mint a Stripe credit grant: yearly_prepaid footage is excluded from the
// stream_hour_month meter (see snapshotManagedStorageSQL), so the prepay invoice is
// the whole charge and there is no metered line to offset -- granting a credit would
// double-benefit the customer. A non-prepay invoice (no matching row) is a no-op; a
// redelivered invoice.paid whose batch is already 'granted' affects 0 rows and is a
// no-op. The whole thing runs in the webhook tx.
func (s *Server) grantPrepaidCreditForInvoice(ctx context.Context, tx pgx.Tx, accountID int64, customerID string, inv *stripe.Invoice) error {
	if s.billing == nil {
		return nil
	}
	invoiceID := ""
	if inv != nil {
		invoiceID = strings.TrimSpace(inv.ID)
	}
	batchKeyMeta := invoiceBatchKey(inv)
	if invoiceID == "" && batchKeyMeta == "" {
		return nil // nothing to match on.
	}

	// Resolve the charged batch for this account. Match on invoice id OR metadata
	// batch_key; require the batch to belong to this account (defense in depth).
	var (
		batchKey     string
		chargedCents int64
		status       string
	)
	err := tx.QueryRow(ctx, `
		SELECT batch_key, charged_cents, status
		FROM prepaid_storage_batches
		WHERE account_id=$1
		  AND ($2 <> '' AND stripe_invoice_id=$2 OR $3 <> '' AND batch_key=$3)
		LIMIT 1
	`, accountID, invoiceID, batchKeyMeta).Scan(&batchKey, &chargedCents, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil // not a prepay invoice we track: leave it alone.
	}
	if err != nil {
		return fmt.Errorf("resolve prepay batch: %w", err)
	}
	if status != "charged" {
		// Already granted (redelivery) or failed/pending: no grant to make here.
		return nil
	}
	if chargedCents <= 0 {
		return fmt.Errorf("prepay batch %s has non-positive charged_cents %d", batchKey, chargedCents)
	}

	// Yearly-prepaid footage is EXCLUDED from the stream_hour_month meter (see
	// snapshotManagedStorageSQL), so there is no metered line to offset: the prepay
	// invoice IS the whole charge. Close the ledger row charged->granted for
	// idempotency (a redelivered invoice.paid then affects 0 rows) but create NO
	// Stripe credit grant, which would hand the customer free money on top of the
	// already-half-price prepay. expires_at still records when the prepaid year lapses.
	expiresAt := time.Now().UTC().AddDate(0, billing.PrepaidCreditMonths, 0)
	if _, err := tx.Exec(ctx, `
		UPDATE prepaid_storage_batches
		SET status='granted', granted_at=now(), expires_at=$2, updated_at=now()
		WHERE batch_key=$1 AND status='charged'
	`, batchKey, expiresAt); err != nil {
		return fmt.Errorf("record granted batch %s: %w", batchKey, err)
	}
	return nil
}

// resolveBillingAccount maps a Stripe customer id to a local account id ONLY via
// the locally-stored account_billing.stripe_customer_id. When a checkout
// client_reference_id is present it is cross-checked: it must match the account
// the customer is already bound to, or (first checkout) it binds the customer to
// that account. Never mints a billing row from an arbitrary customer id.
func (s *Server) resolveBillingAccount(ctx context.Context, tx pgx.Tx, customerID string, clientRefAccountID int64) (int64, error) {
	customerID = strings.TrimSpace(customerID)
	if customerID == "" {
		return 0, nil
	}
	var accountID int64
	err := tx.QueryRow(ctx, `
		SELECT account_id FROM account_billing WHERE stripe_customer_id=$1
	`, customerID).Scan(&accountID)
	if err == nil {
		if clientRefAccountID != 0 && clientRefAccountID != accountID {
			return 0, fmt.Errorf("client_reference_id %d does not match customer's account %d", clientRefAccountID, accountID)
		}
		return accountID, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("resolve billing account: %w", err)
	}
	// No row bound to this customer yet. Only a verified checkout (which carries
	// client_reference_id) may bind the customer to that account's billing row.
	if clientRefAccountID == 0 {
		return 0, nil
	}
	ct, err := tx.Exec(ctx, `
		UPDATE account_billing SET stripe_customer_id=$2, updated_at=now()
		WHERE account_id=$1 AND (stripe_customer_id IS NULL OR stripe_customer_id=$2)
	`, clientRefAccountID, customerID)
	if err != nil {
		return 0, fmt.Errorf("bind customer to account: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return 0, nil
	}
	return clientRefAccountID, nil
}

// pgxQuerier is the subset of pgx used by helpers that must run on either the
// pool or a transaction.
type pgxQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// checkoutDefaultPaymentMethodID reads the saved default payment method off a
// completed Checkout session when Stripe expanded it onto the session's
// subscription. It returns "" when unavailable (the portal/Stripe still hold it);
// has_payment_method does not depend on this id.
func checkoutDefaultPaymentMethodID(sess *stripe.CheckoutSession) string {
	if sess == nil || sess.Subscription == nil {
		return ""
	}
	if pm := sess.Subscription.DefaultPaymentMethod; pm != nil {
		return strings.TrimSpace(pm.ID)
	}
	return ""
}

func customerIDOf(c *stripe.Customer) string {
	if c == nil {
		return ""
	}
	return strings.TrimSpace(c.ID)
}

func clientRefAccountID(ref string) int64 {
	v, err := strconv.ParseInt(strings.TrimSpace(ref), 10, 64)
	if err != nil || v <= 0 {
		return 0
	}
	return v
}

// eventCreatedArg returns the event's created instant as a query arg (nil when
// the event carries no timestamp), so the out-of-order guard can compare it.
func eventCreatedArg(event stripe.Event) any {
	if event.Created <= 0 {
		return nil
	}
	return time.Unix(event.Created, 0).UTC()
}
