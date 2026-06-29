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

	"github.com/daydemir/stoarama/backend/internal/util"
)

// stripeWebhookMaxBody bounds the webhook request body (S-DoS); Stripe events
// are far smaller than this.
const stripeWebhookMaxBody = 64 * 1024

// recordingDayRateCents is the per-recording-day usage price (50 cents); the live
// UI estimate is days * this. The authoritative charge is Stripe's meter sum.
const recordingDayRateCents = 50

// gbMonthRateCents is the managed-storage price (10 cents per GB-month); the live
// UI estimate is avg_gb * this. The authoritative charge is Stripe's gb_month meter.
const gbMonthRateCents = 10

// handleAccountBillingMe returns the account's usage-billing summary: whether a
// card is on file plus a live (DB-derived, never Stripe) estimate of this
// calendar month's recording-days and dollar amount. Stripe meter aggregation is
// async, so the UI estimate always reads our own recording_billing_days view.
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

	// Live month-to-date estimate from our own ledger (never Stripe summaries).
	var days int
	if err := s.pool.QueryRow(r.Context(), `
		SELECT count(*) FROM recording_billing_days
		WHERE account_id=$1 AND rec_day >= date_trunc('month', now() AT TIME ZONE 'UTC')::date
	`, principal.AccountID).Scan(&days); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("count recording days: %v", err))
		return
	}

	// Month-to-date average GB of managed storage, from our own daily snapshots.
	// avg_gb = SUM(bytes_stored)/count(snapshot days)/1e9 across the current UTC
	// month; 0 when there are no snapshots. Mirrors the period-average the
	// gb_month meter reports at period close.
	var snapBytes int64
	var snapDays int
	if err := s.pool.QueryRow(r.Context(), `
		SELECT COALESCE(SUM(bytes_stored),0), COUNT(*)
		FROM account_storage_snapshots
		WHERE account_id=$1 AND snapshot_date >= date_trunc('month', now() AT TIME ZONE 'UTC')::date
	`, principal.AccountID).Scan(&snapBytes, &snapDays); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load storage snapshots: %v", err))
		return
	}
	avgGB := 0.0
	if snapDays > 0 {
		avgGB = float64(snapBytes) / float64(snapDays) / 1e9
	}

	// Best-effort billing-period context for the account view. Null when there is
	// no subscription yet or Stripe is unreachable; never fails the response.
	var periodStart, periodEnd *string
	if s.billing != nil {
		var subID *string
		_ = s.pool.QueryRow(r.Context(), `
			SELECT stripe_subscription_id FROM account_billing WHERE account_id=$1
		`, principal.AccountID).Scan(&subID)
		if subID != nil && strings.TrimSpace(*subID) != "" {
			if start, end, err := s.billing.GetSubscriptionPeriod(r.Context(), strings.TrimSpace(*subID)); err == nil {
				if !start.IsZero() {
					v := start.Format(time.RFC3339)
					periodStart = &v
				}
				if !end.IsZero() {
					v := end.Format(time.RFC3339)
					periodEnd = &v
				}
			}
		}
	}

	// Managed storage is offered only when billing is on AND the operator R2 is
	// configured; otherwise the UI shows BYO only and the provision endpoint 503s.
	managedAvailable := s.billing != nil && s.r2 != nil && s.cfg.ValidateR2() == nil

	util.WriteJSON(w, http.StatusOK, map[string]any{
		"billing_enabled":                s.billing != nil,
		"has_customer":                   customerID != nil && strings.TrimSpace(*customerID) != "",
		"has_payment_method":             hasPaymentMethod,
		"recording_days_this_month":      days,
		"estimated_amount_cents":         days * recordingDayRateCents,
		"managed_available":              managedAvailable,
		"storage_gb_month_avg":           strconv.FormatFloat(avgGB, 'f', 3, 64),
		"estimated_storage_amount_cents": int64(math.Round(avgGB * float64(gbMonthRateCents))),
		"period_start":                   periodStart,
		"period_end":                     periodEnd,
		"recording_day_rate_cents":       recordingDayRateCents,
		"gb_month_rate_cents":            gbMonthRateCents,
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
