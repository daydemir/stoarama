package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

// handleAccountBillingMe returns the account's billing summary, with safe
// defaults when no account_billing row exists yet.
func (s *Server) handleAccountBillingMe(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var (
		customerID         *string
		subscriptionStatus string
		paidQuantity       int
		currentPeriodEnd   *time.Time
		cancelAtPeriodEnd  bool
	)
	err := s.pool.QueryRow(r.Context(), `
		SELECT stripe_customer_id, subscription_status, paid_quantity, current_period_end, cancel_at_period_end
		FROM account_billing
		WHERE account_id=$1
	`, principal.AccountID).Scan(&customerID, &subscriptionStatus, &paidQuantity, &currentPeriodEnd, &cancelAtPeriodEnd)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteJSON(w, http.StatusOK, map[string]any{
				"billing_enabled":      s.billing != nil,
				"has_customer":         false,
				"subscription_status":  "none",
				"paid_quantity":        0,
				"current_period_end":   nil,
				"cancel_at_period_end": false,
			})
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load billing: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"billing_enabled":      s.billing != nil,
		"has_customer":         customerID != nil && strings.TrimSpace(*customerID) != "",
		"subscription_status":  subscriptionStatus,
		"paid_quantity":        paidQuantity,
		"current_period_end":   currentPeriodEnd,
		"cancel_at_period_end": cancelAtPeriodEnd,
	})
}

// handleAccountBillingCheckout opens Stripe Checkout for the account, serializing
// per-account via a FOR UPDATE lock so concurrent clicks cannot mint two
// customers or two subscriptions. If a subscription already exists it returns the
// portal URL instead of a second Checkout.
func (s *Server) handleAccountBillingCheckout(w http.ResponseWriter, r *http.Request) {
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
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin checkout tx: %v", err))
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
		customerID         *string
		subscriptionID     *string
		subscriptionStatus string
	)
	if err := tx.QueryRow(r.Context(), `
		SELECT stripe_customer_id, stripe_subscription_id, subscription_status
		FROM account_billing WHERE account_id=$1 FOR UPDATE
	`, principal.AccountID).Scan(&customerID, &subscriptionID, &subscriptionStatus); err != nil {
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

	hasActiveSub := subscriptionID != nil && strings.TrimSpace(*subscriptionID) != "" &&
		subscriptionStatusGrantsAccess(subscriptionStatus)

	if hasActiveSub {
		// Already subscribed: send them to the portal instead of a 2nd Checkout.
		portalURL, err := s.billing.CreatePortalSession(r.Context(), custID, s.cfg.AppBaseURL+"/recordings")
		if err != nil {
			util.WriteError(w, http.StatusBadGateway, fmt.Sprintf("create portal session: %v", err))
			return
		}
		if err := tx.Commit(r.Context()); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit checkout tx: %v", err))
			return
		}
		util.WriteJSON(w, http.StatusOK, map[string]any{"portal_url": portalURL})
		return
	}

	qty := int64(s.countLiveRecordings(r.Context(), tx, principal.AccountID))
	if qty < 1 {
		qty = 1
	}
	checkoutURL, err := s.billing.CreateCheckoutSession(r.Context(), custID, principal.AccountID, qty)
	if err != nil {
		util.WriteError(w, http.StatusBadGateway, fmt.Sprintf("create checkout session: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit checkout tx: %v", err))
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

// syncSubscriptionQuantity recomputes the absolute live recording count under a
// FOR UPDATE lock and pushes it to Stripe when a subscription item exists. The
// subscription item id is read ONLY from the authed account's billing row
// (never from request input), and a missing billing client / item is a no-op so
// free-mode create/delete still works.
func (s *Server) syncSubscriptionQuantity(ctx context.Context, tx pgx.Tx, accountID int64) error {
	var subItemID *string
	err := tx.QueryRow(ctx, `
		SELECT stripe_subscription_item_id FROM account_billing WHERE account_id=$1 FOR UPDATE
	`, accountID).Scan(&subItemID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("lock billing row for quantity sync: %w", err)
	}
	if subItemID == nil || strings.TrimSpace(*subItemID) == "" {
		return nil
	}
	if s.billing == nil {
		return nil
	}
	qty := int64(s.countLiveRecordings(ctx, tx, accountID))
	if err := s.billing.SetSubscriptionQuantity(ctx, strings.TrimSpace(*subItemID), qty); err != nil {
		return fmt.Errorf("set subscription quantity: %w", err)
	}
	return nil
}

// countLiveRecordings is the absolute seat count: recordings the account intends
// to keep (not canceled). Used as the Stripe quantity everywhere.
func (s *Server) countLiveRecordings(ctx context.Context, q pgxQuerier, accountID int64) int {
	var n int
	if err := q.QueryRow(ctx, `
		SELECT count(*) FROM recordings WHERE account_id=$1 AND status <> 'canceled'
	`, accountID).Scan(&n); err != nil {
		return 0
	}
	return n
}

// recordingIsBillable reports whether a single recording is currently billable
// per the gate view. The scheduler/lease inline this predicate; this exists for
// targeted checks and tests.
func (s *Server) recordingIsBillable(ctx context.Context, accountID, recordingID int64) (bool, error) {
	var billable bool
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(billable, false) FROM recording_billing_state
		WHERE recording_id=$1 AND account_id=$2
	`, recordingID, accountID).Scan(&billable)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return billable, nil
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

// applyStripeEvent re-derives the account's billing state from the event's
// subscription, re-fetching the authoritative subscription object. It resolves
// the account only via the locally-stored customer id (and the checkout
// client_reference_id), never minting a billing row from an arbitrary customer.
func (s *Server) applyStripeEvent(ctx context.Context, tx pgx.Tx, event stripe.Event) error {
	switch event.Type {
	case "checkout.session.completed":
		var sess stripe.CheckoutSession
		if err := json.Unmarshal(event.Data.Raw, &sess); err != nil {
			return fmt.Errorf("decode checkout session: %w", err)
		}
		accountID, err := s.resolveBillingAccount(ctx, tx, customerIDOf(sess.Customer), clientRefAccountID(sess.ClientReferenceID))
		if err != nil || accountID == 0 {
			return err
		}
		if sess.Subscription == nil || strings.TrimSpace(sess.Subscription.ID) == "" {
			return nil
		}
		return s.syncBillingFromSubscription(ctx, tx, accountID, sess.Subscription.ID, eventCreated(event))

	case "customer.subscription.created", "customer.subscription.updated", "customer.subscription.deleted":
		var sub stripe.Subscription
		if err := json.Unmarshal(event.Data.Raw, &sub); err != nil {
			return fmt.Errorf("decode subscription: %w", err)
		}
		accountID, err := s.resolveBillingAccount(ctx, tx, customerIDOf(sub.Customer), 0)
		if err != nil || accountID == 0 {
			return err
		}
		return s.syncBillingFromSubscription(ctx, tx, accountID, sub.ID, eventCreated(event))

	case "invoice.paid", "invoice.payment_failed":
		var inv stripe.Invoice
		if err := json.Unmarshal(event.Data.Raw, &inv); err != nil {
			return fmt.Errorf("decode invoice: %w", err)
		}
		subID := invoiceSubscriptionID(&inv)
		if subID == "" {
			return nil
		}
		accountID, err := s.resolveBillingAccount(ctx, tx, customerIDOf(inv.Customer), 0)
		if err != nil || accountID == 0 {
			return err
		}
		if event.Type == "invoice.payment_failed" {
			if _, err := tx.Exec(ctx, `
				UPDATE account_billing SET last_payment_failed_at=now(), updated_at=now() WHERE account_id=$1
			`, accountID); err != nil {
				return fmt.Errorf("mark payment failed: %w", err)
			}
		}
		return s.syncBillingFromSubscription(ctx, tx, accountID, subID, eventCreated(event))

	default:
		// Unhandled event types are acknowledged (already deduped/stored).
		return nil
	}
}

// syncBillingFromSubscription re-fetches the subscription from Stripe and writes
// status/quantity/period to account_billing, guarded so a stale out-of-order
// event never overwrites newer state (last_event_at monotonic).
func (s *Server) syncBillingFromSubscription(ctx context.Context, tx pgx.Tx, accountID int64, subID string, eventAt time.Time) error {
	sub, err := s.billing.GetSubscription(ctx, subID)
	if err != nil {
		return err
	}
	status := string(sub.Status)
	if !validSubscriptionStatus(status) {
		status = "none"
	}
	itemID, quantity, periodEnd := recorderLineItem(sub, s.cfg.StripePriceID)
	var periodEndArg any
	if !periodEnd.IsZero() {
		periodEndArg = periodEnd
	}
	var itemIDArg any
	if strings.TrimSpace(itemID) != "" {
		itemIDArg = itemID
	}
	var eventAtArg any
	if !eventAt.IsZero() {
		eventAtArg = eventAt
	}

	if _, err := tx.Exec(ctx, `
		UPDATE account_billing
		SET stripe_subscription_id=$2,
		    stripe_subscription_item_id=COALESCE($3, stripe_subscription_item_id),
		    subscription_status=$4,
		    paid_quantity=$5,
		    current_period_end=$6,
		    cancel_at_period_end=$7,
		    last_event_at=GREATEST(COALESCE(last_event_at, 'epoch'::timestamptz), COALESCE($8::timestamptz, now())),
		    updated_at=now()
		WHERE account_id=$1
		  AND ($8::timestamptz IS NULL OR last_event_at IS NULL OR $8::timestamptz >= last_event_at)
	`, accountID, sub.ID, itemIDArg, status, quantity, periodEndArg, subscriptionCancelAtPeriodEnd(sub), eventAtArg); err != nil {
		return fmt.Errorf("upsert account billing: %w", err)
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

func subscriptionStatusGrantsAccess(status string) bool {
	switch strings.TrimSpace(status) {
	case "active", "trialing", "past_due":
		return true
	default:
		return false
	}
}

func validSubscriptionStatus(status string) bool {
	switch strings.TrimSpace(status) {
	case "none", "incomplete", "trialing", "active", "past_due", "canceled", "unpaid", "incomplete_expired":
		return true
	default:
		return false
	}
}

// recorderLineItem returns the subscription item id, quantity, and period end
// for the recorder price line. In stripe-go v82, current_period_end lives on the
// subscription item, not the subscription.
func recorderLineItem(sub *stripe.Subscription, priceID string) (string, int, time.Time) {
	if sub == nil || sub.Items == nil {
		return "", 0, time.Time{}
	}
	priceID = strings.TrimSpace(priceID)
	for _, item := range sub.Items.Data {
		if item == nil {
			continue
		}
		if priceID != "" && (item.Price == nil || item.Price.ID != priceID) {
			continue
		}
		var periodEnd time.Time
		if item.CurrentPeriodEnd > 0 {
			periodEnd = time.Unix(item.CurrentPeriodEnd, 0).UTC()
		}
		return item.ID, int(item.Quantity), periodEnd
	}
	// Fall back to the first item if the price id did not match (e.g. price
	// renamed); period end still comes from that item.
	for _, item := range sub.Items.Data {
		if item == nil {
			continue
		}
		var periodEnd time.Time
		if item.CurrentPeriodEnd > 0 {
			periodEnd = time.Unix(item.CurrentPeriodEnd, 0).UTC()
		}
		return item.ID, int(item.Quantity), periodEnd
	}
	return "", 0, time.Time{}
}

func subscriptionCancelAtPeriodEnd(sub *stripe.Subscription) bool {
	if sub == nil {
		return false
	}
	return sub.CancelAtPeriodEnd
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

func eventCreated(event stripe.Event) time.Time {
	if event.Created <= 0 {
		return time.Time{}
	}
	return time.Unix(event.Created, 0).UTC()
}

func invoiceSubscriptionID(inv *stripe.Invoice) string {
	if inv == nil {
		return ""
	}
	if inv.Parent != nil && inv.Parent.SubscriptionDetails != nil && inv.Parent.SubscriptionDetails.Subscription != nil {
		return strings.TrimSpace(inv.Parent.SubscriptionDetails.Subscription.ID)
	}
	return ""
}
