// Package billing isolates the Stripe SDK behind a small typed client, the same
// way internal/r2 isolates the S3 SDK. It owns the card-on-file Checkout, the
// customer portal, metered usage reporting (recording-days), reading a
// subscription's billing period, and signature-verified webhook parsing.
//
// Billing model: one metered Subscription per account. Usage is reported as
// Stripe Billing Meter events (event_name "recording_day", value = number of
// billable recording-days in the period); Stripe sums them and bills the saved
// card monthly in arrears. priceID is the meter-backed metered price.
package billing

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	stripe "github.com/stripe/stripe-go/v82"
	"github.com/stripe/stripe-go/v82/client"
	"github.com/stripe/stripe-go/v82/webhook"
)

// recordingDayEventName is the meter's event_name (see the billing-setup meter).
const recordingDayEventName = "recording_day"

// gbMonthEventName is the managed-storage meter's event_name (a SECOND Billing
// Meter, aggregation=SUM, value = average GB stored over the period).
const gbMonthEventName = "gb_month"

// Client wraps a per-instance Stripe API client (no global stripe.Key mutation)
// plus the metered recording-day price id, the metered gb-month (managed storage)
// price id, and the app base URL for redirects.
type Client struct {
	sc             *client.API
	priceID        string
	gbMonthPriceID string
	appBaseURL     string
	livemode       bool
}

// New builds a Stripe client bound to secretKey. priceID is the metered
// recording-day price; gbMonthPriceID is the metered managed-storage ($/GB-month)
// price; appBaseURL is used for Checkout/Portal redirect URLs.
func New(secretKey, priceID, gbMonthPriceID, appBaseURL string, livemode bool) (*Client, error) {
	secretKey = strings.TrimSpace(secretKey)
	if secretKey == "" {
		return nil, fmt.Errorf("stripe secret key is required")
	}
	if strings.TrimSpace(priceID) == "" {
		return nil, fmt.Errorf("stripe price id is required")
	}
	if strings.TrimSpace(gbMonthPriceID) == "" {
		return nil, fmt.Errorf("stripe gb-month price id is required")
	}
	if strings.TrimSpace(appBaseURL) == "" {
		return nil, fmt.Errorf("app base url is required for stripe redirects")
	}
	return &Client{
		sc:             client.New(secretKey, nil),
		priceID:        strings.TrimSpace(priceID),
		gbMonthPriceID: strings.TrimSpace(gbMonthPriceID),
		appBaseURL:     strings.TrimRight(strings.TrimSpace(appBaseURL), "/"),
		livemode:       livemode,
	}, nil
}

// Livemode reports the configured mode; webhook handling rejects events whose
// livemode disagrees with this.
func (c *Client) Livemode() bool { return c.livemode }

// EnsureCustomer returns the Stripe customer id for an account, creating one if
// none exists. It is idempotent: it searches by metadata.account_id before
// creating, so a retry never mints a duplicate customer.
func (c *Client) EnsureCustomer(ctx context.Context, accountID int64, email string) (string, error) {
	search := &stripe.CustomerSearchParams{}
	search.Context = ctx
	search.Query = fmt.Sprintf("metadata['account_id']:'%d'", accountID)
	iter := c.sc.Customers.Search(search)
	if iter.Next() {
		if cust := iter.Customer(); cust != nil && strings.TrimSpace(cust.ID) != "" {
			return cust.ID, nil
		}
	}
	if err := iter.Err(); err != nil {
		return "", fmt.Errorf("search stripe customer: %w", err)
	}

	params := &stripe.CustomerParams{}
	params.Context = ctx
	if e := strings.TrimSpace(email); e != "" {
		params.Email = strPtr(e)
	}
	params.AddMetadata("account_id", fmt.Sprintf("%d", accountID))
	cust, err := c.sc.Customers.New(params)
	if err != nil {
		return "", fmt.Errorf("create stripe customer: %w", err)
	}
	return cust.ID, nil
}

// CreateCardOnFileCheckoutSession opens a $0 metered-subscription Checkout that
// SAVES the card as the customer's default payment method. The metered line has
// no quantity (Stripe rejects a quantity on a metered price); billing_mode is
// flexible so a metered-only subscription owes $0 at creation and no empty
// invoice is finalized. Returns the hosted Checkout URL.
func (c *Client) CreateCardOnFileCheckoutSession(ctx context.Context, customerID string, accountID int64) (string, error) {
	if strings.TrimSpace(customerID) == "" {
		return "", fmt.Errorf("customer id is required")
	}
	params := &stripe.CheckoutSessionParams{
		Mode:                    strPtr(string(stripe.CheckoutSessionModeSubscription)),
		Customer:                strPtr(customerID),
		ClientReferenceID:       strPtr(fmt.Sprintf("%d", accountID)),
		PaymentMethodCollection: strPtr(string(stripe.CheckoutSessionPaymentMethodCollectionAlways)),
		SuccessURL:              strPtr(c.appBaseURL + "/recordings?billing=success"),
		CancelURL:               strPtr(c.appBaseURL + "/recordings?billing=cancel"),
		LineItems: []*stripe.CheckoutSessionLineItemParams{
			{Price: strPtr(c.priceID)},        // recording_day (no Quantity on a metered line)
			{Price: strPtr(c.gbMonthPriceID)}, // gb_month managed storage (no Quantity)
		},
		SubscriptionData: &stripe.CheckoutSessionSubscriptionDataParams{
			BillingMode: &stripe.CheckoutSessionSubscriptionDataBillingModeParams{
				Type: strPtr(string(stripe.SubscriptionBillingModeTypeFlexible)),
			},
			Metadata: map[string]string{"account_id": fmt.Sprintf("%d", accountID)},
		},
	}
	params.Context = ctx
	sess, err := c.sc.CheckoutSessions.New(params)
	if err != nil {
		return "", fmt.Errorf("create card-on-file checkout: %w", err)
	}
	return sess.URL, nil
}

// CreatePortalSession opens the Stripe-hosted customer billing portal.
func (c *Client) CreatePortalSession(ctx context.Context, customerID, returnURL string) (string, error) {
	if strings.TrimSpace(customerID) == "" {
		return "", fmt.Errorf("customer id is required")
	}
	if strings.TrimSpace(returnURL) == "" {
		returnURL = c.appBaseURL + "/recordings"
	}
	params := &stripe.BillingPortalSessionParams{
		Customer:  strPtr(customerID),
		ReturnURL: strPtr(returnURL),
	}
	params.Context = ctx
	sess, err := c.sc.BillingPortalSessions.New(params)
	if err != nil {
		return "", fmt.Errorf("create portal session: %w", err)
	}
	return sess.URL, nil
}

// ReportRecordingDays pushes one idempotent meter event recording the number of
// billable recording-days for an account's billing period. days must be > 0
// (a zero-day period reports nothing; Stripe suppresses the empty invoice).
//
// Identifier is "<accountID>-<periodKey>", a per-customer-per-period key, so the
// monthly job is re-runnable without double-billing: the meter-event identifier is
// durably unique per event_name, so a re-send of the same period key is rejected
// (handled as a no-op via isDuplicateMeterEvent), never summed. The customer is mapped via the
// payload "stripe_customer_id" (the meter's customer_mapping) and the day count
// via "value" (the meter's value_settings). Timestamp is omitted, so Stripe
// stamps "now".
func (c *Client) ReportRecordingDays(ctx context.Context, customerID string, accountID int64, periodKey string, days int) error {
	if strings.TrimSpace(customerID) == "" {
		return fmt.Errorf("customer id is required")
	}
	if strings.TrimSpace(periodKey) == "" {
		return fmt.Errorf("period key is required")
	}
	if days <= 0 {
		return fmt.Errorf("days must be positive, got %d", days)
	}
	ev := &stripe.BillingMeterEventParams{
		EventName:  strPtr(recordingDayEventName),
		Identifier: strPtr(fmt.Sprintf("%d-%s", accountID, periodKey)),
		Payload: map[string]string{
			"stripe_customer_id": customerID,
			"value":              strconv.Itoa(days),
		},
	}
	ev.Context = ctx
	if _, err := c.sc.BillingMeterEvents.New(ev); err != nil {
		if isDuplicateMeterEvent(err) {
			return nil // already reported for this period; idempotent no-op.
		}
		return fmt.Errorf("report recording days: %w", err)
	}
	return nil
}

// ReportGBMonth pushes one idempotent meter event recording the average GB of
// managed storage for an account's billing period. It mirrors ReportRecordingDays
// but targets the gb_month meter and sends a DECIMAL string value (e.g. "2.471"),
// which the v1 Meter Events API accepts via the same payload "value" channel.
//
// Identifier is "<accountID>-gbm-<periodKey>": the distinct "-gbm-" namespace
// guarantees it can never collide with the recording_day identifier
// "<accountID>-<periodKey>", so the two meters dedup independently within Stripe's
// rolling window. The customer is mapped via payload "stripe_customer_id".
func (c *Client) ReportGBMonth(ctx context.Context, customerID string, accountID int64, periodKey, gbDecimal string) error {
	if strings.TrimSpace(customerID) == "" {
		return fmt.Errorf("customer id is required")
	}
	if strings.TrimSpace(periodKey) == "" {
		return fmt.Errorf("period key is required")
	}
	if strings.TrimSpace(gbDecimal) == "" {
		return fmt.Errorf("gb decimal value is required")
	}
	ev := &stripe.BillingMeterEventParams{
		EventName:  strPtr(gbMonthEventName),
		Identifier: strPtr(fmt.Sprintf("%d-gbm-%s", accountID, periodKey)),
		Payload: map[string]string{
			"stripe_customer_id": customerID,
			"value":              gbDecimal,
		},
	}
	ev.Context = ctx
	if _, err := c.sc.BillingMeterEvents.New(ev); err != nil {
		if isDuplicateMeterEvent(err) {
			return nil // already reported for this period; idempotent no-op.
		}
		return fmt.Errorf("report gb-month: %w", err)
	}
	return nil
}

// isDuplicateMeterEvent reports whether err is Stripe rejecting a meter event
// because its identifier was already used. The meter-event identifier is durably
// unique per event_name, so a re-send of the SAME period key (a same-day retry, or
// usage that a prior out-of-cycle invoice already consumed) is rejected rather than
// summed. Treating that rejection as a no-op is what makes the metering job safe to
// re-run and is the guarantee that already-consumed usage is never billed twice.
// Stripe returns this as a generic invalid_request_error with no machine code, so
// we match the stable identifier-collision phrase in the message.
func isDuplicateMeterEvent(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "already exists with identifier")
}

// EnsureGBMonthItem lazily adds the gb_month metered item to an EXISTING
// subscription that predates managed storage (Option A backfill; no bulk
// migration). It lists the subscription's items and, only if none already uses
// gbMonthPriceID, creates one with no quantity (Stripe rejects a quantity on a
// metered price). Idempotent: a re-run finds the item present and no-ops.
//
// Exported because the managed-provision path (server_storage.go) calls it
// cross-package as s.billing.EnsureGBMonthItem the moment an account opts into
// managed storage.
func (c *Client) EnsureGBMonthItem(ctx context.Context, subscriptionID string) error {
	if strings.TrimSpace(subscriptionID) == "" {
		return fmt.Errorf("subscription id is required")
	}
	listParams := &stripe.SubscriptionItemListParams{
		Subscription: strPtr(strings.TrimSpace(subscriptionID)),
	}
	listParams.Context = ctx
	iter := c.sc.SubscriptionItems.List(listParams)
	for iter.Next() {
		item := iter.SubscriptionItem()
		if item != nil && item.Price != nil && item.Price.ID == c.gbMonthPriceID {
			return nil // already present; idempotent no-op.
		}
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("list subscription items: %w", err)
	}
	params := &stripe.SubscriptionItemParams{
		Subscription: strPtr(strings.TrimSpace(subscriptionID)),
		Price:        strPtr(c.gbMonthPriceID), // no Quantity on a metered line
	}
	params.Context = ctx
	if _, err := c.sc.SubscriptionItems.New(params); err != nil {
		return fmt.Errorf("add gb-month subscription item: %w", err)
	}
	return nil
}

// GetSubscriptionPeriod returns the current billing-period bounds for the
// metering job. In v82 the period lives on the subscription ITEM (mirroring how
// the old recorderLineItem read CurrentPeriodEnd), so this reads the first
// item's current_period_start/end.
func (c *Client) GetSubscriptionPeriod(ctx context.Context, subID string) (start, end time.Time, err error) {
	if strings.TrimSpace(subID) == "" {
		return time.Time{}, time.Time{}, fmt.Errorf("subscription id is required")
	}
	params := &stripe.SubscriptionParams{}
	params.Context = ctx
	sub, err := c.sc.Subscriptions.Get(strings.TrimSpace(subID), params)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("get subscription: %w", err)
	}
	if sub.Items == nil {
		return time.Time{}, time.Time{}, fmt.Errorf("subscription has no items")
	}
	for _, item := range sub.Items.Data {
		if item == nil {
			continue
		}
		if item.CurrentPeriodStart > 0 {
			start = time.Unix(item.CurrentPeriodStart, 0).UTC()
		}
		if item.CurrentPeriodEnd > 0 {
			end = time.Unix(item.CurrentPeriodEnd, 0).UTC()
		}
		return start, end, nil
	}
	return time.Time{}, time.Time{}, fmt.Errorf("subscription has no items")
}

// Invoice is the minimal, display-only view of a Stripe invoice the account
// billing-history list needs. Amounts are in the invoice currency's minor unit
// (cents for USD). HostedURL/PDFURL are Stripe-hosted links and may be empty.
type Invoice struct {
	Number    string    `json:"number"`
	Status    string    `json:"status"`
	AmountDue int64     `json:"amount_due_cents"`
	Currency  string    `json:"currency"`
	Created   time.Time `json:"created"`
	HostedURL string    `json:"hosted_url"`
	PDFURL    string    `json:"pdf_url"`
}

// ListInvoices returns the customer's most recent invoices (newest first),
// display-only, for the account billing-history panel. limit is clamped to
// [1,100]. A new account billing monthly in arrears legitimately has zero
// invoices; this returns an empty slice in that case (never an error).
func (c *Client) ListInvoices(ctx context.Context, customerID string, limit int) ([]Invoice, error) {
	customerID = strings.TrimSpace(customerID)
	if customerID == "" {
		return nil, fmt.Errorf("customer id is required")
	}
	if limit <= 0 {
		limit = 12
	}
	if limit > 100 {
		limit = 100
	}
	params := &stripe.InvoiceListParams{Customer: strPtr(customerID)}
	params.Context = ctx
	params.Limit = stripe.Int64(int64(limit))
	iter := c.sc.Invoices.List(params)
	out := make([]Invoice, 0, limit)
	for iter.Next() {
		inv := iter.Invoice()
		if inv == nil {
			continue
		}
		item := Invoice{
			Number:    strings.TrimSpace(inv.Number),
			Status:    strings.TrimSpace(string(inv.Status)),
			AmountDue: inv.AmountDue,
			Currency:  strings.ToUpper(string(inv.Currency)),
			HostedURL: strings.TrimSpace(inv.HostedInvoiceURL),
			PDFURL:    strings.TrimSpace(inv.InvoicePDF),
		}
		if inv.Created > 0 {
			item.Created = time.Unix(inv.Created, 0).UTC()
		}
		out = append(out, item)
		if len(out) >= limit {
			break
		}
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("list invoices: %w", err)
	}
	return out, nil
}

// ConstructEvent verifies the Stripe-Signature header (HMAC + the default 5-min
// timestamp tolerance) and returns the parsed event, failing closed on any error.
//
// IgnoreAPIVersionMismatch is set because the account's default API version
// (used by the Dashboard and the Stripe CLI) advances independently of the
// stripe-go version pinned here, and stripe-go otherwise REJECTS any event whose
// version differs, which would 400 every webhook and prevent any account from
// ever becoming billable. This is safe: the HMAC signature is still verified, and
// we only read stable identifiers (customer/subscription/client_reference ids)
// off the event.
func (c *Client) ConstructEvent(payload []byte, sigHeader, secret string) (stripe.Event, error) {
	return webhook.ConstructEventWithOptions(payload, sigHeader, secret, webhook.ConstructEventOptions{
		IgnoreAPIVersionMismatch: true,
	})
}

func strPtr(s string) *string { return &s }
