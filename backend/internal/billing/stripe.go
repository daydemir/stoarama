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

// Client wraps a per-instance Stripe API client (no global stripe.Key mutation)
// plus the metered recording-day price id and the app base URL for redirects.
type Client struct {
	sc         *client.API
	priceID    string
	appBaseURL string
	livemode   bool
}

// New builds a Stripe client bound to secretKey. priceID is the metered
// recording-day price; appBaseURL is used for Checkout/Portal redirect URLs.
func New(secretKey, priceID, appBaseURL string, livemode bool) (*Client, error) {
	secretKey = strings.TrimSpace(secretKey)
	if secretKey == "" {
		return nil, fmt.Errorf("stripe secret key is required")
	}
	if strings.TrimSpace(priceID) == "" {
		return nil, fmt.Errorf("stripe price id is required")
	}
	if strings.TrimSpace(appBaseURL) == "" {
		return nil, fmt.Errorf("app base url is required for stripe redirects")
	}
	return &Client{
		sc:         client.New(secretKey, nil),
		priceID:    strings.TrimSpace(priceID),
		appBaseURL: strings.TrimRight(strings.TrimSpace(appBaseURL), "/"),
		livemode:   livemode,
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
			{Price: strPtr(c.priceID)}, // no Quantity on a metered line
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
// monthly job is re-runnable without double-billing: Stripe enforces identifier
// uniqueness within a rolling window of at least 24 hours, and the per-period
// key keeps re-sends within a period a no-op. The customer is mapped via the
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
		return fmt.Errorf("report recording days: %w", err)
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
