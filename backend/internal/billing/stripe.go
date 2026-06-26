// Package billing isolates the Stripe SDK behind a small typed client, the same
// way internal/r2 isolates the S3 SDK. It owns checkout, the customer portal,
// quantity sync, subscription re-fetch, and signature-verified webhook parsing.
// One subscription per account; quantity is the absolute live recording count.
package billing

import (
	"context"
	"fmt"
	"strings"

	stripe "github.com/stripe/stripe-go/v82"
	"github.com/stripe/stripe-go/v82/client"
	"github.com/stripe/stripe-go/v82/webhook"
)

// Client wraps a per-instance Stripe API client (no global stripe.Key mutation)
// plus the recorder's single price id and the app base URL for redirects.
type Client struct {
	sc         *client.API
	priceID    string
	appBaseURL string
	livemode   bool
}

// New builds a Stripe client bound to secretKey. priceID is the recurring
// monthly recorder price; appBaseURL is used for Checkout/Portal redirect URLs.
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

// CreateCheckoutSession opens a subscription Checkout for qty recorder seats.
func (c *Client) CreateCheckoutSession(ctx context.Context, customerID string, accountID int64, qty int64) (string, error) {
	if strings.TrimSpace(customerID) == "" {
		return "", fmt.Errorf("customer id is required")
	}
	if qty < 1 {
		qty = 1
	}
	params := &stripe.CheckoutSessionParams{
		Mode:              strPtr(string(stripe.CheckoutSessionModeSubscription)),
		Customer:          strPtr(customerID),
		ClientReferenceID: strPtr(fmt.Sprintf("%d", accountID)),
		SuccessURL:        strPtr(c.appBaseURL + "/recordings?billing=success"),
		CancelURL:         strPtr(c.appBaseURL + "/recordings?billing=cancel"),
		LineItems: []*stripe.CheckoutSessionLineItemParams{
			{Price: strPtr(c.priceID), Quantity: stripe.Int64(qty)},
		},
	}
	params.Context = ctx
	sess, err := c.sc.CheckoutSessions.New(params)
	if err != nil {
		return "", fmt.Errorf("create checkout session: %w", err)
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

// SetSubscriptionQuantity updates the recorder line item to qty seats with
// prorations enabled.
func (c *Client) SetSubscriptionQuantity(ctx context.Context, subItemID string, qty int64) error {
	if strings.TrimSpace(subItemID) == "" {
		return fmt.Errorf("subscription item id is required")
	}
	if qty < 0 {
		qty = 0
	}
	params := &stripe.SubscriptionItemParams{
		Quantity:          stripe.Int64(qty),
		ProrationBehavior: strPtr("create_prorations"),
	}
	params.Context = ctx
	if _, err := c.sc.SubscriptionItems.Update(subItemID, params); err != nil {
		return fmt.Errorf("set subscription quantity: %w", err)
	}
	return nil
}

// CancelSubscription cancels the subscription immediately. Used when the account
// drops to zero active recordings: Stripe rejects quantity 0 on a licensed item,
// so instead of setting quantity 0 we cancel the subscription so the account pays
// nothing, and the next active recording cleanly re-subscribes via Checkout.
func (c *Client) CancelSubscription(ctx context.Context, subID string) error {
	if strings.TrimSpace(subID) == "" {
		return fmt.Errorf("subscription id is required")
	}
	params := &stripe.SubscriptionCancelParams{}
	params.Context = ctx
	if _, err := c.sc.Subscriptions.Cancel(strings.TrimSpace(subID), params); err != nil {
		return fmt.Errorf("cancel subscription: %w", err)
	}
	return nil
}

// GetSubscription re-fetches the authoritative subscription object so the
// webhook handler never trusts a possibly-stale event payload for state.
func (c *Client) GetSubscription(ctx context.Context, subID string) (*stripe.Subscription, error) {
	if strings.TrimSpace(subID) == "" {
		return nil, fmt.Errorf("subscription id is required")
	}
	params := &stripe.SubscriptionParams{}
	params.Context = ctx
	sub, err := c.sc.Subscriptions.Get(subID, params)
	if err != nil {
		return nil, fmt.Errorf("get subscription: %w", err)
	}
	return sub, nil
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
// off the event, then re-fetch the authoritative subscription via the pinned API
// client, so cross-version field-shape drift in the payload cannot corrupt state.
func (c *Client) ConstructEvent(payload []byte, sigHeader, secret string) (stripe.Event, error) {
	return webhook.ConstructEventWithOptions(payload, sigHeader, secret, webhook.ConstructEventOptions{
		IgnoreAPIVersionMismatch: true,
	})
}

func strPtr(s string) *string { return &s }
