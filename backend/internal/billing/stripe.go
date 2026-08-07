// Package billing isolates the Stripe SDK behind a small typed client, the same
// way internal/r2 isolates the S3 SDK. It owns the card-on-file Checkout, the
// customer portal, metered usage reporting (recording-hours), reading a
// subscription's billing period, and signature-verified webhook parsing.
//
// Billing model: one metered Subscription per account. Usage is reported as
// Stripe Billing Meter events (event_name "recording_hour", value = number of
// billable recording-hours in the period); Stripe sums them and bills the saved
// card monthly in arrears. priceID is the meter-backed metered price.
package billing

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	stripe "github.com/stripe/stripe-go/v82"
	"github.com/stripe/stripe-go/v82/client"
	"github.com/stripe/stripe-go/v82/webhook"
)

// recordingHourEventName is the meter's event_name (see the billing-setup meter).
const recordingHourEventName = "recording_hour"

// streamHourMonthEventName is the managed-storage meter's event_name (a SECOND
// Billing Meter, aggregation=SUM, value = average stored stream-hours over the
// period).
const streamHourMonthEventName = "stream_hour_month"

const (
	recordingHourUnitAmountCents   int64 = 5
	streamHourMonthUnitAmountCents int64 = 10
	recordingHourLookupKey               = "recording_hour_v1"
	streamHourMonthLookupKey             = "stream_hour_month_v1"
)

const (
	RecordingHourUnitAmountCents   = recordingHourUnitAmountCents
	StreamHourMonthUnitAmountCents = streamHourMonthUnitAmountCents
)

// Client wraps a per-instance Stripe API client (no global stripe.Key mutation)
// plus the metered recording-hour price id, the metered stream-hour-month (managed
// storage) price id, and the app base URL for redirects.
type Client struct {
	sc                     *client.API
	priceID                string
	streamHourMonthPriceID string
	recordingHourMeterID   string
	streamHourMonthMeterID string
	appBaseURL             string
	livemode               bool
}

type PeriodInvoice struct {
	ID             string
	Status         stripe.InvoiceStatus
	RecordingCents int64
	StorageCents   int64
	FinalizesAt    time.Time
}

// New builds a Stripe client bound to secretKey. priceID is the metered
// recording-hour price; streamHourMonthPriceID is the metered managed-storage
// ($/stream-hour-month) price; appBaseURL is used for Checkout/Portal redirect URLs.
func New(secretKey, priceID, streamHourMonthPriceID, appBaseURL string, livemode bool) (*Client, error) {
	secretKey = strings.TrimSpace(secretKey)
	if secretKey == "" {
		return nil, fmt.Errorf("stripe secret key is required")
	}
	if strings.TrimSpace(priceID) == "" {
		return nil, fmt.Errorf("stripe price id is required")
	}
	if strings.TrimSpace(streamHourMonthPriceID) == "" {
		return nil, fmt.Errorf("stripe stream-hour-month price id is required")
	}
	if strings.TrimSpace(appBaseURL) == "" {
		return nil, fmt.Errorf("app base url is required for stripe redirects")
	}
	return &Client{
		sc:                     client.New(secretKey, nil),
		priceID:                strings.TrimSpace(priceID),
		streamHourMonthPriceID: strings.TrimSpace(streamHourMonthPriceID),
		appBaseURL:             strings.TrimRight(strings.TrimSpace(appBaseURL), "/"),
		livemode:               livemode,
	}, nil
}

// Livemode reports the configured mode; webhook handling rejects events whose
// livemode disagrees with this.
func (c *Client) Livemode() bool { return c.livemode }

// ConfigurationRetrievalError means Stripe could not be reached (or rejected a
// read) after bounded retries. It is distinct from a successfully retrieved but
// invalid price/meter configuration.
type ConfigurationRetrievalError struct {
	Object string
	Err    error
}

func (e *ConfigurationRetrievalError) Error() string {
	return fmt.Sprintf("retrieve %s: %v", e.Object, e.Err)
}

func (e *ConfigurationRetrievalError) Unwrap() error { return e.Err }

func retryStripeConfigurationGet(ctx context.Context, object string, get func() error) error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if err = get(); err == nil {
			return nil
		}
		if attempt == 2 {
			break
		}
		delay := time.Duration(250*(1<<attempt)) * time.Millisecond
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return &ConfigurationRetrievalError{Object: object, Err: ctx.Err()}
		case <-timer.C:
		}
	}
	return &ConfigurationRetrievalError{Object: object, Err: err}
}

// ValidateConfiguration resolves the configured prices and meters from Stripe
// before a billing-capable process starts. This is deliberately fail-closed: a
// typo, archived object, wrong account/mode, wrong price, or crossed meter ID must
// stop both Checkout and metering instead of producing incorrect invoices.
func (c *Client) ValidateConfiguration(ctx context.Context, recordingMeterID, storageMeterID string) error {
	var recordingPrice, storagePrice *stripe.Price
	var recordingMeter, storageMeter *stripe.BillingMeter
	if err := retryStripeConfigurationGet(ctx, "recording-hour price", func() error {
		var err error
		recordingPrice, err = c.sc.Prices.Get(c.priceID, &stripe.PriceParams{Params: stripe.Params{Context: ctx}})
		return err
	}); err != nil {
		return err
	}
	if err := retryStripeConfigurationGet(ctx, "stream-hour-month price", func() error {
		var err error
		storagePrice, err = c.sc.Prices.Get(c.streamHourMonthPriceID, &stripe.PriceParams{Params: stripe.Params{Context: ctx}})
		return err
	}); err != nil {
		return err
	}
	if err := retryStripeConfigurationGet(ctx, "recording-hour meter", func() error {
		var err error
		recordingMeter, err = c.sc.BillingMeters.Get(strings.TrimSpace(recordingMeterID), &stripe.BillingMeterParams{Params: stripe.Params{Context: ctx}})
		return err
	}); err != nil {
		return err
	}
	if err := retryStripeConfigurationGet(ctx, "stream-hour-month meter", func() error {
		var err error
		storageMeter, err = c.sc.BillingMeters.Get(strings.TrimSpace(storageMeterID), &stripe.BillingMeterParams{Params: stripe.Params{Context: ctx}})
		return err
	}); err != nil {
		return err
	}
	recordingMeterID = strings.TrimSpace(recordingMeterID)
	storageMeterID = strings.TrimSpace(storageMeterID)
	if err := validateStripePriceAndMeter("recording-hour", recordingPrice, recordingMeter, recordingMeterID, recordingHourEventName, recordingHourLookupKey, recordingHourUnitAmountCents, c.livemode); err != nil {
		return err
	}
	if err := validateStripePriceAndMeter("stream-hour-month", storagePrice, storageMeter, storageMeterID, streamHourMonthEventName, streamHourMonthLookupKey, streamHourMonthUnitAmountCents, c.livemode); err != nil {
		return err
	}
	c.recordingHourMeterID = recordingMeterID
	c.streamHourMonthMeterID = storageMeterID
	return nil
}

func validateStripePriceAndMeter(label string, price *stripe.Price, meter *stripe.BillingMeter, meterID, eventName, lookupKey string, unitAmount int64, livemode bool) error {
	if price == nil || meter == nil {
		return fmt.Errorf("%s Stripe objects are missing", label)
	}
	if !price.Active {
		return fmt.Errorf("%s price is inactive", label)
	}
	if price.Livemode != livemode || meter.Livemode != livemode {
		return fmt.Errorf("%s price/meter mode does not match STRIPE_LIVEMODE", label)
	}
	if price.Currency != stripe.CurrencyUSD || price.UnitAmount != unitAmount {
		return fmt.Errorf("%s price must be USD %d cents per unit", label, unitAmount)
	}
	if price.LookupKey != lookupKey {
		return fmt.Errorf("%s price lookup_key=%q want %q", label, price.LookupKey, lookupKey)
	}
	if price.Type != stripe.PriceTypeRecurring || price.Recurring == nil ||
		price.Recurring.UsageType != stripe.PriceRecurringUsageTypeMetered ||
		price.Recurring.Interval != stripe.PriceRecurringIntervalMonth ||
		price.Recurring.IntervalCount != 1 {
		return fmt.Errorf("%s price must be a monthly metered recurring price", label)
	}
	if price.Recurring.Meter != meterID || meter.ID != meterID {
		return fmt.Errorf("%s price is not attached to its configured meter", label)
	}
	if meter.Status != stripe.BillingMeterStatusActive {
		return fmt.Errorf("%s meter is not active", label)
	}
	if meter.EventName != eventName {
		return fmt.Errorf("%s meter event_name=%q want %q", label, meter.EventName, eventName)
	}
	if meter.DefaultAggregation == nil || meter.DefaultAggregation.Formula != stripe.BillingMeterDefaultAggregationFormulaSum {
		return fmt.Errorf("%s meter must aggregate by sum", label)
	}
	if meter.CustomerMapping == nil || meter.CustomerMapping.Type != stripe.BillingMeterCustomerMappingTypeByID || meter.CustomerMapping.EventPayloadKey != "stripe_customer_id" {
		return fmt.Errorf("%s meter must map customers by stripe_customer_id", label)
	}
	if meter.ValueSettings == nil || meter.ValueSettings.EventPayloadKey != "value" {
		return fmt.Errorf("%s meter value payload key must be value", label)
	}
	return nil
}

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
			{Price: strPtr(c.priceID)},                // recording_hour (no Quantity on a metered line)
			{Price: strPtr(c.streamHourMonthPriceID)}, // stream_hour_month managed storage (no Quantity)
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

// ReportRecordingHours pushes one idempotent meter event recording the number of
// billable recording-hours for an account's billing period. hours must be > 0
// (a zero-hour period reports nothing; Stripe suppresses the empty invoice).
//
// Identifier is "<accountID>-<periodKey>", a per-customer-per-period key, so the
// monthly job's immediate retries are idempotent within Stripe's rolling
// deduplication window (the durable cross-day guard lives in billing_meter_reports).
// A re-send rejected as a duplicate is handled as a no-op. The customer is mapped via the
// payload "stripe_customer_id" (the meter's customer_mapping) and the hour count
// via "value" (the meter's value_settings). eventAt is explicit so a catch-up
// report after period close is attributed to the closed period rather than the
// new current period.
func (c *Client) ReportRecordingHours(ctx context.Context, customerID string, accountID int64, periodKey string, eventAt time.Time, hours int) error {
	if strings.TrimSpace(customerID) == "" {
		return fmt.Errorf("customer id is required")
	}
	if strings.TrimSpace(periodKey) == "" {
		return fmt.Errorf("period key is required")
	}
	if hours <= 0 {
		return fmt.Errorf("hours must be positive, got %d", hours)
	}
	ev := &stripe.BillingMeterEventParams{
		EventName:  strPtr(recordingHourEventName),
		Identifier: strPtr(fmt.Sprintf("%d-%s", accountID, periodKey)),
		Timestamp:  stripe.Int64(eventAt.UTC().Unix()),
		Payload: map[string]string{
			"stripe_customer_id": customerID,
			"value":              strconv.Itoa(hours),
		},
	}
	ev.Context = ctx
	if _, err := c.sc.BillingMeterEvents.New(ev); err != nil {
		if isDuplicateMeterEvent(err) {
			return nil // already reported for this period; idempotent no-op.
		}
		return fmt.Errorf("report recording hours: %w", err)
	}
	return nil
}

// ReportStreamHourMonth pushes one idempotent meter event recording the average
// stored stream-hours of managed footage for an account's billing period. It
// mirrors ReportRecordingHours but targets the stream_hour_month meter and sends a
// DECIMAL string value (e.g. "2.471"), which the v1 Meter Events API accepts via the
// same payload "value" channel.
//
// Identifier is "<accountID>-shm-<periodKey>": the distinct "-shm-" namespace
// guarantees it can never collide with the recording_hour identifier
// "<accountID>-<periodKey>", so the two meters dedup independently within Stripe's
// rolling window. The customer is mapped via payload "stripe_customer_id".
func (c *Client) ReportStreamHourMonth(ctx context.Context, customerID string, accountID int64, periodKey string, eventAt time.Time, hoursDecimal string) error {
	if strings.TrimSpace(customerID) == "" {
		return fmt.Errorf("customer id is required")
	}
	if strings.TrimSpace(periodKey) == "" {
		return fmt.Errorf("period key is required")
	}
	if strings.TrimSpace(hoursDecimal) == "" {
		return fmt.Errorf("stream-hour-month decimal value is required")
	}
	ev := &stripe.BillingMeterEventParams{
		EventName:  strPtr(streamHourMonthEventName),
		Identifier: strPtr(fmt.Sprintf("%d-shm-%s", accountID, periodKey)),
		Timestamp:  stripe.Int64(eventAt.UTC().Unix()),
		Payload: map[string]string{
			"stripe_customer_id": customerID,
			"value":              hoursDecimal,
		},
	}
	ev.Context = ctx
	if _, err := c.sc.BillingMeterEvents.New(ev); err != nil {
		if isDuplicateMeterEvent(err) {
			return nil // already reported for this period; idempotent no-op.
		}
		return fmt.Errorf("report stream-hour-month: %w", err)
	}
	return nil
}

// MeterUsage returns Stripe's aggregate for one customer and exact period. It
// is the reconciliation read for an ambiguous outbox row: equality proves the
// event was accepted even if recorder-control lost its local acknowledgement.
func (c *Client) MeterUsage(ctx context.Context, customerID, meterKind string, start, end time.Time) (float64, error) {
	meterID := c.recordingHourMeterID
	if meterKind == "stream_hour_month" {
		meterID = c.streamHourMonthMeterID
	} else if meterKind != "recording_hour" {
		return 0, fmt.Errorf("unsupported meter kind %q", meterKind)
	}
	if strings.TrimSpace(customerID) == "" || strings.TrimSpace(meterID) == "" {
		return 0, fmt.Errorf("customer and validated meter id are required")
	}
	start = start.UTC().Truncate(time.Minute)
	end = end.UTC().Truncate(time.Minute)
	if !end.After(start) {
		return 0, fmt.Errorf("invalid meter summary range")
	}
	params := &stripe.BillingMeterEventSummaryListParams{
		ID:        strPtr(meterID),
		Customer:  strPtr(customerID),
		StartTime: stripe.Int64(start.Unix()),
		EndTime:   stripe.Int64(end.Unix()),
	}
	params.Context = ctx
	iter := c.sc.BillingMeterEventSummaries.List(params)
	var total float64
	for iter.Next() {
		if summary := iter.BillingMeterEventSummary(); summary != nil {
			total += summary.AggregatedValue
		}
	}
	if err := iter.Err(); err != nil {
		return 0, fmt.Errorf("read %s meter usage: %w", meterKind, err)
	}
	return total, nil
}

// PeriodInvoice finds the unique subscription-cycle invoice for exact frozen
// period bounds and, once finalized, sums its matching metered-price lines.
func (c *Client) PeriodInvoice(ctx context.Context, customerID, subscriptionID string, start, end time.Time) (PeriodInvoice, error) {
	params := &stripe.InvoiceListParams{Customer: strPtr(customerID), Subscription: strPtr(subscriptionID)}
	params.Context = ctx
	params.Limit = stripe.Int64(100)
	iter := c.sc.Invoices.List(params)
	var matches []*stripe.Invoice
	for iter.Next() {
		inv := iter.Invoice()
		if inv == nil || inv.Deleted || inv.Status == stripe.InvoiceStatusVoid {
			continue
		}
		if inv.PeriodStart == start.UTC().Unix() && inv.PeriodEnd == end.UTC().Unix() {
			matches = append(matches, inv)
		}
	}
	if err := iter.Err(); err != nil {
		return PeriodInvoice{}, fmt.Errorf("list period invoices: %w", err)
	}
	if len(matches) != 1 {
		return PeriodInvoice{}, fmt.Errorf("expected one invoice for subscription %s period, found %d", subscriptionID, len(matches))
	}
	inv := matches[0]
	result := PeriodInvoice{ID: inv.ID, Status: inv.Status}
	if inv.AutomaticallyFinalizesAt > 0 {
		result.FinalizesAt = time.Unix(inv.AutomaticallyFinalizesAt, 0).UTC()
	}
	if inv.Status == stripe.InvoiceStatusDraft {
		return result, nil
	}
	lineParams := &stripe.InvoiceListLinesParams{Invoice: strPtr(inv.ID)}
	lineParams.Context = ctx
	lineParams.Limit = stripe.Int64(100)
	lines := c.sc.Invoices.ListLines(lineParams)
	for lines.Next() {
		line := lines.InvoiceLineItem()
		if line == nil || line.Period == nil || line.Period.Start != start.UTC().Unix() || line.Period.End != end.UTC().Unix() || line.Parent == nil || line.Parent.Type != stripe.InvoiceLineItemParentTypeSubscriptionItemDetails || line.Parent.SubscriptionItemDetails == nil || line.Parent.SubscriptionItemDetails.Subscription != subscriptionID || line.Pricing == nil || line.Pricing.Type != stripe.InvoiceLineItemPricingTypePriceDetails || line.Pricing.PriceDetails == nil {
			continue
		}
		switch line.Pricing.PriceDetails.Price {
		case c.priceID:
			result.RecordingCents += line.Amount
		case c.streamHourMonthPriceID:
			result.StorageCents += line.Amount
		}
	}
	if err := lines.Err(); err != nil {
		return PeriodInvoice{}, fmt.Errorf("list period invoice lines: %w", err)
	}
	return result, nil
}

// isDuplicateMeterEvent reports whether err is Stripe rejecting a meter event
// because its identifier was already used inside Stripe's rolling deduplication
// window. The database billing_meter_reports ledger supplies the durable guard;
// this check safely absorbs only immediate duplicate responses.
// Two checks run in OR:
//  1. Substring match on the stable identifier-collision phrase (Stripe currently
//     returns this as a generic invalid_request_error; empirically verified in test mode).
//  2. Structured code check: stripe.ErrorCodeResourceAlreadyExists ("resource_already_exists"),
//     the SDK-documented code for identifier collisions, in case Stripe rewrites the
//     message text but keeps the machine code.
func isDuplicateMeterEvent(err error) bool {
	if err == nil {
		return false
	}
	if strings.Contains(err.Error(), "already exists with identifier") {
		return true
	}
	var se *stripe.Error
	if errors.As(err, &se) && se.Code == stripe.ErrorCodeResourceAlreadyExists {
		return true
	}
	return false
}

// EnsureStreamHourMonthItem lazily adds the stream_hour_month metered item to an
// EXISTING subscription that predates managed storage (Option A backfill; no bulk
// migration). It lists the subscription's items and, only if none already uses
// streamHourMonthPriceID, creates one with no quantity (Stripe rejects a quantity on
// a metered price). Idempotent: a re-run finds the item present and no-ops.
//
// Exported because the managed-provision path (server_storage.go) calls it
// cross-package as s.billing.EnsureStreamHourMonthItem the moment an account opts
// into managed storage.
func (c *Client) EnsureStreamHourMonthItem(ctx context.Context, subscriptionID string) error {
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
		if item != nil && item.Price != nil && item.Price.ID == c.streamHourMonthPriceID {
			return nil // already present; idempotent no-op.
		}
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("list subscription items: %w", err)
	}
	params := &stripe.SubscriptionItemParams{
		Subscription: strPtr(strings.TrimSpace(subscriptionID)),
		Price:        strPtr(c.streamHourMonthPriceID), // no Quantity on a metered line
	}
	params.Context = ctx
	if _, err := c.sc.SubscriptionItems.New(params); err != nil {
		return fmt.Errorf("add stream-hour-month subscription item: %w", err)
	}
	return nil
}

// StreamHourMonthPriceID returns the managed-storage ($/stream-hour-month) metered
// price id. The yearly-prepaid credit grant is scoped to THIS price ONLY (never the
// recording-hour price), so the prepay pass and its tests read it from here.
func (c *Client) StreamHourMonthPriceID() string { return c.streamHourMonthPriceID }

// PrepaidStreamHourMonthRateCents is the effective managed-storage price for
// yearly-prepaid footage: $0.05 per stream-hour-month (half the metered $0.10),
// prepaid 12 months up front.
const PrepaidStreamHourMonthRateCents = 5

// PrepaidCreditMonths is how many months a prepay batch covers: the charge is
// round(stream_hours * PrepaidCreditMonths * PrepaidStreamHourMonthRateCents) and
// the credit grant expires PrepaidCreditMonths after the invoice is paid.
const PrepaidCreditMonths = 12

// PrepaidBatchCents is the prepay charge in cents for stream-hours of footage:
// round(stream_hours * 12 * $0.05). It is the single cents-math authority shared by
// the monthly per-account pass and the retroactive per-recording tier switch. A
// batch that rounds to 0 cents returns 0 (the caller skips it, never charging $0).
func PrepaidBatchCents(streamHours float64) int64 {
	if streamHours <= 0 {
		return 0
	}
	return int64(math.Round(streamHours * float64(PrepaidCreditMonths) * float64(PrepaidStreamHourMonthRateCents)))
}

// PrepaidBatch is the result of charging one yearly-prepaid storage batch: the ids
// of the standalone invoice and its single invoice item, which the ledger stores so
// the invoice.paid webhook can match the paid invoice back to its batch row.
type PrepaidBatch struct {
	InvoiceID     string
	InvoiceItemID string
}

// ChargePrepaidBatch charges one aggregated yearly-prepaid storage batch as a
// STANDALONE invoice billed to the customer's card, distinct from the metered
// subscription's monthly cycle. It (1) creates a one-off invoice item for `cents`
// USD carrying the batch_key in its metadata + description, then (2) creates an
// invoice that PULLS that pending item (PendingInvoiceItemsBehavior=include, so the
// prepay lands on THIS invoice and not the next metered cycle),
// CollectionMethod=charge_automatically + AutoAdvance=true so Stripe finalizes and
// charges the saved card immediately.
//
// batch_key is the idempotency anchor: SetIdempotencyKey(batchKey) on the invoice
// item and SetIdempotencyKey("inv:"+batchKey) on the invoice mean a re-run of the
// monthly pass under the same key returns Stripe's original objects instead of
// double-charging. Combined with the ledger's UNIQUE batch_key, this is the
// no-double-charge guarantee for real money.
func (c *Client) ChargePrepaidBatch(ctx context.Context, customerID, batchKey string, cents int64, metadata map[string]string) (PrepaidBatch, error) {
	customerID = strings.TrimSpace(customerID)
	if customerID == "" {
		return PrepaidBatch{}, fmt.Errorf("customer id is required")
	}
	if strings.TrimSpace(batchKey) == "" {
		return PrepaidBatch{}, fmt.Errorf("batch key is required")
	}
	if cents <= 0 {
		return PrepaidBatch{}, fmt.Errorf("charge cents must be positive, got %d", cents)
	}

	itemParams := &stripe.InvoiceItemParams{
		Customer:    strPtr(customerID),
		Amount:      stripe.Int64(cents),
		Currency:    strPtr(string(stripe.CurrencyUSD)),
		Description: strPtr("Prepaid managed storage (12 months)"),
	}
	itemParams.Context = ctx
	for k, v := range metadata {
		itemParams.AddMetadata(k, v)
	}
	itemParams.AddMetadata("batch_key", batchKey)
	itemParams.SetIdempotencyKey(batchKey)
	item, err := c.sc.InvoiceItems.New(itemParams)
	if err != nil {
		return PrepaidBatch{}, fmt.Errorf("create prepaid invoice item: %w", err)
	}

	invParams := &stripe.InvoiceParams{
		Customer:                    strPtr(customerID),
		CollectionMethod:            strPtr(string(stripe.InvoiceCollectionMethodChargeAutomatically)),
		AutoAdvance:                 stripe.Bool(true),
		PendingInvoiceItemsBehavior: strPtr("include"),
		Description:                 strPtr("Prepaid managed storage (12 months)"),
	}
	invParams.Context = ctx
	for k, v := range metadata {
		invParams.AddMetadata(k, v)
	}
	invParams.AddMetadata("batch_key", batchKey)
	invParams.SetIdempotencyKey("inv:" + batchKey)
	inv, err := c.sc.Invoices.New(invParams)
	if err != nil {
		return PrepaidBatch{}, fmt.Errorf("create prepaid invoice: %w", err)
	}
	return PrepaidBatch{
		InvoiceID:     strings.TrimSpace(inv.ID),
		InvoiceItemID: strings.TrimSpace(item.ID),
	}, nil
}

// CreatePrepaidCreditGrant is RETIRED (no production callers). Yearly-prepaid footage
// is now EXCLUDED from the stream_hour_month meter (snapshotManagedStorageSQL) rather
// than metered-then-credited, so no grant is minted. Do NOT re-wire this into the
// invoice.paid path: with the meter exclusion in place it would double-benefit the
// customer (half-price prepay + a live credit). Kept for reference/history only.
//
// (original behavior) issues a Stripe billing credit grant of `cents` USD that
// applies ONLY to the managed-storage stream_hour_month price (streamHourMonthPriceID),
// so a prepaid recording's monthly metered storage line is netted to $0 while it
// NEVER touches the recording-hour price (which is always metered in arrears). This
// is the load-bearing scope: applying the credit to the recording-hour price would
// silently give away recording-hours for free.
//
// Category=paid (the customer paid for it via the prepay invoice), Amount.Monetary
// in USD, ExpiresAt = the caller-supplied +12mo instant so the prepaid year runs out
// exactly a year after payment. SetIdempotencyKey("grant:"+batchKey) makes the
// webhook that creates it safe against Stripe redelivery: a second invoice.paid for
// the same batch returns the original grant rather than a duplicate.
func (c *Client) CreatePrepaidCreditGrant(ctx context.Context, customerID string, cents int64, expiresAt time.Time, batchKey string, metadata map[string]string) (string, error) {
	customerID = strings.TrimSpace(customerID)
	if customerID == "" {
		return "", fmt.Errorf("customer id is required")
	}
	if cents <= 0 {
		return "", fmt.Errorf("grant cents must be positive, got %d", cents)
	}
	if strings.TrimSpace(batchKey) == "" {
		return "", fmt.Errorf("batch key is required")
	}
	if expiresAt.IsZero() {
		return "", fmt.Errorf("credit grant expiry is required")
	}
	params := prepaidCreditGrantParams(customerID, cents, expiresAt, batchKey, c.streamHourMonthPriceID, metadata)
	params.Context = ctx
	grant, err := c.sc.BillingCreditGrants.New(params)
	if err != nil {
		return "", fmt.Errorf("create prepaid credit grant: %w", err)
	}
	return strings.TrimSpace(grant.ID), nil
}

// prepaidCreditGrantParams builds the credit-grant request. It is factored out (with
// storagePriceID passed in) so a unit test can assert the load-bearing scope: the
// grant applies to the storage price ONLY and NEVER to any other price (the
// recording-hour price). It also sets category=paid, USD monetary amount, the +12mo
// expiry, and the "grant:"+batchKey idempotency key.
func prepaidCreditGrantParams(customerID string, cents int64, expiresAt time.Time, batchKey, storagePriceID string, metadata map[string]string) *stripe.BillingCreditGrantParams {
	params := &stripe.BillingCreditGrantParams{
		Customer: strPtr(customerID),
		Category: strPtr(string(stripe.BillingCreditGrantCategoryPaid)),
		Amount: &stripe.BillingCreditGrantAmountParams{
			Type: strPtr(string(stripe.BillingCreditGrantAmountTypeMonetary)),
			Monetary: &stripe.BillingCreditGrantAmountMonetaryParams{
				Currency: strPtr(string(stripe.CurrencyUSD)),
				Value:    stripe.Int64(cents),
			},
		},
		ApplicabilityConfig: &stripe.BillingCreditGrantApplicabilityConfigParams{
			Scope: &stripe.BillingCreditGrantApplicabilityConfigScopeParams{
				// Storage price ONLY. NEVER the recording-hour price.
				Prices: []*stripe.BillingCreditGrantApplicabilityConfigScopePriceParams{
					{ID: strPtr(storagePriceID)},
				},
			},
		},
		ExpiresAt: stripe.Int64(expiresAt.UTC().Unix()),
	}
	for k, v := range metadata {
		params.AddMetadata(k, v)
	}
	params.AddMetadata("batch_key", batchKey)
	params.SetIdempotencyKey("grant:" + batchKey)
	return params
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
