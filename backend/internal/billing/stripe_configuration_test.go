package billing

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	stripe "github.com/stripe/stripe-go/v82"
)

func TestRetryStripeConfigurationGet(t *testing.T) {
	attempts := 0
	if err := retryStripeConfigurationGet(context.Background(), "price", func() error {
		attempts++
		if attempts < 3 {
			return fmt.Errorf("temporary")
		}
		return nil
	}); err != nil || attempts != 3 {
		t.Fatalf("retry success attempts=%d err=%v", attempts, err)
	}
	attempts = 0
	err := retryStripeConfigurationGet(context.Background(), "meter", func() error {
		attempts++
		return fmt.Errorf("unavailable")
	})
	var retrievalErr *ConfigurationRetrievalError
	if !errors.As(err, &retrievalErr) || attempts != 3 {
		t.Fatalf("retry failure attempts=%d err=%v", attempts, err)
	}
}

func validStripeObjects(livemode bool) (*stripe.Price, *stripe.BillingMeter) {
	meterID := "mtr_test_recording"
	if livemode {
		meterID = "mtr_recording"
	}
	return &stripe.Price{
			ID:         "price_recording",
			Active:     true,
			Livemode:   livemode,
			Currency:   stripe.CurrencyUSD,
			UnitAmount: recordingHourUnitAmountCents,
			LookupKey:  recordingHourLookupKey,
			Type:       stripe.PriceTypeRecurring,
			Recurring: &stripe.PriceRecurring{
				UsageType:     stripe.PriceRecurringUsageTypeMetered,
				Interval:      stripe.PriceRecurringIntervalMonth,
				IntervalCount: 1,
				Meter:         meterID,
			},
		}, &stripe.BillingMeter{
			ID:                 meterID,
			Livemode:           livemode,
			Status:             stripe.BillingMeterStatusActive,
			EventName:          recordingHourEventName,
			DefaultAggregation: &stripe.BillingMeterDefaultAggregation{Formula: stripe.BillingMeterDefaultAggregationFormulaSum},
			CustomerMapping:    &stripe.BillingMeterCustomerMapping{Type: stripe.BillingMeterCustomerMappingTypeByID, EventPayloadKey: "stripe_customer_id"},
			ValueSettings:      &stripe.BillingMeterValueSettings{EventPayloadKey: "value"},
		}
}

func TestValidateStripePriceAndMeter(t *testing.T) {
	price, meter := validStripeObjects(true)
	if err := validateStripePriceAndMeter("recording-hour", price, meter, meter.ID, recordingHourEventName, recordingHourLookupKey, recordingHourUnitAmountCents, true); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		edit func(*stripe.Price, *stripe.BillingMeter)
		want string
	}{
		{"inactive price", func(p *stripe.Price, _ *stripe.BillingMeter) { p.Active = false }, "inactive"},
		{"wrong mode", func(p *stripe.Price, _ *stripe.BillingMeter) { p.Livemode = false }, "mode"},
		{"wrong amount", func(p *stripe.Price, _ *stripe.BillingMeter) { p.UnitAmount = 50 }, "5 cents"},
		{"wrong lookup key", func(p *stripe.Price, _ *stripe.BillingMeter) { p.LookupKey = "recording_day_v1" }, "lookup_key"},
		{"licensed price", func(p *stripe.Price, _ *stripe.BillingMeter) {
			p.Recurring.UsageType = stripe.PriceRecurringUsageTypeLicensed
		}, "monthly metered"},
		{"yearly price", func(p *stripe.Price, _ *stripe.BillingMeter) {
			p.Recurring.Interval = stripe.PriceRecurringIntervalYear
		}, "monthly metered"},
		{"wrong meter", func(p *stripe.Price, _ *stripe.BillingMeter) { p.Recurring.Meter = "mtr_other" }, "attached"},
		{"wrong meter mode", func(_ *stripe.Price, m *stripe.BillingMeter) { m.Livemode = false }, "mode"},
		{"inactive meter", func(_ *stripe.Price, m *stripe.BillingMeter) { m.Status = stripe.BillingMeterStatusInactive }, "not active"},
		{"wrong event", func(_ *stripe.Price, m *stripe.BillingMeter) { m.EventName = "recording_day" }, "event_name"},
		{"wrong aggregation", func(_ *stripe.Price, m *stripe.BillingMeter) {
			m.DefaultAggregation.Formula = stripe.BillingMeterDefaultAggregationFormulaCount
		}, "sum"},
		{"wrong customer key", func(_ *stripe.Price, m *stripe.BillingMeter) { m.CustomerMapping.EventPayloadKey = "customer" }, "stripe_customer_id"},
		{"wrong value key", func(_ *stripe.Price, m *stripe.BillingMeter) { m.ValueSettings.EventPayloadKey = "hours" }, "payload key"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, m := validStripeObjects(true)
			tc.edit(p, m)
			err := validateStripePriceAndMeter("recording-hour", p, m, m.ID, recordingHourEventName, recordingHourLookupKey, recordingHourUnitAmountCents, true)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v want substring %q", err, tc.want)
			}
		})
	}
}
