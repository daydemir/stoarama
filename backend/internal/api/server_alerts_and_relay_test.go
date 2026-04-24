package api

import (
	"testing"
	"time"
)

func TestResendWebhookStatusAndTimes(t *testing.T) {
	deliveredAt := "2026-03-27T00:44:19.331226Z"
	status, delivered, opened, bounced := resendWebhookStatusAndTimes("email.delivered", map[string]any{
		"email_id":     "123",
		"delivered_at": deliveredAt,
	})
	if status != "delivered" {
		t.Fatalf("expected delivered status, got %q", status)
	}
	if delivered == nil || delivered.UTC().Format(time.RFC3339Nano) != deliveredAt {
		t.Fatalf("expected delivered timestamp %s, got %v", deliveredAt, delivered)
	}
	if opened != nil || bounced != nil {
		t.Fatalf("unexpected opened/bounced values: %v %v", opened, bounced)
	}

	status, delivered, opened, bounced = resendWebhookStatusAndTimes("opened", map[string]any{
		"created_at": deliveredAt,
	})
	if status != "opened" || opened == nil {
		t.Fatalf("expected opened status with timestamp, got status=%q opened=%v", status, opened)
	}
	if delivered != nil || bounced != nil {
		t.Fatalf("unexpected delivered/bounced values: %v %v", delivered, bounced)
	}
}
