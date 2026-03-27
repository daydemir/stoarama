package api

import (
	"testing"
	"time"
)

func TestValidateYouTubeRelayRouteTransition(t *testing.T) {
	tests := []struct {
		name    string
		actor   string
		current string
		next    string
		wantErr bool
	}{
		{name: "source can ready assigned", actor: "youtube_relay_source", current: "assigned", next: "source_ready"},
		{name: "source cannot clobber running", actor: "youtube_relay_source", current: "running", next: "source_ready", wantErr: true},
		{name: "sink can mark running from ready", actor: "youtube_relay_sink", current: "source_ready", next: "running"},
		{name: "sink can recover failed to running", actor: "youtube_relay_sink", current: "failed", next: "running"},
		{name: "source cannot mark running", actor: "youtube_relay_source", current: "source_ready", next: "running", wantErr: true},
		{name: "operator can stop", actor: "operator", current: "running", next: "stopped"},
		{name: "stopped cannot revive", actor: "youtube_relay_sink", current: "stopped", next: "running", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateYouTubeRelayRouteTransition(tt.actor, tt.current, tt.next)
			if tt.wantErr && err == nil {
				t.Fatalf("expected error for actor=%s current=%s next=%s", tt.actor, tt.current, tt.next)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error for actor=%s current=%s next=%s: %v", tt.actor, tt.current, tt.next, err)
			}
		})
	}
}

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
