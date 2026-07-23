package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRelayStateAtUsesFleetThreshold(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		last *time.Time
		want relayConnectivityState
	}{
		{name: "never seen", want: relayOffline},
		{name: "fresh", last: timePtr(now.Add(-119 * time.Second)), want: relayOnline},
		{name: "threshold is offline", last: timePtr(now.Add(-relayOnlineThreshold)), want: relayOffline},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := relayStateAt(tc.last, now); got != tc.want {
				t.Fatalf("relayStateAt=%s want %s", got, tc.want)
			}
		})
	}
}

func TestRelayConnectivityMessageContainsDiagnostics(t *testing.T) {
	changed := time.Date(2026, 7, 22, 12, 5, 0, 0, time.UTC)
	heartbeat := changed.Add(-3 * time.Minute)
	transition := relayConnectivityTransition{
		EventID: 9, NodeID: 13, Name: "MIT-MAC-1", Hostname: "mit-mac-1", OrgName: "MIT SCL", OrgEmail: "scl@example.edu",
		State: relayOffline, ChangedAt: changed, LastHeartbeatAt: &heartbeat,
	}
	hash := sha256.Sum256([]byte("deniz@example.com"))
	if got := relayConnectivityIdempotencyKey(transition.EventID, " Deniz@Example.com "); got != fmt.Sprintf("relay-connectivity-9-%x", hash[:8]) {
		t.Fatalf("idempotency key=%q", got)
	}
	if got := relayConnectivitySubject(transition); got != "[Stoarama] Relay MIT-MAC-1 is offline" {
		t.Fatalf("subject=%q", got)
	}
	body := relayConnectivityBody("https://stoarama.com/", transition)
	for _, want := range []string{"MIT-MAC-1 is offline", "MIT SCL <scl@example.edu>", "2026-07-22T12:02:00Z", "https://stoarama.com/org-settings#relay-computers"} {
		if !strings.Contains(body, want) {
			t.Errorf("body missing %q: %s", want, body)
		}
	}
}

func TestRecordRelayConnectivityBaselinesAndQueuesEveryTransition(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run DB-backed relay alert regression")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("relay_alert_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`) }()
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `
		CREATE TYPE relay_connectivity_state AS ENUM ('online', 'offline');
		CREATE TABLE accounts (id BIGINT PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL);
		CREATE TABLE users (email TEXT PRIMARY KEY, is_operator BOOLEAN NOT NULL);
		CREATE TABLE nodes (id BIGINT PRIMARY KEY, account_id BIGINT NOT NULL, node_type TEXT NOT NULL, display_name TEXT NOT NULL, hostname TEXT NOT NULL, status TEXT NOT NULL, last_heartbeat_at TIMESTAMPTZ);
		CREATE TABLE relay_connectivity_alert_states (node_id BIGINT PRIMARY KEY, observed_state relay_connectivity_state NOT NULL, observed_at TIMESTAMPTZ NOT NULL);
		CREATE TABLE relay_connectivity_alert_events (id BIGSERIAL PRIMARY KEY, account_id BIGINT NOT NULL, node_id BIGINT NOT NULL, state relay_connectivity_state NOT NULL, observed_at TIMESTAMPTZ NOT NULL, last_heartbeat_at TIMESTAMPTZ, notified_at TIMESTAMPTZ);
		CREATE TABLE relay_connectivity_alert_deliveries (event_id BIGINT NOT NULL, recipient TEXT NOT NULL, delivered_at TIMESTAMPTZ, PRIMARY KEY (event_id, recipient));
		INSERT INTO accounts VALUES (1, 'MIT SCL', 'scl@example.edu'), (2, 'Other Org', 'other@example.edu');
		INSERT INTO users VALUES ('deniz@aydemir.us', true);
		INSERT INTO nodes VALUES
		  (7, 1, 'relay', 'MIT-MAC-1', 'mit-mac-1', 'active', '2026-07-22T12:00:00Z'),
		  (8, 2, 'relay', 'OTHER-RELAY', 'other-relay', 'active', '2026-07-22T12:00:00Z');
	`); err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 7, 22, 12, 0, 30, 0, time.UTC)
	states, err := currentRelayConnectivity(ctx, pool, now)
	if err != nil || len(states) != 1 || states[0].OrgName != "MIT SCL" {
		t.Fatalf("alert-scoped relay states=%v err=%v", states, err)
	}
	if got, err := recordRelayConnectivity(ctx, pool, now); err != nil || len(got) != 0 {
		t.Fatalf("baseline transitions=%v err=%v, want none", got, err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_connectivity_alert_events (account_id, node_id, state, observed_at)
		VALUES (1, 7, 'offline', $1), (2, 8, 'offline', $1);
		UPDATE nodes SET account_id=2 WHERE id=7;
	`, now); err != nil {
		t.Fatal(err)
	}
	pending, err := pendingRelayConnectivity(ctx, pool)
	if err != nil || len(pending) != 1 || pending[0].EventID != 1 || pending[0].OrgName != "MIT SCL" {
		t.Fatalf("account-snapshotted pending transitions=%v err=%v", pending, err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE nodes SET account_id=1 WHERE id=7;
		DELETE FROM relay_connectivity_alert_events;
		ALTER SEQUENCE relay_connectivity_alert_events_id_seq RESTART WITH 1;
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE nodes SET last_heartbeat_at=$1 WHERE id=7`, now.Add(-relayOnlineThreshold)); err != nil {
		t.Fatal(err)
	}
	if got, err := recordRelayConnectivity(ctx, pool, now); err != nil || len(got) != 1 || got[0].State != relayOffline {
		t.Fatalf("offline transitions=%v err=%v", got, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE users SET is_operator=false`); err != nil {
		t.Fatal(err)
	}
	if recipients, err := pendingRelayConnectivityRecipients(ctx, pool, 1); err != nil || len(recipients) != 1 || recipients[0] != "deniz@aydemir.us" {
		t.Fatalf("snapshotted recipients=%v err=%v", recipients, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE nodes SET last_heartbeat_at=$1 WHERE id=7`, now); err != nil {
		t.Fatal(err)
	}
	if _, err := recordRelayConnectivity(ctx, pool, now.Add(time.Second)); err == nil || !strings.Contains(err.Error(), "no operator recipients") {
		t.Fatalf("missing-recipient error=%v", err)
	}
	var observed relayConnectivityState
	if err := pool.QueryRow(ctx, `SELECT observed_state FROM relay_connectivity_alert_states WHERE node_id=7`).Scan(&observed); err != nil || observed != relayOffline {
		t.Fatalf("state after rejected transition=%s err=%v", observed, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE users SET is_operator=true`); err != nil {
		t.Fatal(err)
	}
	got, err := recordRelayConnectivity(ctx, pool, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].State != relayOffline || got[1].State != relayOnline {
		t.Fatalf("queued transitions=%v, want offline then online", got)
	}
}

func timePtr(value time.Time) *time.Time { return &value }
