package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	appconfig "github.com/daydemir/stoarama/backend/internal/config"
)

// The thresholds are deliberate, measured values rather than knobs: an idle
// laptop may sleep for ~30 minutes, but a node carrying footage must page before
// its outage becomes a five-minute stitching gap.
func TestRelayThresholdsSeparateIdlePresenceFromActiveCapture(t *testing.T) {
	if relayIdleOnlineThreshold != 45*time.Minute {
		t.Fatalf("relayIdleOnlineThreshold = %v, want 45m: shorter values page on idle laptop sleep", relayIdleOnlineThreshold)
	}
	if relayCapturingOnlineThreshold != 2*time.Minute {
		t.Fatalf("relayCapturingOnlineThreshold = %v, want 2m", relayCapturingOnlineThreshold)
	}
}

func TestRelayStateAtUsesCaptureAwareThreshold(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name   string
		last   *time.Time
		active bool
		want   relayConnectivityState
	}{
		{name: "never seen", want: relayOffline},
		{name: "fresh", last: timePtr(now.Add(-119 * time.Second)), want: relayOnline},
		{name: "idle threshold is offline", last: timePtr(now.Add(-relayIdleOnlineThreshold)), want: relayOffline},
		{name: "capturing two minute threshold is offline", last: timePtr(now.Add(-relayCapturingOnlineThreshold)), active: true, want: relayOffline},
		{name: "next minute cron catches outage before lease reclaim", last: timePtr(now.Add(-140 * time.Second)), active: true, want: relayOffline},
		{name: "capturing relay alerts before five minute gap", last: timePtr(now.Add(-4 * time.Minute)), active: true, want: relayOffline},
		// Pinned to absolute durations, not to the named constants, so a revert
		// to a short threshold fails here instead of passing silently. A sleeping
		// laptop's longest measured gap was 30.5 min; 44m must still read online.
		{name: "sleeping laptop gap stays online", last: timePtr(now.Add(-44 * time.Minute)), want: relayOnline},
		{name: "genuinely down past 45m is offline", last: timePtr(now.Add(-46 * time.Minute)), want: relayOffline},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := relayStateAt(tc.last, tc.active, now); got != tc.want {
				t.Fatalf("relayStateAt=%s want %s", got, tc.want)
			}
		})
	}
}

func TestRelayObservedStateRequiresFreshHeartbeatToClearFastOffline(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	stale := now.Add(-4 * time.Minute)
	state := relayConnectivityTransition{State: relayOnline, LastHeartbeatAt: &stale}
	if got := relayObservedState(state, relayOffline, now); got != relayOffline {
		t.Fatalf("idle threshold manufactured recovery=%s, want offline", got)
	}
	fresh := now.Add(-time.Minute)
	state.LastHeartbeatAt = &fresh
	if got := relayObservedState(state, relayOffline, now); got != relayOnline {
		t.Fatalf("fresh heartbeat recovery=%s, want online", got)
	}
}

func TestRelayCapacityStateRequiresIndependentUplinkAndNPlusOneCapacity(t *testing.T) {
	tests := []struct {
		name      string
		demand    int
		domains   int
		remaining int
		want      relayCapacityState
	}{
		{name: "idle fleet does not page", demand: 0, domains: 0, remaining: 0, want: relayCapacityHealthy},
		{name: "one uplink cannot fail over", demand: 1, domains: 1, remaining: 20, want: relayCapacityDegraded},
		{name: "second uplink too small", demand: 3, domains: 2, remaining: 2, want: relayCapacityDegraded},
		{name: "second uplink exactly covers demand", demand: 3, domains: 2, remaining: 3, want: relayCapacityHealthy},
		{name: "three uplinks cover demand", demand: 3, domains: 3, remaining: 6, want: relayCapacityHealthy},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := relayCapacityStateFor(tc.demand, tc.domains, tc.remaining); got != tc.want {
				t.Fatalf("relayCapacityStateFor(%d,%d,%d)=%s, want %s", tc.demand, tc.domains, tc.remaining, got, tc.want)
			}
		})
	}
}

func TestRelayCapacityMessageContainsFailoverDiagnostics(t *testing.T) {
	state := relayCapacityTransition{
		OrgName: "MIT SCL", OrgEmail: "scl@example.edu", State: relayCapacityDegraded,
		ChangedAt: time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC), ActiveDemand: 9,
		LiveFailureDomains: 1, EffectiveCapacity: 50, RemainingCapacity: 0,
	}
	body := relayCapacityBody("https://stoarama.com/", state)
	for _, want := range []string{"MIT SCL <scl@example.edu>", "Active relay capture demand: 9", "Live uplink failure domains: 1", "Capacity after largest uplink loss: 0", "org-settings#relay-computers"} {
		if !strings.Contains(body, want) {
			t.Errorf("body missing %q: %s", want, body)
		}
	}
}

func TestCurrentRelayCapacityCollapsesGroupsAndPreservesUngroupedDomains(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run DB-backed relay capacity regression")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("relay_capacity_%d", time.Now().UnixNano())
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
		CREATE TYPE relay_capacity_state AS ENUM ('healthy','degraded');
		CREATE TABLE accounts (id BIGINT PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL);
		CREATE TABLE users (email TEXT PRIMARY KEY, is_operator BOOLEAN NOT NULL);
		CREATE TABLE relay_groups (id BIGINT PRIMARY KEY, max_streams INTEGER NOT NULL);
		CREATE TABLE nodes (id BIGINT PRIMARY KEY, account_id BIGINT NOT NULL, node_type TEXT NOT NULL, status TEXT NOT NULL, last_heartbeat_at TIMESTAMPTZ, relay_group_id BIGINT, relay_max_streams INTEGER NOT NULL);
		CREATE TABLE recordings (id BIGINT PRIMARY KEY, account_id BIGINT NOT NULL, capture_via TEXT NOT NULL, status TEXT NOT NULL);
		CREATE TABLE recording_jobs (id BIGINT PRIMARY KEY, recording_id BIGINT NOT NULL, status TEXT NOT NULL, lease_expires_at TIMESTAMPTZ, fire_at TIMESTAMPTZ NOT NULL, kind TEXT NOT NULL, window_end_at TIMESTAMPTZ, clip_duration_sec INTEGER NOT NULL);
		CREATE TABLE relay_capacity_alert_states (account_id BIGINT PRIMARY KEY, observed_state relay_capacity_state NOT NULL, observed_at TIMESTAMPTZ NOT NULL);
		CREATE TABLE relay_capacity_alert_events (id BIGSERIAL PRIMARY KEY, account_id BIGINT NOT NULL, state relay_capacity_state NOT NULL, observed_at TIMESTAMPTZ NOT NULL, active_demand INTEGER NOT NULL, live_failure_domains INTEGER NOT NULL, effective_capacity INTEGER NOT NULL, remaining_capacity INTEGER NOT NULL, notified_at TIMESTAMPTZ);
		CREATE TABLE relay_capacity_alert_deliveries (event_id BIGINT NOT NULL, recipient TEXT NOT NULL, delivered_at TIMESTAMPTZ, PRIMARY KEY(event_id,recipient));
		INSERT INTO accounts VALUES (47,'MIT SCL','scl@example.edu');
		INSERT INTO users VALUES ('operator@example.com',true);
		INSERT INTO relay_groups VALUES (1,14);
	`); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, `
		INSERT INTO nodes VALUES
		  (1,47,'relay','active',$1,1,10),
		  (2,47,'relay','active',$1,1,10),
		  (3,47,'relay','active',$1,NULL,3),
		  (4,47,'relay','active',$2,NULL,99)
	`, now.Add(-time.Minute), now.Add(-3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recordings VALUES (1,47,'relay','active'),(2,47,'relay','active'),(3,47,'cloud','active')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO recording_jobs VALUES
		  (1,1,'leased',$1,$2,'continuous_window',$3,60),
		  (2,2,'pending',NULL,$2,'clip',NULL,60),
		  (3,3,'leased',$1,$2,'continuous_window',$3,60),
		  (4,1,'pending',NULL,$2,'clip',NULL,60)
	`, now.Add(time.Minute), now.Add(-30*time.Second), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	state, err := currentRelayCapacity(ctx, pool, now)
	if err != nil {
		t.Fatal(err)
	}
	if state.ActiveDemand != 3 || state.LiveFailureDomains != 2 || state.EffectiveCapacity != 17 || state.RemainingCapacity != 3 || state.State != relayCapacityHealthy || !state.ChangedAt.Equal(now) {
		t.Fatalf("capacity=%+v, want demand=3 including overlapping jobs from one recording, domains=2 effective=17 remaining=3 healthy", state)
	}
	if pending, err := recordRelayCapacity(ctx, pool, now); err != nil || len(pending) != 0 {
		t.Fatalf("healthy baseline pending=%v err=%v, want silent baseline", pending, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE nodes SET last_heartbeat_at=$1 WHERE id=3`, now.Add(-3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	pending, err := recordRelayCapacity(ctx, pool, now.Add(time.Second))
	if err != nil || len(pending) != 1 || pending[0].State != relayCapacityDegraded || pending[0].LiveFailureDomains != 1 || pending[0].RemainingCapacity != 0 {
		t.Fatalf("degraded transition=%v err=%v", pending, err)
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
		CREATE TABLE relay_groups (id BIGINT, account_id BIGINT, name TEXT NOT NULL, max_streams INTEGER NOT NULL, PRIMARY KEY(account_id,id));
		CREATE TABLE nodes (id BIGINT PRIMARY KEY, account_id BIGINT NOT NULL, node_type TEXT NOT NULL, display_name TEXT NOT NULL, hostname TEXT NOT NULL, status TEXT NOT NULL, last_heartbeat_at TIMESTAMPTZ, capabilities_jsonb JSONB NOT NULL DEFAULT '{}', relay_group_id BIGINT, relay_max_streams INTEGER NOT NULL);
		CREATE TABLE recordings (id BIGINT PRIMARY KEY, account_id BIGINT NOT NULL, capture_via TEXT NOT NULL, status TEXT NOT NULL);
		CREATE TABLE recording_jobs (id BIGSERIAL PRIMARY KEY, recording_id BIGINT, lease_owner TEXT, lease_expires_at TIMESTAMPTZ, status TEXT NOT NULL, kind TEXT NOT NULL, fire_at TIMESTAMPTZ NOT NULL, window_end_at TIMESTAMPTZ, clip_duration_sec INTEGER);
		CREATE TABLE connections (id BIGINT PRIMARY KEY, account_id BIGINT NOT NULL, kind TEXT NOT NULL, label TEXT NOT NULL, nas_storage_total_bytes BIGINT, nas_storage_free_bytes BIGINT, nas_storage_reported_at TIMESTAMPTZ);
		CREATE TABLE relay_connectivity_alert_states (node_id BIGINT PRIMARY KEY, observed_state relay_connectivity_state NOT NULL, observed_at TIMESTAMPTZ NOT NULL);
		CREATE TABLE relay_connectivity_alert_events (id BIGSERIAL PRIMARY KEY, account_id BIGINT NOT NULL, node_id BIGINT NOT NULL, state relay_connectivity_state NOT NULL, observed_at TIMESTAMPTZ NOT NULL, last_heartbeat_at TIMESTAMPTZ, notified_at TIMESTAMPTZ);
		CREATE TABLE relay_connectivity_alert_deliveries (event_id BIGINT NOT NULL, recipient TEXT NOT NULL, delivered_at TIMESTAMPTZ, PRIMARY KEY (event_id, recipient));
		INSERT INTO accounts VALUES (47, 'MIT SCL', 'scl@example.edu'), (2, 'Other Org', 'other@example.edu');
		INSERT INTO users VALUES ('deniz@aydemir.us', true);
		INSERT INTO relay_groups VALUES (3,47,'deniz-durham',20);
		INSERT INTO nodes (id,account_id,node_type,display_name,hostname,status,last_heartbeat_at,capabilities_jsonb,relay_group_id,relay_max_streams) VALUES
		  (7, 47, 'relay', 'MIT-MAC-1', 'mit-mac-1', 'active', '2026-07-22T12:00:00Z', '{"relay_version":"abc123","relay_started_at":"2026-07-22T11:00:00Z","active_jobs":2}', 3, 5),
		  (8, 2, 'relay', 'OTHER-RELAY', 'other-relay', 'active', '2026-07-22T12:00:00Z', '{}', NULL, 5);
	`); err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 7, 22, 12, 0, 30, 0, time.UTC)
	states, err := currentRelayConnectivity(ctx, pool, now)
	if err != nil || len(states) != 1 || states[0].OrgName != "MIT SCL" {
		t.Fatalf("alert-scoped relay states=%v err=%v", states, err)
	}
	if states[0].RelayVersion != "abc123" || states[0].RelayStartedAt != "2026-07-22T11:00:00Z" ||
		states[0].RelayGroupID == nil || *states[0].RelayGroupID != 3 || states[0].RelayGroupName != "deniz-durham" ||
		states[0].ReportedJobs != 2 || states[0].LiveLeases != 0 {
		t.Fatalf("relay diagnostics missing from state: %+v", states[0])
	}
	for _, capabilities := range []string{`{"active_jobs":1.5}`, `{"active_jobs":-1}`} {
		if _, err := pool.Exec(ctx, `UPDATE nodes SET capabilities_jsonb=$1 WHERE id=7`, capabilities); err != nil {
			t.Fatal(err)
		}
		malformed, err := currentRelayConnectivity(ctx, pool, now)
		if err != nil || len(malformed) != 1 || malformed[0].ReportedJobs != 0 {
			t.Fatalf("malformed reported jobs capabilities=%s states=%v err=%v", capabilities, malformed, err)
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE nodes SET capabilities_jsonb='{"relay_version":"abc123","relay_started_at":"2026-07-22T11:00:00Z","active_jobs":2}' WHERE id=7`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE nodes SET last_heartbeat_at=$1 WHERE id=7`, now.Add(-4*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO recording_jobs (lease_owner,lease_expires_at,status,kind,fire_at,window_end_at)
		VALUES ('node:7',$1,'leased','continuous_window',$2,$3)
	`, now.AddDate(10, 0, 0), now.Add(-time.Hour), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	states, err = currentRelayConnectivity(ctx, pool, now)
	if err != nil || len(states) != 1 || !states[0].ActiveCapture || states[0].State != relayOffline || states[0].LiveLeases != 1 {
		t.Fatalf("capturing stale relay states=%v err=%v, want fast offline", states, err)
	}
	output := captureStdout(t, func() {
		testDatabaseURL, err := url.Parse(cfg.ConnString())
		if err != nil {
			t.Fatal(err)
		}
		query := testDatabaseURL.Query()
		query.Set("search_path", schema)
		testDatabaseURL.RawQuery = query.Encode()
		runRelayConnectivity(ctx, appconfig.Config{DatabaseURL: testDatabaseURL.String()}, []string{"run", "--dry-run"})
	})
	var dryRun struct {
		DryRun bool                          `json:"dry_run"`
		Relays []relayConnectivityTransition `json:"relays"`
	}
	if err := json.Unmarshal(output, &dryRun); err != nil || !dryRun.DryRun || len(dryRun.Relays) != 1 {
		t.Fatalf("dry-run output=%s err=%v", output, err)
	}
	if relay := dryRun.Relays[0]; relay.RelayStartedAt != "2026-07-22T11:00:00Z" || relay.RelayGroupID == nil || *relay.RelayGroupID != 3 || relay.LiveLeases != 1 {
		t.Fatalf("dry-run relay diagnostics=%+v", relay)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_jobs SET lease_expires_at=$1`, now.Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	states, err = currentRelayConnectivity(ctx, pool, now)
	if err != nil || len(states) != 1 || states[0].ActiveCapture || states[0].State != relayOnline || states[0].LiveLeases != 0 {
		t.Fatalf("expired lease states=%v err=%v, want idle-threshold online", states, err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM recording_jobs`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE nodes SET last_heartbeat_at=$1 WHERE id=7`, now); err != nil {
		t.Fatal(err)
	}
	if got, err := recordRelayConnectivity(ctx, pool, now); err != nil || len(got) != 0 {
		t.Fatalf("baseline transitions=%v err=%v, want none", got, err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_connectivity_alert_events (account_id, node_id, state, observed_at)
		VALUES (47, 7, 'offline', $1), (2, 8, 'offline', $1)
	`, now); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE nodes SET account_id=2 WHERE id=7`); err != nil {
		t.Fatal(err)
	}
	pending, err := pendingRelayConnectivity(ctx, pool)
	if err != nil || len(pending) != 1 || pending[0].EventID != 1 || pending[0].OrgName != "MIT SCL" {
		t.Fatalf("account-snapshotted pending transitions=%v err=%v", pending, err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE nodes SET account_id=47 WHERE id=7;
		DELETE FROM relay_connectivity_alert_events;
		ALTER SEQUENCE relay_connectivity_alert_events_id_seq RESTART WITH 1;
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE nodes SET last_heartbeat_at=$1 WHERE id=7`, now.Add(-relayIdleOnlineThreshold)); err != nil {
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

func captureStdout(t *testing.T, run func()) []byte {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	previous := os.Stdout
	os.Stdout = writer
	run()
	os.Stdout = previous
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	return output
}
