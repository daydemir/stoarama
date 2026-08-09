CREATE TYPE relay_capacity_state AS ENUM ('healthy', 'degraded');

CREATE TABLE relay_capacity_alert_states (
  account_id BIGINT PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
  observed_state relay_capacity_state NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE relay_capacity_alert_events (
  id BIGSERIAL PRIMARY KEY,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  state relay_capacity_state NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  active_demand INTEGER NOT NULL CHECK (active_demand >= 0),
  live_failure_domains INTEGER NOT NULL CHECK (live_failure_domains >= 0),
  effective_capacity INTEGER NOT NULL CHECK (effective_capacity >= 0),
  remaining_capacity INTEGER NOT NULL CHECK (remaining_capacity >= 0),
  notified_at TIMESTAMPTZ
);

CREATE INDEX relay_capacity_alert_events_pending_idx
  ON relay_capacity_alert_events (account_id, id)
  WHERE notified_at IS NULL;

CREATE TABLE relay_capacity_alert_deliveries (
  event_id BIGINT NOT NULL REFERENCES relay_capacity_alert_events(id) ON DELETE CASCADE,
  recipient TEXT NOT NULL,
  delivered_at TIMESTAMPTZ,
  PRIMARY KEY (event_id, recipient)
);
