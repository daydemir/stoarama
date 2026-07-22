CREATE TYPE relay_connectivity_state AS ENUM ('online', 'offline');

-- One durable state machine per active relay. The first observation is a silent
-- baseline, so deploying this monitor does not produce an alert storm for an
-- already-running fleet.
CREATE TABLE relay_connectivity_alert_states (
  node_id        BIGINT PRIMARY KEY REFERENCES nodes(id) ON DELETE CASCADE,
  observed_state relay_connectivity_state NOT NULL,
  observed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Transitions are queued separately so a temporary email failure cannot erase
-- an offline event when the relay recovers before delivery resumes.
CREATE TABLE relay_connectivity_alert_events (
  id          BIGSERIAL PRIMARY KEY,
  node_id     BIGINT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
  state       relay_connectivity_state NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  last_heartbeat_at TIMESTAMPTZ,
  notified_at TIMESTAMPTZ
);

CREATE INDEX relay_connectivity_alert_events_pending_idx
  ON relay_connectivity_alert_events (id)
  WHERE notified_at IS NULL;

CREATE INDEX relay_connectivity_alert_events_node_id_idx
  ON relay_connectivity_alert_events (node_id);

CREATE TABLE relay_connectivity_alert_deliveries (
  event_id    BIGINT NOT NULL REFERENCES relay_connectivity_alert_events(id) ON DELETE CASCADE,
  recipient   TEXT NOT NULL,
  delivered_at TIMESTAMPTZ,
  PRIMARY KEY (event_id, recipient)
);

-- Capacity is server-owned in nodes.relay_max_streams. Remove the obsolete
-- client ceiling from telemetry so operators never see two competing limits.
UPDATE nodes
SET capabilities_jsonb = capabilities_jsonb - 'max_concurrent_streams'
WHERE node_type = 'relay' AND capabilities_jsonb ? 'max_concurrent_streams';
