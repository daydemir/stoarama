CREATE TYPE nas_storage_capacity_state AS ENUM ('healthy', 'warning', 'critical', 'unknown');

CREATE TABLE nas_storage_capacity_alert_states (
  connection_id BIGINT PRIMARY KEY REFERENCES connections(id) ON DELETE CASCADE,
  observed_state nas_storage_capacity_state NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE nas_storage_capacity_alert_events (
  id BIGSERIAL PRIMARY KEY,
  connection_id BIGINT NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
  state nas_storage_capacity_state NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  total_bytes BIGINT,
  free_bytes BIGINT,
  storage_reported_at TIMESTAMPTZ,
  notified_at TIMESTAMPTZ,
  CONSTRAINT chk_nas_storage_capacity_event_bytes CHECK (
    (total_bytes IS NULL AND free_bytes IS NULL)
    OR (total_bytes > 0 AND free_bytes >= 0 AND free_bytes <= total_bytes)
  )
);

CREATE INDEX nas_storage_capacity_alert_events_pending_idx
  ON nas_storage_capacity_alert_events (connection_id, id)
  WHERE notified_at IS NULL;

CREATE TABLE nas_storage_capacity_alert_deliveries (
  event_id BIGINT NOT NULL REFERENCES nas_storage_capacity_alert_events(id) ON DELETE CASCADE,
  recipient TEXT NOT NULL,
  delivered_at TIMESTAMPTZ,
  PRIMARY KEY (event_id, recipient)
);
